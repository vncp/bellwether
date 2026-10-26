//! `joins`: two-dataset corpora for join keys (B3), in both bench shapes,
//! with 1:N skew, recorded semi-join key sets, and exact ground truth.
//!
//! # Shapes
//!
//! Both datasets sit beside each other under `<out>/` (a join key names its
//! right dataset by a folder beside the data root):
//!
//! - Ledger ([`JoinShape::Ledger`]): `<out>/orders/part-NNNN.parquet`
//!   (`order_id: Int64`, `account_id: Int64`, `amount: Decimal128(18, 2)`),
//!   one row per order, in order-id order; and `<out>/payables/part-NNNN.parquet`
//!   (`order_id: Int64`, `payable_id: Int64`, `amount: Decimal128(18, 2)`),
//!   in payment-time order. Two large mapping tables joined on an id.
//! - Market ([`JoinShape::Market`]): `<out>/trades/part-NNNN.parquet`
//!   (`symbol: Utf8`, `minute: Int64`, `price: Decimal128(18, 4)`,
//!   `qty: Int64`) and `<out>/quotes/part-NNNN.parquet` (`symbol`,
//!   `minute`, `bid`, `ask`), both in minute order: trades and quotes on
//!   `(symbol, ts bucket)`, the bucket a whole minute.
//!
//! # 1:N skew
//!
//! Both shapes have a cutoff at `settled` of the way through: orders after
//! it are not paid yet (no payables), and minutes after it are after the
//! close (quotes, no trades). Keys past the cutoff are one-sided and sit
//! together, as the most recent period's keys do.
//!
//! Ledger: an order before the cutoff has payables with probability
//! `coverage`, then
//! `1 + ⌊fanout_max × u⁶⌋` of them (`u` uniform: most orders have one, a few
//! have many), spaced `spread` orders apart in payment time, so one order's
//! payables reach over neighbouring row groups; `right_only` of the payment
//! positions also carry a payable of an order outside the left dataset.
//! Orders without payables are left-only.
//!
//! Market: symbol `s` (of `symbols`, by rank) trades in a minute with
//! probability `1 / (s + 1)^0.7` and is quoted with probability
//! `min(1, 2 / (s + 1)^0.5)`, `1 + ⌊fanout_max / (s + 1)⌋` quotes when it is:
//! a hot symbol is quoted many times every minute, a cold one rarely, and
//! most quoted minutes have no trade (right-only keys).
//!
//! # Workloads and truth
//!
//! `workload/semi-<n>.parquet` for each `n` in `keysets` holds `n` distinct
//! keys on the left dataset's join columns (named as the left names them),
//! drawn uniformly from a window of `2n` consecutive keys (ledger: order
//! ids; market: every symbol over consecutive minutes) centred on the
//! cutoff, as the keys of a recent period are: the keys a semi-join (`in
//! (subquery)`) asks for. About half are past the cutoff, on one side only,
//! and some before it are too (an order with no payable, a quoted minute
//! without a trade). The sizes bracket a membership gate: the preset
//! 10, 10³, 10⁵ sit below, at, and above an `in` list of 1,000.
//! `workload/joins.json` lists them with the join key; `workload/truth.json`
//! gives, per run, the keys both datasets hold, and per side the row groups
//! holding one of those keys (the minimum a semi-join reads) and their rows,
//! and the row groups holding any asked key (what a locate of every key
//! reads), all computed while the rows are generated, never from an index.
//!
//! # Memory (ADR 018)
//!
//! One row group's rows at a time per dataset, plus, per asked key, the row
//! groups holding it; the ledger's payment-time buffer holds at most
//! `spread × fanout_max` orders' payables.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, Decimal128Builder, Int64Builder, RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::stats::chacha;
use crate::{io_err, write_json, BenchError, Result};

/// Format tag of `workload/joins.json`.
pub const JOIN_WORKLOAD_FORMAT: &str = "bellwether-bench-join-workload/1";
/// Format tag of `workload/truth.json`.
pub const JOIN_TRUTH_FORMAT: &str = "bellwether-bench-join-truth/1";

/// Which bench shape the corpus takes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinShape {
    /// Orders and payables on `order_id`.
    Ledger,
    /// Trades and quotes on `(symbol, minute)`.
    Market,
}

impl JoinShape {
    fn tag(self) -> u64 {
        match self {
            Self::Ledger => 1,
            Self::Market => 2,
        }
    }

    /// The left and right datasets' folder names.
    #[must_use]
    pub fn datasets(self) -> (&'static str, &'static str) {
        match self {
            Self::Ledger => ("orders", "payables"),
            Self::Market => ("trades", "quotes"),
        }
    }

    /// The join columns, the same names on both sides.
    #[must_use]
    pub fn columns(self) -> Vec<String> {
        match self {
            Self::Ledger => vec!["order_id".into()],
            Self::Market => vec!["symbol".into(), "minute".into()],
        }
    }

    fn schemas(self) -> (SchemaRef, SchemaRef) {
        let dec = |s| DataType::Decimal128(18, s);
        match self {
            Self::Ledger => (
                Arc::new(Schema::new(vec![
                    Field::new("order_id", DataType::Int64, false),
                    Field::new("account_id", DataType::Int64, false),
                    Field::new("amount", dec(2), false),
                ])),
                Arc::new(Schema::new(vec![
                    Field::new("order_id", DataType::Int64, false),
                    Field::new("payable_id", DataType::Int64, false),
                    Field::new("amount", dec(2), false),
                ])),
            ),
            Self::Market => (
                Arc::new(Schema::new(vec![
                    Field::new("symbol", DataType::Utf8, false),
                    Field::new("minute", DataType::Int64, false),
                    Field::new("price", dec(4), false),
                    Field::new("qty", DataType::Int64, false),
                ])),
                Arc::new(Schema::new(vec![
                    Field::new("symbol", DataType::Utf8, false),
                    Field::new("minute", DataType::Int64, false),
                    Field::new("bid", dec(4), false),
                    Field::new("ask", dec(4), false),
                ])),
            ),
        }
    }
}

/// The corpus parameters.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JoinParams {
    /// The shape.
    pub shape: JoinShape,
    /// RNG seed (ChaCha12 via [`crate::stats::chacha`]).
    pub seed: u64,
    /// Ledger: orders. Market: minutes.
    pub size: u64,
    /// Market: symbols (unused by the ledger).
    pub symbols: u32,
    /// Rows per row group, both datasets.
    pub rows_per_row_group: u32,
    /// Row groups per file, both datasets.
    pub row_groups_per_file: u32,
    /// Ledger: the probability an order has payables.
    pub coverage: f64,
    /// The most right rows one key has.
    pub fanout_max: u32,
    /// Ledger: orders between two payables of one order, in payment time.
    pub spread: u32,
    /// Ledger: the fraction of payment positions that also carry a payable
    /// of an order outside the left dataset.
    pub right_only: f64,
    /// Where the cutoff is, as a fraction of orders (ledger) or minutes
    /// (market): after it, orders have no payables and minutes no trades.
    pub settled: f64,
    /// Keys per recorded semi-join run.
    pub keysets: Vec<u32>,
}

impl JoinParams {
    /// The preset for `shape`.
    #[must_use]
    pub fn preset(shape: JoinShape) -> Self {
        match shape {
            JoinShape::Ledger => Self {
                shape,
                seed: 3,
                size: 400_000,
                symbols: 0,
                rows_per_row_group: 5_000,
                row_groups_per_file: 4,
                coverage: 0.6,
                fanout_max: 40,
                spread: 50,
                right_only: 0.1,
                settled: 0.8,
                keysets: vec![10, 1_000, 100_000],
            },
            JoinShape::Market => Self {
                shape,
                seed: 3,
                size: 2_000,
                symbols: 400,
                rows_per_row_group: 5_000,
                row_groups_per_file: 4,
                coverage: 0.0,
                fanout_max: 24,
                spread: 0,
                right_only: 0.0,
                settled: 0.8,
                keysets: vec![10, 1_000, 100_000],
            },
        }
    }

    /// Refuses parameters that make no sense, saying which.
    pub fn validate(&self) -> Result<()> {
        let bad = |m: &str| Err(BenchError::Invalid(m.into()));
        if self.size == 0 || self.rows_per_row_group == 0 || self.row_groups_per_file == 0 {
            return bad("size, rows per row group, and row groups per file must be positive");
        }
        if self.shape == JoinShape::Market && self.symbols == 0 {
            return bad("the market shape needs at least one symbol");
        }
        if [self.coverage, self.right_only, self.settled]
            .iter()
            .any(|f| !(0.0..=1.0).contains(f))
        {
            return bad("coverage, right_only, and settled are fractions in [0, 1]");
        }
        if self.fanout_max == 0 {
            return bad("fanout_max must be at least 1");
        }
        if self.keysets.is_empty() || self.keysets.contains(&0) {
            return bad("give at least one keyset size, each positive");
        }
        Ok(())
    }

    /// Distinct keys the runs draw from: every order id, or every symbol
    /// in every minute.
    fn key_space(&self) -> u64 {
        match self.shape {
            JoinShape::Ledger => self.size,
            JoinShape::Market => self.size * u64::from(self.symbols),
        }
    }

    /// The cutoff, in keys of [`JoinParams::key_space`].
    fn cutoff(&self) -> u64 {
        (self.key_space() as f64 * self.settled) as u64
    }
}

/// One recorded semi-join run (`workload/joins.json`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinRun {
    /// `semi-<n>`.
    pub id: String,
    /// Its key file, relative to `<out>`.
    pub file: String,
    /// Keys asked.
    pub keys: u64,
}

/// `workload/joins.json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinWorkload {
    /// [`JOIN_WORKLOAD_FORMAT`].
    pub format: String,
    /// The left dataset's folder under `<out>`.
    pub left: String,
    /// The right dataset's folder under `<out>`.
    pub right: String,
    /// The join columns (the same names on both sides).
    pub columns: Vec<String>,
    /// The join key on the pair, as `index --key` takes it.
    pub spec: String,
    /// The runs.
    pub runs: Vec<JoinRun>,
}

/// One dataset's size.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SideSummary {
    /// Files written.
    pub files: u64,
    /// Row groups written.
    pub row_groups: u64,
    /// Rows written.
    pub rows: u64,
    /// On-disk bytes of the data files.
    pub bytes: u64,
    /// Distinct join keys.
    pub keys: u64,
}

/// The corpus.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinSummary {
    /// The shape.
    pub shape: JoinShape,
    /// The left dataset.
    pub left: SideSummary,
    /// The right dataset.
    pub right: SideSummary,
    /// Keys on both sides.
    pub both: u64,
    /// Right rows per shared key: nearest-rank p50, p95, max (the 1:N
    /// skew).
    pub right_rows_per_key: [u64; 3],
}

/// One side of one run's truth.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SideTruth {
    /// `(file relative to the dataset folder, row group)` holding a key both
    /// datasets hold: what a semi-join of the run reads at least.
    pub row_groups: Vec<(String, u32)>,
    /// Rows of those keys.
    pub rows: u64,
    /// Row groups holding any asked key: what a locate of every asked key
    /// reads at least.
    pub located: u64,
}

/// One run's truth.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinRunTruth {
    /// The run.
    pub id: String,
    /// Keys asked.
    pub keys: u64,
    /// Of those, keys both datasets hold.
    pub matched: u64,
    /// The left dataset.
    pub left: SideTruth,
    /// The right dataset.
    pub right: SideTruth,
}

/// `workload/truth.json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinTruth {
    /// [`JOIN_TRUTH_FORMAT`].
    pub format: String,
    /// The corpus.
    pub summary: JoinSummary,
    /// Per run.
    pub runs: Vec<JoinRunTruth>,
}

/// A row's join key, as the truth keys it.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Key {
    Id(i64),
    SymbolMinute(u32, i64),
}

fn symbol(s: u32) -> String {
    format!("S{s:04}")
}

fn writer_properties(rows: u32) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_row_count(Some(rows as usize + 1))
        .set_created_by("bellwether-bench joins".to_string())
        .build()
}

/// One row of either dataset: its key and its other values.
struct Row {
    key: Key,
    a: i64,
    b: i128,
    c: i128,
}

/// Writes one dataset, a row group of `rows_per_row_group` rows at a time,
/// `row_groups_per_file` per file, and records where each asked key is.
struct SideWriter<'a> {
    dir: PathBuf,
    shape: JoinShape,
    schema: SchemaRef,
    props: WriterProperties,
    rows_per_rg: usize,
    rgs_per_file: u32,
    pending: Vec<Row>,
    file: Option<(ArrowWriter<BufWriter<File>>, String)>,
    file_no: u32,
    rg_in_file: u32,
    summary: SideSummary,
    keys_seen: HashMap<Key, u64>,
    asked: &'a HashMap<Key, Vec<usize>>,
    /// Per asked key: the row groups holding it and its rows.
    found: HashMap<Key, (BTreeSet<(String, u32)>, u64)>,
}

impl<'a> SideWriter<'a> {
    fn new(
        dir: PathBuf,
        shape: JoinShape,
        schema: SchemaRef,
        p: &JoinParams,
        asked: &'a HashMap<Key, Vec<usize>>,
    ) -> Result<Self> {
        std::fs::create_dir_all(&dir).map_err(io_err(&dir))?;
        Ok(Self {
            dir,
            shape,
            schema,
            props: writer_properties(p.rows_per_row_group),
            rows_per_rg: p.rows_per_row_group as usize,
            rgs_per_file: p.row_groups_per_file,
            pending: Vec::new(),
            file: None,
            file_no: 0,
            rg_in_file: 0,
            summary: SideSummary::default(),
            keys_seen: HashMap::new(),
            asked,
            found: HashMap::new(),
        })
    }

    fn push(&mut self, row: Row) -> Result<()> {
        self.pending.push(row);
        if self.pending.len() == self.rows_per_rg {
            self.flush_rg()?;
        }
        Ok(())
    }

    fn flush_rg(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        if self.file.is_none() {
            let rel = format!("part-{:04}.parquet", self.file_no);
            let path = self.dir.join(&rel);
            let f = File::create(&path).map_err(io_err(&path))?;
            let w = ArrowWriter::try_new(
                BufWriter::new(f),
                Arc::clone(&self.schema),
                Some(self.props.clone()),
            )?;
            self.file = Some((w, rel));
            self.summary.files += 1;
        }
        let rows = std::mem::take(&mut self.pending);
        let rg = self.rg_in_file;
        let rel = self
            .file
            .as_ref()
            .map(|(_, r)| r.clone())
            .unwrap_or_default();
        for r in &rows {
            *self.keys_seen.entry(r.key.clone()).or_default() += 1;
            if self.asked.contains_key(&r.key) {
                let e = self.found.entry(r.key.clone()).or_default();
                e.0.insert((rel.clone(), rg));
                e.1 += 1;
            }
        }
        let batch = self.batch(&rows)?;
        if let Some((w, _)) = self.file.as_mut() {
            w.write(&batch)?;
            w.flush()?;
        }
        self.summary.rows += rows.len() as u64;
        self.summary.row_groups += 1;
        self.rg_in_file += 1;
        if self.rg_in_file == self.rgs_per_file {
            self.close_file()?;
        }
        Ok(())
    }

    fn close_file(&mut self) -> Result<()> {
        if let Some((w, rel)) = self.file.take() {
            w.close()?;
            let path = self.dir.join(rel);
            self.summary.bytes += std::fs::metadata(&path).map_err(io_err(&path))?.len();
            self.file_no += 1;
            self.rg_in_file = 0;
        }
        Ok(())
    }

    fn batch(&self, rows: &[Row]) -> Result<RecordBatch> {
        let dec = |scale: i8, f: &dyn Fn(&Row) -> i128| -> Result<ArrayRef> {
            let mut b = Decimal128Builder::with_capacity(rows.len());
            for r in rows {
                b.append_value(f(r));
            }
            Ok(Arc::new(b.finish().with_precision_and_scale(18, scale)?))
        };
        let int = |f: &dyn Fn(&Row) -> i64| -> ArrayRef {
            let mut b = Int64Builder::with_capacity(rows.len());
            for r in rows {
                b.append_value(f(r));
            }
            Arc::new(b.finish())
        };
        let id = |r: &Row| match r.key {
            Key::Id(i) => i,
            Key::SymbolMinute(_, m) => m,
        };
        let cols: Vec<ArrayRef> = match self.shape {
            JoinShape::Ledger => vec![int(&id), int(&|r| r.a), dec(2, &|r| r.b)?],
            JoinShape::Market => {
                let mut sym = StringBuilder::with_capacity(rows.len(), rows.len() * 5);
                for r in rows {
                    if let Key::SymbolMinute(s, _) = r.key {
                        sym.append_value(symbol(s));
                    }
                }
                vec![
                    Arc::new(sym.finish()),
                    int(&id),
                    dec(4, &|r| r.b)?,
                    if self.schema.field(3).data_type() == &DataType::Int64 {
                        int(&|r| r.a)
                    } else {
                        dec(4, &|r| r.c)?
                    },
                ]
            }
        };
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), cols)?)
    }

    fn finish(mut self) -> Result<Done> {
        self.flush_rg()?;
        self.close_file()?;
        self.summary.keys = self.keys_seen.len() as u64;
        Ok(Done {
            summary: self.summary,
            keys: self.keys_seen,
            found: self.found,
        })
    }
}

struct Done {
    summary: SideSummary,
    keys: HashMap<Key, u64>,
    found: HashMap<Key, (BTreeSet<(String, u32)>, u64)>,
}

/// Generates both datasets, the workload, and the truth under `out` (which
/// must not hold a corpus).
pub fn generate_joins(out: &Path, p: &JoinParams) -> Result<JoinTruth> {
    p.validate()?;
    let (left_name, right_name) = p.shape.datasets();
    if out.join(left_name).exists() || out.join(right_name).exists() {
        return Err(BenchError::Invalid(format!(
            "{} already holds a corpus; pass an empty --out",
            out.display()
        )));
    }
    // The runs first: their keys depend on the parameters only.
    let mut rng = chacha(&[p.seed, p.shape.tag(), 0x4a4f_494e_0001]);
    let space = p.key_space();
    let mut runs: Vec<(JoinRun, Vec<Key>)> = Vec::new();
    let mut asked: HashMap<Key, Vec<usize>> = HashMap::new();
    for (i, &n) in p.keysets.iter().enumerate() {
        let n = u64::from(n).min(space);
        let window = (2 * n).min(space);
        let start = p.cutoff().saturating_sub(n).min(space - window);
        let mut keys: Vec<Key> = rand::seq::index::sample(&mut rng, window as usize, n as usize)
            .into_iter()
            .map(|k| start + k as u64)
            .map(|k| match p.shape {
                JoinShape::Ledger => Key::Id(k as i64),
                JoinShape::Market => Key::SymbolMinute(
                    (k % u64::from(p.symbols)) as u32,
                    (k / u64::from(p.symbols)) as i64,
                ),
            })
            .collect();
        keys.sort();
        for k in &keys {
            asked.entry(k.clone()).or_default().push(i);
        }
        runs.push((
            JoinRun {
                id: format!("semi-{n}"),
                file: format!("workload/semi-{n}.parquet"),
                keys: n,
            },
            keys,
        ));
    }
    let (ls, rs) = p.shape.schemas();
    let mut left = SideWriter::new(out.join(left_name), p.shape, ls, p, &asked)?;
    let mut right = SideWriter::new(out.join(right_name), p.shape, rs, p, &asked)?;
    let mut rng = chacha(&[p.seed, p.shape.tag(), 0x4a4f_494e_0002]);
    match p.shape {
        JoinShape::Ledger => ledger(p, &mut rng, &mut left, &mut right)?,
        JoinShape::Market => market(p, &mut rng, &mut left, &mut right)?,
    }
    let (l, r) = (left.finish()?, right.finish()?);
    let mut per_key: Vec<u64> = l
        .keys
        .keys()
        .filter_map(|k| r.keys.get(k).copied())
        .collect();
    per_key.sort_unstable();
    let summary = JoinSummary {
        shape: p.shape,
        both: per_key.len() as u64,
        right_rows_per_key: nearest_rank(&per_key),
        left: l.summary.clone(),
        right: r.summary.clone(),
    };
    let mut truth_runs = Vec::with_capacity(runs.len());
    for (run, keys) in &runs {
        let side = |d: &Done, matched: &[&Key]| {
            let mut rgs: BTreeSet<(String, u32)> = BTreeSet::new();
            let mut rows = 0;
            for k in matched {
                if let Some((g, n)) = d.found.get(*k) {
                    rgs.extend(g.iter().cloned());
                    rows += n;
                }
            }
            let located: BTreeSet<&(String, u32)> = keys
                .iter()
                .filter_map(|k| d.found.get(k))
                .flat_map(|(g, _)| g.iter())
                .collect();
            SideTruth {
                row_groups: rgs.into_iter().collect(),
                rows,
                located: located.len() as u64,
            }
        };
        let matched: Vec<&Key> = keys
            .iter()
            .filter(|k| l.found.contains_key(*k) && r.found.contains_key(*k))
            .collect();
        truth_runs.push(JoinRunTruth {
            id: run.id.clone(),
            keys: run.keys,
            matched: matched.len() as u64,
            left: side(&l, &matched),
            right: side(&r, &matched),
        });
        write_keys(&out.join(&run.file), p.shape, keys)?;
    }
    let columns = p.shape.columns();
    write_json(
        &out.join("workload/joins.json"),
        &JoinWorkload {
            format: JOIN_WORKLOAD_FORMAT.into(),
            left: left_name.into(),
            right: right_name.into(),
            spec: format!(
                "join:{left_name}.{}={right_name}.{}",
                columns.join(","),
                columns.join(",")
            ),
            columns,
            runs: runs.iter().map(|(r, _)| r.clone()).collect(),
        },
    )?;
    let truth = JoinTruth {
        format: JOIN_TRUTH_FORMAT.into(),
        summary,
        runs: truth_runs,
    };
    write_json(&out.join("workload/truth.json"), &truth)?;
    Ok(truth)
}

/// Orders in id order; payables in payment time, an order's `c` payables
/// `spread` positions apart from its own position on.
fn ledger(
    p: &JoinParams,
    rng: &mut impl Rng,
    left: &mut SideWriter<'_>,
    right: &mut SideWriter<'_>,
) -> Result<()> {
    let n = p.size as i64;
    let settled = p.cutoff() as i64;
    let spread = i64::from(p.spread.max(1));
    let mut due: BTreeMap<i64, Vec<Row>> = BTreeMap::new();
    let mut payable = 0i64;
    for k in 0..n {
        left.push(Row {
            key: Key::Id(k),
            a: k % 9_973,
            b: i128::from(rng.gen_range(100..1_000_000i64)),
            c: 0,
        })?;
        if k < settled && rng.gen_bool(p.coverage) {
            let u: f64 = rng.r#gen();
            let c = 1 + (f64::from(p.fanout_max) * u.powi(6)) as i64;
            for j in 0..c.min(i64::from(p.fanout_max)) {
                payable += 1;
                due.entry(k + j * spread).or_default().push(Row {
                    key: Key::Id(k),
                    a: payable,
                    b: i128::from(rng.gen_range(1..100_000i64)),
                    c: 0,
                });
            }
        }
        if rng.gen_bool(p.right_only) {
            payable += 1;
            due.entry(k).or_default().push(Row {
                key: Key::Id(n + k),
                a: payable,
                b: i128::from(rng.gen_range(1..100_000i64)),
                c: 0,
            });
        }
        // Payment time `k` is complete: no later order pays before it.
        while let Some(entry) = due.first_entry() {
            if *entry.key() > k {
                break;
            }
            for r in entry.remove() {
                right.push(r)?;
            }
        }
    }
    for (_, rows) in due {
        for r in rows {
            right.push(r)?;
        }
    }
    Ok(())
}

/// Trades and quotes, minute by minute, symbols by rank within a minute.
fn market(
    p: &JoinParams,
    rng: &mut impl Rng,
    left: &mut SideWriter<'_>,
    right: &mut SideWriter<'_>,
) -> Result<()> {
    let close = (p.size as f64 * p.settled) as i64;
    for m in 0..p.size as i64 {
        for s in 0..p.symbols {
            let rank = f64::from(s + 1);
            let px = i128::from(10_000 + i64::from(s) * 37 + m % 500);
            if m < close && rng.gen_bool(rank.powf(-0.7)) {
                for t in 0..rng.gen_range(1..=3i64) {
                    left.push(Row {
                        key: Key::SymbolMinute(s, m),
                        a: 100 * (t + 1),
                        b: px,
                        c: 0,
                    })?;
                }
            }
            if rng.gen_bool((2.0 * rank.powf(-0.5)).min(1.0)) {
                let q = 1 + p.fanout_max / (s + 1);
                for i in 0..q {
                    right.push(Row {
                        key: Key::SymbolMinute(s, m),
                        a: 0,
                        b: px - 1 - i128::from(i % 3),
                        c: px + 1 + i128::from(i % 3),
                    })?;
                }
            }
        }
    }
    Ok(())
}

/// A run's key file: the left's join columns, as the left names them.
fn write_keys(path: &Path, shape: JoinShape, keys: &[Key]) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(io_err(dir))?;
    }
    let (schema, cols): (SchemaRef, Vec<ArrayRef>) = match shape {
        JoinShape::Ledger => {
            let mut b = Int64Builder::with_capacity(keys.len());
            for k in keys {
                if let Key::Id(i) = k {
                    b.append_value(*i);
                }
            }
            (
                Arc::new(Schema::new(vec![Field::new(
                    "order_id",
                    DataType::Int64,
                    false,
                )])),
                vec![Arc::new(b.finish())],
            )
        }
        JoinShape::Market => {
            let mut sym = StringBuilder::new();
            let mut min = Int64Builder::with_capacity(keys.len());
            for k in keys {
                if let Key::SymbolMinute(s, m) = k {
                    sym.append_value(symbol(*s));
                    min.append_value(*m);
                }
            }
            (
                Arc::new(Schema::new(vec![
                    Field::new("symbol", DataType::Utf8, false),
                    Field::new("minute", DataType::Int64, false),
                ])),
                vec![Arc::new(sym.finish()), Arc::new(min.finish())],
            )
        }
    };
    let f = File::create(path).map_err(io_err(path))?;
    let mut w = ArrowWriter::try_new(BufWriter::new(f), Arc::clone(&schema), None)?;
    w.write(&RecordBatch::try_new(schema, cols)?)?;
    w.close()?;
    Ok(())
}

/// Nearest-rank p50, p95, max of a sorted slice (zeros when empty).
fn nearest_rank(sorted: &[u64]) -> [u64; 3] {
    if sorted.is_empty() {
        return [0; 3];
    }
    let at = |q: f64| sorted[crate::stats::nearest_rank_index(q, sorted.len())];
    [at(0.5), at(0.95), sorted[sorted.len() - 1]]
}
