//! `lists`: corpora with a list column, for list keys (B4), in both bench
//! shapes, with recorded membership workloads and exact ground truth.
//!
//! # Shapes
//!
//! - Ledger ([`ListShape::Ledger`]): `<out>/data/part-NNNN.parquet`, columns
//!   `account_id: Int64`, `version: Int64`, `amount: Decimal128(18, 6)`,
//!   `attributes: List<Int64>`: attribute ids per ledger row, membership over
//!   array attributes where the term list is large.
//! - Market ([`ListShape::Market`]): `<out>/data/day=YYYY-MM-DD/part-0000.parquet`
//!   (hive directories, one file per day), columns `basket: Utf8`,
//!   `ts: Timestamp(µs, "UTC")`, `holdings: List<Utf8>`: instrument ids per
//!   basket and time, as index constituents or basket holdings per date.
//!
//! # Skewed element cardinality
//!
//! A non-empty list holds `1..=elements_max` elements (duplicates allowed).
//! Each element is drawn, with probability `head_fraction`, from a shared
//! head of `head` values with weight `1 / (rank + 1)` (so the first few are
//! in nearly every row group), and otherwise from a long tail of
//! `vocabulary - head` values whose draws follow the row's position: a row
//! at fraction `f` of the corpus draws uniformly from a window of tail
//! values starting at `f × tail`, two row groups wide, so a tail value sits
//! in a few neighbouring row groups (attributes or constituents that change
//! slowly over time). `null_fraction` of lists are null and
//! `empty_fraction` empty (neither is indexed, ADR 014).
//!
//! # Workloads and truth
//!
//! `workload/any-<n>.parquet` for each `n` in `terms` holds `n` distinct
//! element values drawn uniformly from the vocabulary (some may be absent
//! from the data), in one column named after the list column, of its
//! element type: a `contains_any` (`has_any`) run whose terms are spread
//! over the whole corpus. `workload/near-<n>.parquet` holds `n` consecutive
//! tail values starting a quarter of the way into the tail (at most the
//! tail): a `contains_any` run whose terms belong to one period, as the
//! constituents of one index on nearby dates do. `workload/all-2.parquet`
//! holds the most frequent head value and one tail value present in the
//! data: a `contains_all` run. `workload/lists.json` lists them;
//! `workload/truth.json` gives, per run, every `(file, row group)` holding a
//! matching row and the matching row count, computed while the rows are
//! generated, never from an index.
//!
//! # Memory (ADR 018)
//!
//! One row group's rows at a time, plus one row-group bitmap per element
//! value (bounded by the vocabulary) for the element statistics.

use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Decimal128Builder, Int64Builder, ListBuilder, RecordBatch, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use rand::Rng;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use crate::corpus::{civil_from_days, days_from_civil, DAY_US};
use crate::stats::chacha;
use crate::{io_err, write_json, BenchError, Result};

/// Format tag of `workload/lists.json`.
pub const LIST_WORKLOAD_FORMAT: &str = "bellwether-bench-list-workload/1";
/// Format tag of `workload/truth.json`.
pub const LIST_TRUTH_FORMAT: &str = "bellwether-bench-list-truth/1";
/// The market shape's first day (`2026-01-01`).
pub const LIST_MARKET_START: (i64, u32, u32) = (2026, 1, 1);

/// Which bench shape the corpus takes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ListShape {
    /// Ledger rows with an `attributes: List<Int64>` column.
    Ledger,
    /// Market baskets with a `holdings: List<Utf8>` column, one hive
    /// directory per day.
    Market,
}

impl ListShape {
    /// Mixed into every seed, so the two shapes draw different elements.
    fn tag(self) -> u64 {
        match self {
            Self::Ledger => 0x4c45_4447,
            Self::Market => 0x4d4b_5420,
        }
    }

    /// The list column's name.
    #[must_use]
    pub fn list_column(self) -> &'static str {
        match self {
            Self::Ledger => "attributes",
            Self::Market => "holdings",
        }
    }

    /// The shape's Arrow schema.
    #[must_use]
    pub fn schema(self) -> SchemaRef {
        let item = |dt: DataType| Arc::new(Field::new("item", dt, true));
        Arc::new(Schema::new(match self {
            Self::Ledger => vec![
                Field::new("account_id", DataType::Int64, false),
                Field::new("version", DataType::Int64, false),
                Field::new("amount", DataType::Decimal128(18, 6), false),
                Field::new("attributes", DataType::List(item(DataType::Int64)), true),
            ],
            Self::Market => vec![
                Field::new("basket", DataType::Utf8, false),
                Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    false,
                ),
                Field::new("holdings", DataType::List(item(DataType::Utf8)), true),
            ],
        }))
    }
}

/// Parameters of a list corpus. Everything written is a deterministic
/// function of them.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ListParams {
    /// The shape.
    pub shape: ListShape,
    /// RNG seed (ChaCha12 via [`crate::stats::chacha`]).
    pub seed: u64,
    /// Files (ledger) or days, one file each (market).
    pub files: u32,
    /// Row groups per file.
    pub row_groups_per_file: u32,
    /// Rows per row group.
    pub rows_per_row_group: u32,
    /// Distinct element values the generator draws from.
    pub vocabulary: u32,
    /// Values in the shared, skewed head (`< vocabulary`).
    pub head: u32,
    /// Probability that an element is drawn from the head, in `[0, 1]`.
    pub head_fraction: f64,
    /// Most elements in one non-empty list (at least 1).
    pub elements_max: u32,
    /// Fraction of null lists, in `[0, 1]`.
    pub null_fraction: f64,
    /// Fraction of empty lists, in `[0, 1]`.
    pub empty_fraction: f64,
    /// Term counts of the `contains_any` runs (each at most `vocabulary`).
    pub terms: Vec<u32>,
}

impl ListParams {
    /// The preset: 40 files (or days) × 4 row groups × 5,000 rows (800,000
    /// rows), 200,000 element values with a head of 50 drawn 30% of the
    /// time, up to 6 elements per list, 2% null and 3% empty lists, and
    /// `contains_any` runs of 10, 10^3, and 10^5 terms.
    #[must_use]
    pub fn preset(shape: ListShape) -> Self {
        Self {
            shape,
            seed: 4,
            files: 40,
            row_groups_per_file: 4,
            rows_per_row_group: 5_000,
            vocabulary: 200_000,
            head: 50,
            head_fraction: 0.3,
            elements_max: 6,
            null_fraction: 0.02,
            empty_fraction: 0.03,
            terms: vec![10, 1_000, 100_000],
        }
    }

    /// Refuses parameters that make no sense, saying which.
    pub fn validate(&self) -> Result<()> {
        let bad = |m: String| Err(BenchError::Invalid(m));
        if self.files == 0 || self.row_groups_per_file == 0 || self.rows_per_row_group == 0 {
            return bad(
                "files, row groups per file, and rows per row group must be positive".into(),
            );
        }
        if self.head == 0 || self.head >= self.vocabulary {
            return bad(format!(
                "the head ({}) must be at least 1 and below the vocabulary ({})",
                self.head, self.vocabulary
            ));
        }
        if self.elements_max == 0 {
            return bad("elements_max must be at least 1".into());
        }
        for (name, f) in [
            ("head_fraction", self.head_fraction),
            ("null_fraction", self.null_fraction),
            ("empty_fraction", self.empty_fraction),
        ] {
            if !(0.0..=1.0).contains(&f) {
                return bad(format!("{name} must lie in [0, 1], not {f}"));
            }
        }
        if self.null_fraction + self.empty_fraction > 1.0 {
            return bad("null_fraction + empty_fraction must not exceed 1".into());
        }
        if let Some(t) = self
            .terms
            .iter()
            .find(|t| **t == 0 || **t > self.vocabulary)
        {
            return bad(format!(
                "a run of {t} terms needs 1..={} (the vocabulary)",
                self.vocabulary
            ));
        }
        Ok(())
    }

    fn total_row_groups(&self) -> u64 {
        u64::from(self.files) * u64::from(self.row_groups_per_file)
    }

    fn total_rows(&self) -> u64 {
        self.total_row_groups() * u64::from(self.rows_per_row_group)
    }
}

/// One recorded membership run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListRun {
    /// Run id: `any-<n>` or `all-2`.
    pub id: String,
    /// `contains_any` or `contains_all`.
    pub query_kind: String,
    /// The key file, relative to the corpus directory.
    pub file: String,
    /// Distinct values in it.
    pub terms: u64,
}

/// `workload/lists.json`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ListWorkload {
    /// [`LIST_WORKLOAD_FORMAT`].
    pub format: String,
    /// The corpus parameters.
    pub params: ListParams,
    /// The list column the runs look up.
    pub column: String,
    /// The runs.
    pub runs: Vec<ListRun>,
}

/// One `(file, row group)`, the file relative to `data/`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ListRowGroup {
    /// File path relative to `data/`.
    pub file: String,
    /// Row group index in the file.
    pub row_group: u32,
}

/// The truth of one run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListRunTruth {
    /// The run id.
    pub id: String,
    /// Rows whose list holds any (or, for `contains_all`, every) value.
    pub rows: u64,
    /// Every row group holding such a row, in file then index order.
    pub row_groups: Vec<ListRowGroup>,
}

/// Summary and element statistics of a generated corpus.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ListSummary {
    /// The shape.
    pub shape: ListShape,
    /// The list column.
    pub column: String,
    /// Files written.
    pub files: u64,
    /// Row groups written.
    pub row_groups: u64,
    /// Rows written.
    pub rows: u64,
    /// On-disk bytes of the data files.
    pub bytes: u64,
    /// Null lists.
    pub null_lists: u64,
    /// Empty lists.
    pub empty_lists: u64,
    /// Distinct element values present.
    pub distinct_elements: u64,
    /// Row groups per present element value: nearest-rank p50, p95, max
    /// (the skew: the head's values are in nearly every row group).
    pub row_groups_per_element: [u64; 3],
}

/// `workload/truth.json`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ListTruth {
    /// [`LIST_TRUTH_FORMAT`].
    pub format: String,
    /// The corpus.
    pub summary: ListSummary,
    /// Per run.
    pub runs: Vec<ListRunTruth>,
}

/// The canonical name of element `e` in the market shape.
#[must_use]
pub fn instrument(e: u32) -> String {
    format!("I{e:06}")
}

/// The ledger shape's attribute id of element `e`.
#[must_use]
pub fn attribute(e: u32) -> i64 {
    1_000_000 + i64::from(e)
}

/// A file's path relative to `data/`.
fn file_rel(p: &ListParams, f: u32) -> String {
    match p.shape {
        ListShape::Ledger => format!("part-{f:04}.parquet"),
        ListShape::Market => {
            let (y, m, d) = LIST_MARKET_START;
            let (y, m, d) = civil_from_days(days_from_civil(y, m, d) + i64::from(f));
            format!("day={y:04}-{m:02}-{d:02}/part-0000.parquet")
        }
    }
}

/// The skewed element draw (module docs) for a row at `fraction` of the
/// corpus.
struct Draw {
    head_cdf: Vec<f64>,
    head: u32,
    tail: u32,
    window: u32,
}

impl Draw {
    fn new(p: &ListParams) -> Self {
        let weights: Vec<f64> = (0..p.head).map(|r| 1.0 / f64::from(r + 1)).collect();
        let total: f64 = weights.iter().sum();
        let mut acc = 0.0;
        let head_cdf = weights
            .iter()
            .map(|w| {
                acc += w / total;
                acc
            })
            .collect();
        let tail = p.vocabulary - p.head;
        let window =
            ((2 * u64::from(tail)) / p.total_row_groups()).clamp(1, u64::from(tail)) as u32;
        Self {
            head_cdf,
            head: p.head,
            tail,
            window,
        }
    }

    fn element(&self, rng: &mut impl Rng, head_fraction: f64, fraction: f64) -> u32 {
        if rng.gen_bool(head_fraction) {
            let u: f64 = rng.gen();
            let r = self.head_cdf.partition_point(|c| *c < u);
            return (r as u32).min(self.head - 1);
        }
        let start = (fraction * f64::from(self.tail)) as u32;
        let offset = rng.gen_range(0..self.window);
        self.head + (start + offset) % self.tail
    }
}

fn writer_properties(rows: u32) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_row_count(Some(rows as usize + 1))
        .set_created_by("bellwether-bench lists".to_string())
        .build()
}

/// Generates the corpus, its workload, and its truth under `out` (which
/// must be empty or absent).
pub fn generate_lists(out: &Path, p: &ListParams) -> Result<ListTruth> {
    p.validate()?;
    if out.join("data").exists() {
        return Err(BenchError::Invalid(format!(
            "{} already holds a corpus; pass an empty --out",
            out.display()
        )));
    }
    // The runs first: their terms depend on the parameters only.
    let mut rng = chacha(&[p.seed, p.shape.tag(), 0x4c49_5354_0001]);
    let mut runs: Vec<(ListRun, HashSet<u32>, bool)> = Vec::new();
    let tail = p.vocabulary - p.head;
    for &n in &p.terms {
        let n = n.min(tail);
        runs.push((
            ListRun {
                id: format!("near-{n}"),
                query_kind: "contains_any".into(),
                file: format!("workload/near-{n}.parquet"),
                terms: u64::from(n),
            },
            (0..n).map(|i| p.head + (tail / 4 + i) % tail).collect(),
            false,
        ));
    }
    for &n in &p.terms {
        let terms: HashSet<u32> =
            rand::seq::index::sample(&mut rng, p.vocabulary as usize, n as usize)
                .into_iter()
                .map(|i| i as u32)
                .collect();
        runs.push((
            ListRun {
                id: format!("any-{n}"),
                query_kind: "contains_any".into(),
                file: format!("workload/any-{n}.parquet"),
                terms: u64::from(n),
            },
            terms,
            false,
        ));
    }
    // contains_all: the most frequent head value and a tail value held
    // beside it by the first row from the middle of the corpus on that has
    // both (a pass over the deterministic draws, nothing written).
    let draw = Draw::new(p);
    let pair = pair_from_the_middle(p, &draw).ok_or_else(|| {
        BenchError::Invalid(
            "no row holds the most frequent head value beside a tail value; raise the rows or \
             elements_max"
                .into(),
        )
    })?;
    runs.push((
        ListRun {
            id: "all-2".into(),
            query_kind: "contains_all".into(),
            file: "workload/all-2.parquet".into(),
            terms: 2,
        },
        [0, pair].into_iter().collect(),
        true,
    ));
    let schema = p.shape.schema();
    let props = writer_properties(p.rows_per_row_group);
    let mut per_element: BTreeMap<u32, RoaringBitmap> = BTreeMap::new();
    let mut truth: Vec<ListRunTruth> = runs
        .iter()
        .map(|(r, _, _)| ListRunTruth {
            id: r.id.clone(),
            rows: 0,
            row_groups: Vec::new(),
        })
        .collect();
    let mut summary = ListSummary {
        shape: p.shape,
        column: p.shape.list_column().to_string(),
        files: 0,
        row_groups: 0,
        rows: 0,
        bytes: 0,
        null_lists: 0,
        empty_lists: 0,
        distinct_elements: 0,
        row_groups_per_element: [0; 3],
    };
    for f in 0..p.files {
        let rel = file_rel(p, f);
        let path = out.join("data").join(&rel);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).map_err(io_err(dir))?;
        }
        let sink = BufWriter::new(File::create(&path).map_err(io_err(&path))?);
        let mut w = ArrowWriter::try_new(sink, Arc::clone(&schema), Some(props.clone()))?;
        for rg in 0..p.row_groups_per_file {
            let rg_id = f * p.row_groups_per_file + rg;
            let (lists, mut rng) = row_group_lists(p, &draw, f, rg);
            for list in &lists {
                match list {
                    None => summary.null_lists += 1,
                    Some(items) if items.is_empty() => summary.empty_lists += 1,
                    Some(_) => {}
                }
                if let Some(items) = list {
                    for e in items {
                        per_element.entry(*e).or_default().insert(rg_id);
                    }
                    for ((_, terms, all), t) in runs.iter().zip(truth.iter_mut()) {
                        let hit = if *all {
                            terms.iter().all(|x| items.contains(x))
                        } else {
                            items.iter().any(|x| terms.contains(x))
                        };
                        if hit {
                            t.rows += 1;
                            let here = ListRowGroup {
                                file: rel.clone(),
                                row_group: rg,
                            };
                            if t.row_groups.last() != Some(&here) {
                                t.row_groups.push(here);
                            }
                        }
                    }
                }
            }
            let batch = row_group_batch(p, &schema, f, rg, &lists, &mut rng)?;
            w.write(&batch)?;
            w.flush()?;
            summary.rows += u64::from(p.rows_per_row_group);
            summary.row_groups += 1;
        }
        w.close()?;
        summary.bytes += std::fs::metadata(&path).map_err(io_err(&path))?.len();
        summary.files += 1;
    }
    summary.distinct_elements = per_element.len() as u64;
    let mut spread: Vec<u64> = per_element.values().map(RoaringBitmap::len).collect();
    spread.sort_unstable();
    summary.row_groups_per_element = nearest_rank(&spread);
    for (r, terms, _) in &runs {
        let mut sorted: Vec<u32> = terms.iter().copied().collect();
        sorted.sort_unstable();
        write_terms(&out.join(&r.file), p.shape, &sorted)?;
    }
    let workload = ListWorkload {
        format: LIST_WORKLOAD_FORMAT.into(),
        params: p.clone(),
        column: p.shape.list_column().into(),
        runs: runs.into_iter().map(|(r, _, _)| r).collect(),
    };
    let truth = ListTruth {
        format: LIST_TRUTH_FORMAT.into(),
        summary,
        runs: truth,
    };
    write_json(&out.join("workload").join("lists.json"), &workload)?;
    write_json(&out.join("workload").join("truth.json"), &truth)?;
    Ok(truth)
}

/// Row group `rg` of file `f`: each row's list, and the generator positioned
/// for the row group's other columns. Deterministic in `(seed, f, rg)`.
fn row_group_lists(
    p: &ListParams,
    draw: &Draw,
    f: u32,
    rg: u32,
) -> (Vec<Option<Vec<u32>>>, rand_chacha::ChaCha12Rng) {
    let mut rng = chacha(&[
        p.seed,
        p.shape.tag(),
        0x4c49_5354_0002,
        u64::from(f),
        u64::from(rg),
    ]);
    let first = u64::from(f * p.row_groups_per_file + rg) * u64::from(p.rows_per_row_group);
    let total = p.total_rows() as f64;
    let lists = (0..u64::from(p.rows_per_row_group))
        .map(|i| {
            let fraction = (first + i) as f64 / total;
            let u: f64 = rng.gen();
            if u < p.null_fraction {
                None
            } else if u < p.null_fraction + p.empty_fraction {
                Some(Vec::new())
            } else {
                let n = rng.gen_range(1..=p.elements_max);
                Some(
                    (0..n)
                        .map(|_| draw.element(&mut rng, p.head_fraction, fraction))
                        .collect(),
                )
            }
        })
        .collect();
    (lists, rng)
}

/// The tail value of the first row, from the corpus's middle row group on,
/// that holds head value 0 beside a tail value.
fn pair_from_the_middle(p: &ListParams, draw: &Draw) -> Option<u32> {
    let total = p.files * p.row_groups_per_file;
    (total / 2..total).find_map(|id| {
        let (lists, _) = row_group_lists(
            p,
            draw,
            id / p.row_groups_per_file,
            id % p.row_groups_per_file,
        );
        lists.into_iter().flatten().find_map(|items| {
            items
                .contains(&0)
                .then(|| items.iter().copied().find(|e| *e >= p.head))
                .flatten()
        })
    })
}

fn nearest_rank(sorted: &[u64]) -> [u64; 3] {
    if sorted.is_empty() {
        return [0; 3];
    }
    let at =
        |q: f64| sorted[((q * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len()) - 1];
    [at(0.5), at(0.95), sorted[sorted.len() - 1]]
}

fn row_group_batch(
    p: &ListParams,
    schema: &SchemaRef,
    f: u32,
    rg: u32,
    lists: &[Option<Vec<u32>>],
    rng: &mut impl Rng,
) -> Result<RecordBatch> {
    let n = lists.len();
    let first = u64::from(f * p.row_groups_per_file + rg) * u64::from(p.rows_per_row_group);
    let columns: Vec<ArrayRef> = match p.shape {
        ListShape::Ledger => {
            let mut account = Int64Builder::with_capacity(n);
            let mut version = Int64Builder::with_capacity(n);
            let mut amount = Decimal128Builder::with_capacity(n);
            let mut attrs = ListBuilder::new(Int64Builder::new());
            for (i, l) in lists.iter().enumerate() {
                account.append_value(rng.gen_range(0..1_000_000));
                version.append_value((first + i as u64) as i64);
                amount.append_value(rng.gen_range(-10_000_000_000i128..10_000_000_000));
                match l {
                    None => attrs.append(false),
                    Some(items) => {
                        for e in items {
                            attrs.values().append_value(attribute(*e));
                        }
                        attrs.append(true);
                    }
                }
            }
            vec![
                Arc::new(account.finish()),
                Arc::new(version.finish()),
                Arc::new(amount.finish().with_precision_and_scale(18, 6)?),
                Arc::new(attrs.finish()),
            ]
        }
        ListShape::Market => {
            let (y, m, d) = LIST_MARKET_START;
            let day = (days_from_civil(y, m, d) + i64::from(f)) * DAY_US;
            let per_rg = DAY_US / i64::from(p.row_groups_per_file);
            let mut basket = StringBuilder::new();
            let mut ts = TimestampMicrosecondBuilder::with_capacity(n).with_timezone("UTC");
            let mut holdings = ListBuilder::new(StringBuilder::new());
            for (i, l) in lists.iter().enumerate() {
                basket.append_value(format!("B{:03}", rng.gen_range(0..500)));
                ts.append_value(
                    day + i64::from(rg) * per_rg + (i as i64 * per_rg) / n.max(1) as i64,
                );
                match l {
                    None => holdings.append(false),
                    Some(items) => {
                        for e in items {
                            holdings.values().append_value(instrument(*e));
                        }
                        holdings.append(true);
                    }
                }
            }
            vec![
                Arc::new(basket.finish()),
                Arc::new(ts.finish()),
                Arc::new(holdings.finish()),
            ]
        }
    };
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

/// Writes a run's terms: one column named after the list column, of its
/// element type.
fn write_terms(path: &Path, shape: ListShape, terms: &[u32]) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(io_err(dir))?;
    }
    let (dt, array): (DataType, ArrayRef) = match shape {
        ListShape::Ledger => {
            let mut b = Int64Builder::with_capacity(terms.len());
            for t in terms {
                b.append_value(attribute(*t));
            }
            (DataType::Int64, Arc::new(b.finish()))
        }
        ListShape::Market => {
            let mut b = StringBuilder::new();
            for t in terms {
                b.append_value(instrument(*t));
            }
            (DataType::Utf8, Arc::new(b.finish()))
        }
    };
    let schema = Arc::new(Schema::new(vec![Field::new(
        shape.list_column(),
        dt,
        false,
    )]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array])?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_created_by("bellwether-bench lists".to_string())
        .build();
    let mut w = ArrowWriter::try_new(
        File::create(path).map_err(io_err(path))?,
        schema,
        Some(props),
    )?;
    w.write(&batch)?;
    w.close()?;
    Ok(())
}
