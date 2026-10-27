//! `tombstone`: corpora for tombstones (B8) on both benchmark shapes, the
//! ledger with voids and the market with retractions, `latest`/`asof`
//! workloads of 10, 10^3, and 10^5 keys, and exact ground truth.
//!
//! The operator a tombstone serves is `arg_max(ts, *) by key` followed by
//! `where not retracted`: each key's newest row (at or before an instant),
//! kept only when it is not a retraction. `asof --live` is that query;
//! `asof` plus the filter is the same query without the tombstone.
//!
//! # Shapes
//!
//! Both shapes write rows in time order (an append log, the way such files
//! arrive), one row per second from 2026-01-01T00:00:00Z, cut into `files`
//! files of `row_groups_per_file` row groups of `rows_per_row_group` rows,
//! under `<out>/data/part-NNNN.parquet` (no partition directories). Each row
//! draws its key uniformly from `keys` keys, so a key's versions spread over
//! the corpus, and is a retraction with probability `retract_fraction`,
//! independently:
//!
//! - **ledger** (voids): `order_id` (`Int64`, the key), `ts`
//!   (`Timestamp(us, UTC)`), `amount` (`Int64`, cents), `void`
//!   (`Boolean`, true on a void; null in `null_fraction` of rows, which is
//!   not live, ADR 014). Tombstone `tombstone=void`.
//! - **market** (retractions): `trade_id` (`Int64`, the key), `symbol`
//!   (`Utf8`, `S0001..=S0500` by trade), `ts` (`Timestamp(us, UTC)`),
//!   `price` (`Float64`), `size` (`Int64`), `status` (`Utf8`: `NEW` for a
//!   trade's first print, `CORR` for a correction, `CANCEL` for a
//!   retraction). Tombstone `tombstone=status=CANCEL`.
//!
//! Since rows are drawn independently, a key's newest row is a retraction
//! with probability about `retract_fraction`; some keys hold only
//! retractions, and some were retracted and written again (resurrected).
//! The truth counts each.
//!
//! # Workloads and truth
//!
//! `workload/keys-<n>.parquet` holds `n` distinct keys drawn uniformly from
//! `0..keys`, in one column named by the key column; a few may hold no row.
//! `workload/tombstone.json` lists the runs, the key, time, and tombstone
//! columns, the tombstone spelling, and the `asof` instant (half way through
//! the rows, between two rows). `workload/truth.json` gives, per run and per
//! lookup (`latest`, `asof`): the keys with a version, those whose newest
//! row is a retraction or has a null tombstone column (no live row), the
//! row groups holding the keys' newest rows (what `asof` reads before its
//! filter) and those holding a live newest row (the least a live lookup
//! reads), computed while the rows are generated, never from an index; and
//! over the whole corpus, the keys with a version, whose newest row is a
//! retraction, which hold only retractions, and which were resurrected.
//!
//! # Memory (ADR 018)
//!
//! One row group's batch at a time, plus a fixed-size state per key (its
//! newest row, at the end and at the instant): `O(keys)`.

use std::collections::BTreeSet;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, RecordBatch, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use rand::seq::SliceRandom;
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::corpus::{days_from_civil, DAY_US};
use crate::stats::chacha;
use crate::{io_err, write_json, BenchError, Result};

/// Format tag of `workload/tombstone.json`.
pub const TOMBSTONE_WORKLOAD_FORMAT: &str = "bellwether-bench-tombstone-workload/1";
/// Format tag of `workload/truth.json`.
pub const TOMBSTONE_TRUTH_FORMAT: &str = "bellwether-bench-tombstone-truth/1";
/// Microseconds between consecutive rows.
pub const ROW_STEP_US: i64 = 1_000_000;
/// Symbols the market shape's trades are spread over.
pub const MARKET_SYMBOLS: i64 = 500;

/// First instant of both shapes: 2026-01-01T00:00:00Z.
#[must_use]
pub fn start_us() -> i64 {
    days_from_civil(2026, 1, 1) * DAY_US
}

/// A benchmark shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TombstoneShape {
    /// A ledger of orders whose newest row may be a void.
    Ledger,
    /// Market trade prints whose newest print may be a cancellation.
    Market,
}

impl TombstoneShape {
    /// The shape's name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ledger => "ledger",
            Self::Market => "market",
        }
    }

    /// The key column.
    #[must_use]
    pub fn key_column(self) -> &'static str {
        match self {
            Self::Ledger => "order_id",
            Self::Market => "trade_id",
        }
    }

    /// The tombstone column.
    #[must_use]
    pub fn tombstone_column(self) -> &'static str {
        match self {
            Self::Ledger => "void",
            Self::Market => "status",
        }
    }

    /// The tombstone as spelled in a spec (`tombstone=<this>`).
    #[must_use]
    pub fn tombstone(self) -> &'static str {
        match self {
            Self::Ledger => "void",
            Self::Market => "status=CANCEL",
        }
    }

    /// The filter `asof` needs without the tombstone, in SQL.
    #[must_use]
    pub fn filter(self) -> &'static str {
        match self {
            Self::Ledger => "not void",
            Self::Market => "status <> 'CANCEL'",
        }
    }

    /// The Arrow schema of every data file.
    #[must_use]
    pub fn schema(self) -> SchemaRef {
        let ts = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        Arc::new(Schema::new(match self {
            Self::Ledger => vec![
                Field::new("order_id", DataType::Int64, false),
                Field::new("ts", ts, false),
                Field::new("amount", DataType::Int64, false),
                Field::new("void", DataType::Boolean, true),
            ],
            Self::Market => vec![
                Field::new("trade_id", DataType::Int64, false),
                Field::new("symbol", DataType::Utf8, false),
                Field::new("ts", ts, false),
                Field::new("price", DataType::Float64, false),
                Field::new("size", DataType::Int64, false),
                Field::new("status", DataType::Utf8, false),
            ],
        }))
    }
}

/// Parameters of a tombstone corpus. Everything written is a deterministic
/// function of them.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TombstoneParams {
    /// The shape.
    pub shape: TombstoneShape,
    /// RNG seed (ChaCha12 via [`crate::stats::chacha`]).
    pub seed: u64,
    /// Files.
    pub files: u32,
    /// Row groups per file.
    pub row_groups_per_file: u32,
    /// Rows per row group.
    pub rows_per_row_group: u32,
    /// Distinct keys the rows draw from.
    pub keys: u32,
    /// Probability that a row is a retraction, in `[0, 1]`.
    pub retract_fraction: f64,
    /// Probability that a row's tombstone column is null, in `[0, 1]`
    /// (ledger only; the market's `status` is never null).
    pub null_fraction: f64,
    /// Keys per run (each at most half of `keys`).
    pub runs: Vec<u32>,
}

impl TombstoneParams {
    /// The preset of a shape: 20 files × 10 row groups × 10,000 rows (2 ×
    /// 10^6 rows) over 2 × 10^5 keys (10 versions per key on average), runs
    /// of 10, 10^3, and 10^5 keys; the ledger voids 20% of rows and nulls
    /// 0.1% of `void`, the market cancels 5% of prints.
    #[must_use]
    pub fn preset(shape: TombstoneShape) -> Self {
        Self {
            shape,
            seed: 11,
            files: 20,
            row_groups_per_file: 10,
            rows_per_row_group: 10_000,
            keys: 200_000,
            retract_fraction: match shape {
                TombstoneShape::Ledger => 0.2,
                TombstoneShape::Market => 0.05,
            },
            null_fraction: match shape {
                TombstoneShape::Ledger => 0.001,
                TombstoneShape::Market => 0.0,
            },
            runs: vec![10, 1_000, 100_000],
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
        if self.keys < 2 {
            return bad(format!("keys must be at least 2, not {}", self.keys));
        }
        for (name, f) in [
            ("retract_fraction", self.retract_fraction),
            ("null_fraction", self.null_fraction),
        ] {
            if !(0.0..=1.0).contains(&f) {
                return bad(format!("{name} must lie in [0, 1], not {f}"));
            }
        }
        if self.shape == TombstoneShape::Market && self.null_fraction > 0.0 {
            return bad(
                "the market's status column is never null; pass --null-fraction 0 (or omit it)"
                    .into(),
            );
        }
        if let Some(n) = self.runs.iter().find(|n| **n == 0 || **n > self.keys / 2) {
            return bad(format!(
                "a run of {n} keys needs 1..={} (half the keys)",
                self.keys / 2
            ));
        }
        if self.runs.len() > 8 {
            return bad(format!("at most 8 runs, not {}", self.runs.len()));
        }
        Ok(())
    }

    fn total_rows(&self) -> u64 {
        u64::from(self.files)
            * u64::from(self.row_groups_per_file)
            * u64::from(self.rows_per_row_group)
    }

    /// The `asof` instant (µs since the epoch): half way through the rows,
    /// half a step after a row, so no row is at it.
    #[must_use]
    pub fn asof_us(&self) -> i64 {
        start_us() + (self.total_rows() / 2) as i64 * ROW_STEP_US + ROW_STEP_US / 2
    }
}

/// One recorded run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TombstoneRun {
    /// Run id: `keys-<n>`.
    pub id: String,
    /// The key file, relative to the corpus directory.
    pub file: String,
    /// Distinct keys in it.
    pub keys: u64,
}

/// `workload/tombstone.json`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TombstoneWorkload {
    /// [`TOMBSTONE_WORKLOAD_FORMAT`].
    pub format: String,
    /// The corpus parameters.
    pub params: TombstoneParams,
    /// The key column.
    pub key: String,
    /// The time column (`ts`).
    pub ts: String,
    /// The tombstone as spelled in a spec (`void`, `status=CANCEL`).
    pub tombstone: String,
    /// The filter `asof` needs without it, in SQL.
    pub filter: String,
    /// The `asof` instant, RFC 3339 with microseconds.
    pub asof: String,
    /// The runs.
    pub runs: Vec<TombstoneRun>,
}

/// The truth of one run under one lookup.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LookupTruth {
    /// Keys of the run with a version (at or before the instant).
    pub keys_with_version: u64,
    /// Of those, keys whose newest row is a retraction.
    pub newest_retracted: u64,
    /// Of those, keys whose newest row's tombstone column is null (not
    /// live, ADR 014).
    pub newest_null: u64,
    /// Row groups holding a key's newest row: what the lookup reads before
    /// its filter.
    pub newest_row_groups: u64,
    /// Row groups holding a key's live newest row: the least a live lookup
    /// reads, and the rows it returns are one per live key.
    pub live_row_groups: u64,
}

/// The truth of one run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TombstoneRunTruth {
    /// The run id.
    pub id: String,
    /// `latest`.
    pub latest: LookupTruth,
    /// `asof` at the workload's instant.
    pub asof: LookupTruth,
}

/// Summary of a generated corpus.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TombstoneSummary {
    /// The shape.
    pub shape: TombstoneShape,
    /// Files written.
    pub files: u64,
    /// Row groups written.
    pub row_groups: u64,
    /// Rows written.
    pub rows: u64,
    /// Rows that are retractions.
    pub retractions: u64,
    /// Rows whose tombstone column is null.
    pub null_tombstones: u64,
    /// On-disk bytes of the data files.
    pub bytes: u64,
    /// Keys with a row.
    pub keys_with_version: u64,
    /// Keys whose newest row is a retraction (what `why`'s `tombstone`
    /// column reports, over the whole corpus).
    pub newest_retracted: u64,
    /// Keys whose every row is a retraction.
    pub only_retractions: u64,
    /// Keys retracted and then written again: a live newest row after a
    /// retraction.
    pub resurrected: u64,
}

/// `workload/truth.json`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TombstoneTruth {
    /// [`TOMBSTONE_TRUTH_FORMAT`].
    pub format: String,
    /// The corpus.
    pub summary: TombstoneSummary,
    /// Per run.
    pub runs: Vec<TombstoneRunTruth>,
}

/// What a row's tombstone column says.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mark {
    Live,
    Retracted,
    Null,
}

/// A key's newest row, and what its history held.
#[derive(Clone, Copy, Default)]
struct KeyState {
    /// `(global row group, mark)` of the newest row; none without a row.
    newest: Option<(u64, Mark)>,
    rows: u32,
    retractions: u32,
    /// A retraction came before the newest row.
    retracted_before: bool,
}

impl KeyState {
    fn push(&mut self, rg: u64, mark: Mark) {
        if let Some((_, Mark::Retracted)) = self.newest {
            self.retracted_before = true;
        }
        self.newest = Some((rg, mark));
        self.rows += 1;
        if mark == Mark::Retracted {
            self.retractions += 1;
        }
    }
}

fn writer_properties(rows: u32) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_row_count(Some(rows as usize + 1))
        .set_created_by("bellwether-bench tombstone".to_string())
        .build()
}

/// An instant (µs since the epoch, UTC) in RFC 3339 with microseconds.
fn rfc3339_us(us: i64) -> String {
    let day = us.div_euclid(DAY_US);
    let rem = us.rem_euclid(DAY_US);
    let (y, m, d) = crate::corpus::civil_from_days(day);
    let secs = rem / 1_000_000;
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:06}Z",
        secs / 3600,
        secs / 60 % 60,
        secs % 60,
        rem % 1_000_000
    )
}

/// Generates the corpus, its workload, and its truth under `out` (which
/// must not hold a corpus).
#[allow(clippy::too_many_lines)] // one row loop writing both shapes
pub fn generate_tombstone(out: &Path, p: &TombstoneParams) -> Result<TombstoneTruth> {
    p.validate()?;
    if out.join("data").exists() {
        return Err(BenchError::Invalid(format!(
            "{} already holds a corpus; pass an empty --out",
            out.display()
        )));
    }
    let shape = p.shape;
    let keys = p.keys as usize;
    // The runs first: their keys depend on the parameters only.
    let mut rng = chacha(&[p.seed, 0x544f_4d42_0001]);
    let all: Vec<i64> = (0..i64::from(p.keys)).collect();
    let runs: Vec<(TombstoneRun, Vec<i64>)> = p
        .runs
        .iter()
        .map(|&n| {
            let mut ks: Vec<i64> = all.choose_multiple(&mut rng, n as usize).copied().collect();
            ks.sort_unstable();
            (
                TombstoneRun {
                    id: format!("keys-{n}"),
                    file: format!("workload/keys-{n}.parquet"),
                    keys: u64::from(n),
                },
                ks,
            )
        })
        .collect();
    let asof = p.asof_us();
    let mut at_end = vec![KeyState::default(); keys];
    let mut at_asof = vec![KeyState::default(); keys];
    let schema = shape.schema();
    let props = writer_properties(p.rows_per_row_group);
    let mut summary = TombstoneSummary {
        shape,
        files: 0,
        row_groups: 0,
        rows: 0,
        retractions: 0,
        null_tombstones: 0,
        bytes: 0,
        keys_with_version: 0,
        newest_retracted: 0,
        only_retractions: 0,
        resurrected: 0,
    };
    for f in 0..p.files {
        let path = out.join("data").join(format!("part-{f:04}.parquet"));
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).map_err(io_err(dir))?;
        }
        let sink = BufWriter::new(File::create(&path).map_err(io_err(&path))?);
        let mut w = ArrowWriter::try_new(sink, Arc::clone(&schema), Some(props.clone()))?;
        for rg in 0..p.row_groups_per_file {
            let rg_id = u64::from(f * p.row_groups_per_file + rg);
            let first = rg_id * u64::from(p.rows_per_row_group);
            let mut rng = chacha(&[p.seed, 0x544f_4d42_0002, u64::from(f), u64::from(rg)]);
            let n = p.rows_per_row_group as usize;
            let mut key = Int64Builder::with_capacity(n);
            let mut ts = TimestampMicrosecondBuilder::with_capacity(n).with_timezone("UTC");
            let mut amount = Int64Builder::with_capacity(n);
            let mut void = BooleanBuilder::with_capacity(n);
            let mut symbol = StringBuilder::with_capacity(n, n * 5);
            let mut price = Float64Builder::with_capacity(n);
            let mut status = StringBuilder::with_capacity(n, n * 4);
            for i in 0..n {
                let k = rng.gen_range(0..p.keys);
                let t = start_us() + (first + i as u64) as i64 * ROW_STEP_US;
                let mark = if p.null_fraction > 0.0 && rng.gen_bool(p.null_fraction) {
                    Mark::Null
                } else if rng.gen_bool(p.retract_fraction) {
                    Mark::Retracted
                } else {
                    Mark::Live
                };
                let state = &mut at_end[k as usize];
                key.append_value(i64::from(k));
                ts.append_value(t);
                match shape {
                    TombstoneShape::Ledger => {
                        amount.append_value(rng.gen_range(-1_000_000..1_000_000));
                        void.append_option(match mark {
                            Mark::Live => Some(false),
                            Mark::Retracted => Some(true),
                            Mark::Null => None,
                        });
                    }
                    TombstoneShape::Market => {
                        symbol.append_value(format!("S{:04}", i64::from(k) % MARKET_SYMBOLS + 1));
                        price.append_value(f64::from(rng.gen_range(1_000u32..50_000)) / 100.0);
                        amount.append_value(rng.gen_range(1..=10_000));
                        status.append_value(match mark {
                            Mark::Retracted => "CANCEL",
                            _ if state.rows == 0 => "NEW",
                            _ => "CORR",
                        });
                    }
                }
                state.push(rg_id, mark);
                if t <= asof {
                    at_asof[k as usize].push(rg_id, mark);
                }
                match mark {
                    Mark::Retracted => summary.retractions += 1,
                    Mark::Null => summary.null_tombstones += 1,
                    Mark::Live => {}
                }
            }
            let columns: Vec<ArrayRef> = match shape {
                TombstoneShape::Ledger => vec![
                    Arc::new(key.finish()),
                    Arc::new(ts.finish()),
                    Arc::new(amount.finish()),
                    Arc::new(void.finish()),
                ],
                TombstoneShape::Market => vec![
                    Arc::new(key.finish()),
                    Arc::new(symbol.finish()),
                    Arc::new(ts.finish()),
                    Arc::new(price.finish()),
                    Arc::new(amount.finish()),
                    Arc::new(status.finish()),
                ],
            };
            w.write(&RecordBatch::try_new(Arc::clone(&schema), columns)?)?;
            w.flush()?;
            summary.rows += n as u64;
            summary.row_groups += 1;
        }
        w.close()?;
        summary.bytes += std::fs::metadata(&path).map_err(io_err(&path))?.len();
        summary.files += 1;
    }
    for s in &at_end {
        let Some((_, mark)) = s.newest else { continue };
        summary.keys_with_version += 1;
        if mark == Mark::Retracted {
            summary.newest_retracted += 1;
        }
        if s.retractions == s.rows {
            summary.only_retractions += 1;
        }
        if mark == Mark::Live && s.retracted_before {
            summary.resurrected += 1;
        }
    }
    let lookup = |states: &[KeyState], ks: &[i64]| -> LookupTruth {
        let mut t = LookupTruth::default();
        let mut newest = BTreeSet::new();
        let mut live = BTreeSet::new();
        for k in ks {
            let Some((rg, mark)) = states[*k as usize].newest else {
                continue;
            };
            t.keys_with_version += 1;
            newest.insert(rg);
            match mark {
                Mark::Live => {
                    live.insert(rg);
                }
                Mark::Retracted => t.newest_retracted += 1,
                Mark::Null => t.newest_null += 1,
            }
        }
        t.newest_row_groups = newest.len() as u64;
        t.live_row_groups = live.len() as u64;
        t
    };
    for (run, ks) in &runs {
        write_keys(&out.join(&run.file), shape.key_column(), ks)?;
    }
    let truth = TombstoneTruth {
        format: TOMBSTONE_TRUTH_FORMAT.into(),
        summary,
        runs: runs
            .iter()
            .map(|(run, ks)| TombstoneRunTruth {
                id: run.id.clone(),
                latest: lookup(&at_end, ks),
                asof: lookup(&at_asof, ks),
            })
            .collect(),
    };
    let workload = TombstoneWorkload {
        format: TOMBSTONE_WORKLOAD_FORMAT.into(),
        params: p.clone(),
        key: shape.key_column().into(),
        ts: "ts".into(),
        tombstone: shape.tombstone().into(),
        filter: shape.filter().into(),
        asof: rfc3339_us(asof),
        runs: runs.into_iter().map(|(r, _)| r).collect(),
    };
    write_json(&out.join("workload").join("tombstone.json"), &workload)?;
    write_json(&out.join("workload").join("truth.json"), &truth)?;
    Ok(truth)
}

/// Writes a run's keys: one `Int64` column named `column`.
fn write_keys(path: &Path, column: &str, keys: &[i64]) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(io_err(dir))?;
    }
    let schema = Arc::new(Schema::new(vec![Field::new(
        column,
        DataType::Int64,
        false,
    )]));
    let mut b = Int64Builder::with_capacity(keys.len());
    b.append_slice(keys);
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(b.finish())])?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_created_by("bellwether-bench tombstone".to_string())
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use arrow::array::{Array, BooleanArray, Int64Array, StringArray, TimestampMicrosecondArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;

    fn small(shape: TombstoneShape) -> TombstoneParams {
        TombstoneParams {
            shape,
            seed: 5,
            files: 3,
            row_groups_per_file: 4,
            rows_per_row_group: 400,
            keys: 600,
            retract_fraction: 0.3,
            null_fraction: match shape {
                TombstoneShape::Ledger => 0.02,
                TombstoneShape::Market => 0.0,
            },
            runs: vec![10, 200],
        }
    }

    /// Per key, the decoded rows: (ts, global row group, live, retracted).
    type Decoded = BTreeMap<i64, Vec<(i64, u64, bool, bool)>>;

    fn decode(out: &Path, shape: TombstoneShape, files: u32) -> Decoded {
        let mut rows: Decoded = BTreeMap::new();
        let mut rg_id = 0u64;
        for f in 0..files {
            let path = out.join("data").join(format!("part-{f:04}.parquet"));
            let b = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
            let groups = b.metadata().num_row_groups();
            for g in 0..groups {
                let r = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap())
                    .unwrap()
                    .with_row_groups(vec![g])
                    .build()
                    .unwrap();
                for batch in r {
                    let batch = batch.unwrap();
                    let col = |n: &str| batch.column(batch.schema().index_of(n).unwrap()).clone();
                    let key = col(shape.key_column());
                    let key = key.as_any().downcast_ref::<Int64Array>().unwrap();
                    let ts = col("ts");
                    let ts = ts
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    let tomb = col(shape.tombstone_column());
                    for i in 0..batch.num_rows() {
                        let (live, retracted) = match shape {
                            TombstoneShape::Ledger => {
                                let v = tomb.as_any().downcast_ref::<BooleanArray>().unwrap();
                                if v.is_null(i) {
                                    (false, false)
                                } else {
                                    (!v.value(i), v.value(i))
                                }
                            }
                            TombstoneShape::Market => {
                                let v = tomb.as_any().downcast_ref::<StringArray>().unwrap();
                                (v.value(i) != "CANCEL", v.value(i) == "CANCEL")
                            }
                        };
                        rows.entry(key.value(i)).or_default().push((
                            ts.value(i),
                            rg_id,
                            live,
                            retracted,
                        ));
                    }
                }
                rg_id += 1;
            }
        }
        rows
    }

    /// The truth matches the rows decoded back from the files, per shape:
    /// the summary's counts, and per run and lookup the keys with a
    /// version, the retracted and null newest rows, and the row groups of
    /// the newest and the live newest rows.
    #[test]
    fn truth_matches_the_decoded_rows() {
        for shape in [TombstoneShape::Ledger, TombstoneShape::Market] {
            let dir = tempfile::tempdir().unwrap();
            let p = small(shape);
            let truth = generate_tombstone(dir.path(), &p).unwrap();
            let rows = decode(dir.path(), shape, p.files);
            let s = &truth.summary;
            assert_eq!(s.rows, 4_800);
            assert_eq!(s.row_groups, 12);
            let all: u64 = rows.values().map(|v| v.len() as u64).sum();
            assert_eq!(all, s.rows);
            let retractions: u64 = rows.values().flatten().filter(|r| r.3).count() as u64;
            assert_eq!(retractions, s.retractions);
            assert_eq!(rows.len() as u64, s.keys_with_version);
            let newest = |v: &Vec<(i64, u64, bool, bool)>, at: Option<i64>| {
                v.iter()
                    .filter(|r| at.is_none_or(|t| r.0 <= t))
                    .max_by_key(|r| r.0)
                    .copied()
            };
            assert_eq!(
                rows.values().filter(|v| newest(v, None).unwrap().3).count() as u64,
                s.newest_retracted
            );
            assert_eq!(
                rows.values().filter(|v| v.iter().all(|r| r.3)).count() as u64,
                s.only_retractions
            );
            assert!(s.only_retractions > 0 && s.resurrected > 0, "{s:?}");
            let workload: TombstoneWorkload = serde_json::from_slice(
                &std::fs::read(dir.path().join("workload/tombstone.json")).unwrap(),
            )
            .unwrap();
            assert_eq!(workload.asof, rfc3339_us(p.asof_us()));
            for (run, t) in workload.runs.iter().zip(&truth.runs) {
                let r = ParquetRecordBatchReaderBuilder::try_new(
                    File::open(dir.path().join(&run.file)).unwrap(),
                )
                .unwrap()
                .build()
                .unwrap();
                let ks: Vec<i64> = r
                    .flat_map(|b| {
                        let b = b.unwrap();
                        let c = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                        c.values().to_vec()
                    })
                    .collect();
                assert_eq!(ks.len() as u64, run.keys);
                for (at, want) in [(None, &t.latest), (Some(p.asof_us()), &t.asof)] {
                    let mut got = LookupTruth::default();
                    let mut all_rg = BTreeSet::new();
                    let mut live_rg = BTreeSet::new();
                    for k in &ks {
                        let Some(n) = rows.get(k).and_then(|v| newest(v, at)) else {
                            continue;
                        };
                        got.keys_with_version += 1;
                        all_rg.insert(n.1);
                        if n.2 {
                            live_rg.insert(n.1);
                        } else if n.3 {
                            got.newest_retracted += 1;
                        } else {
                            got.newest_null += 1;
                        }
                    }
                    got.newest_row_groups = all_rg.len() as u64;
                    got.live_row_groups = live_rg.len() as u64;
                    assert_eq!(&got, want, "{shape:?} {} {at:?}", run.id);
                }
            }
        }
    }

    /// Generation is deterministic, and parameters that make no sense are
    /// refused naming the fix.
    #[test]
    fn deterministic_and_refuses_bad_parameters() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let p = small(TombstoneShape::Market);
        assert_eq!(
            generate_tombstone(a.path(), &p).unwrap(),
            generate_tombstone(b.path(), &p).unwrap()
        );
        assert_eq!(
            std::fs::read(a.path().join("data/part-0001.parquet")).unwrap(),
            std::fs::read(b.path().join("data/part-0001.parquet")).unwrap()
        );
        let e = generate_tombstone(a.path(), &p).unwrap_err().to_string();
        assert!(e.contains("already holds a corpus"), "{e}");
        let null_market = TombstoneParams {
            null_fraction: 0.1,
            ..p.clone()
        };
        assert!(null_market.validate().is_err());
        let big = TombstoneParams {
            runs: vec![400],
            ..p
        };
        let e = big.validate().unwrap_err().to_string();
        assert!(e.contains("half the keys"), "{e}");
        assert_eq!(
            rfc3339_us(start_us() + 1_500_000),
            "2026-01-01T00:00:01.500000Z"
        );
    }
}
