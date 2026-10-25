//! `market`: deterministic synthetic bitemporal market trade prints corpus,
//! recorded time-series workloads, and exact ground truth (WP21).
//!
//! # Layout and row model
//!
//! Data files live under `<out>/data/day=YYYY-MM-DD/part-NNNN.parquet`
//! (Hive-style directory partitioning; `day` is not a column inside the
//! Parquet files). Every file has five non-null columns in order:
//!
//! 1. `symbol` (`Utf8`): symbol identifiers `S0001..=S{symbols:04}`.
//! 2. `ts` (`Timestamp(Microsecond, "UTC")`): event / valid time.
//! 3. `known_at` (`Timestamp(Microsecond, "UTC")`): transaction / knowledge time.
//! 4. `price` (`Float64`): trade price in cents (`10.00..=500.00`).
//! 5. `size` (`Int64`): share quantity (`1..=10_000`).
//!
//! On each event day `d` (`0..days`), each symbol generates
//! `prints_per_symbol_per_day` base prints at seeded random microsecond times
//! within regular trading hours `14:30:00..21:00:00` UTC:
//!
//! - **Normal print:** `known_at = ts + 0..=500_000` µs (0 to 500 ms), placed
//!   in event day `d`'s partition.
//! - **Late print** (`late_fraction`): arrives `delta in 1..=3` days late with
//!   `known_at` on day `d + delta`, placed in the partition of its `known_at`
//!   day (`d + delta`). If `d + delta >= days`, the late arrival falls beyond
//!   the generated window and is **dropped** (never clamped), while the base
//!   print is not emitted on day `d`.
//! - **Restatement** (`restatement_fraction`, drawn independently on normal
//!   on-time prints): emits the original print on day `d` and an additional
//!   corrected row with the same `symbol` and `ts`, a different `price`, and
//!   `known_at` `delta in 1..=3` days later, placed in day `d + delta`'s
//!   partition. If `d + delta >= days`, the restatement row beyond the last
//!   day is **dropped** (the original print on day `d` remains).
//!
//! Within each partition day, all rows landing in that day's partition are
//! sorted by `(symbol, ts, known_at)` and cut evenly across `files_per_day`
//! files (`part-0000.parquet`, `part-0001.parquet`, …), with each file cut
//! into row groups of `rows_per_row_group` rows (the final row group of a file
//! holds the remainder if the file's row count is not a multiple of
//! `rows_per_row_group`). Because a symbol has many prints per day and
//! `rows_per_row_group` is sized smaller than a file's rows, symbols straddle
//! row groups and intraday windows can skip some of a symbol's row groups.
//!
//! # Memory bound (ADR 018)
//!
//! The generator materializes the rows of this synthetic benchmark corpus in
//! memory (`~56` bytes per `MarketRow` plus file/row-group index structures),
//! bounded by `O(total_rows)`: about 7 MB for `MarketRung::Small` (~105k rows)
//! and 120 MB for `MarketRung::Medium` (~1.9M rows).

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float64Builder, Int64Builder, RecordBatch, StringArray, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use rand::seq::SliceRandom;
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::corpus::{civil_from_days, days_from_civil, DAY_US};
use crate::stats::chacha;
use crate::{io_err, write_json, BenchError, Result};

/// Format tag written into `workload/market.json`.
pub const MARKET_WORKLOAD_FORMAT: &str = "bellwether-bench-market-workload/1";
/// Format tag written into `workload/truth.json`.
pub const MARKET_TRUTH_FORMAT: &str = "bellwether-bench-market-truth/1";
/// Default start date (`2026-01-01`).
pub const MARKET_START_DATE: (i32, u32, u32) = (2026, 1, 1);
/// Regular trading hours start offset from midnight UTC: `14:30:00` in microseconds.
pub const MARKET_OPEN_US: i64 = (14 * 3600 + 30 * 60) * 1_000_000;
/// Regular trading hours end offset from midnight UTC: `21:00:00` in microseconds.
pub const MARKET_CLOSE_US: i64 = 21 * 3600 * 1_000_000;
/// Maximum normal knowledge delay (`500 ms` in microseconds).
pub const NORMAL_KNOWN_DELAY_MAX_US: i64 = 500_000;
/// Column names of every generated market Parquet file, in schema order.
pub const MARKET_COLUMNS: [&str; 5] = ["symbol", "ts", "known_at", "price", "size"];

/// Pre-registered rungs for the synthetic `market` corpus.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketRung {
    /// 50 symbols × 10 days, 2 files/day, 500 rows/row group, 200 prints/symbol/day (~105k rows).
    Small,
    /// 200 symbols × 30 days, 4 files/day, 2,000 rows/row group, 300 prints/symbol/day (~1.9M rows).
    Medium,
}

/// Parameters of the synthetic `market` corpus. Everything written under
/// `<out>/data/` is a deterministic pure function of these parameters.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MarketParams {
    /// RNG seed (ChaCha12 via [`crate::stats::chacha`]).
    pub seed: u64,
    /// Number of distinct symbols (`S0001..=S{symbols:04}`).
    pub symbols: u32,
    /// Number of partition days.
    pub days: u32,
    /// First partition date `(year, month, day)` in UTC.
    pub start: (i32, u32, u32),
    /// Parquet files written per partition day.
    pub files_per_day: u32,
    /// Target rows per row group inside each Parquet file.
    pub rows_per_row_group: u32,
    /// Base prints generated per symbol on each event day.
    pub prints_per_symbol_per_day: u32,
    /// Fraction of on-time prints that also produce a corrected restatement row 1–3 days later, in `[0, 1]`.
    pub restatement_fraction: f64,
    /// Fraction of base prints that arrive 1–3 days late in their `known_at` day's partition, in `[0, 1]`.
    pub late_fraction: f64,
}

impl MarketParams {
    /// Returns the parameters for a preset [`MarketRung`].
    #[must_use]
    pub fn rung(rung: MarketRung) -> Self {
        match rung {
            MarketRung::Small => Self {
                seed: 21,
                symbols: 50,
                days: 10,
                start: MARKET_START_DATE,
                files_per_day: 2,
                rows_per_row_group: 500,
                prints_per_symbol_per_day: 200,
                restatement_fraction: 0.05,
                late_fraction: 0.05,
            },
            MarketRung::Medium => Self {
                seed: 21,
                symbols: 200,
                days: 30,
                start: MARKET_START_DATE,
                files_per_day: 4,
                rows_per_row_group: 2_000,
                prints_per_symbol_per_day: 300,
                restatement_fraction: 0.05,
                late_fraction: 0.05,
            },
        }
    }

    /// Parses a `YYYY-MM-DD` date string into `(year, month, day)` and sets `start`.
    pub fn with_start_str(mut self, s: &str) -> Result<Self> {
        self.start = parse_ymd(s)?;
        Ok(self)
    }

    /// Validates that counts are positive, `start` is a valid civil date, and
    /// fractions lie in `[0.0, 1.0]`.
    pub fn validate(&self) -> Result<()> {
        let bad = |msg: &str| Err(BenchError::Invalid(msg.to_string()));
        if self.symbols == 0 {
            return bad("symbols must be at least 1; pass a positive --symbols count");
        }
        if self.days == 0 {
            return bad("days must be at least 1; pass a positive --days count");
        }
        if self.days > 3650 {
            return bad("days must be at most 3650; reduce --days");
        }
        if self.files_per_day == 0 {
            return bad("files_per_day must be at least 1; pass a positive --files-per-day count");
        }
        if self.rows_per_row_group == 0 {
            return bad(
                "rows_per_row_group must be at least 1; pass a positive --rows-per-row-group count",
            );
        }
        if self.prints_per_symbol_per_day == 0 {
            return bad(
                "prints_per_symbol_per_day must be at least 1; pass a positive --prints-per-symbol-per-day count",
            );
        }
        if !self.restatement_fraction.is_finite()
            || !(0.0..=1.0).contains(&self.restatement_fraction)
        {
            return bad(
                "restatement_fraction must be in [0.0, 1.0]; pass a fraction between 0 and 1",
            );
        }
        if !self.late_fraction.is_finite() || !(0.0..=1.0).contains(&self.late_fraction) {
            return bad("late_fraction must be in [0.0, 1.0]; pass a fraction between 0 and 1");
        }
        validate_civil_date(self.start.0, self.start.1, self.start.2)?;
        Ok(())
    }

    /// Epoch day (days since `1970-01-01`) of `self.start`.
    #[must_use]
    pub fn start_epoch_day(&self) -> i64 {
        days_from_civil(i64::from(self.start.0), self.start.1, self.start.2)
    }

    /// Midnight UTC in microseconds since epoch for zero-based partition day `day`.
    #[must_use]
    pub fn day_start_us(&self, day: u32) -> i64 {
        (self.start_epoch_day() + i64::from(day)) * DAY_US
    }

    /// `YYYY-MM-DD` string for zero-based partition day `day`.
    #[must_use]
    pub fn day_string(&self, day: u32) -> String {
        let (y, m, d) = civil_from_days(self.start_epoch_day() + i64::from(day));
        format!("{y:04}-{m:02}-{d:02}")
    }
}

impl Default for MarketParams {
    fn default() -> Self {
        Self::rung(MarketRung::Small)
    }
}

/// Summary of a generated `market` corpus and its workload/truth files.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketSummary {
    /// Total Parquet data files written under `<out>/data/`.
    pub files: u64,
    /// Total row groups written across all data files.
    pub row_groups: u64,
    /// Total rows written across all data files.
    pub rows: u64,
    /// Total restatement rows written (corrected rows arriving 1–3 days after their `ts` day).
    pub restatements: u64,
    /// Total late base prints written (original prints arriving 1–3 days after their `ts` day).
    pub late_prints: u64,
    /// Total on-disk bytes across all Parquet data files under `<out>/data/`.
    pub bytes: u64,
}

/// Parameters controlling workload generation for a `market` corpus.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketWorkloadParams {
    /// RNG seed for workload query generation.
    pub seed: u64,
    /// Number of `window` queries to generate (default 18: 3 window widths × 3 symbol-set sizes × 2).
    pub window_queries: u32,
    /// Number of bitemporal `asof` queries to generate (default 6).
    pub asof_queries: u32,
    /// Number of `latest_known` queries to generate (default 6).
    pub latest_known_queries: u32,
}

impl Default for MarketWorkloadParams {
    fn default() -> Self {
        Self {
            seed: 29,
            window_queries: 18,
            asof_queries: 6,
            latest_known_queries: 6,
        }
    }
}

/// Kind of query in the `market` workload.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketQueryKind {
    /// Symbol set × `[from, to]` inclusive window on `ts`.
    Window,
    /// Bitemporal as-of lookup: symbol set, valid time `V` (`ts <= V`), knowledge time `K` (`known_at <= K`).
    Asof,
    /// Latest known lookup: symbol set, knowledge cutoff `K` (`known_at <= K`, unbounded valid time).
    LatestKnown,
}

/// Width category for a `window` query.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WindowSpan {
    /// 1-hour intraday window within trading hours (`14:30..21:00` UTC).
    Intraday1h,
    /// Full single UTC day (`[day_start, day_end]`).
    OneDay,
    /// Five consecutive UTC days (`[day_start, day_start + 5 days - 1 µs]`).
    FiveDay,
}

/// One recorded query in `workload/market.json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketQuery {
    /// Query identifier (`q0000`, `q0001`, …).
    pub id: String,
    /// Query kind (`window`, `asof`, or `latest_known`).
    pub kind: MarketQueryKind,
    /// Relative path from `<out>` to the single-column `symbol` Parquet key file (`workload/keys/qNNNN.parquet`).
    pub keys_file: String,
    /// Sorted list of queried symbols (matches the contents of `keys_file`).
    pub symbols: Vec<String>,
    /// Window span category when `kind == MarketQueryKind::Window`.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub window_span: Option<WindowSpan>,
    /// Inclusive lower bound on `ts` in RFC 3339 with microseconds (`window` only).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub from: Option<String>,
    /// Inclusive lower bound on `ts` in microseconds since UTC epoch (`window` only).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub from_us: Option<i64>,
    /// Inclusive upper bound on `ts` in RFC 3339 with microseconds (`window` only).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub to: Option<String>,
    /// Inclusive upper bound on `ts` in microseconds since UTC epoch (`window` only).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub to_us: Option<i64>,
    /// Valid-time cutoff `V` in RFC 3339 with microseconds (`asof` only).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub valid: Option<String>,
    /// Valid-time cutoff `V` in microseconds since UTC epoch (`asof` only).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub valid_us: Option<i64>,
    /// Knowledge-time cutoff `K` in RFC 3339 with microseconds (`asof` and `latest_known`).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub known: Option<String>,
    /// Knowledge-time cutoff `K` in microseconds since UTC epoch (`asof` and `latest_known`).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub known_us: Option<i64>,
    /// Human-readable description of what scenario this query exercises.
    pub note: String,
}

/// `<out>/workload/market.json`: the recorded `market` workload manifest.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MarketWorkload {
    /// [`MARKET_WORKLOAD_FORMAT`].
    pub format: String,
    /// Corpus parameters the workload was built for.
    pub corpus_params: MarketParams,
    /// Workload generation parameters.
    pub workload_params: MarketWorkloadParams,
    /// Queries in id order.
    pub queries: Vec<MarketQuery>,
}

/// Identifies one row group by its file path (relative to `<out>`) and zero-based row-group index.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FileRowGroup {
    /// Relative path from `<out>`, e.g. `data/day=2026-01-01/part-0000.parquet`.
    pub file: String,
    /// Zero-based row-group index within `file`.
    pub row_group: u32,
}

/// Ground truth for one `window` query.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowTruth {
    /// Exact set of `(file, row_group)` holding at least one row with `symbol in S` and `ts in [from, to]`.
    pub matching_row_groups: Vec<FileRowGroup>,
    /// Exact count of rows across the corpus with `symbol in S` and `ts in [from, to]`.
    pub matching_rows: u64,
    /// Funnel baseline: all `(file, row_group)` holding at least one row of any symbol in `S` (ignoring `ts`).
    pub symbol_row_groups: Vec<FileRowGroup>,
    /// Funnel baseline: all `(file, row_group)` whose overall `[min(ts), max(ts)]` overlaps `[from, to]` (ignoring `symbol`).
    pub ts_overlap_row_groups: Vec<FileRowGroup>,
    /// Sorted list of partition day strings (`YYYY-MM-DD`) of `matching_row_groups`.
    pub partition_days_touched: Vec<String>,
}

/// Winning row(s) for a single symbol in an `asof` or `latest_known` query.
///
/// Rule: among rows for `symbol` with `known_at <= K` (and `ts <= V` for `asof`),
/// pick the greatest `ts`, breaking ties by greatest `known_at`. Equal `(ts, known_at)`
/// ties keep every matching row and every `(file, row_group)` holding them.
#[derive(Clone, Debug, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct SymbolWinner {
    /// Symbol name (`S0001`).
    pub symbol: String,
    /// Winning valid time `ts` in RFC 3339 with microseconds.
    pub ts: String,
    /// Winning valid time `ts` in microseconds since UTC epoch.
    pub ts_us: i64,
    /// Winning knowledge time `known_at` in RFC 3339 with microseconds.
    pub known_at: String,
    /// Winning knowledge time `known_at` in microseconds since UTC epoch.
    pub known_at_us: i64,
    /// Winning row price(s): if multiple tied rows have identical `(ts, known_at)`, `price` is the first and `prices` lists all in sorted order.
    pub price: f64,
    /// Every winning row's price in ascending order (length > 1 only on exact `(ts, known_at)` ties).
    pub prices: Vec<f64>,
    /// Every `(file, row_group)` holding a winning row for this symbol, sorted and deduplicated.
    pub row_groups: Vec<FileRowGroup>,
}

/// Ground truth for one query in `<out>/workload/truth.json`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MarketQueryTruth {
    /// Query identifier (`q0000`, …).
    pub id: String,
    /// Query kind.
    pub kind: MarketQueryKind,
    /// Populated when `kind == MarketQueryKind::Window`.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub window: Option<WindowTruth>,
    /// Per-symbol winning rows in symbol order, populated when `kind` is `Asof` or `LatestKnown`.
    /// Symbols with no qualifying row before `(V, K)` are omitted.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub winners: Option<Vec<SymbolWinner>>,
}

/// `<out>/workload/truth.json`: ground truth for every query in `workload/market.json`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MarketTruth {
    /// [`MARKET_TRUTH_FORMAT`].
    pub format: String,
    /// Summary of the generated data files.
    pub summary: MarketSummary,
    /// Per-query ground truth in query id order.
    pub queries: Vec<MarketQueryTruth>,
}

/// In-memory row representation before writing and for direct ground-truth computation.
#[derive(Clone, Debug)]
struct MarketRow {
    symbol_idx: u32,
    ts_us: i64,
    known_at_us: i64,
    price: f64,
    size: i64,
    is_late: bool,
    is_restatement: bool,
}

/// Metadata and rows of one generated row group in memory.
#[derive(Clone, Debug)]
struct GeneratedRowGroup {
    file: String,
    day_date: String,
    row_group: u32,
    ts_min: i64,
    ts_max: i64,
    rows: Vec<MarketRow>,
}

/// Formats a 1-based symbol index (`0..symbols` -> `S0001..`).
#[must_use]
pub fn symbol_name(symbol_idx: u32) -> String {
    format!("S{:04}", symbol_idx + 1)
}

/// Parses a `YYYY-MM-DD` civil date string.
pub fn parse_ymd(s: &str) -> Result<(i32, u32, u32)> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 || parts[0].len() != 4 || parts[1].len() != 2 || parts[2].len() != 2 {
        return Err(BenchError::Invalid(format!(
            "start date {s:?} is not in YYYY-MM-DD format; pass a date like 2026-01-01"
        )));
    }
    let y: i32 = parts[0].parse().map_err(|_| {
        BenchError::Invalid(format!("invalid year in start date {s:?}; pass YYYY-MM-DD"))
    })?;
    let m: u32 = parts[1].parse().map_err(|_| {
        BenchError::Invalid(format!(
            "invalid month in start date {s:?}; pass YYYY-MM-DD"
        ))
    })?;
    let d: u32 = parts[2].parse().map_err(|_| {
        BenchError::Invalid(format!("invalid day in start date {s:?}; pass YYYY-MM-DD"))
    })?;
    validate_civil_date(y, m, d)?;
    Ok((y, m, d))
}

fn validate_civil_date(y: i32, m: u32, d: u32) -> Result<()> {
    if !(1970..=9999).contains(&y) {
        return Err(BenchError::Invalid(format!(
            "start year {y} must be in 1970..=9999"
        )));
    }
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return Err(BenchError::Invalid(format!(
            "start date {y:04}-{m:02}-{d:02} has out-of-range month or day"
        )));
    }
    let z = days_from_civil(i64::from(y), m, d);
    let (ry, rm, rd) = civil_from_days(z);
    if ry != i64::from(y) || rm != m || rd != d {
        return Err(BenchError::Invalid(format!(
            "start date {y:04}-{m:02}-{d:02} is not a valid calendar date"
        )));
    }
    Ok(())
}

/// Formats microseconds since the Unix epoch as RFC 3339 UTC with 6 fractional digits
/// (`YYYY-MM-DDTHH:MM:SS.ffffffZ`).
#[must_use]
pub fn format_rfc3339_us(us: i64) -> String {
    let days = us.div_euclid(DAY_US);
    let rem_us = us.rem_euclid(DAY_US);
    let (y, m, d) = civil_from_days(days);
    let total_secs = rem_us / 1_000_000;
    let micros = rem_us % 1_000_000;
    let hh = total_secs / 3600;
    let mm = (total_secs % 3600) / 60;
    let ss = total_secs % 60;
    format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}.{micros:06}Z")
}

/// Relative path of a partition file under `<out>`.
#[must_use]
pub fn market_file_rel_path(date: &str, file_in_day: u32) -> String {
    format!("data/day={date}/part-{file_in_day:04}.parquet")
}

/// Arrow schema of every generated market Parquet file.
#[must_use]
pub fn market_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(MARKET_COLUMNS[0], DataType::Utf8, false),
        Field::new(
            MARKET_COLUMNS[1],
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new(
            MARKET_COLUMNS[2],
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new(MARKET_COLUMNS[3], DataType::Float64, false),
        Field::new(MARKET_COLUMNS[4], DataType::Int64, false),
    ]))
}

fn writer_properties(rows_per_row_group: u32) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_row_count(Some(rows_per_row_group as usize + 1))
        .set_created_by("bellwether-bench market".to_string())
        .build()
}

/// Generates the deterministic in-memory rows partitioned by their `known_at` day.
fn build_partition_days(params: &MarketParams) -> Vec<Vec<MarketRow>> {
    let mut partitions: Vec<Vec<MarketRow>> = (0..params.days).map(|_| Vec::new()).collect();
    let rth_span_us = MARKET_CLOSE_US - MARKET_OPEN_US;

    for event_day in 0..params.days {
        let day_start = params.day_start_us(event_day);
        for sym in 0..params.symbols {
            let mut rng = chacha(&[
                params.seed,
                0x4D41_524B_4554_0001,
                u64::from(event_day),
                u64::from(sym),
            ]);
            for _ in 0..params.prints_per_symbol_per_day {
                let offset_us = rng.gen_range(0..rth_span_us);
                let ts_us = day_start + MARKET_OPEN_US + offset_us;
                let price_cents = rng.gen_range(1_000i64..=50_000i64);
                let price = price_cents as f64 / 100.0;
                let size = rng.gen_range(1i64..=10_000i64);

                let is_late = params.late_fraction > 0.0 && rng.gen_bool(params.late_fraction);
                if is_late {
                    let delta_days = rng.gen_range(1u32..=3u32);
                    let known_day = event_day + delta_days;
                    let known_offset_us = rng.gen_range(0..rth_span_us);
                    // Consume restatement RNG rolls deterministically on the alternate branch not taken.
                    if known_day < params.days {
                        let known_at_us =
                            params.day_start_us(known_day) + MARKET_OPEN_US + known_offset_us;
                        partitions[known_day as usize].push(MarketRow {
                            symbol_idx: sym,
                            ts_us,
                            known_at_us,
                            price,
                            size,
                            is_late: true,
                            is_restatement: false,
                        });
                    }
                    // Late prints beyond the last day are dropped (never clamped).
                } else {
                    let delay_us = rng.gen_range(0..=NORMAL_KNOWN_DELAY_MAX_US);
                    let known_at_us = ts_us + delay_us;
                    partitions[event_day as usize].push(MarketRow {
                        symbol_idx: sym,
                        ts_us,
                        known_at_us,
                        price,
                        size,
                        is_late: false,
                        is_restatement: false,
                    });

                    let is_restated = params.restatement_fraction > 0.0
                        && rng.gen_bool(params.restatement_fraction);
                    if is_restated {
                        let delta_days = rng.gen_range(1u32..=3u32);
                        let restate_day = event_day + delta_days;
                        let restate_offset_us = rng.gen_range(0..rth_span_us);
                        // Ensure the restated price differs from the original price.
                        let delta_cents = rng.gen_range(1i64..=500i64);
                        let new_cents = if price_cents + delta_cents <= 50_000 {
                            price_cents + delta_cents
                        } else {
                            price_cents - delta_cents
                        };
                        let new_price = new_cents as f64 / 100.0;
                        if restate_day < params.days {
                            let restate_known_us = params.day_start_us(restate_day)
                                + MARKET_OPEN_US
                                + restate_offset_us;
                            partitions[restate_day as usize].push(MarketRow {
                                symbol_idx: sym,
                                ts_us,
                                known_at_us: restate_known_us,
                                price: new_price,
                                size,
                                is_late: false,
                                is_restatement: true,
                            });
                        }
                        // Restatements whose known_at day falls beyond `days` are dropped.
                    }
                }
            }
        }
    }

    for day_rows in &mut partitions {
        day_rows.sort_by(|a, b| {
            (a.symbol_idx, a.ts_us, a.known_at_us)
                .cmp(&(b.symbol_idx, b.ts_us, b.known_at_us))
                .then_with(|| a.price.total_cmp(&b.price))
                .then_with(|| a.size.cmp(&b.size))
        });
    }

    partitions
}

fn rows_to_batch(
    rows: &[MarketRow],
    symbols_table: &[String],
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let n = rows.len();
    let mut sym_b = StringBuilder::with_capacity(n, n * 6);
    let mut ts_b = TimestampMicrosecondBuilder::with_capacity(n).with_timezone("UTC");
    let mut known_b = TimestampMicrosecondBuilder::with_capacity(n).with_timezone("UTC");
    let mut price_b = Float64Builder::with_capacity(n);
    let mut size_b = Int64Builder::with_capacity(n);

    for r in rows {
        sym_b.append_value(&symbols_table[r.symbol_idx as usize]);
        ts_b.append_value(r.ts_us);
        known_b.append_value(r.known_at_us);
        price_b.append_value(r.price);
        size_b.append_value(r.size);
    }

    let cols: Vec<ArrayRef> = vec![
        Arc::new(sym_b.finish()),
        Arc::new(ts_b.finish()),
        Arc::new(known_b.finish()),
        Arc::new(price_b.finish()),
        Arc::new(size_b.finish()),
    ];
    Ok(RecordBatch::try_new(schema.clone(), cols)?)
}

fn prepare_market_out_dir(out: &Path) -> Result<()> {
    if out.exists() {
        let is_market =
            out.join("workload").join("market.json").exists() || out.join("data").exists();
        let mut entries = std::fs::read_dir(out).map_err(io_err(out))?;
        if !is_market && entries.next().is_some() {
            return Err(BenchError::Invalid(format!(
                "{} is not empty and is not a market corpus; pass an empty --out directory",
                out.display()
            )));
        }
        for sub in ["data", "workload"] {
            let p = out.join(sub);
            if p.exists() {
                std::fs::remove_dir_all(&p).map_err(io_err(&p))?;
            }
        }
    }
    std::fs::create_dir_all(out).map_err(io_err(out))
}

/// Generates the synthetic `market` corpus under `<out>/data/`, plus the
/// default recorded workload under `<out>/workload/market.json`, key files under
/// `<out>/workload/keys/qNNNN.parquet`, and exact ground truth under
/// `<out>/workload/truth.json`.
pub fn generate_market(out: &Path, p: &MarketParams) -> Result<MarketSummary> {
    let (summary, _, _) = generate_market_all(out, p, &MarketWorkloadParams::default())?;
    Ok(summary)
}

/// Generates (or regenerates) the `market` workload and `truth.json` for `out`
/// using `p` and `wp`. If `<out>/data` has not been generated yet, generates the
/// corpus first.
pub fn generate_market_workload(
    out: &Path,
    corpus_params: &MarketParams,
    wp: &MarketWorkloadParams,
) -> Result<(MarketWorkload, MarketTruth)> {
    let (_, workload, truth) = generate_market_all(out, corpus_params, wp)?;
    Ok((workload, truth))
}

/// Generates the data files, workload, and ground truth in one deterministic pass.
pub fn generate_market_all(
    out: &Path,
    p: &MarketParams,
    wp: &MarketWorkloadParams,
) -> Result<(MarketSummary, MarketWorkload, MarketTruth)> {
    p.validate()?;
    prepare_market_out_dir(out)?;

    let symbols_table: Vec<String> = (0..p.symbols).map(symbol_name).collect();
    let partitions = build_partition_days(p);
    let schema = market_schema();
    let props = writer_properties(p.rows_per_row_group);

    let mut files_count = 0u64;
    let mut rgs_count = 0u64;
    let mut rows_count = 0u64;
    let mut restatements_count = 0u64;
    let mut late_prints_count = 0u64;
    let mut bytes_count = 0u64;
    let mut generated_rgs: Vec<GeneratedRowGroup> = Vec::new();

    for (day_idx, day_rows) in partitions.iter().enumerate() {
        let day = day_idx as u32;
        let date = p.day_string(day);
        let n_files = p.files_per_day as usize;
        let total_day_rows = day_rows.len();

        for f in 0..n_files {
            let start_row = (total_day_rows * f) / n_files;
            let end_row = (total_day_rows * (f + 1)) / n_files;
            let file_rows = &day_rows[start_row..end_row];

            let rel = market_file_rel_path(&date, f as u32);
            let path = out.join(&rel);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(io_err(parent))?;
            }
            let sink = BufWriter::new(File::create(&path).map_err(io_err(&path))?);
            let mut writer = ArrowWriter::try_new(sink, schema.clone(), Some(props.clone()))?;

            let rg_size = p.rows_per_row_group as usize;
            for (rg_idx, chunk) in file_rows.chunks(rg_size).enumerate() {
                let batch = rows_to_batch(chunk, &symbols_table, &schema)?;
                writer.write(&batch)?;
                writer.flush()?;

                let mut ts_min = i64::MAX;
                let mut ts_max = i64::MIN;
                for r in chunk {
                    ts_min = ts_min.min(r.ts_us);
                    ts_max = ts_max.max(r.ts_us);
                    if r.is_restatement {
                        restatements_count += 1;
                    }
                    if r.is_late {
                        late_prints_count += 1;
                    }
                }
                rows_count += chunk.len() as u64;
                rgs_count += 1;
                generated_rgs.push(GeneratedRowGroup {
                    file: rel.clone(),
                    day_date: date.clone(),
                    row_group: rg_idx as u32,
                    ts_min,
                    ts_max,
                    rows: chunk.to_vec(),
                });
            }

            writer.close()?;
            let file_bytes = std::fs::metadata(&path).map_err(io_err(&path))?.len();
            bytes_count += file_bytes;
            files_count += 1;
        }
    }

    let summary = MarketSummary {
        files: files_count,
        row_groups: rgs_count,
        rows: rows_count,
        restatements: restatements_count,
        late_prints: late_prints_count,
        bytes: bytes_count,
    };

    let workload = build_workload(out, p, wp, &symbols_table, &generated_rgs)?;
    let truth = build_truth(&summary, &workload, &symbols_table, &generated_rgs);

    write_json(&out.join("workload").join("market.json"), &workload)?;
    write_json(&out.join("workload").join("truth.json"), &truth)?;

    Ok((summary, workload, truth))
}

fn write_symbol_keys(path: &Path, symbols: &[String]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(io_err(parent))?;
    }
    let schema = Arc::new(Schema::new(vec![Field::new(
        "symbol",
        DataType::Utf8,
        false,
    )]));
    let array = StringArray::from(symbols.to_vec());
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)])?;
    let file = File::create(path).map_err(io_err(path))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_created_by("bellwether-bench market".to_string())
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

/// Reads back a `workload/keys/qNNNN.parquet` symbol list.
pub fn read_symbol_keys(path: &Path) -> Result<Vec<String>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let file = File::open(path).map_err(io_err(path))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch?;
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                BenchError::Invalid(format!("{}: symbol column is not Utf8", path.display()))
            })?;
        for i in 0..col.len() {
            out.push(col.value(i).to_string());
        }
    }
    Ok(out)
}

fn draw_symbol_set(
    symbols_table: &[String],
    target_size: usize,
    rng: &mut rand_chacha::ChaCha12Rng,
) -> Vec<String> {
    let k = target_size.min(symbols_table.len()).max(1);
    let mut indices: Vec<usize> = (0..symbols_table.len()).collect();
    indices.shuffle(rng);
    indices.truncate(k);
    indices.sort_unstable();
    indices
        .into_iter()
        .map(|i| symbols_table[i].clone())
        .collect()
}

fn build_workload(
    out: &Path,
    p: &MarketParams,
    wp: &MarketWorkloadParams,
    symbols_table: &[String],
    generated_rgs: &[GeneratedRowGroup],
) -> Result<MarketWorkload> {
    let mut rng = chacha(&[p.seed, wp.seed, 0x574F_524B_4C4F_4144]);
    let mut queries = Vec::new();
    let mut q_idx = 0usize;

    let set_sizes = [1usize, 10, 50];
    let spans = [
        WindowSpan::Intraday1h,
        WindowSpan::OneDay,
        WindowSpan::FiveDay,
    ];
    let n_window = wp.window_queries as usize;

    for i in 0..n_window {
        let span = spans[i % spans.len()];
        let set_size = set_sizes[(i / spans.len()) % set_sizes.len()];
        let symbols = draw_symbol_set(symbols_table, set_size, &mut rng);

        let (from_us, to_us, note) = match span {
            WindowSpan::Intraday1h => {
                let d = rng.gen_range(0..p.days);
                // Pick an hour start within 14:30..=20:00 UTC so [from, from + 1h] stays inside RTH.
                let max_start_offset = (MARKET_CLOSE_US - MARKET_OPEN_US) - 3_600 * 1_000_000;
                let start_offset = rng.gen_range(0..=max_start_offset);
                let from = p.day_start_us(d) + MARKET_OPEN_US + start_offset;
                let to = from + 3_600 * 1_000_000 - 1;
                (
                    from,
                    to,
                    format!(
                        "1-hour intraday window on day {} across {} symbol(s)",
                        p.day_string(d),
                        symbols.len()
                    ),
                )
            }
            WindowSpan::OneDay => {
                let d = rng.gen_range(0..p.days);
                let from = p.day_start_us(d);
                let to = from + DAY_US - 1;
                (
                    from,
                    to,
                    format!(
                        "1-day window on {} across {} symbol(s)",
                        p.day_string(d),
                        symbols.len()
                    ),
                )
            }
            WindowSpan::FiveDay => {
                let max_start_day = p.days.saturating_sub(5);
                let d = if max_start_day > 0 {
                    rng.gen_range(0..=max_start_day)
                } else {
                    0
                };
                let span_days = p.days.min(5);
                let from = p.day_start_us(d);
                let to = from + i64::from(span_days) * DAY_US - 1;
                (
                    from,
                    to,
                    format!(
                        "{span_days}-day window starting {} across {} symbol(s)",
                        p.day_string(d),
                        symbols.len()
                    ),
                )
            }
        };

        let id = format!("q{q_idx:04}");
        q_idx += 1;
        let keys_rel = format!("workload/keys/{id}.parquet");
        write_symbol_keys(&out.join(&keys_rel), &symbols)?;

        queries.push(MarketQuery {
            id,
            kind: MarketQueryKind::Window,
            keys_file: keys_rel,
            symbols,
            window_span: Some(span),
            from: Some(format_rfc3339_us(from_us)),
            from_us: Some(from_us),
            to: Some(format_rfc3339_us(to_us)),
            to_us: Some(to_us),
            valid: None,
            valid_us: None,
            known: None,
            known_us: None,
            note,
        });
    }

    // Collect restatement rows and late rows from the generated corpus so we can
    // deterministically anchor asof queries right before and right after restatements
    // and late arrivals.
    let mut restatements: Vec<&MarketRow> = Vec::new();
    let mut late_rows: Vec<&MarketRow> = Vec::new();
    for rg in generated_rgs {
        for r in &rg.rows {
            if r.is_restatement {
                restatements.push(r);
            }
            if r.is_late {
                late_rows.push(r);
            }
        }
    }

    let n_asof = wp.asof_queries as usize;
    for i in 0..n_asof {
        let set_size = set_sizes[i % set_sizes.len()];
        let (symbols, valid_us, known_us, note) = if i % 3 != 2 && !restatements.is_empty() {
            // Pair of queries around a restatement: even index = K before restatement became known,
            // odd index = K at/after restatement became known.
            let target = restatements[(i / 2) % restatements.len()];
            let target_sym = symbols_table[target.symbol_idx as usize].clone();
            let mut syms = draw_symbol_set(symbols_table, set_size, &mut rng);
            if !syms.contains(&target_sym) {
                syms[0].clone_from(&target_sym);
                syms.sort();
            }
            let valid = target.ts_us;
            if i % 2 == 0 {
                // K after original print (ts + 500ms) but strictly before restatement known_at
                let known = (target.ts_us + NORMAL_KNOWN_DELAY_MAX_US).min(target.known_at_us - 1);
                (
                    syms,
                    valid,
                    known,
                    format!(
                        "asof around restatement on {target_sym}: K before restatement became known"
                    ),
                )
            } else {
                let known = target.known_at_us;
                (
                    syms,
                    valid,
                    known,
                    format!(
                        "asof around restatement on {target_sym}: K at/after restatement became known"
                    ),
                )
            }
        } else if !late_rows.is_empty() {
            let target = late_rows[i % late_rows.len()];
            let target_sym = symbols_table[target.symbol_idx as usize].clone();
            let mut syms = draw_symbol_set(symbols_table, set_size, &mut rng);
            if !syms.contains(&target_sym) {
                syms[0].clone_from(&target_sym);
                syms.sort();
            }
            let valid = target.ts_us;
            let known = if (i / 3) % 2 == 0 {
                target.known_at_us - 1
            } else {
                target.known_at_us
            };
            let phase = if known < target.known_at_us {
                "before late print arrived"
            } else {
                "at/after late print arrived"
            };
            (
                syms,
                valid,
                known,
                format!("asof on late print for {target_sym}: K {phase}"),
            )
        } else {
            let syms = draw_symbol_set(symbols_table, set_size, &mut rng);
            let d = rng.gen_range(0..p.days);
            let valid = p.day_start_us(d) + MARKET_OPEN_US + 3_600 * 1_000_000;
            let known = valid + DAY_US;
            (
                syms,
                valid,
                known,
                "general bitemporal asof lookup".to_string(),
            )
        };

        let id = format!("q{q_idx:04}");
        q_idx += 1;
        let keys_rel = format!("workload/keys/{id}.parquet");
        write_symbol_keys(&out.join(&keys_rel), &symbols)?;

        queries.push(MarketQuery {
            id,
            kind: MarketQueryKind::Asof,
            keys_file: keys_rel,
            symbols,
            window_span: None,
            from: None,
            from_us: None,
            to: None,
            to_us: None,
            valid: Some(format_rfc3339_us(valid_us)),
            valid_us: Some(valid_us),
            known: Some(format_rfc3339_us(known_us)),
            known_us: Some(known_us),
            note,
        });
    }

    let n_latest = wp.latest_known_queries as usize;
    for i in 0..n_latest {
        let set_size = set_sizes[i % set_sizes.len()];
        let symbols = draw_symbol_set(symbols_table, set_size, &mut rng);
        // Spread K across the corpus timeline (from day 0 mid-session through end of last day).
        let day = ((i as u32) * p.days / (n_latest.max(1) as u32)).min(p.days - 1);
        let offset = rng.gen_range(MARKET_OPEN_US..=MARKET_CLOSE_US);
        let known_us = p.day_start_us(day) + offset;

        let id = format!("q{q_idx:04}");
        q_idx += 1;
        let keys_rel = format!("workload/keys/{id}.parquet");
        write_symbol_keys(&out.join(&keys_rel), &symbols)?;

        queries.push(MarketQuery {
            id,
            kind: MarketQueryKind::LatestKnown,
            keys_file: keys_rel,
            symbols,
            window_span: None,
            from: None,
            from_us: None,
            to: None,
            to_us: None,
            valid: None,
            valid_us: None,
            known: Some(format_rfc3339_us(known_us)),
            known_us: Some(known_us),
            note: format!(
                "latest_known cutoff on day {} across {} symbol(s)",
                p.day_string(day),
                set_size.min(symbols_table.len())
            ),
        });
    }

    Ok(MarketWorkload {
        format: MARKET_WORKLOAD_FORMAT.to_string(),
        corpus_params: p.clone(),
        workload_params: wp.clone(),
        queries,
    })
}

fn build_truth(
    summary: &MarketSummary,
    workload: &MarketWorkload,
    symbols_table: &[String],
    generated_rgs: &[GeneratedRowGroup],
) -> MarketTruth {
    let sym_to_idx: BTreeMap<&str, u32> = symbols_table
        .iter()
        .enumerate()
        .map(|(i, s)| (s.as_str(), i as u32))
        .collect();

    let mut queries_truth = Vec::with_capacity(workload.queries.len());

    for q in &workload.queries {
        let wanted_syms: BTreeSet<u32> = q
            .symbols
            .iter()
            .filter_map(|s| sym_to_idx.get(s.as_str()).copied())
            .collect();

        match q.kind {
            MarketQueryKind::Window => {
                let from_us = q.from_us.unwrap_or(i64::MIN);
                let to_us = q.to_us.unwrap_or(i64::MAX);

                let mut matching_row_groups = Vec::new();
                let mut matching_rows = 0u64;
                let mut symbol_row_groups = Vec::new();
                let mut ts_overlap_row_groups = Vec::new();
                let mut days_touched = BTreeSet::new();

                for rg in generated_rgs {
                    let frg = FileRowGroup {
                        file: rg.file.clone(),
                        row_group: rg.row_group,
                    };

                    if !rg.rows.is_empty() && rg.ts_min <= to_us && rg.ts_max >= from_us {
                        ts_overlap_row_groups.push(frg.clone());
                    }

                    let mut has_sym = false;
                    let mut rg_matches = 0u64;
                    for r in &rg.rows {
                        if wanted_syms.contains(&r.symbol_idx) {
                            has_sym = true;
                            if r.ts_us >= from_us && r.ts_us <= to_us {
                                rg_matches += 1;
                            }
                        }
                    }

                    if has_sym {
                        symbol_row_groups.push(frg.clone());
                    }
                    if rg_matches > 0 {
                        matching_rows += rg_matches;
                        days_touched.insert(rg.day_date.clone());
                        matching_row_groups.push(frg);
                    }
                }

                matching_row_groups.sort();
                symbol_row_groups.sort();
                ts_overlap_row_groups.sort();

                queries_truth.push(MarketQueryTruth {
                    id: q.id.clone(),
                    kind: q.kind,
                    window: Some(WindowTruth {
                        matching_row_groups,
                        matching_rows,
                        symbol_row_groups,
                        ts_overlap_row_groups,
                        partition_days_touched: days_touched.into_iter().collect(),
                    }),
                    winners: None,
                });
            }
            MarketQueryKind::Asof | MarketQueryKind::LatestKnown => {
                // Per symbol: track best (ts_us, known_at_us), its prices, and its (file, rg) locations.
                type WinnerAcc = (i64, i64, Vec<f64>, BTreeSet<FileRowGroup>);
                let valid_cutoff = if q.kind == MarketQueryKind::Asof {
                    q.valid_us.unwrap_or(i64::MAX)
                } else {
                    i64::MAX
                };
                let known_cutoff = q.known_us.unwrap_or(i64::MAX);
                let mut best: BTreeMap<u32, WinnerAcc> = BTreeMap::new();

                for rg in generated_rgs {
                    for r in &rg.rows {
                        if !wanted_syms.contains(&r.symbol_idx) {
                            continue;
                        }
                        if r.ts_us > valid_cutoff || r.known_at_us > known_cutoff {
                            continue;
                        }
                        let candidate_key = (r.ts_us, r.known_at_us);
                        let frg = FileRowGroup {
                            file: rg.file.clone(),
                            row_group: rg.row_group,
                        };
                        match best.get_mut(&r.symbol_idx) {
                            None => {
                                let mut set = BTreeSet::new();
                                set.insert(frg);
                                best.insert(
                                    r.symbol_idx,
                                    (r.ts_us, r.known_at_us, vec![r.price], set),
                                );
                            }
                            Some(entry) => {
                                let cur_key = (entry.0, entry.1);
                                if candidate_key > cur_key {
                                    entry.0 = r.ts_us;
                                    entry.1 = r.known_at_us;
                                    entry.2.clear();
                                    entry.2.push(r.price);
                                    entry.3.clear();
                                    entry.3.insert(frg);
                                } else if candidate_key == cur_key {
                                    entry.2.push(r.price);
                                    entry.3.insert(frg);
                                }
                            }
                        }
                    }
                }

                let mut winners = Vec::with_capacity(best.len());
                for (sym_idx, (ts_us, known_at_us, mut prices, rgs)) in best {
                    prices.sort_by(f64::total_cmp);
                    let price = prices[0];
                    winners.push(SymbolWinner {
                        symbol: symbols_table[sym_idx as usize].clone(),
                        ts: format_rfc3339_us(ts_us),
                        ts_us,
                        known_at: format_rfc3339_us(known_at_us),
                        known_at_us,
                        price,
                        prices,
                        row_groups: rgs.into_iter().collect(),
                    });
                }

                queries_truth.push(MarketQueryTruth {
                    id: q.id.clone(),
                    kind: q.kind,
                    window: None,
                    winners: Some(winners),
                });
            }
        }
    }

    MarketTruth {
        format: MARKET_TRUTH_FORMAT.to_string(),
        summary: summary.clone(),
        queries: queries_truth,
    }
}
