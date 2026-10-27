//! Benchmark harness for Bellwether (WP10, ADR 018, ADR 019).
//!
//! The benchmark design is fixed before any index exists (ADR 019):
//!
//! - [`generate`] writes a synthetic versioned ledger as Parquet, partitioned
//!   by day, with planted anti-patterns. It streams one row group at a time.
//! - [`workload`] writes recorded incremental key sets whose sizes hit given
//!   p50, p95, and max targets, each hashed with BLAKE3.
//! - [`truth`] scans the generated data and writes `expected.json`: the exact
//!   fanout of every planted anti-pattern the advisor must recover.
//! - [`harness`] runs every recorded key set through every strategy (raw
//!   scan, Parquet statistics and bloom filters, DataFusion, DuckDB, and
//!   later the index) and writes `results.json`.
//! - [`slo`] asserts the budgets that apply today and names the ones that
//!   will apply when the index lands.
//! - [`report`] renders `results.json` as a self-contained HTML page.
//! - [`market`] writes a deterministic synthetic bitemporal market trade corpus
//!   (`symbol`, `ts`, `known_at`, `price`, `size`), time-series workloads
//!   (`window`, `asof`, `latest_known`), and exact ground truth (`truth.json`).
//! - [`expr`] writes a corpus with a JSON attribute column on each shape
//!   (ledger, market), key files for equality on a JSON path at three
//!   selectivities, and exact ground truth, for `expr` keys.
//! - [`perf`] reads a lookup's planned row groups (or every row group under a
//!   folder) through the byte-counting reader, for the release measurements
//!   of `scripts/perf.sh`.
//! - [`perfcheck`] judges a `scripts/perf.sh` results file against the
//!   budgets in `docs/measured/budgets.toml` (`bellwether bench perf-check`,
//!   WP22b): two consecutive nightly runs past a budget, or one labeled
//!   pull-request run past it by the 1.5x tolerance, is a regression.
//! - [`text`] writes a text corpus (1–4 KB documents with planted terms at
//!   1% and 0.01% of rows) in the ledger or market shape, with exact ground
//!   truth, for text keys (B5; `scripts/text-funnels.py`).
//! - [`lists`] writes a list column with skewed element cardinality in both
//!   shapes (ledger attributes, market holdings), `contains_any` runs of 10,
//!   10^3, and 10^5 terms and a `contains_all` run, and exact ground truth,
//!   for list keys (B4).
//! - [`joins`] writes two datasets beside each other in both shapes (ledger
//!   orders and payables on `order_id`, market trades and quotes on
//!   `(symbol, minute)`) with 1:N skew, semi-join key sets of 10, 10^3, and
//!   10^5 keys, and exact ground truth per side, for join keys (B3).
//! - [`agg`] writes the ledger corpus for agg keys (B7): decimal amounts at
//!   scale 6 over skewed accounts with null amounts, per-key sum runs of 10,
//!   10^3, and 10^5 accounts, and exact per-run sums (`scripts/agg-funnels.py`).
//! - [`tombstone`] writes the corpora for tombstones (B8) in both shapes
//!   (the ledger with voids, market prints with retractions): rows in time
//!   order, `latest`/`asof` runs of 10, 10^3, and 10^5 keys, and exact
//!   per-run truth (`scripts/tombstone-funnels.py`).
//! - [`relational`] writes the two-dataset corpus relational replay is
//!   priced on (WP15): `events` (an event log keyed by `repo_id`, in time
//!   order) beside `repos` (one row per repository plus restated rows, so
//!   per-key counts differ from 1), key files of 10, 100, and 1,000
//!   repositories, and the consumer plans over them (`plans/*.json`).

#![forbid(unsafe_code)]
// Benchmark arithmetic converts freely between counts, sizes, and floating
// point for quantiles and chart coordinates. Every such value is bounded far
// below the precision limits these lints guard against.
#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]
// Product names (DuckDB, SplitMix64) read better without backticks; chart
// renderers are long, linear SVG emitters; key sets use the std hasher.
#![allow(
    clippy::doc_markdown,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::implicit_hasher
)]
#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod agg;
pub mod corpus;
pub mod env;
pub mod expr;
pub mod generate;
pub mod harness;
pub mod joins;
pub mod lists;
pub mod market;
pub mod mem;
pub mod perf;
pub mod perfcheck;
pub mod relational;
pub mod report;
pub mod results;
pub mod slo;
pub mod stats;
pub mod text;
pub mod tombstone;
pub mod truth;
pub mod workload;

use std::path::PathBuf;

/// A lookup key: one `entity_id`. Key sets handed to an
/// [`harness::IndexUnderTest`] are sorted ascending and deduplicated.
pub type Key = u64;

/// Errors the benchmark can raise.
#[derive(Debug, thiserror::Error)]
pub enum BenchError {
    /// Filesystem failure, with the path involved.
    #[error("io error at {path}: {source}")]
    Io {
        /// Path being read or written.
        path: PathBuf,
        /// Underlying error.
        source: std::io::Error,
    },
    /// Parquet encode or decode failure.
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    /// Arrow failure.
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// JSON failure.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// Parameters or inputs that make no sense.
    #[error("invalid input: {0}")]
    Invalid(String),
    /// A strategy failed (for example DuckDB returned an error).
    #[error("strategy {strategy} failed: {message}")]
    Strategy {
        /// Strategy name.
        strategy: String,
        /// What went wrong.
        message: String,
    },
    /// One or more SLO assertions failed.
    #[error("SLO assertions failed:\n{0}")]
    Slo(String),
}

/// Result alias for the benchmark.
pub type Result<T> = std::result::Result<T, BenchError>;

/// Attaches a path to an IO error.
pub(crate) fn io_err(path: impl Into<PathBuf>) -> impl FnOnce(std::io::Error) -> BenchError {
    let path = path.into();
    move |source| BenchError::Io { path, source }
}

/// Writes pretty JSON with a trailing newline, creating parent directories.
pub(crate) fn write_json<T: serde::Serialize>(path: &std::path::Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(io_err(parent))?;
    }
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    std::fs::write(path, bytes).map_err(io_err(path))
}

/// Reads JSON from a file.
pub(crate) fn read_json<T: serde::de::DeserializeOwned>(path: &std::path::Path) -> Result<T> {
    let bytes = std::fs::read(path).map_err(io_err(path))?;
    Ok(serde_json::from_slice(&bytes)?)
}
