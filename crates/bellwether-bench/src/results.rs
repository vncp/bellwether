//! The `results.json` schema. Documented field by field in the crate README.

use bellwether_types::{Hash, RunCost};
use serde::{Deserialize, Serialize};

use crate::corpus::GenParams;
use crate::env::Environment;
use crate::generate::GeneratorStats;
use crate::truth::Expected;

/// Format tag of `results.json`.
pub const RESULTS_FORMAT: &str = "bellwether-bench-results/2";

/// Strategy name: read every row group.
pub const RAW_SCAN: &str = "raw_scan";
/// Strategy name: prune by footer min/max of `entity_id`.
pub const PARQUET_STATS: &str = "parquet_stats";
/// Strategy name: DuckDB semi-join over the key file.
pub const DUCKDB: &str = "duckdb";
/// Strategy name reserved for the Bellwether index (WP1/WP4).
pub const INDEX: &str = "index";
/// Strategy name: Apache DataFusion's Parquet pruning (statistics, page
/// index, bloom filters).
pub const DATAFUSION: &str = "datafusion";

/// Top level of `results.json`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Results {
    /// [`RESULTS_FORMAT`].
    pub format: String,
    /// `bellwether-bench <version>`.
    pub generated_by: String,
    /// Where and how the numbers were produced (docs/benchmark.md).
    pub environment: Environment,
    /// One entry per corpus, in the order given on the command line.
    pub corpora: Vec<CorpusResult>,
}

/// How a strategy produces its numbers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyKind {
    /// Implements `IndexUnderTest`: returns a `ScanPlan`, whose cost the
    /// harness recomputes from the footers.
    Plan,
    /// An external engine: only latency, matched rows, and the exact amount
    /// total are observable; files, row groups, and bytes are `null`.
    Engine,
}

/// A strategy that took part.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StrategyInfo {
    /// Name, for example [`RAW_SCAN`].
    pub name: String,
    /// Kind.
    pub kind: StrategyKind,
    /// False when the strategy could not run in this build or environment.
    pub available: bool,
    /// One-time setup in milliseconds (open, warm up), if any.
    pub setup_ms: Option<f64>,
    /// Why unavailable, or anything a reader must know.
    pub note: Option<String>,
}

/// Summary of the recorded workload used.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadSummary {
    /// Workload hash from `workload.json`.
    pub hash: Hash,
    /// Number of runs.
    pub runs: u64,
    /// Achieved nearest-rank `[p50, p95, max]` keys per run.
    pub sizes_p50_p95_max: [u64; 3],
    /// Whether sizes were clamped to the entities available.
    pub clamped: bool,
}

/// One index build, reserved for WP4. The build-memory budget is asserted
/// against these, never against lookup results.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BuildResult {
    /// Strategy that built.
    pub strategy: String,
    /// Worker threads.
    pub workers: u64,
    /// Peak resident bytes per worker, measured.
    pub peak_memory_per_worker_bytes: Option<u64>,
    /// Wall-clock milliseconds.
    pub elapsed_ms: Option<f64>,
}

/// Everything measured on one corpus.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CorpusResult {
    /// BLAKE3 of `corpus.json`.
    pub corpus_id: Hash,
    /// Corpus directory as given.
    pub path: String,
    /// Generator parameters.
    pub params: GenParams,
    /// Files.
    pub files_total: u64,
    /// Row groups.
    pub row_groups_total: u64,
    /// Rows.
    pub rows_total: u64,
    /// Compressed row-group bytes (the x axis of latency versus corpus size).
    pub bytes_total: u64,
    /// File bytes on disk including footers.
    pub file_bytes_total: u64,
    /// `generator-stats.json`, if present.
    pub generator: Option<GeneratorStats>,
    /// The workload.
    pub workload: WorkloadSummary,
    /// Whether planned row groups were actually read (`scan_ms` present).
    pub scan_executed: bool,
    /// Strategies in column order.
    pub strategies: Vec<StrategyInfo>,
    /// One entry per recorded run, in run-id order.
    pub runs: Vec<RunResult>,
    /// Index builds (WP4); empty until the index lands.
    #[serde(default)]
    pub build: Vec<BuildResult>,
    /// Per-strategy aggregates over runs, for display. The SLO checks read
    /// `runs`, never this.
    pub summary: Vec<StrategySummary>,
    /// `expected.json`, if present, embedded so the report is self-contained.
    pub ground_truth: Option<Expected>,
}

/// The exact minimum any correct index could read for a run: the row groups
/// that hold at least one queried key, found by scanning `entity_id`. A
/// reference, not a strategy; it checks the others for false negatives.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OracleResult {
    /// Files holding a key.
    pub files: u64,
    /// Row groups holding a key.
    pub row_groups: u64,
    /// Compressed bytes of those row groups, from the footers.
    pub bytes: u64,
    /// Rows whose `entity_id` is in the key set.
    pub matched_rows: u64,
    /// Exact sum of `amount` over those rows, as a decimal string.
    pub amount_total: String,
}

/// Row groups at each stage of pruning, for the funnel chart.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Funnel {
    /// Row groups in the corpus.
    pub row_groups_total: u64,
    /// After footer min/max pruning (`parquet_stats`), recomputed.
    pub after_stats: Option<u64>,
    /// After the Bellwether index; `null` until WP1/WP4 land.
    pub after_index: Option<u64>,
    /// The exact minimum ([`OracleResult::row_groups`]).
    pub oracle: u64,
}

/// One recorded run.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RunResult {
    /// `run-NNNN`.
    pub run_id: String,
    /// Keys in the run.
    pub keys: u64,
    /// The run's key-set hash from `workload.json`.
    pub key_hash: Hash,
    /// Exact minimum.
    pub oracle: OracleResult,
    /// Funnel.
    pub funnel: Funnel,
    /// One entry per available strategy.
    pub results: Vec<StrategyResult>,
}

/// What a plan said about itself, kept beside the recomputed cost.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportedCost {
    /// `ScanPlan::bytes`.
    pub bytes: u64,
    /// `ScanPlan::row_groups_selected`.
    pub row_groups_selected: u64,
    /// `ScanPlan::row_groups_total`.
    pub row_groups_total: u64,
    /// Entries whose `ScanEntry::bytes` disagrees with the footers.
    pub entry_bytes_mismatches: u64,
}

/// One strategy on one run.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StrategyResult {
    /// Strategy name.
    pub strategy: String,
    /// Files, row groups, and bytes **recomputed** from the plan's entries
    /// and the footers; `null` for engines and for invalid plans.
    pub cost: Option<RunCost>,
    /// What the plan reported about itself.
    pub reported: Option<ReportedCost>,
    /// Whether `reported` disagrees with `cost`.
    pub cost_misreported: Option<bool>,
    /// Milliseconds to compute the plan (planning latency).
    pub plan_ms: Option<f64>,
    /// Bytes the strategy read from storage while planning (an engine whose
    /// pruning scans key columns); `0` for the baselines, which read nothing
    /// per lookup; `null` when not measured.
    pub plan_bytes_read: Option<u64>,
    /// Milliseconds to read the planned row groups and filter to the keys
    /// (execution latency, through the harness's byte-counting reader).
    pub scan_ms: Option<f64>,
    /// End-to-end milliseconds: `plan_ms + scan_ms` for plan strategies
    /// (`plan_ms` alone when scans are skipped), query time for engines.
    pub latency_ms: Option<f64>,
    /// Bytes the scan fetched from storage, counted in the reader parquet-rs
    /// drives, at column-chunk granularity (see `ReadBytes::fetched`).
    pub bytes_read: Option<u64>,
    /// Bytes handed to the page decoder (page headers and bodies).
    pub bytes_consumed: Option<u64>,
    /// `bytes_read - cost.bytes` (measured minus planned): zero when the
    /// plan is executed as written (docs/benchmark.md, "measured bytes").
    pub bytes_read_minus_planned: Option<i64>,
    /// Rows matched after filtering; must equal the oracle's.
    pub matched_rows: Option<u64>,
    /// Exact `amount` total over matched rows; must equal the oracle's.
    pub amount_total: Option<String>,
    /// Order-independent digest of the matched rows (every column); plan
    /// strategies must agree with `raw_scan`.
    pub rows_digest: Option<String>,
    /// Oracle row groups missing from the plan. Must be zero (invariant 1).
    pub false_negative_row_groups: Option<u64>,
    /// Peak bytes one lookup held, when the strategy measures it (the index
    /// will; ADR 018). `null` for the baselines.
    pub peak_memory_bytes: Option<u64>,
    /// Error text if the strategy failed on this run.
    pub error: Option<String>,
}

impl StrategyResult {
    /// An empty result for `strategy`, to be filled in.
    #[must_use]
    pub fn new(strategy: &str) -> Self {
        Self {
            strategy: strategy.to_string(),
            cost: None,
            reported: None,
            cost_misreported: None,
            plan_ms: None,
            plan_bytes_read: None,
            scan_ms: None,
            latency_ms: None,
            bytes_read: None,
            bytes_consumed: None,
            bytes_read_minus_planned: None,
            matched_rows: None,
            amount_total: None,
            rows_digest: None,
            false_negative_row_groups: None,
            peak_memory_bytes: None,
            error: None,
        }
    }
}

/// Aggregates for one strategy on one corpus (display only).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StrategySummary {
    /// Strategy name.
    pub strategy: String,
    /// Runs with a result and no error.
    pub runs: u64,
    /// Nearest-rank `[p50, p95, max]` of `latency_ms`.
    pub latency_ms: [f64; 3],
    /// Nearest-rank `[p50, p95, max]` of `plan_ms`, when plans exist.
    pub plan_ms: Option<[f64; 3]>,
    /// Sum of recomputed planned bytes over runs, when plans exist.
    pub bytes_planned_total: Option<u64>,
    /// Sum of measured bytes read over runs, when scans ran.
    pub bytes_read_total: Option<u64>,
    /// Sum of recomputed planned row groups over runs, when plans exist.
    pub row_groups_total: Option<u64>,
}
