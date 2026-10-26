//! Exact replay of recorded workloads for the Bellwether engine (WP6).
//!
//! Replay answers "what would these recorded runs have read" under the
//! current index, a hypothetical key spec, a baseline without an index, or
//! a proposal, from statistics alone: every cost is an exact sum of
//! row-group statistics under a stated plan, never an estimate (ADR 005).
//! Nothing but footers and key columns is read: the current index costs no
//! data reads at all, and a hypothetical spec reads only its key columns,
//! one row group at a time, to build transient segments.
//!
//! - [`workload`]: the recorder. Runs' key sets go into the cache under
//!   their BLAKE3, with a `workload.json` manifest; `bellwether-bench`'s
//!   workload format is read directly ([`record_bench_workload`]).
//! - [`catalog`]: every live row group of one manifest version, with sizes
//!   from the source's footers.
//! - [`plan`]: [`PlanSource`], the plan-building seam, and
//!   [`SegmentPlanner`], this crate's implementation over core segments.
//! - [`build_plan`]: the build crate's planner, the one the index answers
//!   queries with, behind the same seam; [`Replayer::for_snapshot`] plans
//!   through it.
//! - [`transient`]: a hypothetical spec's segments, built over key reads,
//!   and their cache.
//! - [`sketch`](mod@sketch): a transient `sketch` (approximate distinct
//!   counts) over item-column reads, equal to what a built sketch holds. No
//!   plan reads it.
//! - [`baseline`]: raw scan and Parquet-native statistics plus embedded
//!   bloom filters.
//! - [`layout`]: layout proposals under a stated model (`exact: false`).
//! - [`replay`]: [`Replayer`], the [`bellwether_types::Replay`] record, and
//!   its write through the ledger.
//! - [`report`](mod@report): per-run and per-template p50/p95/max.
//! - [`attribution`]: counterfactual hooks for WP13 (ADR 024).
//! - [`slo`]: the latency budget.
//!
//! Invariant owned here: 3, replay is deterministic
//! (`tests/determinism.rs`).
//!
//! Memory (ADR 018): replay holds one run's key set at a time, the
//! planner's block indexes and the blocks a lookup touches, and the cached
//! transient indexes, bounded by bytes. A transient build holds one row
//! group's projected key columns and one capped open segment writer.

#![forbid(unsafe_code)]
#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod attribution;
pub mod baseline;
pub mod build_plan;
pub mod catalog;
pub mod error;
pub mod join;
pub mod keys;
pub mod layout;
pub mod plan;
pub mod replay;
pub mod report;
pub mod sketch;
pub mod slo;
pub mod transient;
pub mod workload;

pub use attribution::{Alternative, CostDelta, RunDelta, RunInteraction};
pub use baseline::{Baseline, BaselinePlanner};
pub use catalog::{Catalog, CatalogRowGroup};
pub use error::{ReplayError, ReplayResult};
pub use join::{pair_cost, PairInput};
pub use keys::{RunKeys, RunKeysBuilder};
pub use layout::{replay_layout, LayoutModel, LayoutReplay, LAYOUT_MODEL_VERSION};
pub use plan::{
    asof_lookback_window, count_cost, membership_residual, planned_time_column, run_cost, servable,
    serves_query, serves_window, serving_temporal_column, text_residual, Lookup, PlanSource,
    RangeRead, SegmentBytes, SegmentPlanner, SpecSegments,
};
pub use replay::{proposed_specs, write_replay, Replayer, Scored, DEFAULT_CACHE_BYTES};
pub use report::{
    nearest_rank, p50_p95_max, report, CostQuantiles, Report, RunRow, TemplateSummary,
};
pub use sketch::TransientSketch;
pub use slo::REPLAY_RUN_P50_BUDGET;
pub use transient::{
    BuildStats, CacheStats, TransientCache, TransientIndex, TransientLimits, CLUSTER_KEY_OVERHEAD,
};
pub use workload::{
    record_bench_workload, BenchWorkloadSummary, KeyInput, KeySetSink, KeySetSource, MemoryKeySets,
    RecordedRun, RunQuery, RunWindow, Workload, WorkloadFile, WorkloadManifest, WorkloadRecorder,
    BENCH_WORKLOAD_FORMAT, QUERY_KIND_ASOF, QUERY_KIND_COUNT, QUERY_KIND_LATEST, QUERY_KIND_LOCATE,
    QUERY_KIND_WINDOW, WORKLOAD_FORMAT,
};
