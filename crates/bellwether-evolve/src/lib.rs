//! Workload-level physical design advisor for the Bellwether engine (WP13a, M6).
//!
//! `bellwether-evolve` implements reduced stage 1 (`advise`, ADR 032 amendment
//! 2026-10-27): deterministic rule-driven key enumeration (`docs/evolve.md`
//! §4.4), key subsumption and set-valued physical design (§5.2), exact numeric
//! knob descent over bloom false-positive rates and key-count gates (§4.3),
//! maintenance cost `m(D)` estimation and realization, anchor selection and
//! preserve-and-extend admission with per-run regression allowance `δ`
//! (ADR 021), and multi-objective Pareto frontier selection with crowding
//! distance and knee-point recommendation (§6).
//!
//! No language model is invoked (ADR 032). Every candidate configuration is
//! priced by exact replay (`bellwether-replay`, ADR 005) and every admitted
//! frontier configuration can be verified by reconciliation (`bellwether-verify`,
//! ADR 004) and applied with a receipt (`bellwether-ledger`, ADR 006).

#![forbid(unsafe_code)]
#![cfg_attr(test, allow(clippy::unwrap_used))]
// Proper names (Pareto, HyperLogLog, BLAKE3, Parquet) read better without backticks.
#![allow(clippy::doc_markdown)]

pub mod anchors;
pub mod enumerate;
pub mod frontier;
pub mod knobs;
pub mod maintenance;
pub mod search;
pub mod subsumption;

pub use anchors::{
    admit, select_anchors, AdmissionDecision, AnchorCheck, AnchorRegression, AnchorRun,
    DiscardReason, DEFAULT_DELTA,
};
pub use enumerate::{enumerate_candidates, CandidateOrigin, EnumeratedSpec};
pub use frontier::{
    compute_frontier, select_knee, CandidateEval, DiscardedCandidate, FrontierEntry, Objectives,
};
pub use knobs::{
    bloom_bytes_estimate, descend_bloom_fpr, evaluate_gates, BloomGridPoint, BloomSweep,
    GateEvaluation, GateTier, BLOOM_FPR_GRID, DEFAULT_BROADCAST_MAX_KEYS, DEFAULT_IN_MAX_KEYS,
};
pub use maintenance::{estimate_maintenance, MaintenanceCost, SpecMaintenance};
pub use search::{
    search_configurations, SearchConfig, SearchOutcome, SearchRunCost, WorkloadSlice,
    DEFAULT_MAX_KEYS,
};
pub use subsumption::{
    canonicalize_set, is_redundant_in_set, subsumes, SubsumptionKind, SubsumptionPair,
};
