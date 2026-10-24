//! Latency budget (`docs/evolve.md` section 12).
//!
//! One replay of one recorded run under one candidate key spec, with the
//! candidate's transient segments already built and cached, takes under
//! [`REPLAY_RUN_P50_BUDGET`] on the bench medium corpus, measured at the
//! workload's p50 run size. The budget is per typical run: a run of 100,000
//! keys does at least 100,000 lookups, and no budget of milliseconds covers
//! it, so the p95 and max runs are measured and reported, not asserted.
//!
//! `tests/latency.rs` asserts it in optimized builds and reports the
//! measurement in debug builds, where timings are not meaningful.

use std::time::Duration;

/// Budget for one replay of the p50 run under one cached candidate.
pub const REPLAY_RUN_P50_BUDGET: Duration = Duration::from_millis(5);
