//! Exact numeric knob descent (`docs/evolve.md` §4.3).
//!
//! Two classes of numeric knobs are tuned by exact replay:
//!
//! 1. **Bloom false-positive rate (`fpr`)**: integer/grid line search over
//!    [`BLOOM_FPR_GRID`] (`[0.0001, 0.001, 0.005, 0.01, 0.02, 0.05, 0.1]`),
//!    pricing each candidate `bloom:cols@fpr` (with optional `@ts` bounds) by
//!    exact replay across all runs in the workloads, alongside the analytical
//!    bloom segment size `rows * (-ln(fpr) / (ln 2)^2) / 8` and the realized
//!    transient segment bytes, keeping the Pareto-optimal `(bytes_read,
//!    segment_bytes)` FPR points.
//! 2. **Key-count gates (`broadcast` vs `in` vs `shuffle`)**: evaluates and
//!    recommends exact key-count gate thresholds from the replayed per-run
//!    distinct key counts and row fanout.

use bellwether_types::{Gate, Hash, KeySpec};
use serde::{Deserialize, Serialize};

/// Standard false-positive rate grid for bloom knob descent (`docs/evolve.md` §4.3).
pub const BLOOM_FPR_GRID: [f64; 7] = [0.0001, 0.001, 0.005, 0.01, 0.02, 0.05, 0.1];

/// Default upper key count for broadcasting a small key set in a join.
pub const DEFAULT_BROADCAST_MAX_KEYS: u64 = 10_000;

/// Default upper key count for pushing an `IN` list before falling back to a shuffle.
pub const DEFAULT_IN_MAX_KEYS: u64 = 100_000;

/// Analytical bloom filter size in bytes: `ceil(rows * (-ln(fpr) / (ln 2)^2) / 8)`.
#[must_use]
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
pub fn bloom_bytes_estimate(rows: u64, fpr: f64) -> u64 {
    if rows == 0 || !(0.0..1.0).contains(&fpr) {
        return 0;
    }
    let ln2_sq = std::f64::consts::LN_2 * std::f64::consts::LN_2;
    let bits_per_row = -fpr.ln() / ln2_sq;
    let total_bytes = (rows as f64 * bits_per_row / 8.0).ceil();
    if total_bytes.is_finite() && total_bytes >= 0.0 {
        total_bytes as u64
    } else {
        0
    }
}

/// One evaluated point on the bloom FPR descent grid.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BloomGridPoint {
    /// The false-positive rate `fpr`.
    pub fpr: f64,
    /// The evaluated [`KeySpec`] (`bloom:cols@fpr` or `bloom:cols@fpr@ts`).
    pub spec: KeySpec,
    /// Total bytes read across all workload runs under this bloom spec.
    pub total_bytes_read: u64,
    /// Nearest-rank p95 run bytes read.
    pub p95_run_bytes_read: u64,
    /// Maximum run bytes read.
    pub max_run_bytes_read: u64,
    /// Analytical bloom segment bytes (`rows * (-ln(fpr) / (ln 2)^2) / 8`).
    pub formula_index_bytes: u64,
    /// Realized transient segment bytes from building the SBBF blocks.
    pub segment_bytes: u64,
    /// Finite difference `Δbytes_read / Δsegment_bytes` from the next looser FPR, when defined.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub marginal_bytes_saved_per_index_byte: Option<f64>,
    /// Whether this point is Pareto-optimal in `(total_bytes_read, p95_run_bytes_read, formula_index_bytes)`.
    pub pareto_optimal: bool,
}

/// Result of coordinate/grid descent over bloom FPR for one column tuple.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BloomSweep {
    /// The bloom key columns.
    pub columns: Vec<String>,
    /// Optional per-key time bounds column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bounds: Option<String>,
    /// Evaluated grid points in ascending FPR order.
    pub points: Vec<BloomGridPoint>,
    /// The knee/recommended FPR on the bloom curve.
    pub chosen_fpr: f64,
}

/// Evaluates `points` (one per FPR in [`BLOOM_FPR_GRID`]) to mark Pareto-optimal
/// FPR settings in `(total_bytes_read, p95_run_bytes_read, effective_index_bytes)`
/// and pick the knee FPR.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn descend_bloom_fpr(
    columns: Vec<String>,
    bounds: Option<String>,
    mut points: Vec<BloomGridPoint>,
) -> BloomSweep {
    points.sort_by(|a, b| a.fpr.total_cmp(&b.fpr));

    // Compute finite differences from looser (i+1) to tighter (i) FPR
    for i in 0..points.len() {
        if i + 1 < points.len() {
            let tighter = &points[i];
            let looser = &points[i + 1];
            let saved = looser
                .total_bytes_read
                .saturating_sub(tighter.total_bytes_read);
            let cost_diff = tighter
                .formula_index_bytes
                .saturating_sub(looser.formula_index_bytes)
                .max(1);
            points[i].marginal_bytes_saved_per_index_byte = Some(saved as f64 / cost_diff as f64);
        }
    }

    // Mark Pareto-optimal points over (total_bytes_read, p95_run_bytes_read, formula_index_bytes)
    let n = points.len();
    for i in 0..n {
        let mut dominated = false;
        for j in 0..n {
            if i == j {
                continue;
            }
            let a = &points[j];
            let b = &points[i];
            let le_all = a.total_bytes_read <= b.total_bytes_read
                && a.p95_run_bytes_read <= b.p95_run_bytes_read
                && a.formula_index_bytes <= b.formula_index_bytes;
            let lt_one = a.total_bytes_read < b.total_bytes_read
                || a.p95_run_bytes_read < b.p95_run_bytes_read
                || a.formula_index_bytes < b.formula_index_bytes;
            if le_all && lt_one {
                dominated = true;
                break;
            }
            // Tie-break identical objectives in favor of higher FPR (smaller formula bytes)
            if a.total_bytes_read == b.total_bytes_read
                && a.p95_run_bytes_read == b.p95_run_bytes_read
                && a.formula_index_bytes == b.formula_index_bytes
                && j > i
            {
                dominated = true;
                break;
            }
        }
        points[i].pareto_optimal = !dominated;
    }

    // Select knee point among Pareto-optimal points (normalized distance to utopia)
    let pareto: Vec<&BloomGridPoint> = points.iter().filter(|p| p.pareto_optimal).collect();
    let chosen_fpr = if pareto.is_empty() {
        0.01
    } else {
        let min_read = pareto.iter().map(|p| p.total_bytes_read).min().unwrap_or(0);
        let max_read = pareto
            .iter()
            .map(|p| p.total_bytes_read)
            .max()
            .unwrap_or(min_read);
        let min_idx = pareto
            .iter()
            .map(|p| p.formula_index_bytes)
            .min()
            .unwrap_or(0);
        let max_idx = pareto
            .iter()
            .map(|p| p.formula_index_bytes)
            .max()
            .unwrap_or(min_idx);

        let read_span = (max_read.saturating_sub(min_read)).max(1) as f64;
        let idx_span = (max_idx.saturating_sub(min_idx)).max(1) as f64;

        pareto
            .into_iter()
            .min_by(|a, b| {
                let da = ((a.total_bytes_read - min_read) as f64 / read_span).powi(2)
                    + 0.1 * ((a.formula_index_bytes - min_idx) as f64 / idx_span).powi(2);
                let db = ((b.total_bytes_read - min_read) as f64 / read_span).powi(2)
                    + 0.1 * ((b.formula_index_bytes - min_idx) as f64 / idx_span).powi(2);
                da.total_cmp(&db)
            })
            .map_or(0.01, |p| p.fpr)
    };

    BloomSweep {
        columns,
        bounds,
        points,
        chosen_fpr,
    }
}

/// Execution strategy tier determined by key-count gates (`docs/evolve.md` §4.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateTier {
    /// Key count fits within the broadcast threshold.
    Broadcast,
    /// Key count exceeds broadcast but fits within the `IN`-list pushdown threshold.
    InList,
    /// Key count exceeds the `IN`-list threshold; fall back to partitioned shuffle / join.
    Shuffle,
}

/// Evaluation and threshold descent for key-count gates across recorded runs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateEvaluation {
    /// Gates supplied in the request (if any).
    pub supplied_gates: Vec<Gate>,
    /// Recommended `broadcast` key-count threshold derived from run key counts and fanout.
    pub recommended_broadcast_limit: u64,
    /// Recommended `in` list key-count threshold derived from run key counts and fanout.
    pub recommended_in_limit: u64,
    /// Nearest-rank `[p50, p95, max]` key counts across all runs.
    pub keys_p50_p95_max: [u64; 3],
    /// Runs classified into `broadcast`, `in_list`, and `shuffle` under the recommended/supplied gates.
    pub broadcast_runs: u64,
    /// Runs classified into `in_list`.
    pub in_list_runs: u64,
    /// Runs classified into `shuffle`.
    pub shuffle_runs: u64,
    /// Per-supplied-gate pass count (how many runs have `keys <= gate.limit`).
    pub gate_fits: Vec<(String, u64, u64, u64)>,
}

/// Computes exact key-count gate thresholds and classifies every run `(workload, run_id, keys, bytes_read)`.
#[must_use]
pub fn evaluate_gates(
    supplied: &[Gate],
    run_keys_and_bytes: &[(Hash, String, u64, u64)],
    total_dataset_bytes: u64,
) -> GateEvaluation {
    let mut key_counts: Vec<u64> = run_keys_and_bytes.iter().map(|(_, _, k, _)| *k).collect();
    key_counts.sort_unstable();
    let quantiles = bellwether_replay::p50_p95_max(&key_counts);

    // Exact threshold descent:
    // - A run is profitable for `broadcast` when keys <= DEFAULT_BROADCAST_MAX_KEYS
    //   and bytes_read <= 25% of dataset bytes; the highest such key count (or p50) sets
    //   the broadcast gate.
    // - A run is profitable for `in` pushdown when keys <= DEFAULT_IN_MAX_KEYS and
    //   bytes_read < total_dataset_bytes (strictly prunes row groups); the highest
    //   such key count (or p95) sets the `in` gate.
    let broadcast_limit = supplied
        .iter()
        .find(|g| g.name == "broadcast" || g.name == "join")
        .map_or_else(
            || {
                run_keys_and_bytes
                    .iter()
                    .filter(|(_, _, k, b)| {
                        *k <= DEFAULT_BROADCAST_MAX_KEYS
                            && (total_dataset_bytes == 0 || *b * 4 <= total_dataset_bytes * 3)
                    })
                    .map(|(_, _, k, _)| *k)
                    .max()
                    .unwrap_or(quantiles[0].clamp(1, DEFAULT_BROADCAST_MAX_KEYS))
            },
            |g| g.limit,
        );

    let in_limit = supplied.iter().find(|g| g.name == "in").map_or_else(
        || {
            run_keys_and_bytes
                .iter()
                .filter(|(_, _, k, b)| {
                    *k <= DEFAULT_IN_MAX_KEYS
                        && (total_dataset_bytes == 0 || *b < total_dataset_bytes)
                })
                .map(|(_, _, k, _)| *k)
                .max()
                .unwrap_or(quantiles[1].clamp(broadcast_limit, DEFAULT_IN_MAX_KEYS))
                .max(broadcast_limit)
        },
        |g| g.limit,
    );

    let mut broadcast_runs = 0u64;
    let mut in_list_runs = 0u64;
    let mut shuffle_runs = 0u64;
    for &(_, _, k, _) in run_keys_and_bytes {
        if k <= broadcast_limit {
            broadcast_runs += 1;
        } else if k <= in_limit {
            in_list_runs += 1;
        } else {
            shuffle_runs += 1;
        }
    }

    let total_runs = run_keys_and_bytes.len() as u64;
    let gate_fits = supplied
        .iter()
        .map(|g| {
            let fitting = run_keys_and_bytes
                .iter()
                .filter(|(_, _, k, _)| *k <= g.limit)
                .count() as u64;
            (g.name.clone(), g.limit, fitting, total_runs)
        })
        .collect();

    GateEvaluation {
        supplied_gates: supplied.to_vec(),
        recommended_broadcast_limit: broadcast_limit,
        recommended_in_limit: in_limit,
        keys_p50_p95_max: quantiles,
        broadcast_runs,
        in_list_runs,
        shuffle_runs,
        gate_fits,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bloom_bytes_monotone_in_fpr() {
        let rows = 100_000;
        let mut prev = u64::MAX;
        for fpr in BLOOM_FPR_GRID {
            let b = bloom_bytes_estimate(rows, fpr);
            assert!(b > 0);
            assert!(b < prev, "fpr {fpr} bytes {b} not < {prev}");
            prev = b;
        }
    }

    #[test]
    fn bloom_descent_marks_pareto_and_picks_knee() {
        let cols = vec!["id".to_string()];
        let points: Vec<BloomGridPoint> = BLOOM_FPR_GRID
            .iter()
            .map(|&fpr| {
                // Simulate exact replay: tighter FPR up to 0.01 reduces bytes read;
                // tighter than 0.01 gives no further reduction on this synthetic test.
                let bytes_read = if fpr <= 0.01 {
                    10_000
                } else if fpr <= 0.05 {
                    25_000
                } else {
                    50_000
                };
                let est = bloom_bytes_estimate(10_000, fpr);
                BloomGridPoint {
                    fpr,
                    spec: KeySpec::Bloom {
                        columns: cols.clone(),
                        fpr,
                        bounds: None,
                    },
                    total_bytes_read: bytes_read,
                    p95_run_bytes_read: bytes_read,
                    max_run_bytes_read: bytes_read,
                    formula_index_bytes: est,
                    segment_bytes: est,
                    marginal_bytes_saved_per_index_byte: None,
                    pareto_optimal: false,
                }
            })
            .collect();
        let sweep = descend_bloom_fpr(cols, None, points);
        // 0.0001, 0.001, 0.005 have same bytes_read as 0.01 but higher formula_index_bytes,
        // so they must be dominated by 0.01!
        for p in &sweep.points {
            if p.fpr < 0.01 {
                assert!(
                    !p.pareto_optimal,
                    "fpr {} should be dominated by 0.01",
                    p.fpr
                );
            }
        }
        let p01 = sweep
            .points
            .iter()
            .find(|p| (p.fpr - 0.01).abs() < 1e-9)
            .unwrap();
        assert!(p01.pareto_optimal);
        assert!((sweep.chosen_fpr - 0.01).abs() < 1e-9);
    }
}
