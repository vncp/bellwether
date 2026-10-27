//! Multi-objective Pareto frontier, crowding distance, and knee-point selection
//! (`docs/evolve.md` §6).
//!
//! Each candidate configuration `D` is evaluated on the 5-objective vector:
//! `(total_bytes_read, p95_run_bytes_read, max_run_bytes_read, spec_count, estimated_index_bytes)`
//! plus template-grouped `[p50, p95, max]` run bytes so a template with many runs
//! cannot drown a template with few runs (`docs/evolve.md` §6).
//!
//! - **Dominance**: `A` dominates `B` iff `A` is `<=` `B` on every objective
//!   component and `<` on at least one.
//! - **Baseline (`a_0`)**: always included on the frontier as the reference
//!   configuration; admitted candidates sit alongside it on the frontier.
//! - **Crowding distance**: NSGA-II crowding distance across the 5 objectives
//!   ranks frontier members by diversity/spread.
//! - **Knee point (`recommended`)**: among admitted non-baseline frontier
//!   members, selects the configuration minimizing weighted normalized distance
//!   to the utopia point (prioritizing read byte reduction at total and p95
//!   while penalizing excessive spec count and index bytes), breaking ties by
//!   fewer total bytes read, fewer specs, and higher crowding distance.

use bellwether_types::{Hash, KeySpec};
use serde::{Deserialize, Serialize};

use crate::anchors::{AnchorCheck, DiscardReason};
use crate::maintenance::MaintenanceCost;

/// The 5-objective vector evaluated for every candidate configuration `D`
/// (`docs/evolve.md` §6).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Objectives {
    /// Total compressed bytes read across all runs in all workloads.
    pub total_bytes_read: u64,
    /// Nearest-rank p95 bytes read per run (or max across templates of template p95 bytes).
    pub p95_run_bytes_read: u64,
    /// Maximum bytes read by any single run.
    pub max_run_bytes_read: u64,
    /// Number of [`KeySpec`]s in the configuration `D`.
    pub spec_count: usize,
    /// Estimated/realized index segment bytes (`m(D)`).
    pub estimated_index_bytes: u64,
}

impl Objectives {
    /// Returns the 5 objective values as `f64` for crowding-distance and knee calculations.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn as_array(&self) -> [f64; 5] {
        [
            self.total_bytes_read as f64,
            self.p95_run_bytes_read as f64,
            self.max_run_bytes_read as f64,
            self.spec_count as f64,
            self.estimated_index_bytes as f64,
        ]
    }

    /// Whether `self` Pareto-dominates `other` across all 5 objectives.
    #[must_use]
    pub fn dominates(&self, other: &Self) -> bool {
        let le_all = self.total_bytes_read <= other.total_bytes_read
            && self.p95_run_bytes_read <= other.p95_run_bytes_read
            && self.max_run_bytes_read <= other.max_run_bytes_read
            && self.spec_count <= other.spec_count
            && self.estimated_index_bytes <= other.estimated_index_bytes;
        let lt_one = self.total_bytes_read < other.total_bytes_read
            || self.p95_run_bytes_read < other.p95_run_bytes_read
            || self.max_run_bytes_read < other.max_run_bytes_read
            || self.spec_count < other.spec_count
            || self.estimated_index_bytes < other.estimated_index_bytes;
        le_all && lt_one
    }
}

/// Full evaluation of a candidate configuration `D`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CandidateEval {
    /// The key specs in `D`.
    pub specs: Vec<KeySpec>,
    /// Canonical short labels ([`KeySpec::identity`]) of `specs`.
    pub labels: Vec<String>,
    /// Whether this entry is the baseline configuration `a_0`.
    pub is_baseline: bool,
    /// Multi-objective cost vector.
    pub objectives: Objectives,
    /// Total files read across all runs.
    pub total_files_read: u64,
    /// Total row groups read across all runs.
    pub total_row_groups_read: u64,
    /// Maintenance cost `m(D)`.
    pub maintenance: MaintenanceCost,
    /// Anchor check outcome against `a_0`.
    pub anchor_check: AnchorCheck,
}

/// A member of the non-dominated Pareto frontier.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FrontierEntry {
    /// Rank on the frontier (`0` is baseline `a_0`, `1..` are admitted non-dominated configurations ordered by knee distance / crowding).
    pub rank: usize,
    /// Whether this entry is the baseline `a_0`.
    pub is_baseline: bool,
    /// Whether this entry is the recommended knee-point configuration.
    pub is_recommended: bool,
    /// The key specs in `D`.
    pub specs: Vec<KeySpec>,
    /// Canonical short labels ([`KeySpec::identity`]) of `specs`.
    pub labels: Vec<String>,
    /// The 5-objective vector.
    pub objectives: Objectives,
    /// Total files read across all runs.
    pub total_files_read: u64,
    /// Total row groups read across all runs.
    pub total_row_groups_read: u64,
    /// Maintenance cost `m(D)`.
    pub maintenance: MaintenanceCost,
    /// Anchor check outcome.
    pub anchor_check: AnchorCheck,
    /// NSGA-II crowding distance on the frontier (`f64::INFINITY` becomes `1e9` in JSON so it is valid JSON).
    pub crowding_distance: f64,
    /// Engine `Proposal` hash written to the ledger for this frontier member (`None` for baseline `a_0` when empty).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposal: Option<Hash>,
    /// Engine `Replay` hashes (one per workload) backing this frontier member's exact costs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replays: Vec<Hash>,
}

/// A candidate configuration that was discarded during search or admission.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DiscardedCandidate {
    /// The key specs in the discarded configuration.
    pub specs: Vec<KeySpec>,
    /// Canonical short labels ([`KeySpec::identity`]).
    pub labels: Vec<String>,
    /// Why the candidate was discarded.
    pub reason: DiscardReason,
    /// Human-readable detail (e.g. which spec subsumed it, or which anchor regressed).
    pub detail: String,
    /// Objectives if the candidate was replayed before discard.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub objectives: Option<Objectives>,
    /// Anchor check if evaluated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor_check: Option<AnchorCheck>,
}

/// Finite sentinel used in place of `f64::INFINITY` so `serde_json` always serializes a number.
pub const BOUNDARY_CROWDING: f64 = 1_000_000_000.0;

/// Computes the non-dominated Pareto frontier from `baseline` (`a_0`) and `admitted`
/// candidates, assigns NSGA-II crowding distances, and selects the knee-point
/// recommendation.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn compute_frontier(
    baseline: CandidateEval,
    admitted: &[CandidateEval],
) -> (Vec<FrontierEntry>, Option<usize>) {
    // 1. Filter `admitted` to non-dominated candidates among `admitted`.
    // Note: baseline `a_0` has `spec_count == current.len()` (often 0) and
    // `added_index_bytes == 0`, so `a_0` is naturally non-dominated on index_bytes,
    // and we always include `a_0` on the frontier (`docs/evolve.md` §3, §6).
    let mut non_dominated: Vec<CandidateEval> = Vec::new();
    for (i, cand) in admitted.iter().enumerate() {
        let dominated = admitted.iter().enumerate().any(|(j, other)| {
            if i == j {
                return false;
            }
            if other.objectives.dominates(&cand.objectives) {
                return true;
            }
            // Dedup identical objective vectors in favor of earlier candidate
            other.objectives == cand.objectives && j < i
        });
        if !dominated {
            non_dominated.push(cand.clone());
        }
    }

    // Combine baseline + non_dominated
    let mut all: Vec<CandidateEval> = Vec::with_capacity(1 + non_dominated.len());
    all.push(baseline);
    all.extend(non_dominated);

    // 2. Compute crowding distance across all frontier members
    let crowding = crowding_distances(&all);

    let mut entries: Vec<FrontierEntry> = all
        .into_iter()
        .zip(crowding)
        .map(|(c, cd)| FrontierEntry {
            rank: 0,
            is_baseline: c.is_baseline,
            is_recommended: false,
            specs: c.specs,
            labels: c.labels,
            objectives: c.objectives,
            total_files_read: c.total_files_read,
            total_row_groups_read: c.total_row_groups_read,
            maintenance: c.maintenance,
            anchor_check: c.anchor_check,
            crowding_distance: cd,
            proposal: None,
            replays: Vec::new(),
        })
        .collect();

    // Sort non-baseline entries by knee score (best trade-off first), then total_bytes_read,
    // then spec_count, then descending crowding_distance.
    if entries.len() > 1 {
        let base_bytes = entries[0].objectives.total_bytes_read;
        let tail = &mut entries[1..];
        let knee_scores = knee_distances(tail, base_bytes);
        let mut indexed: Vec<(usize, f64, FrontierEntry)> = tail
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, e)| (i, knee_scores[i], e))
            .collect();
        indexed.sort_by(|a, b| {
            a.1.total_cmp(&b.1)
                .then_with(|| {
                    a.2.objectives
                        .total_bytes_read
                        .cmp(&b.2.objectives.total_bytes_read)
                })
                .then_with(|| a.2.objectives.spec_count.cmp(&b.2.objectives.spec_count))
                .then_with(|| b.2.crowding_distance.total_cmp(&a.2.crowding_distance))
        });
        for (slot, (_, _, entry)) in tail.iter_mut().zip(indexed) {
            *slot = entry;
        }
    }

    for (idx, e) in entries.iter_mut().enumerate() {
        e.rank = idx;
    }

    let recommended_idx = select_knee(&entries);
    if let Some(idx) = recommended_idx {
        entries[idx].is_recommended = true;
    }

    (entries, recommended_idx)
}

/// Selects the recommended knee-point index in `entries` (`None` if only baseline exists).
#[must_use]
pub fn select_knee(entries: &[FrontierEntry]) -> Option<usize> {
    entries
        .iter()
        .enumerate()
        .find(|(_, e)| !e.is_baseline)
        .map(|(i, _)| i)
}

/// Computes NSGA-II crowding distance for each candidate on the frontier.
#[must_use]
#[allow(clippy::cast_precision_loss, clippy::needless_range_loop)]
pub fn crowding_distances(frontier: &[CandidateEval]) -> Vec<f64> {
    let n = frontier.len();
    if n == 0 {
        return Vec::new();
    }
    if n <= 2 {
        return vec![BOUNDARY_CROWDING; n];
    }

    let mut dist = vec![0.0_f64; n];
    let arrays: Vec<[f64; 5]> = frontier.iter().map(|c| c.objectives.as_array()).collect();

    for m in 0..5 {
        let mut order: Vec<usize> = (0..n).collect();
        order.sort_by(|&a, &b| arrays[a][m].total_cmp(&arrays[b][m]));
        let min_v = arrays[order[0]][m];
        let max_v = arrays[order[n - 1]][m];
        let span = max_v - min_v;
        if span <= 1e-12 {
            continue;
        }
        dist[order[0]] = BOUNDARY_CROWDING;
        dist[order[n - 1]] = BOUNDARY_CROWDING;
        for k in 1..(n - 1) {
            if dist[order[k]] < BOUNDARY_CROWDING {
                let gap = (arrays[order[k + 1]][m] - arrays[order[k - 1]][m]) / span;
                dist[order[k]] += gap;
            }
        }
    }

    dist
}

/// Computes weighted normalized utopia distance for each admitted frontier entry.
///
/// Prioritizes `total_bytes_read` (weight 1.0) and `p95_run_bytes_read` (weight 0.6)
/// and `max_run_bytes_read` (weight 0.4), with regularizing penalties on `spec_count`
/// (weight 0.1) and `estimated_index_bytes` (weight 0.15) so a massive index that
/// saves 1 byte never beats a compact index that saves 99.9% of bytes.
#[allow(clippy::cast_precision_loss)]
fn knee_distances(admitted: &[FrontierEntry], baseline_bytes: u64) -> Vec<f64> {
    if admitted.is_empty() {
        return Vec::new();
    }
    let min_tot = admitted
        .iter()
        .map(|e| e.objectives.total_bytes_read)
        .min()
        .unwrap_or(0) as f64;
    let max_tot = baseline_bytes.max(
        admitted
            .iter()
            .map(|e| e.objectives.total_bytes_read)
            .max()
            .unwrap_or(1),
    ) as f64;
    let span_tot = (max_tot - min_tot).max(1.0);

    let min_p95 = admitted
        .iter()
        .map(|e| e.objectives.p95_run_bytes_read)
        .min()
        .unwrap_or(0) as f64;
    let max_p95 = admitted
        .iter()
        .map(|e| e.objectives.p95_run_bytes_read)
        .max()
        .unwrap_or(1) as f64;
    let span_p95 = (max_p95 - min_p95).max(1.0);

    let min_max = admitted
        .iter()
        .map(|e| e.objectives.max_run_bytes_read)
        .min()
        .unwrap_or(0) as f64;
    let max_max = admitted
        .iter()
        .map(|e| e.objectives.max_run_bytes_read)
        .max()
        .unwrap_or(1) as f64;
    let span_max = (max_max - min_max).max(1.0);

    let min_specs = admitted
        .iter()
        .map(|e| e.objectives.spec_count)
        .min()
        .unwrap_or(1) as f64;
    let max_specs = admitted
        .iter()
        .map(|e| e.objectives.spec_count)
        .max()
        .unwrap_or(1) as f64;
    let span_specs = (max_specs - min_specs).max(1.0);

    let min_idx = admitted
        .iter()
        .map(|e| e.objectives.estimated_index_bytes)
        .min()
        .unwrap_or(0) as f64;
    let max_idx = admitted
        .iter()
        .map(|e| e.objectives.estimated_index_bytes)
        .max()
        .unwrap_or(1) as f64;
    let span_idx = (max_idx - min_idx).max(1.0);

    admitted
        .iter()
        .map(|e| {
            let o = &e.objectives;
            let n_tot = (o.total_bytes_read as f64 - min_tot) / span_tot;
            let n_p95 = (o.p95_run_bytes_read as f64 - min_p95) / span_p95;
            let n_max = (o.max_run_bytes_read as f64 - min_max) / span_max;
            let n_specs = (o.spec_count as f64 - min_specs) / span_specs;
            let n_idx = (o.estimated_index_bytes as f64 - min_idx) / span_idx;
            (1.0 * n_tot * n_tot
                + 0.6 * n_p95 * n_p95
                + 0.4 * n_max * n_max
                + 0.1 * n_specs * n_specs
                + 0.15 * n_idx * n_idx)
                .sqrt()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::anchors::AnchorCheck;
    use crate::maintenance::MaintenanceCost;

    fn dummy_eval(
        label: &str,
        is_baseline: bool,
        tot: u64,
        p95: u64,
        max: u64,
        specs: usize,
        idx_bytes: u64,
    ) -> CandidateEval {
        CandidateEval {
            specs: vec![KeySpec::Exact {
                columns: vec![label.into()],
                counts: false,
                bounds: None,
            }],
            labels: vec![format!("exact:{label}")],
            is_baseline,
            objectives: Objectives {
                total_bytes_read: tot,
                p95_run_bytes_read: p95,
                max_run_bytes_read: max,
                spec_count: specs,
                estimated_index_bytes: idx_bytes,
            },
            total_files_read: 1,
            total_row_groups_read: 1,
            maintenance: MaintenanceCost {
                index_bytes: idx_bytes,
                added_index_bytes: idx_bytes,
                estimated_index_bytes: idx_bytes,
                spec_count: specs,
                build_row_groups: 1,
                build_rows: 100,
                build_columns: vec![label.into()],
                specs: Vec::new(),
            },
            anchor_check: AnchorCheck {
                delta: 0.05,
                anchors_checked: 1,
                max_regression_fraction: 0.0,
                regressions: Vec::new(),
                passed: true,
            },
        }
    }

    #[test]
    fn pareto_eliminates_dominated_and_keeps_spread() {
        let base = dummy_eval("base", true, 100_000, 50_000, 50_000, 0, 0);
        // c1: low bytes, higher index size
        let c1 = dummy_eval("a", false, 10_000, 5_000, 5_000, 1, 2_000);
        // c2: dominated by c1 (higher bytes AND higher index size)
        let c2 = dummy_eval("b", false, 20_000, 10_000, 10_000, 1, 3_000);
        // c3: higher bytes than c1, but smaller index size (non-dominated!)
        let c3 = dummy_eval("c", false, 15_000, 7_500, 7_500, 1, 1_000);
        let c4 = dummy_eval("d", false, 25_000, 12_500, 12_500, 1, 500);

        let (frontier, rec) = compute_frontier(base, &[c1, c2, c3, c4]);
        assert_eq!(frontier.len(), 4, "base + c1 + c3 + c4 (c2 dominated)");
        assert!(rec.is_some());
        let labels: Vec<&str> = frontier.iter().map(|e| e.labels[0].as_str()).collect();
        assert!(!labels.contains(&"exact:b"));
        assert!(labels.contains(&"exact:a"));
        assert!(labels.contains(&"exact:c"));
        assert!(labels.contains(&"exact:d"));
        let interior = frontier
            .iter()
            .find(|e| e.labels == vec!["exact:c"])
            .unwrap();
        assert!(
            interior.crowding_distance > 0.0 && interior.crowding_distance < BOUNDARY_CROWDING,
            "interior point must have positive finite crowding distance, got {}",
            interior.crowding_distance
        );
    }
}
