//! Anchor runs and preserve-and-extend admission (ADR 021, `docs/evolve.md` §3, §6).
//!
//! For each template across the recorded workloads, identifies the nearest-rank
//! `p95` and `max` key-set runs (and the `p95` and `max` baseline-bytes runs)
//! as the **anchor set**.
//!
//! A candidate configuration `D` is admitted (`admit`) if and only if:
//! 1. It does not exceed `max_index_bytes` (when a cap is set).
//! 2. Its total bytes read across all runs is strictly below baseline `a_0`
//!    (`proposed_total_bytes < baseline_total_bytes`).
//! 3. No anchor run regresses by more than `δ` (`delta`, default `0.05`):
//!    for every anchor run `r`, `(c(D, r) - c(a_0, r)) / max(c(a_0, r), 1) <= δ`.

use std::collections::BTreeMap;

use bellwether_replay::{nearest_rank, Workload};
use bellwether_types::Hash;
use serde::{Deserialize, Serialize};

/// Default per-run regression allowance `δ` on anchor runs (ADR 021).
pub const DEFAULT_DELTA: f64 = 0.05;

/// Why a candidate configuration was discarded before or during admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscardReason {
    /// Total bytes read across all runs is not strictly below baseline (`>= a_0`).
    NoGain,
    /// An anchor run regressed by more than `δ` (`delta`).
    AnchorRegression,
    /// Contains a key spec that is subsumed or redundant within the configuration.
    Subsumed,
    /// Estimated or realized index segment bytes exceed `max_index_bytes`.
    OverIndexByteCap,
}

impl DiscardReason {
    /// Stable snake_case identifier.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoGain => "no_gain",
            Self::AnchorRegression => "anchor_regression",
            Self::Subsumed => "subsumed",
            Self::OverIndexByteCap => "over_index_byte_cap",
        }
    }
}

/// One anchor run identified from the recorded workloads (ADR 021).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnchorRun {
    /// Workload hash this run belongs to.
    pub workload: Hash,
    /// Run identifier within the workload.
    pub run_id: String,
    /// Template group of the run.
    pub template: String,
    /// Distinct keys in the run's key set.
    pub keys: u64,
    /// Baseline (`a_0`) bytes read on this run.
    pub baseline_bytes: u64,
    /// Role in the anchor set (`"p95_keys"`, `"max_keys"`, `"p95_bytes"`, `"max_bytes"`).
    pub role: String,
}

/// Regression details for one anchor run under a candidate configuration.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AnchorRegression {
    /// Workload hash.
    pub workload: Hash,
    /// Run identifier.
    pub run_id: String,
    /// Baseline bytes read.
    pub baseline_bytes: u64,
    /// Candidate bytes read.
    pub proposed_bytes: u64,
    /// Relative regression `(proposed_bytes - baseline_bytes) / max(baseline_bytes, 1)`.
    pub regression_fraction: f64,
    /// Whether this regression exceeded `δ` (`delta`).
    pub exceeded_delta: bool,
}

/// Summary of anchor evaluation for one candidate configuration.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AnchorCheck {
    /// Allowance `δ` used.
    pub delta: f64,
    /// Number of anchor runs checked.
    pub anchors_checked: usize,
    /// Maximum relative regression observed across all anchor runs (`0.0` when none regressed).
    pub max_regression_fraction: f64,
    /// Anchor runs where `proposed_bytes > baseline_bytes`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub regressions: Vec<AnchorRegression>,
    /// Whether all anchor runs stayed within `δ`.
    pub passed: bool,
}

/// Outcome of preserve-and-extend admission (`admit`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AdmissionDecision {
    /// Whether the candidate enters the admitted population.
    pub admitted: bool,
    /// Reason for discard when `admitted == false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<DiscardReason>,
    /// Anchor evaluation details.
    pub anchor_check: AnchorCheck,
}

/// Selects the p95 and max key-set runs (and p95/max baseline-bytes runs when keys tie)
/// per template across `workloads` (`(workload, baseline_run_bytes)`).
#[must_use]
pub fn select_anchors(workloads: &[(&Workload, &[u64])]) -> Vec<AnchorRun> {
    // Group runs by template: (workload_hash, run_index, run_id, keys, baseline_bytes)
    let mut by_template: BTreeMap<String, Vec<(Hash, String, u64, u64)>> = BTreeMap::new();
    for (w, base_bytes) in workloads {
        for (i, run) in w.runs().iter().enumerate() {
            let b = base_bytes.get(i).copied().unwrap_or(0);
            by_template.entry(run.template.clone()).or_default().push((
                w.hash(),
                run.run_id.clone(),
                run.keys,
                b,
            ));
        }
    }

    let mut anchors: Vec<AnchorRun> = Vec::new();
    for (template, mut runs) in by_template {
        if runs.is_empty() {
            continue;
        }
        let mut keys_sorted: Vec<u64> = runs.iter().map(|r| r.2).collect();
        keys_sorted.sort_unstable();
        let p95_k = nearest_rank(&keys_sorted, 0.95);
        let max_k = nearest_rank(&keys_sorted, 1.0);

        // Sort runs by (keys, baseline_bytes, run_id) to pick deterministic representatives
        runs.sort_by(|a, b| a.2.cmp(&b.2).then(a.3.cmp(&b.3)).then(a.1.cmp(&b.1)));

        // Pick the run at or closest above p95_k
        if let Some(r_p95) = runs.iter().find(|r| r.2 >= p95_k).or_else(|| runs.last()) {
            anchors.push(AnchorRun {
                workload: r_p95.0,
                run_id: r_p95.1.clone(),
                template: template.clone(),
                keys: r_p95.2,
                baseline_bytes: r_p95.3,
                role: if p95_k == max_k && runs.len() == 1 {
                    "p95_max_keys".into()
                } else {
                    "p95_keys".into()
                },
            });
        }

        // Pick the max-keys run (if distinct from the p95 representative)
        if let Some(r_max) = runs.last() {
            let already = anchors
                .iter()
                .any(|a| a.workload == r_max.0 && a.run_id == r_max.1);
            if !already {
                anchors.push(AnchorRun {
                    workload: r_max.0,
                    run_id: r_max.1.clone(),
                    template: template.clone(),
                    keys: r_max.2,
                    baseline_bytes: r_max.3,
                    role: "max_keys".into(),
                });
            }
        }

        // Also ensure the run with max baseline bytes in this template is anchored
        if let Some(r_max_bytes) = runs.iter().max_by(|a, b| a.3.cmp(&b.3).then(a.2.cmp(&b.2))) {
            let already = anchors
                .iter()
                .any(|a| a.workload == r_max_bytes.0 && a.run_id == r_max_bytes.1);
            if !already {
                anchors.push(AnchorRun {
                    workload: r_max_bytes.0,
                    run_id: r_max_bytes.1.clone(),
                    template,
                    keys: r_max_bytes.2,
                    baseline_bytes: r_max_bytes.3,
                    role: "max_bytes".into(),
                });
            }
        }
    }

    anchors
}

/// Preserve-and-extend admission check (ADR 021).
///
/// - Checks index byte cap (`max_index_bytes`).
/// - Checks net gain (`proposed_total_bytes < baseline_total_bytes`).
/// - Checks every anchor run for regression `> delta`, plus summed per-run
///   regressions across all runs against `delta * baseline_total_bytes`.
#[must_use]
#[allow(clippy::cast_precision_loss, clippy::too_many_arguments)]
pub fn admit(
    baseline_total_bytes: u64,
    proposed_total_bytes: u64,
    anchors: &[AnchorRun],
    proposed_run_bytes: &BTreeMap<(Hash, String), u64>,
    baseline_run_bytes: &BTreeMap<(Hash, String), u64>,
    index_bytes: u64,
    max_index_bytes: Option<u64>,
    delta: f64,
) -> AdmissionDecision {
    let mut regressions = Vec::new();
    let mut max_reg = 0.0_f64;
    let mut any_exceeded = false;

    for a in anchors {
        let prop = proposed_run_bytes
            .get(&(a.workload, a.run_id.clone()))
            .copied()
            .unwrap_or(a.baseline_bytes);
        if prop > a.baseline_bytes {
            let diff = prop - a.baseline_bytes;
            let denom = a.baseline_bytes.max(1) as f64;
            let frac = diff as f64 / denom;
            if frac > max_reg {
                max_reg = frac;
            }
            let exceeded = frac > delta + 1e-12;
            if exceeded {
                any_exceeded = true;
            }
            regressions.push(AnchorRegression {
                workload: a.workload,
                run_id: a.run_id.clone(),
                baseline_bytes: a.baseline_bytes,
                proposed_bytes: prop,
                regression_fraction: frac,
                exceeded_delta: exceeded,
            });
        }
    }

    // Also check non-anchor runs: if any run regresses beyond delta, or summed per-run
    // regressions exceed delta * baseline_total_bytes, flag it on the anchor check!
    let mut summed_regression_bytes = 0u64;
    for (k, &base_b) in baseline_run_bytes {
        let prop_b = proposed_run_bytes.get(k).copied().unwrap_or(base_b);
        if prop_b > base_b {
            let diff = prop_b - base_b;
            summed_regression_bytes = summed_regression_bytes.saturating_add(diff);
            let frac = diff as f64 / (base_b.max(1) as f64);
            if frac > max_reg {
                max_reg = frac;
            }
            if frac > delta + 1e-12 {
                any_exceeded = true;
                if !regressions
                    .iter()
                    .any(|r| r.workload == k.0 && r.run_id == k.1)
                {
                    regressions.push(AnchorRegression {
                        workload: k.0,
                        run_id: k.1.clone(),
                        baseline_bytes: base_b,
                        proposed_bytes: prop_b,
                        regression_fraction: frac,
                        exceeded_delta: true,
                    });
                }
            }
        }
    }
    if baseline_total_bytes > 0 {
        let total_reg_frac = summed_regression_bytes as f64 / baseline_total_bytes as f64;
        if total_reg_frac > delta + 1e-12 {
            any_exceeded = true;
        }
    }

    let anchor_check = AnchorCheck {
        delta,
        anchors_checked: anchors.len(),
        max_regression_fraction: max_reg,
        regressions,
        passed: !any_exceeded,
    };

    if let Some(cap) = max_index_bytes {
        if index_bytes > cap {
            return AdmissionDecision {
                admitted: false,
                reason: Some(DiscardReason::OverIndexByteCap),
                anchor_check,
            };
        }
    }

    if proposed_total_bytes >= baseline_total_bytes {
        return AdmissionDecision {
            admitted: false,
            reason: Some(DiscardReason::NoGain),
            anchor_check,
        };
    }

    if any_exceeded {
        return AdmissionDecision {
            admitted: false,
            reason: Some(DiscardReason::AnchorRegression),
            anchor_check,
        };
    }

    AdmissionDecision {
        admitted: true,
        reason: None,
        anchor_check,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admission_rejects_anchor_regression_above_delta() {
        let w = Hash::of(b"w1");
        let anchors = vec![AnchorRun {
            workload: w,
            run_id: "r_p95".into(),
            template: "t1".into(),
            keys: 100,
            baseline_bytes: 1_000,
            role: "p95_keys".into(),
        }];
        let mut base_map = BTreeMap::new();
        base_map.insert((w, "r_median".into()), 9_000);
        base_map.insert((w, "r_p95".into()), 1_000);

        let mut prop_map = BTreeMap::new();
        // Wins big on median (9000 -> 1000), but regresses by 20% on anchor r_p95 (1000 -> 1200)
        prop_map.insert((w, "r_median".into()), 1_000);
        prop_map.insert((w, "r_p95".into()), 1_200);

        let dec = admit(
            10_000, 2_200, &anchors, &prop_map, &base_map, 512, None, 0.05,
        );
        assert!(!dec.admitted);
        assert_eq!(dec.reason, Some(DiscardReason::AnchorRegression));
        assert!(!dec.anchor_check.passed);
        assert_eq!(dec.anchor_check.regressions.len(), 1);
        assert!(dec.anchor_check.regressions[0].exceeded_delta);

        // With delta = 0.25, it is admitted
        let dec_allow = admit(
            10_000, 2_200, &anchors, &prop_map, &base_map, 512, None, 0.25,
        );
        assert!(dec_allow.admitted);
        assert_eq!(dec_allow.reason, None);
    }

    #[test]
    fn admission_rejects_over_index_byte_cap_and_no_gain() {
        let w = Hash::of(b"w1");
        let base_map = BTreeMap::from([((w, "r1".into()), 1_000)]);
        let prop_map = BTreeMap::from([((w, "r1".into()), 500)]);

        let over_cap = admit(
            1_000,
            500,
            &[],
            &prop_map,
            &base_map,
            2_048,
            Some(1_024),
            0.05,
        );
        assert!(!over_cap.admitted);
        assert_eq!(over_cap.reason, Some(DiscardReason::OverIndexByteCap));

        let no_gain = admit(
            1_000,
            1_000,
            &[],
            &base_map,
            &base_map,
            512,
            Some(1_024),
            0.05,
        );
        assert!(!no_gain.admitted);
        assert_eq!(no_gain.reason, Some(DiscardReason::NoGain));
    }
}
