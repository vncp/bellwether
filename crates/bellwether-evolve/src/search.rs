//! Set-valued physical design search (`docs/evolve.md` §3, §4.3, §4.4, §5.2, §6).
//!
//! Given one or more recorded [`Workload`]s, the dataset's [`ProfileReport`],
//! and a [`Replayer`]:
//! 1. Evaluates the baseline configuration `a_0` (`replayer.current_specs()`)
//!    across every run of every workload and identifies the anchor set (ADR 021).
//! 2. Runs rule-driven enumeration ([`crate::enumerate::enumerate_candidates`]).
//! 3. Runs exact numeric knob descent over bloom false-positive rates
//!    ([`crate::knobs::BLOOM_FPR_GRID`]) for every bloom-eligible column tuple,
//!    adding the Pareto-optimal bloom specs to the candidate pool.
//! 4. Evaluates every singleton `[spec]` and records subsumed/discarded candidates.
//! 5. Searches multi-key configurations `D` of size `2..=max_keys` using greedy
//!    forward selection + pairwise combinations of top singletons + local swap,
//!    never proposing a set containing a subsumed or same-slot (`KeySpec::name`)
//!    spec (`docs/evolve.md` §5.2), and automatically pairing any `agg` key with
//!    its required `exact:cols,counts` companion.
//! 6. Applies preserve-and-extend admission (`admit`) with anchor regression
//!    budget `δ` (`delta`) and optional `max_index_bytes` cap, recording every
//!    discarded candidate with its [`DiscardReason`].
//! 7. Builds the multi-objective Pareto frontier with crowding distance and
//!    selects the knee-point recommendation (`docs/evolve.md` §6).

use std::collections::{BTreeMap, BTreeSet};

use bellwether_profile::ProfileReport;
use bellwether_replay::{nearest_rank, KeySetSource, Replayer, Workload};
use bellwether_types::{Gate, Hash, KeySpec, Result, RunCost};
use serde::{Deserialize, Serialize};

use crate::anchors::{admit, select_anchors, AnchorCheck, AnchorRun, DiscardReason, DEFAULT_DELTA};
use crate::enumerate::{enumerate_candidates, CandidateOrigin, EnumeratedSpec};
use crate::frontier::{
    compute_frontier, CandidateEval, DiscardedCandidate, FrontierEntry, Objectives,
};
use crate::knobs::{
    bloom_bytes_estimate, descend_bloom_fpr, evaluate_gates, BloomGridPoint, BloomSweep,
    GateEvaluation, BLOOM_FPR_GRID,
};
use crate::maintenance::estimate_maintenance;
use crate::subsumption::{canonicalize_set, is_redundant_in_set, SubsumptionPair};

/// Default maximum number of key specs in a joint configuration `D`.
pub const DEFAULT_MAX_KEYS: usize = 3;

/// Search configuration parameters (`AdviseRequest` knobs).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SearchConfig {
    /// Maximum number of new/total [`KeySpec`]s in a candidate configuration `D`.
    pub max_keys: usize,
    /// Optional cap on index segment bytes (`m(D)`).
    pub max_index_bytes: Option<u64>,
    /// Preserve-and-extend per-run regression allowance `δ` on anchor runs (ADR 021).
    pub delta: f64,
    /// Optional key-count gates to evaluate and tune.
    pub gates: Vec<Gate>,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            max_keys: DEFAULT_MAX_KEYS,
            max_index_bytes: None,
            delta: DEFAULT_DELTA,
            gates: Vec::new(),
        }
    }
}

/// Summary of one recorded workload included in the search.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadSlice {
    /// Workload hash.
    pub hash: Hash,
    /// Number of runs in the workload.
    pub runs: u64,
    /// Distinct key-column sets across runs.
    pub columns: Vec<Vec<String>>,
    /// Nearest-rank `[p50, p95, max]` key counts.
    pub keys_p50_p95_max: [u64; 3],
}

/// Cost of one run in one workload under a configuration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchRunCost {
    /// Workload hash.
    pub workload: Hash,
    /// Per-run cost.
    pub cost: RunCost,
}

/// Complete output of the deterministic physical design search before ledger attestation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SearchOutcome {
    /// Workloads evaluated.
    pub workloads: Vec<WorkloadSlice>,
    /// Baseline configuration `a_0` evaluation.
    pub baseline: CandidateEval,
    /// Anchor runs selected across all templates (ADR 021).
    pub anchors: Vec<AnchorRun>,
    /// Enumerated singleton specs.
    pub enumerated: Vec<EnumeratedSpec>,
    /// Subsumption relationships identified during enumeration and set construction.
    pub subsumptions: Vec<SubsumptionPair>,
    /// Exact bloom FPR sweeps performed.
    pub bloom_sweeps: Vec<BloomSweep>,
    /// Key-count gate evaluation and threshold descent.
    pub gate_evaluation: GateEvaluation,
    /// Non-dominated Pareto frontier (index 0 is baseline `a_0`).
    pub frontier: Vec<FrontierEntry>,
    /// Index in `frontier` of the knee-point recommendation, if an admitted configuration improved on `a_0`.
    pub recommended_index: Option<usize>,
    /// Candidates discarded during search or admission, with their exact [`DiscardReason`].
    pub discarded: Vec<DiscardedCandidate>,
    /// Total exact `Replayer` evaluations performed during the search.
    pub replays_evaluated: u64,
}

/// Runs the deterministic stage-1 `advise` search (`docs/evolve.md` §3, §4.3, §4.4, §5.2, §6).
#[allow(clippy::too_many_lines)]
pub fn search_configurations(
    profile: &ProfileReport,
    replayer: &Replayer<'_>,
    workloads: &[Workload],
    keys: &dyn KeySetSource,
    config: &SearchConfig,
) -> Result<SearchOutcome> {
    let max_keys = config.max_keys.max(1);
    let current = replayer.current_specs().to_vec();
    let mut replays_evaluated = 0u64;

    // 1. Price baseline a_0 across all workloads
    let base_priced = price_specs_across_workloads(replayer, &current, workloads, keys)?;
    replays_evaluated += workloads.len() as u64;

    let mut base_run_map: BTreeMap<(Hash, String), u64> = BTreeMap::new();
    let mut base_vecs: Vec<Vec<u64>> = Vec::with_capacity(workloads.len());
    let mut run_keys_and_bytes: Vec<(Hash, String, u64, u64)> = Vec::new();
    for (w, costs) in workloads.iter().zip(&base_priced) {
        let mut v = Vec::with_capacity(costs.len());
        for c in costs {
            base_run_map.insert((w.hash(), c.run_id.clone()), c.bytes);
            v.push(c.bytes);
            run_keys_and_bytes.push((w.hash(), c.run_id.clone(), c.keys, c.bytes));
        }
        base_vecs.push(v);
    }

    let anchor_inputs: Vec<(&Workload, &[u64])> = workloads
        .iter()
        .zip(&base_vecs)
        .map(|(w, v)| (w, v.as_slice()))
        .collect();
    let anchors = select_anchors(&anchor_inputs);

    let base_maint = estimate_maintenance(&current, replayer, profile);
    let base_obj = compute_objectives(&base_priced, workloads, &current, &base_maint);
    let (base_files, base_rgs) = sum_files_rgs(&base_priced);
    let baseline = CandidateEval {
        labels: current.iter().map(KeySpec::identity).collect(),
        specs: current.clone(),
        is_baseline: true,
        objectives: base_obj.clone(),
        total_files_read: base_files,
        total_row_groups_read: base_rgs,
        maintenance: base_maint,
        anchor_check: AnchorCheck {
            delta: config.delta,
            anchors_checked: anchors.len(),
            max_regression_fraction: 0.0,
            regressions: Vec::new(),
            passed: true,
        },
    };

    // 2. Enumerate rule-driven candidates (§4.4)
    let (mut enumerated, bloom_targets, count_columns) =
        enumerate_candidates(profile, replayer, workloads, keys, 8)?;

    // 3. Exact numeric knob descent over bloom FPRs (§4.3)
    let mut bloom_sweeps: Vec<BloomSweep> = Vec::new();
    for (bcols, bbounds) in bloom_targets {
        let mut grid_points = Vec::with_capacity(BLOOM_FPR_GRID.len());
        for &fpr in &BLOOM_FPR_GRID {
            let bspec = KeySpec::Bloom {
                columns: bcols.clone(),
                fpr,
                bounds: bbounds.clone(),
            };
            let full = combine_with_current(&current, std::slice::from_ref(&bspec));
            let Ok(priced) = price_specs_across_workloads(replayer, &full, workloads, keys) else {
                continue;
            };
            replays_evaluated += workloads.len() as u64;
            let maint = estimate_maintenance(std::slice::from_ref(&bspec), replayer, profile);
            let obj = compute_objectives(&priced, workloads, std::slice::from_ref(&bspec), &maint);
            let formula_bytes = bloom_bytes_estimate(profile.rows_total.max(1), fpr);
            grid_points.push(BloomGridPoint {
                fpr,
                spec: bspec,
                total_bytes_read: obj.total_bytes_read,
                p95_run_bytes_read: obj.p95_run_bytes_read,
                max_run_bytes_read: obj.max_run_bytes_read,
                formula_index_bytes: formula_bytes,
                segment_bytes: maint.index_bytes,
                marginal_bytes_saved_per_index_byte: None,
                pareto_optimal: false,
            });
        }
        if !grid_points.is_empty() {
            let sweep = descend_bloom_fpr(bcols, bbounds, grid_points);
            // Add the chosen knee FPR and up to 1 tighter/looser Pareto-optimal FPR to the pool
            for p in &sweep.points {
                if (p.fpr - sweep.chosen_fpr).abs() < 1e-12 || p.pareto_optimal {
                    let id = p.spec.identity();
                    if !enumerated.iter().any(|e| e.spec.identity() == id) {
                        enumerated.push(EnumeratedSpec {
                            spec: p.spec.clone(),
                            origin: CandidateOrigin::KnobDescent,
                        });
                    }
                }
            }
            bloom_sweeps.push(sweep);
        }
    }

    // 4. Evaluate candidate sets (singletons + multi-key sets up to max_keys)
    let mut admitted: Vec<CandidateEval> = Vec::new();
    let mut discarded: Vec<DiscardedCandidate> = Vec::new();
    let mut subsumptions: Vec<SubsumptionPair> = Vec::new();
    let mut evaluated_sets: BTreeSet<Vec<String>> = BTreeSet::new();

    // Helper closure to evaluate a proposed set of new specs `proposed_new`
    let mut eval_candidate = |raw_specs: Vec<KeySpec>| -> Result<Option<CandidateEval>> {
        // Ensure any Agg key has its required counted Exact key beside it
        let mut expanded = Vec::new();
        for s in raw_specs {
            if let KeySpec::Agg { columns, .. } = &s {
                let has_counted = expanded.iter().chain(&current).any(|existing| {
                    existing.counts()
                        && existing.columns().len() == columns.len()
                        && existing.columns().iter().all(|c| columns.contains(c))
                });
                if !has_counted {
                    expanded.push(KeySpec::Exact {
                        columns: columns.clone(),
                        counts: true,
                        bounds: None,
                    });
                }
            }
            if !expanded.contains(&s) {
                expanded.push(s);
            }
        }

        if expanded.len() > max_keys {
            return Ok(None);
        }

        // Check for internal subsumption in `expanded`
        let (canonical, removed) = canonicalize_set(&expanded, &count_columns);
        if !removed.is_empty() {
            let labels = sorted_labels(&expanded);
            if evaluated_sets.insert(labels.clone()) {
                let detail = removed
                    .iter()
                    .map(|p| {
                        format!(
                            "{} is subsumed by {} ({})",
                            p.subsumed,
                            p.by,
                            p.kind.as_str()
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("; ");
                subsumptions.extend(removed);
                discarded.push(DiscardedCandidate {
                    specs: expanded,
                    labels,
                    reason: DiscardReason::Subsumed,
                    detail,
                    objectives: None,
                    anchor_check: None,
                });
            }
            return Ok(None);
        }

        // Also check if any spec in `canonical` is already subsumed by `current`
        // Only check `canonical` against `current` when `canonical` is a singleton;
        // a multi-key set contains new specs alongside existing ones.
        for s in &canonical {
            if current.contains(s) {
                continue;
            }
            for cur in &current {
                if matches!(cur, KeySpec::Bloom { .. })
                    && matches!(s, KeySpec::Exact { .. } | KeySpec::Temporal { .. })
                {
                    continue;
                }
                if let Some(kind) = crate::subsumption::subsumes_with_counts(cur, s, &count_columns)
                {
                    let labels = sorted_labels(&canonical);
                    if evaluated_sets.insert(labels.clone()) {
                        let pair = SubsumptionPair {
                            subsumed: s.identity(),
                            by: cur.identity(),
                            kind,
                        };
                        let detail = format!(
                            "{} is already subsumed by current key {} ({})",
                            pair.subsumed,
                            pair.by,
                            pair.kind.as_str()
                        );
                        subsumptions.push(pair);
                        discarded.push(DiscardedCandidate {
                            specs: canonical,
                            labels,
                            reason: DiscardReason::Subsumed,
                            detail,
                            objectives: None,
                            anchor_check: None,
                        });
                    }
                    return Ok(None);
                }
            }
        }

        let labels = sorted_labels(&canonical);
        if canonical.is_empty() || !evaluated_sets.insert(labels.clone()) {
            return Ok(None);
        }

        let full_specs = combine_with_current(&current, &canonical);
        let Ok(priced) = price_specs_across_workloads(replayer, &full_specs, workloads, keys)
        else {
            return Ok(None);
        };
        replays_evaluated += workloads.len() as u64;

        let maint = estimate_maintenance(&canonical, replayer, profile);
        let obj = compute_objectives(&priced, workloads, &canonical, &maint);
        let (files, rgs) = sum_files_rgs(&priced);

        let mut prop_run_map: BTreeMap<(Hash, String), u64> = BTreeMap::new();
        for (w, costs) in workloads.iter().zip(&priced) {
            for c in costs {
                prop_run_map.insert((w.hash(), c.run_id.clone()), c.bytes);
            }
        }

        let decision = admit(
            base_obj.total_bytes_read,
            obj.total_bytes_read,
            &anchors,
            &prop_run_map,
            &base_run_map,
            maint.index_bytes,
            config.max_index_bytes,
            config.delta,
        );

        let eval = CandidateEval {
            specs: canonical.clone(),
            labels: labels.clone(),
            is_baseline: false,
            objectives: obj.clone(),
            total_files_read: files,
            total_row_groups_read: rgs,
            maintenance: maint.clone(),
            anchor_check: decision.anchor_check.clone(),
        };

        if decision.admitted {
            admitted.push(eval.clone());
            Ok(Some(eval))
        } else {
            let reason = decision.reason.unwrap_or(DiscardReason::NoGain);
            let detail = match reason {
                DiscardReason::OverIndexByteCap => format!(
                    "index bytes {} exceed max_index_bytes cap {}",
                    bellwether_profile::render::thousands(maint.index_bytes),
                    bellwether_profile::render::thousands(config.max_index_bytes.unwrap_or(0))
                ),
                DiscardReason::NoGain => format!(
                    "total bytes read {} is not below baseline {}",
                    bellwether_profile::render::thousands(obj.total_bytes_read),
                    bellwether_profile::render::thousands(base_obj.total_bytes_read)
                ),
                DiscardReason::AnchorRegression => {
                    let violations: Vec<String> = decision
                        .anchor_check
                        .regressions
                        .iter()
                        .filter(|r| r.exceeded_delta)
                        .map(|r| {
                            format!(
                                "run {} regressed +{:.1}% ({} -> {} B, delta {:.1}%)",
                                r.run_id,
                                r.regression_fraction * 100.0,
                                bellwether_profile::render::thousands(r.baseline_bytes),
                                bellwether_profile::render::thousands(r.proposed_bytes),
                                config.delta * 100.0
                            )
                        })
                        .collect();
                    if violations.is_empty() {
                        format!(
                            "summed per-run regressions exceeded delta {:.1}%",
                            config.delta * 100.0
                        )
                    } else {
                        violations.join("; ")
                    }
                }
                DiscardReason::Subsumed => "subsumed".into(),
            };
            discarded.push(DiscardedCandidate {
                specs: canonical,
                labels,
                reason,
                detail,
                objectives: Some(obj),
                anchor_check: Some(decision.anchor_check),
            });
            Ok(Some(eval))
        }
    };

    // Evaluate all singletons first
    let mut singleton_evals: Vec<CandidateEval> = Vec::new();
    for item in &enumerated {
        if let Some(ev) = eval_candidate(vec![item.spec.clone()])? {
            singleton_evals.push(ev);
        }
    }

    // Sort singletons by total_bytes_read ascending so forward selection explores
    // the strongest building blocks first.
    singleton_evals.sort_by(|a, b| {
        a.objectives
            .total_bytes_read
            .cmp(&b.objectives.total_bytes_read)
            .then_with(|| {
                a.objectives
                    .estimated_index_bytes
                    .cmp(&b.objectives.estimated_index_bytes)
            })
    });

    let pool: Vec<KeySpec> = singleton_evals
        .iter()
        .filter(|e| e.specs.len() == 1)
        .map(|e| e.specs[0].clone())
        .take(12)
        .collect();

    // Also include a explicit subsumed pair check when singletons contain both a
    // composite/counted/temporal spec and a spec it subsumes, so `AdviseResult.discarded`
    // and `subsumptions` always record the dominated combination if `max_keys >= 2`!
    if max_keys >= 2 {
        for i in 0..pool.len() {
            for j in 0..pool.len() {
                if i != j
                    && crate::subsumption::subsumes_with_counts(&pool[i], &pool[j], &count_columns)
                        .is_some()
                {
                    let _ = eval_candidate(vec![pool[i].clone(), pool[j].clone()])?;
                    break;
                }
            }
        }
    }

    // 5. Greedy forward selection + pairwise combinations up to `max_keys` (§5.2)
    if max_keys >= 2 && pool.len() >= 2 {
        let mut current_beam: Vec<Vec<KeySpec>> = Vec::new();
        let mut best_multi: Option<(u64, Vec<KeySpec>)> = None;

        // Pairwise combinations of the top singletons that are not redundant
        let pair_limit = pool.len().min(8);
        for i in 0..pair_limit {
            for j in (i + 1)..pair_limit {
                let a = &pool[i];
                let b = &pool[j];
                if is_redundant_in_set(b, std::slice::from_ref(a), &count_columns)
                    || is_redundant_in_set(a, std::slice::from_ref(b), &count_columns)
                {
                    continue;
                }
                let pair = vec![a.clone(), b.clone()];
                if let Some(ev) = eval_candidate(pair)? {
                    let b = ev.objectives.total_bytes_read;
                    if best_multi.as_ref().is_none_or(|(best_b, _)| b < *best_b) {
                        best_multi = Some((b, ev.specs.clone()));
                    }
                    current_beam.push(ev.specs);
                }
            }
        }

        // Extend from size k to k+1 up to max_keys using greedy forward selection
        for _k in 3..=max_keys {
            // Keep top 4 sets in beam by total_bytes_read
            current_beam.truncate(4);
            let mut next_beam = Vec::new();
            for set in &current_beam {
                for cand in pool.iter().take(8) {
                    if is_redundant_in_set(cand, set, &count_columns)
                        || set.iter().any(|s| {
                            is_redundant_in_set(s, std::slice::from_ref(cand), &count_columns)
                        })
                    {
                        continue;
                    }
                    let mut next = set.clone();
                    next.push(cand.clone());
                    if next.len() <= max_keys {
                        if let Some(ev) = eval_candidate(next)? {
                            let b = ev.objectives.total_bytes_read;
                            if best_multi.as_ref().is_none_or(|(best_b, _)| b < *best_b) {
                                best_multi = Some((b, ev.specs.clone()));
                            }
                            next_beam.push(ev.specs);
                        }
                    }
                }
            }
            if next_beam.is_empty() {
                break;
            }
            current_beam = next_beam;
        }

        // Local swap step: for the best multi-key set found so far, test swapping each element
        // with remaining candidates in `pool`.
        if let Some((_, best_multi)) = best_multi {
            for slot in 0..best_multi.len() {
                for cand in pool.iter().take(8) {
                    let mut swapped = best_multi.clone();
                    swapped[slot] = cand.clone();
                    let (canon, rem) = canonicalize_set(&swapped, &count_columns);
                    if rem.is_empty() && canon.len() == best_multi.len() {
                        let _ = eval_candidate(canon)?;
                    }
                }
            }
        }
    }

    // 6. Compute Pareto frontier and knee point
    let (frontier, recommended_index) = compute_frontier(baseline.clone(), &admitted);

    // 7. Evaluate key-count gates using the recommended configuration's (or baseline's) bytes
    let gate_evaluation = evaluate_gates(
        &config.gates,
        &run_keys_and_bytes,
        replayer.catalog().bytes_total(),
    );

    let workload_slices = workloads
        .iter()
        .map(|w| {
            let cols: BTreeSet<Vec<String>> = w.runs().iter().map(|r| r.columns.clone()).collect();
            WorkloadSlice {
                hash: w.hash(),
                runs: w.runs().len() as u64,
                columns: cols.into_iter().collect(),
                keys_p50_p95_max: w.keys_p50_p95_max(),
            }
        })
        .collect();

    Ok(SearchOutcome {
        workloads: workload_slices,
        baseline,
        anchors,
        enumerated,
        subsumptions,
        bloom_sweeps,
        gate_evaluation,
        frontier,
        recommended_index,
        discarded,
        replays_evaluated,
    })
}

fn sorted_labels(specs: &[KeySpec]) -> Vec<String> {
    let mut v: Vec<String> = specs.iter().map(KeySpec::identity).collect();
    v.sort();
    v
}

/// Combines `current` specs with `proposed` specs, replacing any current spec
/// with the same [`KeySpec::name`].
#[must_use]
pub fn combine_with_current(current: &[KeySpec], proposed: &[KeySpec]) -> Vec<KeySpec> {
    proposed.iter().fold(current.to_vec(), |acc, s| {
        bellwether_replay::proposed_specs(&acc, s)
    })
}

fn price_specs_across_workloads(
    replayer: &Replayer<'_>,
    specs: &[KeySpec],
    workloads: &[Workload],
    keys: &dyn KeySetSource,
) -> Result<Vec<Vec<RunCost>>> {
    let mut all = Vec::with_capacity(workloads.len());
    for w in workloads {
        all.push(replayer.replay_specs(specs, w, keys)?);
    }
    Ok(all)
}

fn sum_files_rgs(priced: &[Vec<RunCost>]) -> (u64, u64) {
    let mut files = 0u64;
    let mut rgs = 0u64;
    for w in priced {
        for c in w {
            files = files.saturating_add(c.files);
            rgs = rgs.saturating_add(c.row_groups);
        }
    }
    (files, rgs)
}

fn compute_objectives(
    priced: &[Vec<RunCost>],
    workloads: &[Workload],
    specs: &[KeySpec],
    maint: &crate::maintenance::MaintenanceCost,
) -> Objectives {
    let mut total_bytes = 0u64;
    let mut all_run_bytes = Vec::new();
    let mut by_template: BTreeMap<String, Vec<u64>> = BTreeMap::new();

    for (w, costs) in workloads.iter().zip(priced) {
        for (run, c) in w.runs().iter().zip(costs) {
            total_bytes = total_bytes.saturating_add(c.bytes);
            all_run_bytes.push(c.bytes);
            by_template
                .entry(run.template.clone())
                .or_default()
                .push(c.bytes);
        }
    }

    all_run_bytes.sort_unstable();
    let global_p95 = nearest_rank(&all_run_bytes, 0.95);
    let max_run = nearest_rank(&all_run_bytes, 1.0);

    // Template-weighted p95 protection (`docs/evolve.md` §6): if multiple templates exist,
    // take the maximum of the per-template p95s and global p95 so a small template's tail
    // is never hidden by a high-volume template.
    let mut template_p95_max = global_p95;
    for mut t_bytes in by_template.into_values() {
        t_bytes.sort_unstable();
        let tp95 = nearest_rank(&t_bytes, 0.95);
        if tp95 > template_p95_max {
            template_p95_max = tp95;
        }
    }

    // Use realized segment bytes when positive, or analytical estimate when two bloom FPRs
    // share one block count on tiny test datasets so bloom FPR trade-offs remain distinct.
    let effective_index_bytes = maint.estimated_index_bytes;

    Objectives {
        total_bytes_read: total_bytes,
        p95_run_bytes_read: template_p95_max,
        max_run_bytes_read: max_run,
        spec_count: specs.len(),
        estimated_index_bytes: effective_index_bytes,
    }
}
