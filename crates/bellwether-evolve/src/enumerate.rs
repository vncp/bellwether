//! Rule-driven candidate key enumeration (`docs/evolve.md` §4.4).
//!
//! Reads the dataset's [`ProfileReport`] (fanout per single column and composite,
//! time columns, lints) and every [`Workload`] in the advise request, and
//! deterministically enumerates candidate [`KeySpec`]s covering every lookup mode:
//!
//! - Exact keys (`exact:col`), and with per-key row counts (`exact:col,counts`)
//!   when runs execute `count` or `aggregate` or consumer plans with counted joins/filters.
//! - Composite exact keys (`exact:a,b` ordered with the workload's supplied key
//!   columns first and selective partner columns by fanout, `docs/evolve.md` §4.4).
//! - Per-key time bounds (`exact:col@ts`) when runs carry windows or lookbacks.
//! - Temporal keys (`temporal:col@ts`, and with `tombstone` when runs ask for
//!   `live` rows).
//! - Bitemporal keys (`temporal2:col@valid,known`, with `tombstone` when runs ask
//!   for `live` rows).
//! - Bloom keys (`bloom:col@fpr` and `bloom:col@fpr@ts`), which seed the exact
//!   FPR descent grid in [`crate::knobs`].
//! - Range keys (`range:ts`) on window / lookback time columns.
//! - List keys (`list:col`) for `contains_any` / `contains_all` runs.
//! - Text keys (`text:col,tokenizer=words` and `text:col,tokenizer=trigram`) for
//!   text runs.
//! - Agg keys (`agg:col@val{...}`) beside counted keys for aggregate runs.
//! - Expr keys (`expr:name=expr:exact`) for expression runs.

use std::collections::{BTreeMap, BTreeSet};

use bellwether_core::encoding::check_ts_type;
use bellwether_profile::ProfileReport;
use bellwether_replay::{
    load_plan, servable, KeySetSource, Lookup, RecordedRun, Replayer, RunQuery, Workload,
};
use bellwether_types::{AggregateFn, KeySpec, Result, TextOp, Tokenizer};
use serde::{Deserialize, Serialize};

/// Where an enumerated candidate [`KeySpec`] came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateOrigin {
    /// Directly serves the columns/mode of a recorded workload run.
    Workload,
    /// Recommended by a profile lint or composite fanout measurement (`docs/evolve.md` §4.4).
    Profile,
    /// Per-key time bounds or range key for a windowed/temporal workload.
    Window,
    /// Numeric knob candidate (bloom FPR grid).
    KnobDescent,
    /// Extracted from a `ConsumerPlan` operator (WP15).
    Plan,
}

/// An enumerated singleton [`KeySpec`] candidate and its origin.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EnumeratedSpec {
    /// The candidate key spec.
    pub spec: KeySpec,
    /// Why it was enumerated.
    pub origin: CandidateOrigin,
}

/// Deterministically enumerates candidate [`KeySpec`]s for `workloads` given
/// `profile` and `replayer` (`docs/evolve.md` §4.4).
///
/// Also returns the `(columns, bounds)` tuples eligible for bloom FPR grid descent,
/// and the column sets that require exact-match per-key counts.
#[allow(clippy::too_many_lines, clippy::type_complexity)]
pub fn enumerate_candidates(
    profile: &ProfileReport,
    replayer: &Replayer<'_>,
    workloads: &[Workload],
    keys: &dyn KeySetSource,
    max_profile_composites: usize,
) -> Result<(
    Vec<EnumeratedSpec>,
    Vec<(Vec<String>, Option<String>)>,
    Vec<Vec<String>>,
)> {
    let schema = replayer.schema();
    let current_identities: BTreeSet<String> = replayer
        .current_specs()
        .iter()
        .map(KeySpec::identity)
        .collect();

    let mut out: Vec<EnumeratedSpec> = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut bloom_targets: Vec<(Vec<String>, Option<String>)> = Vec::new();
    let mut count_columns: Vec<Vec<String>> = Vec::new();

    let mut push = |spec: KeySpec, origin: CandidateOrigin| -> bool {
        let id = spec.identity();
        if !current_identities.contains(&id) && seen.insert(id) {
            out.push(EnumeratedSpec { spec, origin });
            true
        } else {
            false
        }
    };

    // Collect all time columns in the dataset that are valid timestamp/version types
    let valid_ts_cols: Vec<String> = profile
        .time_columns
        .iter()
        .filter(|c| {
            schema
                .field_with_name(c)
                .is_ok_and(|f| check_ts_type(f.data_type()).is_ok())
        })
        .cloned()
        .collect();

    let mut all_locate_columns: BTreeSet<Vec<String>> = BTreeSet::new();

    for w in workloads {
        for run in w.runs() {
            let query = run.query()?;
            if let Some(plan_hash) = &run.plan {
                let plan = load_plan(keys, plan_hash)?;
                let checked = replayer.check_plan(&plan)?;
                for s in checked.candidate_specs() {
                    if s.counts() && !count_columns.contains(&s.columns().to_vec()) {
                        count_columns.push(s.columns().to_vec());
                    }
                    push(s, CandidateOrigin::Plan);
                }
                continue;
            }

            let cols = &run.columns;
            match &query {
                RunQuery::Locate => {
                    if let [expr_str] = cols.as_slice() {
                        if bellwether_core::expr::is_expression(expr_str, schema) {
                            let parsed = bellwether_core::expr::Expr::parse(expr_str)?;
                            let name = bellwether_core::expr::suggested_name(&parsed, schema);
                            let spec = bellwether_core::expr::spec(
                                &name,
                                expr_str,
                                bellwether_core::expr::Over::Exact,
                            )?;
                            push(spec, CandidateOrigin::Workload);
                            continue;
                        }
                    }
                    all_locate_columns.insert(cols.clone());
                    push(
                        KeySpec::Exact {
                            columns: cols.clone(),
                            counts: false,
                            bounds: None,
                        },
                        CandidateOrigin::Workload,
                    );
                    if !bloom_targets.contains(&(cols.clone(), None)) {
                        bloom_targets.push((cols.clone(), None));
                    }
                }
                RunQuery::Count => {
                    all_locate_columns.insert(cols.clone());
                    if !count_columns.contains(cols) {
                        count_columns.push(cols.clone());
                    }
                    push(
                        KeySpec::Exact {
                            columns: cols.clone(),
                            counts: true,
                            bounds: None,
                        },
                        CandidateOrigin::Workload,
                    );
                    push(
                        KeySpec::Exact {
                            columns: cols.clone(),
                            counts: false,
                            bounds: None,
                        },
                        CandidateOrigin::Workload,
                    );
                }
                RunQuery::Aggregate(a) => {
                    if !count_columns.contains(cols) {
                        count_columns.push(cols.clone());
                    }
                    push(
                        KeySpec::Exact {
                            columns: cols.clone(),
                            counts: true,
                            bounds: None,
                        },
                        CandidateOrigin::Workload,
                    );
                    push(
                        KeySpec::Agg {
                            columns: cols.clone(),
                            value: a.value.clone(),
                            functions: AggregateFn::canonical(&a.functions),
                        },
                        CandidateOrigin::Workload,
                    );
                }
                RunQuery::Window(win) => {
                    all_locate_columns.insert(cols.clone());
                    let ts = &win.ts_column;
                    let ts_valid = schema
                        .field_with_name(ts)
                        .is_ok_and(|f| check_ts_type(f.data_type()).is_ok());
                    if ts_valid && !cols.contains(ts) {
                        push(
                            KeySpec::Exact {
                                columns: cols.clone(),
                                counts: false,
                                bounds: Some(ts.clone()),
                            },
                            CandidateOrigin::Window,
                        );
                        push(
                            KeySpec::Temporal {
                                columns: cols.clone(),
                                ts_column: ts.clone(),
                                counts: false,
                                tombstone: None,
                            },
                            CandidateOrigin::Window,
                        );
                        if !bloom_targets.contains(&(cols.clone(), Some(ts.clone()))) {
                            bloom_targets.push((cols.clone(), Some(ts.clone())));
                        }
                    }
                    push(
                        KeySpec::Exact {
                            columns: cols.clone(),
                            counts: false,
                            bounds: None,
                        },
                        CandidateOrigin::Workload,
                    );
                    push(
                        KeySpec::Range { column: ts.clone() },
                        CandidateOrigin::Window,
                    );
                }
                RunQuery::Latest | RunQuery::Asof { .. } | RunQuery::Lookback { .. } => {
                    for ts in &valid_ts_cols {
                        if !cols.contains(ts) {
                            push(
                                KeySpec::Temporal {
                                    columns: cols.clone(),
                                    ts_column: ts.clone(),
                                    counts: false,
                                    tombstone: None,
                                },
                                CandidateOrigin::Workload,
                            );
                        }
                    }
                    push(
                        KeySpec::Exact {
                            columns: cols.clone(),
                            counts: false,
                            bounds: None,
                        },
                        CandidateOrigin::Workload,
                    );
                }
                RunQuery::Live(live) => {
                    for ts in &valid_ts_cols {
                        if !cols.contains(ts) {
                            push(
                                KeySpec::Temporal {
                                    columns: cols.clone(),
                                    ts_column: ts.clone(),
                                    counts: false,
                                    tombstone: Some(live.tombstone.clone()),
                                },
                                CandidateOrigin::Workload,
                            );
                            push(
                                KeySpec::Temporal {
                                    columns: cols.clone(),
                                    ts_column: ts.clone(),
                                    counts: false,
                                    tombstone: None,
                                },
                                CandidateOrigin::Workload,
                            );
                        }
                    }
                }
                RunQuery::Bitemporal(b) => {
                    push(
                        KeySpec::Bitemporal {
                            columns: cols.clone(),
                            valid_column: b.valid_column.clone(),
                            known_column: b.known_column.clone(),
                            tombstone: b.tombstone.clone(),
                        },
                        CandidateOrigin::Workload,
                    );
                    if b.tombstone.is_some() {
                        push(
                            KeySpec::Bitemporal {
                                columns: cols.clone(),
                                valid_column: b.valid_column.clone(),
                                known_column: b.known_column.clone(),
                                tombstone: None,
                            },
                            CandidateOrigin::Workload,
                        );
                    }
                }
                RunQuery::ContainsAny | RunQuery::ContainsAll => {
                    if let [col] = cols.as_slice() {
                        push(
                            KeySpec::List {
                                column: col.clone(),
                            },
                            CandidateOrigin::Workload,
                        );
                    }
                }
                RunQuery::Text(op) => {
                    if let [col] = cols.as_slice() {
                        match op {
                            TextOp::HasToken | TextOp::HasPrefix => {
                                push(
                                    KeySpec::Text {
                                        column: col.clone(),
                                        tokenizer: Tokenizer::Words,
                                        bloom: None,
                                    },
                                    CandidateOrigin::Workload,
                                );
                            }
                            TextOp::Contains => {}
                        }
                        push(
                            KeySpec::Text {
                                column: col.clone(),
                                tokenizer: Tokenizer::Trigram,
                                bloom: None,
                            },
                            CandidateOrigin::Workload,
                        );
                    }
                }
                RunQuery::SemiJoin(_) => {
                    // Join keys require right dataset context; exact on left columns still prunes
                    push(
                        KeySpec::Exact {
                            columns: cols.clone(),
                            counts: false,
                            bounds: None,
                        },
                        CandidateOrigin::Workload,
                    );
                }
            }
        }
    }

    // Rule-driven enumeration from ProfileReport fanout entries (singles and composites
    // ordered by selectivity / fanout, `docs/evolve.md` §4.4).
    let run_col_sets: Vec<Vec<String>> = all_locate_columns.into_iter().collect();
    let mut added_from_profile = 0usize;
    for entry in &profile.fanout {
        if added_from_profile >= max_profile_composites {
            break;
        }
        let fcols = &entry.fanout.columns;
        let cand = KeySpec::Exact {
            columns: fcols.clone(),
            counts: false,
            bounds: None,
        };
        // Include if it serves any run by Point lookup (e.g. a selective subset or reordered composite
        // of a multi-column run)
        let serves_point = run_col_sets
            .iter()
            .any(|rc| matches!(servable(&cand, rc), Some(Lookup::Point { .. })));
        if serves_point && push(cand, CandidateOrigin::Profile) {
            added_from_profile += 1;
        }
    }

    // Also if multiple runs supply overlapping columns (e.g. run1 supplies [a], run2 supplies [a, b]),
    // ensure composites ordered by selectivity (lowest p95 row_groups_per_value first) are enumerated!
    for rc in &run_col_sets {
        if rc.len() >= 2 {
            let mut reordered = rc.clone();
            reordered.sort_by(|a, b| {
                let fa = single_fanout(profile, a);
                let fb = single_fanout(profile, b);
                fa.total_cmp(&fb).then_with(|| a.cmp(b))
            });
            if &reordered != rc {
                push(
                    KeySpec::Exact {
                        columns: reordered,
                        counts: false,
                        bounds: None,
                    },
                    CandidateOrigin::Profile,
                );
            }
        }
    }

    // Group aggregate runs that need both counted exact key and agg key
    let mut agg_groups: BTreeMap<(Vec<String>, String), Vec<AggregateFn>> = BTreeMap::new();
    for w in workloads {
        for r in w.runs().iter().filter(|r| r.plan.is_none()) {
            if let Ok(RunQuery::Aggregate(a)) = r.query() {
                agg_groups
                    .entry((r.columns.clone(), a.value.clone()))
                    .or_default()
                    .extend(a.functions.iter().copied());
            }
        }
    }
    for ((cols, value), fns) in agg_groups {
        push(
            KeySpec::Exact {
                columns: cols.clone(),
                counts: true,
                bounds: None,
            },
            CandidateOrigin::Workload,
        );
        push(
            KeySpec::Agg {
                columns: cols,
                value,
                functions: AggregateFn::canonical(&fns),
            },
            CandidateOrigin::Workload,
        );
    }

    let _ = RecordedRun::query;
    Ok((out, bloom_targets, count_columns))
}

fn single_fanout(profile: &ProfileReport, col: &str) -> f64 {
    profile
        .fanout
        .iter()
        .find(|e| e.fanout.columns.len() == 1 && e.fanout.columns[0] == col)
        .map_or(f64::MAX, |e| e.fanout.row_groups_per_value[1])
}
