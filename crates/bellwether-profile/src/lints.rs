//! Rule-catalog lints detectable from profile statistics (`docs/rule-catalog.md`).
//!
//! | Rule | Fires when |
//! |---|---|
//! | `batch_stamp_key` | `bellwether_source::stats::batch_stamp` holds for column `B` against its composite with the chosen partner `K` (see below), under some time column: one value of `B` touches at least 2 files at the median, `B`'s median row groups per value is at least 2 times the composite's, and `B` concords with time across days at least 0.9 |
//! | `low_selectivity_single_column` | the median value of `C` touches at least [`LOW_SELECTIVITY_MIN_FRACTION`] of all row groups, some id-like `O` that is not a batch stamp makes the composite's p95 at most half of that median, and `C` is not a batch stamp (a batch stamp is not a key at all) |
//! | `scattered_time_column` | a date or timestamp column's overlap depth is at least `SCATTER_MIN_DEPTH` and its sortedness below `SCATTER_MAX_SORTEDNESS` (`bellwether_source::stats`) |
//! | `declared_but_unused_key` | a manifest declares a key spec and a recorded workload exists whose runs never supply the spec's leading column (for an `expr` key, its expression) |
//! | `R-sketch-1` (low-cardinality key, [`low_cardinality_key`]) | from `sketch` sidecars, not the profile: a sketched column `B` whose values each cover at least [`LOW_CARDINALITY_MIN_ROWS_PER_VALUE`] rows, beside an id-like sketched column `K` with at least [`LOW_CARDINALITY_PARTNER_RATIO`] times `B`'s distinct values, present in at least [`LOW_CARDINALITY_COPRESENCE`] of the files `B` is present in |
//! | `no_discriminating_column` | the dataset has at least 2 row groups and every measured single column (composites are not searched) has a median value touching at least [`NO_DISCRIMINATING_MIN_FRACTION`] of all row groups: the layout, not a missing index, sets the cost of a lookup |
//! | `R-counts-1` (count before scan, [`COUNT_BEFORE_SCAN`]) | a recorded workload has a `count` run that a later run over the same keys follows ([`WorkloadSummary::counts_before_scan`]), and no declared `exact` spec on exactly the workload's key columns stores counts |
//! | `R-text-1` | the workload records text runs (`contains`, `has_token`, `has_prefix`) on a string column, and for some of those matches no complete text key on the column serves it ([`text_lookups`]; the caller reads the workload's runs, so it is not part of [`run`]) |
//! | `R-expr-1` | lookups supply a function of a dataset column (`bellwether_core::expr`, for example `json_get(payload,'$.symbol')`) that type-checks, and no declared `expr` key is over that expression, normalized |
//!
//! The two decisions the benchmark also makes are not reimplemented here:
//! `batch_stamp_key` is `bellwether_source::stats::batch_stamp` and
//! `scattered_time_column` is `bellwether_source::stats::overlap_depth`, the
//! functions the benchmark calls to decide its `planted` flags
//! (`docs/benchmark.md`, "2026-10-24: shared time measures for two planted
//! flags" and "2026-10-24: one batch-stamp decision"). The scatter lint and
//! its planted flag agree by construction. Partners for both composite
//! rules come from `bellwether_source::stats::partner_order`, which the
//! benchmark also applies: row ids refused unless the workload supplies
//! them, only supplied candidates when the workload supplies any, repeating
//! before near-unique, then the smaller composite p95. The lint fires on the
//! first partner for which its rule holds. The benchmark's only candidate is
//! `entity_id`, which its workload supplies, so wherever a recorded workload
//! supplies `entity_id` both judge the same composite and agree by
//! construction; without one the profiler may judge another partner. The
//! low-selectivity criteria are the quantities the rule catalog names, at
//! the thresholds the benchmark uses (`docs/benchmark.md`, "2026-10-24:
//! planted anti-patterns, rule mapping, and measured planted flags").
//!
//! Time concordance across days is what separates a write stamp from
//! an id sorted or range-partitioned within each day, which restarts or
//! repeats every day.
//!
//! A recommended composite is the measured one: its columns are ordered with
//! the columns the recorded workload supplies first (the order a lookup can
//! seek by prefix), the rest in schema order. Without a workload the message
//! states that order's assumption.
//!
//! `Lint.confidence` maps the catalog's levels to numbers: high [`HIGH`],
//! medium [`MEDIUM`], low [`LOW`]. Any sampled measurement a lint rests on
//! makes it low.
//!
//! The three rules that need a consumer's query plan
//! (`unprefiltered_window`, `lookback_unbounded`,
//! `wide_projection_before_filter`) are out of scope: no `EvidenceBody`
//! carries a plan today. This module never emits them.

use arrow::datatypes::Schema;
use bellwether_core::expr::{suggested_name, Expr, Typed};
use bellwether_source::stats::{
    batch_stamp as batch_stamp_decision, partner_order, BatchStamp, PartnerCandidate,
};
use bellwether_types::{KeySpec, Lint, Result, RowGroupMeta, TextOp};

use crate::fanout::{is_id_like, DistinctSource, FanoutEntry, KeyPlan};
use crate::manifest::ManifestInfo;
use crate::ranges::{ColumnRanges, TimeScatter};
use crate::workload::WorkloadSummary;

/// Rule id: a column that looks like a key but is a batch stamp.
pub const BATCH_STAMP_KEY: &str = "batch_stamp_key";
/// Rule id: a single-column key whose values each touch most row groups.
pub const LOW_SELECTIVITY_SINGLE_COLUMN: &str = "low_selectivity_single_column";
/// Rule id: a time column whose row-group ranges overlap far beyond its rows.
pub const SCATTERED_TIME_COLUMN: &str = "scattered_time_column";
/// Rule id: a declared key spec no recorded run uses.
pub const DECLARED_BUT_UNUSED_KEY: &str = "declared_but_unused_key";
/// Rule id: no measured key prunes, because every value is spread over
/// nearly every row group.
pub const NO_DISCRIMINATING_COLUMN: &str = "no_discriminating_column";
/// Rule id of the `counts` detector (`docs/rule-catalog.md`): a count pass
/// over keys that a later query looks up again, with no counting spec to
/// answer it from the index.
pub const COUNT_BEFORE_SCAN: &str = "R-counts-1";

/// Rule id of the `sketch` detector (`docs/rule-catalog.md`): the
/// low-cardinality key, read from sketches.
pub const LOW_CARDINALITY_KEY_FROM_SKETCHES: &str = "R-sketch-1";
/// `R-sketch-1`: the fewest rows one value of the column must cover on
/// average (rows over approximate distinct values), the threshold of the
/// low-cardinality-key lint.
pub const LOW_CARDINALITY_MIN_ROWS_PER_VALUE: f64 = 100_000.0;
/// `R-sketch-1`: the partner's distinct values over the column's, at least.
pub const LOW_CARDINALITY_PARTNER_RATIO: f64 = 10.0;
/// `R-sketch-1`: files holding both columns over files holding the column,
/// at least.
pub const LOW_CARDINALITY_COPRESENCE: f64 = 0.9;
/// Rule id: lookups filter on a function of a column that no `expr` key
/// indexes.
pub const EXPR_RULE: &str = "R-expr-1";

/// Rule id (B5): text lookups on a string column no text key serves.
pub const TEXT_LOOKUP: &str = "R-text-1";

/// Confidence of a high finding.
pub const HIGH: f64 = 0.9;
/// Confidence of a medium finding.
pub const MEDIUM: f64 = 0.6;
/// Confidence of a low finding.
pub const LOW: f64 = 0.3;

/// `batch_stamp_key`: high confidence needs the composite's p95 files per
/// value at most this fraction of the column's.
pub const BATCH_HIGH_FILES_RATIO: f64 = 0.5;
/// `low_selectivity_single_column`: median row groups over the total.
pub const LOW_SELECTIVITY_MIN_FRACTION: f64 = 0.5;
/// `low_selectivity_single_column`: median over the composite's p95.
pub const LOW_SELECTIVITY_MIN_RATIO_OVER_COMPOSITE: f64 = 2.0;
/// `no_discriminating_column`: the least median row groups per value, over
/// every measured single column, as a fraction of all row groups. At three quarters a
/// point lookup on the best key still reads three row groups in four.
pub const NO_DISCRIMINATING_MIN_FRACTION: f64 = 0.75;

/// Everything the detectors read.
#[derive(Clone, Copy, Debug)]
pub struct LintInputs<'a> {
    /// The dataset schema.
    pub schema: &'a Schema,
    /// Candidate columns, their types, and the time columns.
    pub plan: &'a KeyPlan,
    /// One entry per key of the plan.
    pub entries: &'a [FanoutEntry],
    /// Footer range statistics per analyzed column.
    pub ranges: &'a [ColumnRanges],
    /// The shared overlap depth per time column.
    pub time: &'a [TimeScatter],
    /// Every row group in dataset order, for the shared time concordance.
    pub row_groups: &'a [RowGroupMeta],
    /// The recorded workload, if any.
    pub workload: Option<&'a WorkloadSummary>,
    /// The columns lookups supply: the recorded workload's key columns and
    /// any the caller names ([`crate::ProfileOptions::supplied_columns`],
    /// for example a key file's). Empty when nothing says.
    pub supplied: &'a [String],
    /// The manifest, if the folder is indexed.
    pub manifest: Option<&'a ManifestInfo>,
    /// Rows in the dataset.
    pub rows_total: u64,
}

/// Runs every detector. Lints are in rule order, then column order.
pub fn run(inputs: &LintInputs<'_>) -> Result<Vec<Lint>> {
    let mut lints = batch_stamp(inputs)?;
    let stamps: Vec<String> = lints.iter().map(|l| l.columns[0].clone()).collect();
    lints.extend(low_selectivity(inputs, &stamps));
    lints.extend(scattered_time(inputs));
    lints.extend(declared_but_unused(inputs));
    lints.extend(count_before_scan(inputs));
    lints.extend(no_discriminating(inputs.entries));
    lints.extend(expression_lookups(inputs));
    Ok(lints)
}

fn single<'a>(inputs: &LintInputs<'a>, column: &str) -> Option<&'a FanoutEntry> {
    inputs
        .entries
        .iter()
        .find(|e| e.fanout.columns.len() == 1 && e.fanout.columns[0] == column)
}

/// The measured composite of exactly the columns `{a, b}`.
fn pair<'a>(inputs: &LintInputs<'a>, a: &str, b: &str) -> Option<&'a FanoutEntry> {
    inputs.entries.iter().find(|e| {
        let c = &e.fanout.columns;
        c.len() == 2 && ((c[0] == a && c[1] == b) || (c[0] == b && c[1] == a))
    })
}

fn supplied(inputs: &LintInputs<'_>, column: &str) -> bool {
    inputs.supplied.iter().any(|c| c == column)
}

/// The normalized text of each supplied column that is an expression over
/// dataset columns: a function call, not itself a dataset column, that
/// reads at least one column and type-checks against the schema (so the
/// `expr` key the lint recommends is one `index` accepts).
fn supplied_expressions<'a>(inputs: &'a LintInputs<'_>) -> impl Iterator<Item = String> + 'a {
    inputs.supplied.iter().filter_map(|c| {
        if inputs.schema.field_with_name(c).is_ok() {
            return None;
        }
        let e = Expr::parse(c).ok().filter(Expr::is_call)?;
        if e.columns().is_empty() {
            return None;
        }
        Typed::new(e.clone(), inputs.schema).ok()?;
        Some(e.to_string())
    })
}

/// `R-expr-1`: each expression the lookups supply (a run keyed on a
/// function of a column) that no declared `expr` key is over, normalized.
/// An index on the column the expression reads cannot prune such a lookup,
/// since it holds the column's values and the lookup's are derived. High
/// confidence: the recorded runs state the predicate, nothing is inferred.
fn expression_lookups(inputs: &LintInputs<'_>) -> Vec<Lint> {
    let declared: Vec<(&str, &str)> = inputs
        .manifest
        .map(|m| {
            m.key_specs
                .iter()
                .filter_map(|s| match s {
                    KeySpec::Expr { name, expr, .. } => Some((name.as_str(), expr.as_str())),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();
    let mut seen: Vec<String> = Vec::new();
    let mut out: Vec<Lint> = Vec::new();
    for norm in supplied_expressions(inputs) {
        if declared.iter().any(|(_, e)| *e == norm) || seen.contains(&norm) {
            continue;
        }
        seen.push(norm.clone());
        let Ok(parsed) = Expr::parse(&norm) else {
            continue;
        };
        let columns = parsed.columns();
        let name = suggested_name(&parsed, inputs.schema);
        let near = if declared.is_empty() {
            String::new()
        } else {
            format!(
                " The declared expr keys ({}) are over other expressions; a lookup matches an \
                 expr key only when its expression normalizes to the key's.",
                declared
                    .iter()
                    .map(|(n, e)| format!("{n} = {e}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        out.push(Lint {
            rule: EXPR_RULE.into(),
            columns: columns.clone(),
            message: format!(
                "Lookups filter on {norm}, a function of {cols}, so no index on {cols} can \
                 prune them: an index holds the column's values and these lookups ask for \
                 derived ones, so every row group is read and the expression recomputed per \
                 row.{near} An expr key indexes the derived value: price it with `bellwether \
                 why --from <keys>` (its candidate is expr:{name}={norm}:exact), then build it \
                 with `bellwether index --key \"expr:{name}={norm}:exact\"`.",
                cols = columns.join(", "),
            ),
            confidence: HIGH,
            site: None,
            fix: None,
        });
    }
    out
}

/// Whether anything says which columns lookups supply: a recorded workload,
/// or supplied columns from a key file.
fn informed(inputs: &LintInputs<'_>) -> bool {
    inputs.workload.is_some() || !inputs.supplied.is_empty()
}

/// `candidates` (`(partner, composite)`, in schema order) in the shared
/// partner order of `bellwether_source::stats::partner_order`: row ids
/// refused unless the workload supplies them, only supplied candidates when
/// any is, repeating before near-unique, then the smaller composite p95.
fn by_preference<'a>(
    inputs: &LintInputs<'a>,
    candidates: &[(&'a FanoutEntry, &'a FanoutEntry)],
) -> Vec<(&'a FanoutEntry, &'a FanoutEntry)> {
    let described: Vec<PartnerCandidate> = candidates
        .iter()
        .map(|(p, comp)| PartnerCandidate {
            column: p.fanout.columns[0].clone(),
            distinct: p.fanout.distinct,
            distinct_exact: p.sample.distinct_source == DistinctSource::Exact,
            supplied: supplied(inputs, &p.fanout.columns[0]),
            composite_row_groups_p95: comp.fanout.row_groups_per_value[1],
        })
        .collect();
    partner_order(&described, inputs.rows_total)
        .into_iter()
        .map(|i| candidates[i])
        .collect()
}

/// `(a, b)` for a composite's measured columns.
fn label(columns: &[String]) -> String {
    format!("({})", columns.join(", "))
}

/// The clause stating a composite order's assumption when neither a
/// workload nor a key file says which columns lookups supply.
fn order_assumption(inputs: &LintInputs<'_>, composite: &[String]) -> String {
    if informed(inputs) {
        String::new()
    } else {
        format!(
            " (no recorded workload: this order assumes lookups supply {})",
            composite[0]
        )
    }
}

fn fmt_count(v: f64) -> String {
    if (v - v.round()).abs() < 1e-9 {
        format!("{v:.0}")
    } else {
        format!("{v:.1}")
    }
}

/// The shared decision (`bellwether_source::stats::batch_stamp`) for column
/// `f` against composite `comp`, under the first time column it holds for.
fn shared_batch_stamp(
    inputs: &LintInputs<'_>,
    f: &bellwether_types::Fanout,
    comp: &bellwether_types::Fanout,
) -> Result<Option<(BatchStamp, String)>> {
    for t in &inputs.plan.time_columns {
        let d = batch_stamp_decision(
            f.files_per_value[0],
            f.row_groups_per_value[0],
            comp.row_groups_per_value[0],
            inputs.row_groups,
            &f.columns[0],
            &t.name,
        )?;
        if d.planted {
            return Ok(Some((d, t.name.clone())));
        }
    }
    Ok(None)
}

fn batch_stamp(inputs: &LintInputs<'_>) -> Result<Vec<Lint>> {
    let mut out = Vec::new();
    for b in inputs
        .entries
        .iter()
        .filter(|e| e.fanout.columns.len() == 1)
    {
        let name = &b.fanout.columns[0];
        let f = &b.fanout;
        // Partners are tried in preference order; the first against which
        // the shared decision holds is the one recommended.
        let candidates: Vec<(&FanoutEntry, &FanoutEntry)> = inputs
            .plan
            .columns
            .iter()
            .filter(|c| &c.name != name && is_id_like(&c.data_type))
            .filter_map(|c| Some((single(inputs, &c.name)?, pair(inputs, &c.name, name)?)))
            .collect();
        let mut found = None;
        for (k, comp) in by_preference(inputs, &candidates) {
            if let Some((_, time)) = shared_batch_stamp(inputs, f, &comp.fanout)? {
                found = Some((k, comp, time));
                break;
            }
        }
        let Some((k, comp, time)) = found else {
            continue;
        };
        let key = &k.fanout.columns[0];
        let composite = &comp.fanout.columns;
        let sampled = f.sampled || k.fanout.sampled || comp.fanout.sampled;
        let confidence = if sampled {
            LOW
        } else if inputs.workload.is_some()
            && comp.fanout.files_per_value[1] <= BATCH_HIGH_FILES_RATIO * f.files_per_value[1]
        {
            HIGH
        } else {
            MEDIUM
        };
        out.push(Lint {
            rule: BATCH_STAMP_KEY.into(),
            columns: vec![name.clone()],
            message: format!(
                "{name} looks like an id but behaves like a write-batch stamp: it rises with \
                 {time} across days, and one value touches {} of {} files and {} row groups at \
                 the median, against {} for {}, so look up or join on {} or on {key} alone \
                 instead{}.",
                fmt_count(f.files_per_value[0]),
                f.files_total,
                fmt_count(f.row_groups_per_value[0]),
                fmt_count(comp.fanout.row_groups_per_value[0]),
                label(composite),
                label(composite),
                order_assumption(inputs, composite),
            ),
            confidence,
            site: None,
            fix: None,
        });
    }
    Ok(out)
}

fn low_selectivity(inputs: &LintInputs<'_>, stamps: &[String]) -> Vec<Lint> {
    let mut out = Vec::new();
    for c in inputs
        .entries
        .iter()
        .filter(|e| e.fanout.columns.len() == 1)
    {
        let name = &c.fanout.columns[0];
        let f = &c.fanout;
        if f.row_groups_total == 0 || stamps.contains(name) {
            continue;
        }
        #[allow(clippy::cast_precision_loss)]
        let fraction = f.row_groups_per_value[0] / f.row_groups_total as f64;
        if fraction < LOW_SELECTIVITY_MIN_FRACTION {
            continue;
        }
        let candidates: Vec<(&FanoutEntry, &FanoutEntry)> = inputs
            .plan
            .columns
            .iter()
            // A batch stamp is no key, so never a partner either.
            .filter(|o| &o.name != name && !stamps.contains(&o.name) && is_id_like(&o.data_type))
            .filter_map(|o| Some((single(inputs, &o.name)?, pair(inputs, name, &o.name)?)))
            .collect();
        // The first partner in the shared order for which the criterion holds.
        let Some((o, comp)) = by_preference(inputs, &candidates)
            .into_iter()
            .find(|(_, comp)| {
                f.row_groups_per_value[0]
                    >= LOW_SELECTIVITY_MIN_RATIO_OVER_COMPOSITE
                        * comp.fanout.row_groups_per_value[1]
            })
        else {
            continue;
        };
        let other = &o.fanout.columns[0];
        let composite = &comp.fanout.columns;
        let sampled = f.sampled || comp.fanout.sampled;
        let confidence = if sampled {
            LOW
        } else if !informed(inputs) {
            MEDIUM
        } else if supplied(inputs, name) && supplied(inputs, other) {
            HIGH
        } else if supplied(inputs, &composite[0]) {
            MEDIUM
        } else {
            LOW
        };
        out.push(Lint {
            rule: LOW_SELECTIVITY_SINGLE_COLUMN.into(),
            columns: vec![name.clone()],
            message: format!(
                "As a single-column key {name} prunes almost nothing: its median value touches {} \
                 of {} row groups ({:.0}%), while {} touches at most {} at p95, so index {} \
                 instead{}.",
                fmt_count(f.row_groups_per_value[0]),
                f.row_groups_total,
                fraction * 100.0,
                label(composite),
                fmt_count(comp.fanout.row_groups_per_value[1]),
                label(composite),
                order_assumption(inputs, composite),
            ),
            confidence,
            site: None,
            fix: None,
        });
    }
    out
}

fn scattered_time(inputs: &LintInputs<'_>) -> Vec<Lint> {
    inputs
        .time
        .iter()
        .filter(|t| t.scattered)
        .map(|t| {
            let name = &t.column;
            Lint {
                rule: SCATTERED_TIME_COLUMN.into(),
                columns: vec![name.clone()],
                message: format!(
                    "{name} is out of order: a point in time falls inside {:.1} row-group ranges \
                     on average, and only {:.0}% of consecutive row groups in a file ascend \
                     without overlapping, so range pruning on {name} reads far more than it \
                     needs; sort or cluster each partition by {name} when writing.",
                    t.depth,
                    t.sortedness * 100.0
                ),
                confidence: MEDIUM,
                site: None,
                fix: None,
            }
        })
        .collect()
}

fn declared_but_unused(inputs: &LintInputs<'_>) -> Vec<Lint> {
    let (Some(manifest), Some(_)) = (inputs.manifest, inputs.workload) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for spec in &manifest.key_specs {
        let Some(lead) = spec.columns().first() else {
            continue;
        };
        // An `expr` key is used by a run keyed on its expression, never by
        // its virtual column's name.
        let used = match spec {
            KeySpec::Expr { expr, .. } => supplied_expressions(inputs).any(|e| e == *expr),
            _ => supplied(inputs, lead),
        };
        if used {
            continue;
        }
        let lead = match spec {
            KeySpec::Expr { expr, .. } => expr,
            _ => lead,
        };
        let name = spec.name();
        let identity = spec.identity();
        let bytes: u64 = manifest
            .segment_bytes
            .iter()
            .filter(|(n, _)| *n == identity)
            .map(|(_, b)| *b)
            .sum();
        out.push(Lint {
            rule: DECLARED_BUT_UNUSED_KEY.into(),
            columns: spec.columns().to_vec(),
            message: format!(
                "The key spec {name} is declared but no recorded run looks up by {lead}, so \
                 removing it saves {bytes} bytes of index segments and changes no recorded lookup."
            ),
            confidence: MEDIUM,
            site: None,
            fix: None,
        });
    }
    out
}

/// `R-counts-1`: the recorded workload counts a key set and then looks the
/// same keys up again (a count pass gating the query), and no declared
/// spec counts its key columns (one storing counts whose key is exactly
/// those columns, in any order, exact or temporal, as the planner counts
/// with), so every count costs what the query costs. Advice:
/// `exact:<columns>,counts`, which answers the count from the index with
/// no data file read, exactly. High confidence: both facts are exact (run
/// hashes, declared specs).
fn count_before_scan(inputs: &LintInputs<'_>) -> Vec<Lint> {
    let Some(workload) = inputs.workload else {
        return Vec::new();
    };
    if workload.counts_before_scan == 0 || workload.key_columns.is_empty() {
        return Vec::new();
    }
    let columns = &workload.key_columns;
    let counted = inputs.manifest.is_some_and(|m| {
        m.key_specs.iter().any(|s| {
            s.counts()
                && s.columns().len() == columns.len()
                && s.columns().iter().all(|c| columns.contains(c))
        })
    });
    if counted {
        return Vec::new();
    }
    let cols = columns.join(",");
    let n = workload.counts_before_scan;
    let runs = if n == 1 { "run" } else { "runs" };
    vec![Lint {
        rule: COUNT_BEFORE_SCAN.into(),
        columns: columns.clone(),
        message: format!(
            "{n} recorded count {runs} each precede a later run over the same keys on ({cols}), \
             and no key spec stores counts on ({cols}), so each count reads what the query \
             reads; declaring exact:{cols},counts answers `bellwether count` from the index with \
             no data file read and gives plans exact row counts."
        ),
        confidence: HIGH,
        site: None,
        fix: None,
    }]
}

/// `no_discriminating_column`: the measured single column with the lowest
/// median row groups per value (ties: lower p95, then measured order) still
/// touches nearly every row group, so no column prunes and the layout, not
/// a missing index, decides what a lookup reads. Names that lowest-fanout
/// column. Composites are not searched: a rare pair says nothing about how
/// one column is laid out. One row group has nothing to prune and never fires.
fn no_discriminating(entries: &[FanoutEntry]) -> Option<Lint> {
    // Single columns only: a pair is selective because its combinations
    // are rare, which says nothing about how one column's values are laid
    // out, and the advice is to cluster by one column.
    let best = entries
        .iter()
        .filter(|e| e.fanout.columns.len() == 1)
        .min_by(|a, b| {
            let (fa, fb) = (&a.fanout, &b.fanout);
            fa.row_groups_per_value[0]
                .total_cmp(&fb.row_groups_per_value[0])
                .then(fa.row_groups_per_value[1].total_cmp(&fb.row_groups_per_value[1]))
        })?;
    let f = &best.fanout;
    if f.row_groups_total < 2 {
        return None;
    }
    #[allow(clippy::cast_precision_loss)]
    let fraction = f.row_groups_per_value[0] / f.row_groups_total as f64;
    if fraction < NO_DISCRIMINATING_MIN_FRACTION {
        return None;
    }
    let sampled = entries.iter().any(|e| e.fanout.sampled);
    let name = &f.columns[0];
    Some(Lint {
        rule: NO_DISCRIMINATING_COLUMN.into(),
        columns: f.columns.clone(),
        message: format!(
            "No measured column prunes: the most selective, {name}, has a median value in {} of {} \
             row groups ({:.0}%), and every other single column measured touches at least as \
             many, so each column's values are spread over nearly every row group. The layout, not a \
             missing index, sets what a point lookup reads: an exact index on any of these keys \
             still reads nearly every row group, and only files rewritten clustered by the \
             column lookups supply would prune. A newest-version lookup can still prune by \
             time; price it with `bellwether why --from <keys> --latest-by <time column>`.",
            fmt_count(f.row_groups_per_value[0]),
            f.row_groups_total,
            fraction * 100.0,
        ),
        confidence: if sampled { LOW } else { HIGH },
        site: None, fix: None,
    })
}

/// One sketched single column, as [`low_cardinality_key`] reads it: the
/// facade folds a `sketch` sidecar's segments into these. Every distinct
/// count is approximate unless `distinct_exact`.
#[derive(Clone, Debug, PartialEq)]
pub struct SketchedColumn {
    /// The column.
    pub column: String,
    /// Its dataset type (id-likeness of a partner).
    pub data_type: arrow::datatypes::DataType,
    /// Rows holding a value (non-null). Exact.
    pub rows: u64,
    /// Distinct values: the sketch's estimate, or the exact count.
    pub distinct: u64,
    /// Whether `distinct` is exact.
    pub distinct_exact: bool,
    /// Files holding a value of the column. Exact.
    pub files_present: u64,
    /// Per other column of the same sketch: files holding both. Exact.
    pub copresent: Vec<(String, u64)>,
}

/// `R-sketch-1`, the low-cardinality key, from sketches alone (no data file
/// is read): column `B` fires when its values each cover at least
/// [`LOW_CARDINALITY_MIN_ROWS_PER_VALUE`] rows on average and an id-like
/// column `K` of the same sketch has at least
/// [`LOW_CARDINALITY_PARTNER_RATIO`] times as many distinct values and sits
/// in at least [`LOW_CARDINALITY_COPRESENCE`] of `B`'s files: `B` behaves
/// like a batch stamp beside the real id, and the advice is the composite
/// `(K, B)`. The partner with the most distinct values is named. Medium
/// confidence: the counts are approximate (or exact, which raises it to
/// high when both are).
#[must_use]
pub fn low_cardinality_key(columns: &[SketchedColumn]) -> Vec<Lint> {
    let mut out = Vec::new();
    for b in columns {
        if b.distinct == 0 {
            continue;
        }
        #[allow(clippy::cast_precision_loss)]
        let rows_per_value = b.rows as f64 / b.distinct as f64;
        if rows_per_value < LOW_CARDINALITY_MIN_ROWS_PER_VALUE {
            continue;
        }
        let partner = columns
            .iter()
            .filter(|k| k.column != b.column && crate::fanout::is_id_like(&k.data_type))
            .filter(|k| {
                #[allow(clippy::cast_precision_loss)]
                let ratio = k.distinct as f64 / b.distinct as f64;
                ratio >= LOW_CARDINALITY_PARTNER_RATIO
            })
            .filter(|k| {
                let both = b
                    .copresent
                    .iter()
                    .find(|(c, _)| *c == k.column)
                    .map_or(0, |(_, n)| *n);
                #[allow(clippy::cast_precision_loss)]
                let share = both as f64 / b.files_present.max(1) as f64;
                share >= LOW_CARDINALITY_COPRESENCE
            })
            .max_by_key(|k| k.distinct);
        let Some(k) = partner else {
            continue;
        };
        let exact = b.distinct_exact && k.distinct_exact;
        let tag = |e: bool| if e { "exact" } else { "approximate" };
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let rpv = crate::render::thousands(rows_per_value.round() as u64);
        out.push(Lint {
            rule: LOW_CARDINALITY_KEY_FROM_SKETCHES.to_string(),
            columns: vec![b.column.clone(), k.column.clone()],
            message: format!(
                "{b_col} is a low-cardinality key: about {rpv} rows share each of its {bd} values \
                 ({bt}, from sketches; no data file read), beside {k_col} with {kd} values ({kt}) \
                 in the same files; a lookup keyed on {b_col} alone reads every row of a value, \
                 so key on the composite ({k_col}, {b_col}) instead (no recorded workload: this \
                 assumes lookups supply both {k_col} and {b_col}; a lookup supplying {b_col} \
                 alone is not served by a key led by {k_col}, and a categorical {b_col} may not \
                 be a key at all)",
                b_col = b.column,
                k_col = k.column,
                bd = crate::render::thousands(b.distinct),
                kd = crate::render::thousands(k.distinct),
                bt = tag(b.distinct_exact),
                kt = tag(k.distinct_exact),
            ),
            confidence: if exact { HIGH } else { MEDIUM },
            site: None,
            fix: None,
        });
    }
    out
}

/// One string column a recorded workload looks up by text (B5): its runs
/// per match.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TextUse {
    /// The string column.
    pub column: String,
    /// Runs asking for rows that contain a substring (`contains`, `like
    /// '%x%'`).
    pub contains: u64,
    /// Runs asking for rows holding a word (`has_token`, `has`).
    pub has_token: u64,
    /// Runs asking for rows holding a word with a prefix (`has_prefix`).
    pub has_prefix: u64,
}

impl TextUse {
    /// The runs of `op`.
    #[must_use]
    pub fn runs(&self, op: TextOp) -> u64 {
        match op {
            TextOp::Contains => self.contains,
            TextOp::HasToken => self.has_token,
            TextOp::HasPrefix => self.has_prefix,
        }
    }
}

/// `R-text-1` (B5): a recorded workload looks rows up by text (`contains`,
/// `like '%x%'`, `has`, prefix match) on a string column, and for some of
/// the matches it asks no complete text key in `complete` (the key specs
/// complete at the current manifest) serves it on that column, so each such
/// run reads every row group: Parquet statistics hold a string column's
/// minimum and maximum, which say nothing about the words or substrings
/// inside its values. An exact key on the column does not serve them
/// either: it looks whole values up. Fires once per such column, high
/// confidence (nothing is sampled: the runs are recorded and the specs are
/// the manifest's). Does not fire when every match the runs ask has a
/// serving text key (bloom or not), or without text runs.
#[must_use]
pub fn text_lookups(uses: &[TextUse], complete: &[KeySpec]) -> Vec<Lint> {
    uses.iter()
        .filter_map(|u| {
            let c = &u.column;
            let unserved: Vec<TextOp> = [TextOp::Contains, TextOp::HasToken, TextOp::HasPrefix]
                .into_iter()
                .filter(|op| u.runs(*op) > 0)
                .filter(|op| {
                    !complete
                        .iter()
                        .any(|s| bellwether_core::text::serves(s, c, *op))
                })
                .collect();
            if unserved.is_empty() {
                return None;
            }
            let runs: Vec<String> = unserved
                .iter()
                .map(|op| format!("{} {}", u.runs(*op), op.as_str()))
                .collect();
            // Only the keys for the unserved matches: trigrams for
            // substrings, a words dictionary (exact) for words and prefixes.
            let mut keys = Vec::new();
            if unserved.contains(&TextOp::Contains) {
                keys.push(format!("text:{c},tokenizer=trigram for substrings"));
            }
            if unserved.iter().any(|op| *op != TextOp::Contains) {
                keys.push(format!(
                    "text:{c},tokenizer=words for whole words and word prefixes"
                ));
            }
            Some(Lint {
                rule: TEXT_LOOKUP.into(),
                columns: vec![c.clone()],
                message: format!(
                    "recorded runs look up rows of {c} by text ({}) and no text key serves \
                     them, so each reads every row group: Parquet keeps a string column's \
                     minimum and maximum, which say nothing about the words or substrings \
                     inside it, and an exact key looks whole values up. A text key stores each \
                     token's row groups: {}. `why` prices each on these runs, and `fix` builds \
                     the one it recommends with a receipt.",
                    runs.join(", "),
                    keys.join(", ")
                ),
                confidence: HIGH,
                site: None,
                fix: None,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fanout::SampleInfo;
    use bellwether_types::Fanout;

    fn entry(columns: &[&str], p50: f64) -> FanoutEntry {
        FanoutEntry {
            fanout: Fanout {
                columns: columns.iter().map(|c| (*c).to_string()).collect(),
                distinct: 100,
                sampled: false,
                files_per_value: [1.0; 3],
                row_groups_per_value: [p50, p50, p50],
                files_total: 1,
                row_groups_total: 22,
                sample_size: None,
                quantile_error: None,
            },
            sample: SampleInfo {
                retained: 100,
                admission_bound: String::new(),
                rate: 1.0,
                hll_estimate: 100,
                distinct_source: DistinctSource::Exact,
                quantile_rank_error: None,
                peak_entries: 0,
                peak_capacity: 0,
                high_water: 0,
                capped_at_rows: false,
            },
        }
    }

    /// The taxi shape: each column touches 20 or more of 22 row groups, a
    /// rare pair touches 6. The pair does not silence the lint, which names
    /// the lowest-fanout single column; a selective column does.
    #[test]
    fn no_discriminating_column_searches_single_columns_only() {
        let taxi = [
            entry(&["PULocationID"], 20.0),
            entry(&["DOLocationID"], 21.0),
            entry(&["PULocationID", "DOLocationID"], 6.0),
        ];
        let lint = no_discriminating(&taxi).expect("fires on the taxi shape");
        assert_eq!(lint.columns, vec!["PULocationID".to_string()]);
        assert!(lint.message.contains("20 of 22"), "{}", lint.message);
        let selective = [entry(&["id"], 1.0), entry(&["DOLocationID"], 21.0)];
        assert!(no_discriminating(&selective).is_none());
    }

    fn text_use(column: &str, contains: u64, has_token: u64, has_prefix: u64) -> TextUse {
        TextUse {
            column: column.into(),
            contains,
            has_token,
            has_prefix,
        }
    }

    fn text_key(column: &str, tokenizer: bellwether_types::Tokenizer) -> KeySpec {
        KeySpec::Text {
            column: column.into(),
            tokenizer,
            bloom: None,
        }
    }

    /// `R-text-1` fires on text runs over a string column that no complete
    /// text key serves, naming the matches, the keys, and how to price
    /// them; an exact key on the column, or a text key on another column,
    /// does not silence it, and a words key leaves `contains` unserved.
    #[test]
    fn text_lookups_fire_without_a_serving_text_key() {
        use bellwether_types::Tokenizer;
        let lints = text_lookups(
            &[text_use("body", 3, 2, 0)],
            &[
                KeySpec::Exact {
                    columns: vec!["body".into()],
                    counts: false,
                    bounds: None,
                },
                text_key("title", Tokenizer::Trigram),
            ],
        );
        assert_eq!(lints.len(), 1);
        let l = &lints[0];
        assert_eq!(l.rule, TEXT_LOOKUP);
        assert_eq!(l.columns, ["body".to_string()]);
        assert!(
            l.message.contains("(3 contains, 2 has_token)"),
            "{}",
            l.message
        );
        assert!(
            l.message.contains("text:body,tokenizer=trigram"),
            "{}",
            l.message
        );
        assert!(
            l.message.contains("text:body,tokenizer=words"),
            "{}",
            l.message
        );
        assert!((l.confidence - HIGH).abs() < f64::EPSILON);
        // A words key serves has_token, not contains.
        let partly = text_lookups(
            &[text_use("body", 3, 2, 0)],
            &[text_key("body", Tokenizer::Words)],
        );
        assert_eq!(partly.len(), 1);
        assert!(
            partly[0].message.contains("(3 contains)"),
            "{}",
            partly[0].message
        );
        // It names only the key for the unserved match, never one that exists.
        assert!(
            !partly[0].message.contains("tokenizer=words"),
            "{}",
            partly[0].message
        );
        assert!(
            partly[0].message.contains("tokenizer=trigram"),
            "{}",
            partly[0].message
        );
        // Words only: the words dictionary, not trigrams.
        let words = text_lookups(&[text_use("body", 0, 2, 1)], &[]);
        let m = &words[0].message;
        assert!(m.contains("(2 has_token, 1 has_prefix)"), "{m}");
        assert!(
            m.contains("tokenizer=words") && !m.contains("tokenizer=trigram"),
            "{m}"
        );
        // contains only: no words key is named.
        let only = text_lookups(&[text_use("body", 1, 0, 0)], &[]);
        assert!(
            !only[0].message.contains("tokenizer=words"),
            "{}",
            only[0].message
        );
    }

    /// It does not fire when a complete text key serves every match the
    /// runs ask (a trigram key serves all three), or without text runs.
    #[test]
    fn text_lookups_do_not_fire_when_served_or_without_runs() {
        use bellwether_types::Tokenizer;
        let served = text_lookups(
            &[text_use("body", 3, 2, 1)],
            &[text_key("body", Tokenizer::Trigram)],
        );
        assert!(served.is_empty(), "{served:?}");
        let words = text_lookups(
            &[text_use("body", 0, 2, 1)],
            &[text_key("body", Tokenizer::Words)],
        );
        assert!(words.is_empty(), "{words:?}");
        assert_eq!(text_lookups(&[text_use("body", 0, 0, 0)], &[]), []);
        assert_eq!(text_lookups(&[], &[]), []);
    }
}
