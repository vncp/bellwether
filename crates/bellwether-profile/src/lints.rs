//! Rule-catalog lints detectable from profile statistics (`docs/rule-catalog.md`).
//!
//! | Rule | Fires when |
//! |---|---|
//! | `batch_stamp_key` | `bellwether_source::stats::batch_stamp` holds for column `B` against its composite with the chosen partner `K` (see below), under some time column: one value of `B` touches at least 2 files at the median, `B`'s median row groups per value is at least 2 times the composite's, and `B` concords with time across days at least 0.9 |
//! | `low_selectivity_single_column` | the median value of `C` touches at least [`LOW_SELECTIVITY_MIN_FRACTION`] of all row groups, some id-like `O` that is not a batch stamp makes the composite's p95 at most half of that median, and `C` is not a batch stamp (a batch stamp is not a key at all) |
//! | `scattered_time_column` | a date or timestamp column's overlap depth is at least `SCATTER_MIN_DEPTH` and its sortedness below `SCATTER_MAX_SORTEDNESS` (`bellwether_source::stats`) |
//! | `declared_but_unused_key` | a manifest declares a key spec and a recorded workload exists whose runs never supply the spec's leading column |
//! | `no_discriminating_column` | the dataset has at least 2 row groups and every measured single column (composites are not searched) has a median value touching at least [`NO_DISCRIMINATING_MIN_FRACTION`] of all row groups: the layout, not a missing index, sets the cost of a lookup |
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

use bellwether_source::stats::{
    batch_stamp as batch_stamp_decision, partner_order, BatchStamp, PartnerCandidate,
};
use bellwether_types::{Lint, Result, RowGroupMeta};

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
    lints.extend(no_discriminating(inputs.entries));
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
        if supplied(inputs, lead) {
            continue;
        }
        let name = spec.name();
        let bytes: u64 = manifest
            .segment_bytes
            .iter()
            .filter(|(n, _)| *n == name)
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
}
