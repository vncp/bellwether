//! Per-run and per-template reporting of a [`Replay`].
//!
//! Runs are grouped by template (same plan shape, different keys), and each
//! template reports p50, p95, and max over its runs of keys, files, row
//! groups, and bytes, under the current configuration and under the
//! proposal. A template with two hundred runs and one with three therefore
//! weigh the same in a frontier (`docs/evolve.md` section 6).
//!
//! Every quantile is **nearest-rank**: for `n` sorted values the q-quantile
//! is the value at zero-based index `ceil(q * n) - 1`, the definition
//! `bellwether-bench` uses for `workload.json`, `expected.json`, and
//! `results.json`, so a number means the same thing in all of them.

use std::collections::BTreeMap;

use bellwether_types::{Error, Hash, Replay, Result, RunCost};
use serde::{Deserialize, Serialize};

use crate::workload::Workload;

/// The nearest-rank q-quantile of an ascending slice; 0 when empty.
#[must_use]
pub fn nearest_rank(sorted: &[u64], q: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    // The bench's formula, kept verbatim so both crates agree on every n.
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    let rank = (q * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

/// `[p50, p95, max]` of an ascending slice, zeros when empty.
#[must_use]
pub fn p50_p95_max(sorted: &[u64]) -> [u64; 3] {
    [
        nearest_rank(sorted, 0.5),
        nearest_rank(sorted, 0.95),
        sorted.last().copied().unwrap_or(0),
    ]
}

/// `[p50, p95, max]` over runs of each cost component.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CostQuantiles {
    /// Keys per run.
    pub keys: [u64; 3],
    /// Files per run.
    pub files: [u64; 3],
    /// Row groups per run.
    pub row_groups: [u64; 3],
    /// Bytes per run.
    pub bytes: [u64; 3],
}

impl CostQuantiles {
    /// Quantiles over `costs`.
    #[must_use]
    pub fn of<'a>(costs: impl IntoIterator<Item = &'a RunCost>) -> Self {
        let (mut keys, mut files, mut row_groups, mut bytes) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for c in costs {
            keys.push(c.keys);
            files.push(c.files);
            row_groups.push(c.row_groups);
            bytes.push(c.bytes);
        }
        let q = |mut v: Vec<u64>| {
            v.sort_unstable();
            p50_p95_max(&v)
        };
        Self {
            keys: q(keys),
            files: q(files),
            row_groups: q(row_groups),
            bytes: q(bytes),
        }
    }
}

/// One run under both configurations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRow {
    /// The run.
    pub run_id: String,
    /// Its template.
    pub template: String,
    /// Cost under the current configuration.
    pub current: RunCost,
    /// Cost under the proposal.
    pub proposed: RunCost,
}

/// One template's runs, summarized.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TemplateSummary {
    /// The template.
    pub template: String,
    /// Runs of this template.
    pub runs: u64,
    /// Quantiles under the current configuration.
    pub current: CostQuantiles,
    /// Quantiles under the proposal.
    pub proposed: CostQuantiles,
    /// Whether every proposed cost is exact (false for a layout model).
    pub proposed_exact: bool,
}

/// A [`Replay`] reported per run and per template.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Report {
    /// The workload replayed.
    pub workload: Hash,
    /// The manifest sequence replayed against.
    pub manifest_version: String,
    /// Every run, in workload order.
    pub runs: Vec<RunRow>,
    /// One summary per template, sorted by template.
    pub templates: Vec<TemplateSummary>,
}

/// Reports `replay`, which must have been computed over `workload`: the
/// same hash and exactly its runs, in order.
pub fn report(replay: &Replay, workload: &Workload) -> Result<Report> {
    if replay.workload != workload.hash() {
        return Err(Error::Other(format!(
            "replay is of workload {} but {} was given",
            replay.workload,
            workload.hash()
        )));
    }
    let runs = workload.runs();
    let ids_match = |costs: &[RunCost]| {
        costs.len() == runs.len() && costs.iter().zip(runs).all(|(c, r)| c.run_id == r.run_id)
    };
    if !ids_match(&replay.current) || !ids_match(&replay.proposed) {
        return Err(Error::Other(
            "replay's runs do not match the workload's runs".into(),
        ));
    }
    let rows: Vec<RunRow> = runs
        .iter()
        .zip(replay.current.iter().zip(&replay.proposed))
        .map(|(r, (c, p))| RunRow {
            run_id: r.run_id.clone(),
            template: r.template.clone(),
            current: c.clone(),
            proposed: p.clone(),
        })
        .collect();
    let mut by_template: BTreeMap<&str, Vec<&RunRow>> = BTreeMap::new();
    for row in &rows {
        by_template.entry(&row.template).or_default().push(row);
    }
    let templates = by_template
        .into_iter()
        .map(|(t, rs)| TemplateSummary {
            template: t.to_string(),
            runs: rs.len() as u64,
            current: CostQuantiles::of(rs.iter().map(|r| &r.current)),
            proposed: CostQuantiles::of(rs.iter().map(|r| &r.proposed)),
            proposed_exact: rs.iter().all(|r| r.proposed.exact),
        })
        .collect();
    Ok(Report {
        workload: replay.workload,
        manifest_version: replay.manifest_version.clone(),
        runs: rows,
        templates,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_rank_matches_the_bench_definition() {
        let v: Vec<u64> = (1..=100).collect();
        assert_eq!(nearest_rank(&v, 0.5), 50);
        assert_eq!(nearest_rank(&v, 0.95), 95);
        assert_eq!(nearest_rank(&v, 1.0), 100);
        assert_eq!(nearest_rank(&v, 0.0), 1);
        assert_eq!(nearest_rank(&[], 0.5), 0);
        assert_eq!(p50_p95_max(&[3]), [3, 3, 3]);
        let w: Vec<u64> = (1..=48).collect();
        // ceil(0.95 * 48) = 46.
        assert_eq!(nearest_rank(&w, 0.95), 46);
    }
}
