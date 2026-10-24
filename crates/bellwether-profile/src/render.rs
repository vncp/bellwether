//! The one-screen summary: dataset totals, the fanout table, range
//! analysis, lints, and the Evidence's provenance.

use std::fmt::Write as _;
use std::time::Duration;

use bellwether_types::Hash;

use crate::evidence::InputKind;
use crate::lints::{HIGH, MEDIUM};
use crate::ProfileReport;

/// `1234567` as `1,234,567`.
#[must_use]
pub fn thousands(v: u64) -> String {
    let s = v.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

fn count(v: f64) -> String {
    if (v - v.round()).abs() < 1e-9 && v >= 0.0 {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        thousands(v.round() as u64)
    } else {
        format!("{v:.1}")
    }
}

fn triple(q: &[f64; 3]) -> String {
    format!("{} / {} / {}", count(q[0]), count(q[1]), count(q[2]))
}

/// A duration as `fresh`, `12s`, `5m`, `3h`, or `2d`.
#[must_use]
pub fn age(d: Duration) -> String {
    let s = d.as_secs();
    match s {
        0 => "fresh".into(),
        1..=119 => format!("{s}s"),
        120..=7_199 => format!("{}m", s / 60),
        7_200..=172_799 => format!("{}h", s / 3600),
        _ => format!("{}d", s / 86_400),
    }
}

fn level(confidence: f64) -> &'static str {
    if confidence >= HIGH {
        "high"
    } else if confidence >= MEDIUM {
        "medium"
    } else {
        "low"
    }
}

fn short(h: &Hash) -> String {
    h.to_hex()[..12].to_string()
}

/// Renders `report` in about 30 lines. `evidence` is the hash of the
/// Evidence written for it, if any.
#[must_use]
pub fn render(report: &ProfileReport, evidence: Option<&Hash>) -> String {
    let mut out = String::new();
    header(&mut out, report);
    let width = report
        .fanout
        .iter()
        .map(|e| key_label(&e.fanout.columns).len())
        .chain(report.ranges.iter().map(|r| r.column.len()))
        .max()
        .unwrap_or(3)
        .max(3);
    fanout_table(&mut out, report, width);
    ranges_section(&mut out, report, width);
    lints_section(&mut out, report);
    provenance(&mut out, report, evidence);
    out
}

fn header(out: &mut String, report: &ProfileReport) {
    let _ = writeln!(out, "bellwether profile {}", report.root);
    let indexed = match &report.manifest {
        Some(m) => format!("manifest sequence {}", m.sequence),
        None => "not indexed".into(),
    };
    let workload = match &report.workload {
        Some(w) => format!(
            "workload {} runs, keys p50 {} / p95 {} / max {}",
            w.runs,
            thousands(w.sizes[0]),
            thousands(w.sizes[1]),
            thousands(w.sizes[2])
        ),
        None => "no workload".into(),
    };
    let _ = writeln!(
        out,
        "{} files · {} row groups · {} rows · {indexed} · {workload}",
        thousands(report.files_total),
        thousands(report.row_groups_total),
        thousands(report.rows_total)
    );
    let _ = writeln!(out);
}

fn fanout_table(out: &mut String, report: &ProfileReport, width: usize) {
    let _ = writeln!(
        out,
        "fanout: files and row groups one value touches, p50 / p95 / max"
    );
    let _ = writeln!(
        out,
        "  {:<width$}  {:>12}  {:<18}  {:<18}  sample",
        "key", "distinct", "files/value", "row groups/value"
    );
    for e in &report.fanout {
        let f = &e.fanout;
        let mark = if f.sampled { "~" } else { " " };
        // Every sampled key states its own sample rate and quantile rank
        // error; an unsampled one is exact.
        let mut sample = match e.sample.quantile_rank_error {
            Some(eps) if f.sampled => {
                format!("rate {:.2}%, ranks +/-{eps:.3}", e.sample.rate * 100.0)
            }
            _ if f.sampled => "footer distinct, bounded".to_string(),
            _ => "exact".to_string(),
        };
        if e.sample.capped_at_rows {
            let _ = write!(
                sample,
                ", distinct capped at the row count (estimate {})",
                thousands(e.sample.hll_estimate)
            );
        }
        let _ = writeln!(
            out,
            "  {:<width$}  {:>11}{mark}  {:<18}  {:<18}  {sample}",
            key_label(&f.columns),
            thousands(f.distinct),
            triple(&f.files_per_value),
            triple(&f.row_groups_per_value)
        );
    }
    if report.fanout.iter().any(|e| e.fanout.sampled) {
        let _ = writeln!(
            out,
            "  ~ sampled by value: distinct is a HyperLogLog estimate; each quantile lies within \
             its key's rank error of the true one at 99.9%; a sampled max is a lower bound"
        );
    }
    if !report.skipped.is_empty() {
        let skipped: Vec<String> = report
            .skipped
            .iter()
            .map(|s| format!("{} ({})", s.column, s.reason))
            .collect();
        let _ = writeln!(out, "  not candidates: {}", skipped.join("; "));
    }
    let _ = writeln!(out);
}

fn ranges_section(out: &mut String, report: &ProfileReport, width: usize) {
    let _ = writeln!(out, "ranges");
    for r in &report.ranges {
        let sorted = r
            .within_file_sorted_fraction
            .map_or_else(|| "-".to_string(), |s| format!("{:.0}%", s * 100.0));
        let mut line = format!(
            "  {:<width$}  typical overlap {:>3.0}%  sorted within files {sorted:>4}",
            r.column,
            r.typical_overlap * 100.0
        );
        if let Some(t) = report.time_scatter.iter().find(|t| t.column == r.column) {
            let _ = write!(
                line,
                "  overlap depth {:.2}, {:.0}% of consecutive row groups ascend{}{}",
                t.depth,
                t.sortedness * 100.0,
                if t.excluded > 0 {
                    format!(", {} sentinel range(s) excluded", t.excluded)
                } else {
                    String::new()
                },
                if t.scattered { " (scattered)" } else { "" }
            );
        }
        let _ = writeln!(out, "{line}");
    }
    let _ = writeln!(out);
}

fn lints_section(out: &mut String, report: &ProfileReport) {
    let _ = writeln!(out, "lints");
    if report.lints.is_empty() {
        let _ = writeln!(out, "  none");
    }
    for l in &report.lints {
        let _ = writeln!(
            out,
            "  {} [{}] {} {:.2}",
            l.rule,
            l.columns.join(", "),
            level(l.confidence),
            l.confidence
        );
        let _ = writeln!(out, "    {}", l.message);
    }
    let _ = writeln!(out);
}

fn provenance(out: &mut String, report: &ProfileReport, evidence: Option<&Hash>) {
    let inputs: Vec<String> = report
        .inputs
        .iter()
        .map(|i| {
            let kind = match i.kind {
                InputKind::FooterStats => "footer-stats",
                InputKind::FanoutTable => "fanout",
                InputKind::WorkloadSummary => "workload-summary",
                InputKind::Workload => "workload",
                InputKind::Manifest => "manifest",
            };
            format!("{kind} {} {}", &i.hash.to_hex()[..8], age(i.age))
        })
        .collect();
    let head = evidence.map_or_else(
        || "evidence not recorded".to_string(),
        |h| format!("evidence {}", short(h)),
    );
    let _ = writeln!(out, "{head} · inputs {}", inputs.join(", "));
}

fn key_label(columns: &[String]) -> String {
    if columns.len() == 1 {
        columns[0].clone()
    } else {
        format!("({})", columns.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn number_and_age_formats() {
        assert_eq!(thousands(0), "0");
        assert_eq!(thousands(999), "999");
        assert_eq!(thousands(1_000), "1,000");
        assert_eq!(thousands(1_234_567), "1,234,567");
        assert_eq!(count(3.0), "3");
        assert_eq!(count(2.5), "2.5");
        assert_eq!(age(Duration::ZERO), "fresh");
        assert_eq!(age(Duration::from_secs(90)), "90s");
        assert_eq!(age(Duration::from_secs(600)), "10m");
        assert_eq!(age(Duration::from_secs(7_200)), "2h");
        assert_eq!(age(Duration::from_secs(3 * 86_400)), "3d");
        assert_eq!(level(0.9), "high");
        assert_eq!(level(0.6), "medium");
        assert_eq!(level(0.3), "low");
    }
}
