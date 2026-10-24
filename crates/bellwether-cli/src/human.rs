//! Terminal output: the funnel first, then fixed-width tables. Formatting
//! lives here only; the numbers are the typed result's, printed exactly
//! (with a rounded size beside byte counts for reading).

use std::fmt::Write as _;

use bellwether::spec::label;
use bellwether::types::{EvidenceBody, Hash, Record};
use bellwether::{
    Cost, IndexResult, LinkStatus, LookupKind, PlanResult, ProposeResult, ReceiptResult,
    ReportResult,
};

use crate::Out;

/// `1234567` as `1,234,567`.
pub(crate) fn num(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// A byte count rounded for reading: `1.5 GB`. Decimal units.
#[allow(clippy::cast_precision_loss)]
pub(crate) fn size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "kB", "MB", "GB", "TB"];
    let mut v = bytes as f64;
    let mut u = 0;
    while v >= 1000.0 && u + 1 < UNITS.len() {
        v /= 1000.0;
        u += 1;
    }
    if u == 0 {
        format!("{bytes} B")
    } else {
        format!("{v:.1} {}", UNITS[u])
    }
}

pub(crate) fn short(h: &Hash) -> String {
    h.to_hex()[..12].to_string()
}

pub(crate) fn heading(out: &Out, s: &str) -> String {
    if out.color {
        format!("\x1b[1m{s}\x1b[0m")
    } else {
        s.to_string()
    }
}

/// One funnel row: label, files, row groups, bytes (exact and rounded).
pub(crate) fn funnel_row(s: &mut String, name: &str, c: &Cost, note: &str) {
    let _ = writeln!(
        s,
        "  {name:<34} {:>9} {:>12} {:>18} {:>10}  {note}",
        num(c.files),
        num(c.row_groups),
        num(c.bytes),
        size(c.bytes)
    );
}

pub(crate) fn funnel_header(s: &mut String) {
    let _ = writeln!(
        s,
        "  {:<34} {:>9} {:>12} {:>18} {:>10}",
        "", "files", "row groups", "bytes", ""
    );
}

pub(crate) fn index(r: &IndexResult, out: &Out) -> String {
    let mut s = String::new();
    let what = match r.committed {
        Some(seq) => format!("committed manifest sequence {seq}"),
        None => format!("nothing to do; manifest sequence {} is current", r.sequence),
    };
    let _ = writeln!(s, "{}", heading(out, &format!("index {}: {what}", r.root)));
    let _ = writeln!(
        s,
        "  files: {} new, {} changed, {} removed, {} unchanged; {} row groups read; {} segments written, {} live",
        num(r.files_new),
        num(r.files_changed),
        num(r.files_removed),
        num(r.files_unchanged),
        num(r.row_groups_read),
        num(r.segments_written),
        num(r.segments)
    );
    let _ = writeln!(s, "\n  {:<40} {:>14}", "key spec", "complete at");
    for spec in &r.specs {
        let at = spec
            .complete_at
            .map_or_else(|| "incomplete".to_string(), |n| n.to_string());
        let _ = writeln!(s, "  {:<40} {at:>14}", label(&spec.spec));
    }
    let _ = writeln!(s, "\nmanifest {}", r.manifest);
    s
}

pub(crate) fn plan(r: &PlanResult, out: &Out) -> String {
    let mut s = String::new();
    let what = match &r.lookup {
        LookupKind::Locate => "locate".to_string(),
        LookupKind::Latest => "latest".to_string(),
        LookupKind::Asof { at, ts, ts_column } => format!("asof {at} ({ts_column} = {ts})"),
    };
    let _ = writeln!(
        s,
        "{}",
        heading(
            out,
            &format!(
                "{what}: {} keys on ({}) at manifest sequence {}",
                num(r.keys),
                r.columns.join(", "),
                r.sequence
            )
        )
    );
    let _ = writeln!(
        s,
        "  {:<34} {:>9} {:>12} {:>18}",
        "", "files", "row groups", "bytes"
    );
    let _ = writeln!(
        s,
        "  {:<34} {:>9} {:>12} {:>18}",
        "live",
        num(r.files_total),
        num(r.row_groups_total),
        "-"
    );
    let _ = writeln!(
        s,
        "  {:<34} {:>9} {:>12} {:>18} {:>10}",
        "planned",
        num(r.planned.files),
        num(r.planned.row_groups),
        num(r.planned.bytes),
        size(r.planned.bytes)
    );
    if r.null_tuples > 0 {
        let _ = writeln!(
            s,
            "  {} key rows held a null and match nothing (ADR 014)",
            num(r.null_tuples)
        );
    }
    let specs: Vec<String> = r.specs.iter().map(label).collect();
    let _ = writeln!(
        s,
        "  specs: {}",
        if specs.is_empty() {
            "none (raw scan)".to_string()
        } else {
            specs.join(", ")
        }
    );
    if !r.excluded_incomplete.is_empty() {
        let ex: Vec<String> = r.excluded_incomplete.iter().map(label).collect();
        let _ = writeln!(s, "  not used, incomplete: {}", ex.join(", "));
    }
    if let Record::Evidence(e) = &r.evidence.record {
        if let EvidenceBody::Plan { plan } = &e.body {
            let _ = writeln!(s, "\n  {:<60} {:>11} {:>18}", "file", "row groups", "bytes");
            for entry in plan.entries.iter().take(20) {
                let _ = writeln!(
                    s,
                    "  {:<60} {:>11} {:>18}",
                    tail(&entry.file.path, 60),
                    num(entry.row_groups.len()),
                    num(entry.bytes)
                );
            }
            if plan.entries.len() > 20 {
                let _ = writeln!(s, "  ... {} more files", plan.entries.len() - 20);
            }
        }
    }
    let _ = writeln!(s, "\nevidence {}", r.evidence.hash);
    s
}

fn tail(p: &str, w: usize) -> String {
    let n = p.chars().count();
    if n <= w {
        p.to_string()
    } else {
        let skip = n - (w - 3);
        format!("...{}", p.chars().skip(skip).collect::<String>())
    }
}

pub(crate) fn propose(r: &ProposeResult, _out: &Out) -> String {
    match r {
        ProposeResult::Written { proposal } => format!("proposal {}\n", proposal.hash),
        // The CLI never asks for a dry run; the MCP server does.
        ProposeResult::WouldWrite { proposal } => {
            format!("dry run: would write proposal {}\n", proposal.hash)
        }
        ProposeResult::Refused {
            refusal: bellwether::ProposeRefusal::ReservedProposer { name },
        } => format!(
            "refused: the proposer name {name:?} is reserved for the engine's own candidates; pass \
             your agent or user name with --by\n"
        ),
    }
}

pub(crate) fn receipt(r: &ReceiptResult, out: &Out) -> String {
    let mut s = String::new();
    let state = if r.intact {
        "every link present"
    } else {
        "LINKS FLAGGED"
    };
    let _ = writeln!(
        s,
        "{}",
        heading(out, &format!("receipt {}: {state}", short(&r.receipt)))
    );
    for l in &r.chain {
        let flag = match &l.status {
            LinkStatus::Ok => String::new(),
            LinkStatus::Missing => "  MISSING (not in ledger)".to_string(),
            LinkStatus::WrongKind { found } => {
                format!("  WRONG KIND (is {})", found.with_article())
            }
            LinkStatus::Unreadable { reason } => format!("  UNREADABLE ({reason})"),
        };
        let facts: Vec<String> = l.facts.iter().map(|(k, v)| format!("{k}={v}")).collect();
        let _ = writeln!(
            s,
            "  {}  {:<11}  {:<24}{flag}  {}",
            short(&l.hash),
            l.kind.as_str(),
            l.created_at.as_deref().unwrap_or("-"),
            facts.join("  ")
        );
    }
    s
}

fn quantiles(v: &[f64; 3]) -> String {
    format!("{} / {} / {}", v[0], v[1], v[2])
}

// One linear screen: state, funnel, culprit, recommendation, fanout, lints,
// records.
#[allow(clippy::too_many_lines)]
pub(crate) fn why(r: &bellwether::WhyReport, out: &Out) -> String {
    use bellwether::{CandidateOrigin, CulpritReason};
    let mut s = String::new();
    let state = if r.sequence == 0 {
        "not indexed".to_string()
    } else {
        let specs: Vec<String> = r.current_specs.iter().map(label).collect();
        format!(
            "manifest sequence {}, current keys: {}",
            r.sequence,
            if specs.is_empty() {
                "none".to_string()
            } else {
                specs.join(", ")
            }
        )
    };
    let _ = writeln!(s, "{}", heading(out, &format!("why {}: {state}", r.root)));
    if !r.untracked.is_empty() {
        let _ = writeln!(
            s,
            "  {} file(s) landed after the last index build and are counted in the dataset but not \
             in the costs; run `bellwether index {}` first",
            r.untracked.len(),
            r.root
        );
    }
    let dataset = Cost {
        files: r.dataset.files,
        row_groups: r.dataset.row_groups,
        bytes: r.dataset.bytes,
    };
    let rows = format!("{} rows", num(r.dataset.rows));
    if let (Some(w), Some(current)) = (&r.workload, r.current) {
        let _ = writeln!(
            s,
            "funnel: {} run(s), keys p50/p95/max {}/{}/{}; exact sums of row-group statistics, summed over runs",
            num(w.runs),
            num(w.keys_p50_p95_max[0]),
            num(w.keys_p50_p95_max[1]),
            num(w.keys_p50_p95_max[2])
        );
        funnel_header(&mut s);
        funnel_row(&mut s, "dataset", &dataset, &rows);
        let current_name = if r.current_specs.is_empty() {
            "current (no index)"
        } else {
            "current"
        };
        funnel_row(&mut s, current_name, &current, "");
        for c in &r.candidates {
            let mut notes = Vec::new();
            match c.origin {
                CandidateOrigin::Workload => notes.push("floor: only row groups holding a key"),
                CandidateOrigin::Profile => notes.push("profile candidate"),
            }
            if r.recommended.as_ref().is_some_and(|x| x.spec == c.spec) {
                notes.push("recommended");
            }
            funnel_row(&mut s, &label(&c.spec), &c.proposed, &notes.join(", "));
        }
    } else {
        let _ = writeln!(
            s,
            "funnel: no workload; pass --from <keys.parquet> to replay a run under each candidate key"
        );
        funnel_header(&mut s);
        funnel_row(&mut s, "dataset", &dataset, &rows);
    }
    if let Some(c) = &r.culprit {
        let fan = c.fanout.as_ref().map_or_else(String::new, |f| {
            format!(
                "; one value touches {} files and {} row groups (p50 / p95 / max){}",
                quantiles(&f.files_per_value),
                quantiles(&f.row_groups_per_value),
                if f.sampled { ", sampled" } else { "" }
            )
        });
        let why = match &c.reason {
            CulpritReason::Lint {
                rule, confidence, ..
            } => format!("lint {rule} (confidence {confidence:.2})"),
            CulpritReason::NoIndexServes => {
                "no index serves this lookup, so it reads every row group".to_string()
            }
            CulpritReason::ServingSpec { spec } => format!("the serving key {}", label(spec)),
        };
        let _ = writeln!(s, "\nculprit ({}): {why}{fan}", c.columns.join(", "));
    }
    if let Some(w) = &r.workload {
        match &r.recommended {
            Some(rec) => {
                let _ = writeln!(
                    s,
                    "recommended: {} reads {} bytes ({}) instead of {} ({})",
                    label(&rec.spec),
                    num(rec.proposed.bytes),
                    size(rec.proposed.bytes),
                    num(rec.current.bytes),
                    size(rec.current.bytes)
                );
                let _ = writeln!(
                    s,
                    "  apply it: bellwether fix {} --proposal {} --workload {}",
                    r.root, rec.proposal, w.hash
                );
            }
            None => {
                let _ = writeln!(
                    s,
                    "recommended: none; no candidate reads fewer bytes than the current configuration"
                );
            }
        }
    }
    let _ = writeln!(
        s,
        "\nfanout: files and row groups one value touches, p50 / p95 / max"
    );
    let _ = writeln!(
        s,
        "  {:<36} {:>12}  {:<22} {:<22} sample",
        "key", "distinct", "files/value", "row groups/value"
    );
    for e in &r.profile.fanout {
        let f = &e.fanout;
        let _ = writeln!(
            s,
            "  {:<36} {:>12}  {:<22} {:<22} {}",
            tail(&f.columns.join(", "), 36),
            num(f.distinct),
            quantiles(&f.files_per_value),
            quantiles(&f.row_groups_per_value),
            if f.sampled { "sampled" } else { "exact" }
        );
    }
    if !r.profile.lints.is_empty() {
        let _ = writeln!(s, "\nlints");
        for l in &r.profile.lints {
            let _ = writeln!(
                s,
                "  {} [{}] {:.2}\n    {}",
                l.rule,
                l.columns.join(", "),
                l.confidence,
                l.message
            );
        }
    }
    let _ = writeln!(s, "\nrecords");
    match (&r.evidence.hash, &r.evidence.note) {
        (Some(h), _) => {
            let _ = writeln!(s, "  evidence  {h}");
        }
        (None, note) => {
            let _ = writeln!(
                s,
                "  evidence  {}",
                note.as_deref()
                    .unwrap_or("not recorded; pass a writable location with --index")
            );
        }
    }
    if let Some(w) = &r.workload {
        let _ = writeln!(s, "  workload  {}", w.hash);
    }
    for c in &r.candidates {
        let _ = writeln!(
            s,
            "  proposal  {}  replay {}  {}",
            c.proposal.hash,
            short(&c.replay.hash),
            label(&c.spec)
        );
    }
    s
}

pub(crate) fn report(r: &ReportResult, _out: &Out) -> String {
    format!(
        "wrote {} ({} bytes): {} receipts ({} with realized bytes), {} profile evidence records\n",
        r.path,
        num(r.bytes),
        r.receipts.len(),
        r.realized,
        r.evidence.len()
    )
}
