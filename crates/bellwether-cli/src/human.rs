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

/// `n` with its noun, singular for exactly one: `1 receipt`, `2 receipts`,
/// `1,000 row groups`. Every count a sentence prints goes through it.
pub(crate) fn count(n: u64, one: &str, many: &str) -> String {
    format!("{} {}", num(n), if n == 1 { one } else { many })
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

/// The narrowest label column of the funnel.
const MIN_FUNNEL_LABEL: usize = 34;
/// The narrowest label column of the `index` table.
const MIN_SPEC_LABEL: usize = 40;

/// A label column's width: the longest label, at least `min`, so a long
/// spec label never pushes the numbers out of their columns.
pub(crate) fn label_width<'a>(labels: impl IntoIterator<Item = &'a str>, min: usize) -> usize {
    labels
        .into_iter()
        .map(|l| l.chars().count())
        .max()
        .unwrap_or(0)
        .max(min)
}

/// One funnel row: label, files, row groups, bytes (exact and rounded).
pub(crate) fn funnel_row(s: &mut String, w: usize, name: &str, c: &Cost, note: &str) {
    let _ = writeln!(
        s,
        "  {name:<w$} {:>9} {:>12} {:>18} {:>10}  {note}",
        num(c.files),
        num(c.row_groups),
        num(c.bytes),
        size(c.bytes)
    );
}

pub(crate) fn funnel_header(s: &mut String, w: usize) {
    let _ = writeln!(
        s,
        "  {:<w$} {:>9} {:>12} {:>18} {:>10}",
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
        "  files: {} new, {} changed, {} removed, {} unchanged; {} read; {} written, {} live",
        num(r.files_new),
        num(r.files_changed),
        num(r.files_removed),
        num(r.files_unchanged),
        count(r.row_groups_read, "row group", "row groups"),
        count(r.segments_written, "segment", "segments"),
        num(r.segments)
    );
    let labels: Vec<String> = r.specs.iter().map(|x| label(&x.spec)).collect();
    let w = label_width(
        labels
            .iter()
            .map(String::as_str)
            .chain(r.sidecars.iter().map(|sc| sc.sidecar.as_str())),
        MIN_SPEC_LABEL,
    );
    let heading = if r.sidecars.is_empty() {
        "key spec"
    } else {
        "key spec or sidecar"
    };
    let _ = writeln!(s, "\n  {heading:<w$} {:>14}", "complete at");
    for (spec, name) in r.specs.iter().zip(&labels) {
        let at = spec
            .complete_at
            .map_or_else(|| "incomplete".to_string(), |n| n.to_string());
        let _ = writeln!(s, "  {name:<w$} {at:>14}");
    }
    for sc in &r.sidecars {
        let at = sc
            .complete_at
            .map_or_else(|| "incomplete".to_string(), |n| n.to_string());
        let _ = writeln!(s, "  {:<w$} {at:>14}", sc.sidecar);
    }
    if !r.unknown_segment_kinds.is_empty() {
        let _ = writeln!(
            s,
            "\n  segments of kinds this binary does not know ({}) are kept unchanged and not \
             used; a newer bellwether reads them",
            r.unknown_segment_kinds.join(", ")
        );
    }
    let _ = writeln!(s, "\nmanifest {}", r.manifest);
    s
}

/// What a plan looked up, for its heading.
fn lookup_label(lookup: &LookupKind) -> String {
    match lookup {
        LookupKind::Locate => "locate".to_string(),
        LookupKind::Latest => "latest".to_string(),
        LookupKind::Asof { at, ts, ts_column } => format!("asof {at} ({ts_column} = {ts})"),
        LookupKind::Window {
            from,
            to,
            lo,
            hi,
            ts_column,
        } => format!("locate {ts_column} in [{from}, {to}] ({lo}..={hi})"),
        LookupKind::Lookback {
            at,
            lookback,
            lookback_seconds,
            end,
            lo,
            ts_column,
        } => {
            let span = match lookback_seconds {
                Some(secs) => format!(
                    "{lookback} = {} calendar seconds",
                    num(u64::try_from(*secs).unwrap_or(0))
                ),
                None => format!("{lookback} in {ts_column}'s unit"),
            };
            match (at, end, lo) {
                (Some(at), Some(e), Some(l)) => {
                    format!("lookback {span} on {ts_column}, ending at {at} ({l}..={e})")
                }
                _ => format!(
                    "lookback {span} on {ts_column}, each key's window ending at its own newest \
                     {ts_column}"
                ),
            }
        }
        LookupKind::Bitemporal {
            valid,
            known,
            valid_ts,
            known_ts,
            valid_column,
            known_column,
        } => {
            let bound = |given: &Option<String>, ts: &Option<i64>, column: &str| match (given, ts) {
                (Some(g), Some(t)) => format!("{column} <= {g} ({t})"),
                _ => format!("{column} unbounded"),
            };
            format!(
                "bitemporal {}, {}",
                bound(valid, valid_ts, valid_column),
                bound(known, known_ts, known_column)
            )
        }
    }
}

pub(crate) fn plan(r: &PlanResult, out: &Out) -> String {
    let mut s = String::new();
    let what = lookup_label(&r.lookup);
    let _ = writeln!(
        s,
        "{}",
        heading(
            out,
            &format!(
                "{what}: {} on ({}) at manifest sequence {}",
                count(r.keys, "key", "keys"),
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
            "  {} held a null and match nothing (ADR 014)",
            count(r.null_tuples, "key row", "key rows")
        );
    }
    let specs: Vec<String> = r.specs.iter().map(label).collect();
    let _ = writeln!(s, "  specs: {}", specs_line(&specs, &r.sidecars));
    if !specs.is_empty() && !r.sidecars.is_empty() {
        let _ = writeln!(s, "  sidecars: {}", r.sidecars.join(", "));
    }
    if let Some(c) = &r.path_values {
        let _ = writeln!(
            s,
            "  path values: {c} comes from each file's path, so files outside the window are \
             skipped without reading them"
        );
    }
    if !r.excluded_incomplete.is_empty() {
        let ex: Vec<String> = r.excluded_incomplete.iter().map(label).collect();
        let _ = writeln!(s, "  not used, incomplete: {}", ex.join(", "));
    }
    match (&r.ordered_by, &r.unordered_reason) {
        (Some(c), _) => {
            let _ = writeln!(
                s,
                "  order: by {c} (each row group's minimum, files below in reading order)"
            );
        }
        (None, Some(why)) => {
            let _ = writeln!(s, "  order: unspecified; {why}");
        }
        (None, None) => {}
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
                let _ = writeln!(
                    s,
                    "  ... {} more",
                    count((plan.entries.len() - 20) as u64, "file", "files")
                );
            }
        }
    }
    let _ = writeln!(s, "\nevidence {}", r.evidence.hash);
    s
}

/// The plan's specs line: the specs it used, or, with none, the sidecar
/// that alone planned it (the bitemporal sidecar for a bitemporal lookup),
/// never "raw scan" then.
fn specs_line(specs: &[String], sidecars: &[String]) -> String {
    match (specs.is_empty(), sidecars.is_empty()) {
        (false, _) => specs.join(", "),
        (true, true) => "none (raw scan)".to_string(),
        (true, false) => format!(
            "none; planned by the {} {}",
            if sidecars.len() == 1 {
                "sidecar"
            } else {
                "sidecars"
            },
            sidecars.join(", ")
        ),
    }
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
        ProposeResult::Refused {
            refusal:
                bellwether::ProposeRefusal::SidecarNotProposable {
                    declaration,
                    sidecar,
                    key,
                    command,
                    ..
                },
        } => {
            let alone = key.as_deref().map_or_else(String::new, |k| {
                format!(
                    "; to reach a receipt, propose the key alone with --key {}",
                    bellwether::spec::shell_quote(k)
                )
            });
            let build = command.as_deref().map_or_else(
                || {
                    "build it with `bellwether index` and the source options this engine was \
                     opened with (no command line spells them)"
                        .to_string()
                },
                |c| format!("build it with `{c}`"),
            );
            format!(
                "refused: {declaration} declares the sidecar {sidecar}, and a proposal can carry \
                 only a key spec, so it has no replay, certificate, or receipt yet; {build} (no \
                 receipt){alone}\n"
            )
        }
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
        let mut facts: Vec<String> = l.facts.iter().map(|(k, v)| format!("{k}={v}")).collect();
        if let Some(v) = &l.verdict {
            facts.push(format!("sig={}", v.token()));
        }
        let _ = writeln!(
            s,
            "  {}  {:<11}  {:<24}{flag}  {}",
            short(&l.hash),
            l.kind.as_str(),
            l.created_at.as_deref().unwrap_or("-"),
            facts.join("  ")
        );
    }
    let _ = writeln!(s, "signatures");
    crate::keys_cmds::signature_lines(&mut s, r);
    s
}

fn quantiles(v: &[f64; 3]) -> String {
    format!("{} / {} / {}", v[0], v[1], v[2])
}

// One linear screen: state, funnel, culprit, recommendation, fanout, lints,
// records.
#[allow(clippy::too_many_lines)]
/// `fix_command` is how to call `fix` on this root: the folder, or for a
/// table the root as given with its `--index` (a table root has no default
/// index location).
pub(crate) fn why(r: &bellwether::WhyReport, out: &Out, fix_command: &str) -> String {
    use bellwether::types::KeySpec;
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
    crate::verify_cmds::tables_human(&mut s, &r.tables);
    let basis = match (&r.model, r.exact) {
        (Some(model), false) => format!(
            "{model}: bytes are the logical bytes the engine's aggregates report, not exact \
             (units and rows are exact counts), and no replay is recorded"
        ),
        _ => "exact sums of row-group statistics".to_string(),
    };
    if !r.exact {
        let _ = writeln!(s, "  costs over a table are {basis}");
    }
    let basis = match (&r.model, r.exact) {
        (Some(model), false) => format!("{model} (not exact)"),
        _ => basis,
    };
    if !r.untracked.is_empty() {
        let _ = writeln!(
            s,
            "  {} landed after the last index build and {} counted in the dataset but not in \
             the costs; run `bellwether index {}` first",
            count(r.untracked.len() as u64, "file", "files"),
            if r.untracked.len() == 1 { "is" } else { "are" },
            r.root
        );
    }
    let dataset = Cost {
        files: r.dataset.files,
        row_groups: r.dataset.row_groups,
        bytes: r.dataset.bytes,
    };
    let rows = format!("{} rows", num(r.dataset.rows));
    let kind_label = r.workload.as_ref().and_then(|w| {
        let kind = match &w.lookup {
            Some(LookupKind::Latest) => "latest".to_string(),
            Some(LookupKind::Asof { at, .. }) => format!("asof {at}"),
            // `why` prices locate (in a window or not), latest, or asof.
            _ => return None,
        };
        Some(match &w.lookback {
            Some(l) => format!("{kind}, lookback {}", l.given),
            None => kind,
        })
    });
    if let (Some(w), Some(current)) = (&r.workload, r.current) {
        let kind = kind_label
            .as_ref()
            .map_or_else(String::new, |k| format!(" ({k})"));
        let _ = writeln!(
            s,
            "funnel: {}{kind}, keys p50/p95/max {}/{}/{}; {basis}, summed over runs",
            count(w.runs, "run", "runs"),
            num(w.keys_p50_p95_max[0]),
            num(w.keys_p50_p95_max[1]),
            num(w.keys_p50_p95_max[2])
        );
        for win in &w.windows {
            let _ = writeln!(
                s,
                "  window: rows with {} in {}..={} (the column's unit); every cost below plans \
                 that window",
                win.ts_column, win.lo, win.hi
            );
        }
        let labels: Vec<String> = r.candidates.iter().map(|c| label(&c.spec)).collect();
        let w_col = label_width(labels.iter().map(String::as_str), MIN_FUNNEL_LABEL);
        funnel_header(&mut s, w_col);
        funnel_row(&mut s, w_col, "dataset", &dataset, &rows);
        // Without an index, over Parquet files, the current cost is what the
        // files' own statistics leave (the Replays' `unserved`); the full
        // scan is context above it.
        let current_name = match (r.current_specs.is_empty(), r.full_scan) {
            (true, Some(_)) => "unindexed (min/max pruning)",
            (true, None) => "current (no index)",
            (false, _) => "current",
        };
        if let Some(f) = &r.full_scan {
            funnel_row(
                &mut s,
                w_col,
                "unindexed (full scan)",
                f,
                "context: what a reader that skips nothing reads",
            );
        }
        let current_note = match (&kind_label, r.current_locate) {
            (Some(_), Some(loc)) => format!(
                "a point lookup (locate) would read {}, {} bytes ({})",
                count(loc.row_groups, "row group", "row groups"),
                num(loc.bytes),
                size(loc.bytes)
            ),
            (_, None) if r.full_scan.is_some() => {
                "what the files' own footer statistics skip to without an index; a key must \
                 read less"
                    .to_string()
            }
            _ => String::new(),
        };
        funnel_row(&mut s, w_col, current_name, &current, &current_note);
        for c in &r.candidates {
            let mut notes = Vec::new();
            match (&c.origin, &c.spec, &kind_label) {
                (CandidateOrigin::Workload, KeySpec::Temporal { .. }, Some(k)) => {
                    notes.push(if w.lookback.is_some() {
                        format!(
                            "floor ({k}): only row groups holding a version in each key's window"
                        )
                    } else {
                        format!("floor ({k}): only row groups holding the winning version")
                    });
                }
                (CandidateOrigin::Workload, _, Some(_)) => {
                    notes.push("floor (locate): only row groups holding a key".to_string());
                }
                (CandidateOrigin::Workload, _, None) if w.windows.is_empty() => {
                    notes.push("floor: only row groups holding a key".to_string());
                }
                (CandidateOrigin::Workload, _, None) => {
                    notes.push("exact key on the run's columns".to_string());
                }
                (CandidateOrigin::Window, _, _) => {
                    notes.push(
                        "version chains: only row groups with a version in the window".to_string(),
                    );
                }
                (CandidateOrigin::Profile, _, _) => {
                    notes.push("profile candidate".to_string());
                }
            }
            if let (Some(_), Some(loc)) = (&kind_label, c.locate) {
                notes.push(format!(
                    "locate would read {}, {} bytes ({})",
                    count(loc.row_groups, "row group", "row groups"),
                    num(loc.bytes),
                    size(loc.bytes)
                ));
            }
            if r.recommended.as_ref().is_some_and(|x| x.spec == c.spec) {
                notes.push("recommended".to_string());
            }
            funnel_row(
                &mut s,
                w_col,
                &label(&c.spec),
                &c.proposed,
                &notes.join(", "),
            );
        }
    } else {
        let _ = writeln!(
            s,
            "funnel: no workload; pass --from <keys.parquet> to replay a run under each candidate key"
        );
        funnel_header(&mut s, MIN_FUNNEL_LABEL);
        funnel_row(&mut s, MIN_FUNNEL_LABEL, "dataset", &dataset, &rows);
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
                let span = r.dataset.row_groups * r.workload.as_ref().map_or(1, |w| w.runs);
                match (r.full_scan, r.current) {
                    (Some(_), Some(m)) if m.row_groups < span => format!(
                        "no key index serves this lookup; the files' own min/max statistics \
                         already narrow it to {} of the {} the runs span{}",
                        num(m.row_groups),
                        count(span, "row group", "row groups"),
                        if r.recommended.is_none() {
                            ", and no candidate key reads fewer bytes, so the files' layout \
                             already does what an index would"
                        } else {
                            ""
                        }
                    ),
                    (Some(_), _) => "no index serves this lookup, and the files' min/max \
                                     statistics skip nothing, so it reads every row group"
                        .to_string(),
                    (None, Some(cur)) if cur.row_groups < r.dataset.row_groups => format!(
                        "no key index serves this lookup; only the time window narrows it, to {} \
                         of {}",
                        num(cur.row_groups),
                        count(r.dataset.row_groups, "row group", "row groups")
                    ),
                    _ => "no index serves this lookup, so it reads every row group".to_string(),
                }
            }
            CulpritReason::Layout {
                least_row_groups,
                row_groups,
                lowest_fanout,
            } => {
                let lowest = lowest_fanout.as_ref().map_or_else(String::new, |f| {
                    format!(
                        "; the lowest-fanout column, {}, touches {} row groups per value (p50 / p95 / max)",
                        f.columns.join(", "),
                        quantiles(&f.row_groups_per_value)
                    )
                });
                // A value spread over nearly every row group, or many values
                // each in a few row groups that together cover them all: the
                // sentence must state the one the fanout shows.
                #[allow(clippy::cast_precision_loss)]
                let spread = lowest_fanout.as_ref().is_none_or(|f| {
                    f.row_groups_per_value[0]
                        >= bellwether::LAYOUT_BOUND_FRACTION * f.row_groups_total as f64
                });
                let because = if spread {
                    "because each value is spread over nearly every row group".to_string()
                } else {
                    let col = lowest_fanout
                        .as_ref()
                        .map_or_else(String::new, |f| f.columns.join(", "));
                    format!(
                        "because the requested keys together cover nearly every row group: one value sits in few row groups, but the files are not clustered by {col}"
                    )
                };
                format!(
                    "the layout, not a missing index: every candidate, the floor included, reads at least {} of the {} the runs span, {because}, so no index helps, and only files rewritten clustered by the lookup columns would prune{lowest}",
                    num(*least_row_groups),
                    count(*row_groups, "row group", "row groups")
                )
            }
            CulpritReason::ServingSpec { spec } => format!("the serving key {}", label(spec)),
        };
        // A table read as one unit: every lookup reads all of it whatever
        // the index, which is the cause to state.
        let one_unit = !r.tables.is_empty() && r.tables.iter().all(|t| t.units <= 1);
        if one_unit {
            let _ = writeln!(
                s,
                "\nculprit: the table is read as one unit, so any lookup reads all of it; cut it \
                 into key ranges (range=<column>&units=<n> in the root) to see what an index on \
                 {} would save",
                c.columns.join(", ")
            );
        } else {
            let _ = writeln!(s, "\nculprit ({}): {why}{fan}", c.columns.join(", "));
        }
    }
    if let Some(w) = &r.workload {
        let for_kind = kind_label
            .as_ref()
            .map_or_else(String::new, |k| format!(" (for {k})"));
        let baseline = if r.full_scan.is_some() {
            " with min/max pruning"
        } else {
            ""
        };
        match &r.recommended {
            Some(rec) => {
                let _ = writeln!(
                    s,
                    "recommended{for_kind}: {} reads {} bytes ({}) instead of {} ({}){baseline}",
                    label(&rec.spec),
                    num(rec.proposed.bytes),
                    size(rec.proposed.bytes),
                    num(rec.current.bytes),
                    size(rec.current.bytes)
                );
                if r.tables.is_empty() {
                    let _ = writeln!(
                        s,
                        "  apply it: {fix_command} --proposal {} --workload {}",
                        rec.proposal, w.hash
                    );
                } else {
                    let _ = writeln!(
                        s,
                        "  verify it: {fix_command} --proposal {} --workload {}\n  (attest-only: \
                         fix verifies and writes the certificate; the engine changes nothing in a \
                         table, so it names the statement a human runs)",
                        rec.proposal, w.hash
                    );
                }
            }
            None if r.full_scan.is_some() => {
                let _ = writeln!(
                    s,
                    "recommended{for_kind}: none; no candidate reads fewer bytes than the files' \
                     own min/max statistics already read without an index"
                );
            }
            None => {
                let _ = writeln!(
                    s,
                    "recommended{for_kind}: none; no candidate reads fewer bytes than the current configuration"
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
    if !r.profile.skipped.is_empty() {
        let _ = writeln!(s, "  not candidates:");
        for k in &r.profile.skipped {
            let _ = writeln!(s, "    {}: {}", k.column, k.reason);
        }
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
            "  proposal  {}  replay {}{}  {}",
            c.proposal.hash,
            short(&c.replay.hash),
            if r.exact {
                ""
            } else {
                " (measured, not recorded)"
            },
            label(&c.spec)
        );
    }
    s
}

pub(crate) fn report(r: &ReportResult, _out: &Out) -> String {
    let receipts = r.receipts.len() as u64;
    let signed = r.signed.map_or_else(String::new, |n| {
        format!(
            "; {n} of {} signed (receipt and certificate) by a trusted key",
            count(receipts, "receipt", "receipts")
        )
    });
    format!(
        "wrote {} ({}): {} ({} with realized bytes), {}{signed}\n",
        r.path,
        count(r.bytes, "byte", "bytes"),
        count(receipts, "receipt", "receipts"),
        num(r.realized),
        count(
            r.evidence.len() as u64,
            "profile evidence record",
            "profile evidence records"
        )
    )
}

fn unreadable_line(s: &mut String, unreadable: &[Hash]) {
    if !unreadable.is_empty() {
        let shown: Vec<String> = unreadable.iter().map(short).collect();
        let _ = writeln!(
            s,
            "  {} this build cannot read, skipped: {}",
            count(unreadable.len() as u64, "record", "records"),
            shown.join(" ")
        );
    }
}

pub(crate) fn findings(r: &bellwether::FindingsResult, out: &Out) -> String {
    let mut s = String::new();
    let _ = writeln!(
        s,
        "{}",
        heading(
            out,
            &format!(
                "findings: {}",
                count(r.findings.len() as u64, "root", "roots")
            )
        )
    );
    for f in &r.findings {
        let Record::Evidence(e) = &f.evidence.record else {
            continue;
        };
        let _ = writeln!(
            s,
            "  {}  evidence {}  {}",
            f.root,
            short(&f.evidence.hash),
            e.provenance.created_at
        );
        if let EvidenceBody::Profile { lints, .. } = &e.body {
            for l in lints {
                let _ = writeln!(
                    s,
                    "    {:<30} ({})  {:.2}  {}",
                    l.rule,
                    l.columns.join(", "),
                    l.confidence,
                    l.message
                );
            }
        }
        for h in &f.receipts {
            let _ = writeln!(s, "    receipt {}", short(h));
        }
    }
    unreadable_line(&mut s, &r.unreadable);
    s
}

pub(crate) fn receipts(r: &bellwether::ReceiptsResult, out: &Out) -> String {
    let mut s = String::new();
    let _ = writeln!(
        s,
        "{}",
        heading(out, &format!("receipts: {}", r.receipts.len()))
    );
    for e in &r.receipts {
        let Record::Receipt(rc) = &e.receipt.record else {
            continue;
        };
        let state = if e.intact {
            "every link present"
        } else {
            "LINKS FLAGGED"
        };
        let signature = match (e.signed, &e.not_signed) {
            (None, _) => String::new(),
            (Some(true), _) => "  signed".to_string(),
            (Some(false), Some(bellwether::NotSigned::LinksFlagged { .. })) => {
                "  NOT SIGNED (links flagged)".to_string()
            }
            (Some(false), Some(bellwether::NotSigned::OtherRoot { roots, .. })) => format!(
                "  NOT SIGNED (signed, but for {})",
                bellwether::printable(&roots.join(", "))
            ),
            (Some(false), Some(bellwether::NotSigned::NoRoot)) => {
                "  NOT SIGNED (no evidence names a data root)".to_string()
            }
            (Some(false), _) => {
                let word = |k: bellwether::RecordKind| {
                    e.chain
                        .iter()
                        .find(|l| l.kind == k)
                        .and_then(|l| l.verdict.as_ref())
                        .map_or("not readable", |v| v.word())
                };
                format!(
                    "  NOT SIGNED (receipt {}, certificate {})",
                    word(bellwether::RecordKind::Receipt),
                    word(bellwether::RecordKind::Certificate)
                )
            }
        };
        let _ = writeln!(
            s,
            "  {}  {}  manifest {} -> {}  {state}{signature}",
            short(&e.receipt.hash),
            bellwether::printable(&rc.created_at),
            bellwether::printable(&rc.manifest_before),
            bellwether::printable(&rc.manifest_after)
        );
    }
    unreadable_line(&mut s, &r.unreadable);
    s
}

pub(crate) fn scorecard(r: &bellwether::ScorecardResult, out: &Out) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "{}", heading(out, "scorecard"));
    let _ = writeln!(
        s,
        "  {:<32} {:>9} {:>8} {:>8} {:>8}",
        "rule", "proposals", "passed", "failed", "receipts"
    );
    for row in &r.rules {
        let _ = writeln!(
            s,
            "  {:<32} {:>9} {:>8} {:>8} {:>8}",
            row.rule,
            num(row.proposals),
            num(row.certificates_passed),
            num(row.certificates_failed),
            num(row.receipts)
        );
    }
    unreadable_line(&mut s, &r.unreadable);
    s
}
