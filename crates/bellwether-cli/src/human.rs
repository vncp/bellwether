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
    funnel_row_kind(s, w, name, c, None, note);
}

/// A funnel row with the index kind's column (`KindColumn`) before the
/// note: `Some((width, value))` when the table has one.
pub(crate) fn funnel_row_kind(
    s: &mut String,
    w: usize,
    name: &str,
    c: &Cost,
    kind: Option<(usize, &str)>,
    note: &str,
) {
    let kind = kind.map_or_else(String::new, |(kw, v)| format!(" {v:>kw$}"));
    let _ = writeln!(
        s,
        "  {name:<w$} {:>9} {:>12} {:>18} {:>10}{kind}  {note}",
        num(c.files),
        num(c.row_groups),
        num(c.bytes),
        size(c.bytes)
    );
}

pub(crate) fn funnel_header(s: &mut String, w: usize) {
    funnel_header_kind(s, w, None);
}

/// The funnel header, with the index kind's column header when the table
/// has one: `Some((width, kind))`.
pub(crate) fn funnel_header_kind(s: &mut String, w: usize, kind: Option<(usize, &str)>) {
    let kind = kind.map_or_else(String::new, |(kw, k)| format!(" {k:>kw$}"));
    let _ = writeln!(
        s,
        "  {:<w$} {:>9} {:>12} {:>18} {:>10}{kind}",
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
    if r.sidecars
        .iter()
        .any(|sc| sc.sidecar.starts_with("sketch:"))
    {
        let _ = writeln!(
            s,
            "\n  sketches are approximate distinct counts and are never used to plan; read them \
             with `bellwether sketch cardinality {}`",
            r.root
        );
    }
    if !r.sketches_rebuilt.is_empty() {
        let _ = writeln!(
            s,
            "\n  rebuilt {} from every row group: its segments were missing from the store or \
             damaged",
            r.sketches_rebuilt.join(", ")
        );
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
    if !r.sidecars.is_empty() {
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
            let why = if sidecar.starts_with("sketch:") {
                format!(
                    "{declaration} declares the sketch {sidecar}, which is approximate and never \
                     planned with, so no replay can price it"
                )
            } else {
                format!(
                    "{declaration} declares the sidecar {sidecar}, and a proposal can carry only \
                     a key spec, so it has no replay, certificate, or receipt yet"
                )
            };
            format!("refused: {why}; {build} (no receipt){alone}\n")
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
        match &kind_label {
            None => {
                let _ = writeln!(
                    s,
                    "funnel: {} run(s), keys p50/p95/max {}/{}/{}; {basis}, summed over runs",
                    num(w.runs),
                    num(w.keys_p50_p95_max[0]),
                    num(w.keys_p50_p95_max[1]),
                    num(w.keys_p50_p95_max[2])
                );
            }
            Some(k) => {
                let _ = writeln!(
                    s,
                    "funnel: {} run(s) ({k}), keys p50/p95/max {}/{}/{}; {basis}, summed over runs",
                    num(w.runs),
                    num(w.keys_p50_p95_max[0]),
                    num(w.keys_p50_p95_max[1]),
                    num(w.keys_p50_p95_max[2])
                );
            }
        }
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
        // One trailing column for the index kind that contributes one
        // (`KindColumn`), headed by the kind's name, before the notes.
        let kind_head = r
            .candidates
            .iter()
            .find_map(|c| c.kind_column.as_ref().map(|k| k.kind.clone()));
        let kind_width = kind_head.as_ref().map(|h| {
            r.candidates
                .iter()
                .filter_map(|c| c.kind_column.as_ref().map(|k| k.value.chars().count()))
                .chain(std::iter::once(h.chars().count()))
                .max()
                .unwrap_or(0)
        });
        let kind_cell = |v: &str| kind_width.map(|kw| (kw, v.to_string()));
        funnel_header_kind(&mut s, w_col, kind_width.zip(kind_head.as_deref()));
        let blank = kind_cell("");
        let blank = blank.as_ref().map(|(w, v)| (*w, v.as_str()));
        funnel_row_kind(&mut s, w_col, "dataset", &dataset, blank, &rows);
        let current_name = if r.current_specs.is_empty() {
            "current (no index)"
        } else {
            "current"
        };
        let current_note = match (&kind_label, r.current_locate) {
            (Some(_), Some(loc)) => format!(
                "a point lookup (locate) would read {} row groups, {} bytes ({})",
                num(loc.row_groups),
                num(loc.bytes),
                size(loc.bytes)
            ),
            _ => String::new(),
        };
        funnel_row_kind(&mut s, w_col, current_name, &current, blank, &current_note);
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
                    "locate would read {} row groups, {} bytes ({})",
                    num(loc.row_groups),
                    num(loc.bytes),
                    size(loc.bytes)
                ));
            }
            if r.recommended.as_ref().is_some_and(|x| x.spec == c.spec) {
                notes.push("recommended".to_string());
            }
            let cell = kind_cell(c.kind_column.as_ref().map_or("", |k| k.value.as_str()));
            if let Some(k) = &c.kind_column {
                notes.push(format!(
                    "{}: {}{}",
                    k.kind,
                    k.label,
                    if k.exactness.is_exact() {
                        ""
                    } else {
                        " (not exact)"
                    }
                ));
            }
            funnel_row_kind(
                &mut s,
                w_col,
                &label(&c.spec),
                &c.proposed,
                cell.as_ref().map(|(w, v)| (*w, v.as_str())),
                &notes.join(", "),
            );
        }
    } else {
        let _ = writeln!(
            s,
            "funnel: no workload; {}pass --from <keys.parquet> to replay a run under each \
             candidate key",
            if r.from_sketches {
                "drop --sketches-only and "
            } else {
                ""
            }
        );
        funnel_header(&mut s, MIN_FUNNEL_LABEL);
        funnel_row(&mut s, MIN_FUNNEL_LABEL, "dataset", &dataset, &rows);
        if r.from_sketches {
            let _ = writeln!(
                s,
                "  dataset from the index's manifest and sketches: rows are the sketch's exact \
                 counts, bytes the file sizes the manifest recorded (no footer read)"
            );
        }
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
            CulpritReason::NoIndexServes => match r.current {
                Some(cur) if cur.row_groups < r.dataset.row_groups => format!(
                    "no key index serves this lookup; only the time window narrows it, to {} of \
                     {} row groups",
                    num(cur.row_groups),
                    num(r.dataset.row_groups)
                ),
                _ => "no index serves this lookup, so it reads every row group".to_string(),
            },
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
                    "the layout, not a missing index: every candidate, the floor included, reads at least {} of the {} row groups the runs span, {because}, so no index helps, and only files rewritten clustered by the lookup columns would prune{lowest}",
                    num(*least_row_groups),
                    num(*row_groups)
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
        match &r.recommended {
            Some(rec) => {
                let _ = writeln!(
                    s,
                    "recommended{for_kind}: {} reads {} bytes ({}) instead of {} ({})",
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
    if r.from_sketches {
        let _ = writeln!(
            s,
            "  not measured: --sketches-only opens no data file, footers included; the sketch \
             column is the diagnosis, approximate"
        );
    }
    if let Some(why_not) = &r.sketch_unavailable {
        let _ = writeln!(s, "  {why_not}");
    }
    let sketch_cell = |columns: &[String]| {
        r.sketch
            .iter()
            .find(|i| i.cardinality.columns == columns)
            .map_or_else(String::new, sketch_fanout)
    };
    let _ = writeln!(
        s,
        "  {:<36} {:>12}  {:<22} {:<22} {:<8} {}",
        "key",
        "distinct",
        "files/value",
        "row groups/value",
        "sample",
        if r.sketch.is_empty() { "" } else { "sketch" }
    );
    for e in &r.profile.fanout {
        let f = &e.fanout;
        let _ = writeln!(
            s,
            "  {:<36} {:>12}  {:<22} {:<22} {:<8} {}",
            tail(&f.columns.join(", "), 36),
            num(f.distinct),
            quantiles(&f.files_per_value),
            quantiles(&f.row_groups_per_value),
            if f.sampled { "sampled" } else { "exact" },
            sketch_cell(&f.columns)
        );
    }
    for i in r.sketch.iter().filter(|i| {
        !r.profile
            .fanout
            .iter()
            .any(|e| e.fanout.columns == i.cardinality.columns)
    }) {
        let _ = writeln!(
            s,
            "  {:<36} {:>12}  {:<22} {:<22} {:<8} {}",
            tail(&i.cardinality.columns.join(", "), 36),
            "-",
            "-",
            "-",
            "-",
            sketch_fanout(i)
        );
    }
    if !r.sketch.is_empty() {
        let _ = writeln!(
            s,
            "  sketch: rows per value over ≈distinct values (HyperLogLog, 99.7% interval), files \
             per value, files repeating a value (≥≈N: a file repeating fewer rows is not \
             detected, so 0 never proves uniqueness); approximate unless marked exact; never \
             used to plan"
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
    let exact_scan = if r.reads.exact_index_segments > 0 {
        format!(
            "; {} exact-index segment(s) scanned for exact distinct counts",
            num(r.reads.exact_index_segments)
        )
    } else {
        String::new()
    };
    let _ = writeln!(
        s,
        "\ndata read: {} files, {} row groups; footers of {} files{exact_scan}",
        num(r.reads.files),
        num(r.reads.row_groups),
        num(r.reads.footers)
    );
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
    let signed = r.signed.map_or_else(String::new, |n| {
        format!(
            "; {n} of {} receipts signed (receipt and certificate) by a trusted key",
            r.receipts.len()
        )
    });
    format!(
        "wrote {} ({} bytes): {} receipts ({} with realized bytes), {} profile evidence \
         records{signed}\n",
        r.path,
        num(r.bytes),
        r.receipts.len(),
        r.realized,
        r.evidence.len()
    )
}

fn unreadable_line(s: &mut String, unreadable: &[Hash]) {
    if !unreadable.is_empty() {
        let shown: Vec<String> = unreadable.iter().map(short).collect();
        let _ = writeln!(
            s,
            "  {} record(s) this build cannot read, skipped: {}",
            unreadable.len(),
            shown.join(" ")
        );
    }
}

pub(crate) fn findings(r: &bellwether::FindingsResult, out: &Out) -> String {
    let mut s = String::new();
    let _ = writeln!(
        s,
        "{}",
        heading(out, &format!("findings: {} root(s)", r.findings.len()))
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

/// One sketched item's `why` cell: `≈138,889 rows/value of ≈72 (±2.4%),
/// ≈139 files/value, 9,998 of 10,000 files repeat ≥≈24 rows`, or exact
/// with an exact distinct count. The last figure is the detection floor:
/// fewer repeats in a file go unseen.
fn sketch_fanout(i: &bellwether::ItemCardinality) -> String {
    let c = &i.cardinality;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let rpv = num(c.rows_per_value.round() as u64);
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let fpv = num(c.files_per_value.round() as u64);
    let distinct = match c.exact_distinct {
        Some(d) => format!("{rpv} rows/value of {} (exact)", num(d)),
        None => format!(
            "≈{rpv} rows/value of ≈{} (±{:.1}%)",
            num(c.distinct),
            300.0 * c.relative_standard_error
        ),
    };
    format!(
        "{distinct}, ≈{fpv} files/value, {} of {} files repeat ≥≈{} rows",
        num(c.files_with_duplicates),
        num(c.files),
        num(c.duplicate_floor.max(1))
    )
}

/// `sketch cardinality`: one block per item.
pub(crate) fn cardinality(r: &bellwether::CardinalityResult, out: &Out) -> String {
    use bellwether::SketchSource;
    let mut s = String::new();
    let state = if r.sequence == 0 {
        "not indexed".to_string()
    } else {
        format!("manifest sequence {}", r.sequence)
    };
    let read = if r.files_read == 0 {
        "from sketches; no data file read".to_string()
    } else {
        format!(
            "{} files ({} row groups) read now for the items no sketch holds",
            num(r.files_read),
            num(r.row_groups_read)
        )
    };
    let _ = writeln!(
        s,
        "{}",
        heading(
            out,
            &format!(
                "sketch cardinality {} ({state}): approximate, {read}",
                r.root
            )
        )
    );
    for i in &r.items {
        let c = &i.cardinality;
        let distinct = match c.exact_distinct {
            Some(d) => format!(
                "{} distinct values (exact, from the exact key on {}; the sketch says ≈{})",
                num(d),
                c.columns.join(", "),
                num(c.distinct)
            ),
            None => format!(
                "≈{} distinct values (99.7%: {}..{}, ±{:.1}%)",
                num(c.distinct),
                num(c.distinct_low),
                num(c.distinct_high),
                300.0 * c.relative_standard_error
            ),
        };
        let _ = writeln!(
            s,
            "  {}: {distinct}; {} rows, {} with a null {}",
            c.item,
            num(c.rows),
            num(c.nulls),
            if c.columns.len() > 1 {
                "leading column (not counted)"
            } else {
                "(not counted)"
            }
        );
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let (rpv, fpv) = (
            c.rows_per_value.round() as u64,
            num(c.files_per_value.round() as u64),
        );
        let _ = writeln!(
            s,
            "    {}{} {} per value; each value in ≈{fpv} files; {} of {} files repeat a value \
             (a file of average size repeating fewer than about {} rows is not detected; a \
             sketch cannot prove uniqueness)",
            if c.rows_per_value_exact { "" } else { "≈" },
            num(rpv),
            if rpv == 1 { "row" } else { "rows" },
            num(c.files_with_duplicates),
            num(c.files),
            num(c.duplicate_floor.max(1)),
        );
        let from = match (&i.source, &i.sketch) {
            (SketchSource::Index, Some(label)) => format!("read from {label}"),
            _ => format!(
                "computed now; `bellwether index {} --key sketch:{}` answers it without reading",
                r.root, c.item
            ),
        };
        let _ = writeln!(s, "    {from}");
    }
    if r.exact_index_segments_read > 0 {
        let _ = writeln!(
            s,
            "  exact distinct counts scanned {} exact-index segment(s) (index, not data)",
            num(r.exact_index_segments_read)
        );
    }
    s
}

/// `sketch overlap`: the estimate, its interval, and the sizes.
pub(crate) fn overlap(r: &bellwether::OverlapResult, out: &Out) -> String {
    use bellwether::types::OverlapMethod;
    let o = &r.overlap;
    let mut s = String::new();
    let how = match (o.method, o.signatures) {
        (OverlapMethod::Minhash, Some(k)) => format!("MinHash, {k} signatures"),
        _ => "HyperLogLog inclusion-exclusion; declare both sketches with minhash= for a \
              tighter bound on small overlaps"
            .to_string(),
    };
    let _ = writeln!(
        s,
        "{}",
        heading(
            out,
            &format!(
                "sketch overlap {} ~ {}: approximate ({how})",
                o.left, o.right
            )
        )
    );
    let _ = writeln!(
        s,
        "  Jaccard ≈{:.4} (99.7%: {:.4}..{:.4}); ≈{} values in both, ≈{} in either (left ≈{}, right ≈{})",
        o.jaccard,
        o.jaccard_low,
        o.jaccard_high,
        num(o.intersection),
        num(o.union_distinct),
        num(o.left_distinct),
        num(o.right_distinct)
    );
    let _ = writeln!(s, "  left  {} in {}", r.left_sketch, r.left_root);
    let _ = writeln!(s, "  right {} in {}", r.right_sketch, r.right_root);
    s
}
