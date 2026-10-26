//! Terminal output: the funnel first, then fixed-width tables. Formatting
//! lives here only; the numbers are the typed result's, printed exactly
//! (with a rounded size beside byte counts for reading).

use std::fmt::Write as _;

use bellwether::spec::label;
use bellwether::types::{EvidenceBody, Hash, Record, TextOp, Tokenizer};
use bellwether::{
    Cost, CountResult, IndexResult, LinkStatus, LookupKind, PlanResult, ProposeResult,
    ReceiptResult, ReportResult,
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
/// The widest label the funnel's current row takes.
const CURRENT_LABEL_WIDEST: &str = "current (min/max where no key serves)";
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
    let line = format!(
        "  {name:<w$} {:>9} {:>12} {:>18} {:>10}{kind}  {note}",
        num(c.files),
        num(c.row_groups),
        num(c.bytes),
        size(c.bytes)
    );
    let _ = writeln!(s, "{}", line.trim_end());
}

pub(crate) fn funnel_header(s: &mut String, w: usize) {
    funnel_header_kind(s, w, None);
}

/// The funnel header, with the index kind's column header when the table
/// has one: `Some((width, kind))`.
pub(crate) fn funnel_header_kind(s: &mut String, w: usize, kind: Option<(usize, &str)>) {
    let kind = kind.map_or_else(String::new, |(kw, k)| format!(" {k:>kw$}"));
    let line = format!(
        "  {:<w$} {:>9} {:>12} {:>18} {:>10}{kind}",
        "", "files", "row groups", "bytes", ""
    );
    let _ = writeln!(s, "{}", line.trim_end());
}

/// One line per text key `index` rebuilt because its segments recorded
/// other Unicode tables (B5).
fn text_rebuilt(s: &mut String, rebuilt: &[bellwether::TextRebuilt]) {
    for t in rebuilt {
        let mut was = Vec::new();
        if t.case[0] != t.case[1] {
            was.push(format!("{}, this binary uses {}", t.case[0], t.case[1]));
        }
        if t.words[0] != t.words[1] {
            was.push(format!("{}, this binary uses {}", t.words[0], t.words[1]));
        }
        let _ = writeln!(
            s,
            "\n  rebuilt {}: its segments were built with {}",
            t.spec,
            was.join("; and with ")
        );
    }
}

/// What `index` did with the sidecars of an index written before M5: the
/// folds it made, the sidecars it kept, or (nothing to do) the folds its
/// next build with work makes.
fn sidecar_fold(s: &mut String, r: &IndexResult) {
    for f in &r.sidecars_folded {
        let _ = writeln!(
            s,
            "  folded the pre-M5 sidecar {} into the key spec {} (rebuilt; the sidecar's \
             declaration and segments are dropped)",
            f.sidecar, f.into
        );
    }
    for name in &r.kept_legacy {
        let _ = writeln!(
            s,
            "  kept the pre-M5 sidecar {name} as a sidecar: a key spec on its key columns keeps \
             another time column, so no spec can take it; it is still built and still plans"
        );
    }
    if !r.fold_pending.is_empty() {
        let _ = writeln!(
            s,
            "  this index was written before M5: its next build with work (new or changed files, \
             or a new --key) folds {}",
            r.fold_pending
                .iter()
                .map(|f| format!("{} into {}", f.sidecar, f.into))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
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
    if let Some(from) = r.upgraded_from {
        let _ = writeln!(
            s,
            "  manifest upgraded to storage version {} (this index can no longer be opened by a \
             version {from} binary); {} unchanged files' row-group statistics read from their \
             footers, once",
            bellwether::types::format::STORAGE_VERSION,
            num(r.files_filled)
        );
    }
    sidecar_fold(&mut s, r);
    for spec in &r.specs_repaired {
        let _ = writeln!(
            s,
            "  rebuilt {spec}: two of its segments counted the same row groups (a manifest no \
             build writes), so counts through it were refused; its counts are now read from the \
             data again"
        );
    }
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
    text_rebuilt(&mut s, &r.text_rebuilt);
    join_states(&mut s, &r.joins);
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
        LookupKind::ContainsAny { column } => format!("locate rows whose {column} holds any value"),
        LookupKind::ContainsAll { column } => {
            format!("locate rows whose {column} holds every value")
        }
        LookupKind::Latest => "latest".to_string(),
        LookupKind::SemiJoin {
            right,
            right_columns,
        } => format!("semijoin with {right}.{}", right_columns.join(",")),
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
        LookupKind::Text {
            operator,
            value,
            column,
            ..
        } => format!("locate {}({column}, {value:?})", operator.as_str()),
        LookupKind::TextRuns { columns, operators } => format!(
            "text lookups ({}) on {}, case-folded",
            operators
                .iter()
                .map(|o| o.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            columns.join(", ")
        ),
    }
}

/// A text lookup's plan lines (B5): the keys it went through, the
/// folded argument, and whether the residual is mandatory.
fn text_plan(s: &mut String, lookup: &LookupKind) {
    if let LookupKind::Text {
        operator,
        value,
        folded,
        column,
        serving,
        exact_row_groups,
        unpruned,
    } = lookup
    {
        let keys: Vec<String> = serving.iter().map(label).collect();
        let _ = writeln!(s, "  text keys: {}", keys.join(", "));
        if folded != value {
            let _ = writeln!(s, "  argument case-folded: {folded:?}");
        }
        if let Some(why) = unpruned {
            let _ = writeln!(s, "  not pruned: {why}");
        }
        let residual = format!("{}({column}, {value:?})", operator.as_str());
        if *exact_row_groups {
            let _ = writeln!(
                s,
                "  residual: {residual} on every row read; every planned row group holds a match"
            );
        } else {
            let _ = writeln!(
                s,
                "  residual MANDATORY: {residual} on every row read; the plan is a candidate set, \
                 and a planned row group may hold no match"
            );
        }
    }
}

pub(crate) fn count_result(r: &CountResult, out: &Out) -> String {
    let mut s = String::new();
    let _ = writeln!(
        s,
        "{}",
        heading(
            out,
            &format!(
                "count: {} keys on ({}) at manifest sequence {}",
                num(r.keys),
                r.columns.join(", "),
                r.sequence
            )
        )
    );
    let _ = writeln!(s, "  rows        {:>14}  holding the keys", num(r.rows));
    let _ = writeln!(
        s,
        "  row groups  {:>14} of {}",
        num(r.row_groups),
        num(r.row_groups_total)
    );
    let _ = writeln!(
        s,
        "  files       {:>14} of {}",
        num(r.files),
        num(r.files_total)
    );
    if r.null_tuples > 0 {
        let _ = writeln!(
            s,
            "  {} key rows held a null and match nothing (ADR 014)",
            num(r.null_tuples)
        );
    }
    let _ = writeln!(
        s,
        "  exact, from the per-key counts of {}; no data file was read",
        r.counted_by
    );
    s
}

/// The layout culprit of a list-membership workload (B4), from where the
/// queried values sit.
fn list_layout(least: u64, spanned: u64, values: &bellwether::types::Fanout) -> String {
    let column = values.columns.join(", ");
    let per = quantiles(&values.row_groups_per_value);
    #[allow(clippy::cast_precision_loss)]
    let each_everywhere = values.row_groups_per_value[0]
        >= bellwether::LAYOUT_BOUND_FRACTION * values.row_groups_total as f64;
    let because = if each_everywhere {
        format!(
            "because each queried value found in the data ({}) is in nearly every row group: \
             {per} of the {} row groups per value (p50 / p95 / max)",
            num(values.distinct),
            num(values.row_groups_total)
        )
    } else {
        format!(
            "because the queried values found in the data ({}) each sit in few row groups \
             ({per} of {} per value, p50 / p95 / max) but together cover nearly every row group",
            num(values.distinct),
            num(values.row_groups_total)
        )
    };
    format!(
        "the layout, not a missing index: every candidate, the list key list:{column} included, \
         reads at least {} of the {} row groups the runs span, {because}, so no index helps; \
         only files rewritten so that rows holding the same values sit together would prune",
        num(least),
        num(spanned)
    )
}

/// What a plan's key file holds: list values (B4) or keys.
fn noun(lookup: &LookupKind) -> &'static str {
    match lookup {
        LookupKind::ContainsAny { .. } | LookupKind::ContainsAll { .. } => "values",
        _ => "keys",
    }
}

/// A membership plan's residual (B4), in words.
fn membership_residual(s: &mut String, lookup: &LookupKind) {
    match lookup {
        LookupKind::ContainsAny { column } => {
            let _ = writeln!(
                *s,
                "  residual: keep the rows whose {column} holds one of the values (every planned \
                 row group holds such a row; other rows in it do not match)"
            );
        }
        LookupKind::ContainsAll { column } => {
            let _ = writeln!(
                *s,
                "  residual MANDATORY: keep the rows whose {column} holds every value; a planned \
                 row group holds each value but possibly in different rows"
            );
        }
        _ => {}
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn plan(r: &PlanResult, out: &Out) -> String {
    let mut s = String::new();
    let what = lookup_label(&r.lookup);
    let title = if matches!(r.lookup, LookupKind::Text { .. }) {
        format!("{what}, case-folded, at manifest sequence {}", r.sequence)
    } else {
        format!(
            "{what}: {} on ({}) at manifest sequence {}",
            match noun(&r.lookup) {
                "values" => count(r.keys, "value", "values"),
                _ => count(r.keys, "key", "keys"),
            },
            r.columns.join(", "),
            r.sequence
        )
    };
    let _ = writeln!(s, "{}", heading(out, &title));
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
    if let Some(p) = r.projected_bytes {
        let _ = writeln!(
            s,
            "  {:<34} {:>9} {:>12} {:>18} {:>10}",
            "planned, projected columns",
            "",
            "",
            num(p),
            size(p)
        );
    }
    if r.column_bytes_from_footers > 0 {
        let footers = if r.column_bytes_from_footers == 1 {
            "footer"
        } else {
            "footers"
        };
        let _ = writeln!(
            s,
            "  manifest sequence {} is a storage version 1 manifest, which records no \
             per-column bytes, so the projected bytes were read from {} Parquet {footers}",
            r.sequence,
            num(r.column_bytes_from_footers)
        );
    }
    if let Some(rows) = r.rows {
        let _ = writeln!(
            s,
            "  rows holding the keys: {} (exact, from the index's per-key counts)",
            num(rows)
        );
    }
    if r.null_tuples > 0 {
        let _ = writeln!(
            s,
            "  {} held a null and {} nothing (ADR 014)",
            match noun(&r.lookup) {
                "values" => count(r.null_tuples, "value", "values"),
                _ => count(r.null_tuples, "key row", "key rows"),
            },
            if r.null_tuples == 1 {
                "matches"
            } else {
                "match"
            }
        );
    }
    let specs: Vec<String> = r.specs.iter().map(label).collect();
    let _ = writeln!(s, "  specs: {}", specs_line(&specs, &r.sidecars));
    if !specs.is_empty() && !r.sidecars.is_empty() {
        let _ = writeln!(s, "  sidecars: {}", r.sidecars.join(", "));
    }
    text_plan(&mut s, &r.lookup);
    membership_residual(&mut s, &r.lookup);
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
    let interleaved = match &r.evidence.record {
        Record::Evidence(e) => match &e.body {
            EvidenceBody::Plan { plan } => plan.order.as_ref().map(Vec::len),
            EvidenceBody::Profile { .. } => None,
        },
        _ => None,
    };
    match (&r.ordered_by, &r.unordered_reason, interleaved) {
        (Some(c), _, None) => {
            let _ = writeln!(
                s,
                "  order: by {c} (each row group's minimum, files below in reading order)"
            );
        }
        (Some(c), _, Some(n)) => {
            let _ = writeln!(
                s,
                "  order: by {c} (each row group's minimum); whole files are not in {c} order, \
                 so the {n} row groups are read in the plan's order, across files (`order` in \
                 --json); files below by their earliest row group"
            );
        }
        (None, Some(why), _) => {
            let _ = writeln!(s, "  order: unspecified; {why}");
        }
        (None, None, _) => {}
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
                bellwether::ProposeRefusal::SketchNotProposable {
                    declaration,
                    sketch,
                    command,
                    ..
                },
        } => {
            let build = command.as_deref().map_or_else(
                || {
                    "build it with `bellwether index` and the source options this engine was \
                     opened with (no command line spells them)"
                        .to_string()
                },
                |c| format!("build it with `{c}`"),
            );
            format!(
                "refused: {declaration} declares the sketch {sketch}, which is approximate and \
                 never planned with, so no replay can price it; {build} (no receipt)\n"
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

/// Why no index serving a (non-join) lookup is the culprit, by what the
/// files' statistics and the time window already narrow.
fn no_index_serves(r: &bellwether::WhyReport) -> String {
    let span = r.dataset.row_groups * r.workload.as_ref().map_or(1, |w| w.runs);
    let has_window = r.workload.as_ref().is_some_and(|w| !w.windows.is_empty());
    match (r.unserved.is_some(), r.current) {
        (true, Some(m)) if m.row_groups < span => format!(
            "no key index serves this lookup; the files' own min/max statistics{} already narrow \
             it to {} of the {} the runs span{}",
            if has_window {
                " and the time window"
            } else {
                ""
            },
            num(m.row_groups),
            count(span, "row group", "row groups"),
            if r.recommended.is_none() {
                ", and no candidate key reads fewer bytes, so the files' layout already does \
                 what an index would"
            } else {
                ""
            }
        ),
        (true, _) => "no index serves this lookup, and the files' min/max statistics skip \
                      nothing, so it reads every row group"
            .to_string(),
        (false, Some(cur)) if has_window && cur.row_groups < span => format!(
            "no key index serves this lookup; only the time window narrows it, to {} of {}",
            num(cur.row_groups),
            count(span, "row group", "row groups")
        ),
        _ if r.no_statistics => "no index serves this lookup, and the files carry no min/max \
                                 statistics on its columns, so it reads every row group"
            .to_string(),
        _ => "no index serves this lookup, so it reads every row group".to_string(),
    }
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
    if let Some(t) = &r.twin {
        twin_header(&mut s, t);
    }
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
    let rows = count(r.dataset.rows, "row", "rows");
    let kind_label = r.workload.as_ref().and_then(|w| {
        let kind = match &w.lookup {
            Some(LookupKind::Latest) => "latest".to_string(),
            Some(LookupKind::Asof { at, .. }) => format!("asof {at}"),
            Some(
                k @ (LookupKind::TextRuns { .. }
                | LookupKind::Bitemporal { .. }
                | LookupKind::SemiJoin { .. }),
            ) => lookup_label(k),
            // `why` prices locate (in a window or not), latest, asof,
            // bitemporal, text lookups, or semi-joins.
            _ => return None,
        };
        Some(match &w.lookback {
            Some(l) => format!("{kind}, lookback {}", l.given),
            None => kind,
        })
    });
    // A list-membership workload (B4): its runs' values and the list column.
    let membership = r.workload.as_ref().and_then(|w| match &w.lookup {
        Some(LookupKind::ContainsAny { column }) => Some((column.clone(), "any")),
        Some(LookupKind::ContainsAll { column }) => Some((column.clone(), "every")),
        _ => None,
    });
    if let (Some(w), Some(current)) = (&r.workload, r.current) {
        let text = matches!(w.lookup, Some(LookupKind::TextRuns { .. }));
        let contains_runs = matches!(
            &w.lookup,
            Some(LookupKind::TextRuns { operators, .. }) if operators.contains(&TextOp::Contains)
        );
        // What the runs look up: a list's values (B4), or keys (with the
        // temporal lookup they are priced under), or text values.
        let what = membership.as_ref().map_or_else(
            || {
                kind_label
                    .as_ref()
                    .map_or_else(String::new, |k| format!(" ({k})"))
            },
            |(column, which)| format!(" of rows whose {column} holds {which} value"),
        );
        let _ = writeln!(
            s,
            "funnel: {}{what}, {} p50/p95/max {}/{}/{}; {basis}, summed over runs",
            count(w.runs, "run", "runs"),
            match (text, &membership) {
                (true, _) => "values per run",
                (false, Some(_)) => "values",
                (false, None) => "keys",
            },
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
        if let Some(LookupKind::SemiJoin { right, .. }) = &w.lookup {
            let _ = writeln!(
                s,
                "  each cost below sums both steps of the semi-join: this folder (the dataset \
                 row), then {right} beside it; without a join key each step locates every key"
            );
        }
        let labels: Vec<String> = r.candidates.iter().map(|c| label(&c.spec)).collect();
        // The current row's label (the longest is the min/max one) sets the
        // column too, so every row aligns.
        let w_col = label_width(
            labels
                .iter()
                .map(String::as_str)
                .chain([CURRENT_LABEL_WIDEST]),
            MIN_FUNNEL_LABEL,
        );
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
        // With a kind column the dataset row's cell is blank (the column
        // describes candidates); its note says what the row counts.
        let dataset_note = match &kind_head {
            Some(k) => format!("{rows} (no {k} value: the dataset, not a candidate)"),
            None => rows.clone(),
        };
        funnel_row_kind(&mut s, w_col, "dataset", &dataset, blank, &dataset_note);
        // Over Parquet files with statistics, a run no current key serves is
        // priced as the files' own min/max statistics read it (the Replays'
        // `unserved`); without an index the full scan is context above it.
        let by_stats = r.unserved.is_some();
        let current_name = match (r.current_specs.is_empty(), by_stats) {
            (true, true) => "current: no index (min/max pruning)",
            (true, false) => "current (no index)",
            (false, true) => "current (min/max where no key serves)",
            (false, false) => "current",
        };
        if let Some(f) = &r.full_scan {
            funnel_row_kind(
                &mut s,
                w_col,
                "full scan",
                f,
                blank,
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
            (_, None) if by_stats && r.current_specs.is_empty() => {
                "what the files' own footer statistics skip to without an index; a key must \
                 read less"
                    .to_string()
            }
            (_, None) if by_stats => "lookups no current key serves read what the files' own \
                                      footer statistics skip to"
                .to_string(),
            (_, None) if r.no_statistics => {
                "the files carry no min/max statistics on the lookup columns".to_string()
            }
            _ => String::new(),
        };
        funnel_row_kind(&mut s, w_col, current_name, &current, blank, &current_note);
        for c in &r.candidates {
            let mut notes = Vec::new();
            match (&c.origin, &c.spec, &kind_label) {
                // Priced for count runs, which it answers with no data file
                // read: not the floor the plain exact key is.
                (CandidateOrigin::Workload, KeySpec::Exact { counts: true, .. }, _) => {
                    notes.push("count runs answered from per-key counts, no data read".to_string());
                }
                (
                    CandidateOrigin::Workload,
                    KeySpec::Text {
                        tokenizer: Tokenizer::Words,
                        ..
                    },
                    _,
                ) => {
                    notes.push(if contains_runs {
                        "floor (has_token, has_prefix): only row groups holding a matching word; \
                         it does not serve contains, whose runs read what the current \
                         configuration reads"
                            .to_string()
                    } else {
                        "floor: only row groups holding a matching word".to_string()
                    });
                }
                (CandidateOrigin::Workload, KeySpec::Text { .. }, _) => {
                    notes.push(
                        "trigrams: row groups holding every trigram of a value, a candidate set \
                         (the residual is mandatory)"
                            .to_string(),
                    );
                }
                (CandidateOrigin::Workload, KeySpec::Temporal { .. }, Some(k)) => {
                    notes.push(if w.lookback.is_some() {
                        format!(
                            "floor ({k}): only row groups holding a version in each key's window"
                        )
                    } else {
                        format!("floor ({k}): only row groups holding the winning version")
                    });
                }
                (CandidateOrigin::Workload, KeySpec::List { .. }, _) => {
                    notes.push(match &membership {
                        Some((_, "every")) => {
                            "floor: only row groups holding every value (in some row)".to_string()
                        }
                        _ => "floor: only row groups holding a value".to_string(),
                    });
                }
                (
                    CandidateOrigin::Workload,
                    KeySpec::Bitemporal {
                        valid_column,
                        known_column,
                        ..
                    },
                    _,
                ) => {
                    notes.push(format!(
                        "floor (bitemporal as-of): only row groups holding each key's winning \
                         version, the greatest {valid_column} among versions known by the \
                         {known_column} bound"
                    ));
                }
                (CandidateOrigin::Workload, KeySpec::Join { .. }, _) => {
                    notes.push(
                        "floor (semijoin): only row groups holding a key both datasets hold"
                            .to_string(),
                    );
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
            let cell = kind_cell(c.kind_column.as_ref().map_or("", |k| k.value.as_str()));
            funnel_row_kind(
                &mut s,
                w_col,
                &label(&c.spec),
                &c.proposed,
                cell.as_ref().map(|(w, v)| (*w, v.as_str())),
                &notes.join(", "),
            );
        }
        // What the kind column holds, once per distinct label, under the
        // table rather than among a row's notes.
        let mut legends: Vec<String> = Vec::new();
        for k in r.candidates.iter().filter_map(|c| c.kind_column.as_ref()) {
            let line = format!(
                "  {}: {}{}",
                k.kind,
                k.label,
                if k.exactness.is_exact() {
                    ""
                } else {
                    " (not exact)"
                }
            );
            if !legends.contains(&line) {
                legends.push(line);
            }
        }
        for line in legends {
            let _ = writeln!(s, "{line}");
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
            CulpritReason::NoIndexServes => {
                match r.workload.as_ref().and_then(|w| match &w.lookup {
                    Some(LookupKind::SemiJoin {
                        right,
                        right_columns,
                    }) => Some((right, right_columns)),
                    _ => None,
                }) {
                    Some((right, right_columns)) => format!(
                    "no join key serves this semi-join (join:{}.{}={right}.{} is not declared), \
                     so each step locates every asked key in its own dataset, whether the other \
                     dataset holds it or not",
                    std::path::Path::new(&r.root)
                        .file_name()
                        .map(|n| n.to_string_lossy().into_owned())
                        .unwrap_or_default(),
                    c.columns.join(","),
                    right_columns.join(",")
                ),
                    None => no_index_serves(r),
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
                let because = if lowest_fanout.is_none() && c.columns.len() == 1 {
                    // An expression run: no profiled column holds its
                    // derived values, so the sentence is about them.
                    format!(
                        "because the rows whose {} equals a requested value are spread over nearly every row group",
                        c.columns[0]
                    )
                } else if spread {
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
            CulpritReason::ListLayout {
                least_row_groups,
                row_groups,
                values,
            } => list_layout(*least_row_groups, *row_groups, values),
            CulpritReason::ServingSpec { spec } => format!("the serving key {}", label(spec)),
            CulpritReason::Text { matches } => {
                let parts: Vec<String> = matches
                    .iter()
                    .map(|m| {
                        let runs = format!("{} ({} run(s))", m.operator.as_str(), num(m.runs));
                        if m.served_by.is_empty() {
                            format!("{runs} unserved: no text key serves it, so each reads every row group")
                        } else {
                            let keys: Vec<String> = m.served_by.iter().map(label).collect();
                            format!("{runs} served by {}", keys.join(", "))
                        }
                    })
                    .collect();
                format!("text lookups: {}", parts.join("; "))
            }
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
        let baseline = if r.unserved.is_some() {
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
            None if r.unserved.is_some() => {
                let _ = writeln!(
                    s,
                    "recommended{for_kind}: none; no candidate reads fewer bytes than the current \
                     configuration, where the files' own min/max statistics price the lookups no \
                     current key serves"
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
    if let Some(t) = &r.twin {
        twin_table(&mut s, t);
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
    // The sample and sketch cells are padded for the next column; a table
    // without a sketch column ends at the sample cell, so trim the padding.
    let mut row = |cells: [&str; 6]| {
        let line = format!(
            "  {:<36} {:>12}  {:<22} {:<22} {:<8} {}",
            cells[0], cells[1], cells[2], cells[3], cells[4], cells[5]
        );
        let _ = writeln!(s, "{}", line.trim_end());
    };
    row([
        "key",
        "distinct",
        "files/value",
        "row groups/value",
        "sample",
        if r.sketch.is_empty() { "" } else { "sketch" },
    ]);
    for e in &r.profile.fanout {
        let f = &e.fanout;
        row([
            &tail(&f.columns.join(", "), 36),
            &num(f.distinct),
            &quantiles(&f.files_per_value),
            &quantiles(&f.row_groups_per_value),
            if f.sampled { "sampled" } else { "exact" },
            &sketch_cell(&f.columns),
        ]);
    }
    for i in r.sketch.iter().filter(|i| {
        !r.profile
            .fanout
            .iter()
            .any(|e| e.fanout.columns == i.cardinality.columns)
    }) {
        row([
            &tail(&i.cardinality.columns.join(", "), 36),
            "-",
            "-",
            "-",
            "-",
            &sketch_fanout(i),
        ]);
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

pub(crate) fn engine_workload(r: &bellwether::EngineWorkload, _out: &Out) -> String {
    format!(
        "recorded workload {}: {} keys over ({}) from {} rows of {} (query {}){}\n  diagnose it: \
         bellwether why <folder> --workload {} --twin-of {}\n",
        r.workload,
        num(r.keys),
        r.columns.join(", "),
        num(r.rows),
        r.engine,
        r.tag,
        if r.null_tuples > 0 {
            format!(
                "; {} rows held a null and were dropped (ADR 014)",
                num(r.null_tuples)
            )
        } else {
            String::new()
        },
        r.workload,
        r.engine
    )
}

/// The twin's header (ADR 033): the diagnosis is on a copy, and how fresh
/// the copy is against the engine's table identity.
fn twin_header(s: &mut String, t: &bellwether::TwinReport) {
    let of = &t.of;
    let _ = writeln!(s, "  {}", t.note);
    let behind = i128::from(of.engine_rows) - i128::from(of.lake_rows);
    let fresh = match behind {
        0 => "the same row count".to_string(),
        d if d > 0 => format!(
            "{} rows behind the engine",
            num(u64::try_from(d).unwrap_or(0))
        ),
        d => format!(
            "{} rows ahead of the engine",
            num(u64::try_from(-d).unwrap_or(0))
        ),
    };
    let _ = writeln!(
        s,
        "  freshness: the engine holds {} rows in {} units (identity {}); the lake at sequence \
         {} holds {} rows: {fresh}",
        num(of.engine_rows),
        num(of.units),
        short(&of.identity),
        of.sequence,
        num(of.lake_rows)
    );
}

/// The twin's columns per candidate: fanout, files per key, and the keyset
/// against each gate, then the twin lints.
#[allow(
    clippy::too_many_lines,
    clippy::type_complexity,
    clippy::single_match_else
)]
fn twin_table(s: &mut String, t: &bellwether::TwinReport) {
    let Some(key) = &t.key else {
        let _ = writeln!(
            s,
            "\ntwin: no key sample; pass --from <keys.parquet> or --workload <hash> for fanout, \
             files per key, and gates"
        );
        return;
    };
    let _ = writeln!(
        s,
        "\ntwin: {} sampled keys hold {} rows in {} row groups and {} files of the lake (the \
         prediction fix compares the engine with); the pass read {} rows of the key columns in \
         {} row groups",
        num(key.keyset),
        num(key.rows),
        num(key.row_groups),
        num(key.files),
        num(t.rows_read),
        num(t.row_groups_read)
    );
    let gates: Vec<String> = key
        .gates
        .iter()
        .map(|g| format!("{}<={}", g.name, num(g.limit)))
        .collect();
    let kind = t
        .candidates
        .iter()
        .find_map(|c| c.kind_column.as_ref().map(|k| k.kind.clone()));
    let _ = writeln!(
        s,
        "  {:<34} {:>10}  {:<24} {:<18} {:<}{}",
        "candidate",
        "keyset",
        "rows/key p50/p95/max",
        "files/key",
        if gates.is_empty() {
            String::new()
        } else {
            format!("gates ({})", gates.join(", "))
        },
        kind.as_ref().map_or_else(String::new, |k| format!("  {k}"))
    );
    let mut rows: Vec<(
        String,
        Option<&bellwether::TwinColumns>,
        Option<&str>,
        Vec<String>,
    )> = vec![("the sample's key".to_string(), Some(key), None, Vec::new())];
    for c in &t.candidates {
        rows.push((
            label(&c.spec),
            c.twin.as_ref(),
            c.kind_column.as_ref().map(|k| k.value.as_str()),
            c.spec.columns().to_vec(),
        ));
    }
    for (name, cols, kind_value, spec_columns) in rows {
        match cols {
            Some(c) => {
                let fit: Vec<String> = c
                    .gates
                    .iter()
                    .map(|g| format!("{} {}", g.name, if g.fits { "fits" } else { "OVER" }))
                    .collect();
                let q = |v: &[u64; 3]| format!("{} / {} / {}", num(v[0]), num(v[1]), num(v[2]));
                let _ = writeln!(
                    s,
                    "  {:<34} {:>10}  {:<24} {:<18} {}{}",
                    tail(&name, 34),
                    num(c.keyset),
                    q(&c.rows_per_key),
                    q(&c.files_per_key),
                    fit.join(", "),
                    kind_value.map_or_else(String::new, |k| format!("  {k}"))
                );
            }
            None => {
                let lacks: Vec<&str> = spec_columns
                    .iter()
                    .filter(|c| !key.columns.contains(c))
                    .map(String::as_str)
                    .collect();
                let why = if lacks.is_empty() {
                    "not measured: of a sample over more than four columns, the twin measures \
                     the prefixes and the single columns"
                        .to_string()
                } else {
                    format!("the key sample has no {}", lacks.join(", "))
                };
                let _ = writeln!(s, "  {:<34} {:>10}  {why}", tail(&name, 34), "-");
            }
        }
    }
    if !t.lints.is_empty() {
        let _ = writeln!(s, "  twin lints (advice; they change no plan)");
        for l in &t.lints {
            let _ = writeln!(
                s,
                "    {} [{}]\n      {}",
                l.rule,
                l.columns.join(", "),
                l.message
            );
        }
    }
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

/// Each join key's state (B3), with the command that refreshes it.
fn join_states(s: &mut String, joins: &[bellwether::JoinKeyState]) {
    if joins.is_empty() {
        return;
    }
    let _ = writeln!(
        *s,
        "\n  join keys (right dataset read through its own index):"
    );
    for j in joins {
        let built = if j.refreshed { "rebuilt; " } else { "" };
        let _ = writeln!(*s, "  {built}{}", j.message);
        let _ = writeln!(*s, "    right dataset {}", j.right_root);
        if let Some(next) = &j.next {
            let _ = writeln!(*s, "    run `{next}` to rebuild it");
        }
    }
}

/// `join semijoin`: the two steps and what each reads.
pub(crate) fn semijoin(r: &bellwether::SemiJoinResult, out: &Out) -> String {
    let mut s = String::new();
    let _ = writeln!(
        s,
        "{}",
        heading(
            out,
            &format!(
                "semijoin {}: {} of {} asked keys are on both datasets",
                r.spec.name(),
                num(r.matched),
                num(r.keys)
            )
        )
    );
    if r.null_tuples > 0 {
        let _ = writeln!(
            s,
            "  {} dropped: a key holding a null equals nothing",
            count(r.null_tuples, "input row", "input rows")
        );
    }
    for (n, step) in [(1, &r.left), (2, &r.right)] {
        let _ = writeln!(
            s,
            "  step {n}  {} at manifest sequence {}",
            step.root, step.sequence
        );
        let _ = writeln!(
            s,
            "    reads {} of {} in {}, {}",
            num(step.planned.row_groups),
            count(step.row_groups_total, "row group", "row groups"),
            count(step.planned.files, "file", "files"),
            size(step.planned.bytes)
        );
        if let Some(rows) = step.rows {
            let _ = writeln!(
                s,
                "    {} hold the shared keys (exact)",
                count(rows, "row", "rows")
            );
        }
        let _ = writeln!(
            s,
            "    residual: keep the rows whose ({}) is one of the {} shared keys",
            step.columns.join(", "),
            num(r.matched)
        );
        let _ = writeln!(s, "    plan evidence {}", step.evidence.hash);
    }
    if r.matched < r.keys {
        let other = r.keys - r.matched;
        let (is, them) = if other == 1 {
            ("is", "it")
        } else {
            ("are", "them")
        };
        let _ = writeln!(
            s,
            "  the other {} {is} missing from one dataset or both, so a semi-join keeps no row of \
             {them} and neither step reads {them}",
            count(other, "key", "keys")
        );
    }
    s
}

/// `join overlap`: the key sets' overlap, exact.
#[allow(clippy::cast_precision_loss)]
pub(crate) fn join_overlap(r: &bellwether::JoinOverlapResult, out: &Out) -> String {
    let mut s = String::new();
    let _ = writeln!(
        s,
        "{}",
        heading(
            out,
            &format!(
                "join overlap {}: exact, from the pair index (left sequence {}, right sequence {})",
                r.spec.name(),
                r.sequence,
                r.right_sequence
            )
        )
    );
    let _ = writeln!(s, "  both sides   {:>14}  keys", num(r.both));
    let _ = writeln!(s, "  left only    {:>14}  keys", num(r.left_only));
    let _ = writeln!(s, "  right only   {:>14}  keys", num(r.right_only));
    let mean = |sum: u64, keys: u64| {
        if keys == 0 {
            0.0
        } else {
            sum as f64 / keys as f64
        }
    };
    let _ = writeln!(
        s,
        "  row groups per shared key: right max {}, mean {:.2}; left max {}, mean {:.2}",
        num(r.max_right_row_groups),
        mean(r.sum_right_row_groups, r.both),
        num(r.max_left_row_groups),
        mean(r.sum_left_row_groups, r.both)
    );
    let _ = writeln!(
        s,
        "  right row groups per left key: mean {:.2} (a key on the left only counts 0)",
        mean(r.sum_right_row_groups, r.both + r.left_only)
    );
    if let Some(rows) = r.rows {
        let _ = writeln!(
            s,
            "  rows: left ⋉ right keeps {}, right ⋉ left keeps {}, left ⋈ right is {}",
            num(rows.left),
            num(rows.right),
            num(rows.join)
        );
    }
    let _ = writeln!(s, "  right dataset {}", r.right_root);
    s
}

/// `join verify`: each join key's state.
pub(crate) fn join_verify(r: &bellwether::JoinVerifyResult, out: &Out) -> String {
    let mut s = String::new();
    let verdict = if r.pass {
        "every join key is fresh".to_string()
    } else {
        let stale = r.joins.iter().filter(|j| j.next.is_some()).count() as u64;
        format!("{} stale", count(stale, "join key is", "join keys are"))
    };
    let _ = writeln!(
        s,
        "{}",
        heading(
            out,
            &format!(
                "join verify {} at manifest sequence {}: {verdict}",
                r.root, r.sequence
            )
        )
    );
    for j in &r.joins {
        let _ = writeln!(s, "  {}", j.message);
        let _ = writeln!(s, "    right dataset {}", j.right_root);
        if let Some(next) = &j.next {
            let _ = writeln!(s, "    run `{next}` to rebuild it");
        }
    }
    s
}
