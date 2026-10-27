//! `reconcile` and `fix`: the commands over the verify stage (WP8).

use std::fmt::Write as _;

use bellwether::spec::label;
use bellwether::types::{Coverage, Predicate, ProposalBody, Record};
use bellwether::{
    EngineCompare, EngineRun, FixOutcome, FixRequest, FixResult, KeyRepr, Mismatch,
    ReconcileProblem, ReconcileRequest, ReconcileResult, Refusal, RewriteOutputs, Score,
    TotalColumn,
};
use clap::{Args, Subcommand};

use crate::human::{count, heading, num, short, size};
use crate::{
    done, engine, engine_keyed, hash, Failure, Global, Out, Outcome, RootArg, EXIT_FAILED, EXIT_OK,
    EXIT_REFUSED,
};

/// Commands that write certificates.
#[derive(Subcommand)]
pub(crate) enum VerifyCommand {
    /// Prove two folders, or a table (sql://...) and a folder, agree, by
    /// key, latest version, and exact decimal totals; writes a Certificate. The summary names the first 10
    /// mismatching keys (every one is in --json), prints the exact totals,
    /// and says how many passes the comparison made over each side.
    ///
    /// A side whose index holds agg:<key>@<total>{sum,count} (B7) is
    /// answered from it, reading no row, when only per-key rows and that
    /// one total are compared: every other column ignored, the total too
    /// (--key account --totals amount --ignore amount,memo,...; an ignored
    /// total is still compared, through its exact sum). Otherwise the
    /// summary says why the side was read and what to pass.
    Reconcile(ReconcileArgs),
    /// Score, verify, apply, and attest a proposal (or apply a certificate);
    /// ends with a receipt hash, or a refusal.
    Fix(FixArgs),
}

#[derive(Args)]
pub(crate) struct CompareArgs {
    /// Key columns, comma-separated.
    #[arg(long, value_name = "COLS", value_delimiter = ',')]
    key: Vec<String>,
    /// Compare the newest version per key by this column.
    #[arg(long, value_name = "COL")]
    latest_by: Option<String>,
    /// Columns left out of the row hash, comma-separated.
    #[arg(long, value_name = "COLS", value_delimiter = ',')]
    ignore: Vec<String>,
    /// Columns totalled exactly, comma-separated: a decimal column as
    /// `col`, a float column as `col:dec(S)` (each value rounded half to
    /// even at scale S, then summed exactly; NaN or infinity is an error
    /// naming the key).
    #[arg(long, value_name = "COLS", value_delimiter = ',')]
    totals: Vec<String>,
    /// Columns grouping the totals, comma-separated.
    #[arg(long, value_name = "COLS", value_delimiter = ',')]
    group_by: Vec<String>,
}

#[derive(Args)]
pub(crate) struct ReconcileArgs {
    /// The left folder (its index and ledger are used), or a table:
    /// sql://<engine>/<schema>.<table>?db=<connection> (needs --index).
    left: String,
    /// The right folder, or a table (sql://...).
    right: String,
    #[command(flatten)]
    compare: CompareArgs,
    /// Compare a deterministic sample of this fraction of keys.
    #[arg(long, value_name = "FRACTION")]
    sample: Option<f64>,
    /// Sampling seed.
    #[arg(long, default_value_t = 0, requires = "sample")]
    seed: u64,
    /// Bytes the comparison state may occupy: bytes, or a whole number with
    /// a binary unit, K, M, G, or T (512M, 8G) [default: 256M]. The
    /// comparison reads both sides once per pass and makes as many passes
    /// as it needs to stay inside this budget; a larger budget means fewer
    /// passes.
    #[arg(long, value_name = "BYTES", value_parser = parse_bytes)]
    memory_budget: Option<u64>,
}

/// How many mismatching keys the human summary names; `--json` carries
/// every example the verifier kept (`outcome.mismatches`).
pub(crate) const HUMAN_MISMATCHES: usize = 10;
/// How many group totals the human summary prints; `--json` carries all of
/// them (`outcome.totals`).
pub(crate) const HUMAN_TOTALS: usize = 20;

#[derive(Args)]
pub(crate) struct FixArgs {
    #[command(flatten)]
    root: RootArg,
    /// The proposal to score, verify, apply, and attest.
    #[arg(long, value_name = "HASH", required_unless_present = "certificate")]
    proposal: Option<String>,
    /// Apply and attest this certificate instead (the apply tier).
    #[arg(long, value_name = "HASH", conflicts_with_all = ["proposal", "workload", "verify_only"])]
    certificate: Option<String>,
    /// The recorded workload a key proposal is scored on (why prints it).
    #[arg(long, value_name = "HASH")]
    workload: Option<String>,
    /// Score and verify only; apply nothing.
    #[arg(long)]
    verify_only: bool,
    /// Go on with a key proposal whose replay predicts no saving on the
    /// workload; without it fix refuses with `nothing_to_gain` (exit 3).
    #[arg(long)]
    allow_no_gain: bool,
    /// A rewrite's output before the rewrite (a folder or a sql:// table).
    #[arg(long, value_name = "DIR", requires = "after_output")]
    before_output: Option<String>,
    /// A rewrite's output after it.
    #[arg(long, value_name = "DIR", requires = "before_output")]
    after_output: Option<String>,
    #[command(flatten)]
    compare: CompareArgs,
    /// Sign the receipt and the certificate this run writes (a verify-only
    /// run: its passing certificate) with this key: the seed file
    /// `bellwether keys generate` wrote, readable by its owner only. A
    /// path, never key bytes.
    #[arg(long, value_name = "FILE", env = "BELLWETHER_SIGNING_KEY")]
    signing_key: Option<std::path::PathBuf>,
}

pub(crate) fn run(g: &Global, out: &Out, cmd: &VerifyCommand) -> Result<Outcome, Failure> {
    match cmd {
        VerifyCommand::Reconcile(a) => {
            let request = ReconcileRequest {
                key: a.compare.key.clone(),
                latest_by: a.compare.latest_by.clone(),
                ignore: a.compare.ignore.clone(),
                totals: a.compare.totals.clone(),
                group_by: a.compare.group_by.clone(),
                sample: a.sample.map(|f| (f, a.seed)),
                memory_budget: a.memory_budget,
            };
            let e = engine(g, &a.left)?;
            if let Some(p) = e.reconcile_problems(&request)?.first() {
                return Err(Failure::Usage(problem(p)));
            }
            let r = e.reconcile(&a.right, &request)?;
            let code = if r.outcome.pass { EXIT_OK } else { EXIT_FAILED };
            done(&r, reconcile_human(&r, out), code)
        }
        VerifyCommand::Fix(a) => {
            let rewrite = match (&a.before_output, &a.after_output) {
                (Some(before), Some(after)) => Some(RewriteOutputs {
                    before: before.clone(),
                    after: after.clone(),
                    key: a.compare.key.clone(),
                    latest_by: a.compare.latest_by.clone(),
                    ignore: a.compare.ignore.clone(),
                    totals: a.compare.totals.clone(),
                    group_by: a.compare.group_by.clone(),
                }),
                _ => None,
            };
            let request = FixRequest {
                proposal: a
                    .proposal
                    .as_deref()
                    .map(|p| hash(p, "proposal"))
                    .transpose()?,
                certificate: a
                    .certificate
                    .as_deref()
                    .map(|c| hash(c, "certificate"))
                    .transpose()?,
                workload: a
                    .workload
                    .as_deref()
                    .map(|w| hash(w, "workload"))
                    .transpose()?,
                engine: (a.before_output.is_none()
                    && (a.compare.latest_by.is_some()
                        || !a.compare.ignore.is_empty()
                        || !a.compare.totals.is_empty()
                        || !a.compare.group_by.is_empty()))
                .then(|| EngineCompare {
                    latest_by: a.compare.latest_by.clone(),
                    ignore: a.compare.ignore.clone(),
                    totals: a.compare.totals.clone(),
                    group_by: a.compare.group_by.clone(),
                }),
                rewrite,
                verify_only: a.verify_only,
                allow_no_gain: a.allow_no_gain,
            };
            // The key is read before anything is scored: a key that cannot
            // be used is a usage error, not a receipt without a signature.
            let key = crate::keys_cmds::signing_key(g, &a.root.root, a.signing_key.as_deref())?;
            let e = engine_keyed(g, &a.root.root, key, None)?;
            let table = e.is_table();
            let r = e.fix(&request)?;
            let code = match &r.outcome {
                FixOutcome::Applied { .. }
                | FixOutcome::Attested { .. }
                | FixOutcome::Verified
                | FixOutcome::WouldApply { .. } => EXIT_OK,
                FixOutcome::Refused {
                    refusal: Refusal::CertificateFailed { .. },
                } => EXIT_FAILED,
                FixOutcome::Refused { .. } => EXIT_REFUSED,
            };
            done(&r, fix_human(&r, out, table), code)
        }
    }
}

/// One line saying what to change in the request.
fn problem(p: &ReconcileProblem) -> String {
    match p {
        ReconcileProblem::TotalNotDecimal {
            column,
            data_type,
            float: true,
        } => format!(
            "--totals {column:?} is {data_type}, and totals are exact; give it a decimal scale: \
             --totals {column}:dec(4) rounds each value half to even at 4 decimal places and sums \
             exactly (pick the scale the data is quoted at)"
        ),
        ReconcileProblem::TotalNotDecimal {
            column, data_type, ..
        } => format!(
            "--totals needs a decimal column, or a float column as {column}:dec(<scale>), and \
             {column:?} is {data_type}; write the column as a decimal first"
        ),
        ReconcileProblem::QuantizeNotFloat {
            column,
            data_type,
            scale,
        } => format!(
            "--totals {column}:dec({scale}) quantizes a float column, and {column:?} is \
             {data_type}; pass --totals {column} for a decimal column, or write it as a decimal \
             first"
        ),
        ReconcileProblem::TotalSpecInvalid { reason, .. } => reason.clone(),
        ReconcileProblem::TotalNotFound { column } => {
            format!("--totals names {column:?}, which is not a column of the left folder")
        }
        ReconcileProblem::ZeroMemoryBudget => {
            "--memory-budget must be positive, for example 512M or 8G".to_string()
        }
    }
}

/// `--memory-budget`: bytes, or a whole number with a binary unit, `K`,
/// `M`, `G`, or `T`, optionally spaced and suffixed `B` or `iB`, in any
/// case (`8G`, `8 GB`, `512MiB`): every unit is binary, so `8G` is
/// 8 × 2^30 bytes.
pub(crate) fn parse_bytes(given: &str) -> Result<u64, String> {
    let g = given.trim();
    let split = g.find(|c: char| !c.is_ascii_digit()).unwrap_or(g.len());
    let (digits, unit) = g.split_at(split);
    let unit = unit.trim().to_ascii_uppercase();
    let unit = unit
        .strip_suffix("IB")
        .or_else(|| unit.strip_suffix('B'))
        .unwrap_or(&unit);
    let shift = match unit {
        "" => 0,
        "K" => 10,
        "M" => 20,
        "G" => 30,
        "T" => 40,
        _ => {
            return Err(format!(
                "{given:?} is not a byte count; write bytes (268435456) or a whole number with a \
                 binary unit, K, M, G, or T (512M, 8G; 1G is 2^30 bytes)"
            ))
        }
    };
    let n: u64 = digits.parse().map_err(|_| {
        format!(
            "{given:?} is not a byte count; write bytes (268435456) or a whole number with a \
             binary unit, K, M, G, or T (512M, 8G)"
        )
    })?;
    n.checked_mul(1u64 << shift)
        .ok_or_else(|| format!("{given:?} is more bytes than fit in 64 bits"))
}

/// A key as a tuple of its rendered elements.
fn key(k: &KeyRepr) -> String {
    let v: Vec<&str> = k
        .values
        .iter()
        .map(|v| v.as_deref().unwrap_or("null"))
        .collect();
    format!("({})", v.join(", "))
}

/// Why `n` keys could not be compared, and what to do (the facade's
/// wording, shared with MCP).
fn uncomparable_cause(n: u64) -> String {
    bellwether::uncomparable_cause(n)
}

fn coverage(c: &Coverage) -> String {
    match c {
        Coverage::Full => "full".into(),
        Coverage::Workload { keys, .. } => format!("workload, {}", count(*keys, "key", "keys")),
        Coverage::Sampled { keys, fraction } => {
            format!("sampled, {} ({fraction})", count(*keys, "key", "keys"))
        }
        Coverage::StaticProof { identity } => format!("static proof {identity}"),
    }
}

/// What a row filter keeps, in words: one phrase per kind of test.
#[allow(clippy::too_many_lines)] // one phrase per kind of test
fn rows_asked(filter: &[Predicate]) -> String {
    #[allow(clippy::too_many_lines)] // one phrase per kind of test
    fn walk(p: &Predicate, out: &mut Vec<String>) {
        let phrase = match p {
            Predicate::Between { column, .. } => format!("{column} within the lookup's bounds"),
            Predicate::NotOlderThan { ts_column, .. } => {
                format!("{ts_column} within each key's lookback from its newest time in the data")
            }
            Predicate::LatestPerKey { ts_column, .. } => {
                format!("the newest version by {ts_column}")
            }
            Predicate::In { .. } => "the workload's keys".to_string(),
            Predicate::SemiJoin { columns, .. } => {
                format!("rows whose {} is a semi-join key", columns.join(", "))
            }
            Predicate::Join { left, .. } => format!("rows joined on {}", left.join(", ")),
            Predicate::WindowPartition { columns } => {
                format!("whole partitions of {}", columns.join(", "))
            }
            Predicate::Text {
                column,
                operator,
                values,
            } => match values.as_slice() {
                [one] => format!("{}({column}, {one:?}), case-folded", operator.as_str()),
                many => format!(
                    "{}({column}, any of {} values), case-folded",
                    operator.as_str(),
                    many.len()
                ),
            },
            Predicate::ExprIn { expr, .. } => format!("the workload's keys of {expr}"),
            Predicate::ContainsAny { column, .. } => {
                format!("{column} holding any of the workload's values")
            }
            Predicate::ContainsAll { column, .. } => {
                format!("{column} holding every one of the workload's values")
            }
            Predicate::Live { column, .. } => {
                format!("live rows only: {column} not null and not the tombstone value")
            }
            Predicate::And { all } => {
                let clocks: Vec<&str> = all
                    .iter()
                    .filter_map(|q| match q {
                        Predicate::NotOlderThan { ts_column, .. } => Some(ts_column.as_str()),
                        _ => None,
                    })
                    .collect();
                // A live run (B8): each key's newest version first, then
                // its live rows, in that order (never the newest live row).
                let live = all.iter().find_map(|q| match q {
                    Predicate::Live { column, .. } => Some(column.as_str()),
                    _ => None,
                });
                if let Some(column) = live {
                    for q in all.iter().filter(|q| matches!(q, Predicate::In { .. })) {
                        walk(q, out);
                    }
                    let newest = match clocks.as_slice() {
                        [valid, known] => format!(
                            "each key's as-of version (the rows at its greatest {valid} within \
                             the bounds among those known by then, at the greatest {known}), \
                             found in the data"
                        ),
                        // An asof run bounds the time by its instant
                        // (`Between`); a latest run has no instant.
                        [ts, ..]
                            if all.iter().any(
                                |q| matches!(q, Predicate::Between { column, .. } if column == ts),
                            ) =>
                        {
                            format!(
                                "each key's newest version by {ts} at or before the instant, \
                                 found in the data"
                            )
                        }
                        [ts, ..] => format!("each key's newest version by {ts}, found in the data"),
                        [] => "each key's newest version".to_string(),
                    };
                    let phrase = format!(
                        "{newest}, then its live rows only ({column} not null and not the \
                         tombstone value)"
                    );
                    if !out.contains(&phrase) {
                        out.push(phrase);
                    }
                    return;
                }
                // A bitemporal run's winners: a per-key bound on each of
                // its two clocks, read from the data, not a lookback.
                if let [valid, known] = clocks.as_slice() {
                    for q in all
                        .iter()
                        .filter(|q| !matches!(q, Predicate::NotOlderThan { .. }))
                    {
                        walk(q, out);
                    }
                    let phrase = format!(
                        "each key's as-of version: the rows at its greatest {valid} within the \
                         bounds among those known by then, at the greatest {known}, found in the \
                         data"
                    );
                    if !out.contains(&phrase) {
                        out.push(phrase);
                    }
                    return;
                }
                for q in all {
                    walk(q, out);
                }
                return;
            }
        };
        if !out.contains(&phrase) {
            out.push(phrase);
        }
    }
    let mut out = Vec::new();
    for p in filter {
        walk(p, &mut out);
    }
    out.join("; ")
}

/// The exact totals, with one line per quantized float column saying how
/// its values were rounded.
fn totals_human(s: &mut String, o: &bellwether::Reconciliation) {
    if o.totals.is_empty() {
        return;
    }
    let _ = writeln!(s, "\n  exact totals, left and right");
    let mut quantized: Vec<TotalColumn> = Vec::new();
    for t in &o.totals {
        if let Ok(
            q @ TotalColumn {
                quantize: Some(_), ..
            },
        ) = TotalColumn::parse(&t.column)
        {
            if !quantized.contains(&q) {
                quantized.push(q);
            }
        }
    }
    for q in &quantized {
        let _ = writeln!(
            s,
            "    {}: each float value's exact binary value rounded half to even at {} decimal places, then summed exactly",
            q.label(),
            q.quantize.unwrap_or_default()
        );
    }
    for t in o.totals.iter().take(HUMAN_TOTALS) {
        let g: Vec<String> = t
            .group
            .values
            .iter()
            .map(|(c, v)| format!("{c}={}", v.as_deref().unwrap_or("null")))
            .collect();
        let (l, rt) = (t.left.render(), t.right.render());
        let verdict = if l == rt { "equal" } else { "DIFFER" };
        let _ = writeln!(
            s,
            "    {:<16} {:<28} {l:>24} {rt:>24}  {verdict}",
            t.column,
            if g.is_empty() {
                "all rows".to_string()
            } else {
                g.join(", ")
            }
        );
    }
    if o.totals.len() > HUMAN_TOTALS {
        let _ = writeln!(
            s,
            "    ... {} more in --json",
            count((o.totals.len() - HUMAN_TOTALS) as u64, "group", "groups")
        );
    }
}

#[allow(clippy::many_single_char_names, clippy::too_many_lines)] // one section per outcome part
fn reconcile_human(r: &ReconcileResult, out: &Out) -> String {
    let mut s = String::new();
    let o = &r.outcome;
    let verdict = if o.pass { "PASS" } else { "FAIL" };
    let _ = writeln!(
        s,
        "{}",
        heading(
            out,
            &format!("reconcile: {verdict}, coverage {}", coverage(&o.coverage))
        )
    );
    let c = &o.counts;
    // The typed result counts a key that failed to compare in `mismatched`
    // and again in `no_comparable_version`; shown apart, as the certificate
    // counts them (`mismatched`, `uncomparable`).
    let uncomparable = c.no_comparable_version;
    let _ = writeln!(
        s,
        "  {:<22} {:>14}\n  {:<22} {:>14}\n  {:<22} {:>14}\n  {:<22} {:>14}\n  {:<22} {:>14}\n  {:<22} {:>14}\n  {:<22} {:>14}",
        "keys compared", num(c.keys_compared),
        "matched", num(c.matched),
        "mismatched", num(c.mismatched.saturating_sub(uncomparable)),
        "uncomparable", num(uncomparable),
        "missing on the left", num(c.missing_left),
        "missing on the right", num(c.missing_right),
        "totals that differ", num(c.totals_differ)
    );
    if uncomparable > 0 {
        let _ = writeln!(s, "  {}", uncomparable_cause(uncomparable));
    }
    if !o.ignored_columns.is_empty() {
        let _ = writeln!(s, "  not compared: {}", o.ignored_columns.join(", "));
    }
    let per_key: Vec<(&str, &KeyRepr)> = o
        .mismatches
        .iter()
        .filter_map(|m| match m {
            Mismatch::MissingOnLeft { key, .. } => Some(("missing on the left", key)),
            Mismatch::MissingOnRight { key, .. } => Some(("missing on the right", key)),
            Mismatch::RowHashDiffers { key, .. } => Some(("rows differ", key)),
            Mismatch::NoComparableVersion { key, .. } => Some(("no comparable version", key)),
            Mismatch::TotalDiffers { .. } => None,
        })
        .collect();
    let per_key_total = c.mismatched + c.missing_left + c.missing_right;
    if per_key_total > 0 {
        let shown = per_key.len().min(HUMAN_MISMATCHES);
        let _ = writeln!(
            s,
            "\n  first {shown} of {} ({}): every kept example is in --json",
            count(per_key_total, "mismatching key", "mismatching keys"),
            o.key_columns.join(", ")
        );
        for (what, k) in per_key.iter().take(HUMAN_MISMATCHES) {
            let _ = writeln!(s, "    {what:<22} {}", key(k));
        }
    }
    totals_human(&mut s, o);
    let m = &o.memory;
    let _ = writeln!(
        s,
        "\n  read {} over each side under a {}-byte state budget ({}): {} read on the left, {} on the right",
        count(u64::from(m.passes), "pass", "passes"),
        num(m.budget_bytes),
        count(u64::from(m.restarts), "restart", "restarts"),
        count(m.rows_read[0], "row", "rows"),
        num(m.rows_read[1])
    );
    if m.passes > 1 {
        let _ = writeln!(
            s,
            "  each pass reads both sides again; a larger --memory-budget makes fewer passes"
        );
    }
    for a in o.agg.iter().filter(|a| a.checked.is_none()) {
        let _ = writeln!(
            s,
            "  the {} side's per-key rows and totals came from its index's {}: {} aggregated, \
             none read",
            a.side,
            a.specs.join(", "),
            count(a.rows, "row", "rows")
        );
    }
    // A side whose agg key could have answered and did not: why, and what
    // makes it answer.
    for n in &o.agg_not_used {
        let _ = writeln!(
            s,
            "  the {} side was read, not answered from its index's {}: {}",
            n.side,
            n.spec,
            n.causes.join("; ")
        );
        if let Some(next) = &n.next {
            let _ = writeln!(s, "    next: {next}");
        }
    }
    tables_human(&mut s, &r.tables);
    if !r.lints.is_empty() {
        let _ = writeln!(s, "\n  lints:");
        for l in &r.lints {
            let _ = writeln!(
                s,
                "    {} [{}]\n      {}",
                l.rule,
                l.columns.join(", "),
                l.message
            );
        }
    }
    let _ = writeln!(s, "\ncertificate {}", r.certificate.hash);
    s
}

/// One line per table a run read (ADR 027): its units and rows, and the
/// snapshot it held or the identity re-read that stood in for one.
pub(crate) fn tables_human(s: &mut String, tables: &[bellwether::TableRun]) {
    for t in tables {
        let held = match &t.snapshot {
            Some(snap) => format!("read inside {snap}"),
            None if t.rechecked && t.identity_by == "statistics checksum" => {
                "no snapshot held: every unit's identity (a statistics checksum) re-read at the \
                 end, unchanged; an in-place update that keeps every column's bounds, null \
                 count, and bytes is not seen, so name a version column (version=<column>) or \
                 read inside a snapshot"
                    .into()
            }
            None if t.rechecked => format!(
                "no snapshot held: every unit's identity ({}) re-read at the end, unchanged",
                t.identity_by
            ),
            None => "no snapshot held".into(),
        };
        let _ = writeln!(
            s,
            "  table {}: {} in {} ({}), {held}",
            t.root,
            count(t.rows, "row", "rows"),
            count(t.units, "unit", "units"),
            t.units_by
        );
    }
}

/// What `fix` signed, or that `what` is unsigned.
fn signed_line(s: &mut String, r: &FixResult, what: &str) {
    let Some(first) = r.signatures.first() else {
        let _ = writeln!(
            s,
            "            {what} unsigned (no --signing-key; `bellwether keys generate --out \
             <path>` makes a key)"
        );
        return;
    };
    let kinds: Vec<&str> = r.signatures.iter().map(|x| x.kind.as_str()).collect();
    let _ = writeln!(
        s,
        "            signed the {} with key {} at {}",
        kinds.join(" and the "),
        short(&first.key_id),
        first.signed_at
    );
}

// One linear screen, a line per stage.
#[allow(clippy::too_many_lines)]
fn fix_human(r: &FixResult, out: &Out, table: bool) -> String {
    let mut s = String::new();
    let what = match &r.proposal.record {
        Record::Proposal(p) => match &p.body {
            ProposalBody::Key { spec, sidecars } if sidecars.is_empty() => {
                format!("key {}", label(spec))
            }
            // Every spec the proposal applies, in its one commit.
            ProposalBody::Key { spec, sidecars } => format!(
                "keys {}, applied in one commit",
                std::iter::once(spec)
                    .chain(sidecars)
                    .map(label)
                    .collect::<Vec<_>>()
                    .join(" and ")
            ),
            ProposalBody::Layout { cluster_by } => format!("layout by {}", cluster_by.join(", ")),
            ProposalBody::Rewrite { shape } if !shape.pairs.is_empty() => format!(
                "rewrite of {} engine quer{}, compared by ({})",
                shape.pairs.len(),
                if shape.pairs.len() == 1 { "y" } else { "ies" },
                shape.key.join(", ")
            ),
            ProposalBody::Rewrite { shape } => format!("rewrite to {}", shape.after),
        },
        _ => "?".into(),
    };
    let _ = writeln!(
        s,
        "{}",
        heading(
            out,
            &if table {
                format!(
                    "fix {}: {what}, over a table (no index, no manifest)",
                    short(&r.proposal.hash)
                )
            } else {
                format!(
                    "fix {}: {what}, against manifest sequence {}",
                    short(&r.proposal.hash),
                    r.manifest_before
                )
            }
        )
    );
    if let Some(run) = &r.engine {
        engine_score_human(&mut s, r, run);
    } else {
        score_human(&mut s, r);
    }
    verify_and_outcome_human(&mut s, r);
    s
}

/// Stage 1 over a query engine (ADR 033): each pair as the engine's log
/// recorded the run that exported it, and the twin's prediction beside it.
#[allow(clippy::too_many_lines)]
fn engine_score_human(s: &mut String, r: &FixResult, run: &EngineRun) {
    let replay = match &r.score {
        Some(Score::Measured { replay, .. }) => short(&replay.hash),
        _ => "none written".into(),
    };
    let _ = writeln!(
        s,
        "  1 score   in {}, measured, not exact ({}): replay {replay}",
        run.engine, run.model
    );
    let _ = writeln!(
        s,
        "            {:<6} {:>14} {:>14} {:>14} {:>12} {:>10}",
        "", "rows read", "bytes read", "peak memory", "cpu ms", "result"
    );
    for (i, p) in run.pairs.iter().enumerate() {
        for (side, q) in [("before", &p.before), ("after", &p.after)] {
            let _ = writeln!(
                s,
                "    p{i} {side:<6} {:>14} {:>14} {:>14} {:>12} {:>10}  {}",
                num(q.read_rows),
                size(q.read_bytes),
                size(q.peak_memory_bytes),
                q.cpu_ns
                    .map_or_else(|| "-".to_string(), |c| num(c / 1_000_000)),
                num(q.result_rows),
                q.tag
            );
        }
        if p.before.result_rows != p.after.result_rows {
            let _ = writeln!(
                s,
                "    p{i} returns {} rows before and {} after: the rewrite does not return the \
                 same rows",
                num(p.before.result_rows),
                num(p.after.result_rows)
            );
        }
    }
    if let Some(p) = &run.prediction {
        let ratio = |r: &Option<String>| {
            r.clone()
                .unwrap_or_else(|| "undefined (the twin predicts 0 rows)".into())
        };
        let _ = writeln!(
            s,
            "            twin predicted {} rows in {} row groups for the key sample",
            num(p.predicted_rows),
            num(p.predicted_row_groups)
        );
        for (i, q) in p.pairs.iter().enumerate() {
            let _ = writeln!(
                s,
                "    p{i} the engine read {} rows before (ratio {}) and {} after (ratio {})",
                num(q.before_read_rows),
                ratio(&q.before_ratio),
                num(q.after_read_rows),
                ratio(&q.after_ratio)
            );
        }
    }
    let twin = &run.identity_at_twin;
    let _ = writeln!(
        s,
        "            engine identity {} ({} units, {} rows){}; {}",
        short(&run.identity_before.hash),
        num(run.identity_before.units),
        num(run.identity_before.rows),
        match &run.identity_after {
            Some(a) if a.hash == run.identity_before.hash =>
                ", unchanged through the runs".to_string(),
            Some(a) => format!(", {} after the runs", short(&a.hash)),
            None => String::new(),
        },
        if twin.hash == run.identity_before.hash {
            "the same as when the twin diagnosis ran".to_string()
        } else {
            format!(
                "the twin diagnosis ran against {} ({} units, {} rows), so its prediction \
                 describes an earlier state of the tables",
                short(&twin.hash),
                num(twin.units),
                num(twin.rows)
            )
        }
    );
    if let Some(dir) = &run.scratch {
        let exported: u64 = run
            .pairs
            .iter()
            .filter_map(|p| p.exported.as_ref())
            .flat_map(|e| e.iter().map(|x| x.bytes))
            .sum();
        let _ = writeln!(
            s,
            "            exports: {} under {dir}, {}",
            size(exported),
            if run.kept {
                "kept there (--keep)"
            } else {
                "deleted when the run ended"
            }
        );
    }
}

fn score_human(s: &mut String, r: &FixResult) {
    match &r.score {
        Some(Score::Replayed {
            replay,
            current,
            proposed,
        }) => {
            // Sizes carry their unit: no "bytes" after them.
            let _ = writeln!(
                s,
                "  1 score   {:>10} -> {:>10} over the workload ({} -> {}), replay {}",
                size(current.bytes),
                size(proposed.bytes),
                num(current.row_groups),
                count(proposed.row_groups, "row group", "row groups"),
                short(&replay.hash)
            );
            if matches!(&replay.record, Record::Replay(rp) if rp.unserved.is_some()) {
                let _ = writeln!(
                    s,
                    "            a lookup no key serves is priced as the files' own min/max \
                     statistics read it, as a reader without an index does"
                );
            }
        }
        Some(Score::Measured {
            replay,
            current,
            proposed,
            model,
        }) => {
            let _ = writeln!(
                s,
                "  1 score   {:>10} -> {:>10} over the workload ({} -> {}), {model}: \
                 not exact, so replay {} is not recorded",
                size(current.bytes),
                size(proposed.bytes),
                num(current.row_groups),
                count(proposed.row_groups, "unit", "units"),
                short(&replay.hash)
            );
        }
        Some(Score::NotScorable { reason }) => {
            let _ = writeln!(s, "  1 score   not scorable: {reason}");
        }
        None => {}
    }
}

#[allow(clippy::too_many_lines)]
fn verify_and_outcome_human(s: &mut String, r: &FixResult) {
    if let Some(c) = &r.certificate {
        if let Record::Certificate(cert) = &c.record {
            let _ = writeln!(
                s,
                "  2 verify  {}, coverage {}, {} mismatched, {} uncomparable, {} missing left, {} missing right, certificate {}",
                if cert.pass { "pass" } else { "FAIL" },
                coverage(&cert.coverage),
                num(cert.body.mismatched),
                num(cert.body.uncomparable),
                num(cert.body.missing_left),
                num(cert.body.missing_right),
                short(&c.hash)
            );
            if cert.body.uncomparable > 0 {
                let _ = writeln!(
                    s,
                    "            {}",
                    uncomparable_cause(cert.body.uncomparable)
                );
            }
        }
    }
    // A join key (B3): the right dataset's certificate, which the left one
    // cites, and the keys both compared rows of.
    if let Some(j) = &r.join {
        if let Record::Certificate(cert) = &j.certificate.record {
            let _ = writeln!(
                s,
                "            right dataset {}: {}, {} mismatched, {} missing left, {} missing \
                 right, certificate {} (cited by the one above)",
                j.right_root,
                if cert.pass { "pass" } else { "FAIL" },
                num(cert.body.mismatched),
                num(cert.body.missing_left),
                num(cert.body.missing_right),
                short(&j.certificate.hash)
            );
        }
        let _ = writeln!(
            s,
            "            rows compared on both datasets: those of the {} of {} asked keys both \
             datasets' files hold",
            num(j.matched),
            num(j.keys)
        );
    }
    if let Some(filter) = r.verification.as_ref().and_then(|v| v.row_filter.as_ref()) {
        let _ = writeln!(
            s,
            "            rows compared: those the lookups ask for ({}), bounded by the workload \
             and the data, not by the index",
            rows_asked(filter)
        );
    }
    // A proposed agg key's cells (B7), checked against the rows.
    if let Some(Record::Certificate(cert)) = r.certificate.as_ref().map(|c| &c.record) {
        for a in &cert.body.agg {
            let Some(c) = &a.checked else { continue };
            let _ = writeln!(
                s,
                "            agg cells checked against the rows: {}: {} of {} ({}; {} read), \
                 {} differ",
                a.specs.join(", "),
                count(c.cells, "cell", "cells"),
                count(c.keys, "key", "keys"),
                count(a.rows, "row", "rows"),
                count(c.row_groups_read, "row group", "row groups"),
                num(c.differ)
            );
            for d in c.examples.iter().take(5) {
                let key: Vec<&str> = d
                    .values
                    .iter()
                    .map(|v| v.as_deref().unwrap_or("null"))
                    .collect();
                let _ = writeln!(
                    s,
                    "              key ({}) row group {}: the index would store {}; the rows \
                     make {}",
                    key.join(", "),
                    d.row_group,
                    d.stored,
                    d.rows
                );
            }
        }
    }
    // The invariant is checked by the certificate; without one (a refusal
    // before the verify stage), nothing was.
    if let (Some(inv), Some(_)) = (&r.invariant, &r.certificate) {
        let _ = writeln!(s, "            invariant checked: {inv}");
    }
    match &r.outcome {
        FixOutcome::Applied {
            receipt,
            realized,
            warning,
        } => {
            if let Record::Receipt(rc) = &receipt.record {
                let _ = writeln!(
                    s,
                    "  3 apply   manifest {} -> {}",
                    rc.manifest_before, rc.manifest_after
                );
            }
            let _ = writeln!(s, "  4 attest  receipt {}", receipt.hash);
            signed_line(s, r, "receipt");
            if let Some(r) = realized {
                if let Record::Replay(rp) = &r.record {
                    let bytes: u64 = rp.current.iter().map(|c| c.bytes).sum();
                    let _ = writeln!(
                        s,
                        "  realized {} at manifest {}, replay {}",
                        count(bytes, "byte", "bytes"),
                        rp.manifest_version,
                        short(&r.hash)
                    );
                }
            }
            if let Some(w) = warning {
                let _ = writeln!(s, "  warning: {w}");
            }
        }
        FixOutcome::Attested {
            receipt,
            target,
            deploy,
        } => {
            let _ = writeln!(
                s,
                "  3 apply   attest-only: nothing applied in {target}; a human or a CI gate \
                 deploys the rewritten {}",
                if deploy.len() == 1 {
                    "query"
                } else {
                    "queries"
                }
            );
            let _ = writeln!(s, "  4 attest  receipt {} (attest-only)", receipt.hash);
            signed_line(s, r, "receipt");
            for (i, q) in deploy.iter().enumerate() {
                let _ = writeln!(
                    s,
                    "  deploy p{i}: {}",
                    q.lines().collect::<Vec<_>>().join(" ")
                );
            }
        }
        FixOutcome::Verified => {
            let _ = writeln!(s, "  3 apply   not run (--verify-only)");
            signed_line(s, r, "certificate");
        }
        // The CLI never asks for a dry run; the MCP server does.
        FixOutcome::WouldApply { sequence, expected } => {
            let _ = writeln!(
                s,
                "  3 apply   dry run: would commit {expected} over {sequence}; nothing written"
            );
        }
        FixOutcome::Refused { refusal } => {
            let why = match refusal {
                Refusal::CertificateFailed {
                    mismatched,
                    uncomparable,
                    missing_left,
                    missing_right,
                    totals_differ,
                    agg_cells_differ,
                    ..
                } if *agg_cells_differ > 0 => format!(
                    "the certificate failed: {agg_cells_differ} agg cells of the proposed key differ from the rows they aggregate (above), so building it would store wrong aggregates ({mismatched} mismatched, {uncomparable} uncomparable, {missing_left} missing left, {missing_right} missing right, {totals_differ} totals differ); no receipt"
                ),
                Refusal::CertificateFailed {
                    mismatched,
                    uncomparable,
                    missing_left,
                    missing_right,
                    totals_differ,
                    ..
                } => format!(
                    "the certificate failed ({mismatched} mismatched, {uncomparable} uncomparable, {missing_left} missing left, {missing_right} missing right, {totals_differ} totals differ); no receipt"
                ),
                Refusal::StaleManifest {
                    certified, current, ..
                } => format!(
                    "the certificate was computed against manifest {certified} but the current one is {current}; run fix again"
                ),
                Refusal::ConcurrentCommit {
                    expected,
                    committed,
                } => format!(
                    "another commit landed during the apply (expected sequence {expected}, got {committed}). The key is built and serves lookups at sequence {committed}; no re-index is needed. It cannot be attested, and retrying will not attest it: a retry finds the key already complete (nothing_to_apply) and writes no receipt. `bellwether why` with the same keys reports the cost it now has"
                ),
                Refusal::DataChanged { .. } if engine_rewrite(r) => {
                    "the engine's tables changed after the certificate was computed (their identity differs from the one it cites), so the certificate no longer describes them; run fix on the proposal again to re-verify".into()
                }
                Refusal::DataChanged { .. } => {
                    "the data changed after the certificate was computed (its catalog digest differs), so the certificate no longer describes it; run fix again to re-verify".into()
                }
                Refusal::MeasurementChanged {
                    replay,
                    pair,
                    side,
                    certified_read_rows,
                    measured_read_rows,
                    certified_read_bytes,
                    measured_read_bytes,
                    certified_result_rows,
                    measured_result_rows,
                    ..
                } => format!(
                    "measurement_changed: run again in the state certified, p{pair} {side} read {} rows and {} bytes and returned {} rows, where the certified replay {replay} says {} rows, {} bytes, and {} rows; the numbers a receipt would attest are not the numbers verified (a query that behaves differently between runs, or a background merge, does this), so nothing was written; run fix on the proposal again",
                    num(*measured_read_rows),
                    num(*measured_read_bytes),
                    num(*measured_result_rows),
                    num(*certified_read_rows),
                    num(*certified_read_bytes),
                    num(*certified_result_rows)
                ),
                Refusal::NothingToApply { sequence } => format!(
                    "the key is already complete at sequence {sequence}, so applying it changes nothing; no receipt"
                ),
                Refusal::LayoutNotApplicable => {
                    "layout proposals are applied by evolve, which has not landed".into()
                }
                Refusal::NoReplay { .. } => {
                    "the rewrite verified, but a receipt cites a replay and rewrites have none yet (contract v3 backlog #2)".into()
                }
                Refusal::AttestOnly {
                    target,
                    run,
                    proposed_rewrite,
                    ..
                } => {
                    let mut why = format!(
                        "attest-only: the certificate passed, and {target} is a table, so the \
                         engine applies nothing and writes no receipt (ADR 027)."
                    );
                    if !run.is_empty() {
                        why.push_str(" To apply it, a human runs:");
                        for r in run {
                            why.push_str("\n    ");
                            why.push_str(r);
                        }
                    }
                    if let Some(text) = proposed_rewrite {
                        why.push_str(
                            " The agent's proposed rewrite, verified on the outputs given; the \
                             engine never ran it, so review it before deploying it:\n    ",
                        );
                        why.push_str(text);
                    }
                    why
                }
                Refusal::ExportDenied { message, .. } => format!(
                    "export_denied: {message}. The export is the measured run, so nothing was \
                     measured, and there is no certificate and no receipt"
                ),
                Refusal::NothingToGain {
                    replay,
                    current_bytes,
                    proposed_bytes,
                } if engine_rewrite(r) => format!(
                    "nothing_to_gain: replay {replay} measured the rewrite reading no fewer bytes \
                     in the engine ({} before, {} after), so nothing was verified or attested; \
                     pass --allow-no-gain to go on anyway",
                    num(*current_bytes),
                    num(*proposed_bytes)
                ),
                Refusal::NothingToGain {
                    replay,
                    current_bytes,
                    proposed_bytes,
                } => format!(
                    "nothing_to_gain: replay {replay} predicts no saving on this workload ({} bytes now, {} with the proposal), so nothing was verified or applied (`bellwether why` says \"recommended: none\" for the same reason); pass --allow-no-gain to go on anyway",
                    num(*current_bytes),
                    num(*proposed_bytes)
                ),
            };
            let _ = writeln!(s, "  refused: {why}");
        }
    }
}

/// Whether `r` is a fix of a rewrite of engine queries (ADR 033).
fn engine_rewrite(r: &FixResult) -> bool {
    r.engine.is_some()
        || matches!(
            &r.proposal.record,
            Record::Proposal(p) if matches!(&p.body, ProposalBody::Rewrite { shape } if !shape.pairs.is_empty())
        )
}
