//! `reconcile` and `fix`: the commands over the verify stage (WP8).

use std::fmt::Write as _;

use bellwether::spec::label;
use bellwether::types::{Coverage, Predicate, ProposalBody, Record};
use bellwether::{
    FixOutcome, FixRequest, FixResult, KeyRepr, Mismatch, ReconcileProblem, ReconcileRequest,
    ReconcileResult, Refusal, RewriteOutputs, Score, TotalColumn,
};
use clap::{Args, Subcommand};

use crate::human::{heading, num, short, size};
use crate::{
    done, engine, hash, Failure, Global, Out, Outcome, RootArg, EXIT_FAILED, EXIT_OK, EXIT_REFUSED,
};

/// Commands that write certificates.
#[derive(Subcommand)]
pub(crate) enum VerifyCommand {
    /// Prove two folders, or a table (sql://...) and a folder, agree, by
    /// key, latest version, and exact decimal totals; writes a Certificate. The summary names the first 10
    /// mismatching keys (every one is in --json), prints the exact totals,
    /// and says how many passes the comparison made over each side.
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
                rewrite,
                verify_only: a.verify_only,
                allow_no_gain: a.allow_no_gain,
            };
            let r = engine(g, &a.root.root)?.fix(&request)?;
            let code = match &r.outcome {
                FixOutcome::Applied { .. }
                | FixOutcome::Verified
                | FixOutcome::WouldApply { .. } => EXIT_OK,
                FixOutcome::Refused {
                    refusal: Refusal::CertificateFailed { .. },
                } => EXIT_FAILED,
                FixOutcome::Refused { .. } => EXIT_REFUSED,
            };
            done(&r, fix_human(&r, out), code)
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

fn coverage(c: &Coverage) -> String {
    match c {
        Coverage::Full => "full".into(),
        Coverage::Workload { keys, .. } => format!("workload, {} keys", num(*keys)),
        Coverage::Sampled { keys, fraction } => {
            format!("sampled, {} keys ({fraction})", num(*keys))
        }
        Coverage::StaticProof { identity } => format!("static proof {identity}"),
    }
}

/// What a row filter keeps, in words: one phrase per kind of test.
fn rows_asked(filter: &[Predicate]) -> String {
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
            Predicate::And { all } => {
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
            "    ... {} more groups in --json",
            o.totals.len() - HUMAN_TOTALS
        );
    }
}

#[allow(clippy::many_single_char_names)]
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
    let _ = writeln!(
        s,
        "  {:<22} {:>14}\n  {:<22} {:>14}\n  {:<22} {:>14}\n  {:<22} {:>14}\n  {:<22} {:>14}\n  {:<22} {:>14}",
        "keys compared", num(c.keys_compared),
        "matched", num(c.matched),
        "mismatched", num(c.mismatched),
        "missing on the left", num(c.missing_left),
        "missing on the right", num(c.missing_right),
        "totals that differ", num(c.totals_differ)
    );
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
            "\n  first {shown} of {} mismatching keys ({}): every kept example is in --json",
            num(per_key_total),
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
        "\n  read {} pass(es) over each side under a {}-byte state budget ({} restarts): {} rows read on the left, {} on the right",
        m.passes,
        num(m.budget_bytes),
        m.restarts,
        num(m.rows_read[0]),
        num(m.rows_read[1])
    );
    if m.passes > 1 {
        let _ = writeln!(
            s,
            "  each pass reads both sides again; a larger --memory-budget makes fewer passes"
        );
    }
    tables_human(&mut s, &r.tables);
    let _ = writeln!(s, "\ncertificate {}", r.certificate.hash);
    s
}

/// One line per table a run read (ADR 027): its units and rows, and the
/// snapshot it held or the identity re-read that stood in for one.
pub(crate) fn tables_human(s: &mut String, tables: &[bellwether::TableRun]) {
    for t in tables {
        let held = match &t.snapshot {
            Some(snap) => format!("read inside {snap}"),
            None if t.rechecked => {
                "no snapshot held: every unit's identity re-read at the end, unchanged".into()
            }
            None => "no snapshot held".into(),
        };
        let _ = writeln!(
            s,
            "  table {}: {} rows in {} units ({}), {held}",
            t.root,
            num(t.rows),
            num(t.units),
            t.units_by
        );
    }
}

// One linear screen, a line per stage.
#[allow(clippy::too_many_lines)]
fn fix_human(r: &FixResult, out: &Out) -> String {
    let mut s = String::new();
    let what = match &r.proposal.record {
        Record::Proposal(p) => match &p.body {
            ProposalBody::Key { spec } => format!("key {}", label(spec)),
            ProposalBody::Layout { cluster_by } => format!("layout by {}", cluster_by.join(", ")),
            ProposalBody::Rewrite { shape } => format!("rewrite to {}", shape.after),
        },
        _ => "?".into(),
    };
    let _ = writeln!(
        s,
        "{}",
        heading(
            out,
            &format!(
                "fix {}: {what}, against manifest sequence {}",
                short(&r.proposal.hash),
                r.manifest_before
            )
        )
    );
    match &r.score {
        Some(Score::Replayed {
            replay,
            current,
            proposed,
        }) => {
            let _ = writeln!(
                s,
                "  1 score   {:>10} -> {:>10} bytes over the workload ({} -> {} row groups), replay {}",
                size(current.bytes),
                size(proposed.bytes),
                num(current.row_groups),
                num(proposed.row_groups),
                short(&replay.hash)
            );
        }
        Some(Score::Measured {
            replay,
            current,
            proposed,
            model,
        }) => {
            let _ = writeln!(
                s,
                "  1 score   {:>10} -> {:>10} bytes over the workload ({} -> {} units), {model}: \
                 not exact, so replay {} is not recorded",
                size(current.bytes),
                size(proposed.bytes),
                num(current.row_groups),
                num(proposed.row_groups),
                short(&replay.hash)
            );
        }
        Some(Score::NotScorable { reason }) => {
            let _ = writeln!(s, "  1 score   not scorable: {reason}");
        }
        None => {}
    }
    if let Some(c) = &r.certificate {
        if let Record::Certificate(cert) = &c.record {
            let _ = writeln!(
                s,
                "  2 verify  {}, coverage {}, {} mismatched, {} missing left, {} missing right, certificate {}",
                if cert.pass { "pass" } else { "FAIL" },
                coverage(&cert.coverage),
                num(cert.body.mismatched),
                num(cert.body.missing_left),
                num(cert.body.missing_right),
                short(&c.hash)
            );
        }
    }
    if let Some(filter) = r.verification.as_ref().and_then(|v| v.row_filter.as_ref()) {
        let _ = writeln!(
            s,
            "            rows compared: those the lookups ask for ({}), bounded by the workload \
             and the data, not by the index",
            rows_asked(filter)
        );
    }
    if let Some(inv) = &r.invariant {
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
            if let Some(r) = realized {
                if let Record::Replay(rp) = &r.record {
                    let bytes: u64 = rp.current.iter().map(|c| c.bytes).sum();
                    let _ = writeln!(
                        s,
                        "  realized {} bytes at manifest {}, replay {}",
                        num(bytes),
                        rp.manifest_version,
                        short(&r.hash)
                    );
                }
            }
            if let Some(w) = warning {
                let _ = writeln!(s, "  warning: {w}");
            }
        }
        FixOutcome::Verified => {
            let _ = writeln!(s, "  3 apply   not run (--verify-only)");
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
                    missing_left,
                    missing_right,
                    totals_differ,
                    ..
                } => format!(
                    "the certificate failed ({mismatched} mismatched, {missing_left} missing left, {missing_right} missing right, {totals_differ} totals differ); no receipt"
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
                Refusal::DataChanged { .. } => {
                    "the data changed after the certificate was computed (its catalog digest differs), so the certificate no longer describes it; run fix again to re-verify".into()
                }
                Refusal::NothingToApply { sequence } => format!(
                    "the key is already complete at sequence {sequence}, so applying it changes nothing; no receipt"
                ),
                Refusal::LayoutNotApplicable => {
                    "layout proposals are applied by evolve, which has not landed".into()
                }
                Refusal::NoReplay { .. } => {
                    "the rewrite verified, but a receipt cites a replay and rewrites have none yet (contract v3 backlog #2)".into()
                }
                Refusal::AttestOnly { target, run, .. } => {
                    let mut why = format!(
                        "attest-only: the certificate passed, and {target} is a table, so the \
                         engine applies nothing and writes no receipt (ADR 027). To apply it, a \
                         human runs:"
                    );
                    for r in run {
                        why.push_str("\n    ");
                        why.push_str(r);
                    }
                    why
                }
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
    s
}
