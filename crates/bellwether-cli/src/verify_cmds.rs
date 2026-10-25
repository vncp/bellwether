//! `reconcile` and `fix`: the commands over the verify stage (WP8).

use std::fmt::Write as _;

use bellwether::spec::label;
use bellwether::types::{Coverage, ProposalBody, Record};
use bellwether::{
    FixOutcome, FixRequest, FixResult, ReconcileRequest, ReconcileResult, Refusal, RewriteOutputs,
    Score,
};
use clap::{Args, Subcommand};

use crate::human::{heading, num, short, size};
use crate::{
    done, engine, hash, Failure, Global, Out, Outcome, RootArg, EXIT_FAILED, EXIT_OK, EXIT_REFUSED,
};

/// Commands that write certificates.
#[derive(Subcommand)]
pub(crate) enum VerifyCommand {
    /// Prove two folders agree, by key, latest version, and exact decimal
    /// totals; writes a Certificate.
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
    /// Decimal columns totalled exactly, comma-separated.
    #[arg(long, value_name = "COLS", value_delimiter = ',')]
    totals: Vec<String>,
    /// Columns grouping the totals, comma-separated.
    #[arg(long, value_name = "COLS", value_delimiter = ',')]
    group_by: Vec<String>,
}

#[derive(Args)]
pub(crate) struct ReconcileArgs {
    /// The left folder (its index and ledger are used).
    left: String,
    /// The right folder.
    right: String,
    #[command(flatten)]
    compare: CompareArgs,
    /// Compare a deterministic sample of this fraction of keys.
    #[arg(long, value_name = "FRACTION")]
    sample: Option<f64>,
    /// Sampling seed.
    #[arg(long, default_value_t = 0, requires = "sample")]
    seed: u64,
}

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
    /// A rewrite's output before the rewrite (a folder).
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
            };
            let r = engine(g, &a.left)?.reconcile(&a.right, &request)?;
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
    let _ = writeln!(s, "\ncertificate {}", r.certificate.hash);
    s
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
            };
            let _ = writeln!(s, "  refused: {why}");
        }
    }
    s
}
