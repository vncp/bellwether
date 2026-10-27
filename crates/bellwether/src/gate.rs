//! The receipt gate (WP17): whether a receipt hash a pull request names
//! stands for one pipeline step, checked from the ledger's own records, and
//! the report `bellwether ci` prints and comments.
//!
//! [`Engine::receipt_gate`] runs six checks in order and stops at the first
//! that fails, naming it with the fact behind it:
//!
//! 1. `kind`: the hash is a stored, readable Receipt.
//! 2. `chain`: every link of its explain chain is present, readable, and of
//!    the expected kind.
//! 3. `certificate`: the ledger's receipt rules 12 to 16 re-run on the
//!    stored records (the certificate passed with no differences, verified
//!    the receipt's proposal, the replay scored it, and both were computed
//!    against `manifest_before`). `explain`'s intact flag checks presence and
//!    kind only, so it is never taken as this check.
//! 4. `coverage`: the certificate compared every key, or exactly the keys of
//!    the step's recorded workload.
//! 5. `data`: the certificate is about the step's data as it is now: the
//!    catalog digest it cites for the side that is the step's output (a
//!    rewrite's right side, the after output; otherwise the data root it
//!    scanned) equals the digest of the step's root computed now, as `fix`'s
//!    own gate computes it.
//! 6. `signature`: when required, a key in the engine's trusted keys signed
//!    the receipt and its certificate, each within the key's window, and
//!    every signature object beside them verifies.
//!
//! Nothing here writes, except [`Engine::ci_gate`]'s `why` for a step without
//! a receipt when the request asks for it.

use std::sync::Arc;

use bellwether_ledger::{printable, Ledger, LinkStatus, RecordKind, Verdict};
use bellwether_types::{
    Certificate, Coverage, Error, Hash, KeySpec, ProposalBody, Receipt, Record, Replay, Result,
};
use serde::{Deserialize, Serialize};

use crate::engine::{guard, Engine};
use crate::results::{total, Cost};
use crate::why::WhyRequest;

/// What a pipeline step requires of the receipt that gates it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateRequirements {
    /// The folder the receipt must be about: a rewrite's after output, or
    /// the data root a key or layout receipt indexed.
    pub root: String,
    /// The recorded workload the certificate must cover (a `full` coverage
    /// covers it too).
    pub workload: Hash,
    /// Require a signature by a key in the engine's trusted keys.
    pub require_signature: bool,
}

/// One check of the gate, in the order they run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateCheck {
    /// The hash is a stored, readable Receipt.
    Kind,
    /// Every link of its chain is present, readable, and of its kind.
    Chain,
    /// Rules 12 to 16 re-run on the stored certificate and replay.
    Certificate,
    /// The certificate covers the step's workload.
    Coverage,
    /// The certificate is about the step's data as it is now.
    Data,
    /// A trusted key signed the receipt.
    Signature,
}

impl GateCheck {
    /// The check's name, as `--json` spells it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Kind => "kind",
            Self::Chain => "chain",
            Self::Certificate => "certificate",
            Self::Coverage => "coverage",
            Self::Data => "data",
            Self::Signature => "signature",
        }
    }
}

/// One check that ran, and whether it passed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckOutcome {
    /// The check.
    pub check: GateCheck,
    /// Whether it passed.
    pub pass: bool,
}

/// The first check that failed, with the fact behind it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "failure", rename_all = "snake_case")]
pub enum GateFailure {
    /// No record of that hash in the ledger.
    NotFound {
        /// The ledger looked in.
        ledger: String,
    },
    /// The hash, or a link of its chain, is another kind of record.
    WrongKind {
        /// The record.
        record: Hash,
        /// The kind expected there.
        expected: RecordKind,
        /// The kind stored.
        found: RecordKind,
    },
    /// The record, or a link of its chain, cannot be read: its bytes do not
    /// hash to its name, or it is malformed or of a newer version.
    Unreadable {
        /// The record.
        record: Hash,
        /// The kind expected there.
        kind: RecordKind,
        /// Why, as the ledger says it (escaped).
        reason: String,
    },
    /// A link of the chain is not in the ledger.
    Missing {
        /// The record.
        record: Hash,
        /// The kind expected there.
        kind: RecordKind,
    },
    /// The certificate did not pass, or says it passed with differences.
    CertificateFailed {
        /// The certificate.
        certificate: Hash,
        /// Its `pass`.
        pass: bool,
        /// Keys only on the right.
        missing_left: u64,
        /// Keys only on the left.
        missing_right: u64,
        /// Keys whose rows differ.
        mismatched: u64,
        /// Keys that could not be compared.
        uncomparable: u64,
        /// Totals that differ.
        totals_delta: u64,
    },
    /// The certificate verified, or the replay scored, another proposal
    /// than the receipt's (rules 13 and 15).
    OtherProposal {
        /// The certificate or the replay.
        record: Hash,
        /// Which of the two.
        kind: RecordKind,
        /// The proposal it names (`None`: a certificate of no proposal).
        names: Option<Hash>,
        /// The receipt's proposal.
        proposal: Hash,
    },
    /// The certificate or the replay was computed against another manifest
    /// than the receipt's `manifest_before` (rule 16).
    StaleManifest {
        /// The certificate or the replay.
        record: Hash,
        /// Which of the two.
        kind: RecordKind,
        /// The manifest sequence it was computed against.
        computed: Option<String>,
        /// The receipt's `manifest_before`.
        manifest_before: String,
    },
    /// The certificate compared the keys of another workload.
    WrongWorkload {
        /// The certificate.
        certificate: Hash,
        /// The workload it covered.
        covered: Hash,
        /// The workload the step requires.
        required: Hash,
    },
    /// The certificate compared a sample, or no data (a static proof).
    PartialCoverage {
        /// The certificate.
        certificate: Hash,
        /// Its coverage.
        coverage: Coverage,
    },
    /// The certificate is about other data than the step's root holds now.
    DataChanged {
        /// The certificate.
        certificate: Hash,
        /// The catalog digest it cites for the step's side (`None`: it
        /// cites none there).
        certified: Option<Hash>,
        /// The digest of the step's root now.
        current: Hash,
        /// The step's root, canonical.
        root: String,
    },
    /// No signature object beside the receipt or its certificate.
    Unsigned {
        /// The receipt or the certificate.
        record: Hash,
        /// Which of the two.
        kind: RecordKind,
    },
    /// Every signature beside the record verifies, but none is by a
    /// trusted key within its window.
    UntrustedKey {
        /// The receipt or the certificate.
        record: Hash,
        /// Which of the two.
        kind: RecordKind,
        /// The keys that signed.
        key_ids: Vec<Hash>,
    },
    /// A signature object beside the record does not verify.
    InvalidSignature {
        /// The receipt or the certificate.
        record: Hash,
        /// Which of the two.
        kind: RecordKind,
        /// The keys the objects are filed under.
        key_ids: Vec<Hash>,
    },
}

impl GateFailure {
    /// The failure's stable code, as `--json` spells it.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::NotFound { .. } => "not_found",
            Self::WrongKind { .. } => "wrong_kind",
            Self::Unreadable { .. } => "unreadable",
            Self::Missing { .. } => "missing",
            Self::CertificateFailed { .. } => "certificate_failed",
            Self::OtherProposal { .. } => "other_proposal",
            Self::StaleManifest { .. } => "stale_manifest",
            Self::WrongWorkload { .. } => "wrong_workload",
            Self::PartialCoverage { .. } => "partial_coverage",
            Self::DataChanged { .. } => "data_changed",
            Self::Unsigned { .. } => "unsigned",
            Self::UntrustedKey { .. } => "untrusted_key",
            Self::InvalidSignature { .. } => "invalid_signature",
        }
    }
}

/// What a receipt proposed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    /// An index key.
    Key,
    /// A file layout.
    Layout,
    /// A rewrite.
    Rewrite,
}

/// A replay's totals over its runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayTotals {
    /// The replay.
    pub replay: Hash,
    /// Under the configuration it was computed against.
    pub current: Cost,
    /// Under the proposal.
    pub proposed: Cost,
}

/// A certificate's verdict and what it compared.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CertificateFacts {
    /// The certificate.
    pub certificate: Hash,
    /// Its `pass`.
    pub pass: bool,
    /// What it compared.
    pub coverage: Coverage,
}

/// What the receipt's records say, read as far as they resolve; the
/// comment's row for the step.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReceiptFacts {
    /// The proposal applied or attested.
    pub proposal: Hash,
    /// What it proposed, when it resolved.
    pub change: Option<ChangeKind>,
    /// For a key proposal, the key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<KeySpec>,
    /// Whether the receipt only attests (a rewrite a human deploys).
    pub attest_only: bool,
    /// The manifest sequence before the apply.
    pub manifest_before: String,
    /// The manifest sequence after it.
    pub manifest_after: String,
    /// The certificate, when it resolved.
    pub certificate: Option<CertificateFacts>,
    /// The receipt's replay, when it resolved.
    pub replay: Option<ReplayTotals>,
    /// The realized cost: the newest replay of the same proposal and
    /// workload at `manifest_after` (`fix` writes it after an apply); its
    /// `current` is what the workload reads now. `None` for an attest-only
    /// receipt.
    pub realized: Option<ReplayTotals>,
    /// The receipt's signatures against the trusted keys, when the gate
    /// requires a signature.
    pub signature: Option<Verdict>,
    /// Its certificate's, likewise, when the certificate resolved.
    pub certificate_signature: Option<Verdict>,
}

/// The gate's verdict on one receipt for one step.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GateVerdict {
    /// The receipt hash given.
    pub receipt: Hash,
    /// Whether every check passed.
    pub pass: bool,
    /// The checks that ran, in order; the first that failed is the last.
    pub checks: Vec<CheckOutcome>,
    /// The first failure, with its facts.
    pub failure: Option<GateFailure>,
    /// What the records say, when the receipt resolved.
    pub facts: Option<ReceiptFacts>,
}

impl GateVerdict {
    /// The check that failed, if one did.
    #[must_use]
    pub fn failed_check(&self) -> Option<GateCheck> {
        self.checks.iter().find(|c| !c.pass).map(|c| c.check)
    }

    fn new(receipt: Hash) -> Self {
        Self {
            receipt,
            pass: false,
            checks: Vec::new(),
            failure: None,
            facts: None,
        }
    }

    fn passed(&mut self, check: GateCheck) {
        self.checks.push(CheckOutcome { check, pass: true });
    }

    fn fail(mut self, check: GateCheck, failure: GateFailure) -> Self {
        self.checks.push(CheckOutcome { check, pass: false });
        self.failure = Some(failure);
        self
    }
}

/// Whether a failing step fails the gate.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateMode {
    /// A changed step without a passing receipt fails the gate.
    #[default]
    Require,
    /// Every step is reported; nothing fails the gate.
    Advise,
}

/// Where a pull request gave a step's receipt.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReceiptSource {
    /// The file `.bellwether/receipts/<step>` at the head.
    File,
    /// A `Bellwether-Receipt(<step>): <hash>` line in the pull request body.
    Trailer,
}

/// A receipt hash a pull request gave for a step.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GivenReceipt {
    /// The hash.
    pub hash: Hash,
    /// Where.
    pub from: ReceiptSource,
}

/// Something given for a step that is not a receipt hash, so no receipt
/// was taken.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "problem", rename_all = "snake_case")]
pub enum ReceiptProblem {
    /// The receipt file is not exactly one 64-character lowercase hex hash.
    FileNotAHash,
    /// Two different hashes were given (the file and a trailer, or two
    /// trailers): the gate does not choose between them.
    TwoReceipts {
        /// The first.
        first: GivenReceipt,
        /// The second, which differs.
        second: GivenReceipt,
    },
}

/// One changed step to gate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateStep {
    /// Its name.
    pub name: String,
    /// What its receipt must satisfy.
    pub requirements: GateRequirements,
    /// The receipt given, if one was.
    pub receipt: Option<GivenReceipt>,
    /// What was given instead of a receipt, if anything.
    pub problem: Option<ReceiptProblem>,
}

/// What [`Engine::ci_gate`] gates.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateRequest {
    /// Whether a failing step fails the gate.
    pub gate: GateMode,
    /// With [`GateMode::Advise`]: run `why` on a step without a receipt,
    /// over its workload (it writes Evidence, Proposals, and Replays).
    pub why: bool,
    /// The base commit the configuration was read from.
    pub base: String,
    /// The head commit.
    pub head: String,
    /// The changed steps.
    pub steps: Vec<GateStep>,
    /// The steps no changed path touches, by name; not evaluated.
    pub unchanged: Vec<String>,
}

/// The gate's outcome over every changed step.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateOutcome {
    /// Every changed step passed.
    Pass,
    /// A changed step failed under [`GateMode::Require`].
    Fail,
    /// A changed step failed under [`GateMode::Advise`]: reported only.
    Advisory,
    /// No step's paths changed.
    NothingToCheck,
}

/// One candidate of a step's `why` funnel.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FunnelRow {
    /// The candidate key.
    pub spec: KeySpec,
    /// What the workload reads with it.
    pub proposed: Cost,
}

/// The recommendation of a step's `why`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StepRecommendation {
    /// The key.
    pub spec: KeySpec,
    /// Its proposal: pass it to `fix`.
    pub proposal: Hash,
    /// Its replay.
    pub replay: Hash,
}

/// Evidence for a step without a receipt: `why` over its workload.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StepEvidence {
    /// The profile Evidence `why` wrote.
    pub evidence: Option<Hash>,
    /// What the workload reads now.
    pub current: Option<Cost>,
    /// The candidates, in `why`'s order.
    pub candidates: Vec<FunnelRow>,
    /// The candidate reading the fewest bytes, if any reads fewer.
    pub recommended: Option<StepRecommendation>,
}

/// One changed step's verdict.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StepReport {
    /// Its name.
    pub step: String,
    /// Its root, canonical.
    pub root: String,
    /// Its workload.
    pub workload: Hash,
    /// The receipt given, if one was.
    pub receipt: Option<GivenReceipt>,
    /// What was given instead of a receipt, if anything.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub problem: Option<ReceiptProblem>,
    /// Whether the step passed: its receipt passed every check.
    pub pass: bool,
    /// The gate's verdict on its receipt, when one was given.
    pub verdict: Option<GateVerdict>,
    /// Without a receipt, under [`GateRequest::why`]: `why` over its
    /// workload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence: Option<StepEvidence>,
    /// Without a receipt, under [`GateRequest::why`]: why `why` could not
    /// run, when it could not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub why_error: Option<String>,
}

/// What [`Engine::ci_gate`] found: `bellwether ci --json`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GateReport {
    /// The `bellwether` version that gated.
    pub version: String,
    /// Whether a failing step fails the gate.
    pub gate: GateMode,
    /// The outcome.
    pub outcome: GateOutcome,
    /// The ledger (the index location).
    pub ledger: String,
    /// The data root the ledger indexes.
    pub root: String,
    /// The base commit.
    pub base: String,
    /// The head commit.
    pub head: String,
    /// One per changed step, in the configuration's order.
    pub steps: Vec<StepReport>,
    /// The steps no changed path touches.
    pub unchanged: Vec<String>,
}

impl Engine {
    /// Gates one receipt hash for one step: the checks in the module docs,
    /// in order, stopping at the first that fails. A check that fails is a
    /// verdict, never an error; only a ledger that does not exist or cannot
    /// be read, a step root that cannot be opened, or a signature required
    /// without trusted keys is an error. Writes nothing.
    pub fn receipt_gate(&self, hash: &Hash, req: &GateRequirements) -> Result<GateVerdict> {
        guard(|| self.receipt_gate_inner(hash, req))
    }

    /// Gates every changed step of `req`: [`Engine::receipt_gate`] for each
    /// step with a receipt; for one without, under [`GateMode::Advise`] with
    /// [`GateRequest::why`], `why` over its workload. Writes nothing but
    /// what that `why` writes.
    pub fn ci_gate(&self, req: &GateRequest) -> Result<GateReport> {
        guard(|| self.ci_gate_inner(req))
    }

    fn ci_gate_inner(&self, req: &GateRequest) -> Result<GateReport> {
        let mut steps = Vec::with_capacity(req.steps.len());
        for s in &req.steps {
            let root = canonical_root(&s.requirements.root)?;
            let mut report = StepReport {
                step: s.name.clone(),
                root,
                workload: s.requirements.workload,
                receipt: s.receipt,
                problem: s.problem,
                pass: false,
                verdict: None,
                evidence: None,
                why_error: None,
            };
            if let Some(given) = &s.receipt {
                let v = self.receipt_gate_inner(&given.hash, &s.requirements)?;
                report.pass = v.pass;
                report.verdict = Some(v);
            } else if req.gate == GateMode::Advise && req.why {
                match self.step_evidence(&s.requirements.workload) {
                    Ok(e) => report.evidence = Some(e),
                    Err(e) => report.why_error = Some(printable(&e.to_string())),
                }
            }
            steps.push(report);
        }
        let outcome = if steps.is_empty() {
            GateOutcome::NothingToCheck
        } else if steps.iter().all(|s| s.pass) {
            GateOutcome::Pass
        } else if req.gate == GateMode::Advise {
            GateOutcome::Advisory
        } else {
            GateOutcome::Fail
        };
        Ok(GateReport {
            version: env!("CARGO_PKG_VERSION").to_string(),
            gate: req.gate,
            outcome,
            ledger: self.index_location().to_string(),
            root: self.root().to_string(),
            base: req.base.clone(),
            head: req.head.clone(),
            steps,
            unchanged: req.unchanged.clone(),
        })
    }

    /// `why` over `workload`, summarized for the comment.
    fn step_evidence(&self, workload: &Hash) -> Result<StepEvidence> {
        let r = self.why(&WhyRequest {
            workload: Some(*workload),
            ..WhyRequest::default()
        })?;
        Ok(StepEvidence {
            evidence: r.evidence.hash,
            current: r.current,
            candidates: r
                .candidates
                .iter()
                .map(|c| FunnelRow {
                    spec: c.spec.clone(),
                    proposed: c.proposed,
                })
                .collect(),
            recommended: r.recommended.map(|x| StepRecommendation {
                spec: x.spec,
                proposal: x.proposal,
                replay: x.replay,
            }),
        })
    }

    #[allow(clippy::too_many_lines)] // one block per check, in order
    fn receipt_gate_inner(&self, hash: &Hash, req: &GateRequirements) -> Result<GateVerdict> {
        let root = canonical_root(&req.root)?;
        let trusted = if req.require_signature {
            Some(self.options().trusted_keys.as_ref().ok_or_else(|| {
                Error::InvalidArgument {
                    field: "trusted_keys",
                    reason: "a signature is required, and no trusted keys were given to check it \
                             against"
                        .into(),
                }
            })?)
        } else {
            None
        };
        let Some(store) = self.existing_store()? else {
            return Err(Error::Store(format!(
                "no ledger at {}: the index location does not exist",
                self.index_location()
            )));
        };
        let ledger = Ledger::new(Arc::clone(store));
        let v = GateVerdict::new(*hash);

        // 1. kind
        let receipt = match ledger.get(hash) {
            Ok(Record::Receipt(r)) => r,
            Ok(other) => {
                return Ok(v.fail(
                    GateCheck::Kind,
                    GateFailure::WrongKind {
                        record: *hash,
                        expected: RecordKind::Receipt,
                        found: RecordKind::of(&other),
                    },
                ))
            }
            Err(Error::NotFound(_)) => {
                return Ok(v.fail(
                    GateCheck::Kind,
                    GateFailure::NotFound {
                        ledger: self.index_location().to_string(),
                    },
                ))
            }
            Err(e) => match unreadable(&e) {
                Some(reason) => {
                    return Ok(v.fail(
                        GateCheck::Kind,
                        GateFailure::Unreadable {
                            record: *hash,
                            kind: RecordKind::Receipt,
                            reason,
                        },
                    ))
                }
                None => return Err(e),
            },
        };
        let mut v = v;
        v.passed(GateCheck::Kind);
        let records = Records::read(&ledger, &receipt)?;
        v.facts = Some(facts(&ledger, &receipt, &records, trusted, hash)?);

        // 2. chain
        let chain = ledger.explain(hash)?;
        if let Some(l) = chain.links.iter().find(|l| l.status != LinkStatus::Ok) {
            let failure = match &l.status {
                LinkStatus::Missing | LinkStatus::Ok => GateFailure::Missing {
                    record: l.hash,
                    kind: l.kind,
                },
                LinkStatus::WrongKind { found } => GateFailure::WrongKind {
                    record: l.hash,
                    expected: l.kind,
                    found: *found,
                },
                LinkStatus::Unreadable { reason } => GateFailure::Unreadable {
                    record: l.hash,
                    kind: l.kind,
                    reason: reason.clone(),
                },
            };
            return Ok(v.fail(GateCheck::Chain, failure));
        }
        v.passed(GateCheck::Chain);
        let (Some(cert), Some(replay)) = (&records.certificate, &records.replay) else {
            // Explain resolved both, and the ledger is append-only; read
            // again and gone (a store edited underneath) is still missing.
            return Ok(v.fail(
                GateCheck::Chain,
                GateFailure::Missing {
                    record: receipt.certificate,
                    kind: RecordKind::Certificate,
                },
            ));
        };

        // 3. certificate: rules 12 to 16.
        let b = &cert.body;
        let clean = b.missing_left == 0
            && b.missing_right == 0
            && b.mismatched == 0
            && b.uncomparable == 0
            && b.totals_delta.is_empty();
        if !cert.pass || !clean {
            return Ok(v.fail(
                GateCheck::Certificate,
                GateFailure::CertificateFailed {
                    certificate: receipt.certificate,
                    pass: cert.pass,
                    missing_left: b.missing_left,
                    missing_right: b.missing_right,
                    mismatched: b.mismatched,
                    uncomparable: b.uncomparable,
                    totals_delta: b.totals_delta.len() as u64,
                },
            ));
        }
        if cert.proposal != Some(receipt.proposal) {
            return Ok(v.fail(
                GateCheck::Certificate,
                GateFailure::OtherProposal {
                    record: receipt.certificate,
                    kind: RecordKind::Certificate,
                    names: cert.proposal,
                    proposal: receipt.proposal,
                },
            ));
        }
        if replay.proposal != receipt.proposal {
            return Ok(v.fail(
                GateCheck::Certificate,
                GateFailure::OtherProposal {
                    record: receipt.replay,
                    kind: RecordKind::Replay,
                    names: Some(replay.proposal),
                    proposal: receipt.proposal,
                },
            ));
        }
        if cert.provenance.manifest_version.as_deref() != Some(receipt.manifest_before.as_str()) {
            return Ok(v.fail(
                GateCheck::Certificate,
                GateFailure::StaleManifest {
                    record: receipt.certificate,
                    kind: RecordKind::Certificate,
                    computed: cert.provenance.manifest_version.clone(),
                    manifest_before: receipt.manifest_before.clone(),
                },
            ));
        }
        if replay.manifest_version != receipt.manifest_before {
            return Ok(v.fail(
                GateCheck::Certificate,
                GateFailure::StaleManifest {
                    record: receipt.replay,
                    kind: RecordKind::Replay,
                    computed: Some(replay.manifest_version.clone()),
                    manifest_before: receipt.manifest_before.clone(),
                },
            ));
        }
        v.passed(GateCheck::Certificate);

        // 4. coverage
        match &cert.coverage {
            Coverage::Full => {}
            Coverage::Workload { workload, .. } if *workload == req.workload => {}
            Coverage::Workload { workload, .. } => {
                return Ok(v.fail(
                    GateCheck::Coverage,
                    GateFailure::WrongWorkload {
                        certificate: receipt.certificate,
                        covered: *workload,
                        required: req.workload,
                    },
                ))
            }
            other => {
                return Ok(v.fail(
                    GateCheck::Coverage,
                    GateFailure::PartialCoverage {
                        certificate: receipt.certificate,
                        coverage: other.clone(),
                    },
                ))
            }
        }
        v.passed(GateCheck::Coverage);

        // 5. data: the side of the certificate that is the step's output.
        let rewrite = matches!(
            records.proposal.as_ref().map(|p| &p.body),
            Some(ProposalBody::Rewrite { .. })
        );
        let side = usize::from(rewrite);
        let certified = cert.provenance.inputs.get(side).copied();
        let current = if root == self.root() {
            let pinned = self.pin()?;
            crate::fix::data_digest(&root, &*self.source(pinned.as_ref())?)?
        } else {
            crate::fix::data_digest(&root, &*self.output_source(&root)?)?
        };
        if certified != Some(current) {
            return Ok(v.fail(
                GateCheck::Data,
                GateFailure::DataChanged {
                    certificate: receipt.certificate,
                    certified,
                    current,
                    root,
                },
            ));
        }
        v.passed(GateCheck::Data);

        // 6. signature: the receipt's, then its certificate's.
        if trusted.is_some() {
            let f = v.facts.as_ref();
            let verdicts = [
                (
                    *hash,
                    RecordKind::Receipt,
                    f.and_then(|f| f.signature.clone()),
                ),
                (
                    receipt.certificate,
                    RecordKind::Certificate,
                    f.and_then(|f| f.certificate_signature.clone()),
                ),
            ];
            for (record, kind, verdict) in verdicts {
                let failure = match verdict {
                    Some(Verdict::Signed { .. }) => None,
                    None | Some(Verdict::Unsigned) => Some(GateFailure::Unsigned { record, kind }),
                    Some(Verdict::Untrusted { key_ids, .. }) => Some(GateFailure::UntrustedKey {
                        record,
                        kind,
                        key_ids,
                    }),
                    Some(Verdict::Invalid { invalid, .. }) => Some(GateFailure::InvalidSignature {
                        record,
                        kind,
                        key_ids: invalid.iter().map(|i| i.key_id).collect(),
                    }),
                };
                if let Some(f) = failure {
                    return Ok(v.fail(GateCheck::Signature, f));
                }
            }
            v.passed(GateCheck::Signature);
        }
        v.pass = true;
        Ok(v)
    }
}

/// The comment's facts, from the receipt's records as far as they resolve.
fn facts<S>(
    ledger: &Ledger<S>,
    receipt: &Receipt,
    records: &Records,
    trusted: Option<&bellwether_ledger::TrustedKeys>,
    hash: &Hash,
) -> Result<ReceiptFacts>
where
    S: std::ops::Deref,
    S::Target: bellwether_types::Store,
{
    let (change, spec) = match records.proposal.as_ref().map(|p| &p.body) {
        Some(ProposalBody::Key { spec, .. }) => (Some(ChangeKind::Key), Some(spec.clone())),
        Some(ProposalBody::Layout { .. }) => (Some(ChangeKind::Layout), None),
        Some(ProposalBody::Rewrite { .. }) => (Some(ChangeKind::Rewrite), None),
        None => (None, None),
    };
    let realized = match &records.replay {
        Some(r) if !receipt.attest_only => realized(ledger, receipt, r)?,
        _ => None,
    };
    let (signature, certificate_signature) = match trusted {
        Some(t) => (
            Some(ledger.verify_against(hash, t)?),
            match &records.certificate {
                Some(_) => Some(ledger.verify_against(&receipt.certificate, t)?),
                None => None,
            },
        ),
        None => (None, None),
    };
    Ok(ReceiptFacts {
        proposal: receipt.proposal,
        change,
        spec,
        attest_only: receipt.attest_only,
        manifest_before: receipt.manifest_before.clone(),
        manifest_after: receipt.manifest_after.clone(),
        certificate: records.certificate.as_ref().map(|c| CertificateFacts {
            certificate: receipt.certificate,
            pass: c.pass,
            coverage: c.coverage.clone(),
        }),
        replay: records.replay.as_ref().map(|r| ReplayTotals {
            replay: receipt.replay,
            current: total(&r.current),
            proposed: total(&r.proposed),
        }),
        realized,
        signature,
        certificate_signature,
    })
}

/// The records a receipt names, each when it resolves to its kind.
struct Records {
    proposal: Option<bellwether_types::Proposal>,
    replay: Option<Replay>,
    certificate: Option<Certificate>,
}

impl Records {
    fn read<S>(ledger: &Ledger<S>, r: &Receipt) -> Result<Self>
    where
        S: std::ops::Deref,
        S::Target: bellwether_types::Store,
    {
        Ok(Self {
            proposal: match resolve(ledger, &r.proposal)? {
                Some(Record::Proposal(p)) => Some(p),
                _ => None,
            },
            replay: match resolve(ledger, &r.replay)? {
                Some(Record::Replay(x)) => Some(x),
                _ => None,
            },
            certificate: match resolve(ledger, &r.certificate)? {
                Some(Record::Certificate(c)) => Some(c),
                _ => None,
            },
        })
    }
}

/// A record, or `None` when it is absent or unreadable; a store failure is
/// an error.
fn resolve<S>(ledger: &Ledger<S>, hash: &Hash) -> Result<Option<Record>>
where
    S: std::ops::Deref,
    S::Target: bellwether_types::Store,
{
    match ledger.get(hash) {
        Ok(r) => Ok(Some(r)),
        Err(Error::NotFound(_)) => Ok(None),
        Err(e) if unreadable(&e).is_some() => Ok(None),
        Err(e) => Err(e),
    }
}

/// The newest replay of the receipt's proposal and workload at its
/// `manifest_after` (ties: the smaller hash).
fn realized<S>(
    ledger: &Ledger<S>,
    receipt: &Receipt,
    replay: &Replay,
) -> Result<Option<ReplayTotals>>
where
    S: std::ops::Deref,
    S::Target: bellwether_types::Store,
{
    let mut best: Option<(Hash, Replay)> = None;
    for h in ledger.list(RecordKind::Replay)? {
        let h = h?;
        let Some(Record::Replay(r)) = resolve(ledger, &h)? else {
            continue;
        };
        if r.proposal != receipt.proposal
            || r.workload != replay.workload
            || r.manifest_version != receipt.manifest_after
        {
            continue;
        }
        let newer = best.as_ref().is_none_or(|(bh, b)| {
            (r.created_at.as_str(), std::cmp::Reverse(h))
                > (b.created_at.as_str(), std::cmp::Reverse(*bh))
        });
        if newer {
            best = Some((h, r));
        }
    }
    Ok(best.map(|(h, r)| ReplayTotals {
        replay: h,
        current: total(&r.current),
        proposed: total(&r.proposed),
    }))
}

/// The reason a ledger read failed for the record itself (bytes that do
/// not hash to the name, a malformed or newer record), escaped; `None` for
/// a store failure.
fn unreadable(e: &Error) -> Option<String> {
    match e {
        Error::UnsupportedVersion { .. }
        | Error::UnknownKind { .. }
        | Error::LedgerRule(_)
        | Error::Serde(_)
        | Error::Other(_) => Some(printable(&e.to_string())),
        _ => None,
    }
}

/// `root`, canonical; a root that cannot be opened is the caller's to fix.
fn canonical_root(root: &str) -> Result<String> {
    let c = std::fs::canonicalize(root).map_err(|e| Error::InvalidArgument {
        field: "root",
        reason: format!("cannot open the step's root {root:?} ({e}); name an existing folder"),
    })?;
    c.to_str()
        .map(ToString::to_string)
        .ok_or_else(|| Error::InvalidArgument {
            field: "root",
            reason: format!("the step's root {} is not valid UTF-8", c.display()),
        })
}
