//! Machine-actionable refusals (WP12 addition 10): every refusal carries a
//! stable `code`, the facts an agent needs to act, and one `next` line.
//!
//! Codes come from the facade's typed refusals and typed errors by
//! exhaustive matches, so a new variant fails to compile here rather than
//! falling through to a guess. Nothing is classified by reading a message.

use bellwether::types::Error;
use bellwether::{ProposeRefusal, Reconciliation, Refusal as FixRefusal};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// How many mismatching keys a `certificate_failed` refusal carries.
pub const MISMATCH_SAMPLE: usize = 10;

/// A stable refusal code. `AGENTS.md` lists every one with its meaning, and
/// a test holds the two to the same set.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RefusalCode {
    /// The certificate found differences.
    CertificateFailed,
    /// The certificate was computed against an older manifest.
    StaleManifest,
    /// The data changed after verification.
    DataChanged,
    /// Another commit landed during the apply.
    ConcurrentCommit,
    /// The key is already complete.
    NothingToApply,
    /// Layout proposals cannot be applied in this release.
    LayoutNotApplicable,
    /// A rewrite cannot be attested yet.
    NoReplay,
    /// The proposer name is reserved for the engine.
    ReservedProposer,
    /// Scoring needs keys, a workload, or rewrite outputs.
    NoWorkload,
    /// The folder changed since the index was built.
    StaleIndex,
    /// The hash is a record of another kind.
    WrongKind,
    /// The hash is not in the ledger.
    NotFound,
    /// The arguments do not match the tool's schema.
    InvalidInput,
    /// The key spec or a column is not valid.
    InvalidSpec,
    /// The session's budget is spent.
    BudgetExceeded,
    /// No job has that id.
    JobNotFound,
    /// The job has not finished.
    JobRunning,
    /// No tool has that name.
    UnknownTool,
    /// A data file could not be read.
    SourceError,
    /// The index store could not be read or written.
    StoreError,
    /// The ledger refused a record or a link.
    LedgerRule,
    /// A concurrent writer won a commit race.
    VersionConflict,
    /// A structure is newer than this build.
    UnsupportedVersion,
    /// A kind this build does not know.
    UnknownKind,
    /// A segment is being garbage-collected.
    SegmentClaimed,
    /// Any other engine error.
    EngineError,
}

impl RefusalCode {
    /// Every code, in `AGENTS.md` order.
    pub const ALL: [Self; 26] = [
        Self::CertificateFailed,
        Self::StaleManifest,
        Self::DataChanged,
        Self::ConcurrentCommit,
        Self::NothingToApply,
        Self::LayoutNotApplicable,
        Self::NoReplay,
        Self::ReservedProposer,
        Self::NoWorkload,
        Self::StaleIndex,
        Self::WrongKind,
        Self::NotFound,
        Self::InvalidInput,
        Self::InvalidSpec,
        Self::BudgetExceeded,
        Self::JobNotFound,
        Self::JobRunning,
        Self::UnknownTool,
        Self::SourceError,
        Self::StoreError,
        Self::LedgerRule,
        Self::VersionConflict,
        Self::UnsupportedVersion,
        Self::UnknownKind,
        Self::SegmentClaimed,
        Self::EngineError,
    ];

    /// The code as it is serialized.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CertificateFailed => "certificate_failed",
            Self::StaleManifest => "stale_manifest",
            Self::DataChanged => "data_changed",
            Self::ConcurrentCommit => "concurrent_commit",
            Self::NothingToApply => "nothing_to_apply",
            Self::LayoutNotApplicable => "layout_not_applicable",
            Self::NoReplay => "no_replay",
            Self::ReservedProposer => "reserved_proposer",
            Self::NoWorkload => "no_workload",
            Self::StaleIndex => "stale_index",
            Self::WrongKind => "wrong_kind",
            Self::NotFound => "not_found",
            Self::InvalidInput => "invalid_input",
            Self::InvalidSpec => "invalid_spec",
            Self::BudgetExceeded => "budget_exceeded",
            Self::JobNotFound => "job_not_found",
            Self::JobRunning => "job_running",
            Self::UnknownTool => "unknown_tool",
            Self::SourceError => "source_error",
            Self::StoreError => "store_error",
            Self::LedgerRule => "ledger_rule",
            Self::VersionConflict => "version_conflict",
            Self::UnsupportedVersion => "unsupported_version",
            Self::UnknownKind => "unknown_kind",
            Self::SegmentClaimed => "segment_claimed",
            Self::EngineError => "engine_error",
        }
    }

    /// The one-line instruction an agent acts on.
    #[must_use]
    pub fn next(self) -> &'static str {
        match self {
            Self::CertificateFailed => {
                "do not apply; read the mismatches, propose differently, or ask the human"
            }
            Self::StaleManifest | Self::DataChanged => {
                "call score.verify again on the proposal, then apply.receipt with the new \
                 certificate"
            }
            Self::ConcurrentCommit => {
                "tell the human: the key is built and serves lookups but is not attested; do not \
                 retry"
            }
            Self::NothingToApply => "the key is already complete; there is nothing to attest",
            Self::LayoutNotApplicable => {
                "layout proposals cannot be applied in this release; propose a key or a rewrite"
            }
            Self::NoReplay => {
                "rewrites cannot be attested yet; report the passing certificate to the human"
            }
            Self::ReservedProposer => "pass your own agent name in `by`",
            Self::NoWorkload => {
                "pass `keys` (a Parquet key file) or a recorded `workload` hash for a key, or the \
                 rewrite outputs for a rewrite"
            }
            Self::StaleIndex => {
                "ask the human to run `bellwether index` on the data folder, then call the tool \
                 again"
            }
            Self::WrongKind => {
                "pass the hash of the kind the tool takes, as an earlier step printed it"
            }
            Self::NotFound => {
                "pass a hash this server printed; hashes from another index do not resolve"
            }
            Self::InvalidInput => {
                "fix the arguments to match the tool's input schema; send no extra fields"
            }
            Self::InvalidSpec => "use a column the dataset has and a valid key spec",
            Self::BudgetExceeded => "stop and ask the human; this session's budget is spent",
            Self::JobNotFound => "pass a job id jobs.start or a tool returned in this session",
            Self::JobRunning => "poll jobs.status until the job is done, then call jobs.result",
            Self::UnknownTool => "call one of the tools the server lists",
            Self::SourceError => "check the data files named in the message, or ask the human",
            Self::StoreError => "check the index location named in the message, or ask the human",
            Self::LedgerRule => "read the message; pass the record the tool expects",
            Self::VersionConflict => "retry once",
            Self::UnsupportedVersion | Self::UnknownKind => {
                "ask the human to upgrade bellwether; this build cannot read the index"
            }
            Self::SegmentClaimed => "retry later",
            Self::EngineError => "read the message; it says what to do",
        }
    }
}

/// A refusal: nothing was written (or, for `certificate_failed`, only the
/// failing certificate, which is a legitimate record).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Refusal {
    /// The stable code.
    pub code: RefusalCode,
    /// One line saying what happened.
    pub message: String,
    /// One line saying what to do next.
    pub next: String,
    /// The facts an agent needs to act (an object; per code).
    pub fields: Value,
}

impl Refusal {
    /// A refusal with `code`'s standard `next` line.
    #[must_use]
    pub fn new(code: RefusalCode, message: impl Into<String>, fields: Value) -> Self {
        Self {
            code,
            message: message.into(),
            next: code.next().to_string(),
            fields,
        }
    }
}

/// A facade error as a refusal, by its variant.
#[must_use]
pub fn from_error(e: &Error) -> Refusal {
    let message = e.to_string();
    let (code, fields) = match e {
        Error::StaleIndex {
            untracked,
            changed,
            missing,
            ..
        } => (
            RefusalCode::StaleIndex,
            json!({ "untracked": untracked, "changed": changed, "missing": missing }),
        ),
        Error::WrongKind {
            hash,
            expected,
            found,
            next,
        } => {
            return Refusal {
                code: RefusalCode::WrongKind,
                message,
                next: next.clone(),
                fields: json!({ "hash": hash, "expected": expected, "found": found }),
            }
        }
        Error::NotFound(h) => (RefusalCode::NotFound, json!({ "hash": h })),
        Error::LedgerRule(_) => (RefusalCode::LedgerRule, json!({})),
        Error::KeySpec(_) => (RefusalCode::InvalidSpec, json!({})),
        Error::UnknownKind { what, found } => (
            RefusalCode::UnknownKind,
            json!({ "what": what, "found": found }),
        ),
        Error::UnsupportedVersion {
            what,
            found,
            supported,
        } => (
            RefusalCode::UnsupportedVersion,
            json!({ "what": what, "found": found, "supported": supported }),
        ),
        Error::SegmentClaimed(h) => (RefusalCode::SegmentClaimed, json!({ "segment": h })),
        Error::VersionConflict { expected, found } => (
            RefusalCode::VersionConflict,
            json!({ "expected": expected, "found": found }),
        ),
        Error::Source(_) => (RefusalCode::SourceError, json!({})),
        Error::Store(_) => (RefusalCode::StoreError, json!({})),
        Error::Serde(_) | Error::Other(_) => (RefusalCode::EngineError, json!({})),
    };
    Refusal::new(code, message, fields)
}

/// A typed `propose` refusal.
#[must_use]
pub fn from_propose(r: &ProposeRefusal) -> Refusal {
    match r {
        ProposeRefusal::ReservedProposer { name } => Refusal::new(
            RefusalCode::ReservedProposer,
            format!("the proposer name {name:?} is reserved for the engine's own candidates"),
            json!({ "name": name }),
        ),
    }
}

/// A typed `fix` refusal, with the first mismatching keys when the verify
/// stage ran in this call (`apply.receipt` re-checks a stored certificate,
/// which carries counts only).
#[must_use]
pub fn from_fix(r: &FixRefusal, verification: Option<&Reconciliation>) -> Refusal {
    match r {
        FixRefusal::CertificateFailed {
            certificate,
            mismatched,
            missing_left,
            missing_right,
            totals_differ,
        } => {
            let sample: Vec<Value> = verification
                .map(|v| {
                    v.mismatches
                        .iter()
                        .take(MISMATCH_SAMPLE)
                        .map(|m| serde_json::to_value(m).unwrap_or(Value::Null))
                        .collect()
                })
                .unwrap_or_default();
            Refusal::new(
                RefusalCode::CertificateFailed,
                format!(
                    "certificate {certificate} failed: {mismatched} mismatched, {missing_left} \
                     missing on the left, {missing_right} missing on the right, {totals_differ} \
                     group totals differ"
                ),
                json!({
                    "certificate": certificate,
                    "mismatched": mismatched,
                    "missing_left": missing_left,
                    "missing_right": missing_right,
                    "totals_differ": totals_differ,
                    "mismatches_sample": sample,
                }),
            )
        }
        FixRefusal::StaleManifest {
            certificate,
            certified,
            current,
        } => Refusal::new(
            RefusalCode::StaleManifest,
            format!(
                "certificate {certificate} was computed against manifest {certified}; the \
                 current manifest is {current}"
            ),
            json!({ "certificate": certificate, "certified": certified, "current": current }),
        ),
        FixRefusal::ConcurrentCommit {
            expected,
            committed,
        } => Refusal::new(
            RefusalCode::ConcurrentCommit,
            format!("the apply committed {committed}, not {expected}; another commit landed first"),
            json!({ "expected": expected, "committed": committed }),
        ),
        FixRefusal::DataChanged {
            certificate,
            certified,
            current,
        } => Refusal::new(
            RefusalCode::DataChanged,
            format!("the data changed after certificate {certificate} was computed"),
            json!({ "certificate": certificate, "certified": certified, "current": current }),
        ),
        FixRefusal::NothingToApply { sequence } => Refusal::new(
            RefusalCode::NothingToApply,
            format!("applying the key changed nothing at manifest {sequence}"),
            json!({ "sequence": sequence }),
        ),
        FixRefusal::LayoutNotApplicable => Refusal::new(
            RefusalCode::LayoutNotApplicable,
            "layout proposals cannot be applied in this release",
            json!({}),
        ),
        FixRefusal::NoReplay { certificate } => Refusal::new(
            RefusalCode::NoReplay,
            format!("certificate {certificate} passed, but a rewrite has no replay to attest"),
            json!({ "certificate": certificate }),
        ),
    }
}
