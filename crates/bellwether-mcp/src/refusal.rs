//! Machine-actionable refusals (WP12 addition 10): every refusal carries a
//! stable `code`, the facts an agent needs to act, and one `next` line.
//!
//! Codes come from the facade's typed refusals and typed errors by
//! exhaustive matches, so a new variant fails to compile here rather than
//! falling through to a guess. Nothing is classified by reading a message.
//! A `next` line names MCP tools and fields, never a CLI flag: the facade's
//! typed errors carry the field or the record kind, and the line is built
//! from that here.

use bellwether::types::{Error, HashKind};
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
    /// The proposal's replay predicts no saving on the workload.
    NothingToGain,
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
    /// The lookup needs an index the folder does not have.
    NotIndexed,
    /// The hash is a record of another kind.
    WrongKind,
    /// The hash is not in the ledger.
    NotFound,
    /// The arguments do not match the tool's schema.
    InvalidInput,
    /// An argument matches the schema but cannot be used.
    InvalidArgument,
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
    pub const ALL: [Self; 29] = [
        Self::CertificateFailed,
        Self::StaleManifest,
        Self::DataChanged,
        Self::ConcurrentCommit,
        Self::NothingToApply,
        Self::NothingToGain,
        Self::LayoutNotApplicable,
        Self::NoReplay,
        Self::ReservedProposer,
        Self::NoWorkload,
        Self::StaleIndex,
        Self::NotIndexed,
        Self::WrongKind,
        Self::NotFound,
        Self::InvalidInput,
        Self::InvalidArgument,
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
            Self::NothingToGain => "nothing_to_gain",
            Self::LayoutNotApplicable => "layout_not_applicable",
            Self::NoReplay => "no_replay",
            Self::ReservedProposer => "reserved_proposer",
            Self::NoWorkload => "no_workload",
            Self::StaleIndex => "stale_index",
            Self::NotIndexed => "not_indexed",
            Self::WrongKind => "wrong_kind",
            Self::NotFound => "not_found",
            Self::InvalidInput => "invalid_input",
            Self::InvalidArgument => "invalid_argument",
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
            Self::NothingToGain => {
                "the replay predicts no saving on this workload; propose another key, or pass \
                 allow_no_gain only if the human asks for it anyway"
            }
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
            Self::NotIndexed => {
                "ask the human to index the data folder with a key spec (`bellwether index`), \
                 then call the tool again"
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
            Self::InvalidArgument => "fix the argument `fields.field` names, then call again",
            Self::InvalidSpec => "use a column the dataset has and a valid key spec",
            Self::BudgetExceeded => "stop and ask the human; this session's budget is spent",
            Self::JobNotFound => "pass a job id jobs.start or a tool returned in this session",
            Self::JobRunning => "poll jobs.status until the job is done, then call jobs.result",
            Self::UnknownTool => "call one of the tools the server lists",
            Self::SourceError => "check the data files named in the message, or ask the human",
            Self::StoreError => "check the index location named in the message, or ask the human",
            Self::LedgerRule => "read the message; pass the record the tool expects",
            Self::VersionConflict => "retry once, with `wait: true`",
            Self::UnsupportedVersion | Self::UnknownKind => {
                "ask the human to upgrade bellwether; this build cannot read the index"
            }
            Self::SegmentClaimed => "retry later, with `wait: true`",
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

/// The MCP field a hash of `kind` is passed in, and how an agent gets one.
fn hash_field(kind: HashKind) -> (&'static str, &'static str) {
    match kind {
        HashKind::Evidence => (
            "evidence",
            "cite in `evidence` a hash observe.why or observe.profile returned for this data \
             folder",
        ),
        HashKind::Proposal => (
            "proposal",
            "pass in `proposal` a hash propose.key, propose.layout, propose.rewrite, or score.why \
             returned",
        ),
        HashKind::Certificate => (
            "certificate",
            "pass in `certificate` the certificate hash score.verify returned",
        ),
        HashKind::Receipt => ("receipt", "pass a receipt hash apply.receipt returned"),
        HashKind::Workload => (
            "workload",
            "pass in `workload` the `workload.hash` score.why returned, or call score.why with \
             `keys` to record one",
        ),
    }
}

/// The `next` line of a typed error that names the argument to fix, in MCP
/// field names; `None` for an error whose code's standard line fits.
fn typed_next(e: &Error) -> Option<String> {
    match e {
        Error::WrongKind {
            expected, found, ..
        } => Some(format!("{}, not {found}", hash_field(*expected).1)),
        Error::RecordNotFound { kind, .. } => Some(hash_field(*kind).1.to_string()),
        Error::NoWorkload { field, .. } => match *field {
            "workload" => Some(
                "pass `workload`: the `workload.hash` score.why returned for the keys the slow \
                 lookup used"
                    .into(),
            ),
            "rewrite" => Some(
                "pass `rewrite` with the `before` and `after` output folders and the `key` \
                 columns to compare them by"
                    .into(),
            ),
            _ => None,
        },
        Error::NotIndexed { needs, .. } => {
            let spec = needs
                .as_deref()
                .map_or_else(|| "a key spec".to_string(), |s| format!("the key spec {s}"));
            Some(format!(
                "ask the human to index the data folder with {spec} (`bellwether index`), then \
                 call the tool again"
            ))
        }
        Error::InvalidArgument { field, .. } => Some(format!(
            "fix `{field}` as the message says, then call the tool again (with `wait: true` if \
             the arguments themselves are unchanged)"
        )),
        _ => None,
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
        } => (
            RefusalCode::WrongKind,
            json!({
                "hash": hash,
                "field": hash_field(*expected).0,
                "expected": expected.with_article(),
                "found": found,
            }),
        ),
        Error::RecordNotFound { kind, hash, .. } => (
            RefusalCode::NotFound,
            json!({ "hash": hash, "kind": kind.as_str(), "field": hash_field(*kind).0 }),
        ),
        Error::NoWorkload {
            proposal, field, ..
        } => (
            RefusalCode::NoWorkload,
            json!({ "proposal": proposal, "field": field }),
        ),
        Error::NotIndexed {
            root,
            operation,
            needs,
        } => (
            RefusalCode::NotIndexed,
            json!({ "root": root, "operation": operation, "needs": needs }),
        ),
        Error::InvalidArgument { field, .. } => {
            (RefusalCode::InvalidArgument, json!({ "field": field }))
        }
        Error::NotFound(h) => (RefusalCode::NotFound, json!({ "hash": h })),
        Error::LedgerRule(_) => (RefusalCode::LedgerRule, json!({})),
        Error::KeySpec(_) => (RefusalCode::InvalidSpec, json!({})),
        Error::UnknownColumn {
            column, columns, ..
        } => (
            RefusalCode::InvalidSpec,
            json!({ "column": column, "columns": columns }),
        ),
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
        // No wildcard arm: a new `Error` variant must fail to compile here.
        Error::Serde(_) | Error::Other(_) => (RefusalCode::EngineError, json!({})),
    };
    let mut r = Refusal::new(code, message, fields);
    if let Some(next) = typed_next(e) {
        r.next = next;
    }
    r
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
            format!(
                "the key is already complete at manifest {sequence}, so applying it changes \
                 nothing"
            ),
            json!({ "sequence": sequence }),
        ),
        FixRefusal::NothingToGain {
            replay,
            current_bytes,
            proposed_bytes,
        } => Refusal::new(
            RefusalCode::NothingToGain,
            format!(
                "replay {replay} predicts {proposed_bytes} bytes with the proposal against \
                 {current_bytes} now: applying it saves nothing"
            ),
            json!({
                "replay": replay,
                "current_bytes": current_bytes,
                "proposed_bytes": proposed_bytes,
            }),
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::too_many_lines)]
mod tests {
    use bellwether::types::Hash;

    use super::*;

    /// No `next` line or message built here names a CLI flag.
    fn no_flags(r: &Refusal) {
        assert!(!r.next.contains("--"), "{r:?}");
    }

    #[test]
    fn every_facade_error_maps_to_its_code() {
        let h = Hash::of(b"h");
        let serde = serde_json::from_str::<u8>("x").unwrap_err();
        let cases: Vec<(Error, RefusalCode)> = vec![
            (
                Error::UnsupportedVersion {
                    what: "storage",
                    found: 9,
                    supported: 1,
                },
                RefusalCode::UnsupportedVersion,
            ),
            (Error::NotFound(h), RefusalCode::NotFound),
            (Error::LedgerRule("r".into()), RefusalCode::LedgerRule),
            (Error::KeySpec("k".into()), RefusalCode::InvalidSpec),
            (
                Error::UnknownKind {
                    what: "key spec kind",
                    found: "x".into(),
                },
                RefusalCode::UnknownKind,
            ),
            (Error::SegmentClaimed(h), RefusalCode::SegmentClaimed),
            (
                Error::VersionConflict {
                    expected: "1".into(),
                    found: "2".into(),
                },
                RefusalCode::VersionConflict,
            ),
            (Error::Source("s".into()), RefusalCode::SourceError),
            (Error::Store("s".into()), RefusalCode::StoreError),
            (
                Error::StaleIndex {
                    untracked: vec!["a".into()],
                    changed: vec!["b".into()],
                    missing: vec!["c".into()],
                    message: "m".into(),
                },
                RefusalCode::StaleIndex,
            ),
            (
                Error::WrongKind {
                    hash: h,
                    expected: HashKind::Certificate,
                    found: "a proposal".into(),
                },
                RefusalCode::WrongKind,
            ),
            (
                Error::RecordNotFound {
                    kind: HashKind::Workload,
                    hash: h,
                    place: "the cache at x".into(),
                },
                RefusalCode::NotFound,
            ),
            (
                Error::NoWorkload {
                    proposal: "key",
                    field: "workload",
                    needs: "a recorded workload",
                },
                RefusalCode::NoWorkload,
            ),
            (
                Error::NotIndexed {
                    root: "/d".into(),
                    operation: "locate",
                    needs: None,
                },
                RefusalCode::NotIndexed,
            ),
            (
                Error::InvalidArgument {
                    field: "at",
                    reason: "r".into(),
                },
                RefusalCode::InvalidArgument,
            ),
            (Error::Serde(serde), RefusalCode::EngineError),
            (Error::Other("o".into()), RefusalCode::EngineError),
            (
                Error::UnknownColumn {
                    column: "nope".into(),
                    root: "/d".into(),
                    columns: vec!["id".into(), "ts".into()],
                },
                RefusalCode::InvalidSpec,
            ),
        ];
        for (e, code) in &cases {
            let r = from_error(e);
            assert_eq!(r.code, *code, "{e:?}");
            assert_eq!(r.message, e.to_string());
            assert!(r.fields.is_object(), "{e:?}");
            no_flags(&r);
        }
        // The facts each typed error carries.
        let f = |i: usize| from_error(&cases[i].0).fields;
        assert_eq!(f(1)["hash"], h.to_hex());
        assert_eq!(f(9)["untracked"][0], "a");
        assert_eq!(f(9)["changed"][0], "b");
        assert_eq!(f(9)["missing"][0], "c");
        assert_eq!(f(10)["field"], "certificate");
        assert_eq!(f(10)["expected"], "a certificate");
        assert_eq!(f(10)["found"], "a proposal");
        assert_eq!(f(11)["kind"], "workload");
        assert_eq!(f(11)["field"], "workload");
        assert_eq!(f(12)["field"], "workload");
        assert_eq!(f(13)["operation"], "locate");
        assert_eq!(f(14)["field"], "at");
        assert_eq!(f(17)["column"], "nope");
        assert_eq!(f(17)["columns"], json!(["id", "ts"]));
        // Next lines name the MCP field to fix.
        assert!(from_error(&cases[10].0).next.contains("`certificate`"));
        assert!(from_error(&cases[11].0).next.contains("`workload`"));
        assert!(from_error(&cases[12].0).next.contains("`workload`"));
        assert!(from_error(&cases[14].0).next.contains("`at`"));
        let rewrite = from_error(&Error::NoWorkload {
            proposal: "rewrite",
            field: "rewrite",
            needs: "outputs",
        });
        assert!(rewrite.next.contains("`rewrite`"), "{rewrite:?}");
        let temporal = from_error(&Error::NotIndexed {
            root: "/d".into(),
            operation: "asof",
            needs: Some("temporal:id@<timestamp column>".into()),
        });
        assert!(temporal.next.contains("temporal:id@"), "{temporal:?}");
        for kind in [
            HashKind::Evidence,
            HashKind::Proposal,
            HashKind::Certificate,
            HashKind::Receipt,
            HashKind::Workload,
        ] {
            let r = from_error(&Error::RecordNotFound {
                kind,
                hash: h,
                place: "p".into(),
            });
            assert!(r.next.contains(kind.as_str()), "{r:?}");
            no_flags(&r);
        }
    }

    #[test]
    fn every_fix_and_propose_refusal_maps_to_its_code() {
        let h = Hash::of(b"c");
        let cases = [
            (
                FixRefusal::CertificateFailed {
                    certificate: h,
                    mismatched: 1,
                    missing_left: 2,
                    missing_right: 3,
                    totals_differ: 4,
                },
                RefusalCode::CertificateFailed,
            ),
            (
                FixRefusal::StaleManifest {
                    certificate: h,
                    certified: "1".into(),
                    current: "2".into(),
                },
                RefusalCode::StaleManifest,
            ),
            (
                FixRefusal::ConcurrentCommit {
                    expected: 2,
                    committed: 3,
                },
                RefusalCode::ConcurrentCommit,
            ),
            (
                FixRefusal::DataChanged {
                    certificate: h,
                    certified: None,
                    current: h,
                },
                RefusalCode::DataChanged,
            ),
            (
                FixRefusal::NothingToApply {
                    sequence: "3".into(),
                },
                RefusalCode::NothingToApply,
            ),
            (
                FixRefusal::NothingToGain {
                    replay: h,
                    current_bytes: 10,
                    proposed_bytes: 10,
                },
                RefusalCode::NothingToGain,
            ),
            (
                FixRefusal::LayoutNotApplicable,
                RefusalCode::LayoutNotApplicable,
            ),
            (
                FixRefusal::NoReplay { certificate: h },
                RefusalCode::NoReplay,
            ),
        ];
        for (r, code) in &cases {
            let m = from_fix(r, None);
            assert_eq!(m.code, *code, "{r:?}");
            assert_eq!(m.next, code.next());
            no_flags(&m);
        }
        let failed = from_fix(&cases[0].0, None).fields;
        assert_eq!(
            (
                &failed["mismatched"],
                &failed["missing_left"],
                &failed["missing_right"],
                &failed["totals_differ"]
            ),
            (&json!(1), &json!(2), &json!(3), &json!(4))
        );
        assert_eq!(from_fix(&cases[2].0, None).fields["committed"], 3);
        assert_eq!(from_fix(&cases[4].0, None).fields["sequence"], "3");
        let p = from_propose(&ProposeRefusal::ReservedProposer {
            name: "engine".into(),
        });
        assert_eq!(p.code, RefusalCode::ReservedProposer);
        assert_eq!(p.fields["name"], "engine");
    }

    #[test]
    fn codes_serialize_as_their_names() {
        for c in RefusalCode::ALL {
            assert_eq!(serde_json::to_value(c).unwrap(), c.as_str());
            assert!(!c.next().contains("--"), "{c:?}");
        }
    }
}
