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
    /// The proposal verified and targets a table: the engine applies
    /// nothing to a table; a human runs what `fields.run` lists.
    AttestOnly,
    /// The proposer name is reserved for the engine.
    ReservedProposer,
    /// The key declares a sidecar, which no proposal can carry yet.
    SketchNotProposable,
    /// Scoring needs keys, a workload, or rewrite outputs.
    NoWorkload,
    /// The folder changed since the index was built.
    StaleIndex,
    /// The lookup needs an index the folder does not have.
    NotIndexed,
    /// The pinned manifest sequence is not retained.
    NotRetained,
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
    /// A table changed during a run that held no snapshot.
    SourceChanged,
    /// The operation needs a folder of files and the data root is a table.
    NotAFolder,
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
    /// A query engine refused to export a rewrite's results: no
    /// certificate, no receipt; `fields.permission` names what to grant.
    ExportDenied,
    /// The arguments carried a score, cost, measurement, replay, verdict, or
    /// certificate: a caller never supplies one.
    CallerSuppliedCost,
    /// A query engine's log entry for a run cannot stand as its
    /// measurement (`fields.reason`: served from a cache, a statistic
    /// missing, not the query submitted, several entries, or rows that
    /// are not the rows exported); nothing was recorded.
    MeasurementRefused,
    /// The apply tier measured a rewrite of engine queries again and a
    /// query's numbers disagree with the certified replay's; nothing was
    /// written.
    MeasurementChanged,
    /// A join key's pair index is stale (B3): its left or right dataset
    /// changed since it was built; `fields.side` names which.
    StaleJoin,
    /// A join key (B3) names a right dataset this server does not serve:
    /// only the operator's `--join` is read; `fields.served` names it (null
    /// without `--join`). Refused before any folder is read.
    JoinNotServed,
}

impl RefusalCode {
    /// Every code, in `AGENTS.md` order.
    pub const ALL: [Self; 40] = [
        Self::CertificateFailed,
        Self::StaleManifest,
        Self::DataChanged,
        Self::ConcurrentCommit,
        Self::NothingToApply,
        Self::NothingToGain,
        Self::LayoutNotApplicable,
        Self::NoReplay,
        Self::AttestOnly,
        Self::ReservedProposer,
        Self::SketchNotProposable,
        Self::NoWorkload,
        Self::StaleIndex,
        Self::NotIndexed,
        Self::NotRetained,
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
        Self::SourceChanged,
        Self::NotAFolder,
        Self::StoreError,
        Self::LedgerRule,
        Self::VersionConflict,
        Self::UnsupportedVersion,
        Self::UnknownKind,
        Self::SegmentClaimed,
        Self::EngineError,
        Self::ExportDenied,
        Self::CallerSuppliedCost,
        Self::MeasurementRefused,
        Self::MeasurementChanged,
        Self::StaleJoin,
        Self::JoinNotServed,
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
            Self::AttestOnly => "attest_only",
            Self::ReservedProposer => "reserved_proposer",
            Self::SketchNotProposable => "sketch_not_proposable",
            Self::NoWorkload => "no_workload",
            Self::StaleIndex => "stale_index",
            Self::NotIndexed => "not_indexed",
            Self::NotRetained => "not_retained",
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
            Self::SourceChanged => "source_changed",
            Self::NotAFolder => "not_a_folder",
            Self::StoreError => "store_error",
            Self::LedgerRule => "ledger_rule",
            Self::VersionConflict => "version_conflict",
            Self::UnsupportedVersion => "unsupported_version",
            Self::UnknownKind => "unknown_kind",
            Self::SegmentClaimed => "segment_claimed",
            Self::EngineError => "engine_error",
            Self::ExportDenied => "export_denied",
            Self::CallerSuppliedCost => "caller_supplied_cost",
            Self::MeasurementRefused => "measurement_refused",
            Self::MeasurementChanged => "measurement_changed",
            Self::StaleJoin => "stale_join",
            Self::JoinNotServed => "join_not_served",
        }
    }

    /// The one-line instruction an agent acts on.
    #[must_use]
    #[allow(clippy::too_many_lines)]
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
                "the replay predicts no saving on this workload; propose another key, or tell the \
                 human: only the human can continue, with `bellwether fix --allow-no-gain`"
            }
            Self::LayoutNotApplicable => {
                "layout proposals cannot be applied in this release; propose a key or a rewrite"
            }
            Self::NoReplay => {
                "rewrites cannot be attested yet; report the passing certificate to the human"
            }
            Self::AttestOnly => {
                "report the passing certificate to the human with `fields.run` (statements the engine \
                 wrote) or `fields.proposed_rewrite` (your own text, for the human to review); \
                 the engine applies nothing to a table"
            }
            Self::ReservedProposer => "pass your own agent name in `by`",
            Self::SketchNotProposable => {
                "propose a key spec for a receipt, or ask the human to run the index command in \
                 `fields.command` to build the sketch (no receipt)"
            }
            Self::NoWorkload => {
                "pass `keys` (a Parquet key file), a recorded `workload` hash, or `text` lookups \
                 (score.why) for a key, or the rewrite outputs for a rewrite"
            }
            Self::StaleIndex => {
                "ask the human to run `bellwether index` on the data folder, then call the tool \
                 again"
            }
            Self::NotIndexed => {
                "ask the human to index the data folder with a key spec (`bellwether index`), \
                 then call the tool again"
            }
            Self::NotRetained => {
                "pass in `at_manifest` a sequence the index retains (`fields.oldest` to \
                 `fields.current`), or omit it to plan against the current manifest"
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
            Self::SourceError => {
                "check the data source (files or table) named in the message, or ask the human"
            }
            Self::SourceChanged => {
                "call the tool again when the table is quiet; the table changed during the run \
                 and nothing was recorded"
            }
            Self::NotAFolder => {
                "over a table use observe.why, observe.profile, score.why, and score.verify; \
                 lookups and index builds need a folder of files"
            }
            Self::StoreError => "check the index location named in the message, or ask the human",
            Self::LedgerRule => "read the message; pass the record the tool expects",
            Self::VersionConflict => "retry once, with `wait: true`",
            Self::UnsupportedVersion | Self::UnknownKind => {
                "ask the human to upgrade bellwether; this build cannot read the index"
            }
            Self::SegmentClaimed => "retry later, with `wait: true`",
            Self::EngineError => "read the message; it says what to do",
            Self::ExportDenied => {
                "tell the human which permission `fields.permission` names; the export is the \
                 measured run, so without it nothing is measured and there is no certificate \
                 and no receipt"
            }
            Self::CallerSuppliedCost => {
                "remove the field `fields.field` names and call again; the engine measures, \
                 verifies, and attests on its own"
            }
            Self::MeasurementRefused => {
                "for `cache_served`, propose the rewrite again without the settings that turn a \
                 cache on in its query text (propose.rewrite) and call the tool on the new \
                 proposal; for any other `fields.reason`, call again, and tell the human if it \
                 repeats"
            }
            Self::MeasurementChanged => {
                "do not apply; call score.verify on the proposal again and apply.receipt with \
                 the new certificate, or tell the human the query does not measure the same \
                 twice"
            }
            Self::StaleJoin => {
                "ask the human to run `bellwether index` on the data folder (the left dataset), \
                 which rebuilds the join key's pair index, then call the tool again"
            }
            Self::JoinNotServed => {
                "use only the join key on the right side `fields.served` names; when it is null, \
                 or another right dataset is needed, ask the human to restart the server naming \
                 that right dataset"
            }
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
            "scratch" => Some(
                "ask the human to start the server with --scratch <folder> (a folder of the \
                 lake the engine's exports are written under), then call score.verify again"
                    .into(),
            ),
            _ => None,
        },
        Error::NotIndexed {
            root,
            needs: Some(sketch),
            operation,
        } if sketch.starts_with("sketch:") => Some(format!(
            "ask the human to build the sketch with `bellwether index {root} --key {sketch}` (a \
             sketch is approximate and never planned with){}",
            if *operation == "cardinality" {
                ", or call the tool again with `transient: true` to read the files now"
            } else {
                ", then call the tool again"
            }
        )),
        Error::NotIndexed {
            root,
            needs: Some(agg),
            ..
        } if agg.starts_with("agg:") => Some(format!(
            "ask the human to index the data folder with `bellwether index {root} --key \
             'exact:{},counts' --key '{agg}'` (an agg key is declared beside the counted key on \
             its columns), then call the tool again",
            agg.trim_start_matches("agg:")
                .split('@')
                .next()
                .unwrap_or_default()
        )),
        Error::NotIndexed { needs, .. } => {
            let spec = needs
                .as_deref()
                .map_or_else(|| "a key spec".to_string(), |s| format!("the key spec {s}"));
            Some(format!(
                "ask the human to index the data folder with {spec} (`bellwether index`), then \
                 call the tool again"
            ))
        }
        // A tombstone (B8) priced without the time column ordering versions.
        Error::KeySpec(m) if m.contains("MCP score.why: latest_by") => Some(
            "pass `latest_by` (the time column that orders versions) with `tombstone`, or \
             `valid_by` and `known_by` for a bitemporal lookup, then call score.why again"
                .into(),
        ),
        // A spec refusal whose step is an index build names the command.
        Error::KeySpec(m) if m.contains("`bellwether index ") => Some(
            "ask the human to run the `bellwether index` command the message gives, then call \
             the tool again"
                .into(),
        ),
        Error::InvalidArgument { field, .. } => Some(format!(
            "fix `{field}` as the message says, then call the tool again (with `wait: true` if \
             the arguments themselves are unchanged)"
        )),
        Error::NotRetained { retained, .. } => match (retained.first(), retained.last()) {
            (Some(oldest), Some(current)) => Some(format!(
                "pass in `at_manifest` one of {oldest}..={current}, or omit it to plan against \
                 the current manifest"
            )),
            _ => Some(
                "the folder is not indexed, so there is no manifest to pin: omit `at_manifest`, \
                 or ask the human to index the data folder (`bellwether index`)"
                    .into(),
            ),
        },
        _ => None,
    }
}

/// A facade error as a refusal, by its variant.
#[must_use]
#[allow(clippy::too_many_lines)]
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
        Error::NotRetained {
            index,
            requested,
            retained,
        } => (
            RefusalCode::NotRetained,
            json!({
                "index": index,
                "requested": requested,
                "oldest": retained.first(),
                "current": retained.last(),
            }),
        ),
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
        Error::SourceChanged { root, units, .. } => (
            RefusalCode::SourceChanged,
            json!({ "root": root, "units": units }),
        ),
        Error::NotAFolder { root, operation } => (
            RefusalCode::NotAFolder,
            json!({ "root": root, "operation": operation }),
        ),
        Error::Store(_) => (RefusalCode::StoreError, json!({})),
        Error::ExportDenied {
            engine, permission, ..
        } => (
            RefusalCode::ExportDenied,
            json!({ "engine": engine, "permission": permission }),
        ),
        Error::MeasurementRefused {
            engine,
            tag,
            reason,
            ..
        } => (
            RefusalCode::MeasurementRefused,
            json!({ "engine": engine, "tag": tag, "reason": reason }),
        ),
        Error::StaleJoin {
            spec,
            side,
            pinned,
            current,
            ..
        } => (
            RefusalCode::StaleJoin,
            json!({ "spec": spec, "side": side, "pinned": pinned, "current": current }),
        ),
        Error::JoinNotServed { served, .. } => {
            (RefusalCode::JoinNotServed, json!({ "served": served }))
        }
        // No wildcard arm: a new `Error` variant must fail to compile here.
        Error::Serde(_) | Error::Other(_) => (RefusalCode::EngineError, json!({})),
    };
    let mut r = Refusal::new(code, message, fields);
    if let Some(next) = typed_next(e) {
        r.next = next;
    }
    r
}

/// Fields a caller may never hand the engine: a score, a cost, a
/// measurement, a replay, a verdict, or a certificate under any of these
/// names. No tool's schema has one where the caller writes, so each can
/// only arrive as an unknown field.
pub const COST_FIELDS: &[&str] = &[
    "score",
    "scores",
    "cost",
    "costs",
    "replay",
    "replays",
    "measured",
    "measurement",
    "measurements",
    "stats",
    "query_stats",
    "read_rows",
    "read_bytes",
    "peak_memory_bytes",
    "duration_ns",
    "prediction",
    "verdict",
    "pass",
    "receipt",
    "certificate",
];

/// The first cost-like field an argument validation rejected as unknown
/// (`'score' was unexpected`), if any.
#[must_use]
pub fn caller_supplied(errors: &[String]) -> Option<String> {
    for e in errors {
        let mut rest = e.as_str();
        while let Some(i) = rest.find('\'') {
            let after = &rest[i + 1..];
            let Some(j) = after.find('\'') else { break };
            let name = &after[..j];
            if COST_FIELDS.contains(&name) {
                return Some(name.to_string());
            }
            rest = &after[j + 1..];
        }
    }
    None
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
        ProposeRefusal::SketchNotProposable {
            declaration,
            sketch,
            root,
            command,
        } => {
            let mut r = Refusal::new(
                RefusalCode::SketchNotProposable,
                format!(
                    "{declaration} declares the sketch {sketch}, which is approximate and never \
                     planned with, so no replay can price it and it has no certificate or receipt"
                ),
                json!({
                    "declaration": declaration,
                    "sketch": sketch,
                    "root": root,
                    "command": command,
                }),
            );
            let how = if command.is_some() {
                "the command in `fields.command`"
            } else {
                "`bellwether index` and this server's source options, which no command line \
                 spells (`fields.command` is null)"
            };
            let build = format!(
                "ask the human to index {root} with the declaration {declaration} ({how}; no \
                 receipt)"
            );
            r.next = build;
            r
        }
    }
}

/// A typed `fix` refusal, with the first mismatching keys when the verify
/// stage ran in this call (`apply.receipt` re-checks a stored certificate,
/// which carries counts only).
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn from_fix(r: &FixRefusal, verification: Option<&Reconciliation>) -> Refusal {
    match r {
        FixRefusal::CertificateFailed {
            certificate,
            mismatched,
            uncomparable,
            missing_left,
            missing_right,
            totals_differ,
            agg_cells_differ,
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
            let mut detail = json!({
                "certificate": certificate,
                "mismatched": mismatched,
                "uncomparable": uncomparable,
                "missing_left": missing_left,
                "missing_right": missing_right,
                "totals_differ": totals_differ,
                "mismatches_sample": sample,
            });
            let mut message = format!(
                "certificate {certificate} failed: {mismatched} mismatched, {uncomparable} \
                 uncomparable, {missing_left} missing on the left, {missing_right} missing on \
                 the right, {totals_differ} group totals differ"
            );
            // A proposed agg key (B7) whose cells differ from the rows.
            if *agg_cells_differ > 0 {
                let cells: Vec<Value> = verification
                    .into_iter()
                    .flat_map(|v| &v.agg)
                    .filter_map(|a| a.checked.as_ref())
                    .flat_map(|c| &c.examples)
                    .take(MISMATCH_SAMPLE)
                    .map(|d| serde_json::to_value(d).unwrap_or(Value::Null))
                    .collect();
                message.push_str(", ");
                message.push_str(&agg_cells_differ.to_string());
                message.push_str(" agg cells differ from the rows they aggregate");
                detail["agg_cells_differ"] = json!(agg_cells_differ);
                detail["agg_cells_sample"] = Value::Array(cells);
            }
            if *uncomparable > 0 {
                message = format!(
                    "{message}. {}",
                    bellwether::uncomparable_cause(*uncomparable)
                );
            }
            Refusal::new(RefusalCode::CertificateFailed, message, detail)
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
        FixRefusal::AttestOnly {
            certificate,
            target,
            run,
            proposed_rewrite,
        } => Refusal::new(
            RefusalCode::AttestOnly,
            format!(
                "certificate {certificate} passed; {target} is a table, so the engine applies \
                 nothing and writes no receipt (ADR 027)"
            ),
            json!({
                "certificate": certificate,
                "target": target,
                "run": run,
                "proposed_rewrite": proposed_rewrite,
            }),
        ),
        FixRefusal::ExportDenied {
            engine,
            permission,
            message,
        } => Refusal::new(
            RefusalCode::ExportDenied,
            message.clone(),
            json!({ "engine": engine, "permission": permission }),
        ),
        FixRefusal::NoReplay { certificate } => Refusal::new(
            RefusalCode::NoReplay,
            format!("certificate {certificate} passed, but a rewrite has no replay to attest"),
            json!({ "certificate": certificate }),
        ),
        FixRefusal::MeasurementChanged {
            certificate,
            replay,
            pair,
            side,
            certified_read_rows,
            measured_read_rows,
            certified_read_bytes,
            measured_read_bytes,
            certified_result_rows,
            measured_result_rows,
        } => Refusal::new(
            RefusalCode::MeasurementChanged,
            format!(
                "measured again in the state certificate {certificate} verified, p{pair} \
                 {side} read {measured_read_rows} rows and {measured_read_bytes} bytes and \
                 returned {measured_result_rows} rows; the certified replay {replay} says \
                 {certified_read_rows}, {certified_read_bytes}, and {certified_result_rows}"
            ),
            json!({
                "certificate": certificate,
                "replay": replay,
                "pair": pair,
                "side": side,
                "certified": {"read_rows": certified_read_rows, "read_bytes": certified_read_bytes, "result_rows": certified_result_rows},
                "measured": {"read_rows": measured_read_rows, "read_bytes": measured_read_bytes, "result_rows": measured_result_rows},
            }),
        ),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::too_many_lines)]
mod tests {
    use bellwether::types::Hash;

    use super::*;

    /// No `next` line or message built here names a CLI flag for the agent
    /// to pass. `nothing_to_gain`'s names one only as the human's own CLI
    /// command: no tool takes it, so an agent cannot override the gate.
    fn no_flags(r: &Refusal) {
        if r.code == RefusalCode::NothingToGain {
            nothing_to_gain_is_the_human_s(r.next.as_str());
        } else {
            assert!(!r.next.contains("--"), "{r:?}");
        }
    }

    fn nothing_to_gain_is_the_human_s(next: &str) {
        assert!(
            next.contains("only the human can continue, with `bellwether fix --allow-no-gain`")
                && !next.contains("allow_no_gain"),
            "{next}"
        );
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
            (
                Error::NotRetained {
                    index: "/d/.bellwether".into(),
                    requested: 1,
                    retained: vec![3, 4, 5],
                },
                RefusalCode::NotRetained,
            ),
            (
                Error::MeasurementRefused {
                    engine: "clickhouse://h/db".into(),
                    tag: "bw-1-p0-after".into(),
                    reason: "cache_served",
                    message: "m".into(),
                },
                RefusalCode::MeasurementRefused,
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
        assert_eq!(f(18)["requested"], 1);
        assert_eq!(f(18)["oldest"], 3);
        assert_eq!(f(18)["current"], 5);
        // Next lines name the MCP field to fix.
        assert!(from_error(&cases[10].0).next.contains("`certificate`"));
        assert!(from_error(&cases[11].0).next.contains("`workload`"));
        assert!(from_error(&cases[12].0).next.contains("`workload`"));
        assert!(from_error(&cases[14].0).next.contains("`at`"));
        assert!(from_error(&cases[18].0)
            .next
            .contains("`at_manifest` one of 3..=5"));
        let unindexed = from_error(&Error::NotRetained {
            index: "/d/.bellwether".into(),
            requested: 2,
            retained: Vec::new(),
        });
        assert_eq!(unindexed.fields["oldest"], Value::Null);
        assert!(
            unindexed.next.contains("omit `at_manifest`"),
            "{unindexed:?}"
        );
        no_flags(&unindexed);
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
                    uncomparable: 5,
                    missing_left: 2,
                    missing_right: 3,
                    totals_differ: 4,
                    agg_cells_differ: 5,
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
            (
                FixRefusal::MeasurementChanged {
                    certificate: h,
                    replay: h,
                    pair: 0,
                    side: "after".into(),
                    certified_read_rows: 1,
                    measured_read_rows: 1,
                    certified_read_bytes: 2,
                    measured_read_bytes: 3,
                    certified_result_rows: 4,
                    measured_result_rows: 4,
                },
                RefusalCode::MeasurementChanged,
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
            if c == RefusalCode::NothingToGain {
                nothing_to_gain_is_the_human_s(c.next());
            } else {
                assert!(!c.next().contains("--"), "{c:?}");
            }
        }
    }
}
