//! The Bellwether facade (ADR 010): one [`Engine`] over every engine crate,
//! and the single code path the CLI and the MCP server share.
//!
//! Every operation is a method returning a typed result that serializes
//! with serde. That serialization is the CLI's `--json` output and what the
//! MCP server returns, so a human and an agent see identical evidence. No
//! formatting lives in the results: they carry integers, hashes, and the
//! ledger records each command wrote or read, embedded whole with their
//! hashes ([`Written`]).
//!
//! ```no_run
//! use bellwether::{Engine, EngineOptions, KeyFile};
//!
//! let engine = Engine::open("./orders", EngineOptions::default())?;
//! engine.index(&[bellwether::spec::parse("exact:order_id")?])?;
//! let plan = engine.locate(&KeyFile::new("keys.parquet"))?;
//! println!("{} of {} row groups", plan.planned.row_groups, plan.row_groups_total);
//! # Ok::<(), bellwether_types::Error>(())
//! ```
//!
//! The trust boundary (ADR 009): `propose` writes a Proposal and nothing
//! else; a Proposal has no score field and the facade adds none. Records an
//! agent may not write (Replay, Certificate, Receipt) are written only by
//! the engine's own stages, each computed immediately before it is written.
//!
//! No panic reaches a caller: every public method returns
//! `Result<_, bellwether_types::Error>`, an internal panic is turned into an
//! error naming the issue tracker, and errors say what to do.

#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]

mod engine;
mod fix;
mod index;
mod keys;
mod ledger_views;
mod lookback;
mod propose;
mod receipt;
mod reconcile;
mod report;
mod results;
mod signing;
pub mod spec;
mod why;

pub use bellwether_ledger::{
    printable, InvalidReason, InvalidSignature, OutsideWindow, PublicKey, RecordKind,
    SignatureStatus, SignedBy, SigningKey, TrustError, TrustedKey, TrustedKeys, Verdict,
};
pub use bellwether_profile::ProfileReport;
pub use bellwether_source::{FingerprintPolicy, Partitioning, PathTemplate};
pub use bellwether_source_sql::url::redact_db;
pub use bellwether_source_sql::{
    is_sql_root, EngineKind, TableRun, MEASURED_AGGREGATES, MEASURED_PLAN, MEASURED_WALL,
};
pub use bellwether_types as types;
pub use bellwether_verify::{
    ExactTotal, GroupTotal, KeyRepr, Mismatch, Reconciliation, TotalColumn,
};
pub use engine::{
    schema_from_json, Engine, EngineOptions, FolderStatus, SourceFormat, DEFAULT_INDEX_DIR,
    ISSUE_TRACKER,
};
pub use fix::{
    DataState, FixOutcome, FixRequest, FixResult, Refusal, RewriteOutputs, Score, SignatureWritten,
};
pub use keys::KeyFile;
pub use ledger_views::{
    Finding, FindingsResult, ReceiptEntry, ReceiptsResult, RuleScore, ScorecardResult, UNATTRIBUTED,
};
pub use lookback::{split_window, window_reversed, Lookback};
pub use receipt::signature_summary;
pub use reconcile::{ReconcileProblem, ReconcileRequest, ReconcileResult};
pub use results::{
    ChainLink, Cost, CountResult, IndexResult, LinkStatus, LookbackWindow, LookupKind, NotSigned,
    PlanResult, ProposeRefusal, ProposeResult, ReceiptResult, ReportResult, SidecarState,
    SpecState, Written,
};
pub use signing::{
    generate_key, load_signing_key, load_trusted_keys, public_path, read_public_key,
    refuse_inside_index, trust_key, GeneratedKey, PublicKeyFile, TrustEntry, TrustResult,
};
pub use why::{
    Candidate, CandidateOrigin, Culprit, CulpritReason, Dataset, LookupMode, ProfileEvidence,
    Recommendation, TimeWindow, WhyReport, WhyRequest, WorkloadInfo, DEFAULT_MAX_CANDIDATES,
    ENGINE_PROPOSER, LAYOUT_BOUND_FRACTION, RESERVED_PROPOSERS,
};
