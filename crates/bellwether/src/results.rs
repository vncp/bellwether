//! Typed results. Each serializes with serde, and that serialization is the
//! CLI's `--json` output and what the MCP server returns (ADR 010). Nothing
//! here is pre-formatted text: counts are integers, hashes are hashes, and
//! every ledger record a command wrote or read is embedded whole, with its
//! hash, exactly as the ledger stores it ([`Written`]).

use bellwether_ledger::RecordKind;
use bellwether_types::{Hash, KeySpec, Record};
use serde::{Deserialize, Serialize};

/// A ledger record and its hash. `record` serializes to exactly the bytes
/// stored at `ledger/<hash>.rec`, so `hash` is the BLAKE3 of
/// `serde_json::to_vec(&record)`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Written {
    /// The record's content hash.
    pub hash: Hash,
    /// The record, with its `"record"` kind tag.
    pub record: Record,
}

impl Written {
    /// The record's kind.
    #[must_use]
    pub fn kind(&self) -> RecordKind {
        RecordKind::of(&self.record)
    }
}

/// Files, row groups, and compressed bytes: exact sums of row-group
/// statistics, never estimates (ADR 005).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cost {
    /// Files.
    pub files: u64,
    /// Row groups.
    pub row_groups: u64,
    /// Compressed bytes.
    pub bytes: u64,
}

/// A key spec and the manifest sequence at which it became complete over
/// every live row group (ADR 012), or `None` while it is incomplete.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SpecState {
    /// The spec.
    pub spec: KeySpec,
    /// Completeness sequence.
    pub complete_at: Option<u64>,
}

/// A sidecar (per-key time bounds, bitemporal versions) by its manifest
/// name, and the manifest sequence at which it became complete, or `None`
/// while it is incomplete.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SidecarState {
    /// The sidecar's name: `bounds:symbol@ts`, `temporal2:id@valid,known`.
    pub sidecar: String,
    /// Completeness sequence.
    pub complete_at: Option<u64>,
}

/// What [`crate::Engine::index`] did.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IndexResult {
    /// The canonical data root.
    pub root: String,
    /// The sequence this build committed, or `None` when there was nothing
    /// to do (no new file, no new spec).
    pub committed: Option<u64>,
    /// The current manifest sequence after the build (ADR 020).
    pub sequence: u64,
    /// BLAKE3 of the current manifest's bytes as stored.
    pub manifest: Hash,
    /// Segments the current manifest references.
    pub segments: u64,
    /// Segments this build wrote.
    pub segments_written: u64,
    /// Every declared spec with its completeness.
    pub specs: Vec<SpecState>,
    /// Every declared sidecar this binary knows, with its completeness.
    pub sidecars: Vec<SidecarState>,
    /// Segment or sidecar kinds the manifest lists that this binary does
    /// not know: kept unchanged by builds, never used to plan.
    pub unknown_segment_kinds: Vec<String>,
    /// Files seen for the first time.
    pub files_new: u64,
    /// Tracked files whose fingerprint changed (ADR 013).
    pub files_changed: u64,
    /// Tracked files that are gone.
    pub files_removed: u64,
    /// Tracked files unchanged.
    pub files_unchanged: u64,
    /// Row groups read to build segments.
    pub row_groups_read: u64,
    /// Fresh row-group ids assigned.
    pub row_groups_assigned: u64,
    /// Row-group ids retired (never reused).
    pub row_groups_retired: u64,
}

/// Which lookup a plan answers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LookupKind {
    /// Rows holding the keys.
    Locate,
    /// The newest version of each key.
    Latest,
    /// The version of each key current at `ts`.
    Asof {
        /// The instant as given.
        at: String,
        /// The instant in the timestamp column's dataset unit.
        ts: i64,
        /// The timestamp column.
        ts_column: String,
    },
    /// Rows of the keys whose time column lies in `[lo, hi]`.
    Window {
        /// The window's start as given.
        from: String,
        /// The window's end as given.
        to: String,
        /// `from` in the time column's dataset unit.
        lo: i64,
        /// `to` in the time column's dataset unit.
        hi: i64,
        /// The time column.
        ts_column: String,
    },
    /// The rows of the keys in a lookback window: `asof --at T --lookback
    /// N` reads the window `[T - N, T]` on the time column; `latest
    /// --lookback N` reads each key's own window `[newest - N, newest]`,
    /// `newest` being that key's newest time (index-wide, live row groups
    /// only).
    Lookback {
        /// The instant as given (`asof`), or `None` (`latest`).
        at: Option<String>,
        /// The lookback as given (`252d`).
        lookback: String,
        /// The lookback in calendar seconds (`d` is 86,400 s, `w` 7 days),
        /// or `None` for a bare integer in an integer column's unit.
        lookback_seconds: Option<i64>,
        /// `asof`: the window's end in the time column's dataset unit.
        /// `None` for `latest`, where each key's window ends at its own
        /// newest time.
        end: Option<i64>,
        /// `asof`: the window's start in that unit (`end` minus the
        /// lookback, clamped to the column's range). `None` for `latest`.
        lo: Option<i64>,
        /// The time column.
        ts_column: String,
    },
    /// Per key, the newest row by valid time among rows known by `known`
    /// and valid by `valid` (each unbounded when absent).
    Bitemporal {
        /// The valid-time bound as given.
        valid: Option<String>,
        /// The known-time bound as given.
        known: Option<String>,
        /// `valid` in the valid column's dataset unit.
        valid_ts: Option<i64>,
        /// `known` in the known column's dataset unit.
        known_ts: Option<i64>,
        /// The valid-time column.
        valid_column: String,
        /// The known-time column.
        known_column: String,
    },
}

/// A lookback window on a `latest` or `asof` lookup: every version of each
/// key in `[end − length, end]`, `end` being the asof instant or, for
/// `latest`, the key's newest version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LookbackWindow {
    /// The window as given (`252d`, `36h`; an integer for an integer
    /// timestamp column or a recorded workload).
    pub given: String,
    /// The window's length in the timestamp column's dataset unit.
    pub length: i64,
}

/// What [`crate::Engine::locate`], [`crate::Engine::latest`], and
/// [`crate::Engine::asof`] planned. The plan itself is the `plan` Evidence.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PlanResult {
    /// The lookup.
    pub lookup: LookupKind,
    /// Key columns, in tuple order.
    pub columns: Vec<String>,
    /// Distinct non-null key tuples looked up.
    pub keys: u64,
    /// Input rows dropped because their tuple held a null (ADR 014).
    pub null_tuples: u64,
    /// BLAKE3 of the canonical key set (stored in the cache, cited by the
    /// Evidence).
    pub key_set: Hash,
    /// The manifest sequence planned against.
    pub sequence: u64,
    /// The specs the plan used: every declared spec complete at `sequence`.
    pub specs: Vec<KeySpec>,
    /// Declared specs left out because they are not complete (ADR 012).
    pub excluded_incomplete: Vec<KeySpec>,
    /// The complete sidecars the plan used (per-key time bounds for a
    /// window, the bitemporal sidecar for a bitemporal lookup), by name.
    pub sidecars: Vec<String>,
    /// For a window or lookback, the time column when the index's
    /// partitioning (hive directories or a path template) gives it from
    /// each file's path, so files outside the window were pruned by path.
    pub path_values: Option<String>,
    /// Live files the plan chose from.
    pub files_total: u64,
    /// Live row groups the plan chose from.
    pub row_groups_total: u64,
    /// What the plan reads.
    pub planned: Cost,
    /// The timestamp or date column whose row-group minimum orders the
    /// plan's entries (read in order, each entry's row groups ascending),
    /// when that order holds; `None` when the order is unspecified.
    pub ordered_by: Option<String>,
    /// Why the order is unspecified, when it is.
    pub unordered_reason: Option<String>,
    /// The `plan` Evidence.
    pub evidence: Written,
}

/// What [`crate::Engine::propose`] did: wrote the Proposal, or refused at
/// the trust boundary without writing anything. A Proposal has no score
/// field and this result adds none (ADR 009).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ProposeResult {
    /// The Proposal was written.
    Written {
        /// The Proposal.
        proposal: Box<Written>,
    },
    /// Nothing was written.
    Refused {
        /// Why.
        refusal: ProposeRefusal,
    },
    /// A dry run ([`crate::Engine::propose_dry_run`]): every check passed
    /// and this is the Proposal that would be written, with the hash the
    /// ledger would name it by. Nothing was written; the hash does not
    /// resolve.
    WouldWrite {
        /// The Proposal that would be written.
        proposal: Box<Written>,
    },
}

impl ProposeResult {
    /// The Proposal written, if any (never for a dry run).
    #[must_use]
    pub fn proposal(&self) -> Option<&Written> {
        match self {
            Self::Written { proposal } => Some(proposal),
            Self::Refused { .. } | Self::WouldWrite { .. } => None,
        }
    }
}

/// Why `propose` refused at the trust boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProposeRefusal {
    /// The proposer name is one only the engine writes
    /// ([`crate::RESERVED_PROPOSERS`]).
    ReservedProposer {
        /// The name given.
        name: String,
    },
    /// The declaration builds a sidecar (`temporal2:id@valid,known`
    /// bitemporal versions, or the per-key time bounds of
    /// `exact:symbol@ts`), which a Proposal cannot carry: `ProposalBody::Key`
    /// holds a key spec, and replay prices key specs only. It is built with
    /// `bellwether index --key`, which gives it no receipt
    /// (`docs/contract-v3.md`, entry 42).
    SidecarNotProposable {
        /// The declaration, as `index --key` reads it.
        declaration: String,
        /// The sidecar it declares, by its manifest name.
        sidecar: String,
        /// The key spec the sidecar rides beside, which can be proposed
        /// alone; `None` for a sidecar declared alone (`temporal2`).
        key: Option<String>,
        /// The data root.
        root: String,
        /// The `bellwether index` command that builds it, every argument
        /// shell-quoted, with the flags the engine was opened with that
        /// shape a build or locate the index (`--index`, `--format`,
        /// `--chunk-size`, `--schema`, `--fingerprint-policy`,
        /// `--partitioning` or `--path-template`; `EngineOptions::source_flags`),
        /// so following it builds the folder as it was read and where its
        /// lookups look. `None` when a library caller opened the engine with
        /// source options no flags spell (a CSV dialect other than comma
        /// with a header, say).
        command: Option<String>,
    },
}

/// Whether a link of a receipt's chain resolved as expected.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum LinkStatus {
    /// Resolved to the expected kind.
    Ok,
    /// Not in the ledger (`MISSING`).
    Missing,
    /// Resolved to another kind (`WRONG KIND`).
    WrongKind {
        /// The kind found.
        found: RecordKind,
    },
    /// Present but unreadable by this build (`UNREADABLE`).
    Unreadable {
        /// Why.
        reason: String,
    },
}

/// One link of a receipt's chain (the ledger's `explain`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainLink {
    /// The record's hash.
    pub hash: Hash,
    /// The kind the chain expects here.
    pub kind: RecordKind,
    /// RFC 3339 creation time.
    pub created_at: Option<String>,
    /// Whether it resolved.
    pub status: LinkStatus,
    /// Key facts, as ordered `(name, value)` pairs.
    pub facts: Vec<(String, String)>,
}

/// What [`crate::Engine::receipt`] found: the chain behind a receipt, with
/// every record that resolved.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReceiptResult {
    /// The receipt.
    pub receipt: Hash,
    /// Whether every link is present, readable, and of the expected kind.
    /// This checks links, not the write-time rules; the receipt's guarantees
    /// come from the ledger having accepted it (invariant 2).
    pub intact: bool,
    /// Links in walk order: evidence, proposal, replay, certificate, receipt.
    pub chain: Vec<ChainLink>,
    /// Every link's record that resolved, in chain order.
    pub records: Vec<Written>,
}

/// Costs summed over runs.
pub(crate) fn total(runs: &[bellwether_types::RunCost]) -> Cost {
    runs.iter().fold(Cost::default(), |acc, r| Cost {
        files: acc.files.saturating_add(r.files),
        row_groups: acc.row_groups.saturating_add(r.row_groups),
        bytes: acc.bytes.saturating_add(r.bytes),
    })
}

/// What [`crate::Engine::report`] wrote.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReportResult {
    /// The HTML file written.
    pub path: String,
    /// Its size in bytes.
    pub bytes: u64,
    /// BLAKE3 of the file.
    pub blake3: Hash,
    /// Receipts rendered.
    pub receipts: Vec<Hash>,
    /// Profile Evidence rendered.
    pub evidence: Vec<Hash>,
    /// Receipts with a realized cost (a later replay of the same workload at
    /// the receipt's `manifest_after`).
    pub realized: u64,
    /// Records this build could not read, listed and skipped.
    pub unreadable: Vec<Hash>,
}
