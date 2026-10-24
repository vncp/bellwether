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
    /// Timestamp column whose row-group minimum orders the plan's entries,
    /// when a complete range key on a timestamp or date column is used;
    /// `None` when the order is unspecified.
    pub ordered_by: Option<String>,
    /// Live files the plan chose from.
    pub files_total: u64,
    /// Live row groups the plan chose from.
    pub row_groups_total: u64,
    /// What the plan reads.
    pub planned: Cost,
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
}

impl ProposeResult {
    /// The Proposal written, if any.
    #[must_use]
    pub fn proposal(&self) -> Option<&Written> {
        match self {
            Self::Written { proposal } => Some(proposal),
            Self::Refused { .. } => None,
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
