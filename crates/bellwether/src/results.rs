//! Typed results. Each serializes with serde, and that serialization is the
//! CLI's `--json` output and what the MCP server returns (ADR 010). Nothing
//! here is pre-formatted text: counts are integers, hashes are hashes, and
//! every ledger record a command wrote or read is embedded whole, with its
//! hash, exactly as the ledger stores it ([`Written`]).

use bellwether_ledger::{RecordKind, SignatureStatus, Verdict};
use bellwether_types::{Hash, KeySpec, Record, TextOp, Tombstone};
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
    /// The sidecar's name: `bounds:symbol@ts`, `temporal2:id@valid,known`,
    /// or a sketch's `sketch:batch,id` (approximate; never planned with).
    pub sidecar: String,
    /// Completeness sequence.
    pub complete_at: Option<u64>,
}

/// A sidecar of an index written before M5 that a build folded into a key
/// spec ([`IndexResult::sidecars_folded`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SidecarFolded {
    /// The sidecar's name (`bounds:symbol@ts`, `temporal2:id@valid,known`).
    pub sidecar: String,
    /// The identity of the key spec it now is (`exact:symbol@ts`,
    /// `temporal2:id@valid,known`).
    pub into: String,
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
    /// Re-declared sketches whose segments were missing from the store or
    /// damaged: their segments were dropped and every live row group was
    /// sketched again. Omitted in JSON when none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sketches_rebuilt: Vec<String>,
    /// Counted key specs (by identity) this build rebuilt because two of
    /// their segments counted one row group: a corrupt index, which every
    /// count and plan through the spec refused until then. Omitted in JSON
    /// when none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub specs_repaired: Vec<String>,
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
    /// Unchanged files whose storage version 2 statistics (rows, bytes,
    /// column bytes per row group) this build recorded from their footers:
    /// every tracked file on the first commit over a version 1 manifest.
    /// Zero (omitted in JSON) otherwise.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub files_filled: u64,
    /// The storage version this build's commit upgraded the index from
    /// (ADR 028): a binary of that version can no longer open it. Absent
    /// (omitted in JSON) when the index was already current or nothing was
    /// committed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upgraded_from: Option<u32>,
    /// The sidecars of an index written before M5 (per-key time bounds,
    /// bitemporal versions) this build folded into key specs, each rebuilt
    /// under its new identity. Only what folded; absent (omitted in JSON)
    /// when none did.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sidecars_folded: Vec<SidecarFolded>,
    /// The sidecars of an index written before M5 that no key spec can
    /// take (a spec on their key columns keeps another time column): kept
    /// as sidecars, which still plan, are still built, and are listed in
    /// `sidecars`. Absent (omitted in JSON) when none is.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub kept_legacy: Vec<String>,
    /// On an `index` with nothing to do over an index written before M5:
    /// the folds its next build with work makes. Absent (omitted in JSON)
    /// otherwise.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fold_pending: Vec<SidecarFolded>,
    /// Text keys this build rebuilt whole because their segments were built
    /// under other Unicode tables (B5; after a deliberate upgrade of a
    /// table crate). Absent (omitted in JSON) when none was.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub text_rebuilt: Vec<TextRebuilt>,
    /// Every declared join key (B3) after the build, with its state: each
    /// that was not fresh is rebuilt from both datasets' exact keys
    /// (`refreshed`). Absent (omitted in JSON) when none is declared.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub joins: Vec<crate::join::JoinKeyState>,
}

/// A text key `index` rebuilt because its segments recorded other Unicode
/// tables ([`IndexResult::text_rebuilt`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextRebuilt {
    /// The key.
    pub spec: String,
    /// The case-folding tables its old segments recorded, and this
    /// binary's (`crate version`).
    pub case: [String; 2],
    /// The word-boundary tables its old segments recorded, and this
    /// binary's.
    pub words: [String; 2],
}

#[allow(clippy::trivially_copy_pass_by_ref)] // serde passes a reference
pub(crate) fn is_zero(n: &u64) -> bool {
    *n == 0
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
    /// Rows whose string column matches a text lookup (B5), both sides
    /// case-folded.
    Text {
        /// `has_token`, `contains`, or `has_prefix`.
        operator: TextOp,
        /// The argument as given.
        value: String,
        /// The argument case-folded, as the index and the residual compare
        /// it.
        folded: String,
        /// The string column.
        column: String,
        /// The text keys the plan intersected.
        serving: Vec<KeySpec>,
        /// `true`: every planned row group holds a matching row (a words
        /// dictionary served); apply the residual for the rows. `false`:
        /// the plan is a candidate set (trigrams or a bloom filter), a
        /// planned row group may hold no match, and the residual is
        /// mandatory.
        exact_row_groups: bool,
        /// Why nothing was pruned, when nothing was.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        unpruned: Option<String>,
    },
    /// A workload's semi-join runs (B3), as `why` priced them: the rows of
    /// the data root and of the right dataset whose key the other holds,
    /// two steps per run.
    SemiJoin {
        /// The right dataset: the folder of that name beside the data root.
        right: String,
        /// Its join columns, pairwise with the runs' key columns.
        right_columns: Vec<String>,
    },
    /// A workload's text runs (B5), as `why` priced them: rows whose string
    /// column matches one of each run's values, both sides case-folded.
    TextRuns {
        /// The string columns the runs look up, sorted.
        columns: Vec<String>,
        /// The matches the runs ask for (`contains`, `has_token`,
        /// `has_prefix`), sorted.
        operators: Vec<TextOp>,
    },
    // --- phase 5 kinds ---
    /// Rows whose list column holds at least one of the values
    /// (`contains_any`: `array_contains`, `has_any`, `exists(unnest)`).
    ContainsAny {
        /// The list column.
        column: String,
    },
    /// Rows whose list column holds every one of the values
    /// (`contains_all`).
    ContainsAll {
        /// The list column.
        column: String,
    },
    /// Per key, aggregates of one value column over the keys' rows (B7:
    /// `sum/count/min/max(value) ... group by key`).
    Aggregate {
        /// The aggregated column.
        value: String,
        /// The functions, in the order sum, count, min, max.
        functions: Vec<String>,
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
    /// planned row groups: read the entries in order, each entry's row
    /// groups ascending, or, when files overlap in time, the plan's
    /// `order` (`(entry, row group)` pairs interleaving files). Set exactly
    /// when such an order exists (a complete range key on the column).
    pub ordered_by: Option<String>,
    /// Why there is no order, when there is none.
    pub unordered_reason: Option<String>,
    /// The rows holding the keys in the planned row groups, exact from the
    /// per-key counts of a spec on exactly the lookup's columns that stores
    /// them (`exact:…,counts`; storage version 2). Absent otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<u64>,
    /// With `--columns`: the compressed bytes of only those columns over the
    /// planned row groups, from the manifest's per-column bytes (a text
    /// source has none and counts its whole chunks). Absent otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projected_bytes: Option<u64>,
    /// With `--columns` on a storage version 1 manifest, which records no
    /// per-column bytes: the Parquet footers they were read from instead,
    /// one per planned file. Zero (omitted) otherwise.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub column_bytes_from_footers: u64,
    /// The `plan` Evidence.
    pub evidence: Written,
    /// A `live` lookup (B8, `latest --live`, `asof --live`): the tombstone
    /// that marks a retracted row and what the tombstones pruned. Absent
    /// otherwise, so every other plan serializes as before.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live: Option<LivePlan>,
}

/// What the tombstones of a `live` lookup (B8) did. The rows a consumer
/// keeps are those of `latest`/`asof` followed by `filter`: per key, the
/// rows at its newest time (at or before the instant) that are not
/// tombstones, so a key whose newest row is a tombstone, or whose only rows
/// are, has none. The residual says the same (`Live` after
/// `LatestPerKey`), whatever was pruned.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LivePlan {
    /// The tombstone the serving key declares.
    pub tombstone: Tombstone,
    /// The filter the residual applies after `latest`/`asof`, in SQL:
    /// `not void`, or `status <> 'VOID'` (a null is not live, ADR 014).
    pub filter: String,
    /// The tombstone sidecar whose stored liveness pruned the plan
    /// (`tombstone:<keys>@<times>`); absent when it did not (`unpruned`
    /// says why).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pruned_by: Option<String>,
    /// Why tombstones pruned nothing, and the command that fixes it, when
    /// they did not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unpruned: Option<String>,
    /// Looked-up keys with a version at the instant (before the tombstone
    /// filter).
    pub keys_with_version: u64,
    /// Of those, keys whose newest row is a tombstone, so the plan reads
    /// nothing for them: exact when `pruned_by` is set, zero otherwise
    /// (unknown without the sidecar; the residual still drops them).
    pub keys_retracted: u64,
    /// Row groups holding a key's newest version that the tombstones
    /// pruned (no live row of it there), summed over keys.
    pub row_groups_pruned: u64,
}

/// What [`crate::Engine::count`] answered: `count(*) where (columns) in
/// keys`, exact, from the per-key counts of one spec. Writes nothing.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CountResult {
    /// Key columns, in tuple order.
    pub columns: Vec<String>,
    /// Distinct non-null key tuples counted.
    pub keys: u64,
    /// Input rows dropped because their tuple held a null (ADR 014).
    pub null_tuples: u64,
    /// BLAKE3 of the canonical key set (the one `locate` cites for the same
    /// keys).
    pub key_set: Hash,
    /// The manifest sequence counted against.
    pub sequence: u64,
    /// The spec whose counts answered (its identity, `exact:…,counts`).
    pub counted_by: String,
    /// Rows whose key tuple is one of the keys.
    pub rows: u64,
    /// Live row groups holding at least one of the keys.
    pub row_groups: u64,
    /// Live row groups in the index.
    pub row_groups_total: u64,
    /// Files holding those row groups.
    pub files: u64,
    /// Live files in the index.
    pub files_total: u64,
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
    /// The declaration is a sketch (`sketch:...`): approximate and never
    /// planned with, so no replay can price it and it has no certificate or
    /// receipt. It is built with `bellwether index --key`. (Per-key time
    /// bounds and bitemporal versions are key specs, proposed like any
    /// other; M5.)
    SketchNotProposable {
        /// The declaration, as `index --key` reads it.
        declaration: String,
        /// The sketch, by its manifest name (`sketch:batch,id`).
        sketch: String,
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
    /// For the receipt and its certificate, when they resolved: every
    /// signature object beside the record, each verified on its own (ADR
    /// 029). Absent for the other links and for flagged ones.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signatures: Option<Vec<SignatureStatus>>,
    /// For the receipt and its certificate, when the engine was given
    /// trusted keys: those signatures folded against them (`signed`,
    /// `untrusted`, `unsigned`, or `invalid`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verdict: Option<Verdict>,
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
    /// The data roots the chain's resolved evidence is about: a profile's
    /// root, and the folder of every file a plan names outside this
    /// engine's root. Sorted, each once.
    pub roots: Vec<String>,
    /// With trusted keys: whether the chain is signed, which needs all of
    /// every link intact, the receipt and its certificate each signed by a
    /// trusted key within its window, and the evidence about this engine's
    /// data root. `None` without trusted keys. A receipt alone does not
    /// count: a certificate planted in the store and applied would otherwise
    /// ride on a signed receipt. Signatures bind content, not location, so a
    /// chain copied from another index is refused by its root.
    pub signed: Option<bool>,
    /// With trusted keys, when `signed` is false: the first reason.
    pub not_signed: Option<NotSigned>,
}

/// Why a chain is not signed against trusted keys, in the order checked.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NotSigned {
    /// A link is missing, of the wrong kind, or unreadable, so the chain
    /// does not stand whatever its signatures say.
    LinksFlagged {
        /// The first flagged link.
        link: Hash,
        /// The kind the chain expects there.
        link_kind: RecordKind,
        /// How it is flagged.
        status: LinkStatus,
    },
    /// The receipt or the certificate is not signed by a trusted key in
    /// its window; their links' `verdict`s say how.
    Verdicts,
    /// Both are signed by trusted keys, but the evidence is about another
    /// data root: a signed chain copied from another index.
    OtherRoot {
        /// The roots the evidence names.
        roots: Vec<String>,
        /// This engine's data root.
        expected: String,
    },
    /// Both are signed, but no resolved evidence names a data root, so the
    /// chain cannot be tied to this one.
    NoRoot,
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
    /// With trusted keys: how many of `receipts` are signed, by the rule of
    /// [`ReceiptResult::signed`] (intact links, receipt and certificate each
    /// signed by a trusted key in its window, evidence about this data
    /// root). `None` without trusted keys.
    pub signed: Option<u64>,
}
