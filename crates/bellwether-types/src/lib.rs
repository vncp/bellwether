//! Shared records, traits, and format constants for the Bellwether engine.
//!
//! This crate is the contract every other crate compiles against. It contains
//! no logic beyond construction, hashing, and serialization. Changing a type
//! here is an architectural decision; see `docs/adr/`.
//!
//! The thesis the types encode: an agent may *observe* ([`Evidence`]) and
//! *propose* ([`Proposal`]); only the engine may *score* ([`Replay`]),
//! *verify* ([`Certificate`]), and *apply* ([`Receipt`]). A [`Proposal`] has
//! no score field by construction. A [`Receipt`] cannot be built without a
//! [`Certificate`] hash, and the ledger crate refuses to store one whose
//! certificate does not resolve and pass.

#![forbid(unsafe_code)]
#![cfg_attr(test, allow(clippy::unwrap_used))]

use std::fmt;
use std::time::Duration;

use bytes::Bytes;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

pub mod format {
    //! Format version constants. Every persisted record and every on-disk
    //! structure carries one. See ADR 017 for the forward-compatibility rule.

    /// Version of the manifest and segment layout. History: 1 (the first
    /// format); 2 (2026-10-26, ADR 028: exact and temporal segments may
    /// carry per-key per-row-group counts, every keyed value record is
    /// length-prefixed, manifests record per-row-group rows and bytes and
    /// per-column bytes, and key segments and completeness by
    /// [`KeySpec::identity`](crate::KeySpec::identity)).
    pub const STORAGE_VERSION: u32 = 2;
    /// Version of the ledger record schemas. History: 1 (contract v2);
    /// 2 (contract v2.1, 2026-10-24: `Replay` gains `inputs` and `model`);
    /// 3 (contract v3, 2026-10-26, ADR 028: the record shapes are frozen;
    /// `docs/contract-v3.md` lists every field it added). `Replay.unserved`
    /// (2026-10-26) is an additive label field under ADR 017's 2026-10-26
    /// amendment: an older reader drops it without misreading any other field.
    pub const RECORD_VERSION: u32 = 3;
    /// Version of the canonical byte encoding (ADR 016).
    pub const ENCODING_VERSION: u32 = 1;
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// The error type shared across the engine. Variants are deliberately coarse;
/// crates attach detail in the message and may wrap with their own types.
/// Deliberately exhaustive (the crates are `publish = false`): a new variant
/// must fail to compile where errors map to codes (`bellwether-mcp`'s
/// `refusal.rs`), never fall through to a generic one.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A persisted structure declares a format version newer than this binary
    /// understands. The engine never downgrades in place (ADR 017).
    #[error("unsupported {what} version {found}; this build supports up to {supported}")]
    UnsupportedVersion {
        /// Which structure: "storage", "record", or "encoding".
        what: &'static str,
        /// Version declared by the data.
        found: u32,
        /// Highest version this build reads.
        supported: u32,
    },
    /// A record referenced by hash could not be found.
    #[error("record {0} not found")]
    NotFound(Hash),
    /// A record violates a ledger rule (for example a receipt whose
    /// certificate did not pass).
    #[error("ledger rule violated: {0}")]
    LedgerRule(String),
    /// A key spec references a column the source does not have, or an
    /// incomplete key spec was queried (ADR 012).
    #[error("key spec error: {0}")]
    KeySpec(String),
    /// A persisted structure uses a kind or field this build does not know
    /// at a storage version it otherwise supports; a new kind requires a
    /// storage version bump (ADR 017 amendment).
    #[error("unknown {what} {found:?}; a newer storage version is required to read it")]
    UnknownKind {
        /// Which structure: "key spec kind", "fingerprint field", "record kind".
        what: &'static str,
        /// The unknown value as found.
        found: String,
    },
    /// A storage version 2 manifest or segment carries a shape this binary
    /// does not read: a column's `data_type` as a type map, a source
    /// `dialect`, a key spec with per-key `bounds`, or the bitemporal
    /// layout. Storage version 2's record shapes are final, but its bytes
    /// are final only from the release that adds those shapes (ADR 028), so
    /// a binary without them refuses rather than misreading the index.
    #[error(
        "{what} {found}: storage version 2 segment/manifest shape not supported by this binary; \
         upgrade bellwether to read this index"
    )]
    UnsupportedShape {
        /// Which shape: `manifest column data_type`, `manifest source`,
        /// `key spec with per-key bounds`, `bitemporal key spec`, `segment
        /// layout`, `segment header field`.
        what: &'static str,
        /// Where it was found, as the index names it (a column name, a key
        /// spec identity, a header field).
        found: String,
    },
    /// A commit referenced a segment that garbage collection has claimed for
    /// deletion; the writer re-uploads after the claim clears (ADR 013).
    #[error("segment {0} is claimed for deletion; re-upload after the claim clears")]
    SegmentClaimed(Hash),
    /// A conditional write lost the race; callers rebase and retry (ADR 015).
    #[error("manifest version conflict: expected {expected}, found {found}")]
    VersionConflict {
        /// Version the writer believed was current.
        expected: String,
        /// Version actually current.
        found: String,
    },
    /// The underlying source could not be read.
    #[error("source error: {0}")]
    Source(String),
    /// The underlying store could not be read or written.
    #[error("store error: {0}")]
    Store(String),
    /// The data folder changed since its index was built: files the index
    /// does not track, tracked files whose fingerprint changed, or tracked
    /// files that are gone. A lookup or apply over it could miss rows
    /// (invariant 1), so it is refused; re-index first. The lists are the
    /// data root's paths, sorted; `message` says what to do.
    #[error("{message}")]
    StaleIndex {
        /// Files under the root the index does not track.
        untracked: Vec<String>,
        /// Tracked files whose fingerprint changed (ADR 013).
        changed: Vec<String>,
        /// Tracked files no longer found.
        missing: Vec<String>,
        /// The whole message, naming the command to run.
        message: String,
    },
    /// A hash resolved to a record of another kind than the operation takes
    /// (for example a proposal hash where a certificate is applied). Each
    /// surface says what to pass instead, by `expected`.
    #[error("{hash} is {found}, not {}", .expected.with_article())]
    WrongKind {
        /// The hash given.
        hash: Hash,
        /// The kind the operation takes.
        expected: HashKind,
        /// The kind found, with its article ("a proposal").
        found: String,
    },
    /// A hash the caller named is not where the operation looked: no such
    /// record in the ledger, or no such workload in the cache. Each surface
    /// says which argument to fix, by `kind`.
    #[error("{kind} {hash} is not in {place}")]
    RecordNotFound {
        /// What the hash was given as.
        kind: HashKind,
        /// The hash given.
        hash: Hash,
        /// Where the operation looked ("the ledger at …", "the cache at …").
        place: String,
    },
    /// Scoring or verifying a proposal needs input that was not given: a
    /// recorded workload for a key, the outputs before and after it for a
    /// rewrite.
    #[error("a {proposal} proposal is verified on {needs}, and none was given")]
    NoWorkload {
        /// The proposal kind: "key" or "rewrite".
        proposal: &'static str,
        /// The request field that carries it: "workload" or "rewrite".
        field: &'static str,
        /// What it is, in words.
        needs: &'static str,
    },
    /// A lookup needs an index the data folder does not have: no index at
    /// all (`needs: None`), or no complete key spec of the kind it plans
    /// with (`needs` names it).
    #[error("{}", not_indexed(root, operation, needs.as_deref()))]
    NotIndexed {
        /// The data root.
        root: String,
        /// The operation refused ("locate", "asof").
        operation: &'static str,
        /// The key spec it needs, when an index exists but lacks it.
        needs: Option<String>,
    },
    /// An argument is well-formed but cannot be used: an empty proposer, no
    /// evidence cited, an instant that is not one, a key file that cannot be
    /// read. `field` is the request field's name; `reason` says what is
    /// wrong in words that name no surface's flag.
    #[error("{reason}")]
    InvalidArgument {
        /// The request field: `by`, `evidence`, `at`, `window`, `valid`,
        /// `known`, `keys`, `key_columns`, `cluster_by`, `shape`, `workload`,
        /// `rewrite`, `lookback`, `latest_by`, `columns`, `text`,
        /// `functions`.
        field: &'static str,
        /// What is wrong with it, and what a usable value looks like.
        reason: String,
    },
    /// A column the caller named is not in the dataset (a key spec's, a
    /// layout's, or a profile's candidate column).
    #[error("column {column:?} is not in the dataset under {root}; its columns are {}", .columns.join(", "))]
    UnknownColumn {
        /// The column named.
        column: String,
        /// The data root.
        root: String,
        /// The dataset's columns, in schema order.
        columns: Vec<String>,
    },
    /// A lookup pinned to a committed manifest sequence the index does not
    /// retain: never committed, or pruned by garbage collection (which
    /// removes every manifest up to the newest one that references a
    /// segment it deletes, so the retained sequences are one contiguous run
    /// ending at the current one). Each surface says which sequences to pass
    /// instead, from `retained`.
    #[error("{}", not_retained(index, *requested, retained))]
    NotRetained {
        /// The index location.
        index: String,
        /// The sequence asked for.
        requested: u64,
        /// The sequences that can be pinned, ascending (empty: no index).
        retained: Vec<u64>,
    },
    /// A table changed during a run that held no snapshot (ADR 027): a
    /// unit's identity read when the run ended differs from the one read
    /// when it began, or a unit appeared or vanished. The run's answer
    /// would mix two states of the table, so it is refused, never returned
    /// as a partial answer. `units` are the unit paths that changed
    /// (`sql://<engine>/<schema>.<table>/<unit>`, sorted); `message` says
    /// what to do.
    #[error("{message}")]
    SourceChanged {
        /// The data root (`sql://<engine>/<tables>`).
        root: String,
        /// The units whose identity changed, appeared, or vanished.
        units: Vec<String>,
        /// The whole message, naming what to do.
        message: String,
    },
    /// The operation needs a folder of files and the data root is a table
    /// (`sql://`): the engine builds no index inside a database and applies
    /// nothing to one (ADR 027). Reconcile, `why`, and `fix` (verify and
    /// attest-only) work over a table.
    #[error("{}", not_a_folder(root, operation))]
    NotAFolder {
        /// The data root (`sql://<engine>/<tables>`).
        root: String,
        /// The operation refused (`index`, `locate`, ...).
        operation: &'static str,
    },
    /// A query engine refused to export a result set (ADR 033): the
    /// credentials bellwether runs with may query but may not export, or
    /// the scratch folder cannot be written. Without the export there is no
    /// certificate and so no receipt. `permission` names what to grant;
    /// `message` is the whole sentence.
    #[error("{message}")]
    ExportDenied {
        /// The engine root (`clickhouse://host/db`, credentials removed).
        engine: String,
        /// The permission the export needs, in the engine's own words.
        permission: String,
        /// The whole message, naming what to grant.
        message: String,
    },
    /// A query engine's log entry for a run bellwether submitted cannot
    /// stand as that run's measurement (ADR 033), so nothing is recorded.
    /// `reason` is one of [`MEASUREMENT_REFUSALS`]: the run was served
    /// from a cache (`cache_served`), the log lacks a statistic
    /// (`statistics_missing`), the entry under the tag is not the query
    /// submitted (`not_the_query_submitted`), more than one finished
    /// entry holds the tag (`ambiguous_log_entry`), the statistics name
    /// another tag (`tag_mismatch`), or the rows the run returned are not
    /// the rows it exported (`result_rows_differ`). `message` is the whole
    /// sentence, naming what to do.
    #[error("{message}")]
    MeasurementRefused {
        /// The engine root (`clickhouse://host/db`, credentials removed).
        engine: String,
        /// The tag the run was submitted under.
        tag: String,
        /// One of [`MEASUREMENT_REFUSALS`].
        reason: &'static str,
        /// The whole message, naming what to do.
        message: String,
    },
    /// A join key (B3) whose pair index no longer describes both datasets:
    /// the left dataset (the data root) or the right one (named in the
    /// spec) committed a manifest after the pair index was built over it
    /// (ADR 020), so a semi-join planned from it could miss rows on that
    /// side (invariant 1 per side). Refused, never answered from the old
    /// pair index; re-index the left dataset to rebuild it. `message` names
    /// the command.
    #[error("{message}")]
    StaleJoin {
        /// The join key's name (`join:orders.order_id=payables.order_id`).
        spec: String,
        /// The side that advanced: `left` or `right`.
        side: &'static str,
        /// The manifest sequence of that side the pair index was built at.
        pinned: u64,
        /// That side's current manifest sequence.
        current: u64,
        /// The whole message, naming the command to run.
        message: String,
    },
    /// A join key (B3) names a right dataset this engine was not opened to
    /// read: an MCP server reads only the right side its operator gave with
    /// `--join` (none without it), never one a tool argument names. Refused
    /// before any folder is read, so the refusal is the same whether a
    /// folder of the name asked exists, is indexed, or not. `served` is the
    /// right side served (`<right>.<columns>`), if any; `message` says what
    /// to do.
    #[error("{message}")]
    JoinNotServed {
        /// The right side the engine serves, if any.
        served: Option<String>,
        /// The whole message, naming what to do.
        message: String,
    },
    /// Serialization failure.
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    /// Anything else.
    #[error("{0}")]
    Other(String),
}

/// The reasons an [`Error::MeasurementRefused`] carries, each a stable
/// name.
pub const MEASUREMENT_REFUSALS: [&str; 6] = [
    "cache_served",
    "statistics_missing",
    "not_the_query_submitted",
    "ambiguous_log_entry",
    "tag_mismatch",
    "result_rows_differ",
];

fn not_a_folder(root: &str, operation: &str) -> String {
    format!(
        "{root} is a table, and {operation} needs a folder of files: the engine builds no index \
         inside a database (ADR 027); reconcile, why, and fix (verify, then a refusal naming what \
         to run) work over a table"
    )
}

/// Result alias used throughout the engine.
pub type Result<T> = std::result::Result<T, Error>;

/// What a hash given to an operation names. Errors about a hash carry it,
/// so each surface (the CLI's flags, the MCP server's fields) can say which
/// argument to fix without reading a message.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HashKind {
    /// An Evidence record.
    Evidence,
    /// A Proposal record.
    Proposal,
    /// A Certificate record.
    Certificate,
    /// A Receipt record.
    Receipt,
    /// A recorded workload (a cache entry, not a ledger record).
    Workload,
}

impl HashKind {
    /// The kind's name: `evidence`, `proposal`, `certificate`, `receipt`,
    /// `workload`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Evidence => "evidence",
            Self::Proposal => "proposal",
            Self::Certificate => "certificate",
            Self::Receipt => "receipt",
            Self::Workload => "workload",
        }
    }

    /// The name with its article: "an evidence record", "a proposal".
    #[must_use]
    pub fn with_article(self) -> &'static str {
        match self {
            Self::Evidence => "an evidence record",
            Self::Proposal => "a proposal",
            Self::Certificate => "a certificate",
            Self::Receipt => "a receipt",
            Self::Workload => "a workload",
        }
    }
}

impl fmt::Display for HashKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

fn not_indexed(root: &str, operation: &str, needs: Option<&str>) -> String {
    match needs {
        None => format!("{root} is not indexed, so {operation} has no index to plan with"),
        // A sketch is read, never planned with, and is not a key spec.
        Some(spec) if spec.starts_with("sketch:") => format!(
            "{root} has no complete sketch holding {}, so {operation} has no sketch to read",
            spec.trim_start_matches("sketch:")
        ),
        Some(spec) => format!(
            "{root} has no complete {spec} key spec, so {operation} has no index to plan with"
        ),
    }
}

fn not_retained(index: &str, seq: u64, retained: &[u64]) -> String {
    match (retained.first(), retained.last()) {
        // Sequences start at 1, so 0 was never committed either.
        (Some(&oldest), Some(&current)) if seq > current || seq == 0 => format!(
            "manifest sequence {seq} has not been committed in the index at {index}; the current \
             sequence is {current} and the oldest retained is {oldest}"
        ),
        (Some(&oldest), Some(&current)) => format!(
            "manifest sequence {seq} is no longer retained in the index at {index}: the oldest \
             retained is {oldest} (current {current}); garbage collection drops a sequence once \
             it deletes a segment that sequence used, and commits made before this release kept \
             no copy"
        ),
        _ => format!("manifest sequence {seq} cannot be pinned: there is no index at {index}"),
    }
}

// ---------------------------------------------------------------------------
// Identity and hashing
// ---------------------------------------------------------------------------

/// A BLAKE3 content hash. Records, segments, and cache artifacts are named by
/// this. Serialized as lowercase hex.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Hash(pub [u8; 32]);

impl Hash {
    /// Hashes arbitrary bytes.
    #[must_use]
    pub fn of(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    /// Hashes the canonical JSON form of a serializable value. Canonical means
    /// `serde_json` default output for the record's Rust type: struct fields in
    /// declaration order, no whitespace. Field order is therefore part of the
    /// hash, which is why changing a record type is a contract change.
    pub fn of_record<T: Serialize>(value: &T) -> Result<Self> {
        let bytes = serde_json::to_vec(value)?;
        Ok(Self::of(&bytes))
    }

    /// Lowercase hex.
    #[must_use]
    pub fn to_hex(&self) -> String {
        use std::fmt::Write as _;
        self.0.iter().fold(String::with_capacity(64), |mut acc, b| {
            let _ = write!(acc, "{b:02x}");
            acc
        })
    }

    /// Parses lowercase or uppercase hex.
    pub fn from_hex(s: &str) -> Result<Self> {
        if s.len() != 64 {
            return Err(Error::Other(format!(
                "hash must be 64 hex chars, got {}",
                s.len()
            )));
        }
        let mut out = [0u8; 32];
        for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
            let hi = hex_val(chunk[0])?;
            let lo = hex_val(chunk[1])?;
            out[i] = (hi << 4) | lo;
        }
        Ok(Self(out))
    }
}

fn hex_val(c: u8) -> Result<u8> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(Error::Other(format!("invalid hex char {c:?}"))),
    }
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hash({})", &self.to_hex()[..12])
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl Serialize for Hash {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for Hash {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Self::from_hex(&s).map_err(serde::de::Error::custom)
    }
}

/// With the `schema` feature: a lowercase 64-character hex string, exactly
/// what [`Hash`](struct@Hash)'s `Serialize` writes. Non-semantic: it changes no
/// serialization, no hash, and nothing in the default build.
#[cfg(feature = "schema")]
impl schemars::JsonSchema for Hash {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Hash".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "description": "A BLAKE3 content hash: 64 lowercase hex characters.",
            "pattern": "^[0-9a-f]{64}$"
        })
    }
}

/// Dense identifier for a row group within one index's global id space.
/// Assigned at first sighting of `(FileFingerprint, index_in_file)` and never
/// reused (ADR 013).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct RowGroupId(pub u32);

/// Identity of a source file. Path alone is not identity; an in-place rewrite
/// changes `size`, `etag`, or `tail_blake3` and is detected as drift.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct FileFingerprint {
    /// Full path or URL as the source reports it.
    pub path: String,
    /// Size in bytes.
    pub size: u64,
    /// Object-store etag, or an mtime string for local files, when available.
    pub etag: Option<String>,
    /// Content hash per the fingerprint policy recorded in the manifest:
    /// BLAKE3 of the last 64 KiB (or the whole file if smaller) under the
    /// default `tail64k` policy, or of the whole file under `full`. The field
    /// name reflects the default; docs/format.md states the two limits.
    pub tail_blake3: Hash,
}

// ---------------------------------------------------------------------------
// Sources
// ---------------------------------------------------------------------------

/// Per-column statistics for one row group, as the source reports them.
/// `min`/`max` are canonical-encoded bytes (ADR 016) so they compare without
/// knowing the type.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ColumnStats {
    /// Column name.
    pub column: String,
    /// Canonical-encoded lower bound, if the source knows it. A bound, not
    /// necessarily an attained value; inexact statistics are accepted. For
    /// floats the bound excludes NaN, so a NaN probe is never pruned by range
    /// statistics (docs/format.md, "Statistics semantics").
    pub min: Option<Vec<u8>>,
    /// Canonical-encoded upper bound, with the same semantics as `min`.
    pub max: Option<Vec<u8>>,
    /// Null count, if the source knows it.
    pub null_count: Option<u64>,
    /// Distinct count, if the source knows it (Parquet rarely does; the
    /// profiler estimates otherwise).
    pub distinct_count: Option<u64>,
    /// Whether the source carries a bloom filter for this column in this row
    /// group that the engine can reuse.
    pub has_bloom: bool,
    /// Compressed bytes of this column chunk, if the source knows it. Lets
    /// replay cost a projection, not only a row-group read.
    pub bytes: Option<u64>,
}

/// One row group as the source reports it. For formats without row groups
/// (CSV, JSON lines) the source defines fixed byte-range chunks and reports
/// them here; nothing above this layer knows the difference.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct RowGroupMeta {
    /// Global id within the index. Assigned by the build crate, not the source;
    /// a source leaves this as `None`.
    pub id: Option<RowGroupId>,
    /// The file this row group belongs to.
    pub file: FileFingerprint,
    /// Position within the file, zero-based.
    pub index_in_file: u32,
    /// Row count.
    pub rows: u64,
    /// Compressed bytes on disk for this row group.
    pub bytes: u64,
    /// Per-column statistics, in schema order.
    pub stats: Vec<ColumnStats>,
    /// Byte range `[start, end)` of this row group within the file, for
    /// sources whose row groups are synthetic chunks (CSV, JSON lines). The
    /// build crate persists it in the manifest so the id stays stable
    /// (docs/format.md). `None` for formats with native row groups.
    pub byte_range: Option<(u64, u64)>,
}

/// A source of columnar files. Implemented per format in `bellwether-source`.
/// Every method is read-only and must bound its memory to one row group's
/// projected columns (ADR 018).
pub trait Source: Send + Sync {
    /// Lists files under `root` with their fingerprints. Order is unspecified.
    fn discover(&self, root: &str) -> Result<Vec<FileFingerprint>>;
    /// Reports the row groups of one file with their statistics.
    fn row_groups(&self, file: &FileFingerprint) -> Result<Vec<RowGroupMeta>>;
    /// Reads only `columns` from one row group.
    fn read_columns(
        &self,
        rg: &RowGroupMeta,
        columns: &[String],
    ) -> Result<arrow::array::RecordBatch>;
    /// The Arrow schema of the dataset as the source infers it.
    fn schema(&self, root: &str) -> Result<arrow::datatypes::SchemaRef>;
}

// ---------------------------------------------------------------------------
// Stores
// ---------------------------------------------------------------------------

/// Opaque version token returned by a store for a path (an etag, a generation,
/// a content hash for local files).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct StoreVersion(pub String);

/// Byte storage with conditional writes. Implemented over `object_store` in
/// `bellwether-store`. The two conditional methods are what make manifest
/// commits atomic without locks (ADR 003, ADR 015).
pub trait Store: Send + Sync {
    /// Reads a path. Returns the bytes and the version token. An absent path
    /// is `Error::NotFound`-shaped: implementations return
    /// `Error::Store(msg)` only for I/O failures and must return
    /// `Error::NotFound(Hash::of(path.as_bytes()))` when the path does not
    /// exist, so callers can distinguish "missing" from "broken".
    fn get(&self, path: &str) -> Result<(Bytes, StoreVersion)>;
    /// Writes only if the path does not exist.
    fn put_if_absent(&self, path: &str, body: Bytes) -> Result<StoreVersion>;
    /// Writes only if the current version matches `expected`.
    fn put_if_version(
        &self,
        path: &str,
        body: Bytes,
        expected: &StoreVersion,
    ) -> Result<StoreVersion>;
    /// Unconditional write for content-addressed paths, where the name is the
    /// hash and collisions are by definition identical.
    fn put_content_addressed(&self, path: &str, body: Bytes) -> Result<()>;
    /// Lists paths under a prefix.
    fn list(&self, prefix: &str) -> Result<Vec<String>>;
    /// Deletes a path. Used only by garbage collection after the retention
    /// window (ADR 013).
    fn delete(&self, path: &str) -> Result<()>;
}

// ---------------------------------------------------------------------------
// Keys and plans
// ---------------------------------------------------------------------------

/// What an index key looks like. Every variant takes a column list; a
/// composite key is not a separate kind, it is a list of length greater than
/// one, encoded memcomparably (ADR 002).
///
/// The options added in storage version 2 (`counts`, `bounds`, and the
/// `Bitemporal` kind; ADR 028) are omitted from the JSON when off, so a spec
/// without them serializes as it did in storage version 1. Each is part of
/// the spec's [`KeySpec::identity`]: two specs differing in one are two
/// specs, and redeclaring a spec with another option rebuilds it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum KeySpec {
    /// Exact set membership per row group.
    Exact {
        /// Key columns in prefix order.
        columns: Vec<String>,
        /// Store, per key and per row group holding it, the number of rows
        /// whose key tuple is that key (storage version 2), so a count of
        /// keys is answered from the index with no file read.
        #[serde(default, skip_serializing_if = "is_false")]
        counts: bool,
        /// A time column whose per-key, per-row-group `[min, max]` the
        /// segment carries beside each key (`exact:symbol@ts`), so a window
        /// lookup prunes by each key's own times. Storage version 1 kept
        /// these bounds in a `bounds` sidecar segment.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bounds: Option<String>,
    },
    /// Split-block bloom filter per block (ADR 008).
    Bloom {
        /// Key columns in prefix order.
        columns: Vec<String>,
        /// Target false-positive rate, strictly between 0 and 1.
        fpr: f64,
        /// A time column whose per-key, per-row-group bounds are kept
        /// beside the filters (`bloom:symbol@0.01@ts`). A bloom has no
        /// per-key record to carry them, so they stay a `bounds` segment
        /// listed under this spec.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bounds: Option<String>,
    },
    /// Per-row-group min/max only.
    Range {
        /// The single ordered column.
        column: String,
    },
    /// Exact membership plus a version chain per key for `latest` and `asof`.
    Temporal {
        /// Key columns in prefix order.
        columns: Vec<String>,
        /// Timestamp column that orders versions. Nulls rank last (ADR 014).
        ts_column: String,
        /// Store, per key and per row group holding it, the number of rows
        /// of that key there, over all versions (storage version 2).
        #[serde(default, skip_serializing_if = "is_false")]
        counts: bool,
    },
    /// Bitemporal versions per key (`temporal2:id@valid,known`): every
    /// distinct `(valid, known, row group)` triple over rows where neither
    /// time is null, for as-of lookups on two clocks. Storage version 1 kept
    /// these in a `temporal2` sidecar segment.
    Bitemporal {
        /// Key columns in prefix order.
        columns: Vec<String>,
        /// The time at which a version is true in the world.
        valid_column: String,
        /// The time at which a version became known.
        known_column: String,
    },
    // --- phase 5 kinds ---
    /// An index over a value derived from columns by a closed expression
    /// (`json_get`, `lower`, `substr`, `cast`, `date_trunc`, `hash`; no user
    /// code): the derived value is a virtual column `name`, indexed by an
    /// ordinary exact (optionally with counts) or bloom segment (`over`).
    /// Lookups match it only by normalized expression equality, never by
    /// `name`.
    Expr {
        /// The virtual column's name. Never a dataset column.
        name: String,
        /// The expression in normalized form (`bellwether_core::expr`).
        expr: String,
        /// The segment built over the virtual column: `Exact { columns:
        /// [name], counts }` or `Bloom { columns: [name], fpr }`.
        over: Box<KeySpec>,
        /// Conformance vectors for every function the expression calls, so
        /// a reader recomputes them and refuses the key if its evaluator
        /// disagrees.
        conformance: Vec<ExprVector>,
    },
    /// A token or trigram index over one string column (B5): per token, the
    /// row groups holding a row whose case-folded value has it. Serves
    /// [`Predicate::Text`] lookups (`has_token`, `contains`, `has_prefix`),
    /// never key lookups. Spelled `text:<column>,tokenizer=words|trigram`.
    Text {
        /// The string column.
        column: String,
        /// How a value is split into tokens.
        tokenizer: Tokenizer,
        /// With `Some(fpr)`, one split-block bloom filter per row group over
        /// the row group's tokens at that false-positive rate (ADR 008)
        /// instead of a token dictionary; strictly between 0 and 1.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bloom: Option<f64>,
    },
    /// Inverted index over a list column (B4): per element value, the row
    /// groups holding a row whose list contains it. `column` is an Arrow
    /// `List<T>` or `LargeList<T>` of a type with a canonical encoding
    /// (ADR 016), or a `Map<K, V>` indexed by its keys. A null list, an
    /// empty list, and a null element are not indexed (ADR 014). It serves
    /// only membership lookups (`contains_any`, `contains_all`), never a
    /// lookup by the column's value as a whole.
    List {
        /// The list or map column.
        column: String,
    },
    /// Dataset-pair key overlap (B3): the keys two datasets share on one
    /// column pair, for semi-joins, anti-joins, and `in (subquery)` between
    /// them. Declared on the left dataset (the data root, named by its
    /// folder) and stored in its manifest; the right dataset is the folder
    /// named `right` beside the root, read through its own index. Per key
    /// value on both sides, a row-group bitmap per side (each in its own
    /// dataset's row-group id space); keys on one side only are counted per
    /// side. Built from both sides' complete exact keys on the columns and
    /// pinned to the right manifest's sequence (ADR 020): stale when either
    /// side commits again. Spelled `join:<dataset>.<cols>=<right>.<cols>`
    /// (`join:orders.order_id=payables.order_id`), with `,counts` for
    /// per-side row counts. Serves only semi-join lookups and overlap
    /// statistics, never a lookup on one dataset alone.
    Join {
        /// The left dataset's name: the data root's folder name.
        dataset: String,
        /// The left key columns, in prefix order.
        columns: Vec<String>,
        /// The right dataset's name: a folder beside the data root.
        right: String,
        /// The right key columns, pairwise with `columns`.
        right_columns: Vec<String>,
        /// Store, per shared key and per row group of each side holding
        /// it, its rows there (storage version 2), so the rows a semi-join
        /// keeps and the size of the join are exact from the index. Needs
        /// counted exact keys on both sides.
        #[serde(default, skip_serializing_if = "is_false")]
        counts: bool,
    },
    /// Per-key, per-row-group aggregates of one value column (B7,
    /// `agg:<key>[,<key>]@<value>{sum,count,min,max}`): for every key and
    /// every row group holding it, the rows, the rows whose value is null,
    /// and, as declared, the sum (exact for decimals at the dataset scale
    /// and for integers; a Kahan-compensated pair for floats, which is
    /// never exact), the minimum, and the maximum of the non-null values.
    /// Declared beside an `exact:<key>,counts` spec, whose counts prove
    /// which aggregates cover a lookup's rows. Serves `aggregate` lookups
    /// only, never a key lookup. A row whose leading key column is null is
    /// not aggregated (ADR 014).
    Agg {
        /// Key columns in prefix order.
        columns: Vec<String>,
        /// The aggregated column.
        value: String,
        /// The functions stored, in [`AggregateFn::canonical`] order; `count`
        /// is always among them (the rows prove coverage).
        functions: Vec<AggregateFn>,
    },
}

/// How a [`KeySpec::Text`] splits a value into tokens. Both work on the
/// value case-folded: each code point replaced by its Unicode full case
/// folding (`CaseFolding.txt`, C and F), with no context; never
/// normalized (`docs/format.md`, "Text keys").
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum Tokenizer {
    /// The words of the folded value at Unicode word boundaries (UAX #29).
    Words,
    /// Every 3-byte window of the folded value's UTF-8 bytes.
    Trigram,
}

impl Tokenizer {
    /// The spelling in a spec: `words` or `trigram`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Words => "words",
            Self::Trigram => "trigram",
        }
    }
}

/// A text match ([`Predicate::Text`]). Each compares the row's case-folded
/// value with the case-folded argument.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum TextOp {
    /// One of the value's words equals the argument, which is one word.
    HasToken,
    /// The value contains the argument as a substring.
    Contains,
    /// One of the value's words starts with the argument.
    HasPrefix,
}

impl TextOp {
    /// The spelling in results and plans: `has_token`, `contains`, or
    /// `has_prefix`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::HasToken => "has_token",
            Self::Contains => "contains",
            Self::HasPrefix => "has_prefix",
        }
    }
}

#[allow(clippy::trivially_copy_pass_by_ref)] // serde's skip_serializing_if signature
fn is_false(b: &bool) -> bool {
    !*b
}

/// One conformance vector of an [`KeySpec::Expr`]: a closed expression (no
/// column) and the canonical encoding (ADR 016) of its value, lowercase
/// hex, or `None` when the value is null.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ExprVector {
    /// A closed expression over literals, in normalized form.
    pub expr: String,
    /// Its value's canonical encoding in lowercase hex; `None` for null.
    pub value: Option<String>,
}

impl KeySpec {
    /// The key columns, in prefix order. For an [`KeySpec::Expr`], the
    /// virtual column.
    #[must_use]
    pub fn columns(&self) -> &[String] {
        match self {
            Self::Exact { columns, .. }
            | Self::Bloom { columns, .. }
            | Self::Temporal { columns, .. }
            | Self::Bitemporal { columns, .. }
            | Self::Join { columns, .. }
            | Self::Agg { columns, .. } => columns,
            Self::Range { column } | Self::Text { column, .. } | Self::List { column } => {
                std::slice::from_ref(column)
            }
            Self::Expr { over, .. } => over.columns(),
        }
    }

    /// The spec whose layout the segments are stored in: `over` for an
    /// [`KeySpec::Expr`], the spec itself otherwise.
    #[must_use]
    pub fn physical(&self) -> &KeySpec {
        match self {
            Self::Expr { over, .. } => over.physical(),
            other => other,
        }
    }

    /// Whether the spec stores per-key per-row-group counts.
    #[must_use]
    pub fn counts(&self) -> bool {
        match self {
            Self::Expr { over, .. } => over.counts(),
            other => matches!(
                other,
                Self::Exact { counts: true, .. }
                    | Self::Temporal { counts: true, .. }
                    | Self::Join { counts: true, .. }
            ),
        }
    }

    /// The display name and the spec's slot: the kind and the key columns
    /// (`exact:symbol`, `bloom:symbol`, `temporal:id`, `temporal2:id`). An
    /// index holds at most one spec per name, so declaring another spec of
    /// the same name (another false-positive rate, counts on or off) is a
    /// redefinition that rebuilds it. A text key's name also carries its
    /// tokenizer (`text:body,tokenizer=words`), so a words and a trigram key
    /// coexist on one column; its bloom rate is not part of the name. The
    /// manifest keys segments and completeness by [`KeySpec::identity`],
    /// which carries every field.
    #[must_use]
    pub fn name(&self) -> String {
        let kind = match self {
            Self::Exact { .. } => "exact",
            Self::Bloom { .. } => "bloom",
            Self::Range { .. } => "range",
            Self::Temporal { .. } => "temporal",
            Self::Bitemporal { .. } => "temporal2",
            Self::Text {
                column, tokenizer, ..
            } => return format!("text:{column},tokenizer={}", tokenizer.as_str()),
            Self::Expr {
                name, expr, over, ..
            } => {
                let inner = over.name();
                let inner = inner.split(':').next().unwrap_or_default();
                return format!("expr:{name}={expr}:{inner}");
            }
            Self::List { .. } => "list",
            Self::Join {
                dataset,
                columns,
                right,
                right_columns,
                ..
            } => {
                return format!(
                    "join:{dataset}.{}={right}.{}",
                    columns.join(","),
                    right_columns.join(",")
                )
            }
            Self::Agg { value, .. } => return format!("agg:{}@{value}", self.columns().join(",")),
        };
        format!("{kind}:{}", self.columns().join(","))
    }

    /// The canonical full-spec string (`docs/format.md`, "Key-spec
    /// identity"): every field of the spec, in the short form the command
    /// line reads. Storage version 2 manifests key segments and completeness
    /// by it, so two specs that differ in any field never share a name.
    ///
    /// | Spec | Identity |
    /// |---|---|
    /// | exact, columns `a, b` | `exact:a,b` |
    /// | exact with counts | `exact:a,b,counts` |
    /// | exact with bounds on `ts` | `exact:a@ts` (with counts `exact:a@ts,counts`) |
    /// | bloom at `0.01` | `bloom:a@0.01` (with bounds `bloom:a@0.01@ts`) |
    /// | range | `range:ts` |
    /// | temporal | `temporal:id@ts` (with counts `temporal:id@ts,counts`) |
    /// | bitemporal | `temporal2:id@valid,known` |
    /// | expr | `expr:sym=json_get(payload,'$.symbol'):exact` (with counts `...:exact,counts`; bloom `...:bloom@0.01`) |
    /// | list (B4) | `list:tags` |
    /// | join (B3) | `join:orders.order_id=payables.order_id` (with counts `...,counts`) |
    /// | agg (B7) | `agg:account@amount{sum,count,min,max}` (functions in canonical order) |
    ///
    /// The false-positive rate is Rust's shortest round-trip spelling of the
    /// `f64`. The identity is one-to-one: no key or time column a spec names
    /// may contain `,` or `@`, the separators ([`KeySpec::check_names`]),
    /// and a spec whose last key column of several is named `counts` is
    /// refused when declared, so the trailing `counts` is unambiguous.
    #[must_use]
    pub fn identity(&self) -> String {
        let counts = if self.counts() { ",counts" } else { "" };
        match self {
            Self::Exact {
                columns, bounds, ..
            } => match bounds {
                Some(ts) => format!("exact:{}@{ts}{counts}", columns.join(",")),
                None => format!("exact:{}{counts}", columns.join(",")),
            },
            Self::Bloom {
                columns,
                fpr,
                bounds,
            } => match bounds {
                Some(ts) => format!("bloom:{}@{fpr}@{ts}", columns.join(",")),
                None => format!("bloom:{}@{fpr}", columns.join(",")),
            },
            Self::Range { column } => format!("range:{column}"),
            Self::List { column } => format!("list:{column}"),
            Self::Join { .. } => format!("{}{counts}", self.name()),
            Self::Agg {
                columns,
                value,
                functions,
            } => format!(
                "agg:{}@{value}{{{}}}",
                columns.join(","),
                functions
                    .iter()
                    .map(|f| f.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Self::Temporal {
                columns, ts_column, ..
            } => format!("temporal:{}@{ts_column}{counts}", columns.join(",")),
            Self::Bitemporal {
                columns,
                valid_column,
                known_column,
            } => format!(
                "temporal2:{}@{valid_column},{known_column}",
                columns.join(",")
            ),
            Self::Text {
                column,
                tokenizer,
                bloom,
            } => match bloom {
                Some(fpr) => format!("text:{column},tokenizer={},bloom={fpr}", tokenizer.as_str()),
                None => format!("text:{column},tokenizer={}", tokenizer.as_str()),
            },
            // The name is an identifier and the expression is normalized
            // (a `:` only inside quotes), so the inner kind after the last
            // unquoted `:` is unambiguous.
            Self::Expr {
                name, expr, over, ..
            } => match over.as_ref() {
                Self::Bloom { fpr, .. } => format!("expr:{name}={expr}:bloom@{fpr}"),
                _ => format!("expr:{name}={expr}:exact{counts}"),
            },
        }
    }

    /// The identity rules a spec declared now must meet, so that no two
    /// specs declared from now on share an identity: no key or time column
    /// may contain `,` or `@`, the separators of [`KeySpec::identity`]
    /// (`exact:a,counts` is both the spec on `a` that stores counts and the
    /// spec on a column named `a,counts`), and a column named `counts`
    /// cannot come last in a composite exact spec without bounds (for the
    /// same reason). Checked on new declarations only (a build's given specs
    /// the index does not already declare, a proposal's spec): a spec an
    /// index already declares is read and indexed as it is, and storage
    /// version 2 refuses only an index two of whose specs actually share an
    /// identity (`docs/format.md`, "Key-spec identity").
    pub fn check_names(&self) -> Result<()> {
        let times: Vec<&String> = match self {
            Self::Exact { bounds, .. } | Self::Bloom { bounds, .. } => bounds.iter().collect(),
            Self::Temporal { ts_column, .. } => vec![ts_column],
            Self::Bitemporal {
                valid_column,
                known_column,
                ..
            } => vec![valid_column, known_column],
            // An expr key's name is an identifier and its expression is
            // checked by `bellwether_core::expr::check_spec`.
            Self::Range { .. }
            | Self::Expr { .. }
            | Self::Text { .. }
            | Self::List { .. }
            | Self::Join { .. } => Vec::new(),
            Self::Agg { value, .. } => vec![value],
        };
        let right: &[String] = match self {
            Self::Join { right_columns, .. } => right_columns,
            _ => &[],
        };
        if let Some(c) = self
            .columns()
            .iter()
            .chain(right)
            .chain(times)
            .find(|c| c.contains([',', '@']))
        {
            return Err(Error::KeySpec(format!(
                "the key spec column \"{c}\" contains ',' or '@', which separate the parts of a \
                 key spec's identity, so a new key spec cannot name it; index a column whose \
                 name has neither, or rename this one"
            )));
        }
        if let Self::Agg { columns, value, .. } = self {
            if let Some(c) = columns
                .iter()
                .chain(std::iter::once(value))
                .find(|c| c.contains(['{', '}']))
            {
                return Err(Error::KeySpec(format!(
                    "the agg column \"{c}\" contains '{{' or '}}', which enclose an agg key's \
                     functions in its identity, so a new agg key cannot name it; aggregate a \
                     column whose name has neither, or rename this one"
                )));
            }
        }
        if let Self::Exact {
            columns,
            bounds: None,
            ..
        } = self
        {
            if columns.len() > 1 && columns.last().is_some_and(|c| c == "counts") {
                let rest = columns[..columns.len() - 1].join(",");
                return Err(Error::KeySpec(format!(
                    "a key column named \"counts\" cannot come last in a composite exact spec, \
                     because exact:{rest},counts already means the spec on {rest} that stores \
                     counts; list it first instead, as exact:counts,{rest}"
                )));
            }
        }
        if let Self::Join {
            columns,
            right_columns,
            ..
        } = self
        {
            if let Some(c) = columns
                .iter()
                .chain(right_columns)
                .find(|c| c.contains('='))
            {
                return Err(Error::KeySpec(format!(
                    "the join column \"{c}\" contains '=', which separates the two sides of a \
                     join key's identity, so a new join key cannot name it; rename the column"
                )));
            }
            if right_columns.last().is_some_and(|c| c == "counts") {
                return Err(Error::KeySpec(
                    "a right join column named \"counts\" cannot come last, because a trailing \
                     \",counts\" already means a join key that stores counts; list the columns \
                     in another order on both sides"
                        .into(),
                ));
            }
        }
        Ok(())
    }

    /// The first two of `specs` that share an identity, if any. Specs
    /// declared by this binary never do ([`KeySpec::check_names`]); a
    /// storage version 1 index, whose specs were declared without that rule
    /// and keyed by name, can (two temporal specs whose time column holds an
    /// `@`: `temporal:id` on `ts@x` and `temporal:id@ts` on `x`).
    #[must_use]
    pub fn shared_identity(specs: &[Self]) -> Option<(&Self, &Self)> {
        specs.iter().enumerate().find_map(|(i, a)| {
            specs[i + 1..]
                .iter()
                .find(|b| b.identity() == a.identity())
                .map(|b| (a, b))
        })
    }
}

/// A residual predicate the consumer applies after reading the planned row
/// groups to make the result row-exact (ADR 011). Deliberately small.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Predicate {
    /// Column tuple is one of the listed canonical-encoded values.
    In {
        /// Columns, in the same order the values were encoded.
        columns: Vec<String>,
        /// Canonical-encoded key tuples.
        values: Vec<Vec<u8>>,
    },
    /// Column is within the canonical-encoded bounds, inclusive.
    Between {
        /// The column.
        column: String,
        /// Lower bound.
        lo: Vec<u8>,
        /// Upper bound.
        hi: Vec<u8>,
    },
    /// Keep only the row with the greatest `ts_column` per key tuple.
    LatestPerKey {
        /// Key columns.
        columns: Vec<String>,
        /// Timestamp column.
        ts_column: String,
    },
    /// Drop rows whose `ts_column` is older than the index-wide newest
    /// timestamp recorded for their key (invariant 5). One entry per queried
    /// key: `(canonical key bytes, canonical ts bytes)`. Rows for keys not
    /// listed are kept. Applied before `LatestPerKey`.
    NotOlderThan {
        /// Key columns.
        columns: Vec<String>,
        /// Timestamp column.
        ts_column: String,
        /// Per-key lower bounds.
        bounds: Vec<(Vec<u8>, Vec<u8>)>,
    },
    /// All of the predicates.
    And {
        /// Conjuncts.
        all: Vec<Predicate>,
    },
    /// Keep the rows whose `column` matches at least one of `values` under
    /// `operator`, both sides case-folded (see [`TextOp`]); a null never
    /// matches (ADR 014). The residual of a text lookup
    /// ([`KeySpec::Text`]).
    Text {
        /// The string column.
        column: String,
        /// The match.
        operator: TextOp,
        /// The arguments as given (the consumer folds them as the column).
        values: Vec<String>,
    },
    /// Keep rows whose column tuple is among the keys `keys_from` names
    /// (contract v3; priced by relational replay, WP15).
    SemiJoin {
        /// Columns, in the order the keys are encoded.
        columns: Vec<String>,
        /// Where the keys come from.
        keys_from: KeysFrom,
    },
    /// Join two inputs on equal column tuples (contract v3; priced by
    /// relational replay, WP15).
    Join {
        /// Left join columns.
        left: Vec<String>,
        /// Right join columns, pairwise with `left`.
        right: Vec<String>,
        /// Inner or left.
        kind: JoinKind,
    },
    /// The rows of whole partitions of `columns` (a window's input,
    /// contract v3; priced by relational replay, WP15).
    WindowPartition {
        /// Partition columns.
        columns: Vec<String>,
    },
    // --- phase 5 kinds ---
    /// The value of `expr`, recomputed per row from the row's columns
    /// (`bellwether_core::expr`), is one of the listed canonical-encoded
    /// values. A row whose value is null matches nothing (ADR 014).
    ExprIn {
        /// The expression, in normalized form.
        expr: String,
        /// Canonical-encoded values of the expression's type.
        values: Vec<Vec<u8>>,
    },
    /// The list (or map, by its keys) in `column` holds an element equal to
    /// at least one of `values`, each the canonical encoding (ADR 016) of
    /// one value at the element's dataset type; a float zero is listed in
    /// both encodings. A null or empty list, and a null element, match
    /// nothing (ADR 014).
    ContainsAny {
        /// The list or map column.
        column: String,
        /// Canonical element encodings.
        values: Vec<Vec<u8>>,
    },
    /// The list (or map, by its keys) in `column` holds, for every group in
    /// `values`, an element equal to one of the group's encodings. A group
    /// is one queried value: one canonical encoding, or both encodings of a
    /// float zero. A null or empty list matches nothing (ADR 014).
    ContainsAll {
        /// The list or map column.
        column: String,
        /// One group of canonical element encodings per queried value.
        values: Vec<Vec<Vec<u8>>>,
    },
}

/// Where a semi-join's keys come from (contract v3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "from", rename_all = "snake_case")]
pub enum KeysFrom {
    /// The recorded run's key set.
    Run,
    /// These canonical-encoded key tuples.
    Values {
        /// Canonical-encoded key tuples.
        values: Vec<Vec<u8>>,
    },
    /// The output of another node of the same [`ConsumerPlan`].
    Node {
        /// The node's index in [`ConsumerPlan::nodes`].
        node: u32,
        /// The node's output columns that hold the keys.
        columns: Vec<String>,
    },
}

/// A join's kind (contract v3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum JoinKind {
    /// Rows with a match on both sides.
    Inner,
    /// Every left row, with its matches or none.
    Left,
}

/// The compressed bytes of one column over a scan entry's row groups.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ColumnBytes {
    /// The column.
    pub column: String,
    /// Its compressed bytes, summed over the entry's row groups from the
    /// manifest's per-column bytes. Exact.
    pub bytes: u64,
}

/// One file's share of a scan plan.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ScanEntry {
    /// The file.
    pub file: FileFingerprint,
    /// Row groups to read, by `index_in_file`. Serialized as a sorted list of
    /// indices so the JSON is readable; the in-memory form is a bitmap.
    #[serde(with = "bitmap_as_list")]
    #[cfg_attr(feature = "schema", schemars(with = "Vec<u32>"))]
    pub row_groups: RoaringBitmap,
    /// Compressed bytes those row groups occupy, summed from stats. Exact,
    /// not estimated.
    pub bytes: u64,
    /// With a projection: the bytes of each projected column over these row
    /// groups, in the projection's order (contract v3). A text source's
    /// chunks have no per-column bytes and carry none. Absent without a
    /// projection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<ColumnBytes>>,
}

/// What the index guarantees (ADR 011): every row group holding a queried key
/// is present. Extra rows may be present; apply `residual` for row-exact
/// results.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ScanPlan {
    /// Manifest version the plan was computed against.
    pub manifest_version: String,
    /// Files and row groups to read.
    pub entries: Vec<ScanEntry>,
    /// Predicate to apply after reading, if the caller wants row-exact output.
    pub residual: Option<Predicate>,
    /// Total compressed bytes across entries.
    pub bytes: u64,
    /// Row groups considered before pruning, for the funnel.
    pub row_groups_total: u64,
    /// Row groups in the plan.
    pub row_groups_selected: u64,
    /// With a projection: the projected columns' bytes over every entry
    /// (the sum of the entries' `columns`; for a text source, `bytes`).
    /// Absent without a projection (contract v3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projected_bytes: Option<u64>,
    /// A read order that interleaves files: `(entry, row group)` pairs, in
    /// non-decreasing time minimum, when a complete range key on the
    /// lookup's time column gives one (contract v3). Absent otherwise;
    /// verify ignores it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order: Option<Vec<(u32, u32)>>,
    /// The rows holding the queried keys in the planned row groups: Exact
    /// when the serving spec stores counts (storage version 2, `counts`), so
    /// a gate on a key set's size reads the plan, not the files. Absent when
    /// the serving spec has no counts or the lookup is not by keys alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<Quantity>,
}

impl Eq for KeySpec {}

// ---------------------------------------------------------------------------
// Ledger records
// ---------------------------------------------------------------------------

/// Provenance carried by records derived from measurements (`Evidence`,
/// `Certificate`): the inputs they were derived from and how old the oldest
/// input was when the record was made (invariant 6).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Provenance {
    /// Hashes of cache artifacts, manifest, or other records this depends on.
    pub inputs: Vec<Hash>,
    /// Age of the oldest input at record time. Serialized as integer
    /// nanoseconds so every value round-trips exactly (a float-seconds
    /// encoding loses sub-second precision past about 97 days).
    #[serde(with = "duration_nanos")]
    #[cfg_attr(feature = "schema", schemars(with = "u64"))]
    pub oldest_input_age: Duration,
    /// Manifest version in effect, if any.
    pub manifest_version: Option<String>,
    /// RFC 3339 timestamp of record creation.
    pub created_at: String,
}

mod bitmap_as_list {
    use roaring::RoaringBitmap;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(b: &RoaringBitmap, s: S) -> Result<S::Ok, S::Error> {
        b.iter().collect::<Vec<u32>>().serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<RoaringBitmap, D::Error> {
        let v = Vec::<u32>::deserialize(d)?;
        Ok(v.into_iter().collect())
    }
}

mod duration_nanos {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        let nanos = u64::try_from(d.as_nanos())
            .map_err(|_| serde::ser::Error::custom("duration exceeds u64 nanoseconds"))?;
        nanos.serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        let nanos = u64::deserialize(d)?;
        Ok(Duration::from_nanos(nanos))
    }
}

/// Fanout measurement for one column or column tuple: how many files and row
/// groups one key value touches, at quantiles over observed values.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Fanout {
    /// Columns measured, in prefix order.
    pub columns: Vec<String>,
    /// Distinct values observed or estimated.
    pub distinct: u64,
    /// Whether `distinct` and the quantiles come from a sample of distinct
    /// values rather than all of them. Sampling is by value, never by row.
    pub sampled: bool,
    /// Files per value at p50, p95, max.
    pub files_per_value: [f64; 3],
    /// Row groups per value at p50, p95, max.
    pub row_groups_per_value: [f64; 3],
    /// Total files in the dataset, the denominator for pruning ratios.
    pub files_total: u64,
    /// Total row groups in the dataset.
    pub row_groups_total: u64,
    /// Distinct values the quantiles were computed over, when they come
    /// from a sample (`sampled`); absent when every value was measured
    /// (contract v3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_size: Option<u64>,
    /// The rank error of the sampled quantiles, as a fraction of the values
    /// (a p95 from a sample is the true quantile at a rank within this
    /// fraction of 0.95); absent when every value was measured (contract
    /// v3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quantile_error: Option<f64>,
}

/// A lint the profiler raised.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Lint {
    /// Stable rule id, for example `batch_stamp_key`.
    pub rule: String,
    /// Columns involved.
    pub columns: Vec<String>,
    /// One sentence a non-specialist can act on.
    pub message: String,
    /// Confidence in `[0, 1]` per the rule catalog rubric. Sampled or
    /// partially measured inputs lower it; the rubric says how.
    pub confidence: f64,
    /// Where the lint applies when it is about a consumer artifact rather
    /// than the dataset itself: a file path and step, or `None`.
    pub site: Option<String>,
    /// The rewrite the rule catalog maps this rule to, when it has one
    /// (contract v3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fix: Option<RewriteShape>,
}

/// What an observation found.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EvidenceBody {
    /// Output of `profile`.
    Profile {
        /// Dataset root.
        root: String,
        /// Per-column and per-candidate-tuple fanout.
        fanout: Vec<Fanout>,
        /// Lints raised.
        lints: Vec<Lint>,
        /// Recorded workload summary: key-set sizes at p50, p95, max.
        workload_keys: Option<[u64; 3]>,
        /// When the dataset was profiled as the lake twin of a query engine
        /// (`why --twin-of`, ADR 033): the engine's table identity beside
        /// the lake's, and what the twin predicts the key sample reads.
        /// Absent otherwise, and absent in records written before it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        twin: Option<TwinOf>,
    },
    /// Output of `locate`, `latest`, or `asof`.
    Plan {
        /// The plan.
        plan: ScanPlan,
    },
}

/// A named limit on a key set's size (`--gate <name>:<n>`): a membership
/// list a query engine accepts (`in:1000000`), or the size below which it
/// broadcasts the smaller side of a join (`join:200000`). The names are the
/// caller's; the engine compares and never interprets them, except that
/// the membership lint reads the gate named `in`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Gate {
    /// The gate's name.
    pub name: String,
    /// The largest key set the gate admits.
    pub limit: u64,
}

/// The lake twin's side of a diagnosis (ADR 033): which engine tables the
/// lake copies, their identity when the diagnosis ran, the lake's own state
/// at that moment, and the twin's prediction for the key sample. Every
/// count is exact for the lake and for the engine's table listing; nothing
/// here is an engine cost.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct TwinOf {
    /// The engine root (`clickhouse://host:port/db/table`), credentials and
    /// connection parameters removed.
    pub engine: String,
    /// The engine's table identity when the diagnosis ran: BLAKE3 over the
    /// engine root and every storage unit's path and identity, as the
    /// engine lists them (format.md, "Query engine sources").
    pub identity: Hash,
    /// Storage units the engine listed (parts, partitions, or extents).
    pub units: u64,
    /// Rows the engine's tables held, from the same listing.
    pub engine_rows: u64,
    /// The lake's manifest sequence (`0` when unindexed).
    pub sequence: u64,
    /// Rows the lake held.
    pub lake_rows: u64,
    /// The key sample's recorded workload, when one was given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workload: Option<Hash>,
    /// Distinct keys in the sample (the largest run's).
    #[serde(default)]
    pub keys: u64,
    /// Rows of the lake holding a sampled key: what the twin predicts a
    /// query over these keys must read at least.
    #[serde(default)]
    pub predicted_rows: u64,
    /// Row groups of the lake holding a sampled key.
    #[serde(default)]
    pub predicted_row_groups: u64,
    /// Files of the lake holding a sampled key.
    #[serde(default)]
    pub predicted_files: u64,
    /// The gates the key sample was compared against.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub gates: Vec<Gate>,
}

/// A record of something the engine observed. Agents read these.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Evidence {
    /// Record schema version.
    pub record_version: u32,
    /// Provenance.
    pub provenance: Provenance,
    /// The observation.
    pub body: EvidenceBody,
}

/// A rewrite proposal is data, never code the engine runs (threat model).
/// A rewrite of queries in a query engine (ADR 033) carries the query pairs
/// as text: the engine submits them to the engine named by the twin
/// Evidence the proposal cites, read-only, and never writes or emits a
/// query of its own.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct RewriteShape {
    /// Human-readable description of the current shape.
    pub before: String,
    /// Human-readable description of the target shape.
    pub after: String,
    /// The algebraic or data invariant the rewrite must preserve.
    pub invariant: String,
    /// For a rewrite of engine queries: the (before, after) query pairs,
    /// scored and verified as one set (ADR 023). Empty for a rewrite
    /// described in words, and absent in records written before it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pairs: Vec<QueryPair>,
    /// The key columns each pair's before and after results are reconciled
    /// by. Empty when `pairs` is.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub key: Vec<String>,
    /// The key sample the diagnosis ran on: its recorded workload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keys: Option<Hash>,
    /// The current shape as a typed plan, when the proposer has one
    /// (contract v3; relational replay prices it, WP15).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_plan: Option<ConsumerPlan>,
    /// The target shape as a typed plan (contract v3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_plan: Option<ConsumerPlan>,
}

/// One query of an engine rewrite and its replacement, as the agent wrote
/// them. Text only: never parsed, rewritten, or generated by the engine.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct QueryPair {
    /// The query as it runs today.
    pub before: String,
    /// The query that replaces it.
    pub after: String,
}

/// A consumer's query as a typed plan DAG, never SQL (contract v3; priced by
/// relational replay, WP15). Nodes refer to their inputs by index.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConsumerPlan {
    /// Plan format version; 1.
    pub version: u32,
    /// The nodes.
    pub nodes: Vec<PlanNode>,
    /// The index of the node whose output is the query's result.
    pub root: u32,
}

/// One operator of a [`ConsumerPlan`]. Inputs are node indices.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum PlanNode {
    /// Read columns of a named dataset.
    Scan {
        /// The dataset's name in the plan's dataset map.
        dataset: String,
        /// Columns read.
        columns: Vec<String>,
    },
    /// Keep the rows a predicate holds for.
    Filter {
        /// Input node.
        input: u32,
        /// The predicate.
        predicate: Predicate,
    },
    /// The newest version per key.
    Latest {
        /// Input node.
        input: u32,
        /// Key columns.
        columns: Vec<String>,
        /// The column ordering versions.
        ts: String,
    },
    /// The version per key current at the run's instant.
    AsOf {
        /// Input node.
        input: u32,
        /// Key columns.
        columns: Vec<String>,
        /// The column ordering versions.
        ts: String,
    },
    /// Keep rows whose key is among `keys`.
    SemiJoin {
        /// Input node.
        input: u32,
        /// Key columns.
        columns: Vec<String>,
        /// Where the keys come from.
        keys: KeysFrom,
    },
    /// Join two inputs on column pairs.
    Join {
        /// Left input node.
        left: u32,
        /// Right input node.
        right: u32,
        /// `(left column, right column)` pairs.
        on: Vec<(String, String)>,
        /// Inner or left.
        kind: JoinKind,
    },
    /// A window over partitions, optionally only those of some keys.
    Window {
        /// Input node.
        input: u32,
        /// Partition columns.
        partition_by: Vec<String>,
        /// The partitions of interest, if not all.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        keys: Option<KeysFrom>,
    },
    /// Group and aggregate.
    Aggregate {
        /// Input node.
        input: u32,
        /// Grouping columns.
        group_by: Vec<String>,
        /// The aggregates computed.
        measures: Vec<Measure>,
    },
    /// Repartition rows across workers by columns (a shuffle).
    Exchange {
        /// Input node.
        input: u32,
        /// Partitioning columns.
        by: Vec<String>,
    },
    /// Keep only these columns.
    Project {
        /// Input node.
        input: u32,
        /// Columns kept.
        columns: Vec<String>,
    },
}

/// One aggregate of a [`PlanNode::Aggregate`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Measure {
    /// The function.
    pub function: AggregateFn,
    /// The column it reads; `None` for `count(*)`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
}

/// An aggregate function.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum AggregateFn {
    /// Row count.
    Count,
    /// Sum.
    Sum,
    /// Minimum.
    Min,
    /// Maximum.
    Max,
}

impl AggregateFn {
    /// Every function, in the canonical order an agg key's identity lists
    /// them: `sum`, `count`, `min`, `max`.
    pub const CANONICAL: [Self; 4] = [Self::Sum, Self::Count, Self::Min, Self::Max];

    /// The spelling in specs and results: `sum`, `count`, `min`, `max`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Min => "min",
            Self::Max => "max",
        }
    }

    /// The function a spelling names, or `None`.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Self::CANONICAL.into_iter().find(|f| f.as_str() == s)
    }

    /// The functions an agg key stores when `given` are declared: each
    /// once, in [`AggregateFn::CANONICAL`] order, with `count` always among
    /// them (an agg key's rows prove which aggregates cover a lookup).
    #[must_use]
    pub fn canonical(given: &[Self]) -> Vec<Self> {
        Self::CANONICAL
            .into_iter()
            .filter(|f| *f == Self::Count || given.contains(f))
            .collect()
    }
}

/// The model every float sum of an `agg` key is labelled with: a sum of
/// `f64` values with Kahan-Babuska (Neumaier) compensation, per row group
/// and then across row groups in ascending row-group order. Never exact
/// (B7); every float sum prints `Measured` with this model.
pub const AGG_FLOAT_SUM_MODEL: &str = "agg.kahan_f64";

/// What an agent may propose. There is no score field in any variant; scoring
/// is the engine's (ADR 009).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProposalBody {
    /// Add or change an index key.
    Key {
        /// The proposed spec. Any [`KeySpec`], including one with per-key
        /// time bounds (`exact:symbol@ts`) or a bitemporal spec (contract
        /// v3), which storage version 1 could only declare as sidecars.
        spec: KeySpec,
        /// Specs proposed together with `spec` and applied in the same
        /// commit, for example a bloom with bounds beside an exact key
        /// (contract v3). Empty for a single spec, and then omitted.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        sidecars: Vec<KeySpec>,
    },
    /// Cluster or sort files by these columns.
    Layout {
        /// Columns to cluster by, in priority order.
        cluster_by: Vec<String>,
    },
    /// Rewrite a consumer query or pipeline step.
    Rewrite {
        /// The shape.
        shape: RewriteShape,
    },
}

/// A record of something an agent proposed.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Proposal {
    /// Record schema version.
    pub record_version: u32,
    /// Evidence hashes the proposal cites.
    pub evidence: Vec<Hash>,
    /// Who proposed: an agent name, a user, or `"engine"`.
    pub proposed_by: String,
    /// RFC 3339.
    pub created_at: String,
    /// The proposal.
    pub body: ProposalBody,
}

/// A number with how it was obtained (contract v3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Quantity {
    /// The value when `Exact`; the upper end of the interval when
    /// `Bounded` (the interval itself is in `exactness`); the model's figure
    /// when `Measured`.
    pub value: u64,
    /// How `value` was obtained.
    pub exactness: Exactness,
}

impl Quantity {
    /// An exact value.
    #[must_use]
    pub fn exact(value: u64) -> Self {
        Self {
            value,
            exactness: Exactness::Exact,
        }
    }
}

/// Cost of one recorded run under one configuration. Exact, from statistics,
/// never estimated (ADR 005).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct RunCost {
    /// Identifier of the recorded run.
    pub run_id: String,
    /// Keys in the run's key set.
    pub keys: u64,
    /// Files the run would read.
    pub files: u64,
    /// Row groups the run would read.
    pub row_groups: u64,
    /// Compressed bytes the run would read.
    pub bytes: u64,
    /// `true` when every number above is a sum of recorded statistics.
    /// `false` when the number comes from a stated model instead: a
    /// `ProposalBody::Layout`, whose proposed file layout does not exist yet,
    /// or a `ProposalBody::Rewrite` whose cost is measured rather than
    /// replayed, such as a step inside a database (ADR 027) or a query in a
    /// query engine (ADR 033). The Replay must then name the model. A key
    /// proposal is always exact. Never silently estimated (ADR 005). From
    /// record version 3, `exact` is true exactly when `exactness` is `Exact`
    /// (a ledger rule), so a reader may use either.
    pub exact: bool,
    /// How the numbers above were obtained (contract v3). Omitted when
    /// `Exact`, so an exact cost serializes as in record version 2.
    #[serde(default, skip_serializing_if = "Exactness::is_exact")]
    pub exactness: Exactness,
    /// Rows holding the run's keys in the row groups it reads (contract
    /// v3): exact, from the per-key counts of a serving spec that stores
    /// them on exactly the run's key columns (`ScanPlan.rows`). Absent when
    /// no serving spec counts. `Bounded` and `Measured` are reserved for
    /// costs priced without counts (relational replay, WP15).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<Quantity>,
    /// For a query run in a query engine (ADR 033): what the engine's own
    /// query log recorded for it, read by the engine by the tag it
    /// submitted the query under. Then `files` and `row_groups` are the
    /// storage units the engine reports reading (0 when it reports none)
    /// and `bytes` its `read_bytes`. Absent otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub measured: Option<QueryStats>,
}

/// One query's statistics as a query engine's own log records them (ADR
/// 033). Measured, never exact: the model of the Replay holding them names
/// the log (`clickhouse.query_log`, `kusto.query_log`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct QueryStats {
    /// The tag the engine submitted the query under (its query id).
    pub tag: String,
    /// Rows the engine read.
    pub read_rows: u64,
    /// Bytes the engine read.
    pub read_bytes: u64,
    /// Peak memory, in bytes.
    pub peak_memory_bytes: u64,
    /// CPU time, in nanoseconds, when the log records it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_ns: Option<u64>,
    /// Wall-clock duration, in nanoseconds.
    pub duration_ns: u64,
    /// Rows in the result.
    pub result_rows: u64,
    /// Storage units read (parts, extents), when the log records them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub units_read: Option<u64>,
    /// Storage units the queried tables held, when the log records them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub units_total: Option<u64>,
}

/// How well the lake twin predicted a query engine (ADR 033): the rows
/// and row groups the twin says a query over the key sample must read,
/// beside what the engine's log says each query of each pair read, and
/// their ratio, pair by pair. Each pair is one query over the key sample,
/// so each is compared with the prediction on its own; nothing is summed
/// across pairs, which may read different tables. Recorded on the Replay
/// (so a failing certificate keeps it) and copied onto the Receipt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Prediction {
    /// The twin Evidence the prediction comes from.
    pub evidence: Hash,
    /// Rows of the twin holding a sampled key.
    pub predicted_rows: u64,
    /// Row groups of the twin holding a sampled key.
    pub predicted_row_groups: u64,
    /// Each pair's realized reads and ratios, in the proposal's order.
    pub pairs: Vec<PairPrediction>,
}

/// One pair's side of a [`Prediction`]: what the engine's log says its
/// before and after queries read, and each one's ratio to the twin's
/// predicted rows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct PairPrediction {
    /// Rows the engine read for the pair's query before the rewrite.
    pub before_read_rows: u64,
    /// Rows the engine read for the pair's query after the rewrite.
    pub after_read_rows: u64,
    /// Bytes the engine read before the rewrite.
    pub before_read_bytes: u64,
    /// Bytes the engine read after the rewrite.
    pub after_read_bytes: u64,
    /// `before_read_rows / predicted_rows` as a decimal string with three
    /// places, rounded half to even; absent when `predicted_rows` is 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_ratio: Option<String>,
    /// `after_read_rows / predicted_rows`, likewise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_ratio: Option<String>,
}

/// The cost of one operator of a [`ConsumerPlan`] in one run (contract v3;
/// filled by relational replay, WP15).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct OperatorCost {
    /// The node's index in the plan.
    pub node: u32,
    /// The operator, as [`PlanNode`] tags it (`scan`, `semi_join`, ...).
    pub op: String,
    /// Compressed bytes the operator reads or moves.
    pub bytes: Quantity,
    /// Rows it outputs.
    pub rows_out: Quantity,
}

/// A record of replaying a recorded workload against a proposal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Replay {
    /// Record schema version.
    pub record_version: u32,
    /// The proposal scored.
    pub proposal: Hash,
    /// Manifest version replayed against.
    pub manifest_version: String,
    /// Hash of the recorded workload.
    pub workload: Hash,
    /// Identities of the data the replay read: at least the catalog digest
    /// over the discovered fingerprints and footers. Two replays of one
    /// proposal against different data therefore never share a record
    /// (invariant 3), even before any index exists and `manifest_version`
    /// is `"0"`. Non-empty, enforced by the ledger. Absent in version-1
    /// records.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<Hash>,
    /// The full identities ([`KeySpec::identity`]) of the specs that formed
    /// the current configuration: the complete specs a plan may use
    /// (contract v3). Empty when none, and then omitted.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub configuration: Vec<String>,
    /// Cost per run under the current configuration.
    pub current: Vec<RunCost>,
    /// Cost per run under the proposal.
    pub proposed: Vec<RunCost>,
    /// Per-operator costs of a run priced through a [`ConsumerPlan`]
    /// (contract v3; WP15 fills it). Empty otherwise, and then omitted.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub operators: Vec<OperatorCost>,
    /// Name of the cost model behind any cost with `exact == false`.
    /// Present exactly when such a cost exists (a layout proposal, or a
    /// measured rewrite), absent otherwise; enforced by the ledger against
    /// the proposal's kind. Absent in version-1 records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// How the replay priced runs that no spec of the configuration serves:
    /// absent means every live row group (a raw scan); `"parquet_stats"`
    /// means the row groups the files' min/max statistics keep. Absent in
    /// records written before it existed, which keep their bytes and hashes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unserved: Option<String>,
    /// For a rewrite measured in a query engine: the twin's prediction
    /// beside the engine's measurement (ADR 033). Absent otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prediction: Option<Prediction>,
    /// RFC 3339.
    pub created_at: String,
}

/// How much of the key space a certificate covers.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Coverage {
    /// Every key in the dataset was compared.
    Full,
    /// Only keys present in the named recorded workload were compared.
    Workload {
        /// Workload hash.
        workload: Hash,
        /// Keys compared.
        keys: u64,
    },
    /// No data was compared. The rewrite matches an algebraic identity the
    /// engine recognizes (for example a semi-join pushed before a window on
    /// the same partition key), named here. Counts in the body are zero.
    StaticProof {
        /// Identifier of the identity in the rule catalog.
        identity: String,
    },
    /// A random sample of distinct keys was compared.
    Sampled {
        /// Keys compared.
        keys: u64,
        /// Fraction of distinct keys sampled.
        fraction: f64,
    },
}

/// Outcome of a reconciliation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct CertBody {
    /// Keys present on the left only.
    pub missing_right: u64,
    /// Keys present on the right only.
    pub missing_left: u64,
    /// Keys present on both with differing latest-version row hash. From
    /// record version 3, keys that could not be compared are counted in
    /// `uncomparable` instead.
    pub mismatched: u64,
    /// Columns excluded from the row hash, listed so the certificate is
    /// auditable.
    pub ignored_columns: Vec<String>,
    /// Per-group decimal totals that differed, as `(group, left, right)` with
    /// decimals rendered as strings at declared scale.
    pub totals_delta: Vec<(String, String, String)>,
    /// The first per-key differences, in canonical key order, at most
    /// [`MAX_MISMATCH_EXAMPLES`] (contract v3). Omitted when empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub examples: Vec<MismatchExample>,
    /// Keys that could not be compared: with a version column, every row of
    /// the key on some side has a null version, and a null never wins
    /// (ADR 014). A certificate passes only when this is zero (contract
    /// v3). Omitted when zero.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub uncomparable: u64,
    /// Every total column the comparison summed and how it was quantized,
    /// passing or not (contract v3). Omitted when none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub totals: Vec<TotalSpec>,
    /// The rows compared, when only some rows of the covered keys were
    /// (an `asof`, lookback, or window workload): the predicates, applied
    /// in order (contract v3). Absent when every row of the covered keys
    /// was compared.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_filter: Option<Vec<Predicate>>,
    /// The `agg` keys (B7) a certificate relied on, one entry per side: in
    /// a reconcile, the sides whose per-key rows and sums an index's agg
    /// keys answered instead of a scan; in `fix`'s certificate for an agg
    /// key, the proposed key's cells for the workload's keys, checked
    /// against the decoded rows ([`AggServed::checked`]). Omitted when no
    /// agg key was read.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub agg: Vec<AggServed>,
}

/// One side of a certificate that an `agg` key answered (B7): in a
/// reconcile, instead of a scan; in `fix`'s certificate for an agg key, the
/// proposed key whose cells were checked against the rows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct AggServed {
    /// `left` or `right` in a reconcile; `proposed` for the cells `fix`
    /// checked.
    pub side: String,
    /// The identities ([`KeySpec::identity`]) of the agg keys read, one per
    /// total column.
    pub specs: Vec<String>,
    /// The rows those keys aggregate: every live row of the side in a
    /// reconcile (the count a scan would have read); the rows of the
    /// checked cells in `fix`.
    pub rows: u64,
    /// For `fix`'s certificate: the check of the proposed key's cells
    /// against the decoded rows. A certificate passes only when no cell
    /// differs. Absent in a reconcile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checked: Option<AggCellCheck>,
}

/// The check of an agg key's cells (B7) against the decoded rows: for every
/// key of a workload (projected onto the agg key's columns) and every live
/// row group, the stored cell (rows, nulls, sum, minimum, maximum) equals
/// the one the rows make, and a row group holding none of the key's rows
/// has no cell.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct AggCellCheck {
    /// Keys checked.
    pub keys: u64,
    /// Cells compared: (key, row group) pairs holding a row or a cell.
    pub cells: u64,
    /// Row groups read to compare them.
    pub row_groups_read: u64,
    /// Cells that differ from the rows.
    pub differ: u64,
    /// The first differing cells, in key then row-group order, at most
    /// [`MAX_MISMATCH_EXAMPLES`]. Omitted when none differ.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub examples: Vec<AggCellDiff>,
}

/// One agg cell that differs from the rows it aggregates (B7).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct AggCellDiff {
    /// Lowercase hex of the canonical key tuple (ADR 016).
    pub key: String,
    /// Each key element as Arrow displays it at the dataset type; `None`
    /// for a null element.
    pub values: Vec<Option<String>>,
    /// The global row-group id (ADR 001).
    pub row_group: u32,
    /// The stored cell, rendered (`rows 3, nulls 1, sum 10.50, min 1.00,
    /// max 5.00`), or `none` when the index has no cell there.
    pub stored: String,
    /// The cell the decoded rows make, rendered likewise, or `none` when
    /// the row group holds no row of the key.
    pub rows: String,
}

#[allow(clippy::trivially_copy_pass_by_ref)] // serde's skip_serializing_if signature
fn is_zero(n: &u64) -> bool {
    *n == 0
}

/// The most per-key examples a certificate carries (the verifier's
/// `max_examples`).
pub const MAX_MISMATCH_EXAMPLES: usize = 100;

/// One key that differed, as a certificate records it (contract v3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct MismatchExample {
    /// How the key differed.
    pub kind: MismatchKind,
    /// Lowercase hex of the canonical key tuple (ADR 016): the identity.
    pub key: String,
    /// Each key element as Arrow displays it at the dataset type; `None`
    /// for a null element.
    pub values: Vec<Option<String>>,
}

/// How one key differed (contract v3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum MismatchKind {
    /// Rows on the right only.
    MissingLeft,
    /// Rows on the left only.
    MissingRight,
    /// On both sides, with differing compared versions.
    Mismatched,
    /// No comparable version on some side.
    Uncomparable,
}

/// A total column of a certificate and its quantization (contract v3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct TotalSpec {
    /// The column.
    pub column: String,
    /// For a float column, the decimal scale each value was rounded to
    /// (half to even) before an exact sum; `None` for a decimal or integer
    /// column, summed as stored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quantize: Option<u8>,
}

/// A record that a reconciliation ran and what it found (ADR 004).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Certificate {
    /// Record schema version.
    pub record_version: u32,
    /// The proposal verified, if any.
    pub proposal: Option<Hash>,
    /// Coverage.
    pub coverage: Coverage,
    /// Whether the comparison found zero differences.
    pub pass: bool,
    /// Detail.
    pub body: CertBody,
    /// Provenance.
    pub provenance: Provenance,
}

/// The only record a human or a gate reads. Links the whole chain. The ledger
/// refuses to store one whose certificate does not resolve or did not pass
/// (invariant 2).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Receipt {
    /// Record schema version.
    pub record_version: u32,
    /// Evidence cited.
    pub evidence: Vec<Hash>,
    /// Proposal applied.
    pub proposal: Hash,
    /// Replay that scored it.
    pub replay: Hash,
    /// Certificate that verified it.
    pub certificate: Hash,
    /// Manifest version before apply.
    pub manifest_before: String,
    /// Manifest version after apply.
    pub manifest_after: String,
    /// RFC 3339.
    pub created_at: String,
    /// `true` for an attest-only receipt (ADR 033): the engine scored and
    /// verified the proposal and applied nothing (`manifest_after ==
    /// manifest_before`); a human or a CI gate deploys it. The ledger
    /// accepts one for a rewrite only. Absent (`false`) for a receipt of an
    /// apply.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub attest_only: bool,
    /// For a rewrite measured in a query engine: the Replay's prediction,
    /// copied so the receipt reads alone (the ledger checks they agree).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prediction: Option<Prediction>,
}

/// Any ledger record, for storage and listing.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "record", rename_all = "snake_case")]
pub enum Record {
    /// An observation.
    Evidence(Evidence),
    /// A proposal.
    Proposal(Proposal),
    /// A replay.
    Replay(Replay),
    /// A certificate.
    Certificate(Certificate),
    /// A receipt.
    Receipt(Receipt),
}

impl Record {
    /// Content hash of the record's canonical JSON.
    pub fn hash(&self) -> Result<Hash> {
        Hash::of_record(self)
    }

    /// The record schema version the record declares.
    #[must_use]
    pub fn record_version(&self) -> u32 {
        match self {
            Self::Evidence(r) => r.record_version,
            Self::Proposal(r) => r.record_version,
            Self::Replay(r) => r.record_version,
            Self::Certificate(r) => r.record_version,
            Self::Receipt(r) => r.record_version,
        }
    }
}

// ---------------------------------------------------------------------------
// --- phase 5: sketch (approximate; never read by a planner) ---
// ---------------------------------------------------------------------------

/// `HyperLogLog` precision of a [`SketchSpec`] that names none: `2^14`
/// registers, a relative standard error of about 0.81 %.
pub const SKETCH_DEFAULT_PRECISION: u8 = 14;

/// Confidence of every approximate interval a sketch reports: three standard
/// errors of a normal approximation.
pub const SKETCH_CONFIDENCE: f64 = 0.997;

/// A `sketch` sidecar: per item, per file, `HyperLogLog` registers and an
/// optional `MinHash` signature (`docs/format.md`, "Sketch segments").
///
/// **Approximate, and never planned with.** A sketch answers
/// `count(distinct item)` and "how much do two columns overlap" with an
/// error bound; it is not a [`KeySpec`], it cannot prune a row group, and no
/// planner path reads it (an approximate number in a plan would break
/// invariant 1). The manifest declares it in `schema.sidecars` with kind
/// `sketch`, beside, never among, the key specs.
///
/// Short form `sketch:a,b,a+b[,p=12][,minhash=64]`: each comma-separated item
/// is one column or a `+`-joined tuple of columns (a tuple is encoded like a
/// composite key, ADR 014: a row whose leading column is null is not
/// sketched, a null trailing column is the null marker).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct SketchSpec {
    /// Sketched items in declaration order: one column, or a tuple of columns.
    pub items: Vec<Vec<String>>,
    /// `HyperLogLog` precision `p` (`2^p` registers), 4..=18.
    pub precision: u8,
    /// `MinHash` signature length `k`, when the sketch keeps signatures.
    pub minhash: Option<u16>,
}

impl SketchSpec {
    /// The short label of one item: `a`, or `a+b` for a tuple.
    #[must_use]
    pub fn item_label(item: &[String]) -> String {
        item.join("+")
    }

    /// The name its segments and completeness are recorded under:
    /// `sketch:a,b,a+b`. Parameters are not part of the name; a spec with
    /// other parameters under the same name replaces the old one.
    #[must_use]
    pub fn name(&self) -> String {
        let items: Vec<String> = self.items.iter().map(|i| Self::item_label(i)).collect();
        format!("sketch:{}", items.join(","))
    }

    /// The short form with its parameters: the name, then `,p=<p>` when the
    /// precision is not [`SKETCH_DEFAULT_PRECISION`], then `,minhash=<k>`.
    #[must_use]
    pub fn label(&self) -> String {
        let mut s = self.name();
        if self.precision != SKETCH_DEFAULT_PRECISION {
            s = format!("{s},p={}", self.precision);
        }
        if let Some(k) = self.minhash {
            s = format!("{s},minhash={k}");
        }
        s
    }
}

/// Where an exact distinct count beside a sketch came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ExactDistinctSource {
    /// The distinct keys of a complete `exact` key spec on exactly the item's
    /// columns, counted over live row groups.
    ExactSegment,
}

/// `cardinality(item)`: an **approximate** distinct count from a sketch,
/// with its error bound, beside the exact row counts the sketch stores.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ApproxCardinality {
    /// Always `true`: `distinct` and every number derived from it is a
    /// `HyperLogLog` estimate, never an exact count, unless `exact_distinct`
    /// says otherwise.
    pub approximate: bool,
    /// The item, `a` or `a+b`.
    pub item: String,
    /// The item's columns.
    pub columns: Vec<String>,
    /// Estimated distinct values over every live file, rounded.
    pub distinct: u64,
    /// Lower end of the interval holding the true count with probability
    /// `confidence`.
    pub distinct_low: u64,
    /// Upper end of that interval (never above `rows`).
    pub distinct_high: u64,
    /// Relative standard error of `distinct` at the sketch's precision.
    pub relative_standard_error: f64,
    /// Confidence of `[distinct_low, distinct_high]`: [`SKETCH_CONFIDENCE`].
    pub confidence: f64,
    /// The exact distinct count, when a complete exact key spec on the
    /// same columns holds it.
    pub exact_distinct: Option<u64>,
    /// Where `exact_distinct` came from.
    pub exact_source: Option<ExactDistinctSource>,
    /// Rows whose item value was sketched (leading column non-null). Exact.
    pub rows: u64,
    /// Rows not sketched because the leading column is null. Exact.
    pub nulls: u64,
    /// Fanout: `rows / distinct` (or `rows / exact_distinct` when it is
    /// known), the rows one value covers on average.
    pub rows_per_value: f64,
    /// Whether `rows_per_value` used the exact distinct count.
    pub rows_per_value_exact: bool,
    /// Live files the sketch covers.
    pub files: u64,
    /// Estimated files one value appears in, on average: the sum of the
    /// per-file estimates over `distinct`. Approximate.
    pub files_per_value: f64,
    /// Files whose own estimate is below their row count by more than its
    /// error bound: files that almost certainly repeat a value of the item
    /// (for a tuple such as `symbol+ts`, files where it is not unique).
    /// Files repeating fewer rows than `duplicate_floor` are not counted, so
    /// zero never proves the item unique within every file.
    pub files_with_duplicates: u64,
    /// The detection floor of `files_with_duplicates`: the repeated rows a
    /// file of the average sketched size (`rows / files`) must hold before
    /// its estimate falls outside its bound, `ceil(r · 3σ / (1 + 3σ))` with
    /// `r` that size and `σ` the relative standard error. Approximate.
    #[serde(default)]
    pub duplicate_floor: u64,
}

/// How an overlap was estimated.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum OverlapMethod {
    /// `MinHash` signatures of equal length on both sides: the share of
    /// signature slots whose minimums agree.
    Minhash,
    /// `HyperLogLog` inclusion-exclusion: `|A| + |B| - |A ∪ B|` over the
    /// union of both sketches. Wider bounds than `MinHash` for small overlaps.
    Hll,
}

/// `overlap(a.item, b.item)`: the **approximate** Jaccard similarity of two
/// sketched items' value sets, with its error bound.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ApproxOverlap {
    /// Always `true`: every number here is an estimate.
    pub approximate: bool,
    /// The left item.
    pub left: String,
    /// The right item.
    pub right: String,
    /// How it was estimated.
    pub method: OverlapMethod,
    /// `MinHash` signature length, when `method` is `minhash`.
    pub signatures: Option<u16>,
    /// Estimated Jaccard similarity `|A ∩ B| / |A ∪ B|`, in `[0, 1]`.
    pub jaccard: f64,
    /// Lower end of the interval holding the true similarity with
    /// probability `confidence`.
    pub jaccard_low: f64,
    /// Upper end of that interval.
    pub jaccard_high: f64,
    /// Confidence of the interval: [`SKETCH_CONFIDENCE`].
    pub confidence: f64,
    /// Estimated distinct values on the left.
    pub left_distinct: u64,
    /// Estimated distinct values on the right.
    pub right_distinct: u64,
    /// Estimated distinct values in the union.
    pub union_distinct: u64,
    /// Estimated distinct values in both: `jaccard × union_distinct`.
    pub intersection: u64,
}

// --- phase 5: one `why` column per index kind ---

/// How a number was obtained (contract v3, ADR 026): computed exactly from
/// the index and recorded statistics, bounded by an interval the statistics
/// prove, or measured by a named model. Never estimated (ADR 005).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Exactness {
    /// A sum of recorded statistics and index contents.
    #[default]
    Exact,
    /// The true value lies in `[low, high]`, both proven.
    Bounded {
        /// Proven lower bound.
        low: u64,
        /// Proven upper bound.
        high: u64,
    },
    /// Measured or predicted by the named model (ADR 027).
    Measured {
        /// The model, for example `layout.v1` or a database's measured cost.
        model: String,
    },
}

impl Exactness {
    /// Whether this is [`Exactness::Exact`].
    #[must_use]
    pub fn is_exact(&self) -> bool {
        matches!(self, Self::Exact)
    }
}

/// One index kind's column in a `why` report row: the one number the kind
/// contributes for a candidate (for example, for an `expr` key, the rows
/// whose expression is null and the distinct derived values), rendered as
/// one trailing funnel column headed `kind`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct KindColumn {
    /// The index kind, the column's header (`expr`, `list`, `text`, ...).
    pub kind: String,
    /// What the value is, in a few words.
    pub label: String,
    /// The value, rendered.
    pub value: String,
    /// How the value was obtained.
    pub exactness: Exactness,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provenance() -> Provenance {
        Provenance {
            inputs: vec![Hash::of(b"a")],
            oldest_input_age: Duration::from_secs(90),
            manifest_version: Some("v1".into()),
            created_at: "2026-10-24T00:00:00Z".into(),
        }
    }

    #[test]
    fn not_retained_states_whether_the_sequence_was_ever_committed() {
        let msg = |requested| {
            Error::NotRetained {
                index: "/d/.bellwether".into(),
                requested,
                retained: vec![3, 4, 5],
            }
            .to_string()
        };
        assert!(msg(9).contains("has not been committed"), "{}", msg(9));
        assert!(msg(0).contains("has not been committed"), "{}", msg(0));
        assert!(msg(2).contains("no longer retained"), "{}", msg(2));
        for m in [msg(9), msg(0), msg(2)] {
            assert!(
                m.contains("oldest retained is 3") && !m.contains("--"),
                "{m}"
            );
        }
        let unindexed = Error::NotRetained {
            index: "/d/.bellwether".into(),
            requested: 1,
            retained: Vec::new(),
        }
        .to_string();
        assert!(
            unindexed.contains("there is no index at /d/.bellwether"),
            "{unindexed}"
        );
    }

    #[test]
    fn hash_hex_round_trips() {
        let h = Hash::of(b"bellwether");
        let s = h.to_hex();
        assert_eq!(s.len(), 64);
        assert_eq!(Hash::from_hex(&s).unwrap(), h);
    }

    #[test]
    fn record_hash_is_deterministic_and_content_sensitive() {
        let a = Record::Proposal(Proposal {
            record_version: format::RECORD_VERSION,
            evidence: vec![],
            proposed_by: "test".into(),
            created_at: "2026-10-24T00:00:00Z".into(),
            body: ProposalBody::Key {
                spec: KeySpec::Exact {
                    columns: vec!["id".into()],
                    counts: false,
                    bounds: None,
                },
                sidecars: Vec::new(),
            },
        });
        let b = a.clone();
        assert_eq!(a.hash().unwrap(), b.hash().unwrap());
        let mut c = a.clone();
        if let Record::Proposal(p) = &mut c {
            p.proposed_by = "other".into();
        }
        assert_ne!(a.hash().unwrap(), c.hash().unwrap());
    }

    #[test]
    fn records_round_trip_through_json_with_versions() {
        let records = vec![
            Record::Evidence(Evidence {
                record_version: format::RECORD_VERSION,
                provenance: provenance(),
                body: EvidenceBody::Profile {
                    twin: None,
                    root: "./orders".into(),
                    fanout: vec![Fanout {
                        columns: vec!["batch_id".into()],
                        distinct: 500,
                        sampled: false,
                        files_per_value: [9.0, 60.0, 900.0],
                        row_groups_per_value: [40.0, 500.0, 400.0],
                        files_total: 1_000,
                        row_groups_total: 8_000,
                        sample_size: None,
                        quantile_error: None,
                    }],
                    lints: vec![Lint {
                        rule: "batch_stamp_key".into(),
                        columns: vec!["batch_id".into()],
                        message: "looks like an id, is a timestamp".into(),
                        confidence: 0.9,
                        site: None,
                        fix: None,
                    }],
                    workload_keys: Some([10, 1_000, 10_000]),
                },
            }),
            Record::Certificate(Certificate {
                record_version: format::RECORD_VERSION,
                proposal: None,
                coverage: Coverage::Workload {
                    workload: Hash::of(b"w"),
                    keys: 500,
                },
                pass: true,
                body: CertBody {
                    missing_right: 0,
                    missing_left: 0,
                    mismatched: 0,
                    ignored_columns: vec!["updated_at".into()],
                    totals_delta: vec![],
                    examples: Vec::new(),
                    uncomparable: 0,
                    totals: Vec::new(),
                    row_filter: None,
                    agg: Vec::new(),
                },
                provenance: provenance(),
            }),
            Record::Receipt(Receipt {
                attest_only: false,
                prediction: None,
                record_version: format::RECORD_VERSION,
                evidence: vec![Hash::of(b"e")],
                proposal: Hash::of(b"p"),
                replay: Hash::of(b"r"),
                certificate: Hash::of(b"c"),
                manifest_before: "v1".into(),
                manifest_after: "v2".into(),
                created_at: "2026-10-24T00:00:00Z".into(),
            }),
        ];
        for r in records {
            let json = serde_json::to_string(&r).unwrap();
            assert!(json.contains(&format!("\"record_version\":{}", format::RECORD_VERSION)));
            let back: Record = serde_json::from_str(&json).unwrap();
            assert_eq!(back, r);
        }
    }

    #[test]
    fn scan_plan_round_trips_bitmaps() {
        let mut rgs = RoaringBitmap::new();
        rgs.insert(0);
        rgs.insert(7);
        let plan = ScanPlan {
            manifest_version: "v3".into(),
            entries: vec![ScanEntry {
                file: FileFingerprint {
                    path: "a.parquet".into(),
                    size: 10,
                    etag: None,
                    tail_blake3: Hash::of(b"t"),
                },
                row_groups: rgs,
                bytes: 10,
                columns: None,
            }],
            residual: Some(Predicate::And {
                all: vec![
                    Predicate::LatestPerKey {
                        columns: vec!["id".into()],
                        ts_column: "ts".into(),
                    },
                    Predicate::In {
                        columns: vec!["id".into()],
                        values: vec![vec![0x06, 0x61, 0, 0]],
                    },
                ],
            }),
            bytes: 10,
            row_groups_total: 8,
            row_groups_selected: 2,
            projected_bytes: None,
            order: None,
            rows: None,
        };
        let json = serde_json::to_string(&plan).unwrap();
        assert!(json.contains("\"row_groups\":[0,7]"), "{json}");
        assert!(json.contains("\"op\":\"and\""), "{json}");
        let back: ScanPlan = serde_json::from_str(&json).unwrap();
        assert_eq!(back, plan);
    }

    #[test]
    fn durations_round_trip_exactly_and_reject_bad_input() {
        for bad in ["-1", "1.5", "NaN", "\"x\"", "18446744073709551616"] {
            let json = format!(
                r#"{{"inputs":[],"oldest_input_age":{bad},"manifest_version":null,"created_at":"2026-10-24T00:00:00Z"}}"#
            );
            assert!(serde_json::from_str::<Provenance>(&json).is_err(), "{bad}");
        }
        // 3.9 years plus 987654321 ns: exact under integer nanoseconds.
        let p = Provenance {
            inputs: vec![Hash::of(b"a")],
            oldest_input_age: Duration::new(123_456_789, 987_654_321),
            manifest_version: None,
            created_at: "2026-10-24T00:00:00Z".into(),
        };
        let json = serde_json::to_string(&p).unwrap();
        assert!(
            json.contains("\"oldest_input_age\":123456789987654321"),
            "{json}"
        );
        let back: Provenance = serde_json::from_str(&json).unwrap();
        assert_eq!(back, p);
    }

    #[test]
    fn run_cost_and_static_proof_round_trip() {
        let rc = RunCost {
            measured: None,
            run_id: "r1".into(),
            keys: 10,
            files: 2,
            row_groups: 3,
            bytes: 4096,
            exact: true,
            exactness: Exactness::Exact,
            rows: None,
        };
        let back: RunCost = serde_json::from_str(&serde_json::to_string(&rc).unwrap()).unwrap();
        assert_eq!(back, rc);
        let c = Coverage::StaticProof {
            identity: "semijoin_before_window".into(),
        };
        let back: Coverage = serde_json::from_str(&serde_json::to_string(&c).unwrap()).unwrap();
        assert_eq!(back, c);
    }

    #[test]
    fn key_spec_names_are_stable() {
        let k = KeySpec::Temporal {
            columns: vec!["id".into(), "version".into()],
            ts_column: "ts".into(),
            counts: false,
        };
        assert_eq!(k.name(), "temporal:id,version");
        assert_eq!(
            KeySpec::Range {
                column: "ts".into()
            }
            .columns(),
            ["ts".to_string()]
        );
    }
}

/// The `schema` feature is non-semantic: these tests run with it off and on
/// and pin the same bytes (WP12, contract v2.1).
#[cfg(test)]
mod schema_feature {
    use super::*;

    /// One record of every kind, touching every field with custom serde
    /// (`Hash`, the row-group bitmap, the nanosecond age) and every tagged
    /// enum.
    #[allow(clippy::too_many_lines)]
    fn every_kind() -> Vec<Record> {
        let provenance = Provenance {
            inputs: vec![Hash::of(b"input")],
            oldest_input_age: Duration::from_nanos(1_234_567_890_123),
            manifest_version: Some("3".into()),
            created_at: "2026-10-24T00:00:00.000Z".into(),
        };
        let mut rgs = RoaringBitmap::new();
        rgs.insert(1);
        rgs.insert(70_000);
        vec![
            Record::Evidence(Evidence {
                record_version: format::RECORD_VERSION,
                provenance: provenance.clone(),
                body: EvidenceBody::Plan {
                    plan: ScanPlan {
                        manifest_version: "3".into(),
                        entries: vec![ScanEntry {
                            file: FileFingerprint {
                                path: "a.parquet".into(),
                                size: 10,
                                etag: Some("e".into()),
                                tail_blake3: Hash::of(b"t"),
                            },
                            row_groups: rgs,
                            bytes: 99,
                            columns: None,
                        }],
                        residual: Some(Predicate::In {
                            columns: vec!["id".into()],
                            values: vec![vec![0, 1, 2]],
                        }),
                        bytes: 99,
                        row_groups_total: 4,
                        row_groups_selected: 2,
                        projected_bytes: None,
                        order: None,
                        rows: None,
                    },
                },
            }),
            Record::Proposal(Proposal {
                record_version: format::RECORD_VERSION,
                evidence: vec![Hash::of(b"e")],
                proposed_by: "agent".into(),
                created_at: "2026-10-24T00:00:00.000Z".into(),
                body: ProposalBody::Rewrite {
                    shape: RewriteShape {
                        pairs: Vec::new(),
                        key: Vec::new(),
                        keys: None,
                        before: "b".into(),
                        after: "a".into(),
                        invariant: "i".into(),
                        before_plan: None,
                        after_plan: None,
                    },
                },
            }),
            Record::Replay(Replay {
                prediction: None,
                record_version: format::RECORD_VERSION,
                proposal: Hash::of(b"p"),
                manifest_version: "3".into(),
                workload: Hash::of(b"w"),
                inputs: vec![Hash::of(b"c")],
                current: vec![RunCost {
                    measured: None,
                    run_id: "r".into(),
                    keys: 2,
                    files: 3,
                    row_groups: 4,
                    bytes: 5,
                    exact: true,
                    exactness: Exactness::Exact,
                    rows: None,
                }],
                proposed: vec![],
                model: None,
                unserved: None,
                created_at: "2026-10-24T00:00:00.000Z".into(),
                configuration: Vec::new(),
                operators: Vec::new(),
            }),
            Record::Certificate(Certificate {
                record_version: format::RECORD_VERSION,
                proposal: Some(Hash::of(b"p")),
                coverage: Coverage::Sampled {
                    keys: 7,
                    fraction: 0.25,
                },
                pass: false,
                body: CertBody {
                    missing_right: 1,
                    missing_left: 0,
                    mismatched: 2,
                    ignored_columns: vec![],
                    totals_delta: vec![("g".into(), "1.00".into(), "2.00".into())],
                    examples: Vec::new(),
                    uncomparable: 0,
                    totals: Vec::new(),
                    row_filter: None,
                    agg: Vec::new(),
                },
                provenance,
            }),
            Record::Receipt(Receipt {
                attest_only: false,
                prediction: None,
                record_version: format::RECORD_VERSION,
                evidence: vec![Hash::of(b"e")],
                proposal: Hash::of(b"p"),
                replay: Hash::of(b"r"),
                certificate: Hash::of(b"c"),
                manifest_before: "3".into(),
                manifest_after: "4".into(),
                created_at: "2026-10-24T00:00:00.000Z".into(),
            }),
        ]
    }

    /// The canonical bytes of every record kind at this record version,
    /// pinned (the drift test). A field added, renamed or newly written, or
    /// a derive that changed serialization, changes this hash; the fix is a
    /// `RECORD_VERSION` bump with a fixture, never an edit of the golden
    /// alone.
    #[test]
    fn record_bytes_are_the_same_with_and_without_the_schema_feature() {
        let mut all = Vec::new();
        for r in every_kind() {
            all.extend(serde_json::to_vec(&r).unwrap());
            all.push(b'\n');
        }
        assert_eq!(
            (format::RECORD_VERSION, Hash::of(&all).to_hex().as_str()),
            GOLDEN,
            "record bytes moved: bump RECORD_VERSION, add fixtures/format-<next>/ and read it in \
             crates/bellwether-cli/tests/format_compat.rs, then pin the new golden"
        );
    }

    /// The record version and the hash of `every_kind()`'s bytes at it.
    const GOLDEN: (u32, &str) = (
        3,
        "b59003f39b3c09092346c665bbe85b3b3ac44b1a376f0d4eb6c907d249a84449",
    );

    /// Every field and variant record version 3 added, each set to a value
    /// that is not its default, so it is serialized: the second golden.
    #[allow(clippy::too_many_lines)]
    fn every_v3_field() -> Vec<Record> {
        let fp = FileFingerprint {
            path: "a.parquet".into(),
            size: 10,
            etag: None,
            tail_blake3: Hash::of(b"t"),
        };
        let mut rgs = RoaringBitmap::new();
        rgs.insert(4);
        let cols = |c: &[&str]| c.iter().map(|s| (*s).to_string()).collect::<Vec<_>>();
        let plan = ConsumerPlan {
            version: 1,
            nodes: vec![
                PlanNode::Scan {
                    dataset: "trades".into(),
                    columns: cols(&["symbol", "ts", "px"]),
                },
                PlanNode::Filter {
                    input: 0,
                    predicate: Predicate::WindowPartition {
                        columns: cols(&["symbol"]),
                    },
                },
                PlanNode::Latest {
                    input: 1,
                    columns: cols(&["symbol"]),
                    ts: "ts".into(),
                },
                PlanNode::AsOf {
                    input: 1,
                    columns: cols(&["symbol"]),
                    ts: "ts".into(),
                },
                PlanNode::SemiJoin {
                    input: 0,
                    columns: cols(&["symbol"]),
                    keys: KeysFrom::Run,
                },
                PlanNode::Join {
                    left: 4,
                    right: 0,
                    on: vec![("symbol".into(), "symbol".into())],
                    kind: JoinKind::Inner,
                },
                PlanNode::Window {
                    input: 5,
                    partition_by: cols(&["symbol"]),
                    keys: Some(KeysFrom::Node {
                        node: 4,
                        columns: cols(&["symbol"]),
                    }),
                },
                PlanNode::Aggregate {
                    input: 6,
                    group_by: cols(&["symbol"]),
                    measures: vec![
                        Measure {
                            function: AggregateFn::Count,
                            column: None,
                        },
                        Measure {
                            function: AggregateFn::Sum,
                            column: Some("px".into()),
                        },
                        Measure {
                            function: AggregateFn::Min,
                            column: Some("px".into()),
                        },
                        Measure {
                            function: AggregateFn::Max,
                            column: Some("px".into()),
                        },
                    ],
                },
                PlanNode::Exchange {
                    input: 7,
                    by: cols(&["symbol"]),
                },
                PlanNode::Project {
                    input: 8,
                    columns: cols(&["symbol"]),
                },
            ],
            root: 9,
        };
        let shape = RewriteShape {
            before: "b".into(),
            after: "a".into(),
            invariant: "i".into(),
            pairs: Vec::new(),
            key: Vec::new(),
            keys: None,
            before_plan: Some(plan.clone()),
            after_plan: Some(ConsumerPlan {
                version: 1,
                nodes: plan.nodes[..1].to_vec(),
                root: 0,
            }),
        };
        let provenance = Provenance {
            inputs: vec![Hash::of(b"input")],
            oldest_input_age: Duration::from_nanos(7),
            manifest_version: Some("3".into()),
            created_at: "2026-10-25T00:00:00.000Z".into(),
        };
        let measured = Exactness::Measured {
            model: "measured: logical bytes".into(),
        };
        let cost = RunCost {
            run_id: "r".into(),
            keys: 2,
            files: 1,
            row_groups: 1,
            bytes: 5,
            exact: false,
            exactness: measured.clone(),
            rows: Some(Quantity {
                value: 12,
                exactness: Exactness::Bounded { low: 9, high: 12 },
            }),
            measured: None,
        };
        vec![
            Record::Evidence(Evidence {
                record_version: format::RECORD_VERSION,
                provenance: provenance.clone(),
                body: EvidenceBody::Plan {
                    plan: ScanPlan {
                        manifest_version: "3".into(),
                        entries: vec![ScanEntry {
                            file: fp,
                            row_groups: rgs,
                            bytes: 99,
                            columns: Some(vec![ColumnBytes {
                                column: "px".into(),
                                bytes: 40,
                            }]),
                        }],
                        residual: Some(Predicate::And {
                            all: vec![
                                Predicate::SemiJoin {
                                    columns: cols(&["symbol"]),
                                    keys_from: KeysFrom::Values {
                                        values: vec![vec![6, 65]],
                                    },
                                },
                                Predicate::Join {
                                    left: cols(&["symbol"]),
                                    right: cols(&["sym"]),
                                    kind: JoinKind::Left,
                                },
                                Predicate::WindowPartition {
                                    columns: cols(&["symbol"]),
                                },
                            ],
                        }),
                        bytes: 99,
                        row_groups_total: 8,
                        row_groups_selected: 1,
                        projected_bytes: Some(40),
                        order: Some(vec![(0, 4)]),
                        rows: Some(Quantity::exact(3)),
                    },
                },
            }),
            Record::Evidence(Evidence {
                record_version: format::RECORD_VERSION,
                provenance: provenance.clone(),
                body: EvidenceBody::Profile {
                    root: "/data".into(),
                    fanout: vec![Fanout {
                        columns: cols(&["symbol"]),
                        distinct: 50,
                        sampled: true,
                        files_per_value: [1.0, 2.0, 3.0],
                        row_groups_per_value: [1.0, 2.5, 4.0],
                        files_total: 6,
                        row_groups_total: 24,
                        sample_size: Some(40),
                        quantile_error: Some(0.05),
                    }],
                    lints: vec![Lint {
                        rule: "r".into(),
                        columns: cols(&["symbol"]),
                        message: "m.".into(),
                        confidence: 0.5,
                        site: None,
                        fix: Some(shape.clone()),
                    }],
                    workload_keys: None,
                    twin: None,
                },
            }),
            Record::Proposal(Proposal {
                record_version: format::RECORD_VERSION,
                evidence: vec![Hash::of(b"e")],
                proposed_by: "agent".into(),
                created_at: "2026-10-25T00:00:00.000Z".into(),
                body: ProposalBody::Key {
                    spec: KeySpec::Exact {
                        columns: cols(&["symbol"]),
                        counts: true,
                        bounds: Some("ts".into()),
                    },
                    sidecars: vec![
                        KeySpec::Bitemporal {
                            columns: cols(&["symbol"]),
                            valid_column: "valid".into(),
                            known_column: "known".into(),
                        },
                        KeySpec::Bloom {
                            columns: cols(&["venue"]),
                            fpr: 0.01,
                            bounds: Some("ts".into()),
                        },
                        KeySpec::Temporal {
                            columns: cols(&["id"]),
                            ts_column: "ts".into(),
                            counts: true,
                        },
                    ],
                },
            }),
            Record::Proposal(Proposal {
                record_version: format::RECORD_VERSION,
                evidence: vec![Hash::of(b"e")],
                proposed_by: "agent".into(),
                created_at: "2026-10-25T00:00:00.000Z".into(),
                body: ProposalBody::Rewrite { shape },
            }),
            Record::Replay(Replay {
                record_version: format::RECORD_VERSION,
                proposal: Hash::of(b"p"),
                manifest_version: "3".into(),
                workload: Hash::of(b"w"),
                inputs: vec![Hash::of(b"c")],
                configuration: vec!["exact:symbol,counts".into()],
                current: vec![cost.clone()],
                proposed: vec![cost],
                operators: vec![OperatorCost {
                    node: 4,
                    op: "semi_join".into(),
                    bytes: Quantity {
                        value: 5,
                        exactness: measured,
                    },
                    rows_out: Quantity::exact(9),
                }],
                model: Some("measured: logical bytes".into()),
                prediction: None,
                unserved: Some("parquet_stats".into()),
                created_at: "2026-10-25T00:00:00.000Z".into(),
            }),
            Record::Certificate(Certificate {
                record_version: format::RECORD_VERSION,
                proposal: Some(Hash::of(b"p")),
                coverage: Coverage::Full,
                pass: false,
                body: CertBody {
                    missing_right: 1,
                    missing_left: 1,
                    mismatched: 1,
                    ignored_columns: vec![],
                    totals_delta: vec![],
                    examples: vec![
                        MismatchExample {
                            kind: MismatchKind::MissingLeft,
                            key: "AAPL".into(),
                            values: vec![None, Some("1".into())],
                        },
                        MismatchExample {
                            kind: MismatchKind::MissingRight,
                            key: "MSFT".into(),
                            values: vec![Some("1".into()), None],
                        },
                        MismatchExample {
                            kind: MismatchKind::Mismatched,
                            key: "IBM".into(),
                            values: vec![Some("1".into()), Some("2".into())],
                        },
                        MismatchExample {
                            kind: MismatchKind::Uncomparable,
                            key: "ORCL".into(),
                            values: vec![None, None],
                        },
                    ],
                    uncomparable: 1,
                    totals: vec![
                        TotalSpec {
                            column: "px".into(),
                            quantize: Some(2),
                        },
                        TotalSpec {
                            column: "qty".into(),
                            quantize: None,
                        },
                    ],
                    row_filter: Some(vec![Predicate::SemiJoin {
                        columns: cols(&["symbol"]),
                        keys_from: KeysFrom::Run,
                    }]),
                    agg: Vec::new(),
                },
                provenance,
            }),
        ]
    }

    /// Every record version 3 field, serialized: each name appears in the
    /// bytes, and the bytes are pinned with the record version (a field
    /// renamed, dropped, or retyped moves the hash; the fix is a
    /// `RECORD_VERSION` bump with a fixture).
    #[test]
    fn every_record_version_3_field_is_pinned() {
        let mut all = Vec::new();
        for r in every_v3_field() {
            let line = serde_json::to_vec(&r).unwrap();
            let back: Record = serde_json::from_slice(&line).unwrap();
            assert_eq!(back, r, "round trip");
            all.extend(line);
            all.push(b'\n');
        }
        let text = String::from_utf8(all.clone()).unwrap();
        for field in [
            "\"configuration\"",
            "\"operators\"",
            "\"unserved\"",
            "\"examples\"",
            "\"uncomparable\"",
            "\"totals\"",
            "\"row_filter\"",
            "\"sidecars\"",
            "\"before_plan\"",
            "\"after_plan\"",
            "\"order\"",
            "\"sample_size\"",
            "\"quantile_error\"",
            "\"fix\"",
            "\"semi_join\"",
            "\"join\"",
            "\"window_partition\"",
            "\"keys_from\"",
            "\"bounded\"",
            "\"measured\"",
            "\"bitemporal\"",
            "\"bounds\"",
            "\"counts\"",
            "\"rows\"",
            "\"projected_bytes\"",
            "\"exactness\"",
            "\"rows_out\"",
        ] {
            assert!(text.contains(field), "{field} is not serialized");
        }
        assert_eq!(
            (format::RECORD_VERSION, Hash::of(&all).to_hex().as_str()),
            GOLDEN_V3,
            "record version 3 bytes moved: bump RECORD_VERSION, add fixtures/format-<next>/ \
             and read it in crates/bellwether-cli/tests/format_compat.rs, then pin the new \
             golden\n{text}"
        );
    }

    /// The record version and the hash of `every_v3_field()`'s bytes at it.
    const GOLDEN_V3: (u32, &str) = (
        3,
        "3a0b4e5617d7f91bbb26c399b27351355681eb6f56118212cb30a5e6b6376c14",
    );

    /// The sidecar proposals of record version 3 (`docs/format.md`, "Sidecar
    /// proposal vectors"): a key proposal whose spec carries per-key bounds,
    /// one of a bitemporal spec, and one of a key with specs applied beside
    /// it. Each line of the document is the record's bytes, and reads back
    /// to the record.
    #[test]
    fn sidecar_proposal_vectors_match_docs_format_md() {
        let proposal = |spec: KeySpec, sidecars: Vec<KeySpec>| {
            Record::Proposal(Proposal {
                record_version: format::RECORD_VERSION,
                evidence: vec![Hash::of(b"profile")],
                proposed_by: "agent".into(),
                created_at: "2026-10-25T00:00:00.000Z".into(),
                body: ProposalBody::Key { spec, sidecars },
            })
        };
        let symbol = vec!["symbol".to_string()];
        let records = [
            proposal(
                KeySpec::Exact {
                    columns: symbol.clone(),
                    counts: false,
                    bounds: Some("ts".into()),
                },
                Vec::new(),
            ),
            proposal(
                KeySpec::Bitemporal {
                    columns: vec!["id".into()],
                    valid_column: "valid".into(),
                    known_column: "known".into(),
                },
                Vec::new(),
            ),
            proposal(
                KeySpec::Exact {
                    columns: symbol.clone(),
                    counts: true,
                    bounds: None,
                },
                vec![KeySpec::Bloom {
                    columns: symbol,
                    fpr: 0.01,
                    bounds: Some("ts".into()),
                }],
            ),
        ];
        let doc = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/format.md"),
        )
        .unwrap();
        let section = doc
            .split("### Sidecar proposal vectors")
            .nth(1)
            .unwrap_or_else(|| {
                panic!(
                    "docs/format.md has no \"Sidecar proposal vectors\" section; its block:\n{}",
                    records
                        .iter()
                        .map(|r| serde_json::to_string(r).unwrap())
                        .collect::<Vec<_>>()
                        .join("\n")
                )
            });
        let lines: Vec<&str> = section
            .split("```json\n")
            .nth(1)
            .and_then(|b| b.split("\n```").next())
            .expect("a json block")
            .lines()
            .collect();
        let got: Vec<String> = records
            .iter()
            .map(|r| serde_json::to_string(r).unwrap())
            .collect();
        assert_eq!(lines, got, "docs/format.md's sidecar proposal vectors");
        for (line, r) in lines.iter().zip(&records) {
            assert_eq!(&serde_json::from_str::<Record>(line).unwrap(), r);
        }
    }

    /// With the feature on, the schemas describe what serde writes: a hash
    /// is a 64-hex string, the bitmap a list of integers, the age an
    /// integer, and records are tagged by `record`.
    #[cfg(feature = "schema")]
    #[test]
    fn schemas_describe_the_serialized_form() {
        let hash = serde_json::to_value(schemars::schema_for!(Hash)).unwrap();
        assert_eq!(hash["type"], "string");
        assert_eq!(hash["pattern"], "^[0-9a-f]{64}$");
        let entry = serde_json::to_value(schemars::schema_for!(ScanEntry)).unwrap();
        assert_eq!(entry["properties"]["row_groups"]["type"], "array");
        assert_eq!(
            entry["properties"]["row_groups"]["items"]["type"],
            "integer"
        );
        let prov = serde_json::to_value(schemars::schema_for!(Provenance)).unwrap();
        assert_eq!(prov["properties"]["oldest_input_age"]["type"], "integer");
        let record = serde_json::to_string(&schemars::schema_for!(Record)).unwrap();
        for tag in ["evidence", "proposal", "replay", "certificate", "receipt"] {
            assert!(record.contains(&format!("\"const\":\"{tag}\"")), "{tag}");
        }
        let body = serde_json::to_value(schemars::schema_for!(ProposalBody)).unwrap();
        assert!(
            !has_property(&body, "score"),
            "a proposal has no score: {body}"
        );
        let proposal = serde_json::to_value(schemars::schema_for!(Proposal)).unwrap();
        assert!(!has_property(&proposal, "score"), "{proposal}");
        assert!(has_property(&proposal, "proposed_by"), "{proposal}");
    }

    /// Whether any object schema anywhere in `v` declares property `name`.
    #[cfg(feature = "schema")]
    fn has_property(v: &serde_json::Value, name: &str) -> bool {
        match v {
            serde_json::Value::Object(m) => {
                m.get("properties")
                    .and_then(serde_json::Value::as_object)
                    .is_some_and(|p| p.contains_key(name))
                    || m.values().any(|x| has_property(x, name))
            }
            serde_json::Value::Array(a) => a.iter().any(|x| has_property(x, name)),
            _ => false,
        }
    }
}
