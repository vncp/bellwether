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

    /// Version of the manifest and segment layout.
    pub const STORAGE_VERSION: u32 = 1;
    /// Version of the ledger record schemas. History: 1 (contract v2);
    /// 2 (contract v2.1, 2026-10-24: `Replay` gains `inputs` and `model`).
    pub const RECORD_VERSION: u32 = 2;
    /// Version of the canonical byte encoding (ADR 016).
    pub const ENCODING_VERSION: u32 = 1;
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// The error type shared across the engine. Variants are deliberately coarse;
/// crates attach detail in the message and may wrap with their own types.
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
    /// (for example a proposal hash where a certificate is applied).
    #[error("{hash} is {found}, not {expected}; {next}")]
    WrongKind {
        /// The hash given.
        hash: Hash,
        /// The kind the operation takes, with its article ("a certificate").
        expected: String,
        /// The kind found, with its article ("a proposal").
        found: String,
        /// What to pass instead.
        next: String,
    },
    /// Serialization failure.
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    /// Anything else.
    #[error("{0}")]
    Other(String),
}

/// Result alias used throughout the engine.
pub type Result<T> = std::result::Result<T, Error>;

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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum KeySpec {
    /// Exact set membership per row group.
    Exact {
        /// Key columns in prefix order.
        columns: Vec<String>,
    },
    /// Split-block bloom filter per block (ADR 008).
    Bloom {
        /// Key columns in prefix order.
        columns: Vec<String>,
        /// Target false-positive rate, strictly between 0 and 1.
        fpr: f64,
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
    },
}

impl KeySpec {
    /// The key columns, in prefix order.
    #[must_use]
    pub fn columns(&self) -> &[String] {
        match self {
            Self::Exact { columns }
            | Self::Bloom { columns, .. }
            | Self::Temporal { columns, .. } => columns,
            Self::Range { column } => std::slice::from_ref(column),
        }
    }

    /// A stable name for this spec, used as the segment key-spec id.
    #[must_use]
    pub fn name(&self) -> String {
        let kind = match self {
            Self::Exact { .. } => "exact",
            Self::Bloom { .. } => "bloom",
            Self::Range { .. } => "range",
            Self::Temporal { .. } => "temporal",
        };
        format!("{kind}:{}", self.columns().join(","))
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
    },
    /// Output of `locate`, `latest`, or `asof`.
    Plan {
        /// The plan.
        plan: ScanPlan,
    },
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct RewriteShape {
    /// Human-readable description of the current shape.
    pub before: String,
    /// Human-readable description of the target shape.
    pub after: String,
    /// The algebraic or data invariant the rewrite must preserve.
    pub invariant: String,
}

/// What an agent may propose. There is no score field in any variant; scoring
/// is the engine's (ADR 009).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProposalBody {
    /// Add or change an index key.
    Key {
        /// The proposed spec.
        spec: KeySpec,
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
    /// replayed, such as a step inside a database (ADR 027). The Replay must
    /// then name the model. A key proposal is always exact. Never silently
    /// estimated (ADR 005); contract v3 replaces this flag with a tri-state.
    pub exact: bool,
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
    /// Cost per run under the current configuration.
    pub current: Vec<RunCost>,
    /// Cost per run under the proposal.
    pub proposed: Vec<RunCost>,
    /// Name of the cost model behind any cost with `exact == false`.
    /// Present exactly when such a cost exists (a layout proposal), absent
    /// otherwise; enforced by the ledger against the proposal's kind. Absent
    /// in version-1 records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
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
    /// Keys present on both with differing latest-version row hash.
    pub mismatched: u64,
    /// Columns excluded from the row hash, listed so the certificate is
    /// auditable.
    pub ignored_columns: Vec<String>,
    /// Per-group decimal totals that differed, as `(group, left, right)` with
    /// decimals rendered as strings at declared scale.
    pub totals_delta: Vec<(String, String, String)>,
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
                },
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
                    root: "./orders".into(),
                    fanout: vec![Fanout {
                        columns: vec!["batch_id".into()],
                        distinct: 500,
                        sampled: false,
                        files_per_value: [9.0, 60.0, 900.0],
                        row_groups_per_value: [40.0, 500.0, 400.0],
                        files_total: 1_000,
                        row_groups_total: 8_000,
                    }],
                    lints: vec![Lint {
                        rule: "batch_stamp_key".into(),
                        columns: vec!["batch_id".into()],
                        message: "looks like an id, is a timestamp".into(),
                        confidence: 0.9,
                        site: None,
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
                },
                provenance: provenance(),
            }),
            Record::Receipt(Receipt {
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
            run_id: "r1".into(),
            keys: 10,
            files: 2,
            row_groups: 3,
            bytes: 4096,
            exact: true,
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
                        }],
                        residual: Some(Predicate::In {
                            columns: vec!["id".into()],
                            values: vec![vec![0, 1, 2]],
                        }),
                        bytes: 99,
                        row_groups_total: 4,
                        row_groups_selected: 2,
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
                        before: "b".into(),
                        after: "a".into(),
                        invariant: "i".into(),
                    },
                },
            }),
            Record::Replay(Replay {
                record_version: format::RECORD_VERSION,
                proposal: Hash::of(b"p"),
                manifest_version: "3".into(),
                workload: Hash::of(b"w"),
                inputs: vec![Hash::of(b"c")],
                current: vec![RunCost {
                    run_id: "r".into(),
                    keys: 2,
                    files: 3,
                    row_groups: 4,
                    bytes: 5,
                    exact: true,
                }],
                proposed: vec![],
                model: None,
                created_at: "2026-10-24T00:00:00.000Z".into(),
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
                },
                provenance,
            }),
            Record::Receipt(Receipt {
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

    /// The canonical bytes of every record kind, pinned. A derive that
    /// changed serialization would change this hash.
    #[test]
    fn record_bytes_are_the_same_with_and_without_the_schema_feature() {
        let mut all = Vec::new();
        for r in every_kind() {
            all.extend(serde_json::to_vec(&r).unwrap());
            all.push(b'\n');
        }
        assert_eq!(Hash::of(&all).to_hex(), GOLDEN);
    }

    const GOLDEN: &str = "d00a59c94e95f3829c8a9019e964ffe276757709eb22ce4ff770079998c5fa49";

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
