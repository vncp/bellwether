//! The manifest: the single mutable object of an index, committed by
//! conditional PUT (docs/format.md, ADR 003).
//!
//! Forward compatibility (ADR 017) has two rules, split by who owns the type:
//!
//! - **Manifest-owned structs** (everything defined in this module) carry
//!   `#[serde(flatten)] extra`, so fields written by a newer binary survive a
//!   rewrite by this one.
//! - **Types-owned structs** embedded in the manifest ([`FileFingerprint`],
//!   [`KeySpec`]) have no extra map, because `bellwether-types` is the shared
//!   contract. Adding a `KeySpec` kind, or a field to either type, requires a
//!   `storage_version` bump. A reader that meets an unknown kind or field
//!   there refuses with [`Error::UnsupportedVersion`] rather than a serde error
//!   or, worse, silently dropping the field on rewrite.
//!
//! A manifest whose `storage_version` is newer than [`STORAGE_VERSION`] is
//! refused with [`Error::UnsupportedVersion`] before typed deserialization.

use std::collections::{BTreeMap, BTreeSet};

use bellwether_types::format::STORAGE_VERSION;
use bellwether_types::{Error, FileFingerprint, Hash, KeySpec, Result, RowGroupId};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Unknown fields preserved across a rewrite.
pub type Extra = Map<String, Value>;

/// File name of the manifest under an index root.
pub const MANIFEST_FILE: &str = "manifest.json";
/// Directory of immutable committed manifest history objects under an index root.
pub const MANIFESTS_DIR: &str = "manifests";
/// Directory of immutable segments under an index root.
pub const SEGMENTS_DIR: &str = "segments";
/// Directory of the rebuildable cache under an index root.
pub const CACHE_DIR: &str = "cache";

/// Joins an index root and a relative path into a store path.
#[must_use]
pub fn join(root: &str, rel: &str) -> String {
    let root = root.trim_matches('/');
    if root.is_empty() {
        rel.to_string()
    } else {
        format!("{root}/{rel}")
    }
}

/// Store path of the manifest under `root`.
#[must_use]
pub fn manifest_path(root: &str) -> String {
    join(root, MANIFEST_FILE)
}

/// Store path of the immutable history manifest for `sequence` under `root`:
/// `manifests/<sequence as 20-digit zero-padded decimal>.json`.
#[must_use]
pub fn manifest_history_path(root: &str, sequence: u64) -> String {
    join(root, &format!("{MANIFESTS_DIR}/{sequence:020}.json"))
}

/// Store path of segment `id` under `root`: `segments/<blake3>.seg`.
#[must_use]
pub fn segment_path(root: &str, id: &Hash) -> String {
    join(root, &format!("{SEGMENTS_DIR}/{}.seg", id.to_hex()))
}

/// Current time as RFC 3339 UTC, the timestamp format used in the manifest.
#[must_use]
pub fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// `s`, an RFC 3339 timestamp, in [`now_rfc3339`]'s format: UTC with
/// millisecond precision (anything finer is truncated). Every record a
/// caller stamps with this format shares one spelling.
pub fn rfc3339_millis(s: &str) -> Result<String> {
    Ok(parse_rfc3339(s)?.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
}

/// Parses an RFC 3339 timestamp written by [`now_rfc3339`].
pub fn parse_rfc3339(s: &str) -> Result<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&chrono::Utc))
        .map_err(|e| Error::Other(format!("invalid timestamp {s:?}: {e}")))
}

/// Where the indexed data lives. Mirrors `source` in `bellwether.toml`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SourceInfo {
    /// Source kind, for example `parquet`, `csv`, `jsonl`.
    #[serde(default)]
    pub kind: String,
    /// Root path or URL of the source files.
    #[serde(default)]
    pub root: String,
    /// How files are fingerprinted, if configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint_policy: Option<String>,
    /// Partitioning scheme of the source folder: `"hive"` when `name=value`
    /// directories are columns (docs/format.md, "Partition columns"), absent
    /// for none. Added 2026-10-24; a binary that predates it keeps it as an
    /// unknown field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partitioning: Option<String>,
    /// How a delimited text source is parsed, for `kind: "csv"` (storage
    /// version 2): its delimiter, header and quote. A storage version 1
    /// manifest carried a dialect other than comma-with-header in `kind`
    /// (`csv;delimiter=<byte>;header=<bool>`), which is still read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dialect: Option<SourceDialect>,
    /// Unknown fields (ADR 017).
    #[serde(flatten)]
    pub extra: Extra,
}

/// A delimited text source's dialect (`source.dialect`, storage version 2).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceDialect {
    /// The field delimiter, as its byte value (44 for `,`).
    pub delimiter: u8,
    /// Whether the first line of each file is a header row.
    pub header: bool,
    /// The quote character, as its byte value (34 for `"`).
    pub quote: u8,
    /// Unknown fields (ADR 017).
    #[serde(flatten)]
    pub extra: Extra,
}

/// A manifest column's `data_type`: in a storage version 2 manifest, the
/// segment format's `ColumnType` JSON (`{"type":"int64"}`), or `{"arrow":
/// "<display>"}` for a type without one (a nested or dictionary column);
/// in a storage version 1 manifest, Arrow's display form as a string. The
/// store keeps it as written; the build reads either form.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ColumnDataType {
    /// Arrow's display form (storage version 1).
    Display(String),
    /// A JSON object: `ColumnType` JSON or `{"arrow": "<display>"}`
    /// (storage version 2).
    Typed(Map<String, Value>),
}

impl Default for ColumnDataType {
    fn default() -> Self {
        Self::Display(String::new())
    }
}

impl From<&str> for ColumnDataType {
    fn from(s: &str) -> Self {
        Self::Display(s.to_string())
    }
}

impl From<String> for ColumnDataType {
    fn from(s: String) -> Self {
        Self::Display(s)
    }
}

/// The display string, or the object's compact JSON.
impl std::fmt::Display for ColumnDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Display(s) => f.write_str(s),
            Self::Typed(m) => f.write_str(&Value::Object(m.clone()).to_string()),
        }
    }
}

/// One column of the indexed dataset.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    /// Column name.
    pub name: String,
    /// The column's type ([`ColumnDataType`]).
    #[serde(default)]
    pub data_type: ColumnDataType,
    /// Whether the column is nullable.
    #[serde(default)]
    pub nullable: bool,
    /// Unknown fields (ADR 017).
    #[serde(flatten)]
    pub extra: Extra,
}

/// A sidecar segment declaration in `schema.sidecars`.
///
/// Sidecar kinds live in a manifest-owned list rather than
/// `bellwether_types::KeySpec`, so a binary that predates a sidecar kind can
/// still open the manifest, preserve the declaration on rewrite, and report
/// the unknown kind via [`Manifest::unknown_segment_kinds`]. Storage version
/// 2 declares only sketches here; per-key time bounds and bitemporal
/// versions are key specs (`KeySpec::Exact.bounds`, `KeySpec::Bitemporal`).
/// The `"bounds"` and `"temporal2"` declarations of an index written before
/// M5 are read as they were, and the next build with work folds them into
/// key specs (docs/format.md, "Reading storage version 1").
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SidecarDecl {
    /// Sidecar kind, for example `"bounds"` or `"temporal2"`.
    pub kind: String,
    /// Key columns in tuple order.
    #[serde(default)]
    pub columns: Vec<String>,
    /// Timestamp column(s) the sidecar indexes over.
    #[serde(default)]
    pub time_columns: Vec<String>,
    /// The sidecar's parameters, by name (`sketch`: `p`, `minhash`). Absent
    /// when it has none.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, String>,
    /// Unknown fields (ADR 017).
    #[serde(flatten)]
    pub extra: Extra,
}

impl SidecarDecl {
    /// Canonical sidecar name, stored in [`SegmentRef::key_spec`] for segments
    /// of this sidecar: `{kind}:{columns joined by ','}@{time_columns joined by ','}`,
    /// without the `@` part when it has no time column (`sketch:a,b`).
    #[must_use]
    pub fn name(&self) -> String {
        if self.time_columns.is_empty() {
            return format!("{}:{}", self.kind, self.columns.join(","));
        }
        format!(
            "{}:{}@{}",
            self.kind,
            self.columns.join(","),
            self.time_columns.join(",")
        )
    }
}

/// Segment kinds this build understands in [`SegmentRef::kind`] and
/// [`SidecarDecl::kind`].
pub const KNOWN_SEGMENT_KINDS: [&str; 3] = ["bounds", "temporal2", "sketch"];

/// Dataset schema, declared key specs, and declared sidecars.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SchemaInfo {
    /// Columns in schema order.
    #[serde(default)]
    pub columns: Vec<ColumnDef>,
    /// Declared key specs.
    #[serde(default)]
    pub key_specs: Vec<KeySpec>,
    /// Declared sidecar specs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sidecars: Vec<SidecarDecl>,
    /// Unknown fields (ADR 017).
    #[serde(flatten)]
    pub extra: Extra,
}

/// A byte range `[start, end)` of a text source file treated as a row group.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkRange {
    /// First byte.
    pub start: u64,
    /// One past the last byte.
    pub end: u64,
    /// Unknown fields (ADR 017).
    #[serde(flatten)]
    pub extra: Extra,
}

/// A source file and the global row-group ids assigned to its row groups.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FileEntry {
    /// The file's fingerprint at assignment time.
    pub fingerprint: FileFingerprint,
    /// Ids in `index_in_file` order.
    #[serde(default)]
    pub rowgroup_ids: Vec<RowGroupId>,
    /// Chunk size for byte-range sources; immutable per file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_size: Option<u64>,
    /// Chunk ranges for byte-range sources; immutable per file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunks: Option<Vec<ChunkRange>>,
    /// Rows of each row group, aligned with `rowgroup_ids` (storage version
    /// 2). Absent only in a version 1 manifest, which the first commit by a
    /// version 2 binary fills from the footers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rowgroup_rows: Option<Vec<u64>>,
    /// Compressed bytes of each row group (a text chunk's length), aligned
    /// with `rowgroup_ids` (storage version 2), so a plan sums bytes from
    /// the manifest and opens no footer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rowgroup_bytes: Option<Vec<u64>>,
    /// Compressed bytes of each column chunk, row-group-major: entry
    /// `i * columns + j` is row group `i`'s chunk of schema column `j`
    /// (storage version 2); 0 for a virtual partition column. Absent for a
    /// text source, whose chunks have no per-column bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column_bytes: Option<Vec<u64>>,
    /// Unknown fields (ADR 017).
    #[serde(flatten)]
    pub extra: Extra,
}

impl FileEntry {
    /// The row-group statistics a plan reads from the manifest: rows and
    /// bytes of row group `i` in the file, when recorded (storage version 2).
    #[must_use]
    pub fn rowgroup_stats(&self, i: usize) -> Option<(u64, u64)> {
        Some((
            *self.rowgroup_rows.as_ref()?.get(i)?,
            *self.rowgroup_bytes.as_ref()?.get(i)?,
        ))
    }

    /// The compressed bytes of schema column `column` (of `columns`) in row
    /// group `i` of the file, when recorded.
    #[must_use]
    pub fn column_bytes_of(&self, i: usize, column: usize, columns: usize) -> Option<u64> {
        self.column_bytes
            .as_ref()?
            .get(i.checked_mul(columns)?.checked_add(column)?)
            .copied()
    }

    /// Whether the version 2 arrays are recorded (`column_bytes` may stay
    /// absent for a text source).
    #[must_use]
    pub fn has_rowgroup_stats(&self) -> bool {
        self.rowgroup_rows.is_some() && self.rowgroup_bytes.is_some()
    }

    /// Checks that the version 2 arrays, where present, align with the
    /// file's row groups and the schema's `columns`.
    fn check_aligned(&self, columns: usize) -> Result<()> {
        let n = self.rowgroup_ids.len();
        let bad = |what: &str, found: usize, want: usize| {
            Error::Other(format!(
                "corrupt manifest: file {} has {found} {what} entries for {want}; the manifest \
                 was edited or truncated outside a commit, so restore it from manifests/ or \
                 re-index",
                self.fingerprint.path
            ))
        };
        if let Some(v) = &self.rowgroup_rows {
            if v.len() != n {
                return Err(bad("rowgroup_rows", v.len(), n));
            }
        }
        if let Some(v) = &self.rowgroup_bytes {
            if v.len() != n {
                return Err(bad("rowgroup_bytes", v.len(), n));
            }
        }
        if let Some(v) = &self.column_bytes {
            let want = n.saturating_mul(columns);
            if v.len() != want {
                return Err(bad("column_bytes", v.len(), want));
            }
        }
        Ok(())
    }
}

/// Row-group ids retired by fingerprint drift, and since when (ADR 013).
/// Retired ids are never reused.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RowGroupTombstone {
    /// RFC 3339 time the ids were retired.
    pub since: String,
    /// Retired ids.
    #[serde(default)]
    pub rowgroup_ids: Vec<RowGroupId>,
    /// Unknown fields (ADR 017).
    #[serde(flatten)]
    pub extra: Extra,
}

/// The global row-group id space.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RowGroupSpace {
    /// Next id to assign. Ids are dense and never reused.
    #[serde(default)]
    pub next_id: u32,
    /// Files and their assigned ids.
    #[serde(default)]
    pub files: Vec<FileEntry>,
    /// Retired row groups.
    #[serde(default)]
    pub tombstones: Vec<RowGroupTombstone>,
    /// Unknown fields (ADR 017).
    #[serde(flatten)]
    pub extra: Extra,
}

/// Half-open range of row-group ids `[start, end)` a segment covers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowGroupRange {
    /// First id covered.
    pub start: RowGroupId,
    /// One past the last id covered.
    pub end: RowGroupId,
    /// Unknown fields (ADR 017).
    #[serde(flatten)]
    pub extra: Extra,
}

impl RowGroupRange {
    /// `[start, end)` with no extra fields.
    #[must_use]
    pub fn new(start: u32, end: u32) -> Self {
        Self {
            start: RowGroupId(start),
            end: RowGroupId(end),
            extra: Extra::new(),
        }
    }
}

/// A committed, immutable segment.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SegmentRef {
    /// blake3 of the segment bytes; also its file name.
    pub id: Hash,
    /// Sidecar segment kind (`"bounds"`, `"temporal2"`, `"sketch"`, or a
    /// future kind), or `None` for an ordinary key-spec segment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// The key spec's identity ([`KeySpec::identity`]; its name in a
    /// storage version 1 file) when `kind` is `None`; the sidecar's
    /// [`SidecarDecl::name`] when `kind` is `Some`, except the `bounds`
    /// sidecar of a bloom spec with per-key bounds, listed under the bloom's
    /// identity (`bloom:symbol@0.01@ts`).
    pub key_spec: String,
    /// Row groups covered.
    pub rowgroup_range: RowGroupRange,
    /// Size of the segment in bytes.
    pub bytes: u64,
    /// For a join key's segment (B3): what the pair index was built from,
    /// as its header records it (ADR 020). Absent for every other segment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub join: Option<JoinPin>,
    /// Unknown fields (ADR 017).
    #[serde(flatten)]
    pub extra: Extra,
}

/// What a join key's pair segment (B3) was built from, in its manifest
/// entry (`segments[].join`) and its header alike: the right dataset's
/// manifest, by sequence and by content, and the left's live row groups.
/// The key is stale on the right once the right dataset's current
/// manifest is another (a later sequence, or the same sequence of an
/// index deleted and rebuilt), and stale on the left once the left's live
/// row groups are another set (a file added, changed, or removed).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinPin {
    /// The right dataset's manifest sequence.
    pub right_sequence: u64,
    /// BLAKE3 of the right dataset's manifest bytes as stored
    /// ([`crate::Snapshot::content_hash`]).
    pub right_manifest: Hash,
    /// The identity of the left's live row-group set the pair was built
    /// over (core's `join::live_identity`).
    pub left_live: Hash,
    /// Unknown fields (ADR 017).
    #[serde(flatten)]
    pub extra: Extra,
}

/// When a segment stopped being referenced by `segments` (ADR 013). GC
/// compares `since` against its retention window.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SegmentTombstone {
    /// The unreferenced segment.
    pub id: Hash,
    /// RFC 3339 time it became unreferenced (or was first seen as an orphan).
    pub since: String,
    /// Unknown fields (ADR 017).
    #[serde(flatten)]
    pub extra: Extra,
}

impl SegmentTombstone {
    /// A tombstone for `id` stamped now.
    #[must_use]
    pub fn now(id: Hash) -> Self {
        Self {
            id,
            since: now_rfc3339(),
            extra: Extra::new(),
        }
    }
}

/// A garbage collector's claim on an unreferenced segment it is about to
/// delete. While a claim stands, `commit` refuses any manifest that
/// references the id.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DeleteClaim {
    /// The segment being deleted.
    pub id: Hash,
    /// RFC 3339 time of the claim. A claim older than the claim lease (not
    /// the retention window) is abandoned, meaning its GC run died, and a
    /// later run takes it over.
    pub claimed_at: String,
    /// Random token of the GC run that holds the claim. A run deletes and
    /// releases only claims carrying its own token. `0` (absent) never
    /// matches a run.
    #[serde(default)]
    pub token: u64,
    /// The claiming run's lease, in milliseconds. A taker treats the claim
    /// as abandoned only after the larger of this and its own lease, so a
    /// run configured with a shorter lease can never take over a live claim
    /// whose holder is still allowed to delete. `0` (absent) defers to the
    /// taker's lease.
    #[serde(default)]
    pub lease_ms: u64,
    /// Unknown fields (ADR 017).
    #[serde(flatten)]
    pub extra: Extra,
}

/// The manifest, `<root>/manifest.json`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    /// Storage format version; see [`STORAGE_VERSION`].
    pub storage_version: u32,
    /// Monotonic commit counter, +1 per successful commit (ADR 020). This is
    /// the manifest's identity for callers; the store version token is only
    /// the CAS token. `0` means "never committed".
    #[serde(default)]
    pub sequence: u64,
    /// RFC 3339 time the index was first created. Preserved across commits.
    pub created_at: String,
    /// RFC 3339 time of the commit that wrote this version.
    #[serde(default)]
    pub committed_at: String,
    /// Random value written by each commit attempt, so a writer whose
    /// conditional PUT was applied but reported as failed (for example a
    /// retried request answered with 412) recognises its own win.
    #[serde(default)]
    pub commit_nonce: u64,
    /// Source description.
    #[serde(default)]
    pub source: SourceInfo,
    /// Dataset schema and key specs.
    #[serde(default)]
    pub schema: SchemaInfo,
    /// Row-group id space.
    #[serde(default)]
    pub rowgroup_space: RowGroupSpace,
    /// For each key spec name, the `sequence` at which its segments covered
    /// every tracked row group (ADR 012, ADR 020).
    #[serde(default)]
    pub key_spec_completeness: BTreeMap<String, u64>,
    /// Live segments.
    #[serde(default)]
    pub segments: Vec<SegmentRef>,
    /// Unreferenced segments awaiting GC (ADR 013).
    #[serde(default)]
    pub segment_tombstones: Vec<SegmentTombstone>,
    /// Segments a GC run has claimed for deletion. Managed by GC only;
    /// `commit` restores it if the caller's transformation changes it.
    #[serde(default)]
    pub deleting: Vec<DeleteClaim>,
    /// Unknown fields (ADR 017).
    #[serde(flatten)]
    pub extra: Extra,
}

impl Default for Manifest {
    fn default() -> Self {
        Self::new()
    }
}

impl Manifest {
    /// An empty, never-committed manifest at the current storage version.
    #[must_use]
    pub fn new() -> Self {
        let now = now_rfc3339();
        Self {
            storage_version: STORAGE_VERSION,
            sequence: 0,
            created_at: now.clone(),
            committed_at: now,
            commit_nonce: 0,
            source: SourceInfo::default(),
            schema: SchemaInfo::default(),
            rowgroup_space: RowGroupSpace::default(),
            key_spec_completeness: BTreeMap::new(),
            segments: Vec::new(),
            segment_tombstones: Vec::new(),
            deleting: Vec::new(),
            extra: Extra::new(),
        }
    }

    /// Parses manifest bytes.
    ///
    /// Refuses with [`Error::UnsupportedVersion`], before typed
    /// deserialization: a newer `storage_version` (`what` =
    /// [`UNSUPPORTED_STORAGE`]), and an unknown `KeySpec` kind or an unknown
    /// field in a `KeySpec` or `FileFingerprint` (`what` =
    /// [`UNSUPPORTED_SHAPE`]; those require a version bump, see the module
    /// docs). Refuses a file whose per-row-group arrays do not align with
    /// its row groups.
    ///
    /// A storage version 1 manifest keys segments and completeness by
    /// [`KeySpec::name`]; it is read with those keys rewritten to
    /// [`KeySpec::identity`], the version 2 keys, so every caller sees one
    /// shape. Its `storage_version` stays 1 until a commit stamps 2
    /// (docs/format.md, "Reading storage version 1"). Its specs are read as
    /// they were declared, whatever their column names: two of them that
    /// share an identity (which version 1 allowed) keep their segments
    /// apart under [`V1_SHARED_IDENTITY`] keys, so neither plans until one
    /// is re-declared, and the build refuses to upgrade the index until
    /// then. A version 2 manifest with two such specs is refused as corrupt:
    /// no build commits one.
    pub fn from_slice(bytes: &[u8]) -> Result<Self> {
        let value: Value = serde_json::from_slice(bytes)?;
        let found = value
            .get("storage_version")
            .and_then(Value::as_u64)
            .ok_or_else(|| Error::Other("manifest has no numeric storage_version".into()))?;
        let unsupported = |what| Error::UnsupportedVersion {
            what,
            found: u32::try_from(found).unwrap_or(u32::MAX),
            supported: STORAGE_VERSION,
        };
        if found > u64::from(STORAGE_VERSION) {
            return Err(unsupported(UNSUPPORTED_STORAGE));
        }
        if !types_owned_fields_known(&value, found) {
            return Err(Error::UnknownKind {
                what: UNSUPPORTED_SHAPE,
                found: "unrecognized key-spec kind or fingerprint field".to_string(),
            });
        }
        let mut m: Self = serde_json::from_value(value)?;
        if m.storage_version >= 2 {
            if let Some((a, b)) = KeySpec::shared_identity(&m.schema.key_specs) {
                return Err(Error::Other(format!(
                    "corrupt index: manifest sequence {} declares the key specs {} and {}, which \
                     share the identity {}, and storage version 2 files segments by identity, so \
                     no build commits that; restore the manifest from manifests/",
                    m.sequence,
                    spec_json(a),
                    spec_json(b),
                    a.identity()
                )));
            }
        }
        let columns = m.schema.columns.len();
        for f in &m.rowgroup_space.files {
            f.check_aligned(columns)?;
        }
        if m.storage_version < 2 {
            m.rekey_by_identity();
        }
        Ok(m)
    }

    /// Rewrites a storage version 1 manifest's keys (a key spec segment's
    /// `key_spec` and `key_spec_completeness`) from each spec's name to its
    /// identity. Version 1 allowed one spec per name, so the map is
    /// one-to-one, except for specs that share an identity: those map to
    /// [`V1_SHARED_IDENTITY`] and their name, which no identity equals.
    /// Sidecar segments keep their sidecar names, and a name no declared
    /// spec has is left as it is (it plans with nothing).
    fn rekey_by_identity(&mut self) {
        let by_name: BTreeMap<String, String> = self
            .schema
            .key_specs
            .iter()
            .map(|s| (s.name(), self.v2_key(s)))
            .collect();
        self.rekey(&by_name);
    }

    /// The key a version 1 spec's segments are read under: its identity,
    /// or, when another declared spec shares that identity,
    /// [`V1_SHARED_IDENTITY`] and its name.
    fn v2_key(&self, spec: &KeySpec) -> String {
        let identity = spec.identity();
        let shared = self
            .schema
            .key_specs
            .iter()
            .filter(|s| s.identity() == identity)
            .count()
            > 1;
        if shared {
            format!("{V1_SHARED_IDENTITY}{}", spec.name())
        } else {
            identity
        }
    }

    /// The inverse of [`Manifest::rekey_by_identity`], for writing a
    /// storage version 1 manifest back in version 1's keys.
    fn rekey_by_name(&mut self) {
        let by_identity: BTreeMap<String, String> = self
            .schema
            .key_specs
            .iter()
            .map(|s| (self.v2_key(s), s.name()))
            .collect();
        self.rekey(&by_identity);
    }

    /// Renames key spec segments' `key_spec` and `key_spec_completeness`
    /// keys found in `map`; sidecar segments and other keys stay.
    fn rekey(&mut self, map: &BTreeMap<String, String>) {
        for seg in &mut self.segments {
            if seg.kind.is_none() {
                if let Some(to) = map.get(&seg.key_spec) {
                    seg.key_spec.clone_from(to);
                }
            }
        }
        self.key_spec_completeness = std::mem::take(&mut self.key_spec_completeness)
            .into_iter()
            .map(|(k, v)| (map.get(&k).cloned().unwrap_or(k), v))
            .collect();
    }

    /// Serializes to the canonical on-disk form (sorted keys, pretty). A
    /// storage version 1 manifest (one no commit could upgrade yet, see
    /// `commit`) is written in version 1's keys, each spec's name, so a
    /// version 1 binary still reads it.
    pub fn to_vec(&self) -> Result<Vec<u8>> {
        // Round trip through `Value` so flattened extras and known fields
        // come out in one sorted key order.
        let v = if self.storage_version < 2 {
            let mut v1 = self.clone();
            v1.rekey_by_name();
            serde_json::to_value(&v1)?
        } else {
            serde_json::to_value(self)?
        };
        Ok(serde_json::to_vec_pretty(&v)?)
    }

    /// Ids of live segments.
    #[must_use]
    pub fn segment_ids(&self) -> BTreeSet<Hash> {
        self.segments.iter().map(|s| s.id).collect()
    }

    /// Whether `id` is a live segment.
    #[must_use]
    pub fn references(&self, id: &Hash) -> bool {
        self.segments.iter().any(|s| s.id == *id)
    }

    /// Whether `id` is claimed for deletion by a GC run.
    #[must_use]
    pub fn is_claimed(&self, id: &Hash) -> bool {
        self.deleting.iter().any(|c| c.id == *id)
    }

    /// Whether `id` has a segment tombstone.
    #[must_use]
    pub fn is_tombstoned(&self, id: &Hash) -> bool {
        self.segment_tombstones.iter().any(|t| t.id == *id)
    }

    /// Adds `seg` unless a segment with the same id is already live. This is
    /// the union that makes rebase of additive commits trivial (ADR 015).
    pub fn add_segment(&mut self, seg: SegmentRef) {
        if !self.references(&seg.id) {
            self.segments.push(seg);
        }
    }

    /// Sorted distinct kinds appearing in [`Self::segments`] (`kind: Some(_)`)
    /// or [`SchemaInfo::sidecars`] that are not in [`KNOWN_SEGMENT_KINDS`].
    #[must_use]
    pub fn unknown_segment_kinds(&self) -> Vec<String> {
        let mut out = BTreeSet::new();
        for seg in &self.segments {
            if let Some(k) = seg.kind.as_deref() {
                if !KNOWN_SEGMENT_KINDS.contains(&k) {
                    out.insert(k.to_string());
                }
            }
        }
        for decl in &self.schema.sidecars {
            let k = decl.kind.as_str();
            if !KNOWN_SEGMENT_KINDS.contains(&k) {
                out.insert(decl.kind.clone());
            }
        }
        out.into_iter().collect()
    }

    /// Live ordinary key-spec segments (`kind == None`) for the key spec named
    /// `name`. Never yields sidecar segments, even if a sidecar's `key_spec`
    /// string matches `name`.
    pub fn key_spec_segments<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a SegmentRef> {
        self.segments
            .iter()
            .filter(move |s| s.kind.is_none() && s.key_spec == name)
    }

    /// Live sidecar segments of `kind` for the sidecar named `name`.
    pub fn sidecar_segments<'a>(
        &'a self,
        kind: &'a str,
        name: &'a str,
    ) -> impl Iterator<Item = &'a SegmentRef> {
        self.segments
            .iter()
            .filter(move |s| s.kind.as_deref() == Some(kind) && s.key_spec == name)
    }
}

/// `what` of the [`Error::UnsupportedVersion`] for a newer `storage_version`.
pub const UNSUPPORTED_STORAGE: &str = "storage";
/// `what` of the [`Error::UnsupportedVersion`] for an unknown `KeySpec` kind
/// or an unknown field in a `KeySpec` or `FileFingerprint` (ADR 017
/// amendment: such a change requires a `storage_version` bump).
pub const UNSUPPORTED_SHAPE: &str = "key-spec or fingerprint shape in storage";

/// The prefix of the key a storage version 1 spec's segments and
/// completeness are read under when another spec of the manifest shares its
/// identity, followed by its name. No identity starts with it (an identity
/// starts with its kind), so neither spec plans until one is re-declared
/// (`Manifest::from_slice`).
pub const V1_SHARED_IDENTITY: &str = "v1-shared-identity:";

/// A key spec as its JSON form spells it (the form `--key` reads), for a
/// message that names one exactly.
#[must_use]
pub fn spec_json(spec: &KeySpec) -> String {
    serde_json::to_string(spec).unwrap_or_else(|_| spec.identity())
}

/// Serialized field names of each `KeySpec` kind this build understands in
/// a manifest at `storage_version`. Kept in step with `bellwether-types` by
/// a drift test. `counts` arrived in storage version 2, so a version 1
/// manifest that carries it is refused rather than read.
fn key_spec_fields(kind: &str, storage_version: u64) -> Option<&'static [&'static str]> {
    let v2 = storage_version >= 2;
    match kind {
        "exact" if v2 => Some(&["kind", "columns", "counts", "bounds"]),
        "exact" => Some(&["kind", "columns"]),
        "bloom" if v2 => Some(&["kind", "columns", "fpr", "bounds"]),
        "bloom" => Some(&["kind", "columns", "fpr"]),
        "range" => Some(&["kind", "column"]),
        // a list key (B4) is a storage version 2 kind: a version 1 build
        // refuses a manifest declaring one
        "list" if v2 => Some(&["kind", "column"]),
        // a join key (B3) likewise
        "join" if v2 => Some(&[
            "kind",
            "dataset",
            "columns",
            "right",
            "right_columns",
            "counts",
        ]),
        "temporal" if v2 => Some(&["kind", "columns", "ts_column", "counts"]),
        "temporal" => Some(&["kind", "columns", "ts_column"]),
        // Per-key bounds and bitemporal key specs are storage version 2
        // (M5); version 1 declared them as sidecars.
        "bitemporal" if v2 => Some(&["kind", "columns", "valid_column", "known_column"]),
        // --- phase 5 kinds ---
        // Storage version 2 on: a reader of version 1 refuses the kind.
        "expr" if v2 => Some(&["kind", "name", "expr", "over", "conformance"]),
        "text" if v2 => Some(&["kind", "column", "tokenizer", "bloom"]),
        "agg" if v2 => Some(&["kind", "columns", "value", "functions"]),
        _ => None,
    }
}

/// Serialized field names of `FileFingerprint` this build understands.
/// Kept in step with `bellwether-types` by a drift test.
const FINGERPRINT_FIELDS: &[&str] = &["path", "size", "etag", "tail_blake3"];

/// Whether a serialized `KeySpec` has a kind and fields this build
/// understands, including the inner spec an `expr` key wraps.
fn key_spec_known(spec: &Value, storage_version: u64) -> bool {
    fn keys_within(v: &Value, allowed: &[&str]) -> bool {
        v.as_object()
            .is_some_and(|o| o.keys().all(|k| allowed.contains(&k.as_str())))
    }
    let Some(allowed) = spec
        .get("kind")
        .and_then(Value::as_str)
        .and_then(|k| key_spec_fields(k, storage_version))
    else {
        return false;
    };
    keys_within(spec, allowed)
        && spec
            .get("over")
            .is_none_or(|o| key_spec_known(o, storage_version))
}

/// Whether every types-owned struct in the manifest has only kinds and
/// fields this build understands at the manifest's storage version (see the
/// module docs).
fn types_owned_fields_known(manifest: &Value, storage_version: u64) -> bool {
    fn keys_within(v: &Value, allowed: &[&str]) -> bool {
        v.as_object()
            .is_some_and(|o| o.keys().all(|k| allowed.contains(&k.as_str())))
    }
    let key_specs = manifest
        .pointer("/schema/key_specs")
        .and_then(Value::as_array);
    for spec in key_specs.into_iter().flatten() {
        if !key_spec_known(spec, storage_version) {
            return false;
        }
        // A text key's tokenizer is a closed set too: a later one is a
        // shape this build does not know, never a serde error.
        if spec.get("kind").and_then(Value::as_str) == Some("text")
            && !matches!(
                spec.get("tokenizer").and_then(Value::as_str),
                Some("words" | "trigram")
            )
        {
            return false;
        }
    }
    let files = manifest
        .pointer("/rowgroup_space/files")
        .and_then(Value::as_array);
    for file in files.into_iter().flatten() {
        if let Some(fp) = file.get("fingerprint") {
            if !keys_within(fp, FINGERPRINT_FIELDS) {
                return false;
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    const WITH_UNKNOWN: &str = r#"{
      "storage_version": 1,
      "sequence": 7,
      "created_at": "2026-10-23T00:00:00.000Z",
      "committed_at": "2026-10-24T00:00:00.000Z",
      "commit_nonce": 12345,
      "future_top": {"a": 1},
      "source": {"kind": "parquet", "root": "s3://b/x", "future_source": true},
      "schema": {
        "columns": [{"name": "id", "data_type": "Int64", "nullable": false, "future_col": 7}],
        "key_specs": [{"kind": "exact", "columns": ["id"]}],
        "future_schema": "x"
      },
      "rowgroup_space": {
        "next_id": 2,
        "files": [{
          "fingerprint": {"path": "a.csv", "size": 10, "etag": null,
            "tail_blake3": "0000000000000000000000000000000000000000000000000000000000000000"},
          "rowgroup_ids": [0, 1],
          "chunk_size": 5,
          "chunks": [{"start": 0, "end": 5, "future_chunk": 1}, {"start": 5, "end": 10}],
          "future_file": [1, 2]
        }],
        "tombstones": [{"since": "2026-10-23T00:00:00.000Z", "rowgroup_ids": [5], "future_tomb": 1}],
        "future_space": null
      },
      "key_spec_completeness": {"exact:id": 7},
      "segments": [{
        "id": "1111111111111111111111111111111111111111111111111111111111111111",
        "key_spec": "exact:id",
        "rowgroup_range": {"start": 0, "end": 2, "future_range": "r"},
        "bytes": 42,
        "future_seg": "keep me"
      }],
      "segment_tombstones": [{
        "id": "2222222222222222222222222222222222222222222222222222222222222222",
        "since": "2026-10-23T00:00:00.000Z",
        "future_segtomb": [true]
      }],
      "deleting": [{
        "id": "3333333333333333333333333333333333333333333333333333333333333333",
        "claimed_at": "2026-10-23T00:00:00.000Z",
        "token": 99,
        "lease_ms": 3600000,
        "future_claim": {"by": "gc-2"}
      }]
    }"#;

    #[test]
    fn unknown_fields_survive_round_trip_at_every_manifest_owned_level() {
        let m = Manifest::from_slice(WITH_UNKNOWN.as_bytes()).unwrap();
        assert!(m.extra.contains_key("future_top"));
        assert!(m.source.extra.contains_key("future_source"));
        assert!(m.schema.extra.contains_key("future_schema"));
        assert!(m.schema.columns[0].extra.contains_key("future_col"));
        assert!(m.rowgroup_space.extra.contains_key("future_space"));
        let file = &m.rowgroup_space.files[0];
        assert!(file.extra.contains_key("future_file"));
        assert!(file.chunks.as_ref().unwrap()[0]
            .extra
            .contains_key("future_chunk"));
        assert!(m.rowgroup_space.tombstones[0]
            .extra
            .contains_key("future_tomb"));
        assert!(m.segments[0].extra.contains_key("future_seg"));
        assert!(m.segments[0]
            .rowgroup_range
            .extra
            .contains_key("future_range"));
        assert!(m.segment_tombstones[0].extra.contains_key("future_segtomb"));
        assert!(m.deleting[0].extra.contains_key("future_claim"));
        assert_eq!(
            (m.deleting[0].token, m.deleting[0].lease_ms),
            (99, 3_600_000)
        );
        assert_eq!(m.sequence, 7);
        assert_eq!(m.key_spec_completeness["exact:id"], 7);

        let original: Value = serde_json::from_str(WITH_UNKNOWN).unwrap();
        let rewritten: Value = serde_json::from_slice(&m.to_vec().unwrap()).unwrap();
        assert_eq!(original, rewritten);
    }

    fn expect_unsupported_shape(json: &str) {
        match Manifest::from_slice(json.as_bytes()) {
            Err(Error::UnknownKind {
                what: UNSUPPORTED_SHAPE,
                ..
            }) => {}
            other => panic!("expected UnknownKind (shape), got {other:?}"),
        }
    }

    #[test]
    fn newer_storage_version_is_refused_with_typed_error() {
        let v3 = r#"{"storage_version": 3, "created_at": "x", "segments": "a different shape"}"#;
        match Manifest::from_slice(v3.as_bytes()) {
            Err(Error::UnsupportedVersion {
                what: UNSUPPORTED_STORAGE,
                found: 3,
                supported: 2,
            }) => {}
            other => panic!("expected UnsupportedVersion, got {other:?}"),
        }
    }

    /// The allow-lists the loader checks against must match what
    /// `bellwether-types` actually serializes, every optional field set.
    /// Adding a `KeySpec` variant or a `FileFingerprint` field breaks
    /// compilation here (exhaustive match, full struct literal) or fails
    /// this test, so the allow-list and the `storage_version` bump it
    /// implies (ADR 017) cannot be forgotten.
    #[test]
    fn allow_lists_match_types_serialization() {
        let specs = [
            KeySpec::Exact {
                columns: vec!["a".into()],
                counts: true,
                bounds: Some("t".into()),
            },
            KeySpec::Bloom {
                columns: vec!["a".into()],
                fpr: 0.01,
                bounds: Some("t".into()),
            },
            KeySpec::Bitemporal {
                columns: vec!["a".into()],
                valid_column: "v".into(),
                known_column: "k".into(),
            },
            KeySpec::Range { column: "a".into() },
            KeySpec::Temporal {
                columns: vec!["a".into()],
                ts_column: "t".into(),
                counts: true,
            },
            KeySpec::Expr {
                name: "k".into(),
                expr: "lower(a)".into(),
                over: Box::new(KeySpec::Exact {
                    columns: vec!["k".into()],
                    counts: true,
                    bounds: None,
                }),
                conformance: Vec::new(),
            },
            KeySpec::Text {
                column: "a".into(),
                tokenizer: bellwether_types::Tokenizer::Trigram,
                bloom: Some(0.01),
            },
            KeySpec::List { column: "a".into() },
            KeySpec::Join {
                dataset: "l".into(),
                columns: vec!["a".into()],
                right: "r".into(),
                right_columns: vec!["b".into()],
                counts: true,
            },
            KeySpec::Agg {
                columns: vec!["a".into()],
                value: "v".into(),
                functions: bellwether_types::AggregateFn::CANONICAL.to_vec(),
            },
        ];
        let mut kinds_seen = BTreeSet::new();
        for spec in &specs {
            // Exhaustive on purpose: a new variant must be added to `specs`,
            // or to the list of specs this release keeps out of manifests.
            match spec {
                KeySpec::Exact { .. }
                | KeySpec::Bloom { .. }
                | KeySpec::Range { .. }
                | KeySpec::Temporal { .. }
                | KeySpec::Bitemporal { .. }
                | KeySpec::Expr { .. }
                | KeySpec::Text { .. }
                | KeySpec::List { .. }
                | KeySpec::Join { .. }
                | KeySpec::Agg { .. } => {}
            }
            let v = serde_json::to_value(spec).unwrap();
            let kind = v["kind"].as_str().unwrap().to_string();
            let mut keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
            keys.sort_unstable();
            let mut allowed = key_spec_fields(&kind, 2)
                .unwrap_or_else(|| panic!("no allow-list for kind {kind}"))
                .to_vec();
            allowed.sort_unstable();
            assert_eq!(keys, allowed, "KeySpec kind {kind} drifted");
            kinds_seen.insert(kind);
        }
        assert_eq!(kinds_seen.len(), specs.len());
        // Per-key time bounds and bitemporal specs are key specs from
        // storage version 2 (M5); version 1 declared them as sidecars.
        assert!(key_spec_fields("bitemporal", 1).is_none());
        for kind in ["exact", "bloom"] {
            assert!(!key_spec_fields(kind, 1).unwrap().contains(&"bounds"));
        }
        // counts arrived in storage version 2
        for kind in ["exact", "temporal"] {
            assert!(!key_spec_fields(kind, 1).unwrap().contains(&"counts"));
        }

        let fp = FileFingerprint {
            path: "a".into(),
            size: 1,
            etag: None,
            tail_blake3: Hash::of(b"t"),
        };
        let v = serde_json::to_value(&fp).unwrap();
        let mut keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
        keys.sort_unstable();
        let mut allowed = FINGERPRINT_FIELDS.to_vec();
        allowed.sort_unstable();
        assert_eq!(keys, allowed, "FileFingerprint drifted");
    }

    #[test]
    fn unknown_types_owned_shapes_are_refused_not_dropped() {
        // A KeySpec kind this build does not know.
        expect_unsupported_shape(
            r#"{"storage_version": 1, "created_at": "x",
                "schema": {"key_specs": [{"kind": "zorder", "columns": ["a"]}]}}"#,
        );
        // A field added to a known KeySpec kind.
        expect_unsupported_shape(
            r#"{"storage_version": 1, "created_at": "x",
                "schema": {"key_specs": [{"kind": "exact", "columns": ["a"], "collation": "c"}]}}"#,
        );
        // A field added to FileFingerprint.
        expect_unsupported_shape(
            r#"{"storage_version": 1, "created_at": "x",
                "rowgroup_space": {"files": [{"rowgroup_ids": [], "fingerprint": {
                  "path": "a", "size": 1, "etag": null, "mtime_ns": 5,
                  "tail_blake3": "0000000000000000000000000000000000000000000000000000000000000000"}}]}}"#,
        );
    }

    /// A text key (B5) lives in `schema.key_specs` of a storage version 2
    /// manifest as an ordinary key spec, and this build reads it. Storage
    /// version 1 has no text kind: a version 1 manifest that names one is
    /// refused with the typed unknown-kind error, exactly as a version 1
    /// reader refuses a version 2 manifest's text key (never a serde error,
    /// never by dropping the spec; ADR 017). A field or a tokenizer this
    /// build does not know is refused the same way.
    #[test]
    fn text_key_specs_are_read_at_version_2_and_refused_typed_otherwise() {
        let m = Manifest::from_slice(
            br#"{"storage_version": 2, "created_at": "x",
                "schema": {"key_specs": [{"kind": "text", "column": "body", "tokenizer": "words"},
                  {"kind": "text", "column": "body", "tokenizer": "trigram", "bloom": 0.01}]}}"#,
        )
        .unwrap();
        assert_eq!(
            m.schema.key_specs[0],
            KeySpec::Text {
                column: "body".into(),
                tokenizer: bellwether_types::Tokenizer::Words,
                bloom: None,
            }
        );
        assert_eq!(m.schema.key_specs[1].name(), "text:body,tokenizer=trigram");
        assert_eq!(
            m.schema.key_specs[1].identity(),
            "text:body,tokenizer=trigram,bloom=0.01"
        );
        let again = Manifest::from_slice(&m.to_vec().unwrap()).unwrap();
        assert_eq!(again.schema.key_specs, m.schema.key_specs);
        // Version 1 has no text kind.
        expect_unsupported_shape(
            r#"{"storage_version": 1, "created_at": "x",
                "schema": {"key_specs": [{"kind": "text", "column": "body", "tokenizer": "words"}]}}"#,
        );
        // A field a text key does not have.
        expect_unsupported_shape(
            r#"{"storage_version": 2, "created_at": "x",
                "schema": {"key_specs": [{"kind": "text", "column": "b", "tokenizer": "words",
                  "stemmer": "en"}]}}"#,
        );
        // A tokenizer this build does not know.
        expect_unsupported_shape(
            r#"{"storage_version": 2, "created_at": "x",
                "schema": {"key_specs": [{"kind": "text", "column": "b", "tokenizer": "ngram4"}]}}"#,
        );
    }

    /// An `expr` key (phase 5) is a `KeySpec` kind. A reader whose
    /// allow-list lacks a kind, as every reader built before the kind did,
    /// refuses the manifest with the typed shape error, never a serde error
    /// and never a silent drop on rewrite (ADR 017); this reader reads it,
    /// and checks the spec an `expr` key wraps the same way.
    #[test]
    fn expr_keys_load_and_a_reader_without_the_kind_refuses_typed() {
        let at = |version: u32, spec: &str| {
            format!(
                r#"{{"storage_version": {version}, "created_at": "x",
                    "schema": {{"key_specs": [{spec}]}}}}"#
            )
        };
        let with = |spec: &str| at(2, spec);
        let expr = r#"{"kind": "expr", "name": "k", "expr": "lower(a)",
                       "over": {"kind": "exact", "columns": ["k"], "counts": true},
                       "conformance": []}"#;
        let m = Manifest::from_slice(with(expr).as_bytes()).unwrap();
        assert!(matches!(m.schema.key_specs[0], KeySpec::Expr { .. }));
        // The kind ships with storage version 2: a version 1 manifest that
        // names it is refused typed, as a version 1 reader refuses it.
        expect_unsupported_shape(&at(1, expr));
        // What a reader without the kind sees: the same manifest with a kind
        // name its allow-list does not hold.
        expect_unsupported_shape(&with(
            &expr.replace("\"expr\", \"name\"", "\"expr9\", \"name\""),
        ));
        // A field this build does not know, on the expr key or on the spec it
        // wraps.
        expect_unsupported_shape(&with(
            &expr.replace("\"name\"", "\"collation\": 1, \"name\""),
        ));
        expect_unsupported_shape(&with(&expr.replace(
            "\"columns\": [\"k\"]",
            "\"columns\": [\"k\"], \"collation\": 1",
        )));
        expect_unsupported_shape(&with(
            &expr.replace("\"kind\": \"exact\"", "\"kind\": \"zorder\""),
        ));
    }

    /// A list key (B4) is a storage version 2 kind: a version 2 manifest
    /// declaring it reads back as written, and a version 1 manifest
    /// declaring it (what a version 1 build would be handed) is refused with
    /// the typed unknown-kind error, never a serde error and never by
    /// dropping the declaration on a rewrite; so is a list key with a field
    /// this build does not know. The version 1 binary's own allow-list is
    /// `key_spec_fields(_, 1)`, the one the M5 fixture was written under.
    #[test]
    fn a_list_key_is_a_storage_version_2_kind() {
        let json = |version: u32| {
            format!(
                r#"{{"storage_version": {version}, "created_at": "x",
                "schema": {{"key_specs": [{{"kind": "list", "column": "tags"}}]}}}}"#
            )
        };
        let m = Manifest::from_slice(json(2).as_bytes()).unwrap();
        assert_eq!(
            m.schema.key_specs[0],
            KeySpec::List {
                column: "tags".into()
            }
        );
        assert_eq!(m.schema.key_specs[0].name(), "list:tags");
        let back: Value = serde_json::from_slice(&m.to_vec().unwrap()).unwrap();
        assert_eq!(back["schema"]["key_specs"][0]["kind"], "list");
        assert!(key_spec_fields("list", 1).is_none());
        match Manifest::from_slice(json(1).as_bytes()) {
            Err(Error::UnknownKind {
                what: UNSUPPORTED_SHAPE,
                ..
            }) => {}
            other => panic!("expected UnknownKind (shape), got {other:?}"),
        }
        expect_unsupported_shape(
            r#"{"storage_version": 2, "created_at": "x",
                "schema": {"key_specs": [{"kind": "list", "column": "t", "keys_only": true}]}}"#,
        );
    }

    /// A join key (B3) is a storage version 2 kind, like a list key: a
    /// version 2 manifest declaring it reads back as written, with its
    /// segment's pins (the right manifest by sequence and content, the
    /// left's live set) and an unknown pin field kept (ADR 017); a version
    /// 1 manifest declaring it, or a join key with a field this build does
    /// not know, is refused with the typed shape error.
    #[test]
    fn a_join_key_is_a_storage_version_2_kind_and_its_segment_pins_the_right() {
        let json = |version: u32| {
            format!(
                r#"{{"storage_version": {version}, "created_at": "x",
                "schema": {{"key_specs": [{{"kind": "join", "dataset": "orders",
                    "columns": ["id"], "right": "payables", "right_columns": ["order_id"]}}]}},
                "segments": [{{"id": "{}", "key_spec": "join:orders.id=payables.order_id",
                    "rowgroup_range": {{"start": 0, "end": 4}}, "bytes": 10,
                    "join": {{"right_sequence": 7, "right_manifest": "{}",
                        "left_live": "{}", "later": 1}}}}]}}"#,
                "11".repeat(32),
                "22".repeat(32),
                "33".repeat(32)
            )
        };
        let m = Manifest::from_slice(json(2).as_bytes()).unwrap();
        assert_eq!(
            m.schema.key_specs[0].name(),
            "join:orders.id=payables.order_id"
        );
        let pin = m.segments[0].join.as_ref().unwrap();
        assert_eq!(pin.right_sequence, 7);
        assert_eq!(pin.right_manifest, Hash([0x22; 32]));
        assert_eq!(pin.left_live, Hash([0x33; 32]));
        let back: Value = serde_json::from_slice(&m.to_vec().unwrap()).unwrap();
        assert_eq!(back["schema"]["key_specs"][0]["kind"], "join");
        assert_eq!(back["segments"][0]["join"]["right_sequence"], 7);
        assert_eq!(back["segments"][0]["join"]["later"], 1);
        assert!(key_spec_fields("join", 1).is_none());
        match Manifest::from_slice(json(1).as_bytes()) {
            Err(Error::UnknownKind {
                what: UNSUPPORTED_SHAPE,
                ..
            }) => {}
            other => panic!("expected UnknownKind (shape), got {other:?}"),
        }
        expect_unsupported_shape(
            r#"{"storage_version": 2, "created_at": "x",
                "schema": {"key_specs": [{"kind": "join", "dataset": "a", "columns": ["x"],
                "right": "b", "right_columns": ["y"], "outer": true}]}}"#,
        );
    }

    /// Agg keys (B7) are a storage version 2 kind: read and written back
    /// as declared, and refused by name in a version 1 manifest.
    #[test]
    fn an_agg_key_is_a_storage_version_2_kind() {
        let json = |version: u32| {
            format!(
                r#"{{"storage_version": {version}, "created_at": "x",
                "schema": {{"key_specs": [{{"kind": "agg", "columns": ["account"],
                "value": "amount", "functions": ["sum", "count"]}}]}}}}"#
            )
        };
        let m = Manifest::from_slice(json(2).as_bytes()).unwrap();
        assert_eq!(
            m.schema.key_specs[0].identity(),
            "agg:account@amount{sum,count}"
        );
        let back: Value = serde_json::from_slice(&m.to_vec().unwrap()).unwrap();
        assert_eq!(back["schema"]["key_specs"][0]["kind"], "agg");
        assert_eq!(back["schema"]["key_specs"][0]["value"], "amount");
        assert!(key_spec_fields("agg", 1).is_none());
        match Manifest::from_slice(json(1).as_bytes()) {
            Err(Error::UnknownKind {
                what: UNSUPPORTED_SHAPE,
                ..
            }) => {}
            other => panic!("expected UnknownKind (shape), got {other:?}"),
        }
        expect_unsupported_shape(
            r#"{"storage_version": 2, "created_at": "x",
                "schema": {"key_specs": [{"kind": "agg", "columns": ["a"], "value": "v",
                "functions": ["count"], "weights": true}]}}"#,
        );
    }

    #[test]
    fn legacy_manifest_without_kind_or_sidecars_serializes_byte_identically() {
        let mut m = Manifest::from_slice(WITH_UNKNOWN.as_bytes()).unwrap();
        assert_eq!(m.schema.sidecars, Vec::<SidecarDecl>::new());
        assert!(m.segments.iter().all(|s| s.kind.is_none()));
        assert_eq!(m.unknown_segment_kinds(), Vec::<String>::new());
        // Clear the synthetic future_* extras to test a plain pre-WP21 manifest.
        m.extra.clear();
        m.source.extra.clear();
        m.schema.extra.clear();
        m.schema.columns[0].extra.clear();
        m.rowgroup_space.extra.clear();
        m.rowgroup_space.files[0].extra.clear();
        m.rowgroup_space.files[0].chunks.as_mut().unwrap()[0]
            .extra
            .clear();
        m.rowgroup_space.tombstones[0].extra.clear();
        m.segments[0].extra.clear();
        m.segments[0].rowgroup_range.extra.clear();
        m.segment_tombstones[0].extra.clear();
        m.deleting[0].extra.clear();

        let bytes = m.to_vec().unwrap();
        let s = std::str::from_utf8(&bytes).unwrap();
        assert!(
            !s.contains("\"sidecars\""),
            "empty sidecars must be omitted"
        );
        assert!(
            !s.contains("\"kind\": \"bounds\"") && !s.contains("\"kind\": null"),
            "absent SegmentRef.kind must be omitted"
        );
        let reparsed = Manifest::from_slice(&bytes).unwrap();
        assert_eq!(m, reparsed);
        assert_eq!(bytes, reparsed.to_vec().unwrap());
    }

    #[test]
    fn sidecar_declarations_and_unknown_segment_kinds_round_trip() {
        let decl_bounds = SidecarDecl {
            kind: "bounds".into(),
            columns: vec!["symbol".into(), "venue".into()],
            time_columns: vec!["ts".into()],
            params: BTreeMap::new(),
            extra: Extra::new(),
        };
        assert_eq!(decl_bounds.name(), "bounds:symbol,venue@ts");

        let decl_t2 = SidecarDecl {
            kind: "temporal2".into(),
            columns: vec!["symbol".into()],
            time_columns: vec!["valid".into(), "known".into()],
            params: BTreeMap::new(),
            extra: Extra::new(),
        };
        assert_eq!(decl_t2.name(), "temporal2:symbol@valid,known");
        let decl_sketch = SidecarDecl {
            kind: "sketch".into(),
            columns: vec!["symbol".into(), "symbol+ts".into()],
            params: BTreeMap::from([("p".to_string(), "12".to_string())]),
            ..SidecarDecl::default()
        };
        assert_eq!(decl_sketch.name(), "sketch:symbol,symbol+ts");
        let json = serde_json::to_value(&decl_sketch).unwrap();
        assert_eq!(json["params"]["p"], "12");
        assert!(serde_json::to_value(&decl_t2)
            .unwrap()
            .get("params")
            .is_none());

        let raw = r#"{
          "storage_version": 1,
          "sequence": 3,
          "created_at": "2026-10-23T00:00:00.000Z",
          "committed_at": "2026-10-24T00:00:00.000Z",
          "commit_nonce": 42,
          "source": {"kind": "parquet", "root": "data"},
          "schema": {
            "columns": [
              {"name": "symbol", "data_type": "Utf8", "nullable": false},
              {"name": "ts", "data_type": "Int64", "nullable": false}
            ],
            "key_specs": [{"kind": "exact", "columns": ["symbol"]}],
            "sidecars": [
              {"kind": "bounds", "columns": ["symbol"], "time_columns": ["ts"]},
              {"kind": "counts", "columns": ["symbol"], "time_columns": ["ts"], "future_decl": 9}
            ]
          },
          "rowgroup_space": {"next_id": 1, "files": [], "tombstones": []},
          "key_spec_completeness": {"exact:symbol": 3},
          "segments": [
            {
              "id": "1111111111111111111111111111111111111111111111111111111111111111",
              "key_spec": "exact:symbol",
              "rowgroup_range": {"start": 0, "end": 1},
              "bytes": 10
            },
            {
              "id": "2222222222222222222222222222222222222222222222222222222222222222",
              "kind": "bounds",
              "key_spec": "bounds:symbol@ts",
              "rowgroup_range": {"start": 0, "end": 1},
              "bytes": 20
            },
            {
              "id": "3333333333333333333333333333333333333333333333333333333333333333",
              "kind": "counts",
              "key_spec": "exact:symbol",
              "rowgroup_range": {"start": 0, "end": 1},
              "bytes": 30,
              "future_seg": true
            }
          ],
          "segment_tombstones": [],
          "deleting": []
        }"#;

        let m = Manifest::from_slice(raw.as_bytes()).unwrap();
        assert_eq!(m.unknown_segment_kinds(), vec!["counts".to_string()]);

        // key_spec_segments never yields a segment with kind != None, even when
        // its key_spec equals a declared KeySpec's name ("exact:symbol").
        let ks_ids: Vec<Hash> = m.key_spec_segments("exact:symbol").map(|s| s.id).collect();
        assert_eq!(ks_ids, vec![m.segments[0].id]);

        let bounds_ids: Vec<Hash> = m
            .sidecar_segments("bounds", "bounds:symbol@ts")
            .map(|s| s.id)
            .collect();
        assert_eq!(bounds_ids, vec![m.segments[1].id]);

        let counts_ids: Vec<Hash> = m
            .sidecar_segments("counts", "exact:symbol")
            .map(|s| s.id)
            .collect();
        assert_eq!(counts_ids, vec![m.segments[2].id]);

        // Round-trip preserves both the unknown sidecar declaration and segment
        // byte-for-byte once in canonical form.
        let canonical = m.to_vec().unwrap();
        let m2 = Manifest::from_slice(&canonical).unwrap();
        assert_eq!(m, m2);
        assert_eq!(canonical, m2.to_vec().unwrap());
        let v1: Value = serde_json::from_str(raw).unwrap();
        let v2: Value = serde_json::from_slice(&canonical).unwrap();
        assert_eq!(v1, v2);
    }

    /// Ported from the Argon WP21 implementation: a manifest a newer
    /// writer committed with a segment of a kind this binary does not know
    /// opens, and the kind is reported, never an error.
    #[test]
    fn manifest_listing_unknown_segment_kind_still_opens_and_reports_it() {
        let raw = r#"{
          "storage_version": 1,
          "sequence": 2,
          "created_at": "2026-10-23T00:00:00.000Z",
          "committed_at": "2026-10-24T00:00:00.000Z",
          "commit_nonce": 7,
          "source": {"kind": "parquet", "root": "data"},
          "schema": {
            "columns": [{"name": "symbol", "data_type": "Utf8", "nullable": false}],
            "key_specs": [{"kind": "exact", "columns": ["symbol"]}]
          },
          "rowgroup_space": {"next_id": 1, "files": [], "tombstones": []},
          "key_spec_completeness": {"exact:symbol": 2},
          "segments": [
            {
              "id": "2222222222222222222222222222222222222222222222222222222222222222",
              "key_spec": "future_sidecar:symbol",
              "kind": "future_sidecar:v9",
              "rowgroup_range": {"start": 0, "end": 1},
              "bytes": 10
            }
          ]
        }"#;
        let m = Manifest::from_slice(raw.as_bytes()).unwrap();
        assert_eq!(
            m.unknown_segment_kinds(),
            vec!["future_sidecar:v9".to_string()]
        );
        assert_eq!(m.segments.len(), 1);
    }

    #[test]
    fn paths() {
        assert_eq!(manifest_path(""), "manifest.json");
        assert_eq!(manifest_path("/idx/"), "idx/manifest.json");
        assert_eq!(
            manifest_history_path("idx", 1),
            "idx/manifests/00000000000000000001.json"
        );
        assert_eq!(
            manifest_history_path("", 42),
            "manifests/00000000000000000042.json"
        );
        let id = Hash::of(b"x");
        assert_eq!(
            segment_path("idx", &id),
            format!("idx/segments/{}.seg", id.to_hex())
        );
    }
}
