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
/// Directory of immutable segments under an index root.
pub const SEGMENTS_DIR: &str = "segments";
/// Directory of the rebuildable cache under an index root.
pub const CACHE_DIR: &str = "cache";
/// Directory of archived manifest snapshots by sequence under an index root.
pub const MANIFESTS_DIR: &str = "manifests";

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
/// Store path of the archived manifest for `sequence` under `root`:
/// `manifests/<sequence>.json`.
#[must_use]
pub fn manifest_seq_path(root: &str, sequence: u64) -> String {
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
    /// Optional `--path-template` pattern for filename-derived virtual columns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_template: Option<String>,
    /// Unknown fields (ADR 017).
    #[serde(flatten)]
    pub extra: Extra,
}

/// One column of the indexed dataset.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    /// Column name.
    pub name: String,
    /// Arrow data type, as its `Display` form.
    #[serde(default)]
    pub data_type: String,
    /// Whether the column is nullable.
    #[serde(default)]
    pub nullable: bool,
    /// Unknown fields (ADR 017).
    #[serde(flatten)]
    pub extra: Extra,
}

/// Dataset schema and declared key specs.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SchemaInfo {
    /// Columns in schema order.
    #[serde(default)]
    pub columns: Vec<ColumnDef>,
    /// Declared key specs.
    #[serde(default)]
    pub key_specs: Vec<KeySpec>,
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
    /// Unknown fields (ADR 017).
    #[serde(flatten)]
    pub extra: Extra,
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
    /// Name of the key spec the segment indexes ([`KeySpec::name`]).
    pub key_spec: String,
    /// Row groups covered.
    pub rowgroup_range: RowGroupRange,
    /// Size of the segment in bytes.
    pub bytes: u64,
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
    /// docs).
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
        if !types_owned_fields_known(&value) {
            return Err(Error::UnknownKind {
                what: UNSUPPORTED_SHAPE,
                found: "unrecognized key-spec kind or fingerprint field".to_string(),
            });
        }
        Ok(serde_json::from_value(value)?)
    }

    /// Serializes to the canonical on-disk form (sorted keys, pretty).
    pub fn to_vec(&self) -> Result<Vec<u8>> {
        // Round trip through `Value` so flattened extras and known fields
        // come out in one sorted key order.
        let v = serde_json::to_value(self)?;
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
    /// Segment kinds (`key_spec` prefixes before `:`) referenced in `segments`
    /// that this build does not know. Storage version 1 allows additive
    /// sidecar segment kinds; readers that do not know a segment kind ignore
    /// those segments and can still report the unknown kinds here.
    #[must_use]
    pub fn unknown_segment_kinds(&self) -> Vec<String> {
        let mut out = Vec::new();
        for seg in &self.segments {
            let kind = seg
                .key_spec
                .split_once(':')
                .map_or(seg.key_spec.as_str(), |(k, _)| k);
            if key_spec_fields(kind).is_none() && !out.iter().any(|x| x == kind) {
                out.push(kind.to_string());
            }
        }
        out
    }
}

/// `what` of the [`Error::UnsupportedVersion`] for a newer `storage_version`.
pub const UNSUPPORTED_STORAGE: &str = "storage";
/// `what` of the [`Error::UnsupportedVersion`] for an unknown `KeySpec` kind
/// or an unknown field in a `KeySpec` or `FileFingerprint` (ADR 017
/// amendment: such a change requires a `storage_version` bump).
pub const UNSUPPORTED_SHAPE: &str = "key-spec or fingerprint shape in storage";

/// Serialized field names of each `KeySpec` kind this build understands.
/// Kept in step with `bellwether-types` by a drift test.
fn key_spec_fields(kind: &str) -> Option<&'static [&'static str]> {
    match kind {
        "exact" => Some(&["kind", "columns"]),
        "bloom" => Some(&["kind", "columns", "fpr"]),
        "range" => Some(&["kind", "column"]),
        "temporal" | "bounds" => Some(&["kind", "columns", "ts_column"]),
        "temporal2" => Some(&["kind", "columns", "valid_column", "known_column"]),
        _ => None,
    }
}

/// Serialized field names of `FileFingerprint` this build understands.
/// Kept in step with `bellwether-types` by a drift test.
const FINGERPRINT_FIELDS: &[&str] = &["path", "size", "etag", "tail_blake3"];

/// Whether every types-owned struct in the manifest has only kinds and
/// fields this build understands (see the module docs).
fn types_owned_fields_known(manifest: &Value) -> bool {
    fn keys_within(v: &Value, allowed: &[&str]) -> bool {
        v.as_object()
            .is_some_and(|o| o.keys().all(|k| allowed.contains(&k.as_str())))
    }
    let key_specs = manifest
        .pointer("/schema/key_specs")
        .and_then(Value::as_array);
    for spec in key_specs.into_iter().flatten() {
        let Some(allowed) = spec
            .get("kind")
            .and_then(Value::as_str)
            .and_then(key_spec_fields)
        else {
            return false;
        };
        if !keys_within(spec, allowed) {
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
        let v2 = r#"{"storage_version": 2, "created_at": "x", "segments": "a different shape"}"#;
        match Manifest::from_slice(v2.as_bytes()) {
            Err(Error::UnsupportedVersion {
                what: UNSUPPORTED_STORAGE,
                found: 2,
                supported: 1,
            }) => {}
            other => panic!("expected UnsupportedVersion, got {other:?}"),
        }
    }

    /// The allow-lists the loader checks against must match what
    /// `bellwether-types` actually serializes. Adding a `KeySpec` variant or
    /// a `FileFingerprint` field breaks compilation here (exhaustive match,
    /// full struct literal) or fails this test, so the allow-list and the
    /// `storage_version` bump it implies (ADR 017) cannot be forgotten.
    #[test]
    fn allow_lists_match_types_serialization() {
        let specs = [
            KeySpec::Exact {
                columns: vec!["a".into()],
            },
            KeySpec::Bloom {
                columns: vec!["a".into()],
                fpr: 0.01,
            },
            KeySpec::Range { column: "a".into() },
            KeySpec::Temporal {
                columns: vec!["a".into()],
                ts_column: "t".into(),
            },
            KeySpec::Bounds {
                columns: vec!["a".into()],
                ts_column: "t".into(),
            },
            KeySpec::Temporal2 {
                columns: vec!["a".into()],
                valid_column: "v".into(),
                known_column: "k".into(),
            },
        ];
        let mut kinds_seen = BTreeSet::new();
        for spec in &specs {
            // Exhaustive on purpose: a new variant must be added to `specs`.
            match spec {
                KeySpec::Exact { .. }
                | KeySpec::Bloom { .. }
                | KeySpec::Range { .. }
                | KeySpec::Temporal { .. }
                | KeySpec::Bounds { .. }
                | KeySpec::Temporal2 { .. } => {}
            }
            let v = serde_json::to_value(spec).unwrap();
            let kind = v["kind"].as_str().unwrap().to_string();
            let mut keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
            keys.sort_unstable();
            let mut allowed = key_spec_fields(&kind)
                .unwrap_or_else(|| panic!("no allow-list for kind {kind}"))
                .to_vec();
            allowed.sort_unstable();
            assert_eq!(keys, allowed, "KeySpec kind {kind} drifted");
            kinds_seen.insert(kind);
        }
        assert_eq!(kinds_seen.len(), specs.len());

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

    #[test]
    fn manifest_listing_unknown_segment_kind_still_opens_and_reports_it() {
        let json = r#"{
            "storage_version": 1,
            "sequence": 1,
            "created_at": "2026-10-25T00:00:00.000Z",
            "committed_at": "2026-10-25T00:00:00.000Z",
            "schema": {"key_specs": [{"kind": "exact", "columns": ["symbol"]}]},
            "segments": [
                {
                    "id": "1111111111111111111111111111111111111111111111111111111111111111",
                    "key_spec": "exact:symbol",
                    "rowgroup_range": {"start": 0, "end": 1},
                    "bytes": 100
                },
                {
                    "id": "2222222222222222222222222222222222222222222222222222222222222222",
                    "key_spec": "future_sidecar:symbol",
                    "rowgroup_range": {"start": 0, "end": 1},
                    "bytes": 64
                }
            ]
        }"#;
        let m = Manifest::from_slice(json.as_bytes()).unwrap();
        assert_eq!(m.segments.len(), 2);
        assert_eq!(m.unknown_segment_kinds(), vec!["future_sidecar".to_string()]);
    }

    #[test]
    fn paths() {
        assert_eq!(manifest_path(""), "manifest.json");
        assert_eq!(manifest_path("/idx/"), "idx/manifest.json");
        let id = Hash::of(b"x");
        assert_eq!(
            segment_path("idx", &id),
            format!("idx/segments/{}.seg", id.to_hex())
        );
    }
}
