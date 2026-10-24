//! Storage for Bellwether indexes: the [`bellwether_types::Store`] contract
//! over `object_store` with each backend's conditional PUT, the manifest and
//! its commit protocol, snapshot-isolated readers, a content-addressed cache,
//! and garbage collection.
//!
//! Layout under an index root (docs/format.md):
//!
//! ```text
//! <root>/manifest.json            committed by conditional PUT
//! <root>/manifests/<20-digit>.json immutable committed manifest history
//! <root>/segments/<blake3>.seg    immutable, content-addressed
//! <root>/cache/<hash>.bin         rebuildable, with <hash>.meta.json age sidecar
//! ```
//!
//! Invariant 4 (readers see one manifest version) is held by [`Snapshot`] and
//! tested in `tests/snapshot_isolation.rs`. See the crate README for the
//! commit protocol and the per-backend conditional-write semantics.

#![forbid(unsafe_code)]
#![cfg_attr(test, allow(clippy::unwrap_used))]

mod backend;
mod cache;
mod commit;
mod gc;
mod local;
mod manifest;
mod snapshot;

pub use backend::{
    parse_store_url, s3_builder, BackendKind, ObjectStoreBackend, StoreExt, DEFAULT_PAGE_SIZE,
};
pub use cache::Cache;
pub use commit::{
    claimed_segment, commit, commit_with, read_manifest, CommitOptions, Committed, StagedSegments,
};
pub use gc::{
    gc, gc_with, GcOptions, GcReport, DEFAULT_CLAIM_LEASE, DEFAULT_DELETE_BATCH,
    DEFAULT_MAX_ORPHANS,
};
pub use local::LOCK_TIMEOUT as LOCAL_LOCK_TIMEOUT;
pub use manifest::{
    join, manifest_history_path, manifest_path, now_rfc3339, parse_rfc3339, segment_path,
    ChunkRange, ColumnDef, DeleteClaim, Extra, FileEntry, Manifest, RowGroupRange, RowGroupSpace,
    RowGroupTombstone, SchemaInfo, SegmentRef, SegmentTombstone, SidecarDecl, SourceInfo,
    CACHE_DIR, KNOWN_SEGMENT_KINDS, MANIFESTS_DIR, MANIFEST_FILE, SEGMENTS_DIR, UNSUPPORTED_SHAPE,
    UNSUPPORTED_STORAGE,
};
pub use object_store::ObjectMeta;
pub use snapshot::{retained_sequences, Snapshot};
