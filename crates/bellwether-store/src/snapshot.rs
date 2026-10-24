//! [`Snapshot`]: a reader pinned to one manifest version (invariant 4).
//!
//! A snapshot reads the manifest once, at open, and never again. Every
//! segment read goes through it and is restricted to the segments that
//! manifest references. Segments are immutable and content-addressed, and GC
//! retains unreferenced segments for a retention window (ADR 013), so a
//! commit in progress, or one that lands while the snapshot is open, never
//! changes what the snapshot observes. The view advances only when the
//! reader calls [`Snapshot::reopen`].

use std::sync::Arc;

use bellwether_types::{Error, Hash, Result, StoreVersion};
use bytes::Bytes;

use crate::backend::StoreExt;
use crate::commit::read_manifest;
use crate::manifest::{segment_path, Manifest, SegmentRef};

/// A read handle pinned to one manifest version.
#[derive(Clone)]
pub struct Snapshot<S: StoreExt + ?Sized> {
    store: Arc<S>,
    root: String,
    manifest: Arc<Manifest>,
    version: StoreVersion,
}

impl<S: StoreExt + ?Sized> std::fmt::Debug for Snapshot<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot")
            .field("root", &self.root)
            .field("version", &self.version)
            .field("segments", &self.manifest.segments.len())
            .finish_non_exhaustive()
    }
}

impl<S: StoreExt + ?Sized> Snapshot<S> {
    /// Opens a snapshot of the current manifest under `root`. Fails if there
    /// is no manifest yet, or with [`Error::UnsupportedVersion`] if it is
    /// newer than this build.
    pub fn open(store: Arc<S>, root: &str) -> Result<Self> {
        let (manifest, version) = read_manifest(&*store, root)?
            .ok_or_else(|| Error::Store(format!("no manifest under {root:?}")))?;
        Ok(Self {
            store,
            root: root.to_string(),
            manifest: Arc::new(manifest),
            version,
        })
    }

    /// Opens a fresh snapshot of whatever manifest is current now. The
    /// existing snapshot is unaffected.
    pub fn reopen(&self) -> Result<Self> {
        Self::open(Arc::clone(&self.store), &self.root)
    }

    /// The pinned manifest.
    #[must_use]
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// The pinned manifest's version token.
    #[must_use]
    pub fn version(&self) -> &StoreVersion {
        &self.version
    }

    /// The pinned manifest's `sequence` (ADR 020), the identity to record in
    /// provenance and receipts.
    #[must_use]
    pub fn sequence(&self) -> u64 {
        self.manifest.sequence
    }

    /// The index root this snapshot reads.
    #[must_use]
    pub fn root(&self) -> &str {
        &self.root
    }

    /// Segments of the pinned manifest.
    #[must_use]
    pub fn segments(&self) -> &[SegmentRef] {
        &self.manifest.segments
    }

    /// Reads the byte range `range` of a segment of the pinned manifest, for
    /// block-index and bitmap reads that must not load a whole segment
    /// (ADR 018). Refuses ids not in the pinned manifest. Unlike
    /// [`Self::read_segment`], a partial read cannot be checked against the
    /// content id.
    pub fn read_segment_range(&self, id: &Hash, range: std::ops::Range<u64>) -> Result<Bytes> {
        if !self.manifest.references(id) {
            return Err(Error::Store(format!(
                "segment {id} is not in the pinned manifest version"
            )));
        }
        self.store
            .get_range(&segment_path(&self.root, id), range)?
            .ok_or_else(|| Error::Store(format!("segment {id} missing from store")))
    }

    /// Reads a segment of the pinned manifest and verifies its bytes against
    /// its content id. A segment not referenced by the pinned manifest is
    /// refused even if it exists in the store: readers see one version.
    pub fn read_segment(&self, id: &Hash) -> Result<Bytes> {
        if !self.manifest.references(id) {
            return Err(Error::Store(format!(
                "segment {id} is not in the pinned manifest version"
            )));
        }
        let path = segment_path(&self.root, id);
        let (bytes, _) = self
            .store
            .get_opt(&path)?
            .ok_or_else(|| Error::Store(format!("segment {id} missing from store")))?;
        if Hash::of(&bytes) != *id {
            return Err(Error::Store(format!(
                "segment {id} content does not match its id"
            )));
        }
        Ok(bytes)
    }
}
