//! [`Snapshot`]: a reader pinned to one manifest version (invariant 4).
//!
//! A snapshot reads the manifest once, at open, and never again. Every
//! segment read goes through it and is restricted to the segments that
//! manifest references. Segments are immutable and content-addressed, and GC
//! retains unreferenced segments for a retention window (ADR 013), so a
//! commit in progress, or one that lands while the snapshot is open, never
//! changes what the snapshot observes. The view advances only when the
//! reader calls [`Snapshot::reopen`].

use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::sync::Arc;

use bellwether_types::{Error, Hash, Result, StoreVersion};
use bytes::Bytes;

use crate::backend::{StoreExt, DEFAULT_PAGE_SIZE};
use crate::commit::read_manifest;
use crate::manifest::{
    join, manifest_history_path, segment_path, Manifest, SegmentRef, MANIFESTS_DIR,
};

/// Parses `.../manifests/<20-digit zero-padded decimal>.json` into its sequence.
/// Ignores staging, lock, or any other non-matching filenames.
pub(crate) fn history_sequence_of(path: &str) -> Option<u64> {
    let name = path.rsplit('/').next()?;
    let digits = name.strip_suffix(".json")?;
    if digits.len() != 20 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let seq = digits.parse::<u64>().ok()?;
    (seq > 0).then_some(seq)
}

/// Ascending manifest sequences currently openable with [`Snapshot::open_at`]:
/// every retained history manifest under `<root>/manifests/` plus the current
/// `<root>/manifest.json` (if present).
pub fn retained_sequences<S: StoreExt + ?Sized>(store: &S, root: &str) -> Result<Vec<u64>> {
    let mut seqs = BTreeSet::new();
    if let Some((current, _)) = read_manifest(store, root)? {
        if current.sequence > 0 {
            seqs.insert(current.sequence);
        }
    }
    store.list_pages(&join(root, MANIFESTS_DIR), DEFAULT_PAGE_SIZE, &mut |page| {
        for meta in page {
            if let Some(seq) = history_sequence_of(meta.location.as_ref()) {
                seqs.insert(seq);
            }
        }
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(seqs.into_iter().collect())
}

/// A read handle pinned to one manifest version.
#[derive(Clone)]
pub struct Snapshot<S: StoreExt + ?Sized> {
    store: Arc<S>,
    root: String,
    manifest: Arc<Manifest>,
    version: StoreVersion,
    pinned_sequence: Option<u64>,
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
            pinned_sequence: None,
        })
    }

    /// Opens a snapshot of the committed manifest at `sequence` under `root`.
    ///
    /// Reads `<root>/manifests/<sequence as 20-digit zero-padded decimal>.json`,
    /// falling back to `<root>/manifest.json` when `sequence` is the current
    /// manifest and the history object has not been backfilled yet. Returns
    /// `Ok(None)` when `sequence` is not retained (never committed or already
    /// pruned by GC). Fails if the stored manifest's `sequence` does not match
    /// the requested `sequence`.
    pub fn open_at(store: Arc<S>, root: &str, sequence: u64) -> Result<Option<Self>> {
        if sequence == 0 {
            return Ok(None);
        }
        let hpath = manifest_history_path(root, sequence);
        if let Some((bytes, version)) = store.get_opt(&hpath)? {
            let manifest = Manifest::from_slice(&bytes)?;
            if manifest.sequence != sequence {
                return Err(Error::Store(format!(
                    "manifest history object {hpath} has sequence {}, expected {sequence}",
                    manifest.sequence
                )));
            }
            return Ok(Some(Self {
                store,
                root: root.to_string(),
                manifest: Arc::new(manifest),
                version,
                pinned_sequence: Some(sequence),
            }));
        }
        if let Some((current, version)) = read_manifest(&*store, root)? {
            if current.sequence == sequence {
                return Ok(Some(Self {
                    store,
                    root: root.to_string(),
                    manifest: Arc::new(current),
                    version,
                    pinned_sequence: Some(sequence),
                }));
            }
        }
        Ok(None)
    }

    /// Opens a fresh snapshot: for an ordinary snapshot opened with
    /// [`Self::open`], whatever manifest is current now; for a historical
    /// snapshot opened with [`Self::open_at`], the same pinned `sequence`.
    /// The existing snapshot is unaffected.
    pub fn reopen(&self) -> Result<Self> {
        if let Some(seq) = self.pinned_sequence {
            return Self::open_at(Arc::clone(&self.store), &self.root, seq)?.ok_or_else(|| {
                Error::Store(format!(
                    "historical manifest sequence {seq} under {:?} is no longer retained",
                    self.root
                ))
            });
        }
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
