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
use crate::commit::{read_manifest, read_manifest_at};
use crate::manifest::{manifest_seq_path, segment_path, Manifest, SegmentRef, MANIFESTS_DIR};

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

    /// Opens a snapshot pinned to manifest `sequence` under `root`. Also
    /// verifies that every segment referenced by that manifest is still
    /// present in the store (GC retention permitting); otherwise returns an
    /// error naming the oldest available sequence.
    pub fn open_at(store: Arc<S>, root: &str, sequence: u64) -> Result<(Self, Bytes)> {
        let Some((current, _)) = read_manifest(&*store, root)? else {
            return Err(Error::KeySpec(format!(
                "no index manifest under {root:?}; run `bellwether index` first"
            )));
        };
        if sequence == 0 || sequence > current.sequence {
            let oldest = oldest_available_sequence(&*store, root, &current)?;
            return Err(Error::KeySpec(format!(
                "manifest sequence {sequence} does not exist (available sequences: {oldest}..={})",
                current.sequence
            )));
        }
        let Some((manifest, bytes, version)) = read_manifest_at(&*store, root, sequence)? else {
            let oldest = oldest_available_sequence(&*store, root, &current)?;
            return Err(Error::KeySpec(format!(
                "manifest sequence {sequence} is no longer retained (oldest available sequence is \
                 {oldest}, current is {}); re-run with --at-manifest {oldest} or newer",
                current.sequence
            )));
        };
        if !all_segments_exist(&*store, root, &manifest)? {
            let oldest = oldest_available_sequence(&*store, root, &current)?;
            return Err(Error::KeySpec(format!(
                "manifest sequence {sequence} has segments reclaimed by garbage collection \
                 (oldest available complete snapshot is sequence {oldest}, current is {}); \
                 re-run with --at-manifest {oldest} or newer",
                current.sequence
            )));
        }
        Ok((
            Self {
                store,
                root: root.to_string(),
                manifest: Arc::new(manifest),
                version,
            },
            bytes,
        ))
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

fn all_segments_exist<S: StoreExt + ?Sized>(
    store: &S,
    root: &str,
    manifest: &Manifest,
) -> Result<bool> {
    for seg in &manifest.segments {
        if !store.exists(&segment_path(root, &seg.id))? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Oldest manifest sequence under `root` whose manifest and all referenced
/// segments still exist in `store`.
pub fn oldest_available_sequence<S: StoreExt + ?Sized>(
    store: &S,
    root: &str,
    current: &Manifest,
) -> Result<u64> {
    let prefix = crate::manifest::join(root, MANIFESTS_DIR);
    let paths = store.list(&prefix).unwrap_or_default();
    let mut seqs: Vec<u64> = paths
        .iter()
        .filter_map(|p| {
            let name = p.rsplit('/').next()?;
            let stem = name.strip_suffix(".json")?;
            stem.parse::<u64>().ok()
        })
        .collect();
    seqs.push(current.sequence);
    seqs.sort_unstable();
    seqs.dedup();
    for seq in seqs {
        if seq == current.sequence {
            return Ok(current.sequence);
        }
        if let Ok(Some((m, _, _))) = read_manifest_at(store, root, seq) {
            if all_segments_exist(store, root, &m).unwrap_or(false) {
                return Ok(seq);
            }
        }
    }
    let _ = manifest_seq_path;
    Ok(current.sequence)
}
