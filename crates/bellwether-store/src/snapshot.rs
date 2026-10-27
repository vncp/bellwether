//! [`Snapshot`]: a reader pinned to one manifest version (invariant 4).
//!
//! A snapshot reads the manifest once, at open, and never again. Every
//! segment read goes through it and is restricted to the segments that
//! manifest references. Segments are immutable and content-addressed, and GC
//! retains unreferenced segments for a retention window (ADR 013), so a
//! commit in progress, or one that lands while the snapshot is open, never
//! changes what the snapshot observes. The view advances only when the
//! reader calls [`Snapshot::reopen`].

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError};

use bellwether_types::{Error, Hash, Result, StoreVersion};
use bytes::Bytes;

use crate::backend::{StoreExt, DEFAULT_PAGE_SIZE};
use crate::commit::{read_manifest, read_manifest_raw};
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

/// Bytes [`Snapshot::read_segment_range`] reads ahead and keeps: segment
/// ranges are fetched in aligned windows of this size.
pub const SEGMENT_WINDOW_BYTES: u64 = 64 * 1024;

/// Cap on the segment windows every snapshot of this process keeps, in
/// total. The cache never holds more, whatever the size of the index or
/// the number of snapshots open at once (ADR 018).
pub const SEGMENT_CACHE_BYTES: usize = 32 * 1024 * 1024;

/// A read of more windows than this is not cached: it is fetched exactly.
const MAX_CACHED_READ_WINDOWS: u64 = 16;

/// A window of one snapshot's reads: the snapshot's tag, the segment, and
/// the window's index in it.
type WindowKey = (u64, Hash, u64);

/// The windows of segment bytes the open snapshots have read, oldest
/// evicted first, bounded by [`SEGMENT_CACHE_BYTES`] across all of them.
/// Each window is tagged with the snapshot that read it and served only to
/// that snapshot and its clones; segments are immutable and
/// content-addressed, so a window never goes stale while its snapshot is
/// open.
#[derive(Default)]
struct Windows {
    map: HashMap<WindowKey, Bytes>,
    order: VecDeque<WindowKey>,
    bytes: usize,
}

impl Windows {
    fn insert(&mut self, key: WindowKey, window: Bytes) {
        if window.len() > SEGMENT_CACHE_BYTES || self.map.contains_key(&key) {
            return;
        }
        self.bytes += window.len();
        self.map.insert(key, window);
        self.order.push_back(key);
        while self.bytes > SEGMENT_CACHE_BYTES {
            let Some(old) = self.order.pop_front() else {
                break;
            };
            if let Some(w) = self.map.remove(&old) {
                self.bytes -= w.len();
            }
        }
    }

    /// Drops every window `owner` read.
    fn forget(&mut self, owner: u64) {
        let bytes = &mut self.bytes;
        self.map.retain(|k, w| {
            let keep = k.0 != owner;
            if !keep {
                *bytes -= w.len();
            }
            keep
        });
        self.order.retain(|k| k.0 != owner);
    }
}

/// The process-wide window cache.
static WINDOWS: LazyLock<Mutex<Windows>> = LazyLock::new(Mutex::default);

/// The next snapshot's tag in [`WINDOWS`].
static NEXT_OWNER: AtomicU64 = AtomicU64::new(0);

fn windows() -> MutexGuard<'static, Windows> {
    // A poisoned lock only means another thread panicked mid-insert; every
    // entry is still a whole window of an immutable segment.
    WINDOWS.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A snapshot's tag in [`WINDOWS`], shared by its clones; dropping the
/// last of them drops the windows it read.
struct WindowOwner(u64);

impl Drop for WindowOwner {
    fn drop(&mut self) {
        windows().forget(self.0);
    }
}

/// Bytes the process-wide segment window cache holds now (at most
/// [`SEGMENT_CACHE_BYTES`]).
#[must_use]
pub fn segment_cache_bytes() -> usize {
    windows().bytes
}

/// A read handle pinned to one manifest version.
#[derive(Clone)]
pub struct Snapshot<S: StoreExt + ?Sized> {
    store: Arc<S>,
    root: String,
    manifest: Arc<Manifest>,
    version: StoreVersion,
    pinned_sequence: Option<u64>,
    content_hash: Hash,
    /// Each referenced segment's size, from the manifest.
    sizes: Arc<HashMap<Hash, u64>>,
    /// This snapshot's (and its clones') windows in [`WINDOWS`].
    windows: Arc<WindowOwner>,
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
        let (manifest, version, bytes) = read_manifest_raw(&*store, root)?
            .ok_or_else(|| Error::Store(format!("no manifest under {root:?}")))?;
        Ok(Self::pinned(store, root, manifest, version, None, &bytes))
    }

    fn pinned(
        store: Arc<S>,
        root: &str,
        manifest: Manifest,
        version: StoreVersion,
        pinned_sequence: Option<u64>,
        bytes: &[u8],
    ) -> Self {
        let sizes = manifest.segments.iter().map(|s| (s.id, s.bytes)).collect();
        Self {
            store,
            root: root.to_string(),
            manifest: Arc::new(manifest),
            version,
            pinned_sequence,
            content_hash: Hash::of(bytes),
            sizes: Arc::new(sizes),
            windows: Arc::new(WindowOwner(NEXT_OWNER.fetch_add(1, Ordering::Relaxed))),
        }
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
            return Ok(Some(Self::pinned(
                store,
                root,
                manifest,
                version,
                Some(sequence),
                &bytes,
            )));
        }
        if let Some((current, version, bytes)) = read_manifest_raw(&*store, root)? {
            if current.sequence == sequence {
                return Ok(Some(Self::pinned(
                    store,
                    root,
                    current,
                    version,
                    Some(sequence),
                    &bytes,
                )));
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

    /// BLAKE3 of the pinned manifest's bytes as stored: its content
    /// identity. Two manifests at one sequence (an index deleted and
    /// rebuilt, or rewritten outside the commit protocol) have different
    /// content hashes, so a reader that pinned another index's manifest
    /// tells them apart where the sequence alone cannot (a join key's
    /// right dataset, B3). The history copy of a commit holds the bytes
    /// `manifest.json` held, so both give one hash.
    #[must_use]
    pub fn content_hash(&self) -> Hash {
        self.content_hash
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
    /// content id; the segment format checks its records' own sums.
    ///
    /// Reads go to the store in aligned windows of
    /// [`SEGMENT_WINDOW_BYTES`] (clipped to the segment's size in the
    /// manifest), kept in a cache this process's snapshots share up to
    /// [`SEGMENT_CACHE_BYTES`] in total, so a plan probing many keys of
    /// one segment makes one store round trip per window instead of one
    /// per value. A snapshot (with its clones) reads only the windows it
    /// fetched itself, and they leave the cache when it is dropped: a new
    /// snapshot reads the store afresh. A read wider than a few windows is
    /// fetched exactly and not kept.
    pub fn read_segment_range(&self, id: &Hash, range: std::ops::Range<u64>) -> Result<Bytes> {
        let Some(&size) = self.sizes.get(id) else {
            return Err(Error::Store(format!(
                "segment {id} is not in the pinned manifest version"
            )));
        };
        let path = || segment_path(&self.root, id);
        let missing = || Error::Store(format!("segment {id} missing from store"));
        let (first, last) = (
            range.start / SEGMENT_WINDOW_BYTES,
            range.end.saturating_sub(1) / SEGMENT_WINDOW_BYTES,
        );
        if range.end <= range.start || range.end > size || last - first >= MAX_CACHED_READ_WINDOWS {
            return self.store.get_range(&path(), range)?.ok_or_else(missing);
        }
        let offset = |at: u64| usize::try_from(at % SEGMENT_WINDOW_BYTES).unwrap_or(usize::MAX);
        let end_in_last = usize::try_from(range.end - last * SEGMENT_WINDOW_BYTES)
            .map_err(|_| Error::Store("segment range".into()))?;
        let owner = self.owner();
        if first == last {
            if let Some(w) = windows().map.get(&(owner, *id, first)) {
                return Ok(w.slice(offset(range.start)..end_in_last));
            }
        }
        let have: Vec<Option<Bytes>> = {
            let w = windows();
            (first..=last)
                .map(|i| w.map.get(&(owner, *id, i)).cloned())
                .collect()
        };
        let mut parts: Vec<Bytes> = Vec::with_capacity(have.len());
        if have.iter().all(Option::is_some) {
            parts.extend(have.into_iter().flatten());
        } else {
            // One request for every window from the first missing to the
            // last missing one.
            let lo = first + have.iter().position(Option::is_none).unwrap_or(0) as u64;
            let hi = first + have.iter().rposition(Option::is_none).unwrap_or(0) as u64;
            let start = lo * SEGMENT_WINDOW_BYTES;
            let end = ((hi + 1) * SEGMENT_WINDOW_BYTES).min(size);
            let fetched = self
                .store
                .get_range(&path(), start..end)?
                .ok_or_else(missing)?;
            if fetched.len() as u64 != end - start {
                return Err(Error::Store(format!(
                    "segment {id}: asked for {} bytes at {start}, got {}; the segment is shorter \
                     than the manifest says",
                    end - start,
                    fetched.len()
                )));
            }
            let mut w = windows();
            for (i, had) in (first..=last).zip(have) {
                if let Some(b) = had {
                    parts.push(b);
                    continue;
                }
                let at = usize::try_from((i - lo) * SEGMENT_WINDOW_BYTES)
                    .map_err(|_| Error::Store("segment window offset".into()))?;
                let len =
                    usize::try_from(SEGMENT_WINDOW_BYTES.min(size - i * SEGMENT_WINDOW_BYTES))
                        .map_err(|_| Error::Store("segment window length".into()))?;
                // A window owns its bytes: a slice would keep the whole
                // response (or a backend's whole object) alive past what
                // the cache counts.
                let b = Bytes::copy_from_slice(&fetched[at..at + len]);
                w.insert((owner, *id, i), b.clone());
                parts.push(b);
            }
        }
        if parts.len() == 1 {
            return Ok(parts[0].slice(offset(range.start)..end_in_last));
        }
        let mut out = Vec::with_capacity(
            usize::try_from(range.end - range.start)
                .map_err(|_| Error::Store("segment range".into()))?,
        );
        let n = parts.len();
        for (k, b) in parts.iter().enumerate() {
            let from = if k == 0 { offset(range.start) } else { 0 };
            let to = if k + 1 == n { end_in_last } else { b.len() };
            out.extend_from_slice(&b[from..to]);
        }
        Ok(Bytes::from(out))
    }

    /// This snapshot's tag in the process-wide window cache.
    fn owner(&self) -> u64 {
        self.windows.0
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
