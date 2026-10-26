//! The manifest commit protocol (ADR 003, ADR 015, ADR 020).
//!
//! Per attempt:
//!
//! 1. Read the manifest and its version token (absent on the first commit).
//! 2. Apply the caller's transformation `f`. `f` may return an error to abort
//!    (for example a builder that sees the winner of a race touched
//!    `rowgroup_space`, where a blind rebase would assign overlapping ids).
//! 3. Stamp the commit-owned fields: `storage_version`, `sequence = base + 1`,
//!    `created_at` (kept from the base), `committed_at`, a fresh random
//!    `commit_nonce`, and the base's GC claims (`deleting`).
//! 4. Refuse, with a claim conflict (see [`claimed_segment`]), any new
//!    manifest that references a segment a GC run has claimed for deletion.
//! 5. Reconcile segment tombstones: a segment the new manifest drops gets one
//!    stamped now; a segment it references loses its tombstone (ADR 013).
//! 6. Make sure every segment the new manifest adds is present. A staged
//!    segment is uploaded on every attempt, unconditionally (content-addressed,
//!    so idempotent); trusting `exists` would assume read-after-delete
//!    consistency. An unstaged (pre-uploaded) segment must `exist`. A
//!    manifest referencing a missing segment is never published.
//! 7. CAS the manifest: `put_if_absent` for the first, `put_if_version` after.
//! 8. If the CAS reports **any** error, re-read once. If the current manifest
//!    carries this attempt's nonce, the write was applied and only the
//!    response was lost (a retried request answered 412, a timeout, a failed
//!    fsync after the rename): report success, so an applied write is never
//!    reported as failed. Otherwise, on [`Error::VersionConflict`], back off
//!    with jitter and go to 1, re-applying `f` to the winner's manifest (a
//!    union for additive segment lists), with bounded attempts; on any other
//!    error, return it.
//!
//! Why the claim check in step 4 makes GC safe: GC CASes a claim before it
//! deletes anything, and only claims ids the winning manifest does not
//! reference. Any writer whose base predates the claim loses its CAS and
//! rebases onto a manifest that shows the claim; any writer whose base
//! includes the claim is refused. So no published manifest ever references a
//! segment a GC run may be deleting. (A GC run that stalls past its claim
//! lease is the residual; see the `gc` module.)

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use bellwether_types::format::STORAGE_VERSION;
use bellwether_types::{Error, Hash, Result, StoreVersion};
use bytes::Bytes;

use crate::backend::StoreExt;
use crate::manifest::{
    manifest_history_path, manifest_path, now_rfc3339, segment_path, FileEntry, Manifest,
    SegmentTombstone, UNSUPPORTED_STORAGE,
};

/// The typed refusal for referencing a claimed segment (contract v2
/// `Error::SegmentClaimed`). It is not a version conflict: `commit` returns it
/// without rebasing, and the caller re-uploads after the claim clears.
fn claim_conflict(id: &Hash) -> Error {
    Error::SegmentClaimed(*id)
}

/// If `err` is the refusal `commit` returns for a manifest that references a
/// segment a GC run has claimed for deletion, the claimed id.
///
/// The caller should retry later and **re-upload** the segment (keep it in
/// the staged set): GC deletes the file before releasing the claim.
#[must_use]
pub fn claimed_segment(err: &Error) -> Option<Hash> {
    match err {
        Error::SegmentClaimed(id) => Some(*id),
        _ => None,
    }
}

/// Segment bytes written during a commit, keyed by their blake3 id.
#[derive(Clone, Debug, Default)]
pub struct StagedSegments {
    by_id: BTreeMap<Hash, Bytes>,
}

impl StagedSegments {
    /// An empty set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Stages segment bytes and returns their content id.
    pub fn stage(&mut self, bytes: Bytes) -> Hash {
        let id = Hash::of(&bytes);
        self.by_id.insert(id, bytes);
        id
    }

    /// Bytes for a staged id.
    #[must_use]
    pub fn get(&self, id: &Hash) -> Option<&Bytes> {
        self.by_id.get(id)
    }

    /// Number of staged segments.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// Whether nothing is staged.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }
}

/// Retry policy for [`commit_with`].
#[derive(Clone, Copy, Debug)]
pub struct CommitOptions {
    /// Total CAS attempts before giving up with the last conflict.
    pub max_attempts: u32,
    /// First backoff; doubles per attempt up to `max_backoff`, then jittered
    /// uniformly in `[0.5, 1.5)`.
    pub base_backoff: Duration,
    /// Backoff ceiling.
    pub max_backoff: Duration,
}

impl Default for CommitOptions {
    fn default() -> Self {
        Self {
            max_attempts: 32,
            base_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(500),
        }
    }
}

/// What a successful commit published.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Committed {
    /// The store's version token of the new manifest; the CAS token.
    pub version: StoreVersion,
    /// The new manifest's `sequence` (ADR 020); the identity callers record.
    pub sequence: u64,
}

/// Reads and parses the manifest under `root`, or `None` if there is none.
pub fn read_manifest<S: StoreExt + ?Sized>(
    store: &S,
    root: &str,
) -> Result<Option<(Manifest, StoreVersion)>> {
    Ok(read_manifest_raw(store, root)?.map(|(m, v, _)| (m, v)))
}

/// Reads and parses the manifest under `root` along with the exact raw bytes
/// stored at `manifest.json`, or `None` if there is none.
pub(crate) fn read_manifest_raw<S: StoreExt + ?Sized>(
    store: &S,
    root: &str,
) -> Result<Option<(Manifest, StoreVersion, Bytes)>> {
    match store.get_opt(&manifest_path(root))? {
        None => Ok(None),
        Some((bytes, version)) => Ok(Some((Manifest::from_slice(&bytes)?, version, bytes))),
    }
}

/// Backfill: before committing on a base at sequence N, if `manifests/N` is
/// absent, writes the exact bytes read from `manifest.json`. History never
/// blocks a commit: an object that exists with different bytes
/// (`manifest.json` was rewritten outside the commit protocol, at the same
/// sequence) keeps the bytes that were committed, and a failed write is
/// retried by the next commit.
fn backfill_history<S: StoreExt + ?Sized>(store: &S, root: &str, sequence: u64, bytes: &Bytes) {
    if let Err(hist_err) = ensure_history_object(store, root, sequence, bytes) {
        tracing::warn!(
            sequence,
            error = %hist_err,
            "manifest history backfill skipped; the committed history object is kept"
        );
    }
}

/// Writes `bytes` to `manifests/<sequence as 20-digit zero-padded decimal>.json`
/// with `put_if_absent`. If the history object already exists with identical
/// bytes, succeeds; if it exists with different bytes, fails with
/// [`Error::Store`] (never overwrites).
pub(crate) fn ensure_history_object<S: StoreExt + ?Sized>(
    store: &S,
    root: &str,
    sequence: u64,
    bytes: &Bytes,
) -> Result<()> {
    if sequence == 0 {
        return Ok(());
    }
    let hpath = manifest_history_path(root, sequence);
    match store.put_if_absent(&hpath, bytes.clone()) {
        Ok(_) => Ok(()),
        Err(Error::VersionConflict { .. }) => match store.get_opt(&hpath)? {
            Some((existing, _)) if existing == *bytes => Ok(()),
            Some(_) => Err(Error::Store(format!(
                "manifest history object {hpath} already exists with different bytes; \
                 refusing to overwrite immutable history"
            ))),
            None => Err(Error::Store(format!(
                "manifest history object {hpath} reported a conflict on creation, \
                 then disappeared; retry the operation"
            ))),
        },
        Err(e) => Err(e),
    }
}

/// Commits with [`CommitOptions::default`]. See [`commit_with`].
pub fn commit<S, F>(store: &S, root: &str, staged: &StagedSegments, f: F) -> Result<Committed>
where
    S: StoreExt + ?Sized,
    F: FnMut(&Manifest) -> Result<Manifest>,
{
    commit_with(store, root, staged, CommitOptions::default(), f)
}

/// Runs the commit protocol described in the module docs.
///
/// `f` maps the current manifest (an empty [`Manifest::new`] with
/// `sequence == 0` on the first commit) to the next one. The commit will get
/// `sequence = base.sequence + 1`, which `f` may record (for example in
/// `key_spec_completeness`). `f` is called once per attempt and re-applied to
/// the winner's manifest after a conflict, so it must be **idempotent** and a
/// function of its input only; return an error to abort instead of rebasing.
///
/// The commit-owned fields listed in the module docs are overwritten, and
/// `deleting` (GC claims) is restored from the base whatever `f` returns.
///
/// Segments the next manifest adds must be in `staged` or already present in
/// the store; otherwise the commit fails before touching the manifest. A
/// large build that pre-uploads its segments with
/// [`bellwether_types::Store::put_content_addressed`] and commits with an
/// empty staged set is supported; if GC claims one of them in between, the
/// commit is refused and the builder must re-upload.
///
/// An applied write is never reported as failed when the manifest can be read
/// back (step 8). If the CAS fails with a non-conflict error **and** the
/// read-back fails too, the outcome is unknown and the CAS error is returned;
/// a later read shows whether `f`'s effect landed.
pub fn commit_with<S, F>(
    store: &S,
    root: &str,
    staged: &StagedSegments,
    opts: CommitOptions,
    f: F,
) -> Result<Committed>
where
    S: StoreExt + ?Sized,
    F: FnMut(&Manifest) -> Result<Manifest>,
{
    commit_inner(store, root, staged, opts, ClaimMode::Preserve, f)
}

/// Whether a commit may change the GC claim set.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClaimMode {
    /// User commits: `deleting` is restored from the base.
    Preserve,
    /// GC commits: `deleting` is whatever `f` returns.
    Manage,
}

/// The storage version a commit writes. Never a downgrade in place (ADR
/// 017): the loader already refused anything newer than this build, so
/// writing ours is an upgrade at most. A version 1 manifest is upgraded
/// only by a commit whose files all carry the version 2 row-group
/// statistics (a build fills them); a commit that cannot (compaction, GC,
/// removing a spec over a version 1 index) keeps it at version 1, written
/// in version 1's keys.
fn committed_storage_version(base: &Manifest, next: &Manifest) -> u32 {
    if base.storage_version >= STORAGE_VERSION
        || next
            .rowgroup_space
            .files
            .iter()
            .all(FileEntry::has_rowgroup_stats)
    {
        STORAGE_VERSION
    } else {
        base.storage_version
    }
}

pub(crate) fn commit_inner<S, F>(
    store: &S,
    root: &str,
    staged: &StagedSegments,
    opts: CommitOptions,
    mode: ClaimMode,
    mut f: F,
) -> Result<Committed>
where
    S: StoreExt + ?Sized,
    F: FnMut(&Manifest) -> Result<Manifest>,
{
    let path = manifest_path(root);
    let mut rng = Rng::seeded();
    let mut last_conflict = None;

    for attempt in 0..opts.max_attempts.max(1) {
        if attempt > 0 {
            std::thread::sleep(rng.backoff(attempt, &opts));
        }
        let base = read_manifest_raw(store, root)?;
        let empty;
        let base_manifest = if let Some((m, _, _)) = &base {
            m
        } else {
            empty = Manifest::new();
            &empty
        };

        let mut next = f(base_manifest)?;
        if next.storage_version > STORAGE_VERSION {
            return Err(Error::UnsupportedVersion {
                what: UNSUPPORTED_STORAGE,
                found: next.storage_version,
                supported: STORAGE_VERSION,
            });
        }
        next.storage_version = committed_storage_version(base_manifest, &next);
        next.sequence = base_manifest.sequence + 1;
        let now = now_rfc3339();
        next.created_at = if base.is_some() {
            base_manifest.created_at.clone()
        } else {
            now.clone()
        };
        next.committed_at = now;
        let nonce = rng.next_u64();
        next.commit_nonce = nonce;
        if mode == ClaimMode::Preserve {
            next.deleting.clone_from(&base_manifest.deleting);
        }
        if let Some(seg) = next.segments.iter().find(|s| next.is_claimed(&s.id)) {
            return Err(claim_conflict(&seg.id));
        }
        reconcile_tombstones(base_manifest, &mut next);

        if let Some((m, _, base_bytes)) = &base {
            backfill_history(store, root, m.sequence, base_bytes);
        }
        ensure_segments_present(store, root, staged, base_manifest, &next)?;

        let body = Bytes::from(next.to_vec()?);
        let res = match &base {
            None => store.put_if_absent(&path, body.clone()),
            Some((_, version, _)) => store.put_if_version(&path, body.clone(), version),
        };
        let err = match res {
            Ok(version) => {
                // The CAS succeeded: record the committed manifest in immutable
                // history. A post-CAS history write failure must not report the
                // commit as failed (the commit already landed); the next commit
                // backfills the missing history object from `manifest.json`, and
                // `Snapshot::open_at` / `retained_sequences` include the current
                // `manifest.json` even while its history object is absent.
                if let Err(hist_err) = ensure_history_object(store, root, next.sequence, &body) {
                    tracing::warn!(
                        sequence = next.sequence,
                        error = %hist_err,
                        "manifest CAS succeeded but writing manifest history failed; the next commit will backfill it"
                    );
                }
                return Ok(Committed {
                    version,
                    sequence: next.sequence,
                });
            }
            Err(e) => e,
        };
        // Any failed CAS may still have been applied: a 412 answering a
        // retried request whose first attempt landed, a timeout after the
        // write, a failed fsync after the rename. Our nonce says so. If the
        // re-read itself fails, the outcome is unknown and the CAS error is
        // returned.
        match read_manifest(store, root) {
            Ok(Some((current, version))) if current.commit_nonce == nonce => {
                tracing::debug!(attempt, error = %err, "manifest CAS reported failure but our write landed");
                if let Err(hist_err) = ensure_history_object(store, root, current.sequence, &body) {
                    tracing::warn!(
                        sequence = current.sequence,
                        error = %hist_err,
                        "manifest CAS landed (detected via nonce) but writing manifest history failed; the next commit will backfill it"
                    );
                }
                return Ok(Committed {
                    version,
                    sequence: current.sequence,
                });
            }
            Ok(_) => {}
            Err(read_err) => {
                tracing::debug!(attempt, error = %err, %read_err, "manifest CAS failed and its outcome cannot be read back");
                return Err(err);
            }
        }
        match err {
            e @ Error::VersionConflict { .. } => {
                tracing::debug!(attempt, error = %e, "manifest CAS lost; rebasing");
                last_conflict = Some(e);
            }
            e => return Err(e),
        }
    }
    Err(last_conflict.unwrap_or_else(|| Error::Store("commit made no attempt".into())))
}

/// Step 6: every segment `next` adds relative to `base` is present before the
/// CAS. Staged bytes are uploaded unconditionally on every attempt: they are
/// content-addressed, so the write is idempotent, and not trusting `exists`
/// removes any read-after-delete assumption on S3-compatible stores. A
/// pre-uploaded (unstaged) segment must `exist`; that check does rely on the
/// store being read-after-delete consistent, which S3, GCS, Azure and local
/// disks are. Either way the segment is not claimed in the base, and a GC
/// claim landing after the base read moves the manifest version, so the CAS
/// would lose.
fn ensure_segments_present<S: StoreExt + ?Sized>(
    store: &S,
    root: &str,
    staged: &StagedSegments,
    base: &Manifest,
    next: &Manifest,
) -> Result<()> {
    let base_ids = base.segment_ids();
    for seg in &next.segments {
        if base_ids.contains(&seg.id) {
            continue;
        }
        let seg_path = segment_path(root, &seg.id);
        match staged.get(&seg.id) {
            Some(bytes) => store.put_content_addressed(&seg_path, bytes.clone())?,
            None if store.exists(&seg_path)? => {}
            None => {
                return Err(Error::Store(format!(
                    "segment {} is referenced by the new manifest but neither staged nor present",
                    seg.id
                )))
            }
        }
    }
    Ok(())
}

/// Tombstones segments `next` dropped relative to `base` and clears
/// tombstones of segments `next` references or that are claimed.
fn reconcile_tombstones(base: &Manifest, next: &mut Manifest) {
    let live = next.segment_ids();
    let claimed: BTreeSet<Hash> = next.deleting.iter().map(|c| c.id).collect();
    next.segment_tombstones
        .retain(|t| !live.contains(&t.id) && !claimed.contains(&t.id));
    let already: BTreeSet<Hash> = next.segment_tombstones.iter().map(|t| t.id).collect();
    for id in base.segment_ids() {
        if !live.contains(&id) && !already.contains(&id) && !claimed.contains(&id) {
            next.segment_tombstones.push(SegmentTombstone::now(id));
        }
    }
}

/// Minimal xorshift generator for jitter and commit nonces. Not
/// cryptographic; nonces only need to differ between attempts.
pub(crate) struct Rng(u64);

impl Rng {
    pub(crate) fn seeded() -> Self {
        use std::hash::{BuildHasher, Hasher};
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u128(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
        );
        Self(h.finish() | 1)
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn next_unit(&mut self) -> f64 {
        #[allow(clippy::cast_precision_loss)]
        let unit = (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
        unit
    }

    fn backoff(&mut self, attempt: u32, opts: &CommitOptions) -> Duration {
        let exp = opts
            .base_backoff
            .saturating_mul(1u32 << attempt.min(16))
            .min(opts.max_backoff);
        exp.mul_f64(0.5 + self.next_unit())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::manifest::{DeleteClaim, Extra, RowGroupRange, SegmentRef};
    use crate::ObjectStoreBackend;
    use bellwether_types::Store;

    fn seg(id: Hash, n: u32) -> SegmentRef {
        SegmentRef {
            id,
            kind: None,
            key_spec: "exact:id".into(),
            rowgroup_range: RowGroupRange::new(n, n + 1),
            bytes: 1,
            right_sequence: None,
            extra: Extra::new(),
        }
    }

    fn add(staged: &mut StagedSegments, body: &str, n: u32) -> SegmentRef {
        seg(staged.stage(Bytes::from(body.to_string())), n)
    }

    fn adding(s: &SegmentRef) -> impl FnMut(&Manifest) -> Result<Manifest> + '_ {
        move |m| {
            let mut m = m.clone();
            m.add_segment(s.clone());
            Ok(m)
        }
    }

    /// The memory backend uses `object_store`'s real `PutMode::Update` path,
    /// the same mapping the cloud backends use.
    #[test]
    fn memory_backend_cas_and_rebase() {
        let store = ObjectStoreBackend::memory().unwrap();
        cas_and_rebase(&store);
    }

    #[test]
    fn local_backend_cas_and_rebase() {
        let dir = tempfile::tempdir().unwrap();
        let store = ObjectStoreBackend::local(dir.path()).unwrap();
        cas_and_rebase(&store);
    }

    #[allow(clippy::many_single_char_names)]
    fn cas_and_rebase(store: &ObjectStoreBackend) {
        let root = "idx";
        let mut staged = StagedSegments::new();
        let a = add(&mut staged, "segment a", 0);
        let c1 = commit(store, root, &staged, adding(&a)).unwrap();
        assert_eq!(c1.sequence, 1);

        // put_if_absent on an existing manifest conflicts.
        let err = store
            .put_if_absent(&manifest_path(root), Bytes::from_static(b"{}"))
            .unwrap_err();
        assert!(matches!(err, Error::VersionConflict { .. }), "{err:?}");

        let b = add(&mut staged, "segment b", 1);
        let c2 = commit(store, root, &staged, adding(&b)).unwrap();
        assert_ne!(c1.version, c2.version);
        assert_eq!(c2.sequence, 2);
        let stale =
            store.put_if_version(&manifest_path(root), Bytes::from_static(b"{}"), &c1.version);
        assert!(matches!(stale, Err(Error::VersionConflict { .. })));

        // The loser's commit protocol rebases: its first CAS is forced to
        // lose by committing c from inside the first call to f.
        let c = add(&mut staged, "segment c", 2);
        let d = add(&mut staged, "segment d", 3);
        let mut calls = 0;
        let c4 = commit(store, root, &staged, |m| {
            calls += 1;
            if calls == 1 {
                commit(store, root, &staged, adding(&c)).unwrap();
            }
            let mut m = m.clone();
            m.add_segment(d.clone());
            Ok(m)
        })
        .unwrap();
        assert_eq!(calls, 2, "loser must rebase exactly once");
        assert_eq!(c4.sequence, 4, "sequence is +1 per successful commit");
        let (m, _) = read_manifest(store, root).unwrap().unwrap();
        assert_eq!(m.sequence, 4);
        let ids = m.segment_ids();
        for s in [&a, &b, &c, &d] {
            assert!(ids.contains(&s.id), "union lost a segment");
            assert!(store.exists(&segment_path(root, &s.id)).unwrap());
        }
    }

    #[test]
    fn created_at_is_first_creation_and_committed_at_advances() {
        let store = ObjectStoreBackend::memory().unwrap();
        let staged = StagedSegments::new();
        commit(&store, "", &staged, |m| Ok(m.clone())).unwrap();
        let (m1, _) = read_manifest(&store, "").unwrap().unwrap();
        std::thread::sleep(Duration::from_millis(5));
        commit(&store, "", &staged, |m| Ok(m.clone())).unwrap();
        let (m2, _) = read_manifest(&store, "").unwrap().unwrap();
        assert_eq!(m1.created_at, m2.created_at);
        assert!(m2.committed_at > m1.committed_at);
        assert_ne!(m1.commit_nonce, m2.commit_nonce);
    }

    #[test]
    fn f_can_abort_a_rebase() {
        let store = ObjectStoreBackend::memory().unwrap();
        let staged = StagedSegments::new();
        commit(&store, "", &staged, |m| Ok(m.clone())).unwrap();
        let mut calls = 0;
        let err = commit(&store, "", &staged, |m| {
            calls += 1;
            if calls == 1 {
                // A concurrent build assigns row-group ids.
                commit(&store, "", &staged, |w| {
                    let mut w = w.clone();
                    w.rowgroup_space.next_id = 10;
                    Ok(w)
                })
                .unwrap();
                let mut m = m.clone();
                m.rowgroup_space.next_id = 5;
                return Ok(m);
            }
            if m.rowgroup_space.next_id != 0 {
                return Err(Error::Other("winner touched rowgroup_space".into()));
            }
            Ok(m.clone())
        })
        .unwrap_err();
        assert!(matches!(err, Error::Other(_)));
        let (m, _) = read_manifest(&store, "").unwrap().unwrap();
        assert_eq!(m.rowgroup_space.next_id, 10);
    }

    #[test]
    fn claims_are_preserved_and_refuse_references() {
        let store = ObjectStoreBackend::memory().unwrap();
        let mut staged = StagedSegments::new();
        let a = add(&mut staged, "a", 0);
        commit_inner(
            &store,
            "",
            &staged,
            CommitOptions::default(),
            ClaimMode::Manage,
            |m| {
                let mut m = m.clone();
                m.deleting.push(DeleteClaim {
                    id: a.id,
                    claimed_at: now_rfc3339(),
                    token: 7,
                    lease_ms: 3_600_000,
                    extra: Extra::new(),
                });
                Ok(m)
            },
        )
        .unwrap();
        // A user commit cannot drop the claim...
        commit(&store, "", &staged, |m| {
            let mut m = m.clone();
            m.deleting.clear();
            Ok(m)
        })
        .unwrap();
        let (m, _) = read_manifest(&store, "").unwrap().unwrap();
        assert!(m.is_claimed(&a.id));
        // ...nor reference the claimed id.
        let err = commit(&store, "", &staged, adding(&a)).unwrap_err();
        assert_eq!(claimed_segment(&err), Some(a.id));
        assert!(claimed_segment(&Error::VersionConflict {
            expected: "x".into(),
            found: "y".into()
        })
        .is_none());
    }

    #[test]
    fn refuses_unstaged_missing_segment_and_tombstones_drops() {
        let store = ObjectStoreBackend::memory().unwrap();
        let ghost = seg(Hash::of(b"never written"), 0);
        let err = commit(&store, "", &StagedSegments::new(), adding(&ghost)).unwrap_err();
        assert!(matches!(err, Error::Store(_)));
        assert!(read_manifest(&store, "").unwrap().is_none());

        let mut staged = StagedSegments::new();
        let a = add(&mut staged, "a", 0);
        commit(&store, "", &staged, adding(&a)).unwrap();
        commit(&store, "", &staged, |m| {
            let mut m = m.clone();
            m.segments.clear();
            Ok(m)
        })
        .unwrap();
        let (m, _) = read_manifest(&store, "").unwrap().unwrap();
        assert_eq!(m.segment_tombstones.len(), 1);
        assert_eq!(m.rowgroup_space.tombstones.len(), 0);
        // Re-referencing clears the tombstone.
        commit(&store, "", &staged, adding(&a)).unwrap();
        let (m, _) = read_manifest(&store, "").unwrap().unwrap();
        assert_eq!(m.segment_tombstones.len(), 0);
    }

    /// What the next conditional update of a [`Faulty`] store does wrong.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Fault {
        None,
        /// Applied, then reported as a conflict (a retried request whose
        /// first attempt landed gets 412).
        AppliedThenConflict,
        /// Applied, then reported as a non-conflict error (a timeout after
        /// the write, a failed fsync after the rename).
        AppliedThenError,
        /// Not applied, reported as a non-conflict error.
        ErrorNotApplied,
        /// CAS succeeds, then the post-CAS history `put_if_absent` fails.
        HistoryWriteFailed,
    }

    struct Faulty {
        inner: ObjectStoreBackend,
        fault: std::sync::Mutex<Fault>,
        /// `exists` always answers true, like a stale HEAD after a DELETE on
        /// a store without read-after-delete consistency.
        stale_exists: bool,
    }

    impl Faulty {
        fn new(fault: Fault, stale_exists: bool) -> Self {
            Self {
                inner: ObjectStoreBackend::memory().unwrap(),
                fault: std::sync::Mutex::new(fault),
                stale_exists,
            }
        }
        fn arm(&self, fault: Fault) {
            *self.fault.lock().unwrap() = fault;
        }
    }

    impl Store for Faulty {
        fn get(&self, p: &str) -> Result<(Bytes, StoreVersion)> {
            self.inner.get(p)
        }
        fn put_if_absent(&self, p: &str, b: Bytes) -> Result<StoreVersion> {
            if p.contains("manifests/") && !self.inner.exists(p).unwrap_or(false) {
                let mut guard = self.fault.lock().unwrap();
                if *guard == Fault::HistoryWriteFailed {
                    *guard = Fault::None;
                    return Err(Error::Store("history write failed".into()));
                }
            }
            self.inner.put_if_absent(p, b)
        }
        fn put_if_version(&self, p: &str, b: Bytes, e: &StoreVersion) -> Result<StoreVersion> {
            let mut guard = self.fault.lock().unwrap();
            let fault = if *guard == Fault::HistoryWriteFailed {
                Fault::None
            } else {
                std::mem::replace(&mut *guard, Fault::None)
            };
            drop(guard);
            if fault == Fault::ErrorNotApplied {
                return Err(Error::Store("timed out".into()));
            }
            let v = self.inner.put_if_version(p, b, e)?;
            match fault {
                Fault::AppliedThenConflict => Err(Error::VersionConflict {
                    expected: e.0.clone(),
                    found: v.0,
                }),
                Fault::AppliedThenError => Err(Error::Store("timed out".into())),
                Fault::None | Fault::ErrorNotApplied | Fault::HistoryWriteFailed => Ok(v),
            }
        }
        fn put_content_addressed(&self, p: &str, b: Bytes) -> Result<()> {
            self.inner.put_content_addressed(p, b)
        }
        fn list(&self, p: &str) -> Result<Vec<String>> {
            self.inner.list(p)
        }
        fn delete(&self, p: &str) -> Result<()> {
            self.inner.delete(p)
        }
    }

    impl StoreExt for Faulty {
        fn get_opt(&self, p: &str) -> Result<Option<(Bytes, StoreVersion)>> {
            self.inner.get_opt(p)
        }
        fn exists(&self, p: &str) -> Result<bool> {
            if self.stale_exists {
                return Ok(true);
            }
            self.inner.exists(p)
        }
        fn get_range(&self, p: &str, r: std::ops::Range<u64>) -> Result<Option<Bytes>> {
            self.inner.get_range(p, r)
        }
        fn list_pages(
            &self,
            p: &str,
            n: usize,
            f: &mut dyn FnMut(&[crate::ObjectMeta]) -> Result<std::ops::ControlFlow<()>>,
        ) -> Result<()> {
            self.inner.list_pages(p, n, f)
        }
    }

    fn bump_next_id(calls: &mut u32) -> impl FnMut(&Manifest) -> Result<Manifest> + '_ {
        move |m| {
            *calls += 1;
            let mut m = m.clone();
            m.rowgroup_space.next_id += 1;
            Ok(m)
        }
    }

    #[test]
    fn applied_write_reported_as_conflict_is_a_self_win() {
        let store = Faulty::new(Fault::None, false);
        let staged = StagedSegments::new();
        commit(&store, "", &staged, |m| Ok(m.clone())).unwrap();
        store.arm(Fault::AppliedThenConflict);
        let mut calls = 0;
        let c = commit(&store, "", &staged, bump_next_id(&mut calls)).unwrap();
        assert_eq!(calls, 1, "f must not be re-applied after a self-win");
        assert_eq!(c.sequence, 2);
        let (m, v) = read_manifest(&store, "").unwrap().unwrap();
        assert_eq!(m.rowgroup_space.next_id, 1);
        assert_eq!(c.version, v);
        assert_eq!(
            store.get(&manifest_history_path("", 2)).unwrap().0,
            store.get(&manifest_path("")).unwrap().0
        );
    }

    /// Requirement for apply-then-attest: a write that landed is never
    /// reported as failed, whatever error the CAS returned.
    #[test]
    fn applied_write_reported_as_error_is_a_success() {
        let store = Faulty::new(Fault::None, false);
        let staged = StagedSegments::new();
        commit(&store, "", &staged, |m| Ok(m.clone())).unwrap();
        store.arm(Fault::AppliedThenError);
        let mut calls = 0;
        let c = commit(&store, "", &staged, bump_next_id(&mut calls)).unwrap();
        assert_eq!(calls, 1);
        assert_eq!(c.sequence, 2);
        let (m, v) = read_manifest(&store, "").unwrap().unwrap();
        assert_eq!((m.sequence, m.rowgroup_space.next_id), (2, 1));
        assert_eq!(c.version, v);
        assert_eq!(
            store.get(&manifest_history_path("", 2)).unwrap().0,
            store.get(&manifest_path("")).unwrap().0
        );
    }

    #[test]
    fn unapplied_write_reported_as_error_is_an_error() {
        let store = Faulty::new(Fault::None, false);
        let staged = StagedSegments::new();
        commit(&store, "", &staged, |m| Ok(m.clone())).unwrap();
        store.arm(Fault::ErrorNotApplied);
        let mut calls = 0;
        let err = commit(&store, "", &staged, bump_next_id(&mut calls)).unwrap_err();
        assert!(matches!(err, Error::Store(_)), "{err:?}");
        assert_eq!(calls, 1, "a non-conflict error is not retried");
        let (m, _) = read_manifest(&store, "").unwrap().unwrap();
        assert_eq!((m.sequence, m.rowgroup_space.next_id), (1, 0));
        assert!(!store.exists(&manifest_history_path("", 2)).unwrap());
    }

    #[test]
    fn history_write_failure_after_cas_does_not_fail_commit_and_next_commit_backfills() {
        let store = Faulty::new(Fault::None, false);
        let staged = StagedSegments::new();
        commit(&store, "", &staged, |m| Ok(m.clone())).unwrap();
        store.arm(Fault::HistoryWriteFailed);
        let mut calls = 0;
        let c2 = commit(&store, "", &staged, bump_next_id(&mut calls)).unwrap();
        assert_eq!(c2.sequence, 2);
        assert_eq!(calls, 1);
        assert!(!store.exists(&manifest_history_path("", 2)).unwrap());
        let (m2_bytes, _) = store.get(&manifest_path("")).unwrap();

        // Next commit backfills sequence 2 byte-identically before committing sequence 3.
        let c3 = commit(&store, "", &staged, bump_next_id(&mut calls)).unwrap();
        assert_eq!(c3.sequence, 3);
        assert_eq!(
            store.get(&manifest_history_path("", 2)).unwrap().0,
            m2_bytes
        );
        assert!(store.exists(&manifest_history_path("", 3)).unwrap());
    }

    /// Staged bytes are uploaded even when `exists` claims the file is there
    /// (a stale HEAD after a DELETE), so a manifest never references a
    /// segment that is actually missing.
    #[test]
    fn staged_segments_are_uploaded_without_trusting_exists() {
        let store = Faulty::new(Fault::None, true);
        let mut staged = StagedSegments::new();
        let a = add(&mut staged, "segment a", 0);
        commit(&store, "", &staged, adding(&a)).unwrap();
        assert!(store.inner.exists(&segment_path("", &a.id)).unwrap());
    }

    #[test]
    fn concurrent_writers_union() {
        let dir = tempfile::tempdir().unwrap();
        let store = ObjectStoreBackend::local(dir.path()).unwrap();
        let threads = 8;
        let per = 5;
        std::thread::scope(|s| {
            for t in 0..threads {
                let store = &store;
                s.spawn(move || {
                    for i in 0..per {
                        let mut staged = StagedSegments::new();
                        let sref = add(&mut staged, &format!("seg {t} {i}"), t * 100 + i);
                        commit(store, "r", &staged, adding(&sref)).unwrap();
                    }
                });
            }
        });
        let (m, _) = read_manifest(&store, "r").unwrap().unwrap();
        assert_eq!(m.segments.len(), (threads * per) as usize);
        assert_eq!(m.sequence, u64::from(threads * per));
    }
}
