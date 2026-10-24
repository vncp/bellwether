//! Garbage collection (ADR 013): retention, claim-then-delete with leases,
//! tokens and self-fencing, orphans, and the reviewers' reproduced races
//! (S1, S2, S3, S6, S7) as regression tests.

#![allow(
    clippy::unwrap_used,
    clippy::assert_is_empty,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bellwether_store::{
    claimed_segment, commit, gc, gc_with, manifest_path, read_manifest, segment_path, DeleteClaim,
    Extra, GcOptions, Manifest, ObjectMeta, ObjectStoreBackend, RowGroupRange, SegmentRef,
    Snapshot, StagedSegments, StoreExt,
};
use bellwether_types::{Error, Hash, Result, Store, StoreVersion};
use bytes::Bytes;

fn seg(staged: &mut StagedSegments, body: &str, n: u32) -> SegmentRef {
    SegmentRef {
        id: staged.stage(Bytes::from(body.to_string())),
        key_spec: "exact:id".into(),
        rowgroup_range: RowGroupRange::new(n, n + 1),
        bytes: body.len() as u64,
        extra: Extra::new(),
    }
}

fn adding(s: &SegmentRef) -> impl FnMut(&Manifest) -> Result<Manifest> + '_ {
    move |m| {
        let mut m = m.clone();
        m.add_segment(s.clone());
        Ok(m)
    }
}

fn dropping(s: &SegmentRef) -> impl FnMut(&Manifest) -> Result<Manifest> + '_ {
    move |m| {
        let mut m = m.clone();
        m.segments.retain(|x| x.id != s.id);
        Ok(m)
    }
}

type Hook = Box<dyn FnOnce(&ObjectStoreBackend) + Send>;
type DeleteHook = Box<dyn FnOnce(&ObjectStoreBackend, &str) + Send>;

/// Wraps a store and runs one-shot hooks at chosen points inside GC.
struct Hooked {
    inner: ObjectStoreBackend,
    /// Before the first conditional manifest update: after GC has read its
    /// manifest, before its claim CAS.
    before_cas: Mutex<Option<Hook>>,
    /// On the first conditional manifest update: apply it, run the hook (an
    /// interloper commit), then report a conflict, so the nonce check cannot
    /// recognise the applied write.
    cas_applied_then_conflict: Mutex<Option<Hook>>,
    /// Before the first delete: after the claim CAS and the fence check. The
    /// hook gets the path being deleted.
    before_delete: Mutex<Option<DeleteHook>>,
    /// The first delete fails instead of deleting.
    fail_first_delete: Mutex<bool>,
    /// Before the first listing: after the deletes.
    before_list: Mutex<Option<Hook>>,
}

impl Hooked {
    fn new(inner: ObjectStoreBackend) -> Self {
        Self {
            inner,
            before_cas: Mutex::new(None),
            cas_applied_then_conflict: Mutex::new(None),
            before_delete: Mutex::new(None),
            fail_first_delete: Mutex::new(false),
            before_list: Mutex::new(None),
        }
    }
}

impl Store for Hooked {
    fn get(&self, p: &str) -> Result<(Bytes, StoreVersion)> {
        self.inner.get(p)
    }
    fn put_if_absent(&self, p: &str, b: Bytes) -> Result<StoreVersion> {
        self.inner.put_if_absent(p, b)
    }
    fn put_if_version(&self, p: &str, b: Bytes, e: &StoreVersion) -> Result<StoreVersion> {
        let hook = self.before_cas.lock().unwrap().take();
        if let Some(h) = hook {
            h(&self.inner);
        }
        let interloper = self.cas_applied_then_conflict.lock().unwrap().take();
        if let Some(h) = interloper {
            let v = self.inner.put_if_version(p, b, e)?;
            h(&self.inner);
            return Err(Error::VersionConflict {
                expected: e.0.clone(),
                found: v.0,
            });
        }
        self.inner.put_if_version(p, b, e)
    }
    fn put_content_addressed(&self, p: &str, b: Bytes) -> Result<()> {
        self.inner.put_content_addressed(p, b)
    }
    fn list(&self, p: &str) -> Result<Vec<String>> {
        self.inner.list(p)
    }
    fn delete(&self, p: &str) -> Result<()> {
        let hook = self.before_delete.lock().unwrap().take();
        if let Some(h) = hook {
            h(&self.inner, p);
        }
        if std::mem::take(&mut *self.fail_first_delete.lock().unwrap()) {
            return Err(Error::Store("delete timed out".into()));
        }
        self.inner.delete(p)
    }
}

impl StoreExt for Hooked {
    fn get_opt(&self, p: &str) -> Result<Option<(Bytes, StoreVersion)>> {
        self.inner.get_opt(p)
    }
    fn exists(&self, p: &str) -> Result<bool> {
        self.inner.exists(p)
    }
    fn get_range(&self, p: &str, r: std::ops::Range<u64>) -> Result<Option<Bytes>> {
        self.inner.get_range(p, r)
    }
    fn list_pages(
        &self,
        p: &str,
        n: usize,
        f: &mut dyn FnMut(&[ObjectMeta]) -> Result<ControlFlow<()>>,
    ) -> Result<()> {
        let hook = self.before_list.lock().unwrap().take();
        if let Some(h) = hook {
            h(&self.inner);
        }
        self.inner.list_pages(p, n, f)
    }
}

/// Commits X, then drops it so it is tombstoned.
fn tombstoned_x(store: &ObjectStoreBackend) -> (SegmentRef, StagedSegments) {
    let mut staged = StagedSegments::new();
    let x = seg(&mut staged, "deterministic segment bytes", 0);
    commit(store, "idx", &staged, adding(&x)).unwrap();
    commit(store, "idx", &staged, dropping(&x)).unwrap();
    (x, staged)
}

/// Commits X and Y, then drops both so both are tombstoned.
fn tombstoned_xy(store: &ObjectStoreBackend) -> (SegmentRef, SegmentRef, StagedSegments) {
    let mut staged = StagedSegments::new();
    let x = seg(&mut staged, "segment x", 0);
    let y = seg(&mut staged, "segment y", 1);
    commit(store, "idx", &staged, |m: &Manifest| {
        let mut m = m.clone();
        m.add_segment(x.clone());
        m.add_segment(y.clone());
        Ok(m)
    })
    .unwrap();
    commit(store, "idx", &staged, |m: &Manifest| {
        let mut m = m.clone();
        m.segments.clear();
        Ok(m)
    })
    .unwrap();
    (x, y, staged)
}

/// The invariant every test ends on: every segment the current manifest
/// references exists and reads back with the right content, and no
/// referenced segment is claimed.
fn assert_every_referenced_segment_readable(store: &ObjectStoreBackend) {
    let snap = Snapshot::open(Arc::new(store.clone()), "idx").unwrap();
    for s in snap.segments() {
        assert!(!snap.manifest().is_claimed(&s.id));
        snap.read_segment(&s.id)
            .unwrap_or_else(|e| panic!("referenced segment {} unreadable: {e}", s.id));
    }
}

fn claims(store: &ObjectStoreBackend) -> Vec<DeleteClaim> {
    read_manifest(store, "idx").unwrap().unwrap().0.deleting
}

fn id_of_path(path: &str) -> Hash {
    let name = path.rsplit('/').next().unwrap();
    Hash::from_hex(name.strip_suffix(".seg").unwrap()).unwrap()
}

/// S1, the first reproduced race: a writer re-adds identical bytes for a
/// tombstoned, expired segment after GC has read its manifest, before its
/// claim CAS. GC must not claim the re-referenced id.
#[test]
fn s1_writer_re_adds_between_gc_read_and_gc_cas() {
    let inner = ObjectStoreBackend::memory().unwrap();
    let (x, staged) = tombstoned_x(&inner);
    let hooked = Hooked::new(inner.clone());
    let xw = x.clone();
    *hooked.before_cas.lock().unwrap() = Some(Box::new(move |s: &ObjectStoreBackend| {
        commit(s, "idx", &staged, adding(&xw)).unwrap();
    }));

    let report = gc(&hooked, "idx", Duration::ZERO).unwrap();
    assert_eq!(report.deleted, 0, "GC must not claim a re-referenced id");
    let (m, _) = read_manifest(&inner, "idx").unwrap().unwrap();
    assert!(m.references(&x.id));
    assert!(m.deleting.is_empty());
    assert!(inner.exists(&segment_path("idx", &x.id)).unwrap());
    assert_every_referenced_segment_readable(&inner);
}

/// S2: a writer re-adds X after GC's claim CAS, before its first delete. It
/// is refused with the typed claim conflict, and succeeds by re-uploading
/// once GC has released.
#[test]
fn s2_writer_re_adds_between_claim_and_first_delete() {
    let inner = ObjectStoreBackend::memory().unwrap();
    let (x, staged) = tombstoned_x(&inner);
    let hooked = Hooked::new(inner.clone());
    let xw = x.clone();
    let st = staged.clone();
    let refused = Arc::new(Mutex::new(None));
    let refused_w = Arc::clone(&refused);
    *hooked.before_delete.lock().unwrap() = Some(Box::new(move |s: &ObjectStoreBackend, _| {
        let err = commit(s, "idx", &st, adding(&xw)).unwrap_err();
        *refused_w.lock().unwrap() = claimed_segment(&err);
    }));

    let report = gc(&hooked, "idx", Duration::ZERO).unwrap();
    assert_eq!(report.deleted, 1);
    assert_eq!(*refused.lock().unwrap(), Some(x.id));
    assert!(!inner.exists(&segment_path("idx", &x.id)).unwrap());
    assert!(claims(&inner).is_empty(), "claims released");

    commit(&inner, "idx", &staged, adding(&x)).unwrap();
    assert!(inner.exists(&segment_path("idx", &x.id)).unwrap());
    assert_every_referenced_segment_readable(&inner);
}

/// S3: the same writer arriving after the deletes, before the release.
#[test]
fn s3_writer_re_adds_after_delete_before_release() {
    let inner = ObjectStoreBackend::memory().unwrap();
    let (x, staged) = tombstoned_x(&inner);
    let hooked = Hooked::new(inner.clone());
    let xw = x.clone();
    let st = staged.clone();
    let refused = Arc::new(Mutex::new(None));
    let refused_w = Arc::clone(&refused);
    *hooked.before_list.lock().unwrap() = Some(Box::new(move |s: &ObjectStoreBackend| {
        let err = commit(s, "idx", &st, adding(&xw)).unwrap_err();
        *refused_w.lock().unwrap() = claimed_segment(&err);
    }));

    let report = gc(&hooked, "idx", Duration::ZERO).unwrap();
    assert_eq!(report.deleted, 1);
    assert_eq!(*refused.lock().unwrap(), Some(x.id));
    assert!(claims(&inner).is_empty(), "claims released");
    commit(&inner, "idx", &staged, adding(&x)).unwrap();
    assert_every_referenced_segment_readable(&inner);
}

/// S6, the concurrent-GC race at retention 0. GC1 has claimed X and is about
/// to delete it; GC2 runs inside that window. GC2 must not take GC1's claim
/// over (the lease, not retention, decides staleness), so the writer that
/// re-adds X is refused and GC1's delete cannot hit a referenced segment.
#[test]
fn s6_concurrent_gcs_retention_zero() {
    let inner = ObjectStoreBackend::memory().unwrap();
    let (x, staged) = tombstoned_x(&inner);
    let hooked = Hooked::new(inner.clone());
    let xw = x.clone();
    let st = staged.clone();
    let seen = Arc::new(Mutex::new(None));
    let seen_w = Arc::clone(&seen);
    *hooked.before_delete.lock().unwrap() = Some(Box::new(move |s: &ObjectStoreBackend, _| {
        let r2 = gc(s, "idx", Duration::ZERO).unwrap();
        let w = commit(s, "idx", &st, adding(&xw));
        *seen_w.lock().unwrap() = Some((r2, w.err().and_then(|e| claimed_segment(&e))));
    }));

    let r1 = gc(&hooked, "idx", Duration::ZERO).unwrap();
    let (r2, refused) = seen.lock().unwrap().take().unwrap();
    assert_eq!(r2.stale_claims_taken_over, 0, "GC2 took over a live claim");
    assert_eq!(r2.deleted, 0);
    assert_eq!(
        refused,
        Some(x.id),
        "writer must be refused while GC1 holds X"
    );
    assert_eq!(r1.deleted, 1);
    assert!(!read_manifest(&inner, "idx")
        .unwrap()
        .unwrap()
        .0
        .references(&x.id));
    assert!(claims(&inner).is_empty());
    assert_every_referenced_segment_readable(&inner);

    commit(&inner, "idx", &staged, adding(&x)).unwrap();
    assert_every_referenced_segment_readable(&inner);
}

/// S7: as S6 with retention 50 ms, GC1 merely slow: it stalls 60 ms between
/// claim and delete, longer than retention. Retention no longer makes GC1's
/// claim stale.
#[test]
fn s7_concurrent_gcs_gc1_stalls_past_retention() {
    let inner = ObjectStoreBackend::memory().unwrap();
    let (x, staged) = tombstoned_x(&inner);
    std::thread::sleep(Duration::from_millis(60)); // tombstone older than retention
    let retention = Duration::from_millis(50);
    let hooked = Hooked::new(inner.clone());
    let xw = x.clone();
    let st = staged.clone();
    let seen = Arc::new(Mutex::new(None));
    let seen_w = Arc::clone(&seen);
    *hooked.before_delete.lock().unwrap() = Some(Box::new(move |s: &ObjectStoreBackend, _| {
        std::thread::sleep(Duration::from_millis(60)); // GC1 stalls past retention
        let r2 = gc(s, "idx", retention).unwrap();
        let w = commit(s, "idx", &st, adding(&xw));
        *seen_w.lock().unwrap() = Some((r2, w.err().and_then(|e| claimed_segment(&e))));
    }));

    let r1 = gc(&hooked, "idx", retention).unwrap();
    let (r2, refused) = seen.lock().unwrap().take().unwrap();
    assert_eq!(r2.stale_claims_taken_over, 0, "GC2 took over a live claim");
    assert_eq!(refused, Some(x.id));
    assert_eq!(r1.deleted, 1);
    assert!(!r1.fenced);
    assert!(claims(&inner).is_empty());
    assert_every_referenced_segment_readable(&inner);
    commit(&inner, "idx", &staged, adding(&x)).unwrap();
    assert_every_referenced_segment_readable(&inner);
}

/// The self-fence: a run that has used half its lease stops deleting,
/// releases its claims, and restores the original tombstone of what it did
/// not delete, so a later run deletes it.
#[test]
fn fence_stops_a_run_at_half_its_lease() {
    let inner = ObjectStoreBackend::memory().unwrap();
    let (x, y, _) = tombstoned_xy(&inner);
    let before = read_manifest(&inner, "idx").unwrap().unwrap().0;
    let hooked = Hooked::new(inner.clone());
    // The first delete stalls past half of a 1 s lease; the claim phase has
    // 500 ms of headroom before the fence, so a slow runner cannot fence early.
    *hooked.before_delete.lock().unwrap() = Some(Box::new(|_: &ObjectStoreBackend, _| {
        std::thread::sleep(Duration::from_millis(600));
    }));
    let opts = GcOptions {
        claim_lease: Duration::from_secs(1),
        ..GcOptions::default()
    };

    let r = gc_with(&hooked, "idx", Duration::ZERO, opts).unwrap();
    assert!(r.fenced);
    assert_eq!((r.deleted, r.released_undeleted), (1, 1));
    let (m, _) = read_manifest(&inner, "idx").unwrap().unwrap();
    assert!(m.deleting.is_empty(), "a fenced run still releases");
    let survivor = [&x, &y]
        .into_iter()
        .find(|s| inner.exists(&segment_path("idx", &s.id)).unwrap())
        .unwrap();
    let restored = m
        .segment_tombstones
        .iter()
        .find(|t| t.id == survivor.id)
        .unwrap();
    let original = before
        .segment_tombstones
        .iter()
        .find(|t| t.id == survivor.id)
        .unwrap();
    assert_eq!(
        restored.since, original.since,
        "original tombstone restored"
    );

    let r = gc(&inner, "idx", Duration::ZERO).unwrap();
    assert_eq!(r.deleted, 1);
    assert!(!inner.exists(&segment_path("idx", &survivor.id)).unwrap());
}

/// Back-dates every claim to 2000, as a taker whose clock runs far ahead of
/// the holder's would see them.
fn backdate_claims(store: &ObjectStoreBackend) {
    let (mut m, v) = read_manifest(store, "idx").unwrap().unwrap();
    for c in &mut m.deleting {
        c.claimed_at = "2000-01-01T00:00:00.000Z".into();
    }
    m.sequence += 1;
    store
        .put_if_version(&manifest_path("idx"), Bytes::from(m.to_vec().unwrap()), &v)
        .unwrap();
}

/// Mixed leases: GC2 runs with a zero lease inside GC1's claim-to-delete
/// window. The lease GC1 recorded in its claim (one hour) still protects
/// it, so GC2 takes nothing over and the writer re-adding X is refused.
/// Without the recorded lease, GC2 would take the claim over, release it,
/// the writer would re-add X, and GC1's delete would hit a referenced
/// segment.
#[test]
fn short_lease_run_cannot_take_over_a_live_claim() {
    let inner = ObjectStoreBackend::memory().unwrap();
    let (x, staged) = tombstoned_x(&inner);
    let hooked = Hooked::new(inner.clone());
    let xw = x.clone();
    let st = staged.clone();
    let seen = Arc::new(Mutex::new(None));
    let seen_w = Arc::clone(&seen);
    *hooked.before_delete.lock().unwrap() = Some(Box::new(move |s: &ObjectStoreBackend, _| {
        let short = GcOptions {
            claim_lease: Duration::ZERO,
            ..GcOptions::default()
        };
        let r2 = gc_with(s, "idx", Duration::ZERO, short).unwrap();
        let w = commit(s, "idx", &st, adding(&xw));
        *seen_w.lock().unwrap() = Some((r2, w.err().and_then(|e| claimed_segment(&e))));
    }));

    let r1 = gc(&hooked, "idx", Duration::ZERO).unwrap();
    let (r2, refused) = seen.lock().unwrap().take().unwrap();
    assert_eq!(
        r2.stale_claims_taken_over, 0,
        "short-lease run stole a live claim"
    );
    assert_eq!(refused, Some(x.id));
    assert_eq!(r1.deleted, 1);
    assert!(!read_manifest(&inner, "idx")
        .unwrap()
        .unwrap()
        .0
        .references(&x.id));
    assert!(claims(&inner).is_empty());
    assert_every_referenced_segment_readable(&inner);
    commit(&inner, "idx", &staged, adding(&x)).unwrap();
    assert_every_referenced_segment_readable(&inner);
}

/// GC1 claims X and Y. While it deletes the first, it stalls for `stall`,
/// its claims start to look ancient to GC2 (back-dated: a taker whose clock
/// runs far ahead, or a holder whose host slept), GC2 takes them over,
/// deletes and releases them, and a writer re-adds the second id. GC1 must
/// skip the second id: it re-reads the manifest and finds the claim gone.
fn lost_claims_scenario(opts: GcOptions, stall: Duration) {
    let inner = ObjectStoreBackend::memory().unwrap();
    let (x, y, staged) = tombstoned_xy(&inner);
    let hooked = Hooked::new(inner.clone());
    let (xw, yw, st) = (x.clone(), y.clone(), staged.clone());
    let other = Arc::new(Mutex::new(None));
    let other_w = Arc::clone(&other);
    *hooked.before_delete.lock().unwrap() = Some(Box::new(move |s: &ObjectStoreBackend, path| {
        std::thread::sleep(stall);
        backdate_claims(s);
        let r2 = gc(s, "idx", Duration::ZERO).unwrap();
        assert_eq!((r2.stale_claims_taken_over, r2.deleted), (2, 2));
        let first = id_of_path(path);
        let second = if first == xw.id { &yw } else { &xw };
        commit(s, "idx", &st, adding(second)).unwrap();
        *other_w.lock().unwrap() = Some(second.id);
    }));

    let r1 = gc_with(&hooked, "idx", Duration::ZERO, opts).unwrap();
    assert!(!r1.fenced, "the fence must not be what saves this run");
    assert_eq!((r1.deleted, r1.claims_lost), (1, 1));
    let second = other.lock().unwrap().take().unwrap();
    let (m, _) = read_manifest(&inner, "idx").unwrap().unwrap();
    assert!(m.references(&second));
    assert!(inner.exists(&segment_path("idx", &second)).unwrap());
    assert!(m.deleting.is_empty());
    assert_every_referenced_segment_readable(&inner);
    // The id GC1 deleted while GC2 had restored its tombstone is cleaned up
    // by the next run without error.
    gc(&inner, "idx", Duration::ZERO).unwrap();
    assert_every_referenced_segment_readable(&inner);
}

/// The batch-boundary re-read, with `delete_batch: 1`: the run re-checks
/// its claims before every delete, so even an instantaneous takeover (no
/// time passes on the holder's clocks, as with a taker whose clock runs a
/// lease or more ahead) is caught.
#[test]
fn claims_lost_mid_run_are_not_deleted() {
    let opts = GcOptions {
        delete_batch: 1,
        ..GcOptions::default()
    };
    lost_claims_scenario(opts, Duration::ZERO);
}

/// The wall-clock re-read, at the default batch size: the holder stalls past
/// an eighth of its lease mid-batch (as a host that slept would), so before
/// its next delete it re-reads the manifest even though the batch is not
/// finished, and skips the id it lost. Lease 4 s: re-read after 500 ms,
/// fence at 2 s, stall 700 ms.
#[test]
fn wall_clock_reread_within_a_batch_skips_lost_claims() {
    let opts = GcOptions {
        claim_lease: Duration::from_secs(4),
        ..GcOptions::default()
    };
    assert!(opts.delete_batch > 2, "both ids must be in one batch");
    lost_claims_scenario(opts, Duration::from_millis(700));
}

/// A claim commit that landed but was reported as a conflict, with an
/// interloper commit on top so the nonce check cannot recognise it. Because
/// claims carry the run's token, the retry recognises them as its own; they
/// are deleted and released, not left blocked for a whole lease.
#[test]
fn applied_claim_commit_reported_as_conflict_is_still_released() {
    let inner = ObjectStoreBackend::memory().unwrap();
    let (x, _) = tombstoned_x(&inner);
    let mut staged = StagedSegments::new();
    let z = seg(&mut staged, "interloper", 7);
    let hooked = Hooked::new(inner.clone());
    let zw = z.clone();
    *hooked.cas_applied_then_conflict.lock().unwrap() =
        Some(Box::new(move |s: &ObjectStoreBackend| {
            commit(s, "idx", &staged, adding(&zw)).unwrap();
        }));

    let r = gc(&hooked, "idx", Duration::ZERO).unwrap();
    assert_eq!(r.deleted, 1);
    assert!(!inner.exists(&segment_path("idx", &x.id)).unwrap());
    assert!(claims(&inner).is_empty(), "claims released by token");
    assert!(read_manifest(&inner, "idx")
        .unwrap()
        .unwrap()
        .0
        .references(&z.id));
    assert_every_referenced_segment_readable(&inner);
}

/// A failed delete still releases the run's claims and restores the
/// tombstone, so the id is not blocked for a whole lease.
#[test]
fn failed_delete_releases_claims() {
    let inner = ObjectStoreBackend::memory().unwrap();
    let (x, staged) = tombstoned_x(&inner);
    let hooked = Hooked::new(inner.clone());
    *hooked.fail_first_delete.lock().unwrap() = true;

    let err = gc(&hooked, "idx", Duration::ZERO).unwrap_err();
    assert!(matches!(err, Error::Store(_)), "{err:?}");
    let (m, _) = read_manifest(&inner, "idx").unwrap().unwrap();
    assert!(m.deleting.is_empty());
    assert!(m.is_tombstoned(&x.id));
    assert!(inner.exists(&segment_path("idx", &x.id)).unwrap());
    // Not blocked: a writer may re-add it right away, or the next run
    // deletes it.
    assert!(commit(&inner, "idx", &staged, |m: &Manifest| Ok(m.clone())).is_ok());
    let r = gc(&inner, "idx", Duration::ZERO).unwrap();
    assert_eq!(r.deleted, 1);
}

/// A run that died after claiming leaves the id blocked until a later run
/// finds the claim older than the lease (not retention), takes it over,
/// deletes the file and releases it. A fresh claim is never taken over, even
/// at retention 0.
#[test]
fn abandoned_claims_are_taken_over_only_after_the_lease() {
    let store = ObjectStoreBackend::memory().unwrap();
    let (x, y, staged) = tombstoned_xy(&store);
    // Simulate two dead runs: one long ago, one just now.
    let (mut m, v) = read_manifest(&store, "idx").unwrap().unwrap();
    m.segment_tombstones.clear();
    m.deleting.push(DeleteClaim {
        id: x.id,
        claimed_at: "2000-01-01T00:00:00.000Z".into(),
        token: 111,
        lease_ms: 0,
        extra: Extra::new(),
    });
    m.deleting.push(DeleteClaim {
        id: y.id,
        claimed_at: bellwether_store::now_rfc3339(),
        token: 222,
        lease_ms: 0,
        extra: Extra::new(),
    });
    store
        .put_if_version(&manifest_path("idx"), Bytes::from(m.to_vec().unwrap()), &v)
        .unwrap();

    // Blocked while the claims stand.
    let err = commit(&store, "idx", &staged, adding(&x)).unwrap_err();
    assert_eq!(claimed_segment(&err), Some(x.id));

    let r = gc(&store, "idx", Duration::ZERO).unwrap();
    assert_eq!((r.deleted, r.stale_claims_taken_over), (1, 1));
    assert!(!store.exists(&segment_path("idx", &x.id)).unwrap());
    assert!(store.exists(&segment_path("idx", &y.id)).unwrap());
    let left = claims(&store);
    assert_eq!(left.len(), 1);
    assert_eq!(
        (left[0].id, left[0].token),
        (y.id, 222),
        "fresh claim untouched"
    );

    commit(&store, "idx", &staged, adding(&x)).unwrap();
    assert_every_referenced_segment_readable(&store);
}

#[test]
fn in_flight_snapshot_survives_gc_within_retention() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ObjectStoreBackend::local(dir.path()).unwrap());
    let mut staged = StagedSegments::new();
    let a = seg(&mut staged, "old segment", 0);
    let b = seg(&mut staged, "compacted segment", 0);
    commit(&*store, "idx", &staged, adding(&a)).unwrap();
    let snap = Snapshot::open(Arc::clone(&store), "idx").unwrap();
    commit(&*store, "idx", &staged, |m: &Manifest| {
        let mut m = m.clone();
        m.segments.retain(|s| s.id != a.id);
        m.add_segment(b.clone());
        Ok(m)
    })
    .unwrap();

    let report = gc(&*store, "idx", Duration::from_secs(3600)).unwrap();
    assert_eq!(report.deleted, 0);
    assert_eq!(report.retained, 1);
    assert_eq!(&snap.read_segment(&a.id).unwrap()[..], b"old segment");

    let report = gc(&*store, "idx", Duration::ZERO).unwrap();
    assert_eq!(report.deleted, 1);
    assert!(snap.read_segment(&a.id).is_err());
    let fresh = snap.reopen().unwrap();
    assert!(fresh.read_segment(&b.id).is_ok());
    assert!(fresh.manifest().segment_tombstones.is_empty());
    assert!(fresh.manifest().deleting.is_empty());
}

/// A large build pre-uploads its segments and commits with an empty staged
/// set. GC running in between tombstones the orphans but never deletes them
/// on first sight, and the commit clears the tombstones.
#[test]
fn pre_upload_then_commit() {
    let store = ObjectStoreBackend::memory().unwrap();
    commit(&store, "idx", &StagedSegments::new(), |m: &Manifest| {
        Ok(m.clone())
    })
    .unwrap();
    let mut staged = StagedSegments::new();
    let segs: Vec<_> = (0..3)
        .map(|n| seg(&mut staged, &format!("pre {n}"), n))
        .collect();
    for s in &segs {
        store
            .put_content_addressed(
                &segment_path("idx", &s.id),
                staged.get(&s.id).unwrap().clone(),
            )
            .unwrap();
    }

    let report = gc(&store, "idx", Duration::ZERO).unwrap();
    assert_eq!(report.tombstoned, 3);
    assert_eq!(report.deleted, 0);

    commit(&store, "idx", &StagedSegments::new(), |m: &Manifest| {
        let mut m = m.clone();
        for s in &segs {
            m.add_segment(s.clone());
        }
        Ok(m)
    })
    .unwrap();
    let (m, _) = read_manifest(&store, "idx").unwrap().unwrap();
    assert!(m.segment_tombstones.is_empty());
    let report = gc(&store, "idx", Duration::ZERO).unwrap();
    assert_eq!(report.deleted, 0);
    assert_every_referenced_segment_readable(&store);
}

/// An orphan needs two runs: the first tombstones it, the second (after
/// retention) claims and deletes it.
#[test]
fn two_gc_runs_delete_an_orphan() {
    let store = ObjectStoreBackend::memory().unwrap();
    commit(&store, "idx", &StagedSegments::new(), |m: &Manifest| {
        Ok(m.clone())
    })
    .unwrap();
    let mut staged = StagedSegments::new();
    let orphan = seg(&mut staged, "abandoned", 9);
    let path = segment_path("idx", &orphan.id);
    store
        .put_content_addressed(&path, staged.get(&orphan.id).unwrap().clone())
        .unwrap();

    let first = gc(&store, "idx", Duration::ZERO).unwrap();
    assert_eq!((first.tombstoned, first.deleted), (1, 0));
    assert!(store.exists(&path).unwrap());
    let second = gc(&store, "idx", Duration::ZERO).unwrap();
    assert_eq!((second.tombstoned, second.deleted), (0, 1));
    assert!(!store.exists(&path).unwrap());
    let (m, _) = read_manifest(&store, "idx").unwrap().unwrap();
    assert!(m.segment_tombstones.is_empty() && m.deleting.is_empty());
}

#[test]
fn gc_pages_and_caps_orphans() {
    let dir = tempfile::tempdir().unwrap();
    let store = ObjectStoreBackend::local(dir.path()).unwrap();
    let mut staged = StagedSegments::new();
    let segs: Vec<_> = (0..25)
        .map(|n| seg(&mut staged, &format!("s{n}"), n))
        .collect();
    commit(&store, "idx", &staged, |m: &Manifest| {
        let mut m = m.clone();
        for s in &segs {
            m.add_segment(s.clone());
        }
        Ok(m)
    })
    .unwrap();
    commit(&store, "idx", &staged, |m: &Manifest| {
        let mut m = m.clone();
        m.segments.truncate(5);
        Ok(m)
    })
    .unwrap();

    let mut max_page = 0;
    store
        .list_pages("idx/segments", 4, &mut |page| {
            max_page = max_page.max(page.len());
            Ok(ControlFlow::Continue(()))
        })
        .unwrap();
    assert_eq!(max_page, 4);

    let opts = GcOptions {
        page_size: 4,
        max_orphans: 2,
        delete_batch: 3,
        ..GcOptions::default()
    };
    let report = gc_with(&store, "idx", Duration::ZERO, opts).unwrap();
    assert_eq!(report.deleted, 20);
    assert_eq!(store.list("idx/segments").unwrap().len(), 5);

    // Orphans beyond the cap are deferred, not lost.
    for n in 0..5 {
        let mut st = StagedSegments::new();
        let o = seg(&mut st, &format!("orphan {n}"), 100 + n);
        store
            .put_content_addressed(&segment_path("idx", &o.id), st.get(&o.id).unwrap().clone())
            .unwrap();
    }
    let r1 = gc_with(&store, "idx", Duration::from_secs(3600), opts).unwrap();
    assert_eq!((r1.tombstoned, r1.orphans_deferred), (2, 3));
    let r2 = gc_with(&store, "idx", Duration::from_secs(3600), opts).unwrap();
    assert_eq!((r2.tombstoned, r2.orphans_deferred, r2.retained), (2, 1, 2));
}
