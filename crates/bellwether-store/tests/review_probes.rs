//! Review probes as regression tests: the commit and GC interleavings built
//! against the claim-then-delete protocol during review that the other test
//! files do not already cover.
//!
//! Every safe case ends on the invariant: each segment the current manifest
//! references exists, reads back with the right content, and is not claimed.
//! Each hook records what happened inside it, and the test asserts on that
//! record, so a hook that never fires fails the test instead of passing it.
//!
//! The documented residuals are `#[ignore]`d tests. They assert that the
//! residual **still reproduces**, so `cargo test -- --ignored` passes today
//! and turns red if one is ever closed, at which point the README's
//! "Residual risk" and "Clocks" paragraphs need updating.
//!
//! Probes covered elsewhere, and so not repeated here:
//!
//! | Probe | Covered by |
//! |---|---|
//! | S1, S2, S3, S6, S7 | `tests/gc.rs`, tests of the same names |
//! | S8, applied write reported as a conflict | `applied_write_reported_as_conflict_is_a_self_win` in `src/commit.rs` |
//! | S9b, the closure aborts a commit | `f_can_abort_a_rebase` in `src/commit.rs` |
//! | P1b, a longer-lease taker, pause under the recorded lease | subsumed by P1f below, which is the discriminating version |
//! | P1c, the holder paused past its lease, default batch | `wall_clock_reread_within_a_batch_skips_lost_claims` |
//! | P1d, the same with `delete_batch: 1` | `fence_stops_a_run_at_half_its_lease`, `claims_lost_mid_run_are_not_deleted` |
//! | P2a, a second run while the holder's claim outcome is unknown | `s6_concurrent_gcs_retention_zero` and `applied_claim_commit_reported_as_conflict_is_still_released` |
//! | P3, a writer re-adding a later id of the batch | `s2_writer_re_adds_between_claim_and_first_delete` (the refusal is per id) |
//! | P4d, skew beyond a lease with `delete_batch: 1` | `claims_lost_mid_run_are_not_deleted` |

#![allow(clippy::unwrap_used, clippy::assert_is_empty, clippy::too_many_lines)]

use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bellwether_store::{
    claimed_segment, commit, commit_with, gc, gc_with, manifest_path, read_manifest, segment_path,
    CommitOptions, Extra, GcOptions, GcReport, Manifest, ObjectMeta, ObjectStoreBackend,
    RowGroupRange, SegmentRef, Snapshot, StagedSegments, StoreExt, DEFAULT_CLAIM_LEASE,
    DEFAULT_DELETE_BATCH, DEFAULT_MAX_ORPHANS, DEFAULT_PAGE_SIZE,
};
use bellwether_types::{Error, Hash, Result, Store, StoreVersion};
use bytes::Bytes;

const ROOT: &str = "idx";
const MINUTE: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// A store with one-shot hooks and injected faults
// ---------------------------------------------------------------------------

/// A hook gets the inner store (so what it does is not itself hooked) and the
/// path of the call it interrupts.
type Hook = Box<dyn FnOnce(&ObjectStoreBackend, &str) + Send>;

/// What the conditional manifest update does wrong.
#[derive(Clone, Copy, PartialEq, Eq)]
enum CasFault {
    None,
    /// Every update reports a conflict and writes nothing.
    AlwaysConflict,
    /// The next update is applied, then reported as a non-conflict error, and
    /// the read-back that follows fails too: the outcome is unknown.
    AppliedThenUnknown,
}

struct Probe {
    inner: ObjectStoreBackend,
    /// Inside the first `delete`, before the inner delete: after GC's fence
    /// check and claim re-read for that id.
    before_delete: Mutex<Option<Hook>>,
    /// After the first `put_content_addressed`: a writer has uploaded its
    /// staged segment and not yet committed its manifest.
    after_upload: Mutex<Option<Hook>>,
    /// After the first `exists`: a writer has confirmed a pre-uploaded segment
    /// and not yet committed its manifest.
    after_exists: Mutex<Option<Hook>>,
    /// The first conditional manifest update sleeps this long first.
    cas_delay: Mutex<Option<Duration>>,
    cas_fault: Mutex<CasFault>,
    /// Manifest reads that fail before reads succeed again.
    failing_manifest_reads: Mutex<u32>,
    /// How many injected faults fired.
    faults_fired: Mutex<u32>,
}

impl Probe {
    fn new(inner: &ObjectStoreBackend) -> Self {
        Self {
            inner: inner.clone(),
            before_delete: Mutex::new(None),
            after_upload: Mutex::new(None),
            after_exists: Mutex::new(None),
            cas_delay: Mutex::new(None),
            cas_fault: Mutex::new(CasFault::None),
            failing_manifest_reads: Mutex::new(0),
            faults_fired: Mutex::new(0),
        }
    }

    fn on(
        slot: &Mutex<Option<Hook>>,
        hook: impl FnOnce(&ObjectStoreBackend, &str) + Send + 'static,
    ) {
        *slot.lock().unwrap() = Some(Box::new(hook));
    }

    fn fire(&self, slot: &Mutex<Option<Hook>>, path: &str) {
        let hook = slot.lock().unwrap().take();
        if let Some(h) = hook {
            h(&self.inner, path);
        }
    }

    fn faults_fired(&self) -> u32 {
        *self.faults_fired.lock().unwrap()
    }
}

impl Store for Probe {
    fn get(&self, p: &str) -> Result<(Bytes, StoreVersion)> {
        self.inner.get(p)
    }
    fn put_if_absent(&self, p: &str, b: Bytes) -> Result<StoreVersion> {
        self.inner.put_if_absent(p, b)
    }
    fn put_if_version(&self, p: &str, b: Bytes, e: &StoreVersion) -> Result<StoreVersion> {
        if let Some(d) = self.cas_delay.lock().unwrap().take() {
            std::thread::sleep(d);
        }
        let fault = *self.cas_fault.lock().unwrap();
        match fault {
            CasFault::None => self.inner.put_if_version(p, b, e),
            CasFault::AlwaysConflict => {
                *self.faults_fired.lock().unwrap() += 1;
                Err(Error::VersionConflict {
                    expected: e.0.clone(),
                    found: "a version this probe invented".into(),
                })
            }
            CasFault::AppliedThenUnknown => {
                *self.cas_fault.lock().unwrap() = CasFault::None;
                self.inner.put_if_version(p, b, e)?;
                *self.failing_manifest_reads.lock().unwrap() = 1;
                *self.faults_fired.lock().unwrap() += 1;
                Err(Error::Store("timed out after the write".into()))
            }
        }
    }
    fn put_content_addressed(&self, p: &str, b: Bytes) -> Result<()> {
        self.inner.put_content_addressed(p, b)?;
        self.fire(&self.after_upload, p);
        Ok(())
    }
    fn list(&self, p: &str) -> Result<Vec<String>> {
        self.inner.list(p)
    }
    fn delete(&self, p: &str) -> Result<()> {
        if p.contains("/segments/") {
            self.fire(&self.before_delete, p);
        }
        self.inner.delete(p)
    }
}

impl StoreExt for Probe {
    fn get_opt(&self, p: &str) -> Result<Option<(Bytes, StoreVersion)>> {
        if p.ends_with(bellwether_store::MANIFEST_FILE) {
            let mut failing = self.failing_manifest_reads.lock().unwrap();
            if *failing > 0 {
                *failing -= 1;
                return Err(Error::Store("manifest read-back failed".into()));
            }
        }
        self.inner.get_opt(p)
    }
    fn exists(&self, p: &str) -> Result<bool> {
        let found = self.inner.exists(p);
        self.fire(&self.after_exists, p);
        found
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
        self.inner.list_pages(p, n, f)
    }
}

// ---------------------------------------------------------------------------
// Fixtures and assertions
// ---------------------------------------------------------------------------

/// The memory backend, and the local backend in a fresh per-process temp
/// directory (kept alive by the returned guard).
fn backends() -> Vec<(&'static str, ObjectStoreBackend, Option<tempfile::TempDir>)> {
    let dir = tempfile::tempdir().unwrap();
    let local = ObjectStoreBackend::local(dir.path()).unwrap();
    vec![
        ("memory", ObjectStoreBackend::memory().unwrap(), None),
        ("local", local, Some(dir)),
    ]
}

fn seg(staged: &mut StagedSegments, body: &str, n: u32) -> SegmentRef {
    SegmentRef {
        id: staged.stage(Bytes::from(body.to_string())),
        kind: None,
        key_spec: "exact:id".into(),
        rowgroup_range: RowGroupRange::new(n, n + 1),
        bytes: body.len() as u64,
        join: None,
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

/// Commits X and Y, then drops both, so both are tombstoned (and, at
/// retention zero, claimable at once).
fn tombstoned_xy(store: &ObjectStoreBackend) -> (SegmentRef, SegmentRef, StagedSegments) {
    let mut staged = StagedSegments::new();
    let x = seg(&mut staged, "segment x", 0);
    let y = seg(&mut staged, "segment y", 1);
    commit(store, ROOT, &staged, |m: &Manifest| {
        let mut m = m.clone();
        m.add_segment(x.clone());
        m.add_segment(y.clone());
        Ok(m)
    })
    .unwrap();
    commit(store, ROOT, &staged, |m: &Manifest| {
        let mut m = m.clone();
        m.segments.clear();
        Ok(m)
    })
    .unwrap();
    (x, y, staged)
}

fn id_of(path: &str) -> Hash {
    let name = path.rsplit('/').next().unwrap();
    Hash::from_hex(name.strip_suffix(".seg").unwrap()).unwrap()
}

/// `(first, other)`: the segment whose delete the hook interrupts, and the
/// other one.
fn split<'a>(path: &str, x: &'a SegmentRef, y: &'a SegmentRef) -> (&'a SegmentRef, &'a SegmentRef) {
    if id_of(path) == x.id {
        (x, y)
    } else {
        (y, x)
    }
}

/// Sets every claim's `claimed_at` to `age` ago: what a taker sees when its
/// clock runs `age` ahead of the holder's, without waiting.
fn backdate_claims(store: &ObjectStoreBackend, age: Duration) {
    let at = chrono::Utc::now() - chrono::Duration::from_std(age).unwrap();
    let at = at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    rewrite_claims(store, |claimed_at| claimed_at.clone_from(&at));
}

/// Moves every claim's `claimed_at` back by `skew` from its own value.
fn shift_claims_back(store: &ObjectStoreBackend, skew: Duration) {
    rewrite_claims(store, |claimed_at| {
        let t = bellwether_store::parse_rfc3339(claimed_at).unwrap()
            - chrono::Duration::from_std(skew).unwrap();
        *claimed_at = t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    });
}

fn rewrite_claims(store: &ObjectStoreBackend, mut f: impl FnMut(&mut String)) {
    let (mut m, v) = read_manifest(store, ROOT).unwrap().unwrap();
    assert!(!m.deleting.is_empty(), "no claims to rewrite");
    for c in &mut m.deleting {
        f(&mut c.claimed_at);
    }
    m.sequence += 1;
    store
        .put_if_version(&manifest_path(ROOT), Bytes::from(m.to_vec().unwrap()), &v)
        .unwrap();
}

fn lease(d: Duration) -> GcOptions {
    GcOptions {
        claim_lease: d,
        ..GcOptions::default()
    }
}

/// The invariant: every referenced segment exists, reads back with the right
/// content, and is not claimed.
fn assert_invariant(store: &ObjectStoreBackend, ctx: &str) {
    let snap = Snapshot::open(Arc::new(store.clone()), ROOT).unwrap();
    for s in snap.segments() {
        assert!(
            !snap.manifest().is_claimed(&s.id),
            "{ctx}: referenced segment {} is claimed",
            s.id
        );
        snap.read_segment(&s.id)
            .unwrap_or_else(|e| panic!("{ctx}: referenced segment {} unreadable: {e}", s.id));
    }
}

/// Referenced segments whose file is gone: the data loss the residual tests
/// expect to reproduce.
fn missing_referenced(store: &ObjectStoreBackend) -> Vec<Hash> {
    let (m, _) = read_manifest(store, ROOT).unwrap().unwrap();
    m.segments
        .iter()
        .filter(|s| !store.exists(&segment_path(ROOT, &s.id)).unwrap())
        .map(|s| s.id)
        .collect()
}

/// The id a commit was refused for, from the typed claim refusal; `None` for
/// success or any other error.
fn refused_id<T>(r: &Result<T>) -> Option<Hash> {
    match r {
        Err(e @ Error::SegmentClaimed(id)) => {
            assert_eq!(
                claimed_segment(e),
                Some(*id),
                "helper and typed error agree"
            );
            Some(*id)
        }
        _ => None,
    }
}

/// What a hook observed, filled in by the hook and taken by the test.
type Seen<T> = Arc<Mutex<Option<T>>>;

fn seen<T>() -> (Seen<T>, Seen<T>) {
    let s = Arc::new(Mutex::new(None));
    (Arc::clone(&s), s)
}

fn taken<T>(s: &Seen<T>, ctx: &str) -> T {
    s.lock()
        .unwrap()
        .take()
        .unwrap_or_else(|| panic!("{ctx}: the hook never ran"))
}

// ---------------------------------------------------------------------------
// Writer-side windows
// ---------------------------------------------------------------------------

/// S4: a whole GC run lands between a writer's upload of its staged segment
/// and its manifest CAS. GC claims the tombstoned X, deletes the file the
/// writer just uploaded, and releases. The writer's CAS then loses, and the
/// rebase must upload the staged bytes again rather than trust the upload
/// of the lost attempt.
///
/// Guards against a commit that remembers what an earlier attempt uploaded
/// (the closest existing test, `staged_segments_are_uploaded_without_trusting_exists`,
/// has no GC deleting between attempts).
#[test]
fn s4_gc_between_writer_upload_and_cas_forces_a_reupload() {
    for (backend, store, _dir) in backends() {
        let (x, _, staged) = tombstoned_xy(&store);
        let probe = Probe::new(&store);
        let (hook_seen, test_seen) = seen::<GcReport>();
        Probe::on(&probe.after_upload, move |s, _| {
            *hook_seen.lock().unwrap() = Some(gc(s, ROOT, Duration::ZERO).unwrap());
        });

        let committed = commit(&probe, ROOT, &staged, adding(&x));

        let ctx = format!("S4 on {backend}");
        let r = taken(&test_seen, &ctx);
        assert_eq!(
            r.deleted, 2,
            "{ctx}: GC must have deleted X and Y mid-commit"
        );
        assert!(committed.is_ok(), "{ctx}: {committed:?}");
        let (m, _) = read_manifest(&store, ROOT).unwrap().unwrap();
        assert!(m.references(&x.id), "{ctx}");
        assert_invariant(&store, &ctx);
    }
}

/// S5: a builder pre-uploaded Y and commits with an empty staged set. A
/// first GC run has tombstoned the orphan. A second run claims, deletes and
/// releases Y right after the writer confirmed Y exists, before its CAS.
/// The CAS loses, the rebase re-checks, and the commit fails as "neither
/// staged nor present" instead of publishing a reference to a deleted file.
///
/// Guards against a commit that trusts an existence check from a lost
/// attempt (the closest existing test, `pre_upload_then_commit`, runs GC
/// before the commit, not inside it).
#[test]
fn s5_preuploaded_segment_deleted_inside_the_commit_window_fails_the_commit() {
    for (backend, store, _dir) in backends() {
        let ctx = format!("S5 on {backend}");
        commit(&store, ROOT, &StagedSegments::new(), |m: &Manifest| {
            Ok(m.clone())
        })
        .unwrap();
        let mut staged = StagedSegments::new();
        let y = seg(&mut staged, "pre-uploaded segment", 0);
        store
            .put_content_addressed(
                &segment_path(ROOT, &y.id),
                staged.get(&y.id).unwrap().clone(),
            )
            .unwrap();
        let first = gc(&store, ROOT, Duration::ZERO).unwrap();
        assert_eq!(first.tombstoned, 1, "{ctx}: the orphan is tombstoned first");

        let probe = Probe::new(&store);
        let (hook_seen, test_seen) = seen::<GcReport>();
        Probe::on(&probe.after_exists, move |s, _| {
            *hook_seen.lock().unwrap() = Some(gc(s, ROOT, Duration::ZERO).unwrap());
        });
        let committed = commit(&probe, ROOT, &StagedSegments::new(), adding(&y));

        let second = taken(&test_seen, &ctx);
        assert_eq!(
            second.deleted, 1,
            "{ctx}: the second run deleted Y mid-commit"
        );
        match committed {
            Err(Error::Store(msg)) => {
                assert!(msg.contains("neither staged nor present"), "{ctx}: {msg}");
            }
            other => panic!("{ctx}: expected the commit to fail, got {other:?}"),
        }
        let (m, _) = read_manifest(&store, ROOT).unwrap().unwrap();
        assert!(!m.references(&y.id), "{ctx}");
        assert_invariant(&store, &ctx);
    }
}

/// S9: under a permanent conflict, `commit_with` stops after exactly
/// `max_attempts` attempts (one when it is zero) and returns the plain
/// version conflict, not a claim refusal. Nothing is written.
///
/// Guards the retry bound itself; the existing commit tests cover one rebase
/// and the non-retried non-conflict error, not exhaustion.
#[test]
fn s9_a_permanent_conflict_exhausts_the_bounded_retries() {
    let store = ObjectStoreBackend::memory().unwrap();
    commit(&store, ROOT, &StagedSegments::new(), |m: &Manifest| {
        Ok(m.clone())
    })
    .unwrap();
    let probe = Probe::new(&store);
    *probe.cas_fault.lock().unwrap() = CasFault::AlwaysConflict;

    for (max_attempts, expected_calls) in [(7, 7), (0, 1)] {
        let opts = CommitOptions {
            max_attempts,
            base_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
        };
        let mut calls = 0;
        let err = commit_with(&probe, ROOT, &StagedSegments::new(), opts, |m| {
            calls += 1;
            Ok(m.clone())
        })
        .unwrap_err();
        assert_eq!(calls, expected_calls, "max_attempts {max_attempts}");
        // The plain conflict, never the typed claim refusal.
        assert!(matches!(err, Error::VersionConflict { .. }), "{err:?}");
        assert_eq!(claimed_segment(&err), None);
    }
    assert_eq!(probe.faults_fired(), 8);
    let (m, _) = read_manifest(&store, ROOT).unwrap().unwrap();
    assert_eq!(m.sequence, 1, "nothing was committed");
}

// ---------------------------------------------------------------------------
// Two GC runs with different leases
// ---------------------------------------------------------------------------

/// P1a and P1e: GC1 holds claims recorded with the default one-hour lease.
/// Inside its first delete the claims are made to look 45 minutes old,
/// past half the recorded lease but short of all of it. A zero-lease GC2
/// must not take them over, so a writer re-adding either the id GC1 is
/// deleting or the other one is refused.
///
/// Guards that takeover waits for the whole recorded lease. The closest
/// existing test, `short_lease_run_cannot_take_over_a_live_claim`, uses a
/// fresh claim, so it misses a threshold of half the lease.
#[test]
fn p1a_p1e_a_short_lease_taker_waits_for_the_whole_recorded_lease() {
    for (backend, store, _dir) in backends() {
        let ctx = format!("P1a/P1e on {backend}");
        let (x, y, staged) = tombstoned_xy(&store);
        let probe = Probe::new(&store);
        let (hook_seen, test_seen) = seen();
        let (xw, yw) = (x.clone(), y.clone());
        Probe::on(&probe.before_delete, move |s, path| {
            backdate_claims(s, 45 * MINUTE);
            let r2 = gc_with(s, ROOT, Duration::ZERO, lease(Duration::ZERO)).unwrap();
            let (first, other) = split(path, &xw, &yw);
            let same = refused_id(&commit(s, ROOT, &staged, adding(first)));
            let next = refused_id(&commit(s, ROOT, &staged, adding(other)));
            *hook_seen.lock().unwrap() = Some((r2, (first.id, same), (other.id, next)));
        });

        let r1 = gc(&probe, ROOT, Duration::ZERO).unwrap();

        let (r2, (first, same), (other, next)) = taken(&test_seen, &ctx);
        assert_eq!(
            r2.stale_claims_taken_over, 0,
            "{ctx}: GC2 took over a live claim"
        );
        assert_eq!(r2.deleted, 0, "{ctx}");
        assert_eq!(same, Some(first), "{ctx}: re-adding the id being deleted");
        assert_eq!(next, Some(other), "{ctx}: re-adding the next id");
        assert_eq!(r1.deleted, 2, "{ctx}");
        assert!(read_manifest(&store, ROOT)
            .unwrap()
            .unwrap()
            .0
            .deleting
            .is_empty());
        assert_invariant(&store, &ctx);
    }
}

/// P1f: GC1's claims, recorded with a one-hour lease, look 61 minutes old,
/// past the recorded lease. GC2 runs with a two-hour lease, and takeover
/// waits for the larger of the recorded lease and the taker's own, so GC2
/// takes nothing over and the writer re-adding the id GC1 is deleting is
/// refused.
///
/// Guards the taker's own lease in that rule. Keyed to the recorded lease
/// alone, GC2 would take the claim over, the writer's re-add would succeed,
/// and GC1's delete would hit a referenced segment. This also subsumes P1b.
#[test]
fn p1f_a_taker_also_waits_for_its_own_longer_lease() {
    for (backend, store, _dir) in backends() {
        let ctx = format!("P1f on {backend}");
        let (x, y, staged) = tombstoned_xy(&store);
        let probe = Probe::new(&store);
        let (hook_seen, test_seen) = seen();
        let (xw, yw) = (x.clone(), y.clone());
        Probe::on(&probe.before_delete, move |s, path| {
            backdate_claims(s, 61 * MINUTE);
            let r2 = gc_with(s, ROOT, Duration::ZERO, lease(120 * MINUTE)).unwrap();
            let (first, _) = split(path, &xw, &yw);
            let same = refused_id(&commit(s, ROOT, &staged, adding(first)));
            *hook_seen.lock().unwrap() = Some((r2, first.id, same));
        });

        let r1 = gc(&probe, ROOT, Duration::ZERO).unwrap();

        let (r2, first, same) = taken(&test_seen, &ctx);
        assert_eq!(
            r2.stale_claims_taken_over, 0,
            "{ctx}: GC2 ignored its own lease"
        );
        assert_eq!(same, Some(first), "{ctx}");
        assert_eq!(r1.deleted, 2, "{ctx}");
        assert_invariant(&store, &ctx);
    }
}

// ---------------------------------------------------------------------------
// A claim commit with an unknown outcome
// ---------------------------------------------------------------------------

/// P2b: GC1's claim commit is applied, then reported as a non-conflict
/// error, and the read-back that would reveal the nonce fails too. GC1
/// cannot know its claims landed, so it releases by token anyway and
/// returns the error. The ids are tombstoned again, not blocked for a whole
/// lease, and the next run deletes them.
///
/// Guards the release on the claim phase's error path. The closest existing
/// test, `failed_delete_releases_claims`, covers the release after a failed
/// delete, a different path.
#[test]
fn p2b_a_claim_commit_with_an_unknown_outcome_is_released_by_token() {
    for (backend, store, _dir) in backends() {
        let ctx = format!("P2b on {backend}");
        let (x, y, _) = tombstoned_xy(&store);
        let probe = Probe::new(&store);
        *probe.cas_fault.lock().unwrap() = CasFault::AppliedThenUnknown;

        let err = gc(&probe, ROOT, Duration::ZERO).unwrap_err();

        assert_eq!(probe.faults_fired(), 1, "{ctx}: the fault never fired");
        assert!(matches!(err, Error::Store(_)), "{ctx}: {err:?}");
        let (m, _) = read_manifest(&store, ROOT).unwrap().unwrap();
        assert!(
            m.deleting.is_empty(),
            "{ctx}: claims left blocked: {:?}",
            m.deleting
        );
        for s in [&x, &y] {
            assert!(m.is_tombstoned(&s.id), "{ctx}: tombstone restored");
            assert!(store.exists(&segment_path(ROOT, &s.id)).unwrap(), "{ctx}");
        }
        let next = gc(&store, ROOT, Duration::ZERO).unwrap();
        assert_eq!(next.deleted, 2, "{ctx}");
        assert_invariant(&store, &ctx);
    }
}

// ---------------------------------------------------------------------------
// Clock skew inside the contract
// ---------------------------------------------------------------------------

/// P4a and P4b: equal default leases. Inside GC1's first delete, its claims
/// are made to look 25 minutes old (skew well inside the contract) and then
/// 59 minutes old (just short of the lease, for a holder that has just
/// claimed). A default GC2 takes nothing over and a writer re-adding the
/// other id is refused.
///
/// Guards the takeover threshold near the lease. The closest existing test,
/// `abandoned_claims_are_taken_over_only_after_the_lease`, checks a fresh
/// claim and one from the year 2000, so it misses any threshold below 59
/// minutes, such as half the lease.
#[test]
fn p4a_p4b_skew_below_the_lease_is_not_a_takeover() {
    for skew in [25 * MINUTE, 59 * MINUTE] {
        for (backend, store, _dir) in backends() {
            let ctx = format!("P4 skew {} min on {backend}", skew.as_secs() / 60);
            let (x, y, staged) = tombstoned_xy(&store);
            let probe = Probe::new(&store);
            let (hook_seen, test_seen) = seen();
            let (xw, yw) = (x.clone(), y.clone());
            Probe::on(&probe.before_delete, move |s, path| {
                backdate_claims(s, skew);
                let r2 = gc(s, ROOT, Duration::ZERO).unwrap();
                let (_, other) = split(path, &xw, &yw);
                let next = refused_id(&commit(s, ROOT, &staged, adding(other)));
                *hook_seen.lock().unwrap() = Some((r2, other.id, next));
            });

            let r1 = gc(&probe, ROOT, Duration::ZERO).unwrap();

            let (r2, other, next) = taken(&test_seen, &ctx);
            assert_eq!(
                r2.stale_claims_taken_over, 0,
                "{ctx}: takeover before the lease"
            );
            assert_eq!(next, Some(other), "{ctx}");
            assert_eq!(r1.deleted, 2, "{ctx}");
            assert_invariant(&store, &ctx);
        }
    }
}

// ---------------------------------------------------------------------------
// Documented residuals (ignored; they assert the residual still reproduces)
// ---------------------------------------------------------------------------

/// P1g documents the residual the README names under "Residual risk": GC1
/// passes its fence check, then pauses longer than its lease before the
/// delete call. A zero-lease GC2 takes the claim over and releases it, a
/// writer re-adds the same bytes, and GC1's delete removes a referenced
/// segment. Leases without fencing enforced by the store cannot close this
/// window, which the lease contract accepts. This test asserts the loss
/// still reproduces.
#[test]
#[ignore = "documents a known residual: a holder paused past its lease between fence check and delete"]
fn residual_p1g_pause_between_the_fence_check_and_the_delete() {
    let store = ObjectStoreBackend::memory().unwrap();
    let (x, y, staged) = tombstoned_xy(&store);
    let probe = Probe::new(&store);
    let (hook_seen, test_seen) = seen();
    let (xw, yw) = (x.clone(), y.clone());
    Probe::on(&probe.before_delete, move |s, path| {
        std::thread::sleep(Duration::from_millis(250)); // past GC1's 200 ms lease
        let r2 = gc_with(s, ROOT, Duration::ZERO, lease(Duration::ZERO)).unwrap();
        let (first, _) = split(path, &xw, &yw);
        let w = commit(s, ROOT, &staged, adding(first));
        *hook_seen.lock().unwrap() = Some((r2, first.id, w.is_ok()));
    });

    gc_with(
        &probe,
        ROOT,
        Duration::ZERO,
        lease(Duration::from_millis(200)),
    )
    .unwrap();

    let (r2, first, writer_ok) = taken(&test_seen, "P1g");
    assert_eq!(r2.stale_claims_taken_over, 2);
    assert!(writer_ok);
    assert_eq!(
        missing_referenced(&store),
        vec![first],
        "the residual no longer reproduces"
    );
}

/// P4c documents a residual **outside the clock contract** (README "Clocks":
/// skew between GC hosts must be well under half the lease). A taker whose
/// clock runs two hours ahead sees GC1's live claims as abandoned, takes
/// them over, deletes and releases them, and a writer re-adds the second
/// id. GC1 still deletes it from the claim set it read at the start of the
/// batch. `delete_batch: 1` closes this. This test asserts the loss still
/// reproduces at the default batch size.
#[test]
#[ignore = "documents a known residual outside the clock contract: skew of more than a lease"]
fn residual_p4c_skew_beyond_a_lease_within_a_default_batch() {
    let store = ObjectStoreBackend::memory().unwrap();
    let (x, y, staged) = tombstoned_xy(&store);
    let probe = Probe::new(&store);
    let (hook_seen, test_seen) = seen();
    let (xw, yw) = (x.clone(), y.clone());
    Probe::on(&probe.before_delete, move |s, path| {
        backdate_claims(s, 120 * MINUTE);
        let r2 = gc(s, ROOT, Duration::ZERO).unwrap();
        let (_, other) = split(path, &xw, &yw);
        let w = commit(s, ROOT, &staged, adding(other));
        *hook_seen.lock().unwrap() = Some((r2, other.id, w.is_ok()));
    });

    gc(&probe, ROOT, Duration::ZERO).unwrap();

    let (r2, other, writer_ok) = taken(&test_seen, "P4c");
    assert_eq!((r2.stale_claims_taken_over, r2.deleted), (2, 2));
    assert!(writer_ok);
    assert_eq!(
        missing_referenced(&store),
        vec![other],
        "the residual no longer reproduces"
    );
}

/// P5 documents a residual **outside the clock contract**, and shows it
/// starts at more than half a lease of skew, not a full lease. GC1 (lease
/// 2 s) starts deleting 500 ms into its run, inside its fence. A taker whose
/// clock runs 1.6 s ahead (0.8 of the lease) sees the claims as older than
/// the lease, takes them over and releases them, and a writer re-adds the
/// second id. GC1's next delete comes well within an eighth of the lease of
/// its last re-read, so it still deletes it. This test asserts the loss
/// still reproduces.
#[test]
#[ignore = "documents a known residual outside the clock contract: skew between half a lease and a lease"]
fn residual_p5_skew_between_half_a_lease_and_a_lease() {
    let store = ObjectStoreBackend::memory().unwrap();
    let (x, y, staged) = tombstoned_xy(&store);
    let probe = Probe::new(&store);
    *probe.cas_delay.lock().unwrap() = Some(Duration::from_millis(500));
    let (hook_seen, test_seen) = seen();
    let (xw, yw) = (x.clone(), y.clone());
    Probe::on(&probe.before_delete, move |s, path| {
        shift_claims_back(s, Duration::from_millis(1600));
        let r2 = gc_with(s, ROOT, Duration::ZERO, lease(Duration::ZERO)).unwrap();
        let (_, other) = split(path, &xw, &yw);
        let w = commit(s, ROOT, &staged, adding(other));
        *hook_seen.lock().unwrap() = Some((r2, other.id, w.is_ok()));
    });

    let r1 = gc_with(&probe, ROOT, Duration::ZERO, lease(Duration::from_secs(2))).unwrap();

    let (r2, other, writer_ok) = taken(&test_seen, "P5");
    assert_eq!(r2.stale_claims_taken_over, 2);
    assert!(writer_ok);
    assert!(!r1.fenced && r1.claims_lost == 0, "{r1:?}");
    assert_eq!(
        missing_referenced(&store),
        vec![other],
        "the residual no longer reproduces"
    );
}

// ---------------------------------------------------------------------------
// The README's GC defaults match the constants
// ---------------------------------------------------------------------------

const README: &str = include_str!("../README.md");

/// `3600 s` as "1 hour", `90 s` as "90 seconds".
fn human(d: Duration) -> String {
    assert_eq!(d.subsec_nanos(), 0, "whole seconds only");
    let s = d.as_secs();
    let (n, unit) = if s.is_multiple_of(3600) {
        (s / 3600, "hour")
    } else if s.is_multiple_of(60) {
        (s / 60, "minute")
    } else {
        (s, "second")
    };
    if n == 1 {
        format!("1 {unit}")
    } else {
        format!("{n} {unit}s")
    }
}

/// `10000` as "10,000".
fn thousands(n: usize) -> String {
    let digits = n.to_string();
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// The README's garbage-collection section states the defaults of
/// `GcOptions` (claim lease, delete batch, orphan cap) and that retention is
/// a parameter of `gc`, not an option with a default. This reads the
/// constants, so changing one without the README fails here.
#[test]
fn readme_gc_section_matches_the_gc_option_defaults() {
    // Exhaustive on purpose: adding a field to `GcOptions` (a retention
    // default, say) stops this compiling until the README and this test say
    // what it is.
    let GcOptions {
        page_size,
        max_orphans,
        claim_lease,
        delete_batch,
    } = GcOptions::default();
    assert_eq!(page_size, DEFAULT_PAGE_SIZE);
    assert_eq!(max_orphans, DEFAULT_MAX_ORPHANS);
    assert_eq!(claim_lease, DEFAULT_CLAIM_LEASE);
    assert_eq!(delete_batch, DEFAULT_DELETE_BATCH);

    let section = README
        .split("## Garbage collection")
        .nth(1)
        .and_then(|rest| rest.split("\n## ").next())
        .expect("README has a garbage-collection section");
    let lease = human(DEFAULT_CLAIM_LEASE);
    let expected = [
        // Retention is the `gc` parameter; the options are exactly these.
        "`gc(store, root, retention)`, or `gc_with(..., GcOptions { page_size, max_orphans, claim_lease, delete_batch })`".to_string(),
        format!("(`claim_lease`, default {lease})"),
        format!("the {lease} default is"),
        format!("in batches of `delete_batch` (default {DEFAULT_DELETE_BATCH})"),
        format!("`max_orphans` (default {})", thousands(DEFAULT_MAX_ORPHANS)),
    ];
    for phrase in &expected {
        assert!(
            section.contains(phrase.as_str()),
            "README's GC section no longer says {phrase:?}"
        );
    }

    // Every duration in hours in the section is the lease: no stale copy of
    // an old default survives elsewhere in the text.
    let lease_hours = (DEFAULT_CLAIM_LEASE.as_secs() / 3600).to_string();
    let mut mentions = 0;
    for (at, _) in section.match_indices(" hour") {
        let n = section[..at]
            .rsplit(|c: char| !c.is_ascii_digit())
            .next()
            .unwrap_or_default();
        assert_eq!(n, lease_hours, "README's GC section mentions {n:?} hour(s)");
        mentions += 1;
    }
    assert!(
        mentions >= 2,
        "expected the lease to be stated at least twice"
    );
}
