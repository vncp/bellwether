//! Invariant 4: readers see one manifest version. A commit in progress on one
//! thread never changes what a concurrent reader on another thread observes;
//! the reader's view advances only when it explicitly re-opens.

#![allow(clippy::unwrap_used, clippy::assert_is_empty, clippy::too_many_lines)]

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

use bellwether_store::{
    claimed_segment, commit, Extra, Manifest, ObjectStoreBackend, RowGroupRange, SegmentRef,
    Snapshot, StagedSegments,
};
use bellwether_types::Hash;
use bytes::Bytes;

fn segment(staged: &mut StagedSegments, n: u32) -> SegmentRef {
    // Large enough that a torn or mixed read would be visible.
    let mut body: Vec<u8> = n.to_le_bytes().to_vec();
    body.extend((0..64 * 1024u32).map(|i| (i ^ n).to_le_bytes()[0]));
    let id = staged.stage(Bytes::from(body));
    SegmentRef {
        id,
        kind: None,
        key_spec: "exact:id".into(),
        rowgroup_range: RowGroupRange::new(n, n + 1),
        bytes: 64 * 1024,
        join: None,
        extra: Extra::new(),
    }
}

fn add(store: &ObjectStoreBackend, root: &str, n: u32) {
    let mut staged = StagedSegments::new();
    let s = segment(&mut staged, n);
    commit(store, root, &staged, |m: &Manifest| {
        let mut m = m.clone();
        m.add_segment(s.clone());
        Ok(m)
    })
    .unwrap();
}

/// Snapshot of what a reader sees: the manifest's segment ids and the hash of
/// every segment's bytes as read through the snapshot.
fn observe(snap: &Snapshot<ObjectStoreBackend>) -> (BTreeSet<Hash>, Vec<Hash>) {
    let ids: BTreeSet<Hash> = snap.segments().iter().map(|s| s.id).collect();
    let contents = snap
        .segments()
        .iter()
        .map(|s| Hash::of(&snap.read_segment(&s.id).unwrap()))
        .collect();
    (ids, contents)
}

#[test]
fn reader_view_is_pinned_while_commits_interleave() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ObjectStoreBackend::local(dir.path()).unwrap());
    let root = "idx";
    add(&store, root, 0);
    add(&store, root, 1);

    let snap = Snapshot::open(Arc::clone(&store), root).unwrap();
    let pinned_version = snap.version().clone();
    let pinned = observe(&snap);
    assert_eq!(pinned.0.len(), 2);

    let commits_done = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let start = Arc::new(Barrier::new(2));
    let total_commits = 25u32;

    std::thread::scope(|s| {
        // Writer: keeps committing new segments.
        {
            let store = Arc::clone(&store);
            let commits_done = Arc::clone(&commits_done);
            let stop = Arc::clone(&stop);
            let start = Arc::clone(&start);
            s.spawn(move || {
                start.wait();
                for n in 2..2 + total_commits {
                    add(&store, root, n);
                    commits_done.fetch_add(1, Ordering::SeqCst);
                }
                stop.store(true, Ordering::SeqCst);
            });
        }
        // Reader: repeatedly reads through the same snapshot during commits.
        start.wait();
        let mut reads = 0;
        while !stop.load(Ordering::SeqCst) || reads < 5 {
            let seen_before = commits_done.load(Ordering::SeqCst);
            let view = observe(&snap);
            assert_eq!(view, pinned, "pinned view changed mid-operation");
            assert_eq!(snap.version(), &pinned_version);
            // A segment committed after the snapshot is not visible through it.
            if seen_before > 0 {
                let current = snap.reopen().unwrap();
                let newer: Vec<_> = current
                    .segments()
                    .iter()
                    .filter(|s| !pinned.0.contains(&s.id))
                    .collect();
                assert_ne!(newer.len(), 0);
                assert!(snap.read_segment(&newer[0].id).is_err());
            }
            reads += 1;
        }
    });

    // The old snapshot still sees exactly what it saw.
    assert_eq!(observe(&snap), pinned);
    assert_eq!(snap.version(), &pinned_version);

    // Only an explicit re-open advances the view, to a superset.
    let fresh = snap.reopen().unwrap();
    assert_ne!(fresh.version(), &pinned_version);
    let (fresh_ids, _) = observe(&fresh);
    assert!(fresh_ids.is_superset(&pinned.0));
    assert_eq!(fresh_ids.len(), 2 + total_commits as usize);
}

#[test]
fn reader_view_is_pinned_across_concurrent_writers_on_memory_backend() {
    // Same property on the backend that uses object_store's PutMode::Update.
    let store = Arc::new(ObjectStoreBackend::memory().unwrap());
    let root = "";
    add(&store, root, 0);
    let snap = Snapshot::open(Arc::clone(&store), root).unwrap();
    let pinned = observe(&snap);
    std::thread::scope(|s| {
        for t in 0..4u32 {
            let store = Arc::clone(&store);
            s.spawn(move || {
                for i in 0..5 {
                    add(&store, root, 100 + t * 10 + i);
                }
            });
        }
        for _ in 0..20 {
            assert_eq!(observe(&snap), pinned);
        }
    });
    assert_eq!(observe(&snap), pinned);
    assert_eq!(snap.reopen().unwrap().segments().len(), 21);
}

/// The "build in progress" window, step by step: the writer has uploaded a
/// new segment but not yet swapped the manifest, then has swapped it. The reader's
/// pinned view is unchanged at each step and advances only on re-open.
#[test]
fn build_in_progress_is_invisible_until_reopen() {
    use bellwether_store::{read_manifest, segment_path, StoreExt};
    use bellwether_types::Store;
    use std::sync::mpsc;

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ObjectStoreBackend::local(dir.path()).unwrap());
    let root = "idx";
    add(&store, root, 0);
    let snap = Snapshot::open(Arc::clone(&store), root).unwrap();
    let pinned = observe(&snap);
    let pinned_version = snap.version().clone();

    let (to_reader, from_writer) = mpsc::channel::<(&'static str, Hash)>();
    let (to_writer, from_reader) = mpsc::channel::<()>();

    std::thread::scope(|s| {
        let writer_store = Arc::clone(&store);
        s.spawn(move || {
            let mut staged = StagedSegments::new();
            let new = segment(&mut staged, 1);
            // Step 1: segment written, manifest untouched.
            writer_store
                .put_content_addressed(
                    &segment_path(root, &new.id),
                    staged.get(&new.id).unwrap().clone(),
                )
                .unwrap();
            to_reader.send(("segment written", new.id)).unwrap();
            from_reader.recv().unwrap();
            // Step 2: manifest CAS.
            let (_, before) = read_manifest(&*writer_store, root).unwrap().unwrap();
            commit(&*writer_store, root, &staged, |m: &Manifest| {
                let mut m = m.clone();
                m.add_segment(new.clone());
                Ok(m)
            })
            .unwrap();
            let (_, after) = read_manifest(&*writer_store, root).unwrap().unwrap();
            assert_ne!(after, before);
            to_reader.send(("manifest committed", new.id)).unwrap();
        });

        let (step, new_id) = from_writer.recv().unwrap();
        assert_eq!(step, "segment written");
        assert!(store.exists(&segment_path(root, &new_id)).unwrap());
        assert_eq!(observe(&snap), pinned);
        assert!(snap.read_segment(&new_id).is_err());
        assert!(snap.read_segment_range(&new_id, 0..16).is_err());
        to_writer.send(()).unwrap();

        let (step, new_id) = from_writer.recv().unwrap();
        assert_eq!(step, "manifest committed");
        assert_eq!(observe(&snap), pinned);
        assert_eq!(snap.version(), &pinned_version);
        assert!(snap.read_segment(&new_id).is_err());

        let fresh = snap.reopen().unwrap();
        assert_ne!(fresh.version(), &pinned_version);
        assert!(fresh.manifest().references(&new_id));
        let full = fresh.read_segment(&new_id).unwrap();
        let part = fresh.read_segment_range(&new_id, 100..164).unwrap();
        assert_eq!(&part[..], &full[100..164]);
    });
}

/// A store whose deletes take 15 ms, like a slow object-store DELETE.
struct SlowDelete(ObjectStoreBackend);

impl bellwether_types::Store for SlowDelete {
    fn get(&self, p: &str) -> bellwether_types::Result<(Bytes, bellwether_types::StoreVersion)> {
        self.0.get(p)
    }
    fn put_if_absent(
        &self,
        p: &str,
        b: Bytes,
    ) -> bellwether_types::Result<bellwether_types::StoreVersion> {
        self.0.put_if_absent(p, b)
    }
    fn put_if_version(
        &self,
        p: &str,
        b: Bytes,
        e: &bellwether_types::StoreVersion,
    ) -> bellwether_types::Result<bellwether_types::StoreVersion> {
        self.0.put_if_version(p, b, e)
    }
    fn put_content_addressed(&self, p: &str, b: Bytes) -> bellwether_types::Result<()> {
        self.0.put_content_addressed(p, b)
    }
    fn list(&self, p: &str) -> bellwether_types::Result<Vec<String>> {
        self.0.list(p)
    }
    fn delete(&self, p: &str) -> bellwether_types::Result<()> {
        std::thread::sleep(std::time::Duration::from_millis(15));
        self.0.delete(p)
    }
}

impl bellwether_store::StoreExt for SlowDelete {
    fn get_opt(
        &self,
        p: &str,
    ) -> bellwether_types::Result<Option<(Bytes, bellwether_types::StoreVersion)>> {
        self.0.get_opt(p)
    }
    fn exists(&self, p: &str) -> bellwether_types::Result<bool> {
        self.0.exists(p)
    }
    fn get_range(
        &self,
        p: &str,
        r: std::ops::Range<u64>,
    ) -> bellwether_types::Result<Option<Bytes>> {
        self.0.get_range(p, r)
    }
    fn list_pages(
        &self,
        p: &str,
        n: usize,
        f: &mut dyn FnMut(
            &[bellwether_store::ObjectMeta],
        ) -> bellwether_types::Result<std::ops::ControlFlow<()>>,
    ) -> bellwether_types::Result<()> {
        self.0.list_pages(p, n, f)
    }
}

/// Commits a new segment, retrying while GC holds a claim on it (possible
/// here only because the test's retention is far shorter than a contended
/// commit: the pre-uploaded file can be tombstoned as an orphan and claimed
/// before the commit lands). Keeping the bytes staged makes the retry
/// re-upload.
fn add_retrying(store: &ObjectStoreBackend, root: &str, n: u32) {
    let mut staged = StagedSegments::new();
    let s = segment(&mut staged, n);
    loop {
        let res = commit(store, root, &staged, |m: &Manifest| {
            let mut m = m.clone();
            m.add_segment(s.clone());
            Ok(m)
        });
        match res {
            Ok(_) => return,
            Err(e) if claimed_segment(&e).is_some() => {}
            Err(e) => panic!("append failed: {e}"),
        }
    }
}

/// Seeds claims of GC runs that died long ago (claimed in 2000, foreign
/// tokens) on unreferenced segment files, so live GC runs must take them
/// over while writers and readers run.
fn seed_abandoned_claims(store: &ObjectStoreBackend, root: &str, segs: &[(SegmentRef, Bytes)]) {
    use bellwether_store::{manifest_path, read_manifest, segment_path, DeleteClaim};
    use bellwether_types::Store;
    for (s, bytes) in segs {
        store
            .put_content_addressed(&segment_path(root, &s.id), bytes.clone())
            .unwrap();
    }
    let (mut m, v) = read_manifest(store, root).unwrap().unwrap();
    for (i, (s, _)) in segs.iter().enumerate() {
        m.deleting.push(DeleteClaim {
            id: s.id,
            claimed_at: "2000-01-01T00:00:00.000Z".into(),
            token: 1_000 + u64::try_from(i).unwrap(),
            lease_ms: 0,
            extra: Extra::new(),
        });
    }
    m.sequence += 1;
    store
        .put_if_version(&manifest_path(root), Bytes::from(m.to_vec().unwrap()), &v)
        .unwrap();
}

/// Readers that keep re-opening while writers add, drop and re-add segments
/// and two concurrent GC runs claim, take over abandoned claims, and delete.
/// Every manifest a reader observes must be internally consistent: no
/// referenced segment is claimed for deletion, and every referenced segment
/// is readable. A read may only fail if the snapshot is older than the
/// retention window and the segment is no longer referenced by the current
/// manifest; anything else is a GC bug. This is the test that catches GC
/// deleting a segment a current manifest references.
#[test]
fn readers_always_see_consistent_manifests_under_writers_and_gc() {
    use bellwether_store::{gc_with, segment_path, GcOptions, StoreExt};
    use std::time::{Duration, Instant};

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ObjectStoreBackend::local(dir.path()).unwrap());
    let root = "idx";
    for n in 100..105 {
        add(&store, root, n);
    }
    // Abandoned claims on three segments nobody re-adds and on the
    // re-adder's whole pool (whose first re-adds are therefore refused until
    // a GC run takes the claim over, deletes and releases).
    let mut seed = StagedSegments::new();
    let doomed: Vec<SegmentRef> = (200..203).map(|n| segment(&mut seed, n)).collect();
    let pool_ids: Vec<SegmentRef> = (0..8).map(|n| segment(&mut seed, n)).collect();
    let abandoned: Vec<(SegmentRef, Bytes)> = doomed
        .iter()
        .chain(&pool_ids)
        .map(|s| (s.clone(), seed.get(&s.id).unwrap().clone()))
        .collect();
    seed_abandoned_claims(&store, root, &abandoned);

    let retention = Duration::from_millis(200);
    let run_for = Duration::from_millis(3000);
    let stop = AtomicBool::new(false);
    let deleted = AtomicUsize::new(0);
    let taken_over = AtomicUsize::new(0);
    let reads = AtomicUsize::new(0);
    let refused = AtomicUsize::new(0);
    let tolerated = AtomicUsize::new(0);

    std::thread::scope(|s| {
        // Appender.
        s.spawn(|| {
            let mut n = 1000;
            while !stop.load(Ordering::SeqCst) {
                add_retrying(&store, root, n);
                n += 1;
                std::thread::sleep(Duration::from_millis(5));
            }
        });
        // Compactor: keeps the live set small by dropping the oldest.
        s.spawn(|| {
            while !stop.load(Ordering::SeqCst) {
                commit(&*store, root, &StagedSegments::new(), |m: &Manifest| {
                    let mut m = m.clone();
                    if m.segments.len() > 6 {
                        m.segments.remove(0);
                    }
                    Ok(m)
                })
                .unwrap();
                std::thread::sleep(Duration::from_millis(7));
            }
        });
        // Re-adder: adds then drops identical bytes from a small pool, timed
        // so each segment is re-added after being unreferenced for about the
        // retention window: sometimes still tombstoned, sometimes claimed
        // mid-delete, sometimes already deleted. This is the race that lost
        // data.
        s.spawn(|| {
            let mut staged = StagedSegments::new();
            let pool: Vec<SegmentRef> = (0..8).map(|n| segment(&mut staged, n)).collect();
            // Each item is re-added `retention + jitter` after it was
            // dropped, landing in or around GC's claim-and-delete window.
            let mut due: Vec<Instant> = vec![Instant::now(); pool.len()];
            let mut i: u64 = 0;
            while !stop.load(Ordering::SeqCst) {
                let k = usize::try_from(i).unwrap() % pool.len();
                let sref = &pool[k];
                i += 1;
                let wait = due[k].saturating_duration_since(Instant::now());
                std::thread::sleep(wait);
                let res = commit(&*store, root, &staged, |m: &Manifest| {
                    let mut m = m.clone();
                    m.add_segment(sref.clone());
                    Ok(m)
                });
                match res {
                    Ok(_) => {
                        commit(&*store, root, &staged, |m: &Manifest| {
                            let mut m = m.clone();
                            m.segments.retain(|x| x.id != sref.id);
                            Ok(m)
                        })
                        .unwrap();
                    }
                    Err(e) if claimed_segment(&e).is_some() => {
                        refused.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(e) => panic!("re-add failed: {e}"),
                }
                due[k] = Instant::now() + retention + Duration::from_millis(i * 7 % 26);
            }
        });
        // Two concurrent GC runs, over a store whose deletes are slow, so
        // claims stand long enough for writers and the other run to collide
        // with them. One uses the defaults; the other a shorter (but still
        // pause-proof) lease and one id per batch, so it re-reads the
        // manifest before every delete.
        let gc_options = [
            GcOptions::default(),
            GcOptions {
                claim_lease: Duration::from_secs(4),
                delete_batch: 1,
                ..GcOptions::default()
            },
        ];
        for opts in gc_options {
            let (store, stop, deleted, taken_over) = (&store, &stop, &deleted, &taken_over);
            s.spawn(move || {
                let slow = SlowDelete((**store).clone());
                while !stop.load(Ordering::SeqCst) {
                    let r = gc_with(&slow, root, retention, opts).unwrap();
                    deleted.fetch_add(r.deleted, Ordering::SeqCst);
                    taken_over.fetch_add(r.stale_claims_taken_over, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(1));
                }
            });
        }
        // Readers.
        for _ in 0..2 {
            s.spawn(|| {
                while !stop.load(Ordering::SeqCst) {
                    let opened = Instant::now();
                    let snap = Snapshot::open(Arc::clone(&store), root).unwrap();
                    let m = snap.manifest();
                    let ids: BTreeSet<Hash> = m.segments.iter().map(|x| x.id).collect();
                    assert_eq!(ids.len(), m.segments.len(), "duplicate segment ids");
                    for sref in &m.segments {
                        assert!(
                            !m.is_claimed(&sref.id),
                            "manifest {} references claimed segment {}",
                            m.sequence,
                            sref.id
                        );
                        if let Err(e) = snap.read_segment(&sref.id) {
                            let age = opened.elapsed();
                            let fresh = snap.reopen().unwrap();
                            let still_referenced = fresh.manifest().references(&sref.id);
                            assert!(
                                age >= retention && !still_referenced,
                                "manifest {} references unreadable segment {} \
                                 (snapshot age {age:?}, still referenced by {}: {still_referenced}): {e}",
                                m.sequence,
                                sref.id,
                                fresh.sequence()
                            );
                            tolerated.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                    reads.fetch_add(1, Ordering::SeqCst);
                }
            });
        }
        let start = Instant::now();
        while start.elapsed() < run_for {
            std::thread::sleep(Duration::from_millis(20));
        }
        stop.store(true, Ordering::SeqCst);
    });

    eprintln!(
        "reads {}, gc deleted {}, abandoned claims taken over {}, re-adds refused by a claim {}, \
         slow-reader misses {}",
        reads.load(Ordering::SeqCst),
        deleted.load(Ordering::SeqCst),
        taken_over.load(Ordering::SeqCst),
        refused.load(Ordering::SeqCst),
        tolerated.load(Ordering::SeqCst),
    );
    assert!(
        deleted.load(Ordering::SeqCst) > 0,
        "GC never deleted; the test exercised nothing"
    );
    assert_eq!(
        taken_over.load(Ordering::SeqCst),
        abandoned.len(),
        "every abandoned claim is taken over exactly once"
    );
    for s in &doomed {
        assert!(!store.exists(&segment_path(root, &s.id)).unwrap());
    }
    assert!(reads.load(Ordering::SeqCst) > 10);
    // A final quiescent check of the current manifest.
    let snap = Snapshot::open(Arc::clone(&store), root).unwrap();
    for sref in snap.segments() {
        assert!(!snap.manifest().is_claimed(&sref.id));
        snap.read_segment(&sref.id).unwrap();
    }
}
