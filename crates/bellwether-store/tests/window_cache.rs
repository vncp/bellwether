//! The segment window cache behind [`Snapshot::read_segment_range`]. Every
//! range read through it equals the stored bytes; the
//! process-wide cache stays within [`SEGMENT_CACHE_BYTES`] however many
//! snapshots read at once; a snapshot's windows leave the cache with it;
//! and a snapshot never serves another snapshot's bytes, so a segment
//! damaged or deleted in the store after one snapshot cached it is read
//! afresh by the next (the damaged bytes, refused by the content check, or
//! a typed missing-segment error).
//!
//! One test function: the cache is process-wide, and its byte counts are
//! exact only while no other test in this binary reads segments.

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::collections::HashMap;
use std::sync::{Arc, Barrier};

use bellwether_store::{
    commit, segment_cache_bytes, segment_path, Extra, Manifest, ObjectStoreBackend, RowGroupRange,
    SegmentRef, Snapshot, StagedSegments, SEGMENT_CACHE_BYTES, SEGMENT_WINDOW_BYTES,
};
use bellwether_types::Hash;
use bytes::Bytes;

/// One segment: 2 MiB, 32 windows.
const SEG: usize = 2 << 20;
const W: u64 = SEGMENT_WINDOW_BYTES;

fn body(n: u32) -> Vec<u8> {
    (0..u32::try_from(SEG).unwrap())
        .map(|i| (i.wrapping_mul(2_654_435_761) ^ n.wrapping_mul(97)).to_le_bytes()[1])
        .collect()
}

/// A small deterministic generator for range ends.
struct Lcg(u64);

impl Lcg {
    fn below(&mut self, n: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % n
    }
}

fn slice(b: &[u8], r: &std::ops::Range<u64>) -> Vec<u8> {
    b[usize::try_from(r.start).unwrap()..usize::try_from(r.end).unwrap()].to_vec()
}

#[test]
fn windows_read_true_bytes_stay_bounded_across_snapshots_and_never_cross_snapshots() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ObjectStoreBackend::local(dir.path()).unwrap());
    let root = "idx";
    // 48 segments of 2 MiB: three times what the cache holds.
    let mut staged = StagedSegments::new();
    let mut refs = Vec::new();
    let mut bodies: HashMap<Hash, Vec<u8>> = HashMap::new();
    for n in 0..48u32 {
        let b = body(n);
        let id = staged.stage(Bytes::from(b.clone()));
        refs.push(SegmentRef {
            id,
            kind: None,
            key_spec: "exact:id".into(),
            rowgroup_range: RowGroupRange::new(n, n + 1),
            bytes: SEG as u64,
            join: None,
            extra: Extra::new(),
        });
        bodies.insert(id, b);
    }
    commit(&*store, root, &staged, |m: &Manifest| {
        let mut m = m.clone();
        for s in &refs {
            m.add_segment(s.clone());
        }
        Ok(m)
    })
    .unwrap();
    let ids: Vec<Hash> = refs.iter().map(|s| s.id).collect();
    assert_eq!(segment_cache_bytes(), 0);

    // Ranges inside one window, across windows, at the segment's end, as
    // wide as the cached limit and wider (fetched exactly): each equals
    // the stored bytes, read cold and again warm.
    let snap = Snapshot::open(Arc::clone(&store), root).unwrap();
    let size = SEG as u64;
    let mut ranges = vec![
        0..1,
        100..164,
        W - 6..W + 6,
        W..2 * W,
        3 * W + 1..19 * W - 1,
        3 * W..19 * W,
        2 * W..19 * W,
        size - 10..size,
        0..size,
    ];
    let mut rng = Lcg(7);
    for _ in 0..200 {
        let a = rng.below(size);
        let b = a + 1 + rng.below((size - a).min(5 * W));
        ranges.push(a..b);
    }
    for pass in ["cold", "warm"] {
        for (k, r) in ranges.iter().enumerate() {
            let id = ids[k % 3];
            let got = snap.read_segment_range(&id, r.clone()).unwrap();
            assert_eq!(&got[..], &slice(&bodies[&id], r)[..], "{pass} {r:?}");
        }
    }
    let held = segment_cache_bytes();
    assert!(held > 0 && held <= 3 * SEG, "{held}");
    // A clone shares the snapshot's windows: dropping it frees nothing.
    drop(snap.clone());
    assert_eq!(segment_cache_bytes(), held);

    // Eight snapshots at once, each reading every window of every segment
    // (96 MiB each): the cache never holds more than its cap.
    let threads = 8;
    let barrier = Arc::new(Barrier::new(threads));
    std::thread::scope(|s| {
        for t in 0..threads {
            let (store, barrier, ids, bodies) = (Arc::clone(&store), &barrier, &ids, &bodies);
            s.spawn(move || {
                let mine = Snapshot::open(store, root).unwrap();
                barrier.wait();
                for j in 0..ids.len() {
                    let id = &ids[(j + t * 5) % ids.len()];
                    for w in 0..size / W {
                        let r = w * W + 3..(w + 1) * W - 3;
                        let got = mine.read_segment_range(id, r.clone()).unwrap();
                        assert_eq!(&got[..], &slice(&bodies[id], &r)[..]);
                        let now = segment_cache_bytes();
                        assert!(now <= SEGMENT_CACHE_BYTES, "{now} cached");
                    }
                }
            });
        }
    });
    // Their windows left with them; the first snapshot's may have been
    // evicted, never grown.
    assert!(segment_cache_bytes() <= held);
    drop(snap);
    assert_eq!(segment_cache_bytes(), 0);

    // Damage after caching: the snapshot that cached window 0 keeps
    // serving the bytes of the segment id it pinned; a new snapshot reads
    // the store afresh and sees the damage, which the content check
    // refuses.
    let id = ids[5];
    let file = dir.path().join(segment_path(root, &id));
    let a = Snapshot::open(Arc::clone(&store), root).unwrap();
    let first = a.read_segment_range(&id, 0..16).unwrap();
    assert_eq!(&first[..], &bodies[&id][..16]);
    let mut damaged = std::fs::read(&file).unwrap();
    damaged[3] ^= 0xff;
    std::fs::write(&file, &damaged).unwrap();
    assert_eq!(a.read_segment_range(&id, 0..16).unwrap(), first);
    let b = Snapshot::open(Arc::clone(&store), root).unwrap();
    assert_eq!(
        &b.read_segment_range(&id, 0..16).unwrap()[..],
        &damaged[..16]
    );
    let err = b.read_segment(&id).unwrap_err().to_string();
    assert!(err.contains("does not match its id"), "{err}");

    // Deleted after caching (as GC deletes): a new snapshot's read is the
    // typed missing-segment error, and so is the old snapshot's read of a
    // window it never fetched.
    std::fs::remove_file(&file).unwrap();
    let c = Snapshot::open(Arc::clone(&store), root).unwrap();
    let err = c.read_segment_range(&id, 0..16).unwrap_err().to_string();
    assert!(err.contains("missing"), "{err}");
    let err = a
        .read_segment_range(&id, 5 * W..5 * W + 8)
        .unwrap_err()
        .to_string();
    assert!(err.contains("missing"), "{err}");
    drop((a, b, c));
    assert_eq!(segment_cache_bytes(), 0);
}
