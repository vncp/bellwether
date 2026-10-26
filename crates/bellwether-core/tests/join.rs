//! Join keys (B3): the pair segment's round trip, pinned conformance
//! vectors, nulls (ADR 014), retired row groups, refusals, an every-byte
//! corruption sweep, and a property test of the pair build, `semijoin`, and
//! `overlap_stats` against a brute-force oracle.
#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::cast_sign_loss
)]

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Once;

use arrow::datatypes::DataType;
use bellwether_core::encoding::{encode_tuple_as, Value};
use bellwether_core::join::{
    build_pair, check_comparable, decode_pair, header_pins, live_identity, one_sided,
    overlap_stats, pair_entries, pinned, semijoin, HeaderPins, JoinValue, OverlapStats, PairCaps,
    PairRows, Pins, Side,
};
use bellwether_core::segment::{ColumnType, Layout};
use bellwether_core::{merge, EncodedSegment, SegmentReader, SegmentWriter, WriterOptions};
use bellwether_types::{Hash, KeySpec, RowGroupId};
use proptest::prelude::*;
use roaring::RoaringBitmap;

fn join(counts: bool) -> KeySpec {
    KeySpec::Join {
        dataset: "orders".into(),
        columns: vec!["id".into()],
        right: "payables".into(),
        right_columns: vec!["order_id".into()],
        counts,
    }
}

fn exact(column: &str, counts: bool) -> KeySpec {
    KeySpec::Exact {
        columns: vec![column.into()],
        counts,
        bounds: None,
    }
}

fn int(i: i64) -> Vec<u8> {
    encode_tuple_as(&[Value::Int(i)], &[DataType::Int64]).unwrap()
}

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn rgs(ids: &[u32]) -> RoaringBitmap {
    ids.iter().copied().collect()
}

/// One exact segment over `range` holding `(key, row group, rows)`.
fn exact_segment(
    column: &str,
    counts: bool,
    rows: &[(Vec<u8>, u32, u64)],
    range: std::ops::Range<u32>,
    block_keys: usize,
) -> Vec<u8> {
    let mut w = SegmentWriter::new(
        exact(column, counts),
        WriterOptions {
            block_keys,
            ..WriterOptions::default()
        },
    )
    .unwrap();
    for (k, rg, n) in rows {
        w.insert_rows(k, RowGroupId(*rg), *n).unwrap();
    }
    w.set_rowgroup_range(RowGroupId(range.start)..RowGroupId(range.end))
        .unwrap();
    w.finish().unwrap().bytes
}

fn open(bytes: &[u8]) -> SegmentReader<Vec<u8>> {
    SegmentReader::open(bytes.to_vec()).unwrap()
}

/// The pins a test pair is built at: left sequence `left`, right sequence
/// `right`, and a right manifest identity derived from `right`.
fn pins(left: u64, right: u64) -> Pins {
    Pins {
        left,
        right,
        right_manifest: Hash::of(&right.to_le_bytes()),
    }
}

/// No cap: a pair is one segment.
const UNCAPPED: PairCaps = PairCaps {
    max_keys: u64::MAX,
    max_bytes: u64::MAX,
};

/// A pair's segments under `caps`, in the order `build_pair` emits them.
fn build_segments<RL: bellwether_core::segment::ReadAt, RR: bellwether_core::segment::ReadAt>(
    spec: &KeySpec,
    left: &Side<'_, RL>,
    right: &Side<'_, RR>,
    left_range: Option<(u32, u32)>,
    pins: Pins,
    opts: &WriterOptions,
    caps: PairCaps,
) -> bellwether_types::Result<Vec<EncodedSegment>> {
    let mut out = Vec::new();
    let n = build_pair(spec, left, right, left_range, pins, opts, caps, |s| {
        out.push(s);
        Ok(())
    })?;
    assert_eq!(
        n,
        out.len() as u64,
        "build_pair counts the segments it emits"
    );
    Ok(out)
}

/// An uncapped pair: its one segment.
fn build_one<RL: bellwether_core::segment::ReadAt, RR: bellwether_core::segment::ReadAt>(
    spec: &KeySpec,
    left: &Side<'_, RL>,
    right: &Side<'_, RR>,
    left_range: Option<(u32, u32)>,
    pins: Pins,
    opts: &WriterOptions,
) -> bellwether_types::Result<EncodedSegment> {
    let mut segs = build_segments(spec, left, right, left_range, pins, opts, UNCAPPED)?;
    assert_eq!(segs.len(), 1, "an uncapped pair is one segment");
    Ok(segs.pop().unwrap())
}

/// The pinned pair: left `orders.id` A=1 in rg 0 (3 rows) and rg 2 (1), B=2
/// in rg 1 (2), C=3 in rg 3 (1); right `payables.order_id` (its own ids)
/// A in rg 10 (1), C in rg 11 (4) and rg 12 (1), D=4 in rg 12 (2). Left
/// live 0..4, right live 10..13, pinned at left 5, right 7 (right manifest
/// identity `blake3(7u64 little-endian)`).
fn pinned_pair(counts: bool) -> Vec<u8> {
    let left = exact_segment(
        "id",
        counts,
        &[
            (int(1), 0, 3),
            (int(1), 2, 1),
            (int(2), 1, 2),
            (int(3), 3, 1),
        ],
        0..4,
        1024,
    );
    let right = exact_segment(
        "order_id",
        counts,
        &[
            (int(1), 10, 1),
            (int(3), 11, 4),
            (int(3), 12, 1),
            (int(4), 12, 2),
        ],
        10..13,
        1024,
    );
    let (l, r) = ([open(&left)], [open(&right)]);
    let (llive, rlive) = (rgs(&[0, 1, 2, 3]), rgs(&[10, 11, 12]));
    build_one(
        &join(counts),
        &Side {
            segments: &l,
            live: &llive,
            types: Some(vec![ColumnType::Int64]),
        },
        &Side {
            segments: &r,
            live: &rlive,
            types: Some(vec![ColumnType::Int32]),
        },
        Some((0, 3)),
        pins(5, 7),
        &WriterOptions::default(),
    )
    .unwrap()
    .bytes
}

#[test]
fn a_pair_segment_stores_shared_keys_per_side_and_counts_one_sided_keys() {
    let bytes = pinned_pair(true);
    let r = open(&bytes);
    assert_eq!(r.header().layout, Layout::KeyedJoin);
    assert_eq!(r.key_spec(), &join(true));
    assert_eq!(r.header().left_sequence, Some(5));
    assert_eq!(r.header().right_sequence, Some(7));
    assert_eq!(r.header().key_types, Some(vec![ColumnType::Int64]));
    assert_eq!(r.header().right_key_types, Some(vec![ColumnType::Int32]));
    // The recorded range is the left's: right ids never widen it.
    assert_eq!(r.rowgroup_range(), Some((RowGroupId(0), RowGroupId(3))));
    assert_eq!(r.entries(), 2);
    assert_eq!(one_sided(&r).unwrap(), (1, 1));
    let entries = pair_entries(&r).unwrap();
    let keys: Vec<Vec<u8>> = entries.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(keys, vec![int(1), int(3)]);
    let a = &entries[0].1;
    assert_eq!(a.left.bitmap, rgs(&[0, 2]));
    assert_eq!(a.left.counts, Some(vec![3, 1]));
    assert_eq!(a.right.bitmap, rgs(&[10]));
    assert_eq!(a.right.counts, Some(vec![1]));
    let c = &entries[1].1;
    assert_eq!(c.left.bitmap, rgs(&[3]));
    assert_eq!(c.right.bitmap, rgs(&[11, 12]));
    assert_eq!(c.right.counts, Some(vec![4, 1]));
    let want_pins = HeaderPins {
        left_sequence: 5,
        left_live: live_identity(&rgs(&[0, 1, 2, 3])),
        right_sequence: 7,
        right_manifest: Hash::of(&7u64.to_le_bytes()),
    };
    assert_eq!(header_pins(&r).unwrap(), want_pins);
    assert_eq!(pinned(&[r]).unwrap(), Some(want_pins));
    let r = open(&bytes);
    assert_eq!(
        overlap_stats(&[r]).unwrap(),
        OverlapStats {
            both: 2,
            left_only: 1,
            right_only: 1,
            max_right_row_groups: 2,
            sum_right_row_groups: 3,
            max_left_row_groups: 2,
            sum_left_row_groups: 3,
            rows: Some(PairRows {
                left: 5,
                right: 6,
                // per shared key, left rows times right rows: A 4 x 1, C 1 x 5
                join: 4 + 5,
            }),
        }
    );
    // semijoin: B is on the left only, D on the right only, 9 on neither:
    // none of them is in the answer.
    let r = open(&bytes);
    let hits = semijoin(&[r], &[int(1), int(2), int(4), int(9)]).unwrap();
    assert_eq!(hits.keys().cloned().collect::<Vec<_>>(), vec![int(1)]);
    // Uncounted: the same bitmaps, no counts, no rows.
    let u = open(&pinned_pair(false));
    let s = overlap_stats(std::slice::from_ref(&u)).unwrap();
    assert_eq!((s.both, s.left_only, s.right_only, s.rows), (2, 1, 1, None));
    assert!(pair_entries(&u)
        .unwrap()
        .iter()
        .all(|(_, v)| v.left.counts.is_none() && v.right.counts.is_none()));
}

/// The vectors `docs/format.md` ("Join keys") pins: the shared key `1`'s
/// value record (left record then right record, each `bitmap_len | bitmap
/// | counts`), the index's one-sided counts, and the segment's blake3.
#[test]
fn join_conformance_vectors() {
    let bytes = pinned_pair(true);
    let r = open(&bytes);
    let entries = pair_entries(&r).unwrap();
    let mut record = Vec::new();
    for side in [&entries[0].1.left, &entries[0].1.right] {
        let mut bm = Vec::new();
        side.bitmap.serialize_into(&mut bm).unwrap();
        record.extend_from_slice(&(bm.len() as u32).to_le_bytes());
        record.extend_from_slice(&bm);
        for n in side.counts.as_ref().unwrap() {
            record.push(*n as u8);
        }
    }
    assert_eq!(decode_pair(&record, true).unwrap(), entries[0].1);
    let vectors = [
        ("shared key `1`", hex(&int(1))),
        ("pair value of `1`", hex(&record)),
        (
            "one-sided counts (index suffix)",
            hex(&[1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]),
        ),
        ("pinned pair blake3", Hash::of(&bytes).to_hex()),
    ];
    // The record is in the segment, and the index ends with the one-sided
    // counts, right before the 32-byte footer.
    assert!(bytes.windows(record.len()).any(|w| w == record.as_slice()));
    let n = bytes.len();
    assert_eq!(
        &bytes[n - 32 - 16..n - 32],
        &[1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]
    );
    let doc = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/format.md"))
        .unwrap();
    let rows: Vec<String> = vectors
        .iter()
        .map(|(name, value)| format!("| {name} | `{value}` |"))
        .collect();
    let section = doc.split("## Join keys").nth(1).unwrap_or_else(|| {
        panic!(
            "docs/format.md needs a \"Join keys\" section pinning\n{}",
            rows.join("\n")
        )
    });
    for (name, value) in vectors {
        let row = format!("| {name} | `{value}` |");
        assert!(
            section.contains(&row),
            "docs/format.md (\"Join keys\") must pin the row\n{row}"
        );
    }
}

/// ADR 014: a stored tuple with a trailing null marker equals nothing, so
/// a composite key holding one is one-sided on both sides; a null probe
/// matches nothing.
#[test]
fn tuples_with_a_null_never_pair_and_null_probes_match_nothing() {
    let spec = KeySpec::Join {
        dataset: "l".into(),
        columns: vec!["a".into(), "b".into()],
        right: "r".into(),
        right_columns: vec!["a".into(), "b".into()],
        counts: false,
    };
    let composite = |a: i64, b: Option<i64>| {
        let mut k = int(a);
        match b {
            Some(b) => k.extend(int(b)),
            None => k.push(0x00),
        }
        k
    };
    let seg = |rows: &[(Vec<u8>, u32)]| {
        let mut w = SegmentWriter::new(
            KeySpec::Exact {
                columns: vec!["a".into(), "b".into()],
                counts: false,
                bounds: None,
            },
            WriterOptions::default(),
        )
        .unwrap();
        for (k, rg) in rows {
            w.insert(k, RowGroupId(*rg)).unwrap();
        }
        w.set_rowgroup_range(RowGroupId(0)..RowGroupId(2)).unwrap();
        open(&w.finish().unwrap().bytes)
    };
    let l = [seg(&[(composite(1, None), 0), (composite(1, Some(2)), 1)])];
    let r = [seg(&[(composite(1, None), 0), (composite(1, Some(2)), 0)])];
    let live = rgs(&[0, 1]);
    let bytes = build_one(
        &spec,
        &Side {
            segments: &l,
            live: &live,
            types: None,
        },
        &Side {
            segments: &r,
            live: &live,
            types: None,
        },
        Some((0, 1)),
        pins(1, 1),
        &WriterOptions::default(),
    )
    .unwrap()
    .bytes;
    let p = open(&bytes);
    let keys: Vec<Vec<u8>> = pair_entries(&p).unwrap().into_iter().map(|e| e.0).collect();
    assert_eq!(keys, vec![composite(1, Some(2))]);
    assert_eq!(one_sided(&p).unwrap(), (1, 1));
    assert!(semijoin(&[p], &[composite(1, None)]).unwrap().is_empty());
}

/// Row groups retired by drift (ADR 013) are not part of either side: a
/// key held only in a retired row group is absent there, and a shared
/// key's bitmaps and counts keep only live row groups.
#[test]
fn retired_row_groups_are_dropped_from_both_sides() {
    let left = exact_segment(
        "id",
        true,
        &[(int(1), 0, 2), (int(1), 1, 5), (int(2), 1, 1)],
        0..2,
        1024,
    );
    let right = exact_segment(
        "order_id",
        true,
        &[(int(1), 0, 1), (int(2), 0, 1)],
        0..1,
        1024,
    );
    let (l, r) = ([open(&left)], [open(&right)]);
    let bytes = build_one(
        &join(true),
        &Side {
            segments: &l,
            live: &rgs(&[0]),
            types: None,
        },
        &Side {
            segments: &r,
            live: &rgs(&[0]),
            types: None,
        },
        Some((0, 0)),
        pins(2, 1),
        &WriterOptions::default(),
    )
    .unwrap()
    .bytes;
    let p = open(&bytes);
    let e = pair_entries(&p).unwrap();
    assert_eq!(e.len(), 1);
    assert_eq!(e[0].0, int(1));
    assert_eq!(e[0].1.left.bitmap, rgs(&[0]));
    assert_eq!(e[0].1.left.counts, Some(vec![2]));
    assert_eq!(one_sided(&p).unwrap(), (0, 1));
}

#[test]
fn incomparable_and_float_join_columns_and_wrong_sides_are_refused() {
    let spec = join(false);
    let err = check_comparable(&spec, &[ColumnType::Int64], &[ColumnType::Utf8]).unwrap_err();
    assert!(err.to_string().contains("never encode alike"), "{err}");
    let err = check_comparable(&spec, &[ColumnType::Int64], &[ColumnType::UInt64]).unwrap_err();
    assert!(err.to_string().contains("signed integer"), "{err}");
    let err = check_comparable(&spec, &[ColumnType::Float64], &[ColumnType::Float64]).unwrap_err();
    assert!(err.to_string().contains("-0.0 and 0.0"), "{err}");
    let dec = |scale| ColumnType::Decimal128 {
        precision: 18,
        scale,
    };
    assert!(check_comparable(&spec, &[dec(2)], &[dec(4)]).is_err());
    // Integers of any width, strings of either size, pair.
    check_comparable(&spec, &[ColumnType::Int32], &[ColumnType::Int64]).unwrap();
    check_comparable(&spec, &[ColumnType::Utf8], &[ColumnType::LargeUtf8]).unwrap();
    // A counted join key needs counted exact keys on both sides.
    let uncounted = exact_segment("id", false, &[(int(1), 0, 1)], 0..1, 1024);
    let counted = exact_segment("order_id", true, &[(int(1), 0, 1)], 0..1, 1024);
    let live = rgs(&[0]);
    let (l, r) = ([open(&uncounted)], [open(&counted)]);
    let err = build_one(
        &join(true),
        &Side {
            segments: &l,
            live: &live,
            types: None,
        },
        &Side {
            segments: &r,
            live: &live,
            types: None,
        },
        Some((0, 0)),
        pins(1, 1),
        &WriterOptions::default(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("exact:id,counts"), "{err}");
    // An exact key on other columns is not a side of this join key.
    let other = exact_segment("other", false, &[(int(1), 0, 1)], 0..1, 1024);
    let o = [open(&other)];
    let err = build_one(
        &join(false),
        &Side {
            segments: &o,
            live: &live,
            types: None,
        },
        &Side {
            segments: &r,
            live: &live,
            types: None,
        },
        Some((0, 0)),
        pins(1, 1),
        &WriterOptions::default(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("exact key on id"), "{err}");
    // A join key is never written row by row, and never merged.
    assert!(SegmentWriter::new(join(false), WriterOptions::default()).is_err());
    let p = pinned_pair(false);
    assert!(merge(&[open(&p), open(&p)], &WriterOptions::default()).is_err());
    // Its values are pairs: a key lookup on it is refused, not misread.
    assert!(open(&p).get(&[int(1)]).is_err());
    // A dataset name that is not one folder name is refused.
    let mut bad = join(false);
    if let KeySpec::Join { right, .. } = &mut bad {
        *right = "../payables".into();
    }
    assert!(bellwether_core::join::check_spec(&bad).is_err());
}

// -- corruption sweep -------------------------------------------------------

thread_local! {
    static QUIET: Cell<bool> = const { Cell::new(false) };
}
static HOOK: Once = Once::new();

fn install_hook() {
    HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if !QUIET.with(Cell::get) {
                previous(info);
            }
        }));
    });
}

type Answer = (
    Vec<(Vec<u8>, JoinValue)>,
    OverlapStats,
    BTreeMap<Vec<u8>, JoinValue>,
);

/// Every join read path on `bytes`: the whole-segment read, the overlap
/// statistics, and a semi-join; `None` when any refuses.
fn read_all(bytes: &[u8]) -> Option<Answer> {
    let r = SegmentReader::open(bytes.to_vec()).ok()?;
    let entries = pair_entries(&r).ok()?;
    let readers = [r];
    let stats = overlap_stats(&readers).ok()?;
    let probes: Vec<Vec<u8>> = (0..12).map(int).collect();
    let hits = semijoin(&readers, &probes).ok()?;
    Some((entries, stats, hits))
}

/// Every truncation, and every byte value at every position, of a pair
/// segment with several blocks: opening and every read path refuse or give
/// the clean answer, and never panic. Every region a read touches is
/// checksummed, so a damaged byte is refused, never read as another pair.
#[test]
fn every_byte_of_a_pair_segment_corrupted_is_refused_never_misread() {
    install_hook();
    let rows: Vec<(Vec<u8>, u32, u64)> = (0..10)
        .map(|k| (int(k), (k % 3) as u32, 1 + k as u64))
        .collect();
    let left = exact_segment("id", true, &rows, 0..3, 2);
    let right = exact_segment("order_id", true, &rows[2..], 0..3, 2);
    let (l, r) = ([open(&left)], [open(&right)]);
    let live = rgs(&[0, 1, 2]);
    let clean = build_one(
        &join(true),
        &Side {
            segments: &l,
            live: &live,
            types: None,
        },
        &Side {
            segments: &r,
            live: &live,
            types: None,
        },
        Some((0, 2)),
        pins(3, 4),
        &WriterOptions {
            block_keys: 2,
            ..WriterOptions::default()
        },
    )
    .unwrap()
    .bytes;
    let want = read_all(&clean).unwrap();
    assert_eq!(want.0.len(), 8);
    let check = |bad: &[u8], what: &str| {
        QUIET.with(|q| q.set(true));
        let got = catch_unwind(AssertUnwindSafe(|| read_all(bad)));
        QUIET.with(|q| q.set(false));
        match got {
            Err(panic) => panic!("{what}: a read panicked: {panic:?}"),
            Ok(Some(answer)) => assert!(answer == want, "{what}: read a different answer"),
            Ok(None) => {}
        }
    };
    for n in 0..clean.len() {
        check(&clean[..n], &format!("truncated to {n}"));
    }
    let full = std::env::var("BELLWETHER_FULL_SWEEP").is_ok();
    let values: Vec<u8> = if full {
        (0..=255).collect()
    } else {
        vec![0x00, 0xff, 0x01, 0x80, 0x7f]
    };
    for i in 0..clean.len() {
        let mut variants: BTreeSet<u8> = values.iter().copied().collect();
        variants.extend([clean[i] ^ 0x01, clean[i] ^ 0x10, clean[i].wrapping_add(1)]);
        for v in variants {
            if v == clean[i] {
                continue;
            }
            let mut bad = clean.clone();
            bad[i] = v;
            check(&bad, &format!("byte {i} set to {v:#04x}"));
        }
    }
}

// -- property test against a brute-force oracle ----------------------------

/// `(key, row group)` rows of one side; keys 0..16, row groups 0..6.
fn side_rows() -> impl Strategy<Value = Vec<(u8, u8)>> {
    prop::collection::vec((0u8..16, 0u8..6), 0..40)
}

/// One side's exact segments: rows split at row group 3 into two segments
/// (as a build's contiguous groups), counted.
fn side_segments(column: &str, rows: &[(u8, u8)]) -> Vec<Vec<u8>> {
    let mut counted: BTreeMap<(u8, u8), u64> = BTreeMap::new();
    for r in rows {
        *counted.entry(*r).or_default() += 1;
    }
    [(0u8, 3u8), (3, 6)]
        .iter()
        .map(|(lo, hi)| {
            let part: Vec<(Vec<u8>, u32, u64)> = counted
                .iter()
                .filter(|((_, rg), _)| (*lo..*hi).contains(rg))
                .map(|((k, rg), n)| (int(i64::from(*k)), u32::from(*rg), *n))
                .collect();
            exact_segment(column, true, &part, u32::from(*lo)..u32::from(*hi), 3)
        })
        .collect()
}

/// Per key, the live row groups holding it and their rows.
fn oracle_side(rows: &[(u8, u8)], live: &RoaringBitmap) -> BTreeMap<u8, BTreeMap<u32, u64>> {
    let mut out: BTreeMap<u8, BTreeMap<u32, u64>> = BTreeMap::new();
    for (k, rg) in rows {
        if live.contains(u32::from(*rg)) {
            *out.entry(*k)
                .or_default()
                .entry(u32::from(*rg))
                .or_default() += 1;
        }
    }
    out
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// The pair index equals the oracle over the rows: exactly the keys
    /// both sides hold in live row groups, each side's live row groups and
    /// rows; the one-sided counts; the overlap statistics; and
    /// `semijoin(probes)` keeps exactly the probes both sides hold, with
    /// every row group holding them on each side (invariant 1 per side).
    /// Under a key cap (ADR 018) the pair splits by key range into
    /// `ceil(|L ∩ R| / cap)` segments (at least one), in key order, that
    /// together give the same answers.
    #[test]
    fn the_pair_index_equals_the_oracle(
        left in side_rows(),
        right in side_rows(),
        lretired in prop::collection::btree_set(0u32..6, 0..2),
        rretired in prop::collection::btree_set(0u32..6, 0..2),
        probes in prop::collection::btree_set(0u8..20, 0..10),
        block_keys in 1usize..4,
        max_keys in prop_oneof![Just(u64::MAX), 1u64..4],
    ) {
        let llive: RoaringBitmap = (0u32..6).filter(|i| !lretired.contains(i)).collect();
        let rlive: RoaringBitmap = (0u32..6).filter(|i| !rretired.contains(i)).collect();
        let l: Vec<_> = side_segments("id", &left).iter().map(|b| open(b)).collect();
        let r: Vec<_> = side_segments("order_id", &right).iter().map(|b| open(b)).collect();
        let segs = build_segments(
            &join(true),
            &Side { segments: &l, live: &llive, types: None },
            &Side { segments: &r, live: &rlive, types: None },
            Some((0, 5)),
            pins(1, 1),
            &WriterOptions { block_keys, ..WriterOptions::default() },
            PairCaps { max_keys, max_bytes: u64::MAX },
        )
        .unwrap();
        let (ol, or) = (oracle_side(&left, &llive), oracle_side(&right, &rlive));
        let both: BTreeSet<u8> = ol.keys().filter(|k| or.contains_key(k)).copied().collect();
        let n = both.len() as u64;
        prop_assert_eq!(segs.len() as u64, if n == 0 { 1 } else { n.div_ceil(max_keys) });
        let p: Vec<_> = segs.iter().map(|s| open(&s.bytes)).collect();
        let mut entries = Vec::new();
        let mut one = (0u64, 0u64);
        for s in &p {
            prop_assert!(s.entries() <= max_keys);
            prop_assert_eq!(s.rowgroup_range(), Some((RowGroupId(0), RowGroupId(5))));
            let (lo, ro) = one_sided(s).unwrap();
            one = (one.0 + lo, one.1 + ro);
            entries.extend(pair_entries(s).unwrap());
        }
        prop_assert_eq!(one, ((ol.len() - both.len()) as u64, (or.len() - both.len()) as u64));
        prop_assert!(pinned(&p).unwrap().is_some());
        prop_assert_eq!(entries.len(), both.len());
        let mut join_rows = 0u64;
        for ((key, v), k) in entries.iter().zip(&both) {
            prop_assert_eq!(key, &int(i64::from(*k)));
            let (lw, rw) = (&ol[k], &or[k]);
            prop_assert_eq!(&v.left.bitmap, &lw.keys().copied().collect::<RoaringBitmap>());
            prop_assert_eq!(v.left.counts.clone().unwrap(), lw.values().copied().collect::<Vec<_>>());
            prop_assert_eq!(&v.right.bitmap, &rw.keys().copied().collect::<RoaringBitmap>());
            prop_assert_eq!(v.right.counts.clone().unwrap(), rw.values().copied().collect::<Vec<_>>());
            join_rows += lw.values().sum::<u64>() * rw.values().sum::<u64>();
        }
        let stats = overlap_stats(&p).unwrap();
        prop_assert_eq!(stats.both, both.len() as u64);
        prop_assert_eq!(stats.left_only, (ol.len() - both.len()) as u64);
        prop_assert_eq!(stats.right_only, (or.len() - both.len()) as u64);
        prop_assert_eq!(stats.rows.unwrap().join, join_rows);
        prop_assert_eq!(
            stats.max_right_row_groups,
            both.iter().map(|k| or[k].len() as u64).max().unwrap_or(0)
        );
        let asked: Vec<Vec<u8>> = probes.iter().map(|k| int(i64::from(*k))).collect();
        let hits = semijoin(&p, &asked).unwrap();
        let want: BTreeSet<u8> = probes.intersection(&both).copied().collect();
        prop_assert_eq!(hits.len(), want.len());
        for k in &want {
            let v = &hits[&int(i64::from(*k))];
            // Invariant 1 per side: every live row group holding the key.
            for rg in ol[k].keys() {
                prop_assert!(v.left.bitmap.contains(*rg));
            }
            for rg in or[k].keys() {
                prop_assert!(v.right.bitmap.contains(*rg));
            }
        }
    }
}

/// ADR 018: a byte cap splits the pair too, before the key that would take
/// the open segment past it, so the open segment stays near the cap
/// whatever `|L ∩ R|` is; every segment carries the same pins and the
/// whole left range, and together they answer as the uncapped pair does.
#[test]
fn a_small_byte_cap_splits_the_pair_into_bounded_segments_with_the_same_answers() {
    let rows: Vec<(Vec<u8>, u32, u64)> = (0..200)
        .map(|k| (int(k), (k % 4) as u32, 1 + k as u64 % 3))
        .collect();
    let left = exact_segment("id", true, &rows, 0..4, 16);
    let right = exact_segment("order_id", true, &rows[50..], 0..4, 16);
    let (l, r) = ([open(&left)], [open(&right)]);
    let live = rgs(&[0, 1, 2, 3]);
    let side = |s| Side {
        segments: s,
        live: &live,
        types: None,
    };
    let opts = WriterOptions {
        block_keys: 8,
        ..WriterOptions::default()
    };
    let whole = build_one(
        &join(true),
        &side(&l),
        &side(&r),
        Some((0, 3)),
        pins(2, 9),
        &opts,
    )
    .unwrap();
    let caps = PairCaps {
        max_keys: u64::MAX,
        max_bytes: 512,
    };
    let segs = build_segments(
        &join(true),
        &side(&l),
        &side(&r),
        Some((0, 3)),
        pins(2, 9),
        &opts,
        caps,
    )
    .unwrap();
    assert!(
        segs.len() > 4,
        "150 shared keys under 512 bytes: {} segments",
        segs.len()
    );
    let whole = open(&whole.bytes);
    let parts: Vec<_> = segs.iter().map(|s| open(&s.bytes)).collect();
    let mut last: Option<Vec<u8>> = None;
    for (s, p) in segs.iter().zip(&parts) {
        // The open segment is finished at the first key past the cap: one
        // key's record (a few dozen bytes here) and the footer over it.
        assert!(s.bytes.len() < 1024, "segment of {} bytes", s.bytes.len());
        assert_eq!(header_pins(p).unwrap(), header_pins(&whole).unwrap());
        assert_eq!(p.rowgroup_range(), Some((RowGroupId(0), RowGroupId(3))));
        let keys: Vec<Vec<u8>> = pair_entries(p).unwrap().into_iter().map(|e| e.0).collect();
        // Key ranges ascend and never overlap: each key is in one segment.
        assert!(last.as_ref().is_none_or(|k| k < &keys[0]));
        last = keys.last().cloned();
    }
    let all: Vec<_> = parts
        .iter()
        .flat_map(|p| pair_entries(p).unwrap())
        .collect();
    assert_eq!(all, pair_entries(&whole).unwrap());
    assert_eq!(
        overlap_stats(&parts).unwrap(),
        overlap_stats(std::slice::from_ref(&whole)).unwrap()
    );
    let probes: Vec<Vec<u8>> = (0..210).map(int).collect();
    assert_eq!(
        semijoin(&parts, &probes).unwrap(),
        semijoin(std::slice::from_ref(&whole), &probes).unwrap()
    );
}
