//! Tombstone segments (B8, `docs/format.md` "Tombstones"):
//!
//! - round trip against a brute-force oracle (`proptest`): random rows over
//!   several row groups, written as one segment, as two over disjoint row
//!   groups, and as their merge, read back entry for entry; the dataset-wide
//!   count of keys whose newest row is a tombstone; and the pruning rule,
//!   which for `latest` keeps exactly the row groups holding a live row at
//!   a key's newest time and for `asof` never drops one (invariant 1);
//! - nulls (ADR 014): a null key is not indexed, a row with a null time is
//!   no version, a null tombstone column is not live; a key whose only rows
//!   are tombstones has no live time;
//! - a state column (`tombstone=status=VOID`) and a bitemporal key's
//!   two-clock entries;
//! - the pinned value records, byte for byte, and the table in
//!   `docs/format.md` lists exactly these vectors;
//! - every truncation and a byte-flip sweep at every position, and every
//!   byte value at every body position: never a panic, and never an answer
//!   other than the clean one (the checksums refuse a corrupt record).
#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::too_many_lines
)]

use std::cell::Cell;
use std::collections::BTreeMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;
use std::sync::{Arc, Once};

use arrow::array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether_core::encoding::{encode_tuple, Value};
use bellwether_core::segment::{checksum, PREAMBLE_LEN};
use bellwether_core::tombstone::{self, Liveness, NewestTombstones};
use bellwether_core::{merge, SegmentReader, SegmentWriter, WriterOptions};
use bellwether_types::{Hash, KeySpec, RowGroupId, Tombstone};
use proptest::prelude::*;
use roaring::RoaringBitmap;

const SYMS: [&str; 4] = ["A", "B", "C", ""];

#[derive(Clone, Debug)]
struct Row {
    sym: Option<&'static str>,
    ts: Option<i64>,
    /// `Some(true)`: a tombstone; `Some(false)`: live; `None`: null, not
    /// live.
    void: Option<bool>,
}

fn row() -> impl Strategy<Value = Row> {
    (
        prop::option::weighted(0.9, prop::sample::select(SYMS.to_vec())),
        prop::option::weighted(0.85, 0i64..12),
        prop::option::weighted(0.9, any::<bool>()),
    )
        .prop_map(|(sym, ts, void)| Row { sym, ts, void })
}

fn layout() -> impl Strategy<Value = Vec<Vec<Row>>> {
    prop::collection::vec(prop::collection::vec(row(), 0..16), 1..10)
}

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("sym", DataType::Utf8, true),
        Field::new("ts", DataType::Int64, true),
        Field::new("void", DataType::Boolean, true),
        Field::new("status", DataType::Utf8, true),
    ])
}

/// `void` as given; `status` is `"VOID"` exactly where `void` is true,
/// `"OK"` where false, null where null, so both spellings mark the same
/// rows.
fn batch(rows: &[Row]) -> RecordBatch {
    let sym: ArrayRef = Arc::new(StringArray::from(
        rows.iter().map(|r| r.sym).collect::<Vec<_>>(),
    ));
    let ts: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|r| r.ts).collect::<Vec<_>>(),
    ));
    let void: ArrayRef = Arc::new(BooleanArray::from(
        rows.iter().map(|r| r.void).collect::<Vec<_>>(),
    ));
    let status: ArrayRef = Arc::new(StringArray::from(
        rows.iter()
            .map(|r| r.void.map(|v| if v { "VOID" } else { "OK" }))
            .collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(Arc::new(schema()), vec![sym, ts, void, status]).unwrap()
}

fn spec(value: Option<&str>) -> KeySpec {
    KeySpec::Temporal {
        columns: vec!["sym".into()],
        ts_column: "ts".into(),
        counts: false,
        tombstone: Some(Tombstone {
            column: if value.is_some() { "status" } else { "void" }.into(),
            value: value.map(str::to_string),
        }),
    }
}

fn key(sym: &str) -> Vec<u8> {
    encode_tuple(&[Value::Utf8(sym)]).unwrap()
}

fn probes() -> Vec<Vec<u8>> {
    SYMS.iter().map(|s| key(s)).collect()
}

/// One segment over the row groups `rgs` of `layout`.
fn build(spec: &KeySpec, layout: &[Vec<Row>], rgs: std::ops::Range<usize>) -> Vec<u8> {
    let mut w =
        SegmentWriter::for_dataset(spec.clone(), &schema(), WriterOptions::default()).unwrap();
    w.set_rowgroup_range(RowGroupId(rgs.start as u32)..RowGroupId(rgs.end as u32))
        .unwrap();
    for rg in rgs {
        w.add_batch(RowGroupId(rg as u32), &batch(&layout[rg]))
            .unwrap();
    }
    w.finish().unwrap().bytes
}

fn open(bytes: &[Vec<u8>]) -> Vec<SegmentReader<Vec<u8>>> {
    bytes
        .iter()
        .map(|b| SegmentReader::open(b.clone()).unwrap())
        .collect()
}

type Entries = BTreeMap<Vec<u8>, BTreeMap<u32, Liveness>>;

/// The oracle's entries: per key and row group holding a timed row of it,
/// the newest time, the newest live time, and whether no live row is at
/// the newest time.
fn oracle(layout: &[Vec<Row>]) -> Entries {
    let mut out = Entries::new();
    for (rg, rows) in layout.iter().enumerate() {
        let mut by: BTreeMap<&str, Vec<(i64, bool)>> = BTreeMap::new();
        for r in rows {
            if let (Some(s), Some(t)) = (r.sym, r.ts) {
                by.entry(s).or_default().push((t, r.void == Some(false)));
            }
        }
        for (s, v) in by {
            let newest = v.iter().map(|x| x.0).max().unwrap();
            let live = v.iter().filter(|x| x.1).map(|x| x.0).max();
            out.entry(key(s)).or_default().insert(
                rg as u32,
                Liveness {
                    newest: (newest, 0),
                    live: live.map(|l| (l, 0)),
                    newest_is_tombstone: !v.iter().any(|x| x.0 == newest && x.1),
                },
            );
        }
    }
    out
}

/// Per key: the winning time at or before `asof` (all times for `None`),
/// the row groups holding a row of the key at it, and those holding a live
/// row of the key at it.
fn winners(
    layout: &[Vec<Row>],
    asof: Option<i64>,
) -> BTreeMap<&'static str, (i64, RoaringBitmap, RoaringBitmap)> {
    let mut out = BTreeMap::new();
    for s in SYMS {
        let times = layout
            .iter()
            .flatten()
            .filter(|r| r.sym == Some(s))
            .filter_map(|r| r.ts)
            .filter(|t| asof.is_none_or(|a| *t <= a));
        let Some(w) = times.max() else { continue };
        let (mut at, mut live) = (RoaringBitmap::new(), RoaringBitmap::new());
        for (rg, rows) in layout.iter().enumerate() {
            for r in rows.iter().filter(|r| r.sym == Some(s) && r.ts == Some(w)) {
                at.insert(rg as u32);
                if r.void == Some(false) {
                    live.insert(rg as u32);
                }
            }
        }
        out.insert(s, (w, at, live));
    }
    out
}

fn newest_oracle(layout: &[Vec<Row>]) -> NewestTombstones {
    let mut out = NewestTombstones::default();
    for (_, (_, _, live)) in winners(layout, None) {
        out.keys += 1;
        out.newest_is_tombstone += u64::from(live.is_empty());
    }
    out
}

fn all_rgs(layout: &[Vec<Row>]) -> RoaringBitmap {
    (0..layout.len() as u32).collect()
}

fn check_layout(layout: &[Vec<Row>], value: Option<&str>) {
    let spec = spec(value);
    let n = layout.len();
    let whole = vec![build(&spec, layout, 0..n)];
    let mid = n / 2;
    let split: Vec<Vec<u8>> = if mid == 0 {
        whole.clone()
    } else {
        vec![build(&spec, layout, 0..mid), build(&spec, layout, mid..n)]
    };
    let merged = vec![
        merge::merge(&open(&split), &WriterOptions::default())
            .unwrap()
            .bytes,
    ];
    let want = oracle(layout);
    for segs in [&whole, &split, &merged] {
        let readers = open(segs);
        let got = tombstone::liveness(&readers, &probes()).unwrap();
        assert_eq!(got, want);
        assert_eq!(
            tombstone::newest_tombstones(&readers, &all_rgs(layout)).unwrap(),
            newest_oracle(layout)
        );
        for asof in [None, Some(0), Some(3), Some(6), Some(9), Some(11)] {
            for (s, (w, at, live)) in winners(layout, asof) {
                let entries = got.get(&key(s)).unwrap();
                let kept = tombstone::prune(&at, (w, 0), &[entries]).unwrap();
                // invariant 1: every row group with a live row at the
                // winner stays; nothing outside the winners is added
                assert!(live.is_subset(&kept), "{s} at {asof:?}");
                assert!(kept.is_subset(&at), "{s} at {asof:?}");
                if asof.is_none() {
                    // `latest`: exact
                    assert_eq!(kept, live, "{s}");
                }
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]
    #[test]
    fn segments_round_trip_and_prune_as_the_oracle_says(layout in layout()) {
        check_layout(&layout, None);
    }

    #[test]
    fn a_state_column_marks_the_same_rows(layout in layout()) {
        check_layout(&layout, Some("VOID"));
    }
}

#[test]
fn nulls_follow_adr_014() {
    let r = |sym, ts, void| Row { sym, ts, void };
    let layout = vec![
        vec![
            // a null key is not indexed
            r(None, Some(5), Some(false)),
            // a null time is no version
            r(Some("A"), None, Some(false)),
            r(Some("A"), Some(1), Some(false)),
            // a null tombstone column is not live
            r(Some("B"), Some(4), None),
            // a key whose only rows are tombstones
            r(Some("C"), Some(2), Some(true)),
            r(Some("C"), Some(3), Some(true)),
        ],
        vec![
            // a key with only null times has no entry
            r(Some(""), None, Some(false)),
            // resurrected: a tombstone, then a later live row
            r(Some("A"), Some(2), Some(true)),
            r(Some("A"), Some(3), Some(false)),
        ],
    ];
    let bytes = build(&spec(None), &layout, 0..2);
    let got = tombstone::liveness(&open(std::slice::from_ref(&bytes)), &probes()).unwrap();
    assert_eq!(got, oracle(&layout));
    assert!(!got.contains_key(&key("")));
    assert_eq!(
        got[&key("B")][&0],
        Liveness {
            newest: (4, 0),
            live: None,
            newest_is_tombstone: true
        }
    );
    assert_eq!(
        got[&key("C")][&0],
        Liveness {
            newest: (3, 0),
            live: None,
            newest_is_tombstone: true
        }
    );
    assert_eq!(
        got[&key("A")][&1],
        Liveness {
            newest: (3, 0),
            live: Some((3, 0)),
            newest_is_tombstone: false
        }
    );
    // A, newest live; B and C, newest a tombstone
    assert_eq!(
        tombstone::newest_tombstones(&open(&[bytes]), &all_rgs(&layout)).unwrap(),
        NewestTombstones {
            keys: 3,
            newest_is_tombstone: 2
        }
    );
    // only-tombstones C: its winning row group is pruned; resurrected A:
    // the later live row's row group stays, the earlier one is not a winner
    let c = &got[&key("C")];
    assert!(
        tombstone::prune(&[0u32].into_iter().collect(), (3, 0), &[c])
            .unwrap()
            .is_empty()
    );
    let a = &got[&key("A")];
    assert_eq!(
        tombstone::prune(&[1u32].into_iter().collect(), (3, 0), &[a]).unwrap(),
        [1u32].into_iter().collect::<RoaringBitmap>()
    );
    // asof 2: A's winner is the tombstone at 2 in row group 1 (the live
    // row at 1 in row group 0 is older, never read). Row group 1's newest
    // live time, 3, is after the winner, so the entry cannot tell and the
    // row group stays; the residual (`Live` after `LatestPerKey`) drops the
    // tombstone row, so the answer is no row
    assert_eq!(
        tombstone::prune(&[1u32].into_iter().collect(), (2, 0), &[a]).unwrap(),
        [1u32].into_iter().collect::<RoaringBitmap>()
    );
}

#[test]
fn a_row_group_the_entries_do_not_hold_is_refused_never_pruned() {
    let layout = vec![vec![Row {
        sym: Some("A"),
        ts: Some(4),
        void: Some(false),
    }]];
    let got = tombstone::liveness(&open(&[build(&spec(None), &layout, 0..1)]), &probes()).unwrap();
    let a = &got[&key("A")];
    // row group 7 holds no entry of A, and row group 0's newest is older
    // than the winner: both are a corrupt index
    assert!(tombstone::prune(&[7u32].into_iter().collect(), (4, 0), &[a]).is_err());
    assert!(tombstone::prune(&[0u32].into_iter().collect(), (5, 0), &[a]).is_err());
}

#[test]
fn bitemporal_entries_hold_both_clocks() {
    let spec = KeySpec::Bitemporal {
        columns: vec!["sym".into()],
        valid_column: "valid".into(),
        known_column: "known".into(),
        tombstone: Some(Tombstone {
            column: "void".into(),
            value: None,
        }),
    };
    let mut w = SegmentWriter::new(spec, WriterOptions::default()).unwrap();
    let a = key("A");
    w.insert_liveness(&a, RowGroupId(0), (3, 10), true).unwrap();
    w.insert_liveness(&a, RowGroupId(0), (3, 12), false)
        .unwrap();
    w.insert_liveness(&a, RowGroupId(1), (2, 20), true).unwrap();
    let r = open(&[w.finish().unwrap().bytes]);
    let got = tombstone::liveness(&r, std::slice::from_ref(&a)).unwrap();
    assert_eq!(
        got[&a],
        BTreeMap::from([
            (
                0,
                Liveness {
                    newest: (3, 12),
                    live: Some((3, 10)),
                    newest_is_tombstone: true
                }
            ),
            (
                1,
                Liveness {
                    newest: (2, 20),
                    live: Some((2, 20)),
                    newest_is_tombstone: false
                }
            ),
        ])
    );
    // a temporal key's entry has one clock
    let mut t = SegmentWriter::new(self::spec(None), WriterOptions::default()).unwrap();
    assert!(t.insert_liveness(&a, RowGroupId(0), (3, 1), true).is_err());
}

// --- pinned vectors ------------------------------------------------------

fn from_hex(s: &str) -> Vec<u8> {
    s.split_whitespace()
        .map(|b| u8::from_str_radix(b, 16).unwrap())
        .collect()
}

/// `"A"` in row groups 0 and 2 of `temporal:sym@ts,tombstone=void`: row
/// group 0 holds a live row at 5 and a tombstone at 7, row group 2 a live
/// row at 9. The bitmap {0, 2}, then per row group `newest | live | flags`.
const A_RECORD: &str = "14 00 00 00 3a 30 00 00 01 00 00 00 00 00 01 00 10 00 00 00 00 00 02 00 \
     07 00 00 00 00 00 00 00 05 00 00 00 00 00 00 00 03 \
     09 00 00 00 00 00 00 00 09 00 00 00 00 00 00 00 01";
/// `"B"` in row group 1, only a tombstone at 4: no live time (zeros), flags
/// `02`.
const B_RECORD: &str = "12 00 00 00 3a 30 00 00 01 00 00 00 00 00 00 00 10 00 00 00 01 00 \
     04 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 02";
/// `"A"` in row group 0 of `temporal2:sym@valid,known,tombstone=void`: a
/// live row at (valid 3, known 10) and its restatement as a tombstone at
/// (3, 12).
const A_BITEMPORAL_RECORD: &str =
    "12 00 00 00 3a 30 00 00 01 00 00 00 00 00 00 00 10 00 00 00 00 00 \
     03 00 00 00 00 00 00 00 0c 00 00 00 00 00 00 00 \
     03 00 00 00 00 00 00 00 0a 00 00 00 00 00 00 00 03";

fn pinned() -> Vec<(&'static str, &'static str)> {
    vec![
        ("`\"A\"` key", "06 41 00 00"),
        (
            "temporal value `\"A\"`, row groups {0, 2}: (7, live 5, newest a tombstone), (9, live 9)",
            A_RECORD,
        ),
        (
            "temporal value `\"B\"`, row group {1}: (4, no live row)",
            B_RECORD,
        ),
        (
            "bitemporal value `\"A\"`, row group {0}: ((3, 12), live (3, 10), newest a tombstone)",
            A_BITEMPORAL_RECORD,
        ),
        (
            "`temporal:sym@ts,tombstone=void` tombstone segment blake3",
            TEMPORAL_BLAKE3,
        ),
        (
            "`temporal2:sym@valid,known,tombstone=void` tombstone segment blake3",
            BITEMPORAL_BLAKE3,
        ),
    ]
}

const TEMPORAL_BLAKE3: &str = "c4782e6b8d7f17829b61d456d24d63d4dc5eb926dbafa573a5abaf17c5d56e35";
const BITEMPORAL_BLAKE3: &str = "a72232f65e85ea150b88820c72c0d7920929e5e32fbabc7f18b7deb1685bd03a";

fn pinned_temporal() -> Vec<u8> {
    let mut w = SegmentWriter::new(spec(None), WriterOptions::default()).unwrap();
    w.set_rowgroup_range(RowGroupId(0)..RowGroupId(3)).unwrap();
    let (a, b) = (key("A"), key("B"));
    w.insert_liveness(&a, RowGroupId(0), (5, 0), true).unwrap();
    w.insert_liveness(&a, RowGroupId(0), (7, 0), false).unwrap();
    w.insert_liveness(&b, RowGroupId(1), (4, 0), false).unwrap();
    w.insert_liveness(&a, RowGroupId(2), (9, 0), true).unwrap();
    w.finish().unwrap().bytes
}

fn pinned_bitemporal() -> Vec<u8> {
    let spec = KeySpec::Bitemporal {
        columns: vec!["sym".into()],
        valid_column: "valid".into(),
        known_column: "known".into(),
        tombstone: Some(Tombstone {
            column: "void".into(),
            value: None,
        }),
    };
    let mut w = SegmentWriter::new(spec, WriterOptions::default()).unwrap();
    let a = key("A");
    w.insert_liveness(&a, RowGroupId(0), (3, 10), true).unwrap();
    w.insert_liveness(&a, RowGroupId(0), (3, 12), false)
        .unwrap();
    w.finish().unwrap().bytes
}

fn find_once(hay: &[u8], needle: &[u8], what: &str) -> usize {
    let at: Vec<usize> = hay
        .windows(needle.len())
        .enumerate()
        .filter(|(_, w)| *w == needle)
        .map(|(i, _)| i)
        .collect();
    assert_eq!(at.len(), 1, "{what}: found at {at:?}");
    at[0]
}

#[test]
fn tombstone_value_records_match_the_pinned_bytes() {
    assert_eq!(key("A"), from_hex("06 41 00 00"));
    assert_eq!(
        (
            Hash::of(&pinned_temporal()).to_hex(),
            Hash::of(&pinned_bitemporal()).to_hex()
        ),
        (TEMPORAL_BLAKE3.to_string(), BITEMPORAL_BLAKE3.to_string()),
        "segment blake3"
    );
    for (bytes, records) in [
        (pinned_temporal(), vec![("A", A_RECORD), ("B", B_RECORD)]),
        (pinned_bitemporal(), vec![("A2", A_BITEMPORAL_RECORD)]),
    ] {
        for (what, hex) in records {
            let record = from_hex(hex);
            let at = find_once(&bytes, &record, what);
            // its directory entry: value_offset u64 | value_len u32 | XXH3-64
            let mut entry = (at as u64).to_le_bytes().to_vec();
            entry.extend_from_slice(&u32::try_from(record.len()).unwrap().to_le_bytes());
            entry.extend_from_slice(&checksum(&[&record]).to_le_bytes());
            find_once(&bytes, &entry, what);
        }
        let r = SegmentReader::open(bytes.clone()).unwrap();
        assert_eq!(r.header().storage_version, 2);
        assert_eq!(
            r.header().layout,
            bellwether_core::segment::Layout::KeyedTombstone
        );
    }
}

/// The vectors in `docs/format.md`, "Tombstone conformance vectors".
fn format_md_vectors() -> Vec<(String, String)> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs/format.md");
    let text = std::fs::read_to_string(path).unwrap();
    let start = text
        .find("## Tombstone conformance vectors")
        .expect("docs/format.md has a tombstone conformance section");
    let section = &text[start..];
    let section = &section[..section[3..].find("\n## ").map_or(section.len(), |e| e + 3)];
    section
        .lines()
        .filter_map(|l| {
            let cells: Vec<&str> = l.split('|').map(str::trim).collect();
            if cells.len() != 4 || cells[1] == "Vector" || cells[1].starts_with("---") {
                return None;
            }
            Some((cells[1].to_string(), cells[2].trim_matches('`').to_string()))
        })
        .collect()
}

#[test]
fn the_tombstone_table_matches_docs_format_md() {
    let norm = |s: &str| s.split_whitespace().collect::<Vec<_>>().join(" ");
    let want: Vec<(String, String)> = pinned()
        .into_iter()
        .map(|(a, b)| (a.to_string(), norm(b)))
        .collect();
    assert_eq!(format_md_vectors(), want);
}

// --- corruption ------------------------------------------------------------

static QUIET: Once = Once::new();
thread_local! {
    static SWEEPING: Cell<bool> = const { Cell::new(false) };
}

/// Silences the panic hook on the sweeping thread only, so a panic is
/// caught and reported by the assertion, not printed for every byte.
fn quiet() {
    QUIET.call_once(|| {
        let default = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if !SWEEPING.with(Cell::get) {
                default(info);
            }
        }));
    });
}

/// Everything a reader of a tombstone segment does with it, from opening
/// to merging; `Err` on any refusal.
fn exercise(bytes: &[u8]) -> Result<(Entries, NewestTombstones), String> {
    let r = SegmentReader::open(bytes.to_vec()).map_err(|e| e.to_string())?;
    let readers = vec![r];
    let entries = tombstone::liveness(&readers, &probes()).map_err(|e| e.to_string())?;
    let newest =
        tombstone::newest_tombstones(&readers, &(0..3).collect()).map_err(|e| e.to_string())?;
    merge::merge(&readers, &WriterOptions::default()).map_err(|e| e.to_string())?;
    Ok((entries, newest))
}

fn sweep(clean: &[u8], positions: std::ops::Range<usize>, every_value: bool) -> usize {
    quiet();
    let want = exercise(clean).unwrap();
    let mut refused = 0;
    let mut check = |bad: &[u8], what: &str| {
        SWEEPING.with(|s| s.set(true));
        let got = catch_unwind(AssertUnwindSafe(|| exercise(bad)));
        SWEEPING.with(|s| s.set(false));
        let Ok(got) = got else {
            panic!("{what}: panicked")
        };
        match got {
            Err(_) => refused += 1,
            Ok(got) => assert_eq!(got, want, "{what}: a different answer"),
        }
    };
    for len in 0..clean.len() {
        check(&clean[..len], &format!("truncated to {len}"));
    }
    for at in positions {
        let orig = clean[at];
        let values: Vec<u8> = if every_value {
            (0..=255u8).filter(|v| *v != orig).collect()
        } else {
            vec![0x00, 0xff, orig ^ 0x01, orig ^ 0x10, orig.wrapping_add(1)]
                .into_iter()
                .filter(|v| *v != orig)
                .collect()
        };
        for v in values {
            let mut bad = clean.to_vec();
            bad[at] = v;
            check(&bad, &format!("byte {at} = {v:#04x}"));
        }
    }
    refused
}

#[test]
fn every_flipped_or_truncated_byte_is_refused_or_harmless() {
    for clean in [pinned_temporal(), pinned_bitemporal()] {
        assert!(sweep(&clean, 0..clean.len(), false) > 0);
    }
}

#[test]
fn every_byte_value_at_every_body_position_is_refused_or_harmless() {
    let clean = pinned_temporal();
    let body_end = clean.len() - 8;
    assert!(sweep(&clean, PREAMBLE_LEN..body_end, true) > 0);
}

#[test]
fn segments_of_different_tombstones_do_not_merge() {
    let layout = vec![vec![Row {
        sym: Some("A"),
        ts: Some(1),
        void: Some(false),
    }]];
    let a = build(&spec(None), &layout, 0..1);
    let b = build(&spec(Some("VOID")), &layout, 0..1);
    assert!(merge::merge(&open(&[a.clone(), b]), &WriterOptions::default()).is_err());
    // two segments holding one key in one row group are corrupt, not unioned
    assert!(tombstone::liveness(&open(&[a.clone(), a]), &probes()).is_err());
}
