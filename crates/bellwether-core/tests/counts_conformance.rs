//! Storage version 2 segments with per-key counts (`docs/format.md`,
//! "Counts conformance vectors"):
//!
//! - the pinned value records of a counted exact segment, byte for byte,
//!   with the directory entry that checksums each, and the table in
//!   `docs/format.md` lists exactly these vectors;
//! - every fixture segment (`fixtures/format-v1/`, `fixtures/format-v0.1/`)
//!   is named by its BLAKE3, opens with the reader of its magic and refuses
//!   the other's, is listed under its spec's identity (version 2; its name
//!   in version 1), and reports counts exactly when it stores them (a
//!   storage version 1 segment never: its value bytes are never read as
//!   counts);
//! - merge sums counts of inputs over disjoint row groups and refuses inputs
//!   that both count a key in one row group;
//! - every byte value at every body position, and a byte-flip sweep over
//!   every position, of a counted exact and a counted temporal segment:
//!   never a panic, and never counts other than the clean ones, no key
//!   missing (the checksums refuse a corrupt record instead).
#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::assert_is_empty
)]

use std::cell::Cell;
use std::collections::BTreeMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;
use std::sync::Once;

use bellwether_core::encoding::{encode_tuple, Value};
use bellwether_core::segment::{checksum, FOOTER_LEN_V2, MAGIC, PREAMBLE_LEN};
use bellwether_core::sidecar::SidecarReader;
use bellwether_core::{merge, SegmentReader, SegmentWriter, WriterOptions};
use bellwether_types::{Hash, KeySpec, RowGroupId};

type Counted = BTreeMap<Vec<u8>, Vec<(RowGroupId, u64)>>;

fn from_hex(s: &str) -> Vec<u8> {
    s.split_whitespace()
        .map(|b| u8::from_str_radix(b, 16).unwrap())
        .collect()
}

fn utf8(s: &str) -> Vec<u8> {
    encode_tuple(&[Value::Utf8(s)]).unwrap()
}

/// `"AAPL"` held by row groups 0 (3 rows) and 2 (1 row): the length-prefixed
/// no-run roaring bitmap {0, 2}, then the LEB128 counts 3 and 1.
const AAPL_RECORD: &str =
    "14 00 00 00 3a 30 00 00 01 00 00 00 00 00 01 00 10 00 00 00 00 00 02 00 03 01";
/// `"MSFT"` held by row group 1 (300 rows): bitmap {1}, LEB128 300 = `ac 02`.
const MSFT_RECORD: &str = "12 00 00 00 3a 30 00 00 01 00 00 00 00 00 00 00 10 00 00 00 01 00 ac 02";

fn pinned() -> Vec<(&'static str, &'static str)> {
    vec![
        ("`\"AAPL\"` key", "06 41 41 50 4c 00 00"),
        (
            "exact value `\"AAPL\"`, row groups {0, 2}, counts [3, 1]",
            AAPL_RECORD,
        ),
        (
            "exact value `\"MSFT\"`, row group {1}, counts [300]",
            MSFT_RECORD,
        ),
    ]
}

fn counted_exact() -> KeySpec {
    KeySpec::Exact {
        columns: vec!["symbol".into()],
        counts: true,
        bounds: None,
    }
}

/// The pinned counted exact segment (no recorded types, row groups 0..4).
fn pinned_segment() -> Vec<u8> {
    let (aapl, msft) = (utf8("AAPL"), utf8("MSFT"));
    let mut w = SegmentWriter::new(counted_exact(), WriterOptions::default()).unwrap();
    w.set_rowgroup_range(RowGroupId(0)..RowGroupId(4)).unwrap();
    w.insert_rows(&msft, RowGroupId(1), 300).unwrap();
    w.insert(&aapl, RowGroupId(2)).unwrap();
    w.insert_rows(&aapl, RowGroupId(0), 2).unwrap();
    w.insert(&aapl, RowGroupId(0)).unwrap();
    w.finish().unwrap().bytes
}

fn clean_counts() -> Counted {
    BTreeMap::from([
        (utf8("AAPL"), vec![(RowGroupId(0), 3), (RowGroupId(2), 1)]),
        (utf8("MSFT"), vec![(RowGroupId(1), 300)]),
    ])
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
fn counted_value_records_match_the_pinned_bytes() {
    let bytes = pinned_segment();
    assert_eq!(&bytes[..8], &MAGIC);
    assert_eq!(&bytes[8..12], &2u32.to_le_bytes(), "storage version 2");
    assert_eq!(&bytes[bytes.len() - 8..], &MAGIC);
    assert!(bytes.len() > PREAMBLE_LEN + FOOTER_LEN_V2);
    assert_eq!(utf8("AAPL"), from_hex("06 41 41 50 4c 00 00"));
    for (what, hex) in [("AAPL", AAPL_RECORD), ("MSFT", MSFT_RECORD)] {
        let record = from_hex(hex);
        let at = find_once(&bytes, &record, what);
        // Its directory entry: value_offset u64 | value_len u32 | XXH3-64.
        let mut entry = (at as u64).to_le_bytes().to_vec();
        entry.extend_from_slice(&u32::try_from(record.len()).unwrap().to_le_bytes());
        entry.extend_from_slice(&checksum(&[&record]).to_le_bytes());
        find_once(&bytes, &entry, what);
    }
    let r = SegmentReader::open(bytes).unwrap();
    assert!(r.has_counts());
    assert_eq!(r.header().storage_version, 2);
    let keys = [utf8("AAPL"), utf8("MSFT"), utf8("IBM")];
    assert_eq!(r.counts(&keys).unwrap(), Some(clean_counts()));
}

/// The vectors in `docs/format.md`, "Counts conformance vectors".
fn format_md_vectors() -> Vec<(String, String)> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs/format.md");
    let text = std::fs::read_to_string(path).unwrap();
    let start = text
        .find("## Counts conformance vectors")
        .expect("docs/format.md has a counts conformance section");
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
fn the_counts_table_matches_docs_format_md() {
    let want: Vec<(String, String)> = pinned()
        .into_iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect();
    assert_eq!(format_md_vectors(), want);
}

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures")
        .join(name)
        .join("index")
}

#[test]
fn every_fixture_segment_is_named_by_its_hash_and_counts_only_when_it_stores_them() {
    for (name, storage) in [("format-v1", 1u32), ("format-v0.1", 2)] {
        let dir = fixture(name);
        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.join("manifest.json")).unwrap()).unwrap();
        let mut counted = 0;
        let segments = manifest["segments"].as_array().unwrap();
        assert!(!segments.is_empty());
        for seg in segments {
            let id = seg["id"].as_str().unwrap();
            let bytes = std::fs::read(dir.join("segments").join(format!("{id}.seg"))).unwrap();
            assert_eq!(Hash::of(&bytes).to_hex(), id, "{name}: {id}");
            if seg.get("kind").is_some_and(|k| !k.is_null()) {
                // A sidecar (storage version 1 layout, in both fixtures).
                SidecarReader::open(bytes.clone()).unwrap();
                assert!(SegmentReader::open(bytes).is_err(), "{name}: {id}");
                continue;
            }
            assert!(SidecarReader::open(bytes.clone()).is_err(), "{name}: {id}");
            let r = SegmentReader::open(bytes).unwrap();
            assert_eq!(r.header().storage_version, storage, "{name}: {id}");
            let stores = r.header().key_spec.counts();
            assert_eq!(r.has_counts(), stores, "{name}: {id}");
            if !stores {
                // Never zeros: no counts at all.
                assert_eq!(r.counts(&[utf8("AAPL")]).unwrap(), None);
            }
            counted += usize::from(stores);
            // Version 2 keys segments by full identity; version 1 by name
            // (a reader re-keys it on load).
            let spec = &r.header().key_spec;
            let key = if storage == 2 {
                spec.identity()
            } else {
                spec.name()
            };
            assert_eq!(seg["key_spec"].as_str().unwrap(), key, "{name}: {id}");
        }
        assert_eq!(
            counted > 0,
            storage == 2,
            "{name}: {counted} counted segments"
        );
    }
}

#[test]
fn merge_sums_disjoint_counts_and_refuses_overlapping_ones() {
    let aapl = utf8("AAPL");
    let seg = |rows: &[(u32, u64)], range: std::ops::Range<u32>| {
        let mut w = SegmentWriter::new(counted_exact(), WriterOptions::default()).unwrap();
        w.set_rowgroup_range(RowGroupId(range.start)..RowGroupId(range.end))
            .unwrap();
        for &(rg, n) in rows {
            w.insert_rows(&aapl, RowGroupId(rg), n).unwrap();
        }
        SegmentReader::open(w.finish().unwrap().bytes).unwrap()
    };
    let merged = merge(
        &[seg(&[(0, 2), (1, 5)], 0..2), seg(&[(3, 7)], 2..4)],
        &WriterOptions::default(),
    )
    .unwrap();
    let r = SegmentReader::open(merged.bytes).unwrap();
    assert_eq!(
        r.counts(std::slice::from_ref(&aapl)).unwrap(),
        Some(BTreeMap::from([(
            aapl.clone(),
            vec![(RowGroupId(0), 2), (RowGroupId(1), 5), (RowGroupId(3), 7)]
        )]))
    );
    let err = merge(
        &[seg(&[(1, 2)], 0..2), seg(&[(1, 3)], 1..3)],
        &WriterOptions::default(),
    );
    assert!(err.is_err(), "two inputs counting AAPL in row group 1");
}

/// A counted temporal segment: `"AAPL"` versions in row groups 0 and 1,
/// `"MSFT"` in row group 1 with a null time (counted, never winning).
fn counted_temporal() -> (Vec<u8>, Counted) {
    let spec = KeySpec::Temporal {
        columns: vec!["symbol".into()],
        ts_column: "ts".into(),
        counts: true,
    };
    let (aapl, msft) = (utf8("AAPL"), utf8("MSFT"));
    let mut w = SegmentWriter::new(spec, WriterOptions::default()).unwrap();
    w.insert_version(&aapl, Some(5), RowGroupId(0)).unwrap();
    w.insert_version(&aapl, Some(9), RowGroupId(0)).unwrap();
    w.insert_version(&aapl, Some(9), RowGroupId(1)).unwrap();
    w.insert_version(&msft, None, RowGroupId(1)).unwrap();
    let want = BTreeMap::from([
        (aapl, vec![(RowGroupId(0), 2), (RowGroupId(1), 1)]),
        (msft, vec![(RowGroupId(1), 1)]),
    ]);
    (w.finish().unwrap().bytes, want)
}

thread_local! {
    static QUIET: Cell<bool> = const { Cell::new(false) };
}

/// Runs `f` with panics caught and silenced on this thread only.
fn quietly(f: impl FnOnce()) -> bool {
    static HOOK: Once = Once::new();
    HOOK.call_once(|| {
        let default = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if !QUIET.with(Cell::get) {
                default(info);
            }
        }));
    });
    QUIET.with(|q| q.set(true));
    let panicked = catch_unwind(AssertUnwindSafe(f)).is_err();
    QUIET.with(|q| q.set(false));
    panicked
}

/// Opens corrupted bytes and reads counts, values, and a merge. Returns a
/// description of a wrong answer, if one was read: every key's counts, no
/// key missing (a corrupt index that drops a key is as wrong as one that
/// changes a count), or a typed refusal.
fn exercise(b: Vec<u8>, want: &Counted) -> Option<String> {
    let r = SegmentReader::open(b).ok()?;
    let keys: Vec<Vec<u8>> = want.keys().cloned().collect();
    let _ = r.get(&keys);
    let _ = r.lookup(&keys);
    let _ = r.latest(&keys);
    let _ = merge(std::slice::from_ref(&r), &WriterOptions::default());
    match r.counts(&keys) {
        Ok(Some(got)) if got != *want => Some(format!("{got:?}")),
        Ok(None) => Some("no counts".into()),
        _ => None,
    }
}

#[test]
fn corrupt_counted_segments_never_panic_or_count_wrong() {
    let (temporal, temporal_want) = counted_temporal();
    let mut bad = Vec::new();
    for (name, bytes, want) in [
        ("exact", pinned_segment(), clean_counts()),
        ("temporal", temporal, temporal_want),
    ] {
        assert_eq!(exercise(bytes.clone(), &want), None, "{name} clean");
        let header_len = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
        let body = PREAMBLE_LEN + header_len;
        for pos in 0..bytes.len() {
            // Every byte value over the body; a flip set over the header.
            let values: Vec<u8> = if pos >= body {
                (0..=u8::MAX).collect()
            } else {
                vec![0x00, 0xFF, bytes[pos] ^ 0x01, bytes[pos] ^ 0x20]
            };
            for v in values {
                if v == bytes[pos] {
                    continue;
                }
                let mut b = bytes.clone();
                b[pos] = v;
                let mut wrong = None;
                if quietly(|| wrong = exercise(b, &want)) {
                    bad.push(format!("{name} pos {pos} = {v:#x}: panic"));
                } else if let Some(w) = wrong {
                    bad.push(format!("{name} pos {pos} = {v:#x}: wrong counts {w}"));
                }
            }
        }
        for cut in 0..bytes.len() {
            let mut wrong = None;
            if quietly(|| wrong = exercise(bytes[..cut].to_vec(), &want)) || wrong.is_some() {
                bad.push(format!("{name} cut {cut}"));
            }
        }
    }
    assert!(
        bad.is_empty(),
        "{} failures, first: {:?}",
        bad.len(),
        &bad[..bad.len().min(20)]
    );
}
