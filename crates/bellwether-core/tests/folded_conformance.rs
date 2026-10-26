//! Storage version 2 folded layouts (`docs/format.md`, "Folded layout
//! conformance vectors"): per-key time bounds inside the exact value record
//! (`bitmap | [counts] | bounds`), a bloom's `bounds` segment (the `keyed`
//! layout under the bloom spec, `bitmap | bounds`), and the `bitemporal`
//! layout (`bitmap | count | triples`), which replace the storage version 1
//! `bounds` and `temporal2` sidecar segments.
//!
//! - the pinned value records of `exact:symbol@ts,counts` (with the
//!   all-null sentinel), of the `bounds` segment of `bloom:symbol@0.01@ts`
//!   and of `temporal2:symbol@valid,known`, byte for byte, each with the
//!   directory entry that checksums it, the BLAKE3 of the three segments,
//!   and the table in `docs/format.md` lists exactly these;
//! - the sentinel is stored, read back by `get`, and left out of `bounds`
//!   (it overlaps no window, not even one ending at `i64::MAX`);
//! - merge widens one row group's bounds across inputs, the sentinel
//!   absorbing into any real range, and unites bitemporal versions;
//! - an oracle over a Parquet data file: every value record the writer
//!   builds from the file's row groups equals the one computed row by row
//!   from the file, every window plan through the stored bounds is exactly
//!   the row groups whose per-key range overlaps the window (and so never
//!   misses a row in it), and a split build merges back to the same bytes;
//! - every byte value at every body position, a flip set over the header,
//!   and every truncation, of each folded layout (a bloom's `bounds`
//!   segment included): never a panic, never a value other than the clean
//!   one, no key missing.
#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::type_complexity
)]

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;
use std::sync::{Arc, Once};

use arrow::array::{
    Array, ArrayRef, RecordBatch, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use bellwether_core::encoding::{encode_tuple, Value};
use bellwether_core::segment::{checksum, KeyValue, Layout, ALL_NULL_BOUNDS, PREAMBLE_LEN};
use bellwether_core::sidecar::{RgBounds, Triple};
use bellwether_core::{merge, SegmentReader, SegmentWriter, WriterOptions};
use bellwether_types::{Hash, KeySpec, RowGroupId};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use roaring::RoaringBitmap;

const DAY_US: i64 = 86_400_000_000;

fn from_hex(s: &str) -> Vec<u8> {
    s.split_whitespace()
        .map(|b| u8::from_str_radix(b, 16).unwrap())
        .collect()
}

fn utf8(s: &str) -> Vec<u8> {
    encode_tuple(&[Value::Utf8(s)]).unwrap()
}

fn bounded(counts: bool) -> KeySpec {
    KeySpec::Exact {
        columns: vec!["symbol".into()],
        counts,
        bounds: Some("ts".into()),
    }
}

fn bitemporal() -> KeySpec {
    KeySpec::Bitemporal {
        columns: vec!["symbol".into()],
        valid_column: "valid".into(),
        known_column: "known".into(),
    }
}

fn bloom_bounded() -> KeySpec {
    KeySpec::Bloom {
        columns: vec!["symbol".into()],
        fpr: 0.01,
        bounds: Some("ts".into()),
    }
}

/// `"AAPL"`: row groups {0, 2}, counts [3, 1]; row group 0's times are
/// `[1, 86400000000]` (one of its three rows has a null time), row group 2's
/// one row has a null time, so it stores the all-null sentinel
/// `(i64::MAX, i64::MIN)`.
const BOUNDED_AAPL: &str =
    "14 00 00 00 3a 30 00 00 01 00 00 00 00 00 01 00 10 00 00 00 00 00 02 00 \
     03 01 \
     01 00 00 00 00 00 00 00 00 60 d7 1d 14 00 00 00 \
     ff ff ff ff ff ff ff 7f 00 00 00 00 00 00 00 80";
/// `"MSFT"`: row group {1}, count [2], times `[-1, 1]`.
const BOUNDED_MSFT: &str = "12 00 00 00 3a 30 00 00 01 00 00 00 00 00 00 00 10 00 00 00 01 00 \
     02 \
     ff ff ff ff ff ff ff ff 01 00 00 00 00 00 00 00";
/// `"AAPL"`: versions `(valid 1, known 1, rg 0)` and `(1, 86400000000, rg 2)`.
const BITEMPORAL_AAPL: &str =
    "14 00 00 00 3a 30 00 00 01 00 00 00 00 00 01 00 10 00 00 00 00 00 02 00 \
     02 00 00 00 \
     01 00 00 00 00 00 00 00 01 00 00 00 00 00 00 00 00 00 00 00 \
     01 00 00 00 00 00 00 00 00 60 d7 1d 14 00 00 00 02 00 00 00";
/// `"MSFT"`: version `(86400000000, 86400000000, rg 1)`.
const BITEMPORAL_MSFT: &str = "12 00 00 00 3a 30 00 00 01 00 00 00 00 00 00 00 10 00 00 00 01 00 \
     01 00 00 00 \
     00 60 d7 1d 14 00 00 00 00 60 d7 1d 14 00 00 00 01 00 00 00";
/// The `bounds` segment of `bloom:symbol@0.01@ts`, `"AAPL"`: row groups
/// {0, 2}, bounds `[1, 86400000000]` and the all-null sentinel (no counts).
const BLOOM_BOUNDS_AAPL: &str =
    "14 00 00 00 3a 30 00 00 01 00 00 00 00 00 01 00 10 00 00 00 00 00 02 00 \
     01 00 00 00 00 00 00 00 00 60 d7 1d 14 00 00 00 \
     ff ff ff ff ff ff ff 7f 00 00 00 00 00 00 00 80";
/// The `bounds` segment of `bloom:symbol@0.01@ts`, `"MSFT"`: row group {1},
/// bounds `[-1, 1]`.
const BLOOM_BOUNDS_MSFT: &str =
    "12 00 00 00 3a 30 00 00 01 00 00 00 00 00 00 00 10 00 00 00 01 00 \
     ff ff ff ff ff ff ff ff 01 00 00 00 00 00 00 00";
/// BLAKE3 of the pinned `exact:symbol@ts,counts` segment.
const BOUNDED_BLAKE3: &str = "74acef3b4710583ea99596f607094102ef917fbcdae93cf774ec3930c62f41bf";
/// BLAKE3 of the pinned `bounds` segment of `bloom:symbol@0.01@ts`.
const BLOOM_BOUNDS_BLAKE3: &str =
    "083eb7d2da5c3589bb2a290b634e66346561d8e78fb6135a8906aa8b5b57a775";
/// BLAKE3 of the pinned `temporal2:symbol@valid,known` segment.
const BITEMPORAL_BLAKE3: &str = "0771c6c1083255732be1d9b1d870cd3c9f24063320be2e87f4d27cdc2d48df0a";

fn spaced(hex: &str) -> String {
    hex.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn pinned() -> Vec<(String, String)> {
    [
        ("`\"AAPL\"` key", "06 41 41 50 4c 00 00"),
        (
            "`exact:symbol@ts,counts` value `\"AAPL\"`: row groups {0, 2}, counts [3, 1], bounds [1, 86400000000] and the all-null sentinel",
            BOUNDED_AAPL,
        ),
        (
            "`exact:symbol@ts,counts` value `\"MSFT\"`: row group {1}, counts [2], bounds [-1, 1]",
            BOUNDED_MSFT,
        ),
        (
            "`bloom:symbol@0.01@ts` bounds segment value `\"AAPL\"`: row groups {0, 2}, bounds [1, 86400000000] and the all-null sentinel",
            BLOOM_BOUNDS_AAPL,
        ),
        (
            "`bloom:symbol@0.01@ts` bounds segment value `\"MSFT\"`: row group {1}, bounds [-1, 1]",
            BLOOM_BOUNDS_MSFT,
        ),
        (
            "`temporal2:symbol@valid,known` value `\"AAPL\"`: (1, 1, rg 0), (1, 86400000000, rg 2)",
            BITEMPORAL_AAPL,
        ),
        (
            "`temporal2:symbol@valid,known` value `\"MSFT\"`: (86400000000, 86400000000, rg 1)",
            BITEMPORAL_MSFT,
        ),
        ("`exact:symbol@ts,counts` segment blake3", BOUNDED_BLAKE3),
        (
            "`bloom:symbol@0.01@ts` bounds segment blake3",
            BLOOM_BOUNDS_BLAKE3,
        ),
        ("`temporal2:symbol@valid,known` segment blake3", BITEMPORAL_BLAKE3),
    ]
    .into_iter()
    .map(|(a, b)| (a.to_string(), spaced(b)))
    .collect()
}

/// The pinned `exact:symbol@ts,counts` segment (no recorded types, row
/// groups 0..4), rows inserted out of order.
fn bounded_segment() -> Vec<u8> {
    let (aapl, msft) = (utf8("AAPL"), utf8("MSFT"));
    let mut w = SegmentWriter::new(bounded(true), WriterOptions::default()).unwrap();
    w.set_rowgroup_range(RowGroupId(0)..RowGroupId(4)).unwrap();
    w.insert_timed(&msft, RowGroupId(1), Some(1)).unwrap();
    w.insert_timed(&aapl, RowGroupId(2), None).unwrap();
    w.insert_timed(&aapl, RowGroupId(0), Some(DAY_US)).unwrap();
    w.insert_timed(&aapl, RowGroupId(0), None).unwrap();
    w.insert_timed(&msft, RowGroupId(1), Some(-1)).unwrap();
    w.insert_timed(&aapl, RowGroupId(0), Some(1)).unwrap();
    w.finish().unwrap().bytes
}

/// The pinned `bounds` segment of `bloom:symbol@0.01@ts` (no recorded
/// types, row groups 0..4): the rows of [`bounded_segment`], whose records
/// are the exact spec's without counts.
fn bloom_bounds_segment() -> Vec<u8> {
    let (aapl, msft) = (utf8("AAPL"), utf8("MSFT"));
    let mut w = SegmentWriter::bloom_bounds(bloom_bounded(), WriterOptions::default()).unwrap();
    w.set_rowgroup_range(RowGroupId(0)..RowGroupId(4)).unwrap();
    w.insert_timed(&msft, RowGroupId(1), Some(1)).unwrap();
    w.insert_timed(&aapl, RowGroupId(2), None).unwrap();
    w.insert_timed(&aapl, RowGroupId(0), Some(DAY_US)).unwrap();
    w.insert_timed(&aapl, RowGroupId(0), None).unwrap();
    w.insert_timed(&msft, RowGroupId(1), Some(-1)).unwrap();
    w.insert_timed(&aapl, RowGroupId(0), Some(1)).unwrap();
    w.finish().unwrap().bytes
}

/// The pinned `temporal2:symbol@valid,known` segment (no recorded types,
/// row groups 0..4); a repeated version is stored once.
fn bitemporal_segment() -> Vec<u8> {
    let (aapl, msft) = (utf8("AAPL"), utf8("MSFT"));
    let mut w = SegmentWriter::new(bitemporal(), WriterOptions::default()).unwrap();
    w.set_rowgroup_range(RowGroupId(0)..RowGroupId(4)).unwrap();
    w.insert_triple(&aapl, 1, DAY_US, RowGroupId(2)).unwrap();
    w.insert_triple(&msft, DAY_US, DAY_US, RowGroupId(1))
        .unwrap();
    w.insert_triple(&aapl, 1, 1, RowGroupId(0)).unwrap();
    w.insert_triple(&aapl, 1, 1, RowGroupId(0)).unwrap();
    w.finish().unwrap().bytes
}

fn bitmap(rgs: &[u32]) -> RoaringBitmap {
    rgs.iter().copied().collect()
}

fn clean_bounded() -> BTreeMap<Vec<u8>, KeyValue> {
    BTreeMap::from([
        (
            utf8("AAPL"),
            KeyValue {
                bitmap: bitmap(&[0, 2]),
                counts: Some(vec![3, 1]),
                bounds: Some(vec![(1, DAY_US), ALL_NULL_BOUNDS]),
                ..KeyValue::default()
            },
        ),
        (
            utf8("MSFT"),
            KeyValue {
                bitmap: bitmap(&[1]),
                counts: Some(vec![2]),
                bounds: Some(vec![(-1, 1)]),
                ..KeyValue::default()
            },
        ),
    ])
}

/// [`clean_bounded`] without counts: a bloom's `bounds` segment.
fn clean_bloom_bounds() -> BTreeMap<Vec<u8>, KeyValue> {
    clean_bounded()
        .into_iter()
        .map(|(k, v)| (k, KeyValue { counts: None, ..v }))
        .collect()
}

fn clean_bitemporal() -> BTreeMap<Vec<u8>, KeyValue> {
    BTreeMap::from([
        (
            utf8("AAPL"),
            KeyValue {
                bitmap: bitmap(&[0, 2]),
                triples: vec![(1, 1, 0), (1, DAY_US, 2)],
                ..KeyValue::default()
            },
        ),
        (
            utf8("MSFT"),
            KeyValue {
                bitmap: bitmap(&[1]),
                triples: vec![(DAY_US, DAY_US, 1)],
                ..KeyValue::default()
            },
        ),
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

/// Each record is in the segment exactly once, followed in the directory
/// by its offset, length and XXH3-64.
fn assert_records(bytes: &[u8], records: &[(&str, &str)]) {
    for (what, hex) in records {
        let record = from_hex(hex);
        let at = find_once(bytes, &record, what);
        let mut entry = (at as u64).to_le_bytes().to_vec();
        entry.extend_from_slice(&u32::try_from(record.len()).unwrap().to_le_bytes());
        entry.extend_from_slice(&checksum(&[&record]).to_le_bytes());
        find_once(bytes, &entry, what);
    }
}

/// A bloom's `bounds` segment is a storage version 2 segment (magic
/// `BWSEGMNT`, footer checksum), the `keyed` layout under the bloom spec,
/// each value record `bitmap | bounds` and checksummed in its directory
/// entry; it reads back like the exact spec's bounds, without counts.
#[test]
fn a_bloom_bounds_segment_matches_the_pinned_bytes() {
    let b = bloom_bounds_segment();
    assert_eq!(&b[..8], b"BWSEGMNT");
    assert_eq!(&b[b.len() - 8..], b"BWSEGMNT");
    assert_records(
        &b,
        &[("AAPL", BLOOM_BOUNDS_AAPL), ("MSFT", BLOOM_BOUNDS_MSFT)],
    );
    assert_eq!(Hash::of(&b).to_hex(), BLOOM_BOUNDS_BLAKE3, "segment blake3");
    let keys = [utf8("AAPL"), utf8("MSFT"), utf8("IBM")];
    let r = SegmentReader::open(b).unwrap();
    assert_eq!(r.header().storage_version, 2);
    assert_eq!(r.header().layout, Layout::Keyed);
    assert_eq!(r.header().key_spec, bloom_bounded());
    assert!(!r.has_counts() && r.has_bounds());
    assert_eq!(r.get(&keys).unwrap(), clean_bloom_bounds());
    assert_eq!(r.counts(&keys).unwrap(), None);
    assert_eq!(
        r.bounds(&keys).unwrap(),
        SegmentReader::open(bounded_segment())
            .unwrap()
            .bounds(&keys)
            .unwrap()
    );
    // The bloom's own filters never merge with its bounds.
    let mut filters = SegmentWriter::new(bloom_bounded(), WriterOptions::default()).unwrap();
    filters.insert(&utf8("AAPL"), RowGroupId(0)).unwrap();
    let filters = SegmentReader::open(filters.finish().unwrap().bytes).unwrap();
    assert!(merge(&[r, filters], &WriterOptions::default()).is_err());
    // No other spec takes the layout.
    assert!(SegmentWriter::bloom_bounds(bounded(false), WriterOptions::default()).is_err());
}

#[test]
fn folded_value_records_and_segments_match_the_pinned_bytes() {
    let b = bounded_segment();
    assert_records(&b, &[("AAPL", BOUNDED_AAPL), ("MSFT", BOUNDED_MSFT)]);
    let t = bitemporal_segment();
    assert_records(&t, &[("AAPL", BITEMPORAL_AAPL), ("MSFT", BITEMPORAL_MSFT)]);
    assert_eq!(
        (Hash::of(&b).to_hex(), Hash::of(&t).to_hex()),
        (BOUNDED_BLAKE3.to_string(), BITEMPORAL_BLAKE3.to_string()),
        "segment blake3 (bounded, bitemporal)"
    );

    let keys = [utf8("AAPL"), utf8("MSFT"), utf8("IBM")];
    let r = SegmentReader::open(b).unwrap();
    assert_eq!(r.header().storage_version, 2);
    assert!(r.has_counts() && r.has_bounds());
    assert_eq!(r.get(&keys).unwrap(), clean_bounded());
    // The sentinel row group is stored, and left out of the bounds a
    // planner reads.
    assert_eq!(
        r.bounds(&keys).unwrap(),
        Some(BTreeMap::from([
            (
                utf8("AAPL"),
                vec![RgBounds {
                    rg: RowGroupId(0),
                    min: 1,
                    max: DAY_US
                }]
            ),
            (
                utf8("MSFT"),
                vec![RgBounds {
                    rg: RowGroupId(1),
                    min: -1,
                    max: 1
                }]
            ),
        ]))
    );
    assert!(r.triples(&keys).is_err(), "not a bitemporal segment");

    let r = SegmentReader::open(t).unwrap();
    assert!(!r.has_counts() && !r.has_bounds());
    assert_eq!(r.bounds(&keys).unwrap(), None);
    assert_eq!(r.get(&keys).unwrap(), clean_bitemporal());
    let triple = |valid, known, rg| Triple {
        valid,
        known,
        rg: RowGroupId(rg),
    };
    assert_eq!(
        r.triples(&keys).unwrap(),
        BTreeMap::from([
            (utf8("AAPL"), vec![triple(1, 1, 0), triple(1, DAY_US, 2)]),
            (utf8("MSFT"), vec![triple(DAY_US, DAY_US, 1)]),
        ])
    );
}

/// The rows of `docs/format.md`'s "Folded layout conformance vectors" table.
fn format_md_vectors() -> Vec<(String, String)> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs/format.md");
    let text = std::fs::read_to_string(path).unwrap();
    let start = text
        .find("## Folded layout conformance vectors")
        .expect("docs/format.md has a folded layout conformance section");
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
fn the_folded_layout_table_matches_docs_format_md() {
    assert_eq!(format_md_vectors(), pinned());
}

/// A window ending at `i64::MAX` or starting at `i64::MIN` still never
/// selects a row group whose times are all null.
#[test]
fn the_all_null_sentinel_overlaps_no_window() {
    let r = SegmentReader::open(bounded_segment()).unwrap();
    let aapl = utf8("AAPL");
    let b = r.bounds(std::slice::from_ref(&aapl)).unwrap().unwrap();
    for (lo, hi) in [
        (i64::MIN, i64::MAX),
        (0, i64::MAX),
        (i64::MIN, 0),
        (i64::MAX, i64::MAX),
        (i64::MIN, i64::MIN),
    ] {
        let rgs: Vec<u32> = b[&aapl]
            .iter()
            .filter(|x| x.overlaps(lo, hi))
            .map(|x| x.rg.0)
            .collect();
        assert!(
            !rgs.contains(&2),
            "[{lo}, {hi}] selected the all-null row group"
        );
    }
}

fn open(bytes: Vec<u8>) -> SegmentReader<Vec<u8>> {
    SegmentReader::open(bytes).unwrap()
}

/// Merge widens one row group's bounds across inputs (an uncounted spec
/// may list one row group in two segments), the sentinel absorbing into
/// any real range and staying only where both are all null, keeps bounds
/// aligned with the merged bitmap of a counted spec over disjoint row
/// groups, and unites bitemporal versions.
#[test]
fn merge_widens_bounds_with_the_sentinel_absorbing_and_unites_versions() {
    let aapl = utf8("AAPL");
    let k = std::slice::from_ref(&aapl);
    let seg = |spec: KeySpec, rows: &[(u32, Option<i64>)], range: std::ops::Range<u32>| {
        let mut w = SegmentWriter::new(spec, WriterOptions::default()).unwrap();
        w.set_rowgroup_range(RowGroupId(range.start)..RowGroupId(range.end))
            .unwrap();
        for &(rg, ts) in rows {
            w.insert_timed(&aapl, RowGroupId(rg), ts).unwrap();
        }
        open(w.finish().unwrap().bytes)
    };
    let a = seg(
        bounded(false),
        &[
            (1, Some(5)),
            (1, Some(7)),
            (2, None),
            (3, None),
            (5, Some(9)),
        ],
        0..6,
    );
    let b = seg(
        bounded(false),
        &[
            (1, None),
            (2, Some(2)),
            (2, Some(4)),
            (3, None),
            (4, Some(-3)),
            (5, Some(1)),
        ],
        0..6,
    );
    let m = open(merge(&[a, b], &WriterOptions::default()).unwrap().bytes);
    assert_eq!(
        m.get(k).unwrap()[&aapl],
        KeyValue {
            bitmap: bitmap(&[1, 2, 3, 4, 5]),
            bounds: Some(vec![(5, 7), (2, 4), ALL_NULL_BOUNDS, (-3, -3), (1, 9)]),
            ..KeyValue::default()
        }
    );

    let a = seg(bounded(true), &[(0, Some(10)), (0, None), (2, None)], 0..3);
    let b = seg(bounded(true), &[(1, Some(-4)), (3, Some(8))], 1..4);
    let m = open(merge(&[a, b], &WriterOptions::default()).unwrap().bytes);
    assert_eq!(
        m.get(k).unwrap()[&aapl],
        KeyValue {
            bitmap: bitmap(&[0, 1, 2, 3]),
            counts: Some(vec![2, 1, 1, 1]),
            bounds: Some(vec![(10, 10), (-4, -4), ALL_NULL_BOUNDS, (8, 8)]),
            ..KeyValue::default()
        }
    );

    let tseg = |triples: &[(i64, i64, u32)]| {
        let mut w = SegmentWriter::new(bitemporal(), WriterOptions::default()).unwrap();
        for &(v, kn, rg) in triples {
            w.insert_triple(&aapl, v, kn, RowGroupId(rg)).unwrap();
        }
        open(w.finish().unwrap().bytes)
    };
    let m = open(
        merge(
            &[tseg(&[(1, 1, 0), (3, 2, 1)]), tseg(&[(1, 1, 0), (2, 5, 2)])],
            &WriterOptions::default(),
        )
        .unwrap()
        .bytes,
    );
    assert_eq!(
        m.get(k).unwrap()[&aapl],
        KeyValue {
            bitmap: bitmap(&[0, 1, 2]),
            triples: vec![(1, 1, 0), (2, 5, 2), (3, 2, 1)],
            ..KeyValue::default()
        }
    );

    // A spec with bounds never merges with the same columns without them.
    let with = seg(bounded(false), &[(0, Some(1))], 0..1);
    let mut w = SegmentWriter::new(
        KeySpec::Exact {
            columns: vec!["symbol".into()],
            counts: false,
            bounds: None,
        },
        WriterOptions::default(),
    )
    .unwrap();
    w.insert(&aapl, RowGroupId(1)).unwrap();
    let without = open(w.finish().unwrap().bytes);
    assert!(merge(&[with, without], &WriterOptions::default()).is_err());
}

/// `symbol` and three time columns (`ts`, `valid`, `known`) in `unit`.
fn timed_schema(unit: TimeUnit) -> Arc<Schema> {
    let ts = || DataType::Timestamp(unit, Some("UTC".into()));
    Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, true),
        Field::new("ts", ts(), true),
        Field::new("valid", ts(), true),
        Field::new("known", ts(), true),
    ]))
}

/// A segment of `spec` (a bloom's: its `bounds` segment) written from one
/// row of a dataset of [`timed_schema`] in `unit`.
fn timed_segment(spec: &KeySpec, unit: TimeUnit) -> SegmentReader<Vec<u8>> {
    let schema = timed_schema(unit);
    let opts = WriterOptions::default();
    let mut w = match spec {
        KeySpec::Bloom { .. } => {
            SegmentWriter::bloom_bounds_for_dataset(spec.clone(), &schema, opts).unwrap()
        }
        _ => SegmentWriter::for_dataset(spec.clone(), &schema, opts).unwrap(),
    };
    let time = |v: i64| -> ArrayRef {
        match unit {
            TimeUnit::Millisecond => {
                Arc::new(TimestampMillisecondArray::from(vec![v]).with_timezone("UTC"))
            }
            _ => Arc::new(TimestampMicrosecondArray::from(vec![v]).with_timezone("UTC")),
        }
    };
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["AAPL"])) as ArrayRef,
            time(1),
            time(2),
            time(3),
        ],
    )
    .unwrap();
    w.add_batch(RowGroupId(0), &batch).unwrap();
    open(w.finish().unwrap().bytes)
}

/// The time types a folded segment records (its bounds column's, or its
/// valid and known columns') are checked: a dataset that declares the
/// columns in another unit refuses the segment, and two segments that
/// recorded different units never merge (their `i64` times would mix
/// units silently).
#[test]
fn recorded_time_types_are_checked_against_the_dataset_and_never_merged() {
    let (us_schema, ms_schema) = (
        timed_schema(TimeUnit::Microsecond),
        timed_schema(TimeUnit::Millisecond),
    );
    for spec in [bounded(true), bitemporal(), bloom_bounded()] {
        let us = timed_segment(&spec, TimeUnit::Microsecond);
        let ms = timed_segment(&spec, TimeUnit::Millisecond);
        us.check_dataset(&us_schema).unwrap();
        ms.check_dataset(&ms_schema).unwrap();
        let err = us.check_dataset(&ms_schema).unwrap_err().to_string();
        assert!(
            err.contains("recorded its time columns as") && err.contains("re-index"),
            "{}: {err}",
            spec.identity()
        );
        assert!(
            merge(&[us, ms], &WriterOptions::default()).is_err(),
            "{}: merged across time units",
            spec.identity()
        );
    }
}

// ---------------------------------------------------------------------------
// Oracle over a Parquet data file
// ---------------------------------------------------------------------------

const SYMBOLS: [Option<&str>; 6] = [
    Some("AAPL"),
    Some("MSFT"),
    Some("GOOG"),
    Some("NVDA"),
    Some(""),
    None,
];
const EDGES: [i64; 7] = [i64::MIN, i64::MIN + 1, -DAY_US, -1, 0, DAY_US, i64::MAX];
const ROW_GROUPS: usize = 7;
const ROWS_PER_GROUP: usize = 23;

/// A small deterministic generator, so the file is the same every run.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn time(&mut self) -> Option<i64> {
        match self.below(10) {
            0..=1 => None,
            2..=3 => Some(EDGES[self.below(EDGES.len())]),
            _ => Some(self.below(200) as i64 - 100),
        }
    }
}

fn file_schema() -> Arc<Schema> {
    let ts = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, true),
        Field::new("ts", ts.clone(), true),
        Field::new("valid", ts.clone(), true),
        Field::new("known", ts, true),
    ]))
}

/// A Parquet file of `ROW_GROUPS` row groups. Row group 3 holds `"NVDA"`
/// only with null times, so its bounds are the all-null sentinel there.
fn data_file(seed: u64) -> Bytes {
    let mut rng = Rng(seed);
    let schema = file_schema();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(ROWS_PER_GROUP))
        .build();
    let mut buf = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).unwrap();
    for rg in 0..ROW_GROUPS {
        let (mut sym, mut ts, mut valid, mut known) = (vec![], vec![], vec![], vec![]);
        for _ in 0..ROWS_PER_GROUP {
            let s = SYMBOLS[rng.below(SYMBOLS.len())];
            let forced_null = rg == 3 && s == Some("NVDA");
            sym.push(s);
            ts.push(if forced_null { None } else { rng.time() });
            valid.push(if forced_null { None } else { rng.time() });
            // Restatements: the same valid time known at several times.
            known.push(if rng.below(3) == 0 {
                valid[valid.len() - 1]
            } else {
                rng.time()
            });
        }
        let col = |v: Vec<Option<i64>>| -> ArrayRef {
            Arc::new(TimestampMicrosecondArray::from(v).with_timezone("UTC"))
        };
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(sym)) as ArrayRef,
                col(ts),
                col(valid),
                col(known),
            ],
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.flush().unwrap();
    }
    w.close().unwrap();
    Bytes::from(buf)
}

/// Row group `rg` of the file, as its batches.
fn row_group(file: &Bytes, rg: usize) -> Vec<RecordBatch> {
    ParquetRecordBatchReaderBuilder::try_new(file.clone())
        .unwrap()
        .with_row_groups(vec![rg])
        .build()
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

/// Builds a folded segment over row groups `rgs` of the file, from its
/// batches, as an index build does.
fn build(spec: &KeySpec, file: &Bytes, rgs: std::ops::Range<usize>) -> Vec<u8> {
    let opts = WriterOptions {
        block_keys: 2,
        ..WriterOptions::default()
    };
    let mut w = SegmentWriter::for_dataset(spec.clone(), &file_schema(), opts).unwrap();
    w.set_rowgroup_range(RowGroupId(rgs.start as u32)..RowGroupId(rgs.end as u32))
        .unwrap();
    for rg in rgs {
        for batch in row_group(file, rg) {
            w.add_batch(RowGroupId(rg as u32), &batch).unwrap();
        }
    }
    w.finish().unwrap().bytes
}

/// Every row of the file: (row group, symbol, ts, valid, known), read
/// value by value from the Parquet file, with no segment code.
type Row = (u32, Option<String>, Option<i64>, Option<i64>, Option<i64>);

fn rows(file: &Bytes) -> Vec<Row> {
    let mut out = Vec::new();
    for rg in 0..ROW_GROUPS {
        for batch in row_group(file, rg) {
            let s = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let t = |i: usize| {
                batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .clone()
            };
            let (ts, valid, known) = (t(1), t(2), t(3));
            let opt = |a: &TimestampMicrosecondArray, i: usize| (!a.is_null(i)).then(|| a.value(i));
            for i in 0..batch.num_rows() {
                out.push((
                    rg as u32,
                    (!s.is_null(i)).then(|| s.value(i).to_string()),
                    opt(&ts, i),
                    opt(&valid, i),
                    opt(&known, i),
                ));
            }
        }
    }
    out
}

/// The oracle's value records: per key (rows with a null symbol are not
/// indexed), the row groups holding it, its rows there, and the `[min,
/// max]` of its non-null times there or the sentinel.
fn oracle_bounded(rows: &[Row]) -> BTreeMap<Vec<u8>, KeyValue> {
    let mut per: BTreeMap<Vec<u8>, BTreeMap<u32, (u64, Option<(i64, i64)>)>> = BTreeMap::new();
    for (rg, sym, ts, _, _) in rows {
        let Some(sym) = sym else { continue };
        let e = per
            .entry(utf8(sym))
            .or_default()
            .entry(*rg)
            .or_insert((0, None));
        e.0 += 1;
        if let Some(t) = ts {
            e.1 = Some(e.1.map_or((*t, *t), |(lo, hi)| (lo.min(*t), hi.max(*t))));
        }
    }
    per.into_iter()
        .map(|(k, rgs)| {
            let v = KeyValue {
                bitmap: rgs.keys().copied().collect(),
                counts: Some(rgs.values().map(|v| v.0).collect()),
                bounds: Some(
                    rgs.values()
                        .map(|v| v.1.unwrap_or(ALL_NULL_BOUNDS))
                        .collect(),
                ),
                ..KeyValue::default()
            };
            (k, v)
        })
        .collect()
}

/// The oracle's bitemporal records: per key, every distinct `(valid,
/// known, row group)` over rows where neither time is null; a key with no
/// such row is not stored.
fn oracle_bitemporal(rows: &[Row]) -> BTreeMap<Vec<u8>, KeyValue> {
    let mut per: BTreeMap<Vec<u8>, BTreeSet<(i64, i64, u32)>> = BTreeMap::new();
    for (rg, sym, _, valid, known) in rows {
        if let (Some(sym), Some(v), Some(k)) = (sym, valid, known) {
            per.entry(utf8(sym)).or_default().insert((*v, *k, *rg));
        }
    }
    per.into_iter()
        .map(|(k, t)| {
            let v = KeyValue {
                bitmap: t.iter().map(|x| x.2).collect(),
                triples: t.into_iter().collect(),
                ..KeyValue::default()
            };
            (k, v)
        })
        .collect()
}

fn all_keys() -> Vec<Vec<u8>> {
    let mut k: Vec<Vec<u8>> = SYMBOLS.iter().flatten().map(|s| utf8(s)).collect();
    k.push(utf8("IBM"));
    k
}

#[test]
fn folded_segments_equal_an_oracle_over_a_parquet_file() {
    let windows = {
        let mut w = vec![
            (i64::MIN, i64::MAX),
            (0, i64::MAX),
            (i64::MIN, 0),
            (i64::MAX, i64::MAX),
            (i64::MIN, i64::MIN),
            (-1, 1),
            (DAY_US, DAY_US),
            (5, 4),
        ];
        for lo in (-110..110).step_by(13) {
            for len in [0, 3, 40] {
                w.push((lo, lo + len));
            }
        }
        w
    };
    let keys = all_keys();
    for seed in [0x9e37_79b9_7f4a_7c15_u64, 0x0123_4567_89ab_cdef, 42] {
        let file = data_file(seed);
        let rows = rows(&file);

        // Bounds, counted.
        let spec = bounded(true);
        let whole = build(&spec, &file, 0..ROW_GROUPS);
        let r = open(whole.clone());
        assert_eq!(r.header().time_types.as_ref().map(Vec::len), Some(1));
        let want = oracle_bounded(&rows);
        assert_eq!(r.get(&keys).unwrap(), want, "seed {seed:#x}: value records");
        let nvda = &want[&utf8("NVDA")];
        let at3 = nvda.bitmap.iter().position(|rg| rg == 3).unwrap();
        assert_eq!(nvda.bounds.as_ref().unwrap()[at3], ALL_NULL_BOUNDS);
        let stored = r.bounds(&keys).unwrap().unwrap();
        for (key, v) in &want {
            for &(lo, hi) in &windows {
                // The plan through the stored bounds.
                let planned: BTreeSet<u32> = stored
                    .get(key)
                    .into_iter()
                    .flatten()
                    .filter(|b| b.overlaps(lo, hi))
                    .map(|b| b.rg.0)
                    .collect();
                // Exactly the row groups whose per-key range overlaps.
                let overlapping: BTreeSet<u32> = v
                    .bitmap
                    .iter()
                    .zip(v.bounds.as_ref().unwrap())
                    .filter(|(_, &(a, b))| {
                        (a, b) != ALL_NULL_BOUNDS && lo <= hi && a <= hi && lo <= b
                    })
                    .map(|(rg, _)| rg)
                    .collect();
                assert_eq!(planned, overlapping, "seed {seed:#x} [{lo}, {hi}]");
                // Never missing a row in the window (invariant 1).
                let holding: BTreeSet<u32> = rows
                    .iter()
                    .filter(|(_, s, t, _, _)| {
                        s.as_deref().map(utf8).as_ref() == Some(key)
                            && t.is_some_and(|t| lo <= t && t <= hi)
                    })
                    .map(|r| r.0)
                    .collect();
                assert!(holding.is_subset(&planned), "seed {seed:#x} [{lo}, {hi}]");
            }
        }
        // A split build merges back to the same bytes.
        let parts = [
            open(build(&spec, &file, 0..3)),
            open(build(&spec, &file, 3..ROW_GROUPS)),
        ];
        let opts = WriterOptions {
            block_keys: 2,
            ..WriterOptions::default()
        };
        assert_eq!(merge(&parts, &opts).unwrap().bytes, whole, "seed {seed:#x}");

        // Bitemporal versions.
        let spec = bitemporal();
        let whole = build(&spec, &file, 0..ROW_GROUPS);
        let r = open(whole.clone());
        assert_eq!(r.header().time_types.as_ref().map(Vec::len), Some(2));
        assert_eq!(
            r.get(&keys).unwrap(),
            oracle_bitemporal(&rows),
            "seed {seed:#x}: bitemporal records"
        );
        let parts = [
            open(build(&spec, &file, 0..4)),
            open(build(&spec, &file, 4..ROW_GROUPS)),
        ];
        assert_eq!(merge(&parts, &opts).unwrap().bytes, whole, "seed {seed:#x}");
    }
}

// ---------------------------------------------------------------------------
// Corruption sweeps
// ---------------------------------------------------------------------------

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

/// Opens corrupted bytes and reads every view of the folded values and a
/// merge. Returns a description of a wrong answer, if one was read: every
/// key's whole value record (bitmap, counts, bounds with the sentinel,
/// versions), no key missing, or a typed refusal.
fn exercise(b: Vec<u8>, want: &BTreeMap<Vec<u8>, KeyValue>) -> Option<String> {
    let r = SegmentReader::open(b).ok()?;
    let keys: Vec<Vec<u8>> = want.keys().cloned().collect();
    let _ = r.lookup(&keys);
    let _ = r.counts(&keys);
    let _ = r.bounds(&keys);
    let _ = r.triples(&keys);
    let _ = merge(std::slice::from_ref(&r), &WriterOptions::default());
    match r.get(&keys) {
        Ok(got) if got != *want => Some(format!("{got:?}")),
        _ => None,
    }
}

#[test]
fn corrupt_folded_segments_never_panic_or_read_wrong() {
    let mut uncounted = SegmentWriter::new(bounded(false), WriterOptions::default()).unwrap();
    uncounted
        .insert_timed(&utf8("AAPL"), RowGroupId(0), Some(i64::MIN))
        .unwrap();
    uncounted
        .insert_timed(&utf8("AAPL"), RowGroupId(1), None)
        .unwrap();
    let uncounted_want = BTreeMap::from([(
        utf8("AAPL"),
        KeyValue {
            bitmap: bitmap(&[0, 1]),
            bounds: Some(vec![(i64::MIN, i64::MIN), ALL_NULL_BOUNDS]),
            ..KeyValue::default()
        },
    )]);
    let mut bad = Vec::new();
    for (name, bytes, want) in [
        ("exact@ts,counts", bounded_segment(), clean_bounded()),
        (
            "exact@ts",
            uncounted.finish().unwrap().bytes,
            uncounted_want,
        ),
        ("temporal2", bitemporal_segment(), clean_bitemporal()),
        ("bloom bounds", bloom_bounds_segment(), clean_bloom_bounds()),
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
                    bad.push(format!("{name} pos {pos} = {v:#x}: wrong value {w}"));
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
