//! Regression tests harvested from the three review rounds of the
//! `bellwether-core` pull request. Each test pins a defect the review found,
//! or a fix it verified, that the crate's other tests do not already cover.
//! Where another test covers part of a case, this file adds only the rest:
//!
//! - decimals at mixed scales and widths, through both `KeyColumns` and
//!   `SegmentWriter::for_dataset`, under several dataset types (one dataset
//!   type is covered in `no_false_negatives.rs`);
//! - Time64 overflow in a temporal timestamp column (the key path is covered
//!   in `encoding_order.rs`);
//! - the run cookie on every stored bitmap of exact and temporal segments,
//!   and a genuine run-format bitmap spliced into a segment (one flipped
//!   bitmap is covered in `segment_format.rs`);
//! - `add_batch` staging when a timestamp fails on a later row, and for a row
//!   group that already holds a ready filter (the decimal case is covered in
//!   `segment_format.rs`);
//! - recorded-type refusals when only the timestamp unit differs;
//! - signed-zero widening on merge, from a hand-patched unwidened segment;
//! - header validation of `key_types` arity and `ts_type` presence;
//! - ADR 014 nulls through `get`, `asof`, both `latest` entry points,
//!   `lookup_prefix_range`, and the writer;
//! - a seeded fuzz of `tuple_has_null`.
#![allow(clippy::unwrap_used, missing_docs)]

mod common;

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Decimal128Array, Decimal256Array, Int64Array, RecordBatch, StringArray,
    Time64MicrosecondArray, TimestampMillisecondArray,
};
use arrow::datatypes::{i256, DataType, Field, Schema, TimeUnit};
use bellwether_core::bloom::{CanonicalHash, Sbbf};
use bellwether_core::encoding::{
    encode_tuple, encode_tuple_as, encode_value_as, tuple_has_null, KeyColumns, Value,
};
use bellwether_core::segment::{checksum, ColumnType, SegmentHeader, FOOTER_LEN_V2, PREAMBLE_LEN};
use bellwether_core::{merge, temporal, SegmentReader, SegmentWriter, WriterOptions};
use bellwether_types::{KeySpec, RowGroupId};
use common::*;
use proptest::prelude::*;
use proptest::test_runner::RngSeed;
use roaring::RoaringBitmap;

fn rgs(b: &RoaringBitmap) -> Vec<u32> {
    b.iter().collect()
}

fn rg(i: usize) -> RowGroupId {
    RowGroupId(u32::try_from(i).unwrap())
}

fn one_column_batch(name: &str, col: ArrayRef) -> RecordBatch {
    RecordBatch::try_from_iter(vec![(name, col)]).unwrap()
}

fn temporal_schema(ts: DataType) -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("tag", DataType::Utf8, true),
        Field::new("ts", ts, true),
    ])
}

// ---------------------------------------------------------------------------
// Encoding at the dataset type
// ---------------------------------------------------------------------------

/// 1.50 stored as Decimal128(10,2), Decimal128(10,3), and Decimal256(40,3)
/// is one key under every dataset type, through `KeyColumns` and through the
/// writer, for exact, bloom, and range specs. Guards the first review's
/// blocking false negative: decimals encoded at the file's own scale.
#[test]
fn decimals_at_mixed_scales_and_widths_are_one_key_through_both_encoder_paths() {
    let files: [ArrayRef; 3] = [
        Arc::new(
            Decimal128Array::from(vec![150i128])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        ),
        Arc::new(
            Decimal128Array::from(vec![1500i128])
                .with_precision_and_scale(10, 3)
                .unwrap(),
        ),
        Arc::new(
            Decimal256Array::from(vec![i256::from_i128(1500)])
                .with_precision_and_scale(40, 3)
                .unwrap(),
        ),
    ];
    for dataset in [
        DataType::Decimal128(12, 2),
        DataType::Decimal128(20, 3),
        DataType::Decimal256(40, 3),
        DataType::Decimal256(50, 4),
    ] {
        let query = encode_tuple_as(
            &[Value::Decimal128 {
                value: 150,
                scale: 2,
            }],
            std::slice::from_ref(&dataset),
        )
        .unwrap();
        for file in &files {
            let kc = KeyColumns::new(std::slice::from_ref(file), std::slice::from_ref(&dataset))
                .unwrap();
            assert_eq!(kc.encode(0).unwrap().unwrap(), query, "{dataset}");
        }
        let schema = Schema::new(vec![Field::new("amt", dataset.clone(), true)]);
        for spec in [
            KeySpec::Exact {
                columns: vec!["amt".into()],
                counts: false,
                bounds: None,
            },
            KeySpec::Bloom {
                columns: vec!["amt".into()],
                fpr: 0.01,
                bounds: None,
            },
            KeySpec::Range {
                column: "amt".into(),
            },
        ] {
            let mut w = SegmentWriter::for_dataset(spec.clone(), &schema, WriterOptions::default())
                .unwrap();
            for (i, file) in files.iter().enumerate() {
                w.add_batch(rg(i), &one_column_batch("amt", file.clone()))
                    .unwrap();
            }
            let r = SegmentReader::open(w.finish().unwrap().bytes).unwrap();
            assert_eq!(
                rgs(&r.lookup(std::slice::from_ref(&query)).unwrap()),
                vec![0, 1, 2],
                "{dataset} {spec:?}"
            );
        }
    }
}

/// A Time64(us) timestamp column converted to a nanosecond dataset type
/// overflows i64: `add_batch` returns an error, never a panic or a wrapped
/// value that would rank the wrong version as latest.
#[test]
fn time64_overflow_in_a_temporal_ts_column_is_an_error_not_a_panic() {
    let schema = temporal_schema(DataType::Time64(TimeUnit::Nanosecond));
    let mut w = SegmentWriter::for_dataset(temporal(), &schema, WriterOptions::default()).unwrap();
    let b = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int64Array::from(vec![1i64])) as ArrayRef),
        ("tag", Arc::new(StringArray::from(vec!["a"])) as ArrayRef),
        (
            "ts",
            Arc::new(Time64MicrosecondArray::from(vec![i64::MAX / 10])) as ArrayRef,
        ),
    ])
    .unwrap();
    let res = catch_unwind(AssertUnwindSafe(|| w.add_batch(RowGroupId(0), &b)));
    assert!(matches!(res, Ok(Err(_))), "{res:?}");
    assert_eq!(w.finish().unwrap().rowgroup_range, None);
}

// ---------------------------------------------------------------------------
// Stored bitmaps
// ---------------------------------------------------------------------------

const NO_RUN_COOKIE: [u8; 4] = [0x3a, 0x30, 0x00, 0x00];

fn cookie_positions(bytes: &[u8]) -> Vec<usize> {
    bytes
        .windows(NO_RUN_COOKIE.len())
        .enumerate()
        .filter(|(_, w)| *w == NO_RUN_COOKIE)
        .map(|(i, _)| i)
        .collect()
}

/// Turning any stored bitmap's cookie into roaring's run cookie (0x3a to
/// 0x3b) makes that key's lookup and a merge return errors, in exact and
/// temporal segments, for every bitmap of every block. Guards the second
/// review's finding: roaring decoded an empty run container, a lookup
/// silently answered nothing, and merge panicked or laundered the corruption.
#[test]
fn every_stored_bitmap_with_a_run_cookie_is_refused_in_exact_and_temporal_segments() {
    let tags = ["a", "\0", "", "a\0"];
    let keys: Vec<Vec<u8>> = (0..10usize)
        .map(|i| key(i64::try_from(i).unwrap(), tags[i % 4]))
        .collect();
    let opts = WriterOptions {
        block_keys: 2,
        ..WriterOptions::default()
    };
    for spec in [exact(), temporal()] {
        let mut w = SegmentWriter::new(spec.clone(), opts.clone()).unwrap();
        for (i, k) in keys.iter().enumerate() {
            if matches!(spec, KeySpec::Temporal { .. }) {
                w.insert_version(k, Some(i64::try_from(i).unwrap()), rg(i % 4))
                    .unwrap();
            } else {
                w.insert(k, rg(i % 4)).unwrap();
            }
        }
        let bytes = w.finish().unwrap().bytes;
        let positions = cookie_positions(&bytes);
        assert_eq!(positions.len(), keys.len(), "{spec:?}: one bitmap per key");
        for at in positions {
            let mut b = bytes.clone();
            b[at] = 0x3b;
            let r = SegmentReader::open(b).unwrap();
            let outcome = catch_unwind(AssertUnwindSafe(|| {
                let failing = keys.iter().filter(|k| r.lookup(&[k]).is_err()).count();
                let merged = merge(std::slice::from_ref(&r), &WriterOptions::default());
                (failing, merged.is_err())
            }));
            assert_eq!(
                outcome.ok(),
                Some((1, true)),
                "{spec:?}: run cookie at byte {at}"
            );
        }
    }
}

/// A genuine run-format serialization (cookie `3b 30`) that roaring decodes
/// into a valid, non-empty bitmap whose no-run size equals the record's
/// length, spliced into a real segment in place of the same set's no-run
/// bitmap: only the cookie check refuses it.
#[test]
fn a_genuine_run_format_bitmap_spliced_into_a_segment_is_refused() {
    let mut record = vec![0x3b, 0x30, 0x08, 0x00]; // run cookie, 9 containers
    record.extend_from_slice(&[0x00, 0x01]); // container 8 is a run container
    for container in 0u16..9 {
        record.extend_from_slice(&container.to_le_bytes());
        record.extend_from_slice(&u16::from(container == 8).to_le_bytes());
    }
    record.extend_from_slice(&[0u8; 9 * 4]); // offsets, which readers ignore
    for _ in 0..8 {
        record.extend_from_slice(&5u16.to_le_bytes()); // one value per array
    }
    record.extend_from_slice(&1u16.to_le_bytes()); // one run
    record.extend_from_slice(&0u16.to_le_bytes()); // starting at 0
    record.extend_from_slice(&1u16.to_le_bytes()); // length minus one: {0, 1}
    let decoded = RoaringBitmap::deserialize_from(&record[..]).unwrap();
    assert_eq!(decoded.len(), 10);
    assert_eq!(decoded.serialized_size(), record.len());

    let k = key(1, "a");
    let mut w = SegmentWriter::new(exact(), WriterOptions::default()).unwrap();
    for id in &decoded {
        w.insert(&k, RowGroupId(id)).unwrap();
    }
    let bytes = w.finish().unwrap().bytes;
    let mut no_run = Vec::new();
    decoded.serialize_into(&mut no_run).unwrap();
    let at = bytes
        .windows(no_run.len())
        .position(|win| win == no_run.as_slice())
        .unwrap();
    let clean = SegmentReader::open(bytes.clone()).unwrap();
    assert_eq!(clean.lookup(&[&k]).unwrap(), decoded);
    let mut spliced = bytes;
    spliced[at..at + record.len()].copy_from_slice(&record);
    let r = SegmentReader::open(spliced).unwrap();
    assert!(r.lookup(&[&k]).is_err());
}

// ---------------------------------------------------------------------------
// add_batch staging
// ---------------------------------------------------------------------------

/// A timestamp that is not a whole second fails on the batch's last row:
/// nothing from the batch is recorded, and the writer stays usable.
#[test]
fn add_batch_records_nothing_when_a_timestamp_fails_on_a_later_row() {
    let schema = temporal_schema(DataType::Timestamp(TimeUnit::Second, None));
    let mut w = SegmentWriter::for_dataset(temporal(), &schema, WriterOptions::default()).unwrap();
    let batch = |ids: Vec<i64>, ms: Vec<i64>| {
        RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int64Array::from(ids)) as ArrayRef),
            ("tag", Arc::new(StringArray::from(vec!["a"; 2])) as ArrayRef),
            (
                "ts",
                Arc::new(TimestampMillisecondArray::from(ms)) as ArrayRef,
            ),
        ])
        .unwrap()
    };
    assert!(w
        .add_batch(RowGroupId(7), &batch(vec![1, 2], vec![1_000, 1_001]))
        .is_err());
    w.add_batch(RowGroupId(8), &batch(vec![1, 2], vec![2_000, 3_000]))
        .unwrap();
    let s = w.finish().unwrap();
    assert_eq!(s.rowgroup_range, Some((RowGroupId(8), RowGroupId(8))));
    let r = SegmentReader::open(s.bytes).unwrap();
    let latest = temporal::latest(std::slice::from_ref(&r), &[key(1, "a")]).unwrap();
    assert_eq!(latest[&key(1, "a")].ts, 2);
}

/// A row group that already holds a ready filter refuses a batch and records
/// nothing from it; another row group still takes the same batch.
#[test]
fn add_batch_into_a_row_group_with_a_ready_filter_records_nothing() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, true)]);
    let spec = KeySpec::Bloom {
        columns: vec!["id".into()],
        fpr: 0.1,
        bounds: None,
    };
    let mut w = SegmentWriter::for_dataset(spec, &schema, WriterOptions::default()).unwrap();
    let mut f = Sbbf::<CanonicalHash>::with_ndv_fpp(1, 0.1).unwrap();
    f.insert(&id_key(9));
    w.insert_filter(RowGroupId(3), 1, &f).unwrap();
    let b = one_column_batch("id", Arc::new(Int64Array::from(vec![1i64])));
    assert!(w.add_batch(RowGroupId(3), &b).is_err());
    w.add_batch(RowGroupId(4), &b).unwrap();
    let r = SegmentReader::open(w.finish().unwrap().bytes).unwrap();
    assert_eq!(rgs(&r.lookup(&[id_key(1)]).unwrap()), vec![4]);
    assert_eq!(rgs(&r.lookup(&[id_key(9)]).unwrap()), vec![3]);
}

// ---------------------------------------------------------------------------
// Recorded types
// ---------------------------------------------------------------------------

/// Two temporal segments of one spec whose timestamp columns differ only in
/// unit never fold, never merge, and never open against the other's schema:
/// `asof` timestamps in one unit would be read in the other.
#[test]
fn segments_whose_timestamp_units_differ_never_fold_merge_or_open_across() {
    let build = |unit: TimeUnit| {
        let schema = temporal_schema(DataType::Timestamp(unit, None));
        let w = SegmentWriter::for_dataset(temporal(), &schema, WriterOptions::default()).unwrap();
        w.finish().unwrap().bytes
    };
    let (ms, us) = (build(TimeUnit::Millisecond), build(TimeUnit::Microsecond));
    let pair = [
        SegmentReader::open(ms.clone()).unwrap(),
        SegmentReader::open(us).unwrap(),
    ];
    let k = key(1, "a");
    assert!(temporal::latest(&pair, &[&k]).is_err());
    assert!(temporal::asof(&pair, &[&k], 5).is_err());
    assert!(merge(&pair, &WriterOptions::default()).is_err());
    let us_schema = temporal_schema(DataType::Timestamp(TimeUnit::Microsecond, None));
    assert!(SegmentReader::open_for_dataset(ms.clone(), &us_schema).is_err());
    let ms_schema = temporal_schema(DataType::Timestamp(TimeUnit::Millisecond, None));
    assert!(SegmentReader::open_for_dataset(ms, &ms_schema).is_ok());
}

/// A header must record one key type per key column, and a timestamp type
/// exactly when a temporal spec records key types. Checked on constructed
/// headers and on a segment whose header is patched in place.
#[test]
fn header_validation_checks_key_types_arity_and_ts_type_presence() {
    let check = |spec: KeySpec, keys: Option<Vec<ColumnType>>, ts: Option<ColumnType>| {
        let mut h = SegmentHeader::new(spec);
        h.key_types = keys;
        h.ts_type = ts;
        h.validate(h.storage_version, h.encoding_version)
    };
    let ts = || {
        Some(ColumnType::Timestamp {
            unit: bellwether_core::segment::RecordedUnit::Millisecond,
            tz: None,
        })
    };
    let two = || Some(vec![ColumnType::Int64, ColumnType::Utf8]);
    assert!(check(exact(), two(), None).is_ok());
    assert!(
        check(exact(), Some(vec![ColumnType::Int64]), None).is_err(),
        "too few"
    );
    assert!(
        check(
            exact(),
            Some(vec![ColumnType::Int64, ColumnType::Utf8, ColumnType::Utf8]),
            None
        )
        .is_err(),
        "too many"
    );
    assert!(
        check(exact(), two(), ts()).is_err(),
        "ts type on a non-temporal spec"
    );
    assert!(check(temporal(), two(), ts()).is_ok());
    assert!(
        check(temporal(), two(), None).is_err(),
        "temporal without ts type"
    );
    assert!(
        check(temporal(), None, ts()).is_err(),
        "ts type without key types"
    );
    assert!(check(temporal(), None, None).is_ok(), "an untyped segment");

    // On disk: blank out the second recorded type, keeping every offset.
    let s = build(&exact(), &vec![vec![]], &[], &WriterOptions::default());
    let bytes = &s[0].bytes;
    let header_len =
        usize::try_from(u32::from_le_bytes(bytes[16..20].try_into().unwrap())).unwrap();
    let json = std::str::from_utf8(&bytes[PREAMBLE_LEN..PREAMBLE_LEN + header_len]).unwrap();
    let second = ",{\"type\":\"utf8\"}";
    assert!(json.contains(second), "{json}");
    let patched = json.replace(second, &" ".repeat(second.len()));
    let mut b = bytes.clone();
    b[PREAMBLE_LEN..PREAMBLE_LEN + header_len].copy_from_slice(patched.as_bytes());
    assert!(SegmentReader::open(bytes.clone()).is_ok());
    assert!(SegmentReader::open(b).is_err());
}

// ---------------------------------------------------------------------------
// Signed zero
// ---------------------------------------------------------------------------

/// Recomputes the checksums of a storage version 2 range segment with one
/// block after its bytes were patched: the block's (in its index entry) and
/// the footer's (over the preamble, the header, and the index).
fn reseal_one_block_range(bytes: &mut [u8]) {
    let u64_at = |b: &[u8], at: usize| u64::from_le_bytes(b[at..at + 8].try_into().unwrap());
    let u32_at = |b: &[u8], at: usize| u32::from_le_bytes(b[at..at + 4].try_into().unwrap());
    let foot = bytes.len() - FOOTER_LEN_V2;
    let index_offset = usize::try_from(u64_at(bytes, foot)).unwrap();
    let index_len = usize::try_from(u64_at(bytes, foot + 8)).unwrap();
    assert_eq!(u32_at(bytes, index_offset + 17), 1, "one block");
    // entry: first_min | max_of_max | off u64 | len u32 | n u32 | sum u64
    let mut at = index_offset + 21;
    for _ in 0..2 {
        at += 4 + usize::try_from(u32_at(bytes, at)).unwrap();
    }
    let off = usize::try_from(u64_at(bytes, at)).unwrap();
    let len = usize::try_from(u32_at(bytes, at + 8)).unwrap();
    let block_sum = checksum(&[&bytes[off..off + len]]);
    bytes[at + 16..at + 24].copy_from_slice(&block_sum.to_le_bytes());
    let header_end = PREAMBLE_LEN + usize::try_from(u32_at(bytes, 16)).unwrap();
    let sum = checksum(&[
        &bytes[..header_end],
        &bytes[index_offset..index_offset + index_len],
    ]);
    bytes[foot + 16..foot + 24].copy_from_slice(&sum.to_le_bytes());
}

/// Merge applies the Parquet signed-zero rule to its inputs' stored bounds.
/// The writer already widens every bound, so an unwidened segment, as an
/// older writer would store it, is made by patching -0.0 back to +0.0 (and
/// recomputing the checksums the patch invalidates).
#[test]
fn merge_widens_signed_zero_bounds_of_an_unwidened_segment() {
    let f = |x: f64| encode_tuple(&[Value::Float(x)]).unwrap();
    let mut w = SegmentWriter::new(
        KeySpec::Range { column: "x".into() },
        WriterOptions::default(),
    )
    .unwrap();
    w.insert_min_max(RowGroupId(0), &f(0.0), &f(0.0)).unwrap();
    let mut bytes = w.finish().unwrap().bytes;
    let (neg, pos) = (f(-0.0), f(0.0));
    let mut patched = 0;
    let mut i = 0;
    while i + neg.len() <= bytes.len() {
        if bytes[i..i + neg.len()] == neg[..] {
            bytes[i..i + neg.len()].copy_from_slice(&pos);
            patched += 1;
            i += neg.len();
        } else {
            i += 1;
        }
    }
    assert_eq!(
        patched, 2,
        "the stored minimum and the index's first minimum"
    );
    reseal_one_block_range(&mut bytes);
    let unwidened = SegmentReader::open(bytes).unwrap();
    assert!(unwidened.lookup(&[&neg]).unwrap().is_empty());
    let merged = SegmentReader::open(
        merge(&[unwidened], &WriterOptions::default())
            .unwrap()
            .bytes,
    )
    .unwrap();
    assert_eq!(rgs(&merged.lookup(&[&neg]).unwrap()), vec![0]);
    assert_eq!(rgs(&merged.lookup(&[&pos]).unwrap()), vec![0]);
}

// ---------------------------------------------------------------------------
// ADR 014 nulls
// ---------------------------------------------------------------------------

/// A null trailing column keeps the row prefix-seekable; a probe tuple that
/// contains a null matches nothing on every point-lookup entry point; a row
/// whose leading column is null is not indexed.
#[test]
fn nulls_follow_adr_014_on_exact_bloom_and_temporal_paths() {
    let row = |id: Option<i64>, tag: Option<&str>| Row {
        id,
        tag: tag.map(String::from),
        ts: Some(1),
    };
    // rg0 (5,"a"); rg1 (5,NULL); rg2 (NULL,"a"); rg3 (6,NULL)
    let layout: Layout = vec![
        vec![row(Some(5), Some("a"))],
        vec![row(Some(5), None)],
        vec![row(None, Some("a"))],
        vec![row(Some(6), None)],
    ];
    let mut null_probe = id_key(5);
    null_probe.push(0x00);
    let opts = WriterOptions {
        block_keys: 1,
        ..WriterOptions::default()
    };
    for spec in [exact(), temporal(), bloom(0.01)] {
        let segs = build(&spec, &layout, &[], &opts);
        assert_eq!(
            segs[0].rowgroup_range,
            Some((RowGroupId(0), RowGroupId(3))),
            "{spec:?}"
        );
        let r = SegmentReader::open(segs[0].bytes.clone()).unwrap();
        assert!(r.lookup(&[&null_probe]).unwrap().is_empty(), "{spec:?}");
        assert!(rgs(&r.lookup(&[key(5, "a")]).unwrap()).contains(&0));
        if matches!(spec, KeySpec::Bloom { .. }) {
            continue;
        }
        assert_eq!(rgs(&r.lookup_prefix(&id_key(5)).unwrap()), vec![0, 1]);
        assert_eq!(
            rgs(&r.lookup_prefix_range(&id_key(5), &id_key(6)).unwrap()),
            vec![0, 1, 3]
        );
        assert!(r.get(&[&null_probe]).unwrap().is_empty());
        if matches!(spec, KeySpec::Temporal { .. }) {
            let one = std::slice::from_ref(&r);
            assert!(r.latest(&[&null_probe]).unwrap().is_empty());
            assert!(r.asof(&[&null_probe], 10).unwrap().is_empty());
            assert!(temporal::latest(one, &[&null_probe]).unwrap().is_empty());
            assert!(temporal::asof(one, &[&null_probe], 10).unwrap().is_empty());
        }
    }
    // a row group whose only row has a null leading column records nothing
    let only_leading_null = build(&exact(), &vec![layout[2].clone()], &[], &opts);
    assert_eq!(only_leading_null[0].rowgroup_range, None);
}

/// One tuple element for the `tuple_has_null` fuzz.
#[derive(Clone, Debug)]
enum Element {
    Null,
    Boolean(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    Decimal128(i64),
    Decimal256(i64),
    Utf8(String),
    Binary(Vec<u8>),
    Date32(i32),
    Timestamp(i64),
}

fn element() -> impl Strategy<Value = Element> {
    prop_oneof![
        Just(Element::Null),
        any::<bool>().prop_map(Element::Boolean),
        any::<i64>().prop_map(Element::Int),
        any::<u64>().prop_map(Element::UInt),
        any::<f64>().prop_map(Element::Float),
        any::<i64>().prop_map(Element::Decimal128),
        any::<i64>().prop_map(Element::Decimal256),
        "[\\x00\\x01\\xff a]{0,5}".prop_map(Element::Utf8),
        prop::collection::vec(prop_oneof![Just(0u8), Just(0xFF), any::<u8>()], 0..6)
            .prop_map(Element::Binary),
        any::<i32>().prop_map(Element::Date32),
        any::<i64>().prop_map(Element::Timestamp),
    ]
}

fn encode_elements(elements: &[Element]) -> Vec<u8> {
    let mut out = Vec::new();
    for e in elements {
        let (value, dataset) = match e {
            Element::Null => {
                out.push(0x00);
                continue;
            }
            Element::Boolean(b) => (Value::Boolean(*b), DataType::Boolean),
            Element::Int(i) => (Value::Int(*i), DataType::Int64),
            Element::UInt(u) => (Value::UInt(*u), DataType::UInt64),
            Element::Float(f) => (Value::Float(*f), DataType::Float64),
            Element::Decimal128(v) => (
                Value::Decimal128 {
                    value: i128::from(*v),
                    scale: 0,
                },
                DataType::Decimal128(38, 0),
            ),
            Element::Decimal256(v) => (
                Value::Decimal256 {
                    value: i256::from_i128(i128::from(*v)),
                    scale: 0,
                },
                DataType::Decimal256(76, 0),
            ),
            Element::Utf8(s) => (Value::Utf8(s), DataType::Utf8),
            Element::Binary(b) => (Value::Binary(b), DataType::Binary),
            Element::Date32(d) => (Value::Date32(*d), DataType::Date32),
            Element::Timestamp(t) => (
                Value::Timestamp {
                    value: *t,
                    unit: TimeUnit::Microsecond,
                },
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
        };
        encode_value_as(&value, &dataset, &mut out).unwrap();
    }
    out
}

proptest! {
    // Seeded and bounded, so every run checks the same cases.
    #![proptest_config(ProptestConfig {
        cases: 2048,
        rng_seed: RngSeed::Fixed(0x00B3_11E7),
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    /// `tuple_has_null` reads every element kind exactly: a well-formed tuple
    /// contains a null precisely when one of its elements is the null marker,
    /// a truncated null-free tuple never reads as containing a null (so a
    /// probe is never dropped on a misreading), and no input panics.
    #[test]
    fn tuple_has_null_never_misreads_a_tuple(
        elements in prop::collection::vec(element(), 0..8),
        junk in prop::collection::vec(any::<u8>(), 0..24),
    ) {
        let bytes = encode_elements(&elements);
        let has_null = elements.iter().any(|e| matches!(e, Element::Null));
        prop_assert_eq!(tuple_has_null(&bytes), Some(has_null), "{:02x?}", bytes);
        if !has_null {
            for cut in 0..bytes.len() {
                prop_assert_ne!(tuple_has_null(&bytes[..cut]), Some(true), "cut {} of {:02x?}", cut, bytes);
            }
        }
        let _ = tuple_has_null(&junk);
        let mut tail = bytes;
        tail.extend_from_slice(&junk);
        let _ = tuple_has_null(&tail);
    }
}
