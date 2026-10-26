//! Invariant 1: no false negatives in pruning. If a row group contains a
//! queried key, the result includes it, for the exact, bloom, range, and
//! temporal paths, over random layouts, random segment splits, random block
//! sizes, merged segments, and random key sets (present and absent keys,
//! nulls in key and timestamp columns), and when files disagree with the
//! dataset schema on decimal scale, decimal width, or timestamp unit.
#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::many_single_char_names,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::needless_for_each,
    clippy::case_sensitive_file_extension_comparisons
)]

mod common;

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Decimal128Array, Decimal256Array, Int64Array, RecordBatch, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray,
};
use arrow::datatypes::{i256, DataType, Field, Schema, TimeUnit};
use bellwether_core::encoding::{encode_tuple_as, Value};
use bellwether_core::{merge, temporal, SegmentReader, SegmentWriter, WriterOptions};
use bellwether_types::{KeySpec, RowGroupId};
use common::*;
use proptest::prelude::*;
use roaring::RoaringBitmap;

fn opts(block_keys: usize, range_block: usize) -> WriterOptions {
    WriterOptions {
        block_keys,
        range_block_entries: range_block,
        ..WriterOptions::default()
    }
}

/// Every reader set to check: the split segments, and one merged segment.
fn reader_sets(
    spec: &KeySpec,
    layout: &Layout,
    cuts: &[usize],
    o: &WriterOptions,
) -> Vec<Vec<SegmentReader<Vec<u8>>>> {
    let split = open(&build(spec, layout, cuts, o));
    let merged = merge(&split, o).unwrap();
    vec![split, open(&[merged])]
}

fn union(
    readers: &[SegmentReader<Vec<u8>>],
    f: impl Fn(&SegmentReader<Vec<u8>>) -> RoaringBitmap,
) -> RoaringBitmap {
    readers
        .iter()
        .map(f)
        .fold(RoaringBitmap::new(), |a, b| a | b)
}

fn union_lookup(readers: &[SegmentReader<Vec<u8>>], keys: &[Vec<u8>]) -> RoaringBitmap {
    union(readers, |r| r.lookup(keys).unwrap())
}

fn queries() -> impl Strategy<Value = Vec<(i64, String)>> {
    prop::collection::vec(
        (
            0i64..18,
            prop::sample::select(TAGS.to_vec()).prop_map(String::from),
        ),
        0..10,
    )
}

/// The stored tuple of a row under ADR 014 as amended: none if the leading
/// column is null, a trailing null encoded as the 0x00 marker.
fn row_key(r: &Row) -> Option<Vec<u8>> {
    let id = r.id?;
    Some(match r.tag.as_deref() {
        Some(t) => key(id, t),
        None => null_probe(id),
    })
}

/// `(id, NULL)`: the stored form of a row with a null tag, and a probe that
/// must match nothing.
fn null_probe(id: i64) -> Vec<u8> {
    let mut k = id_key(id);
    k.push(0x00);
    k
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn exact_has_no_false_negatives(layout in layout(), cuts in prop::collection::vec(0usize..20, 0..3),
                                    bk in 1usize..6, q in queries(),
                                    lo in 0i64..18, width in 0i64..6,
                                    tlo in prop::sample::select(TAGS.to_vec()),
                                    thi in prop::sample::select(TAGS.to_vec())) {
        let keys: Vec<Vec<u8>> = q.iter().map(|(i, t)| key(*i, t)).collect();
        let want = q.iter().fold(RoaringBitmap::new(), |acc, (i, t)| acc | truth(&layout, |r| has_key(r, *i, t)));
        let hi = lo + width;
        let (klo, khi) = (key(lo, tlo), key(hi, thi));
        let want_full_range = truth(&layout, |r| row_key(r).is_some_and(|k| k >= klo && k <= khi));
        let want_prefix_range = truth(&layout, |r| r.id.is_some_and(|v| v >= lo && v <= hi));
        for readers in reader_sets(&exact(), &layout, &cuts, &opts(bk, 2)) {
            // exact is exact: no false negatives and no false positives
            prop_assert_eq!(&union_lookup(&readers, &keys), &want);
            // prefix seek on the leading column finds rows whose trailing
            // column is null too (ADR 014, amended)
            for (i, _) in &q {
                let got = union(&readers, |r| r.lookup_prefix(&id_key(*i)).unwrap());
                prop_assert_eq!(got, truth(&layout, |r| r.id == Some(*i)));
                // a probe containing a null matches nothing
                prop_assert!(union_lookup(&readers, &[null_probe(*i)]).is_empty());
            }
            // full-key range and leading-column range
            prop_assert_eq!(&union(&readers, |r| r.lookup_range(&klo, &khi).unwrap()), &want_full_range);
            prop_assert_eq!(&union(&readers, |r| r.lookup_prefix_range(&id_key(lo), &id_key(hi)).unwrap()), &want_prefix_range);
        }
    }

    #[test]
    fn string_leading_composite_has_no_false_negatives(layout in layout(), cuts in prop::collection::vec(0usize..20, 0..3),
                                                       bk in 1usize..6, q in queries()) {
        let keys: Vec<Vec<u8>> = q.iter().map(|(i, t)| tag_key(t, *i)).collect();
        let want = q.iter().fold(RoaringBitmap::new(), |acc, (i, t)| acc | truth(&layout, |r| has_key(r, *i, t)));
        for readers in reader_sets(&exact_tag_first(), &layout, &cuts, &opts(bk, 2)) {
            prop_assert_eq!(&union_lookup(&readers, &keys), &want);
            for (_, t) in &q {
                // "a" must not select "a\0": string delimiting is load-bearing
                let got = union(&readers, |r| r.lookup_prefix(&tag_prefix(t)).unwrap());
                prop_assert_eq!(got, truth(&layout, |r| r.tag.as_deref() == Some(t.as_str())));
            }
        }
    }

    #[test]
    fn bloom_has_no_false_negatives(layout in layout(), cuts in prop::collection::vec(0usize..20, 0..3),
                                    fpr in prop_oneof![Just(0.5), Just(0.1), Just(0.001)], q in queries()) {
        let keys: Vec<Vec<u8>> = q.iter().map(|(i, t)| key(*i, t)).collect();
        let want = q.iter().fold(RoaringBitmap::new(), |acc, (i, t)| acc | truth(&layout, |r| has_key(r, *i, t)));
        for readers in reader_sets(&bloom(fpr), &layout, &cuts, &opts(4, 2)) {
            let got = union_lookup(&readers, &keys);
            prop_assert!(want.is_subset(&got), "missing {:?}", &want - &got);
            for (i, _) in &q {
                prop_assert!(union_lookup(&readers, &[null_probe(*i)]).is_empty());
            }
        }
    }

    #[test]
    fn range_has_no_false_negatives(layout in layout(), cuts in prop::collection::vec(0usize..20, 0..3),
                                    rb in 1usize..5, ids in prop::collection::vec(0i64..18, 0..6),
                                    lo in 0i64..18, width in 0i64..6) {
        let keys: Vec<Vec<u8>> = ids.iter().map(|i| id_key(*i)).collect();
        let want = ids.iter().fold(RoaringBitmap::new(), |acc, i| acc | truth(&layout, |r| r.id == Some(*i)));
        let hi = lo + width;
        let want_range = truth(&layout, |r| r.id.is_some_and(|v| v >= lo && v <= hi));
        for readers in reader_sets(&range(), &layout, &cuts, &opts(4, rb)) {
            let got = union_lookup(&readers, &keys);
            prop_assert!(want.is_subset(&got), "missing {:?}", &want - &got);
            let got = union(&readers, |r| r.lookup_range(&id_key(lo), &id_key(hi)).unwrap());
            prop_assert!(want_range.is_subset(&got), "range missing {:?}", &want_range - &got);
        }
    }

    #[test]
    fn temporal_has_no_false_negatives(layout in layout(), cuts in prop::collection::vec(0usize..20, 0..3),
                                       bk in 1usize..6, q in queries(), asof_ms in 0i64..45) {
        let keys: Vec<Vec<u8>> = q.iter().map(|(i, t)| key(*i, t)).collect();
        let want = q.iter().fold(RoaringBitmap::new(), |acc, (i, t)| acc | truth(&layout, |r| has_key(r, *i, t)));
        for readers in reader_sets(&temporal(), &layout, &cuts, &opts(bk, 2)) {
            prop_assert_eq!(&union_lookup(&readers, &keys), &want);
            for (i, _) in &q {
                let got = union(&readers, |r| r.lookup_prefix(&id_key(*i)).unwrap());
                prop_assert_eq!(got, truth(&layout, |r| r.id == Some(*i)));
                prop_assert!(union_lookup(&readers, &[null_probe(*i)]).is_empty());
                prop_assert!(temporal::latest(&readers, &[null_probe(*i)]).unwrap().is_empty());
            }
            let latest = temporal::latest(&readers, &keys).unwrap();
            let asof = temporal::asof(&readers, &keys, asof_ms).unwrap();
            for (i, t) in &q {
                let k = key(*i, t);
                for (bound, got, label) in [(None, &latest, "latest"), (Some(asof_ms), &asof, "asof")] {
                    match (true_version(&layout, *i, t, bound), got.get(&k)) {
                        (None, None) => {}
                        (Some((ts, rgs)), Some(h)) => {
                            prop_assert_eq!(h.ts, ts, "{} ts", label);
                            prop_assert!(rgs.is_subset(&h.row_groups), "{} missing row groups", label);
                            prop_assert_eq!(&h.row_groups, &rgs);
                        }
                        (e, h) => prop_assert!(false, "{label}: expected {e:?}, got {h:?}"),
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Files that disagree with the dataset schema
// ---------------------------------------------------------------------------

const DATASET_DECIMAL: DataType = DataType::Decimal128(20, 3);

fn amt_schema(dt: DataType) -> Schema {
    Schema::new(vec![Field::new("amt", dt, true)])
}

/// A file's `amt` column holding `cents` (scale 2 values) in one of three
/// physical types: Decimal128(18,2), Decimal128(20,3), Decimal256(40,3).
fn decimal_file(kind: u8, cents: &[Option<i64>]) -> RecordBatch {
    let col: ArrayRef = match kind % 3 {
        0 => Arc::new(
            cents
                .iter()
                .map(|c| c.map(i128::from))
                .collect::<Decimal128Array>()
                .with_precision_and_scale(18, 2)
                .unwrap(),
        ),
        1 => Arc::new(
            cents
                .iter()
                .map(|c| c.map(|c| i128::from(c) * 10))
                .collect::<Decimal128Array>()
                .with_precision_and_scale(20, 3)
                .unwrap(),
        ),
        _ => Arc::new(
            cents
                .iter()
                .map(|c| c.map(|c| i256::from_i128(i128::from(c) * 10)))
                .collect::<Decimal256Array>()
                .with_precision_and_scale(40, 3)
                .unwrap(),
        ),
    };
    RecordBatch::try_from_iter(vec![("amt", col)]).unwrap()
}

fn cents_key(c: i64) -> Vec<u8> {
    encode_tuple_as(
        &[Value::Decimal128 {
            value: i128::from(c),
            scale: 2,
        }],
        &[DATASET_DECIMAL],
    )
    .unwrap()
}

fn amt_segment(spec: &KeySpec, files: &[(u8, Vec<Option<i64>>)]) -> SegmentReader<Vec<u8>> {
    let mut w = SegmentWriter::for_dataset(
        spec.clone(),
        &amt_schema(DATASET_DECIMAL),
        WriterOptions {
            block_keys: 3,
            range_block_entries: 2,
            ..WriterOptions::default()
        },
    )
    .unwrap();
    for (rg, (kind, cents)) in files.iter().enumerate() {
        w.add_batch(
            RowGroupId(u32::try_from(rg).unwrap()),
            &decimal_file(*kind, cents),
        )
        .unwrap();
    }
    SegmentReader::open(w.finish().unwrap().bytes).unwrap()
}

#[test]
fn decimal_at_two_scales_and_two_widths_is_one_key() {
    // rg0: 1.50 as Decimal128(18,2); rg1: 1.500 as Decimal128(20,3);
    // rg2: 1.500 as Decimal256(40,3). Dataset type Decimal128(20,3).
    let files = vec![
        (0u8, vec![Some(150)]),
        (1, vec![Some(150)]),
        (2, vec![Some(150)]),
    ];
    let exact = KeySpec::Exact {
        columns: vec!["amt".into()],
        counts: false,
        bounds: None,
    };
    let r = amt_segment(&exact, &files);
    assert_eq!(
        r.lookup(&[cents_key(150)])
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    // the same value typed at scale 3 is the same key
    let at_scale_3 = encode_tuple_as(
        &[Value::Decimal128 {
            value: 1500,
            scale: 3,
        }],
        &[DATASET_DECIMAL],
    )
    .unwrap();
    assert_eq!(at_scale_3, cents_key(150));
    assert_eq!(r.entries(), 1, "one distinct key, not three");
}

#[test]
fn unrepresentable_or_mismatched_columns_are_errors_not_misses() {
    let spec = KeySpec::Exact {
        columns: vec!["amt".into()],
        counts: false,
        bounds: None,
    };
    let mut w = SegmentWriter::for_dataset(
        spec.clone(),
        &amt_schema(DATASET_DECIMAL),
        WriterOptions::default(),
    )
    .unwrap();
    // 1.2345 is not exact at the dataset scale 3
    let scale4: ArrayRef = Arc::new(
        Decimal128Array::from(vec![12_345i128])
            .with_precision_and_scale(10, 4)
            .unwrap(),
    );
    let b = RecordBatch::try_from_iter(vec![("amt", scale4)]).unwrap();
    assert!(w.add_batch(RowGroupId(0), &b).is_err());
    // an integer column where the dataset declares a decimal
    let ints: ArrayRef = Arc::new(Int64Array::from(vec![1i64]));
    let b = RecordBatch::try_from_iter(vec![("amt", ints)]).unwrap();
    assert!(w.add_batch(RowGroupId(1), &b).is_err());
    // add_batch without a dataset schema is refused
    let mut raw = SegmentWriter::new(spec, WriterOptions::default()).unwrap();
    assert!(raw
        .add_batch(RowGroupId(0), &decimal_file(0, &[Some(1)]))
        .is_err());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn decimal_files_of_mixed_scale_and_width_have_no_false_negatives(
        files in prop::collection::vec((0u8..3, prop::collection::vec(prop::option::weighted(0.9, -20i64..20), 0..12)), 1..10),
        q in prop::collection::vec(-22i64..22, 0..8),
    ) {
        let keys: Vec<Vec<u8>> = q.iter().map(|c| cents_key(*c)).collect();
        let want: RoaringBitmap = files.iter().enumerate()
            .filter(|(_, (_, cents))| cents.iter().any(|c| c.is_some_and(|c| q.contains(&c))))
            .map(|(i, _)| u32::try_from(i).unwrap())
            .collect();
        let exact = amt_segment(&KeySpec::Exact { columns: vec!["amt".into()], counts: false, bounds: None, }, &files);
        prop_assert_eq!(&exact.lookup(&keys).unwrap(), &want);
        let bloom = amt_segment(&KeySpec::Bloom { columns: vec!["amt".into()], fpr: 0.05, bounds: None, }, &files);
        prop_assert!(want.is_subset(&bloom.lookup(&keys).unwrap()));
        let range = amt_segment(&KeySpec::Range { column: "amt".into() }, &files);
        prop_assert!(want.is_subset(&range.lookup(&keys).unwrap()));
    }
}

#[test]
fn timestamp_units_across_files_encode_in_the_dataset_unit() {
    // Dataset unit is microseconds; files store ms, us, and ns.
    let us = DataType::Timestamp(TimeUnit::Microsecond, None);
    let schema = Schema::new(vec![Field::new("t", us.clone(), true)]);
    let spec = KeySpec::Exact {
        columns: vec!["t".into()],
        counts: false,
        bounds: None,
    };
    let mut w = SegmentWriter::for_dataset(spec, &schema, WriterOptions::default()).unwrap();
    let files: Vec<ArrayRef> = vec![
        Arc::new(TimestampMillisecondArray::from(vec![5i64])),
        Arc::new(TimestampMicrosecondArray::from(vec![5_000i64])),
        Arc::new(TimestampNanosecondArray::from(vec![5_000_000i64])),
    ];
    for (rg, col) in files.into_iter().enumerate() {
        let b = RecordBatch::try_from_iter(vec![("t", col)]).unwrap();
        w.add_batch(RowGroupId(u32::try_from(rg).unwrap()), &b)
            .unwrap();
    }
    // a nanosecond value that is not a whole microsecond is an error
    let bad: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![5_000_001i64]));
    assert!(w
        .add_batch(
            RowGroupId(9),
            &RecordBatch::try_from_iter(vec![("t", bad)]).unwrap()
        )
        .is_err());
    let r = SegmentReader::open(w.finish().unwrap().bytes).unwrap();
    let k = encode_tuple_as(
        &[Value::Timestamp {
            value: 5_000,
            unit: TimeUnit::Microsecond,
        }],
        &[us],
    )
    .unwrap();
    assert_eq!(
        r.lookup(&[k]).unwrap().iter().collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
}
