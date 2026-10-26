//! Agg keys (B7): round trip, a pinned conformance vector, nulls (ADR 014),
//! both float zeros, decimal scale, refusals, merge over disjoint row
//! groups only, an every-byte corruption sweep, and a property test of the
//! stored cells, the combined per-key aggregates, and `unanswered`/`cover`
//! against a brute-force oracle over the rows written.
#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::too_many_lines
)]

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Once};

use arrow::array::{
    ArrayRef, Decimal128Builder, Float64Builder, Int64Builder, RecordBatch, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether_core::agg::{
    check_spec, cover, render_scaled, sum_kind, unanswered, value_type, AggCell, AggSum, SumKind,
};
use bellwether_core::encoding::{encode_tuple, encode_tuple_as, Value};
use bellwether_core::merge::visit_merged_values;
use bellwether_core::segment::Layout;
use bellwether_core::{merge, SegmentReader, SegmentWriter, WriterOptions};
use bellwether_types::{AggregateFn, Hash, KeySpec, RowGroupId};
use proptest::prelude::*;
use roaring::RoaringBitmap;

const ALL: [AggregateFn; 4] = AggregateFn::CANONICAL;

fn spec(functions: &[AggregateFn]) -> KeySpec {
    KeySpec::Agg {
        columns: vec!["account".into()],
        value: "amount".into(),
        functions: AggregateFn::canonical(functions),
    }
}

fn schema(value: DataType) -> Schema {
    Schema::new(vec![
        Field::new("account", DataType::Utf8, true),
        Field::new("amount", value, true),
    ])
}

fn dec_schema() -> Schema {
    schema(DataType::Decimal128(18, 6))
}

/// Rows `(account, amount)` with amounts as unscaled `i128` at the batch's
/// decimal type.
fn dec_batch(rows: &[(Option<&str>, Option<i128>)], dt: &DataType) -> RecordBatch {
    let DataType::Decimal128(p, s) = dt else {
        panic!("{dt}")
    };
    let mut k = StringBuilder::new();
    let mut v = Decimal128Builder::new()
        .with_precision_and_scale(*p, *s)
        .unwrap();
    for (a, x) in rows {
        k.append_option(*a);
        v.append_option(*x);
    }
    let k: ArrayRef = Arc::new(k.finish());
    let v: ArrayRef = Arc::new(v.finish());
    RecordBatch::try_new(Arc::new(schema(dt.clone())), vec![k, v]).unwrap()
}

fn float_batch(rows: &[(Option<&str>, Option<f64>)]) -> RecordBatch {
    let mut k = StringBuilder::new();
    let mut v = Float64Builder::new();
    for (a, x) in rows {
        k.append_option(*a);
        v.append_option(*x);
    }
    let k: ArrayRef = Arc::new(k.finish());
    let v: ArrayRef = Arc::new(v.finish());
    RecordBatch::try_new(Arc::new(schema(DataType::Float64)), vec![k, v]).unwrap()
}

fn int_batch(rows: &[(Option<&str>, Option<i64>)]) -> RecordBatch {
    let mut k = StringBuilder::new();
    let mut v = Int64Builder::new();
    for (a, x) in rows {
        k.append_option(*a);
        v.append_option(*x);
    }
    let k: ArrayRef = Arc::new(k.finish());
    let v: ArrayRef = Arc::new(v.finish());
    RecordBatch::try_new(Arc::new(schema(DataType::Int64)), vec![k, v]).unwrap()
}

fn key(s: &str) -> Vec<u8> {
    encode_tuple(&[Value::Utf8(s)]).unwrap()
}

fn dec(v: i128) -> Vec<u8> {
    encode_tuple_as(
        &[Value::Decimal128 { value: v, scale: 6 }],
        &[DataType::Decimal128(18, 6)],
    )
    .unwrap()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn from_hex(s: &str) -> Vec<u8> {
    let s: String = s.split_whitespace().collect();
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

fn open(bytes: &[u8]) -> SegmentReader<Vec<u8>> {
    SegmentReader::open(bytes.to_vec()).unwrap()
}

fn opts(block_keys: usize) -> WriterOptions {
    WriterOptions {
        block_keys,
        ..WriterOptions::default()
    }
}

/// The pinned segment: `agg:account@amount{sum,count,min,max}` over a
/// `Decimal128(18,6)` column. rg 0: `a` 1.500000, `a` null, `b` -2.000000;
/// rg 1: `a` 0.250000, null key 9.000000 (not indexed, ADR 014); rg 2: `b`
/// null only.
fn pinned() -> Vec<u8> {
    let dt = DataType::Decimal128(18, 6);
    let rgs = [
        dec_batch(
            &[
                (Some("a"), Some(1_500_000)),
                (Some("a"), None),
                (Some("b"), Some(-2_000_000)),
            ],
            &dt,
        ),
        dec_batch(&[(Some("a"), Some(250_000)), (None, Some(9_000_000))], &dt),
        dec_batch(&[(Some("b"), None)], &dt),
    ];
    let mut w =
        SegmentWriter::for_dataset(spec(&ALL), &dec_schema(), WriterOptions::default()).unwrap();
    for (i, b) in rgs.iter().enumerate() {
        w.add_batch(RowGroupId(i as u32), b).unwrap();
    }
    w.set_rowgroup_range(RowGroupId(0)..RowGroupId(3)).unwrap();
    w.finish().unwrap().bytes
}

/// `docs/format.md`, "Agg keys": the conformance vector.
const PINNED_HEX: &str = concat!(
    "42575345474d4e5402000000010000002a0100007b2273746f726167655f76657273696f6e223a322c22656e636f6469",
    "6e675f76657273696f6e223a312c226b65795f73706563223a7b226b696e64223a22616767222c22636f6c756d6e7322",
    "3a5b226163636f756e74225d2c2276616c7565223a22616d6f756e74222c2266756e6374696f6e73223a5b2273756d22",
    "2c22636f756e74222c226d696e222c226d6178225d7d2c22636f646563223a226e6f6e65222c226c61796f7574223a22",
    "6b657965645f616767222c22626c6f6f6d5f68617368223a2263616e6f6e6963616c222c226b65795f7479706573223a",
    "5b7b2274797065223a2275746638227d5d2c2276616c75655f74797065223a7b2274797065223a22646563696d616c31",
    "3238222c22707265636973696f6e223a31382c227363616c65223a367d7d140000003a30000001000000000001001000",
    "00000000010002010160e316000000000000000000000000001200000005108000000000000000000000000016e36012",
    "00000005108000000000000000000000000016e36001000190d003000000000000000000000000001200000005108000",
    "000000000000000000000003d0901200000005108000000000000000000000000003d090140000003a30000001000000",
    "000001001000000000000200010001807be1ffffffffffffffffffffffffff1200000005107fffffffffffffffffffff",
    "ffffe17b801200000005107fffffffffffffffffffffffffe17b80010101000000000000000000000000000000000000",
    "000a00040100004004000000804000011000002400000100400000000801020000000000000004000000080000000661",
    "0000066200003e01000000000000960000003c445ea1d0abd717d4010000000000006a0000005b4b2a5b93c5ca800200",
    "00000000000001000000000200000001000000040000000661000004000000066200003e0200000000000020000000e9",
    "f06339f94be43f5e0200000000000040000000adf507283b6cc0b5020000009e020000000000005100000000000000f2",
    "56304a40a356c242575345474d4e54",
);

fn cell(rows: u64, nulls: u64, sum: Option<i128>, min: Option<i128>, max: Option<i128>) -> AggCell {
    AggCell {
        rows,
        nulls,
        sum: sum.map(AggSum::Int),
        min: min.map(dec),
        max: max.map(dec),
    }
}

#[test]
fn agg_segments_round_trip_one_cell_per_key_and_row_group() {
    let bytes = pinned();
    let r = SegmentReader::open_for_dataset(bytes.clone(), &dec_schema()).unwrap();
    assert_eq!(r.key_spec(), &spec(&ALL));
    assert_eq!(r.header().layout, Layout::KeyedAgg);
    assert_eq!(r.entries(), 2, "a and b; the null key is not indexed");
    assert_eq!(r.rowgroup_range(), Some((RowGroupId(0), RowGroupId(2))));
    let cells = r.agg_cells(&[key("a"), key("b"), key("zz")]).unwrap();
    assert_eq!(
        cells[&key("a")],
        vec![
            (
                RowGroupId(0),
                cell(2, 1, Some(1_500_000), Some(1_500_000), Some(1_500_000))
            ),
            (
                RowGroupId(1),
                cell(1, 0, Some(250_000), Some(250_000), Some(250_000))
            ),
        ]
    );
    // A cell of only nulls: counted, summed as an integer zero, no min/max.
    assert_eq!(
        cells[&key("b")],
        vec![
            (
                RowGroupId(0),
                cell(1, 0, Some(-2_000_000), Some(-2_000_000), Some(-2_000_000))
            ),
            (RowGroupId(2), cell(1, 1, Some(0), None, None)),
        ]
    );
    assert!(!cells.contains_key(&key("zz")));
    // The keyed lookups see the agg key's bitmaps as any keyed segment's.
    let got = r.get(&[key("a")]).unwrap();
    assert_eq!(
        got[&key("a")].bitmap,
        [0u32, 1].into_iter().collect::<RoaringBitmap>()
    );
    // The value column's dataset type is recorded and checked.
    assert_eq!(
        serde_json::to_value(r.header().value_type.as_ref().unwrap()).unwrap(),
        serde_json::json!({"type": "decimal128", "precision": 18, "scale": 6})
    );
    let other = schema(DataType::Decimal128(18, 4));
    assert!(SegmentReader::open_for_dataset(bytes, &other).is_err());
}

#[test]
fn the_pinned_agg_segment_matches_docs_format_md_byte_for_byte() {
    let bytes = pinned();
    if std::env::var("BELLWETHER_PRINT_VECTORS").is_ok() {
        println!("PINNED {}", hex(&bytes));
        println!("ID {}", Hash::of(&bytes));
    }
    assert_eq!(hex(&bytes), hex(&from_hex(PINNED_HEX)));
}

#[test]
fn combined_cells_and_coverage_answer_from_counts() {
    let r = open(&pinned());
    let cells = r.agg_cells(&[key("a")]).unwrap();
    let mut total = AggCell::empty();
    for (_, c) in &cells[&key("a")] {
        total.combine(c).unwrap();
    }
    assert_eq!(
        total,
        cell(3, 1, Some(1_750_000), Some(250_000), Some(1_500_000))
    );
    assert_eq!(total.values(), 2);
    assert_eq!(total.sum.unwrap().render(Some(6)), "1.750000");
    let per_rg: BTreeMap<u32, AggCell> = cells[&key("a")]
        .iter()
        .map(|(rg, c)| (rg.0, c.clone()))
        .collect();
    // Counts equal to the cells' rows: everything answered.
    let counts: BTreeMap<u32, u64> = [(0, 2), (1, 1)].into_iter().collect();
    let spec = spec(&ALL);
    let none = unanswered(&counts, &per_rg, true, &spec).unwrap();
    assert!(none.is_empty());
    let c = cover(&counts, &per_rg, &none).unwrap();
    assert_eq!((c.rows, c.residual_rows), (3, 0));
    assert_eq!(c.covered.unwrap(), total);
    // A finer lookup counting fewer rows in row group 0: that row group is
    // read, row group 1 answered.
    let finer: BTreeMap<u32, u64> = [(0, 1), (1, 1)].into_iter().collect();
    let read = unanswered(&finer, &per_rg, false, &spec).unwrap();
    assert_eq!(read, [0u32].into_iter().collect::<RoaringBitmap>());
    let c = cover(&finer, &per_rg, &read).unwrap();
    assert_eq!((c.rows, c.residual_rows, c.residual.len()), (2, 1, 1));
    assert_eq!(
        c.covered.unwrap(),
        cell(1, 0, Some(250_000), Some(250_000), Some(250_000))
    );
    // Counts the cells cannot hold are a corrupt index, never a total:
    // more rows counted than aggregated, a missing cell, or (same columns)
    // fewer.
    let more: BTreeMap<u32, u64> = [(0, 3)].into_iter().collect();
    assert!(unanswered(&more, &per_rg, false, &spec)
        .unwrap_err()
        .to_string()
        .contains("corrupt index"));
    let missing: BTreeMap<u32, u64> = [(7, 1)].into_iter().collect();
    assert!(unanswered(&missing, &per_rg, false, &spec).is_err());
    let fewer: BTreeMap<u32, u64> = [(0, 1)].into_iter().collect();
    assert!(unanswered(&fewer, &per_rg, true, &spec).is_err());
}

#[test]
fn file_decimals_are_rescaled_to_the_dataset_scale_or_refused() {
    // A file at scale 2 into a dataset at scale 6: 1.25 is 1_250_000.
    let file = DataType::Decimal128(10, 2);
    let mut w =
        SegmentWriter::for_dataset(spec(&ALL), &dec_schema(), WriterOptions::default()).unwrap();
    w.add_batch(
        RowGroupId(0),
        &dec_batch(&[(Some("a"), Some(125)), (Some("a"), Some(-1))], &file),
    )
    .unwrap();
    let r = open(&w.finish().unwrap().bytes);
    let c = &r.agg_cells(&[key("a")]).unwrap()[&key("a")][0].1;
    assert_eq!(c.sum, Some(AggSum::Int(1_240_000)));
    assert_eq!(c.min, Some(dec(-10_000)));
    // A file at a finer scale than the dataset's that does not divide
    // exactly is refused, all or nothing.
    let finer = DataType::Decimal128(18, 8);
    let narrow = schema(DataType::Decimal128(18, 6));
    let mut w = SegmentWriter::for_dataset(spec(&ALL), &narrow, WriterOptions::default()).unwrap();
    assert!(w
        .add_batch(RowGroupId(0), &dec_batch(&[(Some("a"), Some(1))], &finer))
        .is_err());
    assert_eq!(open(&w.finish().unwrap().bytes).entries(), 0);
    assert_eq!(render_scaled(-5, 3), "-0.005");
    assert_eq!(render_scaled(1_750_000, 6), "1.750000");
    assert_eq!(render_scaled(12, -2), "1200");
    assert_eq!(render_scaled(0, 2), "0.00");
}

#[test]
fn float_sums_are_compensated_and_both_zeros_order() {
    let mut w = SegmentWriter::for_dataset(
        spec(&ALL),
        &schema(DataType::Float64),
        WriterOptions::default(),
    )
    .unwrap();
    w.add_batch(
        RowGroupId(0),
        &float_batch(&[
            (Some("z"), Some(0.0)),
            (Some("z"), Some(-0.0)),
            (Some("z"), None),
            (Some("k"), Some(1e16)),
            (Some("k"), Some(1.0)),
            (Some("k"), Some(-1e16)),
        ]),
    )
    .unwrap();
    w.add_batch(RowGroupId(1), &float_batch(&[(Some("z"), None)]))
        .unwrap();
    let r = open(&w.finish().unwrap().bytes);
    let cells = r.agg_cells(&[key("z"), key("k")]).unwrap();
    let z = &cells[&key("z")];
    let zero = |x: f64| encode_tuple_as(&[Value::Float(x)], &[DataType::Float64]).unwrap();
    assert_eq!(z[0].1.min, Some(zero(-0.0)), "-0.0 orders before +0.0");
    assert_eq!(z[0].1.max, Some(zero(0.0)));
    assert!(!z[0].1.sum.unwrap().is_exact());
    // A row group of only nulls stores an integer zero; combined with a
    // float sum it adds nothing and the sum stays a float.
    assert_eq!(z[1].1.sum, Some(AggSum::Int(0)));
    let mut all = z[0].1.clone();
    all.combine(&z[1].1).unwrap();
    assert!(matches!(all.sum, Some(AggSum::Float { .. })));
    assert_eq!((all.rows, all.nulls), (4, 2));
    // Neumaier keeps the 1.0 that a naive sum loses.
    let k = &cells[&key("k")][0].1;
    assert_eq!(k.sum.unwrap().render(None), "1");
    assert_eq!(
        sum_kind("amount", &DataType::Float32).unwrap(),
        SumKind::Float
    );
}

#[test]
fn integer_sums_are_exact_i128_and_overflow_is_an_error() {
    let mut w = SegmentWriter::for_dataset(
        spec(&ALL),
        &schema(DataType::Int64),
        WriterOptions::default(),
    )
    .unwrap();
    w.add_batch(
        RowGroupId(0),
        &int_batch(&[(Some("a"), Some(i64::MAX)), (Some("a"), Some(i64::MAX))]),
    )
    .unwrap();
    let r = open(&w.finish().unwrap().bytes);
    let c = &r.agg_cells(&[key("a")]).unwrap()[&key("a")][0].1;
    assert_eq!(c.sum, Some(AggSum::Int(2 * i128::from(i64::MAX))));
    let mut big = AggCell {
        rows: 1,
        nulls: 0,
        sum: Some(AggSum::Int(i128::MAX)),
        min: None,
        max: None,
    };
    assert!(big.combine(&big.clone()).is_err());
}

#[test]
fn specs_values_and_layouts_that_cannot_aggregate_are_refused_with_the_reason() {
    let bad_value = KeySpec::Agg {
        columns: vec!["account".into()],
        value: "account".into(),
        functions: vec![AggregateFn::Count],
    };
    assert!(check_spec(&bad_value)
        .unwrap_err()
        .to_string()
        .contains("also a key column"));
    let unordered = KeySpec::Agg {
        columns: vec!["account".into()],
        value: "amount".into(),
        functions: vec![AggregateFn::Count, AggregateFn::Sum],
    };
    assert!(check_spec(&unordered)
        .unwrap_err()
        .to_string()
        .contains("order sum, count, min, max"));
    let no_count = KeySpec::Agg {
        columns: vec!["account".into()],
        value: "amount".into(),
        functions: vec![AggregateFn::Sum],
    };
    assert!(check_spec(&no_count).is_err());
    // A string can be counted, minimized, and maximized, never summed.
    let text = schema(DataType::Utf8);
    let e = value_type(&spec(&ALL), &text).unwrap_err();
    assert!(e.to_string().contains("{count,min,max}"), "{e}");
    assert!(value_type(&spec(&[AggregateFn::Min]), &text).is_ok());
    let wide = schema(DataType::Decimal256(40, 2));
    assert!(value_type(&spec(&ALL), &wide)
        .unwrap_err()
        .to_string()
        .contains("Decimal256"));
    let nested = schema(DataType::List(Arc::new(Field::new(
        "item",
        DataType::Int64,
        true,
    ))));
    assert!(value_type(&spec(&[AggregateFn::Count]), &nested).is_err());
    let missing = Schema::new(vec![Field::new("account", DataType::Utf8, true)]);
    assert!(value_type(&spec(&ALL), &missing).is_err());
    // An agg writer is fed rows, not bare keys, and agg lookups refuse
    // another layout.
    let mut w =
        SegmentWriter::for_dataset(spec(&ALL), &dec_schema(), WriterOptions::default()).unwrap();
    assert!(w.insert(&key("a"), RowGroupId(0)).is_err());
    let mut exact = SegmentWriter::new(
        KeySpec::Exact {
            columns: vec!["account".into()],
            counts: false,
            bounds: None,
        },
        WriterOptions::default(),
    )
    .unwrap();
    exact.insert(&key("a"), RowGroupId(0)).unwrap();
    let exact = open(&exact.finish().unwrap().bytes);
    assert!(exact.agg_cells(&[key("a")]).is_err());
}

/// Two segments over disjoint row groups merge into the cells one segment
/// over all of them holds; segments aggregating one key in the same row
/// group are refused, never added twice.
#[test]
fn merging_folds_disjoint_row_groups_only() {
    let dt = DataType::Decimal128(18, 6);
    let batches = [
        dec_batch(&[(Some("a"), Some(1)), (Some("b"), Some(2))], &dt),
        dec_batch(&[(Some("a"), Some(10)), (Some("a"), None)], &dt),
        dec_batch(&[(Some("c"), Some(-7))], &dt),
    ];
    let build = |range: std::ops::Range<usize>| {
        let mut w = SegmentWriter::for_dataset(spec(&ALL), &dec_schema(), opts(1)).unwrap();
        for i in range.clone() {
            w.add_batch(RowGroupId(i as u32), &batches[i]).unwrap();
        }
        w.set_rowgroup_range(RowGroupId(range.start as u32)..RowGroupId(range.end as u32))
            .unwrap();
        w.finish().unwrap().bytes
    };
    let whole = build(0..3);
    let parts = [open(&build(0..1)), open(&build(1..3))];
    let merged = merge(&parts, &WriterOptions::default()).unwrap();
    let probes = [key("a"), key("b"), key("c")];
    assert_eq!(
        open(&merged.bytes).agg_cells(&probes).unwrap(),
        open(&whole).agg_cells(&probes).unwrap()
    );
    // The merged value stream, as the reconcile reads it.
    let mut seen = Vec::new();
    visit_merged_values(&parts, |k, v| {
        seen.push((k.to_vec(), v.cells().unwrap().len()));
        Ok(())
    })
    .unwrap();
    assert_eq!(seen, vec![(key("a"), 2), (key("b"), 1), (key("c"), 1)]);
    let overlapping = [open(&build(0..2)), open(&build(1..3))];
    assert!(merge(&overlapping, &WriterOptions::default()).is_err());
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

/// Opens `bytes` and runs every agg read path: cells, `get`, the merged
/// value visit, and merge alone and with a clean segment. Returns whether
/// it panicked.
fn exercise(bytes: &[u8], clean: &[u8]) -> bool {
    QUIET.with(|q| q.set(true));
    let panicked = catch_unwind(AssertUnwindSafe(|| {
        if let Ok(r) = SegmentReader::open(bytes.to_vec()) {
            let probes = [key("a"), key("b"), key("c"), key("zz")];
            let _ = r.get(&probes);
            let _ = r.agg_cells(&probes);
            let readers = [r];
            let _ = visit_merged_values(&readers, |_, v| {
                if let Some(cells) = v.cells() {
                    let mut t = AggCell::empty();
                    for (_, c) in cells {
                        t.combine(c)?;
                    }
                }
                Ok(())
            });
            let _ = merge(&readers, &WriterOptions::default());
            let [r] = readers;
            let _ = merge(&[r, open(clean)], &WriterOptions::default());
        }
    }))
    .is_err();
    QUIET.with(|q| q.set(false));
    panicked
}

/// Every truncation, and every byte value at every position, of an agg
/// segment with several blocks, nulls, an all-null cell, and both sum
/// tags: opening and every read path return a value or an error, never a
/// panic.
#[test]
fn every_byte_of_an_agg_segment_corrupted_never_panics() {
    install_hook();
    let dt = DataType::Decimal128(18, 6);
    let mut w = SegmentWriter::for_dataset(spec(&ALL), &dec_schema(), opts(1)).unwrap();
    w.add_batch(
        RowGroupId(0),
        &dec_batch(
            &[
                (Some("a"), Some(5)),
                (Some("b"), None),
                (Some("c"), Some(-3)),
            ],
            &dt,
        ),
    )
    .unwrap();
    w.add_batch(
        RowGroupId(1),
        &dec_batch(&[(Some("a"), Some(999_999_999_999_999_999))], &dt),
    )
    .unwrap();
    w.set_rowgroup_range(RowGroupId(0)..RowGroupId(3)).unwrap();
    let clean = w.finish().unwrap().bytes;
    let mut wf = SegmentWriter::for_dataset(
        spec(&ALL),
        &schema(DataType::Float64),
        WriterOptions::default(),
    )
    .unwrap();
    wf.add_batch(RowGroupId(0), &float_batch(&[(Some("a"), Some(1.5))]))
        .unwrap();
    let float = wf.finish().unwrap().bytes;
    for seg in [&clean, &float] {
        for n in 0..seg.len() {
            assert!(!exercise(&seg[..n], seg), "truncated to {n}");
        }
        let full = std::env::var("BELLWETHER_FULL_SWEEP").is_ok();
        let values: Vec<u8> = if full {
            (0..=255).collect()
        } else {
            vec![0x00, 0xff, 0x01, 0x80, 0x7f]
        };
        for i in 0..seg.len() {
            let mut variants: BTreeSet<u8> = values.iter().copied().collect();
            variants.extend([seg[i] ^ 0x01, seg[i] ^ 0x10, seg[i].wrapping_add(1)]);
            for v in variants {
                if v == seg[i] {
                    continue;
                }
                let mut bad = seg.clone();
                bad[i] = v;
                assert!(!exercise(&bad, seg), "byte {i} set to {v:#04x}");
            }
        }
    }
}

// -- property test against a brute-force oracle ----------------------------

/// One row: account `k{0..5}` or a null key, and an amount in units of
/// 0.000001 or null.
type Row = (Option<u8>, Option<i64>);

fn rows() -> impl Strategy<Value = Vec<Row>> {
    prop::collection::vec(
        (
            prop::option::weighted(0.9, 0u8..5),
            prop::option::weighted(0.8, -1_000_000i64..1_000_000),
        ),
        0..8,
    )
}

fn name(k: u8) -> String {
    format!("k{k}")
}

fn batch_of(rows: &[Row]) -> RecordBatch {
    let names: Vec<Option<String>> = rows.iter().map(|(k, _)| k.map(name)).collect();
    let rows: Vec<(Option<&str>, Option<i128>)> = rows
        .iter()
        .zip(&names)
        .map(|((_, v), n)| (n.as_deref(), v.map(i128::from)))
        .collect();
    dec_batch(&rows, &DataType::Decimal128(18, 6))
}

/// The oracle's aggregate of key `k` over `rows`.
fn oracle(rows: &[Row], k: u8) -> Option<AggCell> {
    let vals: Vec<Option<i64>> = rows
        .iter()
        .filter(|(x, _)| *x == Some(k))
        .map(|(_, v)| *v)
        .collect();
    if vals.is_empty() {
        return None;
    }
    let present: Vec<i64> = vals.iter().flatten().copied().collect();
    Some(AggCell {
        rows: vals.len() as u64,
        nulls: (vals.len() - present.len()) as u64,
        sum: Some(AggSum::Int(present.iter().map(|v| i128::from(*v)).sum())),
        min: present.iter().min().map(|v| dec(i128::from(*v))),
        max: present.iter().max().map(|v| dec(i128::from(*v))),
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Every stored cell equals the oracle's aggregate of its key in its
    /// row group (nulls counted, never summed or compared); every key and
    /// row group the oracle has holds a cell and no other does; combining a
    /// key's cells over any split into segments gives the oracle's total;
    /// and with counts equal to the oracle's rows nothing is residual.
    #[test]
    fn cells_and_totals_equal_the_oracle(
        layout in prop::collection::vec(rows(), 1..8),
        split in 1usize..4,
        block_keys in 1usize..4,
    ) {
        let mut segs = Vec::new();
        for (chunk_no, chunk) in layout.chunks(split).enumerate() {
            let mut w = SegmentWriter::for_dataset(spec(&ALL), &dec_schema(), opts(block_keys)).unwrap();
            let start = (chunk_no * split) as u32;
            for (i, rows) in chunk.iter().enumerate() {
                w.add_batch(RowGroupId(start + i as u32), &batch_of(rows)).unwrap();
            }
            w.set_rowgroup_range(RowGroupId(start)..RowGroupId(start + chunk.len() as u32)).unwrap();
            segs.push(w.finish().unwrap().bytes);
        }
        let readers: Vec<_> = segs.iter().map(|b| open(b)).collect();
        let probes: Vec<Vec<u8>> = (0..6u8).map(|k| key(&name(k))).collect();
        let mut stored: BTreeMap<Vec<u8>, BTreeMap<u32, AggCell>> = BTreeMap::new();
        for r in &readers {
            for (k, cells) in r.agg_cells(&probes).unwrap() {
                for (rg, c) in cells {
                    prop_assert!(stored.entry(k.clone()).or_default().insert(rg.0, c).is_none());
                }
            }
        }
        for k in 0..6u8 {
            let mut want: BTreeMap<u32, AggCell> = BTreeMap::new();
            for (rg, rows) in layout.iter().enumerate() {
                if let Some(c) = oracle(rows, k) {
                    want.insert(rg as u32, c);
                }
            }
            let got = stored.remove(&key(&name(k))).unwrap_or_default();
            prop_assert_eq!(&got, &want);
            let all: Vec<Row> = layout.iter().flatten().copied().collect();
            let counts: BTreeMap<u32, u64> = want.iter().map(|(rg, c)| (*rg, c.rows)).collect();
            let none = unanswered(&counts, &got, true, &spec(&ALL)).unwrap();
            prop_assert!(none.is_empty());
            let c = cover(&counts, &got, &none).unwrap();
            prop_assert_eq!(c.covered, oracle(&all, k));
        }
    }
}
