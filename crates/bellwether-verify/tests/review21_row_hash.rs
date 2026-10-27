//! Review probe for PR #21: row-hash vectors the golden does not cover,
//! with the expected bytes built by hand from `docs/format.md` (no
//! `bellwether_core` encoder, no `bellwether_verify` code in the
//! expectation). xxhash-rust is the primitive.
//!
//! The row is read from a second file whose types differ from the dataset's
//! (Decimal256 at scale 3 under a dataset scale 2, a millisecond timestamp
//! under a dataset in seconds), so the hash must use the dataset types. One
//! column name is multi-byte UTF-8, so the length prefix must count bytes.

#![allow(
    clippy::unwrap_used,
    clippy::pedantic,
    clippy::cloned_ref_to_slice_refs,
    missing_docs,
    dead_code
)]

mod common;

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Decimal256Array, DictionaryArray, Float64Array,
    Int64Array, LargeStringArray, RecordBatch, StringArray, TimestampMillisecondArray,
    TimestampSecondArray, UInt64Array,
};
use arrow::datatypes::{i256, DataType, Int32Type, TimeUnit};
use bellwether_source::ParquetSource;
use bellwether_verify::{row_hash, Mismatch, Options, ReconcileSpec, Reconciler, Side};
use common::{dirs, reconcile, root, write_batch};
use xxhash_rust::xxh3::xxh3_128;

fn dict(values: &[&str]) -> ArrayRef {
    Arc::new(
        values
            .iter()
            .copied()
            .collect::<DictionaryArray<Int32Type>>(),
    )
}

/// The first file: it fixes the dataset types.
fn anchor() -> RecordBatch {
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        ("çé", Arc::new(StringArray::from(vec!["z"])) as ArrayRef),
        (
            "d256",
            Arc::new(
                Decimal256Array::from(vec![i256::from_i128(1)])
                    .with_precision_and_scale(40, 2)
                    .unwrap(),
            ) as ArrayRef,
        ),
        ("day", Arc::new(Date32Array::from(vec![1])) as ArrayRef),
        ("f", Arc::new(Float64Array::from(vec![0.5])) as ArrayRef),
        ("u", Arc::new(UInt64Array::from(vec![1u64])) as ArrayRef),
        ("b", Arc::new(BooleanArray::from(vec![false])) as ArrayRef),
        ("dict", dict(&["q"])),
        (
            "t",
            Arc::new(TimestampSecondArray::from(vec![1])) as ArrayRef,
        ),
    ])
    .unwrap()
}

/// The probed row, at file types other than the dataset's.
fn probe(f: f64) -> RecordBatch {
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![7])) as ArrayRef),
        ("çé", Arc::new(StringArray::from(vec!["x\0y"])) as ArrayRef),
        (
            "d256",
            Arc::new(
                Decimal256Array::from(vec![i256::from_i128(-123_450)])
                    .with_precision_and_scale(40, 3)
                    .unwrap(),
            ) as ArrayRef,
        ),
        ("day", Arc::new(Date32Array::from(vec![-3])) as ArrayRef),
        ("f", Arc::new(Float64Array::from(vec![f])) as ArrayRef),
        ("u", Arc::new(UInt64Array::from(vec![7u64])) as ArrayRef),
        (
            "b",
            Arc::new(BooleanArray::from(vec![Some(true)])) as ArrayRef,
        ),
        ("dict", dict(&["hello"])),
        (
            "t",
            Arc::new(TimestampMillisecondArray::from(vec![5_000])) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn i64_payload(v: i64) -> [u8; 8] {
    let mut b = v.to_be_bytes();
    b[0] ^= 0x80;
    b
}

fn f64_payload(f: f64) -> [u8; 8] {
    let bits = if f.is_nan() {
        0x7FF8_0000_0000_0000
    } else {
        f.to_bits()
    };
    let flipped = if bits >> 63 == 1 {
        !bits
    } else {
        bits | (1 << 63)
    };
    flipped.to_be_bytes()
}

/// `docs/format.md`'s row-hash input for the probe row, by hand.
fn expected_input(f: f64) -> Vec<u8> {
    let mut out = Vec::new();
    let mut col = |name: &str, value: &[u8]| {
        // A 4-byte big-endian u32 of the name's UTF-8 byte count.
        out.extend_from_slice(&u32::try_from(name.len()).unwrap().to_be_bytes());
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(value);
    };
    let mut id = vec![0x02];
    id.extend_from_slice(&i64_payload(7));
    col("id", &id);
    // "çé" is 4 bytes, 2 chars. "x\0y" escapes the zero byte.
    col("çé", &[0x06, b'x', 0x00, 0xFF, b'y', 0x00, 0x00]);
    // -123.450 at scale 3 is -12345 at the dataset scale 2; 32 bytes BE,
    // sign bit flipped, behind 0x05 0x20.
    let mut d = vec![0x05, 0x20];
    let mut raw = i256::from_i128(-12_345).to_be_bytes();
    raw[0] ^= 0x80;
    d.extend_from_slice(&raw);
    col("d256", &d);
    // Date32: 0x08, kind 0x01, unit 0x05 (day), then days.
    let mut day = vec![0x08, 0x01, 0x05];
    day.extend_from_slice(&i64_payload(-3));
    col("day", &day);
    let mut fl = vec![0x04];
    fl.extend_from_slice(&f64_payload(f));
    col("f", &fl);
    let mut u = vec![0x03];
    u.extend_from_slice(&7u64.to_be_bytes());
    col("u", &u);
    col("b", &[0x01, 0x01]);
    // A dictionary value encodes as its value type.
    col("dict", &[0x06, b'h', b'e', b'l', b'l', b'o', 0x00, 0x00]);
    // 5000 ms is 5 s in the dataset unit: 0x08, kind 0x04, unit 0x01.
    let mut t = vec![0x08, 0x04, 0x01];
    t.extend_from_slice(&i64_payload(5));
    col("t", &t);
    out
}

fn reconciler_hash(f: f64) -> String {
    let (l, r) = dirs();
    write_batch(l.path(), "a.parquet", &anchor(), 4);
    write_batch(l.path(), "b.parquet", &probe(f), 4);
    write_batch(r.path(), "a.parquet", &anchor(), 4);
    // Inferred, a folder takes its widest decimal (M5): the dataset types
    // under test are the anchor's, so they are declared.
    let schema = anchor().schema();
    let ls = ParquetSource::new().with_schema(Arc::clone(&schema));
    let rs = ParquetSource::new().with_schema(schema);
    let (lr, rr) = (root(l.path()), root(r.path()));
    let out = Reconciler::new(ReconcileSpec::new(["id"]), Options::default())
        .unwrap()
        .run(&Side::new(&ls, &lr), &Side::new(&rs, &rr))
        .unwrap();
    assert_eq!(
        out.hashed_columns,
        ["id", "çé", "d256", "day", "f", "u", "b", "dict", "t"]
    );
    let [Mismatch::MissingOnRight { left, key }] = out.mismatches.as_slice() else {
        panic!("{:?}", out.mismatches);
    };
    // The key's canonical identity, by hand.
    let mut k = vec![0x02];
    k.extend_from_slice(&i64_payload(7));
    let khex: String = k.iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(key.canonical, khex);
    left.hash.clone()
}

#[test]
fn guard_row_hash_vectors_beyond_the_golden() {
    for f in [1.5, -0.0, f64::NAN, -2.25] {
        let expected = xxh3_128(&expected_input(f));
        assert_eq!(reconciler_hash(f), format!("{expected:032x}"), "f = {f}");
    }
}

#[test]
fn guard_the_public_row_hash_agrees_and_order_and_prefix_matter() {
    let batch = probe(1.5);
    let cols: Vec<(String, DataType)> = vec![
        ("id".into(), DataType::Int64),
        ("çé".into(), DataType::Utf8),
        ("d256".into(), DataType::Decimal256(40, 2)),
        ("day".into(), DataType::Date32),
        ("f".into(), DataType::Float64),
        ("u".into(), DataType::UInt64),
        ("b".into(), DataType::Boolean),
        (
            "dict".into(),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        ),
        ("t".into(), DataType::Timestamp(TimeUnit::Second, None)),
    ];
    let h = row_hash(&batch, 0, &cols).unwrap();
    assert_eq!(h, xxh3_128(&expected_input(1.5)));
    // A hash without the name prefix, or with a little-endian or char-count
    // prefix, or another seed, is a different hash: the definition is
    // load-bearing in every part.
    let input = expected_input(1.5);
    assert_ne!(h, xxhash_rust::xxh3::xxh3_128_with_seed(&input, 1));
    let mut le = Vec::new();
    for (name, _) in &cols {
        let _ = name;
    }
    le.extend_from_slice(&input);
    le[0..4].copy_from_slice(&2u32.to_le_bytes());
    assert_ne!(h, xxh3_128(&le));
    let mut swapped = cols.clone();
    swapped.swap(1, 2);
    assert_ne!(row_hash(&batch, 0, &swapped).unwrap(), h);
}

#[test]
fn guard_type_variants_with_one_encoding_compare_equal() {
    // Left: Utf8 and dictionary; right: LargeUtf8 and plain Utf8.
    let (l, r) = dirs();
    let left = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
        (
            "s",
            Arc::new(StringArray::from(vec!["a", "b\0"])) as ArrayRef,
        ),
        ("d", dict(&["x", "y"])),
    ])
    .unwrap();
    let right = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
        (
            "s",
            Arc::new(LargeStringArray::from(vec!["a", "b\0"])) as ArrayRef,
        ),
        ("d", Arc::new(StringArray::from(vec!["x", "y"])) as ArrayRef),
    ])
    .unwrap();
    write_batch(l.path(), "x.parquet", &left, 1);
    write_batch(r.path(), "x.parquet", &right, 1);
    let out = reconcile(l.path(), r.path(), ReconcileSpec::new(["id"])).unwrap();
    assert!(out.pass, "{out:#?}");
    // And a one-byte difference in the dictionary value is found.
    let right2 = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
        (
            "s",
            Arc::new(LargeStringArray::from(vec!["a", "b\0"])) as ArrayRef,
        ),
        ("d", Arc::new(StringArray::from(vec!["x", "z"])) as ArrayRef),
    ])
    .unwrap();
    let (l2, r2) = dirs();
    write_batch(l2.path(), "x.parquet", &left, 1);
    write_batch(r2.path(), "x.parquet", &right2, 1);
    let out = reconcile(l2.path(), r2.path(), ReconcileSpec::new(["id"])).unwrap();
    assert_eq!(out.counts.mismatched, 1);
}

#[test]
fn guard_a_composite_key_is_the_concatenation_of_its_elements() {
    let (l, r) = dirs();
    let left = RecordBatch::try_from_iter([
        (
            "a",
            Arc::new(StringArray::from(vec![Some("k\0"), None])) as ArrayRef,
        ),
        (
            "b",
            Arc::new(Int64Array::from(vec![Some(-1), Some(4)])) as ArrayRef,
        ),
    ])
    .unwrap();
    let right = RecordBatch::try_from_iter([
        (
            "a",
            Arc::new(StringArray::from(vec![Some("other")])) as ArrayRef,
        ),
        ("b", Arc::new(Int64Array::from(vec![Some(0)])) as ArrayRef),
    ])
    .unwrap();
    write_batch(l.path(), "x.parquet", &left, 4);
    write_batch(r.path(), "x.parquet", &right, 4);
    let out = reconcile(l.path(), r.path(), ReconcileSpec::new(["a", "b"])).unwrap();
    let mut got: Vec<String> = out
        .mismatches
        .iter()
        .filter_map(|m| match m {
            Mismatch::MissingOnRight { key, .. } => Some(key.canonical.clone()),
            _ => None,
        })
        .collect();
    got.sort();
    let mut k1 = vec![0x06, b'k', 0x00, 0xFF, 0x00, 0x00, 0x02];
    k1.extend_from_slice(&i64_payload(-1));
    let mut k2 = vec![0x00, 0x02];
    k2.extend_from_slice(&i64_payload(4));
    let mut want: Vec<String> = [k1, k2]
        .iter()
        .map(|k| k.iter().map(|b| format!("{b:02x}")).collect())
        .collect();
    want.sort();
    assert_eq!(got, want);
}
