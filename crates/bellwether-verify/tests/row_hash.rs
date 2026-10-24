//! The row hash is exactly `docs/format.md`'s definition, observed through
//! the reconciler itself: a key missing on the right exposes the left row's
//! hash, which must equal xxh3-128 over bytes this test builds by hand.

#![allow(clippy::unwrap_used, missing_docs)]

mod common;

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Decimal128Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use bellwether_verify::{row_hash, Mismatch, ReconcileSpec};
use common::{dirs, reconcile, write_batch};
use xxhash_rust::xxh3::xxh3_128;

/// The golden row's hash, pinned. If this changes, every certificate ever
/// computed stops being comparable with new ones: a format change.
const GOLDEN: u128 = 0x0016_a08f_6b47_314e_7456_6e88_24a9_3834;

fn anchor_row() -> RecordBatch {
    // Dataset types come from this first file: Decimal128(18, 2), µs.
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        (
            "amount",
            Arc::new(
                Decimal128Array::from(vec![100])
                    .with_precision_and_scale(18, 2)
                    .unwrap(),
            ),
        ),
        ("name", Arc::new(StringArray::from(vec!["x"]))),
        (
            "ts",
            Arc::new(TimestampMicrosecondArray::from(vec![5]).with_timezone("UTC")),
        ),
        ("flag", Arc::new(BooleanArray::from(vec![Some(true)]))),
    ])
    .unwrap()
}

fn golden_row() -> RecordBatch {
    // A second file at another decimal scale and timestamp unit: the hash
    // must use the dataset's (scale 2, µs), not the file's (scale 3, ms).
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![7])) as ArrayRef),
        (
            "amount",
            Arc::new(
                Decimal128Array::from(vec![-1500])
                    .with_precision_and_scale(18, 3)
                    .unwrap(),
            ),
        ),
        ("name", Arc::new(StringArray::from(vec!["a\0b"]))),
        (
            "ts",
            Arc::new(TimestampMillisecondArray::from(vec![1]).with_timezone("UTC")),
        ),
        ("flag", Arc::new(BooleanArray::from(vec![None::<bool>]))),
    ])
    .unwrap()
}

/// `docs/format.md`'s row hash input for the golden row, built by hand:
/// per column in schema order, a 4-byte big-endian name length, the name,
/// then the canonical value (or the 0x00 null marker).
fn golden_input() -> Vec<u8> {
    let mut b = Vec::new();
    let mut col = |name: &str, value: &[u8]| {
        b.extend_from_slice(&u32::try_from(name.len()).unwrap().to_be_bytes());
        b.extend_from_slice(name.as_bytes());
        b.extend_from_slice(value);
    };
    // Int64 7: tag 0x02, big-endian with the sign bit flipped.
    col("id", &[0x02, 0x80, 0, 0, 0, 0, 0, 0, 0x07]);
    // -1.500 at scale 3 is -1.50 at the dataset scale 2: -150 as i128,
    // big-endian, sign bit flipped, behind the decimal and width tags.
    let mut amount = vec![0x05, 0x10];
    let mut raw = (-150i128).to_be_bytes();
    raw[0] ^= 0x80;
    amount.extend_from_slice(&raw);
    col("amount", &amount);
    // "a\0b": the zero byte escaped as 00 FF, terminated by 00 00.
    col("name", &[0x06, b'a', 0x00, 0xFF, b'b', 0x00, 0x00]);
    // 1 ms is 1000 µs in the dataset unit: temporal tag, timestamp kind,
    // microsecond unit, then the Int64 encoding.
    col("ts", &[0x08, 0x04, 0x03, 0x80, 0, 0, 0, 0, 0, 0x03, 0xE8]);
    // A null is the single marker.
    col("flag", &[0x00]);
    b
}

#[test]
fn the_reconciler_hashes_rows_exactly_as_format_md_states() {
    let (l, r) = dirs();
    write_batch(l.path(), "a.parquet", &anchor_row(), 4);
    write_batch(l.path(), "b.parquet", &golden_row(), 4);
    write_batch(r.path(), "a.parquet", &anchor_row(), 4);
    let out = reconcile(l.path(), r.path(), ReconcileSpec::new(["id"])).unwrap();
    assert_eq!(out.hashed_columns, ["id", "amount", "name", "ts", "flag"]);
    let [Mismatch::MissingOnRight { key, left }] = out.mismatches.as_slice() else {
        panic!("{:?}", out.mismatches);
    };
    assert_eq!(key.values, [Some("7".to_string())]);
    let expected = xxh3_128(&golden_input());
    assert_eq!(left.hash, format!("{expected:032x}"));
    assert_eq!(
        expected, GOLDEN,
        "the pinned golden value: {expected:#034x}"
    );
}

#[test]
fn the_public_row_hash_matches_the_reconciler() {
    let batch = golden_row();
    let columns = [
        ("id".to_string(), DataType::Int64),
        ("amount".to_string(), DataType::Decimal128(18, 2)),
        ("name".to_string(), DataType::Utf8),
        (
            "ts".to_string(),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ),
        ("flag".to_string(), DataType::Boolean),
    ];
    assert_eq!(
        row_hash(&batch, 0, &columns).unwrap(),
        xxh3_128(&golden_input())
    );
    // The name prefix and the order are part of the hash.
    let mut reordered = columns.clone();
    reordered.swap(0, 2);
    assert_ne!(
        row_hash(&batch, 0, &reordered).unwrap(),
        xxh3_128(&golden_input())
    );
}
