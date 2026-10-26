//! `R-sketch-1` (the low-cardinality key, from sketches): fires at the
//! catalog's thresholds and on nothing short of them.

#![allow(clippy::unwrap_used, missing_docs)]

use arrow::datatypes::{DataType, TimeUnit};
use bellwether_profile::lints::{
    low_cardinality_key, SketchedColumn, HIGH, LOW_CARDINALITY_KEY_FROM_SKETCHES,
    LOW_CARDINALITY_MIN_ROWS_PER_VALUE, MEDIUM,
};

fn col(
    name: &str,
    dt: DataType,
    rows: u64,
    distinct: u64,
    files: u64,
    with: &[(&str, u64)],
) -> SketchedColumn {
    SketchedColumn {
        column: name.into(),
        data_type: dt,
        rows,
        distinct,
        distinct_exact: false,
        files_present: files,
        copresent: with.iter().map(|(c, n)| ((*c).to_string(), *n)).collect(),
    }
}

/// A write stamp: 72 values over 10,000,000 rows, beside an id with
/// 1,000,000 values in the same 10,000 files.
fn stamp_and_id() -> Vec<SketchedColumn> {
    vec![
        col(
            "batch",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            10_000_000,
            72,
            10_000,
            &[("id", 10_000)],
        ),
        col(
            "id",
            DataType::Int64,
            10_000_000,
            1_000_000,
            10_000,
            &[("batch", 10_000)],
        ),
    ]
}

#[test]
fn fires_on_a_stamp_beside_an_id() {
    let lints = low_cardinality_key(&stamp_and_id());
    assert_eq!(lints.len(), 1);
    let l = &lints[0];
    assert_eq!(l.rule, LOW_CARDINALITY_KEY_FROM_SKETCHES);
    assert_eq!(l.columns, vec!["batch".to_string(), "id".to_string()]);
    assert!((l.confidence - MEDIUM).abs() < 1e-12);
    assert!(
        l.message
            .contains("about 138889 rows share each of its 72 values"),
        "{}",
        l.message
    );
    assert!(l.message.contains("approximate"), "{}", l.message);
    assert!(l.message.contains("(id, batch)"), "{}", l.message);
    // Exact counts on both sides raise the confidence.
    let mut exact = stamp_and_id();
    for c in &mut exact {
        c.distinct_exact = true;
    }
    assert!((low_cardinality_key(&exact)[0].confidence - HIGH).abs() < 1e-12);
}

#[test]
fn quiet_below_each_threshold() {
    // Values cover fewer rows than the threshold.
    let mut few = stamp_and_id();
    few[0].distinct = (10_000_000.0 / LOW_CARDINALITY_MIN_ROWS_PER_VALUE) as u64 + 1;
    assert!(low_cardinality_key(&few).is_empty());
    // The partner has fewer than ten times the values.
    let mut close = stamp_and_id();
    close[1].distinct = 719;
    assert!(low_cardinality_key(&close).is_empty());
    // The partner is not id-like (a float measurement).
    let mut float = stamp_and_id();
    float[1].data_type = DataType::Float64;
    assert!(low_cardinality_key(&float).is_empty());
    // The two rarely share a file.
    let mut apart = stamp_and_id();
    apart[0].copresent = vec![("id".into(), 8_999)];
    assert!(low_cardinality_key(&apart).is_empty());
    // Alone, or empty.
    assert!(low_cardinality_key(&stamp_and_id()[..1]).is_empty());
    let mut empty = stamp_and_id();
    empty[0].distinct = 0;
    assert!(low_cardinality_key(&empty).is_empty());
}

#[test]
fn names_the_partner_with_the_most_values() {
    let mut cols = stamp_and_id();
    cols.push(col(
        "account",
        DataType::Utf8,
        10_000_000,
        5_000_000,
        10_000,
        &[],
    ));
    cols[0].copresent.push(("account".into(), 10_000));
    let l = low_cardinality_key(&cols);
    assert_eq!(
        l[0].columns,
        vec!["batch".to_string(), "account".to_string()]
    );
}
