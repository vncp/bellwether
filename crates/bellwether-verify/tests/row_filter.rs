//! The row filter (`ReconcileSpec::row_filter`): only the rows a lookup's
//! plan residual keeps are compared, on both sides; a row is kept when any
//! residual keeps it.

#![allow(clippy::unwrap_used, missing_docs)]

mod common;

use arrow::datatypes::{DataType, TimeUnit};
use bellwether_core::encoding::{encode_value_as, Value};
use bellwether_types::Predicate;
use bellwether_verify::{ReconcileSpec, VerifyError};
use common::{dirs, r, reconcile, ts_type, write, R};

fn ts(v: i64) -> Vec<u8> {
    let mut out = Vec::new();
    encode_value_as(
        &Value::Timestamp {
            value: v,
            unit: TimeUnit::Microsecond,
        },
        &ts_type(),
        &mut out,
    )
    .unwrap();
    out
}

fn id(v: i64) -> Vec<u8> {
    let mut out = Vec::new();
    encode_value_as(&Value::Int(v), &DataType::Int64, &mut out).unwrap();
    out
}

fn between(lo: i64, hi: i64) -> Predicate {
    Predicate::Between {
        column: "ts".into(),
        lo: ts(lo),
        hi: ts(hi),
    }
}

/// Keys 1..=3, versions at ts 1000, 2000, 3000.
fn rows() -> Vec<R> {
    let mut out = Vec::new();
    for k in 1..=3 {
        for v in 1..=3 {
            out.push(r(k, 1_000 * v, i128::from(k * 10 + v), "a"));
        }
    }
    out
}

/// Left is `rows()`; right is it with the row of key `k` at `t` edited.
fn pass_with(edit: (i64, i64), spec: ReconcileSpec) -> bool {
    let (l, rt) = dirs();
    let left = rows();
    let mut right = left.clone();
    for x in &mut right {
        if x.id == Some(edit.0) && x.ts == Some(edit.1) {
            x.note = Some("edited".into());
        }
    }
    write(l.path(), "x.parquet", &left, 2);
    write(rt.path(), "x.parquet", &right, 4);
    let out = reconcile(l.path(), rt.path(), spec).unwrap();
    out.pass
}

#[test]
fn a_window_compares_only_rows_inside_it() {
    let spec = || ReconcileSpec::new(["id"]).rows_where(vec![between(1_500, 2_500)]);
    // Outside the window: not compared on either side.
    assert!(pass_with((2, 1_000), spec()));
    assert!(pass_with((2, 3_000), spec()));
    // Inside it: a mismatch.
    assert!(!pass_with((2, 2_000), spec()));
    // Without the filter every row counts.
    assert!(!pass_with((2, 1_000), ReconcileSpec::new(["id"])));
    // The window's ends are inclusive.
    let edge = || ReconcileSpec::new(["id"]).rows_where(vec![between(2_000, 2_000)]);
    assert!(!pass_with((1, 2_000), edge()));
    let (l, rt) = dirs();
    write(l.path(), "x.parquet", &rows(), 3);
    write(rt.path(), "x.parquet", &rows(), 9);
    let out = reconcile(l.path(), rt.path(), spec()).unwrap();
    assert!(out.pass);
    assert_eq!(out.counts.keys_compared, 3);
    assert_eq!(out.row_filter, Some(vec![between(1_500, 2_500)]));
}

#[test]
fn a_per_key_bound_keeps_newer_rows_of_listed_keys_and_in_drops_other_keys() {
    // Key 1 from 2000 on, key 2 every row (no bound), key 3 not asked for.
    let lookback = || {
        ReconcileSpec::new(["id"]).rows_where(vec![Predicate::And {
            all: vec![
                Predicate::In {
                    columns: vec!["id".into()],
                    values: vec![id(1), id(2)],
                },
                Predicate::NotOlderThan {
                    columns: vec!["id".into()],
                    ts_column: "ts".into(),
                    bounds: vec![(id(1), ts(2_000))],
                },
                // The version rule, not a row test: keeps every row.
                Predicate::LatestPerKey {
                    columns: vec!["id".into()],
                    ts_column: "ts".into(),
                },
            ],
        }])
    };
    assert!(pass_with((1, 1_000), lookback()));
    assert!(!pass_with((1, 2_000), lookback()));
    assert!(!pass_with((2, 1_000), lookback()));
    assert!(pass_with((3, 3_000), lookback()));
}

#[test]
fn a_row_any_residual_keeps_is_compared() {
    let either =
        || ReconcileSpec::new(["id"]).rows_where(vec![between(0, 1_000), between(3_000, 3_000)]);
    assert!(!pass_with((1, 1_000), either()));
    assert!(!pass_with((1, 3_000), either()));
    assert!(pass_with((1, 2_000), either()));
}

#[test]
fn a_null_time_is_in_no_window_and_an_empty_filter_is_refused() {
    let (l, rt) = dirs();
    let mut left = rows();
    left.push(R {
        ts: None,
        ..r(1, 0, 1, "a")
    });
    write(l.path(), "x.parquet", &left, 4);
    write(rt.path(), "x.parquet", &rows(), 4);
    let spec = ReconcileSpec::new(["id"]).rows_where(vec![between(i64::MIN + 1, i64::MAX)]);
    assert!(reconcile(l.path(), rt.path(), spec).unwrap().pass);
    let e = reconcile(
        l.path(),
        rt.path(),
        ReconcileSpec::new(["id"]).rows_where(Vec::new()),
    )
    .unwrap_err();
    assert!(matches!(e, VerifyError::InvalidSpec(_)), "{e}");
}
