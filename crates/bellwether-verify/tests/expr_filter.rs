//! Reconcile scoped by an expression (`expr` keys): the `ExprIn` row filter
//! keeps exactly the rows whose recomputed expression is one of the values,
//! with and without workload coverage keyed on the expression (`keys_of`),
//! and a value no row holds is counted absent on both sides.

#![allow(clippy::unwrap_used, missing_docs)]

mod common;

use std::time::Duration;

use arrow::datatypes::DataType;
use bellwether_core::encoding::{encode_value_as, Value};
use bellwether_types::{Hash, Predicate};
use bellwether_verify::{CoverageRequest, ReconcileSpec, WorkloadKeys};
use common::{dirs, r, reconcile, write, R};

const EXPR: &str = "lower(status)";

fn enc(s: &str) -> Vec<u8> {
    let mut out = Vec::new();
    encode_value_as(&Value::Utf8(s), &DataType::Utf8, &mut out).unwrap();
    out
}

/// 30 ids; `lower(status)` is `a`, `b`, or `c` by `id % 3`, spelled in mixed
/// case. The right side differs only on id 2, whose value is `c`.
fn sides() -> (tempfile::TempDir, tempfile::TempDir) {
    let (l, rt) = dirs();
    let rows: Vec<R> = (0..30)
        .map(|id| {
            r(
                id,
                1_000,
                i128::from(id * 100),
                ["A", "b", "C"][usize::try_from(id % 3).unwrap()],
            )
        })
        .collect();
    let mut right = rows.clone();
    right[2].amount = Some(1);
    write(l.path(), "x.parquet", &rows, 7);
    write(rt.path(), "x.parquet", &right, 7);
    (l, rt)
}

fn filter(values: &[&str]) -> Vec<Predicate> {
    vec![Predicate::ExprIn {
        expr: EXPR.into(),
        values: values.iter().map(|v| enc(v)).collect(),
    }]
}

fn workload(values: &[&str]) -> CoverageRequest {
    CoverageRequest::Workload(WorkloadKeys {
        workload: Hash::of(b"expr workload"),
        age: Duration::from_secs(60),
        keys: values.iter().map(|v| enc(v)).collect(),
    })
}

#[test]
fn the_expression_filter_keeps_exactly_the_rows_of_its_values() {
    let (l, rt) = sides();
    // Outside the values the sides agree; inside them they do not.
    let out = reconcile(
        l.path(),
        rt.path(),
        ReconcileSpec::new(["id"]).rows_where(filter(&["a", "b"])),
    )
    .unwrap();
    assert!(out.pass, "{:?}", out.mismatches);
    assert_eq!(out.counts.matched, 20);
    let out = reconcile(
        l.path(),
        rt.path(),
        ReconcileSpec::new(["id"]).rows_where(filter(&["c"])),
    )
    .unwrap();
    assert!(!out.pass);
    assert_eq!((out.counts.matched, out.counts.mismatched), (9, 1));
}

#[test]
fn workload_coverage_keyed_on_the_expression_scopes_and_counts_absent_values() {
    let (l, rt) = sides();
    let spec = |values: &[&str]| {
        ReconcileSpec::new(["id"])
            .rows_where(filter(values))
            .coverage(workload(values))
            .keys_of(EXPR)
    };
    // `zz` is held by no row on either side.
    let out = reconcile(l.path(), rt.path(), spec(&["a", "b", "zz"])).unwrap();
    assert!(out.pass, "{:?}", out.mismatches);
    assert_eq!(out.counts.keys_compared, 3);
    assert_eq!(out.counts.absent_both, 1, "only zz is absent");
    let out = reconcile(l.path(), rt.path(), spec(&["c"])).unwrap();
    assert!(!out.pass);
    assert_eq!(out.counts.absent_both, 0);
}
