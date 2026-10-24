//! The shared dataset schema: column order, one-sided columns, types.

#![allow(clippy::unwrap_used, missing_docs)]

mod common;

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Int32Array, Int64Array, ListArray, RecordBatch, StringArray, UInt64Array,
};
use arrow::datatypes::Int64Type;
use bellwether_verify::{ReconcileSpec, VerifyError};
use common::{dirs, reconcile, write_batch};

fn batch(cols: Vec<(&str, ArrayRef)>) -> RecordBatch {
    RecordBatch::try_from_iter(cols).unwrap()
}

fn ids() -> ArrayRef {
    Arc::new(Int64Array::from(vec![1, 2, 3]))
}

fn names() -> ArrayRef {
    Arc::new(StringArray::from(vec!["a", "b", "c"]))
}

#[test]
fn column_order_may_differ_between_sides() {
    // The row hash uses one shared order (the left schema's), so the same
    // rows with physically reordered columns are equal.
    let (l, r) = dirs();
    write_batch(
        l.path(),
        "x.parquet",
        &batch(vec![
            ("id", ids()),
            ("name", names()),
            ("n", Arc::new(Int64Array::from(vec![7, 8, 9]))),
        ]),
        2,
    );
    write_batch(
        r.path(),
        "x.parquet",
        &batch(vec![
            ("n", Arc::new(Int64Array::from(vec![7, 8, 9]))),
            ("name", names()),
            ("id", ids()),
        ]),
        2,
    );
    let out = reconcile(l.path(), r.path(), ReconcileSpec::new(["id"])).unwrap();
    assert!(out.pass, "{out:#?}");
    assert_eq!(out.hashed_columns, ["id", "name", "n"]);
    // Swapping two values between same-typed columns is a difference.
    let (l2, r2) = dirs();
    write_batch(
        l2.path(),
        "x.parquet",
        &batch(vec![
            ("id", ids()),
            ("a", Arc::new(Int64Array::from(vec![1, 1, 1]))),
            ("b", Arc::new(Int64Array::from(vec![2, 2, 2]))),
        ]),
        2,
    );
    write_batch(
        r2.path(),
        "x.parquet",
        &batch(vec![
            ("id", ids()),
            ("b", Arc::new(Int64Array::from(vec![1, 1, 1]))),
            ("a", Arc::new(Int64Array::from(vec![2, 2, 2]))),
        ]),
        2,
    );
    let out = reconcile(l2.path(), r2.path(), ReconcileSpec::new(["id"])).unwrap();
    assert_eq!(out.counts.mismatched, 3);
}

#[test]
fn a_column_on_one_side_only_is_an_error_unless_ignored() {
    let (l, r) = dirs();
    write_batch(
        l.path(),
        "x.parquet",
        &batch(vec![("id", ids()), ("name", names())]),
        2,
    );
    write_batch(r.path(), "x.parquet", &batch(vec![("id", ids())]), 2);
    let err = reconcile(l.path(), r.path(), ReconcileSpec::new(["id"])).unwrap_err();
    assert!(
        matches!(err, VerifyError::Schema(ref m) if m.contains("left only")),
        "{err}"
    );
    let err = reconcile(r.path(), l.path(), ReconcileSpec::new(["id"])).unwrap_err();
    assert!(
        matches!(err, VerifyError::Schema(ref m) if m.contains("right only")),
        "{err}"
    );
    let out = reconcile(
        l.path(),
        r.path(),
        ReconcileSpec::new(["id"]).ignore(["name"]),
    )
    .unwrap();
    assert!(out.pass);
    assert_eq!(out.ignored_columns, ["name"]);
}

#[test]
fn types_without_a_shared_encoding_are_errors() {
    // Int64 against UInt64: different tags, every row would differ.
    let (l, r) = dirs();
    write_batch(l.path(), "x.parquet", &batch(vec![("id", ids())]), 2);
    write_batch(
        r.path(),
        "x.parquet",
        &batch(vec![(
            "id",
            Arc::new(UInt64Array::from(vec![1u64, 2, 3])) as ArrayRef,
        )]),
        2,
    );
    let err = reconcile(l.path(), r.path(), ReconcileSpec::new(["id"])).unwrap_err();
    assert!(
        matches!(err, VerifyError::Schema(ref m) if m.contains("canonical encoding")),
        "{err}"
    );
}

#[test]
fn integer_widths_share_an_encoding() {
    let (l, r) = dirs();
    write_batch(l.path(), "x.parquet", &batch(vec![("id", ids())]), 2);
    write_batch(
        r.path(),
        "x.parquet",
        &batch(vec![(
            "id",
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
        )]),
        2,
    );
    assert!(
        reconcile(l.path(), r.path(), ReconcileSpec::new(["id"]))
            .unwrap()
            .pass
    );
}

#[test]
fn a_type_with_no_canonical_encoding_must_be_ignored() {
    let (l, r) = dirs();
    // Nested types have no canonical encoding in encoding version 1.
    let dur = || {
        Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1)]),
            Some(vec![]),
            None,
        ])) as ArrayRef
    };
    write_batch(
        l.path(),
        "x.parquet",
        &batch(vec![("id", ids()), ("d", dur())]),
        2,
    );
    write_batch(
        r.path(),
        "x.parquet",
        &batch(vec![("id", ids()), ("d", dur())]),
        2,
    );
    let err = reconcile(l.path(), r.path(), ReconcileSpec::new(["id"])).unwrap_err();
    assert!(
        matches!(err, VerifyError::Schema(ref m) if m.contains("ignored_columns")),
        "{err}"
    );
    assert!(
        reconcile(l.path(), r.path(), ReconcileSpec::new(["id"]).ignore(["d"]))
            .unwrap()
            .pass
    );
}

#[test]
fn a_file_missing_a_dataset_column_is_an_error_never_a_pass() {
    let (l, r) = dirs();
    // The left dataset schema comes from its first file, which has `name`;
    // its second file does not.
    write_batch(
        l.path(),
        "a.parquet",
        &batch(vec![("id", ids()), ("name", names())]),
        2,
    );
    write_batch(
        l.path(),
        "b.parquet",
        &batch(vec![(
            "id",
            Arc::new(Int64Array::from(vec![4])) as ArrayRef,
        )]),
        2,
    );
    write_batch(
        r.path(),
        "a.parquet",
        &batch(vec![("id", ids()), ("name", names())]),
        2,
    );
    assert!(reconcile(l.path(), r.path(), ReconcileSpec::new(["id"])).is_err());
}
