//! List membership in the row filter (B4): `ContainsAny` keeps a row whose
//! list holds one of the values, `ContainsAll` one whose list holds every
//! one (in that row, not across rows), a null or empty list holds nothing,
//! and workload-rows coverage refuses a filter that keeps no row.

#![allow(clippy::unwrap_used, missing_docs)]

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, ListBuilder, RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether_core::encoding::{encode_value_as, Value};
use bellwether_source::ParquetSource;
use bellwether_types::{Hash, Predicate};
use bellwether_verify::{CoverageRequest, Options, ReconcileSpec, Reconciler, Side, VerifyError};
use parquet::arrow::ArrowWriter;

fn s(v: &str) -> Vec<u8> {
    let mut out = Vec::new();
    encode_value_as(&Value::Utf8(v), &DataType::Utf8, &mut out).unwrap();
    out
}

/// `(id, tags)`; `None` is a null list.
fn write(dir: &Path, rows: &[(i64, Option<Vec<&str>>)]) {
    let mut ids = Vec::new();
    let mut tags = ListBuilder::new(StringBuilder::new());
    for (id, t) in rows {
        ids.push(*id);
        match t {
            None => tags.append(false),
            Some(v) => {
                for x in v {
                    tags.values().append_value(x);
                }
                tags.append(true);
            }
        }
    }
    let tags = tags.finish();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tags", arrow::array::Array::data_type(&tags).clone(), true),
    ]));
    let cols: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(ids)), Arc::new(tags)];
    let batch = RecordBatch::try_new(Arc::clone(&schema), cols).unwrap();
    let mut w =
        ArrowWriter::try_new(File::create(dir.join("x.parquet")).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn run(left: &Path, right: &Path, spec: ReconcileSpec) -> bellwether_verify::Result<(bool, u64)> {
    let (ls, rs) = (ParquetSource::new(), ParquetSource::new());
    let (lr, rr) = (left.to_str().unwrap(), right.to_str().unwrap());
    let out =
        Reconciler::new(spec, Options::default())?.run(&Side::new(&ls, lr), &Side::new(&rs, rr))?;
    Ok((out.pass, out.counts.keys_compared))
}

fn spec(filter: Predicate) -> ReconcileSpec {
    // The list column is never hashed (it has no canonical row encoding);
    // the filter alone reads it, as `fix` sets it up.
    ReconcileSpec::new(["id"])
        .ignore(["tags"])
        .rows_where(vec![filter])
        .coverage(CoverageRequest::WorkloadRows {
            workload: Hash::of(b"w"),
            age: std::time::Duration::ZERO,
        })
}

/// Left holds every row; right drops row 2, whose list holds `a` but not
/// `b`. `ContainsAll [a, b]` keeps rows 1 and 4 only, so it passes; a test
/// that kept a row holding ANY of them would see row 2 missing on the right.
#[test]
fn contains_all_keeps_a_row_only_when_its_own_list_holds_every_value() {
    let left = tempfile::tempdir().unwrap();
    let right = tempfile::tempdir().unwrap();
    let all = [
        (1, Some(vec!["a", "b"])),
        (2, Some(vec!["a", "a"])),
        (3, Some(vec!["b"])),
        (4, Some(vec!["c", "b", "a"])),
        (5, None),
        (6, Some(vec![])),
    ];
    write(left.path(), &all);
    let kept: Vec<_> = all.iter().filter(|r| r.0 != 2).cloned().collect();
    write(right.path(), &kept);
    let both = Predicate::ContainsAll {
        column: "tags".into(),
        values: vec![vec![s("a")], vec![s("b")]],
    };
    let (pass, compared) = run(left.path(), right.path(), spec(both)).unwrap();
    assert!(pass);
    assert_eq!(compared, 2, "rows 1 and 4");
    // ContainsAny [a]: row 2 is asked for and missing on the right.
    let any = Predicate::ContainsAny {
        column: "tags".into(),
        values: vec![s("a")],
    };
    let (pass, compared) = run(left.path(), right.path(), spec(any)).unwrap();
    assert!(!pass);
    assert_eq!(compared, 3, "rows 1, 2, 4");
    // Null and empty lists hold nothing: dropping them is never seen.
    let none: Vec<_> = all.iter().filter(|r| r.0 < 5).cloned().collect();
    let r2 = tempfile::tempdir().unwrap();
    write(r2.path(), &none);
    let any_b = Predicate::ContainsAny {
        column: "tags".into(),
        values: vec![s("b")],
    };
    assert!(run(left.path(), r2.path(), spec(any_b)).unwrap().0);
}

/// Workload-rows coverage with a filter that keeps no row on either side
/// compares nothing, and says so, rather than certifying zero rows.
#[test]
fn a_membership_filter_that_keeps_no_row_compares_nothing() {
    let left = tempfile::tempdir().unwrap();
    let right = tempfile::tempdir().unwrap();
    let rows = [(1, Some(vec!["a"])), (2, Some(vec!["b"]))];
    write(left.path(), &rows);
    write(right.path(), &rows);
    let absent = Predicate::ContainsAny {
        column: "tags".into(),
        values: vec![s("zz")],
    };
    match run(left.path(), right.path(), spec(absent)) {
        Err(VerifyError::NothingCompared(why)) => {
            assert!(why.contains("check the recorded values"), "{why}");
        }
        other => panic!("{other:?}"),
    }
    // Workload-rows coverage without a filter is refused.
    let e = Reconciler::new(
        ReconcileSpec::new(["id"]).coverage(CoverageRequest::WorkloadRows {
            workload: Hash::of(b"w"),
            age: std::time::Duration::ZERO,
        }),
        Options::default(),
    )
    .err()
    .unwrap()
    .to_string();
    assert!(e.contains("row filter"), "{e}");
}
