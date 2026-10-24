//! Deterministic tests for the gaps the review's mutation run found: each
//! difference alone must fail the reconcile.

#![allow(clippy::unwrap_used, missing_docs)]

mod common;

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::DataType;
use bellwether_verify::{row_hash, Mismatch, ReconcileSpec};
use common::{dirs, ledger, r, reconcile, write, write_batch};
use xxhash_rust::xxh3::xxh3_128;

/// P1: a key on the right only is the sole difference.
#[test]
fn a_key_only_on_the_right_fails_alone() {
    let (l, rt) = dirs();
    let rows = ledger(5, 1);
    let mut more = rows.clone();
    more.push(r(99, 1, 1, "a"));
    write(l.path(), "x.parquet", &rows, 4);
    write(rt.path(), "x.parquet", &more, 4);
    let out = reconcile(l.path(), rt.path(), ReconcileSpec::new(["id"])).unwrap();
    assert!(!out.pass);
    assert_eq!(out.counts.missing_left, 1);
    assert_eq!(
        (
            out.counts.missing_right,
            out.counts.mismatched,
            out.counts.totals_differ
        ),
        (0, 0, 0)
    );
}

/// P2: a key on the left only is the sole difference.
#[test]
fn a_key_only_on_the_left_fails_alone() {
    let (l, rt) = dirs();
    let rows = ledger(5, 1);
    let mut more = rows.clone();
    more.push(r(99, 1, 1, "a"));
    write(l.path(), "x.parquet", &more, 4);
    write(rt.path(), "x.parquet", &rows, 4);
    let out = reconcile(l.path(), rt.path(), ReconcileSpec::new(["id"])).unwrap();
    assert!(!out.pass);
    assert_eq!(out.counts.missing_right, 1);
    assert_eq!(
        (
            out.counts.missing_left,
            out.counts.mismatched,
            out.counts.totals_differ
        ),
        (0, 0, 0)
    );
}

/// H7: the name-length prefix counts UTF-8 bytes, not characters.
#[test]
fn a_multi_byte_column_name_is_prefixed_by_its_byte_length() {
    let name = "montant_€é"; // 10 characters, 13 bytes
    assert_eq!((name.chars().count(), name.len()), (10, 13));
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![7])) as ArrayRef),
        (name, Arc::new(StringArray::from(vec!["x"])) as ArrayRef),
    ])
    .unwrap();
    let mut input = Vec::new();
    input.extend_from_slice(&2u32.to_be_bytes());
    input.extend_from_slice(b"id");
    input.extend_from_slice(&[0x02, 0x80, 0, 0, 0, 0, 0, 0, 0x07]);
    input.extend_from_slice(&13u32.to_be_bytes());
    input.extend_from_slice(name.as_bytes());
    input.extend_from_slice(&[0x06, b'x', 0x00, 0x00]);
    let expected = xxh3_128(&input);
    let columns = [
        ("id".to_string(), DataType::Int64),
        (name.to_string(), DataType::Utf8),
    ];
    assert_eq!(row_hash(&batch, 0, &columns).unwrap(), expected);
    assert_eq!(expected, GOLDEN_MULTIBYTE, "pinned: {expected:#034x}");
    // Through the reconciler: the left row's hash, exposed by a key missing
    // on the right.
    let (l, rt) = dirs();
    write_batch(l.path(), "x.parquet", &batch, 4);
    let other = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![8])) as ArrayRef),
        (name, Arc::new(StringArray::from(vec!["x"])) as ArrayRef),
    ])
    .unwrap();
    write_batch(rt.path(), "x.parquet", &other, 4);
    let out = reconcile(l.path(), rt.path(), ReconcileSpec::new(["id"])).unwrap();
    let hash = out
        .mismatches
        .iter()
        .find_map(|m| match m {
            Mismatch::MissingOnRight { left, .. } => Some(left.hash.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!(hash, format!("{expected:032x}"));
}

const GOLDEN_MULTIBYTE: u128 = 0x6bc4_4370_be11_f1e2_ed90_1bf0_0c1d_cc27;

/// T5: a group present only on the right is a differing total.
#[test]
fn a_group_only_on_the_right_is_a_differing_total() {
    let (l, rt) = dirs();
    let rows = ledger(6, 1);
    let mut moved = rows.clone();
    moved[2].status = "z".into(); // a group the left never has
    write(l.path(), "x.parquet", &rows, 4);
    write(rt.path(), "x.parquet", &moved, 4);
    // status out of the row hash: only the totals can see the move.
    let spec = ReconcileSpec::new(["id"])
        .ignore(["status"])
        .totals(["amount"])
        .group_by(["status"]);
    let out = reconcile(l.path(), rt.path(), spec).unwrap();
    assert!(!out.pass);
    assert_eq!(out.counts.mismatched, 0);
    let z = out
        .mismatches
        .iter()
        .find_map(|m| match m {
            Mismatch::TotalDiffers {
                group, left, right, ..
            } if group.values[0].1.as_deref() == Some("z") => Some((left.render(), right.render())),
            _ => None,
        })
        .expect("the right-only group is reported");
    assert_eq!(z, ("0.00".to_string(), "2.00".to_string()));
}
