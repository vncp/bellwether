//! Exact decimal totals at the dataset scale.

#![allow(clippy::unwrap_used, missing_docs)]

mod common;

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Decimal128Array, Decimal256Array, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::i256;
use bellwether_verify::{
    CoverageRequest, ExactTotal, Mismatch, ReconcileSpec, Reconciliation, TotalValue, VerifyError,
};
use common::{batch_at, dirs, ledger, r, reconcile, write, write_batch};

fn dec_batch(ids: &[i64], unscaled: &[i128], precision: u8, scale: i8) -> RecordBatch {
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef),
        (
            "amount",
            Arc::new(
                Decimal128Array::from(unscaled.to_vec())
                    .with_precision_and_scale(precision, scale)
                    .unwrap(),
            ),
        ),
    ])
    .unwrap()
}

fn total(out: &bellwether_verify::Reconciliation, column: &str) -> (ExactTotal, ExactTotal) {
    let t = out.totals.iter().find(|t| t.column == column).unwrap();
    (t.left, t.right)
}

#[test]
fn files_at_different_scales_give_equal_totals() {
    // Left: the first file (dataset scale 3) and a second file at scale 2.
    // Right: every row in one scale-3 file. Same values, so equal hashes and
    // equal totals at the dataset scale.
    let (l, rt) = dirs();
    let rows = ledger(30, 2);
    write_batch(l.path(), "a.parquet", &batch_at(&rows[..25], 3), 8);
    write_batch(l.path(), "b.parquet", &batch_at(&rows[25..], 2), 8);
    write_batch(rt.path(), "a.parquet", &batch_at(&rows, 3), 50);
    let out = reconcile(
        l.path(),
        rt.path(),
        ReconcileSpec::new(["id"]).totals(["amount"]),
    )
    .unwrap();
    assert!(out.pass, "{out:#?}");
    let (left, right) = total(&out, "amount");
    let cents: i128 = rows.iter().map(|r| r.amount.unwrap()).sum();
    assert_eq!(left, right);
    assert_eq!(
        left,
        ExactTotal {
            unscaled: TotalValue::I128(cents * 10),
            scale: 3
        }
    );
    assert_eq!(out.totals[0].left_rows, 60);
}

#[test]
fn a_value_unrepresentable_at_the_dataset_scale_is_an_error_not_a_pass() {
    let (l, rt) = dirs();
    // Dataset scale 2 (first file); the second file holds 1.235 at scale 3.
    write_batch(l.path(), "a.parquet", &dec_batch(&[1], &[150], 18, 2), 4);
    write_batch(l.path(), "b.parquet", &dec_batch(&[2], &[1235], 18, 3), 4);
    // The right side holds the very same files, so a comparison that rounded
    // or skipped the value would pass.
    write_batch(rt.path(), "a.parquet", &dec_batch(&[1], &[150], 18, 2), 4);
    write_batch(rt.path(), "b.parquet", &dec_batch(&[2], &[1235], 18, 3), 4);
    for spec in [
        ReconcileSpec::new(["id"]),
        ReconcileSpec::new(["id"]).totals(["amount"]),
        ReconcileSpec::new(["id"])
            .ignore(["amount"])
            .totals(["amount"]),
    ] {
        let err = reconcile(l.path(), rt.path(), spec).unwrap_err();
        let VerifyError::Unrepresentable { column, file, .. } = &err else {
            panic!("{err}");
        };
        assert_eq!(column, "amount");
        assert!(file.ends_with("b.parquet"), "{file}");
    }
}

#[test]
fn totals_are_exact_scaled_integers_per_group() {
    let (l, rt) = dirs();
    let rows = ledger(10, 1);
    write(l.path(), "x.parquet", &rows, 3);
    write(rt.path(), "x.parquet", &rows, 7);
    let out = reconcile(
        l.path(),
        rt.path(),
        ReconcileSpec::new(["id"])
            .totals(["amount"])
            .group_by(["status"]),
    )
    .unwrap();
    assert!(out.pass);
    let groups: Vec<(Option<String>, String)> = out
        .totals
        .iter()
        .map(|t| (t.group.values[0].1.clone(), t.left.render()))
        .collect();
    // Status by id mod 3; amount = id × 100 cents.
    assert_eq!(
        groups,
        [
            (Some("a".into()), "18.00".into()),
            (Some("b".into()), "12.00".into()),
            (Some("c".into()), "15.00".into()),
        ]
    );
}

#[test]
fn totals_with_latest_by_sum_the_compared_versions() {
    let (l, rt) = dirs();
    // Left holds history, right only the newest version of each key.
    write(
        l.path(),
        "x.parquet",
        &[r(1, 1, 100, "a"), r(1, 2, 250, "a"), r(2, 1, 40, "b")],
        10,
    );
    write(
        rt.path(),
        "x.parquet",
        &[r(1, 2, 250, "a"), r(2, 1, 40, "b")],
        10,
    );
    let spec = ReconcileSpec::new(["id"]).totals(["amount"]);
    let out = reconcile(l.path(), rt.path(), spec.clone().latest_by("ts")).unwrap();
    assert!(out.pass, "{out:#?}");
    assert_eq!(total(&out, "amount").0.render(), "2.90");
    // Over every row, the history adds 1.00 on the left.
    let all = reconcile(l.path(), rt.path(), spec).unwrap();
    assert!(!all.pass);
    let (left, right) = total(&all, "amount");
    assert_eq!(
        (left.render(), right.render()),
        ("3.90".into(), "2.90".into())
    );
}

#[test]
fn a_differing_total_is_reported_with_its_group_and_both_values() {
    let (l, rt) = dirs();
    let rows = ledger(6, 1);
    let mut changed = rows.clone();
    changed[4].amount = Some(changed[4].amount.unwrap() + 1); // id 4, status "b"
    write(l.path(), "x.parquet", &rows, 10);
    write(rt.path(), "x.parquet", &changed, 10);
    // With amount out of the row hash, only the total can see the change.
    let spec = ReconcileSpec::new(["id"])
        .ignore(["amount"])
        .totals(["amount"])
        .group_by(["status"]);
    let out = reconcile(l.path(), rt.path(), spec).unwrap();
    assert!(!out.pass);
    assert_eq!(out.counts.mismatched, 0);
    assert_eq!(out.counts.totals_differ, 1);
    let Mismatch::TotalDiffers {
        group,
        column,
        left,
        right,
    } = &out.mismatches[0]
    else {
        panic!("{:?}", out.mismatches);
    };
    assert_eq!(column, "amount");
    assert_eq!(
        group.values,
        [("status".to_string(), Some("b".to_string()))]
    );
    assert_eq!(
        (left.render(), right.render()),
        ("5.00".into(), "5.01".into())
    );
    assert_eq!(
        group.label(column),
        r#"{"column":"amount","group":[["status","b"]]}"#
    );
}

#[test]
fn a_total_that_overflows_its_integer_is_an_error() {
    let (l, rt) = dirs();
    let big = 10i128.pow(38) - 1;
    let b = dec_batch(&[1, 2], &[big, big], 38, 0);
    write_batch(l.path(), "x.parquet", &b, 4);
    write_batch(rt.path(), "x.parquet", &b, 4);
    let err = reconcile(
        l.path(),
        rt.path(),
        ReconcileSpec::new(["id"]).totals(["amount"]),
    )
    .unwrap_err();
    assert!(
        matches!(err, VerifyError::Overflow { ref column, width: "i128", .. } if column == "amount"),
        "{err}"
    );
    // An intermediate overflow that the exact total undoes is not one.
    let (l2, r2) = dirs();
    let b = dec_batch(&[1, 2, 3], &[big, big, -big], 38, 0);
    write_batch(l2.path(), "x.parquet", &b, 1);
    write_batch(r2.path(), "x.parquet", &b, 3);
    let out = reconcile(
        l2.path(),
        r2.path(),
        ReconcileSpec::new(["id"]).totals(["amount"]),
    )
    .unwrap();
    assert_eq!(total(&out, "amount").0.unscaled, TotalValue::I128(big));
}

#[test]
fn decimal256_totals_are_i256() {
    let (l, rt) = dirs();
    let v = i256::from_i128(10i128.pow(37))
        .checked_mul(i256::from_i128(1000))
        .unwrap();
    let b = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
        (
            "amount",
            Arc::new(
                Decimal256Array::from(vec![v, v])
                    .with_precision_and_scale(60, 4)
                    .unwrap(),
            ),
        ),
        ("s", Arc::new(StringArray::from(vec!["x", "y"]))),
    ])
    .unwrap();
    write_batch(l.path(), "x.parquet", &b, 4);
    write_batch(rt.path(), "x.parquet", &b, 4);
    let out = reconcile(
        l.path(),
        rt.path(),
        ReconcileSpec::new(["id"]).totals(["amount"]),
    )
    .unwrap();
    assert!(out.pass);
    let (left, _) = total(&out, "amount");
    assert_eq!(left.unscaled, TotalValue::I256(v.wrapping_add(v)));
    assert_eq!(left.scale, 4);
    assert_eq!(left.render(), "2000000000000000000000000000000000000.0000");
}

#[test]
fn a_small_decimal256_total_keeps_its_width_through_json() {
    // A Decimal256 total that fits in an i128 must still read back as i256,
    // and the whole result, sampled coverage included, must round-trip.
    let (l, rt) = dirs();
    let b = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
        (
            "amount",
            Arc::new(
                Decimal256Array::from(vec![
                    i256::from_i128(5),
                    i256::from_i128(-7),
                    i256::from_i128(9),
                ])
                .with_precision_and_scale(40, 2)
                .unwrap(),
            ),
        ),
    ])
    .unwrap();
    write_batch(l.path(), "x.parquet", &b, 2);
    write_batch(rt.path(), "x.parquet", &b, 2);
    let spec = ReconcileSpec::new(["id"])
        .totals(["amount"])
        .coverage(CoverageRequest::Sampled {
            fraction: 0.999,
            seed: 5,
        });
    let out = reconcile(l.path(), rt.path(), spec).unwrap();
    let json = serde_json::to_string(&out).unwrap();
    let back: Reconciliation = serde_json::from_str(&json).unwrap();
    assert_eq!(back, out);
    assert!(matches!(out.totals[0].left.unscaled, TotalValue::I256(_)));
}
