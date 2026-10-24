//! Coverage: every key, a recorded workload's keys, or a seeded sample.

#![allow(clippy::unwrap_used, missing_docs)]

mod common;

use std::collections::BTreeSet;
use std::time::Duration;

use arrow::datatypes::DataType;
use bellwether_core::encoding::{encode_tuple_as, Value};
use bellwether_types::{Coverage, Hash};
use bellwether_verify::{CoverageRequest, Mismatch, ReconcileSpec, VerifyError, WorkloadKeys};
use common::{dirs, ledger, reconcile, write, R};
use xxhash_rust::xxh3::xxh3_64_with_seed;

fn key(id: i64) -> Vec<u8> {
    encode_tuple_as(&[Value::Int(id)], &[DataType::Int64]).unwrap()
}

fn workload(ids: &[i64]) -> CoverageRequest {
    CoverageRequest::Workload(WorkloadKeys {
        workload: Hash::of(b"recorded workload"),
        age: Duration::from_secs(90),
        keys: ids.iter().map(|&i| key(i)).collect(),
    })
}

fn mismatched_ids(ms: &[Mismatch]) -> BTreeSet<i64> {
    ms.iter()
        .filter_map(|m| match m {
            Mismatch::MissingOnLeft { key, .. }
            | Mismatch::MissingOnRight { key, .. }
            | Mismatch::RowHashDiffers { key, .. }
            | Mismatch::NoComparableVersion { key, .. } => key.values[0].as_ref()?.parse().ok(),
            Mismatch::TotalDiffers { .. } => None,
        })
        .collect()
}

/// Left and right differ on keys 3 (a cell), 5 (left only), and 15
/// (right only).
fn differing() -> (tempfile::TempDir, tempfile::TempDir) {
    let (l, rt) = dirs();
    let rows = ledger(20, 2);
    let mut right: Vec<R> = rows.iter().filter(|x| x.id != Some(5)).cloned().collect();
    for x in &mut right {
        if x.id == Some(3) {
            x.status = "changed".into();
        }
    }
    let left: Vec<R> = rows.iter().filter(|x| x.id != Some(15)).cloned().collect();
    write(l.path(), "x.parquet", &left, 6);
    write(rt.path(), "x.parquet", &right, 6);
    (l, rt)
}

#[test]
fn workload_coverage_reports_nothing_outside_the_supplied_keys_and_says_workload() {
    let (l, rt) = differing();
    let full = reconcile(l.path(), rt.path(), ReconcileSpec::new(["id"])).unwrap();
    assert_eq!(mismatched_ids(&full.mismatches), BTreeSet::from([3, 5, 15]));

    let spec = ReconcileSpec::new(["id"]).coverage(workload(&[1, 2, 3, 4, 100, 3]));
    let out = reconcile(l.path(), rt.path(), spec).unwrap();
    assert_eq!(
        out.coverage,
        Coverage::Workload {
            workload: Hash::of(b"recorded workload"),
            keys: 5
        }
    );
    assert_eq!(mismatched_ids(&out.mismatches), BTreeSet::from([3]));
    assert_eq!(out.counts.keys_compared, 5);
    assert_eq!(out.counts.matched, 3);
    assert_eq!(out.counts.mismatched, 1);
    assert_eq!(out.counts.absent_both, 1, "key 100 is on neither side");
    assert_eq!(out.counts.missing_left + out.counts.missing_right, 0);
    assert!(!out.pass);
    // Scoped to keys that agree, the reconcile passes.
    let out = reconcile(
        l.path(),
        rt.path(),
        ReconcileSpec::new(["id"]).coverage(workload(&[1, 2])),
    )
    .unwrap();
    assert!(out.pass);
}

#[test]
fn workload_totals_cover_only_the_supplied_keys() {
    let (l, rt) = differing();
    let spec = ReconcileSpec::new(["id"])
        .totals(["amount"])
        .coverage(workload(&[1, 2]));
    let out = reconcile(l.path(), rt.path(), spec).unwrap();
    // ids 1 and 2, two versions each: 100+101 and 200+201 cents.
    assert_eq!(out.totals[0].left.render(), "6.02");
    assert_eq!(out.totals[0].left_rows, 4);
}

#[test]
fn malformed_null_or_empty_workload_keys_are_errors() {
    let (l, rt) = differing();
    let bad = |keys: Vec<Vec<u8>>| {
        ReconcileSpec::new(["id"]).coverage(CoverageRequest::Workload(WorkloadKeys {
            workload: Hash::of(b"w"),
            age: Duration::ZERO,
            keys,
        }))
    };
    let uint = encode_tuple_as(&[Value::UInt(1)], &[DataType::UInt64]).unwrap();
    for keys in [
        vec![],
        vec![vec![0x00]],
        vec![key(1)[..5].to_vec()],
        vec![uint],
        vec![[key(1), key(2)].concat()],
    ] {
        let err = reconcile(l.path(), rt.path(), bad(keys.clone())).unwrap_err();
        assert!(
            matches!(err, VerifyError::InvalidSpec(_)),
            "{keys:?}: {err}"
        );
    }
}

fn big() -> (tempfile::TempDir, tempfile::TempDir, BTreeSet<i64>) {
    let (l, rt) = dirs();
    let rows = ledger(2_000, 1);
    let mut changed = rows.clone();
    let mut differ = BTreeSet::new();
    for (i, x) in changed.iter_mut().enumerate() {
        if i % 40 == 7 {
            x.status = "edited".into();
            differ.insert(x.id.unwrap());
        }
    }
    write(l.path(), "x.parquet", &rows, 256);
    write(rt.path(), "x.parquet", &changed, 300);
    (l, rt, differ)
}

fn sampled(fraction: f64, seed: u64) -> ReconcileSpec {
    ReconcileSpec::new(["id"]).coverage(CoverageRequest::Sampled { fraction, seed })
}

#[test]
fn sampled_coverage_is_deterministic_by_seed() {
    let (l, rt, differ) = big();
    let a = reconcile(l.path(), rt.path(), sampled(0.25, 42)).unwrap();
    let b = reconcile(l.path(), rt.path(), sampled(0.25, 42)).unwrap();
    assert_eq!(a, b, "the same seed selects the same sample");
    let c = reconcile(l.path(), rt.path(), sampled(0.25, 43)).unwrap();
    assert_ne!(
        mismatched_ids(&a.mismatches),
        mismatched_ids(&c.mismatches),
        "another seed selects another sample"
    );
    // The sample is exactly the keys whose seeded hash is under the
    // threshold, recomputed here independently.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let threshold = (0.25 * 18_446_744_073_709_551_616.0) as u64;
    let expected: BTreeSet<i64> = (0..2_000)
        .filter(|&id| xxh3_64_with_seed(&key(id), 42) < threshold)
        .collect();
    let Coverage::Sampled { keys, fraction } = a.coverage else {
        panic!("{:?}", a.coverage);
    };
    assert_eq!(keys, expected.len() as u64);
    #[allow(clippy::cast_precision_loss)]
    let realized = expected.len() as f64 / 2_000.0;
    assert!(
        (fraction - realized).abs() < f64::EPSILON,
        "realized {fraction} vs {realized}"
    );
    assert_eq!(
        mismatched_ids(&a.mismatches),
        differ.intersection(&expected).copied().collect()
    );
}

#[test]
fn sampling_everything_covers_every_key() {
    let (l, rt, differ) = big();
    let out = reconcile(l.path(), rt.path(), sampled(1.0, 7)).unwrap();
    assert_eq!(
        out.coverage,
        Coverage::Sampled {
            keys: 2_000,
            fraction: 1.0
        }
    );
    assert_eq!(out.counts.mismatched, differ.len() as u64);
}

#[test]
fn a_sample_of_nothing_reports_the_requested_fraction() {
    // Two sides with no rows: the realized fraction is 0/0.
    let (l, rt) = dirs();
    write(l.path(), "x.parquet", &[] as &[R], 4);
    write(rt.path(), "x.parquet", &[] as &[R], 4);
    let out = reconcile(l.path(), rt.path(), sampled(0.5, 1)).unwrap();
    assert_eq!(
        out.coverage,
        Coverage::Sampled {
            keys: 0,
            fraction: 0.5
        }
    );
    assert!(out.pass);
}
