//! Core reconcile behaviour: keys, versions, multisets, nulls, schemas.

#![allow(clippy::unwrap_used, missing_docs)]

mod common;

use bellwether_types::Coverage;
use bellwether_verify::{CoverageRequest, Mismatch, ReconcileSpec, Sides, VerifyError};
use common::{dirs, ledger, r, reconcile, write, R};

fn key_of(m: &Mismatch) -> Option<String> {
    match m {
        Mismatch::MissingOnLeft { key, .. }
        | Mismatch::MissingOnRight { key, .. }
        | Mismatch::RowHashDiffers { key, .. }
        | Mismatch::NoComparableVersion { key, .. } => key.values[0].clone(),
        Mismatch::TotalDiffers { .. } => None,
    }
}

#[test]
fn identical_sources_pass_with_zero_mismatches() {
    let (l, rt) = dirs();
    let rows = ledger(50, 3);
    // Different file and row-group layouts, same rows.
    write(l.path(), "a.parquet", &rows[..70], 16);
    write(l.path(), "b.parquet", &rows[70..], 7);
    write(rt.path(), "all.parquet", &rows, 100);
    for spec in [
        ReconcileSpec::new(["id"]),
        ReconcileSpec::new(["id"]).latest_by("ts"),
        ReconcileSpec::new(["id", "status"])
            .totals(["amount"])
            .group_by(["status"]),
    ] {
        let out = reconcile(l.path(), rt.path(), spec).unwrap();
        assert!(out.pass, "{out:#?}");
        assert_eq!(out.mismatches, vec![]);
        assert_eq!(out.counts.keys_compared, 50);
        assert_eq!(out.counts.matched, 50);
        assert_eq!(out.coverage, Coverage::Full);
        assert_eq!(out.sides[0].rows, 150);
        assert_eq!(out.sides[1].rows, 150);
    }
}

#[test]
fn one_changed_cell_is_one_row_hash_mismatch_on_exactly_that_key() {
    let (l, rt) = dirs();
    let rows = ledger(40, 1);
    let mut changed = rows.clone();
    changed[17].note = Some("edited".into());
    write(l.path(), "x.parquet", &rows, 8);
    write(rt.path(), "x.parquet", &changed, 8);
    let out = reconcile(l.path(), rt.path(), ReconcileSpec::new(["id"])).unwrap();
    assert!(!out.pass);
    assert_eq!(out.counts.mismatched, 1);
    assert_eq!(out.counts.missing_left + out.counts.missing_right, 0);
    assert_eq!(out.counts.matched, 39);
    assert_eq!(out.mismatches.len(), 1);
    let Mismatch::RowHashDiffers { key, left, right } = &out.mismatches[0] else {
        panic!("{:?}", out.mismatches[0]);
    };
    assert_eq!(key.values, [Some("17".to_string())]);
    assert_eq!(left.compared_rows, 1);
    assert_eq!(right.compared_rows, 1);
    assert_ne!(left.hash, right.hash);
}

#[test]
fn keys_on_one_side_only_are_missing_on_the_other() {
    let (l, rt) = dirs();
    write(
        l.path(),
        "x.parquet",
        &[r(1, 1, 1, "a"), r(2, 1, 1, "a")],
        10,
    );
    write(
        rt.path(),
        "x.parquet",
        &[r(2, 1, 1, "a"), r(3, 1, 1, "a")],
        10,
    );
    let out = reconcile(l.path(), rt.path(), ReconcileSpec::new(["id"])).unwrap();
    assert!(!out.pass);
    assert_eq!((out.counts.missing_right, out.counts.missing_left), (1, 1));
    assert!(
        matches!(&out.mismatches[0], Mismatch::MissingOnRight { key, .. } if key.values[0].as_deref() == Some("1"))
    );
    assert!(
        matches!(&out.mismatches[1], Mismatch::MissingOnLeft { key, .. } if key.values[0].as_deref() == Some("3"))
    );
}

#[test]
fn latest_by_passes_when_an_older_version_differs_but_the_newest_matches() {
    let (l, rt) = dirs();
    // Left holds history; right holds a different history with the same
    // newest version per key.
    write(
        l.path(),
        "h.parquet",
        &[r(1, 10, 100, "a"), r(1, 20, 200, "b"), r(2, 5, 7, "a")],
        2,
    );
    write(
        rt.path(),
        "h.parquet",
        &[r(1, 15, 999, "z"), r(1, 20, 200, "b"), r(2, 5, 7, "a")],
        2,
    );
    let latest = reconcile(
        l.path(),
        rt.path(),
        ReconcileSpec::new(["id"]).latest_by("ts"),
    )
    .unwrap();
    assert!(latest.pass, "{latest:#?}");
    // Without the version rule the multisets differ.
    let all = reconcile(l.path(), rt.path(), ReconcileSpec::new(["id"])).unwrap();
    assert!(!all.pass);
    assert_eq!(all.counts.mismatched, 1);
    // And when the newest versions differ, latest_by fails.
    let (l2, r2) = dirs();
    write(
        l2.path(),
        "h.parquet",
        &[r(1, 10, 100, "a"), r(1, 20, 200, "b")],
        2,
    );
    write(
        r2.path(),
        "h.parquet",
        &[r(1, 10, 100, "a"), r(1, 20, 201, "b")],
        2,
    );
    let out = reconcile(
        l2.path(),
        r2.path(),
        ReconcileSpec::new(["id"]).latest_by("ts"),
    )
    .unwrap();
    assert_eq!(out.counts.mismatched, 1);
}

#[test]
fn a_null_timestamp_never_wins() {
    let (l, rt) = dirs();
    let mut null_version = r(1, 0, 555, "stale");
    null_version.ts = None;
    // Left: a null-version row that differs, plus the real newest version.
    write(
        l.path(),
        "x.parquet",
        &[r(1, 50, 100, "a"), null_version.clone()],
        1,
    );
    write(rt.path(), "x.parquet", &[r(1, 50, 100, "a")], 1);
    let out = reconcile(
        l.path(),
        rt.path(),
        ReconcileSpec::new(["id"]).latest_by("ts"),
    )
    .unwrap();
    assert!(
        out.pass,
        "a null version must not replace the newest one: {out:#?}"
    );
    // Order within the file does not matter.
    let (l2, r2) = dirs();
    write(
        l2.path(),
        "x.parquet",
        &[null_version, r(1, 50, 100, "a")],
        1,
    );
    write(r2.path(), "x.parquet", &[r(1, 50, 100, "a")], 1);
    let out = reconcile(
        l2.path(),
        r2.path(),
        ReconcileSpec::new(["id"]).latest_by("ts"),
    )
    .unwrap();
    assert!(out.pass, "{out:#?}");
}

#[test]
fn a_key_whose_versions_are_all_null_fails_to_compare() {
    let (l, rt) = dirs();
    let mut a = r(1, 0, 1, "a");
    a.ts = None;
    write(l.path(), "x.parquet", &[a.clone(), r(2, 5, 1, "a")], 10);
    write(rt.path(), "x.parquet", &[a, r(2, 5, 1, "a")], 10);
    let out = reconcile(
        l.path(),
        rt.path(),
        ReconcileSpec::new(["id"]).latest_by("ts"),
    )
    .unwrap();
    // Identical data, but key 1 has no version on either side: it cannot be
    // certified. It counts in `mismatched` (the certificate's counter) and
    // is broken out in `no_comparable_version`.
    assert!(!out.pass);
    assert_eq!(out.counts.no_comparable_version, 1);
    assert_eq!(out.counts.mismatched, 1);
    assert_eq!(out.counts.missing_left + out.counts.missing_right, 0);
    assert_eq!(out.counts.matched, 1);
    assert!(matches!(
        &out.mismatches[0],
        Mismatch::NoComparableVersion {
            sides: Sides::Both,
            ..
        }
    ));
}

#[test]
fn ties_at_the_newest_version_compare_as_a_multiset() {
    let (l, rt) = dirs();
    write(
        l.path(),
        "x.parquet",
        &[r(1, 9, 1, "a"), r(1, 9, 2, "b")],
        1,
    );
    write(
        rt.path(),
        "x.parquet",
        &[r(1, 9, 2, "b"), r(1, 9, 1, "a")],
        1,
    );
    assert!(
        reconcile(
            l.path(),
            rt.path(),
            ReconcileSpec::new(["id"]).latest_by("ts")
        )
        .unwrap()
        .pass
    );
    let (l2, r2) = dirs();
    write(
        l2.path(),
        "x.parquet",
        &[r(1, 9, 1, "a"), r(1, 9, 2, "b")],
        1,
    );
    write(r2.path(), "x.parquet", &[r(1, 9, 1, "a")], 1);
    let out = reconcile(
        l2.path(),
        r2.path(),
        ReconcileSpec::new(["id"]).latest_by("ts"),
    )
    .unwrap();
    assert_eq!(out.counts.mismatched, 1);
}

#[test]
fn without_latest_by_the_multiset_of_rows_must_match() {
    let (l, rt) = dirs();
    let row = r(1, 1, 1, "a");
    write(l.path(), "x.parquet", &[row.clone(), row.clone()], 10);
    write(rt.path(), "x.parquet", std::slice::from_ref(&row), 10);
    let out = reconcile(l.path(), rt.path(), ReconcileSpec::new(["id"])).unwrap();
    assert_eq!(out.counts.mismatched, 1, "a duplicated row is a difference");
    let (l2, r2) = dirs();
    write(
        l2.path(),
        "x.parquet",
        &[r(1, 1, 1, "a"), r(1, 2, 2, "b")],
        10,
    );
    write(
        r2.path(),
        "x.parquet",
        &[r(1, 2, 2, "b"), r(1, 1, 1, "a")],
        10,
    );
    assert!(
        reconcile(l2.path(), r2.path(), ReconcileSpec::new(["id"]))
            .unwrap()
            .pass
    );
}

#[test]
fn rows_with_null_keys_are_compared_not_dropped() {
    let (l, rt) = dirs();
    let null_key = |cents| R {
        id: None,
        ..r(0, 1, cents, "a")
    };
    write(l.path(), "x.parquet", &[null_key(1), r(1, 1, 1, "a")], 10);
    write(rt.path(), "x.parquet", &[null_key(2), r(1, 1, 1, "a")], 10);
    let out = reconcile(l.path(), rt.path(), ReconcileSpec::new(["id"])).unwrap();
    assert!(!out.pass, "the null-key rows differ and must be seen");
    assert_eq!(out.counts.mismatched, 1);
    assert_eq!(key_of(&out.mismatches[0]), None);
    let Mismatch::RowHashDiffers { key, .. } = &out.mismatches[0] else {
        panic!()
    };
    assert_eq!(key.canonical, "00", "the null marker");
}

#[test]
fn an_ignored_column_may_change_and_is_listed() {
    let (l, rt) = dirs();
    let rows = ledger(10, 1);
    let mut changed = rows.clone();
    for row in &mut changed {
        row.note = Some("rewritten".into());
    }
    write(l.path(), "x.parquet", &rows, 4);
    write(rt.path(), "x.parquet", &changed, 4);
    assert!(
        !reconcile(l.path(), rt.path(), ReconcileSpec::new(["id"]))
            .unwrap()
            .pass
    );
    let out = reconcile(
        l.path(),
        rt.path(),
        ReconcileSpec::new(["id"]).ignore(["note"]),
    )
    .unwrap();
    assert!(out.pass);
    assert_eq!(out.ignored_columns, ["note"]);
    assert_eq!(out.hashed_columns, ["id", "ts", "amount", "status"]);
}

#[test]
fn static_proof_is_a_typed_error() {
    let (l, rt) = dirs();
    write(l.path(), "x.parquet", &ledger(2, 1), 4);
    write(rt.path(), "x.parquet", &ledger(2, 1), 4);
    let spec = ReconcileSpec::new(["id"]).coverage(CoverageRequest::StaticProof {
        identity: "semijoin_before_window".into(),
    });
    assert!(matches!(
        reconcile(l.path(), rt.path(), spec),
        Err(VerifyError::StaticProofUnsupported { identity }) if identity == "semijoin_before_window"
    ));
}

#[test]
fn spec_errors_are_typed() {
    let (l, rt) = dirs();
    write(l.path(), "x.parquet", &ledger(2, 1), 4);
    write(rt.path(), "x.parquet", &ledger(2, 1), 4);
    for spec in [
        ReconcileSpec::new(Vec::<String>::new()),
        ReconcileSpec::new(["id", "id"]),
        ReconcileSpec::new(["id"]).ignore(["id"]),
        ReconcileSpec::new(["id"]).latest_by("id"),
        ReconcileSpec::new(["id"]).ignore(["no_such_column"]),
        ReconcileSpec::new(["id"]).totals(["status"]),
        ReconcileSpec::new(["id"]).latest_by("status"),
        ReconcileSpec::new(["id"]).coverage(CoverageRequest::Sampled {
            fraction: 0.0,
            seed: 1,
        }),
        ReconcileSpec::new(["id"]).coverage(CoverageRequest::Sampled {
            fraction: 1.5,
            seed: 1,
        }),
        ReconcileSpec::new(["id"]).coverage(CoverageRequest::Sampled {
            fraction: f64::NAN,
            seed: 1,
        }),
    ] {
        let err = reconcile(l.path(), rt.path(), spec.clone()).unwrap_err();
        assert!(
            matches!(err, VerifyError::InvalidSpec(_) | VerifyError::Schema(_)),
            "{spec:?}: {err}"
        );
    }
}
