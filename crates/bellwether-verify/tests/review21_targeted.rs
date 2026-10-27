//! Review probes for PR #21 (reviewer, not the author).
//!
//! `guard_*` tests assert behaviour the crate must have and are expected to
//! pass on the PR head (they double as mutation killers). `finding_*` tests
//! assert the behaviour the review requires and are expected to FAIL on the
//! PR head: each one is a reproduction of a required change.
//!
//! Expectations here never come from `bellwether_verify` code: they are
//! stated from the data the test writes.

#![allow(
    clippy::unwrap_used,
    clippy::pedantic,
    clippy::cloned_ref_to_slice_refs,
    missing_docs,
    dead_code
)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Decimal128Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bellwether_build::{BuildOptions, Builder, SourceKind};
use bellwether_ledger::{Ledger, RecordKind};
use bellwether_source::ParquetSource;
use bellwether_store::ObjectStoreBackend;
use bellwether_types::{Coverage, Hash, KeySpec};
use bellwether_verify::{
    record, CertificateContext, CoverageRequest, IndexPlanner, ManifestAnchor, Options,
    ReconcileSpec, Reconciler, Side, VerifyError, WorkloadKeys,
};
use common::{dirs, r, reconcile, root, schema_at, write, write_batch, Spy, R};

fn latest(spec: ReconcileSpec) -> ReconcileSpec {
    spec.latest_by("ts")
}

fn anchor() -> CertificateContext {
    CertificateContext {
        manifest: Some(ManifestAnchor {
            sequence: 3,
            hash: Hash::of(b"anchor"),
            age: Duration::ZERO,
        }),
        ..CertificateContext::default()
    }
}

/// Hand-built canonical Int64 key (format.md: 0x02, BE, sign bit flipped).
fn int_key(v: i64) -> Vec<u8> {
    let mut b = vec![0x02];
    let mut x = v.to_be_bytes();
    x[0] ^= 0x80;
    b.extend_from_slice(&x);
    b
}

fn workload(keys: Vec<Vec<u8>>) -> CoverageRequest {
    CoverageRequest::Workload(WorkloadKeys {
        workload: Hash::of(b"w"),
        age: Duration::ZERO,
        keys,
    })
}

// ---------------------------------------------------------------------------
// Focus 1: false passes that must not happen (guards)
// ---------------------------------------------------------------------------

#[test]
fn guard_an_ignored_key_column_is_refused_even_in_a_composite_key() {
    let (l, rt) = dirs();
    write(l.path(), "x.parquet", &[r(1, 1, 1, "a")], 4);
    write(rt.path(), "x.parquet", &[r(1, 1, 1, "b")], 4);
    for spec in [
        ReconcileSpec::new(["id"]).ignore(["id"]),
        ReconcileSpec::new(["id", "status"]).ignore(["status"]),
        latest(ReconcileSpec::new(["id", "status"])).ignore(["status"]),
    ] {
        let err = reconcile(l.path(), rt.path(), spec.clone()).unwrap_err();
        assert!(
            matches!(err, VerifyError::InvalidSpec(_)),
            "{spec:?}: {err}"
        );
    }
}

#[test]
fn guard_a_change_only_in_a_discarded_version_passes_and_a_changed_newest_fails() {
    let (l, rt) = dirs();
    write(
        l.path(),
        "x.parquet",
        &[r(1, 10, 100, "a"), r(1, 20, 200, "b")],
        1,
    );
    write(
        rt.path(),
        "x.parquet",
        &[r(1, 10, 101, "a"), r(1, 20, 200, "b")],
        1,
    );
    let out = reconcile(l.path(), rt.path(), latest(ReconcileSpec::new(["id"]))).unwrap();
    assert!(out.pass, "older version differs, newest equal: {out:#?}");
    // Newest differs, older equal.
    let (l2, r2) = dirs();
    write(
        l2.path(),
        "x.parquet",
        &[r(1, 10, 100, "a"), r(1, 20, 200, "b")],
        1,
    );
    write(
        r2.path(),
        "x.parquet",
        &[r(1, 10, 100, "a"), r(1, 20, 200, "c")],
        1,
    );
    let out = reconcile(l2.path(), r2.path(), latest(ReconcileSpec::new(["id"]))).unwrap();
    assert!(!out.pass);
    assert_eq!(out.counts.mismatched, 1);
    // Newest equal in content but at a different timestamp: the ts is
    // hashed, so it differs.
    let (l3, r3) = dirs();
    write(
        l3.path(),
        "x.parquet",
        &[r(1, 10, 100, "a"), r(1, 20, 200, "b")],
        1,
    );
    write(
        r3.path(),
        "x.parquet",
        &[r(1, 10, 100, "a"), r(1, 21, 200, "b")],
        1,
    );
    let out = reconcile(l3.path(), r3.path(), latest(ReconcileSpec::new(["id"]))).unwrap();
    assert!(!out.pass);
}

#[test]
fn guard_ties_at_the_newest_timestamp_are_multisets() {
    let a = r(1, 5, 1, "a");
    let b = r(1, 5, 2, "b");
    let c = r(1, 5, 3, "c");
    let cases: [(&[R], &[R], bool); 5] = [
        (&[a.clone(), b.clone()], &[b.clone(), a.clone()], true),
        (&[a.clone(), b.clone()], &[a.clone(), c.clone()], false),
        (&[a.clone(), b.clone()], &[a.clone()], false),
        (&[a.clone(), b.clone()], &[a.clone(), a.clone()], false),
        (&[a.clone(), a.clone()], &[a.clone()], false),
    ];
    for (left, right, pass) in cases {
        let (l, rt) = dirs();
        write(l.path(), "x.parquet", left, 1);
        write(rt.path(), "x.parquet", right, 1);
        let out = reconcile(l.path(), rt.path(), latest(ReconcileSpec::new(["id"]))).unwrap();
        assert_eq!(out.pass, pass, "{left:?} vs {right:?}");
    }
}

fn dec(ids: &[i64], unscaled: &[i128], scale: i8) -> RecordBatch {
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef),
        (
            "amount",
            Arc::new(
                Decimal128Array::from(unscaled.to_vec())
                    .with_precision_and_scale(18, scale)
                    .unwrap(),
            ),
        ),
    ])
    .unwrap()
}

/// Reconciles with both sides read at the dataset schema `(id, amount
/// Decimal128(18, 2))`, declared: inferred, a folder takes its widest
/// decimal (M5), so a scale-2 dataset over a scale-3 file is declared.
fn reconcile_at_scale_2(
    left: &std::path::Path,
    right: &std::path::Path,
    spec: ReconcileSpec,
) -> bellwether_verify::Result<bellwether_verify::Reconciliation> {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, true),
        arrow::datatypes::Field::new(
            "amount",
            arrow::datatypes::DataType::Decimal128(18, 2),
            true,
        ),
    ]));
    let ls = ParquetSource::new().with_schema(Arc::clone(&schema));
    let rs = ParquetSource::new().with_schema(schema);
    let (lr, rr) = (root(left), root(right));
    Reconciler::new(spec, Options::default())?.run(&Side::new(&ls, &lr), &Side::new(&rs, &rr))
}

#[test]
fn guard_a_difference_beyond_the_dataset_scale_is_an_error_and_a_rescaled_equal_passes() {
    // Dataset scale 2 (declared). b.parquet holds 1.234 on the left and
    // 1.235 on the right at scale 3: both differ only beyond scale 2.
    let (l, rt) = dirs();
    write_batch(l.path(), "a.parquet", &dec(&[1], &[100], 2), 4);
    write_batch(l.path(), "b.parquet", &dec(&[2], &[1234], 3), 4);
    write_batch(rt.path(), "a.parquet", &dec(&[1], &[100], 2), 4);
    write_batch(rt.path(), "b.parquet", &dec(&[2], &[1235], 3), 4);
    for spec in [
        ReconcileSpec::new(["id"]),
        ReconcileSpec::new(["id"]).totals(["amount"]),
        ReconcileSpec::new(["id"])
            .ignore(["amount"])
            .totals(["amount"]),
    ] {
        let err = reconcile_at_scale_2(l.path(), rt.path(), spec).unwrap_err();
        assert!(matches!(err, VerifyError::Unrepresentable { .. }), "{err}");
    }
    // Inferred, both sides take scale 3 (the widest, M5): the difference
    // is visible, a mismatch, never an error or a rounding pass.
    let out = reconcile(l.path(), rt.path(), ReconcileSpec::new(["id"])).unwrap();
    assert!(!out.pass);
    assert_eq!(out.counts.mismatched, 1, "{out:#?}");
    // 1.230 at scale 3 equals 1.23 at scale 2: equal after rescaling.
    let (l2, r2) = dirs();
    write_batch(l2.path(), "a.parquet", &dec(&[1], &[100], 2), 4);
    write_batch(l2.path(), "b.parquet", &dec(&[2], &[1230], 3), 4);
    write_batch(r2.path(), "a.parquet", &dec(&[1, 2], &[100, 123], 2), 4);
    let out = reconcile_at_scale_2(
        l2.path(),
        r2.path(),
        ReconcileSpec::new(["id"]).totals(["amount"]),
    )
    .unwrap();
    assert!(out.pass, "{out:#?}");
    // And 1.231 at scale 3 against 1.23: an error, never a rounding pass.
    let (l3, r3) = dirs();
    write_batch(l3.path(), "a.parquet", &dec(&[1], &[100], 2), 4);
    write_batch(l3.path(), "b.parquet", &dec(&[2], &[1231], 3), 4);
    write_batch(r3.path(), "a.parquet", &dec(&[1, 2], &[100, 123], 2), 4);
    assert!(matches!(
        reconcile_at_scale_2(l3.path(), r3.path(), ReconcileSpec::new(["id"])),
        Err(VerifyError::Unrepresentable { .. })
    ));
}

#[test]
fn guard_a_column_on_one_side_only_is_an_error_for_every_role() {
    let (l, rt) = dirs();
    let two = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        ("x", Arc::new(StringArray::from(vec!["a"])) as ArrayRef),
    ])
    .unwrap();
    let one = RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
        .unwrap();
    write_batch(l.path(), "x.parquet", &two, 4);
    write_batch(rt.path(), "x.parquet", &one, 4);
    for spec in [
        ReconcileSpec::new(["id"]),
        ReconcileSpec::new(["id", "x"]),
        ReconcileSpec::new(["id"])
            .ignore(["x"])
            .group_by(["x"])
            .totals(Vec::<String>::new()),
    ] {
        let res = reconcile(l.path(), rt.path(), spec.clone());
        assert!(res.is_err(), "{spec:?}: {res:?}");
    }
}

#[test]
fn guard_an_empty_right_side_never_passes() {
    // (a) A right root with no files and no schema: an error.
    let (l, rt) = dirs();
    write(l.path(), "x.parquet", &[r(1, 1, 1, "a")], 4);
    assert!(reconcile(l.path(), rt.path(), ReconcileSpec::new(["id"])).is_err());
    // (b) A right root with no files and a supplied schema: every key missing.
    let ls = ParquetSource::new();
    let rs = ParquetSource::new().with_schema(schema_at(2));
    let (lr, rr) = (root(l.path()), root(rt.path()));
    let out = Reconciler::new(ReconcileSpec::new(["id"]), Options::default())
        .unwrap()
        .run(&Side::new(&ls, &lr), &Side::new(&rs, &rr))
        .unwrap();
    assert!(!out.pass);
    assert_eq!(out.counts.missing_right, 1);
    // (c) A right file with zero rows.
    write(rt.path(), "x.parquet", &[] as &[R], 4);
    let out = reconcile(l.path(), rt.path(), ReconcileSpec::new(["id"])).unwrap();
    assert!(!out.pass);
    assert_eq!(out.counts.missing_right, 1);
    let out = reconcile(l.path(), rt.path(), latest(ReconcileSpec::new(["id"]))).unwrap();
    assert!(!out.pass);
}

// ---------------------------------------------------------------------------
// Focus 7: refusals, each typed, with nothing written
// ---------------------------------------------------------------------------

fn ledger_records(ledger: &Ledger<Arc<ObjectStoreBackend>>) -> usize {
    [
        RecordKind::Evidence,
        RecordKind::Proposal,
        RecordKind::Replay,
        RecordKind::Certificate,
        RecordKind::Receipt,
    ]
    .into_iter()
    .map(|k| ledger.list(k).unwrap().count())
    .sum()
}

#[test]
fn guard_refusals_are_typed_and_nothing_is_written() {
    let store = Arc::new(ObjectStoreBackend::memory().unwrap());
    let ledger = Ledger::new(Arc::clone(&store));
    let certify = |l: &std::path::Path, rt: &std::path::Path, spec: ReconcileSpec| {
        let (ls, rs) = (ParquetSource::new(), ParquetSource::new());
        let (lr, rr) = (root(l), root(rt));
        let res = Reconciler::new(spec, Options::default())
            .and_then(|rec| rec.certify(&Side::new(&ls, &lr), &Side::new(&rs, &rr), &anchor()));
        if let Ok(c) = &res {
            record(&ledger, c).unwrap();
        }
        res
    };
    // A workload key containing a null.
    let (l, rt) = dirs();
    write(l.path(), "x.parquet", &[r(1, 1, 1, "a")], 4);
    write(rt.path(), "x.parquet", &[r(1, 1, 1, "a")], 4);
    let null_key = vec![0x00];
    assert!(matches!(
        certify(
            l.path(),
            rt.path(),
            ReconcileSpec::new(["id"]).coverage(workload(vec![null_key]))
        ),
        Err(VerifyError::InvalidSpec(_))
    ));
    let composite_with_null = [int_key(1), vec![0x00]].concat();
    assert!(matches!(
        certify(
            l.path(),
            rt.path(),
            ReconcileSpec::new(["id", "note"]).coverage(workload(vec![composite_with_null]))
        ),
        Err(VerifyError::InvalidSpec(_))
    ));
    // A type that cannot be cast: Int64 against Utf8 under one name.
    let (l2, r2) = dirs();
    write_batch(
        l2.path(),
        "x.parquet",
        &RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
            .unwrap(),
        4,
    );
    write_batch(
        r2.path(),
        "x.parquet",
        &RecordBatch::try_from_iter([("id", Arc::new(StringArray::from(vec!["1"])) as ArrayRef)])
            .unwrap(),
        4,
    );
    assert!(matches!(
        certify(l2.path(), r2.path(), ReconcileSpec::new(["id"])),
        Err(VerifyError::Schema(_))
    ));
    // Overflow in a total.
    let (l3, r3) = dirs();
    let max = 10i128.pow(38) - 1;
    let b = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
        (
            "amount",
            Arc::new(
                Decimal128Array::from(vec![max, max])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
        ),
    ])
    .unwrap();
    write_batch(l3.path(), "x.parquet", &b, 4);
    write_batch(r3.path(), "x.parquet", &b, 4);
    assert!(matches!(
        certify(
            l3.path(),
            r3.path(),
            ReconcileSpec::new(["id"]).totals(["amount"])
        ),
        Err(VerifyError::Overflow { .. })
    ));
    // Two sides whose inferred decimals differ in scale (the left's widest
    // is scale 3, the right's scale 2; M5) share no canonical encoding: a
    // schema refusal saying to read both under one dataset schema (the
    // facade adds the command-line step, `--schema <file>`).
    let (l4, r4) = dirs();
    write_batch(l4.path(), "a.parquet", &dec(&[1], &[100], 2), 4);
    write_batch(l4.path(), "b.parquet", &dec(&[2], &[1235], 3), 4);
    write_batch(r4.path(), "a.parquet", &dec(&[1], &[100], 2), 4);
    assert!(matches!(
        certify(l4.path(), r4.path(), ReconcileSpec::new(["id"])),
        Err(VerifyError::Schema(m)) if m.contains("one dataset schema")
    ));
    // StaticProof.
    assert!(matches!(
        certify(
            l.path(),
            rt.path(),
            ReconcileSpec::new(["id"]).coverage(CoverageRequest::StaticProof {
                identity: "x".into()
            })
        ),
        Err(VerifyError::StaticProofUnsupported { .. })
    ));
    assert_eq!(ledger_records(&ledger), 0, "a refusal wrote a record");
}

#[test]
fn guard_a_null_workload_key_is_refused_before_any_read() {
    let (l, rt) = dirs();
    write(l.path(), "x.parquet", &[r(1, 1, 1, "a")], 4);
    write(rt.path(), "x.parquet", &[r(1, 1, 1, "a")], 4);
    let (ls, rs) = (
        Spy::new(ParquetSource::new()),
        Spy::new(ParquetSource::new()),
    );
    let (lr, rr) = (root(l.path()), root(rt.path()));
    let err = Reconciler::new(
        ReconcileSpec::new(["id"]).coverage(workload(vec![vec![0x00]])),
        Options::default(),
    )
    .unwrap()
    .run(&Side::new(&ls, &lr), &Side::new(&rs, &rr))
    .unwrap_err();
    assert!(matches!(err, VerifyError::InvalidSpec(_)), "{err}");
    assert_eq!(ls.reads() + rs.reads(), 0);
}

// ---------------------------------------------------------------------------
// Findings: each asserts what the review requires; expected to FAIL on head
// ---------------------------------------------------------------------------

/// Settled: a key whose every version has a null timestamp counts in
/// `uncomparable` on the certificate (M5, ADR 028; contract-v3 row 26), not
/// in `mismatched`, never a pass:false certificate with every count zero.
#[test]
fn finding_a_key_with_no_comparable_version_counts_as_uncomparable() {
    let (l, rt) = dirs();
    let mut a = r(1, 0, 1, "a");
    a.ts = None;
    write(l.path(), "x.parquet", &[a.clone(), r(2, 5, 1, "a")], 4);
    write(rt.path(), "x.parquet", &[a, r(2, 5, 1, "a")], 4);
    let (ls, rs) = (ParquetSource::new(), ParquetSource::new());
    let (lr, rr) = (root(l.path()), root(rt.path()));
    let c = Reconciler::new(latest(ReconcileSpec::new(["id"])), Options::default())
        .unwrap()
        .certify(&Side::new(&ls, &lr), &Side::new(&rs, &rr), &anchor())
        .unwrap()
        .certificate;
    assert!(!c.pass);
    assert_eq!(
        (c.body.mismatched, c.body.uncomparable),
        (0, 1),
        "pass=false with body {:?}: a reader sees no difference",
        c.body
    );
}

/// The same when the null-version key is absent on the other side.
#[test]
fn finding_a_null_version_key_present_on_one_side_only_is_counted() {
    let (l, rt) = dirs();
    let mut a = r(1, 0, 1, "a");
    a.ts = None;
    write(l.path(), "x.parquet", &[a, r(2, 5, 1, "a")], 4);
    write(rt.path(), "x.parquet", &[r(2, 5, 1, "a")], 4);
    let (ls, rs) = (ParquetSource::new(), ParquetSource::new());
    let (lr, rr) = (root(l.path()), root(rt.path()));
    let c = Reconciler::new(latest(ReconcileSpec::new(["id"])), Options::default())
        .unwrap()
        .certify(&Side::new(&ls, &lr), &Side::new(&rs, &rr), &anchor())
        .unwrap()
        .certificate;
    assert!(!c.pass);
    let b = &c.body;
    assert!(
        b.missing_left + b.missing_right + b.mismatched + b.uncomparable >= 1,
        "pass=false with all-zero body {b:?}"
    );
}

/// Settled: two unindexed sides record manifest version "0" (with both
/// catalog digests) instead of refusing (contract-v3 rows 29 and 30).
#[test]
fn finding_two_unindexed_sides_certify_at_manifest_zero() {
    let (l, rt) = dirs();
    write(l.path(), "x.parquet", &[r(1, 1, 1, "a")], 4);
    write(rt.path(), "x.parquet", &[r(1, 1, 1, "a")], 4);
    let (ls, rs) = (ParquetSource::new(), ParquetSource::new());
    let (lr, rr) = (root(l.path()), root(rt.path()));
    let res = Reconciler::new(ReconcileSpec::new(["id"]), Options::default())
        .unwrap()
        .certify(
            &Side::new(&ls, &lr),
            &Side::new(&rs, &rr),
            &CertificateContext::default(),
        );
    let c = res.unwrap().certificate;
    assert_eq!(c.provenance.manifest_version.as_deref(), Some("0"));
    assert!(c.provenance.inputs.len() >= 2);
}

/// Null key elements collapse into one key, and `latest_by` then keeps only
/// the newest of rows that are different entities: a row missing on the
/// right passes. The review requires a typed error under `latest_by`.
#[test]
fn finding_null_keys_under_latest_by_hide_a_missing_row() {
    let (l, rt) = dirs();
    let x = R {
        id: None,
        ..r(0, 5, 100, "x")
    };
    let y = R {
        id: None,
        ..r(0, 3, 999, "y")
    };
    write(l.path(), "x.parquet", &[x.clone(), y], 4);
    write(rt.path(), "x.parquet", &[x], 4);
    let res = reconcile(
        l.path(),
        rt.path(),
        latest(ReconcileSpec::new(["id"])).totals(["amount"]),
    );
    match res {
        Err(_) => {}
        Ok(out) => panic!(
            "row y (null key, ts 3, 9.99) is missing on the right, yet pass = {}, counts {:?}, \
             totals {:?}",
            out.pass,
            out.counts,
            out.totals
                .iter()
                .map(|t| (t.left.render(), t.right.render()))
                .collect::<Vec<_>>()
        ),
    }
}

/// A workload whose keys are found on neither side compares nothing and
/// passes with `Workload { keys: N }`. Ledger rule 12 checks only `pass`, so
/// this certificate can back a Receipt.
#[test]
fn finding_a_workload_found_on_neither_side_passes_vacuously() {
    let (l, rt) = dirs();
    // The two sides differ on every key.
    write(
        l.path(),
        "x.parquet",
        &[r(1, 1, 1, "a"), r(2, 1, 1, "a")],
        4,
    );
    write(
        rt.path(),
        "x.parquet",
        &[r(1, 1, 2, "b"), r(2, 1, 2, "b")],
        4,
    );
    let spec = ReconcileSpec::new(["id"]).coverage(workload(vec![int_key(1_000), int_key(2_000)]));
    match reconcile(l.path(), rt.path(), spec) {
        Err(_) => {}
        Ok(out) => panic!(
            "nothing was compared, yet pass = {} with coverage {:?}, counts {:?}",
            out.pass, out.coverage, out.counts
        ),
    }
}

/// The realistic form: a decimal key encoded at another scale passes
/// `check_probe` (the scale is not in the bytes), matches nothing, and the
/// reconcile passes although the keyed row differs.
#[test]
fn finding_a_workload_key_at_another_decimal_scale_passes_vacuously() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Decimal128(10, 2), false),
        Field::new("v", DataType::Utf8, false),
    ]));
    let batch = |v: &str| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(
                    Decimal128Array::from(vec![150])
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ) as ArrayRef,
                Arc::new(StringArray::from(vec![v])) as ArrayRef,
            ],
        )
        .unwrap()
    };
    let (l, rt) = dirs();
    write_batch(l.path(), "x.parquet", &batch("left"), 4);
    write_batch(rt.path(), "x.parquet", &batch("right"), 4);
    // The caller means 1.50 but encodes 1500 (scale 3): 15.00 at scale 2.
    let mut key = vec![0x05, 0x10];
    let mut raw = 1500i128.to_be_bytes();
    raw[0] ^= 0x80;
    key.extend_from_slice(&raw);
    let spec = ReconcileSpec::new(["k"]).coverage(workload(vec![key]));
    match reconcile(l.path(), rt.path(), spec) {
        Err(_) => {}
        Ok(out) => panic!(
            "key 1.50 differs but pass = {} with coverage {:?}, absent_both {}",
            out.pass, out.coverage, out.counts.absent_both
        ),
    }
}

/// A sample that selects no key of a non-empty key space compares nothing
/// and passes.
#[test]
fn finding_a_sample_of_zero_keys_from_nonempty_data_passes_vacuously() {
    let (l, rt) = dirs();
    let rows: Vec<R> = (0..50).map(|i| r(i, 1, 1, "a")).collect();
    let changed: Vec<R> = (0..50).map(|i| r(i, 1, 2, "b")).collect();
    write(l.path(), "x.parquet", &rows, 16);
    write(rt.path(), "x.parquet", &changed, 16);
    let spec = ReconcileSpec::new(["id"]).coverage(CoverageRequest::Sampled {
        fraction: 1e-12,
        seed: 1,
    });
    match reconcile(l.path(), rt.path(), spec) {
        Err(_) => {}
        Ok(out) => {
            assert!(
                !out.pass,
                "every key differs, yet pass = true with coverage {:?}",
                out.coverage
            );
        }
    }
}

/// A planned side plus a proposal hash that does not resolve: the
/// certificate is refused by the ledger, but its plan Evidence is already
/// written. Refusals must leave nothing behind.
#[test]
fn finding_a_refused_certificate_leaves_no_orphan_plan_evidence() {
    let (data, other) = dirs();
    let rows: Vec<R> = (0..20).map(|i| r(i, 1, 1, "a")).collect();
    write(data.path(), "x.parquet", &rows, 5);
    write(other.path(), "x.parquet", &rows, 5);
    let store = Arc::new(ObjectStoreBackend::memory().unwrap());
    let canonical = std::fs::canonicalize(data.path())
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    Builder::new(Arc::clone(&store), "idx")
        .build(
            &canonical,
            &[KeySpec::Exact {
                columns: vec!["id".into()],
                counts: false,
                bounds: None,
            }],
            &BuildOptions::new(SourceKind::Parquet),
        )
        .unwrap();
    let planner = IndexPlanner::open(&store, "idx").unwrap();
    let left_root = planner.source_root().to_string();
    let left = ParquetSource::new().with_schema(Arc::clone(planner.schema()));
    let right = ParquetSource::new();
    let right_root = root(other.path());
    let certified = Reconciler::new(
        ReconcileSpec::new(["id"]).coverage(workload(vec![int_key(3)])),
        Options::default(),
    )
    .unwrap()
    .certify(
        &Side::new(&left, &left_root).indexed(&planner),
        &Side::new(&right, &right_root),
        &CertificateContext {
            proposal: Some(Hash::of(b"no such proposal")),
            ..CertificateContext::default()
        },
    )
    .unwrap();
    let ledger_store = Arc::new(ObjectStoreBackend::memory().unwrap());
    let ledger = Ledger::new(Arc::clone(&ledger_store));
    assert!(record(&ledger, &certified).is_err());
    assert_eq!(
        ledger_records(&ledger),
        0,
        "the refused certificate's plan Evidence was written anyway"
    );
}

/// Both sides empty (folders with a supplied schema): the reconcile passes
/// having compared no key under Full coverage. Reported, not required.
#[test]
fn note_two_empty_sides_pass_under_full_coverage() {
    let (l, rt) = dirs();
    let ls = ParquetSource::new().with_schema(schema_at(2));
    let rs = ParquetSource::new().with_schema(schema_at(2));
    let (lr, rr) = (root(l.path()), root(rt.path()));
    let out = Reconciler::new(ReconcileSpec::new(["id"]), Options::default())
        .unwrap()
        .run(&Side::new(&ls, &lr), &Side::new(&rs, &rr))
        .unwrap();
    eprintln!(
        "two empty sides: pass = {}, coverage = {:?}, keys_compared = {}",
        out.pass, out.coverage, out.counts.keys_compared
    );
    let s = ReconcileSpec::new(["id"]).coverage(CoverageRequest::Sampled {
        fraction: 0.5,
        seed: 1,
    });
    let out = Reconciler::new(s, Options::default())
        .unwrap()
        .run(&Side::new(&ls, &lr), &Side::new(&rs, &rr))
        .unwrap();
    eprintln!(
        "two empty sides sampled: pass = {}, coverage = {:?}",
        out.pass, out.coverage
    );
    let _ = Coverage::Full;
}
