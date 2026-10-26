//! The Certificate record: every field, provenance (invariant 6), and the
//! ledger as the only write path.

#![allow(clippy::unwrap_used, missing_docs)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::DataType;
use bellwether_core::encoding::{encode_tuple_as, Value};
use bellwether_ledger::{rfc3339::is_rfc3339, Ledger, RecordKind};
use bellwether_source::ParquetSource;
use bellwether_store::ObjectStoreBackend;
use bellwether_types::{format, Coverage, Hash, Record, Source};
use bellwether_verify::{
    record, AgedInput, CertificateContext, CoverageRequest, ManifestAnchor, Options, ReconcileSpec,
    Reconciler, Side, VerifyError, WorkloadKeys,
};
use common::{dirs, ledger, root, write};

fn anchor(sequence: u64, age: Duration) -> ManifestAnchor {
    ManifestAnchor {
        sequence,
        hash: Hash::of(b"manifest"),
        age,
    }
}

/// The schema identity with core's recorded type spelling.
fn identity(schema: &arrow::datatypes::Schema) -> Vec<bellwether_source::catalog::SchemaColumn> {
    bellwether_source::catalog::schema_identity(schema, |t| {
        bellwether_core::segment::ColumnType::of(t)
            .ok()
            .and_then(|c| serde_json::to_string(&c).ok())
    })
}

/// An unindexed folder's catalog digest (docs/format.md), computed here from
/// the folder itself: its schema and every row group's report.
fn catalog(dir: &std::path::Path) -> Hash {
    let source = ParquetSource::new();
    let files: Vec<_> = source
        .discover(&root(dir))
        .unwrap()
        .into_iter()
        .map(|f| {
            let rgs = source.row_groups(&f).unwrap();
            (f, rgs)
        })
        .collect();
    let schema = source.schema(&root(dir)).unwrap();
    bellwether_source::catalog::unindexed(&identity(&schema), &files).unwrap()
}

/// Under a fixed clock the certificate's `created_at` is the clock, no run
/// time is added to any input's age, and two runs over the same inputs make
/// byte-identical certificates (the facade's `--clock`, ADR 010).
#[test]
fn a_fixed_clock_makes_certificates_byte_identical() {
    let (l, r) = dirs();
    let rows = ledger(20, 2);
    write(l.path(), "x.parquet", &rows, 9);
    write(r.path(), "x.parquet", &rows, 13);
    let (ls, rs) = (ParquetSource::new(), ParquetSource::new());
    let (lr, rr) = (root(l.path()), root(r.path()));
    let clock = "2026-10-24T12:00:00.000Z";
    let ctx = CertificateContext {
        manifest: Some(anchor(7, Duration::from_secs(5))),
        inputs: vec![AgedInput {
            hash: Hash::of(b"extra"),
            age: Duration::from_millis(1),
        }],
        clock: Some(clock.to_string()),
        ..CertificateContext::default()
    };
    let run = || {
        Reconciler::new(ReconcileSpec::new(["id"]), Options::default())
            .unwrap()
            .certify(&Side::new(&ls, &lr), &Side::new(&rs, &rr), &ctx)
            .unwrap()
            .certificate
    };
    let a = run();
    std::thread::sleep(Duration::from_millis(20));
    let b = run();
    assert_eq!(a.provenance.created_at, clock);
    assert_eq!(a.provenance.oldest_input_age, Duration::from_secs(5));
    assert_eq!(
        serde_json::to_vec(&a).unwrap(),
        serde_json::to_vec(&b).unwrap()
    );
}

#[test]
fn a_passing_certificate_fills_every_field_and_reaches_the_ledger() {
    let (l, r) = dirs();
    let rows = ledger(20, 2);
    write(l.path(), "x.parquet", &rows, 9);
    write(r.path(), "x.parquet", &rows, 13);
    let (ls, rs) = (ParquetSource::new(), ParquetSource::new());
    let (lr, rr) = (root(l.path()), root(r.path()));
    let spec = ReconcileSpec::new(["id"])
        .latest_by("ts")
        .ignore(["note"])
        .totals(["amount"]);
    let ctx = CertificateContext {
        proposal: None,
        manifest: Some(anchor(7, Duration::from_secs(5))),
        inputs: vec![AgedInput {
            hash: Hash::of(b"extra"),
            age: Duration::from_millis(1),
        }],
        clock: None,
        agg: Vec::new(),
    };
    let certified = Reconciler::new(spec, Options::default())
        .unwrap()
        .certify(&Side::new(&ls, &lr), &Side::new(&rs, &rr), &ctx)
        .unwrap();
    let c = &certified.certificate;
    assert_eq!(c.record_version, format::RECORD_VERSION);
    assert_eq!(c.proposal, None);
    assert_eq!(c.coverage, Coverage::Full);
    assert!(c.pass);
    assert_eq!(
        (c.body.missing_left, c.body.missing_right, c.body.mismatched),
        (0, 0, 0)
    );
    assert_eq!(c.body.ignored_columns, ["note"]);
    assert_eq!(c.body.totals_delta.len(), 0);
    assert_eq!(
        c.provenance.inputs,
        [
            catalog(l.path()),
            catalog(r.path()),
            Hash::of(b"manifest"),
            Hash::of(b"extra")
        ]
    );
    assert_eq!(c.provenance.manifest_version.as_deref(), Some("7"));
    assert!(
        is_rfc3339(&c.provenance.created_at),
        "{}",
        c.provenance.created_at
    );
    // The oldest input is the anchor: 5 s old when handed over, plus the run.
    assert!(c.provenance.oldest_input_age > Duration::from_secs(5));
    assert!(c.provenance.oldest_input_age < Duration::from_secs(5 * 60));
    assert_eq!(certified.evidence.len(), 0);

    let store = Arc::new(ObjectStoreBackend::memory().unwrap());
    let ledger = Ledger::new(Arc::clone(&store));
    let hash = record(&ledger, &certified).unwrap();
    assert_eq!(ledger.kind(&hash).unwrap(), RecordKind::Certificate);
    assert_eq!(
        ledger.get(&hash).unwrap(),
        Record::Certificate(certified.certificate.clone())
    );
    // Content addressing makes the write idempotent.
    assert_eq!(record(&ledger, &certified).unwrap(), hash);
}

#[test]
fn a_failing_certificate_records_counts_and_totals_and_is_stored() {
    let (l, r) = dirs();
    let rows = ledger(10, 1);
    let mut changed = rows.clone();
    changed.remove(3);
    changed[0].amount = Some(1);
    write(l.path(), "x.parquet", &rows, 4);
    write(r.path(), "x.parquet", &changed, 4);
    let (ls, rs) = (ParquetSource::new(), ParquetSource::new());
    let (lr, rr) = (root(l.path()), root(r.path()));
    let ctx = CertificateContext {
        manifest: Some(anchor(3, Duration::ZERO)),
        ..CertificateContext::default()
    };
    let certified = Reconciler::new(
        ReconcileSpec::new(["id"])
            .totals(["amount"])
            .group_by(["status"]),
        Options::default(),
    )
    .unwrap()
    .certify(&Side::new(&ls, &lr), &Side::new(&rs, &rr), &ctx)
    .unwrap();
    let c = &certified.certificate;
    assert!(!c.pass);
    assert_eq!(
        (c.body.missing_left, c.body.missing_right, c.body.mismatched),
        (0, 1, 1)
    );
    // id 0 (status a) lost its 0.00 and gained 0.01; id 3 (status a) and its
    // 3.00 are gone: group a goes from 18.00 to 15.01.
    assert_eq!(
        c.body.totals_delta,
        [(
            r#"{"column":"amount","group":[["status","a"]]}"#.to_string(),
            "18.00".to_string(),
            "15.01".to_string()
        )]
    );
    let store = Arc::new(ObjectStoreBackend::memory().unwrap());
    let ledger = Ledger::new(Arc::clone(&store));
    let hash = record(&ledger, &certified).unwrap();
    assert_eq!(ledger.get(&hash).unwrap(), Record::Certificate(c.clone()));
}

#[test]
fn a_key_that_failed_to_compare_fails_the_certificate_and_counts_as_mismatched() {
    let (l, r) = dirs();
    let mut a = common::r(1, 0, 1, "a");
    a.ts = None;
    write(l.path(), "x.parquet", &[a.clone()], 4);
    write(r.path(), "x.parquet", &[a], 4);
    let (ls, rs) = (ParquetSource::new(), ParquetSource::new());
    let (lr, rr) = (root(l.path()), root(r.path()));
    let ctx = CertificateContext {
        manifest: Some(anchor(1, Duration::ZERO)),
        ..CertificateContext::default()
    };
    let certified = Reconciler::new(
        ReconcileSpec::new(["id"]).latest_by("ts"),
        Options::default(),
    )
    .unwrap()
    .certify(&Side::new(&ls, &lr), &Side::new(&rs, &rr), &ctx)
    .unwrap();
    assert!(!certified.certificate.pass);
    assert_eq!(certified.outcome.counts.no_comparable_version, 1);
    assert_eq!(certified.outcome.counts.mismatched, 1);
    // Counted in `mismatched` on the record (contract-v3 #26).
    let b = &certified.certificate.body;
    assert_eq!((b.missing_left, b.missing_right, b.mismatched), (0, 0, 1));
    // The ledger stores a failing certificate (rule 9 binds only a pass).
    let ledger = Ledger::new(Arc::new(ObjectStoreBackend::memory().unwrap()));
    record(&ledger, &certified).unwrap();
}

#[test]
fn an_unindexed_reconcile_is_anchored_at_manifest_zero_with_both_catalog_digests() {
    // Neither side indexed and no anchor: manifest "0", the sequence of a
    // manifest never committed, and each side's catalog digest as an input,
    // as the replay crate does (contract-v3 #29).
    let (l, r) = dirs();
    let rows = ledger(12, 2);
    write(l.path(), "a.parquet", &rows[..10], 4);
    write(l.path(), "b.parquet", &rows[10..], 3);
    write(r.path(), "x.parquet", &rows, 5);
    let (ls, rs) = (ParquetSource::new(), ParquetSource::new());
    let (lr, rr) = (root(l.path()), root(r.path()));
    let certified = Reconciler::new(ReconcileSpec::new(["id"]), Options::default())
        .unwrap()
        .certify(
            &Side::new(&ls, &lr),
            &Side::new(&rs, &rr),
            &CertificateContext::default(),
        )
        .unwrap();
    let c = &certified.certificate;
    assert!(c.pass);
    assert_eq!(c.provenance.manifest_version.as_deref(), Some("0"));
    assert_eq!(c.provenance.inputs, [catalog(l.path()), catalog(r.path())]);
    assert_eq!(certified.outcome.sides[0].catalog, catalog(l.path()));
    let ledger = Ledger::new(Arc::new(ObjectStoreBackend::memory().unwrap()));
    let hash = record(&ledger, &certified).unwrap();
    assert_eq!(ledger.get(&hash).unwrap(), Record::Certificate(c.clone()));
    // A changed row-group layout of the same rows is another catalog.
    let (l2, _r2) = dirs();
    write(l2.path(), "a.parquet", &rows, 7);
    assert_ne!(catalog(l2.path()), catalog(l.path()));
}

#[test]
fn the_ledger_refuses_a_certificate_for_an_unknown_proposal() {
    let (l, r) = dirs();
    write(l.path(), "x.parquet", &ledger(5, 1), 4);
    write(r.path(), "x.parquet", &ledger(5, 1), 4);
    let (ls, rs) = (ParquetSource::new(), ParquetSource::new());
    let (lr, rr) = (root(l.path()), root(r.path()));
    let ctx = CertificateContext {
        proposal: Some(Hash::of(b"no such proposal")),
        manifest: Some(anchor(1, Duration::ZERO)),
        inputs: vec![],
        clock: None,
        agg: Vec::new(),
    };
    let certified = Reconciler::new(ReconcileSpec::new(["id"]), Options::default())
        .unwrap()
        .certify(&Side::new(&ls, &lr), &Side::new(&rs, &rr), &ctx)
        .unwrap();
    let ledger = Ledger::new(Arc::new(ObjectStoreBackend::memory().unwrap()));
    let err = record(&ledger, &certified).unwrap_err();
    assert!(
        matches!(
            err,
            VerifyError::Engine(bellwether_types::Error::LedgerRule(_))
        ),
        "{err}"
    );
}

#[test]
fn a_workload_certificate_cites_the_workload_and_its_age() {
    let (l, r) = dirs();
    write(l.path(), "x.parquet", &ledger(10, 1), 4);
    write(r.path(), "x.parquet", &ledger(10, 1), 4);
    let (ls, rs) = (ParquetSource::new(), ParquetSource::new());
    let (lr, rr) = (root(l.path()), root(r.path()));
    let workload = Hash::of(b"workload");
    let spec = ReconcileSpec::new(["id"]).coverage(CoverageRequest::Workload(WorkloadKeys {
        workload,
        age: Duration::from_secs(3_600),
        keys: vec![encode_tuple_as(&[Value::Int(2)], &[DataType::Int64]).unwrap()],
    }));
    let ctx = CertificateContext {
        manifest: Some(anchor(4, Duration::from_secs(10))),
        ..CertificateContext::default()
    };
    let c = Reconciler::new(spec, Options::default())
        .unwrap()
        .certify(&Side::new(&ls, &lr), &Side::new(&rs, &rr), &ctx)
        .unwrap()
        .certificate;
    assert_eq!(c.coverage, Coverage::Workload { workload, keys: 1 });
    assert!(c.provenance.inputs.contains(&workload));
    assert!(c.provenance.oldest_input_age >= Duration::from_secs(3_600));
    assert!(c.pass);
}
