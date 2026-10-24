//! The PR #23 review's probes, kept as regression tests: data that drifts
//! between verify and apply, a stale index at `fix`, the reconcile anchor,
//! rewrite and layout refusals, a selective key, and signed zero in verify
//! coverage.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

mod common;

use std::path::Path;
use std::sync::Arc;

use bellwether::types::{Hash, ProposalBody, Record, RewriteShape, Source};
use bellwether::{
    spec, Engine, EngineOptions, FixOutcome, FixRequest, KeyFile, ProposeResult, ReconcileRequest,
    Refusal, RewriteOutputs, WhyRequest,
};
use common::{ledger_corpus, write_file, write_keys};

fn open(root: &Path) -> Engine {
    Engine::open(root.to_str().unwrap(), EngineOptions::default()).unwrap()
}

fn receipts(index: &Path) -> usize {
    let Ok(rd) = std::fs::read_dir(index.join("ledger")) else {
        return 0;
    };
    rd.filter(|e| {
        std::fs::read(e.as_ref().unwrap().path())
            .unwrap()
            .starts_with(br#"{"record":"receipt""#)
    })
    .count()
}

fn setup(data: &Path, keyfile: &Path) -> (Engine, Hash, Hash) {
    let e = open(data);
    let w = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(keyfile)),
            ..WhyRequest::default()
        })
        .unwrap();
    (
        e,
        w.workload.as_ref().unwrap().hash,
        w.candidates[0].proposal.hash,
    )
}

/// Data changes between `fix --verify-only` and `fix --certificate`, with
/// no manifest commit in between: the gate recomputes the data root's
/// catalog digest and refuses, unindexed and indexed. On an indexed folder
/// a fresh `fix` is refused up front with the `bellwether index` message.
#[test]
fn probe_data_drift_between_verify_and_apply() {
    for indexed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        ledger_corpus(&data, 3, 4, 25, 400);
        let kf = dir.path().join("k.parquet");
        write_keys(&kf, "id", &[5, 77, 123]);
        if indexed {
            open(&data)
                .index(&[spec::parse("exact:batch").unwrap()])
                .unwrap();
        }
        let (e, wl, p) = setup(&data, &kf);
        let v = e
            .fix(&FixRequest {
                proposal: Some(p),
                workload: Some(wl),
                verify_only: true,
                ..FixRequest::default()
            })
            .unwrap();
        assert!(matches!(v.outcome, FixOutcome::Verified), "{:?}", v.outcome);
        let cert = v.certificate.as_ref().unwrap().hash;
        write_file(
            &data.join("part-999.parquet"),
            &[vec![
                (5, 999, Some(9_999_000_000), "late".into()),
                (77, 999, Some(9_999_000_001), "late".into()),
            ]],
        );
        let r = e
            .fix(&FixRequest {
                certificate: Some(cert),
                ..FixRequest::default()
            })
            .unwrap();
        assert!(
            matches!(
                r.outcome,
                FixOutcome::Refused {
                    refusal: Refusal::DataChanged { .. }
                }
            ),
            "[indexed={indexed}] {:?}",
            r.outcome
        );
        assert_eq!(
            receipts(&data.join(".bellwether")),
            0,
            "[indexed={indexed}]"
        );
        if indexed {
            let err = e
                .fix(&FixRequest {
                    proposal: Some(p),
                    workload: Some(wl),
                    ..FixRequest::default()
                })
                .unwrap_err()
                .to_string();
            assert!(err.contains("bellwether index"), "{err}");
        }
    }
}

#[test]
fn probe_reconcile_anchor_and_rewrite_layout() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data, 2, 2, 10, 50);
    let copy = dir.path().join("copy");
    ledger_corpus(&copy, 2, 2, 10, 50);
    let diff = dir.path().join("diff");
    ledger_corpus(&diff, 2, 2, 10, 50);
    write_file(
        &diff.join("part-001.parquet"),
        &[vec![(3, 0, Some(1), "changed".into())]],
    );
    let e = open(&data);
    let req = ReconcileRequest {
        key: vec!["id".into()],
        ..ReconcileRequest::default()
    };
    for (right, pass) in [(&copy, true), (&diff, false)] {
        let r = e.reconcile(right.to_str().unwrap(), &req).unwrap();
        let Record::Certificate(c) = &r.certificate.record else {
            panic!()
        };
        assert_eq!(c.pass, pass);
        assert_eq!(c.provenance.manifest_version.as_deref(), Some("0"));
        assert_eq!(c.proposal, None);
    }
    e.index(&[spec::parse("exact:id").unwrap()]).unwrap();
    let r = e.reconcile(copy.to_str().unwrap(), &req).unwrap();
    let Record::Certificate(c) = &r.certificate.record else {
        panic!()
    };
    assert_eq!(c.provenance.manifest_version.as_deref(), Some("1"));
    // A reconcile certificate verified no proposal: nothing to apply.
    let a = e.fix(&FixRequest {
        certificate: Some(r.certificate.hash),
        ..FixRequest::default()
    });
    assert!(a.unwrap_err().to_string().contains("nothing to apply"));

    let kf = dir.path().join("k.parquet");
    write_keys(&kf, "id", &[3]);
    let ev = e.locate(&KeyFile::new(&kf)).unwrap().evidence.hash;
    let rw = e
        .propose(
            ProposalBody::Rewrite {
                shape: RewriteShape {
                    before: "a".into(),
                    after: "b".into(),
                    invariant: "same rows by id".into(),
                },
            },
            &[ev],
            "agent",
        )
        .unwrap();
    let ProposeResult::Written { proposal: rw } = rw else {
        panic!()
    };
    let seq_before = e.index(&[]).unwrap().sequence;
    for (after, failing) in [(&copy, false), (&diff, true)] {
        let r = e
            .fix(&FixRequest {
                proposal: Some(rw.hash),
                rewrite: Some(RewriteOutputs {
                    before: data.to_str().unwrap().into(),
                    after: after.to_str().unwrap().into(),
                    key: vec!["id".into()],
                    ..RewriteOutputs::default()
                }),
                ..FixRequest::default()
            })
            .unwrap();
        assert_eq!(r.invariant.as_deref(), Some("same rows by id"));
        let refusal = match &r.outcome {
            FixOutcome::Refused { refusal } => refusal.clone(),
            other => panic!("{other:?}"),
        };
        if failing {
            assert!(matches!(refusal, Refusal::CertificateFailed { .. }));
        } else {
            assert!(matches!(refusal, Refusal::NoReplay { .. }));
        }
    }
    let ly = e
        .propose(
            ProposalBody::Layout {
                cluster_by: vec!["id".into()],
            },
            &[ev],
            "agent",
        )
        .unwrap();
    let ProposeResult::Written { proposal: ly } = ly else {
        panic!()
    };
    let r = e
        .fix(&FixRequest {
            proposal: Some(ly.hash),
            ..FixRequest::default()
        })
        .unwrap();
    assert!(matches!(
        r.outcome,
        FixOutcome::Refused {
            refusal: Refusal::LayoutNotApplicable
        }
    ));
    assert!(r.certificate.is_none());
    assert_eq!(e.index(&[]).unwrap().sequence, seq_before);
    assert_eq!(receipts(&data.join(".bellwether")), 0);
}

/// A selective key: files with no planned row group; fix still verifies,
/// applies, and attests.
#[test]
fn probe_selective_key_leaves_files_unplanned() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data, 3, 4, 25, 400);
    let kf = dir.path().join("k.parquet");
    write_keys(&kf, "id", &[5]);
    let (e, wl, p) = setup(&data, &kf);
    let r = e
        .fix(&FixRequest {
            proposal: Some(p),
            workload: Some(wl),
            ..FixRequest::default()
        })
        .unwrap();
    assert!(
        matches!(r.outcome, FixOutcome::Applied { .. }),
        "{:?}",
        r.outcome
    );
    let v = r.verification.unwrap();
    assert_eq!((v.sides[0].files, v.sides[1].files), (3, 1));
    assert_eq!(receipts(&data.join(".bellwether")), 1);
}

/// The review's signed-zero probe, with the workload keys built the way
/// `fix` builds them (both zero encodings per float element): a planned side
/// that omits the -0.0 row group fails.
#[test]
fn probe_workload_coverage_and_the_other_zero() {
    use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use bellwether_replay::RunKeysBuilder;
    use bellwether_source::ParquetSource;
    use bellwether_verify::{
        CoverageRequest, Options, ReconcileSpec, Reconciler, Side, WorkloadKeys,
    };
    use parquet::arrow::ArrowWriter;

    fn write(path: &Path, xs: &[f64], vs: &[i64]) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let b = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Float64Array::from(xs.to_vec())) as ArrayRef,
                Arc::new(Int64Array::from(vs.to_vec())),
            ],
        )
        .unwrap();
        let mut w =
            ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None).unwrap();
        w.write(&b).unwrap();
        w.close().unwrap();
    }

    let dir = tempfile::tempdir().unwrap();
    let full = dir.path().join("full");
    let planned = dir.path().join("planned");
    std::fs::create_dir_all(&full).unwrap();
    std::fs::create_dir_all(&planned).unwrap();
    write(&full.join("a.parquet"), &[0.0, 1.0], &[1, 2]);
    write(&full.join("b.parquet"), &[-0.0, 2.0], &[3, 4]);
    write(&planned.join("a.parquet"), &[0.0, 1.0], &[1, 2]);
    let src = ParquetSource::new();
    let schema = src.schema(full.to_str().unwrap()).unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, false)])),
        vec![Arc::new(Float64Array::from(vec![0.0])) as ArrayRef],
    )
    .unwrap();
    let mut b = RunKeysBuilder::new(&["x".to_string()], &schema).unwrap();
    b.push_batch(&batch).unwrap();
    let rk = b.finish();
    let keys = rk.all_probes(&[0]);
    assert_eq!(keys.len(), 2, "+0.0 and -0.0");
    let spec = ReconcileSpec::new(vec!["x".to_string()]).coverage(CoverageRequest::Workload(
        WorkloadKeys {
            workload: Hash::of(b"w"),
            age: std::time::Duration::ZERO,
            keys,
        },
    ));
    let r = Reconciler::new(spec, Options::default())
        .unwrap()
        .run(
            &Side::new(&src, full.to_str().unwrap()),
            &Side::new(&src, planned.to_str().unwrap()),
        )
        .unwrap();
    assert!(!r.pass, "{:?}", r.counts);
    assert!(r.counts.missing_right >= 1);
}
