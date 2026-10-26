//! WP21 item 1 through the facade: an index over a hive-partitioned folder
//! records the scheme, plans on the partition column, and `fix` runs score,
//! verify, apply, attest end to end. The `data_changed` gate recomputes the
//! data root's catalog digest through the same source the verifier used,
//! so a source built without the partition scheme on either path would be
//! refused there.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::similar_names)]

mod common;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether::types::{EvidenceBody, Record};
use bellwether::{
    spec, Engine, EngineOptions, FixOutcome, FixRequest, KeyFile, Partitioning, WhyRequest,
};
use common::{ledger_corpus, write_keys};
use parquet::arrow::ArrowWriter;

fn hive(root: &Path) -> Engine {
    Engine::open(
        root.to_str().unwrap(),
        EngineOptions {
            partitioning: Some(Partitioning::Hive),
            ..EngineOptions::default()
        },
    )
    .unwrap()
}

fn write_region_keys(path: &Path, regions: &[&str]) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "region",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(regions.to_vec())) as ArrayRef],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

#[test]
fn a_hive_partitioned_folder_indexes_plans_and_attests() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data.join("region=eu"), 2, 2, 20, 100);
    ledger_corpus(&data.join("region=us"), 3, 2, 20, 100);

    let e = hive(&data);
    let r = e.index(&[spec::parse("exact:region").unwrap()]).unwrap();
    assert!(r.committed.is_some());
    let kf = dir.path().join("regions.parquet");
    write_region_keys(&kf, &["eu"]);
    let plan = e.locate(&KeyFile::new(&kf)).unwrap();
    // Two files of two row groups each live under region=eu.
    assert_eq!((plan.planned.files, plan.planned.row_groups), (2, 4));
    assert_eq!(plan.row_groups_total, 10);
    let Record::Evidence(ev) = &plan.evidence.record else {
        panic!()
    };
    let EvidenceBody::Plan { plan: p } = &ev.body else {
        panic!()
    };
    assert!(p.entries.iter().all(|x| x.file.path.contains("region=eu")));

    // Reopening without the option uses the recorded scheme; asking for
    // none is refused.
    let plain = Engine::open(data.to_str().unwrap(), EngineOptions::default()).unwrap();
    assert_eq!(
        plain.locate(&KeyFile::new(&kf)).unwrap().planned.row_groups,
        4
    );
    let err = Engine::open(
        data.to_str().unwrap(),
        EngineOptions {
            partitioning: Some(Partitioning::None),
            ..EngineOptions::default()
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("partitioning"), "{err}");

    // Score, verify, apply, attest a key on `id` over the partitioned data.
    // The files' own min/max statistics already find these ids, so the key
    // saves nothing (`why` recommends none); apply it anyway, which is what
    // this test is about.
    let ids = dir.path().join("ids.parquet");
    write_keys(&ids, "id", &[3, 42]);
    let w = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&ids)),
            ..WhyRequest::default()
        })
        .unwrap();
    let fixed = e
        .fix(&FixRequest {
            proposal: Some(w.candidates[0].proposal.hash),
            workload: Some(w.workload.as_ref().unwrap().hash),
            allow_no_gain: true,
            ..FixRequest::default()
        })
        .unwrap();
    assert!(
        matches!(fixed.outcome, FixOutcome::Applied { .. }),
        "{:?}",
        fixed.outcome
    );
}
