//! Contract v2.1 `Replay::inputs` (adopted from the PR 18 review's probes):
//! an unindexed folder's manifest version is `"0"`, which names no file set,
//! so the catalog digest in `inputs` is what tells two replays of one
//! proposal apart (invariant 3).
//!
//! - The same data at two paths: equal costs, different inputs, so two
//!   records where contract v2 wrote one.
//! - One folder before and after a file lands: both `"0"`, different
//!   inputs, two records.

#![allow(clippy::unwrap_used, missing_docs)]

mod typed;

use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use bellwether_ledger::{Ledger, RecordKind};
use bellwether_replay::{KeyInput, MemoryKeySets, Replayer, Workload, WorkloadRecorder};
use bellwether_source::ParquetSource;
use bellwether_store::ObjectStoreBackend;
use bellwether_types::format::RECORD_VERSION;
use bellwether_types::{Hash, KeySpec, Proposal, ProposalBody, Record};

const AT: &str = "2026-10-24T00:00:00Z";

/// A ledger holding one exact-key proposal on `k_i64`, and a one-run
/// workload over keys present in every typed corpus.
fn setup() -> (
    Ledger<Arc<ObjectStoreBackend>>,
    Hash,
    MemoryKeySets,
    Workload,
) {
    let ledger = Ledger::new(Arc::new(ObjectStoreBackend::memory().unwrap()));
    let keys = MemoryKeySets::new();
    let mut rec = WorkloadRecorder::new(&keys, typed::dataset_schema());
    let batch = RecordBatch::try_from_iter(vec![(
        "k_i64",
        Arc::new(Int64Array::from(vec![7, 1001, 1002, 1150])) as ArrayRef,
    )])
    .unwrap();
    rec.record("r1", KeyInput::Batch(&batch)).unwrap();
    let workload = rec.finish().unwrap();
    let proposal = ledger
        .put(&Record::Proposal(Proposal {
            record_version: RECORD_VERSION,
            evidence: vec![],
            proposed_by: "inputs-test".into(),
            created_at: AT.into(),
            body: ProposalBody::Key {
                spec: KeySpec::Exact {
                    columns: vec!["k_i64".into()],
                },
            },
        }))
        .unwrap();
    (ledger, proposal, keys, workload)
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let p = e.unwrap().path();
        let dest = to.join(p.file_name().unwrap());
        if p.is_dir() {
            copy_dir(&p, &dest);
        } else {
            std::fs::copy(&p, &dest).unwrap();
        }
    }
}

#[test]
fn the_same_data_at_two_paths_gives_equal_costs_and_two_records() {
    let a = typed::write_corpus(6, 3, 40, 1);
    let copy = tempfile::tempdir().unwrap();
    copy_dir(Path::new(&a.data), copy.path());
    let source = ParquetSource::new().with_schema(typed::dataset_schema());
    let (ledger, proposal, keys, workload) = setup();
    let mut written = Vec::new();
    for folder in [a.data.as_str(), copy.path().to_str().unwrap()] {
        let replayer =
            Replayer::unindexed(&source, folder, &bellwether_source::Partitioning::None).unwrap();
        let (h, scored) = replayer
            .record(&ledger, &proposal, &workload, &keys, AT)
            .unwrap();
        assert_eq!(scored.replay.inputs, vec![replayer.catalog().digest()]);
        written.push((h, scored.replay));
    }
    let (ra, rb) = (&written[0].1, &written[1].1);
    assert_eq!(ra.manifest_version, "0");
    assert_eq!(rb.manifest_version, "0");
    assert_eq!((ra.workload, ra.proposal), (rb.workload, rb.proposal));
    assert_eq!(ra.current, rb.current, "the same data costs the same");
    assert_eq!(ra.proposed, rb.proposed);
    assert!(ra.proposed.iter().any(|c| c.row_groups > 0));
    assert_ne!(ra.inputs, rb.inputs, "the digests name different files");
    assert_ne!(written[0].0, written[1].0);
    assert_eq!(ledger.list(RecordKind::Replay).unwrap().count(), 2);
}

#[test]
fn a_folder_before_and_after_a_file_lands_gives_two_records() {
    let corpus = typed::write_corpus(6, 3, 40, 8);
    let source = ParquetSource::new().with_schema(typed::dataset_schema());
    let (ledger, proposal, keys, workload) = setup();
    let before = Replayer::unindexed(
        &source,
        &corpus.data,
        &bellwether_source::Partitioning::None,
    )
    .unwrap()
    .record(&ledger, &proposal, &workload, &keys, AT)
    .unwrap();
    let extra = typed::write_corpus(6, 3, 40, 9);
    std::fs::copy(
        &extra.files()[0],
        Path::new(&corpus.data).join("part-900.parquet"),
    )
    .unwrap();
    let after = Replayer::unindexed(
        &source,
        &corpus.data,
        &bellwether_source::Partitioning::None,
    )
    .unwrap()
    .record(&ledger, &proposal, &workload, &keys, AT)
    .unwrap();
    assert_eq!(before.1.replay.manifest_version, "0");
    assert_eq!(after.1.replay.manifest_version, "0");
    assert_ne!(before.1.replay.inputs, after.1.replay.inputs);
    assert_ne!(before.0, after.0);
    assert_eq!(ledger.list(RecordKind::Replay).unwrap().count(), 2);
}
