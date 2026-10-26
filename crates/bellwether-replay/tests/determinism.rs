//! Invariant 3: replay is deterministic. Same manifest version, same data,
//! same workload hash, and same proposal give a byte-identical `Replay`.
//!
//! The parent writes one corpus. "Same data" means the same files: the
//! catalog digest in `Replay::inputs` names each file by path, size, mtime,
//! and content hash, so the same bytes at another path are other data
//! (`tests/inputs.rs`). The test re-executes this binary twice, in two
//! modes that differ in everything else that must not matter. Each child
//! builds the index from scratch in its own store directory (so store
//! paths and hash-map seeds differ), records the workload, scores the same
//! proposal against the committed index (a transient spec on top of
//! committed segments read by range from a pinned snapshot), writes the
//! `Replay` through the ledger, and emits its canonical JSON.
//!
//! - Mode A: the committed index split into two segments per spec, runs
//!   recorded in id order, default transient limits, a cold cache.
//! - Mode B: one segment per spec, runs recorded in reverse order,
//!   `max_segment_rows = 1` (a transient segment per row group), and a cache
//!   warmed with an unrelated spec first.
//! - Mode C (in-process): `max_segment_rows = 57`.
//!
//! The parent requires A, B, and C to be byte-equal, inputs included, and
//! to hash to a checked-in golden value once the catalog digest (which
//! names this run's temporary paths and mtimes) is masked, so a change to
//! any number, field, or ordering in the record fails here. `created_at` is
//! pinned, as the contract requires of callers.
//!
//! Updating the golden value is a deliberate act: it means the record a
//! given input produces has changed, and the PR that changes it says why.
//! It was last updated for contract v2.1 (`RECORD_VERSION` 2, `inputs`),
//! together with recorded runs gaining a query kind and the column-set
//! template (both part of the workload hash). It was computed with parquet
//! 53.4.1, whose writer fixes the row-group byte sizes; the arrow/parquet
//! upgrade is expected to change it.

#![allow(clippy::unwrap_used, missing_docs)]

mod common;

use std::process::Command;
use std::sync::Arc;

use bellwether_ledger::Ledger;
use bellwether_replay::{KeyInput, Replayer, TransientLimits, WorkloadRecorder};
use bellwether_store::{Cache, ObjectStoreBackend, Snapshot};
use bellwether_types::format::RECORD_VERSION;
use bellwether_types::{Hash, KeySpec, Proposal, ProposalBody, Record};
use common::{BuildOptions, CorpusSpec};

const CREATED_AT: &str = "2026-10-24T00:00:00Z";
const CHILD_OUT: &str = "BELLWETHER_REPLAY_DETERMINISM_OUT";
const CHILD_MODE: &str = "BELLWETHER_REPLAY_DETERMINISM_MODE";
const CHILD_CORPUS: &str = "BELLWETHER_REPLAY_DETERMINISM_CORPUS";
/// BLAKE3 of the Replay record's canonical JSON for the fixture below, with
/// the catalog digest's hex replaced by zeros. Re-pinned 2026-10-24 for the
/// parquet 53 -> 59 upgrade: every run's files and row groups are unchanged;
/// only `bytes` moved, because the new writer omits page-header statistics,
/// so the fixture's column chunks are smaller (docs/benchmark.md).
const GOLDEN: &str = "7da78de5e096bc52a472f1ab71cb6e562474ab2f0657f9296dda4874c21e9030";

fn cols(c: &[&str]) -> Vec<String> {
    c.iter().map(|s| (*s).to_string()).collect()
}

/// Builds the index over `corpus` from scratch, in a store of its own, in
/// `mode`, and returns the Replay record's bytes as the ledger stores them.
fn replay_bytes(mode: &str, corpus: &common::Corpus) -> Vec<u8> {
    let scratch = tempfile::tempdir().unwrap();
    let store = Arc::new(ObjectStoreBackend::local(scratch.path().join("store")).unwrap());
    let root = "idx";
    // The current index prunes entity lookups by min/max only; the proposal
    // adds a composite exact key the runs reach by prefix, so the proposed
    // plan is the per-key intersection of a committed and a transient spec.
    let current = vec![
        KeySpec::Range {
            column: "entity_id".into(),
        },
        KeySpec::Exact {
            columns: cols(&["batch_id"]),
            counts: false,
            bounds: None,
        },
    ];
    let sequence = common::build_index(
        &store,
        root,
        corpus,
        &current,
        BuildOptions {
            split: mode != "B",
            ..BuildOptions::default()
        },
    );
    assert_eq!(sequence, 1);

    let cache = Cache::new(Arc::clone(&store), root);
    let mut rec = WorkloadRecorder::new(&cache, Arc::clone(&corpus.schema));
    let mut runs = common::entity_runs(corpus, 12, 99);
    if mode == "B" {
        runs.reverse();
    }
    for (id, tuples) in runs {
        rec.record(
            &id,
            KeyInput::Batch(&common::key_batch(&["entity_id"], &tuples)),
        )
        .unwrap();
    }
    let workload = rec.finish().unwrap();

    let ledger = Ledger::new(Arc::clone(&store));
    let proposal = ledger
        .put(&Record::Proposal(Proposal {
            record_version: RECORD_VERSION,
            evidence: vec![],
            proposed_by: "determinism-test".into(),
            created_at: CREATED_AT.into(),
            body: ProposalBody::Key {
                spec: KeySpec::Exact {
                    columns: cols(&["entity_id", "batch_id"]),
                    counts: false,
                    bounds: None,
                },
                sidecars: Vec::new(),
            },
        }))
        .unwrap();

    let source = corpus.source();
    let snapshot = Snapshot::open(Arc::clone(&store), root).unwrap();
    let mut replayer = Replayer::for_snapshot(&source, &snapshot).unwrap();
    match mode {
        "B" => {
            replayer = replayer.with_limits(TransientLimits {
                max_segment_rows: 1,
                ..TransientLimits::default()
            });
            replayer
                .transient(&KeySpec::Exact {
                    columns: cols(&["payload"]),
                    counts: false,
                    bounds: None,
                })
                .unwrap();
        }
        "C" => {
            replayer = replayer.with_limits(TransientLimits {
                max_segment_rows: 57,
                ..TransientLimits::default()
            });
        }
        _ => {}
    }
    let (hash, scored) = replayer
        .record(&ledger, &proposal, &workload, &cache, CREATED_AT)
        .unwrap();
    assert_eq!(scored.replay.manifest_version, "1");
    assert_eq!(scored.replay.workload, workload.hash());
    assert_eq!(scored.replay.inputs, vec![replayer.catalog().digest()]);
    let bytes = serde_json::to_vec(&Record::Replay(scored.replay)).unwrap();
    assert_eq!(
        Hash::of(&bytes),
        hash,
        "the ledger stores exactly these bytes"
    );
    assert_eq!(ledger.get_raw(&hash).unwrap().as_ref(), bytes.as_slice());
    bytes
}

/// Child mode: writes the record for the mode in `CHILD_MODE`, over the
/// corpus at `CHILD_CORPUS`, to the path in `CHILD_OUT`; a no-op in a
/// normal test run.
#[test]
fn child_writes_replay() {
    if let (Some(out), Ok(mode), Ok(data)) = (
        std::env::var_os(CHILD_OUT),
        std::env::var(CHILD_MODE),
        std::env::var(CHILD_CORPUS),
    ) {
        let corpus = common::Corpus::open(&data, &CorpusSpec::default());
        std::fs::write(out, replay_bytes(&mode, &corpus)).unwrap();
    }
}

#[test]
fn replay_is_byte_identical_across_processes() {
    let corpus = common::write_corpus(&CorpusSpec::default());
    let exe = std::env::current_exe().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut outputs = Vec::new();
    for mode in ["A", "B"] {
        let out = dir.path().join(format!("replay-{mode}.json"));
        let status = Command::new(&exe)
            .args(["--exact", "child_writes_replay", "--test-threads=1"])
            .env(CHILD_OUT, &out)
            .env(CHILD_MODE, mode)
            .env(CHILD_CORPUS, &corpus.data)
            .status()
            .unwrap();
        assert!(status.success(), "child {mode} failed");
        outputs.push(std::fs::read(&out).unwrap());
    }
    assert!(
        outputs[0].starts_with(br#"{"record":"replay""#),
        "child wrote no record"
    );
    assert_eq!(
        outputs[0],
        outputs[1],
        "two processes replayed the same inputs to different bytes:\n{}\n{}",
        String::from_utf8_lossy(&outputs[0]),
        String::from_utf8_lossy(&outputs[1])
    );
    assert_eq!(
        replay_bytes("C", &corpus),
        outputs[0],
        "in-process replay differs"
    );
    let Record::Replay(r) = serde_json::from_slice(&outputs[0]).unwrap() else {
        panic!("not a replay");
    };
    assert_eq!(r.inputs.len(), 1, "the catalog digest");
    let masked = String::from_utf8(outputs[0].clone())
        .unwrap()
        .replace(&r.inputs[0].to_hex(), &"0".repeat(64));
    let got = Hash::of(masked.as_bytes()).to_hex();
    assert_eq!(got, GOLDEN, "golden Replay changed; record:\n{masked}");
}
