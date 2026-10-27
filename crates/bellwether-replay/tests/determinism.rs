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
//! `Replay` through the ledger, and emits its canonical JSON. It does so
//! twice: for a workload of lookups, and for the same runs priced through
//! a consumer plan (relational replay, WP15), whose record lists every
//! node's cost per run; each has its own golden value.
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
//! It was last updated for record version 4 (B8), which moves the record's
//! `record_version` and its proposal's hash and nothing else. Before that,
//! for contract v2.1 (`RECORD_VERSION` 2, `inputs`), together with recorded
//! runs gaining a query kind and the column-set template (both part of the
//! workload hash). It was computed with parquet 53.4.1, whose writer fixes
//! the row-group byte sizes; the arrow/parquet upgrade is expected to
//! change it.

#![allow(clippy::unwrap_used, missing_docs, clippy::too_many_lines)]

mod common;

use std::process::Command;
use std::sync::Arc;

use bellwether_ledger::Ledger;
use bellwether_replay::{KeyInput, Replayer, TransientLimits, WorkloadRecorder};
use bellwether_store::{Cache, ObjectStoreBackend, Snapshot};
use bellwether_types::format::RECORD_VERSION;
use bellwether_types::{
    AggregateFn, ConsumerPlan, Hash, KeySpec, KeysFrom, Measure, PlanNode, Proposal, ProposalBody,
    Record,
};
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
/// Re-pinned 2026-10-26 for M5 (ADR 028): replay fills `configuration`
/// (the current specs' identities, here `range:entity_id` and
/// `exact:batch_id`); every cost is unchanged.
/// Re-pinned 2026-10-26 for record version 4 (B8, ADR 028): the Replay and
/// the Proposal it cites are written at version 4, so `record_version` and
/// `proposal` (the proposal's hash) move; the change is the version
/// constant alone, so every cost and every other field is unchanged.
const GOLDEN: &str = "da2ee890e8699fc1a7992c7d2c94c24b0f107cde1df2edf3e2f10df3781d383c";
/// The same for the consumer-plan workload (relational replay, WP15): the
/// runs priced through a plan (scan, semi-join on the run's keys, a group
/// by `batch_id`, an exchange), scoring `exact:entity_id`; the record lists
/// every node's cost per run.
const GOLDEN_PLAN: &str = "0d98b230ed8652041084d04deed69939e4de6478f6f5bfe95f920d2066e829e5";

fn cols(c: &[&str]) -> Vec<String> {
    c.iter().map(|s| (*s).to_string()).collect()
}

/// The consumer plan the plan workload's runs are priced through.
fn plan() -> ConsumerPlan {
    ConsumerPlan {
        version: 1,
        nodes: vec![
            PlanNode::Scan {
                dataset: "main".into(),
                columns: cols(&["entity_id", "batch_id"]),
            },
            PlanNode::SemiJoin {
                input: 0,
                columns: cols(&["entity_id"]),
                keys: KeysFrom::Run,
            },
            PlanNode::Aggregate {
                input: 1,
                group_by: cols(&["batch_id"]),
                measures: vec![Measure {
                    function: AggregateFn::Count,
                    column: None,
                }],
            },
            PlanNode::Exchange {
                input: 2,
                by: cols(&["batch_id"]),
            },
        ],
        root: 3,
    }
}

/// Builds the index over `corpus` from scratch, in a store of its own, in
/// `mode`, and returns the Replay records' bytes as the ledger stores them:
/// the lookup workload's, then the consumer-plan workload's.
fn replay_bytes(mode: &str, corpus: &common::Corpus) -> (Vec<u8>, Vec<u8>) {
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
    let mut runs = common::entity_runs(corpus, 12, 99);
    if mode == "B" {
        runs.reverse();
    }
    let record = |with: Option<&ConsumerPlan>| {
        let mut rec = WorkloadRecorder::new(&cache, Arc::clone(&corpus.schema));
        if let Some(p) = with {
            rec = rec.with_plan(p).unwrap();
        }
        for (id, tuples) in &runs {
            rec.record(
                id,
                KeyInput::Batch(&common::key_batch(&["entity_id"], tuples)),
            )
            .unwrap();
        }
        rec.finish().unwrap()
    };
    let workload = record(None);
    let plan_workload = record(Some(&plan()));

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
    let written = |proposal: &Hash, workload: &bellwether_replay::Workload| {
        let (hash, scored) = replayer
            .record(&ledger, proposal, workload, &cache, CREATED_AT)
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
    };
    let lookups = written(&proposal, &workload);
    let plan_proposal = ledger
        .put(&Record::Proposal(Proposal {
            record_version: RECORD_VERSION,
            evidence: vec![],
            proposed_by: "determinism-test".into(),
            created_at: CREATED_AT.into(),
            body: ProposalBody::Key {
                spec: KeySpec::Exact {
                    columns: cols(&["entity_id"]),
                    counts: true,
                    bounds: None,
                },
                sidecars: Vec::new(),
            },
        }))
        .unwrap();
    let plans = written(&plan_proposal, &plan_workload);
    (lookups, plans)
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
        let (lookups, plans) = replay_bytes(&mode, &corpus);
        let mut out_plan = out.clone();
        out_plan.push(".plan");
        std::fs::write(out, lookups).unwrap();
        std::fs::write(out_plan, plans).unwrap();
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
        let mut out_plan = out.clone().into_os_string();
        out_plan.push(".plan");
        outputs.push((
            std::fs::read(&out).unwrap(),
            std::fs::read(out_plan).unwrap(),
        ));
    }
    let in_process = replay_bytes("C", &corpus);
    for (which, golden, pick) in [
        ("lookup", GOLDEN, 0usize),
        ("consumer-plan", GOLDEN_PLAN, 1),
    ] {
        let get = |o: &(Vec<u8>, Vec<u8>)| if pick == 0 { o.0.clone() } else { o.1.clone() };
        check(
            which,
            golden,
            &get(&outputs[0]),
            &get(&outputs[1]),
            &get(&in_process),
        );
    }
}

/// The three records of one workload are byte-equal, and hash to `golden`
/// with the catalog digest masked.
fn check(which: &str, golden: &str, a: &[u8], b: &[u8], c: &[u8]) {
    let outputs = [a.to_vec(), b.to_vec()];
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
        c,
        outputs[0].as_slice(),
        "{which}: in-process replay differs"
    );
    let Record::Replay(r) = serde_json::from_slice(&outputs[0]).unwrap() else {
        panic!("not a replay");
    };
    assert_eq!(r.inputs.len(), 1, "the catalog digest");
    let masked = String::from_utf8(outputs[0].clone())
        .unwrap()
        .replace(&r.inputs[0].to_hex(), &"0".repeat(64));
    let got = Hash::of(masked.as_bytes()).to_hex();
    assert_eq!(
        got, golden,
        "{which}: golden Replay changed; record:\n{masked}"
    );
}
