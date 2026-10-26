//! Aggregate runs (B7) in replay: the plan an agg key gives a run is the
//! same on every planner path (the build crate's planner and this crate's
//! `SegmentPlanner`, each over the built index and over a transient agg
//! key), equals what the built index answers (`Planner::aggregate`), and
//! matches an oracle over the rows decoded with the `parquet` crate; a run
//! no agg key answers costs what its locate costs; an agg key priced
//! without its counted key, and a run recorded over a value that cannot be
//! aggregated, are refused by name.

#![allow(clippy::unwrap_used, missing_docs, clippy::too_many_lines)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{Array, StringArray, UInt64Array};
use bellwether_build::{AggLookup, BuildOptions, Builder, SourceKind};
use bellwether_replay::{
    KeyInput, MemoryKeySets, PlanSource, Replayer, RunAggregate, RunQuery, SegmentPlanner,
    Workload, WorkloadRecorder,
};
use bellwether_store::{ObjectStoreBackend, Snapshot};
use bellwether_types::{AggregateFn, Hash, KeySpec, ProposalBody};
use common::{Corpus, CorpusSpec, Val};

fn exact(columns: &[&str]) -> KeySpec {
    KeySpec::Exact {
        columns: columns.iter().map(|c| (*c).to_string()).collect(),
        counts: true,
        bounds: None,
    }
}

fn agg() -> KeySpec {
    KeySpec::Agg {
        columns: vec!["entity_id".into()],
        value: "batch_id".into(),
        functions: vec![AggregateFn::Sum, AggregateFn::Count],
    }
}

fn run_agg() -> RunAggregate {
    RunAggregate::new("batch_id", &[AggregateFn::Sum])
}

fn corpus() -> Corpus {
    common::write_corpus(&CorpusSpec {
        files: 3,
        row_groups_per_file: 3,
        rows_per_row_group: 120,
        entities: 60,
        ..CorpusSpec::default()
    })
}

fn build(corpus: &Corpus, store: &str, specs: &[KeySpec]) -> Arc<ObjectStoreBackend> {
    let store = Arc::new(ObjectStoreBackend::local(corpus.dir.path().join(store)).unwrap());
    let opts = BuildOptions {
        schema: Some(Arc::clone(&corpus.schema)),
        max_segment_row_groups: 2,
        ..BuildOptions::new(SourceKind::Parquet)
    };
    let r = Builder::new(Arc::clone(&store), "idx")
        .build(&corpus.data, specs, &opts)
        .unwrap();
    assert!(r.sequence.is_some(), "{r:?}");
    store
}

fn entity_keys() -> BTreeSet<Vec<Val>> {
    (0..70).step_by(3).map(|e| vec![Val::U(e)]).collect()
}

/// One status per entity: a finer lookup than the agg key's, so a row
/// group where the entity also has rows of another status is read.
fn pair_keys() -> BTreeSet<Vec<Val>> {
    (0..60)
        .step_by(4)
        .map(|e| {
            vec![
                Val::S(common::STATUSES[(e as usize / 4) % 5].to_string()),
                Val::U(e),
            ]
        })
        .collect()
}

fn workload(runs: &[(&str, RunQuery, &[&str], &BTreeSet<Vec<Val>>)]) -> (MemoryKeySets, Workload) {
    let keys = MemoryKeySets::new();
    let mut rec = WorkloadRecorder::new(&keys, common::corpus_schema());
    for (id, query, columns, tuples) in runs {
        rec.record_query(
            id,
            KeyInput::Batch(&common::key_batch(columns, tuples)),
            query.clone(),
            None,
        )
        .unwrap();
    }
    let w = rec.finish().unwrap();
    (keys, w)
}

/// Oracle: the row groups (path, index in file) where some key of `keys`
/// on `(status, entity_id)` has rows and its entity has rows of another
/// status too: those the counts cannot prove answered by the entity's
/// cell. Decoded with the `parquet` crate.
fn residual_oracle(corpus: &Corpus, keys: &BTreeSet<Vec<Val>>) -> BTreeSet<(String, u32)> {
    let mut out = BTreeSet::new();
    for path in corpus.files() {
        common::for_each_row_group(&path, &["status", "entity_id"], |rg, cols| {
            let s = cols[0].as_any().downcast_ref::<StringArray>().unwrap();
            let e = cols[1].as_any().downcast_ref::<UInt64Array>().unwrap();
            let mut pair: BTreeMap<(String, u64), u64> = BTreeMap::new();
            let mut entity: BTreeMap<u64, u64> = BTreeMap::new();
            for row in 0..e.len() {
                if e.is_null(row) {
                    continue;
                }
                *pair
                    .entry((s.value(row).to_string(), e.value(row)))
                    .or_default() += 1;
                *entity.entry(e.value(row)).or_default() += 1;
            }
            let read = keys.iter().any(|k| {
                let (Val::S(st), Val::U(id)) = (&k[0], &k[1]) else {
                    panic!()
                };
                let n = pair.get(&(st.clone(), *id)).copied().unwrap_or(0);
                n > 0 && entity[id] > n
            });
            if read {
                let path = path.canonicalize().unwrap();
                out.insert((path.to_str().unwrap().to_string(), rg));
            }
        });
    }
    out
}

/// Every planner path plans an aggregate run alike: the build crate's
/// planner and `SegmentPlanner`, over the built agg key and over a
/// transient one; their plan is what `Planner::aggregate` answers on the
/// built index and the oracle's, and replay's predicted cost is its cost.
#[test]
fn predicted_equals_planned_on_every_planner_path() {
    let corpus = corpus();
    let counted = [exact(&["entity_id"]), exact(&["status", "entity_id"])];
    let mut all = counted.to_vec();
    all.push(agg());
    let built = build(&corpus, "built", &all);
    let bare = build(&corpus, "bare", &counted);
    let (ek, pk) = (entity_keys(), pair_keys());
    let q = RunQuery::Aggregate(run_agg());
    let (keys, w) = workload(&[
        ("by-entity", q.clone(), &["entity_id"], &ek),
        ("by-pair", q.clone(), &["status", "entity_id"], &pk),
    ]);
    let source = corpus.source();
    let files: Vec<_> = corpus
        .files()
        .iter()
        .map(|p| p.canonicalize().unwrap())
        .collect();
    let bytes = common::footer_bytes(&files);
    let oracle = residual_oracle(&corpus, &pk);
    assert!(!oracle.is_empty(), "the finer lookup reads some row group");

    let snap = Snapshot::open(Arc::clone(&built), "idx").unwrap();
    let replayer = Replayer::for_snapshot(&source, &snap).unwrap();
    let build_planner = bellwether_build::Planner::from_snapshot(snap.clone()).unwrap();
    let ours = SegmentPlanner::from_snapshot(&snap, Arc::clone(replayer.catalog())).unwrap();
    let bare_snap = Snapshot::open(Arc::clone(&bare), "idx").unwrap();
    let bare_replayer = Replayer::for_snapshot(&source, &bare_snap).unwrap();
    let transient = bare_replayer.transient(&agg()).unwrap();
    let bare_build = bellwether_build::Planner::from_snapshot(bare_snap.clone()).unwrap();
    let bare_ours =
        SegmentPlanner::from_snapshot(&bare_snap, Arc::clone(bare_replayer.catalog())).unwrap();
    let bare_build_t = bare_build
        .with_transient(std::slice::from_ref(&transient))
        .unwrap();
    let bare_ours_t = bare_ours
        .with_transient(std::slice::from_ref(&transient))
        .unwrap();
    let paths: [(&str, &dyn PlanSource); 4] = [
        ("build planner, built", &build_planner),
        ("replay planner, built", &ours),
        ("build planner, transient", &*bare_build_t),
        ("replay planner, transient", &*bare_ours_t),
    ];
    let predicted = bare_replayer.replay_specs(&all, &w, &keys).unwrap();
    for (i, (run, want)) in w
        .runs()
        .iter()
        .zip([BTreeSet::new(), oracle.clone()])
        .enumerate()
    {
        let rk = w.run_keys(&keys, run).unwrap();
        let answered = build_planner
            .aggregate(
                &all,
                &AggLookup {
                    columns: rk.columns().to_vec(),
                    keys: rk.tuples().map(<[u8]>::to_vec).collect(),
                    value: "batch_id".into(),
                    functions: run_agg().functions,
                },
            )
            .unwrap();
        assert_eq!(common::cells(&answered.residual), want, "{}", run.run_id);
        for (name, p) in paths {
            let plan = p.plan_aggregate(&all, &rk, &run_agg()).unwrap();
            assert_eq!(common::cells(&plan), want, "{name}: {}", run.run_id);
            assert_eq!(plan.residual, answered.residual.residual, "{name}");
        }
        let (files, row_groups, b) = common::cost_of_cells(&want, &bytes);
        let c = &predicted[i];
        assert_eq!((c.files, c.row_groups, c.bytes), (files, row_groups, b), "{c:?}");
    }
    // Predicted with the built agg key, the same.
    let current = replayer.replay_specs(&all, &w, &keys).unwrap();
    assert_eq!(current, predicted);
}

/// A run no agg key answers (none declared, or one not storing the
/// function asked) costs what the locate of its keys costs.
#[test]
fn an_unanswered_aggregate_run_costs_its_locate() {
    let corpus = corpus();
    let counted = exact(&["entity_id"]);
    let store = build(&corpus, "s", std::slice::from_ref(&counted));
    let ek = entity_keys();
    let min = RunQuery::Aggregate(RunAggregate::new("batch_id", &[AggregateFn::Min]));
    let (keys, w) = workload(&[
        ("sum", RunQuery::Aggregate(run_agg()), &["entity_id"], &ek),
        ("min", min, &["entity_id"], &ek),
        ("locate", RunQuery::Locate, &["entity_id"], &ek),
    ]);
    let source = corpus.source();
    let snap = Snapshot::open(store, "idx").unwrap();
    let replayer = Replayer::for_snapshot(&source, &snap).unwrap();
    let at = |id: &str| w.runs().iter().position(|r| r.run_id == id).unwrap();
    let (sum, min, loc) = (at("sum"), at("min"), at("locate"));
    let locate = |c: &bellwether_types::RunCost| (c.files, c.row_groups, c.bytes);
    // No agg key: both read what the locate reads.
    let costs = replayer
        .replay_specs(std::slice::from_ref(&counted), &w, &keys)
        .unwrap();
    assert!(costs[loc].row_groups > 0);
    assert_eq!(locate(&costs[sum]), locate(&costs[loc]));
    assert_eq!(locate(&costs[min]), locate(&costs[loc]));
    // An agg key storing sum and count: the sum run reads nothing, the min
    // run (a function it does not store) still reads its locate.
    let costs = replayer.replay_specs(&[counted, agg()], &w, &keys).unwrap();
    assert_eq!(locate(&costs[sum]), (0, 0, 0));
    assert_eq!(locate(&costs[min]), locate(&costs[loc]));
}

/// Scoring an agg key without the counted key beside it is refused, naming
/// the propose step first; a planner asked for an aggregate no agg key
/// answers names both declarations; recording a run over a value that
/// cannot be aggregated is refused by name.
#[test]
fn refusals_name_what_to_declare() {
    let corpus = corpus();
    let store = build(&corpus, "s", &[exact(&["status"])]);
    let ek = entity_keys();
    let (keys, w) = workload(&[("sum", RunQuery::Aggregate(run_agg()), &["entity_id"], &ek)]);
    let source = corpus.source();
    let snap = Snapshot::open(store, "idx").unwrap();
    let replayer = Replayer::for_snapshot(&source, &snap).unwrap();
    let err = replayer
        .score(
            &Hash::of(b"p"),
            &ProposalBody::Key {
                spec: agg(),
                sidecars: Vec::new(),
            },
            &w,
            &keys,
            "2026-10-26T00:00:00.000Z",
        )
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("--key 'exact:entity_id,counts'") && err.contains("bellwether propose"),
        "{err}"
    );
    let rk = w.run_keys(&keys, &w.runs()[0]).unwrap();
    let planner = bellwether_build::Planner::from_snapshot(snap.clone()).unwrap();
    let err = planner
        .plan_aggregate(&[exact(&["status"])], &rk, &run_agg())
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("exact:entity_id,counts") && err.contains("agg:entity_id@batch_id"),
        "{err}"
    );

    let k = MemoryKeySets::new();
    let mut rec = WorkloadRecorder::new(&k, common::corpus_schema());
    let batch = common::key_batch(&["entity_id"], &ek);
    for (value, f, want) in [
        ("nope", AggregateFn::Sum, "nope"),
        ("status", AggregateFn::Sum, "status"),
    ] {
        let err = rec
            .record_query(
                value,
                KeyInput::Batch(&batch),
                RunQuery::Aggregate(RunAggregate::new(value, &[f])),
                None,
            )
            .unwrap_err()
            .to_string();
        assert!(err.contains(want), "{err}");
    }
    // The minimum of a string is defined; a run of it records.
    rec.record_query(
        "min-status",
        KeyInput::Batch(&batch),
        RunQuery::Aggregate(RunAggregate::new("status", &[AggregateFn::Min])),
        None,
    )
    .unwrap();
}
