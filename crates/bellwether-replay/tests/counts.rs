//! Row counts in replay (`ScanPlan.rows`, `RunCost.rows`) against rows
//! decoded with the `parquet` crate, through both planners (the build
//! crate's over an index and this crate's `SegmentPlanner`):
//!
//! - only a counting spec on exactly the run's columns counts a run;
//! - a file rewrite retires row groups, and their counts are never added;
//! - a `count` run whose spec stores counts reads nothing (the index
//!   answers), and without such a spec costs what its locate costs;
//! - two segments counting one key in one row group are refused, never
//!   summed.

#![allow(clippy::unwrap_used, missing_docs, clippy::too_many_lines)]

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Array, StringArray, UInt64Array};
use bellwether_build::{BuildOptions, Builder, SourceKind};
use bellwether_replay::{
    KeyInput, MemoryKeySets, PlanSource, Replayer, RunQuery, SegmentPlanner, Workload,
    WorkloadRecorder,
};
use bellwether_store::{Manifest, ObjectStoreBackend, Snapshot};
use bellwether_types::{Error, KeySpec, Store};
use bytes::Bytes;
use common::{Corpus, CorpusSpec, Val};

fn exact(columns: &[&str], counts: bool) -> KeySpec {
    KeySpec::Exact {
        columns: columns.iter().map(|c| (*c).to_string()).collect(),
        counts,
        bounds: None,
    }
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

fn build(store: &Arc<ObjectStoreBackend>, corpus: &Corpus, specs: &[KeySpec]) {
    let opts = BuildOptions {
        schema: Some(Arc::clone(&corpus.schema)),
        max_segment_row_groups: 2,
        ..BuildOptions::new(SourceKind::Parquet)
    };
    let r = Builder::new(Arc::clone(store), "idx")
        .build(&corpus.data, specs, &opts)
        .unwrap();
    assert!(r.sequence.is_some(), "{r:?}");
}

/// Rows of the corpus files whose `(status, entity_id)` or `entity_id`
/// tuple is one of `keys` (a null matches nothing).
fn rows_holding(corpus: &Corpus, columns: &[&str], keys: &BTreeSet<Vec<Val>>) -> u64 {
    let mut n = 0;
    for path in corpus.files() {
        common::for_each_row_group(&path, columns, |_, cols| {
            for row in 0..cols[0].len() {
                let t: Option<Vec<Val>> = cols
                    .iter()
                    .map(|c| {
                        if c.is_null(row) {
                            return None;
                        }
                        if let Some(a) = c.as_any().downcast_ref::<UInt64Array>() {
                            return Some(Val::U(a.value(row)));
                        }
                        let s = c.as_any().downcast_ref::<StringArray>().unwrap();
                        Some(Val::S(s.value(row).to_string()))
                    })
                    .collect();
                if t.is_some_and(|t| keys.contains(&t)) {
                    n += 1;
                }
            }
        });
    }
    n
}

fn entity_keys() -> BTreeSet<Vec<Val>> {
    (0..80).step_by(3).map(|e| vec![Val::U(e)]).collect()
}

fn pair_keys() -> BTreeSet<Vec<Val>> {
    let mut out = BTreeSet::new();
    for e in (0..60).step_by(4) {
        for s in common::STATUSES {
            out.insert(vec![Val::S(s.to_string()), Val::U(e)]);
        }
    }
    out
}

/// A workload of one run per `(id, query, columns, keys)`.
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

/// The rows both planners give each run of `w` under `specs`.
fn planned_rows(
    store: &Arc<ObjectStoreBackend>,
    corpus: &Corpus,
    specs: &[KeySpec],
    keys: &MemoryKeySets,
    w: &Workload,
) -> Vec<(Option<u64>, Option<u64>)> {
    let source = corpus.source();
    let snapshot = Snapshot::open(Arc::clone(store), "idx").unwrap();
    let replayer = Replayer::for_snapshot(&source, &snapshot).unwrap();
    let build = bellwether_build::Planner::from_snapshot(snapshot.clone()).unwrap();
    let ours = SegmentPlanner::from_snapshot(&snapshot, Arc::clone(replayer.catalog())).unwrap();
    w.runs()
        .iter()
        .map(|run| {
            let rk = w.run_keys(keys, run).unwrap();
            let rows = |p: &dyn PlanSource| {
                p.plan_query(specs, &rk, &RunQuery::Locate)
                    .unwrap()
                    .rows
                    .map(|q| q.value)
            };
            (rows(&build), rows(&ours))
        })
        .collect()
}

/// Only a counting spec on exactly the run's columns counts it, in both
/// planners; a file rewrite retires row groups, and neither planner adds
/// their counts.
#[test]
fn counts_come_from_a_spec_on_exactly_the_run_columns_and_only_live_row_groups() {
    let corpus = corpus();
    let store = Arc::new(ObjectStoreBackend::local(corpus.dir.path().join("store")).unwrap());
    let specs = vec![exact(&["entity_id"], true)];
    build(&store, &corpus, &specs);
    let (ek, pk) = (entity_keys(), pair_keys());
    let (keys, w) = workload(&[
        ("by-entity", RunQuery::Locate, &["entity_id"], &ek),
        ("by-pair", RunQuery::Locate, &["status", "entity_id"], &pk),
    ]);
    let want = rows_holding(&corpus, &["entity_id"], &ek);
    assert!(want > 0);
    assert_eq!(
        planned_rows(&store, &corpus, &specs, &keys, &w),
        vec![(Some(want), Some(want)), (None, None)],
        "exact:entity_id,counts counts entity runs, never (status, entity_id) runs"
    );

    // Rewrite one file: its old row groups are retired.
    let other = common::write_corpus(&CorpusSpec {
        files: 3,
        row_groups_per_file: 2,
        rows_per_row_group: 150,
        entities: 60,
        seed: 99,
        ..CorpusSpec::default()
    });
    std::fs::copy(&other.files()[1], &corpus.files()[1]).unwrap();
    build(&store, &corpus, &specs);
    let m = Manifest::from_slice(&store.get("idx/manifest.json").unwrap().0).unwrap();
    assert!(!m.rowgroup_space.tombstones.is_empty());
    let want = rows_holding(&corpus, &["entity_id"], &ek);
    assert_eq!(
        planned_rows(&store, &corpus, &specs, &keys, &w)[0],
        (Some(want), Some(want)),
        "after a rewrite, against the decoded files"
    );
}

/// A `count` run whose keys a spec counts reads nothing: the index answers
/// it, with the decoded files' rows. Without counts it costs what the same
/// keys' locate costs.
#[test]
fn a_count_run_reads_nothing_when_its_spec_stores_counts() {
    let corpus = corpus();
    let store = Arc::new(ObjectStoreBackend::local(corpus.dir.path().join("store")).unwrap());
    let counted = exact(&["entity_id"], true);
    build(&store, &corpus, std::slice::from_ref(&counted));
    let ek = entity_keys();
    let (keys, w) = workload(&[
        ("count", RunQuery::Count, &["entity_id"], &ek),
        ("locate", RunQuery::Locate, &["entity_id"], &ek),
    ]);
    let source = corpus.source();
    let snapshot = Snapshot::open(Arc::clone(&store), "idx").unwrap();
    let replayer = Replayer::for_snapshot(&source, &snapshot).unwrap();
    let want = rows_holding(&corpus, &["entity_id"], &ek);

    let costs = replayer
        .replay_specs(std::slice::from_ref(&counted), &w, &keys)
        .unwrap();
    let (count, locate) = (&costs[0], &costs[1]);
    assert_eq!(
        (count.files, count.row_groups, count.bytes),
        (0, 0, 0),
        "{count:?}"
    );
    assert_eq!(count.rows.as_ref().map(|q| q.value), Some(want));
    assert!(locate.bytes > 0 && locate.row_groups > 0, "{locate:?}");
    assert_eq!(locate.rows.as_ref().map(|q| q.value), Some(want));

    let plain = exact(&["entity_id"], false);
    let costs = replayer
        .replay_specs(std::slice::from_ref(&plain), &w, &keys)
        .unwrap();
    assert_eq!(costs[0].rows, None);
    assert_eq!(
        (costs[0].files, costs[0].row_groups, costs[0].bytes),
        (locate.files, locate.row_groups, locate.bytes),
        "without counts, a count reads what its locate reads"
    );
}

/// Two segments of one counted spec over the same row groups: both
/// planners refuse the run as a corrupt index rather than counting its rows
/// twice.
#[test]
fn overlapping_counted_segments_are_refused_by_both_planners() {
    let corpus = corpus();
    let store = Arc::new(ObjectStoreBackend::local(corpus.dir.path().join("store")).unwrap());
    let specs = vec![exact(&["entity_id"], true)];
    build(&store, &corpus, &specs);
    let (bytes, version) = store.get("idx/manifest.json").unwrap();
    let mut m = Manifest::from_slice(&bytes).unwrap();
    assert!(m.segments.len() > 1);
    // One whole-range segment of the same spec, built into another store
    // (another id), listed beside the index's own segments.
    let other = Builder::new(
        Arc::new(ObjectStoreBackend::local(corpus.dir.path().join("store2")).unwrap()),
        "idx",
    );
    let opts = BuildOptions {
        schema: Some(Arc::clone(&corpus.schema)),
        ..BuildOptions::new(SourceKind::Parquet)
    };
    other.build(&corpus.data, &specs, &opts).unwrap();
    let whole = other.manifest().unwrap().unwrap().segments[0].clone();
    assert!(!m.references(&whole.id));
    let path = format!("idx/segments/{}.seg", whole.id.to_hex());
    let (whole_bytes, _) = other.store().get(&path).unwrap();
    store.put_if_absent(&path, whole_bytes).unwrap();
    m.segments.push(whole);
    store
        .put_if_version("idx/manifest.json", Bytes::from(m.to_vec().unwrap()), &version)
        .unwrap();

    let ek = entity_keys();
    let (keys, w) = workload(&[("by-entity", RunQuery::Locate, &["entity_id"], &ek)]);
    let source = corpus.source();
    let snapshot = Snapshot::open(Arc::clone(&store), "idx").unwrap();
    let replayer = Replayer::for_snapshot(&source, &snapshot).unwrap();
    let build = bellwether_build::Planner::from_snapshot(snapshot.clone()).unwrap();
    let ours = SegmentPlanner::from_snapshot(&snapshot, Arc::clone(replayer.catalog())).unwrap();
    let rk = w.run_keys(&keys, &w.runs()[0]).unwrap();
    for (p, name) in [(&build as &dyn PlanSource, "build"), (&ours, "replay")] {
        match p.plan_query(&specs, &rk, &RunQuery::Locate) {
            Err(Error::Other(m)) => assert!(
                m.starts_with("corrupt index:") && m.contains("exact:entity_id,counts"),
                "{name}: {m}"
            ),
            other => panic!("{name}: expected a corrupt-index refusal, got {other:?}"),
        }
    }
}
