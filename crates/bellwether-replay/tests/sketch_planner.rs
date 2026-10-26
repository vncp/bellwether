//! A `sketch` sidecar never reaches a replay plan: two indexes over one
//! corpus, the same key specs, one of them also declaring a sketch, give
//! `SegmentPlanner::from_snapshot` and the replayer over each snapshot the
//! same plan for every run and spec, and the same current and hypothetical
//! replay costs.

#![allow(clippy::unwrap_used, missing_docs, clippy::too_many_lines)]

mod common;

use std::sync::Arc;

use bellwether_build::{BuildOptions, Builder};
use bellwether_replay::{Catalog, PlanSource, Replayer, SegmentPlanner};
use bellwether_store::{ObjectStoreBackend, Snapshot};
use bellwether_types::{KeySpec, SketchSpec, Source};
use common::CorpusSpec;

fn cols(c: &[&str]) -> Vec<String> {
    c.iter().map(|s| (*s).to_string()).collect()
}

#[test]
fn a_sketch_changes_no_replay_plan() {
    let corpus = common::write_corpus(&CorpusSpec::default());
    let specs = vec![
        KeySpec::Exact {
            columns: cols(&["entity_id"]),
        },
        KeySpec::Temporal {
            columns: cols(&["entity_id"]),
            ts_column: "version_ts".into(),
        },
        KeySpec::Range {
            column: "entity_id".into(),
        },
    ];
    let sketch = SketchSpec {
        items: vec![
            cols(&["entity_id"]),
            cols(&["batch_id"]),
            cols(&["entity_id", "batch_id"]),
        ],
        precision: 12,
        minhash: Some(16),
    };
    let build = |sketches: Vec<SketchSpec>| {
        let store = Arc::new(ObjectStoreBackend::memory().unwrap());
        let report = Builder::new(Arc::clone(&store), "idx")
            .build(
                &corpus.data,
                &specs,
                &BuildOptions {
                    schema: Some(Arc::clone(&corpus.schema)),
                    max_segment_row_groups: 5,
                    sketches,
                    ..BuildOptions::default()
                },
            )
            .unwrap();
        assert_eq!(report.sequence, Some(1));
        Snapshot::open(store, "idx").unwrap()
    };
    let plain = build(Vec::new());
    let sketched = build(vec![sketch]);
    assert!(
        sketched
            .manifest()
            .segments
            .iter()
            .any(|s| s.kind.as_deref() == Some("sketch")),
        "the sketched index holds sketch segments (non-vacuity)"
    );
    let source = corpus.source();
    let catalog_of = |s: &Snapshot<ObjectStoreBackend>| {
        let schema = source.schema(&s.manifest().source.root).unwrap();
        Arc::new(Catalog::from_manifest(s.manifest(), &source, schema).unwrap())
    };
    let (ca, cb) = (catalog_of(&plain), catalog_of(&sketched));
    let pa = SegmentPlanner::from_snapshot(&plain, Arc::clone(&ca)).unwrap();
    let pb = SegmentPlanner::from_snapshot(&sketched, Arc::clone(&cb)).unwrap();
    let runs = common::entity_runs(&corpus, 16, 5);
    let (keys, workload) = common::record(&["entity_id"], &runs);
    let total = ca.len() as u64;
    let mut pruned = 0;
    let mut configs: Vec<Vec<KeySpec>> = specs.iter().map(|s| vec![s.clone()]).collect();
    configs.push(specs.clone());
    for config in &configs {
        for run in workload.runs() {
            let rk = workload.run_keys(&keys, run).unwrap();
            let a = PlanSource::plan(&pa, config, &rk).unwrap();
            let b = PlanSource::plan(&pb, config, &rk).unwrap();
            assert_eq!(a, b, "{:?} {}", config, run.run_id);
            if a.row_groups_selected < total {
                pruned += 1;
            }
        }
    }
    assert!(pruned > 0, "some plan must prune (non-vacuity)");

    let ra = Replayer::for_snapshot(&source, &plain).unwrap();
    let rb = Replayer::for_snapshot(&source, &sketched).unwrap();
    assert_eq!(ra.current_specs(), specs.as_slice());
    assert_eq!(ra.current_specs(), rb.current_specs());
    assert_eq!(
        ra.replay_current(&workload, &keys).unwrap(),
        rb.replay_current(&workload, &keys).unwrap()
    );
    let hypothetical = vec![KeySpec::Exact {
        columns: cols(&["entity_id", "batch_id"]),
    }];
    assert_eq!(
        ra.replay_specs(&hypothetical, &workload, &keys).unwrap(),
        rb.replay_specs(&hypothetical, &workload, &keys).unwrap()
    );
}
