//! Replay through the build crate's planner (WP4): a committed index built
//! by `bellwether_build::Builder`, and this crate's transient segments for
//! the same specs, plan every run to the same `ScanPlan`.
//!
//! For each configuration (exact, temporal, composite prefix, range, bloom,
//! and range ∩ exact) and every run, three whole plans are compared, files,
//! row groups, bytes, and residual:
//!
//! 1. WP4's `Planner` over the committed segments (`PlanSource::plan`);
//! 2. WP4's `Planner` with this crate's transient segments in their place
//!    (`PlanSource::with_transient`, so `Planner::plan_with_transient`);
//! 3. this crate's `SegmentPlanner` with the same transient segments.
//!
//! Every side is taken over the one snapshot's catalog (the builder
//! canonicalizes the source root, so fingerprints name canonical paths).
//! The replayer over the snapshot plans through WP4, and its costs equal
//! those through `SegmentPlanner::from_snapshot`, run by run.

#![allow(clippy::unwrap_used, missing_docs, clippy::too_many_lines)]

mod common;

use std::sync::Arc;

use bellwether_build::{BuildOptions, Builder, Planner};
use bellwether_replay::{
    Catalog, PlanSource, Replayer, SegmentPlanner, TransientIndex, TransientLimits,
};
use bellwether_store::{ObjectStoreBackend, Snapshot};
use bellwether_types::{KeySpec, Source};
use common::CorpusSpec;

fn cols(c: &[&str]) -> Vec<String> {
    c.iter().map(|s| (*s).to_string()).collect()
}

fn specs() -> Vec<KeySpec> {
    vec![
        KeySpec::Exact {
            columns: cols(&["entity_id"]),
        },
        KeySpec::Temporal {
            columns: cols(&["entity_id"]),
            ts_column: "version_ts".into(),
        },
        KeySpec::Exact {
            columns: cols(&["entity_id", "batch_id"]),
        },
        KeySpec::Range {
            column: "entity_id".into(),
        },
        KeySpec::Bloom {
            columns: cols(&["entity_id"]),
            fpr: 0.01,
        },
    ]
}

#[test]
fn committed_and_transient_segments_plan_identically_through_wp4_and_this_crate() {
    let corpus = common::write_corpus(&CorpusSpec {
        null_row_groups: vec![(1, 2)],
        ..CorpusSpec::default()
    });
    let store = Arc::new(ObjectStoreBackend::memory().unwrap());
    let specs = specs();
    let report = Builder::new(Arc::clone(&store), "idx")
        .build(
            &corpus.data,
            &specs,
            &BuildOptions {
                schema: Some(Arc::clone(&corpus.schema)),
                max_segment_row_groups: 5,
                ..BuildOptions::default()
            },
        )
        .unwrap();
    assert_eq!(report.sequence, Some(1));
    let snapshot = Snapshot::open(Arc::clone(&store), "idx").unwrap();
    let planner = Planner::from_snapshot(snapshot.clone()).unwrap();
    assert_eq!(PlanSource::specs(&planner), specs, "every spec is complete");
    assert!(
        snapshot.manifest().segments.len() > specs.len(),
        "the committed index is split"
    );
    let source = corpus.source();
    let schema = source.schema(&snapshot.manifest().source.root).unwrap();
    let catalog = Arc::new(Catalog::from_manifest(snapshot.manifest(), &source, schema).unwrap());
    let segment_planner = SegmentPlanner::new(Arc::clone(&catalog));
    let runs = common::entity_runs(&corpus, 16, 5);
    let (keys, workload) = common::record(&["entity_id"], &runs);
    let limits = TransientLimits {
        max_segment_rows: 450,
        ..TransientLimits::default()
    };
    let mut configs: Vec<Vec<KeySpec>> = specs.iter().map(|s| vec![s.clone()]).collect();
    configs.push(vec![specs[3].clone(), specs[0].clone()]);
    let total = catalog.len() as u64;
    let mut pruned = 0;
    for config in &configs {
        let transients: Vec<Arc<TransientIndex>> = config
            .iter()
            .map(|s| Arc::new(TransientIndex::build(&source, &catalog, s, &limits).unwrap()))
            .collect();
        let wp4_transient = PlanSource::with_transient(&planner, &transients).unwrap();
        let ours_transient = segment_planner.with_transient(&transients).unwrap();
        for run in workload.runs() {
            let rk = workload.run_keys(&keys, run).unwrap();
            let committed = PlanSource::plan(&planner, config, &rk, None).unwrap();
            let via_wp4 = wp4_transient.plan(config, &rk, None).unwrap();
            let via_ours = ours_transient.plan(config, &rk, None).unwrap();
            let what = format!(
                "{:?} {}",
                config.iter().map(KeySpec::name).collect::<Vec<_>>(),
                run.run_id
            );
            assert_eq!(committed, via_wp4, "{what}: WP4 committed vs transient");
            assert_eq!(committed, via_ours, "{what}: WP4 committed vs this crate");
            catalog.verify_plan(&committed).unwrap();
            if committed.row_groups_selected < total {
                pruned += 1;
            }
        }
    }
    assert!(pruned > 0, "some plan must prune (non-vacuity)");

    // The replayer over the snapshot plans through WP4; its costs equal the
    // in-crate planner's over the same committed segments, current and
    // hypothetical, run by run.
    let via_wp4 = Replayer::for_snapshot(&source, &snapshot).unwrap();
    assert_eq!(via_wp4.current_specs(), specs.as_slice());
    let ours = Replayer::new(
        &source,
        Arc::clone(&catalog),
        Box::new(SegmentPlanner::from_snapshot(&snapshot, Arc::clone(&catalog)).unwrap()),
        specs.clone(),
    )
    .unwrap();
    assert_eq!(
        via_wp4.replay_current(&workload, &keys).unwrap(),
        ours.replay_current(&workload, &keys).unwrap()
    );
    let hypothetical = vec![
        specs[3].clone(),
        KeySpec::Bloom {
            columns: cols(&["entity_id", "batch_id"]),
            fpr: 0.05,
        },
    ];
    assert_eq!(
        via_wp4
            .replay_specs(&hypothetical, &workload, &keys)
            .unwrap(),
        ours.replay_specs(&hypothetical, &workload, &keys).unwrap()
    );
}
