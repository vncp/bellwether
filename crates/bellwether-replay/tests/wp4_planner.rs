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

use bellwether_build::{BuildOptions, Builder, Lookup as BuildLookup, Planner};
use bellwether_replay::{
    Catalog, KeyInput, MemoryKeySets, PlanSource, Replayer, RunQuery, SegmentPlanner,
    TransientIndex, TransientLimits, WorkloadRecorder,
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
            let committed = PlanSource::plan(&planner, config, &rk).unwrap();
            let via_wp4 = wp4_transient.plan(config, &rk).unwrap();
            let via_ours = ours_transient.plan(config, &rk).unwrap();
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

    // Temporal replay (`latest` and `asof`) of a temporal candidate equals
    // the build crate's `Planner` Latest/AsOf plan after indexing the same
    // spec: row-group set, bytes, and residual, both unindexed (transient
    // segments via `SegmentPlanner`) and over the snapshot (`Planner`).
    let temporal_spec = specs[1].clone();
    let temporal_configs = [
        vec![temporal_spec.clone()],
        vec![temporal_spec.clone(), specs[3].clone()],
    ];
    // File 1 starts at 1_767_225_600_000_000 + 3_600_000_000; an instant at
    // the end of file 1 excludes later files' versions.
    let asof_ts = 1_767_225_600_000_000_i64 + 2 * 3_600_000_000;
    // A lookback window of one hour, ending at each key's newest version or
    // at the asof instant, plans through the same code in both planners.
    let hour: i64 = 3_600_000_000;
    for query in [
        RunQuery::Latest,
        RunQuery::Asof { ts: asof_ts },
        RunQuery::Window {
            asof: None,
            lookback: hour,
        },
        RunQuery::Window {
            asof: Some(asof_ts),
            lookback: hour,
        },
    ] {
        let t_keys = MemoryKeySets::new();
        let mut rec = WorkloadRecorder::new(&t_keys, Arc::clone(&corpus.schema));
        for (id, tuples) in &runs {
            rec.record_query(
                id,
                KeyInput::Batch(&common::key_batch(&["entity_id"], tuples)),
                query,
                None,
            )
            .unwrap();
        }
        let t_workload = rec.finish().unwrap();
        let mut narrower_than_locate = 0usize;
        for config in &temporal_configs {
            let transients: Vec<Arc<TransientIndex>> = config
                .iter()
                .map(|s| Arc::new(TransientIndex::build(&source, &catalog, s, &limits).unwrap()))
                .collect();
            let wp4_transient = PlanSource::with_transient(&planner, &transients).unwrap();
            let ours_transient = segment_planner.with_transient(&transients).unwrap();
            for run in t_workload.runs() {
                let rk = t_workload.run_keys(&t_keys, run).unwrap();
                let lookup = match &query {
                    RunQuery::Latest => BuildLookup::Latest {
                        columns: rk.columns().to_vec(),
                        keys: rk.tuples().map(<[u8]>::to_vec).collect(),
                    },
                    RunQuery::Asof { ts } => BuildLookup::AsOf {
                        columns: rk.columns().to_vec(),
                        keys: rk.tuples().map(<[u8]>::to_vec).collect(),
                        ts: *ts,
                    },
                    RunQuery::Window { asof, lookback } => BuildLookup::Window {
                        columns: rk.columns().to_vec(),
                        keys: rk.tuples().map(<[u8]>::to_vec).collect(),
                        asof: *asof,
                        lookback: *lookback,
                    },
                    RunQuery::Locate => unreachable!(),
                };
                let build_plan = planner.plan(config, &lookup).unwrap();
                let committed = PlanSource::plan_query(&planner, config, &rk, &query).unwrap();
                let via_wp4 = wp4_transient.plan_query(config, &rk, &query).unwrap();
                let via_ours = ours_transient.plan_query(config, &rk, &query).unwrap();
                assert_eq!(build_plan, committed);
                assert_eq!(build_plan, via_wp4);
                assert_eq!(build_plan, via_ours);
                catalog.verify_plan(&via_ours).unwrap();
                let locate_plan = PlanSource::plan(&planner, config, &rk).unwrap();
                if build_plan.bytes < locate_plan.bytes {
                    narrower_than_locate += 1;
                }
            }
            assert_eq!(
                via_wp4.replay_specs(config, &t_workload, &t_keys).unwrap(),
                ours.replay_specs(config, &t_workload, &t_keys).unwrap()
            );
        }
        assert!(
            narrower_than_locate > 0,
            "{query:?} must prune beyond locate on repeated keys"
        );
    }

    // Unindexed folder (`Catalog::discover`, manifest version "0"): the
    // transient temporal replay's row-group cells and bytes equal the
    // committed `Planner`'s Latest/AsOf plan after indexing the same spec.
    let unindexed = Replayer::unindexed(&source, &snapshot.manifest().source.root).unwrap();
    for query in [
        RunQuery::Latest,
        RunQuery::Asof { ts: asof_ts },
        RunQuery::Window {
            asof: None,
            lookback: hour,
        },
        RunQuery::Window {
            asof: Some(asof_ts),
            lookback: hour,
        },
    ] {
        for run in workload.runs() {
            let rk = workload.run_keys(&keys, run).unwrap();
            let unindexed_plan = unindexed
                .plan_of_query(std::slice::from_ref(&temporal_spec), &rk, &query)
                .unwrap();
            let indexed_plan = via_wp4
                .plan_of_query(std::slice::from_ref(&temporal_spec), &rk, &query)
                .unwrap();
            assert_eq!(common::cells(&unindexed_plan), common::cells(&indexed_plan));
            assert_eq!(unindexed_plan.bytes, indexed_plan.bytes);
            assert_eq!(
                unindexed_plan.row_groups_selected,
                indexed_plan.row_groups_selected
            );
            assert_eq!(unindexed_plan.residual, indexed_plan.residual);
        }
    }
}
