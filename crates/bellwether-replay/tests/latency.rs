//! Latency budget (`docs/evolve.md` section 12, `slo::REPLAY_RUN_P50_BUDGET`):
//! one replay of one recorded run under one candidate key spec, with the
//! candidate's transient segments already built and cached, at the
//! workload's p50 run size.
//!
//! The timed operation is `Replayer::cost_of_keys` for the run's key set
//! (already loaded): planning through the transient segments, materializing
//! and verifying the plan, and costing it. The budget is asserted only in
//! optimized builds; a debug build prints the measurement and skips the
//! assertion, because unoptimized timings say nothing about the budget.
//!
//! `BELLWETHER_REPLAY_LATENCY_CORPUS` names a bench corpus with a workload
//! to measure on; without it the test measures an in-test corpus only. CI's
//! "latency budgets" job generates the bench small corpus and runs this test
//! optimized (`cargo test --release`), so the budget is enforced there.
//!
//! Each case also holds the same run, replayed against a committed index
//! built by the build crate's `Builder` through its planner
//! (`Replayer::for_snapshot`, which opens the committed segments by range
//! and reads footers on every plan), to the same budget.

#![allow(clippy::unwrap_used, missing_docs, clippy::cast_precision_loss)]

mod common;

use std::path::Path;
use std::time::{Duration, Instant};

use std::sync::Arc;

use bellwether_build::{BuildOptions, Builder};
use bellwether_replay::{
    record_bench_workload, KeySetSource, MemoryKeySets, Replayer, Workload, WorkloadRecorder,
    REPLAY_RUN_P50_BUDGET,
};
use bellwether_source::ParquetSource;
use bellwether_store::{ObjectStoreBackend, Snapshot};
use bellwether_types::{KeySpec, Source};
use common::CorpusSpec;

/// Median time of `reps` replays of the run whose key count is the
/// workload's p50, and that run's key count.
fn time_p50_run(
    replayer: &Replayer<'_>,
    spec: &KeySpec,
    workload: &Workload,
    keys: &dyn KeySetSource,
    reps: usize,
) -> (Duration, u64) {
    let p50 = workload.keys_p50_p95_max()[0];
    let run = workload.runs().iter().find(|r| r.keys == p50).unwrap();
    let rk = workload.run_keys(keys, run).unwrap();
    let specs = std::slice::from_ref(spec);
    // Build and cache the candidate's segments first: the budget is for a
    // replay, not a build.
    replayer.transient(spec).unwrap();
    replayer.cost_of_keys(specs, &run.run_id, &rk).unwrap();
    let mut times: Vec<Duration> = (0..reps)
        .map(|_| {
            let t = Instant::now();
            let c = replayer.cost_of_keys(specs, &run.run_id, &rk).unwrap();
            let e = t.elapsed();
            assert_eq!(c.keys, p50);
            e
        })
        .collect();
    times.sort_unstable();
    (times[times.len() / 2], p50)
}

/// Builds a committed index of `spec` over `data` with the build crate and
/// judges the median replay of the workload's p50 run through its planner.
fn judge_committed(
    what: &str,
    source: &dyn Source,
    data: &str,
    spec: &KeySpec,
    workload: &Workload,
    keys: &dyn KeySetSource,
    reps: usize,
) {
    let store = Arc::new(ObjectStoreBackend::memory().unwrap());
    let schema = source.schema(data).unwrap();
    Builder::new(Arc::clone(&store), "idx")
        .build(
            data,
            std::slice::from_ref(spec),
            &BuildOptions {
                schema: Some(schema),
                ..BuildOptions::default()
            },
        )
        .unwrap();
    let snapshot = Snapshot::open(Arc::clone(&store), "idx").unwrap();
    let replayer = Replayer::for_snapshot(source, &snapshot).unwrap();
    assert_eq!(replayer.current_specs(), std::slice::from_ref(spec));
    let p50 = workload.keys_p50_p95_max()[0];
    let run = workload.runs().iter().find(|r| r.keys == p50).unwrap();
    let rk = workload.run_keys(keys, run).unwrap();
    let specs = std::slice::from_ref(spec);
    replayer.cost_of_keys(specs, &run.run_id, &rk).unwrap();
    let mut times: Vec<Duration> = (0..reps)
        .map(|_| {
            let t = Instant::now();
            replayer.cost_of_keys(specs, &run.run_id, &rk).unwrap();
            t.elapsed()
        })
        .collect();
    times.sort_unstable();
    judge(
        &format!("{what}, committed index through the build planner"),
        times[times.len() / 2],
        p50,
    );
}

fn judge(what: &str, median: Duration, keys: u64) {
    let optimized = !cfg!(debug_assertions);
    eprintln!(
        "{what}: p50 run ({keys} keys) replays in {:.3} ms (budget {} ms; {})",
        median.as_secs_f64() * 1e3,
        REPLAY_RUN_P50_BUDGET.as_millis(),
        if optimized {
            "asserted"
        } else {
            "debug build: measured, not asserted"
        }
    );
    if optimized {
        assert!(
            median < REPLAY_RUN_P50_BUDGET,
            "{what}: {median:?} exceeds {REPLAY_RUN_P50_BUDGET:?}"
        );
    }
}

#[test]
fn replay_of_the_p50_run_under_one_candidate_meets_the_budget() {
    let corpus = common::write_corpus(&CorpusSpec {
        files: 8,
        row_groups_per_file: 4,
        rows_per_row_group: 500,
        entities: 20_000,
        ..CorpusSpec::default()
    });
    let source = corpus.source();
    let replayer = Replayer::unindexed(
        &source,
        &corpus.data,
        &bellwether_source::Partitioning::None,
    )
    .unwrap();
    let runs = common::entity_runs(&corpus, 24, 17);
    let (keys, workload) = common::record(&["entity_id"], &runs);
    let spec = KeySpec::Exact {
        columns: vec!["entity_id".into()],
    };
    let (median, n) = time_p50_run(&replayer, &spec, &workload, &keys, 25);
    judge("in-test corpus, one cached candidate", median, n);
    judge_committed(
        "in-test corpus",
        &source,
        &corpus.data,
        &spec,
        &workload,
        &keys,
        25,
    );
}

#[test]
fn replay_on_a_bench_corpus_meets_the_budget() {
    let test = "replay_on_a_bench_corpus_meets_the_budget";
    let Some(dir) = std::env::var_os("BELLWETHER_REPLAY_LATENCY_CORPUS") else {
        eprintln!(
            "SKIPPED {test}: set BELLWETHER_REPLAY_LATENCY_CORPUS to a bellwether-bench corpus \
             with a workload (the CI job uses the small recipe)"
        );
        return;
    };
    let corpus = Path::new(&dir);
    let source = ParquetSource::new();
    let replayer = Replayer::unindexed(
        &source,
        corpus.join("data").to_str().unwrap(),
        &bellwether_source::Partitioning::None,
    )
    .unwrap();
    let keys = MemoryKeySets::new();
    let mut rec = WorkloadRecorder::new(&keys, replayer.schema().clone());
    record_bench_workload(&mut rec, &corpus.join("workload.json")).unwrap();
    let workload = rec.finish().unwrap();
    eprintln!(
        "{test}: {} files, {} row groups, {} rows; workload keys p50/p95/max {:?}",
        replayer.catalog().files().len(),
        replayer.catalog().len(),
        replayer.catalog().rows_total(),
        workload.keys_p50_p95_max()
    );
    for spec in [
        KeySpec::Exact {
            columns: vec!["entity_id".into()],
        },
        KeySpec::Bloom {
            columns: vec!["entity_id".into()],
            fpr: 0.01,
        },
    ] {
        let t = Instant::now();
        let idx = replayer.transient(&spec).unwrap();
        eprintln!(
            "{test}: built {} in {:.1} s: {} segments, {} bytes",
            spec.name(),
            t.elapsed().as_secs_f64(),
            idx.stats().segments,
            idx.bytes()
        );
        let (median, n) = time_p50_run(&replayer, &spec, &workload, &keys, 25);
        judge(
            &format!("bench corpus, {}, one cached candidate", spec.name()),
            median,
            n,
        );
        judge_committed(
            &format!("bench corpus, {}", spec.name()),
            &source,
            corpus.join("data").to_str().unwrap(),
            &spec,
            &workload,
            &keys,
            25,
        );
        // The tail, reported: no budget of milliseconds covers 100,000 keys.
        for q in [1usize, 2] {
            let size = workload.keys_p50_p95_max()[q];
            let run = workload.runs().iter().find(|r| r.keys == size).unwrap();
            let rk = workload.run_keys(&keys, run).unwrap();
            let t = Instant::now();
            replayer
                .cost_of_keys(std::slice::from_ref(&spec), &run.run_id, &rk)
                .unwrap();
            eprintln!(
                "{test}: {} run of {size} keys replays in {:.1} ms",
                if q == 1 { "p95" } else { "max" },
                t.elapsed().as_secs_f64() * 1e3
            );
        }
    }
}
