//! Replay against a `bellwether-bench` corpus and its oracle-verified
//! results (WP6 deliverable 9). Skipped, with a message, unless
//! `BELLWETHER_BENCH_CORPUS` names a corpus directory holding the bench's
//! `workload.json`, `expected.json`, and a `results.json` written by
//! `bellwether-bench run --out <corpus>/results.json`.
//!
//! - Replay of each baseline equals the bench harness's `raw_scan` and
//!   `parquet_stats` costs run by run: files, row groups, bytes, keys.
//! - Replay of the exact key on the run column equals the bench's exact
//!   oracle run by run, and so does the per-key intersection of a range and
//!   an exact spec (two-sided: no row group missing, none extra).
//! - Every hypothetical spec's plan contains the oracle's row groups
//!   (invariant 1 through replay).
//! - The transient segments of each planted composite key reproduce the
//!   exact fanout `expected.json` records, value by value.

// Fanout counts are far below 2^52, so converting them to the f64 the
// expected.json quantiles are written in is exact.
#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::cast_precision_loss
)]

mod common;

use std::collections::BTreeSet;
use std::path::Path;

use arrow::datatypes::Schema;
use bellwether_replay::{
    nearest_rank, record_bench_workload, Baseline, MemoryKeySets, Replayer, RunKeys,
    RunKeysBuilder, Workload, WorkloadRecorder,
};
use bellwether_source::ParquetSource;
use bellwether_types::{KeySpec, RunCost, Source};
use serde_json::Value;

fn cols(c: &[&str]) -> Vec<String> {
    c.iter().map(|s| (*s).to_string()).collect()
}

fn read_json(path: &Path) -> Option<Value> {
    let bytes = std::fs::read(path).ok()?;
    Some(serde_json::from_slice(&bytes).unwrap())
}

fn setup<'a>(source: &'a ParquetSource, corpus: &Path) -> (Replayer<'a>, MemoryKeySets, Workload) {
    let data = corpus.join("data");
    let replayer = Replayer::unindexed(
        source,
        data.to_str().unwrap(),
        &bellwether_source::Partitioning::None,
    )
    .unwrap();
    let keys = MemoryKeySets::new();
    let mut rec = WorkloadRecorder::new(&keys, replayer.schema().clone());
    let summary = record_bench_workload(&mut rec, &corpus.join("workload.json")).unwrap();
    let workload = rec.finish().unwrap();
    assert_eq!(summary.runs, workload.runs().len());
    (replayer, keys, workload)
}

fn cost_of(run: &Value, strategy: &str) -> Value {
    run["results"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["strategy"] == strategy)
        .unwrap_or_else(|| panic!("results.json has no {strategy} result"))["cost"]
        .clone()
}

fn assert_cost(got: &RunCost, want: &Value, what: &str) {
    let n = |k: &str| want[k].as_u64().unwrap_or_else(|| panic!("{what}: no {k}"));
    assert_eq!(
        (got.files, got.row_groups, got.bytes),
        (n("files"), n("row_groups"), n("bytes")),
        "{what} {}: replay {got:?} != bench {want}",
        got.run_id
    );
    if let Some(k) = want.get("keys").and_then(Value::as_u64) {
        assert_eq!(got.keys, k, "{what} {}: keys", got.run_id);
    }
    assert!(got.exact);
}

#[test]
fn baselines_and_exact_specs_equal_the_bench_oracle_run_by_run() {
    let test = "baselines_and_exact_specs_equal_the_bench_oracle_run_by_run";
    let Some(corpus) = common::bench_corpus(test) else {
        return;
    };
    let Some(results) = read_json(&corpus.join("results.json")) else {
        eprintln!("SKIPPED {test}: no results.json in the corpus; run bellwether-bench run --out <corpus>/results.json");
        return;
    };
    let source = ParquetSource::new();
    let (replayer, keys, workload) = setup(&source, &corpus);
    let bench_runs = results["corpora"][0]["runs"].as_array().unwrap();
    assert_eq!(bench_runs.len(), workload.runs().len());

    let raw = replayer
        .replay_baseline(Baseline::RawScan, &workload, &keys)
        .unwrap();
    let stats = replayer
        .replay_baseline(Baseline::ParquetStats, &workload, &keys)
        .unwrap();
    let exact = KeySpec::Exact {
        columns: cols(&["entity_id"]),
        counts: false,
        bounds: None,
    };
    let range = KeySpec::Range {
        column: "entity_id".into(),
    };
    let exact_costs = replayer
        .replay_specs(std::slice::from_ref(&exact), &workload, &keys)
        .unwrap();
    // Two serving specs: the per-key intersection path.
    let both = replayer
        .replay_specs(&[range.clone(), exact.clone()], &workload, &keys)
        .unwrap();

    for (i, run) in bench_runs.iter().enumerate() {
        let id = run["run_id"].as_str().unwrap();
        assert_eq!(raw[i].run_id, id);
        assert_cost(&raw[i], &cost_of(run, "raw_scan"), "raw_scan");
        assert_cost(&stats[i], &cost_of(run, "parquet_stats"), "parquet_stats");
        assert_cost(
            &exact_costs[i],
            &run["oracle"],
            "exact(entity_id) vs oracle",
        );
        assert_cost(&both[i], &run["oracle"], "range ∩ exact vs oracle");
    }
    eprintln!(
        "{test}: {} runs equal raw_scan, parquet_stats, and the oracle",
        bench_runs.len()
    );
}

#[test]
fn hypothetical_specs_never_report_fewer_row_groups_than_the_oracle() {
    let test = "hypothetical_specs_never_report_fewer_row_groups_than_the_oracle";
    let Some(corpus) = common::bench_corpus(test) else {
        return;
    };
    let source = ParquetSource::new();
    let (replayer, keys, workload) = setup(&source, &corpus);
    let exact = KeySpec::Exact {
        columns: cols(&["entity_id"]),
        counts: false,
        bounds: None,
    };
    let candidates = [
        KeySpec::Bloom {
            columns: cols(&["entity_id"]),
            fpr: 0.05,
            bounds: None,
        },
        KeySpec::Range {
            column: "entity_id".into(),
        },
        // The run column is a prefix of the composite: prefix lookups.
        KeySpec::Exact {
            columns: cols(&["entity_id", "batch_id"]),
            counts: false,
            bounds: None,
        },
        KeySpec::Temporal {
            columns: cols(&["entity_id"]),
            ts_column: "version_ts".into(),
            counts: false,
        },
    ];
    for run in workload.runs() {
        let rk = workload.run_keys(&keys, run).unwrap();
        let oracle = common::cells(
            &replayer
                .plan_of_keys(std::slice::from_ref(&exact), &rk)
                .unwrap(),
        );
        for spec in &candidates {
            let got = common::cells(
                &replayer
                    .plan_of_keys(std::slice::from_ref(spec), &rk)
                    .unwrap(),
            );
            let missing: Vec<_> = oracle.difference(&got).collect();
            assert!(
                missing.is_empty(),
                "{} under {spec:?} misses oracle row groups {missing:?}",
                run.run_id
            );
            if matches!(spec, KeySpec::Exact { .. } | KeySpec::Temporal { .. }) {
                assert_eq!(got, oracle, "{} under {spec:?}", run.run_id);
            }
        }
    }
}

/// Every distinct tuple over `columns` in the corpus, canonical at the
/// dataset types, read one row group at a time.
fn distinct(
    replayer: &Replayer<'_>,
    source: &dyn Source,
    schema: &Schema,
    columns: &[&str],
) -> RunKeys {
    let c = cols(columns);
    let mut b = RunKeysBuilder::new(&c, schema).unwrap();
    for rg in replayer.catalog().row_groups() {
        b.push_batch(&source.read_columns(&rg.meta, &c).unwrap())
            .unwrap();
    }
    b.finish()
}

#[test]
fn transient_segments_reproduce_the_planted_fanout() {
    let test = "transient_segments_reproduce_the_planted_fanout";
    let Some(corpus) = common::bench_corpus(test) else {
        return;
    };
    let Some(expected) = read_json(&corpus.join("expected.json")) else {
        eprintln!("SKIPPED {test}: no expected.json; run bellwether-bench ground-truth");
        return;
    };
    let source = ParquetSource::new();
    let (replayer, _keys, _workload) = setup(&source, &corpus);
    let schema = replayer.schema().clone();
    let catalog = replayer.catalog().clone();
    let mut checked = 0;
    for ap in expected["anti_patterns"].as_array().unwrap() {
        for field in ["fanout", "fix_fanout"] {
            let Some(f) = ap.get(field).filter(|f| !f.is_null()) else {
                continue;
            };
            let columns: Vec<&str> = f["columns"]
                .as_array()
                .unwrap()
                .iter()
                .map(|c| c.as_str().unwrap())
                .collect();
            let spec = KeySpec::Exact {
                columns: cols(&columns),
                counts: false,
                bounds: None,
            };
            let index = replayer.transient(&spec).unwrap();
            let tuples = distinct(&replayer, &source, &schema, &columns);
            let probes: Vec<Vec<u8>> = tuples.tuples().map(<[u8]>::to_vec).collect();
            let (mut files, mut rgs) = (Vec::new(), Vec::new());
            for chunk in probes.chunks(8192) {
                let found = index.get(chunk).unwrap();
                assert_eq!(
                    found.len(),
                    chunk.len(),
                    "every value of {columns:?} is indexed"
                );
                for bitmap in found.values() {
                    let fs: BTreeSet<usize> = bitmap
                        .iter()
                        .map(|id| catalog.get(id).unwrap().file)
                        .collect();
                    files.push(fs.len() as u64);
                    rgs.push(bitmap.len());
                }
            }
            files.sort_unstable();
            rgs.sort_unstable();
            let q = |v: &[u64]| {
                [
                    nearest_rank(v, 0.5),
                    nearest_rank(v, 0.95),
                    *v.last().unwrap(),
                ]
                .map(|x| x as f64)
            };
            let want = |k: &str| -> [f64; 3] {
                let a = f[k].as_array().unwrap();
                [
                    a[0].as_f64().unwrap(),
                    a[1].as_f64().unwrap(),
                    a[2].as_f64().unwrap(),
                ]
            };
            let rule = ap["rule"].as_str().unwrap();
            assert_eq!(
                tuples.len() as u64,
                f["distinct"].as_u64().unwrap(),
                "{rule} {field} {columns:?}: distinct values"
            );
            assert_eq!(
                q(&files),
                want("files_per_value"),
                "{rule} {field} {columns:?}: files"
            );
            assert_eq!(
                q(&rgs),
                want("row_groups_per_value"),
                "{rule} {field} {columns:?}: row groups"
            );
            assert_eq!(
                f["files_total"].as_u64().unwrap(),
                catalog.files().len() as u64
            );
            assert_eq!(
                f["row_groups_total"].as_u64().unwrap(),
                catalog.len() as u64
            );
            checked += 1;
            eprintln!(
                "{test}: {rule} {field} {columns:?}: {} values, files {:?}, row groups {:?}",
                tuples.len(),
                q(&files),
                q(&rgs)
            );
        }
    }
    assert!(checked >= 4, "expected.json held only {checked} fanouts");
}
