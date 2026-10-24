//! Exact decimal totals against the benchmark's own oracle.
//!
//! Ignored by default: it needs a corpus written by `bellwether-bench`
//! (`generate`, `workload`, then `run`, which records each run's exact
//! `amount` total over the rows of its keys). For every recorded run, the
//! corpus is reconciled against itself under workload coverage on that run's
//! keys, and the left total must equal the oracle's decimal string:
//!
//! ```text
//! BELLWETHER_BENCH_CORPUS=<dir> cargo test -p bellwether-verify --test bench_corpus -- --ignored
//! ```

#![allow(clippy::unwrap_used, missing_docs)]

use std::fs::File;
use std::path::PathBuf;
use std::time::Duration;

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::{DataType, UInt64Type};
use bellwether_core::encoding::{encode_tuple_as, Value};
use bellwether_source::ParquetSource;
use bellwether_types::Hash;
use bellwether_verify::{CoverageRequest, Options, ReconcileSpec, Reconciler, Side, WorkloadKeys};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn corpus() -> Option<PathBuf> {
    std::env::var_os("BELLWETHER_BENCH_CORPUS").map(PathBuf::from)
}

fn run_keys(path: &PathBuf) -> Vec<Vec<u8>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
        .unwrap()
        .build()
        .unwrap();
    let mut keys = Vec::new();
    for batch in reader {
        let batch: RecordBatch = batch.unwrap();
        let ids = batch
            .column_by_name("entity_id")
            .unwrap()
            .as_primitive::<UInt64Type>();
        for id in ids.iter().flatten() {
            keys.push(encode_tuple_as(&[Value::UInt(id)], &[DataType::UInt64]).unwrap());
        }
    }
    keys
}

#[test]
#[ignore = "needs BELLWETHER_BENCH_CORPUS, written by bellwether-bench generate, workload, and run"]
fn totals_equal_the_bench_oracle_for_every_recorded_run() {
    let Some(dir) = corpus() else {
        eprintln!("BELLWETHER_BENCH_CORPUS is not set; nothing to check");
        return;
    };
    let results: serde_json::Value =
        serde_json::from_reader(File::open(dir.join("results.json")).unwrap()).unwrap();
    let data = dir.join("data").to_str().unwrap().to_string();
    let source = ParquetSource::new();
    let side = Side::new(&source, &data);
    let runs = results["corpora"][0]["runs"].as_array().unwrap();
    assert_ne!(runs.len(), 0);
    for run in runs {
        let id = run["run_id"].as_str().unwrap();
        let keys = run_keys(&dir.join("workload").join(format!("{id}.parquet")));
        let spec = ReconcileSpec::new(["entity_id"])
            .totals(["amount"])
            .coverage(CoverageRequest::Workload(WorkloadKeys {
                workload: Hash::of(id.as_bytes()),
                age: Duration::ZERO,
                keys,
            }));
        let out = Reconciler::new(spec, Options::default())
            .unwrap()
            .run(&side, &side)
            .unwrap();
        assert!(out.pass, "{id}");
        let oracle = &run["oracle"];
        let total = out.totals.first().unwrap();
        assert_eq!(
            total.left.render(),
            oracle["amount_total"].as_str().unwrap(),
            "{id}"
        );
        assert_eq!(
            total.left_rows,
            oracle["matched_rows"].as_u64().unwrap(),
            "{id}"
        );
        eprintln!(
            "{id}: {} rows, total {} (oracle agrees)",
            total.left_rows, total.left
        );
    }
    // The whole corpus against itself, newest version per entity.
    let spec = ReconcileSpec::new(["entity_id"]).latest_by("version_ts");
    let out = Reconciler::new(spec, Options::default())
        .unwrap()
        .run(&side, &side)
        .unwrap();
    assert!(out.pass);
    assert_eq!(
        out.sides[0].rows,
        results["corpora"][0]["rows_total"].as_u64().unwrap()
    );
}
