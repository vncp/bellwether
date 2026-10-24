//! The recorded-workload summary trusts nothing in `workload.json`: every
//! run is read back, counted, and hashed, and any disagreement is an error.

#![allow(clippy::unwrap_used)]

mod common;

use std::fs::File;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether_profile::workload::summarize;
use common::write_bench_workload;
use parquet::arrow::ArrowWriter;

fn edit(dir: &std::path::Path, f: impl FnOnce(&mut serde_json::Value)) {
    let path = dir.join("workload.json");
    let mut v: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    f(&mut v);
    std::fs::write(path, serde_json::to_vec(&v).unwrap()).unwrap();
}

#[test]
fn summary_counts_and_verifies_every_run() {
    let tmp = tempfile::tempdir().unwrap();
    let runs: Vec<Vec<u64>> = (1..=20u64).map(|n| (0..n * n).collect()).collect();
    let hash = write_bench_workload(tmp.path(), &runs);
    let s = summarize(tmp.path()).unwrap();
    assert_eq!(s.hash, hash);
    assert_eq!(s.runs, 20);
    assert_eq!(s.key_columns, ["entity_id"]);
    // Sizes n^2 for n in 1..=20: nearest-rank p50 is the 10th, p95 the 19th.
    assert_eq!(s.sizes, [100, 361, 400]);
    // The manifest file itself may be named.
    assert_eq!(summarize(&tmp.path().join("workload.json")).unwrap(), s);
}

#[test]
fn any_disagreement_with_workload_json_is_an_error() {
    type Mutation = Box<dyn Fn(&mut serde_json::Value)>;
    let cases: Vec<(&str, Mutation)> = vec![
        ("format", Box::new(|v| v["format"] = "other/1".into())),
        (
            "run hash",
            Box::new(|v| v["runs"][0]["hash"] = v["runs"][1]["hash"].clone()),
        ),
        ("key count", Box::new(|v| v["runs"][0]["keys"] = 99.into())),
        (
            "workload hash",
            Box::new(|v| v["hash"] = v["runs"][0]["hash"].clone()),
        ),
        (
            "escaping path",
            Box::new(|v| v["runs"][0]["file"] = "../outside.parquet".into()),
        ),
        ("no runs", Box::new(|v| v["runs"] = serde_json::json!([]))),
    ];
    for (name, mutate) in cases {
        let tmp = tempfile::tempdir().unwrap();
        write_bench_workload(tmp.path(), &[vec![1, 2, 3], vec![4, 5]]);
        edit(tmp.path(), mutate);
        assert!(summarize(tmp.path()).is_err(), "{name} must be refused");
    }
}

#[test]
fn keys_must_be_uint64() {
    let tmp = tempfile::tempdir().unwrap();
    write_bench_workload(tmp.path(), &[vec![1, 2, 3]]);
    let schema = std::sync::Arc::new(Schema::new(vec![Field::new(
        "entity_id",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![std::sync::Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(
        File::create(tmp.path().join("workload/run-0000.parquet")).unwrap(),
        schema,
        None,
    )
    .unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let err = summarize(tmp.path()).unwrap_err();
    assert!(err.to_string().contains("uint64"), "{err}");
}
