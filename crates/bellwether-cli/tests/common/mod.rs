//! Shared helpers for the CLI tests: tiny corpora written with the
//! `parquet` crate, the binary, and checks that every record a command
//! prints is exactly what the ledger stores.

#![allow(dead_code, clippy::unwrap_used, clippy::expect_used)]

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use assert_cmd::Command;
use bellwether::types::{Hash, Record};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde_json::Value;

/// The `bellwether` binary.
pub fn bw() -> Command {
    Command::cargo_bin("bellwether").unwrap()
}

/// `files` files × `rgs` row groups × `rows` rows; row `k` has id
/// `k % entities`, batch `k / 50`, ts `k` seconds, payload `p<k>`.
pub fn corpus(dir: &Path, files: usize, rgs: usize, rows: usize, entities: i64) {
    corpus_with(dir, files, rgs, rows, |k| k % entities);
}

/// [`corpus`] with row `k`'s id `(k * 37) % entities`: consecutive rows get
/// ids far apart, so every row group's id range spans nearly the whole key
/// space and the files' own min/max statistics skip nothing for a point
/// lookup. The loop tests use it: on ids sorted into row groups, min/max
/// pruning already reads what `exact:id` would, so `why` recommends nothing
/// and `fix` refuses `nothing_to_gain`. `entities` must not be a multiple
/// of 37.
pub fn scattered(dir: &Path, files: usize, rgs: usize, rows: usize, entities: i64) {
    assert_ne!(entities % 37, 0, "37 must not divide entities");
    corpus_with(dir, files, rgs, rows, |k| (k * 37) % entities);
}

fn corpus_with(dir: &Path, files: usize, rgs: usize, rows: usize, id: impl Fn(i64) -> i64) {
    std::fs::create_dir_all(dir).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("batch", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let mut k: i64 = 0;
    for f in 0..files {
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(usize::MAX))
            .build();
        let file = File::create(dir.join(format!("part-{f:03}.parquet"))).unwrap();
        let mut w = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for _ in 0..rgs {
            let n = i64::try_from(rows).unwrap();
            let ks: Vec<i64> = (k..k + n).collect();
            k += n;
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from_iter_values(ks.iter().map(|k| id(*k)))) as ArrayRef,
                    Arc::new(Int64Array::from_iter_values(ks.iter().map(|k| k / 50))),
                    Arc::new(
                        TimestampMicrosecondArray::from_iter_values(
                            ks.iter().map(|k| k * 1_000_000),
                        )
                        .with_timezone("UTC"),
                    ),
                    Arc::new(StringArray::from_iter_values(
                        ks.iter().map(|k| format!("p{k:06}")),
                    )),
                ],
            )
            .unwrap();
            w.write(&batch).unwrap();
            w.flush().unwrap();
        }
        w.close().unwrap();
    }
}

/// A key file with one Int64 column.
pub fn keys(path: &Path, name: &str, keys: &[i64]) {
    let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(keys.to_vec())) as ArrayRef],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// A scaled-down bench recipe layout that plants `batch_stamp_key` (three
/// days, eight files per day, late rows; the bench's own `recipe_like`), and
/// a recorded workload of `runs` runs. Returns the data root.
pub fn planted(corpus_dir: &Path, runs: u32) -> PathBuf {
    use bellwether_bench::corpus::GenParams;
    use bellwether_bench::workload::WorkloadParams;
    let params = GenParams {
        seed: 7,
        files: 24,
        row_groups_per_file: 2,
        rows_per_row_group: 250,
        entities: 8_000,
        churn: 0.5,
        days: 3,
        payload_bytes: 32,
        late_fraction: 0.05,
    };
    bellwether_bench::generate::generate(&params, corpus_dir).unwrap();
    bellwether_bench::workload::generate_workload(
        corpus_dir,
        &WorkloadParams {
            seed: 11,
            runs,
            p50: 20,
            p95: 200,
            max: 400,
        },
    )
    .unwrap();
    corpus_dir.join("data")
}

/// Runs the binary with `args`, asserts exit code `code`, returns stdout.
pub fn run(args: &[&str], code: i32) -> String {
    let out = bw().args(args).output().unwrap();
    assert_eq!(
        out.status.code(),
        Some(code),
        "args {args:?}\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

pub fn path(p: &Path) -> &str {
    p.to_str().unwrap()
}

pub fn index_dir(root: &Path) -> PathBuf {
    root.join(".bellwether")
}

/// Every `{"hash", "record"}` object in `v`, each parsed into a
/// `bellwether_types::Record` and checked against the ledger: the record's
/// canonical bytes hash to `hash` and equal the file at
/// `ledger/<hash>.rec`. Returns the records found.
pub fn check_records(index: &Path, v: &Value) -> Vec<(Hash, Record)> {
    let mut found = Vec::new();
    walk(v, &mut |obj| {
        if obj.len() == 2 && obj.contains_key("hash") && obj.contains_key("record") {
            let hash: Hash = serde_json::from_value(obj["hash"].clone()).unwrap();
            let record: Record = serde_json::from_value(obj["record"].clone()).unwrap();
            let bytes = serde_json::to_vec(&record).unwrap();
            assert_eq!(Hash::of(&bytes), hash, "hash of the printed record");
            let stored =
                std::fs::read(index.join("ledger").join(format!("{}.rec", hash.to_hex()))).unwrap();
            assert_eq!(stored, bytes, "printed record equals the stored bytes");
            found.push((hash, record));
        }
    });
    found
}

fn walk(v: &Value, f: &mut dyn FnMut(&serde_json::Map<String, Value>)) {
    match v {
        Value::Object(m) => {
            f(m);
            for x in m.values() {
                walk(x, f);
            }
        }
        Value::Array(a) => {
            for x in a {
                walk(x, f);
            }
        }
        _ => {}
    }
}

/// Parses one `--json` line into `T` and asserts the line is exactly
/// `serde_json::to_string(&T)` plus a newline: the CLI prints the facade's
/// typed result and nothing else (ADR 010).
pub fn typed<T: serde::de::DeserializeOwned + serde::Serialize>(stdout: &str) -> T {
    assert!(stdout.ends_with('\n'), "one line, newline-terminated");
    assert_eq!(stdout.matches('\n').count(), 1, "exactly one line");
    let t: T = serde_json::from_str(stdout).unwrap();
    assert_eq!(serde_json::to_string(&t).unwrap() + "\n", stdout);
    t
}
