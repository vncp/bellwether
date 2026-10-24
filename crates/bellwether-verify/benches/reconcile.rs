//! Reconcile throughput (rows per second per side) and the memory bound.
//!
//! With `BELLWETHER_BENCH_CORPUS=<dir>` it reconciles a corpus written by
//! `bellwether-bench generate` against itself (`<dir>/data`, key
//! `entity_id`, newest by `version_ts`, `amount` totals by `status`);
//! otherwise it writes a synthetic ledger of the same shape. The budget is
//! `BELLWETHER_VERIFY_BUDGET` bytes (default: the crate default), so the
//! multi-pass path can be timed too. No budget is asserted: docs/benchmark.md
//! states none for reconcile.
//!
//! ```text
//! cargo bench -p bellwether-verify --bench reconcile
//! ```

#![allow(missing_docs, clippy::unwrap_used)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Decimal128Array, RecordBatch, StringArray, TimestampMicrosecondArray, UInt64Array,
};
use bellwether_source::ParquetSource;
use bellwether_verify::{Options, ReconcileSpec, Reconciler, Side, DEFAULT_MEMORY_BUDGET};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// A synthetic ledger: `files` files of `groups` row groups of `rows` rows,
/// entities re-versioned across files.
fn synthetic(dir: &Path, files: u64, groups: u64, rows: u64, entities: u64) {
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();
    let statuses = ["new", "open", "held", "paid", "void"];
    for f in 0..files {
        let n = groups * rows;
        let ids: Vec<u64> = (0..n)
            .map(|i| (f * n + i).wrapping_mul(2_654_435_761) % entities)
            .collect();
        let batch = RecordBatch::try_from_iter([
            (
                "entity_id",
                Arc::new(UInt64Array::from(ids.clone())) as ArrayRef,
            ),
            (
                "version_ts",
                Arc::new(
                    TimestampMicrosecondArray::from(
                        (0..n)
                            .map(|i| i64::try_from(f * n + i).unwrap())
                            .collect::<Vec<_>>(),
                    )
                    .with_timezone("UTC"),
                ),
            ),
            (
                "status",
                Arc::new(StringArray::from(
                    ids.iter()
                        .map(|i| statuses[usize::try_from(i % 5).unwrap()])
                        .collect::<Vec<_>>(),
                )),
            ),
            (
                "amount",
                Arc::new(
                    Decimal128Array::from(
                        ids.iter()
                            .map(|&i| i128::from(i) * 7 - 3_000)
                            .collect::<Vec<_>>(),
                    )
                    .with_precision_and_scale(18, 2)
                    .unwrap(),
                ),
            ),
            (
                "payload",
                Arc::new(StringArray::from(vec![
                    "x".repeat(64);
                    usize::try_from(n).unwrap()
                ])),
            ),
        ])
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(usize::try_from(rows).unwrap()))
            .build();
        let file = std::fs::File::create(data.join(format!("part-{f:05}.parquet"))).unwrap();
        let mut w = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
}

fn rss_bytes() -> Option<u64> {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()?;
    String::from_utf8(out.stdout)
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(|kib| kib * 1024)
}

fn bench(c: &mut Criterion) {
    let (root, _tmp): (PathBuf, Option<tempfile::TempDir>) =
        if let Some(dir) = std::env::var_os("BELLWETHER_BENCH_CORPUS") {
            (PathBuf::from(dir), None)
        } else {
            let tmp = tempfile::tempdir().unwrap();
            synthetic(tmp.path(), 4, 4, 5_000, 30_000);
            (tmp.path().to_path_buf(), Some(tmp))
        };
    let data = root.join("data").to_str().unwrap().to_string();
    let budget = std::env::var("BELLWETHER_VERIFY_BUDGET")
        .ok()
        .and_then(|b| b.parse().ok())
        .unwrap_or(DEFAULT_MEMORY_BUDGET);
    let spec = ReconcileSpec::new(["entity_id"])
        .latest_by("version_ts")
        .totals(["amount"])
        .group_by(["status"]);
    let reconciler = Reconciler::new(
        spec,
        Options {
            memory_budget: budget,
            ..Options::default()
        },
    )
    .unwrap();
    let source = ParquetSource::new();
    let side = Side::new(&source, &data);
    let before = rss_bytes();
    let probe = reconciler.run(&side, &side).unwrap();
    let after = rss_bytes();
    assert!(probe.pass);
    let rows = probe.sides[0].rows;
    eprintln!(
        "reconcile: {rows} rows per side, {} keys, {} passes, {} restarts, peak state {} B of {} B, \
         peak batch {} B, rss {:?} -> {:?} B",
        probe.counts.keys_compared,
        probe.memory.passes,
        probe.memory.restarts,
        probe.memory.peak_state_bytes,
        probe.memory.budget_bytes,
        probe.memory.peak_batch_bytes,
        before,
        after
    );
    let mut group = c.benchmark_group("reconcile");
    group.throughput(Throughput::Elements(rows));
    group.sample_size(10);
    group.bench_function("full_latest_by_totals", |b| {
        b.iter(|| reconciler.run(&side, &side).unwrap());
    });
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
