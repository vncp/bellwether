//! Build throughput in row groups per second, at several corpus sizes.
//!
//! Each corpus has `files` Parquet files of 4 row groups of 2048 rows
//! (`id`, `grp`, `ts`, and a 64-byte `payload` no spec reads); the build
//! indexes `exact:id`, `temporal:id` by `ts`, and `bloom:grp` into an
//! in-memory store. Throughput is reported per row group, so a build that
//! is linear in files shows the same rate at every size.
//!
//! `cargo bench -p bellwether-build --bench build` measures it;
//! `cargo test -p bellwether-build --benches` runs each case once.

#![allow(missing_docs, clippy::unwrap_used, clippy::cast_possible_wrap)]

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_build::{BuildOptions, Builder, SourceKind};
use bellwether_store::ObjectStoreBackend;
use bellwether_types::KeySpec;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

const ROW_GROUPS_PER_FILE: usize = 4;
const ROWS_PER_ROW_GROUP: usize = 2048;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("grp", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn corpus(files: usize) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let rows = ROW_GROUPS_PER_FILE * ROWS_PER_ROW_GROUP;
    for f in 0..files {
        let base = (f * rows) as i64;
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                // every id has two versions, in different row groups
                Arc::new(Int64Array::from(
                    (0..rows as i64)
                        .map(|i| base + i % (rows as i64 / 2))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    (0..rows)
                        .map(|i| format!("g{}", i % 16))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(TimestampMicrosecondArray::from(
                    (0..rows as i64).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    (0..rows).map(|i| format!("{i:064}")).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(ROWS_PER_ROW_GROUP))
            .build();
        let path = dir.path().join(format!("part-{f:05}.parquet"));
        let mut w =
            ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema(), Some(props))
                .unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
    dir
}

fn bench_build(c: &mut Criterion) {
    let specs = [
        KeySpec::Exact {
            columns: vec!["id".into()],
            counts: false,
            bounds: None,
        },
        KeySpec::Temporal {
            columns: vec!["id".into()],
            ts_column: "ts".into(),
            counts: false,
            tombstone: None,
        },
        KeySpec::Bloom {
            columns: vec!["grp".into()],
            fpr: 0.01,
            bounds: None,
        },
    ];
    let corpora: Vec<(u64, tempfile::TempDir)> = [4usize, 8, 16]
        .into_iter()
        .map(|files| ((files * ROW_GROUPS_PER_FILE) as u64, corpus(files)))
        .collect();
    // One worker shows the work is linear in files (a constant rate per row
    // group); the default pool shows how it spreads over work items.
    for (name, threads) in [("build/one_worker", Some(1)), ("build/pool", None)] {
        let opts = BuildOptions {
            schema: Some(schema()),
            max_segment_row_groups: 8,
            threads,
            ..BuildOptions::new(SourceKind::Parquet)
        };
        let mut group = c.benchmark_group(name);
        group.sample_size(10);
        for (row_groups, dir) in &corpora {
            let root = dir.path().to_str().unwrap().to_string();
            group.throughput(Throughput::Elements(*row_groups));
            group.bench_with_input(
                BenchmarkId::new("row_groups", row_groups),
                &root,
                |b, root| {
                    b.iter_batched(
                        || Arc::new(ObjectStoreBackend::memory().unwrap()),
                        |store| {
                            let r = Builder::new(store, "idx")
                                .build(root, &specs, &opts)
                                .unwrap();
                            assert_eq!(r.row_groups_read, *row_groups);
                            r
                        },
                        BatchSize::PerIteration,
                    );
                },
            );
        }
        group.finish();
    }
}

criterion_group!(benches, bench_build);
criterion_main!(benches);
