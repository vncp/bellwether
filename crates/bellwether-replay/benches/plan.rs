//! Planning latency through the store: one `locate`, `latest`, or one-day
//! window run of 10, 300, or 3,000 symbols, planned by the build planner
//! (the replay seam [`PlanSource`], the planner the CLI's lookups use) over
//! an index in a local-filesystem store, each iteration pinning a fresh
//! snapshot as one CLI call does. This is the path the 2026-10-26 profiles
//! found dominated by store round trips (one per value read).
//!
//! The corpus is a small market: 30 daily files of 8 row groups of 4,096
//! rows, every one of 5,000 symbols trading every day, each file sorted by
//! symbol so a symbol sits in one row group per day. The index holds
//! `exact:symbol@ts` (per-key time bounds, which serve the window) and
//! `temporal:symbol@ts`.
//!
//! `cargo bench -p bellwether-replay --bench plan` measures it;
//! `cargo test -p bellwether-replay --benches` runs each case once.

#![allow(missing_docs, clippy::unwrap_used, clippy::cast_possible_wrap)]

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_build::{BuildOptions, Builder, Planner, SourceKind};
use bellwether_replay::{PlanSource, RunKeysBuilder, RunQuery, RunWindow};
use bellwether_store::ObjectStoreBackend;
use bellwether_types::KeySpec;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

const DAYS: i64 = 30;
const ROW_GROUPS: usize = 8;
const ROWS: usize = 4_096;
const SYMBOLS: usize = 5_000;
const DAY_MICROS: i64 = 86_400_000_000;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("price", DataType::Int64, false),
    ]))
}

fn symbol(i: usize) -> String {
    format!("S{i:05}")
}

/// One day's file: `ROW_GROUPS * ROWS` trades, symbols in order.
fn write_day(dir: &std::path::Path, day: i64) {
    let n = ROW_GROUPS * ROWS;
    let symbols: Vec<String> = (0..n).map(|r| symbol(r * SYMBOLS / n)).collect();
    let ts: Vec<i64> = (0..n)
        .map(|r| day * DAY_MICROS + ((r as i64 * 7_919) % DAY_MICROS))
        .collect();
    let price: Vec<i64> = (0..n).map(|r| (r as i64 * 31 + day) % 10_000).collect();
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from(symbols)) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(ts)),
            Arc::new(Int64Array::from(price)),
        ],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(ROWS))
        .build();
    let path = dir.join(format!("day-{day:03}.parquet"));
    let mut w =
        ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema(), Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn keys(n: usize) -> bellwether_replay::RunKeys {
    let key_schema = Arc::new(Schema::new(vec![Field::new(
        "symbol",
        DataType::Utf8,
        false,
    )]));
    let mut b = RunKeysBuilder::new(&["symbol".to_string()], &key_schema).unwrap();
    let picked: Vec<String> = (0..n).map(|i| symbol(i * SYMBOLS / n)).collect();
    b.push_batch(
        &RecordBatch::try_new(
            key_schema,
            vec![Arc::new(StringArray::from(picked)) as ArrayRef],
        )
        .unwrap(),
    )
    .unwrap();
    b.finish()
}

fn bench(c: &mut Criterion) {
    let data = tempfile::tempdir().unwrap();
    for day in 0..DAYS {
        write_day(data.path(), day);
    }
    let index = tempfile::tempdir().unwrap();
    let store = Arc::new(ObjectStoreBackend::local(index.path()).unwrap());
    let specs = vec![
        KeySpec::Exact {
            columns: vec!["symbol".into()],
            counts: false,
            bounds: Some("ts".into()),
        },
        KeySpec::Temporal {
            columns: vec!["symbol".into()],
            ts_column: "ts".into(),
            counts: false,
            tombstone: None,
        },
    ];
    let opts = BuildOptions {
        schema: Some(schema()),
        ..BuildOptions::new(SourceKind::Parquet)
    };
    let built = Builder::new(Arc::clone(&store), "")
        .build(data.path().to_str().unwrap(), &specs, &opts)
        .unwrap();
    assert_eq!(built.row_groups_read, DAYS as u64 * ROW_GROUPS as u64);

    // The middle day.
    let day = DAYS / 2;
    let queries = [
        ("locate", RunQuery::Locate),
        ("latest", RunQuery::Latest),
        (
            "window_1d",
            RunQuery::Window(RunWindow {
                ts_column: "ts".into(),
                lo: day * DAY_MICROS,
                hi: (day + 1) * DAY_MICROS - 1,
            }),
        ),
    ];
    for (name, query) in &queries {
        let mut g = c.benchmark_group(format!("plan/{name}"));
        g.sample_size(10);
        for n in [10usize, 300, 3_000] {
            let run = keys(n);
            // Non-vacuous: the plan prunes and selects something.
            let p = Planner::open(Arc::clone(&store), "").unwrap();
            let plan = p.plan_query(&specs, &run, query).unwrap();
            let total = DAYS as u64 * ROW_GROUPS as u64;
            assert!(
                plan.row_groups_selected > 0 && plan.row_groups_selected < total,
                "{name} {n}: {} of {total}",
                plan.row_groups_selected
            );
            g.bench_with_input(BenchmarkId::new("keys", n), &run, |b, run| {
                b.iter(|| {
                    let p = Planner::open(Arc::clone(&store), "").unwrap();
                    p.plan_query(&specs, run, query).unwrap()
                });
            });
        }
        g.finish();
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
