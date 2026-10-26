//! One replay of one recorded run under one cached candidate key spec, the
//! operation `slo::REPLAY_RUN_P50_BUDGET` bounds (`docs/evolve.md` section
//! 12), at a p50-sized run of 50 keys. The corpus is written once: 960 row
//! groups (120 files of 8), the medium rung's row-group count, with 1,000
//! rows each so it builds in seconds.
//!
//! `cargo bench -p bellwether-replay --bench replay`
#![allow(missing_docs, clippy::unwrap_used, clippy::cast_possible_truncation)]

use std::fs::File;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether_replay::{Replayer, RunKeysBuilder};
use bellwether_source::ParquetSource;
use bellwether_types::KeySpec;
use criterion::{criterion_group, criterion_main, Criterion};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

const FILES: u64 = 120;
const ROW_GROUPS: u64 = 8;
const ROWS: u64 = 1_000;
const ENTITIES: u64 = 1_000_000;

fn mix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn bench(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "entity_id",
        DataType::UInt64,
        false,
    )]));
    for f in 0..FILES {
        let ids: Vec<u64> = (0..ROW_GROUPS * ROWS)
            .map(|r| mix(f * 1_000_003 + r) % ENTITIES)
            .collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt64Array::from(ids)) as ArrayRef],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(ROWS as usize))
            .build();
        let mut w = ArrowWriter::try_new(
            File::create(dir.path().join(format!("part-{f:04}.parquet"))).unwrap(),
            Arc::clone(&schema),
            Some(props),
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
    let source = ParquetSource::new().with_schema(Arc::clone(&schema));
    let replayer = Replayer::unindexed(
        &source,
        dir.path().to_str().unwrap(),
        &bellwether_source::Partitioning::None,
    )
    .unwrap();
    let mut b = RunKeysBuilder::new(&["entity_id".to_string()], &schema).unwrap();
    let keys: Vec<u64> = (0..50).map(|i| mix(i) % ENTITIES).collect();
    b.push_batch(
        &RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt64Array::from(keys)) as ArrayRef],
        )
        .unwrap(),
    )
    .unwrap();
    let run = b.finish();
    for spec in [
        KeySpec::Exact {
            columns: vec!["entity_id".into()],
            counts: false,
            bounds: None,
        },
        KeySpec::Bloom {
            columns: vec!["entity_id".into()],
            fpr: 0.01,
            bounds: None,
        },
    ] {
        replayer.transient(&spec).unwrap();
        let specs = [spec.clone()];
        c.bench_function(&format!("replay_p50_run/{}", spec.name()), |bench| {
            bench.iter(|| replayer.cost_of_keys(&specs, "run", &run).unwrap());
        });
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
