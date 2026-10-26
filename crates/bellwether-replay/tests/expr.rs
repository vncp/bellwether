//! `expr` keys priced before they exist: what replay predicts from a
//! transient derived column equals what the build crate's planner plans
//! once the key is built (row groups, files, and bytes per run), and the
//! `why` column's two numbers (rows whose expression is null, distinct
//! derived values) equal an oracle that reads every document with
//! `serde_json`.

#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::cast_possible_wrap
)]

use std::collections::BTreeSet;
use std::fs::File;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bellwether_build::{BuildOptions, Builder, Lookup, Planner, SourceKind};
use bellwether_core::encoding::{self, Value};
use bellwether_core::expr::{self, Over};
use bellwether_replay::{KeyInput, MemoryKeySets, Replayer, TransientLimits, WorkloadRecorder};
use bellwether_source::{ParquetSource, Partitioning};
use bellwether_store::{ObjectStoreBackend, Snapshot};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

const SYM: &str = "json_get(payload,'$.symbol')";

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, true),
    ]))
}

/// 3 files × 6 row groups × 30 rows; row group `g` holds symbols `s<g>`
/// and `s<g+1>`, and some rows have no symbol (null cell, missing member,
/// broken document).
fn corpus(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut paths = Vec::new();
    for f in 0..3usize {
        let mut cells: Vec<Option<String>> = Vec::new();
        for r in 0..6usize {
            let g = f * 6 + r;
            for i in 0..30usize {
                cells.push(match i % 10 {
                    0 => None,
                    1 => Some(r#"{"venue":"X"}"#.into()),
                    2 => Some(format!(r#"{{"symbol":"s{g}""#)),
                    _ => Some(format!(
                        r#"{{"symbol":"s{}","seq":{i}}}"#,
                        g + usize::from(i % 3 == 0)
                    )),
                });
            }
        }
        let n = cells.len() as i64;
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from_iter_values(0..n)) as ArrayRef,
                Arc::new(StringArray::from(cells)),
            ],
        )
        .unwrap();
        let path = dir.join(format!("part-{f}.parquet"));
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(30))
            .build();
        let mut w =
            ArrowWriter::try_new(File::create(&path).unwrap(), schema(), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        paths.push(path);
    }
    paths
}

fn key_batch(column: &str, keys: &[&str]) -> RecordBatch {
    RecordBatch::try_from_iter([(
        column,
        Arc::new(StringArray::from(keys.to_vec())) as ArrayRef,
    )])
    .unwrap()
}

fn enc(s: &str) -> Vec<u8> {
    let mut out = Vec::new();
    encoding::encode_value_as(&Value::Utf8(s), &DataType::Utf8, &mut out).unwrap();
    out
}

#[test]
fn predicted_equals_planned_and_the_why_column_equals_the_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = std::fs::canonicalize(dir.path()).unwrap().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let paths = corpus(&data_dir);
    let data = data_dir.to_str().unwrap().to_string();
    let source = ParquetSource::new();
    let runs: Vec<Vec<&str>> = vec![
        vec!["s3"],
        vec!["s0", "s17"],
        vec!["s5", "s6", "s7", "s8"],
        vec!["nowhere"],
        vec!["s18", "s2"],
    ];
    // Recorded with the expression spelled another way: the run holds it
    // normalized.
    let keys = MemoryKeySets::new();
    let mut rec = WorkloadRecorder::new(&keys, schema());
    for (i, r) in runs.iter().enumerate() {
        rec.record(
            &format!("run-{i}"),
            KeyInput::Batch(&key_batch("JSON_GET( payload , '$[\"symbol\"]' )", r)),
        )
        .unwrap();
    }
    let workload = rec.finish().unwrap();
    assert!(workload.runs().iter().all(|r| r.columns == [SYM]));
    for over in [Over::Exact, Over::Bloom(0.01)] {
        let spec = expr::spec("sym", SYM, over).unwrap();
        let transient = Replayer::unindexed(&source, &data, &Partitioning::None).unwrap();
        let predicted = transient
            .replay_specs(std::slice::from_ref(&spec), &workload, &keys)
            .unwrap();
        let store = Arc::new(ObjectStoreBackend::memory().unwrap());
        let r = Builder::new(Arc::clone(&store), "idx")
            .build(
                &data,
                std::slice::from_ref(&spec),
                &BuildOptions::new(SourceKind::Parquet),
            )
            .unwrap();
        assert!(r.sequence.is_some());
        let snapshot = Snapshot::open(Arc::clone(&store), "idx").unwrap();
        let committed = Replayer::for_snapshot(&source, &snapshot).unwrap();
        assert_eq!(committed.current_specs(), std::slice::from_ref(&spec));
        let current = committed.replay_current(&workload, &keys).unwrap();
        assert_eq!(current, predicted, "{over:?}: committed replay");
        let planner = Planner::open(Arc::clone(&store), "idx").unwrap();
        for (cost, r) in predicted.iter().zip(&runs) {
            let plan = planner
                .plan(
                    std::slice::from_ref(&spec),
                    &Lookup::Keys {
                        columns: vec![SYM.into()],
                        keys: r.iter().map(|k| enc(k)).collect(),
                    },
                )
                .unwrap();
            assert_eq!(
                (cost.files, cost.row_groups, cost.bytes),
                (
                    plan.entries.len() as u64,
                    plan.row_groups_selected,
                    plan.bytes
                ),
                "{over:?} {r:?}: predicted vs planned"
            );
        }
        // An exact key plans the row groups holding a key, and no other.
        if over == Over::Exact {
            assert_eq!(predicted[0].row_groups, 2, "s3 lives in row groups 2 and 3");
            assert_eq!(predicted[3].row_groups, 0, "an absent key reads nothing");
        }
    }
    // The `why` column of the exact key: rows whose expression is null and
    // distinct derived values, against serde_json over every document.
    let spec = expr::spec("sym", SYM, Over::Exact).unwrap();
    let transient = Replayer::unindexed(&source, &data, &Partitioning::None).unwrap();
    let index = transient.transient(&spec).unwrap();
    let (mut nulls, mut distinct) = (0u64, BTreeSet::new());
    for p in &paths {
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(p).unwrap())
            .unwrap()
            .build()
            .unwrap();
        for b in reader {
            let b = b.unwrap();
            let a = b.column(1).as_string::<i32>();
            for i in 0..a.len() {
                let v = (!a.is_null(i))
                    .then(|| serde_json::from_str::<serde_json::Value>(a.value(i)).ok())
                    .flatten()
                    .and_then(|v| v.get("symbol").and_then(|s| s.as_str().map(String::from)));
                match v {
                    Some(s) => {
                        distinct.insert(s);
                    }
                    None => nulls += 1,
                }
            }
        }
    }
    assert_eq!(index.stats().null_rows, nulls);
    assert_eq!(
        index.distinct_keys(),
        Some((distinct.len() as u64, distinct.len() as u64))
    );
    let bloom = transient
        .transient(&expr::spec("sym", SYM, Over::Bloom(0.01)).unwrap())
        .unwrap();
    assert_eq!(
        bloom.distinct_keys(),
        None,
        "a bloom segment counts no keys"
    );
    // Forced into one segment per row group, a value in two row groups sits
    // in two segments: the count is bounded, the largest segment's to their
    // sum, never claimed exact.
    let split = Replayer::unindexed(&source, &data, &Partitioning::None)
        .unwrap()
        .with_limits(TransientLimits {
            max_segment_rows: 1,
            ..TransientLimits::default()
        });
    let parts = split.transient(&spec).unwrap();
    assert_eq!(parts.segment_ranges().len(), 18);
    let (low, high) = parts.distinct_keys().unwrap();
    assert!(low < high, "{low} {high}");
    assert!(low <= distinct.len() as u64 && distinct.len() as u64 <= high);
    assert_eq!(parts.stats().null_rows, nulls);

    // A declared expr key over another expression never serves a run on
    // this one: the run is priced as a scan of every row group, never from
    // the other key's segments.
    let other = expr::spec("seq", "json_get(payload,'$.seq')", Over::Exact).unwrap();
    let scan = transient.replay_specs(&[], &workload, &keys).unwrap();
    let priced = transient
        .replay_specs(std::slice::from_ref(&other), &workload, &keys)
        .unwrap();
    assert_eq!(priced, scan);
    assert!(priced.iter().all(|c| c.row_groups == 18));
}
