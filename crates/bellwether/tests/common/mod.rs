//! Test corpora written directly with the `parquet` crate, and an oracle
//! that reads them back without any engine code.

#![allow(dead_code, clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeSet;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// `id: Int64, batch: Int64, ts: Timestamp(µs, UTC), payload: Utf8`.
pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("batch", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

/// One row: `(id, batch, ts micros, payload)`.
pub type Row = (i64, i64, Option<i64>, String);

/// Writes `row_groups` (each a list of rows) to `path`, one Parquet row
/// group per entry.
pub fn write_file(path: &Path, row_groups: &[Vec<Row>]) {
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(usize::MAX))
        .build();
    let file = File::create(path).unwrap();
    let mut w = ArrowWriter::try_new(file, schema(), Some(props)).unwrap();
    for rows in row_groups {
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef,
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1))),
                Arc::new(
                    TimestampMicrosecondArray::from(rows.iter().map(|r| r.2).collect::<Vec<_>>())
                        .with_timezone("UTC"),
                ),
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|r| r.3.clone()),
                )),
            ],
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.flush().unwrap();
    }
    w.close().unwrap();
}

/// A versioned ledger: `files` files of `rgs` row groups of `rows` rows.
/// Row `k` of the whole corpus has id `k % entities` (so each id has
/// several versions spread over row groups), batch `k / 50` (a write
/// stamp), ts `k` seconds, and a payload.
pub fn ledger_corpus(dir: &Path, files: usize, rgs: usize, rows: usize, entities: i64) {
    std::fs::create_dir_all(dir).unwrap();
    let mut k: i64 = 0;
    for f in 0..files {
        let mut groups = Vec::new();
        for _ in 0..rgs {
            let mut g = Vec::new();
            for _ in 0..rows {
                g.push((
                    k % entities,
                    k / 50,
                    Some(k * 1_000_000),
                    format!("p{k:06}"),
                ));
                k += 1;
            }
            groups.push(g);
        }
        write_file(&dir.join(format!("part-{f:03}.parquet")), &groups);
    }
}

/// Writes a key file with one Int64 column `name`.
pub fn write_keys(path: &Path, name: &str, keys: &[i64]) {
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

/// Oracle: `(file name, row group index)` of every row group under `dir`
/// holding an `id` in `keys`, read with the `parquet` crate.
pub fn oracle(dir: &Path, keys: &[i64]) -> BTreeSet<(String, usize)> {
    let want: BTreeSet<i64> = keys.iter().copied().collect();
    let mut out = BTreeSet::new();
    let mut names: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
        .collect();
    names.sort();
    for p in names {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&p).unwrap()).unwrap();
        let n = builder.metadata().num_row_groups();
        for rg in 0..n {
            let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&p).unwrap())
                .unwrap()
                .with_row_groups(vec![rg])
                .build()
                .unwrap();
            for b in reader {
                let b = b.unwrap();
                let ids = b
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                if (0..ids.len()).any(|i| want.contains(&ids.value(i))) {
                    out.insert((p.file_name().unwrap().to_string_lossy().to_string(), rg));
                }
            }
        }
    }
    out
}

/// The ledger file of `hash` under an index directory.
pub fn ledger_bytes(index: &Path, hash: &str) -> Vec<u8> {
    std::fs::read(index.join("ledger").join(format!("{hash}.rec"))).unwrap()
}
