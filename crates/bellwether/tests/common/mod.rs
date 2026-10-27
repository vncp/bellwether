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
    ledger_corpus_with(dir, files, rgs, rows, |k| k % entities);
}

/// [`ledger_corpus`] with row `k`'s id `(k * 37) % entities`: consecutive
/// rows get ids far apart, so every row group's id range spans nearly the
/// whole key space and the files' own min/max statistics skip nothing for
/// a point lookup. The loop tests use it: on ids sorted into row groups,
/// min/max pruning already reads what `exact:id` would, so `why`
/// recommends nothing and `fix` refuses `nothing_to_gain`. `entities` must
/// not be a multiple of 37.
pub fn scattered_corpus(dir: &Path, files: usize, rgs: usize, rows: usize, entities: i64) {
    assert_ne!(entities % 37, 0, "37 must not divide entities");
    ledger_corpus_with(dir, files, rgs, rows, |k| (k * 37) % entities);
}

fn ledger_corpus_with(dir: &Path, files: usize, rgs: usize, rows: usize, id: impl Fn(i64) -> i64) {
    std::fs::create_dir_all(dir).unwrap();
    let mut k: i64 = 0;
    for f in 0..files {
        let mut groups = Vec::new();
        for _ in 0..rgs {
            let mut g = Vec::new();
            for _ in 0..rows {
                g.push((id(k), k / 50, Some(k * 1_000_000), format!("p{k:06}")));
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

/// `account: Int64` (nullable), `amount: Decimal128(18, 6)` (nullable),
/// `entry: Int64` (unique): the agg corpora's schema.
pub fn agg_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("account", DataType::Int64, true),
        Field::new("amount", DataType::Decimal128(18, 6), true),
        Field::new("entry", DataType::Int64, false),
    ]))
}

/// One agg corpus row: `(account, amount in millionths, entry)`.
pub type AggRow = (Option<i64>, Option<i128>, i64);

/// Row `k` of [`agg_corpus`]: account 0 for every third row (a hot key),
/// else `k % accounts`; no account for every 23rd row and no amount for
/// every 11th (ADR 014); amounts of both signs with all six decimals used.
#[must_use]
pub fn agg_row(k: i64, accounts: i64) -> AggRow {
    let account = if k % 23 == 0 {
        None
    } else if k % 3 == 0 {
        Some(0)
    } else {
        Some(k % accounts)
    };
    let amount = (k % 11 != 0).then(|| i128::from((k * 1_234_567) % 9_999_991) - 5_000_000);
    (account, amount, k)
}

/// A ledger of decimal amounts: `files` files of `rgs` row groups of
/// `rows` rows, row `k` being [`agg_row`]`(k, accounts)`.
pub fn agg_corpus(dir: &Path, files: usize, rgs: usize, rows: usize, accounts: i64) {
    std::fs::create_dir_all(dir).unwrap();
    let mut k: i64 = 0;
    for f in 0..files {
        let mut groups = Vec::new();
        for _ in 0..rgs {
            groups.push(
                (0..rows)
                    .map(|_| {
                        k += 1;
                        agg_row(k - 1, accounts)
                    })
                    .collect::<Vec<_>>(),
            );
        }
        write_agg_file(&dir.join(format!("part-{f:03}.parquet")), &groups);
    }
}

/// Writes agg corpus `row_groups` to `path`, one Parquet row group each.
pub fn write_agg_file(path: &Path, row_groups: &[Vec<AggRow>]) {
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(usize::MAX))
        .build();
    let mut w =
        ArrowWriter::try_new(File::create(path).unwrap(), agg_schema(), Some(props)).unwrap();
    for rows in row_groups {
        let batch = RecordBatch::try_new(
            agg_schema(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(
                    arrow::array::Decimal128Array::from(
                        rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                    )
                    .with_precision_and_scale(18, 6)
                    .unwrap(),
                ),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2))),
            ],
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.flush().unwrap();
    }
    w.close().unwrap();
}

/// One account's aggregates, from the decoded files.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AggOracle {
    pub rows: u64,
    pub nulls: u64,
    pub sum: i128,
    pub min: Option<i128>,
    pub max: Option<i128>,
}

/// Oracle: every account's aggregates over every `.parquet` file under
/// `dir`, read with the `parquet` crate (rows with no account are no key).
pub fn agg_oracle(dir: &Path) -> std::collections::BTreeMap<i64, AggOracle> {
    let mut out: std::collections::BTreeMap<i64, AggOracle> = std::collections::BTreeMap::new();
    let mut names: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
        .collect();
    names.sort();
    for p in names {
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&p).unwrap())
            .unwrap()
            .build()
            .unwrap();
        for b in reader {
            let b = b.unwrap();
            let acc = b
                .column_by_name("account")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone();
            let amt = b
                .column_by_name("amount")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Decimal128Array>()
                .unwrap()
                .clone();
            for i in 0..b.num_rows() {
                if acc.is_null(i) {
                    continue;
                }
                let o = out.entry(acc.value(i)).or_default();
                o.rows += 1;
                if amt.is_null(i) {
                    o.nulls += 1;
                    continue;
                }
                let v = amt.value(i);
                o.sum += v;
                o.min = Some(o.min.map_or(v, |m| m.min(v)));
                o.max = Some(o.max.map_or(v, |m| m.max(v)));
            }
        }
    }
    out
}

/// `v` millionths as a decimal string with six places (`-0.000012`).
#[must_use]
pub fn dec6(v: i128) -> String {
    let sign = if v < 0 { "-" } else { "" };
    let a = v.unsigned_abs();
    format!("{sign}{}.{:06}", a / 1_000_000, a % 1_000_000)
}
