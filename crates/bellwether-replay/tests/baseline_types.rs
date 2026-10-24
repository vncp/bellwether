//! The statistics baseline probes the embedded filter of every Parquet
//! physical type a run column can carry, or refuses (`docs/benchmark.md`:
//! the baseline prunes by footer min/max and by any embedded bloom filter,
//! never silently less).
//!
//! The corpus carries filters on columns of all six physical types a run
//! column can have: `FLOAT`, `DOUBLE`, `BYTE_ARRAY` (a string),
//! `FIXED_LEN_BYTE_ARRAY` (fixed binary and a wide decimal), `INT32` (a
//! date and a narrow decimal), and `INT64` (a millisecond timestamp). For every run the baseline's
//! row groups must contain the oracle's (no false negative) and lie inside
//! an independent bloom-only plan computed with the `parquet` crate's typed
//! filter checks (so a baseline that ignored a filter, which would keep row
//! groups the filter excludes, fails). A `Decimal256` column with a filter
//! is refused with `ReplayError::UnprobeableFilter`.

#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]

mod common;

use std::collections::BTreeSet;
use std::fs::File;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Date32Array, Decimal128Array, Decimal256Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow::datatypes::{i256, DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_replay::{
    BaselinePlanner, KeyInput, MemoryKeySets, ReplayError, Replayer, WorkloadRecorder,
};
use bellwether_source::{FingerprintPolicy, ParquetSource};
use parquet::arrow::ArrowWriter;
use parquet::data_type::{ByteArray, FixedLenByteArray};
use parquet::file::properties::{ReaderProperties, WriterProperties};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::serialized_reader::ReadOptionsBuilder;
use parquet::schema::types::ColumnPath;

const FILES: usize = 4;
const RGS: usize = 3;
const ROWS: usize = 50;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k_f32", DataType::Float32, false),
        Field::new("k_fsb", DataType::FixedSizeBinary(4), false),
        Field::new("k_dec30", DataType::Decimal128(30, 2), false),
        Field::new("k_dec9", DataType::Decimal128(9, 2), false),
        Field::new("k_date", DataType::Date32, false),
        Field::new(
            "k_ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new("k_d256", DataType::Decimal256(40, 2), false),
        Field::new("k_f64", DataType::Float64, false),
        Field::new("k_str", DataType::Utf8, false),
    ]))
}

/// The `k_f64` value of `v`: not exactly representable as an `f32`.
fn f64_of(v: i64) -> f64 {
    v as f64 * 0.1 + 1e-9
}

/// The `k_str` value of `v`.
fn str_of(v: i64) -> String {
    format!("key-{v:05}")
}

/// Row `n`'s value: 600 distinct values over 600 rows, shuffled so each row
/// group's min/max spans most of the domain and only the filters prune.
fn val(n: usize) -> i64 {
    ((n * 7919) % 600) as i64 + 1
}

fn write() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    for f in 0..FILES {
        let n0 = f * RGS * ROWS;
        let vs: Vec<i64> = (0..RGS * ROWS).map(|i| val(n0 + i)).collect();
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Float32Array::from(
                    vs.iter().map(|v| *v as f32 * 0.5).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(
                        vs.iter().map(|v| (*v as u32).to_be_bytes()),
                    )
                    .unwrap(),
                ),
                Arc::new(
                    Decimal128Array::from(
                        vs.iter()
                            .map(|v| i128::from(*v) * 1_000_000_007)
                            .collect::<Vec<_>>(),
                    )
                    .with_precision_and_scale(30, 2)
                    .unwrap(),
                ),
                Arc::new(
                    Decimal128Array::from(
                        vs.iter().map(|v| i128::from(*v) - 300).collect::<Vec<_>>(),
                    )
                    .with_precision_and_scale(9, 2)
                    .unwrap(),
                ),
                Arc::new(Date32Array::from(
                    vs.iter().map(|v| 19_000 + *v as i32).collect::<Vec<_>>(),
                )),
                Arc::new(
                    TimestampMillisecondArray::from(
                        vs.iter()
                            .map(|v| 1_700_000_000_000 + v * 1_000)
                            .collect::<Vec<_>>(),
                    )
                    .with_timezone("UTC"),
                ),
                Arc::new(
                    Decimal256Array::from(
                        vs.iter()
                            .map(|v| i256::from_i128(i128::from(*v)))
                            .collect::<Vec<_>>(),
                    )
                    .with_precision_and_scale(40, 2)
                    .unwrap(),
                ),
                Arc::new(Float64Array::from(
                    vs.iter().map(|v| f64_of(*v)).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    vs.iter().map(|v| str_of(*v)).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let mut props = WriterProperties::builder().set_max_row_group_size(ROWS);
        for c in [
            "k_f32", "k_fsb", "k_dec30", "k_dec9", "k_date", "k_ts", "k_d256", "k_f64", "k_str",
        ] {
            props = props
                .set_column_bloom_filter_enabled(ColumnPath::from(c), true)
                .set_column_bloom_filter_ndv(ColumnPath::from(c), ROWS as u64)
                .set_column_bloom_filter_fpp(ColumnPath::from(c), 0.01);
        }
        let mut w = ArrowWriter::try_new(
            File::create(data.join(format!("part-{f}.parquet"))).unwrap(),
            schema(),
            Some(props.build()),
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
    let data = data.to_str().unwrap().to_string();
    (dir, data)
}

/// The key batch of a run over one column: values `vs` in that column's
/// type, as the corpus writes them.
fn keys_of(column: &str, vs: &[i64]) -> RecordBatch {
    let arr: ArrayRef = match column {
        "k_f32" => Arc::new(Float32Array::from(
            vs.iter().map(|v| *v as f32 * 0.5).collect::<Vec<_>>(),
        )),
        "k_fsb" => Arc::new(
            FixedSizeBinaryArray::try_from_iter(vs.iter().map(|v| (*v as u32).to_be_bytes()))
                .unwrap(),
        ),
        "k_dec30" => Arc::new(
            Decimal128Array::from(
                vs.iter()
                    .map(|v| i128::from(*v) * 1_000_000_007)
                    .collect::<Vec<_>>(),
            )
            .with_precision_and_scale(30, 2)
            .unwrap(),
        ),
        "k_dec9" => Arc::new(
            Decimal128Array::from(vs.iter().map(|v| i128::from(*v) - 300).collect::<Vec<_>>())
                .with_precision_and_scale(9, 2)
                .unwrap(),
        ),
        "k_date" => Arc::new(Date32Array::from(
            vs.iter().map(|v| 19_000 + *v as i32).collect::<Vec<_>>(),
        )),
        "k_ts" => Arc::new(
            TimestampMillisecondArray::from(
                vs.iter()
                    .map(|v| 1_700_000_000_000 + v * 1_000)
                    .collect::<Vec<_>>(),
            )
            .with_timezone("UTC"),
        ),
        "k_f64" => Arc::new(Float64Array::from(
            vs.iter().map(|v| f64_of(*v)).collect::<Vec<_>>(),
        )),
        "k_str" => Arc::new(StringArray::from(
            vs.iter().map(|v| str_of(*v)).collect::<Vec<_>>(),
        )),
        other => panic!("{other}"),
    };
    RecordBatch::try_from_iter(vec![(column, arr)]).unwrap()
}

/// Whether the row group's filter on `column` may hold `v`, checked with the
/// `parquet` crate's typed values (the PLAIN encoding each writer hashes).
fn filter_may_hold(f: &parquet::bloom_filter::Sbbf, column: &str, v: i64) -> bool {
    let be_fixed = |x: i128, len: usize| {
        let b = x.to_be_bytes();
        FixedLenByteArray::from(ByteArray::from(b[16 - len..].to_vec()))
    };
    match column {
        "k_f32" => f.check(&(v as f32 * 0.5)),
        "k_fsb" => f.check(&FixedLenByteArray::from(ByteArray::from(
            (v as u32).to_be_bytes().to_vec(),
        ))),
        // Decimal128(30, 2) is FIXED_LEN_BYTE_ARRAY(13).
        "k_dec30" => f.check(&be_fixed(i128::from(v) * 1_000_000_007, 13)),
        // Decimal128(9, 2) is INT32.
        "k_dec9" => f.check(&((v - 300) as i32)),
        "k_date" => f.check(&(19_000 + v as i32)),
        "k_ts" => f.check(&(1_700_000_000_000 + v * 1_000)),
        "k_f64" => f.check(&f64_of(v)),
        "k_str" => f.check(&ByteArray::from(str_of(v).as_str())),
        other => panic!("{other}"),
    }
}

/// Row groups whose embedded filter may hold one of `vs`: the bloom-only
/// plan, computed independently of bellwether.
fn bloom_only(paths: &[std::path::PathBuf], column: &str, vs: &[i64]) -> BTreeSet<(String, u32)> {
    let mut out = BTreeSet::new();
    for path in paths {
        let options = ReadOptionsBuilder::new()
            .with_reader_properties(
                ReaderProperties::builder()
                    .set_read_bloom_filter(true)
                    .build(),
            )
            .build();
        let r = SerializedFileReader::new_with_options(File::open(path).unwrap(), options).unwrap();
        let ci = (0..r.metadata().file_metadata().schema_descr().num_columns())
            .find(|&i| r.metadata().file_metadata().schema_descr().column(i).name() == column)
            .unwrap();
        for g in 0..r.num_row_groups() {
            let rg = r.get_row_group(g).unwrap();
            let f = rg
                .get_column_bloom_filter(ci)
                .expect("every row group carries a filter");
            if vs.iter().any(|v| filter_may_hold(f, column, *v)) {
                out.insert((path.to_str().unwrap().to_string(), g as u32));
            }
        }
    }
    out
}

/// Row groups that hold one of `vs` in `column`, by reading the column.
fn oracle(paths: &[std::path::PathBuf], column: &str, vs: &[i64]) -> BTreeSet<(String, u32)> {
    let want: BTreeSet<i64> = vs.iter().copied().collect();
    let mut out = BTreeSet::new();
    for path in paths {
        common::for_each_row_group(path, &[column], |g, cols| {
            // Every column's value is a function of the shuffled index:
            // recover it through the date column's relation, uniformly.
            let arr = &cols[0];
            for row in 0..arr.len() {
                let v = match column {
                    "k_f32" => {
                        (arr.as_any()
                            .downcast_ref::<Float32Array>()
                            .unwrap()
                            .value(row)
                            * 2.0) as i64
                    }
                    "k_fsb" => i64::from(u32::from_be_bytes(
                        arr.as_any()
                            .downcast_ref::<FixedSizeBinaryArray>()
                            .unwrap()
                            .value(row)
                            .try_into()
                            .unwrap(),
                    )),
                    "k_dec30" => {
                        (arr.as_any()
                            .downcast_ref::<Decimal128Array>()
                            .unwrap()
                            .value(row)
                            / 1_000_000_007) as i64
                    }
                    "k_dec9" => {
                        (arr.as_any()
                            .downcast_ref::<Decimal128Array>()
                            .unwrap()
                            .value(row)
                            + 300) as i64
                    }
                    "k_date" => {
                        i64::from(
                            arr.as_any()
                                .downcast_ref::<Date32Array>()
                                .unwrap()
                                .value(row),
                        ) - 19_000
                    }
                    "k_ts" => {
                        (arr.as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .unwrap()
                            .value(row)
                            - 1_700_000_000_000)
                            / 1_000
                    }
                    "k_f64" => {
                        let x = arr
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap()
                            .value(row);
                        (1..=600)
                            .find(|v| f64_of(*v).to_bits() == x.to_bits())
                            .unwrap_or(-1)
                    }
                    "k_str" => arr
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(row)["key-".len()..]
                        .parse()
                        .unwrap(),
                    other => panic!("{other}"),
                };
                if want.contains(&v) {
                    out.insert((path.to_str().unwrap().to_string(), g));
                    break;
                }
            }
        });
    }
    out
}

fn paths(data: &str) -> Vec<std::path::PathBuf> {
    let mut out: Vec<_> = std::fs::read_dir(data)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    out.sort();
    out
}

#[test]
fn every_physical_type_is_probed_inside_the_bloom_only_plan() {
    let (_dir, data) = write();
    let source = ParquetSource::new().with_schema(schema());
    let replayer = Replayer::unindexed(&source, &data).unwrap();
    let files = paths(&data);
    let total = replayer.catalog().len();
    let keys = MemoryKeySets::new();
    let mut rec = WorkloadRecorder::new(&keys, schema());
    // Present values and values no file holds (601.., 0).
    let sets: [&[i64]; 3] = [&[5, 77, 300], &[1, 2, 3, 4, 601, 999], &[0, 700]];
    let columns = [
        "k_f32", "k_f64", "k_str", "k_fsb", "k_dec30", "k_dec9", "k_date", "k_ts",
    ];
    for c in columns {
        for (i, vs) in sets.iter().enumerate() {
            rec.record(&format!("{c}-{i}"), KeyInput::Batch(&keys_of(c, vs)))
                .unwrap();
        }
    }
    let workload = rec.finish().unwrap();
    let mut pruned = BTreeSet::new();
    for c in columns {
        let stats = BaselinePlanner::parquet_stats(
            replayer.catalog(),
            &[c.to_string()],
            FingerprintPolicy::Tail64K,
        )
        .unwrap();
        for (i, vs) in sets.iter().enumerate() {
            let run = workload.run(&format!("{c}-{i}")).unwrap();
            let rk = workload.run_keys(&keys, run).unwrap();
            let got = common::cells(&stats.plan(&rk).unwrap());
            let want = oracle(&files, c, vs);
            let blooms = bloom_only(&files, c, vs);
            let missing: Vec<_> = want.difference(&got).collect();
            assert!(missing.is_empty(), "{c}-{i}: misses {missing:?}");
            let extra: Vec<_> = got.difference(&blooms).collect();
            assert!(
                extra.is_empty(),
                "{c}-{i}: keeps row groups its filters exclude: {extra:?}"
            );
            if got.len() < total {
                pruned.insert(c);
            }
        }
    }
    assert_eq!(
        pruned.len(),
        columns.len(),
        "every column's filters must prune some run (non-vacuity): {pruned:?}"
    );
}

#[test]
fn a_filter_the_baseline_cannot_probe_is_refused() {
    let (_dir, data) = write();
    let source = ParquetSource::new().with_schema(schema());
    let replayer = Replayer::unindexed(&source, &data).unwrap();
    let err = BaselinePlanner::parquet_stats(
        replayer.catalog(),
        &["k_d256".to_string()],
        FingerprintPolicy::Tail64K,
    )
    .unwrap_err();
    assert!(
        matches!(&err, ReplayError::UnprobeableFilter { column, why, .. } if column == "k_d256" && why.contains("Decimal256")),
        "{err}"
    );
}
