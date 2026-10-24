//! Hive partition columns from paths (WP21 item 1) on a
//! `day=YYYY-MM-DD/symbol=X/` layout. Ground truth comes from the paths the
//! test wrote and from decoding the files with the `parquet` crate, never
//! from the source under test.

#![allow(
    clippy::unwrap_used,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    missing_docs
)]

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, Date32Array, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_source::canonical::{encode_temporal, encode_utf8};
use bellwether_source::partition::parse_date;
use bellwether_source::ParquetSource;
use bellwether_types::Source;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

const DAYS: [&str; 2] = ["2026-01-02", "2026-01-05"];
const SYMBOLS: [&str; 3] = ["AAPL", "MSFT", "BRK/B"];

fn ts_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

fn file_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", ts_type(), false),
        Field::new("price", DataType::Float64, false),
    ]))
}

/// Writes `rows_per_rg`-row row groups of (ts, price) to `path`.
fn write_file(path: &Path, row_groups: usize, rows_per_rg: usize, base: i64) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(rows_per_rg))
        .build();
    let mut w =
        ArrowWriter::try_new(File::create(path).unwrap(), file_schema(), Some(props)).unwrap();
    let n = row_groups * rows_per_rg;
    let ts: Vec<i64> = (0..n as i64).map(|i| base + i * 1_000_000).collect();
    let price: Vec<f64> = (0..n).map(|i| 100.0 + i as f64).collect();
    let batch = RecordBatch::try_new(
        file_schema(),
        vec![
            Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
            Arc::new(Float64Array::from(price)),
        ],
    )
    .unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// The `day=…/symbol=…` layout under `root`: one file per (day, symbol),
/// two row groups of three rows each. Symbols are percent-encoded as Hive
/// writes them (`BRK/B` is `BRK%2FB`).
fn layout(root: &Path) -> Vec<(String, String, String)> {
    let mut out = Vec::new();
    for day in DAYS {
        for sym in SYMBOLS {
            let dir_sym = sym.replace('/', "%2F");
            let path = root
                .join(format!("day={day}"))
                .join(format!("symbol={dir_sym}"))
                .join("part-0.parquet");
            write_file(&path, 2, 3, 0);
            out.push((
                path.to_str().unwrap().to_string(),
                day.to_string(),
                sym.to_string(),
            ));
        }
    }
    out.sort();
    out
}

fn canonical_root(dir: &tempfile::TempDir) -> String {
    std::fs::canonicalize(dir.path())
        .unwrap()
        .to_str()
        .unwrap()
        .to_string()
}

fn date_bytes(day: &str) -> Vec<u8> {
    encode_temporal(0x01, 0x05, i64::from(parse_date(day).unwrap()))
}

#[test]
fn partition_values_become_exact_statistics_and_nothing_is_read() {
    let dir = tempfile::tempdir().unwrap();
    let root = canonical_root(&dir);
    let files = layout(Path::new(&root));
    let source = ParquetSource::new().with_hive_partitions(root.clone());

    // The inferred dataset schema: the file's columns, then the partition
    // columns in path order, typed by the fixed table.
    let schema = source.schema(&root).unwrap();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, ["ts", "price", "day", "symbol"]);
    assert_eq!(
        schema.field_with_name("day").unwrap().data_type(),
        &DataType::Date32
    );
    assert_eq!(
        schema.field_with_name("symbol").unwrap().data_type(),
        &DataType::Utf8
    );

    let discovered = source.discover(&root).unwrap();
    assert_eq!(discovered.len(), files.len());
    let typed = ParquetSource::new()
        .with_schema(Arc::clone(&schema))
        .with_hive_partitions(root.clone());
    for fp in &discovered {
        let (_, day, sym) = files.iter().find(|f| f.0 == fp.path).unwrap();
        let rgs = typed.row_groups(fp).unwrap();
        assert_eq!(rgs.len(), 2);
        for rg in &rgs {
            let stat = |c: &str| rg.stats.iter().find(|s| s.column == c).unwrap().clone();
            let d = stat("day");
            assert_eq!(d.min.as_deref(), Some(date_bytes(day).as_slice()));
            assert_eq!(d.max, d.min);
            assert_eq!(d.null_count, Some(0));
            assert_eq!(d.bytes, Some(0), "a virtual column costs no bytes");
            let s = stat("symbol");
            assert_eq!(s.min.as_deref(), Some(encode_utf8(sym).as_slice()));
            assert_eq!(s.max, s.min);
            // Schema order: file columns, then partition columns.
            let order: Vec<&str> = rg.stats.iter().map(|s| s.column.as_str()).collect();
            assert_eq!(order, ["ts", "price", "day", "symbol"]);
        }
        // read_columns synthesizes the virtual columns at the dataset type,
        // one value per row, in the requested order; a projection of only
        // virtual columns decodes nothing but still carries the row count.
        let batch = typed
            .read_columns(&rgs[1], &["symbol".into(), "price".into(), "day".into()])
            .unwrap();
        assert_eq!(batch.num_rows(), 3);
        let syms = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(syms.iter().all(|v| v == Some(sym.as_str())));
        let days = batch
            .column(2)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert!(days.iter().all(|v| v == parse_date(day)));
        let prices = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(prices.value(0), 103.0, "the second row group's first row");
        let only = typed.read_columns(&rgs[0], &["day".into()]).unwrap();
        assert_eq!(only.num_rows(), 3);
        assert_eq!(only.num_columns(), 1);
    }
}

#[test]
fn the_dataset_schema_types_partition_values_or_refuses_them() {
    let dir = tempfile::tempdir().unwrap();
    let root = canonical_root(&dir);
    let files = layout(Path::new(&root));
    let source = ParquetSource::new().with_hive_partitions(root.clone());
    let fp = source.discover(&root).unwrap().remove(0);
    let (_, day, _) = files.iter().find(|f| f.0 == fp.path).unwrap();

    // day typed as a string by the dataset schema: its statistics are the
    // string's encoding, not the date's.
    let as_text = Arc::new(Schema::new(vec![
        Field::new("ts", ts_type(), false),
        Field::new("price", DataType::Float64, false),
        Field::new("day", DataType::Utf8, true),
        Field::new("symbol", DataType::Utf8, true),
    ]));
    let rgs = ParquetSource::new()
        .with_schema(as_text)
        .with_hive_partitions(root.clone())
        .row_groups(&fp)
        .unwrap();
    let d = rgs[0].stats.iter().find(|s| s.column == "day").unwrap();
    assert_eq!(d.min.as_deref(), Some(encode_utf8(day).as_slice()));

    // symbol typed as an integer: "AAPL" does not cast, which is refused.
    let as_int = Arc::new(Schema::new(vec![
        Field::new("ts", ts_type(), false),
        Field::new("symbol", DataType::Int64, true),
    ]));
    let err = ParquetSource::new()
        .with_schema(as_int)
        .with_hive_partitions(root.clone())
        .row_groups(&fp)
        .unwrap_err();
    assert!(err.to_string().contains("symbol="), "{err}");

    // A dataset schema that does not declare a partition column: not
    // reported (an index recorded before partitioning keeps its digest).
    let without = Arc::new(Schema::new(vec![Field::new("ts", ts_type(), false)]));
    let rgs = ParquetSource::new()
        .with_schema(without)
        .with_hive_partitions(root.clone())
        .row_groups(&fp)
        .unwrap();
    assert!(rgs[0]
        .stats
        .iter()
        .all(|s| s.column != "day" && s.column != "symbol"));

    // Without hive partitioning nothing changes: no virtual column, and
    // asking for one is an unknown column.
    let plain = ParquetSource::new();
    let rgs = plain.row_groups(&fp).unwrap();
    assert_eq!(rgs[0].stats.len(), 2);
    assert!(plain.read_columns(&rgs[0], &["day".into()]).is_err());
}

fn write_with_symbol(path: &Path, symbols: &[&str], stats: bool) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("qty", DataType::Int64, false),
    ]));
    let mut props = WriterProperties::builder();
    if !stats {
        props = props.set_statistics_enabled(EnabledStatistics::None);
    }
    let mut w = ArrowWriter::try_new(
        File::create(path).unwrap(),
        Arc::clone(&schema),
        Some(props.build()),
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(symbols.to_vec())),
            Arc::new(Int64Array::from(vec![1i64; symbols.len()])),
        ],
    )
    .unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

#[test]
fn a_file_whose_path_and_content_disagree_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let root = canonical_root(&dir);
    let agree = Path::new(&root).join("symbol=AAPL/a.parquet");
    write_with_symbol(&agree, &["AAPL", "AAPL"], true);
    let source = ParquetSource::new().with_hive_partitions(root.clone());
    let fp = source.discover(&root).unwrap().remove(0);
    let rgs = source.row_groups(&fp).unwrap();
    // The stored column is kept once, not duplicated.
    assert_eq!(
        rgs[0].stats.iter().filter(|s| s.column == "symbol").count(),
        1
    );
    // A stored column is read from the file, not synthesized.
    let b = source.read_columns(&rgs[0], &["symbol".into()]).unwrap();
    assert_eq!(b.num_rows(), 2);

    for (name, symbols, stats) in [
        ("symbol=AAPL/b.parquet", &["AAPL", "MSFT"][..], true),
        ("symbol=AAPL/c.parquet", &["MSFT", "MSFT"][..], true),
        // Unknown statistics cannot show agreement: refused, never trusted.
        ("symbol=AAPL/d.parquet", &["AAPL", "AAPL"][..], false),
    ] {
        let d2 = tempfile::tempdir().unwrap();
        let r2 = canonical_root(&d2);
        write_with_symbol(&Path::new(&r2).join(name), symbols, stats);
        let s2 = ParquetSource::new().with_hive_partitions(r2.clone());
        let fp = s2.discover(&r2).unwrap().remove(0);
        let err = s2.row_groups(&fp).unwrap_err();
        assert!(err.to_string().contains("disagree"), "{name}: {err}");
    }
}

#[test]
fn discovery_refuses_mixed_partition_sets_and_ignores_the_root_s_own_components() {
    let dir = tempfile::tempdir().unwrap();
    let base = canonical_root(&dir);
    // The root itself lives under an `env=prod` directory, which is not a
    // column of the data.
    let root = Path::new(&base).join("env=prod").join("trades");
    write_file(&root.join("day=2026-01-02/a.parquet"), 1, 2, 0);
    let root = root.to_str().unwrap().to_string();
    let source = ParquetSource::new().with_hive_partitions(root.clone());
    let schema = source.schema(&root).unwrap();
    assert!(schema.field_with_name("env").is_err());
    assert!(schema.field_with_name("day").is_ok());

    // A file without the day directory beside one with it is ambiguous.
    write_file(&Path::new(&root).join("b.parquet"), 1, 2, 0);
    let err = source.discover(&root).unwrap_err();
    assert!(err.to_string().contains("same partition"), "{err}");
}

#[test]
fn a_null_partition_value_is_a_null() {
    let dir = tempfile::tempdir().unwrap();
    let root = canonical_root(&dir);
    write_file(
        &Path::new(&root).join("venue=__HIVE_DEFAULT_PARTITION__/a.parquet"),
        1,
        4,
        0,
    );
    let source = ParquetSource::new().with_hive_partitions(root.clone());
    let fp = source.discover(&root).unwrap().remove(0);
    let rgs = source.row_groups(&fp).unwrap();
    let v = rgs[0].stats.iter().find(|s| s.column == "venue").unwrap();
    assert_eq!(
        (v.min.clone(), v.max.clone(), v.null_count),
        (None, None, Some(4))
    );
    let b = source.read_columns(&rgs[0], &["venue".into()]).unwrap();
    assert_eq!(b.column(0).null_count(), 4);
}

#[test]
fn a_column_is_typed_by_every_value_under_the_root_not_the_first_file() {
    // `symbol=123` sorts before `symbol=ABC`; typing by the first file
    // would make the column Int64 and refuse (or mis-encode) the second.
    let dir = tempfile::tempdir().unwrap();
    let root = canonical_root(&dir);
    write_file(&Path::new(&root).join("symbol=123/a.parquet"), 1, 2, 0);
    write_file(&Path::new(&root).join("symbol=ABC/b.parquet"), 1, 2, 0);
    write_file(
        &Path::new(&root).join("symbol=__HIVE_DEFAULT_PARTITION__/c.parquet"),
        1,
        2,
        0,
    );
    let source = ParquetSource::new().with_hive_partitions(root.clone());
    let schema = source.schema(&root).unwrap();
    assert_eq!(
        schema.field_with_name("symbol").unwrap().data_type(),
        &DataType::Utf8
    );
    // A source without a dataset schema encodes every file's statistics at
    // that one type.
    for fp in source.discover(&root).unwrap() {
        let rgs = source.row_groups(&fp).unwrap();
        let s = rgs[0].stats.iter().find(|s| s.column == "symbol").unwrap();
        if let Some(min) = &s.min {
            assert_eq!(min[0], 0x06, "{}: Utf8 tag", fp.path);
        }
    }
    // All integers stay Int64.
    let d2 = tempfile::tempdir().unwrap();
    let r2 = canonical_root(&d2);
    write_file(&Path::new(&r2).join("n=7/a.parquet"), 1, 2, 0);
    write_file(&Path::new(&r2).join("n=-3/b.parquet"), 1, 2, 0);
    let s2 = ParquetSource::new().with_hive_partitions(r2.clone());
    assert_eq!(
        s2.schema(&r2)
            .unwrap()
            .field_with_name("n")
            .unwrap()
            .data_type(),
        &DataType::Int64
    );
}
