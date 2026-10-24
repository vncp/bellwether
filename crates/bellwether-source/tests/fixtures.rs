//! Row-group statistics and projected reads against `fixtures/source/`.

#![allow(clippy::unwrap_used, missing_docs)]

mod common;

use arrow::array::{Array, AsArray, Int64Array, StringArray};
use arrow::datatypes::Int64Type;
use bellwether_source::canonical::{
    encode_decimal128, encode_f64, encode_i64, encode_temporal, encode_u64, encode_utf8, TU_DAY,
    TU_MICROSECOND, UNIT_DATE32, UNIT_TIMESTAMP,
};
use bellwether_source::{fingerprint, ParquetSource};
use bellwether_types::{format::ENCODING_VERSION, Error, RowGroupMeta, Source};
use common::{
    csv_source, fixture, fixture_dir, from_hex, jsonl_source, load_expected, parquet_fixtures,
    text_rows, to_expected, TEXT_CHUNK_SIZE,
};

fn row_groups(source: &dyn Source, name: &str) -> Vec<RowGroupMeta> {
    let fp = fingerprint(&fixture(name)).unwrap();
    source.row_groups(&fp).unwrap()
}

fn source_for(format: &str) -> Box<dyn Source> {
    match format {
        "parquet" => Box::new(ParquetSource::new()),
        "csv" => Box::new(csv_source()),
        "jsonl" => Box::new(jsonl_source()),
        other => panic!("unknown fixture format {other}"),
    }
}

#[test]
fn stats_match_expected_json() {
    let expected = load_expected();
    assert_eq!(expected.encoding_version, ENCODING_VERSION);
    assert_eq!(expected.text_chunk_size, TEXT_CHUNK_SIZE);
    assert_eq!(expected.files.len(), 7);
    for file in &expected.files {
        let source = source_for(&file.format);
        let fp = fingerprint(&fixture(&file.file)).unwrap();
        assert_eq!(fp.size, file.size, "{}: size", file.file);
        let got = to_expected(&source.row_groups(&fp).unwrap());
        assert_eq!(got, file.row_groups, "{}: row groups", file.file);
    }
}

#[test]
fn expected_json_has_hand_checkable_values() {
    let expected = load_expected();
    let get = |file: &str, rg: usize, col: &str| {
        let f = expected.files.iter().find(|f| f.file == file).unwrap();
        f.row_groups[rg]
            .stats
            .iter()
            .find(|s| s.column == col)
            .unwrap()
            .clone()
    };
    let s = get("multi_rg.parquet", 0, "id");
    assert_eq!(from_hex(s.min.as_ref().unwrap()), encode_i64(1));
    assert_eq!(from_hex(s.max.as_ref().unwrap()), encode_i64(4));
    let s = get("multi_rg.parquet", 2, "name");
    assert_eq!(from_hex(s.min.as_ref().unwrap()), encode_utf8("ibis"));
    // 2024-01-01T00:00:00Z, encoded in the dataset unit (microseconds).
    let s = get("timestamps.parquet", 0, "ts");
    assert_eq!(
        from_hex(s.min.as_ref().unwrap()),
        encode_temporal(UNIT_TIMESTAMP, TU_MICROSECOND, 1_704_067_200_000_000)
    );
    let s = get("timestamps.parquet", 1, "day");
    assert_eq!(
        from_hex(s.max.as_ref().unwrap()),
        encode_temporal(UNIT_DATE32, TU_DAY, 19724)
    );
    // Edge cases. NaN is excluded from float bounds; an all-NaN row group
    // has unknown bounds.
    let s = get("edge.parquet", 0, "f");
    assert_eq!(from_hex(s.min.as_ref().unwrap()), encode_f64(-3.0));
    assert_eq!(from_hex(s.max.as_ref().unwrap()), encode_f64(1.5));
    let s = get("edge.parquet", 1, "f");
    assert_eq!((s.min, s.max), (None, None));
    // UInt32 >= 2^31 orders as unsigned.
    let s = get("edge.parquet", 0, "u");
    assert_eq!(from_hex(s.min.as_ref().unwrap()), encode_u64(5));
    assert_eq!(from_hex(s.max.as_ref().unwrap()), encode_u64(3_000_000_000));
    let s = get("edge.parquet", 1, "u");
    assert_eq!(from_hex(s.max.as_ref().unwrap()), encode_u64(4_294_967_295));
    // Non-ASCII strings order bytewise on UTF-8.
    let s = get("edge.parquet", 0, "s");
    assert_eq!(from_hex(s.min.as_ref().unwrap()), encode_utf8("zebra"));
    assert_eq!(
        from_hex(s.max.as_ref().unwrap()),
        encode_utf8("\u{65e5}\u{672c}")
    );
    let s = get("edge.parquet", 1, "s");
    assert_eq!(
        from_hex(s.max.as_ref().unwrap()),
        encode_utf8("\u{dc}n\u{ef}code")
    );
    // Decimal(38, 4), stored as FIXED_LEN_BYTE_ARRAY.
    let s = get("edge.parquet", 0, "d");
    assert_eq!(from_hex(s.min.as_ref().unwrap()), encode_decimal128(-1));
    assert_eq!(
        from_hex(s.max.as_ref().unwrap()),
        encode_decimal128(12_345_678_901_234_567_890_123_456_789_012)
    );
    let s = get("edge.parquet", 1, "d");
    assert_eq!(
        from_hex(s.max.as_ref().unwrap()),
        encode_decimal128(99_999_999_999_999_999_999_999_999_999_999_999_999)
    );
    // Text: first CSV chunk holds rows (7, gamma, 3.5, 00:00) and
    // (3, alpha, -1.25, 01:00).
    let s = get("events.csv", 0, "id");
    assert_eq!(from_hex(s.min.as_ref().unwrap()), encode_i64(3));
    assert_eq!(from_hex(s.max.as_ref().unwrap()), encode_i64(7));
    let s = get("events.csv", 0, "name");
    assert_eq!(from_hex(s.min.as_ref().unwrap()), encode_utf8("alpha"));
    assert_eq!(from_hex(s.max.as_ref().unwrap()), encode_utf8("gamma"));
    let s = get("events.csv", 0, "amount");
    assert_eq!(from_hex(s.min.as_ref().unwrap()), encode_f64(-1.25));
    let s = get("events.csv", 0, "ts");
    assert_eq!(
        from_hex(s.max.as_ref().unwrap()),
        encode_temporal(UNIT_TIMESTAMP, TU_MICROSECOND, 1_704_070_800_000_000)
    );
    // With a supplied schema, JSON lines `ts` is a timestamp too.
    let s = get("events.jsonl", 0, "ts");
    assert_eq!(
        from_hex(s.min.as_ref().unwrap()),
        encode_temporal(UNIT_TIMESTAMP, TU_MICROSECOND, 1_704_067_200_000_000)
    );
    let s = get("nulls.parquet", 1, "key");
    assert_eq!((s.min, s.max, s.null_count), (None, None, Some(2)));
    let s = get("nulls.parquet", 0, "val");
    assert_eq!(from_hex(s.max.as_ref().unwrap()), encode_u64(50));
    for rg in 0..2 {
        assert!(get("bloom.parquet", rg, "sku").has_bloom);
        assert!(!get("bloom.parquet", rg, "id").has_bloom);
    }
    let csv = expected
        .files
        .iter()
        .find(|f| f.file == "events.csv")
        .unwrap();
    let rows: u64 = csv.row_groups.iter().map(|r| r.rows).sum();
    assert_eq!(rows, 12);
    assert!(csv.row_groups.len() > 1);
}

#[test]
fn parquet_row_groups_report_structure() {
    let rgs = row_groups(&ParquetSource::new(), "multi_rg.parquet");
    assert_eq!(
        rgs.iter().map(|r| r.rows).collect::<Vec<_>>(),
        vec![4, 4, 2]
    );
    for (i, rg) in rgs.iter().enumerate() {
        assert_eq!(rg.id, None);
        assert_eq!(rg.index_in_file as usize, i);
        assert!(rg.bytes > 0);
        let cols: Vec<_> = rg.stats.iter().map(|s| s.column.as_str()).collect();
        assert_eq!(cols, vec!["id", "name", "amount"]);
    }
}

#[test]
fn parquet_column_bytes_sum_to_row_group_bytes() {
    let source = ParquetSource::new();
    for fx in parquet_fixtures() {
        let fp = fingerprint(&fixture(fx.name)).unwrap();
        let rgs = source.row_groups(&fp).unwrap();
        let cols = source.column_bytes(&fp).unwrap();
        assert_eq!(cols.len(), rgs.len());
        for (rg, per_col) in rgs.iter().zip(&cols) {
            assert_eq!(per_col.len(), rg.stats.len());
            assert!(per_col.iter().all(|&b| b > 0), "{}", fx.name);
            assert_eq!(per_col.iter().sum::<u64>(), rg.bytes, "{}", fx.name);
        }
    }
}

#[test]
fn parquet_read_columns_projects_one_row_group() {
    let source = ParquetSource::new();
    let rgs = row_groups(&source, "multi_rg.parquet");
    let fx = parquet_fixtures()
        .into_iter()
        .find(|f| f.name == "multi_rg.parquet")
        .unwrap();

    let batch = source
        .read_columns(&rgs[1], &["name".into(), "id".into()])
        .unwrap();
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(batch.schema().field(0).name(), "name");
    assert_eq!(batch.schema().field(1).name(), "id");
    assert_eq!(batch.num_rows(), 4);
    assert_eq!(
        batch.column(1).as_primitive::<Int64Type>(),
        &Int64Array::from(vec![5, 6, 7, 8])
    );
    let want_names = fx.batch.column_by_name("name").unwrap().slice(4, 4);
    assert_eq!(batch.column(0).as_ref(), want_names.as_ref());

    // Last, short row group; a single column.
    let batch = source.read_columns(&rgs[2], &["amount".into()]).unwrap();
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(
        batch.column(0).as_ref(),
        fx.batch
            .column_by_name("amount")
            .unwrap()
            .slice(8, 2)
            .as_ref()
    );

    // No columns: row count only.
    let batch = source.read_columns(&rgs[0], &[]).unwrap();
    assert_eq!((batch.num_columns(), batch.num_rows()), (0, 4));

    // Unknown column.
    let err = source.read_columns(&rgs[0], &["nope".into()]).unwrap_err();
    assert!(matches!(err, Error::KeySpec(_)), "{err}");
}

#[test]
fn parquet_read_columns_preserves_nulls_in_key_column() {
    let source = ParquetSource::new();
    let rgs = row_groups(&source, "nulls.parquet");
    let batch = source.read_columns(&rgs[0], &["key".into()]).unwrap();
    assert_eq!(
        batch.column(0).as_string::<i32>(),
        &StringArray::from(vec![Some("k1"), None, Some("k3"), None, Some("k0")])
    );
    let batch = source.read_columns(&rgs[1], &["key".into()]).unwrap();
    assert_eq!(batch.column(0).null_count(), 2);
}

#[test]
fn parquet_discover_and_schema() {
    let source = ParquetSource::new();
    let root = fixture_dir().to_str().unwrap().to_string();
    let mut names: Vec<_> = source
        .discover(&root)
        .unwrap()
        .into_iter()
        .map(|fp| {
            std::path::Path::new(&fp.path)
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string()
        })
        .collect();
    names.sort();
    assert_eq!(
        names,
        vec![
            "bloom.parquet",
            "edge.parquet",
            "multi_rg.parquet",
            "nulls.parquet",
            "timestamps.parquet"
        ]
    );
    for fx in parquet_fixtures() {
        let schema = source.schema(&fixture(fx.name)).unwrap();
        assert_eq!(schema.fields(), fx.batch.schema().fields(), "{}", fx.name);
    }
}

fn text_read_back(source: &dyn Source, name: &str) {
    let rgs = row_groups(source, name);
    assert!(rgs.len() > 1, "{name}: expected several chunks");
    let mut ids = Vec::new();
    let mut names = Vec::new();
    for rg in &rgs {
        let batch = source
            .read_columns(rg, &["name".into(), "id".into()])
            .unwrap();
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema().field(0).name(), "name");
        assert_eq!(
            batch.num_rows() as u64,
            rg.rows,
            "{name} chunk {}",
            rg.index_in_file
        );
        let id = batch.column(1).as_primitive::<Int64Type>();
        ids.extend(id.iter().map(Option::unwrap));
        let n = batch.column(0).as_string::<i32>();
        names.extend(n.iter().map(|s| s.map(str::to_string)));
    }
    let want = text_rows();
    assert_eq!(ids, want.iter().map(|r| r.0).collect::<Vec<_>>());
    assert_eq!(
        names,
        want.iter()
            .map(|r| r.1.map(str::to_string))
            .collect::<Vec<_>>()
    );
}

#[test]
fn csv_read_columns_matches_rows() {
    text_read_back(&csv_source(), "events.csv");
}

#[test]
fn jsonl_read_columns_matches_rows() {
    text_read_back(&jsonl_source(), "events.jsonl");
}

#[test]
fn text_stats_only_for_requested_columns() {
    let source = bellwether_source::ChunkedTextSource::new(bellwether_source::TextFormat::csv())
        .with_chunk_size(TEXT_CHUNK_SIZE)
        .with_stats_columns(["id"]);
    for rg in row_groups(&source, "events.csv") {
        for s in &rg.stats {
            if s.column == "id" {
                assert!(s.min.is_some() && s.max.is_some());
                assert_eq!(s.null_count, Some(0));
            } else {
                assert_eq!((&s.min, &s.max, s.null_count), (&None, &None, None));
            }
        }
    }
}

#[test]
fn text_default_chunk_is_one_row_group() {
    let source = bellwether_source::ChunkedTextSource::new(bellwether_source::TextFormat::csv());
    let rgs = row_groups(&source, "events.csv");
    assert_eq!(rgs.len(), 1);
    assert_eq!(rgs[0].rows, 12);
}

#[test]
fn parquet_stats_encode_as_the_dataset_type() {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;
    // multi_rg.amount is Decimal128(10, 2) in the file; the dataset says (12, 4).
    let dataset = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("amount", DataType::Decimal128(12, 4), false),
    ]));
    let source = ParquetSource::new().with_schema(Arc::clone(&dataset));
    let rgs = row_groups(&source, "multi_rg.parquet");
    let amount = rgs[0].stats.iter().find(|s| s.column == "amount").unwrap();
    assert_eq!(
        amount.min.as_deref(),
        Some(encode_decimal128(-25_000).as_slice())
    );
    assert_eq!(
        amount.max.as_deref(),
        Some(encode_decimal128(105_000).as_slice())
    );
    assert_eq!(
        source.schema(&fixture("multi_rg.parquet")).unwrap(),
        dataset
    );

    // timestamps.ts is microseconds in the file; the dataset says ms.
    let dataset = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        false,
    )]));
    let source = ParquetSource::new().with_schema(dataset);
    let rgs = row_groups(&source, "timestamps.parquet");
    let ts = rgs[0].stats.iter().find(|s| s.column == "ts").unwrap();
    assert_eq!(
        ts.min.as_deref(),
        Some(
            encode_temporal(
                UNIT_TIMESTAMP,
                bellwether_source::canonical::TU_MILLISECOND,
                1_704_067_200_000
            )
            .as_slice()
        )
    );

    // A dataset scale the file's values cannot be represented at: unknown
    // bounds, never rounded.
    let dataset = Arc::new(Schema::new(vec![Field::new(
        "amount",
        DataType::Decimal128(10, 1),
        false,
    )]));
    let source = ParquetSource::new().with_schema(dataset);
    let rgs = row_groups(&source, "multi_rg.parquet");
    let amount = rgs[0].stats.iter().find(|s| s.column == "amount").unwrap();
    // rg0 amounts: 10.50, -2.50, 0.99, 0.00; 0.99 is not exact at scale 1,
    // but only min/max are encoded: max 10.50 -> 105, min -2.50 -> -25.
    assert_eq!(
        amount.min.as_deref(),
        Some(encode_decimal128(-25).as_slice())
    );
    let rg1 = rgs[1].stats.iter().find(|s| s.column == "amount").unwrap();
    // rg1 min is -0.01: not representable at scale 1.
    assert_eq!(rg1.min, None);
}

#[test]
fn inferred_text_types_may_differ_between_formats() {
    use bellwether_source::{ChunkedTextSource, TextFormat};
    let csv = ChunkedTextSource::new(TextFormat::csv());
    let json = ChunkedTextSource::new(TextFormat::JsonLines);
    let c = csv.schema(&fixture("events.csv")).unwrap();
    let j = json.schema(&fixture("events.jsonl")).unwrap();
    assert!(matches!(
        c.field_with_name("ts").unwrap().data_type(),
        arrow::datatypes::DataType::Timestamp(_, _)
    ));
    assert_eq!(
        j.field_with_name("ts").unwrap().data_type(),
        &arrow::datatypes::DataType::Utf8
    );
}
