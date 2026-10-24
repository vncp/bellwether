//! Shared fixture definitions. The generator (`generate_fixtures.rs`) writes
//! these files into `fixtures/source/`. The tests read them back and compare
//! against `fixtures/source/expected.json`.

#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::format_push_string,
    clippy::format_collect,
    clippy::too_many_lines,
    missing_docs
)]

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Date32Array, Decimal128Array, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray, TimestampMicrosecondArray, UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_source::{ChunkedTextSource, TextFormat};
use bellwether_types::RowGroupMeta;
use serde::{Deserialize, Serialize};

/// Chunk size the text fixtures are indexed with in `expected.json`.
pub const TEXT_CHUNK_SIZE: u64 = 64;

/// Columns of the CSV and JSON lines fixtures.
pub const TEXT_COLUMNS: [&str; 4] = ["id", "name", "amount", "ts"];

pub fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/source")
}

pub fn fixture(name: &str) -> String {
    fixture_dir().join(name).to_str().unwrap().to_string()
}

/// One Parquet fixture: the batch written, the row-group size, and the
/// columns that carry an embedded bloom filter.
pub struct ParquetFixture {
    pub name: &'static str,
    pub batch: RecordBatch,
    pub max_row_group_size: usize,
    pub bloom_columns: Vec<&'static str>,
}

fn batch(cols: Vec<(&str, ArrayRef)>) -> RecordBatch {
    RecordBatch::try_from_iter(cols).unwrap()
}

pub fn parquet_fixtures() -> Vec<ParquetFixture> {
    let names = [
        "ant", "bee", "cat", "dog", "eel", "fox", "gnu", "hen", "ibis", "jay",
    ];
    let multi = batch(vec![
        (
            "id",
            Arc::new(Int64Array::from((1..=10).collect::<Vec<i64>>())) as ArrayRef,
        ),
        ("name", Arc::new(StringArray::from(names.to_vec()))),
        (
            "amount",
            Arc::new(
                Decimal128Array::from(vec![1050, -250, 99, 0, 123_456, 7, -1, 500, 42, 1000])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
        ),
    ]);

    // 2024-01-01T00:00:00Z in microseconds, then hourly, deliberately unsorted.
    let base = 1_704_067_200_000_000_i64;
    let hour = 3_600_000_000_i64;
    let ts = batch(vec![
        (
            "id",
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])) as ArrayRef,
        ),
        (
            "ts",
            Arc::new(
                TimestampMicrosecondArray::from(vec![
                    base + 2 * hour,
                    base,
                    base + hour,
                    base + 30 * hour,
                    base + 24 * hour,
                    base + 25 * hour,
                ])
                .with_timezone("UTC"),
            ),
        ),
        (
            "day",
            Arc::new(Date32Array::from(vec![
                19723, 19723, 19723, 19724, 19724, 19724,
            ])),
        ),
    ]);

    let nulls = batch(vec![
        (
            "key",
            Arc::new(StringArray::from(vec![
                Some("k1"),
                None,
                Some("k3"),
                None,
                Some("k0"),
                None,
                None,
            ])) as ArrayRef,
        ),
        (
            "val",
            Arc::new(UInt32Array::from(vec![10, 20, 30, 40, 50, 60, 70])),
        ),
        (
            "score",
            Arc::new(Float64Array::from(vec![
                Some(1.5),
                Some(-2.25),
                None,
                Some(0.0),
                Some(9.75),
                Some(-0.5),
                None,
            ])),
        ),
    ]);

    let bloom = batch(vec![
        (
            "id",
            Arc::new(Int64Array::from(vec![100, 101, 102, 103, 104, 105])) as ArrayRef,
        ),
        (
            "sku",
            Arc::new(StringArray::from(vec![
                "sku-c", "sku-a", "sku-b", "sku-f", "sku-e", "sku-d",
            ])),
        ),
    ]);

    // Edge cases: NaN, UInt32 >= 2^31, non-ASCII strings, Decimal with
    // precision > 18 (stored as FIXED_LEN_BYTE_ARRAY).
    let edge = batch(vec![
        (
            "f",
            Arc::new(Float64Array::from(vec![
                f64::NAN,
                1.5,
                -3.0,
                f64::NAN,
                f64::NAN,
            ])) as ArrayRef,
        ),
        (
            "u",
            Arc::new(UInt32Array::from(vec![
                3_000_000_000,
                5,
                2_147_483_648,
                4_294_967_295,
                0,
            ])),
        ),
        (
            "s",
            Arc::new(StringArray::from(vec![
                "\u{e9}",
                "zebra",
                "\u{65e5}\u{672c}",
                "\u{dc}n\u{ef}code",
                "abc",
            ])),
        ),
        (
            "d",
            Arc::new(
                Decimal128Array::from(vec![
                    12_345_678_901_234_567_890_123_456_789_012_i128,
                    -1,
                    0,
                    99_999_999_999_999_999_999_999_999_999_999_999_999,
                    -5,
                ])
                .with_precision_and_scale(38, 4)
                .unwrap(),
            ),
        ),
    ]);

    vec![
        ParquetFixture {
            name: "multi_rg.parquet",
            batch: multi,
            max_row_group_size: 4,
            bloom_columns: vec![],
        },
        ParquetFixture {
            name: "timestamps.parquet",
            batch: ts,
            max_row_group_size: 3,
            bloom_columns: vec![],
        },
        ParquetFixture {
            name: "nulls.parquet",
            batch: nulls,
            max_row_group_size: 5,
            bloom_columns: vec![],
        },
        ParquetFixture {
            name: "bloom.parquet",
            batch: bloom,
            max_row_group_size: 3,
            bloom_columns: vec!["sku"],
        },
        ParquetFixture {
            name: "edge.parquet",
            batch: edge,
            max_row_group_size: 3,
            bloom_columns: vec![],
        },
    ]
}

/// The CSV fixture. Includes an empty (null) `name` and no trailing newline
/// on the last line, to exercise chunking at end of file.
pub fn csv_text() -> String {
    let mut s = String::from("id,name,amount,ts\n");
    for (i, row) in text_rows().iter().enumerate() {
        let (id, name, amount, ts) = row;
        s.push_str(&format!("{id},{},{amount},{ts}", name.unwrap_or("")));
        if i + 1 < text_rows().len() {
            s.push('\n');
        }
    }
    s
}

/// The JSON lines fixture, with the same rows. A null `name` is omitted.
pub fn jsonl_text() -> String {
    let mut s = String::new();
    for (id, name, amount, ts) in text_rows() {
        match name {
            Some(n) => s.push_str(&format!(
                "{{\"id\":{id},\"name\":\"{n}\",\"amount\":{amount},\"ts\":\"{ts}\"}}\n"
            )),
            None => s.push_str(&format!(
                "{{\"id\":{id},\"amount\":{amount},\"ts\":\"{ts}\"}}\n"
            )),
        }
    }
    s
}

#[allow(clippy::type_complexity)]
pub fn text_rows() -> Vec<(i64, Option<&'static str>, f64, &'static str)> {
    vec![
        (7, Some("gamma"), 3.5, "2024-01-01T00:00:00"),
        (3, Some("alpha"), -1.25, "2024-01-01T01:00:00"),
        (12, None, 0.0, "2024-01-01T02:00:00"),
        (1, Some("delta"), 10.0, "2024-01-02T00:00:00"),
        (9, Some("beta"), 2.75, "2024-01-02T01:00:00"),
        (4, Some("epsilon"), 100.5, "2024-01-02T02:00:00"),
        (15, Some("zeta"), -7.0, "2024-01-03T00:00:00"),
        (2, Some("eta"), 0.5, "2024-01-03T01:00:00"),
        (11, None, 6.0, "2024-01-03T02:00:00"),
        (5, Some("theta"), 8.25, "2024-01-04T00:00:00"),
        (8, Some("iota"), -3.5, "2024-01-04T01:00:00"),
        (6, Some("kappa"), 1.0, "2024-01-04T02:00:00"),
    ]
}

/// Dataset schema supplied to both text sources, so `ts` is a timestamp in
/// CSV and JSON lines alike.
pub fn text_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
    ]))
}

pub fn csv_source() -> ChunkedTextSource {
    ChunkedTextSource::new(TextFormat::csv())
        .with_chunk_size(TEXT_CHUNK_SIZE)
        .with_schema(text_schema())
        .with_stats_columns(TEXT_COLUMNS)
}

pub fn jsonl_source() -> ChunkedTextSource {
    ChunkedTextSource::new(TextFormat::JsonLines)
        .with_chunk_size(TEXT_CHUNK_SIZE)
        .with_schema(text_schema())
        .with_stats_columns(TEXT_COLUMNS)
}

// ---------------------------------------------------------------------------
// expected.json schema
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Expected {
    /// Canonical encoding version the hex bytes follow.
    pub encoding_version: u32,
    /// Chunk size used for the text fixtures.
    pub text_chunk_size: u64,
    pub files: Vec<ExpectedFile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpectedFile {
    pub file: String,
    pub format: String,
    pub size: u64,
    pub row_groups: Vec<ExpectedRowGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpectedRowGroup {
    pub index: u32,
    pub rows: u64,
    pub bytes: u64,
    pub stats: Vec<ExpectedStats>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpectedStats {
    pub column: String,
    /// Canonical-encoded min as lowercase hex.
    pub min: Option<String>,
    /// Canonical-encoded max as lowercase hex.
    pub max: Option<String>,
    pub null_count: Option<u64>,
    pub distinct_count: Option<u64>,
    pub has_bloom: bool,
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

pub fn from_hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

pub fn to_expected(rgs: &[RowGroupMeta]) -> Vec<ExpectedRowGroup> {
    rgs.iter()
        .map(|rg| ExpectedRowGroup {
            index: rg.index_in_file,
            rows: rg.rows,
            bytes: rg.bytes,
            stats: rg
                .stats
                .iter()
                .map(|s| ExpectedStats {
                    column: s.column.clone(),
                    min: s.min.as_deref().map(hex),
                    max: s.max.as_deref().map(hex),
                    null_count: s.null_count,
                    distinct_count: s.distinct_count,
                    has_bloom: s.has_bloom,
                })
                .collect(),
        })
        .collect()
}

pub fn load_expected() -> Expected {
    let raw = std::fs::read_to_string(fixture_dir().join("expected.json")).unwrap();
    serde_json::from_str(&raw).unwrap()
}
