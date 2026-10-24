//! Regenerates `fixtures/source/`. Run explicitly:
//!
//! ```text
//! cargo test -p bellwether-source --test generate_fixtures -- --ignored
//! ```
//!
//! For Parquet files, row counts and statistics in `expected.json` are
//! computed from the in-memory data, independently of the Parquet footer.
//! Only `bytes` and `size` are read back from the written file. For the text
//! files, `expected.json` is a golden snapshot of `ChunkedTextSource` output,
//! cross-checked by hand-written assertions in `tests/fixtures.rs`.

#![allow(clippy::unwrap_used, missing_docs)]

mod common;

use std::fs::File;

use arrow::array::Array;
use bellwether_source::canonical::{encode_value, is_nan_encoding};
use bellwether_source::fingerprint;
use bellwether_types::{format::ENCODING_VERSION, Source};
use common::{
    csv_source, csv_text, fixture, fixture_dir, hex, jsonl_source, jsonl_text, parquet_fixtures,
    to_expected, Expected, ExpectedFile, ExpectedRowGroup, ExpectedStats, ParquetFixture,
    TEXT_CHUNK_SIZE,
};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::ColumnPath;

fn write_parquet(fx: &ParquetFixture) {
    let mut props =
        WriterProperties::builder().set_max_row_group_row_count(Some(fx.max_row_group_size));
    for col in &fx.bloom_columns {
        props = props
            .set_column_bloom_filter_enabled(ColumnPath::from(*col), true)
            .set_column_bloom_filter_fpp(ColumnPath::from(*col), 0.01)
            .set_column_bloom_filter_max_ndv(ColumnPath::from(*col), 16);
    }
    let file = File::create(fixture(fx.name)).unwrap();
    let mut w = ArrowWriter::try_new(file, fx.batch.schema(), Some(props.build())).unwrap();
    w.write(&fx.batch).unwrap();
    w.close().unwrap();
}

/// Stats from the data itself: bytewise extremes of canonical encodings.
fn independent_stats(fx: &ParquetFixture, offset: usize, len: usize) -> Vec<ExpectedStats> {
    let schema = fx.batch.schema();
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(c, field)| {
            let col = fx.batch.column(c).slice(offset, len);
            let mut min: Option<Vec<u8>> = None;
            let mut max: Option<Vec<u8>> = None;
            for r in 0..col.len() {
                if let Some(e) = encode_value(col.as_ref(), r).unwrap() {
                    // Float bounds exclude NaN (Parquet writer convention).
                    if is_nan_encoding(&e) {
                        continue;
                    }
                    if min.as_ref().is_none_or(|m| e < *m) {
                        min = Some(e.clone());
                    }
                    if max.as_ref().is_none_or(|m| e > *m) {
                        max = Some(e);
                    }
                }
            }
            ExpectedStats {
                column: field.name().clone(),
                min: min.as_deref().map(hex),
                max: max.as_deref().map(hex),
                null_count: Some(u64::try_from(col.null_count()).unwrap()),
                distinct_count: None,
                has_bloom: fx.bloom_columns.contains(&field.name().as_str()),
            }
        })
        .collect()
}

fn expected_parquet(fx: &ParquetFixture) -> ExpectedFile {
    let path = fixture(fx.name);
    let reader = SerializedFileReader::new(File::open(&path).unwrap()).unwrap();
    let md = reader.metadata();
    let total = fx.batch.num_rows();
    let row_groups = (0..total.div_ceil(fx.max_row_group_size))
        .map(|i| {
            let offset = i * fx.max_row_group_size;
            let len = fx.max_row_group_size.min(total - offset);
            ExpectedRowGroup {
                index: u32::try_from(i).unwrap(),
                rows: u64::try_from(len).unwrap(),
                bytes: u64::try_from(md.row_group(i).compressed_size()).unwrap(),
                stats: independent_stats(fx, offset, len),
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(row_groups.len(), md.num_row_groups());
    ExpectedFile {
        file: fx.name.to_string(),
        format: "parquet".into(),
        size: std::fs::metadata(&path).unwrap().len(),
        row_groups,
    }
}

fn expected_text(name: &str, format: &str, source: &dyn Source) -> ExpectedFile {
    let fp = fingerprint(&fixture(name)).unwrap();
    ExpectedFile {
        file: name.to_string(),
        format: format.into(),
        size: fp.size,
        row_groups: to_expected(&source.row_groups(&fp).unwrap()),
    }
}

#[test]
#[ignore = "writes fixtures/source; run explicitly to regenerate"]
fn generate_fixtures() {
    std::fs::create_dir_all(fixture_dir()).unwrap();
    let mut files = Vec::new();
    for fx in parquet_fixtures() {
        write_parquet(&fx);
        files.push(expected_parquet(&fx));
    }
    std::fs::write(fixture("events.csv"), csv_text()).unwrap();
    std::fs::write(fixture("events.jsonl"), jsonl_text()).unwrap();
    files.push(expected_text("events.csv", "csv", &csv_source()));
    files.push(expected_text("events.jsonl", "jsonl", &jsonl_source()));
    let expected = Expected {
        encoding_version: ENCODING_VERSION,
        text_chunk_size: TEXT_CHUNK_SIZE,
        files,
    };
    let mut json = serde_json::to_string_pretty(&expected).unwrap();
    json.push('\n');
    std::fs::write(fixture_dir().join("expected.json"), json).unwrap();
}
