//! A Parquet folder whose files store one decimal column at different
//! precisions and scales infers it at the widest (contract v3 #33): the
//! largest scale beside the most integer digits (precision minus scale) of
//! any file, whichever file is first, so every file's values convert
//! exactly. Past 38 digits it is a `Decimal256`.

#![allow(clippy::unwrap_used, missing_docs)]

use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Decimal128Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether_source::ParquetSource;
use bellwether_types::Source;
use parquet::arrow::ArrowWriter;

fn write(dir: &Path, name: &str, precision: u8, scale: i8) {
    let dt = DataType::Decimal128(precision, scale);
    let schema = Arc::new(Schema::new(vec![Field::new("amount", dt, true)]));
    let values = Decimal128Array::from(vec![Some(1_i128), None])
        .with_precision_and_scale(precision, scale)
        .unwrap();
    let batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values) as ArrayRef]).unwrap();
    let mut w =
        ArrowWriter::try_new(std::fs::File::create(dir.join(name)).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn inferred(files: &[(&str, u8, i8)]) -> bellwether_types::Result<DataType> {
    let dir = tempfile::tempdir().unwrap();
    for (name, p, s) in files {
        write(dir.path(), name, *p, *s);
    }
    let schema = ParquetSource::new().schema(dir.path().to_str().unwrap())?;
    Ok(schema
        .field_with_name("amount")
        .unwrap()
        .data_type()
        .clone())
}

#[test]
fn a_decimal_column_takes_the_most_integer_digits_beside_the_largest_scale() {
    // 8, 11 and 1 integer digits; scales 2, 1 and 4: 11 + 4 digits.
    let files = [
        ("a.parquet", 10, 2),
        ("b.parquet", 12, 1),
        ("c.parquet", 5, 4),
    ];
    assert_eq!(inferred(&files).unwrap(), DataType::Decimal128(15, 4));
    // The same whichever file is listed first.
    let reordered = [
        ("a.parquet", 12, 1),
        ("b.parquet", 5, 4),
        ("c.parquet", 10, 2),
    ];
    assert_eq!(inferred(&reordered).unwrap(), DataType::Decimal128(15, 4));
    // 38 integer digits beside a scale of 9: a Decimal256 of 47 digits.
    let wide = [("a.parquet", 10, 9), ("b.parquet", 38, 0)];
    assert_eq!(inferred(&wide).unwrap(), DataType::Decimal256(47, 9));
    // 38 integer digits beside a scale of 38: 76, the most a decimal holds.
    let widest = [("a.parquet", 38, 38), ("b.parquet", 38, 0)];
    assert_eq!(inferred(&widest).unwrap(), DataType::Decimal256(76, 38));
}
