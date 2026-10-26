//! Every spelling of core's `ColumnType` (`docs/format.md`, "Column types
//! (M5)"): the frozen JSON a segment header records for a key or time
//! column and a storage version 2 manifest records for a column, one row
//! per variant, checked against the table in `docs/format.md` both ways
//! (each row is what the type serializes to and reads back from).

#![allow(clippy::unwrap_used, missing_docs)]

use std::path::PathBuf;

use arrow::datatypes::{DataType, TimeUnit};
use bellwether_core::segment::ColumnType;

/// One Arrow type per `ColumnType` variant (and per unit and zone form),
/// as `docs/format.md` names it.
fn types() -> Vec<(&'static str, DataType)> {
    vec![
        ("Boolean", DataType::Boolean),
        ("Int8", DataType::Int8),
        ("Int16", DataType::Int16),
        ("Int32", DataType::Int32),
        ("Int64", DataType::Int64),
        ("UInt8", DataType::UInt8),
        ("UInt16", DataType::UInt16),
        ("UInt32", DataType::UInt32),
        ("UInt64", DataType::UInt64),
        ("Float16", DataType::Float16),
        ("Float32", DataType::Float32),
        ("Float64", DataType::Float64),
        ("Decimal128(10, 2)", DataType::Decimal128(10, 2)),
        ("Decimal256(40, -2)", DataType::Decimal256(40, -2)),
        ("Utf8", DataType::Utf8),
        ("LargeUtf8", DataType::LargeUtf8),
        ("Utf8View", DataType::Utf8View),
        ("Binary", DataType::Binary),
        ("LargeBinary", DataType::LargeBinary),
        ("BinaryView", DataType::BinaryView),
        ("FixedSizeBinary(16)", DataType::FixedSizeBinary(16)),
        ("Date32", DataType::Date32),
        ("Date64", DataType::Date64),
        ("Time32(Second)", DataType::Time32(TimeUnit::Second)),
        (
            "Time32(Millisecond)",
            DataType::Time32(TimeUnit::Millisecond),
        ),
        (
            "Time64(Microsecond)",
            DataType::Time64(TimeUnit::Microsecond),
        ),
        ("Time64(Nanosecond)", DataType::Time64(TimeUnit::Nanosecond)),
        (
            "Timestamp(Second, None)",
            DataType::Timestamp(TimeUnit::Second, None),
        ),
        (
            "Timestamp(Millisecond, \"UTC\")",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        ),
        (
            "Timestamp(Microsecond, \"+01:00\")",
            DataType::Timestamp(TimeUnit::Microsecond, Some("+01:00".into())),
        ),
        (
            "Timestamp(Nanosecond, \"America/New_York\")",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("America/New_York".into())),
        ),
    ]
}

fn computed() -> Vec<(String, String)> {
    types()
        .into_iter()
        .map(|(name, dt)| {
            let t = ColumnType::of(&dt).unwrap();
            let json = serde_json::to_string(&t).unwrap();
            // It reads back to the same type.
            let back: ColumnType = serde_json::from_str(&json).unwrap();
            assert_eq!(back.data_type(), dt, "{name}");
            (name.to_string(), json)
        })
        .collect()
}

/// The rows of the `ColumnType` table in `docs/format.md`.
fn documented() -> Vec<(String, String)> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs/format.md");
    let text = std::fs::read_to_string(path).unwrap();
    let start = text
        .find("| Arrow type | `ColumnType` JSON |")
        .expect("docs/format.md has the ColumnType table");
    text[start..]
        .lines()
        .skip(2)
        .take_while(|l| l.starts_with('|'))
        .map(|l| {
            let cells: Vec<&str> = l
                .split(" | ")
                .map(|c| c.trim_matches(['|', ' ', '`']))
                .collect();
            (cells[0].to_string(), cells[1].to_string())
        })
        .collect()
}

#[test]
fn every_column_type_spelling_is_the_one_docs_format_md_lists() {
    assert_eq!(documented(), computed());
}

/// A dictionary column is recorded as its value type (the encoding sees
/// only decoded values); a type with no canonical encoding has no
/// spelling (a manifest spells it `{"arrow": "<display>"}`).
#[test]
fn a_dictionary_is_its_value_type_and_a_nested_type_has_none() {
    let dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    assert_eq!(ColumnType::of(&dict).unwrap(), ColumnType::Utf8);
    let list = DataType::List(std::sync::Arc::new(arrow::datatypes::Field::new(
        "element",
        DataType::Utf8,
        true,
    )));
    assert!(ColumnType::of(&list).is_err());
}
