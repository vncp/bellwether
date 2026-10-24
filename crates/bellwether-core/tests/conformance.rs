//! Encoding conformance with the pinned test vectors of `docs/format.md`
//! (encoding version 1).
//!
//! Every vector is reproduced byte for byte through both encoder entry
//! points: the scalar path (`encode_value_as`) and the Arrow path
//! (`encode_array_value_as` over a one-element array of the dataset type).
//! A second test parses the table in `docs/format.md` and requires it to
//! list exactly the vectors encoded here, so neither the document nor this
//! file can gain or change a vector without the other. Any source-side
//! statistics encoder must reproduce the same table.
#![allow(clippy::unwrap_used, missing_docs)]

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use bellwether_core::encoding::{encode_array_value_as, encode_value_as, Value};

/// One pinned vector: the label exactly as `docs/format.md` writes it, the
/// value, the dataset type it is encoded under, a one-element array holding
/// it, and the expected bytes as space-separated hex.
struct Vector {
    label: &'static str,
    value: Value<'static>,
    dataset: DataType,
    array: ArrayRef,
    hex: &'static str,
}

fn pinned() -> Vec<Vector> {
    let us = DataType::Timestamp(TimeUnit::Microsecond, None);
    vec![
        Vector {
            label: "Timestamp(\u{b5}s) = 1",
            value: Value::Timestamp {
                value: 1,
                unit: TimeUnit::Microsecond,
            },
            dataset: us,
            array: Arc::new(TimestampMicrosecondArray::from(vec![1i64])),
            hex: "08 04 03 80 00 00 00 00 00 00 01",
        },
        Vector {
            label: "Int64 = 0",
            value: Value::Int(0),
            dataset: DataType::Int64,
            array: Arc::new(Int64Array::from(vec![0i64])),
            hex: "02 80 00 00 00 00 00 00 00",
        },
        Vector {
            label: "Int64 = -1",
            value: Value::Int(-1),
            dataset: DataType::Int64,
            array: Arc::new(Int64Array::from(vec![-1i64])),
            hex: "02 7f ff ff ff ff ff ff ff",
        },
        Vector {
            label: "Utf8 = \"a\"",
            value: Value::Utf8("a"),
            dataset: DataType::Utf8,
            array: Arc::new(StringArray::from(vec!["a"])),
            hex: "06 61 00 00",
        },
        Vector {
            // the document writes the NUL byte as the two characters \0
            label: "Utf8 = \"a\\0b\"",
            value: Value::Utf8("a\0b"),
            dataset: DataType::Utf8,
            array: Arc::new(StringArray::from(vec!["a\0b"])),
            hex: "06 61 00 ff 62 00 00",
        },
        Vector {
            label: "Boolean = true",
            value: Value::Boolean(true),
            dataset: DataType::Boolean,
            array: Arc::new(BooleanArray::from(vec![true])),
            hex: "01 01",
        },
    ]
}

fn bytes(hex: &str) -> Vec<u8> {
    hex.split_whitespace()
        .map(|b| u8::from_str_radix(b, 16).unwrap())
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

#[test]
fn pinned_vectors_encode_byte_for_byte() {
    for v in pinned() {
        let want = bytes(v.hex);
        let mut scalar = Vec::new();
        encode_value_as(&v.value, &v.dataset, &mut scalar).unwrap();
        assert_eq!(hex(&scalar), hex(&want), "scalar path: {}", v.label);
        let mut arrow = Vec::new();
        assert!(encode_array_value_as(v.array.as_ref(), 0, &v.dataset, &mut arrow).unwrap());
        assert_eq!(hex(&arrow), hex(&want), "Arrow path: {}", v.label);
    }
}

/// The `| `label` | `hex` |` rows of the pinned-vector table in
/// `docs/format.md`.
fn format_md_vectors() -> BTreeMap<String, String> {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/format.md");
    let doc = std::fs::read_to_string(path).unwrap();
    let section = doc
        .split("## Pinned test vectors")
        .nth(1)
        .expect("docs/format.md has a pinned test vectors section");
    let section = section.split("\n## ").next().unwrap_or(section);
    let mut rows = BTreeMap::new();
    for line in section.lines() {
        let cells: Vec<&str> = line
            .split('|')
            .map(str::trim)
            .filter(|c| !c.is_empty())
            .collect();
        if let [label, value] = cells[..] {
            if let (Some(label), Some(value)) = (
                label.strip_prefix('`').and_then(|l| l.strip_suffix('`')),
                value.strip_prefix('`').and_then(|v| v.strip_suffix('`')),
            ) {
                rows.insert(label.to_string(), hex(&bytes(value)));
            }
        }
    }
    rows
}

#[test]
fn pinned_table_matches_docs_format_md() {
    let doc = format_md_vectors();
    let ours: BTreeMap<String, String> = pinned()
        .into_iter()
        .map(|v| (v.label.to_string(), hex(&bytes(v.hex))))
        .collect();
    for (label, want) in &doc {
        match ours.get(label) {
            Some(got) => assert_eq!(got, want, "vector {label} differs from docs/format.md"),
            None => panic!("docs/format.md pins {label} = {want}; add it to tests/conformance.rs"),
        }
    }
    for label in ours.keys() {
        assert!(
            doc.contains_key(label),
            "tests/conformance.rs encodes {label}, which docs/format.md does not pin"
        );
    }
}

#[test]
fn sidecar_payload_vectors_match_docs_format_md() {
    use bellwether_core::{BitemporalChain, KeyBounds};
    use bellwether_types::RowGroupId;

    let kb = KeyBounds::from_ranges(vec![(RowGroupId(3), 100, 250)]);
    let mut kb_bytes = Vec::new();
    kb.write_to(&mut kb_bytes).unwrap();
    assert_eq!(
        hex(&kb_bytes),
        "01 00 00 00 03 00 00 00 64 00 00 00 00 00 00 00 fa 00 00 00 00 00 00 00"
    );
    assert_eq!(KeyBounds::read_from(&kb_bytes).unwrap(), kb);

    let bc = BitemporalChain::from_versions(vec![(10, 20, RowGroupId(1))]);
    let mut bc_bytes = Vec::new();
    bc.write_to(&mut bc_bytes).unwrap();
    assert_eq!(
        hex(&bc_bytes),
        "01 00 00 00 0a 00 00 00 00 00 00 00 14 00 00 00 00 00 00 00 01 00 00 00"
    );
    assert_eq!(BitemporalChain::read_from(&bc_bytes).unwrap(), bc);
}
