//! Text sources as sides: byte-range chunks stand in for row groups, and a
//! CSV side reconciles against a JSON lines side when both are read with
//! one dataset schema.

#![allow(clippy::unwrap_used, missing_docs)]

use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_source::{ChunkedTextSource, TextFormat};
use bellwether_verify::{Mismatch, Options, ReconcileSpec, Reconciler, Side, VerifyError};

fn fixture(name: &str) -> String {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/source")
        .join(name)
        .to_str()
        .unwrap()
        .to_string()
}

/// The dataset schema the source fixtures are indexed with.
fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

fn csv() -> ChunkedTextSource {
    ChunkedTextSource::new(TextFormat::csv())
        .with_schema(schema())
        .with_chunk_size(64)
}

fn json() -> ChunkedTextSource {
    ChunkedTextSource::new(TextFormat::JsonLines)
        .with_schema(schema())
        .with_chunk_size(64)
}

#[test]
fn csv_and_json_lines_sides_read_with_one_schema_agree() {
    let (c, j) = (csv(), json());
    let (cr, jr) = (fixture("events.csv"), fixture("events.jsonl"));
    let out = Reconciler::new(
        ReconcileSpec::new(["id"]).latest_by("ts"),
        Options::default(),
    )
    .unwrap()
    .run(&Side::new(&c, &cr), &Side::new(&j, &jr))
    .unwrap();
    assert!(out.pass, "{out:#?}");
    assert_eq!(out.counts.keys_compared, 12);
    assert!(out.sides[0].row_groups > 1, "several byte-range chunks");
    assert_eq!(out.sides[0].rows, 12);
}

#[test]
fn a_changed_text_row_is_found() {
    let dir = tempfile::tempdir().unwrap();
    let edited = std::fs::read_to_string(fixture("events.csv"))
        .unwrap()
        .replace("9,beta,2.75", "9,beta,2.76");
    let path = dir.path().join("events.csv");
    std::fs::write(&path, edited).unwrap();
    let (c, j) = (csv(), json());
    let (cr, jr) = (path.to_str().unwrap().to_string(), fixture("events.jsonl"));
    let out = Reconciler::new(ReconcileSpec::new(["id"]), Options::default())
        .unwrap()
        .run(&Side::new(&c, &cr), &Side::new(&j, &jr))
        .unwrap();
    assert_eq!(out.counts.mismatched, 1);
    let Mismatch::RowHashDiffers { key, .. } = &out.mismatches[0] else {
        panic!("{:?}", out.mismatches);
    };
    assert_eq!(key.values, [Some("9".to_string())]);
}

#[test]
fn inferred_text_schemas_that_disagree_are_refused() {
    // Without a supplied schema CSV infers `ts` as a timestamp and JSON
    // lines keeps it a string: no shared encoding, so an error.
    let c = ChunkedTextSource::new(TextFormat::csv());
    let j = ChunkedTextSource::new(TextFormat::JsonLines);
    let (cr, jr) = (fixture("events.csv"), fixture("events.jsonl"));
    let err = Reconciler::new(ReconcileSpec::new(["id"]), Options::default())
        .unwrap()
        .run(&Side::new(&c, &cr), &Side::new(&j, &jr))
        .unwrap_err();
    assert!(
        matches!(err, VerifyError::Schema(ref m) if m.contains("\"ts\"")),
        "{err}"
    );
}
