//! Manifests written before the 2026-10-24 dependency upgrade spell column
//! types the way Arrow 53 printed them (`Timestamp(Microsecond,
//! Some("UTC"))`); Arrow 59 prints `Timestamp(µs, "UTC")`, and storage
//! version 2 (M5) writes core's `ColumnType` JSON. An index built by the
//! old release must still open, plan, and take incremental builds, and its
//! first new commit rewrites the spelling to the current form without
//! changing any type (contract v3, entry 9).

#![allow(clippy::unwrap_used, missing_docs)]

mod common;

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Int64Array, RecordBatch, StringArray, Time32MillisecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_build::{BuildOptions, Lookup, SourceKind};
use bellwether_types::Store;
use bytes::Bytes;
use common::*;

/// The columns and, for each, the spelling Arrow 53 wrote into manifests.
fn columns() -> Vec<(Field, &'static str)> {
    vec![
        (Field::new("id", DataType::Int64, false), "Int64"),
        (Field::new("grp", DataType::Utf8, true), "Utf8"),
        (
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            r#"Timestamp(Microsecond, Some("UTC"))"#,
        ),
        (
            Field::new("tod", DataType::Time64(TimeUnit::Nanosecond), true),
            "Time64(Nanosecond)",
        ),
        (
            Field::new("ms", DataType::Time32(TimeUnit::Millisecond), true),
            "Time32(Millisecond)",
        ),
    ]
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(
        columns().into_iter().map(|(f, _)| f).collect::<Vec<_>>(),
    ))
}

fn write_file(s: &Scratch, name: &str, base: i64) {
    let ids: Vec<i64> = (base..base + 8).collect();
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids.clone())) as ArrayRef,
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|i| format!("g{}", i % 3))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(
                TimestampMicrosecondArray::from(ids.iter().map(|i| i * 1_000).collect::<Vec<_>>())
                    .with_timezone("UTC"),
            ),
            Arc::new(Time64NanosecondArray::from(
                ids.iter().map(|i| i * 7).collect::<Vec<_>>(),
            )),
            Arc::new(Time32MillisecondArray::from(
                ids.iter()
                    .map(|i| i32::try_from(*i).unwrap())
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    write_parquet(&s.path(name), &batch, 4);
}

fn manifest_json(store: &bellwether_store::ObjectStoreBackend) -> serde_json::Value {
    serde_json::from_slice(&manifest_bytes(store)).unwrap()
}

/// Each column's `data_type` as written: a string as itself, an object as
/// its compact JSON.
fn spellings(m: &serde_json::Value) -> Vec<String> {
    m["schema"]["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| match &c["data_type"] {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        })
        .collect()
}

/// The storage version 2 spelling of every column: core's `ColumnType`
/// JSON.
fn typed() -> Vec<String> {
    columns()
        .iter()
        .map(|(f, _)| {
            let t = bellwether_core::segment::ColumnType::of(f.data_type()).unwrap();
            serde_json::to_value(t).unwrap().to_string()
        })
        .collect()
}

fn lookup(keys: &[i64]) -> Lookup {
    Lookup::Keys {
        columns: cols(&["id"]),
        keys: keys.iter().map(|k| enc_i64(*k)).collect(),
    }
}

#[test]
fn an_index_with_arrow_53_type_spellings_opens_plans_and_rebuilds() {
    let s = Scratch::new();
    write_file(&s, "a.parquet", 0);
    write_file(&s, "b.parquet", 100);
    let store = memory_store();
    let b = builder(&store);
    let specs = [exact(&["id"])];
    let opts = BuildOptions {
        schema: Some(schema()),
        ..BuildOptions::new(SourceKind::Parquet)
    };
    build_ok(&b, &s.root_str(), &specs, &opts);
    let keys = [3, 104, 999];
    let before = planner(&store).plan(&specs, &lookup(&keys)).unwrap();

    // Hand-write the old release's spelling into every column of the
    // committed manifest, exactly as Arrow 53 printed each type.
    let path = "idx/manifest.json";
    let (_, version) = store.get(path).unwrap();
    let mut m = manifest_json(&store);
    assert_eq!(spellings(&m), typed(), "this build writes ColumnType JSON");
    assert_eq!(typed()[0], r#"{"type":"int64"}"#);
    for (col, (_, old)) in m["schema"]["columns"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .zip(columns())
    {
        col["data_type"] = serde_json::Value::String(old.to_string());
    }
    let old_manifest = serde_json::to_vec(&m).unwrap();
    store
        .put_if_version(path, Bytes::from(old_manifest), &version)
        .unwrap();
    assert_eq!(
        spellings(&manifest_json(&store)),
        columns()
            .iter()
            .map(|(_, old)| (*old).to_string())
            .collect::<Vec<_>>()
    );

    // The old manifest opens and plans exactly as the new one did.
    let after = planner(&store).plan(&specs, &lookup(&keys)).unwrap();
    assert_eq!(after, before);

    // An incremental build with no supplied schema takes the dataset schema
    // from the old manifest: it parses, the new file is indexed under the
    // same types, and the commit writes the current spelling.
    write_file(&s, "c.parquet", 200);
    let r = build_ok(
        &b,
        &s.root_str(),
        &specs,
        &BuildOptions::new(SourceKind::Parquet),
    );
    assert_eq!((r.files_new, r.files_unchanged), (1, 2));
    assert_eq!(spellings(&manifest_json(&store)), typed());
    let plan = planner(&store).plan(&specs, &lookup(&[203])).unwrap();
    assert_eq!(plan_rgs(&plan).len(), 1, "the new file's key is served");
}
