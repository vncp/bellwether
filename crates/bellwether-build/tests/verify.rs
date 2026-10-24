//! `verify()` re-fingerprints every tracked file under the manifest's
//! policy and reports drift without writing anything; only the `full`
//! policy sees an in-place patch outside the last 64 KiB (ADR 013).

#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::cast_possible_wrap,
    clippy::assert_is_empty,
    clippy::too_many_lines,
    clippy::type_complexity,
    clippy::cast_precision_loss,
    clippy::redundant_closure_for_method_calls,
    clippy::format_push_string
)]

mod common;

use std::io::{Seek, SeekFrom, Write};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use bellwether_build::{BuildOptions, SourceKind};
use bellwether_source::FingerprintPolicy;
use common::*;

#[test]
fn verify_reports_changed_and_missing_files_and_writes_nothing() {
    let s = Scratch::new();
    for f in ["a", "b", "c"] {
        write_numbered(&s.path(&format!("{f}.parquet")), 0, 8, 4);
    }
    let store = memory_store();
    let b = builder(&store);
    build_ok(
        &b,
        &s.root_str(),
        &[exact(&["id"])],
        &parquet_opts(&simple_schema()),
    );
    assert!(b.verify().unwrap().is_empty());
    write_numbered(&s.path("a.parquet"), 50, 12, 4);
    std::fs::remove_file(s.path("c.parquet")).unwrap();
    let before = manifest_bytes(&store);
    let drift = b.verify().unwrap();
    assert_eq!(manifest_bytes(&store), before, "verify writes nothing");
    assert_eq!(
        drift.iter().map(|d| d.path.clone()).collect::<Vec<_>>(),
        vec![s.path_str("a.parquet"), s.path_str("c.parquet")]
    );
    let new_a = drift[0].new.as_ref().unwrap();
    assert_ne!(new_a.size, drift[0].old.size);
    assert!(
        drift[1].new.is_none(),
        "a missing file has no new fingerprint"
    );
    // the next build retires both and re-indexes a
    let r = build_ok(
        &b,
        &s.root_str(),
        &[exact(&["id"])],
        &parquet_opts(&simple_schema()),
    );
    assert_eq!((r.files_changed, r.files_removed), (1, 1));
    assert!(b.verify().unwrap().is_empty());
}

/// A CSV file over 64 KiB, patched in place near its start with its size
/// and mtime restored.
fn patched_csv_case(policy: Option<FingerprintPolicy>) -> usize {
    let s = Scratch::new();
    let path = s.path("big.csv");
    let mut text = String::from("id,name\n");
    for i in 0..12_000 {
        text.push_str(&format!("{i},name-{i:08}\n"));
    }
    assert!(text.len() > 3 * 64 * 1024);
    std::fs::write(&path, &text).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let store = memory_store();
    let b = builder(&store);
    let opts = BuildOptions {
        schema: Some(schema),
        chunk_size: Some(32 * 1024),
        fingerprint_policy: policy,
        ..BuildOptions::new(SourceKind::csv())
    };
    build_ok(&b, &s.root_str(), &[exact(&["id"])], &opts);
    let m = b.manifest().unwrap().unwrap();
    let want = policy.map_or("tail64k", |p| match p {
        FingerprintPolicy::Tail64K => "tail64k",
        FingerprintPolicy::Full => "full",
    });
    assert_eq!(m.source.fingerprint_policy.as_deref(), Some(want));
    let mtime = std::fs::metadata(&path).unwrap().modified().unwrap();
    let mut f = std::fs::File::options().write(true).open(&path).unwrap();
    f.seek(SeekFrom::Start(20)).unwrap();
    f.write_all(b"9").unwrap();
    f.set_modified(mtime).unwrap();
    drop(f);
    assert_eq!(std::fs::metadata(&path).unwrap().modified().unwrap(), mtime);
    b.verify().unwrap().len()
}

#[test]
fn only_the_full_policy_sees_a_patch_outside_the_tail() {
    assert_eq!(patched_csv_case(None), 0, "the documented tail64k limit");
    assert_eq!(patched_csv_case(Some(FingerprintPolicy::Full)), 1);
}
