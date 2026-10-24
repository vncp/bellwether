//! Footer statistics and fanout tables are reused from the store's
//! content-addressed cache by input hash, their age is reported, a reused
//! profile is identical to a fresh one and reads no data, and any input or
//! option that changes a number changes the key.

#![allow(clippy::unwrap_used)]

mod common;

use std::time::Duration;

use bellwether_profile::{evidence, profile, InputKind, ProfileOptions, ProfileReport};
use bellwether_store::Cache;
use common::{path_str, temp_store, Layout, SpySource};

fn input(report: &ProfileReport, kind: InputKind) -> &bellwether_profile::InputRef {
    report.inputs.iter().find(|i| i.kind == kind).unwrap()
}

#[test]
fn a_second_profile_reuses_the_cache_reads_no_data_and_reports_age() {
    let tmp = tempfile::tempdir().unwrap();
    let data = Layout::planted().write(&tmp.path().join("data"));
    let root = path_str(&data);
    let (_s, store) = temp_store();
    let spy = SpySource::parquet();
    let first = profile(&spy, &root, &store, &ProfileOptions::for_source("parquet")).unwrap();
    for kind in [InputKind::FooterStats, InputKind::FanoutTable] {
        let i = input(&first, kind);
        assert!(!i.cache_hit);
        assert_eq!(i.age, Duration::ZERO);
        // The cited artifact exists in the cache under its input hash.
        assert!(Cache::new(store.clone(), "")
            .get_cached(&i.hash)
            .unwrap()
            .is_some());
    }
    assert_ne!(spy.reads(), Vec::new());

    std::thread::sleep(Duration::from_millis(30));
    spy.clear();
    let second = profile(&spy, &root, &store, &ProfileOptions::for_source("parquet")).unwrap();
    assert!(spy.reads().is_empty(), "a cache hit reads no data");
    assert_eq!(
        *spy.footer_reads.lock().unwrap(),
        0,
        "nor any row-group footer"
    );
    // What it does read from the source: the schema once (a Parquet source
    // parses the first file's footer for it) and one fingerprinting pass.
    assert_eq!(*spy.schema_calls.lock().unwrap(), 1);
    assert_eq!(*spy.discover_calls.lock().unwrap(), 1);
    for kind in [InputKind::FooterStats, InputKind::FanoutTable] {
        let (a, b) = (input(&first, kind), input(&second, kind));
        assert!(b.cache_hit);
        assert_eq!(a.hash, b.hash);
        assert!(
            b.age >= Duration::from_millis(10),
            "age comes from the cache: {:?}",
            b.age
        );
    }
    assert_eq!(first.fanout, second.fanout);
    assert_eq!(first.lints, second.lints);
    assert_eq!(first.time_scatter, second.time_scatter);
    assert_eq!(first.ranges, second.ranges);
    let (e1, e2) = (evidence(&first).unwrap(), evidence(&second).unwrap());
    assert_eq!(
        e1.body, e2.body,
        "a reused profile states the same findings"
    );
    assert!(e2.provenance.oldest_input_age >= Duration::from_millis(10));
}

#[test]
fn a_rewritten_file_misses_the_cache() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("data");
    Layout::planted().write(&dir);
    let root = path_str(&dir);
    let (_s, store) = temp_store();
    let spy = SpySource::parquet();
    let first = profile(&spy, &root, &store, &ProfileOptions::for_source("parquet")).unwrap();
    // Same layout, other data: every fingerprint changes.
    Layout {
        seed: 99,
        ..Layout::planted()
    }
    .write(&dir);
    spy.clear();
    let second = profile(&spy, &root, &store, &ProfileOptions::for_source("parquet")).unwrap();
    for kind in [InputKind::FooterStats, InputKind::FanoutTable] {
        assert!(!input(&second, kind).cache_hit);
        assert_ne!(input(&first, kind).hash, input(&second, kind).hash);
    }
    assert_ne!(spy.reads(), Vec::new());
}

#[test]
fn options_that_change_numbers_change_the_fanout_key_only() {
    let tmp = tempfile::tempdir().unwrap();
    let root = path_str(&Layout::planted().write(&tmp.path().join("data")));
    let (_s, store) = temp_store();
    let source = SpySource::parquet();
    let base = profile(
        &source,
        &root,
        &store,
        &ProfileOptions::for_source("parquet"),
    )
    .unwrap();
    let variants = [
        ProfileOptions {
            columns: Some(vec!["entity_id".into(), "batch_id".into()]),
            ..ProfileOptions::for_source("parquet")
        },
        ProfileOptions {
            max_tracked_per_key: Some(500),
            ..ProfileOptions::for_source("parquet")
        },
        ProfileOptions {
            seed: 1,
            ..ProfileOptions::for_source("parquet")
        },
        ProfileOptions {
            max_tuple_len: 1,
            ..ProfileOptions::for_source("parquet")
        },
    ];
    for options in variants {
        let r = profile(&source, &root, &store, &options).unwrap();
        assert!(
            input(&r, InputKind::FooterStats).cache_hit,
            "footers do not depend on options"
        );
        assert_eq!(
            input(&r, InputKind::FooterStats).hash,
            input(&base, InputKind::FooterStats).hash
        );
        assert_ne!(
            input(&r, InputKind::FanoutTable).hash,
            input(&base, InputKind::FanoutTable).hash
        );
        assert!(!input(&r, InputKind::FanoutTable).cache_hit);
    }
    // A source configuration change is a footer change.
    let r = profile(
        &source,
        &root,
        &store,
        &ProfileOptions {
            source_config: Some("csv:chunk=64".into()),
            ..ProfileOptions::for_source("parquet")
        },
    )
    .unwrap();
    assert!(!input(&r, InputKind::FooterStats).cache_hit);
}

#[test]
fn reuse_false_recomputes_and_overwrites() {
    let tmp = tempfile::tempdir().unwrap();
    let root = path_str(&Layout::flat().write(&tmp.path().join("data")));
    let (_s, store) = temp_store();
    let spy = SpySource::parquet();
    let first = profile(&spy, &root, &store, &ProfileOptions::for_source("parquet")).unwrap();
    spy.clear();
    let again = profile(
        &spy,
        &root,
        &store,
        &ProfileOptions {
            reuse_cache: false,
            ..ProfileOptions::for_source("parquet")
        },
    )
    .unwrap();
    assert_ne!(spy.reads(), Vec::new());
    assert!(again.inputs.iter().all(|i| !i.cache_hit));
    assert_eq!(first.fanout, again.fanout);
    assert_eq!(
        input(&first, InputKind::FanoutTable).hash,
        input(&again, InputKind::FanoutTable).hash
    );
}

#[test]
fn a_different_dataset_schema_is_a_different_footer_key() {
    let tmp = tempfile::tempdir().unwrap();
    let root = path_str(&Layout::flat().write(&tmp.path().join("data")));
    let (_s, store) = temp_store();
    let plain = bellwether_source::ParquetSource::new();
    let first = profile(
        &plain,
        &root,
        &store,
        &ProfileOptions::for_source("parquet"),
    )
    .unwrap();
    // The same files under a supplied schema that widens entity_id to Int64:
    // statistics encode differently, so the footers are not reused.
    let mut fields: Vec<arrow::datatypes::Field> = Layout::schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields[0] = arrow::datatypes::Field::new("entity_id", arrow::datatypes::DataType::Int64, false);
    let wide = bellwether_source::ParquetSource::new()
        .with_schema(std::sync::Arc::new(arrow::datatypes::Schema::new(fields)));
    let second = profile(&wide, &root, &store, &ProfileOptions::for_source("parquet")).unwrap();
    let (a, b) = (
        input(&first, InputKind::FooterStats),
        input(&second, InputKind::FooterStats),
    );
    assert_ne!(a.hash, b.hash);
    assert!(!b.cache_hit);
}

#[test]
fn time_columns_are_measured_fresh_never_served_stale() {
    let tmp = tempfile::tempdir().unwrap();
    let root = path_str(&Layout::planted().write(&tmp.path().join("data")));
    let (_s, store) = temp_store();
    let source = SpySource::parquet();
    let with = profile(
        &source,
        &root,
        &store,
        &ProfileOptions::for_source("parquet"),
    )
    .unwrap();
    assert!(with
        .time_scatter
        .iter()
        .any(|t| t.column == "version_ts" && t.scattered));
    assert!(with.lints.iter().any(|l| l.rule == "scattered_time_column"));
    // Without time columns the fanout table is reused (time columns are never
    // read by the pass), and the time measure and its lint are gone.
    let without = profile(
        &source,
        &root,
        &store,
        &ProfileOptions {
            time_columns: Some(Vec::new()),
            ..ProfileOptions::for_source("parquet")
        },
    )
    .unwrap();
    assert!(input(&without, InputKind::FanoutTable).cache_hit);
    assert_eq!(without.time_scatter, Vec::new());
    assert!(without
        .lints
        .iter()
        .all(|l| l.rule != "scattered_time_column"));
}

#[test]
fn a_rewritten_run_file_is_reread_not_served_from_the_cache() {
    let tmp = tempfile::tempdir().unwrap();
    let root = path_str(&Layout::flat().write(&tmp.path().join("data")));
    let wl = tmp.path().join("wl");
    common::write_bench_workload(&wl, &[vec![1, 2, 3], vec![4, 5]]);
    let (_s, store) = temp_store();
    let options = ProfileOptions {
        workload: Some(wl.clone()),
        ..ProfileOptions::for_source("parquet")
    };
    let source = bellwether_source::ParquetSource::new();
    let first = profile(&source, &root, &store, &options).unwrap();
    assert_eq!(first.workload.as_ref().unwrap().sizes, [2, 3, 3]);
    // Replace one run's keys behind workload.json's back: the run file's
    // fingerprint changes the summary's key, the run is read again, and the
    // hash it no longer matches is refused.
    let keep = std::fs::read(wl.join("workload.json")).unwrap();
    common::write_bench_workload(&wl, &[vec![7, 8, 9, 10], vec![4, 5]]);
    std::fs::write(wl.join("workload.json"), keep).unwrap();
    let err = profile(&source, &root, &store, &options).unwrap_err();
    assert!(err.to_string().contains("hashing to"), "{err}");
}

#[test]
fn a_workload_that_reorders_a_tuple_is_a_different_fanout_key() {
    use arrow::array::{RecordBatch, StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    // entity_id comes second in the schema; a workload supplying it moves it
    // to the front of the composite, which must be measured afresh.
    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("status", DataType::Utf8, false),
        Field::new("entity_id", DataType::UInt64, false),
    ]));
    let rgs = (0..4u64)
        .map(|r| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    std::sync::Arc::new(StringArray::from(vec!["a", "b", "a", "b"])),
                    std::sync::Arc::new(UInt64Array::from(
                        (0..4).map(|i| r * 4 + i).collect::<Vec<u64>>(),
                    )),
                ],
            )
            .unwrap()
        })
        .collect();
    let source = common::MemSource::new(schema, vec![rgs]);
    let (_s, store) = temp_store();
    let plain = profile(&source, "mem", &store, &ProfileOptions::for_source("mem")).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    common::write_bench_workload(&tmp.path().join("wl"), &[vec![1, 2, 3]]);
    let reordered = profile(
        &source,
        "mem",
        &store,
        &ProfileOptions {
            workload: Some(tmp.path().join("wl")),
            ..ProfileOptions::for_source("mem")
        },
    )
    .unwrap();
    let (a, b) = (
        input(&plain, InputKind::FanoutTable),
        input(&reordered, InputKind::FanoutTable),
    );
    assert_ne!(a.hash, b.hash);
    assert!(!b.cache_hit);
    assert!(input(&reordered, InputKind::FooterStats).cache_hit);
    let composite = |r: &ProfileReport| {
        r.fanout
            .iter()
            .find(|e| e.fanout.columns.len() == 2)
            .unwrap()
            .fanout
            .columns
            .clone()
    };
    assert_eq!(composite(&plain), ["status", "entity_id"]);
    assert_eq!(composite(&reordered), ["entity_id", "status"]);
}
