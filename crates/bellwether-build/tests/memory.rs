//! ADR 018: a build worker holds one row group's key columns at a time,
//! reads only key columns, and caps the segments it accumulates; the output
//! does not depend on the thread count.

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

use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_build::BuildOptions;
use bellwether_core::SegmentReader;
use bellwether_types::Store;
use common::*;

/// The simple ledger plus a wide `payload` column no key spec names.
fn wide_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("grp", DataType::Utf8, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("payload", DataType::Utf8, true),
    ]))
}

fn write_wide(s: &Scratch, files: usize, rgs: usize, rows_per_rg: usize) {
    for f in 0..files {
        let n = rgs * rows_per_rg;
        let base = (f * n) as i64;
        let batch = RecordBatch::try_new(
            wide_schema(),
            vec![
                Arc::new(Int64Array::from(
                    (0..n as i64).map(|i| base + i % 7).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    (0..n).map(|i| format!("g{}", i % 3)).collect::<Vec<_>>(),
                )),
                Arc::new(TimestampMicrosecondArray::from(
                    (0..n as i64).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    (0..n).map(|_| "x".repeat(200)).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        write_parquet(&s.path(&format!("f{f:02}.parquet")), &batch, rows_per_rg);
    }
}

fn opts_with(probe: &Arc<Probe>, threads: usize) -> BuildOptions {
    BuildOptions {
        observer: Some(probe.clone()),
        threads: Some(threads),
        max_segment_row_groups: 2,
        ..parquet_opts(&wide_schema())
    }
}

#[test]
fn each_worker_holds_one_row_group_of_key_columns_at_a_time() {
    for threads in [1usize, 3] {
        let s = Scratch::new();
        write_wide(&s, 6, 4, 16);
        let store = memory_store();
        let probe = Arc::new(Probe::default());
        let specs = [exact(&["id"]), temporal(&["id"], "ts"), bloom(&["grp"])];
        let r = build_ok(
            &builder(&store),
            &s.root_str(),
            &specs,
            &opts_with(&probe, threads),
        );
        assert_eq!(r.row_groups_read, 24);
        assert_eq!(
            probe.reads.load(Ordering::SeqCst),
            24,
            "each row group read once for three specs"
        );
        let peak = probe.peak_alive.load(Ordering::SeqCst);
        assert!(
            peak >= 1 && peak <= threads as u64,
            "{peak} batches alive with {threads} workers"
        );
        if threads == 1 {
            assert_eq!(peak, 1, "a worker drops its batch before reading the next");
        }
        assert!(r.peak_row_groups_held >= 1 && r.peak_row_groups_held <= threads as u64);
        // only key columns: the union of the specs' key and timestamp
        // columns, never the payload
        let read_cols = probe.columns.lock().unwrap().clone();
        assert_eq!(
            read_cols.into_iter().collect::<Vec<_>>(),
            vec![cols(&["id", "ts", "grp"])]
        );
    }
}

#[test]
fn segments_respect_the_key_cap() {
    let s = Scratch::new();
    // 7 distinct ids per file, 16 rows per row group: each row group has 7
    // distinct ids, and 16 distinct (id, ts) pairs
    write_wide(&s, 2, 4, 16);
    let store = memory_store();
    let probe = Arc::new(Probe::default());
    let opts = BuildOptions {
        max_segment_keys: 20,
        max_segment_row_groups: 100,
        ..opts_with(&probe, 1)
    };
    let specs = [exact(&["id"]), temporal(&["id"], "ts")];
    let r = build_ok(&builder(&store), &s.root_str(), &specs, &opts);
    assert!(r.peak_segment_keys <= 20, "{}", r.peak_segment_keys);
    let m = builder(&store).manifest().unwrap().unwrap();
    // exact: 7 keys per row group, at most two row groups (14 <= 20) per
    // segment; temporal: 16 pairs per row group, one per segment
    let count = |name: &str| m.segments.iter().filter(|x| x.key_spec == name).count();
    assert_eq!(count("exact:id"), 4);
    assert_eq!(count("temporal:id@ts"), 8);
    for seg in &m.segments {
        let bytes = store
            .get(&bellwether_store::segment_path("idx", &seg.id))
            .unwrap()
            .0;
        let reader = SegmentReader::open(bytes).unwrap();
        assert!(
            reader.entries() <= 20,
            "{} holds {} keys",
            seg.key_spec,
            reader.entries()
        );
    }
    // the caps change segment boundaries, never answers
    let rows: Vec<Row> = (0..2)
        .flat_map(|f| parquet_rows(&s.path(&format!("f{f:02}.parquet")), &wide_schema()))
        .collect();
    let p = planner(&store);
    let keys: Vec<Vec<u8>> = (0..70).map(enc_i64).collect();
    check_keys(&p, &rows, &[exact(&["id"])], &cols(&["id"]), &keys);
    check_latest(
        &p,
        &rows,
        &[temporal(&["id"], "ts")],
        &cols(&["id"]),
        &keys,
        &cols(&["id"]),
        "ts",
        None,
    );
}

#[test]
fn segments_do_not_depend_on_the_thread_count() {
    let s = Scratch::new();
    write_wide(&s, 5, 3, 8);
    let specs = [exact(&["id"]), bloom(&["grp"]), range("ts")];
    let mut seen: Vec<BTreeSet<(String, u32, u32, String)>> = Vec::new();
    for threads in [1usize, 4] {
        let store = memory_store();
        let probe = Arc::new(Probe::default());
        build_ok(
            &builder(&store),
            &s.root_str(),
            &specs,
            &opts_with(&probe, threads),
        );
        let m = builder(&store).manifest().unwrap().unwrap();
        seen.push(
            m.segments
                .iter()
                .map(|x| {
                    (
                        x.key_spec.clone(),
                        x.rowgroup_range.start.0,
                        x.rowgroup_range.end.0,
                        x.id.to_hex(),
                    )
                })
                .collect(),
        );
    }
    assert_eq!(seen[0], seen[1]);
}
