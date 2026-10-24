//! Memory is bounded per operation (ADR 018): the profile reads one row
//! group's key and time columns at a time and nothing else, its sketches are
//! fixed-size, and its sampler tables never exceed the stated bound, the same
//! bound on a corpus eight times larger. A control run without eviction must
//! break the bound, so the check is not vacuous.

#![allow(clippy::unwrap_used)]

mod common;

use std::sync::Arc;

use arrow::array::{RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether_profile::sampler::peak_bytes_bound;
use bellwether_profile::{profile, ProfileOptions, ProfileReport};
use common::{path_str, temp_store, Layout, MemSource, SpySource};

/// `files` files of `rgs` row groups of `rows` rows: a unique `id` per row
/// (the worst case for a table) and a three-valued `kind`.
fn unique_ids(files: u64, rgs: u64, rows: u64) -> MemSource {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("kind", DataType::UInt64, false),
    ]));
    let mut next = 0u64;
    let files = (0..files)
        .map(|_| {
            (0..rgs)
                .map(|_| {
                    let ids: Vec<u64> = (next..next + rows).collect();
                    next += rows;
                    let kinds: Vec<u64> = ids.iter().map(|i| i % 3).collect();
                    RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(UInt64Array::from(ids)),
                            Arc::new(UInt64Array::from(kinds)),
                        ],
                    )
                    .unwrap()
                })
                .collect()
        })
        .collect();
    MemSource::new(schema, files)
}

const BUDGET: usize = 64 * 1024;

fn run(source: &MemSource, unbounded: bool) -> ProfileReport {
    let (_s, store) = temp_store();
    profile(
        source,
        "mem",
        &store,
        &ProfileOptions {
            memory_budget: BUDGET,
            unbounded_tables: unbounded,
            ..ProfileOptions::for_source("parquet")
        },
    )
    .unwrap()
}

#[test]
fn tables_stay_within_the_stated_bound_on_a_corpus_eight_times_larger() {
    let small = run(&unique_ids(2, 4, 2_000), false);
    let large = run(&unique_ids(8, 8, 2_000), false);
    for r in [&small, &large] {
        let m = &r.memory;
        // Three keys share the budget: id, kind, (id, kind).
        assert_eq!(m.keys, 3);
        assert!(m.tables_bound_bytes <= BUDGET as u64, "{m:?}");
        assert_eq!(
            m.tables_bound_bytes,
            3 * peak_bytes_bound(usize::try_from(m.high_water).unwrap()) as u64
        );
        assert!(
            m.peak_capacity <= m.high_water,
            "observed table capacity over the mark: {m:?}"
        );
        assert_eq!(
            m.sketches_bytes,
            3 << bellwether_profile::DEFAULT_HLL_PRECISION
        );
        assert_eq!(m.max_row_group_rows, 2_000);
        for e in &r.fanout {
            assert!(e.sample.peak_entries <= m.high_water);
            assert!(e.sample.peak_capacity <= m.high_water);
        }
        let id = r
            .fanout
            .iter()
            .find(|e| e.fanout.columns == ["id"])
            .unwrap();
        assert!(
            id.fanout.sampled,
            "unique ids must overflow a 64 KiB budget"
        );
    }
    // The bound depends on the configuration, not on the data.
    assert_eq!(
        small.memory.tables_bound_bytes,
        large.memory.tables_bound_bytes
    );
    assert_eq!(small.memory.high_water, large.memory.high_water);
    assert_eq!(small.rows_total * 8, large.rows_total);
}

#[test]
fn without_eviction_the_bound_is_broken() {
    // Negative control: the same data and budget with eviction disabled.
    let bounded = run(&unique_ids(2, 4, 2_000), false);
    let unbounded = run(&unique_ids(2, 4, 2_000), true);
    let mark = bounded.memory.high_water;
    assert!(
        unbounded.memory.peak_capacity > mark,
        "{:?}",
        unbounded.memory
    );
    let id = unbounded
        .fanout
        .iter()
        .find(|e| e.fanout.columns == ["id"])
        .unwrap();
    assert!(!id.fanout.sampled);
    assert_eq!(id.fanout.distinct, 16_000);
}

#[test]
fn reads_one_row_group_at_a_time_and_only_key_columns() {
    // A 600-byte payload (a document, not a key) is skipped by auto
    // selection, listed with its reason, and never read; the time column is
    // analyzed from footers alone, so it is never read either.
    let tmp = tempfile::tempdir().unwrap();
    let layout = Layout {
        payload_bytes: 600,
        ..Layout::planted()
    };
    let data = layout.write(&tmp.path().join("data"));
    let spy = SpySource::parquet();
    let (_s, store) = temp_store();
    let report = profile(
        &spy,
        &path_str(&data),
        &store,
        &ProfileOptions::for_source("parquet"),
    )
    .unwrap();
    assert_eq!(report.key_columns, ["entity_id", "batch_id", "status"]);
    let skipped: Vec<&str> = report.skipped.iter().map(|s| s.column.as_str()).collect();
    assert_eq!(skipped, ["version_ts", "payload"]);
    assert!(
        report.skipped[1].reason.contains("bytes per row"),
        "{:?}",
        report.skipped
    );
    let files = u64::from(layout.days * layout.files_per_day);
    assert_eq!(
        *spy.footer_reads.lock().unwrap(),
        files,
        "one footer read per file"
    );
    let reads = spy.reads();
    let rgs = usize::try_from(report.row_groups_total).unwrap();
    assert_eq!(reads.len(), rgs, "one read per row group, a single pass");
    for (i, r) in reads.iter().enumerate() {
        assert_eq!(
            r.columns,
            ["entity_id", "batch_id", "status"],
            "only the candidate key columns are read"
        );
        if i > 0 {
            let p = &reads[i - 1];
            assert!(
                (p.path.as_str(), p.index_in_file) < (r.path.as_str(), r.index_in_file),
                "global order, each row group once"
            );
        }
    }
    assert!(report.time_scatter.iter().any(|t| t.column == "version_ts"));
}
