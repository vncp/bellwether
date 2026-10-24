//! PR 17 review probes, kept as regression tests. Each test asserts the
//! behavior the rule catalog, the README, or the brief promises.
//! Run: cargo test -p bellwether-profile --test review_probes -- --nocapture --test-threads=1
//!
//! Adapted from the reviewer's file only where a review decision changed the
//! API, each adaptation keeping the probe's assertion:
//! - every `ProfileOptions::for_source("parquet")` names its source configuration, which
//!   is now required (decision 6);
//! - p1a's failure message prints the shared overlap depth and sortedness,
//!   which replaced the gap ratio (decision 1);
//! - p3 finds the composite of `status` and `entity_id` by its columns and
//!   checks its numbers against the oracle for the order it is listed in
//!   (decision 4: measured in listed order, never relabeled);
//! - p4 also accepts the profile refusing footer counts beyond 4 sigma of the
//!   sketch (decision 5);
//! - p8 checks that an unset source configuration is refused and that two
//!   configurations naming different chunk sizes never share footers.

#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::unwrap_used,
    dead_code,
    unused_imports
)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StringArray, TimestampMicrosecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_profile::{lints, profile, render, FanoutEntry, ProfileOptions, ProfileReport};
use bellwether_source::ParquetSource;
use bellwether_types::{ColumnStats, FileFingerprint, Result, RowGroupMeta, Source};
use common::{mix, oracle, path_str, temp_store, MemSource};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

fn lint_on<'a>(r: &'a ProfileReport, rule: &str, col: &str) -> Option<&'a bellwether_types::Lint> {
    r.lints
        .iter()
        .find(|l| l.rule == rule && l.columns == [col.to_string()])
}

fn exact<'a>(r: &'a ProfileReport, cols: &[&str]) -> Option<&'a FanoutEntry> {
    r.fanout.iter().find(|e| e.fanout.columns == cols)
}

fn show(r: &ProfileReport) {
    eprintln!("{}", render(r, None));
}

fn ts_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("id", DataType::UInt64, false),
    ]))
}

fn ts_batch(ts: &[i64], first_id: u64) -> RecordBatch {
    RecordBatch::try_new(
        ts_schema(),
        vec![
            Arc::new(TimestampMicrosecondArray::from(ts.to_vec()).with_timezone("UTC")),
            Arc::new(UInt64Array::from(
                (0..ts.len() as u64)
                    .map(|i| first_id + i)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn run_mem(source: &MemSource, options: ProfileOptions) -> ProfileReport {
    let (_s, store) = temp_store();
    profile(source, "mem", &store, &options).unwrap()
}

// ---------------------------------------------------------------------------
// Probe 1: scattered_time_column on a sorted, disjoint time column.
// ---------------------------------------------------------------------------

/// One writer, timestamps strictly increasing (sorted, disjoint row-group
/// ranges). Arrival density varies: 9,000 rows one second apart, then 1,000
/// rows 100 seconds apart (day and night traffic), twice. Range pruning on
/// `ts` selects one row group out of 40 for a typical window.
#[test]
fn p1a_sorted_time_column_with_uneven_density_is_not_scattered() {
    let mut t = 1_767_225_600_000_000i64; // 2026-01-01T00:00:00Z
    let mut all = Vec::new();
    for i in 0..20_000u64 {
        let step = if i % 10_000 < 9_000 {
            1_000_000
        } else {
            100_000_000
        };
        t += step;
        all.push(t);
    }
    let rgs: Vec<RecordBatch> = all
        .chunks(500)
        .enumerate()
        .map(|(k, c)| ts_batch(c, k as u64 * 500))
        .collect();
    let source = MemSource::new(ts_schema(), vec![rgs]);
    let r = run_mem(&source, ProfileOptions::for_source("parquet"));
    show(&r);
    let ranges = r.ranges.iter().find(|x| x.column == "ts").unwrap();
    assert!(ranges.disjoint, "probe setup: ranges are disjoint");
    assert_eq!(ranges.sorted_fraction, Some(1.0), "probe setup: sorted");
    let scatter = r.time_scatter.iter().find(|x| x.column == "ts").unwrap();
    eprintln!("p1a scatter: {scatter:?}");
    assert!(
        lint_on(&r, lints::SCATTERED_TIME_COLUMN, "ts").is_none(),
        "FALSE POSITIVE: scattered_time_column on a sorted column with disjoint ranges (overlap depth {:.2}, sortedness {:.3}): {:?}",
        scatter.depth,
        scatter.sortedness,
        lint_on(&r, lints::SCATTERED_TIME_COLUMN, "ts").map(|l| &l.message)
    );
}

/// Business hours only: one row every 10 s from 09:00 to 17:00 UTC on each
/// of 10 days, one writer, sorted. Row groups of 5,000 rows straddle the
/// overnight gap.
#[test]
fn p1b_sorted_business_hours_column_is_not_scattered() {
    let day0 = 1_767_225_600_000_000i64;
    let mut all = Vec::new();
    for d in 0..10i64 {
        let open = day0 + d * 86_400_000_000 + 9 * 3_600_000_000;
        for k in 0..2_880i64 {
            all.push(open + k * 10_000_000);
        }
    }
    let rgs: Vec<RecordBatch> = all
        .chunks(5_000)
        .enumerate()
        .map(|(k, c)| ts_batch(c, k as u64 * 5_000))
        .collect();
    let source = MemSource::new(ts_schema(), vec![rgs]);
    let r = run_mem(&source, ProfileOptions::for_source("parquet"));
    show(&r);
    let ranges = r.ranges.iter().find(|x| x.column == "ts").unwrap();
    assert!(
        ranges.disjoint && ranges.sorted_fraction == Some(1.0),
        "probe setup"
    );
    assert!(
        lint_on(&r, lints::SCATTERED_TIME_COLUMN, "ts").is_none(),
        "FALSE POSITIVE: scattered_time_column on sorted business-hours data: {:?}",
        lint_on(&r, lints::SCATTERED_TIME_COLUMN, "ts").map(|l| &l.message)
    );
}

// ---------------------------------------------------------------------------
// Probe 2: batch_stamp_key on true ids.
// ---------------------------------------------------------------------------

fn id_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::UInt64, false),
        Field::new("event_id", DataType::UInt64, false),
    ]))
}

fn id_batch(rows: &[(u64, u64)]) -> RecordBatch {
    RecordBatch::try_new(
        id_schema(),
        vec![
            Arc::new(UInt64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// One file per day for 3 days, each holding every one of 10,000 entities
/// once, sorted by `entity_id` (the usual "sort by key within each daily
/// partition" layout). `event_id` is unique per row.
#[test]
fn p2a_true_id_sorted_within_daily_files_is_not_a_batch_stamp() {
    let mut files = Vec::new();
    for day in 0..3u64 {
        let rows: Vec<(u64, u64)> = (0..10_000u64).map(|e| (e, day * 1_000_000 + e)).collect();
        files.push(rows.chunks(1_000).map(id_batch).collect::<Vec<_>>());
    }
    let r = run_mem(
        &MemSource::new(id_schema(), files),
        ProfileOptions::for_source("parquet"),
    );
    show(&r);
    assert!(
        lint_on(&r, lints::BATCH_STAMP_KEY, "entity_id").is_none(),
        "FALSE POSITIVE: batch_stamp_key on the entity id: {:?}",
        lint_on(&r, lints::BATCH_STAMP_KEY, "entity_id").map(|l| &l.message)
    );
}

/// An id column written by parallel writers: each day, 8 writers each own a
/// contiguous range of 1,250 entity ids (range-partitioned), and write their
/// rows in arrival order (not sorted). Every entity has one event per day.
#[test]
fn p2b_range_partitioned_parallel_writers_do_not_make_an_id_a_batch_stamp() {
    let mut files = Vec::new();
    for day in 0..3u64 {
        for w in 0..8u64 {
            let mut rows: Vec<(u64, u64)> = (w * 1_250..(w + 1) * 1_250)
                .map(|e| (e, day * 1_000_000 + e))
                .collect();
            rows.sort_by_key(|r| mix(r.0 ^ (day << 40))); // arrival order
            files.push(rows.chunks(625).map(id_batch).collect::<Vec<_>>());
        }
    }
    let r = run_mem(
        &MemSource::new(id_schema(), files),
        ProfileOptions::for_source("parquet"),
    );
    show(&r);
    assert!(
        lint_on(&r, lints::BATCH_STAMP_KEY, "entity_id").is_none(),
        "FALSE POSITIVE: batch_stamp_key on an id written by range-partitioned parallel writers: {:?}",
        lint_on(&r, lints::BATCH_STAMP_KEY, "entity_id").map(|l| &l.message)
    );
}

/// Control: hash-partitioned parallel writers. Every file spans the id space,
/// so the narrowness gate rejects the id. Expected to pass.
#[test]
fn p2c_hash_partitioned_parallel_writers_control() {
    let mut files = Vec::new();
    for day in 0..3u64 {
        for w in 0..8u64 {
            let mut rows: Vec<(u64, u64)> = (0..10_000u64)
                .filter(|e| mix(*e) % 8 == w)
                .map(|e| (e, day * 1_000_000 + e))
                .collect();
            rows.sort_by_key(|r| mix(r.0 ^ (day << 40)));
            files.push(rows.chunks(625).map(id_batch).collect::<Vec<_>>());
        }
    }
    let r = run_mem(
        &MemSource::new(id_schema(), files),
        ProfileOptions::for_source("parquet"),
    );
    assert!(
        lint_on(&r, lints::BATCH_STAMP_KEY, "entity_id").is_none(),
        "{:?}",
        r.lints
    );
}

// ---------------------------------------------------------------------------
// Probe 3: a recommended composite is relabeled to the lint's column order,
// but measured in schema order; with nulls (ADR 014) the numbers differ.
// Its leading column after relabeling, status, is low-cardinality.
// ---------------------------------------------------------------------------

#[test]
fn p3_relabeled_composite_numbers_match_their_column_order() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("data");
    std::fs::create_dir_all(&dir).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::UInt64, true),
        Field::new("status", DataType::Utf8, true),
    ]));
    let statuses = ["posted", "settled", "void"];
    for f in 0..2u64 {
        let props = WriterProperties::builder()
            .set_max_row_group_size(200)
            .build();
        let mut w = ArrowWriter::try_new(
            File::create(dir.join(format!("part-{f}.parquet"))).unwrap(),
            schema.clone(),
            Some(props),
        )
        .unwrap();
        for rg in 0..4u64 {
            let mut e = Vec::new();
            let mut s = Vec::new();
            for i in 0..200u64 {
                let r = mix(f * 1_000_000 + rg * 1_000 + i);
                e.push((r % 10 != 0).then_some(r % 400)); // 10% null entity ids
                s.push((r % 100 >= 30).then(|| statuses[((r >> 20) % 3) as usize]));
                // 30% null status
            }
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt64Array::from(e)),
                    Arc::new(StringArray::from(s)),
                ],
            )
            .unwrap();
            w.write(&batch).unwrap();
            w.flush().unwrap();
        }
        w.close().unwrap();
    }
    let (_s, store) = temp_store();
    let r = profile(
        &ParquetSource::new(),
        &path_str(&dir),
        &store,
        &ProfileOptions::for_source("parquet"),
    )
    .unwrap();
    show(&r);
    let lint = lint_on(&r, lints::LOW_SELECTIVITY_SINGLE_COLUMN, "status")
        .expect("probe setup: lint fires on status");
    eprintln!("lint: {}", lint.message);
    let got = r
        .fanout
        .iter()
        .find(|e| {
            let c = &e.fanout.columns;
            c.len() == 2
                && c.contains(&"status".to_string())
                && c.contains(&"entity_id".to_string())
        })
        .expect("the composite of status and entity_id is measured");
    let order: Vec<&str> = got.fanout.columns.iter().map(String::as_str).collect();
    let other: Vec<&str> = order.iter().rev().copied().collect();
    let truth = oracle(&dir, &[order.clone(), other]);
    assert_eq!(exact(&r, &order).map(|e| &e.fanout), Some(&got.fanout));
    assert!(
        lint.message.contains(&format!("({})", order.join(", "))),
        "the lint names the composite in its measured order: {}",
        lint.message
    );
    eprintln!(
        "reported {order:?}: distinct {} rg {:?}; oracle {order:?}: distinct {} rg {:?}; oracle reversed: distinct {} rg {:?}",
        got.fanout.distinct,
        got.fanout.row_groups_per_value,
        truth[0].distinct,
        truth[0].row_groups_per_value(),
        truth[1].distinct,
        truth[1].row_groups_per_value()
    );
    assert_eq!(
        (
            got.fanout.distinct,
            got.fanout.row_groups_per_value,
            got.fanout.files_per_value
        ),
        (
            truth[0].distinct,
            truth[0].row_groups_per_value(),
            truth[0].files_per_value()
        ),
        "the fanout labeled {order:?} must measure {order:?} under ADR 014"
    );
}

// ---------------------------------------------------------------------------
// Probe 4: footer distinct counts are trusted as exact when ranges are
// disjoint. A writer whose count is approximate (or counts the null) makes a
// wrong number that is labeled unsampled.
// ---------------------------------------------------------------------------

struct Inflated(MemSource, f64);

impl Source for Inflated {
    fn discover(&self, r: &str) -> Result<Vec<FileFingerprint>> {
        self.0.discover(r)
    }
    fn row_groups(&self, f: &FileFingerprint) -> Result<Vec<RowGroupMeta>> {
        let mut rgs = self.0.row_groups(f)?;
        for rg in &mut rgs {
            for s in &mut rg.stats {
                s.distinct_count = s.distinct_count.map(|d| (d as f64 * self.1).round() as u64);
            }
        }
        Ok(rgs)
    }
    fn read_columns(&self, rg: &RowGroupMeta, c: &[String]) -> Result<RecordBatch> {
        self.0.read_columns(rg, c)
    }
    fn schema(&self, r: &str) -> Result<SchemaRef> {
        self.0.schema(r)
    }
}

#[test]
fn p4_an_unsampled_distinct_is_never_an_unverified_footer_number() {
    // 32 row groups of 1,000 sorted, disjoint ids; the writer reports each
    // row group's distinct count 10% high (an approximate count).
    let files: Vec<Vec<RecordBatch>> = (0..4u64)
        .map(|f| {
            (0..8u64)
                .map(|i| {
                    common::u64_batch(
                        "v",
                        (0..1_000u64).map(|j| (f * 8 + i) * 1_000 + j).collect(),
                    )
                })
                .collect()
        })
        .collect();
    let schema = files[0][0].schema();
    let mut mem = MemSource::new(schema, files);
    mem.distinct_counts = true;
    let source = Inflated(mem, 1.1);
    let (_s, store) = temp_store();
    let r = match profile(
        &source,
        "mem",
        &store,
        &ProfileOptions {
            max_tracked_per_key: Some(14),
            ..ProfileOptions::for_source("parquet")
        },
    ) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("p4: refused: {e}");
            assert!(e.to_string().contains("footer distinct counts"), "{e}");
            return;
        }
    };
    let e = &r.fanout[0];
    eprintln!("p4: {:?} {:?}", e.fanout, e.sample);
    if !e.fanout.sampled {
        assert_eq!(
            e.fanout.distinct, 32_000,
            "labeled unsampled (exact) but distinct is the writer's approximate count; the sketch said {:.0}",
            e.sample.hll_estimate
        );
    }
}

// ---------------------------------------------------------------------------
// Probe 5: the one-screen summary states the error of every sampled key.
// ---------------------------------------------------------------------------

#[test]
fn p5_render_states_each_sampled_keys_own_error() {
    // Two independent high-cardinality keys sample to different sizes.
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::UInt64, false),
        Field::new("b", DataType::UInt64, false),
    ]));
    let files: Vec<Vec<RecordBatch>> = (0..4u64)
        .map(|f| {
            (0..4u64)
                .map(|i| {
                    let base = (f * 4 + i) * 3_000;
                    RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(UInt64Array::from(
                                (0..3_000u64)
                                    .map(|j| mix(base + j) % 20_000)
                                    .collect::<Vec<_>>(),
                            )),
                            Arc::new(UInt64Array::from(
                                (0..3_000u64)
                                    .map(|j| mix(base + j + 7) % 9_000)
                                    .collect::<Vec<_>>(),
                            )),
                        ],
                    )
                    .unwrap()
                })
                .collect()
        })
        .collect();
    let r = run_mem(
        &MemSource::new(schema, files),
        ProfileOptions {
            max_tracked_per_key: Some(1_000),
            max_tuple_len: 1,
            ..ProfileOptions::for_source("parquet")
        },
    );
    let text = render(&r, None);
    eprintln!("{text}");
    let eps: Vec<(Vec<String>, f64, f64)> = r
        .fanout
        .iter()
        .filter(|e| e.fanout.sampled)
        .map(|e| {
            (
                e.fanout.columns.clone(),
                e.sample.quantile_rank_error.unwrap(),
                e.sample.rate,
            )
        })
        .collect();
    eprintln!("per-key rank errors and rates: {eps:?}");
    assert!(eps.len() >= 2, "probe setup: two sampled keys");
    for (cols, e, _) in &eps {
        assert!(
            text.contains(&format!("{e:.3}")),
            "the summary never states the rank error {e:.3} of sampled key {cols:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Probe 6: auto selection keeps string ids in tiny row groups.
// ---------------------------------------------------------------------------

#[test]
fn p6_auto_selection_keeps_a_string_id_in_tiny_row_groups() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("data");
    std::fs::create_dir_all(&dir).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_ref", DataType::Utf8, false),
        Field::new("qty", DataType::UInt64, false),
    ]));
    let props = WriterProperties::builder()
        .set_max_row_group_size(5)
        .build();
    let mut w = ArrowWriter::try_new(
        File::create(dir.join("part-0.parquet")).unwrap(),
        schema.clone(),
        Some(props),
    )
    .unwrap();
    for rg in 0..100u64 {
        let refs: Vec<String> = (0..5u64)
            .map(|i| format!("o{:06}", (rg * 5 + i) / 3))
            .collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(refs)),
                Arc::new(UInt64Array::from(vec![1u64; 5])),
            ],
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.flush().unwrap();
    }
    w.close().unwrap();
    let (_s, store) = temp_store();
    let r = profile(
        &ParquetSource::new(),
        &path_str(&dir),
        &store,
        &ProfileOptions::for_source("parquet"),
    )
    .unwrap();
    eprintln!("key columns: {:?}", r.key_columns);
    assert!(
        r.key_columns.iter().any(|c| c == "order_ref"),
        "a 7-character string id was dropped as a payload: {:?}",
        r.key_columns
    );
}

// ---------------------------------------------------------------------------
// Probe 7: is the stated sampling error honest? 400 seeds of a forced sample:
// DKW band coverage for p50/p95, HyperLogLog calibration, max <= true max.
// ---------------------------------------------------------------------------

#[test]
fn p7_stated_sampling_error_is_honest_over_many_seeds() {
    use bellwether_profile::hll::Hll;
    let occurs = |v: u64, r: u64| mix(v.wrapping_mul(0x9E37_79B9) ^ r) % (1 + v % 9) == 0;
    let mut per: BTreeMap<u64, (BTreeSet<u64>, BTreeSet<u64>)> = BTreeMap::new();
    let mut files = Vec::new();
    for f in 0..4u64 {
        let mut rgs = Vec::new();
        for i in 0..8u64 {
            let r = f * 8 + i;
            let vals: Vec<u64> = (0..30_000u64).filter(|&v| occurs(v, r)).collect();
            for &v in &vals {
                let e = per.entry(v).or_default();
                e.0.insert(r);
                e.1.insert(f);
            }
            rgs.push(common::u64_batch("v", vals));
        }
        files.push(rgs);
    }
    let mut prg: Vec<u64> = per.values().map(|e| e.0.len() as u64).collect();
    let mut pf: Vec<u64> = per.values().map(|e| e.1.len() as u64).collect();
    prg.sort_unstable();
    pf.sort_unstable();
    let truth = prg.len() as f64;
    let schema = files[0][0].schema();
    let source = MemSource::new(schema, files);
    let rse = Hll::relative_standard_error(14);
    let (mut checks, mut outside, mut within1, mut within2, mut worst) = (0, 0, 0, 0, 0.0f64);
    let seeds = 400u64;
    for seed in 1..=seeds {
        let r = run_mem(
            &source,
            ProfileOptions {
                max_tracked_per_key: Some(448),
                seed,
                ..ProfileOptions::for_source("parquet")
            },
        );
        let e = &r.fanout[0];
        assert!(e.fanout.sampled);
        let eps = e.sample.quantile_rank_error.unwrap();
        for (i, q) in [0.5, 0.95].into_iter().enumerate() {
            for (got, pop) in [
                (e.fanout.row_groups_per_value[i], &prg),
                (e.fanout.files_per_value[i], &pf),
            ] {
                let lo = common::nearest_rank(pop, (q - eps).max(0.0)) as f64;
                let hi = common::nearest_rank(pop, (q + eps).min(1.0)) as f64;
                checks += 1;
                if got < lo || got > hi {
                    outside += 1;
                }
            }
        }
        assert!(e.fanout.row_groups_per_value[2] <= *prg.last().unwrap() as f64);
        assert!(e.fanout.files_per_value[2] <= *pf.last().unwrap() as f64);
        let z = (e.fanout.distinct as f64 - truth) / truth / rse;
        if z.abs() <= 1.0 {
            within1 += 1;
        }
        if z.abs() <= 2.0 {
            within2 += 1;
        }
        worst = worst.max(z.abs());
    }
    eprintln!(
        "p7: {checks} quantile checks, {outside} outside the stated 99.9% DKW band; HLL within 1 sigma {:.0}%, within 2 sigma {:.0}%, worst {worst:.2} sigma",
        100.0 * within1 as f64 / seeds as f64,
        100.0 * within2 as f64 / seeds as f64
    );
    assert!(
        outside as f64 <= 0.001 * checks as f64 + 1.0,
        "DKW band violated too often"
    );
    assert!(
        within2 as f64 >= 0.9 * seeds as f64,
        "HLL error larger than stated"
    );
}

// ---------------------------------------------------------------------------
// Probe 8: the default `source_config` lets a reconfigured text source reuse
// footers (row groups) computed under another chunk size.
// ---------------------------------------------------------------------------

#[test]
fn p8_a_changed_chunk_size_never_reuses_another_chunkings_numbers() {
    use bellwether_source::{ChunkedTextSource, TextFormat};
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("data");
    std::fs::create_dir_all(&dir).unwrap();
    let mut text = String::from("id,name\n");
    for i in 0..3_000u64 {
        text.push_str(&format!("{},n{}\n", i % 700, i % 13));
    }
    std::fs::write(dir.join("a.csv"), text).unwrap();
    let (_s, store) = temp_store();
    let root = path_str(&dir);
    let coarse = ChunkedTextSource::new(TextFormat::csv()).with_chunk_size(8_192);
    let fine = ChunkedTextSource::new(TextFormat::csv()).with_chunk_size(1_024);
    // Without a source configuration nothing is cached or reused: refused.
    let unset = profile(&fine, &root, &store, &ProfileOptions::default());
    assert!(
        unset.is_err(),
        "an unset source_config must be refused, not defaulted"
    );
    let coarse_opts = ProfileOptions::for_source("csv:chunk=8192");
    let fine_opts = ProfileOptions::for_source("csv:chunk=1024");
    let fresh_fine = {
        let (_s2, store2) = temp_store();
        profile(&fine, &root, &store2, &fine_opts).unwrap()
    };
    let a = profile(&coarse, &root, &store, &coarse_opts).unwrap();
    let b = profile(&fine, &root, &store, &fine_opts).unwrap();
    eprintln!(
        "p8: coarse rgs {}, fine (fresh store) rgs {}, fine (shared store) rgs {}",
        a.row_groups_total, fresh_fine.row_groups_total, b.row_groups_total
    );
    assert_eq!(
        b.row_groups_total, fresh_fine.row_groups_total,
        "the 1 KiB-chunk profile silently reused the 8 KiB chunking's footers and fanout"
    );
}

// ---------------------------------------------------------------------------
// Probe 9: the fanout-table cache stores TimeScatter means as JSON floats;
// this workspace's serde_json parses without `float_roundtrip`, so a cached
// mean can come back one ulp off and a cache hit is not identical to a fresh
// profile (seen under mutant R2: mean_holding 12.473684210526315 vs ...317).
// ---------------------------------------------------------------------------

#[test]
fn p9_cached_time_scatter_floats_round_trip_exactly() {
    let mean_holding = 237.0f64 / 19.0; // the value from the R2 run
    let json = serde_json::to_string(&mean_holding).unwrap();
    let back: f64 = serde_json::from_str(&json).unwrap();
    assert_eq!(
        back.to_bits(),
        mean_holding.to_bits(),
        "{json} parsed back as {back:?}: a cache hit can change a gap ratio by one ulp"
    );
}

/// Sorted, evenly spaced timestamps (one row per second, one writer) with a
/// single open-record sentinel 9999-12-31 in the last row group.
#[test]
fn p1c_one_sentinel_timestamp_does_not_make_a_sorted_column_scattered() {
    let start = 1_767_225_600_000_000i64;
    let mut all: Vec<i64> = (0..20_000i64).map(|i| start + i * 1_000_000).collect();
    *all.last_mut().unwrap() = 253_402_214_400_000_000; // 9999-12-31T00:00:00Z
    let rgs: Vec<RecordBatch> = all
        .chunks(500)
        .enumerate()
        .map(|(k, c)| ts_batch(c, k as u64 * 500))
        .collect();
    let source = MemSource::new(ts_schema(), vec![rgs]);
    let r = run_mem(&source, ProfileOptions::for_source("parquet"));
    let scatter = r.time_scatter.iter().find(|x| x.column == "ts").unwrap();
    eprintln!("p1c scatter: {scatter:?}");
    assert!(
        lint_on(&r, lints::SCATTERED_TIME_COLUMN, "ts").is_none(),
        "FALSE POSITIVE from one sentinel: {:?}",
        lint_on(&r, lints::SCATTERED_TIME_COLUMN, "ts").map(|l| &l.message)
    );
}

/// A UUID-text entity id in ordinary row groups (2,000 rows): the commonest
/// string key there is.
#[test]
fn p6b_auto_selection_keeps_a_uuid_text_id() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("data");
    std::fs::create_dir_all(&dir).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("customer_uuid", DataType::Utf8, false),
        Field::new("qty", DataType::UInt64, false),
    ]));
    let props = WriterProperties::builder()
        .set_max_row_group_size(2_000)
        .build();
    let mut w = ArrowWriter::try_new(
        File::create(dir.join("part-0.parquet")).unwrap(),
        schema.clone(),
        Some(props),
    )
    .unwrap();
    for rg in 0..4u64 {
        let ids: Vec<String> = (0..2_000u64)
            .map(|i| {
                let a = mix(rg * 2_000 + i);
                let b = mix(a);
                format!(
                    "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
                    a >> 32,
                    (a >> 16) & 0xffff,
                    a & 0xffff,
                    b >> 48,
                    b & 0xffff_ffff_ffff
                )
            })
            .collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(UInt64Array::from(vec![1u64; 2_000])),
            ],
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.flush().unwrap();
    }
    w.close().unwrap();
    let (_s, store) = temp_store();
    let r = profile(
        &ParquetSource::new(),
        &path_str(&dir),
        &store,
        &ProfileOptions::for_source("parquet"),
    )
    .unwrap();
    eprintln!("key columns: {:?}", r.key_columns);
    assert!(
        r.key_columns.iter().any(|c| c == "customer_uuid"),
        "UUID text id dropped as a payload: {:?}",
        r.key_columns
    );
}
