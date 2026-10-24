//! Every lint threshold, tested from both sides on layouts built here, so CI
//! runs them without bench corpora. Each pair of layouts differs only in the
//! one quantity its threshold judges: one sits at or just past it, the other
//! just short of it.

#![allow(
    clippy::unwrap_used,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]

mod common;

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_profile::{lints, profile, DistinctSource, ProfileOptions, ProfileReport};
use bellwether_source::stats::{
    time_correlation, BATCH_MIN_TIME_CORRELATION, SCATTER_MAX_SORTEDNESS, SCATTER_MIN_DEPTH,
};
use bellwether_types::{FileFingerprint, Result, RowGroupMeta, Source};
use common::{temp_store, u64_batch, MemSource};

const DAY: i64 = 86_400_000_000;
/// 2026-01-01T00:00:00Z in microseconds.
const T0: i64 = 1_767_225_600_000_000;

fn run(source: &dyn Source) -> ProfileReport {
    let (_s, store) = temp_store();
    profile(source, "mem", &store, &ProfileOptions::for_source("mem")).unwrap()
}

fn fired(r: &ProfileReport, rule: &str, column: &str) -> bool {
    r.lints
        .iter()
        .any(|l| l.rule == rule && l.columns == [column.to_string()])
}

fn ts_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

// ---------------------------------------------------------------------------
// scattered_time_column: depth >= 1.5 and sortedness < 0.9
// ---------------------------------------------------------------------------

fn ts_only(files: &[&[(i64, i64)]]) -> MemSource {
    let schema = Arc::new(Schema::new(vec![Field::new("ts", ts_type(), false)]));
    let files = files
        .iter()
        .map(|rgs| {
            rgs.iter()
                .map(|&(lo, hi)| {
                    let ts: ArrayRef = Arc::new(
                        TimestampMicrosecondArray::from(vec![T0 + lo, T0 + hi])
                            .with_timezone("UTC"),
                    );
                    RecordBatch::try_new(schema.clone(), vec![ts]).unwrap()
                })
                .collect()
        })
        .collect();
    MemSource::new(schema, files)
}

#[test]
fn scatter_depth_threshold() {
    // One file, two row groups whose ranges overlap: [0, 2L-1] and [L, 2L-1]
    // covers 3L over a union of 2L, depth exactly 1.5; starting the second
    // range one unit later gives (3L-1)/2L, just below. The pair is unsorted
    // either way, so only depth decides.
    const L: i64 = 1_000;
    let at = run(&ts_only(&[&[(0, 2 * L - 1), (L, 2 * L - 1)]]));
    let t = &at.time_scatter[0];
    assert!((t.depth - SCATTER_MIN_DEPTH).abs() < 1e-12, "{t:?}");
    assert!(t.sortedness < SCATTER_MAX_SORTEDNESS);
    assert!(fired(&at, lints::SCATTERED_TIME_COLUMN, "ts"));
    let below = run(&ts_only(&[&[(0, 2 * L - 1), (L + 1, 2 * L - 1)]]));
    let t = &below.time_scatter[0];
    assert!(t.depth < SCATTER_MIN_DEPTH && t.depth > 1.49, "{t:?}");
    assert!(!fired(&below, lints::SCATTERED_TIME_COLUMN, "ts"));
}

#[test]
fn scatter_sortedness_threshold() {
    // Two near-identical files stack to depth about 2; each holds 51
    // ascending row groups (50 pairs, 100 in all). Pulling the minimum of `k`
    // row groups of the first file below its predecessor's maximum leaves
    // (100 - k) / 100 sorted: k = 10 gives 0.90 (not below), k = 11 gives
    // 0.89.
    let file = |k: i64| -> Vec<(i64, i64)> {
        (0..51)
            .map(|i| {
                let lo = i * 100 - if i >= 1 && i <= k { 10 } else { 0 };
                (lo, i * 100 + 99)
            })
            .collect()
    };
    let sorted = file(0);
    let one = file(10);
    let two = file(11);
    let r1 = run(&ts_only(&[&one, &sorted]));
    let t = &r1.time_scatter[0];
    assert!(
        (t.sortedness - SCATTER_MAX_SORTEDNESS).abs() < 1e-12,
        "{t:?}"
    );
    assert!(t.depth >= SCATTER_MIN_DEPTH);
    assert!(!fired(&r1, lints::SCATTERED_TIME_COLUMN, "ts"));
    let r2 = run(&ts_only(&[&two, &sorted]));
    let t = &r2.time_scatter[0];
    assert!((t.sortedness - 0.89).abs() < 1e-12, "{t:?}");
    assert!(fired(&r2, lints::SCATTERED_TIME_COLUMN, "ts"));
}

#[test]
fn touching_ranges_are_sorted() {
    // Two files stack to depth about 2. Each file's row groups touch: the
    // next minimum equals the previous maximum, which ascends without
    // overlapping (`>=`), so sortedness is 1 and nothing fires.
    let file: Vec<(i64, i64)> = (0..10).map(|i| (i * 100, (i + 1) * 100)).collect();
    let r = run(&ts_only(&[&file, &file]));
    let t = &r.time_scatter[0];
    assert!((t.sortedness - 1.0).abs() < 1e-12, "{t:?}");
    assert!(t.depth >= 1.5, "{t:?}");
    assert!(!fired(&r, lints::SCATTERED_TIME_COLUMN, "ts"));
}

#[test]
fn sentinel_range_factor_threshold() {
    // Three ranges of 100 units (closed) in one file and one long range from
    // the start in another: the lower median is 100, so a range of 800 is
    // kept and one of 801 is excluded as a sentinel.
    let short: &[(i64, i64)] = &[(0, 99), (100, 199), (200, 299)];
    let kept = run(&ts_only(&[short, &[(0, 799)]]));
    let t = &kept.time_scatter[0];
    assert_eq!(t.excluded, 0, "{t:?}");
    assert_eq!((t.covered, t.union), (1_100, 800));
    let excluded = run(&ts_only(&[short, &[(0, 800)]]));
    let t = &excluded.time_scatter[0];
    assert_eq!(t.excluded, 1, "{t:?}");
    assert_eq!((t.covered, t.union), (300, 300));
}

/// Reports `per_row` compressed bytes per row for column `s`.
struct Wide(MemSource, u64);

impl Source for Wide {
    fn discover(&self, r: &str) -> Result<Vec<FileFingerprint>> {
        self.0.discover(r)
    }
    fn row_groups(&self, f: &FileFingerprint) -> Result<Vec<RowGroupMeta>> {
        let mut rgs = self.0.row_groups(f)?;
        for rg in &mut rgs {
            let rows = rg.rows;
            for s in rg.stats.iter_mut().filter(|s| s.column == "s") {
                s.bytes = Some(rows * self.1);
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
fn auto_selection_width_threshold() {
    // A string column at 256 compressed bytes per row stays a candidate; at
    // 257 it is skipped as a document or blob, and the report says why.
    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
    let source = |per_row: u64| {
        let rgs = (0..4)
            .map(|r| {
                let values: Vec<String> = (0..10).map(|i| format!("k{r}-{i}")).collect();
                RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(values))])
                    .unwrap()
            })
            .collect();
        Wide(MemSource::new(schema.clone(), vec![rgs]), per_row)
    };
    let at = run(&source(256));
    assert_eq!(at.key_columns, ["s"]);
    assert!(at.skipped.is_empty(), "{:?}", at.skipped);
    let over = run(&source(257));
    assert!(over.key_columns.is_empty(), "{:?}", over.key_columns);
    assert_eq!(over.skipped.len(), 1);
    assert_eq!(over.skipped[0].column, "s");
    assert!(
        over.skipped[0]
            .reason
            .contains("257 compressed bytes per row"),
        "{:?}",
        over.skipped
    );
}

// ---------------------------------------------------------------------------
// batch_stamp_key: files p50, the composite ratio, narrowness, concordance
// ---------------------------------------------------------------------------

fn batch_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", ts_type(), false),
        Field::new("b", DataType::Int64, false),
        Field::new("id", DataType::UInt64, false),
    ]))
}

/// One row group: rows `(ts, b, id)`.
fn batch_rg(rows: &[(i64, i64, u64)]) -> RecordBatch {
    RecordBatch::try_new(
        batch_schema(),
        vec![
            Arc::new(
                TimestampMicrosecondArray::from(rows.iter().map(|r| r.0).collect::<Vec<_>>())
                    .with_timezone("UTC"),
            ),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// A write-stamp layout: `days` days, `files` files per day, `rgs` row groups
/// per file; every row group of day `d` holds every stamp `d * 1000 + 0..10`,
/// with ids from `ids(day, file, stamp)`.
fn stamps(
    days: i64,
    files: i64,
    rgs: i64,
    ids: impl Fn(i64, i64, i64) -> Vec<u64>,
) -> Vec<Vec<RecordBatch>> {
    let mut out = Vec::new();
    for d in 0..days {
        for f in 0..files {
            let mut file = Vec::new();
            for g in 0..rgs {
                let mut rows = Vec::new();
                for s in 0..10 {
                    for id in ids(d, f, s) {
                        rows.push((T0 + d * DAY + g * 1_000 + s, d * 1_000 + s, id));
                    }
                }
                file.push(batch_rg(&rows));
            }
            out.push(file);
        }
    }
    out
}

/// Unique ids per row.
/// One id per (file, stamp), on two rows, so the id is no row id (a row id
/// is never a partner) while each (id, stamp) pair stays in one row group.
fn unique(d: i64, f: i64, s: i64) -> Vec<u64> {
    let id = (d * 10_000 + f * 100 + s) as u64;
    vec![id, id]
}

#[test]
fn batch_files_threshold() {
    // Two files per day, one row group each: a stamp touches 2 files at the
    // median, exactly the threshold. One file per day with two row groups:
    // the row-group gates still pass (2 row groups per stamp, composite 1),
    // narrowness is the same 0.5, but a stamp touches 1 file.
    let at = run(&MemSource::new(batch_schema(), stamps(2, 2, 1, unique)));
    assert!(fired(&at, lints::BATCH_STAMP_KEY, "b"), "{:?}", at.lints);
    let below = run(&MemSource::new(
        batch_schema(),
        stamps(2, 1, 2, |d, _f, s| {
            vec![
                (d * 10_000 + s) as u64,
                (d * 10_000 + s) as u64,
                (d * 10_000 + 500 + s) as u64,
            ]
        }),
    ));
    let b = below
        .fanout
        .iter()
        .find(|e| e.fanout.columns == ["b"])
        .unwrap();
    assert!((b.fanout.files_per_value[0] - 1.0).abs() < 1e-9);
    assert!((b.fanout.row_groups_per_value[0] - 2.0).abs() < 1e-9);
    assert!(
        !fired(&below, lints::BATCH_STAMP_KEY, "b"),
        "{:?}",
        below.lints
    );
}

#[test]
fn batch_composite_ratio_threshold() {
    // Four files per day: a stamp touches 4 row groups. When every (id,
    // stamp) pair is in exactly 2 files the composite's median is 2 and
    // 4 >= 2 x 2 fires; when most pairs are in 3 files, 4 < 2 x 3.
    let two = stamps(2, 4, 1, |d, f, s| {
        let group = f / 2;
        (0..6)
            .map(|i| (d * 10_000 + group * 100 + i * 10 + s) as u64)
            .map(|v| v / 10 * 10)
            .collect()
    });
    let at = run(&MemSource::new(batch_schema(), two));
    let comp = at
        .fanout
        .iter()
        .find(|e| {
            e.fanout.columns.len() == 2
                && e.fanout.columns.contains(&"b".to_string())
                && e.fanout.columns.contains(&"id".to_string())
        })
        .unwrap();
    assert!(
        (comp.fanout.row_groups_per_value[0] - 2.0).abs() < 1e-9,
        "{:?}",
        comp.fanout
    );
    assert!(fired(&at, lints::BATCH_STAMP_KEY, "b"), "{:?}", at.lints);
    let three = stamps(2, 4, 1, |d, f, _s| {
        if f < 3 {
            (0..7).map(|i| (d * 10_000 + i) as u64).collect()
        } else {
            (0..6).map(|i| (d * 10_000 + 500 + i) as u64).collect()
        }
    });
    let below = run(&MemSource::new(batch_schema(), three));
    let comp = below
        .fanout
        .iter()
        .find(|e| {
            e.fanout.columns.len() == 2
                && e.fanout.columns.contains(&"b".to_string())
                && e.fanout.columns.contains(&"id".to_string())
        })
        .unwrap();
    assert!(
        (comp.fanout.row_groups_per_value[0] - 3.0).abs() < 1e-9,
        "{:?}",
        comp.fanout
    );
    assert!(
        !fired(&below, lints::BATCH_STAMP_KEY, "b"),
        "{:?}",
        below.lints
    );
}

#[test]
fn batch_high_confidence_ratio_threshold() {
    // Two days of 12 writers, with a workload: a stamp touches 12 files at
    // p95. Each (id, stamp) pair recurs in the files of one group: groups
    // of 6 put the composite's p95 at 6 = 0.5 x 12 (high); groups of 7 and
    // 5 put it at 7 (medium). Both pass the shared decision.
    let layout = |groups: fn(i64) -> i64| {
        MemSource::new(
            batch_schema(),
            stamps(2, 12, 1, move |d, f, s| {
                (0..3)
                    .map(|i| (d * 10_000 + groups(f) * 100 + i * 10 + s) as u64)
                    .collect()
            }),
        )
    };
    let tmp = tempfile::tempdir().unwrap();
    common::write_bench_workload(&tmp.path().join("wl"), &[vec![1, 2, 3]]);
    let with_workload = |source: &MemSource| {
        let (_s, store) = temp_store();
        profile(
            source,
            "mem",
            &store,
            &ProfileOptions {
                workload: Some(tmp.path().join("wl")),
                ..ProfileOptions::for_source("mem")
            },
        )
        .unwrap()
    };
    let confidence = |r: &ProfileReport| {
        r.lints
            .iter()
            .find(|l| l.rule == lints::BATCH_STAMP_KEY)
            .unwrap_or_else(|| panic!("{:?}", r.lints))
            .confidence
    };
    let at = with_workload(&layout(|f| f / 6));
    assert!(
        (confidence(&at) - lints::HIGH).abs() < 1e-12,
        "{:?}",
        at.lints
    );
    let over = with_workload(&layout(|f| i64::from(f >= 7)));
    assert!(
        (confidence(&over) - lints::MEDIUM).abs() < 1e-12,
        "{:?}",
        over.lints
    );
}

/// A source whose `ts` maximum of chosen files' row groups is moved to day 0,
/// placing those row groups on the first day for the concordance without
/// touching anything the fanout pass reads.
struct MovedToDayZero {
    inner: MemSource,
    files: Vec<usize>,
}

impl Source for MovedToDayZero {
    fn discover(&self, r: &str) -> Result<Vec<FileFingerprint>> {
        self.inner.discover(r)
    }
    fn row_groups(&self, f: &FileFingerprint) -> Result<Vec<RowGroupMeta>> {
        let mut rgs = self.inner.row_groups(f)?;
        let index = self.inner.files.iter().position(|(fp, _)| fp == f).unwrap();
        if self.files.contains(&index) {
            for rg in &mut rgs {
                let s = rg.stats.iter_mut().find(|s| s.column == "ts").unwrap();
                let mut bytes = vec![0x08, 0x04, 0x03];
                bytes.extend_from_slice(&(((T0 + 5_000) as u64) ^ (1 << 63)).to_be_bytes());
                s.max = Some(bytes.clone());
                s.min = Some(bytes);
            }
        }
        Ok(rgs)
    }
    fn read_columns(&self, rg: &RowGroupMeta, c: &[String]) -> Result<RecordBatch> {
        self.inner.read_columns(rg, c)
    }
    fn schema(&self, r: &str) -> Result<SchemaRef> {
        self.inner.schema(r)
    }
}

#[test]
fn batch_time_concordance_threshold() {
    // Two days of `n` files; moving `k` second-day files to day 0 leaves
    // n x (n - k) of (n + k)(n - k) cross-day pairs in order: n / (n + k).
    // 9 files, k = 1: exactly 0.9 fires; 17 files, k = 2: 0.895 does not.
    let moved = |n: usize, k: usize| MovedToDayZero {
        inner: MemSource::new(batch_schema(), stamps(2, n as i64, 1, unique)),
        files: (n..n + k).collect(),
    };
    let measured = |source: &dyn Source| {
        let rgs: Vec<RowGroupMeta> = source
            .discover("mem")
            .unwrap()
            .iter()
            .flat_map(|f| source.row_groups(f).unwrap())
            .collect();
        time_correlation(&rgs, "b", "ts").unwrap().unwrap()
    };
    let (a, b) = (moved(9, 1), moved(17, 2));
    let (ca, cb) = (measured(&a), measured(&b));
    assert!((ca - 0.9).abs() < 1e-12, "{ca}");
    assert!((cb - 17.0 / 19.0).abs() < 1e-12, "{cb}");
    assert!(ca >= BATCH_MIN_TIME_CORRELATION && cb < BATCH_MIN_TIME_CORRELATION);
    let above = run(&a);
    assert!(
        fired(&above, lints::BATCH_STAMP_KEY, "b"),
        "{:?}",
        above.lints
    );
    let below = run(&b);
    let stamp = below
        .fanout
        .iter()
        .find(|e| e.fanout.columns == ["b"])
        .unwrap();
    assert!(
        stamp.fanout.files_per_value[0] >= 2.0,
        "the fanout gates still pass"
    );
    assert!(
        !fired(&below, lints::BATCH_STAMP_KEY, "b"),
        "{:?}",
        below.lints
    );
}

// ---------------------------------------------------------------------------
// low_selectivity_single_column: median fraction, composite ratio
// ---------------------------------------------------------------------------

fn cat_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("c", DataType::Utf8, false),
        Field::new("id", DataType::UInt64, false),
    ]))
}

/// One file of ten row groups; row group `r` holds rows `rows(r)`.
fn cats(rows: impl Fn(usize) -> Vec<(&'static str, u64)>) -> MemSource {
    let schema = cat_schema();
    let rgs = (0..10)
        .map(|r| {
            let rows = rows(r);
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(
                        rows.iter().map(|x| x.0).collect::<Vec<_>>(),
                    )),
                    Arc::new(UInt64Array::from(
                        rows.iter().map(|x| x.1).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap()
        })
        .collect();
    MemSource::new(schema, vec![rgs])
}

#[test]
fn low_selectivity_fraction_threshold() {
    // Two values in five of ten row groups each: the median touches 50%.
    // Three values in 4, 4, and 2: the median touches 40%. Ids repeat within a
    // row group, so none is a row id.
    let at = run(&cats(|r| {
        let v = if r < 5 { "x" } else { "y" };
        (0..3).map(|i| (v, (r * 10 + i / 2) as u64)).collect()
    }));
    assert!(
        fired(&at, lints::LOW_SELECTIVITY_SINGLE_COLUMN, "c"),
        "{:?}",
        at.lints
    );
    let below = run(&cats(|r| {
        let v = match r {
            0..=3 => "x",
            4..=7 => "y",
            _ => "z",
        };
        (0..3).map(|i| (v, (r * 10 + i / 2) as u64)).collect()
    }));
    assert!(
        !fired(&below, lints::LOW_SELECTIVITY_SINGLE_COLUMN, "c"),
        "{:?}",
        below.lints
    );
}

#[test]
fn low_selectivity_composite_ratio_threshold() {
    // One value in every row group (median 10 of 10). Each id recurs in `w`
    // consecutive row groups, so the composite's p95 is `w`: w = 5 gives
    // 10 >= 2 x 5, w = 6 gives 10 < 12.
    let span = |w: usize| {
        cats(move |r| {
            (0..10usize)
                .filter(|i| (r + 10 - i) % 10 < w)
                .map(|i| ("x", i as u64))
                .collect()
        })
    };
    let at = run(&span(5));
    assert!(
        fired(&at, lints::LOW_SELECTIVITY_SINGLE_COLUMN, "c"),
        "{:?}",
        at.lints
    );
    let below = run(&span(6));
    assert!(
        !fired(&below, lints::LOW_SELECTIVITY_SINGLE_COLUMN, "c"),
        "{:?}",
        below.lints
    );
}

// ---------------------------------------------------------------------------
// footer distinct counts: within 4 sigma of the sketch, or refused
// ---------------------------------------------------------------------------

/// Adds `extra` (which may be negative) to the footer distinct counts,
/// spread over the row groups, so their sum moves by exactly `extra`.
struct Shifted(MemSource, i64);

impl Source for Shifted {
    fn discover(&self, r: &str) -> Result<Vec<FileFingerprint>> {
        self.0.discover(r)
    }
    fn row_groups(&self, f: &FileFingerprint) -> Result<Vec<RowGroupMeta>> {
        let mut rgs = self.0.row_groups(f)?;
        let file = self.0.files.iter().position(|(fp, _)| fp == f).unwrap();
        for rg in &mut rgs {
            let slot = (file * 8 + rg.index_in_file as usize) as i64;
            // Row group `slot` of 32 takes its share of `extra`.
            let share = (self.1 * (slot + 1)).div_euclid(32) - (self.1 * slot).div_euclid(32);
            for s in &mut rg.stats {
                s.distinct_count = s.distinct_count.map(|d| (d as i64 + share) as u64);
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
fn footer_distinct_sigma_threshold() {
    // 32 pairwise-disjoint row groups of 1,000 ids, each id twice (so a
    // count above 1,000 is still plausible), sampled. The footer sum is used
    // only while the sketch estimate E lies within 4 standard errors of it:
    // S (1 - t) <= E <= S (1 + t). Sums two values inside each edge of that
    // band are used; two values outside are refused.
    let data = |counts: bool| {
        let files: Vec<Vec<RecordBatch>> = (0..4u64)
            .map(|f| {
                (0..8u64)
                    .map(|i| {
                        let base = (f * 8 + i) * 1_000;
                        u64_batch("v", (0..2_000).map(|j| base + j / 2).collect())
                    })
                    .collect()
            })
            .collect();
        let mut mem = MemSource::new(files[0][0].schema(), files);
        mem.distinct_counts = counts;
        mem
    };
    let options = ProfileOptions {
        max_tracked_per_key: Some(14),
        ..ProfileOptions::for_source("mem")
    };
    let (_s, store) = temp_store();
    let plain = profile(&data(false), "mem", &store, &options).unwrap();
    assert_eq!(plain.fanout[0].sample.distinct_source, DistinctSource::Hll);
    let estimate = plain.fanout[0].sample.hll_estimate as f64;
    // 4 standard errors of a 2^14-register sketch, 4 x 1.04 / 128, written
    // out rather than read from the constants it pins.
    let t = 0.0325;
    let truth = 32_000i64;
    let low_edge = (estimate / (1.0 + t)).ceil() as i64;
    let high_edge = (estimate / (1.0 - t)).floor() as i64;
    for (sum, accepted) in [
        (low_edge - 2, false),
        (low_edge + 2, true),
        (high_edge - 2, true),
        (high_edge + 2, false),
    ] {
        let (_t, store) = temp_store();
        let got = profile(&Shifted(data(true), sum - truth), "mem", &store, &options);
        if accepted {
            let e = &got.unwrap().fanout[0];
            assert_eq!(e.sample.distinct_source, DistinctSource::FooterDisjoint);
            assert_eq!(e.fanout.distinct, sum as u64);
            assert!(!e.fanout.sampled, "a verified footer sum is exact");
            assert_eq!(e.fanout.row_groups_per_value, [1.0; 3]);
        } else {
            let err = got.unwrap_err();
            assert!(
                err.to_string().contains("footer distinct counts"),
                "{sum}: {err}"
            );
        }
    }
}

#[test]
fn a_row_id_is_never_a_partner() {
    // One value of `c` in every row group; the only partner is `id`. When
    // each id is on one row it is a row id (distinct = rows) and no lint
    // recommends it; on two rows of the same row group it is a key that
    // repeats, and low selectivity recommends (c, id).
    let unique = cats(|r| (0..4).map(|i| ("x", (r * 10 + i) as u64)).collect());
    let r = run(&unique);
    assert!(
        r.lints.iter().all(|l| !l.message.contains("id")),
        "{:?}",
        r.lints
    );
    let repeating = cats(|r| (0..4).map(|i| ("x", (r * 10 + i / 2) as u64)).collect());
    let r = run(&repeating);
    assert!(
        fired(&r, lints::LOW_SELECTIVITY_SINGLE_COLUMN, "c"),
        "{:?}",
        r.lints
    );
}

#[test]
fn a_supplied_row_id_is_the_workloads_key() {
    // One value of `c` in every row group; `entity_id` is unique per row, an
    // exact row id. Unsupplied it is refused; when the recorded workload
    // supplies it, it is the workload's key and becomes the partner.
    let schema = Arc::new(Schema::new(vec![
        Field::new("c", DataType::Utf8, false),
        Field::new("entity_id", DataType::UInt64, false),
    ]));
    let rgs = (0..10u64)
        .map(|r| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["x"; 4])),
                    Arc::new(UInt64Array::from(
                        (0..4).map(|i| r * 10 + i).collect::<Vec<u64>>(),
                    )),
                ],
            )
            .unwrap()
        })
        .collect();
    let source = MemSource::new(schema, vec![rgs]);
    let r = run(&source);
    assert!(
        !fired(&r, lints::LOW_SELECTIVITY_SINGLE_COLUMN, "c"),
        "{:?}",
        r.lints
    );
    let tmp = tempfile::tempdir().unwrap();
    common::write_bench_workload(&tmp.path().join("wl"), &[vec![1, 2, 3]]);
    let (_s, store) = temp_store();
    let with = profile(
        &source,
        "mem",
        &store,
        &ProfileOptions {
            workload: Some(tmp.path().join("wl")),
            ..ProfileOptions::for_source("mem")
        },
    )
    .unwrap();
    let lint = with
        .lints
        .iter()
        .find(|l| l.rule == lints::LOW_SELECTIVITY_SINGLE_COLUMN)
        .unwrap_or_else(|| panic!("{:?}", with.lints));
    assert!(lint.message.contains("(entity_id, c)"), "{lint:?}");
}
