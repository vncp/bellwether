//! Fanout is exact when unsampled, on layouts generated here and checked
//! against a brute-force oracle that reads the Parquet files directly; the
//! lints fire on the planted layout and stay silent where a pattern is not
//! planted. These run in CI; `tests/ground_truth.rs` repeats the checks on
//! benchmark corpora when they are present.

#![allow(clippy::unwrap_used, clippy::cast_precision_loss)]

mod common;

use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether_profile::{lints, profile, FanoutEntry, ProfileOptions, ProfileReport};
use bellwether_source::stats::{SCATTER_MAX_SORTEDNESS, SCATTER_MIN_DEPTH};
use bellwether_source::ParquetSource;
use common::{oracle, path_str, temp_store, Layout, MemSource};

const KEYS: [&[&str]; 6] = [
    &["entity_id"],
    &["batch_id"],
    &["status"],
    &["entity_id", "batch_id"],
    &["entity_id", "status"],
    &["batch_id", "status"],
];

fn entry<'a>(report: &'a ProfileReport, columns: &[&str]) -> &'a FanoutEntry {
    report
        .fanout
        .iter()
        .find(|e| {
            e.fanout.columns.len() == columns.len()
                && columns
                    .iter()
                    .all(|c| e.fanout.columns.iter().any(|x| x == c))
        })
        .unwrap_or_else(|| panic!("no fanout for {columns:?}"))
}

fn lint_on<'a>(
    report: &'a ProfileReport,
    rule: &str,
    column: &str,
) -> Option<&'a bellwether_types::Lint> {
    report
        .lints
        .iter()
        .find(|l| l.rule == rule && l.columns == [column.to_string()])
}

fn run(layout: &Layout) -> (tempfile::TempDir, std::path::PathBuf, ProfileReport) {
    let tmp = tempfile::tempdir().unwrap();
    let data = layout.write(&tmp.path().join("data"));
    let (_s, store) = temp_store();
    let report = profile(
        &ParquetSource::new(),
        &path_str(&data),
        &store,
        &ProfileOptions::for_source("parquet"),
    )
    .unwrap();
    (tmp, data, report)
}

fn assert_exact(report: &ProfileReport, data: &std::path::Path, layout: &Layout) {
    let keys: Vec<Vec<&str>> = KEYS.iter().map(|k| k.to_vec()).collect();
    let truth = oracle(data, &keys);
    let files = u64::from(layout.days * layout.files_per_day);
    for (key, t) in keys.iter().zip(&truth) {
        let f = &entry(report, key).fanout;
        assert!(
            !f.sampled,
            "{key:?} must be unsampled at the default budget"
        );
        assert_eq!(f.distinct, t.distinct, "{key:?} distinct");
        assert_eq!(
            f.files_per_value,
            t.files_per_value(),
            "{key:?} files per value"
        );
        assert_eq!(
            f.row_groups_per_value,
            t.row_groups_per_value(),
            "{key:?} row groups per value"
        );
        assert_eq!(f.files_total, files);
        assert_eq!(f.row_groups_total, files * u64::from(layout.rgs_per_file));
    }
}

#[test]
fn planted_layout_fanout_is_exact_and_every_lint_fires() {
    let layout = Layout::planted();
    let (_tmp, data, report) = run(&layout);
    // Auto selection: every id-like column, the 48-byte string payload
    // included (no id is dropped by width below 256 bytes per row); the
    // timestamp is a time column, listed as skipped with its reason.
    assert_eq!(
        report.key_columns,
        ["entity_id", "batch_id", "status", "payload"]
    );
    assert_eq!(report.time_columns, ["version_ts"]);
    assert_eq!(report.skipped.len(), 1, "{:?}", report.skipped);
    assert_eq!(report.skipped[0].column, "version_ts");
    assert_eq!(report.rows_total, layout.rows());
    assert_exact(&report, &data, &layout);

    let b =
        lint_on(&report, lints::BATCH_STAMP_KEY, "batch_id").expect("batch_stamp_key on batch_id");
    assert!(b.message.contains("(entity_id, batch_id)"), "{}", b.message);
    assert!(
        (b.confidence - lints::MEDIUM).abs() < 1e-9,
        "no workload: medium"
    );
    assert!(b.site.is_none());
    assert!(lint_on(&report, lints::BATCH_STAMP_KEY, "entity_id").is_none());
    assert!(lint_on(&report, lints::BATCH_STAMP_KEY, "status").is_none());
    let l = lint_on(&report, lints::LOW_SELECTIVITY_SINGLE_COLUMN, "status")
        .expect("low selectivity on status");
    // The partner is the repeating id, not the near-unique payload, and the
    // composite is named as measured: schema order without a workload, with
    // the order's assumption stated.
    assert!(l.message.contains("(entity_id, status)"), "{}", l.message);
    assert!(
        l.message.contains("assumes lookups supply entity_id"),
        "{}",
        l.message
    );
    assert!(
        lint_on(&report, lints::LOW_SELECTIVITY_SINGLE_COLUMN, "batch_id").is_none(),
        "a batch stamp is not a key at all"
    );
    assert!(lint_on(&report, lints::SCATTERED_TIME_COLUMN, "version_ts").is_some());
    // Never the plan rules.
    for rule in [
        "lookback_unbounded",
        "unprefiltered_window",
        "wide_projection_before_filter",
    ] {
        assert!(report.lints.iter().all(|l| l.rule != rule));
    }
    // Composites are listed in the order they were measured; nothing is
    // relabeled.
    assert!(report
        .fanout
        .iter()
        .any(|e| e.fanout.columns == ["entity_id", "batch_id"]));
    assert!(report
        .fanout
        .iter()
        .any(|e| e.fanout.columns == ["entity_id", "status"]));
    let scatter = report
        .time_scatter
        .iter()
        .find(|t| t.column == "version_ts")
        .unwrap();
    assert!(scatter.scattered, "{scatter:?}");
    assert!(scatter.depth >= SCATTER_MIN_DEPTH && scatter.sortedness < SCATTER_MAX_SORTEDNESS);
}

#[test]
fn flat_layout_plants_neither_batch_stamp_nor_time_scatter() {
    let layout = Layout::flat();
    let (_tmp, data, report) = run(&layout);
    assert_exact(&report, &data, &layout);
    // One file per day: a batch value stays in one file.
    assert!((entry(&report, &["batch_id"]).fanout.files_per_value[0] - 1.0).abs() < 1e-9);
    assert!(
        lint_on(&report, lints::BATCH_STAMP_KEY, "batch_id").is_none(),
        "{:?}",
        report.lints
    );
    assert!(
        lint_on(&report, lints::SCATTERED_TIME_COLUMN, "version_ts").is_none(),
        "{:?}",
        report.lints
    );
    let scatter = report
        .time_scatter
        .iter()
        .find(|t| t.column == "version_ts")
        .unwrap();
    // One writer per day, sorted: disjoint ranges.
    assert!((scatter.depth - 1.0).abs() < 1e-9, "{scatter:?}");
    assert!(!scatter.scattered);
    // status still touches every row group.
    assert!(lint_on(&report, lints::LOW_SELECTIVITY_SINGLE_COLUMN, "status").is_some());
}

#[test]
fn one_file_per_day_is_not_a_batch_stamp_even_when_a_batch_spans_row_groups() {
    // Row groups of 5 rows: a 20-minute batch (about 8 rows) spans two or
    // three row groups of its one file. The benchmark plants the pattern only
    // when one value touches at least two files, so the lint must stay silent
    // although the row-group gates alone would pass.
    let layout = Layout {
        days: 2,
        files_per_day: 1,
        rgs_per_file: 120,
        rows_per_rg: 5,
        ..Layout::flat()
    };
    let (_tmp, data, report) = run(&layout);
    let batch = &entry(&report, &["batch_id"]).fanout;
    assert!((batch.files_per_value[0] - 1.0).abs() < 1e-9, "{batch:?}");
    assert!(
        batch.row_groups_per_value[0] >= 2.0,
        "the layout must make a batch span row groups: {batch:?}"
    );
    let composite = &entry(&report, &["entity_id", "batch_id"]).fanout;
    assert!(
        batch.row_groups_per_value[0]
            >= bellwether_source::stats::BATCH_MIN_RATIO * composite.row_groups_per_value[0],
        "the row-group ratio gate passes here; only the files gate decides"
    );
    assert!(
        lint_on(&report, lints::BATCH_STAMP_KEY, "batch_id").is_none(),
        "{:?}",
        report.lints
    );
    let truth = oracle(&data, &[vec!["batch_id"]]);
    assert_eq!(batch.row_groups_per_value, truth[0].row_groups_per_value());
}

#[test]
fn parallel_writers_without_late_rows_are_not_time_scatter() {
    // Many files per day, no late rows: footer ranges stack across files
    // (depth about 6), but every file still ascends.
    let layout = Layout {
        late_fraction: 0.0,
        ..Layout::planted()
    };
    let (_tmp, _data, report) = run(&layout);
    let scatter = report
        .time_scatter
        .iter()
        .find(|t| t.column == "version_ts")
        .unwrap();
    assert!(scatter.depth >= SCATTER_MIN_DEPTH, "{scatter:?}");
    assert!((scatter.sortedness - 1.0).abs() < 1e-9, "{scatter:?}");
    assert!(!scatter.scattered);
    assert!(lint_on(&report, lints::SCATTERED_TIME_COLUMN, "version_ts").is_none());
    assert!(lint_on(&report, lints::BATCH_STAMP_KEY, "batch_id").is_some());
}

#[test]
fn one_day_is_never_a_batch_stamp_and_two_days_are() {
    // The review's one-day and two-day corpora as test layouts: 8 writers a
    // day, one row group each, no late rows. One day leaves time concordance
    // undefined, so batch_id is not called a stamp. Two days make it one,
    // and then low selectivity is never raised on it.
    let one = Layout {
        days: 1,
        files_per_day: 8,
        rgs_per_file: 1,
        rows_per_rg: 500,
        entities: 8_000,
        late_fraction: 0.0,
        ..Layout::planted()
    };
    let (_t1, _d1, r1) = run(&one);
    assert!(
        lint_on(&r1, lints::BATCH_STAMP_KEY, "batch_id").is_none(),
        "{:?}",
        r1.lints
    );
    // One day in eight row groups per file: narrow ranges, many files per
    // value, and a selective composite, so every gate but time concordance
    // passes, and that one alone keeps the lint silent.
    let narrow = Layout {
        rgs_per_file: 8,
        rows_per_rg: 125,
        ..one
    };
    let (_t3, _d3, r3) = run(&narrow);
    let stamp = &entry(&r3, &["batch_id"]).fanout;
    let composite = &entry(&r3, &["entity_id", "batch_id"]).fanout;
    assert!(stamp.files_per_value[0] >= 2.0, "{stamp:?}");
    assert!(
        stamp.row_groups_per_value[0] >= 2.0 * composite.row_groups_per_value[0],
        "{stamp:?} {composite:?}"
    );
    let ranges = r3.ranges.iter().find(|r| r.column == "batch_id").unwrap();
    assert!(ranges.typical_overlap <= 0.5, "{ranges:?}");
    assert!(
        lint_on(&r3, lints::BATCH_STAMP_KEY, "batch_id").is_none(),
        "{:?}",
        r3.lints
    );
    let two = Layout { days: 2, ..one };
    let (_t2, _d2, r2) = run(&two);
    assert!(
        lint_on(&r2, lints::BATCH_STAMP_KEY, "batch_id").is_some(),
        "{:?}",
        r2.lints
    );
    assert!(
        lint_on(&r2, lints::LOW_SELECTIVITY_SINGLE_COLUMN, "batch_id").is_none(),
        "{:?}",
        r2.lints
    );
    // Every composite a lint names is a measured one, in its measured order.
    let columns: Vec<String> = Layout::schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    for report in [&r1, &r2] {
        for lint in &report.lints {
            for named in lint.message.split('(').skip(1) {
                let Some((inside, _)) = named.split_once(')') else {
                    continue;
                };
                let parts: Vec<String> = inside.split(", ").map(str::to_string).collect();
                if parts.len() < 2 || !parts.iter().all(|p| columns.contains(p)) {
                    continue;
                }
                assert!(
                    report.fanout.iter().any(|e| e.fanout.columns == parts),
                    "{} names {parts:?}, which is not measured in that order",
                    lint.rule
                );
            }
        }
    }
}

#[test]
fn a_supplied_column_leads_its_composites_even_when_later_in_the_schema() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("status", DataType::Utf8, false),
        Field::new("entity_id", DataType::UInt64, false),
    ]));
    let rgs = (0..4u64)
        .map(|r| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["a", "b", "a", "b"])),
                    Arc::new(UInt64Array::from(
                        (0..4).map(|i| r * 4 + i).collect::<Vec<u64>>(),
                    )),
                ],
            )
            .unwrap()
        })
        .collect();
    let source = MemSource::new(schema, vec![rgs]);
    let composite = |report: &ProfileReport| {
        report
            .fanout
            .iter()
            .find(|e| e.fanout.columns.len() == 2)
            .unwrap()
            .fanout
            .columns
            .clone()
    };
    let (_s, store) = temp_store();
    let schema_order = profile(&source, "mem", &store, &ProfileOptions::for_source("mem")).unwrap();
    assert_eq!(composite(&schema_order), ["status", "entity_id"]);
    // The bench workload supplies entity_id, so it leads.
    let tmp = tempfile::tempdir().unwrap();
    common::write_bench_workload(&tmp.path().join("wl"), &[vec![1, 2, 3]]);
    let (_t, store) = temp_store();
    let supplied = profile(
        &source,
        "mem",
        &store,
        &ProfileOptions {
            workload: Some(tmp.path().join("wl")),
            ..ProfileOptions::for_source("mem")
        },
    )
    .unwrap();
    assert_eq!(composite(&supplied), ["entity_id", "status"]);
}

#[test]
fn a_batch_stamp_is_never_a_low_selectivity_partner() {
    // The review's few-entities layout: 100 entities, fewer than the batch
    // values, 8 writers a day for 3 days. batch_id is a stamp; low
    // selectivity on status must pair it with something else or stay silent.
    let layout = Layout {
        days: 3,
        files_per_day: 8,
        rgs_per_file: 2,
        rows_per_rg: 50,
        entities: 100,
        late_fraction: 0.0,
        payload_bytes: 32,
        seed: 25,
    };
    let (_tmp, _data, report) = run(&layout);
    assert!(
        lint_on(&report, lints::BATCH_STAMP_KEY, "batch_id").is_some(),
        "{:?}",
        report.lints
    );
    for l in report
        .lints
        .iter()
        .filter(|l| l.rule == lints::LOW_SELECTIVITY_SINGLE_COLUMN)
    {
        assert!(!l.message.contains("batch_id"), "{l:?}");
    }
    // And the stamp's own partner is not the unique payload.
    let stamp = lint_on(&report, lints::BATCH_STAMP_KEY, "batch_id").unwrap();
    assert!(!stamp.message.contains("payload"), "{stamp:?}");
}

#[test]
fn nulls_follow_adr_014_per_tuple() {
    // a: leading column; b: trailing. Rows across two row groups:
    //   rg0: (1, x) (1, null) (null, x)
    //   rg1: (1, x) (2, null) (null, null)
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::UInt64, true),
        Field::new("b", DataType::Utf8, true),
    ]));
    let batch = |a: Vec<Option<u64>>, b: Vec<Option<&str>>| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(a)),
                Arc::new(StringArray::from(b)),
            ],
        )
        .unwrap()
    };
    let source = MemSource::new(
        schema.clone(),
        vec![vec![
            batch(
                vec![Some(1), Some(1), None],
                vec![Some("x"), None, Some("x")],
            ),
            batch(vec![Some(1), Some(2), None], vec![Some("x"), None, None]),
        ]],
    );
    let (_s, store) = temp_store();
    let report = profile(
        &source,
        "mem",
        &store,
        &ProfileOptions {
            columns: Some(vec!["a".into(), "b".into()]),
            ..ProfileOptions::for_source("parquet")
        },
    )
    .unwrap();
    // [a]: values 1 (both row groups) and 2: nulls are not indexed.
    let a = &entry(&report, &["a"]).fanout;
    assert_eq!(a.distinct, 2);
    assert_eq!(a.row_groups_per_value, [1.0, 2.0, 2.0]);
    // [b]: only "x".
    assert_eq!(entry(&report, &["b"]).fanout.distinct, 1);
    // [a, b]: (1,x) in both, (1,null), (2,null); rows with a null `a` are
    // not indexed, a null `b` is a distinct trailing marker.
    let ab = &entry(&report, &["a", "b"]).fanout;
    assert_eq!(ab.distinct, 3);
    assert_eq!(ab.row_groups_per_value, [1.0, 2.0, 2.0]);
}

#[test]
fn explicit_columns_are_validated() {
    let source = MemSource::new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::UInt64, false)])),
        vec![vec![common::u64_batch("a", vec![1, 2, 3])]],
    );
    let (_s, store) = temp_store();
    for bad in [vec!["missing".to_string()], vec!["a".into(), "a".into()]] {
        let err = profile(
            &source,
            "mem",
            &store,
            &ProfileOptions {
                columns: Some(bad.clone()),
                ..ProfileOptions::for_source("parquet")
            },
        )
        .unwrap_err();
        assert!(
            matches!(err, bellwether_types::Error::KeySpec(_)),
            "{bad:?}: {err}"
        );
    }
    let err = profile(
        &source,
        "mem",
        &store,
        &ProfileOptions {
            time_columns: Some(vec!["a".into()]),
            ..ProfileOptions::for_source("parquet")
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("not a date or timestamp"), "{err}");
}

#[test]
fn a_source_that_returns_the_wrong_row_count_is_an_error() {
    struct Short(MemSource);
    impl bellwether_types::Source for Short {
        fn discover(
            &self,
            r: &str,
        ) -> bellwether_types::Result<Vec<bellwether_types::FileFingerprint>> {
            self.0.discover(r)
        }
        fn row_groups(
            &self,
            f: &bellwether_types::FileFingerprint,
        ) -> bellwether_types::Result<Vec<bellwether_types::RowGroupMeta>> {
            let mut rgs = self.0.row_groups(f)?;
            for rg in &mut rgs {
                rg.rows += 1;
            }
            Ok(rgs)
        }
        fn read_columns(
            &self,
            rg: &bellwether_types::RowGroupMeta,
            c: &[String],
        ) -> bellwether_types::Result<RecordBatch> {
            self.0.read_columns(rg, c)
        }
        fn schema(&self, r: &str) -> bellwether_types::Result<arrow::datatypes::SchemaRef> {
            self.0.schema(r)
        }
    }
    let source = Short(MemSource::new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::UInt64, false)])),
        vec![vec![common::u64_batch("a", vec![1, 2, 3])]],
    ));
    let (_s, store) = temp_store();
    let err = profile(
        &source,
        "mem",
        &store,
        &ProfileOptions::for_source("parquet"),
    )
    .unwrap_err();
    assert!(err.to_string().contains("statistics say"), "{err}");
}
