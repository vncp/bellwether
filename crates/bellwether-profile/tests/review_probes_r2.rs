//! PR 17 review, second round: probes against cc70f24, kept as regression
//! tests unchanged.
//! Run: cargo test -p bellwether-profile --test review_probes_r2 -- --nocapture --test-threads=1

#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::unwrap_used,
    dead_code,
    unused_imports
)]

mod common;

use std::sync::Arc;

use arrow::array::{RecordBatch, TimestampMicrosecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_profile::{lints, profile, render, ProfileOptions, ProfileReport};
use common::{mix, temp_store, MemSource};

const HOUR: i64 = 3_600_000_000;
const T0: i64 = 1_767_225_600_000_000; // 2026-01-01T00:00:00Z

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("id", DataType::UInt64, false),
    ]))
}

/// One writer, 200 hourly row groups of 100 rows written in arrival order.
/// Every third row group also holds one row back-dated by `late_hours`.
fn hourly(late_hours: i64) -> MemSource {
    let schema = schema();
    let rgs: Vec<RecordBatch> = (0..200i64)
        .map(|h| {
            let mut ts: Vec<i64> = (0..100i64)
                .map(|i| T0 + h * HOUR + i * (HOUR / 100))
                .collect();
            if h >= 48 && h % 3 == 0 {
                ts[50] = T0 + h * HOUR - late_hours * HOUR;
            }
            let ids: Vec<u64> = (0..100u64).map(|i| h as u64 * 100 + i).collect();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
                    Arc::new(UInt64Array::from(ids)),
                ],
            )
            .unwrap()
        })
        .collect();
    MemSource::new(schema, vec![rgs])
}

fn run(source: &MemSource) -> ProfileReport {
    let (_s, store) = temp_store();
    profile(source, "mem", &store, &ProfileOptions::for_source("mem")).unwrap()
}

/// How many row groups a one-hour range query on `ts` must read, averaged
/// over every hour, from the footer bounds: what the lint is about.
fn mean_selected(source: &MemSource) -> f64 {
    let rgs: Vec<(i64, i64)> = source.files[0]
        .1
        .iter()
        .map(|b| {
            let a = b
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let v: Vec<i64> = (0..a.len()).map(|i| a.value(i)).collect();
            (*v.iter().min().unwrap(), *v.iter().max().unwrap())
        })
        .collect();
    let mut total = 0usize;
    for h in 48..200i64 {
        let (lo, hi) = (T0 + h * HOUR, T0 + (h + 1) * HOUR - 1);
        total += rgs.iter().filter(|&&(a, b)| a <= hi && b >= lo).count();
    }
    total as f64 / 152.0
}

/// Late rows four hours back: ranges widen 5x, under the 8x sentinel limit.
#[test]
fn p10b_late_rows_four_hours_back_are_scattered() {
    let s = hourly(4);
    let r = run(&s);
    let t = &r.time_scatter[0];
    eprintln!(
        "p10b: one-hour query reads {:.1} row groups; {t:?}",
        mean_selected(&s)
    );
    assert!(
        r.lints
            .iter()
            .any(|l| l.rule == lints::SCATTERED_TIME_COLUMN),
        "{:?}",
        r.lints
    );
}

/// The same layout with the late rows two days back: each widened range is
/// 49x the median, so the shared measure drops a third of the row groups as
/// sentinels, and with them the evidence of disorder.
#[test]
fn p10_late_rows_two_days_back_are_scattered_too() {
    let s = hourly(48);
    let r = run(&s);
    let t = &r.time_scatter[0];
    eprintln!(
        "p10: one-hour query reads {:.1} row groups; {t:?}",
        mean_selected(&s)
    );
    assert!(
        r.lints
            .iter()
            .any(|l| l.rule == lints::SCATTERED_TIME_COLUMN),
        "MISSED: a third of the row groups hold two-day-late rows ({} excluded as sentinels), \
         a one-hour range query reads {:.1} row groups, yet no scattered_time_column: {t:?}",
        t.excluded,
        mean_selected(&s)
    );
}
