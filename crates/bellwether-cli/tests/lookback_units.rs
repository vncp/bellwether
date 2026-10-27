//! PR #30 review, items 2 and 3: an `asof` lookback recorded in one time
//! column's unit and priced on another's. `why --latest-by day --at
//! 2026-01-04 --lookback 1d` records the window `[2026-01-03, 2026-01-04]`
//! in days; a proposal of `exact:sym@ts` plans it on `ts` (microseconds),
//! so the window converts to `[2026-01-03T00:00, 2026-01-04T23:59:59.999999]`,
//! rounding the upper bound outward. Rounding it inward (`hi · f`, the
//! defect) ends the window at `2026-01-04T00:00` and prices 4 of the 8 row
//! groups the run's rows sit in; fix's verify, filtering `ts` with the same
//! window, then compares no row (or the wrong ones).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

mod common;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Date32Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use common::{path, run};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde_json::Value;

const CLOCK: &str = "2026-10-25T00:00:00Z";
const DAY_US: i64 = 86_400_000_000;
const HOUR_US: i64 = 3_600_000_000;
/// 2026-01-04 in days since the epoch.
const AT_DAY: i64 = 20_457;

/// `A`'s time in each of ten row groups: two outside the day window
/// `[2026-01-03, 2026-01-04]` (row groups 0 and 9) and eight inside it,
/// four of them after 2026-01-04T00:00 (row groups 5 to 8).
fn a_times() -> [i64; 10] {
    let d3 = (AT_DAY - 1) * DAY_US;
    let d4 = AT_DAY * DAY_US;
    [
        d3 - HOUR_US,
        d3,
        d3 + 12 * HOUR_US,
        d4 - 1,
        d4,
        d4 + 1,
        d4 + 6 * HOUR_US,
        d4 + 12 * HOUR_US,
        d4 + DAY_US - 1,
        d4 + DAY_US,
    ]
}

/// One file, ten row groups of two rows: `A` at [`a_times`] and `B` five
/// days after (row groups 0 to 4) or before (5 to 9), so every row group's
/// `day` range spans the window and `range:day` keeps all ten for `A`. `day` is `ts`'s UTC date.
fn corpus(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("sym", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("day", DataType::Date32, false),
    ]));
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(usize::MAX))
        .build();
    let file = File::create(dir.join("part-0.parquet")).unwrap();
    let mut w = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
    for (i, a) in a_times().into_iter().enumerate() {
        let b = a + if i < 5 { 5 } else { -5 } * DAY_US;
        let ts = [a, b];
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["A", "B"])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(ts.to_vec()).with_timezone("UTC")),
                Arc::new(Date32Array::from(
                    ts.iter()
                        .map(|t| i32::try_from(t.div_euclid(DAY_US)).unwrap())
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.flush().unwrap();
    }
    w.close().unwrap();
}

fn keys(path: &Path) {
    let schema = Arc::new(Schema::new(vec![Field::new("sym", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec!["A"])) as ArrayRef],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn last(out: &str) -> Value {
    serde_json::from_str(out.lines().last().unwrap()).unwrap()
}

fn hash(v: &Value) -> String {
    v.get("hash").map_or(v, |h| h).as_str().unwrap().to_string()
}

#[test]
fn a_day_lookback_priced_on_a_microsecond_column_keeps_every_row_group_of_the_window() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data);
    let k = dir.path().join("k.parquet");
    keys(&k);
    let (d, k) = (path(&data), path(&k));
    // The current configuration plans the run on `day` (its window column),
    // so the run's instant and length are measured in days.
    run(
        &[
            "--clock",
            CLOCK,
            "-q",
            "index",
            d,
            "--key",
            "exact:sym",
            "--key",
            "range:day",
        ],
        0,
    );
    let why = last(&run(
        &[
            "--clock",
            CLOCK,
            "--json",
            "why",
            d,
            "--from",
            k,
            "--latest-by",
            "day",
            "--at",
            "2026-01-04",
            "--lookback",
            "1d",
        ],
        0,
    ));
    assert_eq!(
        why["workload"]["lookup"]["ts"], AT_DAY,
        "{}",
        why["workload"]
    );
    assert_eq!(why["current"]["row_groups"], 10, "range:day keeps all ten");
    let workload = hash(&why["workload"]);
    let evidence = hash(&why["evidence"]);
    let proposal = hash(
        &last(&run(
            &[
                "--clock",
                CLOCK,
                "--json",
                "propose",
                d,
                "--key",
                "exact:sym@ts",
                "--by",
                "test",
                "--evidence",
                &evidence,
            ],
            0,
        ))["proposal"],
    );
    let fix = last(&run(
        &[
            "--clock",
            CLOCK,
            "--json",
            "fix",
            d,
            "--proposal",
            &proposal,
            "--workload",
            &workload,
        ],
        0,
    ));
    // Replay prices the eight row groups the run's rows sit in: the window
    // on `ts` ends at 2026-01-04T23:59:59.999999, not at 00:00.
    let replay = &fix["score"]["replay"]["record"];
    assert_eq!(replay["proposed"][0]["row_groups"], 8, "{replay}");
    assert_eq!(replay["current"][0]["row_groups"], 10, "{replay}");
    // Verify compares the run's rows, filtering `ts` with the same window.
    assert_eq!(fix["outcome"]["kind"], "applied", "{}", fix["outcome"]);
    let counts = &fix["verification"]["counts"];
    assert_eq!(counts["keys_compared"], 1, "{counts}");
    assert_eq!(counts["matched"], 1, "{counts}");
    let filter = &fix["certificate"]["record"]["body"]["row_filter"][0]["all"];
    let between = filter
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["op"] == "between")
        .unwrap();
    assert_eq!(between["column"], "ts", "{filter}");
    // The built index plans the same row groups for the window on `ts`.
    let plan = last(&run(
        &[
            "--clock",
            CLOCK,
            "--json",
            "locate",
            d,
            "--keys",
            k,
            "--from",
            &((AT_DAY - 1) * DAY_US).to_string(),
            "--to",
            &((AT_DAY + 1) * DAY_US - 1).to_string(),
            "--ts",
            "ts",
        ],
        0,
    ));
    assert_eq!(plan["planned"]["row_groups"], 8, "{}", plan["planned"]);
    let rgs: Vec<u64> = plan["evidence"]["record"]["body"]["plan"]["entries"][0]["row_groups"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r.as_u64().unwrap())
        .collect();
    assert_eq!(rgs, (1..=8).collect::<Vec<_>>());
}
