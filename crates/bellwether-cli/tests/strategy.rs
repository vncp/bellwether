//! Owner strategy requests 1 and 2 through the binary on a lake of one
//! file per session whose date is only in the file name: `--path-template`
//! on index, locate, latest, asof, and why; `locate --window`,
//! `asof --at --lookback`, `latest --lookback`; and `why` pricing a window
//! exactly as `locate` plans it. Each plan is checked against the rows the
//! test wrote.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::cast_possible_wrap
)]

mod common;

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use common::{bw, path, run};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde_json::Value;

const TEMPLATE: &str = "daily/{day:date}.parquet";
/// 2026-03-02 in days since the epoch.
const FIRST_DAY: i64 = 20_514;
const US_PER_DAY: i64 = 86_400_000_000;

/// Five sessions, one file each, two rows per row group: AAPL and MSFT
/// every day, NVDA on the first two only. Returns (file, row group,
/// symbol, ts) of every row.
fn lake(root: &Path) -> Vec<(String, u64, String, i64)> {
    lake_at(root, |date| format!("daily/{date}.parquet"))
}

/// [`lake`] with each session's file at `file_of(date)` below `root`.
fn lake_at(root: &Path, file_of: fn(&str) -> String) -> Vec<(String, u64, String, i64)> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("close", DataType::Int64, false),
    ]));
    let mut out = Vec::new();
    for i in 0..5i64 {
        let day = FIRST_DAY + i;
        let mut rows = vec![
            ("AAPL", day * US_PER_DAY + 1),
            ("MSFT", day * US_PER_DAY + 2),
        ];
        if i < 2 {
            rows.push(("NVDA", day * US_PER_DAY + 3));
        }
        if i == 1 {
            // Two NVDA prints in one row group: the per-key bounds hold a
            // min and a max that differ.
            rows.insert(3, ("NVDA", day * US_PER_DAY + 9));
        }
        rows.push(("MSFT", day * US_PER_DAY + 4));
        let date = format!("2026-03-{:02}", 2 + i);
        let rel = file_of(&date);
        let file = root.join(&rel);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(
                    TimestampMicrosecondArray::from(rows.iter().map(|r| r.1).collect::<Vec<_>>())
                        .with_timezone("UTC"),
                ),
                Arc::new(Int64Array::from_iter_values(0..rows.len() as i64)),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build();
        let mut w = ArrowWriter::try_new(
            File::create(&file).unwrap(),
            Arc::clone(&schema),
            Some(props),
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        for (k, (sym, ts)) in rows.iter().enumerate() {
            out.push((rel.clone(), (k / 2) as u64, (*sym).to_string(), *ts));
        }
    }
    out
}

fn symbols(path: &Path, syms: &[&str]) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "symbol",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(syms.to_vec())) as ArrayRef],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn json(s: &str) -> Value {
    serde_json::from_str(s.trim()).unwrap()
}

fn planned(v: &Value, root: &Path) -> BTreeSet<(String, u64)> {
    let base = std::fs::canonicalize(root).unwrap();
    v["evidence"]["record"]["body"]["plan"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|e| {
            let rel = common::rel_under(&base, e["file"]["path"].as_str().unwrap());
            e["row_groups"]
                .as_array()
                .unwrap()
                .iter()
                .map(move |r| (rel.clone(), r.as_u64().unwrap()))
        })
        .collect()
}

/// Row groups holding a row of `syms` with `ts` in `[lo, hi]` (µs).
fn truth(
    rows: &[(String, u64, String, i64)],
    syms: &[&str],
    lo: i64,
    hi: i64,
) -> BTreeSet<(String, u64)> {
    rows.iter()
        .filter(|r| syms.contains(&r.2.as_str()) && lo <= r.3 && r.3 <= hi)
        .map(|r| (r.0.clone(), r.1))
        .collect()
}

/// Row groups holding a row of each of `syms` in that symbol's own window
/// `[newest − n, newest]` (µs), `newest` its newest row.
fn per_key_lookback(
    rows: &[(String, u64, String, i64)],
    syms: &[&str],
    n: i64,
) -> BTreeSet<(String, u64)> {
    syms.iter()
        .flat_map(|s| {
            let newest = rows.iter().filter(|r| r.2 == *s).map(|r| r.3).max();
            newest.map_or_else(BTreeSet::new, |hi| truth(rows, &[s], hi - n, hi))
        })
        .collect()
}

#[test]
fn lookback_windows_on_a_path_template_lake() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("lake");
    let rows = lake(&root);
    let r = path(&root);
    let keys = dir.path().join("keys.parquet");
    symbols(&keys, &["NVDA", "AAPL"]);
    let k = path(&keys);

    // A bad template is a usage error; a template no file matches names
    // the files.
    let out = bw()
        .args([
            "index",
            r,
            "--path-template",
            "{day:float}.parquet",
            "--key",
            "exact:symbol",
        ])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("str, int, and date"));
    let out = bw()
        .args([
            "index",
            r,
            "--path-template",
            "{day:date}.parquet",
            "--key",
            "exact:symbol",
        ])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(4));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("5 of 5 Parquet files") && err.contains("daily/2026-03-02.parquet"),
        "{err}"
    );

    run(
        &[
            "index",
            r,
            "--path-template",
            TEMPLATE,
            "--key",
            "temporal:symbol@ts",
            "--key",
            "exact:symbol",
        ],
        0,
    );
    // The template is recorded: another one is refused.
    let out = bw()
        .args([
            "locate",
            r,
            "--keys",
            k,
            "--path-template",
            "daily/{day}.parquet",
        ])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(4));
    assert!(String::from_utf8_lossy(&out.stderr).contains("--path-template"));

    // locate --window on the path column: pruned by file name.
    let v = json(&run(
        &[
            "locate",
            r,
            "--keys",
            k,
            "--window",
            "2026-03-03..2026-03-04",
            "--ts",
            "day",
            "--json",
        ],
        0,
    ));
    assert_eq!(v["path_values"], "day");
    let day_truth = |lo: i64, hi: i64| -> BTreeSet<(String, u64)> {
        rows.iter()
            .filter(|x| {
                ["NVDA", "AAPL"].contains(&x.2.as_str()) && (lo..=hi).contains(&(x.3 / US_PER_DAY))
            })
            .map(|x| (x.0.clone(), x.1))
            .collect()
    };
    assert_eq!(planned(&v, &root), day_truth(FIRST_DAY + 1, FIRST_DAY + 2));

    // asof --at T --lookback N on ts (the temporal key's column).
    let at = (FIRST_DAY + 3) * US_PER_DAY + 1;
    let v = json(&run(
        &[
            "asof",
            r,
            "--keys",
            k,
            "--at",
            &at.to_string(),
            "--lookback",
            "2d",
            "--json",
        ],
        0,
    ));
    assert_eq!(v["lookup"]["kind"], "lookback");
    assert_eq!(v["lookup"]["lo"], at - 2 * US_PER_DAY);
    let want = truth(&rows, &["NVDA", "AAPL"], at - 2 * US_PER_DAY, at);
    assert_eq!(planned(&v, &root), want);
    assert!(!want.is_empty());

    // latest --lookback: each key's own window, ending at its own newest
    // version.
    let v = json(&run(
        &["latest", r, "--keys", k, "--lookback", "36h", "--json"],
        0,
    ));
    assert!(v["lookup"]["end"].is_null(), "no one end: {v}");
    assert_eq!(
        planned(&v, &root),
        per_key_lookback(&rows, &["NVDA", "AAPL"], 36 * 3_600_000_000)
    );

    // Calendar units only: months and trading days are usage errors.
    for bad in ["1mo", "252td", "x"] {
        let out = bw()
            .args(["latest", r, "--keys", k, "--lookback", bad])
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(2), "{bad}");
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("trading"),
            "{bad}"
        );
    }

    // why prices the same window: the current cost is the locate plan's.
    let window = format!("{}..{}", at - 2 * US_PER_DAY, at);
    let plan = json(&run(
        &[
            "locate", r, "--keys", k, "--window", &window, "--ts", "ts", "--json",
        ],
        0,
    ));
    let why = json(&run(
        &[
            "why", r, "--from", k, "--window", &window, "--ts", "ts", "--json",
        ],
        0,
    ));
    assert_eq!(why["workload"]["windows"][0]["ts_column"], "ts");
    assert_eq!(why["current"]["bytes"], plan["planned"]["bytes"]);
    assert_eq!(why["current"]["row_groups"], plan["planned"]["row_groups"]);
    let human = run(
        &["why", r, "--from", k, "--window", &window, "--ts", "ts"],
        0,
    );
    assert!(human.contains("window: rows with ts in"), "{human}");

    // The index's temporal key is on ts: `why --latest-by day` would measure
    // the lookback and the instant in days and plan them on ts, so it is a
    // usage error naming both columns and the flag to pass.
    for extra in [
        &["--lookback", "2d"][..],
        &["--at", "2026-03-05", "--lookback", "2d"],
        &[],
        &["--at", "2026-03-05"],
    ] {
        let mut args = vec!["why", r, "--from", k, "--latest-by", "day"];
        args.extend_from_slice(extra);
        let out = bw().args(&args).output().unwrap();
        assert_eq!(out.status.code(), Some(2), "{extra:?}");
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(
            err.contains("by ts (its temporal key temporal:symbol@ts), not by day")
                && err.contains("pass --latest-by ts"),
            "{extra:?}: {err}"
        );
    }
    // With --latest-by ts the current cost is what the verbs plan.
    let latest = json(&run(
        &["latest", r, "--keys", k, "--lookback", "36h", "--json"],
        0,
    ));
    let why = json(&run(
        &[
            "why",
            r,
            "--from",
            k,
            "--latest-by",
            "ts",
            "--lookback",
            "36h",
            "--json",
        ],
        0,
    ));
    assert_eq!(why["current"]["bytes"], latest["planned"]["bytes"]);
    // A window on day offers the temporal key on day beside the current one
    // on ts: candidates differ by their time column.
    let why = json(&run(
        &[
            "why",
            r,
            "--from",
            k,
            "--window",
            "2026-03-03..2026-03-04",
            "--ts",
            "day",
            "--json",
        ],
        0,
    ));
    assert!(
        why["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["spec"]["kind"] == "temporal" && c["spec"]["ts_column"] == "day"),
        "{}",
        why["candidates"]
    );
}

#[test]
fn an_unindexed_why_prices_a_window_as_the_index_then_plans_it() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("lake");
    lake(&root);
    let r = path(&root);
    let keys = dir.path().join("keys.parquet");
    symbols(&keys, &["MSFT"]);
    let k = path(&keys);
    let why = json(&run(
        &[
            "why",
            r,
            "--path-template",
            TEMPLATE,
            "--from",
            k,
            "--window",
            "2026-03-03..2026-03-04",
            "--ts",
            "day",
            "--json",
        ],
        0,
    ));
    // No index: the path values alone narrow the current cost.
    assert_eq!(why["current"]["files"], 2);
    let exact = why["candidates"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["spec"]["kind"] == "exact")
        .unwrap()["proposed"]
        .clone();
    run(
        &[
            "index",
            r,
            "--path-template",
            TEMPLATE,
            "--key",
            "exact:symbol",
        ],
        0,
    );
    let plan = json(&run(
        &[
            "locate",
            r,
            "--keys",
            k,
            "--window",
            "2026-03-03..2026-03-04",
            "--json",
        ],
        0,
    ));
    assert_eq!(plan["planned"]["bytes"], exact["bytes"]);
    assert_eq!(plan["planned"]["row_groups"], exact["row_groups"]);
}

/// Ported from the Argon WP21 implementation: partition values for CSV and
/// JSON lines files. Oracle: the rows the test wrote.
#[test]
fn a_csv_lake_takes_its_day_from_the_file_name() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("lake");
    // (file, symbol) of every row; one chunk (row group 0) per small file.
    let mut rows: Vec<(String, String)> = Vec::new();
    for i in 0..5 {
        let name = format!("daily/2026-03-{:02}.csv", 2 + i);
        let mut text = String::from("symbol,close\n");
        for sym in ["AAPL", "MSFT"].iter().take(if i % 2 == 0 { 2 } else { 1 }) {
            text.push_str(&[*sym, ",", &i.to_string(), "\n"].concat());
            rows.push((name.clone(), (*sym).to_string()));
        }
        std::fs::create_dir_all(root.join("daily")).unwrap();
        std::fs::write(root.join(&name), text).unwrap();
    }
    let r = path(&root);
    let keys = dir.path().join("keys.parquet");
    symbols(&keys, &["MSFT"]);
    let k = path(&keys);
    let t = "daily/{day:date}.csv";
    run(
        &[
            "index",
            r,
            "--format",
            "csv",
            "--path-template",
            t,
            "--key",
            "exact:day",
            "--key",
            "exact:symbol",
        ],
        0,
    );
    let v = json(&run(
        &[
            "locate",
            r,
            "--format",
            "csv",
            "--keys",
            k,
            "--window",
            "2026-03-03..2026-03-04",
            "--ts",
            "day",
            "--json",
        ],
        0,
    ));
    assert_eq!(v["path_values"], "day");
    let want: BTreeSet<(String, u64)> = rows
        .iter()
        .filter(|(f, s)| s == "MSFT" && (f.contains("03-03") || f.contains("03-04")))
        .map(|(f, _)| (f.clone(), 0))
        .collect();
    assert_eq!(planned(&v, &root), want);
    assert_eq!(want.len(), 1, "MSFT trades on even sessions only");
    // A CSV file the template does not describe is named.
    std::fs::write(root.join("daily/notes.csv"), "symbol,close\nX,1\n").unwrap();
    let out = bw()
        .args(["index", r, "--format", "csv", "--key", "exact:symbol"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(4));
    assert!(String::from_utf8_lossy(&out.stderr).contains("daily/notes.csv"));
}

/// `latest --lookback` without a temporal key gives each key its own window
/// ending at its own newest time, from the per-key bounds sidecar (never a
/// row group's or the dataset's maximum).
#[test]
fn latest_lookback_through_per_key_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("lake");
    let rows = lake(&root);
    let r = path(&root);
    let keys = dir.path().join("keys.parquet");
    // NVDA's history ends on the second session; MSFT's runs to the fifth,
    // so one window for both would leave NVDA without a row.
    symbols(&keys, &["NVDA", "MSFT"]);
    let k = path(&keys);
    run(
        &[
            "index",
            r,
            "--path-template",
            TEMPLATE,
            "--key",
            "exact:symbol@ts",
        ],
        0,
    );
    let v = json(&run(
        &["latest", r, "--keys", k, "--lookback", "1d", "--json"],
        0,
    ));
    assert!(v["lookup"]["end"].is_null(), "no one end: {v}");
    let want = per_key_lookback(&rows, &["NVDA", "MSFT"], US_PER_DAY);
    assert_eq!(planned(&v, &root), want);
    let nvda = per_key_lookback(&rows, &["NVDA"], US_PER_DAY);
    assert!(
        !nvda.is_empty() && nvda.is_subset(&want),
        "NVDA keeps its own window"
    );
    // `why` prices the current configuration through the same per-key
    // bounds, never as a full scan: its current cost is what `latest` and
    // `asof` plan, so a temporal key that would read the same is no gain.
    let why = |extra: &[&str]| {
        let mut args = vec!["why", r, "--from", k, "--latest-by", "ts"];
        args.extend_from_slice(extra);
        args.push("--json");
        json(&run(&args, 0))
    };
    let w = why(&["--lookback", "1d"]);
    assert_eq!(w["current"]["bytes"], v["planned"]["bytes"]);
    assert_eq!(w["current"]["row_groups"], v["planned"]["row_groups"]);
    let at = (FIRST_DAY + 3) * US_PER_DAY + 1;
    let asof = json(&run(
        &[
            "asof",
            r,
            "--keys",
            k,
            "--at",
            &at.to_string(),
            "--lookback",
            "2d",
            "--json",
        ],
        0,
    ));
    let w = why(&["--at", &at.to_string(), "--lookback", "2d"]);
    assert_eq!(w["current"]["bytes"], asof["planned"]["bytes"]);
    assert_eq!(w["current"]["row_groups"], asof["planned"]["row_groups"]);
    // The bounds measure ts: a lookback in day's unit is refused.
    let out = bw()
        .args([
            "why",
            r,
            "--from",
            k,
            "--latest-by",
            "day",
            "--lookback",
            "2d",
        ])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("by ts (its per-key bounds), not by day") && err.contains("--latest-by ts"),
        "{err}"
    );
    // Reversed windows and uncommitted sequences are usage errors.
    let out = bw()
        .args([
            "locate",
            r,
            "--keys",
            k,
            "--window",
            "2026-03-04..2026-03-03",
        ])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("--window"));
    let out = bw()
        .args(["locate", r, "--keys", k, "--at-manifest", "9"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains('1'));
}

/// Range keys on two time columns leave `asof --at --lookback` without one
/// window column: the verb and `why` both refuse and name `--ts`, and
/// following that on `why` prices exactly what `asof ... --ts` plans.
#[test]
fn why_takes_ts_for_an_asof_lookback_as_asof_does() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("lake");
    let rows = lake(&root);
    let r = path(&root);
    let keys = dir.path().join("keys.parquet");
    symbols(&keys, &["NVDA", "AAPL"]);
    let k = path(&keys);
    run(
        &[
            "index",
            r,
            "--path-template",
            TEMPLATE,
            "--key",
            "exact:symbol",
            "--key",
            "range:ts",
            "--key",
            "range:day",
        ],
        0,
    );
    let at = ((FIRST_DAY + 3) * US_PER_DAY + 1).to_string();
    let lookback = ["--at", &at, "--lookback", "2d"];
    for args in [
        [&["asof", r, "--keys", k][..], &lookback].concat(),
        [&["why", r, "--from", k, "--latest-by", "ts"][..], &lookback].concat(),
    ] {
        let out = bw().args(&args).output().unwrap();
        assert_ne!(out.status.code(), Some(0), "{args:?}");
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(
            err.contains("several time columns (day, ts); name one with --ts"),
            "{args:?}: {err}"
        );
    }
    // Following it: `why --ts ts` prices the window `asof --ts ts` plans.
    let plan = json(&run(
        &[
            &["asof", r, "--keys", k, "--ts", "ts"][..],
            &lookback,
            &["--json"],
        ]
        .concat(),
        0,
    ));
    let want = truth(
        &rows,
        &["NVDA", "AAPL"],
        (FIRST_DAY + 1) * US_PER_DAY + 1,
        (FIRST_DAY + 3) * US_PER_DAY + 1,
    );
    assert_eq!(planned(&plan, &root), want);
    let why = json(&run(
        &[
            &["why", r, "--from", k, "--latest-by", "ts", "--ts", "ts"][..],
            &lookback,
            &["--json"],
        ]
        .concat(),
        0,
    ));
    assert_eq!(why["current"]["bytes"], plan["planned"]["bytes"]);
    assert_eq!(why["current"]["row_groups"], plan["planned"]["row_groups"]);
    assert_eq!(why["workload"]["windows"][0]["ts_column"], "ts");
    // The window starts exactly at at − 2d, where AAPL has a print
    // (2026-03-03, alone with MSFT in its row group).
    let lo = (FIRST_DAY + 1) * US_PER_DAY + 1;
    assert!(rows.iter().any(|r| r.2 == "AAPL" && r.3 == lo));
    assert_eq!(why["workload"]["windows"][0]["lo"], lo);
    assert_eq!(
        why["workload"]["windows"][0]["hi"],
        (FIRST_DAY + 3) * US_PER_DAY + 1
    );
    // The temporal candidate on ts predicts what `asof --ts ts` plans once
    // it is built: AAPL's version at exactly at − 2d keeps its row group.
    let predicted = why["candidates"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["spec"]["kind"] == "temporal" && c["spec"]["ts_column"] == "ts")
        .unwrap()["proposed"]
        .clone();
    run(&["index", r, "--key", "temporal:symbol@ts"], 0);
    let plan = json(&run(
        &[
            &["asof", r, "--keys", k, "--ts", "ts"][..],
            &lookback,
            &["--json"],
        ]
        .concat(),
        0,
    ));
    assert_eq!(planned(&plan, &root), want);
    assert_eq!(predicted["bytes"], plan["planned"]["bytes"]);
    assert_eq!(predicted["row_groups"], plan["planned"]["row_groups"]);
    // --ts and --latest-by name one column; --ts needs a key file and the
    // asof lookback (or a window).
    for (args, says) in [
        (
            [
                &["why", r, "--from", k, "--latest-by", "day", "--ts", "ts"][..],
                &lookback,
            ]
            .concat(),
            "pass --latest-by ts --ts ts",
        ),
        (
            vec!["why", r, "--from", k, "--ts", "ts"],
            "with --latest-by ts --at <instant> --lookback <length>",
        ),
        (
            vec!["why", r, "--from", k, "--latest-by", "ts", "--ts", "ts"],
            "with --latest-by ts --at <instant> --lookback <length>",
        ),
    ] {
        let out = bw().args(&args).output().unwrap();
        assert_eq!(out.status.code(), Some(2), "{args:?}");
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(err.contains(says), "{args:?}: {err}");
    }
}

/// The showcase defect: `fix` on a folder never indexed built the index
/// without the partitioning it was opened with, so the manifest recorded
/// none and a later lookup with the same flag was refused (exit 4, "built
/// with partitioning none"). Through the binary, for a path template and
/// for hive directories: an unindexed `why` over a window, `fix` of its
/// recommendation, then `locate` with the same flag plans exactly what the
/// receipt's replay predicted.
#[test]
fn fix_on_an_unindexed_partitioned_folder_records_its_partitioning() {
    fn templated(d: &str) -> String {
        format!("daily/{d}.parquet")
    }
    fn hive(d: &str) -> String {
        format!("day={d}/part-0.parquet")
    }
    let template_flags = ["--path-template", TEMPLATE];
    let hive_flags = ["--partitioning", "hive"];
    for (flags, file_of) in [
        (&template_flags[..], templated as fn(&str) -> String),
        (&hive_flags[..], hive),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("lake");
        let rows = lake_at(&root, file_of);
        let r = path(&root);
        let keys = dir.path().join("keys.parquet");
        symbols(&keys, &["NVDA"]);
        let k = path(&keys);
        let with = |args: &[&str]| -> Vec<String> {
            args.iter().chain(flags).map(|s| (*s).to_string()).collect()
        };
        let call = |args: &[&str], code: i32| {
            let a = with(args);
            run(&a.iter().map(String::as_str).collect::<Vec<_>>(), code)
        };
        let window = "2026-03-02..2026-03-03";
        let why = json(&call(
            &[
                "why", r, "--from", k, "--window", window, "--ts", "day", "--json",
            ],
            0,
        ));
        // No row group without NVDA has a symbol range that spans it, so
        // the files' own min/max statistics already skip to its row groups
        // in the window and nothing is recommended; the cheapest candidate
        // is applied anyway, with --allow-no-gain.
        assert!(why["recommended"].is_null(), "{flags:?}: {why}");
        let rec = why["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .min_by_key(|c| c["proposed"]["bytes"].as_u64().unwrap())
            .unwrap();
        let proposal = rec["proposal"]["hash"].as_str().unwrap();
        let workload = why["workload"]["hash"].as_str().unwrap();
        // Per-key bounds are a key spec (M5): proposing one on the folder
        // not yet indexed writes a proposal. A sketch is refused, and the
        // index command it names carries the same source flag, so following
        // it literally builds the folder as it was read.
        let ev = why["evidence"]["hash"].as_str().unwrap();
        let written = call(
            &["propose", r, "--evidence", ev, "--key", "exact:symbol@ts"],
            0,
        );
        assert!(written.starts_with("proposal "), "{written}");
        let refused = call(
            &["propose", r, "--evidence", ev, "--key", "sketch:symbol"],
            3,
        );
        let flag = if flags[0] == "--path-template" {
            format!("--path-template '{TEMPLATE}'")
        } else {
            flags.join(" ")
        };
        let command = refused
            .split('`')
            .find(|s| s.starts_with("bellwether index "))
            .unwrap_or_else(|| panic!("{refused}"));
        assert!(
            command.ends_with(&format!(" {flag} --key sketch:symbol")),
            "{refused}"
        );
        let fix = json(&call(
            &[
                "fix",
                r,
                "--proposal",
                proposal,
                "--workload",
                workload,
                "--json",
                "--allow-no-gain",
            ],
            0,
        ));
        assert_eq!(fix["outcome"]["kind"], "applied", "{flags:?}: {fix}");
        let manifest: Value =
            serde_json::from_slice(&std::fs::read(root.join(".bellwether/manifest.json")).unwrap())
                .unwrap();
        assert!(
            manifest["source"]["partitioning"].is_string(),
            "{flags:?}: {}",
            manifest["source"]
        );
        let plan = json(&call(
            &[
                "locate", r, "--keys", k, "--window", window, "--ts", "day", "--json",
            ],
            0,
        ));
        // Exactly the row groups holding NVDA in the window's two days.
        let want: BTreeSet<(String, u64)> = rows
            .iter()
            .filter(|row| row.2 == "NVDA")
            .map(|row| (row.0.clone(), row.1))
            .collect();
        assert_eq!(planned(&plan, &root), want, "{flags:?}");
        assert_eq!(
            plan["planned"]["bytes"], rec["proposed"]["bytes"],
            "{flags:?}"
        );
    }
}

/// The index command a sketch refusal names carries every flag that
/// shapes the build or locates the index, each argument shell-quoted:
/// followed literally through a shell on a CSV folder whose root has a
/// space, with a non-default chunk size, an index elsewhere, the full
/// fingerprint policy, and a `--schema` file, it builds the manifest `fix`
/// builds with the same flags (source, schema, row-group space), and the
/// lookups with those flags find it. The refusal also comes only after
/// the checks a proposal gets: an unknown column and a reserved proposer
/// are refused as such.
#[test]
fn the_sketch_refusal_command_builds_what_fix_builds() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("my lake");
    std::fs::create_dir_all(&root).unwrap();
    for i in 0..6 {
        let mut text = String::from("symbol,ts,close\n");
        for k in 0..6 {
            let _ = writeln!(text, "S{i},2026-03-0{}T00:00:0{k}Z,{i}.{k}25", 2 + i % 3);
        }
        std::fs::write(root.join(format!("part-{i}.csv")), text).unwrap();
    }
    let schema = dir.path().join("schema s.json");
    std::fs::write(
        &schema,
        r#"[{"name": "symbol", "type": "Utf8"},
            {"name": "ts", "type": "Timestamp(Microsecond, Some(\"+00:00\"))"},
            {"name": "close", "type": "Decimal128(10, 3)"}]"#,
    )
    .unwrap();
    let index = dir.path().join("idx elsewhere");
    let keys = dir.path().join("keys.parquet");
    symbols(&keys, &["S3"]);
    let (r, k) = (path(&root), path(&keys));
    let flags = [
        "--format",
        "csv",
        "--chunk-size",
        "64",
        "--schema",
        path(&schema),
        "--fingerprint-policy",
        "full",
        "--index",
        path(&index),
    ];
    let call = |args: &[&str], code: i32| run(&[&flags[..], args].concat(), code);
    let why = json(&call(&["why", r, "--from", k, "--json"], 0));
    let ev = why["evidence"]["hash"].as_str().unwrap().to_string();

    // The checks every proposal gets come first.
    let out = bw()
        .args(
            [
                &flags[..],
                &["propose", r, "--evidence", &ev, "--key", "sketch:nosuch"],
            ]
            .concat(),
        )
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(4));
    assert!(String::from_utf8_lossy(&out.stderr).contains("nosuch"));
    let reserved = json(&call(
        &[
            "propose",
            r,
            "--evidence",
            &ev,
            "--by",
            "engine",
            "--key",
            "exact:symbol@ts",
            "--json",
        ],
        3,
    ));
    assert_eq!(reserved["refusal"]["kind"], "reserved_proposer");

    let refused = json(&call(
        &[
            "propose",
            r,
            "--evidence",
            &ev,
            "--key",
            "sketch:symbol",
            "--json",
        ],
        3,
    ));
    assert_eq!(refused["refusal"]["kind"], "sketch_not_proposable");
    let command = refused["refusal"]["command"].as_str().unwrap();
    assert!(command.starts_with("bellwether index "), "{command}");
    common::run_printed(command, 0);
    let manifest = || -> Value {
        serde_json::from_slice(&std::fs::read(index.join("manifest.json")).unwrap()).unwrap()
    };
    let built = manifest();
    // The same flags find what the command built: nothing left to do.
    let again = json(&call(&["index", r, "--key", "sketch:symbol", "--json"], 0));
    assert!(again["committed"].is_null(), "{again}");

    // The same folder through why and fix with the same flags.
    std::fs::remove_dir_all(&index).unwrap();
    let why = json(&call(&["why", r, "--from", k, "--json"], 0));
    let proposal = why["recommended"]["proposal"].as_str().unwrap();
    let workload = why["workload"]["hash"].as_str().unwrap();
    let fix = json(&call(
        &[
            "fix",
            r,
            "--proposal",
            proposal,
            "--workload",
            workload,
            "--json",
        ],
        0,
    ));
    assert_eq!(fix["outcome"]["kind"], "applied", "{fix}");
    let fixed = manifest();
    for part in ["source", "rowgroup_space"] {
        assert_eq!(built[part], fixed[part], "{part}");
    }
    assert_eq!(built["schema"]["columns"], fixed["schema"]["columns"]);
    assert_eq!(
        built["source"]["fingerprint_policy"], "full",
        "{}",
        built["source"]
    );
    assert_eq!(built["rowgroup_space"]["files"][0]["chunk_size"], 64);
}
