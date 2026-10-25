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
        rows.push(("MSFT", day * US_PER_DAY + 4));
        let date = format!("2026-03-{:02}", 2 + i);
        let file = root.join(format!("daily/{date}.parquet"));
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
            out.push((
                format!("daily/{date}.parquet"),
                (k / 2) as u64,
                (*sym).to_string(),
                *ts,
            ));
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
    let base = format!(
        "{}/",
        std::fs::canonicalize(root).unwrap().to_str().unwrap()
    );
    v["evidence"]["record"]["body"]["plan"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|e| {
            let rel = e["file"]["path"]
                .as_str()
                .unwrap()
                .strip_prefix(&base)
                .unwrap()
                .to_string();
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

    // latest --lookback: one window ending at the keys' newest version.
    let v = json(&run(
        &["latest", r, "--keys", k, "--lookback", "36h", "--json"],
        0,
    ));
    let newest = rows
        .iter()
        .filter(|x| ["NVDA", "AAPL"].contains(&x.2.as_str()))
        .map(|x| x.3)
        .max()
        .unwrap();
    assert_eq!(v["lookup"]["end"], newest);
    assert_eq!(
        planned(&v, &root),
        truth(
            &rows,
            &["NVDA", "AAPL"],
            newest - 36 * 3_600_000_000,
            newest
        )
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
