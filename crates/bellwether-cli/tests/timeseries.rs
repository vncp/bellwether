//! WP21 end to end through the binary on the `market` corpus: hive
//! partitions, window lookups with per-key bounds, and bitemporal as-of,
//! each plan checked against the corpus's ground truth (`truth.json`, which
//! the bench crate's own test checks against the decoded files).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

mod common;

use std::collections::BTreeSet;
use std::path::Path;

use common::{path, run};
use serde_json::Value;

fn json(s: &str) -> Value {
    serde_json::from_str(s.trim()).unwrap()
}

/// `(path relative to the corpus dir, row group)` of a lookup's plan.
fn planned(v: &Value, out: &Path) -> BTreeSet<(String, u64)> {
    let base = std::fs::canonicalize(out).unwrap();
    let base = format!("{}/", base.to_str().unwrap());
    v["evidence"]["record"]["body"]["plan"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|e| {
            let p = e["file"]["path"].as_str().unwrap();
            let rel = p.strip_prefix(&base).unwrap().to_string();
            e["row_groups"]
                .as_array()
                .unwrap()
                .iter()
                .map(move |r| (rel.clone(), r.as_u64().unwrap()))
        })
        .collect()
}

fn rgs(list: &Value) -> BTreeSet<(String, u64)> {
    list.as_array()
        .unwrap()
        .iter()
        .map(|x| {
            (
                x["file"].as_str().unwrap().to_string(),
                x["row_group"].as_u64().unwrap(),
            )
        })
        .collect()
}

#[test]
fn the_market_workload_plans_match_the_ground_truth() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("market");
    run(
        &[
            "bench",
            "market",
            "--out",
            path(&out),
            "--symbols",
            "12",
            "--days",
            "6",
            "--files-per-day",
            "2",
            "--rows-per-row-group",
            "100",
            "--prints-per-symbol-per-day",
            "40",
        ],
        0,
    );
    let data = out.join("data");
    let indexed = json(&run(
        &[
            "index",
            path(&data),
            "--partitioning",
            "hive",
            "--json",
            "--key",
            "exact:symbol@ts",
            "--key",
            "range:ts",
            "--key",
            "range:day",
            "--key",
            "temporal2:symbol@ts,known_at",
        ],
        0,
    ));
    // The partition column is in the dataset schema, typed by inference.
    assert!(indexed["sidecars"]
        .as_array()
        .unwrap()
        .iter()
        .all(|s| s["complete_at"] == 1));
    let workload: Value =
        serde_json::from_slice(&std::fs::read(out.join("workload/market.json")).unwrap()).unwrap();
    let truth: Value =
        serde_json::from_slice(&std::fs::read(out.join("workload/truth.json")).unwrap()).unwrap();
    let mut checked = [0usize; 3];
    for (q, t) in workload["queries"]
        .as_array()
        .unwrap()
        .iter()
        .zip(truth["queries"].as_array().unwrap())
    {
        assert_eq!(q["id"], t["id"]);
        let keys = out.join(q["keys_file"].as_str().unwrap());
        match q["kind"].as_str().unwrap() {
            "window" => {
                let v = json(&run(
                    &[
                        "locate",
                        path(&data),
                        "--keys",
                        path(&keys),
                        "--json",
                        "--from",
                        q["from_us"].to_string().as_str(),
                        "--to",
                        q["to_us"].to_string().as_str(),
                    ],
                    0,
                ));
                assert_eq!(v["lookup"]["ts_column"], "ts");
                assert_eq!(v["sidecars"][0], "bounds:symbol@ts");
                let got = planned(&v, &out);
                let w = &t["window"];
                // Never misses a matching row group (invariant 1), never
                // reads one the key spec or the range key would skip.
                assert!(
                    rgs(&w["matching_row_groups"]).is_subset(&got),
                    "{}",
                    q["id"]
                );
                assert!(got.is_subset(&rgs(&w["symbol_row_groups"])), "{}", q["id"]);
                assert!(
                    got.is_subset(&rgs(&w["ts_overlap_row_groups"])),
                    "{}",
                    q["id"]
                );
                checked[0] += 1;
            }
            kind => {
                let known = q["known_us"].to_string();
                let valid = q["valid_us"].to_string();
                let args: Vec<&str> = if kind == "asof" {
                    vec!["asof", "--valid", &valid, "--known", &known]
                } else {
                    vec!["latest", "--known", &known]
                };
                let mut all = args.clone();
                all.extend(["--keys", path(&keys), "--json", path(&data)]);
                let v = json(&run(&all, 0));
                assert_eq!(v["lookup"]["kind"], "bitemporal");
                let want: BTreeSet<(String, u64)> = t["winners"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .flat_map(|w| rgs(&w["row_groups"]))
                    .collect();
                // Exactly the row groups holding each symbol's winner.
                assert_eq!(planned(&v, &out), want, "{}", q["id"]);
                checked[if kind == "asof" { 1 } else { 2 }] += 1;
            }
        }
    }
    assert!(checked.iter().all(|c| *c > 0), "{checked:?}");

    // A window over the partition column plans exactly that day's files,
    // without reading any data page (the value comes from the path).
    let day = q0_day(&workload);
    let keys = out.join(workload["queries"][0]["keys_file"].as_str().unwrap());
    let v = json(&run(
        &[
            "locate",
            path(&data),
            "--keys",
            path(&keys),
            "--json",
            "--ts",
            "day",
            "--from",
            &day,
            "--to",
            &day,
        ],
        0,
    ));
    assert!(planned(&v, &out)
        .iter()
        .all(|(f, _)| f.starts_with(&format!("data/day={day}/"))));
}

fn q0_day(workload: &Value) -> String {
    workload["queries"][0]["from"].as_str().unwrap()[..10].to_string()
}

#[test]
fn malformed_time_series_arguments_are_refused_with_what_to_do() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("d");
    common::corpus(&data, 2, 2, 10, 5);
    let keys = dir.path().join("k.parquet");
    common::keys(&keys, "id", &[1]);
    run(&["index", path(&data), "--key", "exact:id"], 0);
    // A window with no time column to use.
    let out = common::bw()
        .args([
            "locate",
            path(&data),
            "--keys",
            path(&keys),
            "--from",
            "0",
            "--to",
            "5",
        ])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(4), "an engine refusal");
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(err.contains("--ts") && err.contains("exact:id@"), "{err}");
    // A bitemporal lookup without a temporal2 key.
    let out = common::bw()
        .args(["latest", path(&data), "--keys", path(&keys), "--known", "5"])
        .output()
        .unwrap();
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(err.contains("temporal2:id@"), "{err}");
    // asof needs an instant; --at and --valid do not mix.
    run(&["asof", path(&data), "--keys", path(&keys)], 2);
    run(
        &[
            "asof",
            path(&data),
            "--keys",
            path(&keys),
            "--at",
            "1",
            "--valid",
            "1",
        ],
        2,
    );
    // A malformed declaration is a usage error naming the form.
    let out = common::bw()
        .args(["index", path(&data), "--key", "temporal2:id@valid"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    // --at-manifest refuses a write.
    let out = common::bw()
        .args(["index", path(&data), "--key", "exact:id"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0));
    run(
        &[
            "locate",
            path(&data),
            "--keys",
            path(&keys),
            "--at-manifest",
            "1",
        ],
        0,
    );
    let out = common::bw()
        .args([
            "locate",
            path(&data),
            "--keys",
            path(&keys),
            "--at-manifest",
            "7",
        ])
        .output()
        .unwrap();
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(
        err.contains("--at-manifest") && err.contains("1..=1"),
        "{err}"
    );
}
