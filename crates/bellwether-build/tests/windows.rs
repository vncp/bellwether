//! Owner strategy requests 1 and 2: a lake of one file per session whose
//! date is only in the file name (`daily/{day:date}.parquet`), and time
//! windows planned through every source that can bound them: the path
//! values, a temporal spec's version chains, a range key, and the key specs.
//! Ground truth: rows decoded with the `parquet` crate, each given the day
//! its file name spells (converted by Arrow's own cast, not by the code
//! under test); the expected plans are computed from them, never from the
//! index.

#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::too_many_lines
)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::TimestampMicrosecondArray;
use arrow::array::{Array, Date32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_build::{BuildOptions, Lookup, SourceKind};
use bellwether_core::encoding::Value;
use bellwether_source::{Partitioning, PathTemplate};
use common::*;

const TEMPLATE: &str = "daily/{day:date}.parquet";
const DAYS: [&str; 5] = [
    "2026-03-02",
    "2026-03-03",
    "2026-03-04",
    "2026-03-05",
    "2026-03-06",
];
const US_PER_DAY: i64 = 86_400_000_000;

fn template(t: &str) -> Partitioning {
    Partitioning::Template(PathTemplate::parse(t).unwrap())
}

fn opts(t: &str) -> BuildOptions {
    BuildOptions {
        partitioning: Some(template(t)),
        ..BuildOptions::new(SourceKind::Parquet)
    }
}

/// The columns stored in every file (no `day`: it is only in the name).
fn stored() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("close", DataType::Int64, false),
    ]))
}

/// Days since the epoch of `YYYY-MM-DD`, by Arrow's cast.
fn day_number(d: &str) -> i64 {
    let a = arrow::compute::cast(&StringArray::from(vec![d]), &DataType::Date32).unwrap();
    i64::from(a.as_any().downcast_ref::<Date32Array>().unwrap().value(0))
}

/// One file per session: AAPL every day (straddling row groups), MSFT
/// every day, NVDA on even sessions only, a late AAPL print from the day
/// before, an AAPL print with no time, and a row with no symbol.
fn corpus(s: &Scratch) -> BTreeMap<String, i64> {
    let mut days = BTreeMap::new();
    for (i, d) in DAYS.iter().enumerate() {
        let day = day_number(d);
        let base = day * US_PER_DAY;
        let mut rows: Vec<(Option<&str>, Option<i64>)> = vec![
            (Some("AAPL"), Some(base - 3_600_000_000)),
            (Some("AAPL"), Some(base + 1)),
            (Some("AAPL"), Some(base + 2)),
            (Some("AAPL"), None),
            (Some("MSFT"), Some(base + 5)),
            (Some("MSFT"), Some(base + 6)),
            (None, Some(base + 7)),
        ];
        if i % 2 == 0 {
            rows.push((Some("NVDA"), Some(base + 8)));
        }
        let batch = RecordBatch::try_new(
            stored(),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(
                    TimestampMicrosecondArray::from(rows.iter().map(|r| r.1).collect::<Vec<_>>())
                        .with_timezone("UTC"),
                ),
                Arc::new(Int64Array::from_iter_values(0..rows.len() as i64)),
            ],
        )
        .unwrap();
        let name = format!("daily/{d}.parquet");
        write_parquet(&s.path(&name), &batch, 2);
        days.insert(s.path_str(&name), day);
    }
    days
}

/// Every row, each with the `day` its path spells, under the dataset types.
fn rows(days: &BTreeMap<String, i64>) -> Vec<Row> {
    let mut out = Vec::new();
    for (path, &day) in days {
        for mut r in parquet_rows(std::path::Path::new(path), &stored()) {
            r.cols.insert(
                "day".into(),
                Some(enc(
                    &Value::Date32(i32::try_from(day).unwrap()),
                    &DataType::Date32,
                )),
            );
            out.push(r);
        }
    }
    out
}

fn ts_us(r: &Row) -> Option<i64> {
    let bytes = r.get("ts")?;
    let tail: [u8; 8] = bytes[bytes.len() - 8..].try_into().unwrap();
    Some((u64::from_be_bytes(tail) ^ (1 << 63)) as i64)
}

fn window(column: &str, keys: &[&str], lo: i64, hi: i64) -> Lookup {
    Lookup::Window {
        columns: cols(&["symbol"]),
        keys: keys.iter().map(|k| enc_str(k)).collect(),
        ts_column: column.into(),
        lo,
        hi,
    }
}

/// The rows of `keys` whose `column` lies in `[lo, hi]`.
fn truth(
    all: &[Row],
    days: &BTreeMap<String, i64>,
    column: &str,
    keys: &[&str],
    lo: i64,
    hi: i64,
) -> Vec<Row> {
    let keys: BTreeSet<Vec<u8>> = keys.iter().map(|k| enc_str(k)).collect();
    all.iter()
        .filter(|r| {
            let t = match column {
                "day" => Some(days[&r.path]),
                _ => ts_us(r),
            };
            r.get("symbol").is_some_and(|k| keys.contains(k))
                && t.is_some_and(|t| lo <= t && t <= hi)
        })
        .cloned()
        .collect()
}

fn rgs(rows: &[Row]) -> BTreeSet<(String, u32)> {
    rows.iter().map(|r| (r.path.clone(), r.rg)).collect()
}

const KEY_SETS: [&[&str]; 4] = [&["AAPL"], &["NVDA"], &["AAPL", "MSFT", "NVDA"], &["ZZZZ"]];

#[test]
fn a_day_window_is_pruned_by_path_values_with_no_rewrite() {
    let s = Scratch::new();
    let days = corpus(&s);
    let all = rows(&days);
    let store = memory_store();
    builder(&store)
        .build(&s.root_str(), &[exact(&["symbol"])], &opts(TEMPLATE))
        .unwrap();
    let p = planner(&store);
    assert_eq!(
        p.manifest().source.partitioning.as_deref(),
        Some("template:daily/{day:date}.parquet")
    );
    assert_eq!(
        p.column_types(&cols(&["day"])).unwrap()[0].1,
        DataType::Date32,
        "typed by the placeholder"
    );
    let first = day_number(DAYS[0]);
    let last = day_number(DAYS[4]);
    let points = [
        first - 400,
        first - 1,
        first,
        first + 1,
        first + 2,
        last,
        last + 1,
    ];
    let live: BTreeSet<(String, u32)> = rgs(&all);
    let mut pruned = 0;
    for &lo in &points {
        for &hi in &points {
            for keys in KEY_SETS {
                let l = window("day", keys, lo, hi);
                assert_eq!(p.path_column_for(&l).unwrap(), Some("day".into()));
                let want = truth(&all, &days, "day", keys, lo, hi);
                // Exact key plus exact path values: exactly the row groups
                // holding a key in a file of the window.
                let plan = p.plan(&[exact(&["symbol"])], &l).unwrap();
                assert_eq!(plan_rgs(&plan), rgs(&want), "{keys:?} [{lo}, {hi}]");
                let got =
                    apply_residual(plan.residual.as_ref().unwrap(), planned_rows(&plan, &all));
                assert_eq!(sorted(got), sorted(want.clone()), "{keys:?} [{lo}, {hi}]");
                // No spec at all: every live row group of the files in the
                // window, still never a missed row.
                let raw = p.plan(&[], &l).unwrap();
                let files: BTreeSet<&String> = days
                    .iter()
                    .filter(|(_, d)| lo <= hi && lo <= **d && **d <= hi)
                    .map(|(f, _)| f)
                    .collect();
                let expect: BTreeSet<(String, u32)> = live
                    .iter()
                    .filter(|(f, _)| files.contains(f))
                    .cloned()
                    .collect();
                assert_eq!(plan_rgs(&raw), expect, "{keys:?} [{lo}, {hi}] raw");
                let got = apply_residual(raw.residual.as_ref().unwrap(), planned_rows(&raw, &all));
                assert_eq!(sorted(got), sorted(want), "{keys:?} [{lo}, {hi}] raw");
                pruned += live.len() - plan_rgs(&raw).len();
            }
        }
    }
    assert!(pruned > 0, "path values pruned nothing");
}

#[test]
fn every_source_that_bounds_a_time_window_keeps_every_matching_row_group() {
    let s = Scratch::new();
    let days = corpus(&s);
    let all = rows(&days);
    let configs: [&[bellwether_types::KeySpec]; 4] = [
        &[temporal(&["symbol"], "ts")],
        &[exact(&["symbol"]), range("ts")],
        &[exact(&["symbol"])],
        &[range("ts")],
    ];
    let store = memory_store();
    let mut every: Vec<bellwether_types::KeySpec> = Vec::new();
    for spec in configs.iter().flat_map(|c| c.iter()) {
        if !every.contains(spec) {
            every.push(spec.clone());
        }
    }
    builder(&store)
        .build(&s.root_str(), &every, &opts(TEMPLATE))
        .unwrap();
    let p = planner(&store);
    let first = day_number(DAYS[0]) * US_PER_DAY;
    let points = [
        i64::MIN + 1,
        first - 3_600_000_001,
        first - 3_600_000_000,
        first,
        first + 1,
        first + 2,
        first + US_PER_DAY - 3_600_000_000,
        first + US_PER_DAY + 6,
        first + 4 * US_PER_DAY + 8,
        i64::MAX,
    ];
    let mut chains_pruned = 0;
    for &lo in &points {
        for &hi in &points {
            for keys in KEY_SETS {
                let l = window("ts", keys, lo, hi);
                assert_eq!(p.path_column_for(&l).unwrap(), None, "ts is in the files");
                let want = truth(&all, &days, "ts", keys, lo, hi);
                let mut plans = Vec::new();
                for config in configs {
                    let plan = p.plan(config, &l).unwrap();
                    // Invariant 1: no row group holding a matching row is
                    // missed, and the residual is row-exact.
                    assert!(
                        rgs(&want).is_subset(&plan_rgs(&plan)),
                        "{config:?} {keys:?} [{lo}, {hi}]"
                    );
                    let got =
                        apply_residual(plan.residual.as_ref().unwrap(), planned_rows(&plan, &all));
                    assert_eq!(
                        sorted(got),
                        sorted(want.clone()),
                        "{config:?} {keys:?} [{lo}, {hi}]"
                    );
                    plans.push(plan_rgs(&plan));
                }
                // The version chains are exact per key: exactly the row
                // groups where a key has a non-null time in the window.
                assert_eq!(plans[0], rgs(&want), "chains {keys:?} [{lo}, {hi}]");
                chains_pruned += plans[2].len() - plans[0].len();
            }
        }
    }
    assert!(chains_pruned > 0, "the chains pruned nothing the key kept");
}

/// `latest --lookback`: each key's own window `[newest_k − n, newest_k]`,
/// `newest_k` its newest non-null time anywhere, read through a temporal
/// spec's version chains or, with none, through per-key bounds. Both plans
/// keep every row group holding a row of a window, keep exactly those with
/// the chains, and their residuals return exactly the windows' rows.
#[test]
fn a_lookback_is_per_key_through_chains_or_bounds() {
    let s = Scratch::new();
    let days = corpus(&s);
    let all = rows(&days);
    let store = memory_store();
    // `exact:symbol@ts`: the exact spec carries per-key bounds.
    let specs = [
        temporal(&["symbol"], "ts"),
        bellwether_types::KeySpec::Exact {
            columns: cols(&["symbol"]),
            counts: false,
            bounds: Some("ts".into()),
        },
    ];
    builder(&store)
        .build(&s.root_str(), &specs, &opts(TEMPLATE))
        .unwrap();
    let p = planner(&store);
    let symbol = cols(&["symbol"]);
    assert_eq!(
        p.lookback_column(&specs, &symbol).unwrap(),
        Some("ts".into())
    );
    assert_eq!(
        p.lookback_column(&specs[1..], &symbol).unwrap(),
        Some("ts".into()),
        "per-key bounds"
    );
    let mut newest: BTreeMap<Vec<u8>, i64> = BTreeMap::new();
    for r in &all {
        if let (Some(k), Some(t)) = (r.get("symbol"), ts_us(r)) {
            let e = newest.entry(k.clone()).or_insert(t);
            *e = (*e).max(t);
        }
    }
    let live = rgs(&all);
    let mut bounds_pruned = 0;
    for n in [0, 1, 4, US_PER_DAY, 2 * US_PER_DAY + 3, i64::MAX] {
        for keys in KEY_SETS {
            let encoded: Vec<Vec<u8>> = keys.iter().map(|k| enc_str(k)).collect();
            let want: Vec<Row> = all
                .iter()
                .filter(|r| {
                    let (Some(k), Some(t)) = (r.get("symbol"), ts_us(r)) else {
                        return false;
                    };
                    encoded.iter().any(|e| e.as_slice() == k) && newest[k].saturating_sub(n) <= t
                })
                .cloned()
                .collect();
            let l = Lookup::Lookback {
                columns: symbol.clone(),
                keys: encoded.clone(),
                lookback: n,
            };
            let chains = p.plan(&specs, &l).unwrap();
            assert_eq!(plan_rgs(&chains), rgs(&want), "chains {keys:?} {n}");
            let by_bounds = p.plan(&specs[1..], &l).unwrap();
            assert!(
                rgs(&want).is_subset(&plan_rgs(&by_bounds)),
                "bounds {keys:?} {n}"
            );
            for plan in [&chains, &by_bounds] {
                let got = apply_residual(plan.residual.as_ref().unwrap(), planned_rows(plan, &all));
                assert_eq!(sorted(got), sorted(want.clone()), "{keys:?} {n}");
            }
            bounds_pruned += live.len() - plan_rgs(&by_bounds).len();
        }
    }
    assert!(bounds_pruned > 0, "per-key bounds pruned nothing");
    // With neither a temporal spec nor bounds there is no newest time.
    assert!(p
        .plan(
            &[exact(&["symbol"])],
            &Lookup::Lookback {
                columns: cols(&["close"]),
                keys: vec![],
                lookback: 1,
            }
        )
        .is_err());
    assert!(p
        .plan(
            &specs,
            &Lookup::Lookback {
                columns: symbol,
                keys: vec![enc_str("AAPL")],
                lookback: -1,
            }
        )
        .is_err());
}

#[test]
fn a_changed_template_and_files_that_do_not_match_are_refused() {
    let s = Scratch::new();
    corpus(&s);
    let store = memory_store();
    builder(&store)
        .build(&s.root_str(), &[exact(&["symbol"])], &opts(TEMPLATE))
        .unwrap();
    let e = builder(&store)
        .build(
            &s.root_str(),
            &[exact(&["symbol"])],
            &opts("daily/{day}.parquet"),
        )
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("path template \"daily/{day:date}.parquet\"")
            && e.contains("path template \"daily/{day}.parquet\""),
        "{e}"
    );
    // Twelve files the template does not describe: all counted, ten named,
    // none skipped.
    std::fs::create_dir_all(s.path("other")).unwrap();
    for i in 0..12 {
        std::fs::copy(
            s.path(&format!("daily/{}.parquet", DAYS[0])),
            s.path(&format!("other/x{i:02}.parquet")),
        )
        .unwrap();
    }
    let e = builder(&store)
        .build(
            &s.root_str(),
            &[exact(&["symbol"])],
            &BuildOptions::new(SourceKind::Parquet),
        )
        .unwrap_err()
        .to_string();
    assert!(e.contains("12 of 17 Parquet files"), "{e}");
    assert!(e.contains("other/x09.parquet and 2 more"), "{e}");
    assert!(!e.contains("x10"), "{e}");
}
