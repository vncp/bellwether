//! Invariant 1 for `date_trunc`, `cast`, and `substr` expr keys over files
//! in two timestamp units (ms and us, zone UTC), with days before 1970,
//! nulls, and invalid JSON, against an oracle that never calls the
//! evaluator. Adopted from the PR #20 review's probe.
#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::many_single_char_names,
    clippy::unnecessary_wraps
)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray,
};
use arrow::datatypes::{
    DataType, Field, Schema, SchemaRef, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType,
};
use bellwether_build::{Builder, Lookup, Planner};
use bellwether_core::encoding::Value;
use bellwether_core::expr::{self, Over};
use bellwether_types::KeySpec;
use common::{enc, enc_i64, enc_str, memory_store, parquet_opts, plan_rgs, write_parquet, Scratch};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

const DAY_US: i64 = 86_400_000_000;

fn tz() -> Option<Arc<str>> {
    Some("UTC".into())
}

fn dataset() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, tz()), true),
    ]))
}

fn payload(i: i64) -> Option<String> {
    match i % 11 {
        0 => None,
        1 => Some("{\"n\":".into()),
        2 => Some(format!("{{\"n\":\"{}\",\"s\":\"ÀB{}\"}}", i % 7, i % 5)),
        3 => Some("{\"n\":1.5,\"s\":null}".into()),
        _ => Some(format!("{{\"n\":{},\"s\":\"Ab{}x\"}}", i % 7, i % 5)),
    }
}

/// Before 1970 too: a negative day must floor, not truncate toward zero.
fn ts_us(i: i64) -> Option<i64> {
    if i % 13 == 0 {
        None
    } else {
        Some((i / 37 - 4) * DAY_US / 3 + (i % 5) * 1_000 - 777_000)
    }
}

fn oracle_day(t: i64) -> i64 {
    t.div_euclid(DAY_US) * DAY_US
}
fn oracle_n(doc: &str) -> Option<i64> {
    let v: serde_json::Value = serde_json::from_str(doc).ok()?;
    let txt = match v.get("n")? {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        _ => return None,
    };
    let d = txt.strip_prefix('-').unwrap_or(&txt);
    if d.is_empty() || !d.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    txt.parse().ok()
}
fn oracle_s2(doc: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(doc).ok()?;
    let s = v.get("s")?.as_str()?.to_string();
    Some(s.chars().flat_map(char::to_lowercase).take(2).collect())
}

#[test]
fn probe_invariant1_date_trunc_cast_substr_mixed_units() {
    let s = Scratch::new();
    let dir = s.path("data");
    std::fs::create_dir_all(&dir).unwrap();
    let mut paths = Vec::new();
    for (f, unit_ms) in [(0, true), (1, false), (2, true)] {
        let ids: Vec<i64> = (f * 300..f * 300 + 300).collect();
        let pay: Vec<Option<String>> = ids.iter().map(|i| payload(*i)).collect();
        let ts: ArrayRef = if unit_ms {
            Arc::new(
                TimestampMillisecondArray::from(
                    ids.iter()
                        .map(|i| ts_us(*i).map(|t| t.div_euclid(1000)))
                        .collect::<Vec<_>>(),
                )
                .with_timezone_opt(tz()),
            )
        } else {
            Arc::new(
                TimestampMicrosecondArray::from(
                    ids.iter()
                        .map(|i| ts_us(*i).map(|t| t.div_euclid(1000) * 1000))
                        .collect::<Vec<_>>(),
                )
                .with_timezone_opt(tz()),
            )
        };
        let file_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, true),
            Field::new("ts", ts.data_type().clone(), true),
        ]));
        let b = RecordBatch::try_new(
            file_schema,
            vec![
                Arc::new(Int64Array::from(ids.clone())) as ArrayRef,
                Arc::new(StringArray::from(pay)),
                ts,
            ],
        )
        .unwrap();
        let p = dir.join(format!("part-{f}.parquet"));
        write_parquet(&p, &b, 25);
        paths.push(p.to_str().unwrap().to_string());
    }
    let specs = vec![
        expr::spec("d", "date_trunc(ts,day)", Over::Exact).unwrap(),
        expr::spec(
            "n",
            "cast(json_get(payload,'$.n'),int64)",
            Over::Bloom(0.01),
        )
        .unwrap(),
        expr::spec(
            "s2",
            "substr(lower(json_get(payload,'$.s')),1,2)",
            Over::Exact,
        )
        .unwrap(),
    ];
    let store = memory_store();
    let b = Builder::new(Arc::clone(&store), "idx");
    common::build_ok(&b, dir.to_str().unwrap(), &specs, &parquet_opts(&dataset()));
    let planner = Planner::open(Arc::clone(&store), "idx").unwrap();

    // oracle from decoded files
    let mut by_day: BTreeMap<i64, BTreeSet<(String, u32)>> = BTreeMap::new();
    let mut by_n: BTreeMap<i64, BTreeSet<(String, u32)>> = BTreeMap::new();
    let mut by_s: BTreeMap<String, BTreeSet<(String, u32)>> = BTreeMap::new();
    for p in &paths {
        let n = ParquetRecordBatchReaderBuilder::try_new(File::open(p).unwrap())
            .unwrap()
            .metadata()
            .num_row_groups();
        for rg in 0..n {
            let r = ParquetRecordBatchReaderBuilder::try_new(File::open(p).unwrap())
                .unwrap()
                .with_row_groups(vec![rg])
                .build()
                .unwrap();
            for batch in r {
                let batch = batch.unwrap();
                let pay = batch.column(1).as_string::<i32>();
                let tsc = batch.column(2);
                for i in 0..batch.num_rows() {
                    let at = (p.clone(), rg as u32);
                    if !tsc.is_null(i) {
                        let t = match tsc.data_type() {
                            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                                tsc.as_primitive::<TimestampMillisecondType>().value(i) * 1000
                            }
                            _ => tsc.as_primitive::<TimestampMicrosecondType>().value(i),
                        };
                        by_day.entry(oracle_day(t)).or_default().insert(at.clone());
                    }
                    if !pay.is_null(i) {
                        if let Some(v) = oracle_n(pay.value(i)) {
                            by_n.entry(v).or_default().insert(at.clone());
                        }
                        if let Some(v) = oracle_s2(pay.value(i)) {
                            by_s.entry(v).or_default().insert(at.clone());
                        }
                    }
                }
            }
        }
    }
    assert!(by_day.keys().any(|d| *d < 0), "corpus has pre-1970 days");
    let tsdt = DataType::Timestamp(TimeUnit::Microsecond, tz());
    let mut checked = 0;
    for (day, want) in &by_day {
        let key = enc(
            &Value::Timestamp {
                value: *day,
                unit: TimeUnit::Microsecond,
            },
            &tsdt,
        );
        let plan = planner
            .plan(
                &specs,
                &Lookup::Keys {
                    columns: vec!["date_trunc(ts, 'day')".into()],
                    keys: vec![key],
                },
            )
            .unwrap();
        assert_eq!(&plan_rgs(&plan), want, "day {day}");
        checked += 1;
    }
    for (n, want) in &by_n {
        let plan = planner
            .plan(
                &specs,
                &Lookup::Keys {
                    columns: vec!["CAST(json_get(payload,'$[\"n\"]'), int64)".into()],
                    keys: vec![enc_i64(*n)],
                },
            )
            .unwrap();
        let got = plan_rgs(&plan);
        assert!(
            want.is_subset(&got),
            "n {n}: missed {:?}",
            want.difference(&got).collect::<Vec<_>>()
        );
        checked += 1;
    }
    for (s2, want) in &by_s {
        let plan = planner
            .plan(
                &specs,
                &Lookup::Keys {
                    columns: vec!["substr(lower(json_get(payload,'$.s')),1,2)".into()],
                    keys: vec![enc_str(s2)],
                },
            )
            .unwrap();
        assert_eq!(&plan_rgs(&plan), want, "s2 {s2:?}");
        checked += 1;
    }
    println!(
        "PROBE invariant-1 lookups checked: {checked} (days {}, n {}, s2 {})",
        by_day.len(),
        by_n.len(),
        by_s.len()
    );
    let _ = KeySpec::Exact { columns: vec![] };
}
