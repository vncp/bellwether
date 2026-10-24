//! WP21 item 1 through the builder and the planner: hive partition columns
//! (`day=YYYY-MM-DD/symbol=X/`) are recorded in the manifest's schema,
//! indexed like any column, and pruned on. Ground truth: rows decoded with
//! the `parquet` crate plus the partition values the test itself wrote into
//! the paths.

#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]

mod common;

use std::sync::Arc;

use arrow::array::{Float64Array, RecordBatch, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use bellwether_build::{BuildOptions, Lookup, SourceKind};
use bellwether_core::encoding::Value;
use bellwether_source::partition::parse_date;
use bellwether_source::Partitioning;
use common::*;

const DAYS: [&str; 3] = ["2026-01-02", "2026-01-05", "2026-01-06"];
const SYMBOLS: [&str; 3] = ["AAPL", "MSFT", "NVDA"];

fn write_layout(s: &Scratch) -> Vec<(String, String, String)> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("price", DataType::Float64, false),
    ]));
    let mut out = Vec::new();
    for (d, day) in DAYS.iter().enumerate() {
        for (k, sym) in SYMBOLS.iter().enumerate() {
            let rel = format!("day={day}/symbol={sym}/part-0.parquet");
            let base = i64::from(parse_date(day).unwrap()) * 86_400_000_000;
            let ts: Vec<i64> = (0..8).map(|i| base + i * 60_000_000).collect();
            let price: Vec<f64> = (0..8).map(|i| (d * 100 + k * 10 + i) as f64).collect();
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
                    Arc::new(Float64Array::from(price)),
                ],
            )
            .unwrap();
            write_parquet(&s.path(&rel), &batch, 4);
            out.push((s.path_str(&rel), (*day).to_string(), (*sym).to_string()));
        }
    }
    out
}

/// Every row, decoded, with the partition columns the test wrote into the
/// paths added under the dataset types.
fn truth(files: &[(String, String, String)], schema: &Schema) -> Vec<Row> {
    let mut rows = Vec::new();
    for (path, day, sym) in files {
        for mut r in parquet_rows(std::path::Path::new(path), schema) {
            r.cols.insert(
                "day".into(),
                Some(enc(
                    &Value::Date32(parse_date(day).unwrap()),
                    &DataType::Date32,
                )),
            );
            r.cols.insert("symbol".into(), Some(enc_str(sym)));
            rows.push(r);
        }
    }
    rows
}

fn hive() -> BuildOptions {
    BuildOptions {
        partitioning: Some(Partitioning::Hive),
        ..BuildOptions::new(SourceKind::Parquet)
    }
}

#[test]
fn partition_columns_are_recorded_indexed_and_pruned_on() {
    let s = Scratch::new();
    let files = write_layout(&s);
    let store = memory_store();
    let specs = [range("day"), exact(&["symbol"])];
    builder(&store)
        .build(&s.root_str(), &specs, &hive())
        .unwrap();
    let p = planner(&store);
    let m = p.manifest();
    assert_eq!(m.source.partitioning.as_deref(), Some("hive"));
    let names: Vec<&str> = m.schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["ts", "price", "day", "symbol"]);
    assert_eq!(
        p.schema().field_with_name("day").unwrap().data_type(),
        &DataType::Date32
    );
    let schema = Arc::clone(p.schema());
    let rows = truth(&files, &schema);
    assert_eq!(rows.len(), DAYS.len() * SYMBOLS.len() * 8);

    // A window on the partition column plans exactly the day's row groups,
    // pruned through the range spec built from the virtual column.
    for day in DAYS {
        let v = p
            .encode_tuple(&cols(&["day"]), &[Value::Date32(parse_date(day).unwrap())])
            .unwrap();
        let plan = p
            .plan(
                &specs,
                &Lookup::Between {
                    column: "day".into(),
                    lo: v.clone(),
                    hi: v.clone(),
                },
            )
            .unwrap();
        let want = holding(&rows, |r| r.get("day") == Some(&v));
        assert_eq!(plan_rgs(&plan), want, "{day}");
        assert_eq!(plan.row_groups_selected, 2 * SYMBOLS.len() as u64);
        let got = sorted(apply_residual(
            plan.residual.as_ref().unwrap(),
            planned_rows(&plan, &rows),
        ));
        let want_rows = sorted(
            rows.iter()
                .filter(|r| r.get("day") == Some(&v))
                .cloned()
                .collect(),
        );
        assert_eq!(got, want_rows);
    }

    // An exact key on the symbol directory equals the oracle.
    for sym in SYMBOLS {
        let plan = check_keys(&p, &rows, &specs, &cols(&["symbol"]), &[enc_str(sym)]);
        assert_eq!(plan.row_groups_selected, 2 * DAYS.len() as u64, "{sym}");
    }

    // A later build without the option keeps the recorded scheme and does
    // nothing; asking for none on a hive index is refused.
    let again = builder(&store)
        .build(
            &s.root_str(),
            &specs,
            &BuildOptions::new(SourceKind::Parquet),
        )
        .unwrap();
    assert_eq!(again.sequence, None);
    let none = BuildOptions {
        partitioning: Some(Partitioning::None),
        ..BuildOptions::new(SourceKind::Parquet)
    };
    let err = builder(&store)
        .build(&s.root_str(), &specs, &none)
        .unwrap_err();
    assert!(err.to_string().contains("partitioning"), "{err}");
}

#[test]
fn an_index_built_without_partitioning_refuses_to_switch() {
    let s = Scratch::new();
    write_layout(&s);
    let store = memory_store();
    builder(&store)
        .build(
            &s.root_str(),
            &[range("ts")],
            &BuildOptions::new(SourceKind::Parquet),
        )
        .unwrap();
    let p = planner(&store);
    assert_eq!(p.manifest().source.partitioning, None);
    assert!(p.schema().field_with_name("day").is_err());
    let err = builder(&store)
        .build(&s.root_str(), &[range("ts")], &hive())
        .unwrap_err();
    assert!(err.to_string().contains("partitioning"), "{err}");
}

#[test]
fn hive_partitioning_is_refused_for_text_sources() {
    let s = Scratch::new();
    std::fs::create_dir_all(s.path("day=2026-01-02")).unwrap();
    std::fs::write(s.path("day=2026-01-02/a.csv"), "id\n1\n").unwrap();
    let opts = BuildOptions {
        partitioning: Some(Partitioning::Hive),
        ..BuildOptions::new(SourceKind::csv())
    };
    let err = builder(&memory_store())
        .build(&s.root_str(), &[exact(&["id"])], &opts)
        .unwrap_err();
    assert!(err.to_string().contains("Parquet sources only"), "{err}");
}
