//! WP21 item 2: plans ordered by a timestamp range key's row-group
//! minimum. Ground truth: every planned row group decoded with the `parquet`
//! crate, its minimum timestamp computed from the rows themselves.

#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::similar_names
)]

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_build::{BuildOptions, Lookup, PlanOrder, SourceKind};
use bellwether_types::{KeySpec, ScanPlan};
use common::*;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]))
}

/// Writes rows `(id, ts)` to `name`, two rows per row group.
fn write(s: &Scratch, name: &str, rows: &[(i64, Option<i64>)]) {
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(
                TimestampMicrosecondArray::from(rows.iter().map(|r| r.1).collect::<Vec<_>>())
                    .with_timezone("UTC"),
            ),
        ],
    )
    .unwrap();
    write_parquet(&s.path(name), &batch, 2);
}

/// The minimum non-null `ts` of each (file, row group), from decoded rows.
fn decoded_mins(s: &Scratch, names: &[&str]) -> BTreeMap<(String, u32), Option<Vec<u8>>> {
    let mut out: BTreeMap<(String, u32), Option<Vec<u8>>> = BTreeMap::new();
    for name in names {
        for r in parquet_rows(&s.path(name), &schema()) {
            let e = out.entry((r.path.clone(), r.rg)).or_insert(None);
            if let Some(ts) = r.get("ts") {
                if e.as_ref().is_none_or(|m| ts < m) {
                    *e = Some(ts.clone());
                }
            }
        }
    }
    out
}

/// The planned row groups in reading order: entries in order, each
/// entry's row groups ascending.
fn reading_order(plan: &ScanPlan) -> Vec<(String, u32)> {
    plan.entries
        .iter()
        .flat_map(|e| e.row_groups.iter().map(move |rg| (e.file.path.clone(), rg)))
        .collect()
}

/// Whether the decoded minimums are non-decreasing along `order`, a row
/// group without a minimum counting as greater than every value.
fn time_ordered(order: &[(String, u32)], mins: &BTreeMap<(String, u32), Option<Vec<u8>>>) -> bool {
    let rank = |k: &(String, u32)| {
        let m = &mins[k];
        (m.is_none(), m.clone())
    };
    order.windows(2).all(|w| rank(&w[0]) <= rank(&w[1]))
}

fn every_id(ids: &[i64]) -> Lookup {
    let keys = ids.iter().map(|i| enc_i64(*i)).collect();
    Lookup::Keys {
        columns: cols(&["id"]),
        keys,
    }
}

fn opts() -> BuildOptions {
    BuildOptions::new(SourceKind::Parquet)
}

#[test]
fn entries_follow_the_range_key_minimum_across_files_and_nulls_sort_last() {
    let s = Scratch::new();
    // Path order a, b, c, z; time order c, b, a, then z (all null).
    write(
        &s,
        "a.parquet",
        &[
            (1, Some(500)),
            (2, Some(510)),
            (1, Some(520)),
            (2, Some(530)),
        ],
    );
    write(
        &s,
        "b.parquet",
        &[
            (1, Some(300)),
            (2, Some(310)),
            (1, Some(320)),
            (2, Some(330)),
        ],
    );
    write(
        &s,
        "c.parquet",
        &[(1, Some(100)), (2, Some(110)), (1, Some(120)), (2, None)],
    );
    write(&s, "z.parquet", &[(1, None), (2, None)]);
    let names = ["a.parquet", "b.parquet", "c.parquet", "z.parquet"];
    let store = memory_store();
    let specs = [exact(&["id"]), range("ts")];
    builder(&store)
        .build(&s.root_str(), &specs, &opts())
        .unwrap();
    let p = planner(&store);
    let lookup = every_id(&[1, 2]);
    let plain = p.plan(&specs, &lookup).unwrap();
    let (plan, order) = p.plan_ordered(&specs, &lookup).unwrap();
    assert_eq!(
        order,
        PlanOrder::Ordered {
            column: "ts".into()
        }
    );
    // The same row groups, bytes, and residual; only the order differs.
    assert_eq!(plan_rgs(&plan), plan_rgs(&plain));
    assert_eq!((plan.bytes, &plan.residual), (plain.bytes, &plain.residual));
    let order_seen = reading_order(&plan);
    assert!(
        time_ordered(&order_seen, &decoded_mins(&s, &names)),
        "{order_seen:?}"
    );
    let files: Vec<&str> = plan
        .entries
        .iter()
        .map(|e| e.file.path.rsplit('/').next().unwrap())
        .collect();
    assert_eq!(files, ["c.parquet", "b.parquet", "a.parquet", "z.parquet"]);
    // Deterministic.
    assert_eq!(p.plan_ordered(&specs, &lookup).unwrap().0, plan);
}

#[test]
fn an_order_that_does_not_hold_is_never_claimed() {
    let s = Scratch::new();
    // b's two row groups straddle a's: no order of whole files is a time
    // order.
    write(
        &s,
        "a.parquet",
        &[
            (1, Some(200)),
            (1, Some(210)),
            (1, Some(300)),
            (1, Some(310)),
        ],
    );
    write(
        &s,
        "b.parquet",
        &[
            (1, Some(100)),
            (1, Some(110)),
            (1, Some(250)),
            (1, Some(260)),
        ],
    );
    let store = memory_store();
    let specs = [exact(&["id"]), range("ts")];
    builder(&store)
        .build(&s.root_str(), &specs, &opts())
        .unwrap();
    let p = planner(&store);
    let (plan, order) = p.plan_ordered(&specs, &every_id(&[1])).unwrap();
    let mins = decoded_mins(&s, &["a.parquet", "b.parquet"]);
    assert!(!time_ordered(&reading_order(&plan), &mins));
    let PlanOrder::Unspecified { reason } = order else {
        panic!("{order:?}")
    };
    assert!(
        reason.contains("a.parquet") && reason.contains("b.parquet"),
        "{reason}"
    );
    // Still deterministic: b holds the earliest row group.
    assert!(plan.entries[0].file.path.ends_with("b.parquet"));

    // A file whose own row groups run backwards in time.
    let s = Scratch::new();
    write(
        &s,
        "d.parquet",
        &[
            (1, Some(900)),
            (1, Some(910)),
            (1, Some(100)),
            (1, Some(110)),
        ],
    );
    let store = memory_store();
    builder(&store)
        .build(&s.root_str(), &specs, &opts())
        .unwrap();
    let p = planner(&store);
    let (plan, order) = p.plan_ordered(&specs, &every_id(&[1])).unwrap();
    assert!(!time_ordered(
        &reading_order(&plan),
        &decoded_mins(&s, &["d.parquet"])
    ));
    assert!(
        matches!(&order, PlanOrder::Unspecified { reason } if reason.contains("d.parquet")),
        "{order:?}"
    );
    // One planned row group of that file is trivially in order.
    let one = Lookup::Between {
        column: "ts".into(),
        lo: p.encode_tuple(&cols(&["ts"]), &[ts_value(900)]).unwrap(),
        hi: p.encode_tuple(&cols(&["ts"]), &[ts_value(910)]).unwrap(),
    };
    let (plan, order) = p.plan_ordered(&specs, &one).unwrap();
    assert_eq!(plan.row_groups_selected, 1);
    assert!(matches!(order, PlanOrder::Ordered { .. }), "{order:?}");
}

fn ts_value(v: i64) -> bellwether_core::encoding::Value<'static> {
    bellwether_core::encoding::Value::Timestamp {
        value: v,
        unit: TimeUnit::Microsecond,
    }
}

#[test]
fn without_a_timestamp_range_key_the_order_is_unspecified_and_by_path() {
    let s = Scratch::new();
    write(&s, "a.parquet", &[(1, Some(500)), (2, Some(510))]);
    write(&s, "b.parquet", &[(1, Some(100)), (2, Some(110))]);
    let store = memory_store();
    // A range key on an integer column does not order by time.
    let specs = [exact(&["id"]), range("id")];
    builder(&store)
        .build(&s.root_str(), &specs, &opts())
        .unwrap();
    let p = planner(&store);
    let lookup = every_id(&[1, 2]);
    let (plan, order) = p.plan_ordered(&specs, &lookup).unwrap();
    assert!(
        matches!(&order, PlanOrder::Unspecified { reason } if reason.contains("range key")),
        "{order:?}"
    );
    assert_eq!(
        plan,
        p.plan(&specs, &lookup).unwrap(),
        "path order, unchanged"
    );

    // Two timestamp range keys: a window on one of them orders by it.
    let s = Scratch::new();
    let two = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new(
            "t2",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]));
    for (name, base) in [("a.parquet", 500i64), ("b.parquet", 100)] {
        let batch = RecordBatch::try_new(
            Arc::clone(&two),
            vec![
                Arc::new(Int64Array::from(vec![1, 1])),
                Arc::new(TimestampMicrosecondArray::from(vec![base, base + 1])),
                // t2 runs the other way.
                Arc::new(TimestampMicrosecondArray::from(vec![
                    1000 - base,
                    1001 - base,
                ])),
            ],
        )
        .unwrap();
        write_parquet(&s.path(name), &batch, 2);
    }
    let store = memory_store();
    let specs: Vec<KeySpec> = vec![exact(&["id"]), range("ts"), range("t2")];
    builder(&store)
        .build(&s.root_str(), &specs, &opts())
        .unwrap();
    let p = planner(&store);
    let (_, order) = p.plan_ordered(&specs, &every_id(&[1])).unwrap();
    assert!(
        matches!(&order, PlanOrder::Unspecified { reason } if reason.contains("several")),
        "{order:?}"
    );
    let unit = |v: i64| bellwether_core::encoding::Value::Timestamp {
        value: v,
        unit: TimeUnit::Microsecond,
    };
    let window = |c: &str| Lookup::Between {
        column: c.into(),
        lo: p.encode_tuple(&cols(&[c]), &[unit(0)]).unwrap(),
        hi: p.encode_tuple(&cols(&[c]), &[unit(2000)]).unwrap(),
    };
    let (plan, order) = p.plan_ordered(&specs, &window("t2")).unwrap();
    assert_eq!(
        order,
        PlanOrder::Ordered {
            column: "t2".into()
        }
    );
    assert!(plan.entries[0].file.path.ends_with("a.parquet"));
    let (plan, order) = p.plan_ordered(&specs, &window("ts")).unwrap();
    assert_eq!(
        order,
        PlanOrder::Ordered {
            column: "ts".into()
        }
    );
    assert!(plan.entries[0].file.path.ends_with("b.parquet"));
}
