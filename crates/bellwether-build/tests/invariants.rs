//! Invariants 1 and 5 end to end: built by the builder, planned by the
//! planner, checked against ground truth decoded independently.
//!
//! - Invariant 1: every row group holding a queried key is in the plan, for
//!   exact, bloom, range, prefix, composite-by-projection, and temporal
//!   lookups, whatever the segment caps, work split, or thread count.
//! - Invariant 5: a temporal lookup never returns a superseded row, even
//!   when the newest version lives in a row group another spec pruned, and
//!   even after the file holding the newest version was rewritten.

#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::cast_possible_wrap,
    clippy::assert_is_empty,
    clippy::too_many_lines,
    clippy::type_complexity,
    clippy::cast_precision_loss,
    clippy::redundant_closure_for_method_calls,
    clippy::format_push_string
)]

mod common;

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_build::{BuildOptions, Lookup};
use bellwether_core::encoding::Value;
use bellwether_types::Predicate;
use common::*;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use proptest::prelude::*;

const VALS: [f64; 6] = [-1.0, -0.0, 0.0, 1.5, f64::NAN, 2.5];

#[derive(Clone, Debug)]
struct R {
    id: Option<i64>,
    grp: u8,
    val: Option<usize>,
    ts: Option<i64>,
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("grp", DataType::Utf8, true),
        Field::new("val", DataType::Float64, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
    ]))
}

fn ts_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, None)
}

fn enc_ts(t: i64) -> Vec<u8> {
    enc(
        &Value::Timestamp {
            value: t,
            unit: TimeUnit::Microsecond,
        },
        &ts_type(),
    )
}

fn batch(rows: &[R]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.id).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| Some(format!("g{}", r.grp)))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter()
                    .map(|r| r.val.map(|i| VALS[i]))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(TimestampMicrosecondArray::from(
                rows.iter().map(|r| r.ts).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// Writes one Parquet file whose row groups are exactly `groups`.
fn write_groups(path: &std::path::Path, groups: &[Vec<R>]) {
    let props = WriterProperties::builder()
        .set_max_row_group_size(1 << 20)
        .build();
    let mut w =
        ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema(), Some(props)).unwrap();
    for g in groups {
        w.write(&batch(g)).unwrap();
        w.flush().unwrap();
    }
    w.close().unwrap();
}

fn row() -> impl Strategy<Value = R> {
    (
        prop::option::weighted(0.9, 0i64..12),
        0u8..4,
        prop::option::weighted(0.9, 0usize..VALS.len()),
        prop::option::weighted(0.85, 0i64..30),
    )
        .prop_map(|(id, grp, val, ts)| R { id, grp, val, ts })
}

type Layout = Vec<Vec<Vec<R>>>;

fn layout() -> impl Strategy<Value = Layout> {
    prop::collection::vec(
        prop::collection::vec(prop::collection::vec(row(), 1..7), 1..4),
        1..4,
    )
}

#[derive(Clone, Debug)]
struct Queries {
    ids: Vec<i64>,
    tuples: Vec<(i64, u8)>,
    vals: Vec<usize>,
    asof: i64,
    between: (usize, usize),
    keys_cap: u64,
    item_rgs: usize,
    threads: usize,
}

fn queries() -> impl Strategy<Value = Queries> {
    (
        prop::collection::vec(0i64..14, 0..6),
        prop::collection::vec((0i64..14, 0u8..5), 0..5),
        prop::collection::vec(0usize..VALS.len(), 0..3),
        0i64..32,
        (0usize..VALS.len(), 0usize..VALS.len()),
        1u64..40,
        1usize..5,
        1usize..4,
    )
        .prop_map(
            |(ids, tuples, vals, asof, between, keys_cap, item_rgs, threads)| Queries {
                ids,
                tuples,
                vals,
                asof,
                between,
                keys_cap,
                item_rgs,
                threads,
            },
        )
}

fn tuple(id: i64, grp: u8) -> Vec<u8> {
    [enc_i64(id), enc_str(&format!("g{grp}"))].concat()
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 32, ..ProptestConfig::default() })]

    #[test]
    fn invariant1_and_5_hold_over_random_layouts(layout in layout(), q in queries()) {
        let s = Scratch::new();
        let mut paths = Vec::new();
        for (i, groups) in layout.iter().enumerate() {
            let p = s.path(&format!("part-{i}.parquet"));
            write_groups(&p, groups);
            paths.push(p);
        }
        let store = memory_store();
        let b = builder(&store);
        let t = temporal(&["id"], "ts");
        let specs = [
            exact(&["id"]),
            bloom(&["grp"]),
            range("val"),
            exact(&["id", "grp"]),
            t.clone(),
            exact(&["val"]),
        ];
        let opts = BuildOptions {
            max_segment_keys: q.keys_cap,
            max_segment_row_groups: q.item_rgs,
            threads: Some(q.threads),
            ..parquet_opts(&schema())
        };
        build_ok(&b, &s.root_str(), &specs, &opts);
        let m = b.manifest().unwrap().unwrap();
        for spec in &specs {
            prop_assert_eq!(bellwether_build::completeness(&m, spec), Some(1));
        }
        let rows: Vec<Row> = paths.iter().flat_map(|p| parquet_rows(p, &schema())).collect();
        let p = planner(&store);

        let ids: Vec<Vec<u8>> = q.ids.iter().map(|&i| enc_i64(i)).collect();
        let tuples: Vec<Vec<u8>> = q.tuples.iter().map(|&(i, g)| tuple(i, g)).collect();
        // keys closed under the two zeros, so canonical equality is SQL equality
        let mut vals: Vec<Vec<u8>> = Vec::new();
        for &v in &q.vals {
            vals.push(enc_f64(VALS[v]));
            if VALS[v] == 0.0 {
                vals.push(enc_f64(0.0));
                vals.push(enc_f64(-0.0));
            }
        }
        let (id_c, grp_c, val_c, idg) = (cols(&["id"]), cols(&["grp"]), cols(&["val"]), cols(&["id", "grp"]));

        // invariant 1 on every kind of lookup
        check_keys(&p, &rows, &[exact(&["id"])], &id_c, &ids);
        check_keys(&p, &rows, std::slice::from_ref(&t), &id_c, &ids);
        check_keys(&p, &rows, &[exact(&["id", "grp"])], &id_c, &ids);
        check_keys(&p, &rows, &[bloom(&["grp"])], &grp_c, &tuples.iter().map(|k| k[9..].to_vec()).collect::<Vec<_>>());
        check_keys(&p, &rows, &[exact(&["id", "grp"])], &idg, &tuples);
        check_keys(&p, &rows, &[exact(&["id"]), bloom(&["grp"])], &idg, &tuples);
        check_keys(&p, &rows, &[exact(&["id"]), bloom(&["grp"]), exact(&["id", "grp"])], &idg, &tuples);
        check_keys(&p, &rows, &[exact(&["val"])], &val_c, &vals);
        check_keys(&p, &rows, &[range("val")], &val_c, &vals);

        // range lookups: the residual over the planned rows equals the
        // residual over every row, and matches SQL BETWEEN for non-NaN bounds
        let (lo, hi) = (VALS[q.between.0], VALS[q.between.1]);
        let lookup = Lookup::Between { column: "val".into(), lo: enc_f64(lo), hi: enc_f64(hi) };
        for specs in [&[range("val")][..], &[exact(&["val"])][..], &[range("val"), exact(&["val"])][..]] {
            let plan = p.plan(specs, &lookup).unwrap();
            let residual = plan.residual.as_ref().unwrap();
            let truth = sorted(apply_residual(residual, rows.clone()));
            prop_assert!(holding(&truth, |_| true).is_subset(&plan_rgs(&plan)));
            prop_assert_eq!(sorted(apply_residual(residual, planned_rows(&plan, &rows))), truth.clone());
            if !lo.is_nan() && !hi.is_nan() {
                let sql = truth.iter().filter(|r| r.get("val").is_some()).count();
                let want = rows.iter().filter(|r| {
                    let v = r.get("val").map(|b| b.as_slice());
                    VALS.iter().any(|&x| v == Some(enc_f64(x).as_slice()) && lo <= x && x <= hi)
                }).count();
                prop_assert_eq!(sql, want);
            }
        }

        // invariant 5: latest and asof, alone and with a spec that prunes by
        // another column
        check_latest(&p, &rows, std::slice::from_ref(&t), &id_c, &ids, &id_c, "ts", None);
        check_latest(&p, &rows, &[t.clone(), bloom(&["grp"])], &idg, &tuples, &id_c, "ts", None);
        check_latest(&p, &rows, &[t.clone(), exact(&["id", "grp"])], &idg, &tuples, &id_c, "ts", None);
        let at = Some((q.asof, enc_ts(q.asof)));
        check_latest(&p, &rows, std::slice::from_ref(&t), &id_c, &ids, &id_c, "ts", at.clone());
        check_latest(&p, &rows, &[t.clone(), bloom(&["grp"])], &idg, &tuples, &id_c, "ts", at);
    }
}

/// The scenario invariant 5 exists for. Key 1's newest version (ts 20) is
/// in row group 1, in region `west`; an older version (ts 10) is in row
/// group 0 with key 2, in region `east`. Looking up the latest of
/// `(1, east)` and `(2, east)` with a region spec prunes row group 1 for
/// key 1, but row group 0 is read for key 2, so key 1's stale row is read
/// too. Only the index-wide bound drops it.
#[test]
fn invariant5_a_version_in_a_pruned_row_group_still_bounds_older_rows() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("ts", ts_type(), true),
    ]));
    let s = Scratch::new();
    let path = s.path("ledger.parquet");
    let rg = |rows: Vec<(i64, &str, i64)>| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(TimestampMicrosecondArray::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    };
    let props = WriterProperties::builder()
        .set_max_row_group_size(1 << 20)
        .build();
    let mut w = ArrowWriter::try_new(
        std::fs::File::create(&path).unwrap(),
        Arc::clone(&schema),
        Some(props),
    )
    .unwrap();
    w.write(&rg(vec![(1, "east", 10), (2, "east", 10)]))
        .unwrap();
    w.flush().unwrap();
    w.write(&rg(vec![(1, "west", 20)])).unwrap();
    w.close().unwrap();

    let store = memory_store();
    let b = builder(&store);
    let t = temporal(&["id"], "ts");
    build_ok(
        &b,
        &s.root_str(),
        &[t.clone(), exact(&["region"])],
        &parquet_opts(&schema),
    );
    let rows = parquet_rows(&path, &schema);
    let p = planner(&store);
    let columns = cols(&["id", "region"]);
    let keys = vec![
        [enc_i64(1), enc_str("east")].concat(),
        [enc_i64(2), enc_str("east")].concat(),
    ];
    let plan = check_latest(
        &p,
        &rows,
        &[t, exact(&["region"])],
        &columns,
        &keys,
        &cols(&["id"]),
        "ts",
        None,
    );
    // row group 1 (key 1's newest version) was pruned by the region spec ...
    assert_eq!(
        plan_rgs(&plan).iter().map(|x| x.1).collect::<Vec<_>>(),
        vec![0]
    );
    // ... yet the residual's bound for key 1 is its newest timestamp
    let Some(Predicate::And { all }) = &plan.residual else {
        panic!("{:?}", plan.residual)
    };
    let bounds = all
        .iter()
        .find_map(|p| match p {
            Predicate::NotOlderThan { bounds, .. } => Some(bounds.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        bounds,
        vec![(enc_i64(1), enc_ts(20)), (enc_i64(2), enc_ts(10))]
    );
    // and it is load-bearing: without it the stale (1, east, 10) row wins
    let without: Vec<Predicate> = all
        .iter()
        .filter(|p| !matches!(p, Predicate::NotOlderThan { .. }))
        .cloned()
        .collect();
    let stale = apply_residual(&Predicate::And { all: without }, planned_rows(&plan, &rows));
    assert_eq!(stale.len(), 2, "{stale:?}");
    let got = apply_residual(plan.residual.as_ref().unwrap(), planned_rows(&plan, &rows));
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].get("id"), Some(&enc_i64(2)));
}

/// A file is rewritten so that key 7 loses its newest version. The old
/// segment still holds the retired row group's ts 100; the live newest
/// version is ts 50. A bound of 100 would drop every live row of key 7.
#[test]
fn invariant5_a_retired_version_never_bounds_live_rows() {
    let s = Scratch::new();
    let path = s.path("f.parquet");
    let one = |rows: &[(i64, i64)]| {
        let rs: Vec<R> = rows
            .iter()
            .map(|&(id, ts)| R {
                id: Some(id),
                grp: 0,
                val: Some(0),
                ts: Some(ts),
            })
            .collect();
        vec![rs]
    };
    write_groups(&path, &one(&[(7, 100), (8, 5)]));
    let store = memory_store();
    let b = builder(&store);
    let t = temporal(&["id"], "ts");
    build_ok(
        &b,
        &s.root_str(),
        std::slice::from_ref(&t),
        &parquet_opts(&schema()),
    );
    // the rewrite drops key 7's ts-100 version
    write_groups(&path, &one(&[(7, 50), (7, 40), (8, 5), (9, 1)]));
    let r = build_ok(
        &b,
        &s.root_str(),
        std::slice::from_ref(&t),
        &parquet_opts(&schema()),
    );
    assert_eq!(
        (r.files_changed, r.row_groups_retired, r.row_groups_assigned),
        (1, 1, 1)
    );
    let m = b.manifest().unwrap().unwrap();
    assert_eq!(
        m.segments.len(),
        2,
        "the old segment is still live; core merge cannot drop retired ids"
    );
    let rows = parquet_rows(&path, &schema());
    let p = planner(&store);
    let id_c = cols(&["id"]);
    let keys = vec![enc_i64(7), enc_i64(8), enc_i64(9)];
    let plan = check_latest(
        &p,
        &rows,
        std::slice::from_ref(&t),
        &id_c,
        &keys,
        &id_c,
        "ts",
        None,
    );
    assert_eq!(plan_rgs(&plan).len(), 1);
    assert!(plan
        .entries
        .iter()
        .all(|e| e.file.path == path.to_str().unwrap()));
    check_latest(
        &p,
        &rows,
        std::slice::from_ref(&t),
        &id_c,
        &keys,
        &id_c,
        "ts",
        Some((45, enc_ts(45))),
    );
    check_latest(
        &p,
        &rows,
        std::slice::from_ref(&t),
        &id_c,
        &keys,
        &id_c,
        "ts",
        Some((200, enc_ts(200))),
    );
    // exact lookups never return the retired row group either
    check_keys(&p, &rows, &[t], &id_c, &keys);
}

/// ADR 014: a key whose versions all have null timestamps has no latest;
/// its rows are members of their row groups but never returned by latest,
/// even when those row groups are planned for another key.
#[test]
fn a_key_with_only_null_timestamps_has_no_latest() {
    let s = Scratch::new();
    let path = s.path("n.parquet");
    let row = |id: i64, ts: Option<i64>| R {
        id: Some(id),
        grp: 0,
        val: Some(0),
        ts,
    };
    write_groups(
        &path,
        &[
            vec![row(5, None), row(6, Some(3))],
            vec![row(5, None), row(6, Some(1))],
        ],
    );
    let store = memory_store();
    let b = builder(&store);
    let t = temporal(&["id"], "ts");
    build_ok(
        &b,
        &s.root_str(),
        std::slice::from_ref(&t),
        &parquet_opts(&schema()),
    );
    let rows = parquet_rows(&path, &schema());
    let p = planner(&store);
    let id_c = cols(&["id"]);
    // membership: key 5 is in both row groups
    let plan = check_keys(&p, &rows, std::slice::from_ref(&t), &id_c, &[enc_i64(5)]);
    assert_eq!(plan.row_groups_selected, 2);
    // latest: key 5 has none; key 6's winner is in row group 0, which also
    // holds a null-timestamp row of key 5 that must not come back
    let plan = check_latest(
        &p,
        &rows,
        std::slice::from_ref(&t),
        &id_c,
        &[enc_i64(5), enc_i64(6)],
        &id_c,
        "ts",
        None,
    );
    assert_eq!(
        plan_rgs(&plan).iter().map(|x| x.1).collect::<Vec<_>>(),
        vec![0]
    );
    let got = apply_residual(plan.residual.as_ref().unwrap(), planned_rows(&plan, &rows));
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].get("id"), Some(&enc_i64(6)));
}
