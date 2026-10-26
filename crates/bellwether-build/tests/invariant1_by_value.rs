//! Invariant 1 end to end against an oracle that decodes every file itself
//! and compares VALUES (SQL equality), never the canonical bytes the code
//! under test produces. Parquet files at two decimal scales and two
//! timestamp units under one dataset type; nulls, an all-null row group,
//! signed zero and NaN on a float key; keys in no file; CSV and JSON lines
//! chunks recomputed from the bytes.
//!
//! Added as regression tests from the PR 19 review.

#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::type_complexity,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::float_cmp,
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::too_many_arguments,
    clippy::assert_is_empty,
    clippy::redundant_closure_for_method_calls,
    clippy::format_push_string,
    clippy::doc_markdown
)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Decimal128Array, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray,
};
use arrow::datatypes::{
    DataType, Decimal128Type, Field, Float64Type, Int64Type, Schema, SchemaRef, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType,
};
use bellwether_build::{BuildOptions, Builder, Lookup, Planner, SourceKind};
use bellwether_core::encoding::Value;
use bellwether_store::ObjectStoreBackend;
use bellwether_types::{KeySpec, ScanPlan};
use common::{apply_residual, cols, memory_store, Row, Scratch};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

static CHECKS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static NONEMPTY: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// A decoded value, compared with SQL equality.
#[derive(Clone, Debug)]
enum V {
    Null,
    I(i64),
    F(f64),
    /// unscaled, scale
    D(i128, i8),
    /// nanoseconds since the epoch
    T(i128),
    S(String),
}

fn sql_eq(a: &V, b: &V) -> bool {
    match (a, b) {
        (V::I(x), V::I(y)) => x == y,
        // SQL: 0.0 = -0.0; the canonical encoding has one NaN, and a NaN
        // probe matches NaN rows
        (V::F(x), V::F(y)) => (x.is_nan() && y.is_nan()) || x == y,
        (V::D(x, sx), V::D(y, sy)) => {
            let s = (*sx).max(*sy);
            x * 10i128.pow((s - sx) as u32) == y * 10i128.pow((s - sy) as u32)
        }
        (V::T(x), V::T(y)) => x == y,
        (V::S(x), V::S(y)) => x == y,
        _ => false,
    }
}

fn to_v(a: &ArrayRef, i: usize) -> V {
    if a.is_null(i) {
        return V::Null;
    }
    match a.data_type() {
        DataType::Int64 => V::I(a.as_primitive::<Int64Type>().value(i)),
        DataType::Float64 => V::F(a.as_primitive::<Float64Type>().value(i)),
        DataType::Decimal128(_, s) => V::D(a.as_primitive::<Decimal128Type>().value(i), *s),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            V::T(i128::from(a.as_primitive::<TimestampMillisecondType>().value(i)) * 1_000_000)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            V::T(i128::from(a.as_primitive::<TimestampMicrosecondType>().value(i)) * 1_000)
        }
        DataType::Utf8 => V::S(a.as_string::<i32>().value(i).to_string()),
        other => panic!("oracle does not decode {other}"),
    }
}

/// (path, row group, column -> value) for every row, decoded with the
/// parquet crate at the FILE's own types.
type ORow = (String, u32, BTreeMap<String, V>);

fn decode_parquet(path: &Path) -> Vec<ORow> {
    let n = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
        .unwrap()
        .metadata()
        .num_row_groups();
    let p = path.to_str().unwrap().to_string();
    let mut out = Vec::new();
    for rg in 0..n {
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
            .unwrap()
            .with_row_groups(vec![rg])
            .build()
            .unwrap();
        for batch in reader {
            let batch = batch.unwrap();
            for r in 0..batch.num_rows() {
                let mut m = BTreeMap::new();
                for (ci, f) in batch.schema().fields().iter().enumerate() {
                    m.insert(f.name().clone(), to_v(batch.column(ci), r));
                }
                out.push((p.clone(), rg as u32, m));
            }
        }
    }
    out
}

fn truth(rows: &[ORow], probe: &[(&str, V)]) -> BTreeSet<(String, u32)> {
    rows.iter()
        .filter(|(_, _, m)| probe.iter().all(|(c, v)| sql_eq(&m[*c], v)))
        .map(|(p, rg, _)| (p.clone(), *rg))
        .collect()
}

fn planned(plan: &ScanPlan) -> BTreeSet<(String, u32)> {
    plan.entries
        .iter()
        .flat_map(|e| e.row_groups.iter().map(move |rg| (e.file.path.clone(), rg)))
        .collect()
}

fn write_groups(path: &Path, schema: &SchemaRef, groups: Vec<Vec<ArrayRef>>) {
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(1 << 20))
        .build();
    let mut w =
        ArrowWriter::try_new(File::create(path).unwrap(), Arc::clone(schema), Some(props)).unwrap();
    for g in groups {
        w.write(&RecordBatch::try_new(Arc::clone(schema), g).unwrap())
            .unwrap();
        w.flush().unwrap();
    }
    w.close().unwrap();
}

fn file_schema(dec: (u8, i8), unit: TimeUnit) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("val", DataType::Float64, true),
        Field::new("amt", DataType::Decimal128(dec.0, dec.1), true),
        Field::new("ts", DataType::Timestamp(unit, None), true),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn dataset() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("val", DataType::Float64, true),
        Field::new("amt", DataType::Decimal128(20, 3), true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("name", DataType::Utf8, true),
    ]))
}

type P1 = (
    Option<i64>,
    Option<f64>,
    Option<i128>,
    Option<i64>,
    Option<&'static str>,
);

fn cols_p(rows: &[P1], dec: (u8, i8), unit: TimeUnit) -> Vec<ArrayRef> {
    let ts: ArrayRef = match unit {
        TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(
            rows.iter().map(|r| r.3).collect::<Vec<_>>(),
        )),
        TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(
            rows.iter().map(|r| r.3).collect::<Vec<_>>(),
        )),
        _ => unreachable!(),
    };
    vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.1).collect::<Vec<_>>(),
        )),
        Arc::new(
            Decimal128Array::from(rows.iter().map(|r| r.2).collect::<Vec<_>>())
                .with_precision_and_scale(dec.0, dec.1)
                .unwrap(),
        ),
        ts,
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.4).collect::<Vec<_>>(),
        )),
    ]
}

/// p1: Decimal(10,2), Timestamp(ms). p2: Decimal(12,3), Timestamp(µs).
fn write_corpus(s: &Scratch) -> Vec<std::path::PathBuf> {
    let (d1, u1) = ((10u8, 2i8), TimeUnit::Millisecond);
    let (d2, u2) = ((12u8, 3i8), TimeUnit::Microsecond);
    let p1 = s.path("p1.parquet");
    let g = |rows: &[P1], d, u| cols_p(rows, d, u);
    write_groups(
        &p1,
        &file_schema(d1, u1),
        vec![
            g(
                &[
                    (Some(1), Some(-0.0), Some(150), Some(1000), Some("a")),
                    (Some(2), Some(f64::NAN), None, Some(2000), Some("b")),
                    (None, Some(1.0), Some(225), None, None),
                ],
                d1,
                u1,
            ),
            g(
                &[
                    (Some(3), None, Some(10), Some(3000), Some("c")),
                    (None, None, None, None, None),
                    (Some(1), Some(2.5), Some(150), Some(5000), Some("a")),
                    (Some(6), None, None, Some(1000), None),
                ],
                d1,
                u1,
            ),
            // an all-null `id` row group
            g(
                &[
                    (None, Some(0.0), Some(999), Some(4000), Some("d")),
                    (None, None, None, None, None),
                ],
                d1,
                u1,
            ),
        ],
    );
    let p2 = s.path("p2.parquet");
    write_groups(
        &p2,
        &file_schema(d2, u2),
        vec![
            g(
                &[
                    (Some(4), Some(0.0), Some(1500), Some(1_000_000), Some("a")),
                    (Some(1), Some(-1.0), Some(100), Some(6_000_000), Some("e")),
                    (Some(6), Some(3.0), Some(2250), Some(1_000_000), Some("g")),
                ],
                d2,
                u2,
            ),
            g(
                &[
                    (Some(5), Some(-0.0), Some(2250), Some(2_000_000), Some("b")),
                    (
                        Some(2),
                        Some(f64::NAN),
                        Some(3001),
                        Some(7_000_000),
                        Some("f"),
                    ),
                ],
                d2,
                u2,
            ),
        ],
    );
    vec![p1, p2]
}

fn specs() -> Vec<KeySpec> {
    let e = |c: &[&str]| KeySpec::Exact {
        columns: c.iter().map(|s| (*s).to_string()).collect(),
        counts: false,
        bounds: None,
    };
    let b = |c: &str| KeySpec::Bloom {
        columns: vec![c.into()],
        fpr: 0.01,
        bounds: None,
    };
    let r = |c: &str| KeySpec::Range { column: c.into() };
    vec![
        e(&["id"]),
        e(&["val"]),
        e(&["amt"]),
        e(&["ts"]),
        e(&["name"]),
        r("id"),
        r("val"),
        r("amt"),
        r("ts"),
        b("id"),
        b("val"),
        b("amt"),
        b("name"),
        e(&["id", "name"]),
        e(&["name", "id"]),
        KeySpec::Temporal {
            columns: vec!["id".into()],
            ts_column: "ts".into(),
            counts: false,
        },
    ]
}

/// Canonical rows (core's encoder at the dataset types) for evaluating
/// residuals; the superset check above never uses them.
fn canonical_rows(paths: &[std::path::PathBuf]) -> Vec<Row> {
    paths
        .iter()
        .flat_map(|p| common::parquet_rows(p, &dataset()))
        .collect()
}

fn check_superset(
    p: &Planner<ObjectStoreBackend>,
    rows: &[ORow],
    crow: &[Row],
    spec: &KeySpec,
    columns: &[&str],
    probe_vals: &[V],
    probe_scalars: &[Value<'_>],
    prefix_only: bool,
) {
    let c = cols(columns);
    let key = p.encode_tuple(&c, probe_scalars).unwrap();
    let plan = p
        .plan(
            std::slice::from_ref(spec),
            &Lookup::Keys {
                columns: c.clone(),
                keys: vec![key],
            },
        )
        .unwrap_or_else(|e| panic!("{} on {columns:?}: {e}", spec.name()));
    let probe: Vec<(&str, V)> = columns
        .iter()
        .copied()
        .zip(probe_vals.iter().cloned())
        .collect();
    let want = truth(rows, &probe);
    let got = planned(&plan);
    CHECKS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    if !want.is_empty() {
        NONEMPTY.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    assert!(
        want.is_subset(&got),
        "INVARIANT 1: {} probe {probe:?} misses {:?} (plan {got:?})",
        spec.name(),
        want.difference(&got).collect::<Vec<_>>()
    );
    // residual exactness: the residual over the planned rows returns
    // exactly the rows whose values match (SQL equality, by value)
    let res = apply_residual(
        plan.residual.as_ref().unwrap(),
        crow.iter()
            .filter(|r| got.contains(&(r.path.clone(), r.rg)))
            .cloned()
            .collect(),
    );
    let res_rows: BTreeSet<(String, u32, usize)> =
        res.iter().map(|r| (r.path.clone(), r.rg, r.row)).collect();
    // oracle rows matching, located by (path, rg, ordinal within file)
    let mut ordinal: BTreeMap<String, usize> = BTreeMap::new();
    let mut want_rows = BTreeSet::new();
    for (path, rg, m) in rows {
        let o = ordinal.entry(path.clone()).or_insert(0);
        if probe.iter().all(|(c, v)| sql_eq(&m[*c], v)) {
            want_rows.insert((path.clone(), *rg, *o));
        }
        *o += 1;
    }
    if !prefix_only {
        assert_eq!(
            res_rows,
            want_rows,
            "residual of {} for {probe:?} is not the matching rows",
            spec.name()
        );
    }
}

#[test]
fn invariant1_by_value_over_two_scales_two_units_nulls_and_zeros() {
    let s = Scratch::new();
    let paths = write_corpus(&s);
    let store = memory_store();
    let b = Builder::new(Arc::clone(&store), "idx");
    for (max_keys, item) in [(1u64 << 20, 64usize), (1, 1), (3, 2)] {
        let store = memory_store();
        let b2 = Builder::new(Arc::clone(&store), "idx");
        let opts = BuildOptions {
            schema: Some(dataset()),
            max_segment_keys: max_keys,
            max_segment_row_groups: item,
            threads: Some(2),
            ..BuildOptions::new(SourceKind::Parquet)
        };
        let r = b2.build(&s.root_str(), &specs(), &opts);
        let r = r.unwrap_or_else(|e| panic!("build at cap {max_keys}/{item}: {e}"));
        assert!(r.sequence.is_some());
        run_all(&store, &paths);
    }
    let opts = BuildOptions {
        schema: Some(dataset()),
        ..BuildOptions::new(SourceKind::Parquet)
    };
    b.build(&s.root_str(), &specs(), &opts).unwrap();
    run_all(&store, &paths);
    let (c, n) = (
        CHECKS.load(std::sync::atomic::Ordering::SeqCst),
        NONEMPTY.load(std::sync::atomic::Ordering::SeqCst),
    );
    eprintln!("checks {c}, with a non-empty truth {n}");
    assert!(c > 400 && n > 200, "checks {c}, non-empty {n}");
}

fn run_all(store: &Arc<ObjectStoreBackend>, paths: &[std::path::PathBuf]) {
    let rows: Vec<ORow> = paths.iter().flat_map(|p| decode_parquet(p)).collect();
    let crow = canonical_rows(paths);
    let p = Planner::open(Arc::clone(store), "idx").unwrap();
    let all = specs();
    let sp = |name: &str| all.iter().find(|s| s.name() == name).unwrap().clone();
    // id
    for id in [0i64, 1, 2, 3, 4, 5, 6, 99] {
        for spec in ["exact:id", "range:id", "bloom:id", "temporal:id"] {
            check_superset(
                &p,
                &rows,
                &crow,
                &sp(spec),
                &["id"],
                &[V::I(id)],
                &[Value::Int(id)],
                false,
            );
        }
        // prefix via the composite
        check_superset(
            &p,
            &rows,
            &crow,
            &sp("exact:id,name"),
            &["id"],
            &[V::I(id)],
            &[Value::Int(id)],
            false,
        );
    }
    // val: signed zero, NaN, absent
    for v in [0.0, -0.0, f64::NAN, 1.0, 2.5, -1.0, 3.0, 42.0] {
        for spec in ["exact:val", "range:val", "bloom:val"] {
            check_superset(
                &p,
                &rows,
                &crow,
                &sp(spec),
                &["val"],
                &[V::F(v)],
                &[Value::Float(v)],
                false,
            );
        }
    }
    // amt: probes at scales 2, 3 and 1 and 0, all under the dataset scale 3
    let amts: [(i128, i8); 9] = [
        (150, 2),
        (1500, 3),
        (15, 1),
        (1, 1),
        (100, 3),
        (2250, 3),
        (3001, 3),
        (999, 2),
        (77, 0),
    ];
    for (v, sc) in amts {
        for spec in ["exact:amt", "range:amt", "bloom:amt"] {
            check_superset(
                &p,
                &rows,
                &crow,
                &sp(spec),
                &["amt"],
                &[V::D(v, sc)],
                &[Value::Decimal128 {
                    value: v,
                    scale: sc,
                }],
                false,
            );
        }
    }
    // ts: probes in ms, µs and s under the dataset µs
    let tss: [(i64, TimeUnit, i128); 7] = [
        (1000, TimeUnit::Millisecond, 1_000_000_000),
        (1_000_000, TimeUnit::Microsecond, 1_000_000_000),
        (5000, TimeUnit::Millisecond, 5_000_000_000),
        (7_000_000, TimeUnit::Microsecond, 7_000_000_000),
        (2, TimeUnit::Second, 2_000_000_000),
        (4, TimeUnit::Second, 4_000_000_000),
        (123, TimeUnit::Second, 123_000_000_000),
    ];
    for (v, unit, ns) in tss {
        for spec in ["exact:ts", "range:ts"] {
            check_superset(
                &p,
                &rows,
                &crow,
                &sp(spec),
                &["ts"],
                &[V::T(ns)],
                &[Value::Timestamp { value: v, unit }],
                false,
            );
        }
    }
    // name
    for n in ["a", "b", "g", "zz", ""] {
        for spec in ["exact:name", "bloom:name"] {
            check_superset(
                &p,
                &rows,
                &crow,
                &sp(spec),
                &["name"],
                &[V::S(n.into())],
                &[Value::Utf8(n)],
                false,
            );
        }
        check_superset(
            &p,
            &rows,
            &crow,
            &sp("exact:name,id"),
            &["name"],
            &[V::S(n.into())],
            &[Value::Utf8(n)],
            false,
        );
    }
    // composite by projection and both orders
    for (id, n) in [(1i64, "a"), (2, "f"), (2, "b"), (99, "a"), (6, "g")] {
        for spec in ["exact:id,name", "exact:name,id", "exact:id", "bloom:name"] {
            check_superset(
                &p,
                &rows,
                &crow,
                &sp(spec),
                &["id", "name"],
                &[V::I(id), V::S(n.into())],
                &[Value::Int(id), Value::Utf8(n)],
                false,
            );
        }
    }
    // intersections of every pair of single-column specs on (id, val)
    for (id, v) in [(1i64, -0.0), (1, 0.0), (5, 0.0), (2, f64::NAN), (4, -0.0)] {
        let probe = [V::I(id), V::F(v)];
        let key = p
            .encode_tuple(&cols(&["id", "val"]), &[Value::Int(id), Value::Float(v)])
            .unwrap();
        for a in ["exact:id", "range:id", "bloom:id", "temporal:id"] {
            for bb in ["exact:val", "range:val", "bloom:val"] {
                let plan = p
                    .plan(
                        &[sp(a), sp(bb)],
                        &Lookup::Keys {
                            columns: cols(&["id", "val"]),
                            keys: vec![key.clone()],
                        },
                    )
                    .unwrap();
                let want = truth(
                    &rows,
                    &[("id", probe[0].clone()), ("val", probe[1].clone())],
                );
                assert!(
                    want.is_subset(&planned(&plan)),
                    "INVARIANT 1: {a} ∩ {bb} misses {:?} for ({id}, {v})",
                    want.difference(&planned(&plan)).collect::<Vec<_>>()
                );
            }
        }
    }
    // temporal: latest and asof by value
    for id in [1i64, 2, 3, 4, 5, 6, 99] {
        for asof in [
            None,
            Some(1_000_000i64),
            Some(1_500_000),
            Some(5_000_000),
            Some(10_000_000),
        ] {
            check_temporal(&p, &rows, &crow, &sp("temporal:id"), id, asof);
        }
    }
}

fn check_temporal(
    p: &Planner<ObjectStoreBackend>,
    rows: &[ORow],
    crow: &[Row],
    spec: &KeySpec,
    id: i64,
    asof_us: Option<i64>,
) {
    let key = p.encode_tuple(&cols(&["id"]), &[Value::Int(id)]).unwrap();
    let lookup = match asof_us {
        None => Lookup::Latest {
            columns: cols(&["id"]),
            keys: vec![key],
        },
        Some(ts) => Lookup::AsOf {
            columns: cols(&["id"]),
            keys: vec![key],
            ts,
        },
    };
    let plan = p.plan(std::slice::from_ref(spec), &lookup).unwrap();
    // newest non-null ts of `id` (at or before asof), by value
    let limit = asof_us.map(|t| i128::from(t) * 1000);
    let best = rows
        .iter()
        .filter(|(_, _, m)| sql_eq(&m["id"], &V::I(id)))
        .filter_map(|(_, _, m)| match m["ts"] {
            V::T(t) => Some(t),
            _ => None,
        })
        .filter(|t| limit.is_none_or(|l| *t <= l))
        .max();
    let mut want_rows = BTreeSet::new();
    let mut want_rgs = BTreeSet::new();
    let mut ordinal: BTreeMap<String, usize> = BTreeMap::new();
    for (path, rg, m) in rows {
        let o = ordinal.entry(path.clone()).or_insert(0);
        if let (true, V::T(t), Some(b)) = (sql_eq(&m["id"], &V::I(id)), &m["ts"], best) {
            if *t == b {
                want_rows.insert((path.clone(), *rg, *o));
                want_rgs.insert((path.clone(), *rg));
            }
        }
        *o += 1;
    }
    let got = planned(&plan);
    assert!(
        want_rgs.is_subset(&got),
        "INVARIANT 1/5: latest({id}) asof {asof_us:?} misses {:?}",
        want_rgs.difference(&got).collect::<Vec<_>>()
    );
    let res = apply_residual(
        plan.residual.as_ref().unwrap(),
        crow.iter()
            .filter(|r| got.contains(&(r.path.clone(), r.rg)))
            .cloned()
            .collect(),
    );
    let res_rows: BTreeSet<(String, u32, usize)> =
        res.iter().map(|r| (r.path.clone(), r.rg, r.row)).collect();
    assert_eq!(res_rows, want_rows, "latest({id}) asof {asof_us:?}");
}

// ---------------------------------------------------------------------------
// CSV and JSON lines
// ---------------------------------------------------------------------------

fn text_dataset() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("val", DataType::Float64, true),
        Field::new("name", DataType::Utf8, true),
    ]))
}

/// The chunk rule restated: chunk k+1 starts just after the first `\n` at
/// or after `start_k + chunk_size - 1`.
fn chunks(bytes: &[u8], size: u64) -> Vec<(u64, u64)> {
    let mut out = Vec::new();
    let mut start = 0u64;
    let n = bytes.len() as u64;
    while start < n {
        let t = start + size;
        let end = if t >= n {
            n
        } else {
            let from = (t - 1) as usize;
            bytes[from..]
                .iter()
                .position(|&b| b == b'\n')
                .map_or(n, |p| (from + p + 1) as u64)
        };
        out.push((start, end));
        start = end;
    }
    out
}

type TRow = (Option<i64>, Option<f64>, Option<&'static str>);

fn text_rows() -> Vec<TRow> {
    vec![
        (Some(1), Some(-0.0), Some("a")),
        (Some(2), None, Some("b")),
        (None, Some(0.0), Some("c")),
        (Some(3), Some(1.5), None),
        (Some(1), Some(0.0), Some("d")),
        (None, None, None),
        (Some(7), Some(-0.0), Some("e")),
        (Some(2), Some(2.5), Some("a")),
        (None, None, None),
        (None, None, None),
        (None, None, None),
        (Some(9), Some(-1.0), Some("f")),
    ]
}

fn csv_line(r: &TRow) -> String {
    let id = r.0.map(|v| v.to_string()).unwrap_or_default();
    let val = r.1.map(|v| format!("{v:?}")).unwrap_or_default();
    let name = r.2.unwrap_or("");
    format!("{id},{val},{name}\n")
}

fn json_line(r: &TRow) -> String {
    let mut parts = Vec::new();
    if let Some(v) = r.0 {
        parts.push(format!("\"id\":{v}"));
    }
    if let Some(v) = r.1 {
        parts.push(format!("\"val\":{v:?}"));
    }
    if let Some(v) = r.2 {
        parts.push(format!("\"name\":\"{v}\""));
    }
    format!("{{{}}}\n", parts.join(","))
}

fn text_case(kind: SourceKind, file: &str, chunk: u64) {
    let s = Scratch::new();
    let rows = text_rows();
    let mut body = String::new();
    // line index of each data row within the file
    let mut line_start = Vec::new();
    if matches!(kind, SourceKind::Csv { .. }) {
        body.push_str("id,val,name\n");
    }
    for r in &rows {
        line_start.push(body.len() as u64);
        body.push_str(&match kind {
            SourceKind::JsonLines => json_line(r),
            _ => csv_line(r),
        });
    }
    let path = s.path(file);
    std::fs::write(&path, &body).unwrap();
    let chunk_ranges = chunks(body.as_bytes(), chunk);
    let chunk_of = |off: u64| {
        chunk_ranges
            .iter()
            .position(|(a, b)| *a <= off && off < *b)
            .unwrap() as u32
    };
    let store = memory_store();
    let b = Builder::new(Arc::clone(&store), "idx");
    let specs = vec![
        KeySpec::Exact {
            columns: vec!["id".into()],
            counts: false,
            bounds: None,
        },
        KeySpec::Exact {
            columns: vec!["val".into()],
            counts: false,
            bounds: None,
        },
        KeySpec::Range {
            column: "val".into(),
        },
        KeySpec::Bloom {
            columns: vec!["name".into()],
            fpr: 0.01,
            bounds: None,
        },
        KeySpec::Exact {
            columns: vec!["id".into(), "name".into()],
            counts: false,
            bounds: None,
        },
    ];
    let opts = BuildOptions {
        schema: Some(text_dataset()),
        chunk_size: Some(chunk),
        ..BuildOptions::new(kind)
    };
    let r = b.build(&s.root_str(), &specs, &opts).unwrap();
    assert!(r.sequence.is_some());
    // the manifest's chunks equal the restated rule
    let m = b.manifest().unwrap().unwrap();
    let recorded: Vec<(u64, u64)> = m.rowgroup_space.files[0]
        .chunks
        .as_ref()
        .unwrap()
        .iter()
        .map(|c| (c.start, c.end))
        .collect();
    assert_eq!(recorded, chunk_ranges, "{file} chunks");
    let p = Planner::open(Arc::clone(&store), "idx").unwrap();
    let pstr = path.to_str().unwrap().to_string();
    let oracle: Vec<ORow> = rows
        .iter()
        .zip(&line_start)
        .map(|(r, off)| {
            let mut m = BTreeMap::new();
            m.insert("id".to_string(), r.0.map_or(V::Null, V::I));
            m.insert("val".to_string(), r.1.map_or(V::Null, V::F));
            m.insert(
                "name".to_string(),
                r.2.map_or(V::Null, |s| V::S(s.to_string())),
            );
            (pstr.clone(), chunk_of(*off), m)
        })
        .collect();
    let check = |spec: &KeySpec, columns: &[&str], vals: &[V], scalars: &[Value<'_>]| {
        let key = p.encode_tuple(&cols(columns), scalars).unwrap();
        let plan = p
            .plan(
                std::slice::from_ref(spec),
                &Lookup::Keys {
                    columns: cols(columns),
                    keys: vec![key],
                },
            )
            .unwrap();
        let probe: Vec<(&str, V)> = columns.iter().copied().zip(vals.iter().cloned()).collect();
        let want = truth(&oracle, &probe);
        let got = planned(&plan);
        assert!(
            want.is_subset(&got),
            "INVARIANT 1 ({file}, chunk {chunk}): {} {probe:?} misses {:?}",
            spec.name(),
            want.difference(&got).collect::<Vec<_>>()
        );
        // bytes are the recomputed chunk lengths
        let bytes: u64 = got
            .iter()
            .map(|(_, rg)| {
                let (a, b) = chunk_ranges[*rg as usize];
                b - a
            })
            .sum();
        assert_eq!(plan.bytes, bytes, "{file} plan bytes");
    };
    for id in [1i64, 2, 3, 7, 9, 99] {
        check(&specs[0], &["id"], &[V::I(id)], &[Value::Int(id)]);
        check(&specs[4], &["id"], &[V::I(id)], &[Value::Int(id)]);
    }
    for v in [0.0, -0.0, 1.5, 2.5, -1.0, 8.0] {
        check(&specs[1], &["val"], &[V::F(v)], &[Value::Float(v)]);
        check(&specs[2], &["val"], &[V::F(v)], &[Value::Float(v)]);
    }
    for n in ["a", "b", "f", "q"] {
        check(&specs[3], &["name"], &[V::S(n.into())], &[Value::Utf8(n)]);
    }
    for (id, n) in [(1i64, "a"), (1, "d"), (2, "a"), (5, "a")] {
        check(
            &specs[4],
            &["id", "name"],
            &[V::I(id), V::S(n.into())],
            &[Value::Int(id), Value::Utf8(n)],
        );
    }
    // every live chunk with no non-null `id` is still covered
    assert_eq!(
        bellwether_build::completeness(&m, &specs[0]),
        Some(r.sequence.unwrap())
    );
}

#[test]
fn invariant1_by_value_on_csv_chunks() {
    for chunk in [16u64, 23, 40, 64, 1 << 20] {
        text_case(SourceKind::csv(), "t.csv", chunk);
    }
}

#[test]
fn invariant1_by_value_on_jsonl_chunks() {
    for chunk in [16u64, 23, 40, 64, 1 << 20] {
        text_case(SourceKind::JsonLines, "t.jsonl", chunk);
    }
}
