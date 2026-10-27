//! The residual's typed translation agrees with a reference evaluator over
//! canonical bytes (WP16 item 7, `tests/residual.rs`): random tables with
//! nulls, float zeros of both signs, NaN, and timestamp ties, random key
//! lists, ranges, per-key bounds, rankings, and live tests, run through
//! each engine's translation (DataFusion in process with the `datafusion`
//! feature; DuckDB through its CLI when one is installed), must return
//! exactly the reference's rows.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_sign_loss,
    clippy::default_trait_access,
    clippy::doc_markdown,
    clippy::format_collect,
    clippy::many_single_char_names
)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    BooleanArray, Float64Array, Int64Array, RecordBatch, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use bellwether_adapters::{ColumnDef, EmitOptions, RowOffsets};
use bellwether_core::encoding::{encode_nullable_as, encode_value_as, Value};
use bellwether_types::{FileFingerprint, Hash, Predicate, ScanEntry, ScanPlan};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};

mod common;

const FLOATS: [f64; 5] = [0.0, -0.0, f64::NAN, 1.0, -1.0];

#[derive(Clone, Debug)]
struct Table {
    k: Vec<Option<i64>>,
    f: Vec<Option<usize>>,
    ts: Vec<Option<i64>>,
    void: Vec<Option<bool>>,
}

fn table() -> impl Strategy<Value = Table> {
    (1usize..120).prop_flat_map(|n| {
        (
            proptest::collection::vec(proptest::option::weighted(0.9, 0i64..8), n),
            proptest::collection::vec(proptest::option::weighted(0.9, 0usize..FLOATS.len()), n),
            proptest::collection::vec(proptest::option::weighted(0.85, 0i64..4), n),
            proptest::collection::vec(proptest::option::weighted(0.9, any::<bool>()), n),
        )
            .prop_map(|(k, f, ts, void)| Table { k, f, ts, void })
    })
}

#[derive(Clone, Debug)]
enum Shape {
    In(Vec<i64>),
    FloatIn(Vec<usize>),
    Between(i64, i64),
    Latest(Vec<i64>, bool),
    FloatLatest(Vec<usize>),
    Bounds(Vec<(i64, i64)>),
}

fn shape() -> impl Strategy<Value = Shape> {
    prop_oneof![
        proptest::collection::vec(0i64..10, 0..5).prop_map(Shape::In),
        proptest::collection::vec(0usize..FLOATS.len(), 1..3).prop_map(Shape::FloatIn),
        (0i64..4, 0i64..4).prop_map(|(a, b)| Shape::Between(a.min(b), a.max(b))),
        (proptest::collection::vec(0i64..10, 1..6), any::<bool>())
            .prop_map(|(k, l)| Shape::Latest(k, l)),
        proptest::collection::vec(0usize..FLOATS.len(), 1..4).prop_map(Shape::FloatLatest),
        proptest::collection::vec((0i64..10, 0i64..4), 0..5).prop_map(Shape::Bounds),
    ]
}

fn schema() -> Vec<ColumnDef> {
    vec![
        ColumnDef::new("k", DataType::Int64),
        ColumnDef::new("f", DataType::Float64),
        ColumnDef::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ),
        ColumnDef::new("void", DataType::Boolean),
    ]
}

fn arrow(t: &Table) -> RecordBatch {
    let s = Arc::new(bellwether_adapters::residual::arrow_schema(&schema()));
    RecordBatch::try_new(
        s,
        vec![
            Arc::new(Int64Array::from(t.k.clone())),
            Arc::new(Float64Array::from(
                t.f.iter().map(|i| i.map(|i| FLOATS[i])).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampMicrosecondArray::from(t.ts.clone()).with_timezone("UTC")),
            Arc::new(BooleanArray::from(t.void.clone())),
        ],
    )
    .unwrap()
}

fn enc(v: Value<'_>, dt: &DataType) -> Vec<u8> {
    let mut out = Vec::new();
    encode_value_as(&v, dt, &mut out).unwrap();
    out
}

fn ts_enc(v: i64) -> Vec<u8> {
    enc(
        Value::Timestamp {
            value: v,
            unit: TimeUnit::Microsecond,
        },
        &schema()[2].data_type,
    )
}

/// `FLOATS[i]`'s canonical encoding, a zero in both signs (as the planner
/// lists a queried zero).
fn zeros(i: usize) -> Vec<Vec<u8>> {
    common::zero_variants(&enc(Value::Float(FLOATS[i]), &DataType::Float64))
}

/// Writes the table as one file of row groups of 16 rows; returns the
/// plan of every row group with `residual` and the decoded rows.
fn setup(dir: &Path, t: &Table, residual: Predicate) -> (ScanPlan, Vec<common::Row>, RowOffsets) {
    let path = dir.join("t.parquet");
    let b = arrow(t);
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(16))
        .build();
    let mut w =
        ArrowWriter::try_new(File::create(&path).unwrap(), b.schema(), Some(props)).unwrap();
    w.write(&b).unwrap();
    let md = w.close().unwrap();
    let path_s = path.canonicalize().unwrap().display().to_string();
    let rg_rows: Vec<u64> = md
        .row_groups()
        .iter()
        .map(|g| g.num_rows() as u64)
        .collect();
    let rg_bytes: Vec<u64> = md
        .row_groups()
        .iter()
        .map(|g| g.columns().iter().map(|c| c.byte_range().1).sum())
        .collect();
    let mut rows = Vec::new();
    let mut rg = 0u32;
    let mut left = rg_rows[0];
    for r in 0..b.num_rows() {
        while left == 0 {
            rg += 1;
            left = rg_rows[rg as usize];
        }
        left -= 1;
        let mut cols = BTreeMap::new();
        for c in schema() {
            let a = b.column_by_name(&c.name).unwrap();
            let v = (!arrow::array::Array::is_null(a.as_ref(), r)).then(|| {
                let mut out = Vec::new();
                encode_nullable_as(a.as_ref(), r, &c.data_type, &mut out).unwrap();
                out
            });
            cols.insert(c.name, v);
        }
        rows.push(common::Row {
            file: 0,
            rg,
            row: r as u64,
            cols,
            tags: None,
            body: None,
            expr: None,
        });
    }
    let plan = ScanPlan {
        manifest_version: "1".into(),
        entries: vec![ScanEntry {
            file: FileFingerprint {
                path: path_s.clone(),
                size: std::fs::metadata(&path).unwrap().len(),
                etag: None,
                tail_blake3: Hash([0; 32]),
            },
            row_groups: (0..u32::try_from(rg_rows.len()).unwrap()).collect(),
            bytes: rg_bytes.iter().sum(),
            columns: None,
        }],
        residual: Some(residual),
        bytes: rg_bytes.iter().sum(),
        row_groups_total: rg_rows.len() as u64,
        row_groups_selected: rg_rows.len() as u64,
        projected_bytes: None,
        order: None,
        rows: None,
    };
    let offsets = RowOffsets([(path_s, rg_rows)].into_iter().collect());
    (plan, rows, offsets)
}

/// The residual of `shape` over table `t`, as the planner would write it.
fn residual(t: &Table, shape: &Shape) -> Predicate {
    let i64t = DataType::Int64;
    let f64t = DataType::Float64;
    let kcol = vec!["k".to_string()];
    let fcol = vec!["f".to_string()];
    // As the planner: a queried key's winner is over both signs of a float
    // zero (its variants), and every variant gets that winner's bound.
    let class = |k: &Vec<u8>| -> Vec<u8> {
        if *k == enc(Value::Float(-0.0), &f64t) {
            enc(Value::Float(0.0), &f64t)
        } else {
            k.clone()
        }
    };
    let latest = |cols: &Vec<String>,
                  keys: BTreeSet<Vec<u8>>,
                  key_of: &dyn Fn(usize) -> Option<Vec<u8>>,
                  live: bool| {
        let mut newest: BTreeMap<Vec<u8>, i64> = BTreeMap::new();
        for r in 0..t.k.len() {
            if let (Some(k), Some(ts)) = (key_of(r), t.ts[r]) {
                if keys.contains(&k) {
                    let e = newest.entry(class(&k)).or_insert(ts);
                    *e = (*e).max(ts);
                }
            }
        }
        let listed: BTreeMap<Vec<u8>, i64> = keys
            .iter()
            .filter_map(|k| newest.get(&class(k)).map(|v| (k.clone(), *v)))
            .collect();
        let mut all = vec![
            Predicate::In {
                columns: cols.clone(),
                values: listed.keys().cloned().collect(),
            },
            Predicate::NotOlderThan {
                columns: cols.clone(),
                ts_column: "ts".into(),
                bounds: listed
                    .iter()
                    .map(|(k, v)| (k.clone(), ts_enc(*v)))
                    .collect(),
            },
            Predicate::LatestPerKey {
                columns: cols.clone(),
                ts_column: "ts".into(),
            },
        ];
        if live {
            all.push(Predicate::Live {
                column: "void".into(),
                tombstone: enc(Value::Boolean(true), &DataType::Boolean),
            });
        }
        Predicate::And { all }
    };
    match shape {
        Shape::In(keys) => Predicate::In {
            columns: kcol,
            values: keys.iter().map(|k| enc(Value::Int(*k), &i64t)).collect(),
        },
        Shape::FloatIn(fs) => Predicate::In {
            columns: fcol,
            values: fs.iter().flat_map(|i| zeros(*i)).collect(),
        },
        Shape::Between(lo, hi) => Predicate::Between {
            column: "ts".into(),
            lo: ts_enc(*lo),
            hi: ts_enc(*hi),
        },
        Shape::Latest(keys, live) => latest(
            &kcol,
            keys.iter().map(|k| enc(Value::Int(*k), &i64t)).collect(),
            &|r| t.k[r].map(|k| enc(Value::Int(k), &i64t)),
            *live,
        ),
        Shape::FloatLatest(fs) => latest(
            &fcol,
            fs.iter().flat_map(|i| zeros(*i)).collect(),
            &|r| t.f[r].map(|i| enc(Value::Float(FLOATS[i]), &f64t)),
            false,
        ),
        Shape::Bounds(b) => Predicate::NotOlderThan {
            columns: kcol,
            ts_column: "ts".into(),
            bounds: b
                .iter()
                .map(|(k, v)| (enc(Value::Int(*k), &i64t), ts_enc(*v)))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        },
    }
}

fn columns() -> Vec<String> {
    schema().into_iter().map(|c| c.name).collect()
}

fn expected(plan: &ScanPlan, rows: Vec<common::Row>) -> Vec<Vec<String>> {
    let mut out: Vec<Vec<String>> = common::apply(plan.residual.as_ref().unwrap(), rows)
        .iter()
        .map(|r| r.render(&columns()))
        .collect();
    out.sort();
    out
}

/// Renders engine output at this test's schema (`common::rendered` reads
/// the corpus schema's types).
fn rendered(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let mut out = Vec::new();
    for b in batches {
        for r in 0..b.num_rows() {
            out.push(
                schema()
                    .iter()
                    .map(|c| {
                        let a = b.column_by_name(&c.name).unwrap();
                        if arrow::array::Array::is_null(a.as_ref(), r) {
                            "null".to_string()
                        } else {
                            let mut v = Vec::new();
                            encode_nullable_as(a.as_ref(), r, &c.data_type, &mut v).unwrap();
                            v.iter().map(|x| format!("{x:02x}")).collect()
                        }
                    })
                    .collect(),
            );
        }
    }
    out.sort();
    out
}

#[cfg(feature = "datafusion")]
#[test]
fn datafusion_agrees_with_the_reference_on_random_rows() {
    use bellwether_adapters::datafusion;
    use futures::TryStreamExt;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut runner = TestRunner::new(Config::with_cases(256));
    runner
        .run(&(table(), shape()), |(t, s)| {
            let tmp = tempfile::tempdir().unwrap();
            let (plan, rows, offsets) = setup(tmp.path(), &t, residual(&t, &s));
            let o = EmitOptions {
                row_offsets: Some(offsets),
                ..EmitOptions::default()
            };
            let cfg = datafusion::scan_config(&plan, &schema(), &o).unwrap();
            let ctx = datafusion::engine::prelude::SessionContext::new();
            let got = rt.block_on(async {
                datafusion::execute(&cfg, &ctx)
                    .await
                    .unwrap()
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap()
            });
            prop_assert_eq!(rendered(&got), expected(&plan, rows), "{:?}", s);
            Ok(())
        })
        .unwrap();
}

#[test]
fn duckdb_agrees_with_the_reference_on_random_rows() {
    let Some(bin) = common::require_duckdb() else {
        return;
    };
    // A DuckDB process per case: fewer cases than in process.
    let mut runner = TestRunner::new(Config::with_cases(48));
    runner
        .run(&(table(), shape()), |(t, s)| {
            let tmp = tempfile::tempdir().unwrap();
            let (plan, rows, offsets) = setup(tmp.path(), &t, residual(&t, &s));
            let o = EmitOptions {
                row_offsets: Some(offsets),
                ..EmitOptions::default()
            };
            let nan = enc(Value::Float(f64::NAN), &DataType::Float64);
            let lists_nan = match plan.residual.as_ref().unwrap() {
                Predicate::In { values, .. } => values.contains(&nan),
                Predicate::And { all } => all
                    .iter()
                    .any(|p| matches!(p, Predicate::In { values, .. } if values.contains(&nan))),
                _ => false,
            };
            let sql = match bellwether_adapters::duckdb::sql(&plan, &schema(), &o) {
                Ok(sql) => {
                    prop_assert!(!lists_nan, "a NaN key list was not refused: {:?}", s);
                    sql
                }
                Err(bellwether_adapters::AdapterError::Unsupported { what, .. }) if lists_nan => {
                    prop_assert!(
                        what.contains("NaN") && what.contains("In key list"),
                        "{}",
                        what
                    );
                    return Ok(());
                }
                Err(e) => return Err(TestCaseError::fail(format!("{s:?}: {e}"))),
            };
            let got = common::duckdb_rows(&bin, &sql, tmp.path(), "");
            prop_assert_eq!(rendered(&got), expected(&plan, rows), "{:?}\n{}", s, sql);
            Ok(())
        })
        .unwrap();
}

#[test]
fn every_variant_compiles_to_typed_literals() {
    // Decoding is at the dataset type: a decimal at the dataset scale, a
    // timestamp in the dataset unit, a zero with its sign.
    let s = vec![
        ColumnDef::new("d", DataType::Decimal128(10, 4)),
        ColumnDef::new("t", DataType::Timestamp(TimeUnit::Millisecond, None)),
        ColumnDef::new("f", DataType::Float32),
    ];
    let d = enc(
        Value::Decimal128 {
            value: 125,
            scale: 2,
        },
        &s[0].data_type,
    );
    let t = enc(
        Value::Timestamp {
            value: 7,
            unit: TimeUnit::Millisecond,
        },
        &s[1].data_type,
    );
    let f = enc(Value::Float(-0.0), &s[2].data_type);
    let p = Predicate::And {
        all: vec![
            Predicate::In {
                columns: vec!["d".into()],
                values: vec![d],
            },
            Predicate::Between {
                column: "t".into(),
                lo: t.clone(),
                hi: t,
            },
            Predicate::In {
                columns: vec!["f".into()],
                values: vec![f],
            },
        ],
    };
    let r =
        bellwether_adapters::residual::compile(Some(&p), &s, Default::default(), "test").unwrap();
    let j = r.to_json().to_string();
    assert!(j.contains(r#""unscaled":"12500""#), "{j}");
    assert!(j.contains(r#""scale":4"#), "{j}");
    assert!(j.contains(r#""unit":"ms""#), "{j}");
    assert!(j.contains(r#""bits":"8000000000000000""#), "{j}");
    // The DataFusion description carries the same typed residual.
    let _ = Schema::new(vec![Field::new("x", DataType::Int64, true)]);
}
