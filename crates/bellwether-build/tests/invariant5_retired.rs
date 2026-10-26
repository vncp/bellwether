//! Invariant 5 through the plan when a file is rewritten so its old
//! row-group ids retire while an old segment still holds the key's newest
//! version: latest and asof give the live answer. Also: the newest version
//! pruned by another spec, with asof.
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

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_build::{BuildOptions, Builder, CompactOptions, Lookup, Planner, SourceKind};
use bellwether_core::encoding::Value;
use bellwether_store::ObjectStoreBackend;
use bellwether_types::{KeySpec, Predicate};
use common::{apply_residual, cols, memory_store, parquet_rows, Row, Scratch};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
    ]))
}

/// One Parquet file whose row groups are exactly `groups` of (id, region, ts).
fn write(path: &std::path::Path, groups: &[&[(i64, &str, i64)]]) {
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(1 << 20))
        .build();
    let mut w =
        ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema(), Some(props)).unwrap();
    for g in groups {
        let b = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(g.iter().map(|r| r.0).collect::<Vec<_>>())),
                Arc::new(StringArray::from(g.iter().map(|r| r.1).collect::<Vec<_>>())),
                Arc::new(TimestampMicrosecondArray::from(
                    g.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        w.write(&b).unwrap();
        w.flush().unwrap();
    }
    w.close().unwrap();
}

fn temporal() -> KeySpec {
    KeySpec::Temporal {
        columns: vec!["id".into()],
        ts_column: "ts".into(),
        counts: false,
    }
}

fn exact(c: &[&str]) -> KeySpec {
    KeySpec::Exact {
        columns: c.iter().map(|s| (*s).to_string()).collect(),
        counts: false,
        bounds: None,
    }
}

fn opts() -> BuildOptions {
    BuildOptions {
        schema: Some(schema()),
        ..BuildOptions::new(SourceKind::Parquet)
    }
}

/// (path, ts) of the rows a latest/asof lookup of `id` returns, after the
/// residual, over the planned row groups.
fn answer(
    p: &Planner<ObjectStoreBackend>,
    rows: &[Row],
    specs: &[KeySpec],
    id: i64,
    asof: Option<i64>,
) -> BTreeSet<(String, i64)> {
    let key = p.encode_tuple(&cols(&["id"]), &[Value::Int(id)]).unwrap();
    let lookup = match asof {
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
    let plan = p.plan(specs, &lookup).unwrap();
    let rgs: BTreeSet<(String, u32)> = plan
        .entries
        .iter()
        .flat_map(|e| e.row_groups.iter().map(move |rg| (e.file.path.clone(), rg)))
        .collect();
    let planned: Vec<Row> = rows
        .iter()
        .filter(|r| rgs.contains(&(r.path.clone(), r.rg)))
        .cloned()
        .collect();
    apply_residual(plan.residual.as_ref().unwrap(), planned)
        .iter()
        .map(|r| {
            let ts = r.get("ts").unwrap();
            // canonical Timestamp(µs): 08 04 03 + i64 BE with sign bit flipped
            let mut b = [0u8; 8];
            b.copy_from_slice(&ts[3..11]);
            b[0] ^= 0x80;
            (r.path.clone(), i64::from_be_bytes(b))
        })
        .collect()
}

fn rows_of(paths: &[&std::path::Path]) -> Vec<Row> {
    paths
        .iter()
        .flat_map(|p| parquet_rows(p, &schema()))
        .collect()
}

#[test]
fn a_retired_newest_version_never_hides_the_live_answer() {
    let s = Scratch::new();
    let (f, g) = (s.path("f.parquet"), s.path("g.parquet"));
    let (fs, gs) = (s.path_str("f.parquet"), s.path_str("g.parquet"));
    // v1: key 7's newest version (ts 100) is in f; an older one (ts 10) in g
    write(&f, &[&[(7, "x", 100), (8, "x", 5)]]);
    write(&g, &[&[(7, "x", 10), (9, "x", 1)], &[(11, "x", 100)]]);
    let store = memory_store();
    let b = Builder::new(Arc::clone(&store), "idx");
    let specs = [temporal(), exact(&["id"])];
    b.build(&s.root_str(), &specs, &opts()).unwrap();
    {
        let p = Planner::open(Arc::clone(&store), "idx").unwrap();
        let rows = rows_of(&[&f, &g]);
        assert_eq!(
            answer(&p, &rows, &[temporal()], 7, None),
            [(fs.clone(), 100)].into()
        );
    }
    // v2: f rewritten WITHOUT key 7; its old id retires, but the segment of
    // build 1 still holds (100, retired id) in key 7's chain
    write(&f, &[&[(8, "x", 5), (10, "x", 3)]]);
    let r = b.build(&s.root_str(), &specs, &opts()).unwrap();
    assert_eq!((r.files_changed, r.row_groups_retired), (1, 1));
    let m = b.manifest().unwrap().unwrap();
    assert!(
        m.segments.len() >= 3,
        "the pre-rewrite segments are still referenced"
    );
    let rows = rows_of(&[&f, &g]);
    let p = Planner::open(Arc::clone(&store), "idx").unwrap();
    for specs in [vec![temporal()], vec![temporal(), exact(&["id"])]] {
        // latest and asof give g's live version, never nothing
        assert_eq!(
            answer(&p, &rows, &specs, 7, None),
            [(gs.clone(), 10)].into()
        );
        assert_eq!(
            answer(&p, &rows, &specs, 7, Some(200)),
            [(gs.clone(), 10)].into()
        );
        assert_eq!(
            answer(&p, &rows, &specs, 7, Some(150)),
            [(gs.clone(), 10)].into()
        );
        assert_eq!(answer(&p, &rows, &specs, 7, Some(9)), BTreeSet::new());
        // a key untouched by the rewrite
        assert_eq!(
            answer(&p, &rows, &specs, 11, None),
            [(gs.clone(), 100)].into()
        );
    }

    // v3: f rewritten again, now with an OLDER live version of key 7 (50)
    // than the retired one (100) and a newer one than g's (10)
    write(&f, &[&[(7, "x", 50), (8, "x", 5)], &[(7, "x", 40)]]);
    b.build(&s.root_str(), &specs, &opts()).unwrap();
    let rows = rows_of(&[&f, &g]);
    let p = Planner::open(Arc::clone(&store), "idx").unwrap();
    for specs in [vec![temporal()], vec![temporal(), exact(&["id"])]] {
        assert_eq!(
            answer(&p, &rows, &specs, 7, None),
            [(fs.clone(), 50)].into()
        );
        assert_eq!(
            answer(&p, &rows, &specs, 7, Some(150)),
            [(fs.clone(), 50)].into()
        );
        assert_eq!(
            answer(&p, &rows, &specs, 7, Some(45)),
            [(fs.clone(), 40)].into()
        );
        assert_eq!(
            answer(&p, &rows, &specs, 7, Some(20)),
            [(gs.clone(), 10)].into()
        );
    }
    // compaction keeps the retired ids inside the merged segment; answers
    // must not change
    let c = b.compact(&CompactOptions::default()).unwrap();
    assert!(c.merges >= 1, "{c:?}");
    let p = Planner::open(Arc::clone(&store), "idx").unwrap();
    for specs in [vec![temporal()], vec![temporal(), exact(&["id"])]] {
        assert_eq!(
            answer(&p, &rows, &specs, 7, None),
            [(fs.clone(), 50)].into()
        );
        assert_eq!(
            answer(&p, &rows, &specs, 7, Some(150)),
            [(fs.clone(), 50)].into()
        );
        assert_eq!(
            answer(&p, &rows, &specs, 7, Some(45)),
            [(fs.clone(), 40)].into()
        );
    }
}

#[test]
fn a_retired_tie_still_returns_the_live_rows_at_that_ts() {
    let s = Scratch::new();
    let (f, g) = (s.path("f.parquet"), s.path("g.parquet"));
    let gs = s.path_str("g.parquet");
    write(&f, &[&[(7, "x", 100)]]);
    write(&g, &[&[(7, "x", 100), (7, "y", 1)]]);
    let store = memory_store();
    let b = Builder::new(Arc::clone(&store), "idx");
    b.build(&s.root_str(), &[temporal()], &opts()).unwrap();
    write(&f, &[&[(8, "x", 100)]]);
    b.build(&s.root_str(), &[temporal()], &opts()).unwrap();
    let rows = rows_of(&[&f, &g]);
    let p = Planner::open(Arc::clone(&store), "idx").unwrap();
    assert_eq!(
        answer(&p, &rows, &[temporal()], 7, None),
        [(gs.clone(), 100)].into()
    );
    assert_eq!(
        answer(&p, &rows, &[temporal()], 7, Some(99)),
        [(gs, 1)].into()
    );
}

/// Signed zero on a float temporal key: the two encodings are one key
/// under SQL equality. -0.0's newest version is retired; +0.0's live one
/// must win, and -0.0's live older version must not.
#[test]
fn a_retired_zero_variant_does_not_bound_the_other_zero() {
    let fschema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Float64, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
    ]));
    let w = |path: &std::path::Path, rows: &[(f64, i64)]| {
        let b = RecordBatch::try_new(
            Arc::clone(&fschema),
            vec![
                Arc::new(Float64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(TimestampMicrosecondArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        common::write_parquet(path, &b, 1 << 20);
    };
    let s = Scratch::new();
    let (f, g) = (s.path("f.parquet"), s.path("g.parquet"));
    let gs = s.path_str("g.parquet");
    w(&f, &[(-0.0, 100)]);
    w(&g, &[(0.0, 50), (-0.0, 20)]);
    let store = memory_store();
    let b = Builder::new(Arc::clone(&store), "idx");
    let t = KeySpec::Temporal {
        columns: vec!["k".into()],
        ts_column: "ts".into(),
        counts: false,
    };
    let o = BuildOptions {
        schema: Some(Arc::clone(&fschema)),
        ..BuildOptions::new(SourceKind::Parquet)
    };
    b.build(&s.root_str(), std::slice::from_ref(&t), &o)
        .unwrap();
    w(&f, &[(1.0, 100)]);
    b.build(&s.root_str(), std::slice::from_ref(&t), &o)
        .unwrap();
    let rows: Vec<Row> = [&f, &g]
        .iter()
        .flat_map(|p| parquet_rows(p, &fschema))
        .collect();
    let p = Planner::open(Arc::clone(&store), "idx").unwrap();
    for probe in [0.0, -0.0] {
        let key = p
            .encode_tuple(&cols(&["k"]), &[Value::Float(probe)])
            .unwrap();
        let plan = p
            .plan(
                std::slice::from_ref(&t),
                &Lookup::Latest {
                    columns: cols(&["k"]),
                    keys: vec![key],
                },
            )
            .unwrap();
        let rgs: BTreeSet<(String, u32)> = plan
            .entries
            .iter()
            .flat_map(|e| e.row_groups.iter().map(move |rg| (e.file.path.clone(), rg)))
            .collect();
        let got = apply_residual(
            plan.residual.as_ref().unwrap(),
            rows.iter()
                .filter(|r| rgs.contains(&(r.path.clone(), r.rg)))
                .cloned()
                .collect(),
        );
        assert_eq!(got.len(), 1, "latest({probe}) = {got:?}");
        assert_eq!(got[0].path, gs);
        assert_eq!(got[0].row, 0, "the (+0.0, 50) row");
    }
}

/// Invariant 5 with asof: key 1's asof winner sits in a row group the
/// region spec prunes, and a version newer than the asof instant sits in
/// a third row group; the residual must drop the stale east row and keep
/// key 2's.
#[test]
fn invariant5_asof_winner_in_a_pruned_row_group_bounds_older_rows() {
    let s = Scratch::new();
    let f = s.path("l.parquet");
    write(
        &f,
        &[
            &[(1, "east", 10), (2, "east", 10)],
            &[(1, "west", 20)],
            &[(1, "east", 30)],
        ],
    );
    let store = memory_store();
    let b = Builder::new(Arc::clone(&store), "idx");
    let specs = [temporal(), exact(&["region"])];
    b.build(&s.root_str(), &specs, &opts()).unwrap();
    let rows = rows_of(&[&f]);
    let p = Planner::open(Arc::clone(&store), "idx").unwrap();
    let keys = vec![
        p.encode_tuple(
            &cols(&["id", "region"]),
            &[Value::Int(1), Value::Utf8("east")],
        )
        .unwrap(),
        p.encode_tuple(
            &cols(&["id", "region"]),
            &[Value::Int(2), Value::Utf8("east")],
        )
        .unwrap(),
    ];
    let plan = p
        .plan(
            &specs,
            &Lookup::AsOf {
                columns: cols(&["id", "region"]),
                keys,
                ts: 25,
            },
        )
        .unwrap();
    let rgs: BTreeSet<u32> = plan
        .entries
        .iter()
        .flat_map(|e| e.row_groups.iter())
        .collect();
    let Some(Predicate::And { all }) = &plan.residual else {
        panic!()
    };
    let bounds = all
        .iter()
        .find_map(|x| match x {
            Predicate::NotOlderThan { bounds, .. } => Some(bounds.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!(bounds.len(), 2, "{bounds:?}");
    let got = apply_residual(
        plan.residual.as_ref().unwrap(),
        rows.iter()
            .filter(|r| rgs.contains(&r.rg))
            .cloned()
            .collect(),
    );
    assert_eq!(got.len(), 1, "{got:?} from row groups {rgs:?}");
    assert_eq!(got[0].rg, 0);
    assert_eq!(got[0].row, 1, "key 2's east row only");
}
