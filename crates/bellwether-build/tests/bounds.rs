//! WP21 item 4: per-key time bounds (`exact:symbol@ts`), a `bounds` sidecar
//! beside an exact spec. Ground truth: rows decoded with the `parquet`
//! crate; the expected plan is computed from them, never from the index.

#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_build::{BuildOptions, Lookup, SourceKind};
use bellwether_core::encoding::ts_value_as;
use bellwether_core::sidecar::{SidecarDef, SidecarKind};
use bellwether_store::{commit, StagedSegments};
use common::*;

/// Year 9999-12-31T23:59:59.999999 in microseconds: a common "open end"
/// sentinel.
const END_OF_TIME: i64 = 253_402_300_799_999_999;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("size", DataType::Int64, false),
    ]))
}

fn bounds_def() -> SidecarDef {
    SidecarDef {
        kind: SidecarKind::Bounds,
        columns: vec!["symbol".into()],
        time_columns: vec!["ts".into()],
    }
}

fn opts(sidecars: bool) -> BuildOptions {
    BuildOptions {
        sidecars: if sidecars {
            vec![bounds_def()]
        } else {
            Vec::new()
        },
        ..BuildOptions::new(SourceKind::Parquet)
    }
}

type Print = (Option<&'static str>, Option<i64>);

/// Rows sorted by symbol then time, so each symbol's rows straddle row
/// groups of `per_rg` rows; a late print, nulls, and sentinels mixed in.
fn write(s: &Scratch, name: &str, rows: &[Print], per_rg: usize) {
    let batch = RecordBatch::try_new(
        schema(),
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
    write_parquet(&s.path(name), &batch, per_rg);
}

fn corpus(s: &Scratch) {
    // Day 1: AAPL all day (straddles three row groups), MSFT a morning and
    // a sentinel, a null-time AAPL print, and a null symbol.
    write(
        s,
        "d1.parquet",
        &[
            (Some("AAPL"), Some(100)),
            (Some("AAPL"), Some(200)),
            (Some("AAPL"), Some(300)),
            (Some("AAPL"), Some(400)),
            (Some("AAPL"), None),
            (Some("AAPL"), Some(600)),
            (Some("MSFT"), Some(150)),
            (Some("MSFT"), Some(END_OF_TIME)),
            (None, Some(250)),
        ],
        2,
    );
    // Day 2: a late AAPL print from day 1 beside today's MSFT, and a
    // pre-1970 print.
    write(
        s,
        "d2.parquet",
        &[
            (Some("AAPL"), Some(-5)),
            (Some("AAPL"), Some(1_000)),
            (Some("MSFT"), Some(1_100)),
            (Some("MSFT"), Some(1_200)),
            (Some("NVDA"), Some(1_150)),
            (Some("NVDA"), Some(1_250)),
        ],
        // Three to a row group, so the first holds AAPL and MSFT: its time
        // range covers MSFT's 1_100, AAPL's own does not.
        3,
    );
}

fn ts_us(row: &Row) -> Option<i64> {
    let bytes = row.get("ts")?;
    // Decode the canonical Int64 tail of a timestamp encoding.
    let tail: [u8; 8] = bytes[bytes.len() - 8..].try_into().unwrap();
    Some((u64::from_be_bytes(tail) ^ (1 << 63)) as i64)
}

/// Every row with its symbol and microseconds.
fn rows(s: &Scratch) -> Vec<Row> {
    let mut out = parquet_rows(&s.path("d1.parquet"), &schema());
    out.extend(parquet_rows(&s.path("d2.parquet"), &schema()));
    out
}

#[test]
fn ts_decoding_in_this_test_matches_the_encoder() {
    // The oracle's decoding of canonical timestamps agrees with core.
    let arr =
        TimestampMicrosecondArray::from(vec![Some(-5), Some(END_OF_TIME)]).with_timezone("UTC");
    let dt = schema().field(1).data_type().clone();
    assert_eq!(ts_value_as(&arr, 1, &dt).unwrap(), Some(END_OF_TIME));
    let mut enc = Vec::new();
    bellwether_core::encoding::encode_value_as(
        &bellwether_core::encoding::Value::Timestamp {
            value: -5,
            unit: TimeUnit::Microsecond,
        },
        &dt,
        &mut enc,
    )
    .unwrap();
    let r = Row {
        path: String::new(),
        rg: 0,
        row: 0,
        cols: [("ts".to_string(), Some(enc))].into(),
    };
    assert_eq!(ts_us(&r), Some(-5));
}

/// Per (key, row group) the min and max non-null time: the bounds oracle.
fn key_bounds(rows: &[Row]) -> BTreeMap<(Vec<u8>, String, u32), (i64, i64)> {
    let mut out: BTreeMap<(Vec<u8>, String, u32), (i64, i64)> = BTreeMap::new();
    for r in rows {
        let (Some(k), Some(t)) = (r.get("symbol"), ts_us(r)) else {
            continue;
        };
        let e = out
            .entry((k.clone(), r.path.clone(), r.rg))
            .or_insert((t, t));
        e.0 = e.0.min(t);
        e.1 = e.1.max(t);
    }
    out
}

fn window(columns: &[&str], keys: &[&str], lo: i64, hi: i64) -> Lookup {
    Lookup::Window {
        columns: cols(columns),
        keys: keys.iter().map(|k| enc_str(k)).collect(),
        ts_column: "ts".into(),
        lo,
        hi,
    }
}

#[test]
fn a_window_plans_exactly_the_row_groups_where_the_key_s_own_times_overlap() {
    let s = Scratch::new();
    corpus(&s);
    let all = rows(&s);
    let bounds = key_bounds(&all);
    let with = memory_store();
    let specs = [exact(&["symbol"]), range("ts")];
    builder(&with)
        .build(&s.root_str(), &specs, &opts(true))
        .unwrap();
    let without = memory_store();
    builder(&without)
        .build(&s.root_str(), &specs, &opts(false))
        .unwrap();
    let (p, q) = (planner(&with), planner(&without));
    assert_eq!(p.sidecar_completeness(&bounds_def()), Some(1));

    let points = [
        i64::MIN + 1,
        -5,
        0,
        100,
        150,
        199,
        250,
        300,
        400,
        600,
        1_000,
        1_050,
        1_100,
        1_150,
        END_OF_TIME,
        i64::MAX,
    ];
    let key_sets: [&[&str]; 4] = [&["AAPL"], &["MSFT"], &["AAPL", "MSFT", "NVDA"], &["ZZZZ"]];
    let mut pruned_beyond_range = 0;
    for &lo in &points {
        for &hi in &points {
            for keys in key_sets {
                let l = window(&["symbol"], keys, lo, hi);
                let plan = p.plan(&specs, &l).unwrap();
                let base = q.plan(&specs, &l).unwrap();
                let key_bytes: BTreeSet<Vec<u8>> = keys.iter().map(|k| enc_str(k)).collect();
                // The bounds oracle: (file, rg) where a queried key's own
                // [min, max] overlaps the window.
                let want: BTreeSet<(String, u32)> = bounds
                    .iter()
                    .filter(|((k, _, _), (a, b))| {
                        lo <= hi && key_bytes.contains(k) && *a <= hi && lo <= *b
                    })
                    .map(|((_, f, rg), _)| (f.clone(), *rg))
                    .collect();
                assert_eq!(plan_rgs(&plan), want, "{keys:?} [{lo}, {hi}]");
                // Never more than the key spec and range spec together.
                assert!(plan_rgs(&plan).is_subset(&plan_rgs(&base)));
                pruned_beyond_range += plan_rgs(&base).len() - plan_rgs(&plan).len();
                // The residual makes it row-exact, and no matching row is
                // lost (invariant 1).
                let truth: Vec<Row> = all
                    .iter()
                    .filter(|r| {
                        r.get("symbol").is_some_and(|k| key_bytes.contains(k))
                            && ts_us(r).is_some_and(|t| lo <= t && t <= hi)
                    })
                    .cloned()
                    .collect();
                let got =
                    apply_residual(plan.residual.as_ref().unwrap(), planned_rows(&plan, &all));
                assert_eq!(sorted(got), sorted(truth.clone()), "{keys:?} [{lo}, {hi}]");
                let got =
                    apply_residual(base.residual.as_ref().unwrap(), planned_rows(&base, &all));
                assert_eq!(
                    sorted(got),
                    sorted(truth),
                    "{keys:?} [{lo}, {hi}] without bounds"
                );
            }
        }
    }
    assert!(
        pruned_beyond_range > 0,
        "the sidecar pruned nothing the range key kept"
    );
}

#[test]
fn an_incomplete_sidecar_is_not_used_and_unknown_kinds_are_kept_and_ignored() {
    let s = Scratch::new();
    corpus(&s);
    let all = rows(&s);
    let store = memory_store();
    let specs = [exact(&["symbol"])];
    builder(&store)
        .build(&s.root_str(), &specs, &opts(true))
        .unwrap();
    // Another writer adds a segment and a declaration of a kind this binary
    // does not know, under the exact spec's own name.
    let m = builder(&store).manifest().unwrap().unwrap();
    let foreign = bellwether_store::SegmentRef {
        kind: Some("counts".into()),
        ..m.segments[0].clone()
    };
    let decl = bellwether_store::SidecarDecl {
        kind: "counts".into(),
        columns: vec!["symbol".into()],
        ..Default::default()
    };
    commit(&*store, "idx", &StagedSegments::new(), |cur| {
        let mut next = cur.clone();
        next.segments.push(foreign.clone());
        next.schema.sidecars.push(decl.clone());
        Ok(next)
    })
    .unwrap();
    let p = planner(&store);
    assert_eq!(
        p.manifest().unknown_segment_kinds(),
        vec!["counts".to_string()]
    );
    let l = window(&["symbol"], &["AAPL"], 100, 200);
    let plan = p.plan(&specs, &l).unwrap();
    let got = apply_residual(plan.residual.as_ref().unwrap(), planned_rows(&plan, &all));
    assert_eq!(got.len(), 2);
    // A rebuild over a new file keeps the unknown segment and declaration
    // untouched and still completes every known target.
    write(&s, "d3.parquet", &[(Some("AAPL"), Some(5_000))], 2);
    builder(&store)
        .build(
            &s.root_str(),
            &specs,
            &BuildOptions::new(SourceKind::Parquet),
        )
        .unwrap();
    let p = planner(&store);
    let m = p.manifest();
    assert!(m.segments.iter().any(|x| x == &foreign));
    assert!(m.schema.sidecars.iter().any(|d| d.kind == "counts"));
    assert_eq!(
        p.sidecar_completeness(&bounds_def()),
        Some(1),
        "continuously complete"
    );

    // Dropping the sidecar's coverage of a row group makes it incomplete:
    // it is no longer used, and the plan is still correct.
    commit(&*store, "idx", &StagedSegments::new(), |cur| {
        let mut next = cur.clone();
        let victim = next
            .segments
            .iter()
            .position(|x| x.kind.as_deref() == Some("bounds"))
            .unwrap();
        next.segments.remove(victim);
        Ok(next)
    })
    .unwrap();
    let p = planner(&store);
    assert_eq!(p.sidecar_completeness(&bounds_def()), None);
    assert_eq!(p.sidecars_for(&l), Vec::<SidecarDef>::new());
    let all = {
        let mut a = all;
        a.extend(parquet_rows(&s.path("d3.parquet"), &schema()));
        a
    };
    let plan = p.plan(&specs, &l).unwrap();
    let got = apply_residual(plan.residual.as_ref().unwrap(), planned_rows(&plan, &all));
    assert_eq!(got.len(), 2);
}
