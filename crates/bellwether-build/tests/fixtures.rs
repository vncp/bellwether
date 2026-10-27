//! Builds over the shared source fixtures (`fixtures/source/`), checking the
//! manifest the build writes and every plan against ground truth decoded
//! independently of the code under test.

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

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_build::{completeness, BuildOptions, Lookup, SourceKind};
use bellwether_core::encoding::Value;
use common::*;

fn expected_bytes(file: &str) -> Vec<u64> {
    let raw = std::fs::read_to_string(fixture("expected.json")).unwrap();
    let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
    let f = v["files"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["file"] == file)
        .unwrap();
    f["row_groups"]
        .as_array()
        .unwrap()
        .iter()
        .map(|rg| rg["bytes"].as_u64().unwrap())
        .collect()
}

fn dec(v: i128) -> Vec<u8> {
    enc(
        &Value::Decimal128 { value: v, scale: 2 },
        &DataType::Decimal128(10, 2),
    )
}

#[test]
fn multi_rg_exact_range_bloom_composite_and_prefix() {
    let s = Scratch::new();
    let path = s.copy_fixture("multi_rg.parquet");
    let store = memory_store();
    let b = builder(&store);
    let specs = [
        exact(&["id"]),
        range("amount"),
        bloom(&["name"]),
        exact(&["id", "name"]),
    ];
    // No schema supplied: inferred from the footer and recorded.
    let r = build_ok(
        &b,
        &s.root_str(),
        &specs,
        &BuildOptions::new(SourceKind::Parquet),
    );
    assert_eq!(
        (r.files_new, r.row_groups_assigned, r.row_groups_read),
        (1, 3, 3)
    );
    assert_eq!(r.files_listed, 1);
    let m = b.manifest().unwrap().unwrap();
    assert_eq!(m.sequence, 1);
    // Storage version 2 spells each type as the segment format's
    // `ColumnType` JSON.
    let cols: Vec<(&str, String)> = m
        .schema
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c.data_type.to_string()))
        .collect();
    assert_eq!(
        cols,
        vec![
            ("id", r#"{"type":"int64"}"#.to_string()),
            ("name", r#"{"type":"utf8"}"#.to_string()),
            (
                "amount",
                r#"{"precision":10,"scale":2,"type":"decimal128"}"#.to_string()
            )
        ]
    );
    assert_eq!(m.source.kind, "parquet");
    assert_eq!(m.source.fingerprint_policy.as_deref(), Some("tail64k"));
    assert_eq!(m.rowgroup_space.next_id, 3);
    assert_eq!(m.rowgroup_space.files.len(), 1);
    let f = &m.rowgroup_space.files[0];
    assert_eq!(f.fingerprint.path, path.to_str().unwrap());
    assert_eq!(
        f.rowgroup_ids.iter().map(|i| i.0).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert!(f.chunk_size.is_none() && f.chunks.is_none());
    for spec in &specs {
        assert_eq!(completeness(&m, spec), Some(1), "{}", spec.name());
        let segs: Vec<_> = m
            .segments
            .iter()
            .filter(|x| x.key_spec == spec.identity())
            .collect();
        assert_eq!(segs.len(), 1);
        assert_eq!(
            (segs[0].rowgroup_range.start.0, segs[0].rowgroup_range.end.0),
            (0, 3)
        );
    }

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("amount", DataType::Decimal128(10, 2), true),
    ]);
    let rows = parquet_rows(&path, &schema);
    let p = planner(&store);
    let bytes = expected_bytes("multi_rg.parquet");

    // exact
    let plan = check_keys(
        &p,
        &rows,
        &[exact(&["id"])],
        &cols_of(&["id"]),
        &[enc_i64(2), enc_i64(9), enc_i64(100)],
    );
    assert_eq!(
        plan_rgs(&plan).iter().map(|x| x.1).collect::<Vec<_>>(),
        vec![0, 2]
    );
    assert_eq!(plan.bytes, bytes[0] + bytes[2]);
    assert_eq!((plan.row_groups_total, plan.row_groups_selected), (3, 2));
    assert_eq!(plan.manifest_version, "1");

    // composite, exact and by projection with per-key intersection
    let ab = cols_of(&["id", "name"]);
    let keys = vec![
        [enc_i64(5), enc_str("eel")].concat(),
        [enc_i64(1), enc_str("jay")].concat(),
    ];
    let plan = check_keys(&p, &rows, &[exact(&["id", "name"])], &ab, &keys);
    assert_eq!(
        plan_rgs(&plan).iter().map(|x| x.1).collect::<Vec<_>>(),
        vec![1]
    );
    // id 1 is in row group 0 and "jay" in row group 2: intersected per key,
    // (1, jay) selects nothing
    let plan = check_keys(
        &p,
        &rows,
        &[exact(&["id"]), exact(&["id", "name"])],
        &ab,
        &keys,
    );
    assert_eq!(
        plan_rgs(&plan).iter().map(|x| x.1).collect::<Vec<_>>(),
        vec![1]
    );
    check_keys(&p, &rows, &[exact(&["id"]), bloom(&["name"])], &ab, &keys);

    // prefix seek on the leading column of a composite
    let plan = check_keys(
        &p,
        &rows,
        &[exact(&["id", "name"])],
        &cols_of(&["id"]),
        &[enc_i64(5), enc_i64(10)],
    );
    assert_eq!(
        plan_rgs(&plan).iter().map(|x| x.1).collect::<Vec<_>>(),
        vec![1, 2]
    );

    // bloom alone, and raw scan with no spec
    check_keys(
        &p,
        &rows,
        &[bloom(&["name"])],
        &cols_of(&["name"]),
        &[enc_str("hen"), enc_str("zzz")],
    );
    let raw = check_keys(&p, &rows, &[], &cols_of(&["name"]), &[enc_str("hen")]);
    assert_eq!(raw.row_groups_selected, 3);
    assert_eq!(raw.bytes, bytes.iter().sum::<u64>());

    // range: amounts in [100.00, 2000.00] live only in row group 1
    let lookup = Lookup::Between {
        column: "amount".into(),
        lo: dec(10_000),
        hi: dec(200_000),
    };
    let plan = p.plan(&[range("amount")], &lookup).unwrap();
    assert_eq!(
        plan_rgs(&plan).iter().map(|x| x.1).collect::<Vec<_>>(),
        vec![1]
    );
    let got = sorted(apply_residual(
        plan.residual.as_ref().unwrap(),
        planned_rows(&plan, &rows),
    ));
    let want = sorted(apply_residual(
        plan.residual.as_ref().unwrap(),
        rows.clone(),
    ));
    assert_eq!(got.len(), 1);
    assert_eq!(got, want);
    // the leading column of a keyed spec serves a range too
    let lookup = Lookup::Between {
        column: "id".into(),
        lo: enc_i64(4),
        hi: enc_i64(5),
    };
    let plan = p
        .plan(&[exact(&["id"]), exact(&["id", "name"])], &lookup)
        .unwrap();
    assert_eq!(
        plan_rgs(&plan).iter().map(|x| x.1).collect::<Vec<_>>(),
        vec![0, 1]
    );

    // spec and lookup mismatches are errors, never empty plans
    assert!(p
        .plan(
            &[range("amount")],
            &Lookup::Keys {
                columns: cols_of(&["id"]),
                keys: vec![enc_i64(1)]
            }
        )
        .is_err());
    assert!(p
        .plan(
            &[exact(&["id"])],
            &Lookup::Keys {
                columns: cols_of(&["id"]),
                keys: vec![enc_str("1")]
            }
        )
        .is_err());
    assert!(p
        .plan(
            &[exact(&["name"])],
            &Lookup::Keys {
                columns: cols_of(&["name"]),
                keys: vec![]
            }
        )
        .is_err());
}

fn cols_of(c: &[&str]) -> Vec<String> {
    cols(c)
}

#[test]
fn nulls_parquet_an_all_null_row_group_is_covered() {
    let s = Scratch::new();
    let path = s.copy_fixture("nulls.parquet");
    let store = memory_store();
    let b = builder(&store);
    build_ok(
        &b,
        &s.root_str(),
        &[exact(&["key"])],
        &BuildOptions::new(SourceKind::Parquet),
    );
    let m = b.manifest().unwrap().unwrap();
    // row group 1 holds only null keys; the segment still covers it
    assert_eq!(m.rowgroup_space.next_id, 2);
    assert_eq!(m.segments.len(), 1);
    assert_eq!(
        (
            m.segments[0].rowgroup_range.start.0,
            m.segments[0].rowgroup_range.end.0
        ),
        (0, 2)
    );
    assert_eq!(completeness(&m, &exact(&["key"])), Some(1));
    let schema = Schema::new(vec![
        Field::new("key", DataType::Utf8, true),
        Field::new("val", DataType::UInt32, true),
        Field::new("score", DataType::Float64, true),
    ]);
    let rows = parquet_rows(&path, &schema);
    let p = planner(&store);
    let plan = check_keys(
        &p,
        &rows,
        &[exact(&["key"])],
        &cols(&["key"]),
        &[enc_str("k1"), enc_str("k0"), enc_str("nope")],
    );
    assert_eq!(
        plan_rgs(&plan).iter().map(|x| x.1).collect::<Vec<_>>(),
        vec![0]
    );
}

#[test]
fn edge_parquet_unsigned_unicode_decimal38_and_nan() {
    let s = Scratch::new();
    let path = s.copy_fixture("edge.parquet");
    let store = memory_store();
    let b = builder(&store);
    let specs = [
        exact(&["u"]),
        exact(&["s"]),
        exact(&["d"]),
        exact(&["f"]),
        range("f"),
    ];
    build_ok(
        &b,
        &s.root_str(),
        &specs,
        &BuildOptions::new(SourceKind::Parquet),
    );
    let schema = Schema::new(vec![
        Field::new("f", DataType::Float64, true),
        Field::new("u", DataType::UInt32, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("d", DataType::Decimal128(38, 4), true),
    ]);
    let rows = parquet_rows(&path, &schema);
    let p = planner(&store);
    let u = |v: u64| enc(&Value::UInt(v), &DataType::UInt32);
    let plan = check_keys(
        &p,
        &rows,
        &[exact(&["u"])],
        &cols(&["u"]),
        &[u(3_000_000_000), u(4_294_967_295)],
    );
    assert_eq!(plan.row_groups_selected, 2);
    check_keys(
        &p,
        &rows,
        &[exact(&["s"])],
        &cols(&["s"]),
        &[enc_str("\u{65e5}\u{672c}")],
    );
    let d = enc(
        &Value::Decimal128 {
            value: 99_999_999_999_999_999_999_999_999_999_999_999_999,
            scale: 4,
        },
        &DataType::Decimal128(38, 4),
    );
    check_keys(&p, &rows, &[exact(&["d"])], &cols(&["d"]), &[d]);
    // NaN: one canonical encoding; every row group holding one is planned
    let plan = check_keys(
        &p,
        &rows,
        &[exact(&["f"])],
        &cols(&["f"]),
        &[enc_f64(f64::NAN)],
    );
    assert_eq!(plan.row_groups_selected, 2);
    let lookup = Lookup::Between {
        column: "f".into(),
        lo: enc_f64(f64::NAN),
        hi: enc_f64(f64::NAN),
    };
    assert_eq!(
        p.plan(&[range("f")], &lookup).unwrap().row_groups_selected,
        2
    );
}

#[test]
fn timestamps_parquet_latest_and_asof() {
    let s = Scratch::new();
    let path = s.copy_fixture("timestamps.parquet");
    let store = memory_store();
    let b = builder(&store);
    let spec = temporal(&["day"], "ts");
    build_ok(
        &b,
        &s.root_str(),
        &[spec.clone(), exact(&["id"])],
        &BuildOptions::new(SourceKind::Parquet),
    );
    let ts_type = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("ts", ts_type.clone(), true),
        Field::new("day", DataType::Date32, true),
    ]);
    let rows = parquet_rows(&path, &schema);
    let p = planner(&store);
    let day = |d: i32| enc(&Value::Date32(d), &DataType::Date32);
    let days = cols(&["day"]);
    let keys = vec![day(19_723), day(19_724), day(1)];
    let plan = check_latest(
        &p,
        &rows,
        std::slice::from_ref(&spec),
        &days,
        &keys,
        &days,
        "ts",
        None,
    );
    assert_eq!(plan.row_groups_selected, 2);
    let base = 1_704_067_200_000_000_i64;
    let hour = 3_600_000_000_i64;
    let at = |t: i64| {
        (
            t,
            enc(
                &Value::Timestamp {
                    value: t,
                    unit: TimeUnit::Microsecond,
                },
                &ts_type,
            ),
        )
    };
    // as of base + 1h, day 19723's version is the base + 1h row
    let plan = check_latest(
        &p,
        &rows,
        std::slice::from_ref(&spec),
        &days,
        &keys[..1],
        &days,
        "ts",
        Some(at(base + hour)),
    );
    assert_eq!(plan.row_groups_selected, 1);
    // no version of day 19724 at or before base
    let plan = check_latest(
        &p,
        &rows,
        std::slice::from_ref(&spec),
        &days,
        &keys[1..2],
        &days,
        "ts",
        Some(at(base)),
    );
    assert_eq!(plan.row_groups_selected, 0);
    // latest needs every temporal column in the lookup
    let lookup = Lookup::Latest {
        columns: cols(&["id"]),
        keys: vec![enc(&Value::Int(1), &DataType::Int32)],
    };
    assert!(p.plan(&[spec, exact(&["id"])], &lookup).is_err());
}

fn text_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
    ]))
}

fn text_case(file: &str, kind: SourceKind) {
    let s = Scratch::new();
    let path = s.copy_fixture(file);
    let store = memory_store();
    let b = builder(&store);
    let schema = text_schema();
    let opts = BuildOptions {
        schema: Some(Arc::clone(&schema)),
        chunk_size: Some(64),
        ..BuildOptions::new(kind)
    };
    let r = build_ok(
        &b,
        &s.root_str(),
        &[exact(&["id"]), temporal(&["name"], "ts")],
        &opts,
    );
    let m = b.manifest().unwrap().unwrap();
    let f = &m.rowgroup_space.files[0];
    assert_eq!(f.chunk_size, Some(64));
    let want: Vec<(u64, u64)> = text_chunks(&std::fs::read(&path).unwrap(), 64)
        .into_iter()
        .map(|r| (r.start, r.end))
        .collect();
    let got: Vec<(u64, u64)> = f
        .chunks
        .as_ref()
        .unwrap()
        .iter()
        .map(|c| (c.start, c.end))
        .collect();
    assert_eq!(got, want, "persisted chunk ranges");
    assert!(want.len() > 3);
    assert_eq!(f.rowgroup_ids.len(), want.len());
    assert_eq!(r.row_groups_read, want.len() as u64);

    let rows = text_rows(&path, 64, &schema, kind);
    assert_eq!(rows.len(), 12);
    let p = planner(&store);
    let plan = check_keys(
        &p,
        &rows,
        &[exact(&["id"])],
        &cols(&["id"]),
        &[enc_i64(12), enc_i64(6), enc_i64(99)],
    );
    assert_eq!(plan.row_groups_selected, 2);
    // text bytes are the recorded chunk lengths
    let chunk_len = |i: u32| want[i as usize].1 - want[i as usize].0;
    assert_eq!(
        plan.bytes,
        plan_rgs(&plan).iter().map(|x| chunk_len(x.1)).sum::<u64>()
    );
    let names = cols(&["name"]);
    check_latest(
        &p,
        &rows,
        &[temporal(&["name"], "ts")],
        &names,
        &[enc_str("eta"), enc_str("alpha")],
        &names,
        "ts",
        None,
    );

    // A spec added later reads the tracked file at its recorded chunk size
    // (64), whatever chunk size the options now name for new files; a new
    // file gets the new chunk size.
    std::fs::copy(&path, s.path(&format!("z_{file}"))).unwrap();
    let opts128 = BuildOptions {
        chunk_size: Some(128),
        ..opts.clone()
    };
    let r = build_ok(
        &b,
        &s.root_str(),
        &[exact(&["id"]), temporal(&["name"], "ts"), range("amount")],
        &opts128,
    );
    assert_eq!(r.specs_rebuilt, vec!["range:amount".to_string()]);
    let m = b.manifest().unwrap().unwrap();
    let sizes: Vec<Option<u64>> = m
        .rowgroup_space
        .files
        .iter()
        .map(|f| f.chunk_size)
        .collect();
    let paths: Vec<&str> = m
        .rowgroup_space
        .files
        .iter()
        .map(|f| f.fingerprint.path.as_str())
        .collect();
    assert_eq!(
        paths,
        vec![
            path.to_str().unwrap(),
            s.path_str(&format!("z_{file}")).as_str()
        ]
    );
    assert_eq!(sizes, vec![Some(64), Some(128)]);
    for spec in [exact(&["id"]), temporal(&["name"], "ts"), range("amount")] {
        assert!(completeness(&m, &spec).is_some(), "{}", spec.name());
    }
    let mut rows = text_rows(&path, 64, &schema, kind);
    rows.extend(text_rows(&s.path(&format!("z_{file}")), 128, &schema, kind));
    let p = planner(&store);
    check_keys(
        &p,
        &rows,
        &[exact(&["id"])],
        &cols(&["id"]),
        &[enc_i64(12), enc_i64(1)],
    );
    let lookup = Lookup::Between {
        column: "amount".into(),
        lo: enc_f64(-1.5),
        hi: enc_f64(0.75),
    };
    let plan = p.plan(&[range("amount")], &lookup).unwrap();
    let got = sorted(apply_residual(
        plan.residual.as_ref().unwrap(),
        planned_rows(&plan, &rows),
    ));
    let want = sorted(apply_residual(
        plan.residual.as_ref().unwrap(),
        rows.clone(),
    ));
    assert_eq!(got, want);
    assert_eq!(want.len(), 6, "-1.25, 0.0, 0.5 in each file");

    // a build with no chunk size given now has two recorded sizes: fine
    // while no new file appears, an error once one does
    let none = BuildOptions {
        chunk_size: None,
        ..opts
    };
    let r = b
        .build(
            &s.root_str(),
            &[exact(&["id"]), temporal(&["name"], "ts"), range("amount")],
            &none,
        )
        .unwrap();
    assert_eq!(r.sequence, None);
    std::fs::copy(&path, s.path(&format!("c_{file}"))).unwrap();
    assert!(b.build(&s.root_str(), &[exact(&["id"])], &none).is_err());
}

#[test]
fn csv_persists_chunks_and_reads_each_file_at_its_recorded_chunk_size() {
    text_case("events.csv", SourceKind::csv());
}

#[test]
fn jsonl_persists_chunks_and_reads_each_file_at_its_recorded_chunk_size() {
    text_case("events.jsonl", SourceKind::JsonLines);
}
