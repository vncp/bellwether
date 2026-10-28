//! DuckDB (WP16 item 3, 5): every lookup shape's SQL run by the `duckdb`
//! CLI returns exactly the reference evaluator's rows, and the bytes
//! DuckDB reads are measured at an HTTP range server: row groups touched
//! contain the planned ones, and the bytes beyond the plan are at most the
//! unplanned row groups of the planned files. Skipped, with the reason,
//! without a `duckdb` CLI (or, for the bytes, without `httpfs`).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::doc_markdown)]

mod common;

use std::collections::{BTreeMap, BTreeSet};

use bellwether_adapters::measure::{classify, FileSpans};
use bellwether_adapters::{duckdb, AdapterError, EmitOptions, Relocate};
use common::range_server::RangeServer;

fn opts(c: &common::Corpus) -> EmitOptions {
    EmitOptions {
        row_offsets: Some(common::row_offsets(c)),
        partitions: Some(common::partitions(c)),
        ..EmitOptions::default()
    }
}

#[test]
fn every_lookup_shape_returns_the_reference_rows() {
    let Some(bin) = common::require_duckdb() else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let c = common::corpus(tmp.path());
    let schema = common::dataset();
    let columns = common::all_columns();
    let mut ran = 0;
    for case in common::cases(&c) {
        let sql = duckdb::sql(&case.plan, &schema, &opts(&c));
        if !case.sql_exact {
            match sql {
                Err(
                    e @ AdapterError::Unsupported {
                        engine: "duckdb", ..
                    },
                ) => {
                    let msg = e.to_string();
                    assert!(msg.contains("emit for DataFusion"), "{}: {msg}", case.name);
                }
                other => panic!("{}: not refused: {other:?}", case.name),
            }
            continue;
        }
        let sql = sql.unwrap_or_else(|e| panic!("{}: {e}", case.name));
        let got = common::rendered(&common::duckdb_rows(&bin, &sql, tmp.path(), ""), &columns);
        let want = common::expected(&c, &case.plan, &columns);
        common::assert_rows(&got, &want, &format!("{}:\n{sql}", case.name));
        ran += 1;
    }
    assert!(ran >= 20, "{ran}");
}

#[test]
fn ordered_output_follows_the_read_order() {
    let Some(bin) = common::require_duckdb() else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let c = common::corpus(tmp.path());
    let mut plan = common::locate(&c, &["id"], &common::ids(&[1, 2, 3, 4, 5]));
    // Interleave: the last entry's row groups first, then the rest.
    let pairs: Vec<(u32, u32)> = plan
        .entries
        .iter()
        .enumerate()
        .flat_map(|(i, e)| {
            e.row_groups
                .iter()
                .map(move |rg| (u32::try_from(i).unwrap(), rg))
        })
        .collect();
    let mut order: Vec<(u32, u32)> = pairs.iter().rev().copied().collect();
    order.rotate_left(1);
    plan.order = Some(order.clone());
    let o = EmitOptions {
        ordered: true,
        ..opts(&c)
    };
    let sql = duckdb::sql(&plan, &common::dataset(), &o).unwrap();
    let got = common::duckdb_rows(&bin, &sql, tmp.path(), "");
    let ids: Vec<i64> = got
        .iter()
        .flat_map(|b| {
            b.column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .iter()
                .map(Option::unwrap)
                .collect::<Vec<_>>()
        })
        .collect();
    // The reference order: each (entry, row group) in `order`, its rows by
    // row number, kept by the residual.
    let mut want = Vec::new();
    let keys: BTreeSet<i64> = [1, 2, 3, 4, 5].into();
    for (e, rg) in order {
        let path = &plan.entries[e as usize].file.path;
        let fi = c.files.iter().position(|f| &f.path == path).unwrap();
        let mut rows: Vec<&common::Row> = c
            .rows
            .iter()
            .filter(|r| r.file == fi && r.rg == rg)
            .collect();
        rows.sort_by_key(|r| r.row);
        for r in rows {
            if let Some(v) = r.get("id") {
                let k = i64::from_be_bytes(v[1..9].try_into().unwrap()) ^ i64::MIN;
                if keys.contains(&k) {
                    want.push(k);
                }
            }
        }
    }
    assert_eq!(ids, want);
}

#[test]
fn bytes_read_are_measured_at_a_range_server() {
    let Some(bin) = common::require_duckdb() else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let c = common::corpus(tmp.path());
    if let Err(e) = common::duckdb_run(&bin, "INSTALL httpfs; LOAD httpfs;") {
        let why = format!("DuckDB's httpfs extension is not installable here ({e})");
        common::skip_or_fail::<()>("BELLWETHER_REQUIRE_DUCKDB", &why);
        return;
    }
    let server = RangeServer::start(&c.root);
    let spans: BTreeMap<String, FileSpans> = c
        .files
        .iter()
        .map(|f| (f.path.clone(), FileSpans::of_local(&f.path).unwrap()))
        .collect();
    let schema = common::dataset();
    let columns = common::all_columns();
    let mut table = Vec::new();
    for case in common::cases(&c).into_iter().filter(|k| k.sql_exact) {
        let o = EmitOptions {
            relocate: Some(Relocate {
                from: server.root.display().to_string(),
                to: server.base.clone(),
            }),
            ..opts(&c)
        };
        let sql = duckdb::sql(&case.plan, &schema, &o).unwrap();
        server.reset();
        let got = common::rendered(
            &common::duckdb_rows(&bin, &sql, tmp.path(), "LOAD httpfs;\n"),
            &columns,
        );
        common::assert_rows(&got, &common::expected(&c, &case.plan, &columns), case.name);
        let realized = classify(&server.fetches(), &spans);
        let planned: BTreeSet<(String, u32)> = case
            .plan
            .entries
            .iter()
            .flat_map(|e| e.row_groups.iter().map(move |rg| (e.file.path.clone(), rg)))
            .collect();
        let touched = realized.touched();
        assert!(
            touched.is_superset(&planned),
            "{}: DuckDB skipped a planned row group: planned {planned:?}, touched {touched:?}",
            case.name
        );
        // Bytes beyond the plan, bounded by the unplanned row groups of the
        // planned files (DuckDB reads nothing of an unplanned file's data).
        let unplanned: u64 = case
            .plan
            .entries
            .iter()
            .map(|e| {
                let f = c.files.iter().find(|f| f.path == e.file.path).unwrap();
                f.rg_bytes
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| !e.row_groups.contains(u32::try_from(*i).unwrap()))
                    .map(|(_, b)| b)
                    .sum::<u64>()
            })
            .sum();
        let planned_files: BTreeSet<&str> = case
            .plan
            .entries
            .iter()
            .map(|e| e.file.path.as_str())
            .collect();
        for (p, fb) in &realized.files {
            if !planned_files.contains(p.as_str()) {
                assert_eq!(
                    fb.data, 0,
                    "{}: DuckDB read data of an unplanned file {p}",
                    case.name
                );
            }
        }
        let data = realized.data();
        assert!(
            data <= case.plan.bytes + unplanned,
            "{}: data {data} > planned {} + unplanned row groups of planned files {unplanned}",
            case.name,
            case.plan.bytes
        );
        table.push(format!(
            "{:<52} planned {:>7}  data {:>7}  delta {:>+7}  footer {:>6}  other {:>6}  rg planned {:>2} touched {:>2}",
            case.name,
            case.plan.bytes,
            data,
            i128::from(data) - i128::from(case.plan.bytes),
            realized.footer(),
            realized.other(),
            planned.len(),
            touched.len()
        ));
    }
    eprintln!(
        "DuckDB at the range server (data = column-chunk bytes served):\n{}",
        table.join("\n")
    );
}

#[test]
fn a_local_folder_with_a_space_is_read_at_its_url_route() {
    // The served root holds the corpus in `local lake/`: each plan path,
    // relocated from the local root to the server, is read by DuckDB at its
    // URL route (`/local lake/region=…/part-….parquet`), and the server maps
    // the route back to the planned local file.
    let Some(bin) = common::require_duckdb() else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let c = common::corpus(&tmp.path().join("local lake"));
    if let Err(e) = common::duckdb_run(&bin, "INSTALL httpfs; LOAD httpfs;") {
        let why = format!("DuckDB's httpfs extension is not installable here ({e})");
        common::skip_or_fail::<()>("BELLWETHER_REQUIRE_DUCKDB", &why);
        return;
    }
    let server = RangeServer::start(tmp.path());
    let spans: BTreeMap<String, FileSpans> = c
        .files
        .iter()
        .map(|f| (f.path.clone(), FileSpans::of_local(&f.path).unwrap()))
        .collect();
    let schema = common::dataset();
    let columns = common::all_columns();
    let mut ran = 0;
    for case in common::cases(&c).into_iter().filter(|k| k.sql_exact) {
        let o = EmitOptions {
            relocate: Some(Relocate {
                from: server.root.display().to_string(),
                to: server.base.clone(),
            }),
            ..opts(&c)
        };
        let sql = duckdb::sql(&case.plan, &schema, &o).unwrap();
        server.reset();
        let got = common::rendered(
            &common::duckdb_rows(&bin, &sql, tmp.path(), "LOAD httpfs;\n"),
            &columns,
        );
        common::assert_rows(&got, &common::expected(&c, &case.plan, &columns), case.name);
        let fetches = server.fetches();
        for f in &fetches {
            assert!(
                spans.contains_key(&f.path),
                "{}: served {} is not a corpus file",
                case.name,
                f.path
            );
        }
        let planned: BTreeSet<(String, u32)> = case
            .plan
            .entries
            .iter()
            .flat_map(|e| e.row_groups.iter().map(move |rg| (e.file.path.clone(), rg)))
            .collect();
        let touched = classify(&fetches, &spans).touched();
        assert!(
            touched.is_superset(&planned),
            "{}: planned {planned:?}, touched {touched:?}",
            case.name
        );
        ran += 1;
    }
    assert!(ran > 0);
}

#[test]
fn a_nan_key_is_refused_by_name_not_run_loosely() {
    // DuckDB prunes row groups by min/max statistics that leave NaN out
    // (checked on 2.0): the NaN row of `nan_case` would be lost. The
    // adapter refuses before any SQL exists, inline or as a relation.
    let tmp = tempfile::tempdir().unwrap();
    let nan = common::nan_case(tmp.path());
    let o = EmitOptions {
        row_offsets: Some(nan.offsets.clone()),
        ..EmitOptions::default()
    };
    for plan in [&nan.plan, &nan.composite] {
        let err = duckdb::sql(plan, &nan.schema, &o).unwrap_err();
        let msg = err.to_string();
        assert!(
            matches!(
                err,
                AdapterError::Unsupported {
                    engine: "duckdb",
                    ..
                }
            ),
            "{msg}"
        );
        assert!(
            msg.contains("In key list") && msg.contains("NaN") && msg.contains("\"f\""),
            "{msg}"
        );
        assert!(msg.contains("emit for DataFusion"), "{msg}");
    }
}
