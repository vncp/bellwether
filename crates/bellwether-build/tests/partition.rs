//! Partition values from Hive-style `day=YYYY-MM-DD/symbol=X/` paths and
//! `--path-template` virtual columns: profiler sees them, builder indexes
//! them, planner prunes on them against a decoded oracle (invariant 1).

#![allow(clippy::unwrap_used, missing_docs)]

mod common;

use std::collections::BTreeSet;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::DataType;
use bellwether_build::{BuildOptions, Lookup, SourceKind};
use bellwether_core::encoding::Value;
use bellwether_source::partition::parse_date32;
use bellwether_source::ParquetSource;
use bellwether_types::Source;
use common::*;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn write_bars(path: &Path, seqs: &[i64], closes: &[f64]) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let batch = RecordBatch::try_from_iter(vec![
        ("seq", Arc::new(Int64Array::from(seqs.to_vec())) as ArrayRef),
        (
            "close",
            Arc::new(Float64Array::from(closes.to_vec())) as ArrayRef,
        ),
    ])
    .unwrap();
    write_parquet(path, &batch, 2);
}

/// Independent ground-truth oracle decoding Parquet files with the `parquet`
/// crate and extracting `(day, symbol)` from the relative path.
fn decode_hive_oracle(root: &Path) -> Vec<(String, u32, i32, String, i64)> {
    let mut out = Vec::new();
    for day_entry in std::fs::read_dir(root).unwrap() {
        let day_dir = day_entry.unwrap().path();
        if !day_dir.is_dir() {
            continue;
        }
        let day_name = day_dir.file_name().unwrap().to_str().unwrap();
        let Some(day_str) = day_name.strip_prefix("day=") else {
            continue;
        };
        let day_val = parse_date32(day_str).unwrap();
        for sym_entry in std::fs::read_dir(&day_dir).unwrap() {
            let sym_dir = sym_entry.unwrap().path();
            if !sym_dir.is_dir() {
                continue;
            }
            let sym_name = sym_dir.file_name().unwrap().to_str().unwrap();
            let Some(sym_str) = sym_name.strip_prefix("symbol=") else {
                continue;
            };
            for file_entry in std::fs::read_dir(&sym_dir).unwrap() {
                let fp = file_entry.unwrap().path();
                if fp.extension().and_then(|e| e.to_str()) != Some("parquet") {
                    continue;
                }
                let canon = std::fs::canonicalize(&fp)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string();
                let meta_builder =
                    ParquetRecordBatchReaderBuilder::try_new(File::open(&fp).unwrap()).unwrap();
                let num_rgs = meta_builder.metadata().num_row_groups();
                for rg in 0..num_rgs {
                    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&fp).unwrap())
                        .unwrap()
                        .with_row_groups(vec![rg])
                        .build()
                        .unwrap();
                    for batch in reader {
                        let b = batch.unwrap();
                        let seqs = b
                            .column_by_name("seq")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap();
                        for r in 0..b.num_rows() {
                            out.push((
                                canon.clone(),
                                u32::try_from(rg).unwrap(),
                                day_val,
                                sym_str.to_string(),
                                seqs.value(r),
                            ));
                        }
                    }
                }
            }
        }
    }
    out
}

#[test]
#[allow(clippy::too_many_lines)]
fn hive_day_symbol_layout_profiles_builds_and_prunes_against_decoded_oracle() {
    let s = Scratch::new();
    let files = [
        (
            "day=2026-03-01/symbol=AAPL/part-0.parquet",
            &[1, 2, 3, 4][..],
            &[180.0, 181.0, 182.0, 183.0][..],
        ),
        (
            "day=2026-03-01/symbol=MSFT/part-0.parquet",
            &[1, 2, 3, 4][..],
            &[400.0, 401.0, 402.0, 403.0][..],
        ),
        (
            "day=2026-03-02/symbol=AAPL/part-0.parquet",
            &[5, 6, 7, 8][..],
            &[184.0, 185.0, 186.0, 187.0][..],
        ),
        (
            "day=2026-03-02/symbol=NVDA/part-0.parquet",
            &[1, 2, 3, 4][..],
            &[900.0, 905.0, 910.0, 915.0][..],
        ),
    ];
    for (rel, seqs, closes) in files {
        write_bars(&s.path(rel), seqs, closes);
    }

    let source = ParquetSource::new();
    let inferred = source.schema(&s.root_str()).unwrap();
    assert_eq!(
        inferred.field_with_name("day").unwrap().data_type(),
        &DataType::Date32
    );
    assert_eq!(
        inferred.field_with_name("symbol").unwrap().data_type(),
        &DataType::Utf8
    );
    let store = memory_store();

    let b = builder(&store);
    let specs = [exact(&["seq"])];
    build_ok(
        &b,
        &s.root_str(),
        &specs,
        &BuildOptions::new(SourceKind::Parquet),
    );
    let p = planner(&store);
    let oracle_rows = decode_hive_oracle(Path::new(&s.root_str()));

    // 1. Lookup on virtual column `symbol = AAPL` alone (even though only `exact:seq` is indexed).
    let sym_cols = cols(&["symbol"]);
    let aapl_key = p.encode_tuple(&sym_cols, &[Value::Utf8("AAPL")]).unwrap();
    let plan_sym = p
        .plan(
            &specs,
            &Lookup::Keys {
                columns: sym_cols,
                keys: vec![aapl_key],
            },
        )
        .unwrap();
    let want_aapl: BTreeSet<(String, u32)> = oracle_rows
        .iter()
        .filter(|(_, _, _, sym, _)| sym == "AAPL")
        .map(|(path, rg, _, _, _)| (path.clone(), *rg))
        .collect();
    assert_eq!(plan_rgs(&plan_sym), want_aapl);
    assert_eq!(plan_sym.row_groups_selected, 4);

    // 2. Lookup on `(symbol, seq)` = `("AAPL", 6)`: intersects `exact:seq` (row groups holding 6)
    // with virtual column `symbol = AAPL`.
    let sym_seq_cols = cols(&["symbol", "seq"]);
    let probe = p
        .encode_tuple(&sym_seq_cols, &[Value::Utf8("AAPL"), Value::Int(6)])
        .unwrap();
    let plan_both = p
        .plan(
            &specs,
            &Lookup::Keys {
                columns: sym_seq_cols,
                keys: vec![probe],
            },
        )
        .unwrap();
    let want_both: BTreeSet<(String, u32)> = oracle_rows
        .iter()
        .filter(|(_, _, _, sym, seq)| sym == "AAPL" && *seq == 6)
        .map(|(path, rg, _, _, _)| (path.clone(), *rg))
        .collect();
    assert_eq!(plan_rgs(&plan_both), want_both);
    assert_eq!(plan_both.row_groups_selected, 1);

    // 3. Range lookup (`Between`) on virtual `day` column for `2026-03-02..=2026-03-02`.
    let d2 = parse_date32("2026-03-02").unwrap();
    let d2_enc = enc(&Value::Date32(d2), &DataType::Date32);
    let plan_day = p
        .plan(
            &specs,
            &Lookup::Between {
                column: "day".into(),
                lo: d2_enc.clone(),
                hi: d2_enc,
            },
        )
        .unwrap();
    let want_day: BTreeSet<(String, u32)> = oracle_rows
        .iter()
        .filter(|(_, _, day, _, _)| *day == d2)
        .map(|(path, rg, _, _, _)| (path.clone(), *rg))
        .collect();
    assert_eq!(plan_rgs(&plan_day), want_day);
    assert_eq!(plan_day.row_groups_selected, 4);
}

#[test]
fn path_template_indexes_temporal_on_virtual_day_column_and_detects_template_change() {
    let s = Scratch::new();
    let batch1 = RecordBatch::try_from_iter(vec![
        (
            "symbol",
            Arc::new(arrow::array::StringArray::from(vec!["AAPL", "MSFT"])) as ArrayRef,
        ),
        (
            "close",
            Arc::new(Float64Array::from(vec![180.0, 400.0])) as ArrayRef,
        ),
    ])
    .unwrap();
    let batch2 = RecordBatch::try_from_iter(vec![
        (
            "symbol",
            Arc::new(arrow::array::StringArray::from(vec!["AAPL", "MSFT"])) as ArrayRef,
        ),
        (
            "close",
            Arc::new(Float64Array::from(vec![185.0, 405.0])) as ArrayRef,
        ),
    ])
    .unwrap();
    write_parquet(&s.path("2026-03-01.parquet"), &batch1, 2);
    write_parquet(&s.path("2026-03-02.parquet"), &batch2, 2);

    let store = memory_store();
    let b = builder(&store);
    let mut opts = BuildOptions::new(SourceKind::Parquet);
    opts.path_template = Some("{day:date}.parquet".into());
    let specs = [temporal(&["symbol"], "day")];
    build_ok(&b, &s.root_str(), &specs, &opts);

    let m = b.manifest().unwrap().unwrap();
    assert_eq!(
        m.source.path_template.as_deref(),
        Some("{day:date}.parquet")
    );

    // Changing the template on an existing index is refused.
    let mut changed_opts = opts.clone();
    changed_opts.path_template = Some("{session:date}.parquet".into());
    let err = b
        .build(&s.root_str(), &specs, &changed_opts)
        .unwrap_err()
        .to_string();
    assert!(err.contains("--path-template"), "got: {err}");

    // AsOf lookup at 2026-03-01 returns only the 2026-03-01 file's row group.
    let p = planner(&store);
    let sym_cols = cols(&["symbol"]);
    let aapl = p.encode_tuple(&sym_cols, &[Value::Utf8("AAPL")]).unwrap();
    let d1 = i64::from(parse_date32("2026-03-01").unwrap());
    let plan = p
        .plan(
            &specs,
            &Lookup::AsOf {
                columns: sym_cols,
                keys: vec![aapl],
                ts: d1,
            },
        )
        .unwrap();
    assert_eq!(plan.entries.len(), 1);
    assert!(plan.entries[0].file.path.ends_with("2026-03-01.parquet"));
}
