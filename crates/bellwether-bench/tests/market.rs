//! Integration tests for the synthetic `market` corpus, time-series workload,
//! and independent ground-truth oracle (WP21).

#![allow(
    clippy::unwrap_used,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::many_single_char_names,
    clippy::too_many_lines,
    clippy::doc_markdown
)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::{Path, PathBuf};

use arrow::array::{Array, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray};
use bellwether_bench::corpus::DAY_US;
use bellwether_bench::market::{
    format_rfc3339_us, generate_market, generate_market_all, read_symbol_keys, FileRowGroup,
    MarketParams, MarketQueryKind, MarketQueryTruth, MarketRung, MarketSummary, MarketTruth,
    MarketWorkload, MarketWorkloadParams, SymbolWinner, WindowSpan, WindowTruth,
    MARKET_TRUTH_FORMAT, NORMAL_KNOWN_DELAY_MAX_US,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn tmp(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("bellwether-bench-market")
        .join(name);
    if dir.exists() {
        std::fs::remove_dir_all(&dir).unwrap();
    }
    dir
}

/// Every file under `root`, relative path to raw bytes.
fn tree_bytes(root: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut out = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else {
                let rel = path.strip_prefix(root).unwrap().display().to_string();
                out.insert(rel, std::fs::read(&path).unwrap());
            }
        }
    }
    out
}

#[derive(Clone, Debug)]
struct DecodedRow {
    symbol: String,
    ts_us: i64,
    known_at_us: i64,
    price: f64,
    size: i64,
}

#[derive(Clone, Debug)]
struct DecodedRowGroup {
    file: String,
    day_date: String,
    row_group: u32,
    rows: Vec<DecodedRow>,
}

/// Independently decodes every Parquet file under `<out>/data` using the `parquet` crate.
fn decode_corpus(out: &Path) -> (Vec<DecodedRowGroup>, u64, u64) {
    let data_dir = out.join("data");
    let mut day_dirs: Vec<PathBuf> = std::fs::read_dir(&data_dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    day_dirs.sort();

    let mut all_rgs = Vec::new();
    let mut files_count = 0u64;
    let mut bytes_count = 0u64;

    for day_dir in day_dirs {
        let dir_name = day_dir.file_name().unwrap().to_str().unwrap();
        assert!(
            dir_name.starts_with("day=") && dir_name.len() == 14,
            "unexpected day directory name: {dir_name}"
        );
        let day_date = dir_name["day=".len()..].to_string();

        let mut part_files: Vec<PathBuf> = std::fs::read_dir(&day_dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        part_files.sort();

        for part_path in part_files {
            let fname = part_path.file_name().unwrap().to_str().unwrap();
            assert!(
                fname.starts_with("part-")
                    && std::path::Path::new(fname)
                        .extension()
                        .is_some_and(|ext| ext.eq_ignore_ascii_case("parquet")),
                "unexpected part filename: {fname}"
            );
            files_count += 1;
            bytes_count += std::fs::metadata(&part_path).unwrap().len();
            let rel = part_path
                .strip_prefix(out)
                .unwrap()
                .components()
                .map(|c| c.as_os_str().to_str().unwrap())
                .collect::<Vec<_>>()
                .join("/");

            let file = File::open(&part_path).unwrap();
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
            let num_rgs = builder.metadata().num_row_groups();
            let schema = builder.schema().clone();
            let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            assert_eq!(
                col_names,
                vec!["symbol", "ts", "known_at", "price", "size"],
                "file {rel} must have the 5 market columns and no Hive `day` column inside the file"
            );

            for rg_idx in 0..num_rgs {
                let f_rg = File::open(&part_path).unwrap();
                let reader = ParquetRecordBatchReaderBuilder::try_new(f_rg)
                    .unwrap()
                    .with_row_groups(vec![rg_idx])
                    .with_batch_size(65_536)
                    .build()
                    .unwrap();
                let mut rows = Vec::new();
                for batch in reader {
                    let batch = batch.unwrap();
                    let sym_col = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    let ts_col = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    let known_col = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    let price_col = batch
                        .column(3)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap();
                    let size_col = batch
                        .column(4)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    assert_eq!(sym_col.null_count(), 0);
                    assert_eq!(ts_col.null_count(), 0);
                    assert_eq!(known_col.null_count(), 0);
                    assert_eq!(price_col.null_count(), 0);
                    assert_eq!(size_col.null_count(), 0);

                    for i in 0..batch.num_rows() {
                        rows.push(DecodedRow {
                            symbol: sym_col.value(i).to_string(),
                            ts_us: ts_col.value(i),
                            known_at_us: known_col.value(i),
                            price: price_col.value(i),
                            size: size_col.value(i),
                        });
                    }
                }
                all_rgs.push(DecodedRowGroup {
                    file: rel.clone(),
                    day_date: day_date.clone(),
                    row_group: rg_idx as u32,
                    rows,
                });
            }
        }
    }

    (all_rgs, files_count, bytes_count)
}

/// Independently recomputes `MarketTruth` purely from the decoded Parquet files and workload JSON.
fn recompute_truth_oracle(
    out: &Path,
    decoded_rgs: &[DecodedRowGroup],
    files_count: u64,
    bytes_count: u64,
    workload: &MarketWorkload,
) -> MarketTruth {
    // Count total rows, late prints, and restatements from the decoded rows.
    // A row is in day D's partition; its event day is `ts_us.div_euclid(DAY_US)` and its known day is `known_at_us.div_euclid(DAY_US)`.
    // For any `(symbol, ts_us)`, if there is an on-time row (`known_at_us - ts_us <= 500_000`) and a later row (`known_at_us > ts_us + 500_000`),
    // the later row is a restatement. If there is only a row with `known_day > ts_day`, it is a late print.
    let mut by_sym_ts: BTreeMap<(String, i64), Vec<&DecodedRow>> = BTreeMap::new();
    let mut rows_total = 0u64;
    for rg in decoded_rgs {
        for r in &rg.rows {
            rows_total += 1;
            assert!(
                (1..=10_000).contains(&r.size) && (10.0..=500.0).contains(&r.price),
                "decoded row size {} or price {} out of range",
                r.size,
                r.price
            );
            by_sym_ts
                .entry((r.symbol.clone(), r.ts_us))
                .or_default()
                .push(r);
        }
    }

    let mut restatements = 0u64;
    let mut late_prints = 0u64;
    for rows in by_sym_ts.values() {
        let has_ontime = rows
            .iter()
            .any(|r| r.known_at_us - r.ts_us <= NORMAL_KNOWN_DELAY_MAX_US);
        for r in rows {
            let ts_day = r.ts_us.div_euclid(DAY_US);
            let known_day = r.known_at_us.div_euclid(DAY_US);
            if known_day > ts_day {
                assert!(
                    (1..=3).contains(&(known_day - ts_day)),
                    "late/restated row must arrive 1..=3 days later"
                );
                if has_ontime {
                    restatements += 1;
                } else {
                    late_prints += 1;
                }
            }
        }
    }

    let summary = MarketSummary {
        files: files_count,
        row_groups: decoded_rgs.len() as u64,
        rows: rows_total,
        restatements,
        late_prints,
        bytes: bytes_count,
    };

    let mut queries = Vec::with_capacity(workload.queries.len());
    for q in &workload.queries {
        // Verify key file matches q.symbols
        let disk_keys = read_symbol_keys(&out.join(&q.keys_file)).unwrap();
        assert_eq!(
            disk_keys, q.symbols,
            "query {} key file does not match manifest symbols",
            q.id
        );
        let sym_set: BTreeSet<&str> = q.symbols.iter().map(String::as_str).collect();

        match q.kind {
            MarketQueryKind::Window => {
                let from_us = q.from_us.unwrap();
                let to_us = q.to_us.unwrap();
                assert_eq!(q.from.as_deref(), Some(format_rfc3339_us(from_us).as_str()));
                assert_eq!(q.to.as_deref(), Some(format_rfc3339_us(to_us).as_str()));

                let mut matching_row_groups = Vec::new();
                let mut matching_rows = 0u64;
                let mut symbol_row_groups = Vec::new();
                let mut ts_overlap_row_groups = Vec::new();
                let mut days_touched = BTreeSet::new();

                for rg in decoded_rgs {
                    let frg = FileRowGroup {
                        file: rg.file.clone(),
                        row_group: rg.row_group,
                    };
                    let ts_min = rg.rows.iter().map(|r| r.ts_us).min().unwrap();
                    let ts_max = rg.rows.iter().map(|r| r.ts_us).max().unwrap();
                    if ts_min <= to_us && ts_max >= from_us {
                        ts_overlap_row_groups.push(frg.clone());
                    }

                    let has_sym = rg.rows.iter().any(|r| sym_set.contains(r.symbol.as_str()));
                    if has_sym {
                        symbol_row_groups.push(frg.clone());
                    }

                    let count = rg
                        .rows
                        .iter()
                        .filter(|r| {
                            sym_set.contains(r.symbol.as_str())
                                && r.ts_us >= from_us
                                && r.ts_us <= to_us
                        })
                        .count() as u64;
                    if count > 0 {
                        matching_rows += count;
                        days_touched.insert(rg.day_date.clone());
                        matching_row_groups.push(frg);
                    }
                }

                matching_row_groups.sort();
                symbol_row_groups.sort();
                ts_overlap_row_groups.sort();

                queries.push(MarketQueryTruth {
                    id: q.id.clone(),
                    kind: q.kind,
                    window: Some(WindowTruth {
                        matching_row_groups,
                        matching_rows,
                        symbol_row_groups,
                        ts_overlap_row_groups,
                        partition_days_touched: days_touched.into_iter().collect(),
                    }),
                    winners: None,
                });
            }
            MarketQueryKind::Asof | MarketQueryKind::LatestKnown => {
                let valid_us = if q.kind == MarketQueryKind::Asof {
                    let v = q.valid_us.unwrap();
                    assert_eq!(q.valid.as_deref(), Some(format_rfc3339_us(v).as_str()));
                    v
                } else {
                    assert!(q.valid.is_none() && q.valid_us.is_none());
                    i64::MAX
                };
                let known_us = q.known_us.unwrap();
                assert_eq!(
                    q.known.as_deref(),
                    Some(format_rfc3339_us(known_us).as_str())
                );

                let mut winners = Vec::new();
                for sym in &q.symbols {
                    let mut candidates: Vec<(&DecodedRow, FileRowGroup)> = Vec::new();
                    for rg in decoded_rgs {
                        for r in &rg.rows {
                            if &r.symbol == sym && r.ts_us <= valid_us && r.known_at_us <= known_us
                            {
                                candidates.push((
                                    r,
                                    FileRowGroup {
                                        file: rg.file.clone(),
                                        row_group: rg.row_group,
                                    },
                                ));
                            }
                        }
                    }
                    if let Some(&(best_row, _)) = candidates
                        .iter()
                        .max_by_key(|(r, _)| (r.ts_us, r.known_at_us))
                    {
                        let win_key = (best_row.ts_us, best_row.known_at_us);
                        let mut prices = Vec::new();
                        let mut rgs = BTreeSet::new();
                        for (r, frg) in candidates {
                            if (r.ts_us, r.known_at_us) == win_key {
                                prices.push(r.price);
                                rgs.insert(frg);
                            }
                        }
                        prices.sort_by(f64::total_cmp);
                        winners.push(SymbolWinner {
                            symbol: sym.clone(),
                            ts: format_rfc3339_us(win_key.0),
                            ts_us: win_key.0,
                            known_at: format_rfc3339_us(win_key.1),
                            known_at_us: win_key.1,
                            price: prices[0],
                            prices,
                            row_groups: rgs.into_iter().collect(),
                        });
                    }
                }

                queries.push(MarketQueryTruth {
                    id: q.id.clone(),
                    kind: q.kind,
                    window: None,
                    winners: Some(winners),
                });
            }
        }
    }

    MarketTruth {
        format: MARKET_TRUTH_FORMAT.to_string(),
        summary,
        queries,
    }
}

#[test]
fn market_determinism_layout_and_oracle_truth() {
    let dir_a = tmp("small-a");
    let dir_b = tmp("small-b");
    let params = MarketParams::rung(MarketRung::Small);

    let (summary_a, workload_a, truth_a) =
        generate_market_all(&dir_a, &params, &MarketWorkloadParams::default()).unwrap();
    let summary_b = generate_market(&dir_b, &params).unwrap();
    assert_eq!(summary_a, summary_b);

    // 1. Determinism: identical byte-for-byte output across two runs.
    let bytes_a = tree_bytes(&dir_a);
    let bytes_b = tree_bytes(&dir_b);
    assert_eq!(
        bytes_a, bytes_b,
        "two runs with identical MarketParams must produce byte-identical files"
    );

    // Changing seed must change output bytes.
    let dir_c = tmp("small-c");
    let mut params_c = params.clone();
    params_c.seed = 99;
    generate_market(&dir_c, &params_c).unwrap();
    assert_ne!(bytes_a, tree_bytes(&dir_c));

    // 2. Decode every Parquet file with the parquet crate and verify layout & sorting.
    let (decoded_rgs, files_count, bytes_count) = decode_corpus(&dir_a);
    assert_eq!(files_count, u64::from(params.days * params.files_per_day));
    assert_eq!(files_count, summary_a.files);
    assert_eq!(bytes_count, summary_a.bytes);
    assert_eq!(decoded_rgs.len() as u64, summary_a.row_groups);

    // Group decoded row groups by day partition to verify within-day global sort order
    // `(symbol, ts, known_at)` and row-group sizing (`rows_per_row_group`).
    let mut by_day: BTreeMap<String, Vec<&DecodedRowGroup>> = BTreeMap::new();
    let mut by_file: BTreeMap<String, Vec<&DecodedRowGroup>> = BTreeMap::new();
    for rg in &decoded_rgs {
        by_day.entry(rg.day_date.clone()).or_default().push(rg);
        by_file.entry(rg.file.clone()).or_default().push(rg);
    }
    assert_eq!(by_day.len(), params.days as usize);

    for (file, rgs) in &by_file {
        for (i, rg) in rgs.iter().enumerate() {
            if i + 1 < rgs.len() {
                assert_eq!(
                    rg.rows.len(),
                    params.rows_per_row_group as usize,
                    "non-final row group {i} in {file} must have exactly rows_per_row_group rows"
                );
            } else {
                assert!(
                    !rg.rows.is_empty() && rg.rows.len() <= params.rows_per_row_group as usize,
                    "final row group in {file} must have 1..=rows_per_row_group rows"
                );
            }
        }
    }

    let mut symbols_straddling_rgs = 0usize;
    for (day_date, rgs) in &by_day {
        let mut prev: Option<(&str, i64, i64)> = None;
        let mut sym_rgs: BTreeMap<&str, usize> = BTreeMap::new();
        for rg in rgs {
            let mut seen_in_rg = BTreeSet::new();
            for r in &rg.rows {
                // Every row in day=YYYY-MM-DD must have its known_at on that UTC day.
                let known_date = &format_rfc3339_us(r.known_at_us)[..10];
                assert_eq!(
                    known_date, day_date,
                    "row in partition day={day_date} has known_at on {known_date}"
                );
                let cur = (r.symbol.as_str(), r.ts_us, r.known_at_us);
                if let Some(p) = prev {
                    assert!(
                        p <= cur,
                        "rows within day={day_date} are not sorted by (symbol, ts, known_at): {p:?} > {cur:?}"
                    );
                }
                prev = Some(cur);
                seen_in_rg.insert(r.symbol.as_str());
            }
            for s in seen_in_rg {
                *sym_rgs.entry(s).or_default() += 1;
            }
        }
        symbols_straddling_rgs += sym_rgs.values().filter(|&&c| c >= 2).count();
    }
    assert!(
        symbols_straddling_rgs > 0,
        "symbols must straddle multiple row groups within a day so intraday windows can skip row groups"
    );

    // 3. Verify restatements and late prints exist and match configured fractions.
    assert!(
        summary_a.restatements > 0,
        "expected restatements > 0 for restatement_fraction = {}",
        params.restatement_fraction
    );
    assert!(
        summary_a.late_prints > 0,
        "expected late_prints > 0 for late_fraction = {}",
        params.late_fraction
    );

    // 4. Verify workload queries cover all required kinds, window spans, and symbol set sizes.
    let disk_workload: MarketWorkload =
        serde_json::from_slice(&std::fs::read(dir_a.join("workload/market.json")).unwrap())
            .unwrap();
    assert_eq!(disk_workload, workload_a);
    assert!(disk_workload.queries.iter().any(
        |q| q.kind == MarketQueryKind::Window && q.window_span == Some(WindowSpan::Intraday1h)
    ));
    assert!(disk_workload
        .queries
        .iter()
        .any(|q| q.kind == MarketQueryKind::Window && q.window_span == Some(WindowSpan::OneDay)));
    assert!(disk_workload
        .queries
        .iter()
        .any(|q| q.kind == MarketQueryKind::Window && q.window_span == Some(WindowSpan::FiveDay)));
    for size in [1usize, 10, 50] {
        assert!(
            disk_workload
                .queries
                .iter()
                .any(|q| q.kind == MarketQueryKind::Window && q.symbols.len() == size),
            "missing window query with symbol set size {size}"
        );
    }
    assert!(disk_workload
        .queries
        .iter()
        .any(|q| q.kind == MarketQueryKind::Asof));
    assert!(disk_workload
        .queries
        .iter()
        .any(|q| q.kind == MarketQueryKind::LatestKnown));

    // Verify that the pair of restatement asof queries (q index 0 and 1 of asof) actually
    // observe the original price before the restatement and the restated price at/after it.
    let asof_truths: Vec<&MarketQueryTruth> = truth_a
        .queries
        .iter()
        .filter(|q| q.kind == MarketQueryKind::Asof)
        .collect();
    assert!(asof_truths.len() >= 2);
    let w_before = asof_truths[0].winners.as_ref().unwrap();
    let w_after = asof_truths[1].winners.as_ref().unwrap();
    let differed = w_before.iter().any(|wb| {
        w_after.iter().any(|wa| {
            wb.symbol == wa.symbol
                && wb.ts_us == wa.ts_us
                && wb.known_at_us < wa.known_at_us
                && (wb.price - wa.price).abs() > 1e-6
        })
    });
    assert!(
        differed,
        "paired restatement asof queries must observe the price change when K crosses the restatement's known_at"
    );

    // 5. Independent oracle: recomputed truth from decoded Parquet files equals truth.json.
    let disk_truth: MarketTruth =
        serde_json::from_slice(&std::fs::read(dir_a.join("workload/truth.json")).unwrap()).unwrap();
    assert_eq!(disk_truth, truth_a);
    let oracle_truth = recompute_truth_oracle(
        &dir_a,
        &decoded_rgs,
        files_count,
        bytes_count,
        &disk_workload,
    );
    assert_eq!(
        disk_truth, oracle_truth,
        "truth.json must equal the independent Parquet-decoding oracle for every query"
    );

    eprintln!(
        "SMALL_RUNG_SUMMARY: files={}, row_groups={}, rows={}, restatements={}, late_prints={}, bytes={}",
        summary_a.files,
        summary_a.row_groups,
        summary_a.rows,
        summary_a.restatements,
        summary_a.late_prints,
        summary_a.bytes
    );
}

#[test]
fn market_param_validation_rejects_bad_inputs() {
    let base = MarketParams::rung(MarketRung::Small);
    assert!(base.validate().is_ok());

    let mut p = base.clone();
    p.symbols = 0;
    assert!(p.validate().is_err());

    let mut p = base.clone();
    p.days = 0;
    assert!(p.validate().is_err());

    let mut p = base.clone();
    p.files_per_day = 0;
    assert!(p.validate().is_err());

    let mut p = base.clone();
    p.rows_per_row_group = 0;
    assert!(p.validate().is_err());

    let mut p = base.clone();
    p.prints_per_symbol_per_day = 0;
    assert!(p.validate().is_err());

    let mut p = base.clone();
    p.restatement_fraction = -0.01;
    assert!(p.validate().is_err());

    let mut p = base.clone();
    p.late_fraction = 1.01;
    assert!(p.validate().is_err());

    assert!(base.clone().with_start_str("2026-02-29").is_err());
    assert_eq!(
        base.with_start_str("2026-03-15").unwrap().start,
        (2026, 3, 15)
    );
}
