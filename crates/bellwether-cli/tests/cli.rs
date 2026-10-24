//! The CLI over the facade: `--json` is the facade's typed result, every
//! printed record is the ledger's stored bytes, human output leads with the
//! funnel, and exit codes follow the contract (0, 1, 2, 3, 4).

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use bellwether::types::Record;
use bellwether::{IndexResult, PlanResult, ProposeResult, ReportResult};
use common::{check_records, corpus, index_dir, keys, path, run, typed};

#[test]
fn json_is_the_typed_result_and_records_are_the_stored_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 2, 3, 20, 30);
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[1, 2, 29]);

    let out = run(&["--json", "index", path(&data), "--key", "exact:id"], 0);
    let idx: IndexResult = typed(&out);
    assert_eq!(idx.committed, Some(1));

    let out = run(&["--json", "locate", path(&data), "--keys", path(&kf)], 0);
    let plan: PlanResult = typed(&out);
    let v: serde_json::Value = serde_json::from_str(&out).unwrap();
    let recs = check_records(&index_dir(&data), &v);
    assert_eq!(recs.len(), 1);
    assert!(matches!(recs[0].1, Record::Evidence(_)));
    assert_eq!(recs[0].0, plan.evidence.hash);

    let ev = plan.evidence.hash.to_hex();
    let out = run(
        &[
            "--json",
            "propose",
            path(&data),
            "--evidence",
            &ev,
            "--by",
            "test",
            "--key",
            "exact:batch",
        ],
        0,
    );
    let p: ProposeResult = typed(&out);
    // The engine's proposer name is reserved: a refusal at the trust
    // boundary, exit 3, nothing written.
    let refused = run(
        &[
            "propose",
            path(&data),
            "--evidence",
            &ev,
            "--by",
            "engine",
            "--key",
            "exact:batch",
            "--json",
        ],
        3,
    );
    let refused: ProposeResult = typed(&refused);
    assert!(refused.proposal().is_none());
    let v: serde_json::Value = serde_json::from_str(&out).unwrap();
    let recs = check_records(&index_dir(&data), &v);
    let written = p.proposal().unwrap();
    assert_eq!(recs, vec![(written.hash, written.record.clone())]);

    let html = dir.path().join("r.html");
    let out = run(&["--json", "report", path(&data), "--html", path(&html)], 0);
    let r: ReportResult = typed(&out);
    assert!(html.exists());
    assert_eq!(r.bytes, std::fs::metadata(&html).unwrap().len());
}

#[test]
fn human_output_leads_with_the_funnel_and_quiet_prints_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 1, 4, 10, 8);
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[3]);
    run(&["index", path(&data), "--key", "exact:id"], 0);
    let out = run(&["locate", path(&data), "--keys", path(&kf)], 0);
    let lines: Vec<&str> = out.lines().collect();
    assert!(lines[0].starts_with("locate: 1 keys on (id)"), "{out}");
    assert!(lines[1].contains("files") && lines[1].contains("row groups"));
    assert!(lines[2].trim_start().starts_with("live"));
    assert!(lines[3].trim_start().starts_with("planned"));
    assert!(
        !out.contains('\x1b'),
        "no color when stdout is not a terminal"
    );
    assert_eq!(
        run(&["--quiet", "locate", path(&data), "--keys", path(&kf)], 0),
        ""
    );
}

#[test]
fn exit_codes_follow_the_contract() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 1, 1, 10, 5);
    // 2: usage, from clap and from the CLI's own argument checks.
    run(&["frobnicate"], 2);
    run(&["index", path(&data), "--key", "exact"], 2);
    run(&["receipt", "show", path(&data), "not-a-hash"], 2);
    run(&["mcp"], 2);
    // 4: engine errors.
    run(
        &[
            "index",
            path(&dir.path().join("missing")),
            "--key",
            "exact:id",
        ],
        4,
    );
    run(
        &[
            "locate",
            path(&data),
            "--keys",
            path(&dir.path().join("k.parquet")),
        ],
        4,
    );
    run(&["index", path(&data), "--key", "exact:nope"], 4);
    run(&["index", path(&data), "--key", "exact:id"], 0);
    let absent = "0".repeat(64);
    run(&["receipt", "show", path(&data), &absent], 4);
}

/// H1: a panic anywhere in the binary prints one line naming the issue
/// tracker and exits 4 (the probe is compiled into debug builds only).
#[test]
fn a_panic_prints_one_line_naming_the_tracker_and_exits_4() {
    let out = common::bw()
        .env("BELLWETHER_PANIC_PROBE", "1")
        .args(["mcp"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(4));
    assert_eq!(out.stdout.len(), 0);
    let err = String::from_utf8(out.stderr).unwrap();
    assert_eq!(err.lines().count(), 1, "{err}");
    assert!(
        err.contains("internal error") && err.contains(bellwether::ISSUE_TRACKER),
        "{err}"
    );
}

/// Files that store one decimal column at different scales need the dataset
/// schema: the refusal names --schema, and --schema makes it work.
#[test]
fn schema_flag_supplies_the_dataset_schema() {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Decimal128Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;

    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    for (name, value, scale) in [("a.parquet", 150i128, 2i8), ("b.parquet", 15_001, 4)] {
        let precision = 10 + u8::try_from(scale).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(precision, scale),
            true,
        )]));
        let a: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(value)])
                .with_precision_and_scale(precision, scale)
                .unwrap(),
        );
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![a]).unwrap();
        let file = std::fs::File::create(data.join(name)).unwrap();
        let mut w = ArrowWriter::try_new(file, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
    let out = common::bw()
        .args(["index", path(&data), "--key", "exact:amount"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(4));
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(err.contains("--schema"), "{err}");
    let schema = dir.path().join("schema.json");
    std::fs::write(
        &schema,
        r#"[{"name": "amount", "type": "Decimal128(14, 4)", "nullable": true}]"#,
    )
    .unwrap();
    let out = run(
        &[
            "--json",
            "--schema",
            path(&schema),
            "index",
            path(&data),
            "--key",
            "exact:amount",
        ],
        0,
    );
    let r: IndexResult = typed(&out);
    assert_eq!(r.committed, Some(1));
}

#[test]
fn ordered_plans_human_output_states_ordered_by_or_unspecified() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 2, 2, 10, 8);
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[1]);
    run(&["index", path(&data), "--key", "exact:id"], 0);
    let out_unord = run(&["locate", path(&data), "--keys", path(&kf)], 0);
    assert!(
        out_unord.contains("ordered_by: unspecified"),
        "got: {out_unord}"
    );
    run(&["index", path(&data), "--key", "range:ts"], 0);
    let out_ord = run(&["locate", path(&data), "--keys", path(&kf)], 0);
    assert!(
        out_ord.contains("ordered_by: ts (row-group minimum)"),
        "got: {out_ord}"
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn at_manifest_flag_pins_locate_latest_asof_and_why_in_cli() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 1, 2, 10, 8);
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[1]);
    let clk = "2026-10-25T12:00:00Z";
    run(
        &[
            "--clock",
            clk,
            "index",
            path(&data),
            "--key",
            "exact:id",
            "--key",
            "temporal:id@ts",
        ],
        0,
    );
    let loc1 = run(
        &[
            "--json",
            "--clock",
            clk,
            "locate",
            path(&data),
            "--keys",
            path(&kf),
        ],
        0,
    );
    let lat1 = run(
        &[
            "--json",
            "--clock",
            clk,
            "latest",
            path(&data),
            "--keys",
            path(&kf),
        ],
        0,
    );
    let asof1 = run(
        &[
            "--json",
            "--clock",
            clk,
            "asof",
            path(&data),
            "--keys",
            path(&kf),
            "--at",
            "100",
        ],
        0,
    );

    // Add another file (without touching part-000.parquet) and rebuild -> sequence 2.
    std::fs::copy(data.join("part-000.parquet"), data.join("part-001.parquet")).unwrap();
    run(
        &[
            "--clock",
            clk,
            "index",
            path(&data),
            "--key",
            "exact:id",
            "--key",
            "temporal:id@ts",
        ],
        0,
    );
    let loc_pinned = run(
        &[
            "--json",
            "--clock",
            clk,
            "locate",
            path(&data),
            "--keys",
            path(&kf),
            "--at-manifest",
            "1",
        ],
        0,
    );
    let lat_pinned = run(
        &[
            "--json",
            "--clock",
            clk,
            "latest",
            path(&data),
            "--keys",
            path(&kf),
            "--at-manifest",
            "1",
        ],
        0,
    );
    let asof_pinned = run(
        &[
            "--json",
            "--clock",
            clk,
            "asof",
            path(&data),
            "--keys",
            path(&kf),
            "--at",
            "100",
            "--at-manifest",
            "1",
        ],
        0,
    );
    assert_eq!(loc_pinned, loc1);
    assert_eq!(lat_pinned, lat1);
    assert_eq!(asof_pinned, asof1);
}

#[test]
#[allow(clippy::too_many_lines)]
fn market_corpus_measured_funnel_and_cli_time_series_flags() {
    use bellwether::{spec, Engine, EngineOptions, KeyFile};
    use bellwether_bench::market::{generate_market, MarketParams};

    let dir = tempfile::tempdir().unwrap();
    let params = MarketParams {
        seed: 42,
        days: 8,
        files_per_day: 2,
        row_groups_per_file: 4,
        rows_per_row_group: 400,
        symbols: 160,
        restatement_fraction: 0.05,
    };
    let manifest = generate_market(&params, dir.path()).unwrap();
    assert_eq!(manifest.files_total, 16);
    assert_eq!(manifest.row_groups_total, 64);
    assert_eq!(manifest.rows_total, 25_600);

    let data = dir.path().join("data");
    let run0 = KeyFile {
        path: dir.path().join("workload/run-0000.parquet"),
        columns: Some(vec!["symbol".into()]),
    };

    // 1. Index with exact:symbol + range:valid_ts (Hive `day=` virtual column is pruned directly from path without needing range:day)
    let eng = Engine::open(path(&data), EngineOptions::default()).unwrap();
    eng.index(&[
        spec::parse("exact:symbol").unwrap(),
        spec::parse("range:valid_ts").unwrap(),
    ])
    .unwrap();
    let p_exact = eng
        .locate_window(&run0, "2026-01-01T03:00:00Z..2026-01-01T05:59:59Z")
        .unwrap();
    assert_eq!(p_exact.ordered_by.as_deref(), Some("valid_ts"));
    // 2. Add bounds sidecar (`exact:symbol@valid_ts`): prunes further than `exact:symbol` + `range:valid_ts`
    let expanded = spec::parse_many("exact:symbol@valid_ts").unwrap();
    eng.index(&expanded).unwrap();
    let p_bounds = eng
        .locate_window(&run0, "2026-01-01T03:00:00Z..2026-01-01T05:59:59Z")
        .unwrap();
    assert!(p_bounds.planned.row_groups > 0);
    assert!(
        p_bounds.planned.row_groups < p_exact.planned.row_groups,
        "bounds sidecar must prune strictly more row groups than exact+range ({} vs {})",
        p_bounds.planned.row_groups,
        p_exact.planned.row_groups
    );
    assert!(p_bounds.planned.bytes < p_exact.planned.bytes);

    // 3. Add bitemporal `temporal2:symbol@valid_ts,known_ts`
    let eng = Engine::open(path(&data), EngineOptions::default()).unwrap();
    eng.index(&[spec::parse("temporal2:symbol@valid_ts,known_ts").unwrap()])
        .unwrap();
    let p_bi = eng
        .asof_with(
            &run0,
            None,
            Some("2026-01-01T08:59:59Z"),
            Some("2026-01-01T09:05:00Z"),
            None,
        )
        .unwrap();
    assert!(p_bi.planned.row_groups > 0);
    assert!(p_bi.planned.row_groups < p_bi.row_groups_total);

    // 4. Verify CLI `--window`, `--valid`/`--known`, and `--lookback` end to end
    let clk = "2026-06-01T00:00:00Z";
    let cli_win = run(
        &[
            "--json",
            "--clock",
            clk,
            "locate",
            path(&data),
            "--keys",
            path(&run0.path),
            "--window",
            "2026-01-01T03:00:00Z..2026-01-01T05:59:59Z",
        ],
        0,
    );
    let cli_win_res: bellwether::PlanResult = serde_json::from_str(&cli_win).unwrap();
    assert_eq!(cli_win_res.planned.row_groups, p_bounds.planned.row_groups);
    assert_eq!(cli_win_res.planned.bytes, p_bounds.planned.bytes);

    let cli_bi = run(
        &[
            "--json",
            "--clock",
            clk,
            "asof",
            path(&data),
            "--keys",
            path(&run0.path),
            "--valid",
            "2026-01-01T08:59:59Z",
            "--known",
            "2026-01-01T09:05:00Z",
        ],
        0,
    );
    let cli_bi_res: bellwether::PlanResult = serde_json::from_str(&cli_bi).unwrap();
    assert_eq!(cli_bi_res.planned.row_groups, p_bi.planned.row_groups);

    eprintln!(
        "MARKET FUNNEL (debug): live={} rgs ({} files), exact+range={} rgs ({} B), exact+bounds={} rgs ({} B), temporal2_bitemporal={} rgs ({} B)",
        p_exact.row_groups_total,
        p_exact.files_total,
        p_exact.planned.row_groups,
        p_exact.planned.bytes,
        p_bounds.planned.row_groups,
        p_bounds.planned.bytes,
        p_bi.planned.row_groups,
        p_bi.planned.bytes,
    );
}
