//! The CLI over the facade: `--json` is the facade's typed result, every
//! printed record is the ledger's stored bytes, human output leads with the
//! funnel, and exit codes follow the contract (0, 1, 2, 3, 4).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

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

/// Regression tests for real-data defects 1, 2, 11, 12, 14:
/// - Defect 1: datasets with nested columns index and profile on flat keys,
///   skipping nested columns in the profile and refusing nested KEY columns
///   with a typed error and no Rust `Debug` output.
/// - Defect 2: dotted nested-field names (`repo.id`) are refused with a
///   flatten hint and no `Debug` output; documented in `docs/contract-v3.md`.
/// - Defect 11: `.json` files starting with `{` and with valid JSON-object
///   lines are discovered as JSON lines; non-JSONL `.json` or empty folders
///   name accepted extensions.
/// - Defect 12: `index` table aligns long spec labels (`complete at` column).
/// - Defect 14: `docs/agent-loop.md` has no stale "does not yet" status line.
#[test]
fn realdata_defects_nested_columns_dot_json_index_table_and_docs() {
    use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, StructArray};
    use arrow::datatypes::{DataType, Field, Fields, Schema};
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();

    // Defect 1 & 2 on Parquet with a struct column.
    let nested_dir = dir.path().join("ghnested");
    std::fs::create_dir_all(&nested_dir).unwrap();
    let repo_fields = Fields::from(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]);
    let struct_arr = StructArray::new(
        repo_fields.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef,
            Arc::new(StringArray::from(vec!["r1", "r2", "r3"])) as ArrayRef,
        ],
        None,
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("repo", DataType::Struct(repo_fields), true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(struct_arr) as ArrayRef,
        ],
    )
    .unwrap();
    let f = std::fs::File::create(nested_dir.join("part-0.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(f, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let why_human = run(&["why", path(&nested_dir)], 0);
    assert!(
        why_human.contains(
            "not candidates: repo (a struct (id, name): a nested column, flatten to index)"
        ),
        "{why_human}"
    );
    assert!(!why_human.contains("Field {"), "{why_human}");

    // Indexing on a flat column succeeds even though `repo` is a struct.
    let idx_out = run(&["index", path(&nested_dir), "--key", "exact:id"], 0);
    assert!(
        idx_out.contains("committed manifest sequence 1"),
        "{idx_out}"
    );

    // Indexing on the nested column itself is refused with a sentence (no Field { Debug }).
    let err_out = common::bw()
        .args(["index", path(&nested_dir), "--key", "exact:repo"])
        .output()
        .unwrap();
    assert_eq!(err_out.status.code(), Some(4));
    let stderr = String::from_utf8_lossy(&err_out.stderr);
    assert!(stderr.contains("repo"), "{stderr}");
    assert!(stderr.contains("nested column"), "{stderr}");
    assert!(!stderr.contains("Field {"), "{stderr}");

    // Defect 2: addressing `repo.id` says nested field addressing is not supported and to flatten first.
    let dot_err = common::bw()
        .args(["why", path(&nested_dir), "--columns", "repo.id"])
        .output()
        .unwrap();
    assert_eq!(dot_err.status.code(), Some(4));
    let dot_stderr = String::from_utf8_lossy(&dot_err.stderr);
    assert!(dot_stderr.contains("flatten"), "{dot_stderr}");
    assert!(!dot_stderr.contains("Field {"), "{dot_stderr}");

    // Defect 11: `.json` file with JSON lines is discovered and indexed.
    let json_dir = dir.path().join("ghjson");
    std::fs::create_dir_all(&json_dir).unwrap();
    std::fs::write(
        json_dir.join("2024-01-01-0.json"),
        "{\"id\":1,\"actor\":{\"id\":10,\"login\":\"a\"}}\n{\"id\":2,\"actor\":{\"id\":20,\"login\":\"b\"}}\n",
    )
    .unwrap();
    let json_why = run(&["why", path(&json_dir), "--format", "jsonl"], 0);
    assert!(json_why.contains("2 rows"), "{json_why}");
    let json_idx = run(
        &[
            "index",
            path(&json_dir),
            "--format",
            "jsonl",
            "--key",
            "exact:id",
        ],
        0,
    );
    assert!(
        json_idx.contains("committed manifest sequence 1"),
        "{json_idx}"
    );

    // A `.json` file whose first byte is not `{` is refused naming accepted extensions.
    let bad_json_dir = dir.path().join("badjson");
    std::fs::create_dir_all(&bad_json_dir).unwrap();
    std::fs::write(bad_json_dir.join("array.json"), "[{\"id\":1}]\n").unwrap();
    let bad_out = common::bw()
        .args(["why", path(&bad_json_dir), "--format", "jsonl"])
        .output()
        .unwrap();
    assert_eq!(bad_out.status.code(), Some(4));
    let bad_stderr = String::from_utf8_lossy(&bad_out.stderr);
    assert!(bad_stderr.contains(".jsonl"), "{bad_stderr}");
    assert!(bad_stderr.contains(".ndjson"), "{bad_stderr}");

    // Defect 12: long spec labels align `complete at` column in `index` human output.
    let long_dir = dir.path().join("longspec");
    corpus(&long_dir, 1, 2, 10, 20);
    let table = run(
        &[
            "index",
            path(&long_dir),
            "--key",
            "exact:id",
            "--key",
            "temporal:id,batch,payload@ts",
        ],
        0,
    );
    let spec_lines: Vec<&str> = table
        .lines()
        .filter(|l| {
            l.contains("complete at")
                || l.contains("exact:id")
                || l.contains("temporal:id,batch,payload@ts")
        })
        .collect();
    assert_eq!(spec_lines.len(), 3, "{table}");
    let widths: Vec<usize> = spec_lines.iter().map(|l| l.chars().count()).collect();
    assert_eq!(
        widths[0], widths[1],
        "header and short spec aligned:\n{table}"
    );
    assert_eq!(
        widths[1], widths[2],
        "short and long spec aligned:\n{table}"
    );

    // Defect 14: docs/agent-loop.md has no stale "does not yet" status line,
    // and docs/contract-v3.md + docs/rule-catalog.md have the required entries.
    let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let agent_loop = std::fs::read_to_string(repo_root.join("docs/agent-loop.md")).unwrap();
    assert!(
        !agent_loop.contains("the engine that writes them does not"),
        "docs/agent-loop.md still has stale status text"
    );
    let contract_v3 = std::fs::read_to_string(repo_root.join("docs/contract-v3.md")).unwrap();
    assert!(
        contract_v3.contains("repo.id"),
        "contract-v3 missing nested field entry"
    );
    let rule_cat = std::fs::read_to_string(repo_root.join("docs/rule-catalog.md")).unwrap();
    assert!(
        rule_cat.contains("### no_discriminating_column"),
        "rule-catalog missing no_discriminating_column"
    );
}
