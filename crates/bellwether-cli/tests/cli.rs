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
    // The MCP server opens the engine before the handshake: a missing root
    // fails with nothing on stdout.
    let out = run(&["mcp", path(&dir.path().join("missing"))], 4);
    assert!(out.is_empty(), "{out}");
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
