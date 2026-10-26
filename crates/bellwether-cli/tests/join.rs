//! Join keys (B3) on the command line and over MCP: `index --key
//! join:...`, `join semijoin|overlap|verify`, `why --mode semijoin --join`,
//! and their refusals. ADR 010: each MCP tool's text is the CLI's `--json`
//! line byte for byte (`observe.semijoin`, `observe.join_overlap`,
//! `observe.join_verify`, `score.why` with `join`), refusals included; the
//! server takes the join key from its operator (`--join`), never from a
//! tool argument. The human output names both steps, the `join` column,
//! and, for a stale key, the command that rebuilds it.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

mod common;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use common::{check_records, index_dir, path, run};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde_json::{json, Value};

const CLOCK: &str = "2026-10-26T12:00:00.000Z";
const JOIN: &str = "join:orders.order_id=payables.paid_order";

fn write(path: &Path, key: &str, ids: &[i64], per_rg: usize) {
    let schema = Arc::new(Schema::new(vec![
        Field::new(key, DataType::Int64, true),
        Field::new("amount", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from((0..ids.len() as i64).collect::<Vec<_>>())),
        ],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(per_rg))
        .build();
    let mut w = ArrowWriter::try_new(File::create(path).unwrap(), schema, Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// orders 0..200 clustered; payables for 100..300 with 1:N skew, shuffled.
fn corpus(root: &Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let (l, r) = (root.join("orders"), root.join("payables"));
    std::fs::create_dir_all(&l).unwrap();
    std::fs::create_dir_all(&r).unwrap();
    write(
        &l.join("o.parquet"),
        "order_id",
        &(0..200).collect::<Vec<_>>(),
        10,
    );
    let mut pay = Vec::new();
    for id in 100..300i64 {
        for _ in 0..=(id % 3) {
            pay.push(id);
        }
    }
    let n = pay.len();
    let shuffled: Vec<i64> = (0..n).map(|i| pay[(i * 53) % n]).collect();
    write(&r.join("p.parquet"), "paid_order", &shuffled, 25);
    (
        std::fs::canonicalize(l).unwrap(),
        std::fs::canonicalize(r).unwrap(),
    )
}

fn keys(p: &Path, ids: &[i64]) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "order_id",
        DataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(File::create(p).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn server(data: &Path, join: Option<&str>) -> bellwether_mcp::Server {
    bellwether_mcp::Server::new(&bellwether_mcp::Config {
        root: path(data).to_string(),
        engine: bellwether::EngineOptions {
            clock: Some(CLOCK.into()),
            ..bellwether::EngineOptions::default()
        },
        budget: bellwether_mcp::Budget::default(),
        join: join.map(str::to_string),
    })
    .unwrap()
}

fn cli(args: &[&str], code: i32) -> String {
    run(&[&["--clock", CLOCK, "--json"], args].concat(), code)
}

fn human(args: &[&str], code: i32) -> String {
    run(&[&["--clock", CLOCK], args].concat(), code)
}

fn both(
    srv: &bellwether_mcp::Server,
    tool: &str,
    args: Value,
    cli_args: &[&str],
    code: i32,
) -> Value {
    let r = srv.call(tool, args);
    let m = r.text + "\n";
    let c = cli(cli_args, code);
    assert_eq!(c, m, "{tool}: the CLI line and the MCP text differ");
    serde_json::from_str(&c).unwrap()
}

fn refused(
    srv: &bellwether_mcp::Server,
    tool: &str,
    args: Value,
    cli_args: &[&str],
    code: i32,
    code_name: &str,
) -> (Value, String) {
    let r = srv.call(tool, args);
    let refusal = r.envelope.refusal.expect("a refusal");
    assert_eq!(refusal.code.as_str(), code_name, "{refusal:?}");
    let out = assert_cmd::Command::cargo_bin("bellwether")
        .unwrap()
        .args([&["--clock", CLOCK, "--json"], cli_args].concat())
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(code));
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(
        stderr.contains(&refusal.message),
        "CLI: {stderr}\nMCP: {}",
        refusal.message
    );
    (refusal.fields, stderr)
}

#[test]
fn join_tools_are_byte_identical_over_cli_and_mcp() {
    let dir = tempfile::tempdir().unwrap();
    let (l, r) = corpus(dir.path());
    let k = dir.path().join("k.parquet");
    keys(&k, &[5, 99, 100, 150, 199, 250]);
    let (d, rd, kp) = (path(&l), path(&r), path(&k));
    run(&["index", rd, "--key", "exact:paid_order"], 0);
    run(&["index", d, "--key", "exact:order_id"], 0);
    let srv = server(&l, Some("payables.paid_order"));

    // why --mode semijoin --join = score.why with join.
    let why = both(
        &srv,
        "score.why",
        json!({"keys": kp, "join": true, "wait": true}),
        &[
            "why",
            d,
            "--from",
            kp,
            "--mode",
            "semijoin",
            "--join",
            "payables.paid_order",
        ],
        0,
    );
    check_records(&index_dir(&l), &why);
    assert_eq!(why["workload"]["query_kind"], "semijoin");
    assert_eq!(why["candidates"][0]["spec"]["kind"], "join");
    assert_eq!(why["candidates"][0]["kind_column"]["kind"], "join");
    assert_eq!(why["recommended"]["spec"]["kind"], "join");
    // Two runs (why --from repeated = score.why more_keys): R-join-1.
    let k2 = dir.path().join("k2.parquet");
    keys(&k2, &[120, 130]);
    let k2p = path(&k2);
    let two = both(
        &srv,
        "score.why",
        json!({"keys": kp, "more_keys": [k2p], "join": true, "wait": true}),
        &[
            "why",
            d,
            "--from",
            kp,
            "--from",
            k2p,
            "--mode",
            "semijoin",
            "--join",
            "payables.paid_order",
        ],
        0,
    );
    assert_eq!(two["workload"]["runs"], 2);
    assert!(two["profile"]["lints"]
        .as_array()
        .unwrap()
        .iter()
        .any(|l| l["rule"] == "R-join-1"));
    assert!(!why["profile"]["lints"]
        .as_array()
        .unwrap()
        .iter()
        .any(|l| l["rule"] == "R-join-1"));

    // Before the join key: observe.semijoin is refused like join semijoin.
    let (fields, stderr) = refused(
        &srv,
        "observe.semijoin",
        json!({"keys": kp}),
        &[
            "join",
            "semijoin",
            d,
            "--join",
            "payables.paid_order",
            "--keys",
            kp,
        ],
        4,
        "not_indexed",
    );
    assert_eq!(fields["needs"], JOIN);
    assert!(stderr.contains(JOIN), "{stderr}");

    run(&["index", d, "--key", JOIN], 0);
    let s = both(
        &srv,
        "observe.semijoin",
        json!({"keys": kp}),
        &[
            "join",
            "semijoin",
            d,
            "--join",
            "payables.paid_order",
            "--keys",
            kp,
        ],
        0,
    );
    assert_eq!(s["matched"], 3);
    assert_eq!(s["left"]["root"], d);
    assert_eq!(s["right"]["root"], rd);
    let o = both(
        &srv,
        "observe.join_overlap",
        json!({}),
        &["join", "overlap", d, "--join", "payables.paid_order"],
        0,
    );
    assert_eq!(o["both"], 100);
    assert_eq!(o["left_only"], 100);
    assert_eq!(o["right_only"], 100);
    let v = both(
        &srv,
        "observe.join_verify",
        json!({}),
        &["join", "verify", d, "--join", "payables.paid_order"],
        0,
    );
    assert_eq!(v["pass"], true);

    // The right commits again: stale_join both ways, with the command.
    write(&r.join("p2.parquet"), "paid_order", &[5], 25);
    run(&["index", rd], 0);
    let (fields, stderr) = refused(
        &srv,
        "observe.semijoin",
        json!({"keys": kp}),
        &[
            "join",
            "semijoin",
            d,
            "--join",
            "payables.paid_order",
            "--keys",
            kp,
        ],
        4,
        "stale_join",
    );
    assert_eq!(fields["side"], "right");
    assert!(
        stderr.contains(&format!("bellwether index {d}")),
        "{stderr}"
    );
    let v = both(
        &srv,
        "observe.join_verify",
        json!({}),
        &["join", "verify", d],
        1,
    );
    assert_eq!(v["pass"], false);
    assert_eq!(v["joins"][0]["state"]["kind"], "stale_right");

    // A server started without --join refuses, naming the flag.
    let bare = server(&l, None);
    let r = bare.call("observe.semijoin", json!({"keys": kp}));
    let refusal = r.envelope.refusal.unwrap();
    assert_eq!(refusal.code.as_str(), "invalid_argument");
    assert!(refusal.message.contains("--join"), "{}", refusal.message);
    // No tool takes the right dataset as an argument.
    let r = srv.call(
        "observe.semijoin",
        json!({"keys": kp, "right": "elsewhere"}),
    );
    assert_eq!(r.envelope.refusal.unwrap().code.as_str(), "invalid_input");
}

#[test]
fn join_human_output_and_usage_errors() {
    let dir = tempfile::tempdir().unwrap();
    let (l, r) = corpus(dir.path());
    let k = dir.path().join("k.parquet");
    keys(&k, &[5, 150, 199]);
    let (d, rd, kp) = (path(&l), path(&r), path(&k));
    run(&["index", rd, "--key", "exact:paid_order"], 0);
    run(&["index", d, "--key", "exact:order_id"], 0);
    let w = human(
        &[
            "why",
            d,
            "--from",
            kp,
            "--mode",
            "semijoin",
            "--join",
            "payables.paid_order",
        ],
        0,
    );
    assert!(w.contains("join: asked keys both datasets hold"), "{w}");
    assert!(w.contains("recommended (for semijoin with payables.paid_order): join:orders.order_id=payables.paid_order"), "{w}");
    assert!(w.contains("no join key serves this semi-join"), "{w}");
    let header = w
        .lines()
        .find(|l| l.contains("row groups") && l.contains("bytes"))
        .unwrap();
    assert!(header.trim_end().ends_with("join"), "{header}");
    // Declaring the join key with the left exact key in one call.
    let idx = human(&["index", d, "--key", "exact:order_id", "--key", JOIN], 0);
    assert!(idx.contains("join keys"), "{idx}");
    assert!(
        idx.contains("rebuilt; join:orders.order_id=payables.paid_order: fresh"),
        "{idx}"
    );
    assert!(idx.contains(&format!("right dataset {rd}")), "{idx}");
    let s = human(
        &[
            "join",
            "semijoin",
            d,
            "--join",
            "payables.paid_order",
            "--keys",
            kp,
        ],
        0,
    );
    assert!(
        s.contains("semijoin join:orders.order_id=payables.paid_order: 2 of 3 asked keys are on both datasets"),
        "{s}"
    );
    assert!(s.contains("step 1") && s.contains("step 2"), "{s}");
    assert!(
        s.contains("residual: keep the rows whose (paid_order) is one of the 2 shared keys"),
        "{s}"
    );
    assert!(s.contains("the other 1 key is missing from one dataset or both, so a semi-join keeps no row of it"), "{s}");
    let o = human(&["join", "overlap", d, "--join", JOIN], 0);
    assert!(o.contains("both sides") && o.contains("right only"), "{o}");
    let w = human(
        &[
            "why",
            d,
            "--from",
            kp,
            "--mode",
            "semijoin",
            "--join",
            "payables.paid_order",
        ],
        0,
    );
    assert!(w.contains("(semijoin with payables.paid_order)"), "{w}");
    assert!(w.contains("each cost below sums both steps"), "{w}");
    // stale on the left, then refreshed by index
    write(&l.join("o2.parquet"), "order_id", &[1000], 10);
    run(&["index", d], 0);
    let v = human(&["join", "verify", d], 0);
    assert!(v.contains("every join key is fresh"), "{v}");
    // usage
    run(
        &["why", d, "--from", kp, "--join", "payables.paid_order"],
        2,
    );
    run(&["why", d, "--from", kp, "--mode", "semijoin"], 2);
    run(&["join", "semijoin", d, "--keys", kp], 2);
    let out = assert_cmd::Command::cargo_bin("bellwether")
        .unwrap()
        .args(["join", "overlap", d, "--join", "payables"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(4));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("payables.order_id"), "{stderr}");
}

#[test]
fn fix_on_a_join_proposal_prints_both_certificates() {
    let dir = tempfile::tempdir().unwrap();
    let (l, r) = corpus(dir.path());
    let k = dir.path().join("k.parquet");
    keys(&k, &[5, 150, 199, 250]);
    let (d, rd, kp) = (path(&l), path(&r), path(&k));
    run(&["index", rd, "--key", "exact:paid_order"], 0);
    run(&["index", d, "--key", "exact:order_id"], 0);
    let why: Value = serde_json::from_str(&cli(
        &[
            "why",
            d,
            "--from",
            kp,
            "--mode",
            "semijoin",
            "--join",
            "payables.paid_order",
        ],
        0,
    ))
    .unwrap();
    let p = why["recommended"]["proposal"].as_str().unwrap().to_string();
    let w = why["workload"]["hash"].as_str().unwrap().to_string();
    let fixed = human(&["fix", d, "--proposal", &p, "--workload", &w], 0);
    assert!(
        fixed.contains(&format!("right dataset {rd}: pass")),
        "{fixed}"
    );
    assert!(
        fixed.contains("rows compared on both datasets: those of the 2 of 4 asked keys"),
        "{fixed}"
    );
    assert!(fixed.contains("receipt"), "{fixed}");
    let v = human(&["join", "verify", d], 0);
    assert!(v.contains("every join key is fresh"), "{v}");
}
