//! List keys (B4) on the command line and over MCP: `index --key
//! list:<col>`, `locate --any/--all`, `why --mode contains-any|contains-all`,
//! and their refusals. ADR 010: each MCP tool's text is the CLI's `--json`
//! line byte for byte (`observe.locate` with `contains`, `score.why` with
//! `contains`), refusals included. The human output names the membership
//! lookup, the `list` column, and the `R-list-1` lint.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::assert_is_empty
)]

mod common;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Int64Array, ListBuilder, RecordBatch, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use common::{check_records, index_dir, path, run};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde_json::{json, Value};

const CLOCK: &str = "2026-10-26T12:00:00.000Z";

/// Three files of two row groups of ten rows; tags clustered by file,
/// `hot` everywhere, null and empty lists.
fn corpus(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    let tags = ListBuilder::new(StringBuilder::new())
        .finish()
        .data_type()
        .clone();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tags", tags, true),
    ]));
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(1 << 20))
        .build();
    let mut id = 0i64;
    for f in 0..3 {
        let file = File::create(dir.join(format!("part-{f}.parquet"))).unwrap();
        let mut w = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props.clone())).unwrap();
        for rg in 0..2 {
            let mut ids = Vec::new();
            let mut b = ListBuilder::new(StringBuilder::new());
            for r in 0..10 {
                ids.push(id);
                id += 1;
                match r {
                    0 => b.append(false),
                    1 => b.append(true),
                    _ => {
                        b.values().append_value("hot");
                        b.values().append_value(format!("f{f}-{rg}"));
                        b.append(true);
                    }
                }
            }
            let cols: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(ids)), Arc::new(b.finish())];
            w.write(&RecordBatch::try_new(Arc::clone(&schema), cols).unwrap())
                .unwrap();
            w.flush().unwrap();
        }
        w.close().unwrap();
    }
}

fn values(p: &Path, v: &[Option<&str>]) {
    let schema = Arc::new(Schema::new(vec![Field::new("tags", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(v.to_vec())) as ArrayRef],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(File::create(p).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn server(data: &Path) -> bellwether_mcp::Server {
    bellwether_mcp::Server::new(&bellwether_mcp::Config {
        root: path(data).to_string(),
        engine: bellwether::EngineOptions {
            clock: Some(CLOCK.into()),
            ..bellwether::EngineOptions::default()
        },
        budget: bellwether_mcp::Budget::default(),
        join: None,
        workspace: false,
    })
    .unwrap()
}

fn cli(args: &[&str], code: i32) -> String {
    run(&[&["--clock", CLOCK, "--json"], args].concat(), code)
}

fn human(args: &[&str], code: i32) -> String {
    run(&[&["--clock", CLOCK], args].concat(), code)
}

/// Calls `tool` in process first, then the CLI; asserts the CLI exits
/// `code` and prints the tool's text; returns it parsed.
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

/// A refusal both ways: the MCP refusal's code is `code_name` and its
/// message is the one the CLI prints to stderr (exit `code`), where the
/// CLI adds its own next step; returns (MCP next step, CLI stderr).
fn refused(
    srv: &bellwether_mcp::Server,
    tool: &str,
    args: Value,
    cli_args: &[&str],
    code: i32,
    code_name: &str,
) -> (String, String) {
    let r = srv.call(tool, args);
    let refusal = r.envelope.refusal.expect("a refusal");
    assert_eq!(refusal.code.as_str(), code_name, "{refusal:?}");
    let out = assert_cmd::Command::cargo_bin("bellwether")
        .unwrap()
        .args([&["--clock", CLOCK, "--json"], cli_args].concat())
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(code));
    assert!(out.stdout.is_empty());
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(
        stderr.contains(&refusal.message),
        "CLI: {stderr}\nMCP: {}",
        refusal.message
    );
    (refusal.next, stderr)
}

#[test]
fn list_lookups_are_byte_identical_over_cli_and_mcp() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data);
    let v = dir.path().join("v.parquet");
    values(&v, &[Some("f1-0"), Some("f2-1")]);
    let all = dir.path().join("all.parquet");
    values(&all, &[Some("hot"), Some("f0-1")]);
    let (d, k, ka) = (path(&data), path(&v), path(&all));
    let srv = server(&data);

    // score.why with contains = why --mode contains-any.
    let why = both(
        &srv,
        "score.why",
        json!({"keys": k, "contains": "any", "wait": true}),
        &["why", d, "--from", k, "--mode", "contains-any"],
        0,
    );
    check_records(&index_dir(&data), &why);
    assert_eq!(why["workload"]["query_kind"], "contains_any");
    assert_eq!(why["candidates"][0]["spec"]["kind"], "list");
    assert_eq!(why["candidates"][0]["kind_column"]["kind"], "list");
    assert_eq!(why["candidates"][0]["kind_column"]["value"], "7");
    assert!(why["profile"]["lints"]
        .as_array()
        .unwrap()
        .iter()
        .any(|l| l["rule"] == "R-list-1"));

    // why priced on the recorded workload as recorded = score.why on it
    // (before any index: a manifest's age is measured on the wall clock).
    let h = why["workload"]["hash"].as_str().unwrap();
    let again = both(
        &srv,
        "score.why",
        json!({"workload": h, "wait": true}),
        &["why", d, "--workload", h],
        0,
    );
    assert_eq!(
        again["candidates"][0]["proposed"],
        why["candidates"][0]["proposed"]
    );
    // ...and score.why with contains = why --mode contains-all.
    let w_all = both(
        &srv,
        "score.why",
        json!({"keys": ka, "contains": "all", "wait": true}),
        &["why", d, "--from", ka, "--mode", "contains-all"],
        0,
    );
    assert_eq!(w_all["workload"]["query_kind"], "contains_all");

    // Before the list key: observe.locate with contains is refused the
    // same way locate --any is, naming the key to declare.
    let (next, stderr) = refused(
        &srv,
        "observe.locate",
        json!({"keys": k, "contains": "any"}),
        &["locate", d, "--keys", k, "--any"],
        4,
        "not_indexed",
    );
    assert!(stderr.contains("--key list:tags"), "{stderr}");
    assert!(next.contains("bellwether index"), "{next}");

    run(&["index", d, "--key", "list:tags"], 0);
    // observe.locate {contains: any|all} = locate --any|--all.
    let any = both(
        &srv,
        "observe.locate",
        json!({"keys": k, "contains": "any"}),
        &["locate", d, "--keys", k, "--any"],
        0,
    );
    assert_eq!(any["lookup"]["kind"], "contains_any");
    assert_eq!(any["planned"]["row_groups"], 2);
    let a = both(
        &srv,
        "observe.locate",
        json!({"keys": ka, "contains": "all"}),
        &["locate", d, "--keys", ka, "--all"],
        0,
    );
    assert_eq!(a["lookup"]["kind"], "contains_all");
    assert_eq!(a["planned"]["row_groups"], 1);
    // A plain locate by the list column: the same refusal both ways.
    let (_, plain) = refused(
        &srv,
        "observe.locate",
        json!({"keys": k}),
        &["locate", d, "--keys", k],
        4,
        "invalid_argument",
    );
    assert!(
        plain.contains("--any") && plain.contains("--all"),
        "{plain}"
    );
}

#[test]
fn list_human_output_and_usage_errors() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data);
    let v = dir.path().join("v.parquet");
    values(&v, &[Some("f1-0")]);
    let (d, k) = (path(&data), path(&v));
    // why: the funnel names the membership lookup, the list column holds
    // the distinct elements, and the lint says what to declare.
    let why = human(&["why", d, "--from", k, "--mode", "contains-any"], 0);
    assert!(why.contains("rows whose tags holds any value"), "{why}");
    assert!(why.contains("list:tags"), "{why}");
    assert!(
        why.contains("floor: only row groups holding a value"),
        "{why}"
    );
    assert!(why.contains("list: distinct elements"), "{why}");
    assert!(why.contains("R-list-1"), "{why}");
    let header = why
        .lines()
        .find(|l| l.contains("row groups") && l.contains("bytes"))
        .unwrap();
    assert!(header.trim_end().ends_with("list"), "{header}");
    // locate --any before the key: what to run
    let out = assert_cmd::Command::cargo_bin("bellwether")
        .unwrap()
        .args(["locate", d, "--keys", k, "--any"])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("bellwether index") && stderr.contains("list:tags"),
        "{stderr}"
    );
    run(&["index", d, "--key", "list:tags"], 0);
    let any = human(&["locate", d, "--keys", k, "--any"], 0);
    assert!(
        any.contains("locate rows whose tags holds any value"),
        "{any}"
    );
    // usage: --any with a window, --any with --all
    run(&["locate", d, "--keys", k, "--any", "--window", "1..2"], 2);
    run(&["locate", d, "--keys", k, "--any", "--all"], 2);
    // usage: a membership mode with --latest-by
    run(
        &[
            "why",
            d,
            "--from",
            k,
            "--mode",
            "contains-all",
            "--latest-by",
            "id",
        ],
        2,
    );
}
