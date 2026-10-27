//! The server over a real stdio round trip (deliverables 1, 5, 6, 8): the
//! `bellwether-mcp` binary is spawned, every line it writes to stdout is a
//! JSON-RPC message, a tool's text over the wire is byte for byte the
//! in-process reply (and so the CLI's `--json` line), the published schemas
//! are the catalog's, the prompt is `AGENTS.md`, the resources read the
//! ledger, and a bad root fails before the handshake with exit 4 and
//! nothing on stdout.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

mod common;

use common::{json, ledger_corpus, objects, ok, server, text, write_keys, Client, CLOCK};
use serde_json::{json, Value};

const EXE: &str = env!("CARGO_BIN_EXE_bellwether-mcp");

#[test]
fn a_stdio_session_matches_the_in_process_server() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data, 2, 3, 25, 300);
    let keys = dir.path().join("k.parquet");
    write_keys(&keys, "id", &[4, 99]);
    let root = data.to_str().unwrap();

    let mut c = Client::spawn(EXE, &["--clock", CLOCK, root]);
    let init = c.initialize();
    let info = &init["result"]["serverInfo"];
    assert_eq!(info["name"], "bellwether-mcp");
    let caps = &init["result"]["capabilities"];
    for cap in ["tools", "resources", "prompts"] {
        assert!(caps.get(cap).is_some(), "{cap} missing from {caps}");
    }

    // The published tools are the catalog, schemas included.
    let listed = c.request("tools/list", json!({}));
    let listed = listed["result"]["tools"].as_array().unwrap().clone();
    let catalog = bellwether_mcp::tools::catalog();
    assert_eq!(listed.len(), catalog.len());
    for (l, t) in listed.iter().zip(&catalog) {
        assert_eq!(l["name"], t.name);
        assert_eq!(l["inputSchema"], t.input, "{}", t.name);
    }

    // observe.why over the wire, then the same call in process over the same
    // folder under the same clock: the same bytes.
    let wire = c.call("observe.why", json!({}));
    assert_eq!(wire["isError"], false);
    let srv = server(&data);
    let local = ok(&srv, "observe.why", json!({}));
    assert_eq!(text(&wire), local.text, "ADR 010 over stdio");
    // The envelope too, except the budget (wall time differs per process).
    let mut wire_env = wire["structuredContent"].clone();
    let mut local_env = serde_json::to_value(&local.envelope).unwrap();
    wire_env.as_object_mut().unwrap().remove("budget");
    local_env.as_object_mut().unwrap().remove("budget");
    assert_eq!(wire_env, local_env);
    let ev = json(&local)["evidence"]["hash"]
        .as_str()
        .unwrap()
        .to_string();

    // A refusal over the wire: typed, with isError set and nothing written.
    let index = data.join(".bellwether");
    let before = objects(&index);
    let r = c.call(
        "propose.key",
        json!({"spec": {"kind": "exact", "columns": ["id"]}, "evidence": [ev], "by": "a",
               "score": 0.99}),
    );
    assert_eq!(r["isError"], true);
    // A score is a caller-supplied cost, refused by name (ADR 033).
    assert_eq!(
        r["structuredContent"]["refusal"]["code"],
        "caller_supplied_cost"
    );
    assert_ne!(
        r["structuredContent"]["refusal"]["next"].as_str().unwrap(),
        ""
    );
    assert_eq!(objects(&index), before);

    // score.why with wait set returns the result; without it, a job.
    let scored = c.call(
        "score.why",
        json!({"keys": keys.to_str().unwrap(), "wait": true}),
    );
    assert_eq!(scored["isError"], false);
    let local = ok(
        &srv,
        "score.why",
        json!({"keys": keys.to_str().unwrap(), "wait": true}),
    );
    assert_eq!(text(&scored), local.text);
    let job = c.call("score.why", json!({"keys": keys.to_str().unwrap()}));
    assert_eq!(job["structuredContent"]["status"], "running");
    assert_eq!(
        job["structuredContent"]["job"]["id"]
            .as_str()
            .unwrap()
            .len(),
        64
    );

    // The one prompt is AGENTS.md.
    let prompts = c.request("prompts/list", json!({}));
    let prompts = prompts["result"]["prompts"].as_array().unwrap();
    assert_eq!(prompts.len(), 1);
    assert_eq!(prompts[0]["name"], bellwether_mcp::prompt::NAME);
    let got = c.request(
        "prompts/get",
        json!({"name": bellwether_mcp::prompt::NAME, "arguments": {}}),
    );
    assert_eq!(
        got["result"]["messages"][0]["content"]["text"],
        bellwether_mcp::prompt::AGENTS_MD
    );
    let other = c.request("prompts/get", json!({"name": "anything_else"}));
    assert!(other.get("error").is_some(), "{other}");

    // The resources read the ledger through the facade.
    let res = c.request("resources/list", json!({}));
    let uris: Vec<&str> = res["result"]["resources"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["uri"].as_str().unwrap())
        .collect();
    assert_eq!(
        uris,
        [
            bellwether_mcp::FINDINGS,
            bellwether_mcp::RECEIPTS,
            bellwether_mcp::SCORECARD
        ]
    );
    for uri in uris {
        let read = c.request("resources/read", json!({ "uri": uri }));
        assert_eq!(
            read["result"]["contents"][0]["text"].as_str().unwrap(),
            srv.resource(uri).unwrap(),
            "{uri}"
        );
    }
    let missing = c.request("resources/read", json!({"uri": "bellwether://nothing"}));
    assert!(missing.get("error").is_some(), "{missing}");

    // Every line the server wrote was a protocol message (checked in
    // `request`), and closing stdin ends the session cleanly.
    let lines = c.lines.len();
    assert!(lines >= 12, "{lines}");
    let (status, rest, _stderr) = c.close();
    assert!(status.success(), "{status:?}");
    for line in rest.lines() {
        let v: Value = serde_json::from_str(line).unwrap();
        assert_eq!(v["jsonrpc"], "2.0");
    }
}

#[test]
fn a_bad_root_fails_before_the_handshake_with_nothing_on_stdout() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("missing");
    let out = std::process::Command::new(EXE)
        .arg(missing.to_str().unwrap())
        .stdin(std::process::Stdio::null())
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(4));
    assert_eq!(out.stdout, [] as [u8; 0]);
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(err.starts_with("bellwether-mcp: "), "{err}");
    assert_eq!(err.lines().count(), 1, "{err}");
}

/// The binary takes every engine and budget flag `bellwether mcp` takes:
/// both flatten the same definitions (`bellwether_mcp::args`). A schema
/// file that cannot be read is a usage error.
#[test]
fn the_binary_takes_the_shared_engine_and_budget_flags() {
    use clap::Args as _;
    let shared = bellwether_mcp::BudgetArgs::augment_args(
        bellwether_mcp::EngineArgs::augment_args(clap::Command::new("shared")),
    );
    let help = std::process::Command::new(EXE)
        .arg("--help")
        .output()
        .unwrap();
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout).unwrap();
    let mut flags = 0;
    for a in shared.get_arguments().filter(|a| !a.is_hide_set()) {
        let long = format!("--{}", a.get_long().unwrap());
        assert!(help.contains(&long), "{long} missing from:\n{help}");
        flags += 1;
    }
    assert_eq!(
        flags, 14,
        "index, format, chunk-size, schema, fingerprint-policy, partitioning, path-template, \
         twin-of, scratch, keep, dataset, 3 budgets"
    );

    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data, 1, 1, 5, 5);
    let out = std::process::Command::new(EXE)
        .args([
            "--schema",
            "/nonexistent/schema.json",
            data.to_str().unwrap(),
        ])
        .stdin(std::process::Stdio::null())
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert_eq!(out.stdout, [] as [u8; 0]);
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(err.contains("cannot read the schema file"), "{err}");

    // So is a malformed path template, before the engine opens.
    let out = std::process::Command::new(EXE)
        .args(["--path-template", "{day:date", data.to_str().unwrap()])
        .stdin(std::process::Stdio::null())
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert_eq!(out.stdout, [] as [u8; 0]);
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(err.starts_with("bellwether-mcp: "), "{err}");
}

#[test]
fn inline_key_values_emit_and_apply_refresh_work_over_stdio() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    common::scattered_corpus(&data, 3, 4, 25, 400);
    let keys = dir.path().join("k.parquet");
    write_keys(&keys, "id", &[5, 77, 123]);
    let root = data.to_str().unwrap();
    let k = keys.to_str().unwrap();

    let srv = server(&data);
    srv.engine()
        .index(&[
            bellwether::spec::parse("exact:id,counts").unwrap(),
            bellwether::spec::parse("temporal:id@ts").unwrap(),
            bellwether::spec::parse("agg:id@batch{sum,count}").unwrap(),
        ])
        .unwrap();

    let mut c = Client::spawn(EXE, &["--clock", CLOCK, root]);
    c.initialize();

    // (a) observe.locate with inline key_values + emit: "duckdb"
    let located_inline = c.call(
        "observe.locate",
        json!({
            "key_columns": ["id"],
            "key_values": [[5], [77], [123]],
            "emit": "duckdb"
        }),
    );
    assert_eq!(located_inline["isError"], false, "{located_inline}");
    let inline_v: Value = serde_json::from_str(text(&located_inline)).unwrap();
    let file_located = ok(&srv, "observe.locate", json!({"keys": k}));
    let file_v = json(&file_located);
    assert_eq!(inline_v["key_set"], file_v["key_set"]);
    assert_eq!(inline_v["planned"], file_v["planned"]);
    assert_eq!(inline_v["emitted"]["engine"], "duckdb");
    assert_eq!(inline_v["emitted"]["residual"], "full");
    assert_eq!(inline_v["emitted"]["ordered"], false);
    let sql = inline_v["emitted"]["text"].as_str().unwrap();
    assert!(sql.contains("read_parquet") && sql.contains("123"), "{sql}");

    // observe.asof with key_values + emit: "datafusion", and observe.count / observe.aggregate with key_values.
    let asof_inline = c.call(
        "observe.asof",
        json!({
            "key_columns": ["id"],
            "key_values": [[5], [77]],
            "at": "1970-01-01T00:01:00Z",
            "emit": "datafusion"
        }),
    );
    assert_eq!(asof_inline["isError"], false, "{asof_inline}");
    let asof_v: Value = serde_json::from_str(text(&asof_inline)).unwrap();
    assert_eq!(asof_v["emitted"]["engine"], "datafusion");

    let count_inline = c.call(
        "observe.count",
        json!({
            "key_columns": ["id"],
            "key_values": [[5], [77], [123]]
        }),
    );
    assert_eq!(count_inline["isError"], false, "{count_inline}");
    let count_v: Value = serde_json::from_str(text(&count_inline)).unwrap();
    assert_eq!(count_v["keys"], 3);

    let agg_inline = c.call(
        "observe.aggregate",
        json!({
            "key_columns": ["id"],
            "key_values": [[5], [77]],
            "value": "batch"
        }),
    );
    assert_eq!(agg_inline["isError"], false, "{agg_inline}");
    let agg_v: Value = serde_json::from_str(text(&agg_inline)).unwrap();
    assert_eq!(agg_v["keys"], 2);
    // Contradictory or incomplete key_values inputs are refused with invalid_argument.
    for (bad_args, want_field) in [
        (
            json!({"keys": k, "key_columns": ["id"], "key_values": [[5]]}),
            "key_values",
        ),
        (
            json!({"key_columns": ["id"], "key_values": []}),
            "key_values",
        ),
        (json!({"key_values": [[5]]}), "key_columns"),
    ] {
        let r = c.call("observe.locate", bad_args.clone());
        assert_eq!(r["isError"], true, "{bad_args}");
        assert_eq!(
            r["structuredContent"]["refusal"]["code"], "invalid_argument",
            "{bad_args}: {r}"
        );
        assert_eq!(
            r["structuredContent"]["refusal"]["fields"]["field"], want_field,
            "{bad_args}: {r}"
        );
    }

    // (b) score.why with inline key_values produces the same workload hash and funnel as keys file.
    let why_inline = c.call(
        "score.why",
        json!({
            "key_columns": ["id"],
            "key_values": [[5], [77], [123]],
            "wait": true
        }),
    );
    assert_eq!(why_inline["isError"], false, "{why_inline}");
    let why_inline_v: Value = serde_json::from_str(text(&why_inline)).unwrap();
    let why_file = ok(&srv, "score.why", json!({"keys": k, "wait": true}));
    let why_file_v = json(&why_file);
    assert_eq!(
        why_inline_v["workload"]["hash"],
        why_file_v["workload"]["hash"]
    );
    assert_eq!(why_inline_v["current"], why_file_v["current"]);

    // (c) apply.refresh healing a stale_index so a subsequent observe.locate succeeds.
    common::write_file(
        &data.join("part-999.parquet"),
        &[vec![(5, 1, Some(1_000_000), "late".into())]],
    );
    let stale = c.call(
        "observe.locate",
        json!({"key_columns": ["id"], "key_values": [[5]]}),
    );
    assert_eq!(stale["isError"], true, "{stale}");
    assert_eq!(
        stale["structuredContent"]["refusal"]["code"], "stale_index",
        "{stale}"
    );
    assert!(
        stale["structuredContent"]["refusal"]["next"]
            .as_str()
            .unwrap()
            .contains("apply.refresh"),
        "{stale}"
    );

    let dry = c.call("apply.refresh", json!({"dry_run": true}));
    assert_eq!(dry["isError"], false, "{dry}");
    let dry_v: Value = serde_json::from_str(text(&dry)).unwrap();
    assert_eq!(dry_v["would_refresh"], true, "{dry_v}");
    assert_eq!(dry_v["untracked"].as_array().unwrap().len(), 1, "{dry_v}");

    // Still stale after dry_run.
    let still_stale = c.call(
        "observe.locate",
        json!({"key_columns": ["id"], "key_values": [[5]]}),
    );
    assert_eq!(still_stale["isError"], true);

    let refreshed = c.call("apply.refresh", json!({"wait": true}));
    assert_eq!(refreshed["isError"], false, "{refreshed}");
    let ref_v: Value = serde_json::from_str(text(&refreshed)).unwrap();
    assert_eq!(ref_v["committed"], 2, "{ref_v}");
    assert_eq!(ref_v["files_new"], 1, "{ref_v}");

    let healed = c.call(
        "observe.locate",
        json!({"key_columns": ["id"], "key_values": [[5]]}),
    );
    assert_eq!(healed["isError"], false, "{healed}");
    let healed_v: Value = serde_json::from_str(text(&healed)).unwrap();
    assert_eq!(healed_v["sequence"], 2);

    let (status, _, _) = c.close();
    assert!(status.success());
}
