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
