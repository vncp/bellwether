//! ADR 010 for the MCP server (WP12 deliverable 6): for the same inputs on
//! the same ledger, the text an MCP tool returns is the CLI's `--json` line
//! byte for byte (without its newline), for every tool that has a CLI
//! counterpart, refusals included. Both sides run under the same fixed
//! clock, so the records they write are identical. This extends WP9's
//! facade-serialization test (`review_probe.rs`) from the facade to the
//! server. Also: `bellwether mcp` serves the same server over stdio.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

mod common;

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};

use common::{corpus, index_dir, keys, path, run};
use serde_json::{json, Value};

/// The fixed clock both sides use.
const CLOCK: &str = "2026-10-24T12:00:00.000Z";

fn server(data: &Path) -> bellwether_mcp::Server {
    bellwether_mcp::Server::new(&bellwether_mcp::Config {
        root: path(data).to_string(),
        engine: bellwether::EngineOptions {
            clock: Some(CLOCK.into()),
            ..bellwether::EngineOptions::default()
        },
        budget: bellwether_mcp::Budget::default(),
    })
    .unwrap()
}

/// The CLI's `--json` line for `args` under the fixed clock.
fn cli(args: &[&str], code: i32) -> String {
    run(&[&["--clock", CLOCK, "--json"], args].concat(), code)
}

/// Calls `tool` and returns its text with the CLI's newline appended.
fn mcp(srv: &bellwether_mcp::Server, tool: &str, args: Value) -> (String, Value) {
    let r = srv.call(tool, args);
    let v: Value = serde_json::from_str(&r.text).unwrap();
    (r.text + "\n", v)
}

/// Calls `tool` in process (first), then the CLI with `cli_args`, asserts
/// the CLI exits `code` and prints the tool's text, and returns it parsed.
fn both(
    srv: &bellwether_mcp::Server,
    tool: &str,
    args: Value,
    cli_args: &[&str],
    code: i32,
) -> Value {
    let (m, v) = mcp(srv, tool, args);
    same(&cli(cli_args, code), &m, tool);
    v
}

fn h(v: &Value, ptr: &str) -> String {
    v.pointer(ptr)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("no {ptr} in {v}"))
        .to_string()
}

/// Asserts `cli == mcp`, showing the text around the first differing byte.
fn same(cli: &str, mcp: &str, what: &str) {
    if cli == mcp {
        return;
    }
    let at = cli
        .bytes()
        .zip(mcp.bytes())
        .position(|(a, b)| a != b)
        .unwrap_or(cli.len().min(mcp.len()));
    let from = at.saturating_sub(120);
    let around = |s: &str| {
        String::from_utf8_lossy(&s.as_bytes()[from..(at + 120).min(s.len())]).into_owned()
    };
    panic!(
        "{what}: the CLI line and the MCP text differ at byte {at}\n cli: …{}\n mcp: …{}",
        around(cli),
        around(mcp)
    );
}

/// Copies every file under `from` into `to` (the index directory, saved and
/// restored so the apply can run once from each side).
fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let p = e.unwrap().path();
        let dest = to.join(p.file_name().unwrap());
        if p.is_dir() {
            copy_tree(&p, &dest);
        } else {
            std::fs::copy(&p, &dest).unwrap();
        }
    }
}

#[test]
fn every_tool_with_a_cli_counterpart_returns_the_cli_json_line() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 2, 3, 30, 200);
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[7, 8, 150]);
    let (d, k) = (path(&data), path(&kf));
    let srv = server(&data);

    // observe.why = why.
    let why = both(&srv, "observe.why", json!({}), &["why", d], 0);
    let ev = h(&why, "/evidence/hash");

    // score.why = why --from.
    let scored = both(
        &srv,
        "score.why",
        json!({"keys": k, "wait": true}),
        &["why", d, "--from", k],
        0,
    );
    let workload = h(&scored, "/workload/hash");
    let candidate = h(&scored, "/candidates/0/proposal/hash");

    // propose.key = propose --key; the reserved proposer refusal too.
    let spec = json!({"kind": "exact", "columns": ["id"]});
    both(
        &srv,
        "propose.key",
        json!({"spec": spec, "evidence": [ev], "by": "agent"}),
        &[
            "propose",
            d,
            "--evidence",
            &ev,
            "--by",
            "agent",
            "--key",
            "exact:id",
        ],
        0,
    );
    let refused = both(
        &srv,
        "propose.key",
        json!({"spec": spec, "evidence": [ev], "by": "engine"}),
        &[
            "propose",
            d,
            "--evidence",
            &ev,
            "--by",
            "engine",
            "--key",
            "exact:id",
        ],
        3,
    );
    assert_eq!(refused["outcome"], "refused");
    // propose.layout and propose.rewrite.
    both(
        &srv,
        "propose.layout",
        json!({"cluster_by": ["batch", "id"], "evidence": [ev], "by": "agent"}),
        &[
            "propose",
            d,
            "--evidence",
            &ev,
            "--by",
            "agent",
            "--layout",
            "batch,id",
        ],
        0,
    );
    both(
        &srv,
        "propose.rewrite",
        json!({"shape": {"before": "b", "after": "a", "invariant": "i"}, "evidence": [ev],
               "by": "agent"}),
        &[
            "propose",
            d,
            "--evidence",
            &ev,
            "--by",
            "agent",
            "--before",
            "b",
            "--after",
            "a",
            "--invariant",
            "i",
        ],
        0,
    );

    // score.verify = fix --verify-only.
    let verified = both(
        &srv,
        "score.verify",
        json!({"proposal": candidate, "workload": workload, "wait": true}),
        &[
            "fix",
            d,
            "--proposal",
            &candidate,
            "--workload",
            &workload,
            "--verify-only",
        ],
        0,
    );
    assert_eq!(verified["outcome"]["kind"], "verified");
    let cert = h(&verified, "/certificate/hash");

    // apply.receipt = fix --certificate: the index is saved, the CLI
    // applies, the index is restored, and the server applies the same
    // certificate on the same ledger.
    let index = index_dir(&data);
    let saved = dir.path().join("saved-index");
    copy_tree(&index, &saved);
    let c = cli(&["fix", d, "--certificate", &cert], 0);
    std::fs::remove_dir_all(&index).unwrap();
    copy_tree(&saved, &index);
    let srv = server(&data);
    let (m, applied) = mcp(
        &srv,
        "apply.receipt",
        json!({"certificate": cert, "wait": true}),
    );
    assert_eq!(applied["outcome"]["kind"], "applied");
    same(&c, &m, "apply.receipt");
    // Applying again is refused the same way by both.
    let again = both(
        &srv,
        "apply.receipt",
        json!({"certificate": cert, "wait": true}),
        &["fix", d, "--certificate", &cert],
        3,
    );
    assert_eq!(again["outcome"]["kind"], "refused");

    // observe.locate = locate (after the apply built the index); observe.asof
    // = asof (after a temporal key is built).
    both(
        &srv,
        "observe.locate",
        json!({"keys": k}),
        &["locate", d, "--keys", k],
        0,
    );
    cli(&["index", d, "--key", "temporal:id@ts"], 0);
    let at = "1970-01-01T00:01:00Z";
    both(
        &srv,
        "observe.asof",
        json!({"keys": k, "at": at}),
        &["asof", d, "--keys", k, "--at", at],
        0,
    );

    // observe.profile = ledger findings; the resources = ledger *.
    both(
        &srv,
        "observe.profile",
        json!({}),
        &["ledger", "findings", d],
        0,
    );
    for (uri, sub) in [
        (bellwether_mcp::FINDINGS, "findings"),
        (bellwether_mcp::RECEIPTS, "receipts"),
        (bellwether_mcp::SCORECARD, "scorecard"),
    ] {
        same(
            &cli(&["ledger", sub, d], 0),
            &(srv.resource(uri).unwrap() + "\n"),
            uri,
        );
    }
}

/// `bellwether mcp <root>` is the same server: a handshake and one tool
/// call over its stdio, then a clean exit when stdin closes.
#[test]
fn bellwether_mcp_serves_over_stdio() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 1, 2, 20, 50);
    let mut child = Command::new(env!("CARGO_BIN_EXE_bellwether"))
        .args(["--clock", CLOCK, "mcp", path(&data)])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut send = |v: Value| {
        let mut line = serde_json::to_string(&v).unwrap();
        line.push('\n');
        stdin.write_all(line.as_bytes()).unwrap();
        stdin.flush().unwrap();
    };
    let mut recv = |id: u64| loop {
        let mut line = String::new();
        assert!(stdout.read_line(&mut line).unwrap() > 0, "stdout closed");
        let v: Value = serde_json::from_str(line.trim_end()).unwrap();
        assert_eq!(v["jsonrpc"], "2.0");
        if v["id"] == json!(id) {
            return v;
        }
    };
    send(
        json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
        "protocolVersion": "2025-11-25", "capabilities": {},
        "clientInfo": {"name": "t", "version": "0"}}}),
    );
    let init = recv(1);
    assert_eq!(init["result"]["serverInfo"]["name"], "bellwether-mcp");
    send(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
    send(json!({"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                "params": {"name": "observe.why", "arguments": {}}}));
    let r = recv(2);
    let text = r["result"]["content"][0]["text"].as_str().unwrap();
    assert_eq!(format!("{text}\n"), cli(&["why", path(&data)], 0));
    drop(send);
    drop(stdin);
    assert!(child.wait().unwrap().success());
}
