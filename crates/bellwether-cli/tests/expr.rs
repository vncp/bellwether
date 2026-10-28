//! `expr` keys through the CLI and the MCP server, on the `bench expr`
//! market corpus: `why --from` proposes an `expr` key with its `expr`
//! column, `fix` certifies and applies it (a receipt), and `locate` plans
//! exactly the row groups `why` predicted, holding every ground-truth row
//! group. Every step's MCP text equals the CLI's `--json` line byte for
//! byte (ADR 010), under one fixed clock. A near miss is refused by both,
//! naming the declared key.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

mod common;

use std::collections::BTreeSet;
use std::path::Path;

use bellwether_bench::expr::{generate_expr, ExprParams, ExprShape, ExprTruth};
use common::{bw, index_dir, path, run};
use serde_json::{json, Value};

const CLOCK: &str = "2026-10-24T12:00:00.000Z";
const SYM: &str = "json_get(payload,'$.symbol')";

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

/// The MCP tool's text (with the CLI's newline) must equal the CLI line.
fn both(srv: &bellwether_mcp::Server, tool: &str, args: Value, cli_args: &[&str]) -> Value {
    let r = srv.call(tool, args);
    let m = r.text + "\n";
    let c = cli(cli_args, 0);
    assert!(c == m, "{tool}: CLI and MCP differ\n cli: {c}\n mcp: {m}");
    serde_json::from_str(&c).unwrap()
}

fn h(v: &Value, ptr: &str) -> String {
    v.pointer(ptr)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("no {ptr} in {v}"))
        .to_string()
}

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

fn planned(v: &Value, data: &Path) -> BTreeSet<(String, u64)> {
    let mut out = BTreeSet::new();
    for e in v["evidence"]["record"]["body"]["plan"]["entries"]
        .as_array()
        .unwrap()
    {
        let p = Path::new(e["file"]["path"].as_str().unwrap());
        let rel = p.strip_prefix(data).unwrap().to_str().unwrap().to_string();
        for rg in e["row_groups"].as_array().unwrap() {
            out.insert((rel.clone(), rg.as_u64().unwrap()));
        }
    }
    out
}

#[test]
fn why_fix_receipt_locate_round_trip_with_cli_mcp_byte_identity() {
    let dir = tempfile::tempdir().unwrap();
    let out = std::fs::canonicalize(dir.path()).unwrap().join("expr");
    let params = ExprParams {
        files: 2,
        row_groups_per_file: 10,
        rows_per_row_group: 40,
        keys_per_query: 2,
        ..ExprParams::small(ExprShape::Market)
    };
    generate_expr(&out, &params).unwrap();
    let data = out.join("data");
    let truth: ExprTruth =
        serde_json::from_slice(&std::fs::read(out.join("workload/truth.json")).unwrap()).unwrap();
    let rare = truth.queries.iter().find(|q| q.id == "rare").unwrap();
    let kf = out.join(&rare.keys_file);
    let (d, k) = (path(&data), path(&kf));
    let srv = server(&data);

    // score.why = why --from: one expr candidate, with its `expr` column.
    let why = both(
        &srv,
        "score.why",
        json!({"keys": k, "wait": true}),
        &["why", d, "--from", k],
    );
    let cands = why["candidates"].as_array().unwrap();
    let (i, cand) = cands
        .iter()
        .enumerate()
        .find(|(_, c)| c["spec"]["kind"] == "expr")
        .unwrap_or_else(|| panic!("no expr candidate: {why}"));
    assert_eq!(cand["spec"]["expr"], SYM);
    assert_eq!(cand["spec"]["name"], "json_get_payload_symbol");
    assert_eq!(cand["spec"]["over"]["kind"], "exact");
    let col = &cand["kind_column"];
    assert_eq!(col["kind"], "expr");
    assert_eq!(col["exactness"]["kind"], "exact");
    let value = col["value"].as_str().unwrap();
    assert!(
        value.contains(" null / ") && value.ends_with(" distinct"),
        "{value}"
    );
    assert_eq!(why["recommended"]["spec"], cand["spec"], "{why}");
    let predicted = cand["proposed"].clone();
    assert!(
        predicted["row_groups"].as_u64().unwrap() < why["current"]["row_groups"].as_u64().unwrap()
    );
    let workload = h(&why, "/workload/hash");
    let proposal = h(&why, &format!("/candidates/{i}/proposal/hash"));
    // The human funnel carries the expr column, headed by the kind.
    let human = run(&["--clock", CLOCK, "why", d, "--from", k], 0);
    assert!(
        human.lines().any(|l| l.trim_end().ends_with(" expr"))
            && human.contains(value)
            && human.contains("expr: rows whose expression is null"),
        "{human}"
    );

    // The wide values are in every row group: the layout culprit speaks of
    // the derived value, never of an unrelated profiled column.
    let wide = truth.queries.iter().find(|q| q.id == "wide").unwrap();
    let wk = out.join(&wide.keys_file);
    let human = run(&["--clock", CLOCK, "why", d, "--from", path(&wk)], 0);
    assert!(
        human.contains(&format!(
            "the rows whose {SYM} equals a requested value are spread over nearly every row group"
        )) && !human.contains("not clustered by"),
        "{human}"
    );

    // score.verify = fix --verify-only.
    let verified = both(
        &srv,
        "score.verify",
        json!({"proposal": proposal, "workload": workload, "wait": true}),
        &[
            "fix",
            d,
            "--proposal",
            &proposal,
            "--workload",
            &workload,
            "--verify-only",
        ],
    );
    assert_eq!(verified["outcome"]["kind"], "verified", "{verified}");
    let cert = h(&verified, "/certificate/hash");

    // apply.receipt = fix --certificate, once from each side on the same
    // ledger state.
    let index = index_dir(&data);
    let saved = dir.path().join("saved-index");
    copy_tree(&index, &saved);
    let c = cli(&["fix", d, "--certificate", &cert], 0);
    std::fs::remove_dir_all(&index).unwrap();
    copy_tree(&saved, &index);
    let srv = server(&data);
    let r = srv.call("apply.receipt", json!({"certificate": cert, "wait": true}));
    assert_eq!(c, r.text.clone() + "\n", "apply.receipt");
    let applied: Value = serde_json::from_str(&r.text).unwrap();
    assert_eq!(applied["outcome"]["kind"], "applied", "{applied}");

    // observe.locate = locate --keys: the plan `why` predicted, holding
    // every ground-truth row group, with the residual recomputing the
    // expression.
    let located = both(
        &srv,
        "observe.locate",
        json!({"keys": k}),
        &["locate", d, "--keys", k],
    );
    assert_eq!(located["planned"]["row_groups"], predicted["row_groups"]);
    assert_eq!(located["planned"]["bytes"], predicted["bytes"]);
    let got = planned(&located, &data);
    let want: BTreeSet<(String, u64)> = rare.row_groups.iter().cloned().collect();
    assert_eq!(got, want, "an exact expr key plans exactly the truth");
    assert_eq!(
        located["evidence"]["record"]["body"]["plan"]["residual"]["op"],
        "expr_in"
    );

    // A near miss: refused by both (the CLI exits 4), naming the declared
    // key and the command that would declare the asked expression.
    let near = dir.path().join("near.parquet");
    {
        use arrow::array::{ArrayRef, StringArray};
        let batch = arrow::array::RecordBatch::try_from_iter([(
            "json_get(payload,'$.Symbol')",
            std::sync::Arc::new(StringArray::from(vec!["S-R0003-0"])) as ArrayRef,
        )])
        .unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(
            std::fs::File::create(&near).unwrap(),
            batch.schema(),
            None,
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
    let o = bw()
        .args(["--json", "locate", d, "--keys", path(&near)])
        .output()
        .unwrap();
    assert_eq!(o.status.code(), Some(4));
    let err = String::from_utf8_lossy(&o.stderr);
    assert!(
        err.contains("no expr key indexes json_get(payload,'$.Symbol')")
            && err.contains("expr:json_get_payload_symbol=json_get(payload,'$.symbol'):exact")
            && err.contains(&format!("bellwether index {d} --key 'expr:json_get_payload_symbol=json_get(payload,'\\''$.Symbol'\\''):exact'")),
        "{err}"
    );
    let r = srv.call("observe.locate", json!({"keys": path(&near)}));
    let refusal = r.envelope.refusal.expect("refused");
    assert_eq!(refusal.code.as_str(), "invalid_spec");
    assert!(
        refusal
            .message
            .contains("no expr key indexes json_get(payload,'$.Symbol')"),
        "{}",
        refusal.message
    );

    // latest and asof on an expression key file: the locate-only refusal,
    // from the CLI (exit 4) and the server (invalid_spec), never a temporal
    // key that cannot exist.
    for args in [
        vec!["latest", d, "--keys", k],
        vec!["asof", d, "--keys", k, "--at", "2026-01-02T00:00:00Z"],
    ] {
        let o = bw().args(&args).output().unwrap();
        assert_eq!(o.status.code(), Some(4), "{args:?}");
        let err = String::from_utf8_lossy(&o.stderr);
        assert!(
            err.contains("is an expression; an expr key answers point lookups only")
                && err.contains(&format!("bellwether locate {d} --keys"))
                && !err.contains("temporal:"),
            "{args:?}: {err}"
        );
    }
    let r = srv.call(
        "observe.asof",
        json!({"keys": k, "at": "2026-01-02T00:00:00Z"}),
    );
    let refusal = r.envelope.refusal.expect("refused");
    assert_eq!(refusal.code.as_str(), "invalid_spec");
    assert!(
        refusal
            .message
            .contains("an expr key answers point lookups only"),
        "{}",
        refusal.message
    );

    // count on the expression: refused (exit 3 or 4) while no expr key
    // stores counts, naming the key that would; after `index --key
    // expr:...:exact,counts`, the rows holding the values, from the index,
    // byte-identical from the CLI and the server, equal to the truth.
    let o = bw().args(["count", d, "--keys", k]).output().unwrap();
    let err = String::from_utf8_lossy(&o.stderr);
    assert_ne!(o.status.code(), Some(0));
    assert!(
        err.contains("expr:json_get_payload_symbol=json_get(payload,'$.symbol'):exact,counts"),
        "{err}"
    );
    cli(
        &[
            "index",
            d,
            "--key",
            "expr:sym_counts=json_get(payload,'$.symbol'):exact,counts",
        ],
        0,
    );
    let counted = both(
        &srv,
        "observe.count",
        json!({"keys": k}),
        &["count", d, "--keys", k],
    );
    assert_eq!(counted["rows"], rare.rows, "{counted}");
    assert_eq!(
        counted["counted_by"],
        "expr:sym_counts=json_get(payload,'$.symbol'):exact,counts"
    );

    // `index --key` reads the spec spelling, and its JSON names the key.
    let spelled = cli(
        &[
            "index",
            d,
            "--key",
            "expr:sym_bloom=JSON_GET(payload, '$.symbol'):bloom@0.01",
        ],
        0,
    );
    let spelled: Value = serde_json::from_str(&spelled).unwrap();
    let bloom = spelled["specs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["spec"]["name"] == "sym_bloom")
        .unwrap_or_else(|| panic!("{spelled}"));
    assert_eq!(bloom["spec"]["expr"], SYM, "stored normalized");
    assert_eq!(bloom["spec"]["over"]["kind"], "bloom");
    assert_eq!(bloom["spec"]["over"]["fpr"], 0.01);
}
