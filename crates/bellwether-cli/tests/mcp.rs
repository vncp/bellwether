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

use common::{corpus, index_dir, keys, path, run, scattered};
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
        join: None,
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
    scattered(&data, 2, 3, 30, 200);
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
    let canon = std::fs::canonicalize(&data).unwrap();
    let canon = path(&canon);
    // Per-key time bounds and bitemporal versions are key specs (M5): the
    // short form proposes them, the same record from both sides, and a
    // repeated `--key` is the MCP `sidecars` argument.
    for (declaration, kind) in [
        ("temporal2:id@ts,batch", "bitemporal"),
        ("exact:id@ts", "exact"),
    ] {
        let written = both(
            &srv,
            "propose.key",
            json!({"spec": declaration, "evidence": [ev], "by": "agent"}),
            &[
                "propose",
                d,
                "--evidence",
                &ev,
                "--by",
                "agent",
                "--key",
                declaration,
            ],
            0,
        );
        assert_eq!(written["outcome"], "written");
        assert_eq!(written["proposal"]["record"]["body"]["spec"]["kind"], kind);
    }
    let with_sidecar = both(
        &srv,
        "propose.key",
        json!({"spec": "exact:id", "sidecars": ["temporal2:id@ts,batch"],
            "evidence": [ev], "by": "agent"}),
        &[
            "propose",
            d,
            "--evidence",
            &ev,
            "--by",
            "agent",
            "--key",
            "exact:id",
            "--key",
            "temporal2:id@ts,batch",
        ],
        0,
    );
    let body = &with_sidecar["proposal"]["record"]["body"];
    assert_eq!(body["spec"]["kind"], "exact", "{body}");
    assert_eq!(body["sidecars"][0]["kind"], "bitemporal", "{body}");
    // A sketch is approximate and never planned with: the same typed
    // refusal from both sides (exit 3), naming the index command.
    let args = json!({"spec": "sketch:id", "evidence": [ev], "by": "agent"});
    let refused = both(
        &srv,
        "propose.key",
        args.clone(),
        &[
            "propose",
            d,
            "--evidence",
            &ev,
            "--by",
            "agent",
            "--key",
            "sketch:id",
        ],
        3,
    );
    assert_eq!(refused["outcome"], "refused");
    assert_eq!(refused["refusal"]["kind"], "sketch_not_proposable");
    let refusal = srv.call("propose.key", args).envelope.refusal.unwrap();
    assert_eq!(refusal.code.as_str(), "sketch_not_proposable");
    assert_eq!(
        refusal.fields["command"],
        json!(format!("bellwether index {canon} --key sketch:id"))
    );
    assert!(!refusal.next.contains("--"), "{}", refusal.next);
    let short = both(
        &srv,
        "propose.key",
        json!({"spec": "exact:id", "evidence": [ev], "by": "agent"}),
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
    assert_eq!(short["outcome"], "written");
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
    let located = both(
        &srv,
        "observe.locate",
        json!({"keys": k}),
        &["locate", d, "--keys", k],
        0,
    );
    let applied_at = located["sequence"].as_u64().unwrap();
    cli(&["index", d, "--key", "temporal:id@ts"], 0);
    let at = "1970-01-01T00:01:00Z";
    let asof = both(
        &srv,
        "observe.asof",
        json!({"keys": k, "at": at}),
        &["asof", d, "--keys", k, "--at", at],
        0,
    );
    let current = asof["sequence"].as_u64().unwrap();
    assert!(current > applied_at, "{current} {applied_at}");

    // observe.locate with at_manifest = locate --at-manifest: pinned to the
    // sequence before the temporal key, both plan that sequence, and the
    // plan is the one returned then.
    let seq = applied_at.to_string();
    let pinned = both(
        &srv,
        "observe.locate",
        json!({"keys": k, "at_manifest": applied_at}),
        &["locate", d, "--keys", k, "--at-manifest", &seq],
        0,
    );
    assert_eq!(pinned["sequence"], applied_at);
    assert_eq!(pinned["planned"], located["planned"]);
    assert_eq!(pinned["evidence"], located["evidence"]);
    // A sequence never committed: the server refuses with `not_retained`,
    // naming the range to pass instead.
    let unretained = srv.call(
        "observe.locate",
        json!({"keys": k, "at_manifest": current + 5}),
    );
    let refusal = unretained.envelope.refusal.as_ref().unwrap();
    assert_eq!(refusal.code.as_str(), "not_retained", "{refusal:?}");
    assert_eq!(refusal.fields["requested"], current + 5);
    assert_eq!(refusal.fields["current"], current);
    assert!(
        refusal.next.contains(&format!("..={current}")),
        "{refusal:?}"
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

/// One row of the bitemporal lake: (file, row group, id, valid s, known s).
type BitemporalRow = (String, u64, i64, i64, i64);

/// Three files of four row groups of 30 rows: row `k` has id `(k * 7) %
/// 20` (every row group holds every id), was true from `k / 9` seconds and
/// became known `k % 4` seconds later (`valid`, `known`: UTC µs).
fn bitemporal_lake(dir: &Path) -> Vec<BitemporalRow> {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, RecordBatch, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    std::fs::create_dir_all(dir).unwrap();
    let ts = || DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("valid", ts(), true),
        Field::new("known", ts(), true),
    ]));
    let mut rows = Vec::new();
    let mut k = 0i64;
    for f in 0..3 {
        let name = format!("part-{f}.parquet");
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(usize::MAX))
            .build();
        let file = std::fs::File::create(dir.join(&name)).unwrap();
        let mut w = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for rg in 0..4u64 {
            let ks: Vec<i64> = (k..k + 30).collect();
            k += 30;
            let id: Vec<i64> = ks.iter().map(|k| (k * 7) % 20).collect();
            let valid: Vec<i64> = ks.iter().map(|k| k / 9).collect();
            let known: Vec<i64> = ks.iter().zip(&valid).map(|(k, v)| v + k % 4).collect();
            let us = |s: &[i64]| -> ArrayRef {
                Arc::new(
                    TimestampMicrosecondArray::from_iter_values(s.iter().map(|s| s * 1_000_000))
                        .with_timezone("UTC"),
                )
            };
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(id.clone())) as ArrayRef,
                    us(&valid),
                    us(&known),
                ],
            )
            .unwrap();
            w.write(&batch).unwrap();
            w.flush().unwrap();
            for i in 0..ks.len() {
                rows.push((name.clone(), rg, id[i], valid[i], known[i]));
            }
        }
        w.close().unwrap();
    }
    rows
}

/// The (file, row group) pairs of a plan line's Evidence, relative to `root`.
fn plan_row_groups(v: &Value, root: &Path) -> std::collections::BTreeSet<(String, u64)> {
    let base = format!("{}/", path(&std::fs::canonicalize(root).unwrap()));
    v["evidence"]["record"]["body"]["plan"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|e| {
            let rel = e["file"]["path"]
                .as_str()
                .unwrap()
                .strip_prefix(&base)
                .unwrap()
                .to_string();
            e["row_groups"]
                .as_array()
                .unwrap()
                .iter()
                .map(move |r| (rel.clone(), r.as_u64().unwrap()))
        })
        .collect()
}

/// A bitemporal key end to end, from both sides, byte for byte (M5, ADR
/// 028): `why --valid-by --known-by` (`score.why`) prices the current
/// configuration and recommends `temporal2:id@valid,known`; it is proposed
/// (`propose.key`), scored and verified (`score.verify`), applied with a
/// receipt (`apply.receipt`), the receipt is in the ledger (the receipts
/// resource), and `asof --valid --known` (`observe.asof`) then plans exactly
/// the row groups holding each key's winning version, which is what the
/// receipt's replay predicted.
#[test]
fn a_bitemporal_key_goes_from_why_to_receipt_to_asof_on_both_sides() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let rows = bitemporal_lake(&data);
    let kf = dir.path().join("k.parquet");
    // Key 1 has a late print: its version valid at 29 s became known at
    // 32 s, after the known bound, so its winner is the one valid at 27 s.
    keys(&kf, "id", &[1, 3, 11]);
    let (d, k) = (path(&data), path(&kf));
    // A bitemporal run is priced over an index.
    cli(&["index", d, "--key", "exact:id"], 0);
    let srv = server(&data);
    let (valid, known) = ("1970-01-01T00:00:30Z", "1970-01-01T00:00:31Z");

    let why = both(
        &srv,
        "score.why",
        json!({"keys": k, "valid_by": "valid", "known_by": "known", "valid": valid,
               "known": known, "wait": true}),
        &[
            "why",
            d,
            "--from",
            k,
            "--valid-by",
            "valid",
            "--known-by",
            "known",
            "--valid",
            valid,
            "--known",
            known,
        ],
        0,
    );
    // The current configuration is priced (no bitemporal key serves the
    // runs, so they read every row group holding a key: all of them), and
    // the bitemporal key is the recommendation.
    assert_eq!(why["current"]["row_groups"], 12, "{why}");
    assert_eq!(
        why["recommended"]["spec"],
        json!({"kind": "bitemporal", "columns": ["id"], "valid_column": "valid",
               "known_column": "known"}),
        "{why}"
    );
    let ev = h(&why, "/evidence/hash");
    let workload = h(&why, "/workload/hash");
    // The human output words the bitemporal candidate as the bitemporal
    // as-of floor, not a locate's (fix's verify wording is checked below).
    let human = |args: &[&str]| run(&[&["--clock", CLOCK][..], args].concat(), 0);
    let why = human(&[
        "why",
        d,
        "--from",
        k,
        "--valid-by",
        "valid",
        "--known-by",
        "known",
        "--valid",
        valid,
        "--known",
        known,
    ]);
    assert!(
        why.contains("floor (bitemporal as-of)") && !why.contains("floor (locate)"),
        "{why}"
    );

    let proposed = both(
        &srv,
        "propose.key",
        json!({"spec": "temporal2:id@valid,known", "evidence": [ev], "by": "agent"}),
        &[
            "propose",
            d,
            "--evidence",
            &ev,
            "--by",
            "agent",
            "--key",
            "temporal2:id@valid,known",
        ],
        0,
    );
    let proposal = h(&proposed, "/proposal/hash");
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
        0,
    );
    assert_eq!(verified["outcome"]["kind"], "verified", "{verified}");
    // Verify compared each key's winning row, the late print's included:
    // per key, the greatest valid time at or before the bound among rows
    // known at or before it (never a version known later).
    let winners = [1, 3, 11]
        .iter()
        .filter_map(|id| {
            rows.iter()
                .filter(|r| r.2 == *id && r.3 <= 30 && r.4 <= 31)
                .map(|r| (r.3, r.4))
                .max()
        })
        .count();
    assert_eq!(winners, 3);
    assert_eq!(
        verified["verification"]["counts"]["matched"],
        json!(winners),
        "{verified}"
    );
    let cert = h(&verified, "/certificate/hash");

    // The apply, once from each side over the same saved index.
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
    same(&c, &m, "apply.receipt");
    assert_eq!(applied["outcome"]["kind"], "applied", "{applied}");
    let receipt = h(&applied, "/outcome/receipt/hash");
    let receipts = srv.resource(bellwether_mcp::RECEIPTS).unwrap();
    same(
        &cli(&["ledger", "receipts", d], 0),
        &(receipts.clone() + "\n"),
        "receipts",
    );
    assert!(receipts.contains(&receipt), "{receipts}");

    // asof --valid --known plans exactly the winners' row groups: per key,
    // the greatest valid time at or before 30 s among rows known at or
    // before 31 s, then the greatest known time at it.
    let asof = both(
        &srv,
        "observe.asof",
        json!({"keys": k, "valid": valid, "known": known}),
        &["asof", d, "--keys", k, "--valid", valid, "--known", known],
        0,
    );
    let mut want = std::collections::BTreeSet::new();
    for id in [1, 3, 11] {
        let best = rows
            .iter()
            .filter(|r| r.2 == id && r.3 <= 30 && r.4 <= 31)
            .map(|r| (r.3, r.4))
            .max()
            .unwrap();
        want.extend(
            rows.iter()
                .filter(|r| r.2 == id && (r.3, r.4) == best)
                .map(|r| (r.0.clone(), r.1)),
        );
    }
    assert_eq!(plan_row_groups(&asof, &data), want, "{asof}");
    assert!(want.len() < 12, "the bitemporal key prunes: {want:?}");
    assert!(
        asof["specs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["kind"] == "bitemporal"),
        "{asof}"
    );
    // The replay the receipt cites predicted what asof reads.
    assert_eq!(
        verified["score"]["proposed"]["bytes"], asof["planned"]["bytes"],
        "{verified}"
    );
    // fix's verify compares each key's as-of version, not a lookback.
    let human = |args: &[&str]| run(&[&["--clock", CLOCK][..], args].concat(), 0);
    let fix = human(&[
        "fix",
        d,
        "--proposal",
        &proposal,
        "--workload",
        &workload,
        "--verify-only",
    ]);
    assert!(
        fix.contains("each key's as-of version") && !fix.contains("lookback"),
        "{fix}"
    );
}

/// A bitemporal run on a folder with no index is priced over an index of
/// its bitemporal key: `why` refuses naming the command and the exact key
/// to declare, with no placeholder left unfilled.
#[test]
fn a_bitemporal_why_without_an_index_names_the_key_to_declare() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    bitemporal_lake(&data);
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[3]);
    let out = common::bw()
        .args([
            "why",
            path(&data),
            "--from",
            path(&kf),
            "--valid-by",
            "valid",
            "--known-by",
            "known",
        ])
        .output()
        .unwrap();
    let err = String::from_utf8_lossy(&out.stderr);
    assert_ne!(out.status.code(), Some(0), "{err}");
    assert!(
        err.contains("bellwether index") && err.contains("--key temporal2:id@valid,known"),
        "{err}"
    );
    assert!(!err.contains('<') && !err.contains('>'), "{err}");
}

/// `cols` Int64 columns `c0..` over `rows` rows in two row groups (value
/// `(row * (i + 3)) % 97` in column `i`, so every column is id-like and
/// distinct), and a key file holding the first four rows' tuples.
fn wide(dir: &Path, cols: usize, rows: i64) -> (std::path::PathBuf, std::path::PathBuf) {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    let schema = Arc::new(Schema::new(
        (0..cols)
            .map(|i| Field::new(format!("c{i}"), DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    let batch = |range: std::ops::Range<i64>| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            (0..cols)
                .map(|i| {
                    let m = i64::try_from(i).unwrap() + 3;
                    Arc::new(Int64Array::from_iter_values(
                        range.clone().map(|r| (r * m) % 97),
                    )) as ArrayRef
                })
                .collect(),
        )
        .unwrap()
    };
    let data = dir.join("wide");
    std::fs::create_dir_all(&data).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(usize::MAX))
        .build();
    let file = std::fs::File::create(data.join("part-000.parquet")).unwrap();
    let mut w = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
    for half in [0..rows / 2, rows / 2..rows] {
        w.write(&batch(half)).unwrap();
        w.flush().unwrap();
    }
    w.close().unwrap();
    let kf = dir.join("wide-keys.parquet");
    let mut w = ArrowWriter::try_new(
        std::fs::File::create(&kf).unwrap(),
        Arc::clone(&schema),
        None,
    )
    .unwrap();
    w.write(&batch(0..4)).unwrap();
    w.close().unwrap();
    (data, kf)
}

/// ADR 010 with non-default arguments: each one reaches the facade the way
/// its CLI flag does, so dropping one (or changing a default) changes the
/// text and fails here. `observe.why` `columns` = `why --columns`;
/// `score.why` `key_columns` = `why --key`, `max_candidates` =
/// `--max-candidates`, and with neither, more profile candidates than the
/// default cap, the default is the CLI's.
#[test]
fn non_default_arguments_reach_the_facade_as_the_cli_flags_do() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 2, 3, 30, 200);
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[7, 8, 150]);
    let (d, k) = (path(&data), path(&kf));
    let srv = server(&data);

    let all = mcp(&srv, "observe.why", json!({})).0;
    let batch = both(
        &srv,
        "observe.why",
        json!({"columns": ["batch"]}),
        &["why", d, "--columns", "batch"],
        0,
    );
    assert_ne!(
        serde_json::to_string(&batch).unwrap() + "\n",
        all,
        "columns changed nothing"
    );
    let one = both(
        &srv,
        "score.why",
        json!({"keys": k, "max_candidates": 1, "wait": true}),
        &["why", d, "--from", k, "--max-candidates", "1"],
        0,
    );
    let profile_candidates = |v: &Value| {
        v["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|c| c["origin"] == "profile")
            .count()
    };
    assert!(profile_candidates(&one) <= 1);

    // A wide table: ten id-like columns, every one a profile candidate.
    let (wd, wk) = wide(dir.path(), 10, 200);
    let (wd, wk) = (path(&wd), path(&wk));
    let wide_srv = server(Path::new(wd));
    let cols: Vec<String> = (0..10).map(|i| format!("c{i}")).collect();
    let joined = cols.join(",");
    let capped = both(
        &wide_srv,
        "score.why",
        json!({"keys": wk, "key_columns": cols, "columns": cols, "wait": true}),
        &[
            "why",
            wd,
            "--from",
            wk,
            "--key",
            &joined,
            "--columns",
            &joined,
        ],
        0,
    );
    assert_eq!(
        profile_candidates(&capped),
        bellwether::DEFAULT_MAX_CANDIDATES,
        "the default cap binds: {capped}"
    );
    // key_columns: a lookup on c0 alone is another workload.
    let narrow = both(
        &wide_srv,
        "score.why",
        json!({"keys": wk, "key_columns": ["c0"], "wait": true}),
        &["why", wd, "--from", wk, "--key", "c0"],
        0,
    );
    assert_eq!(narrow["workload"]["columns"], json!([["c0"]]), "{narrow}");
}

/// `bellwether mcp` takes every engine and budget flag `bellwether-mcp`
/// takes, from the same definitions (`bellwether_mcp::args`), so the two
/// open an unindexed CSV or JSON lines folder the same way.
#[test]
fn bellwether_mcp_takes_the_shared_engine_and_budget_flags() {
    use clap::Args as _;
    let shared = bellwether_mcp::BudgetArgs::augment_args(
        bellwether_mcp::EngineArgs::augment_args(clap::Command::new("shared")),
    );
    let help = run(&["mcp", "--help"], 0);
    let mut flags = 0;
    for a in shared.get_arguments().filter(|a| !a.is_hide_set()) {
        let long = format!("--{}", a.get_long().unwrap());
        assert!(help.contains(&long), "{long} missing from:\n{help}");
        flags += 1;
    }
    assert_eq!(flags, 13);
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

/// `observe.count` = `count` and `observe.locate` with `columns` = `locate
/// --columns`, byte for byte, and a count with no counting spec is refused
/// by both naming the same `index --key`.
#[test]
fn count_and_a_projected_locate_return_the_cli_json_line() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    // 180 rows, ids 0..20, nine rows each; keys 7 and 8 hold 18, 150 none.
    corpus(&data, 2, 3, 30, 20);
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[7, 8, 150]);
    let (d, k) = (path(&data), path(&kf));
    let srv = server(&data);
    cli(&["index", d, "--key", "exact:id"], 0);

    // No counting spec: the server refuses with not_indexed and the CLI
    // exits 4, both naming `exact:id,counts` (the CLI as the `index --key`
    // to run).
    let r = srv.call("observe.count", json!({"keys": k}));
    let refusal = r.envelope.refusal.as_ref().unwrap();
    assert_eq!(refusal.code.as_str(), "not_indexed", "{refusal:?}");
    assert_eq!(refusal.fields["needs"], "exact:id,counts", "{refusal:?}");
    assert!(refusal.next.contains("exact:id,counts"), "{refusal:?}");
    let out = common::bw()
        .args(["--clock", CLOCK, "count", d, "--keys", k])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(4));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("--key exact:id,counts"), "{stderr}");

    cli(&["index", d, "--key", "exact:id,counts"], 0);
    let counted = both(
        &srv,
        "observe.count",
        json!({"keys": k}),
        &["count", d, "--keys", k],
        0,
    );
    assert_eq!(counted["rows"], 18, "{counted}");
    assert_eq!(counted["keys"], 3);
    assert_eq!(counted["counted_by"], "exact:id,counts");
    assert_eq!(counted["row_groups_total"], 6);

    // The plan carries the same exact rows, and a projection's bytes.
    let located = both(
        &srv,
        "observe.locate",
        json!({"keys": k, "columns": ["id", "ts"]}),
        &["locate", d, "--keys", k, "--columns", "id,ts"],
        0,
    );
    assert_eq!(located["rows"], 18, "{located}");
    let projected = located["projected_bytes"].as_u64().unwrap();
    let planned = located["planned"]["bytes"].as_u64().unwrap();
    assert!(
        projected > 0 && projected < planned,
        "{projected} {planned}"
    );
    // Without a projection: no projected bytes, no per-column bytes.
    let plain = both(
        &srv,
        "observe.locate",
        json!({"keys": k}),
        &["locate", d, "--keys", k],
        0,
    );
    assert!(plain.get("projected_bytes").is_none(), "{plain}");
    assert_eq!(plain["planned"], located["planned"]);
}

/// One file of four row groups: key 1's rows all have a null `ts` (row
/// groups 0 and 1), key 2 has a time in every row group, key 3 in the last
/// two.
fn null_version_lake(dir: &Path) {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, RecordBatch, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    std::fs::create_dir_all(dir).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]));
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(usize::MAX))
        .build();
    let file = std::fs::File::create(dir.join("part-0.parquet")).unwrap();
    let mut w = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
    let rgs: [&[(i64, Option<i64>)]; 4] = [
        &[(1, None), (2, Some(1))],
        &[(1, None), (2, Some(2))],
        &[(2, Some(3)), (3, Some(3))],
        &[(2, Some(4)), (3, Some(4))],
    ];
    for rows in rgs {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef,
                Arc::new(
                    TimestampMicrosecondArray::from(
                        rows.iter()
                            .map(|r| r.1.map(|s| s * 1_000_000))
                            .collect::<Vec<_>>(),
                    )
                    .with_timezone("UTC"),
                ),
            ],
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.flush().unwrap();
    }
    w.close().unwrap();
}

/// PR #30 review, item 5: a workload key whose every row has a null
/// version time cannot be compared (ADR 014), so the certificate fails
/// with it `uncomparable`, not `mismatched`. The fix refusal carries the
/// count apart and says why and what to do, in the CLI's human output and
/// in the MCP refusal, which is the CLI's `--json` line.
#[test]
fn a_key_with_no_version_is_refused_as_uncomparable_with_its_cause_on_both_sides() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    null_version_lake(&data);
    let k = dir.path().join("k.parquet");
    keys(&k, "id", &[1, 2]);
    let (d, k) = (path(&data), path(&k));
    cli(&["index", d, "--key", "exact:id"], 0);
    let why = cli(&["why", d, "--from", k, "--latest-by", "ts"], 0);
    let why: Value = serde_json::from_str(why.lines().last().unwrap()).unwrap();
    let (workload, ev) = (h(&why, "/workload/hash"), h(&why, "/evidence/hash"));
    let proposed = cli(
        &[
            "propose",
            d,
            "--evidence",
            &ev,
            "--by",
            "agent",
            "--key",
            "temporal:id@ts",
        ],
        0,
    );
    let proposal = h(
        &serde_json::from_str(proposed.lines().last().unwrap()).unwrap(),
        "/proposal/hash",
    );
    let srv = server(&data);
    let args = [
        "fix",
        d,
        "--proposal",
        &proposal,
        "--workload",
        &workload,
        "--verify-only",
    ];
    let reply = srv.call(
        "score.verify",
        json!({"proposal": proposal, "workload": workload, "wait": true}),
    );
    same(&cli(&args, 1), &(reply.text.clone() + "\n"), "score.verify");
    let refused: Value = serde_json::from_str(&reply.text).unwrap();
    assert_eq!(refused["outcome"]["refusal"]["kind"], "certificate_failed");
    assert_eq!(
        refused["outcome"]["refusal"]["uncomparable"], 1,
        "{refused}"
    );
    assert_eq!(refused["outcome"]["refusal"]["mismatched"], 0, "{refused}");
    let cause = "1 key could not be compared (uncomparable): every row of such a key on one side \
                 has a null version time";
    let r = reply.envelope.refusal.as_ref().unwrap();
    assert!(
        r.message.contains("0 mismatched, 1 uncomparable"),
        "{}",
        r.message
    );
    assert!(r.message.contains(cause), "{}", r.message);
    assert_eq!(r.fields["uncomparable"], 1);
    // The human output names the count and the cause.
    let human = run(&[&["--clock", CLOCK][..], &args].concat(), 1);
    assert!(human.contains("0 mismatched, 1 uncomparable"), "{human}");
    assert!(human.contains(cause), "{human}");
}
