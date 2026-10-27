//! Tombstones (B8) end to end through the binary and the MCP server, byte
//! for byte (ADR 010):
//!
//! - `why --latest-by ts --tombstone void` prices `latest` runs that keep
//!   only live rows, recommends `temporal:id@ts,tombstone=void`, reports
//!   the `tombstone` column (keys whose newest row is a tombstone) and the
//!   `R-tombstone-1` lint; the key is proposed (`propose.key`), verified
//!   (`score.verify`) and applied with a receipt (`apply.receipt`), and the
//!   lint no longer fires;
//! - `latest --live` and `asof --live` (`observe.asof` with `live`) plan
//!   exactly the row groups holding each key's live newest rows, which is
//!   what the receipt's replay predicted; `asof` without `--live` prints no
//!   `live` field;
//! - a lookup with `--live` through a key that declares no tombstone is
//!   refused naming the spec to declare, from both sides;
//! - a tombstone sidecar left incomplete (as a binary without tombstones
//!   leaves it after appending a file) plans without pruning, says so in
//!   both outputs and names `bellwether index`, and `bellwether index`
//!   completes it.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

mod common;

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use common::{index_dir, keys, path, run};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde_json::{json, Value};

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

fn cli(args: &[&str], code: i32) -> String {
    run(&[&["--clock", CLOCK, "--json"], args].concat(), code)
}

fn human(args: &[&str], code: i32) -> String {
    run(&[&["--clock", CLOCK], args].concat(), code)
}

fn mcp(srv: &bellwether_mcp::Server, tool: &str, args: Value) -> (String, Value) {
    let r = srv.call(tool, args);
    let v: Value = serde_json::from_str(&r.text).unwrap();
    (r.text + "\n", v)
}

fn both(
    srv: &bellwether_mcp::Server,
    tool: &str,
    args: Value,
    cli_args: &[&str],
    code: i32,
) -> Value {
    let (m, v) = mcp(srv, tool, args);
    let c = cli(cli_args, code);
    assert_eq!(c, m, "{tool}: the CLI line and the MCP text differ");
    v
}

fn h(v: &Value, ptr: &str) -> String {
    v.pointer(ptr)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("no {ptr} in {v}"))
        .to_string()
}

/// One row: (file, row group, id, ts, void).
type LakeRow = (String, u64, i64, i64, Option<bool>);

/// `files` files of four row groups of 30 rows, numbered on from `first`:
/// row `k` has id `(k * 7) % 20` (every row group holds every id), ts `k`,
/// and `void` true when `k % 5 == 0` (a retraction), null when `k % 37 ==
/// 0`, else false.
fn lake(dir: &Path, files: std::ops::Range<u64>) -> Vec<LakeRow> {
    std::fs::create_dir_all(dir).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("ts", DataType::Int64, true),
        Field::new("void", DataType::Boolean, true),
    ]));
    let mut rows = Vec::new();
    for f in files {
        let name = format!("part-{f}.parquet");
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(usize::MAX))
            .build();
        let file = std::fs::File::create(dir.join(&name)).unwrap();
        let mut w = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for rg in 0..4u64 {
            let first = (f * 4 + rg) * 30;
            let ks: Vec<i64> = (first..first + 30)
                .map(|k| i64::try_from(k).unwrap())
                .collect();
            let id: Vec<i64> = ks.iter().map(|k| (k * 7) % 20).collect();
            let void: Vec<Option<bool>> = ks
                .iter()
                .map(|k| (k % 37 != 0).then_some(k % 5 == 0))
                .collect();
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(id.clone())) as ArrayRef,
                    Arc::new(Int64Array::from(ks.clone())),
                    Arc::new(BooleanArray::from(void.clone())),
                ],
            )
            .unwrap();
            w.write(&batch).unwrap();
            w.flush().unwrap();
            for i in 0..ks.len() {
                rows.push((name.clone(), rg, id[i], ks[i], void[i]));
            }
        }
        w.close().unwrap();
    }
    rows
}

/// Per key, the rows at its newest ts at or before `at`, then the live
/// ones: their (file, row group) pairs.
fn truth(rows: &[LakeRow], ids: &[i64], at: Option<i64>) -> BTreeSet<(String, u64)> {
    let mut out = BTreeSet::new();
    for id in ids {
        let Some(newest) = rows
            .iter()
            .filter(|r| r.2 == *id && at.is_none_or(|a| r.3 <= a))
            .map(|r| r.3)
            .max()
        else {
            continue;
        };
        out.extend(
            rows.iter()
                .filter(|r| r.2 == *id && r.3 == newest && r.4 == Some(false))
                .map(|r| (r.0.clone(), r.1)),
        );
    }
    out
}

fn plan_row_groups(v: &Value, root: &Path) -> BTreeSet<(String, u64)> {
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

/// Keys whose newest row is a tombstone (`retracted`), and others.
fn key_ids(rows: &[LakeRow]) -> (Vec<i64>, Vec<i64>) {
    let (mut retracted, mut live) = (Vec::new(), Vec::new());
    for id in 0..20 {
        if truth(rows, &[id], None).is_empty() {
            retracted.push(id);
        } else {
            live.push(id);
        }
    }
    (retracted, live)
}

#[test]
fn a_tombstone_key_goes_from_why_to_receipt_to_live_lookups_on_both_sides() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let rows = lake(&data, 0..3);
    let (retracted, live) = key_ids(&rows);
    assert!(retracted.len() >= 2 && live.len() >= 2, "{retracted:?}");
    let ids: Vec<i64> = retracted.iter().chain(&live).take(6).copied().collect();
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &ids);
    let (d, k) = (path(&data), path(&kf));
    cli(&["index", d, "--key", "exact:id"], 0);
    let srv = server(&data);

    let why = cli(
        &[
            "why",
            d,
            "--from",
            k,
            "--latest-by",
            "ts",
            "--tombstone",
            "void",
        ],
        0,
    );
    let why: Value = serde_json::from_str(&why).unwrap();
    assert_eq!(
        why["recommended"]["spec"],
        json!({"kind": "temporal", "columns": ["id"], "ts_column": "ts",
               "tombstone": {"column": "void"}}),
        "{why}"
    );
    let candidate = why["candidates"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["spec"]["tombstone"].is_object())
        .unwrap();
    let column = &candidate["kind_column"];
    assert_eq!(column["kind"], "tombstone", "{candidate}");
    assert_eq!(column["exactness"], json!({"kind": "exact"}), "{candidate}");
    let n_retracted = key_ids(&rows).0.len();
    assert!(
        column["value"]
            .as_str()
            .unwrap()
            .starts_with(&format!("{n_retracted} of 20 (")),
        "{column}"
    );
    // the rows behind it, against the lake (PR #33 review item 8)
    let value = column["value"].as_str().unwrap();
    assert!(
        value.contains(&format!(
            "; of {} timed rows, {} are tombstones and {} have a null void",
            rows.len(),
            rows.iter().filter(|r| r.4 == Some(true)).count(),
            rows.iter().filter(|r| r.4.is_none()).count()
        )),
        "{value}"
    );
    assert!(!value.contains("warning"), "{value}");
    let lints = why["profile"]["lints"].as_array().unwrap();
    let lint = lints
        .iter()
        .find(|l| l["rule"] == "R-tombstone-1")
        .unwrap_or_else(|| panic!("R-tombstone-1 did not fire: {lints:?}"));
    assert!(
        lint["message"]
            .as_str()
            .unwrap()
            .contains("--latest-by ts --tombstone void"),
        "{lint}"
    );
    let ev = h(&why, "/evidence/hash");
    let workload = h(&why, "/workload/hash");
    // The human report heads the column with the kind.
    let report = human(
        &[
            "why",
            d,
            "--from",
            k,
            "--latest-by",
            "ts",
            "--tombstone",
            "void",
        ],
        0,
    );
    assert!(report.contains("tombstone"), "{report}");
    assert!(report.contains("R-tombstone-1"), "{report}");

    let proposed = both(
        &srv,
        "propose.key",
        json!({"spec": "temporal:id@ts,tombstone=void", "evidence": [ev], "by": "agent"}),
        &[
            "propose",
            d,
            "--evidence",
            &ev,
            "--by",
            "agent",
            "--key",
            "temporal:id@ts,tombstone=void",
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
    // The certificate compared each key's newest rows kept by the live
    // filter: the run's rows, not every newest row.
    let filter = &verified["certificate"]["record"]["body"]["row_filter"];
    let live_test = filter
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|f| f["all"].as_array().cloned().unwrap_or_default())
        .find(|p| p["op"] == "live")
        .unwrap_or_else(|| panic!("no live test in the certificate's row filter: {filter}"));
    assert_eq!(live_test["column"], "void", "{filter}");
    // The human line of a live `latest` run: the newest version, then the
    // live rows, and no instant or lookback (it has neither; PR #33 review
    // item 4).
    let text = human(
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
    assert!(
        text.contains(
            "each key's newest version by ts, found in the data, then its live rows only (void \
             not null and not the tombstone value)"
        ),
        "{text}"
    );
    assert!(
        !text.contains("lookback") && !text.contains("instant"),
        "{text}"
    );
    let cert = h(&verified, "/certificate/hash");
    let (m, applied) = mcp(
        &srv,
        "apply.receipt",
        json!({"certificate": cert, "wait": true}),
    );
    assert_eq!(applied["outcome"]["kind"], "applied", "{applied}: {m}");
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(index_dir(&data).join("manifest.json")).unwrap())
            .unwrap();
    assert!(
        manifest["schema"]["key_specs"]
            .as_array()
            .unwrap()
            .iter()
            .all(|s| s.get("tombstone").is_none()),
        "{manifest}"
    );
    assert!(
        manifest["schema"]["sidecars"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["kind"] == "tombstone"),
        "{manifest}"
    );

    // latest --live plans exactly the live newest rows' row groups, the
    // receipt's prediction.
    let latest = cli(&["latest", d, "--keys", k, "--live"], 0);
    let latest: Value = serde_json::from_str(&latest).unwrap();
    assert_eq!(
        plan_row_groups(&latest, &data),
        truth(&rows, &ids, None),
        "{latest}"
    );
    assert_eq!(latest["live"]["pruned_by"], "tombstone:id@ts", "{latest}");
    assert_eq!(latest["live"]["filter"], "not void");
    assert_eq!(
        latest["live"]["keys_without_row_groups"],
        json!(ids.iter().filter(|i| retracted.contains(i)).count()),
        "{latest}"
    );
    assert_eq!(
        verified["score"]["proposed"]["bytes"], latest["planned"]["bytes"],
        "{verified}"
    );
    let plain = cli(&["latest", d, "--keys", k], 0);
    let plain: Value = serde_json::from_str(&plain).unwrap();
    assert!(plain.get("live").is_none(), "{plain}");
    assert!(
        latest["planned"]["row_groups"].as_u64() <= plain["planned"]["row_groups"].as_u64(),
        "{latest}\n{plain}"
    );
    let text = human(&["latest", d, "--keys", k, "--live"], 0);
    assert!(
        text.contains("live: rows where not void among each key's newest version"),
        "{text}"
    );
    assert!(text.contains("pruned by tombstone:id@ts"), "{text}");
    // the line states what was measured (PR #33 review item 5)
    assert!(
        text.contains(&format!(
            "tombstones: {} of {} with a version answered with no row group read (no live row \
             at their newest time); {} (key, row group) pair",
            latest["live"]["keys_without_row_groups"],
            if latest["live"]["keys_with_version"] == 1 {
                "1 key".to_string()
            } else {
                format!("{} keys", latest["live"]["keys_with_version"])
            },
            latest["live"]["key_row_group_pairs_pruned"]
        )),
        "{text}"
    );
    assert!(
        !text.contains("have a tombstone as their newest row"),
        "{text}"
    );

    // asof --live = observe.asof with live, at several instants
    for at in ["0", "100", "200", "359", "1000"] {
        let asof = both(
            &srv,
            "observe.asof",
            json!({"keys": k, "at": at, "live": true}),
            &["asof", d, "--keys", k, "--at", at, "--live"],
            0,
        );
        let planned = plan_row_groups(&asof, &data);
        let want = truth(&rows, &ids, Some(at.parse().unwrap()));
        let plain = both(
            &srv,
            "observe.asof",
            json!({"keys": k, "at": at}),
            &["asof", d, "--keys", k, "--at", at],
            0,
        );
        assert!(plain.get("live").is_none(), "{plain}");
        // never short (invariant 1), never more than asof reads; at or
        // after every key's newest time, exact (a key's winning row group
        // holds no later row of it)
        assert!(want.is_subset(&planned), "{at}: {asof}");
        assert!(planned.is_subset(&plan_row_groups(&plain, &data)), "{at}");
        if at == "359" || at == "1000" {
            assert_eq!(planned, want, "{at}: {asof}");
        }
    }

    // The lint no longer fires once the key is complete.
    let why = cli(
        &[
            "why",
            d,
            "--from",
            k,
            "--latest-by",
            "ts",
            "--tombstone",
            "void",
        ],
        0,
    );
    assert!(!why.contains("R-tombstone-1"), "{why}");
}

#[test]
fn a_live_lookup_through_a_key_without_a_tombstone_names_the_spec_to_declare() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    lake(&data, 0..1);
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[1, 2]);
    let (d, k) = (path(&data), path(&kf));
    cli(&["index", d, "--key", "temporal:id@ts"], 0);
    let srv = server(&data);
    let out = common::bw()
        .args(["latest", d, "--keys", k, "--live"])
        .output()
        .unwrap();
    let err = String::from_utf8_lossy(&out.stderr);
    assert_ne!(out.status.code(), Some(0), "{err}");
    assert!(
        err.contains("temporal:id@ts,tombstone=<column>") && err.contains("bellwether index"),
        "{err}"
    );
    // The same refusal from the MCP tool: its message is the CLI's error.
    let reply = srv.call(
        "observe.asof",
        json!({"keys": k, "at": "100", "live": true}),
    );
    let r = reply.envelope.refusal.as_ref().unwrap();
    assert_eq!(
        r.fields["needs"], "temporal:id@ts,tombstone=<column>",
        "{}",
        reply.text
    );
    let out = common::bw()
        .args(["--json", "asof", d, "--keys", k, "--at", "100", "--live"])
        .output()
        .unwrap();
    let err = String::from_utf8_lossy(&out.stderr);
    assert_ne!(out.status.code(), Some(0), "{err}");
    assert!(err.contains(&r.message), "{err}\n{}", r.message);
    // a tombstone that cannot mark a row is refused when declared
    let out = common::bw()
        .args(["index", d, "--key", "temporal:id@ts,tombstone=ts"])
        .output()
        .unwrap();
    let err = String::from_utf8_lossy(&out.stderr);
    assert_ne!(out.status.code(), Some(0), "{err}");
    assert!(err.contains("is a time column of the key"), "{err}");
}

/// Condition (c): the tombstone segments over an appended file are missing,
/// as after a build by a binary without tombstones. The live lookup plans
/// as without tombstones, with the residual exact, and both outputs say why
/// and name the command; `bellwether index` completes the sidecar.
#[test]
fn an_incomplete_tombstone_sidecar_is_stated_and_fixed_by_index() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let mut rows = lake(&data, 0..2);
    let kf = dir.path().join("k.parquet");
    let (retracted, live) = key_ids(&rows);
    let ids: Vec<i64> = retracted.iter().chain(&live).take(6).copied().collect();
    keys(&kf, "id", &ids);
    let (d, k) = (path(&data), path(&kf));
    cli(&["index", d, "--key", "temporal:id@ts,tombstone=void"], 0);
    rows.extend(lake(&data, 2..3));
    cli(&["index", d], 0);
    // Drop the tombstone segments over part-2's row groups, which the
    // commit just listed, from the manifest.
    let mpath = index_dir(&data).join("manifest.json");
    let mut m: Value = serde_json::from_slice(&std::fs::read(&mpath).unwrap()).unwrap();
    let part2: BTreeSet<u64> = m["rowgroup_space"]["files"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|f| {
            f["fingerprint"]["path"]
                .as_str()
                .unwrap()
                .ends_with("part-2.parquet")
        })
        .flat_map(|f| {
            f["rowgroup_ids"]
                .as_array()
                .unwrap()
                .iter()
                .map(|i| i.as_u64().unwrap())
                .collect::<Vec<_>>()
        })
        .collect();
    let segments = m["segments"].as_array_mut().unwrap();
    let before = segments.len();
    segments.retain(|s| {
        let r = &s["rowgroup_range"];
        let (lo, hi) = (r["start"].as_u64().unwrap(), r["end"].as_u64().unwrap());
        s["kind"] != "tombstone" || !(lo..hi).any(|i| part2.contains(&i))
    });
    assert!(segments.len() < before);
    std::fs::write(&mpath, serde_json::to_vec_pretty(&m).unwrap()).unwrap();

    let srv = server(&data);
    let asof = both(
        &srv,
        "observe.asof",
        json!({"keys": k, "at": "1000", "live": true}),
        &["asof", d, "--keys", k, "--at", "1000", "--live"],
        0,
    );
    assert!(asof["live"].get("pruned_by").is_none(), "{asof}");
    let why = asof["live"]["unpruned"].as_str().unwrap();
    assert!(
        why.contains("is not complete") && why.contains("bellwether index"),
        "{why}"
    );
    // Not pruned: the plan without tombstones, which holds the answer.
    let plain = cli(&["asof", d, "--keys", k, "--at", "1000"], 0);
    let plain: Value = serde_json::from_str(&plain).unwrap();
    assert_eq!(
        plan_row_groups(&asof, &data),
        plan_row_groups(&plain, &data)
    );
    assert!(truth(&rows, &ids, Some(1000)).is_subset(&plan_row_groups(&asof, &data)));
    let residual = asof["evidence"]["record"]["body"]["plan"]["residual"].to_string();
    assert!(residual.contains("\"live\""), "{residual}");
    let text = human(&["latest", d, "--keys", k, "--live"], 0);
    assert!(
        text.contains("tombstones: not used for pruning;") && text.contains("bellwether index"),
        "{text}"
    );
    // `why` names the declared tombstone and the true cause, an incomplete
    // sidecar that `bellwether index` completes; R-tombstone-1 stays quiet
    // (the tombstone is declared), PR #33 review item 3.
    let report = human(
        &[
            "why",
            d,
            "--from",
            k,
            "--latest-by",
            "ts",
            "--tombstone",
            "void",
        ],
        0,
    );
    let head = report.lines().next().unwrap();
    assert!(
        head.contains("temporal:id@ts,tombstone=void (tombstone sidecar incomplete)"),
        "{report}"
    );
    assert!(
        report.contains(&format!(
            "`bellwether index {}` completes it",
            path(&std::fs::canonicalize(&data).unwrap())
        )),
        "{report}"
    );
    assert!(!report.contains("R-tombstone-1"), "{report}");
    let why: Value = serde_json::from_str(&cli(
        &[
            "why",
            d,
            "--from",
            k,
            "--latest-by",
            "ts",
            "--tombstone",
            "void",
        ],
        0,
    ))
    .unwrap();
    assert_eq!(
        why["tombstones_incomplete"],
        json!([{"kind": "temporal", "columns": ["id"], "ts_column": "ts",
                "tombstone": {"column": "void"}}]),
        "{why}"
    );
    // bellwether index completes it: pruned again.
    cli(&["index", d], 0);
    let asof = cli(&["asof", d, "--keys", k, "--at", "1000", "--live"], 0);
    let asof: Value = serde_json::from_str(&asof).unwrap();
    assert_eq!(asof["live"]["pruned_by"], "tombstone:id@ts", "{asof}");
    assert_eq!(
        plan_row_groups(&asof, &data),
        truth(&rows, &ids, Some(1000))
    );
}

/// Pruning counters count eliminated candidates, not retracted results:
/// a retained group can still have no live winner at the as-of instant.
#[test]
fn live_pruning_counts_are_not_retraction_counts() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("void", DataType::Boolean, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])) as ArrayRef,
            Arc::new(Int64Array::from(vec![5, 10, 5])),
            Arc::new(BooleanArray::from(vec![true, false, true])),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(data.join("part.parquet")).unwrap(),
        schema,
        None,
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let kf = dir.path().join("keys.parquet");
    keys(&kf, "id", &[1, 2]);
    let (d, k) = (path(&data), path(&kf));
    cli(&["index", d, "--key", "temporal:id@ts,tombstone=void"], 0);
    let plan: Value =
        serde_json::from_str(&cli(&["asof", d, "--keys", k, "--at", "7", "--live"], 0)).unwrap();
    // Both keys' winners at 7 are retracted. Key 1's group must still be
    // read because its later live row makes the stored bounds conservative.
    assert_eq!(plan["live"]["keys_with_version"], 2);
    assert_eq!(plan["live"]["keys_without_row_groups"], 1);
    assert_eq!(plan["live"]["key_row_group_pairs_pruned"], 1);
    assert_eq!(plan["planned"]["row_groups"], 1);
}

/// The last 8 bytes of a canonical encoding (ADR 016) of an i64, as the
/// value: big-endian with the sign bit flipped.
fn i64_tail(v: &Value) -> i64 {
    let b: Vec<u8> = v
        .as_array()
        .unwrap()
        .iter()
        .map(|x| u8::try_from(x.as_u64().unwrap()).unwrap())
        .collect();
    let tail: [u8; 8] = b[b.len() - 8..].try_into().unwrap();
    i64::from_be_bytes((u64::from_be_bytes(tail) ^ (1 << 63)).to_be_bytes())
}

/// The certificate of an `asof` live run compares each key's newest
/// version at or before the instant, then its live rows: its row filter's
/// `NotOlderThan` bounds are, per key with a version at or before the
/// instant, that newest time, against the decoded rows, and its `Between`
/// ends at the instant (PR #33 review, mutant M10: a certificate whose
/// newest versions ignore the instant compares no row of a key retracted
/// after it, and passes a plan missing its live winner).
#[test]
fn an_asof_live_certificate_compares_the_newest_rows_at_or_before_the_instant() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let rows = lake(&data, 0..2);
    let ids: Vec<i64> = (0..20).collect();
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &ids);
    let (d, k) = (path(&data), path(&kf));
    cli(&["index", d, "--key", "exact:id"], 0);
    let at = 150i64;
    let why = cli(
        &[
            "why",
            d,
            "--from",
            k,
            "--latest-by",
            "ts",
            "--at",
            &at.to_string(),
            "--tombstone",
            "void",
        ],
        0,
    );
    let why: Value = serde_json::from_str(&why).unwrap();
    let (ev, workload) = (h(&why, "/evidence/hash"), h(&why, "/workload/hash"));
    let proposed = cli(
        &[
            "propose",
            d,
            "--evidence",
            &ev,
            "--by",
            "agent",
            "--key",
            "temporal:id@ts,tombstone=void",
        ],
        0,
    );
    let proposed: Value = serde_json::from_str(&proposed).unwrap();
    let proposal = h(&proposed, "/proposal/hash");
    let verified = cli(
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
    let verified: Value = serde_json::from_str(&verified).unwrap();
    assert_eq!(verified["outcome"]["kind"], "verified", "{verified}");
    let filter = &verified["certificate"]["record"]["body"]["row_filter"];
    let tests: Vec<Value> = filter
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|f| f["all"].as_array().cloned().unwrap_or_default())
        .collect();
    let bounds = tests
        .iter()
        .find(|p| p["op"] == "not_older_than")
        .unwrap_or_else(|| panic!("no not_older_than in {filter}"));
    let got: std::collections::BTreeMap<i64, i64> = bounds["bounds"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| (i64_tail(&b[0]), i64_tail(&b[1])))
        .collect();
    let mut want = std::collections::BTreeMap::new();
    for id in &ids {
        if let Some(n) = rows
            .iter()
            .filter(|r| r.2 == *id && r.3 <= at)
            .map(|r| r.3)
            .max()
        {
            want.insert(*id, n);
        }
    }
    assert_eq!(got, want, "{filter}");
    let between = tests
        .iter()
        .find(|p| p["op"] == "between")
        .unwrap_or_else(|| panic!("no between in {filter}"));
    assert_eq!(i64_tail(&between["hi"]), at, "{filter}");
    assert!(tests.iter().any(|p| p["op"] == "live"), "{filter}");
    // The human line says what was compared, newest version first, then
    // the live rows (PR #33 review item 4).
    let text = human(
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
    assert!(
        text.contains(
            "each key's newest version by ts at or before the instant, found in the data, then \
             its live rows only (void not null and not the tombstone value)"
        ),
        "{text}"
    );
    assert!(!text.contains("lookback"), "{text}");
}

/// `asof --live --ts` is a usage error, never a silently ignored flag, and
/// `index` states what the rows it read hold for the tombstone (PR #33
/// review, items 7 and 8).
#[test]
fn live_with_ts_is_a_usage_error_and_index_counts_tombstone_rows() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let rows = lake(&data, 0..1);
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[1, 2]);
    let (d, k) = (path(&data), path(&kf));
    let out = human(&["index", d, "--key", "temporal:id@ts,tombstone=void"], 0);
    let tombstones = rows.iter().filter(|r| r.4 == Some(true)).count();
    let nulls = rows.iter().filter(|r| r.4.is_none()).count();
    assert!(
        out.contains(&format!(
            "tombstone:id@ts (tombstone=void): of the {} timed rows this build read, {tombstones} \
             are tombstones and {nulls} have a null void",
            rows.len()
        )),
        "{out}"
    );
    assert!(!out.contains("warning:"), "{out}");
    let out = common::bw()
        .args([
            "asof", d, "--keys", k, "--at", "5", "--live", "--ts", "nope",
        ])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2), "{out:?}");
}

/// A tombstone column null in every row marks no row live: `why` says so in
/// the candidate's column and never recommends the key (PR #33 review item
/// 8: it recommended `tombstone=void` as "reads 0 bytes").
#[test]
fn why_never_recommends_a_tombstone_whose_column_is_all_null() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("ts", DataType::Int64, true),
        Field::new("void", DataType::Boolean, true),
    ]));
    for f in 0..2i64 {
        let file = std::fs::File::create(data.join(format!("part-{f}.parquet"))).unwrap();
        let mut w = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
        let ks: Vec<i64> = (f * 40..f * 40 + 40).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(
                    ks.iter().map(|k| k % 10).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(ks.clone())),
                Arc::new(BooleanArray::from(vec![None::<bool>; ks.len()])),
            ],
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[1, 2, 3]);
    let (d, k) = (path(&data), path(&kf));
    cli(&["index", d, "--key", "exact:id"], 0);
    let args = [
        "why",
        d,
        "--from",
        k,
        "--latest-by",
        "ts",
        "--tombstone",
        "void",
    ];
    let why: Value = serde_json::from_str(&cli(&args, 0)).unwrap();
    let candidate = why["candidates"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["spec"]["tombstone"].is_object())
        .unwrap_or_else(|| panic!("no tombstone candidate: {why}"));
    let reason = candidate["not_recommended"].as_str().unwrap();
    assert!(
        reason.contains("every row read has a null void"),
        "{reason}"
    );
    let value = candidate["kind_column"]["value"].as_str().unwrap();
    assert!(
        value.contains("of 80 timed rows, 0 are tombstones and 80 have a null void; warning:"),
        "{value}"
    );
    assert!(
        why["recommended"]["spec"]["tombstone"].is_null(),
        "{}",
        why["recommended"]
    );
}
