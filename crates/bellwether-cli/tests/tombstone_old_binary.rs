//! A binary from before tombstones keeps working on a tombstone index (B8;
//! ADR 028, additive sidecar kinds). With `BELLWETHER_OLD_BIN` naming a
//! `bellwether` built at the M5 freeze (e1bd168), this test:
//!
//! - indexes a folder with `temporal:id@ts,tombstone=void` (this build),
//!   which declares the temporal key unchanged and the tombstone as a
//!   `tombstone` sidecar;
//! - has the old binary plan `latest` and `asof` through it: the same
//!   output and Evidence, byte for byte, as this build's lookups without
//!   `--live` (this build's `specs` also names the key's tombstone);
//! - appends a file and has the old binary `index` the folder (a rewrite of
//!   the manifest): the key spec, the sidecar's declaration, and its
//!   segments are all kept; the sidecar is then incomplete;
//! - has this build answer `latest --live` unpruned (saying so and naming
//!   `bellwether index`) with every live newest row in the plan, then
//!   `bellwether index` completes the sidecar and the plan is exactly the
//!   live newest rows' row groups again.
//!
//! A second test holds ADR 028's rule for the tombstone declaration's
//! unknown fields and for records: a field the old binary does not know on
//! the declaration survives the old binary's rewrite and this build's; and
//! the records a live workload writes (a key spec with a tombstone in a
//! Proposal, `Predicate::Live`, `RecordedRun.tombstone`) are read by the old
//! binary fail-closed: skipped as records it cannot read, its receipt view
//! flags the chain, and its `fix` on them refuses and writes nothing.
//!
//! Without `BELLWETHER_OLD_BIN` it prints `SKIPPED:` and passes: no old
//! binary is built in CI. Build one with `git worktree add /tmp/old e1bd168
//! && cargo build -p bellwether-cli` there.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

mod common;

use std::collections::BTreeSet;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use common::{index_dir, keys, path, run};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde_json::Value;

const CLOCK: &str = "2026-10-24T12:00:00.000Z";

/// One row: (file, row group, id, ts, void).
type LakeRow = (String, u64, i64, i64, Option<bool>);

/// Files of four row groups of 30 rows: row `k` has id `(k * 7) % 20`, ts
/// `k`, and `void` true when `k % 5 == 0`, null when `k % 37 == 0`.
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

/// Per key, the rows at its newest ts at or before `at` that are live:
/// their (file, row group) pairs.
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

fn new(args: &[&str]) -> String {
    run(&[&["--clock", CLOCK, "--json"], args].concat(), 0)
}

fn old(bin: &str, args: &[&str]) -> String {
    let out = Command::new(bin)
        .args([&["--clock", CLOCK, "--json"], args].concat())
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "old binary {args:?}: {}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

/// The old binary's lookup and this build's (without `--live`) agree: the
/// same JSON, Evidence (and so plan) byte for byte, except that this build
/// lists the declared spec with its tombstone, which the old binary does
/// not know; without it the lists are equal.
fn same_plan(bin: &str, args: &[&str]) {
    let o: Value = serde_json::from_str(&old(bin, args)).unwrap();
    let mut n: Value = serde_json::from_str(&new(args)).unwrap();
    for s in n["specs"].as_array_mut().unwrap() {
        s.as_object_mut().unwrap().remove("tombstone");
    }
    assert_eq!(o, n, "{args:?}: the old binary planned differently");
}

fn manifest(data: &Path) -> Value {
    serde_json::from_slice(&std::fs::read(index_dir(data).join("manifest.json")).unwrap()).unwrap()
}

/// The tombstone sidecar's declaration and segment ids.
fn sidecar(m: &Value) -> (Vec<Value>, BTreeSet<String>) {
    let decl = m["schema"]["sidecars"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["kind"] == "tombstone")
        .cloned()
        .collect();
    let segs = m["segments"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["kind"] == "tombstone")
        .map(|s| s["id"].as_str().unwrap().to_string())
        .collect();
    (decl, segs)
}

#[test]
fn the_m5_binary_opens_plans_and_keeps_a_tombstone_index() {
    let Ok(bin) = std::env::var("BELLWETHER_OLD_BIN") else {
        eprintln!(
            "SKIPPED: BELLWETHER_OLD_BIN is not set; it names a bellwether binary built at \
             e1bd168 (M5), against which this test runs"
        );
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let mut rows = lake(&data, 0..2);
    let ids: Vec<i64> = (0..20).collect();
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &ids);
    let (d, k) = (path(&data), path(&kf));
    new(&["index", d, "--key", "temporal:id@ts,tombstone=void"]);
    let m = manifest(&data);
    let (decl, segs) = sidecar(&m);
    assert_eq!(decl.len(), 1, "{m}");
    assert!(!segs.is_empty(), "{m}");
    assert_eq!(
        m["schema"]["key_specs"],
        serde_json::json!([{"kind": "temporal", "columns": ["id"], "ts_column": "ts"}])
    );
    // A retracted key, so pruning has something to do.
    assert!(truth(&rows, &ids, None).len() < 20);

    // The old binary plans the temporal key exactly as this build does
    // without --live.
    for args in [
        vec!["latest", d, "--keys", k],
        vec!["asof", d, "--keys", k, "--at", "150"],
    ] {
        same_plan(&bin, &args);
    }

    // A rewrite by the old binary: a new file, then `index`.
    rows.extend(lake(&data, 2..3));
    let out = old(&bin, &["index", d]);
    let m2 = manifest(&data);
    assert_eq!(m2["schema"]["key_specs"], m["schema"]["key_specs"], "{m2}");
    let (decl2, segs2) = sidecar(&m2);
    assert_eq!(decl2, decl, "the old binary dropped the declaration: {m2}");
    assert_eq!(
        segs2, segs,
        "the old binary changed the sidecar's segments: {m2}"
    );
    assert!(
        m2["sequence"].as_u64() > m["sequence"].as_u64(),
        "no rewrite happened: {out}"
    );
    for args in [
        vec!["latest", d, "--keys", k],
        vec!["asof", d, "--keys", k, "--at", "300"],
    ] {
        same_plan(&bin, &args);
    }

    // This build: the sidecar is incomplete, the live plan is unpruned and
    // holds the answer, and says what to run.
    let live: Value = serde_json::from_str(&new(&["latest", d, "--keys", k, "--live"])).unwrap();
    assert!(live["live"].get("pruned_by").is_none(), "{live}");
    let unpruned = live["live"]["unpruned"].as_str().unwrap();
    assert!(
        unpruned.contains("is not complete") && unpruned.contains("bellwether index"),
        "{unpruned}"
    );
    assert!(truth(&rows, &ids, None).is_subset(&plan_row_groups(&live, &data)));
    new(&["index", d]);
    let (_, segs3) = sidecar(&manifest(&data));
    assert!(segs3.is_superset(&segs) && segs3.len() > segs.len());
    let live: Value = serde_json::from_str(&new(&["latest", d, "--keys", k, "--live"])).unwrap();
    assert_eq!(live["live"]["pruned_by"], "tombstone:id@ts", "{live}");
    assert_eq!(plan_row_groups(&live, &data), truth(&rows, &ids, None));
    // And the old binary still opens and plans the completed index.
    same_plan(&bin, &["latest", d, "--keys", k]);
}

/// The old binary's run, whatever its exit: (code, stdout, stderr), human.
fn old_any(bin: &str, args: &[&str]) -> (Option<i32>, String, String) {
    let out = Command::new(bin)
        .args([&["--clock", CLOCK], args].concat())
        .output()
        .unwrap();
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

fn ledger_files(data: &Path) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut stack = vec![index_dir(data)];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else {
                out.insert(p.display().to_string());
            }
        }
    }
    out
}

#[test]
fn the_m5_binary_keeps_unknown_fields_and_reads_live_records_fail_closed() {
    let Ok(bin) = std::env::var("BELLWETHER_OLD_BIN") else {
        eprintln!("SKIPPED: BELLWETHER_OLD_BIN is not set (an e1bd168 bellwether binary)");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    lake(&data, 0..2);
    let ids: Vec<i64> = (0..20).collect();
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &ids);
    let (d, k) = (path(&data), path(&kf));

    // The declaration's unknown field survives the old binary's rewrite and
    // this build's.
    new(&["index", d, "--key", "temporal:id@ts,tombstone=void"]);
    let mpath = index_dir(&data).join("manifest.json");
    let mut m: Value = serde_json::from_slice(&std::fs::read(&mpath).unwrap()).unwrap();
    for s in m["schema"]["sidecars"].as_array_mut().unwrap() {
        if s["kind"] == "tombstone" {
            s["future_field"] = serde_json::json!({"x": 1});
        }
    }
    std::fs::write(&mpath, serde_json::to_vec_pretty(&m).unwrap()).unwrap();
    let (decl, _) = sidecar(&manifest(&data));
    assert_eq!(decl[0]["future_field"], serde_json::json!({"x": 1}));
    lake(&data, 2..3);
    old(&bin, &["index", d]);
    assert_eq!(
        sidecar(&manifest(&data)).0,
        decl,
        "the old binary dropped it"
    );
    lake(&data, 3..4);
    new(&["index", d]);
    assert_eq!(sidecar(&manifest(&data)).0, decl, "this build dropped it");

    // Live records, end to end with this build: a workload of live runs, a
    // proposal of the key with its tombstone, a certificate, a receipt.
    let why: Value = serde_json::from_str(&new(&[
        "why",
        d,
        "--from",
        k,
        "--latest-by",
        "ts",
        "--tombstone",
        "void",
    ]))
    .unwrap();
    let ev = why["evidence"]["hash"].as_str().unwrap().to_string();
    let workload = why["workload"]["hash"].as_str().unwrap().to_string();
    let proposed: Value = serde_json::from_str(&new(&[
        "propose",
        d,
        "--evidence",
        &ev,
        "--by",
        "agent",
        "--key",
        "temporal:id@ts,tombstone=void",
    ]))
    .unwrap();
    let proposal = proposed["proposal"]["hash"].as_str().unwrap().to_string();
    let before = ledger_files(&data);
    // The old binary's ledger views skip what it cannot read and say so.
    let views: Vec<String> = ["findings", "receipts", "scorecard"]
        .iter()
        .map(|v| {
            let (code, out, err) = old_any(&bin, &["ledger", v, d]);
            assert_eq!(code, Some(0), "ledger {v}: {out}{err}");
            format!("ledger {v}:\n{out}{err}")
        })
        .collect();
    assert!(
        views.iter().any(|v| v.contains("cannot read")),
        "the old binary did not say it skipped the live records: {views:#?}"
    );
    // Its `why --workload` on the live workload refuses, writing nothing.
    let (code, out, err) = old_any(&bin, &["why", d, "--workload", &workload]);
    assert_ne!(code, Some(0), "the old binary read a live workload: {out}");
    assert!(err.contains("canonical form"), "{err}");
    // Its `fix` on the proposal of a key with a tombstone refuses too.
    let (code, out, err) = old_any(
        &bin,
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
    assert_ne!(
        code,
        Some(0),
        "the old binary verified a live proposal: {out}"
    );
    assert!(!err.is_empty(), "{out}");
    assert_eq!(ledger_files(&data), before, "the old binary wrote a record");
}
