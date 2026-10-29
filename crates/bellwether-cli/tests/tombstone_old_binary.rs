//! A binary from before tombstones keeps working on a tombstone index (B8;
//! ADR 028, additive sidecar kinds). With `BELLWETHER_OLD_BIN` naming a
//! `bellwether` built at the M5 freeze (e1bd168), this test:
//!
//! - indexes a folder with `temporal:id@ts,tombstone=void` (this build),
//!   which declares the temporal key unchanged and the tombstone as a
//!   `tombstone` sidecar;
//! - has the old binary plan `latest` and `asof` through it: the same
//!   output and plan Evidence as this build's lookups without `--live`,
//!   but for the record version (this build writes 4, the old binary 3,
//!   so the hashes differ) and this build's `specs` naming the tombstone;
//! - appends a file and has the old binary `index` the folder (a rewrite of
//!   the manifest): the key spec, the sidecar's declaration, and its
//!   segments are all kept; the sidecar is then incomplete;
//! - has this build answer `latest --live` unpruned (saying so and naming
//!   `bellwether index`) with every live newest row in the plan, then
//!   `bellwether index` completes the sidecar and the plan is exactly the
//!   live newest rows' row groups again.
//!
//! A second test holds ADR 028's rule for the tombstone declaration's
//! unknown fields and the record version 4 boundary: a field the old
//! binary does not know on the declaration survives the old binary's
//! rewrite and this build's; and the live chain this build writes (a
//! workload of live runs, a Proposal whose key spec carries a tombstone, a
//! Replay, a Certificate whose row filter ends in `live`, plan Evidence, a
//! Receipt), every record at version 4, is refused by the old binary
//! before it parses one: its ledger views list every record as one it
//! cannot read; `fix --proposal` (with and without `--verify-only`), `fix
//! --certificate` and `receipt show` exit 4 naming record version 4; the
//! live workload is refused by `why --workload` and by `fix` of the old
//! binary's own proposal; and none of them writes a ledger record, a
//! manifest, or a segment.
//!
//! Without `BELLWETHER_OLD_BIN` it prints `SKIPPED:` and passes: no old
//! binary is built in CI. Build one with `git worktree add /tmp/old e1bd168
//! && cargo build -p bellwether-cli` there.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether::types::format::RECORD_VERSION;
use bellwether::types::{Hash, Record};
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
    let base = std::fs::canonicalize(root).unwrap();
    v["evidence"]["record"]["body"]["plan"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|e| {
            let rel = common::rel_under(&base, e["file"]["path"].as_str().unwrap());
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
/// same JSON and plan Evidence byte for byte, except that this build lists
/// the declared spec with its tombstone, which the old binary does not know
/// (without it the lists are equal), and writes the Evidence at record
/// version 4 where the old binary writes 3 (so their hashes differ; each is
/// the hash of its own record).
fn same_plan(bin: &str, args: &[&str]) {
    let mut o: Value = serde_json::from_str(&old(bin, args)).unwrap();
    let mut n: Value = serde_json::from_str(&new(args)).unwrap();
    for s in n["specs"].as_array_mut().unwrap() {
        s.as_object_mut().unwrap().remove("tombstone");
    }
    for (v, version) in [(&mut o, 3), (&mut n, RECORD_VERSION)] {
        let ev = &mut v["evidence"];
        let record: Record = serde_json::from_value(ev["record"].clone()).unwrap();
        assert_eq!(record.record_version(), version, "{args:?}");
        assert_eq!(
            ev["hash"].as_str().unwrap(),
            Hash::of_record(&record).unwrap().to_hex(),
            "{args:?}"
        );
        ev["record"]["record_version"] = Value::Null;
        ev["hash"] = Value::Null;
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

/// Every file under the index but its cache (ledger records, manifests,
/// segments), with the hash of its bytes.
fn index_files(data: &Path) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let mut stack = vec![index_dir(data)];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                if p.file_name().is_some_and(|n| n != "cache") {
                    stack.push(p);
                }
            } else {
                let bytes = std::fs::read(&p).unwrap();
                out.insert(p.display().to_string(), Hash::of(&bytes).to_hex());
            }
        }
    }
    out
}

/// Every ledger record: (hash, kind, record version), read by this build.
fn ledger_records(data: &Path) -> Vec<(String, String, u32)> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(index_dir(data).join("ledger")).unwrap() {
        let p = e.unwrap().path();
        let Some(name) = p
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .strip_suffix(".rec")
        else {
            continue;
        };
        let record: Record = serde_json::from_slice(&std::fs::read(&p).unwrap()).unwrap();
        let kind = serde_json::to_value(&record).unwrap()["record"]
            .as_str()
            .unwrap()
            .to_string();
        out.push((name.to_string(), kind, record.record_version()));
    }
    out.sort();
    out
}

#[test]
fn the_m5_binary_keeps_unknown_fields_and_refuses_record_version_4() {
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

    // The live chain, end to end with this build, on a folder indexed by
    // `exact:id` only: a workload of live runs and its profile Evidence, a
    // proposal of the key with its tombstone, a replay and a certificate
    // (`--verify-only`), then the receipt (`fix --certificate`, the apply
    // tier).
    let data = dir.path().join("live");
    lake(&data, 0..2);
    let d = path(&data);
    new(&["index", d, "--key", "exact:id"]);
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
    let verified: Value = serde_json::from_str(&new(&[
        "fix",
        d,
        "--proposal",
        &proposal,
        "--workload",
        &workload,
        "--verify-only",
    ]))
    .unwrap();
    assert_eq!(verified["outcome"]["kind"], "verified", "{verified}");
    let certificate = verified["certificate"]["hash"]
        .as_str()
        .unwrap()
        .to_string();
    let filter = &verified["certificate"]["record"]["body"]["row_filter"];
    assert_eq!(
        filter[0]["all"].as_array().unwrap().last().unwrap()["op"],
        "live",
        "{filter}"
    );
    // The old binary's own records: profile Evidence (and whatever else its
    // `why` writes) and a key proposal without a tombstone, all at version
    // 3, to be fixed against the live workload below.
    let written_new: BTreeSet<String> = ledger_records(&data).into_iter().map(|r| r.0).collect();
    let old_why: Value = serde_json::from_str(&old(&bin, &["why", d, "--from", k])).unwrap();
    let old_ev = old_why["evidence"]["hash"].as_str().unwrap().to_string();
    let old_proposed: Value = serde_json::from_str(&old(
        &bin,
        &[
            "propose",
            d,
            "--evidence",
            &old_ev,
            "--by",
            "agent",
            "--key",
            "exact:id",
        ],
    ))
    .unwrap();
    let old_proposal = old_proposed["proposal"]["hash"]
        .as_str()
        .unwrap()
        .to_string();

    let old_records: BTreeSet<String> = ledger_records(&data)
        .into_iter()
        .map(|r| r.0)
        .filter(|h| !written_new.contains(h))
        .collect();
    assert!(old_records.contains(&old_ev) && old_records.contains(&old_proposal));
    let applied: Value =
        serde_json::from_str(&new(&["fix", d, "--certificate", &certificate])).unwrap();
    assert_eq!(applied["outcome"]["kind"], "applied", "{applied}");
    let receipt = applied["outcome"]["receipt"]["hash"]
        .as_str()
        .unwrap()
        .to_string();

    // Every record this build wrote is at version 4, and they hold every
    // kind; the old binary's are at 3.
    let records = ledger_records(&data);
    let mut kinds = BTreeSet::new();
    for (h, kind, version) in &records {
        let want = if old_records.contains(h) { 3 } else { 4 };
        assert_eq!(*version, want, "{kind} {h}");
        if want == 4 {
            kinds.insert(kind.as_str());
        }
    }
    assert_eq!(
        kinds,
        ["certificate", "evidence", "proposal", "receipt", "replay"].into()
    );

    let before = index_files(&data);
    // The old binary's ledger views list every version 4 record as one it
    // cannot read, and no other.
    for v in ["findings", "receipts", "scorecard"] {
        let (code, out, err) = old_any(&bin, &["ledger", v, d]);
        assert_eq!(code, Some(0), "ledger {v}: {out}{err}");
        let skipped = out
            .lines()
            .find_map(|l| l.split_once("records this build cannot read, skipped:"))
            .unwrap_or_else(|| panic!("ledger {v} skipped nothing:\n{out}"));
        let n = records.iter().filter(|r| r.2 == 4).count();
        assert_eq!(skipped.0.trim(), n.to_string(), "ledger {v}:\n{out}");
        for (h, kind, version) in &records {
            assert_eq!(
                skipped.1.contains(&h[..12]),
                *version == 4,
                "ledger {v}, {kind} {h}:\n{out}"
            );
        }
    }
    assert_eq!(index_files(&data), before, "a ledger view wrote");
    // Reading a version 4 record by its hash: refused before it is parsed
    // (exit 4, the record version named), writing nothing.
    let refusals: [&[&str]; 4] = [
        &[
            "fix",
            d,
            "--proposal",
            &proposal,
            "--workload",
            &workload,
            "--verify-only",
        ],
        &["fix", d, "--proposal", &proposal, "--workload", &workload],
        &["fix", d, "--certificate", &certificate],
        &["receipt", "show", d, &receipt],
    ];
    for args in refusals {
        let (code, out, err) = old_any(&bin, args);
        assert_eq!(code, Some(4), "{args:?}: {out}{err}");
        assert!(
            err.contains("unsupported record version 4; this build supports up to 3"),
            "{args:?}: {err}"
        );
        assert_eq!(index_files(&data), before, "{args:?} wrote");
    }
    // The live workload is refused wherever the old binary loads it: its
    // `why --workload`, and `fix` of its own proposal on it.
    let workload_refusals: [&[&str]; 2] = [
        &["why", d, "--workload", &workload],
        &[
            "fix",
            d,
            "--proposal",
            &old_proposal,
            "--workload",
            &workload,
            "--verify-only",
        ],
    ];
    for args in workload_refusals {
        let (code, out, err) = old_any(&bin, args);
        assert_ne!(code, Some(0), "{args:?} read a live workload: {out}");
        assert!(
            err.contains(&format!(
                "workload {workload} is not stored in canonical form"
            )),
            "{args:?}: {err}"
        );
        assert_eq!(index_files(&data), before, "{args:?} wrote");
    }
}
