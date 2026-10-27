//! Relational replay (WP15) on the command line and over MCP, on the bench's
//! two-dataset corpus (`events` beside `repos`): `why --plan` with
//! `--dataset`, byte for byte the MCP `score.why` result with `plan` and
//! `datasets` (ADR 010); a key proposal over a consumer-plan workload
//! verified on the keys the plan looks up and applied; a plan rewrite
//! (`propose --before-plan --after-plan`) scored, its actual outputs
//! reconciled, and attested; and the refusals: a dataset not bound, a data
//! change between the score and the attest.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

mod common;

use std::collections::BTreeSet;
use std::fs::File;
use std::path::{Path, PathBuf};

use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use bellwether::{FixOutcome, FixResult, Refusal, WhyReport};
use bellwether_bench::relational::{generate_relational, RelationalParams};
use common::{check_records, index_dir, path, run, typed};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use serde_json::{json, Value};

const CLOCK: &str = "2026-10-27T12:00:00.000Z";

struct Corpus {
    _dir: tempfile::TempDir,
    root: PathBuf,
    events: PathBuf,
    repos: PathBuf,
}

fn corpus() -> Corpus {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("rel");
    generate_relational(
        &root,
        &RelationalParams {
            seed: 15,
            repos: 300,
            events: 30_000,
            event_rows_per_row_group: 1_000,
            event_row_groups_per_file: 5,
            repo_rows_per_row_group: 100,
            restated_fraction: 0.2,
            null_fraction: 0.01,
            keysets: vec![10, 40],
        },
    )
    .unwrap();
    let root = std::fs::canonicalize(root).unwrap();
    Corpus {
        events: root.join("events"),
        repos: root.join("repos"),
        root,
        _dir: dir,
    }
}

fn server(c: &Corpus) -> bellwether_mcp::Server {
    bellwether_mcp::Server::new(&bellwether_mcp::Config {
        root: path(&c.events).to_string(),
        engine: bellwether::EngineOptions {
            clock: Some(CLOCK.into()),
            datasets: [("repos".to_string(), path(&c.repos).to_string())]
                .into_iter()
                .collect(),
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

/// The `(repo_id, type)` rows of `events` whose `repo_id` is in `keys`, as
/// a consumer's query returns them, written to `out/part.parquet`.
fn output(events: &Path, keys: &BTreeSet<i64>, out: &Path) {
    std::fs::create_dir_all(out).unwrap();
    let mut ids = Vec::new();
    let mut types = Vec::new();
    let mut files: Vec<PathBuf> = std::fs::read_dir(events)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
        .collect();
    files.sort();
    for f in files {
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(f).unwrap())
            .unwrap()
            .build()
            .unwrap();
        for batch in reader {
            let b = batch.unwrap();
            let id = b
                .column_by_name("repo_id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone();
            let ty = b
                .column_by_name("type")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone();
            for i in 0..b.num_rows() {
                if !id.is_null(i) && keys.contains(&id.value(i)) {
                    ids.push(id.value(i));
                    types.push(ty.value(i).to_string());
                }
            }
        }
    }
    let batch = RecordBatch::try_from_iter([
        (
            "repo_id",
            std::sync::Arc::new(Int64Array::from(ids)) as ArrayRef,
        ),
        (
            "type",
            std::sync::Arc::new(StringArray::from(types)) as ArrayRef,
        ),
    ])
    .unwrap();
    let mut w = ArrowWriter::try_new(
        File::create(out.join("part.parquet")).unwrap(),
        batch.schema(),
        None,
    )
    .unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn key_set(keys: &Path) -> BTreeSet<i64> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(keys).unwrap())
        .unwrap()
        .build()
        .unwrap();
    let mut out = BTreeSet::new();
    for b in reader {
        let b = b.unwrap();
        let a = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        out.extend(a.iter().flatten());
    }
    out
}

#[test]
fn why_plan_is_byte_identical_over_cli_and_mcp_and_names_unbound_datasets() {
    let c = corpus();
    let (ev, rp) = (path(&c.events), path(&c.repos));
    let keys = c.root.join("workload/keys-10.parquet");
    let join = c.root.join("plans/join.json");
    run(&["index", rp, "--key", "exact:repo_id,counts"], 0);
    run(&["index", ev, "--key", "exact:repo_id,counts"], 0);
    let srv = server(&c);
    let r = srv.call(
        "score.why",
        json!({"keys": path(&keys), "plan": path(&join), "wait": true}),
    );
    let m = r.text + "\n";
    let line = cli(
        &[
            "--dataset",
            &format!("repos={rp}"),
            "why",
            ev,
            "--from",
            path(&keys),
            "--plan",
            path(&join),
        ],
        0,
    );
    assert_eq!(
        line, m,
        "score.why with plan: the CLI line and the MCP text differ"
    );
    let why: WhyReport = typed(&line);
    let v: Value = serde_json::from_str(&line).unwrap();
    check_records(&index_dir(&c.events), &v);
    assert_eq!(why.plans.len(), 1);
    let p = &why.plans[0];
    assert_eq!(p.runs, 1);
    assert_eq!(
        p.current.iter().map(|o| o.op.as_str()).collect::<Vec<_>>(),
        ["scan", "semi_join", "scan", "join", "project"]
    );
    // Both sides counted: the join's rows are exact, as is every read.
    assert!(
        p.current[3].rows_out.exactness.is_exact(),
        "{:?}",
        p.current[3]
    );
    assert!(p.current.iter().all(|o| o.bytes.exactness.is_exact()));
    assert_eq!(
        p.datasets
            .iter()
            .map(|d| d.name.as_str())
            .collect::<Vec<_>>(),
        ["main", "repos"]
    );

    // A plan that scans a dataset not bound: refused by name, before
    // anything is read, and the same over MCP.
    let bare = bellwether_mcp::Server::new(&bellwether_mcp::Config {
        root: ev.to_string(),
        engine: bellwether::EngineOptions {
            clock: Some(CLOCK.into()),
            ..bellwether::EngineOptions::default()
        },
        budget: bellwether_mcp::Budget::default(),
        join: None,
    })
    .unwrap();
    let r = bare.call(
        "score.why",
        json!({"keys": path(&keys), "plan": path(&join), "wait": true}),
    );
    let refusal = r.envelope.refusal.unwrap();
    assert_eq!(refusal.code.as_str(), "invalid_argument");
    assert!(refusal.message.contains("\"repos\""), "{}", refusal.message);
    assert!(refusal.message.contains("--dataset"), "{}", refusal.message);
    let out = common::bw()
        .args([
            "--clock",
            CLOCK,
            "why",
            ev,
            "--from",
            path(&keys),
            "--plan",
            path(&join),
        ])
        .output()
        .unwrap();
    assert_ne!(out.status.code(), Some(0));
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.contains(&refusal.message), "{stderr}");
    // A tool names datasets only from those the human bound.
    let r = srv.call(
        "score.why",
        json!({"keys": path(&keys), "plan": path(&join), "datasets": ["elsewhere"], "wait": true}),
    );
    assert_eq!(
        r.envelope.refusal.unwrap().code.as_str(),
        "invalid_argument"
    );

    // The human table: one row per node with its exactness.
    let human = run(
        &[
            "--clock",
            CLOCK,
            "--dataset",
            &format!("repos={rp}"),
            "why",
            ev,
            "--from",
            path(&keys),
            "--plan",
            path(&join),
        ],
        0,
    );
    assert!(human.contains("operators of plan"), "{human}");
    assert!(!human.contains("Some("), "{human}");
}

#[test]
fn a_key_over_a_plan_workload_is_verified_on_the_keys_the_plan_looks_up() {
    let c = corpus();
    let (ev, rp) = (path(&c.events), path(&c.repos));
    let keys = c.root.join("workload/keys-40.parquet");
    let window = c.root.join("plans/window.json");
    run(&["index", rp, "--key", "exact:repo_id,counts"], 0);
    run(&["index", ev, "--key", "exact:type"], 0);
    let line = cli(
        &[
            "--dataset",
            &format!("repos={rp}"),
            "why",
            ev,
            "--from",
            path(&keys),
            "--plan",
            path(&window),
        ],
        0,
    );
    let why: WhyReport = typed(&line);
    let rec = why.recommended.clone().expect("a recommended key");
    assert_eq!(rec.spec.columns(), ["repo_id"]);
    let workload = why.workload.as_ref().unwrap().hash;
    let out = cli(
        &[
            "--dataset",
            &format!("repos={rp}"),
            "fix",
            ev,
            "--proposal",
            &rec.proposal.to_hex(),
            "--workload",
            &workload.to_hex(),
        ],
        0,
    );
    let fix: FixResult = typed(&out);
    let v: Value = serde_json::from_str(&out).unwrap();
    check_records(&index_dir(&c.events), &v);
    assert!(
        matches!(fix.outcome, FixOutcome::Applied { .. }),
        "{:?}",
        fix.outcome
    );
    let cert = fix.verification.unwrap();
    assert!(cert.pass);
    // Every key the window looked up was compared (the run's 40).
    assert_eq!(cert.counts.matched, 40);

    // A key a plan uses only on a join that no run or literal keys
    // restrict is looked up with every key main holds, not the workload's:
    // verify refuses it, naming what to do, and writes no certificate.
    let full = c.root.join("full_join.json");
    std::fs::write(
        &full,
        serde_json::to_vec(&json!({"version": 1, "root": 3, "nodes": [
            {"op": "scan", "dataset": "main", "columns": ["repo_id", "type"]},
            {"op": "scan", "dataset": "repos", "columns": ["repo_id", "name"]},
            {"op": "join", "left": 0, "right": 1, "on": [["repo_id", "repo_id"]], "kind": "inner"},
            {"op": "project", "input": 2, "columns": ["repo_id", "type", "name"]}]}))
        .unwrap(),
    )
    .unwrap();
    let why: WhyReport = typed(&cli(
        &[
            "--dataset",
            &format!("repos={rp}"),
            "why",
            ev,
            "--from",
            path(&keys),
            "--plan",
            path(&full),
        ],
        0,
    ));
    let workload = why.workload.as_ref().unwrap().hash.to_hex();
    let evidence = why.evidence.hash.unwrap().to_hex();
    let p = run(
        &[
            "propose",
            ev,
            "--evidence",
            &evidence,
            "--key",
            "exact:repo_id,counts",
        ],
        0,
    );
    let proposal = p.trim().trim_start_matches("proposal ").to_string();
    let out = common::bw()
        .args([
            "--clock",
            CLOCK,
            "--dataset",
            &format!("repos={rp}"),
            "fix",
            ev,
            "--proposal",
            &proposal,
            "--workload",
            &workload,
            "--verify-only",
        ])
        .output()
        .unwrap();
    assert_ne!(out.status.code(), Some(0));
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.contains("every key main holds"), "{stderr}");
    assert!(stderr.contains("keys_from: run"), "{stderr}");
}

#[test]
fn a_plan_rewrite_is_scored_reconciled_and_attested() {
    let c = corpus();
    let (ev, rp) = (path(&c.events), path(&c.repos));
    let keys_file = c.root.join("workload/keys-10.parquet");
    run(&["index", rp, "--key", "exact:repo_id,counts"], 0);
    run(&["index", ev, "--key", "exact:repo_id,counts"], 0);
    // The before plan reads every column of events, the after plan only
    // the two its projection keeps.
    let before_plan = c.root.join("plans/semijoin.json");
    let mut after: Value = serde_json::from_slice(&std::fs::read(&before_plan).unwrap()).unwrap();
    after["nodes"][0]["columns"] = json!(["repo_id", "type"]);
    let after_plan = c.root.join("after.json");
    std::fs::write(&after_plan, serde_json::to_vec(&after).unwrap()).unwrap();
    let why: WhyReport = typed(&cli(
        &[
            "why",
            ev,
            "--from",
            path(&keys_file),
            "--plan",
            path(&before_plan),
        ],
        0,
    ));
    let workload = why.workload.as_ref().unwrap().hash.to_hex();
    let evidence = why.evidence.hash.unwrap().to_hex();
    let p = run(
        &[
            "propose",
            ev,
            "--evidence",
            &evidence,
            "--before",
            "the semi-join reading every events column",
            "--after",
            "the semi-join reading only repo_id and type",
            "--invariant",
            "the same (repo_id, type) rows",
            "--before-plan",
            path(&before_plan),
            "--after-plan",
            path(&after_plan),
        ],
        0,
    );
    let proposal = p.trim().trim_start_matches("proposal ").to_string();
    let k = key_set(&keys_file);
    let (bo, ao) = (c.root.join("out-before"), c.root.join("out-after"));
    output(&c.events, &k, &bo);
    output(&c.events, &k, &ao);
    let fix_args = |out_after: &Path| {
        vec![
            "fix".to_string(),
            ev.to_string(),
            "--proposal".into(),
            proposal.clone(),
            "--workload".into(),
            workload.clone(),
            "--before-output".into(),
            path(&bo).to_string(),
            "--after-output".into(),
            path(out_after).to_string(),
            "--key".into(),
            "repo_id,type".into(),
        ]
    };
    let args = fix_args(&ao);
    let out = cli(&args.iter().map(String::as_str).collect::<Vec<_>>(), 0);
    let fix: FixResult = typed(&out);
    let v: Value = serde_json::from_str(&out).unwrap();
    check_records(&index_dir(&c.events), &v);
    let FixOutcome::Attested { receipt, .. } = &fix.outcome else {
        panic!("{:?}", fix.outcome)
    };
    let bellwether::types::Record::Receipt(r) = &receipt.record else {
        panic!("not a receipt")
    };
    assert!(r.attest_only);
    assert_eq!(r.manifest_before, r.manifest_after);
    let Some(bellwether::Score::Replayed {
        current, proposed, ..
    }) = &fix.score
    else {
        panic!("{:?}", fix.score)
    };
    assert!(proposed.bytes < current.bytes, "{current:?} {proposed:?}");

    // Outputs that differ fail the certificate: no receipt.
    let wrong = c.root.join("out-wrong");
    let mut fewer = k.clone();
    let first = *fewer.iter().next().unwrap();
    fewer.remove(&first);
    output(&c.events, &fewer, &wrong);
    let args = fix_args(&wrong);
    let out = cli(&args.iter().map(String::as_str).collect::<Vec<_>>(), 1);
    let fix: FixResult = typed(&out);
    assert!(
        matches!(
            fix.outcome,
            FixOutcome::Refused {
                refusal: Refusal::CertificateFailed { .. }
            }
        ),
        "{:?}",
        fix.outcome
    );
}
