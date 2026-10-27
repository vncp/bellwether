//! Relational replay (WP15) on the command line and over MCP, on the bench's
//! two-dataset corpus (`events` beside `repos`): `why --plan` with
//! `--dataset`, byte for byte the MCP `score.why` result with `plan` and
//! `datasets` (ADR 010); a key proposal over a consumer-plan workload
//! verified on the keys the plan looks up and applied; a filter-pushdown
//! rewrite (`propose --before-plan --after-plan`: the semi-join on the
//! run's keys moved below the aggregate) scored, its actual outputs
//! reconciled, and attested, with `why` blaming the plan's shape, not the
//! layout; and the refusals: a dataset not bound, outputs that differ.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::{Path, PathBuf};

use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch};
use bellwether::{CulpritReason, FixOutcome, FixResult, Refusal, WhyReport};
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
            // Many repositories, skewed: the rarest have a handful of
            // events, which sit in a few of the time-ordered row groups.
            repos: 3_000,
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

/// Per row group of `events` (files in name order), its `repo_id`s.
fn repo_ids_by_row_group(events: &Path) -> Vec<Vec<Option<i64>>> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(events)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
        .collect();
    files.sort();
    let mut out = Vec::new();
    for f in files {
        let groups = ParquetRecordBatchReaderBuilder::try_new(File::open(&f).unwrap())
            .unwrap()
            .metadata()
            .num_row_groups();
        for g in 0..groups {
            let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&f).unwrap())
                .unwrap()
                .with_row_groups(vec![g])
                .build()
                .unwrap();
            let mut ids = Vec::new();
            for batch in reader {
                let b = batch.unwrap();
                let id = b
                    .column_by_name("repo_id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .clone();
                ids.extend(id.iter());
            }
            out.push(ids);
        }
    }
    out
}

/// The rarest `n` repositories with events (fewest row groups, then id),
/// written to `path` as a key file: their events sit in a strict subset of
/// the row groups, asserted, so a key on `repo_id` prunes.
fn sparse_keys(events: &Path, n: usize, path: &Path) -> BTreeSet<i64> {
    let groups = repo_ids_by_row_group(events);
    let mut at: BTreeMap<i64, BTreeSet<usize>> = BTreeMap::new();
    for (g, ids) in groups.iter().enumerate() {
        for id in ids.iter().flatten() {
            at.entry(*id).or_default().insert(g);
        }
    }
    let mut rare: Vec<(usize, i64)> = at.iter().map(|(k, g)| (g.len(), *k)).collect();
    rare.sort_unstable();
    let keys: BTreeSet<i64> = rare.iter().take(n).map(|(_, k)| *k).collect();
    let covered: BTreeSet<usize> = keys.iter().flat_map(|k| at[k].iter().copied()).collect();
    assert!(
        covered.len() * 2 <= groups.len(),
        "the keys sit in {} of {} row groups",
        covered.len(),
        groups.len()
    );
    common::keys(path, "repo_id", &keys.iter().copied().collect::<Vec<_>>());
    keys
}

/// What a consumer's `select repo_id, count(*) ... where repo_id in (keys)
/// group by repo_id` returns over `events`, written to `out/part.parquet`.
fn counts_output(events: &Path, keys: &BTreeSet<i64>, out: &Path) {
    std::fs::create_dir_all(out).unwrap();
    let mut counts: BTreeMap<i64, i64> = BTreeMap::new();
    for id in repo_ids_by_row_group(events)
        .into_iter()
        .flatten()
        .flatten()
    {
        if keys.contains(&id) {
            *counts.entry(id).or_default() += 1;
        }
    }
    let batch = RecordBatch::try_from_iter([
        (
            "repo_id",
            std::sync::Arc::new(Int64Array::from(counts.keys().copied().collect::<Vec<_>>()))
                as ArrayRef,
        ),
        (
            "count",
            std::sync::Arc::new(Int64Array::from(
                counts.values().copied().collect::<Vec<_>>(),
            )) as ArrayRef,
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
    // Repositories whose events sit in a strict subset of the row groups:
    // a key on repo_id prunes the window's read.
    let keys = c.root.join("workload/keys-sparse.parquet");
    let wanted = sparse_keys(&c.events, 5, &keys);
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
    // Every key the window looked up was compared (the run's five).
    assert_eq!(cert.counts.matched, wanted.len() as u64);

    // A key a plan uses only on a join that no run or literal keys
    // restrict is looked up with every key main holds, not the workload's:
    // verify refuses it, naming what to do, and writes no certificate.
    let stderr = verify_refused(
        &c,
        &keys,
        "full_join",
        &json!({"version": 1, "root": 3, "nodes": [
            {"op": "scan", "dataset": "main", "columns": ["repo_id", "type"]},
            {"op": "scan", "dataset": "repos", "columns": ["repo_id", "name"]},
            {"op": "join", "left": 0, "right": 1, "on": [["repo_id", "repo_id"]], "kind": "inner"},
            {"op": "project", "input": 2, "columns": ["repo_id", "type", "name"]}]}),
        &["exact:repo_id,counts"],
    );
    assert!(stderr.contains("every key main holds"), "{stderr}");
    assert!(stderr.contains("keys_from: run"), "{stderr}");
}

/// Records `plan` (written as `<name>.json`) over the run of `keys` with
/// `why`, proposes `specs` as one key proposal, and returns the stderr of
/// `fix --verify-only`, asserted to fail.
fn verify_refused(c: &Corpus, keys: &Path, name: &str, plan: &Value, specs: &[&str]) -> String {
    let (ev, rp) = (path(&c.events), path(&c.repos));
    let file = c.root.join(format!("{name}.json"));
    std::fs::write(&file, serde_json::to_vec(plan).unwrap()).unwrap();
    let dataset = format!("repos={rp}");
    let why: WhyReport = typed(&cli(
        &[
            "--dataset",
            &dataset,
            "why",
            ev,
            "--from",
            path(keys),
            "--plan",
            path(&file),
        ],
        0,
    ));
    let workload = why.workload.as_ref().unwrap().hash.to_hex();
    let evidence = why.evidence.hash.unwrap().to_hex();
    let mut args = vec!["propose", ev, "--evidence", evidence.as_str()];
    for s in specs {
        args.extend(["--key", *s]);
    }
    let p = run(&args, 0);
    let proposal = p.trim().trim_start_matches("proposal ").to_string();
    let out = common::bw()
        .args([
            "--clock",
            CLOCK,
            "--dataset",
            &dataset,
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
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert_ne!(out.status.code(), Some(0), "{name}: {stderr}");
    stderr
}

/// A canonical-encoded `Int64` (`docs/format.md`, "Canonical encoding":
/// the integer tag, then the big-endian value with its sign bit flipped).
fn int(v: i64) -> Vec<u8> {
    let mut b = vec![0x02];
    b.extend_from_slice(&(v ^ i64::MIN).to_be_bytes());
    b
}

/// A canonical-encoded millisecond timestamp (the temporal tag, the
/// timestamp kind, the millisecond unit, then the value as for an integer).
fn millis(v: i64) -> Vec<u8> {
    let mut b = vec![0x08, 0x04, 0x02];
    b.extend_from_slice(&(v ^ i64::MIN).to_be_bytes());
    b
}

/// A canonical-encoded string (the string tag, the bytes with each zero
/// byte escaped, then two zero bytes).
fn utf8(s: &str) -> Vec<u8> {
    assert!(!s.as_bytes().contains(&0));
    let mut b = vec![0x06];
    b.extend_from_slice(s.as_bytes());
    b.extend_from_slice(&[0, 0]);
    b
}

#[test]
fn verify_refuses_plan_lookups_one_certificate_cannot_compare() {
    let c = corpus();
    let (ev, rp) = (path(&c.events), path(&c.repos));
    let keys = c.root.join("workload/keys-sparse.parquet");
    let wanted = sparse_keys(&c.events, 5, &keys);
    run(&["index", rp, "--key", "exact:repo_id,counts"], 0);
    run(&["index", ev, "--key", "exact:repo_id,counts"], 0);
    let scan = |columns: &[&str]| json!({"op": "scan", "dataset": "main", "columns": columns});
    let semi = |input: u32| json!({"op": "semi_join", "input": input, "columns": ["repo_id"], "keys": {"from": "run"}});
    let between = |input: u32, column: &str, lo: Vec<u8>, hi: Vec<u8>| {
        json!({"op": "filter", "input": input,
               "predicate": {"op": "between", "column": column, "lo": lo, "hi": hi}})
    };

    // The plan only groups by the proposed key's column: nothing is looked
    // up through it, so there is nothing to verify.
    let stderr = verify_refused(
        &c,
        &keys,
        "groups_only",
        &json!({"version": 1, "root": 2, "nodes": [
            scan(&["repo_id", "type"]),
            semi(0),
            {"op": "aggregate", "input": 1, "group_by": ["type"], "measures": [{"function": "count"}]}]}),
        &["exact:type"],
    );
    assert!(
        stderr.contains("look nothing up on main through exact:type"),
        "{stderr}"
    );
    assert!(stderr.contains("bellwether index"), "{stderr}");

    // Whole keys through the exact key and a range through the range key.
    let stderr = verify_refused(
        &c,
        &keys,
        "keys_and_range",
        &json!({"version": 1, "root": 2, "nodes": [
            scan(&["repo_id", "created_at"]),
            semi(0),
            between(1, "created_at", millis(0), millis(i64::MAX / 2))]}),
        &["exact:repo_id", "range:created_at"],
    );
    assert!(
        stderr.contains("both by whole keys and by ranges"),
        "{stderr}"
    );
    assert!(stderr.contains("separately"), "{stderr}");

    // Ranges through two range keys on two columns.
    let stderr = verify_refused(
        &c,
        &keys,
        "two_ranges",
        &json!({"version": 1, "root": 3, "nodes": [
            scan(&["repo_id", "created_at"]),
            semi(0),
            between(1, "repo_id", int(0), int(i64::MAX)),
            between(2, "created_at", millis(0), millis(i64::MAX / 2))]}),
        &["range:repo_id", "range:created_at"],
    );
    assert!(stderr.contains("by ranges on several columns"), "{stderr}");
    assert!(stderr.contains("one range key at a time"), "{stderr}");

    // The run's keys on repo_id, literal keys on (repo_id, type).
    let first = *wanted.iter().next().unwrap();
    let tuple = [int(first), utf8("PushEvent")].concat();
    let stderr = verify_refused(
        &c,
        &keys,
        "two_column_sets",
        &json!({"version": 1, "root": 2, "nodes": [
            scan(&["repo_id", "type"]),
            semi(0),
            {"op": "filter", "input": 1, "predicate": {"op": "in", "columns": ["repo_id", "type"],
             "values": [tuple]}}]}),
        &["exact:repo_id", "exact:repo_id,type"],
    );
    assert!(stderr.contains("on several key-column sets"), "{stderr}");
    assert!(stderr.contains("one key at a time"), "{stderr}");

    // Every row of the run's keys, joined to the newest version of a
    // literal key: one certificate compares one version rule.
    let other = (1..=3_000).find(|k| !wanted.contains(k)).unwrap();
    let stderr = verify_refused(
        &c,
        &keys,
        "locate_and_latest",
        &json!({"version": 1, "root": 5, "nodes": [
            scan(&["repo_id", "created_at"]),
            semi(0),
            scan(&["repo_id", "created_at"]),
            {"op": "filter", "input": 2, "predicate": {"op": "in", "columns": ["repo_id"],
             "values": [int(other)]}},
            {"op": "latest", "input": 3, "columns": ["repo_id"], "ts": "created_at"},
            {"op": "join", "left": 1, "right": 4, "on": [["repo_id", "repo_id"]], "kind": "inner"}]}),
        &["temporal:repo_id@created_at"],
    );
    assert!(
        stderr.contains("only the newest version of others"),
        "{stderr}"
    );
    assert!(stderr.contains("separate workloads"), "{stderr}");
}

#[test]
fn a_plan_rewrite_is_scored_reconciled_and_attested() {
    let c = corpus();
    let (ev, rp) = (path(&c.events), path(&c.repos));
    let keys_file = c.root.join("workload/keys-sparse.parquet");
    let k = sparse_keys(&c.events, 5, &keys_file);
    run(&["index", rp, "--key", "exact:repo_id,counts"], 0);
    run(&["index", ev, "--key", "exact:repo_id,counts"], 0);
    // A filter pushdown: the before plan counts every repository's events
    // and only then keeps the run's keys, so its scan reads every row
    // group; the after plan keeps the keys first, so the key prunes.
    let plan = |nodes: Value, root: u32| {
        serde_json::to_vec(&json!({"version": 1, "root": root, "nodes": nodes})).unwrap()
    };
    let scan = json!({"op": "scan", "dataset": "main", "columns": ["repo_id"]});
    let before_plan = c.root.join("before.json");
    std::fs::write(
        &before_plan,
        plan(
            json!([scan,
                {"op": "aggregate", "input": 0, "group_by": ["repo_id"], "measures": [{"function": "count"}]},
                {"op": "semi_join", "input": 1, "columns": ["repo_id"], "keys": {"from": "run"}}]),
            2,
        ),
    )
    .unwrap();
    let after_plan = c.root.join("after.json");
    std::fs::write(
        &after_plan,
        plan(
            json!([scan,
                {"op": "semi_join", "input": 0, "columns": ["repo_id"], "keys": {"from": "run"}},
                {"op": "aggregate", "input": 1, "group_by": ["repo_id"], "measures": [{"function": "count"}]}]),
            2,
        ),
    )
    .unwrap();
    let line = cli(
        &[
            "why",
            ev,
            "--from",
            path(&keys_file),
            "--plan",
            path(&before_plan),
        ],
        0,
    );
    let why: WhyReport = typed(&line);
    // The culprit is the plan's shape, not the layout: the keys sit in
    // few row groups, and the semi-join applies them above the aggregate.
    let culprit = why.culprit.clone().expect("a culprit");
    let CulpritReason::PlanShape {
        keys_at,
        above,
        least_row_groups,
        row_groups,
        keys_row_groups,
        keys_span,
        ..
    } = &culprit.reason
    else {
        panic!("{culprit:?}")
    };
    assert_eq!(
        keys_at.as_ref().map(|n| (n.node, n.op.as_str())),
        Some((2, "semi_join"))
    );
    assert_eq!(
        above.as_ref().map(|n| (n.node, n.op.as_str())),
        Some((1, "aggregate"))
    );
    assert_eq!(
        least_row_groups, row_groups,
        "the before plan reads everything"
    );
    assert!(
        keys_row_groups * 2 <= *keys_span,
        "{keys_row_groups} of {keys_span}"
    );
    let human = run(
        &[
            "--clock",
            CLOCK,
            "why",
            ev,
            "--from",
            path(&keys_file),
            "--plan",
            path(&before_plan),
        ],
        0,
    );
    let line = human
        .lines()
        .find(|l| l.starts_with("culprit"))
        .unwrap_or_else(|| panic!("{human}"));
    assert!(line.contains("the plan's shape"), "{line}");
    assert!(line.contains("node 2 (semi_join)"), "{line}");
    assert!(line.contains("above node 1 (aggregate)"), "{line}");
    assert!(!line.contains("the layout, not"), "{line}");
    let workload = why.workload.as_ref().unwrap().hash.to_hex();
    let evidence = why.evidence.hash.unwrap().to_hex();
    let p = run(
        &[
            "propose",
            ev,
            "--evidence",
            &evidence,
            "--before",
            "count every repository's events, then keep the run's keys",
            "--after",
            "keep the run's keys, then count their events",
            "--invariant",
            "the same (repo_id, count) rows",
            "--before-plan",
            path(&before_plan),
            "--after-plan",
            path(&after_plan),
        ],
        0,
    );
    let proposal = p.trim().trim_start_matches("proposal ").to_string();
    let (bo, ao) = (c.root.join("out-before"), c.root.join("out-after"));
    counts_output(&c.events, &k, &bo);
    counts_output(&c.events, &k, &ao);
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
            "repo_id".into(),
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
    counts_output(&c.events, &fewer, &wrong);
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
