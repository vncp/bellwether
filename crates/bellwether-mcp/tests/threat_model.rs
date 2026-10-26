//! The threat model, held by tests (`docs/threat-model.md`, ADR 009): the
//! whole loop through the tools; a score smuggled into any propose tool is
//! rejected by the schema at any depth; `apply.receipt` refuses a failing
//! certificate, one computed against an older manifest, and a hash that is
//! not a certificate, each with its typed code and nothing written; dry runs
//! write nothing; the success path writes a Receipt whose explain chain is
//! intact; refusals carry codes, facts, and a next line. Every test that
//! asserts nothing was written runs under a fixed clock and under the
//! system clock, so a rewrite of an identical record cannot hide a write.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::assert_is_empty,
    clippy::used_underscore_binding
)]

mod common;

use std::path::{Path, PathBuf};

use bellwether_mcp::{Budget, Status};
use common::{
    code, delta, finish, json, ledger_corpus, objects, ok, scattered_corpus, server, server_at,
    write_file, write_keys, CLOCKS,
};
use serde_json::{json, Value};

struct Loop {
    _dir: tempfile::TempDir,
    data: PathBuf,
    index: PathBuf,
    keys: PathBuf,
}

fn folder() -> Loop {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    scattered_corpus(&data, 3, 4, 25, 400);
    let keys = dir.path().join("k.parquet");
    write_keys(&keys, "id", &[5, 77, 123]);
    Loop {
        index: data.join(".bellwether"),
        data,
        keys,
        _dir: dir,
    }
}

fn s(p: &Path) -> &str {
    p.to_str().unwrap()
}

/// observe.why -> score.why -> propose.key -> score.verify -> apply.receipt,
/// every write accounted for, and the receipt's chain intact in the
/// envelope.
#[test]
fn the_loop_through_the_tools_ends_in_an_intact_receipt() {
    for clock in CLOCKS {
        the_loop(clock);
    }
}

fn the_loop(clock: Option<&str>) {
    let f = folder();
    let srv = server_at(&f.data, Budget::default(), clock);

    let before = objects(&f.index);
    let why = ok(&srv, "observe.why", json!({}));
    assert_eq!(why.envelope.status, Status::Ok);
    let ev = json(&why)["evidence"]["hash"].as_str().unwrap().to_string();
    assert!(why
        .envelope
        .cites
        .iter()
        .any(|c| c.hash.to_hex() == ev && c.kind == "evidence"));
    let (kinds, _) = delta(&before, &objects(&f.index));
    assert_eq!(kinds, vec!["evidence"], "observe writes Evidence only");
    // Under the system clock, the next profile Evidence is a millisecond
    // later at least, so it is a distinct record.
    std::thread::sleep(std::time::Duration::from_millis(5));

    let before = objects(&f.index);
    let scored = ok(
        &srv,
        "score.why",
        json!({ "keys": s(&f.keys), "wait": true }),
    );
    let sv = json(&scored);
    let workload = sv["workload"]["hash"].as_str().unwrap().to_string();
    let (kinds, _) = delta(&before, &objects(&f.index));
    // Under a fixed clock the profile Evidence is the same record
    // observe.why wrote, so only the engine's Proposals and Replays are new;
    // under the system clock it is a new Evidence.
    let expected = if clock.is_some() {
        vec!["proposal", "replay"]
    } else {
        vec!["evidence", "proposal", "replay"]
    };
    assert_eq!(kinds, expected, "[clock={clock:?}]");
    assert!(scored.envelope.budget.spent.replays >= 1);

    let before = objects(&f.index);
    let dry = ok(
        &srv,
        "propose.key",
        json!({ "spec": {"kind": "exact", "columns": ["id"]}, "evidence": [ev], "by": "agent",
                "dry_run": true }),
    );
    assert_eq!(json(&dry)["outcome"], "would_write");
    assert_eq!(objects(&f.index), before, "propose dry run wrote");
    let proposed = ok(
        &srv,
        "propose.key",
        json!({ "spec": {"kind": "exact", "columns": ["id"]}, "evidence": [ev], "by": "agent" }),
    );
    let (kinds, other) = delta(&before, &objects(&f.index));
    assert_eq!(kinds, vec!["proposal"]);
    assert!(other.is_empty(), "{other:?}");
    let p = json(&proposed)["proposal"]["hash"]
        .as_str()
        .unwrap()
        .to_string();
    if clock.is_some() {
        assert_eq!(
            p,
            json(&dry)["proposal"]["hash"].as_str().unwrap(),
            "dry run predicts the hash under a fixed clock"
        );
    }

    let verified = ok(
        &srv,
        "score.verify",
        json!({ "proposal": p, "workload": workload, "wait": true }),
    );
    let vv = json(&verified);
    assert_eq!(vv["outcome"]["kind"], "verified");
    let cert = vv["certificate"]["hash"].as_str().unwrap().to_string();

    let before = objects(&f.index);
    let dry = ok(
        &srv,
        "apply.receipt",
        json!({ "certificate": cert, "dry_run": true }),
    );
    assert_eq!(json(&dry)["outcome"]["kind"], "would_apply");
    assert_eq!(objects(&f.index), before, "apply dry run wrote");

    let applied = ok(
        &srv,
        "apply.receipt",
        json!({ "certificate": cert, "wait": true }),
    );
    let av = json(&applied);
    assert_eq!(av["outcome"]["kind"], "applied");
    let receipt = av["outcome"]["receipt"]["hash"]
        .as_str()
        .unwrap()
        .to_string();
    let (kinds, _) = delta(&before, &objects(&f.index));
    assert_eq!(
        kinds,
        vec!["receipt", "replay"],
        "apply writes the realized Replay and the Receipt"
    );
    let chain = applied
        .envelope
        .chains
        .iter()
        .find(|c| c.receipt.to_hex() == receipt)
        .expect("the receipt's chain is in the envelope");
    assert!(chain.intact);
    assert_eq!(chain.links.len(), 5);
    assert!(chain.links.iter().all(|l| l.status == "ok"));
    assert!(applied
        .envelope
        .cites
        .iter()
        .any(|c| c.hash.to_hex() == receipt && c.kind == "receipt"));

    // The resources read the same ledger.
    let receipts: Value =
        serde_json::from_str(&srv.resource(bellwether_mcp::RECEIPTS).unwrap()).unwrap();
    assert_eq!(receipts["receipts"][0]["receipt"]["hash"], receipt.as_str());
    assert_eq!(receipts["receipts"][0]["intact"], true);
    let findings: Value =
        serde_json::from_str(&srv.resource(bellwether_mcp::FINDINGS).unwrap()).unwrap();
    assert_eq!(findings["findings"].as_array().unwrap().len(), 1);
    let card: Value =
        serde_json::from_str(&srv.resource(bellwether_mcp::SCORECARD).unwrap()).unwrap();
    assert!(!card["rules"].as_array().unwrap().is_empty());
    // observe.profile is the findings view. Under a fixed clock its newest
    // profile Evidence is the one the receipt cites, so it mentions the
    // receipt with its chain; under the system clock score.why wrote a newer
    // one, which no receipt cites yet.
    let profile = ok(&srv, "observe.profile", json!({}));
    assert_eq!(json(&profile), findings);
    let mentioned = profile
        .envelope
        .chains
        .iter()
        .any(|c| c.receipt.to_hex() == receipt && c.intact);
    assert_eq!(mentioned, clock.is_some(), "[clock={clock:?}]");
}

/// A `score` field anywhere in a propose tool's arguments is rejected by the
/// schema, top level or nested, and nothing is written.
#[test]
fn a_score_cannot_be_smuggled_into_any_propose_tool() {
    for clock in CLOCKS {
        no_smuggled_score(clock);
    }
}

fn no_smuggled_score(clock: Option<&str>) {
    let f = folder();
    let srv = server_at(&f.data, Budget::default(), clock);
    let why = ok(&srv, "observe.why", json!({}));
    let ev = json(&why)["evidence"]["hash"].as_str().unwrap().to_string();
    let before = objects(&f.index);
    let spec = json!({"kind": "exact", "columns": ["id"]});
    let shape = json!({"before": "b", "after": "a", "invariant": "i"});
    // A score or a replay is a caller-supplied cost, refused by name (ADR
    // 033); any other unknown field is a plain schema error.
    let cost = "caller_supplied_cost";
    let cases = [
        (
            "propose.key",
            json!({"spec": spec, "evidence": [ev], "by": "a", "score": 1.0}),
            cost,
        ),
        (
            "propose.key",
            json!({"spec": {"kind": "exact", "columns": ["id"], "score": 9}, "evidence": [ev], "by": "a"}),
            cost,
        ),
        (
            "propose.key",
            json!({"spec": spec, "evidence": [ev], "by": "a", "dry_run": true, "confidence": 1}),
            "invalid_input",
        ),
        (
            "propose.layout",
            json!({"cluster_by": ["id"], "evidence": [ev], "by": "a", "score": 1}),
            cost,
        ),
        (
            "propose.rewrite",
            json!({"shape": shape, "evidence": [ev], "by": "a", "score": 1}),
            cost,
        ),
        (
            "propose.rewrite",
            json!({"shape": {"before": "b", "after": "a", "invariant": "i", "score": 1}, "evidence": [ev], "by": "a"}),
            cost,
        ),
        (
            "propose.key",
            json!({"spec": {"kind": "bloom", "columns": ["id"], "fpr": 0.01, "replay": {}}, "evidence": [ev], "by": "a"}),
            cost,
        ),
    ];
    for (tool, args, want) in cases {
        let r = srv.call(tool, args.clone());
        assert_eq!(code(&r), want, "{tool} {args}");
        let errors = &r.envelope.refusal.as_ref().unwrap().fields["errors"];
        assert!(!errors.as_array().unwrap().is_empty());
    }
    // The same specs without the extra field are accepted (as dry runs).
    for spec in [
        json!({"kind": "bloom", "columns": ["id"], "fpr": 0.01}),
        json!({"kind": "temporal", "columns": ["id"], "ts_column": "ts"}),
        json!({"kind": "range", "column": "ts"}),
    ] {
        ok(
            &srv,
            "propose.key",
            json!({"spec": spec, "evidence": [ev], "by": "a", "dry_run": true}),
        );
    }
    // A propose tool's published schema has no score property anywhere.
    for t in bellwether_mcp::tools::catalog() {
        assert!(!t.input.to_string().contains("\"score\":"), "{}", t.name);
    }
    assert_eq!(objects(&f.index), before, "a rejected call wrote");
}

/// apply.receipt refuses a failing certificate, a certificate against an
/// older manifest, and a hash that is not a certificate, each with its
/// typed code, and writes nothing.
#[test]
fn apply_refuses_unverified_applies_and_writes_nothing() {
    for clock in CLOCKS {
        apply_refuses(clock);
    }
}

fn apply_refuses(clock: Option<&str>) {
    let f = folder();
    let srv = server_at(&f.data, Budget::default(), clock);
    let why = ok(&srv, "observe.why", json!({}));
    let ev = json(&why)["evidence"]["hash"].as_str().unwrap().to_string();

    // A failing certificate: a rewrite verified on outputs that differ.
    let dir = f._dir.path();
    let (before_out, after_out) = (dir.join("before"), dir.join("after"));
    ledger_corpus(&before_out, 1, 1, 10, 10);
    ledger_corpus(&after_out, 1, 1, 10, 10);
    write_file(
        &after_out.join("part-000.parquet"),
        &[vec![(0, 0, Some(0), "changed".into())]],
    );
    let rw = ok(
        &srv,
        "propose.rewrite",
        json!({"shape": {"before": "b", "after": "a", "invariant": "same rows"}, "evidence": [ev], "by": "agent"}),
    );
    let rp = json(&rw)["proposal"]["hash"].as_str().unwrap().to_string();
    let v = srv.call(
        "score.verify",
        json!({"proposal": rp, "wait": true,
               "rewrite": {"before": s(&before_out), "after": s(&after_out), "key": ["id"]}}),
    );
    assert_eq!(code(&v), "certificate_failed");
    let failing = json(&v)["certificate"]["hash"]
        .as_str()
        .unwrap()
        .to_string();
    let fields = &v.envelope.refusal.as_ref().unwrap().fields;
    assert!(
        fields["mismatched"].as_u64().unwrap()
            + fields["missing_left"].as_u64().unwrap()
            + fields["missing_right"].as_u64().unwrap()
            > 0
    );
    assert!(
        !fields["mismatches_sample"].as_array().unwrap().is_empty(),
        "{fields}"
    );

    // A passing key certificate, then a commit makes it stale.
    let scored = ok(&srv, "score.why", json!({"keys": s(&f.keys), "wait": true}));
    let sv = json(&scored);
    let wl = sv["workload"]["hash"].as_str().unwrap().to_string();
    let cand = sv["candidates"][0]["proposal"]["hash"]
        .as_str()
        .unwrap()
        .to_string();
    let v = ok(
        &srv,
        "score.verify",
        json!({"proposal": cand, "workload": wl, "wait": true}),
    );
    let stale = json(&v)["certificate"]["hash"]
        .as_str()
        .unwrap()
        .to_string();
    srv.engine()
        .index(&[bellwether::spec::parse("exact:payload").unwrap()])
        .unwrap();

    let before = objects(&f.index);
    for dry in [true, false] {
        let r = srv.call(
            "apply.receipt",
            json!({"certificate": failing, "dry_run": dry, "wait": true}),
        );
        assert_eq!(code(&r), "certificate_failed", "[dry={dry}]");
        let r = srv.call(
            "apply.receipt",
            json!({"certificate": stale, "dry_run": dry, "wait": true}),
        );
        assert_eq!(code(&r), "stale_manifest", "[dry={dry}]");
        let rf = &r.envelope.refusal.as_ref().unwrap().fields;
        assert_eq!(rf["certified"], "0");
        assert_eq!(rf["current"], "1");
        assert!(r
            .envelope
            .refusal
            .as_ref()
            .unwrap()
            .next
            .contains("score.verify"));
        let r = srv.call(
            "apply.receipt",
            json!({"certificate": cand, "dry_run": dry, "wait": true}),
        );
        assert_eq!(code(&r), "wrong_kind", "[dry={dry}]");
        let wf = &r.envelope.refusal.as_ref().unwrap().fields;
        assert_eq!(wf["expected"], "a certificate");
        assert_eq!(wf["found"], "a proposal");
        let absent = "ab".repeat(32);
        let r = srv.call(
            "apply.receipt",
            json!({"certificate": absent, "dry_run": dry, "wait": true}),
        );
        assert_eq!(code(&r), "not_found", "[dry={dry}]");
        let r = srv.call(
            "apply.receipt",
            json!({"certificate": "not-a-hash", "dry_run": dry}),
        );
        assert_eq!(code(&r), "invalid_input", "[dry={dry}]");
    }
    assert_eq!(objects(&f.index), before, "a refused apply wrote");
}

/// Refusals with facts: the reserved proposer, a stale index with its file
/// lists, no workload, and the session budget.
#[test]
fn refusals_carry_codes_facts_and_next() {
    for clock in CLOCKS {
        refusals(clock);
    }
}

fn refusals(clock: Option<&str>) {
    let f = folder();
    let srv = server_at(&f.data, Budget::default(), clock);
    let why = ok(&srv, "observe.why", json!({}));
    let ev = json(&why)["evidence"]["hash"].as_str().unwrap().to_string();
    let before = objects(&f.index);
    for by in ["engine", " Engine "] {
        let r = srv.call(
            "propose.key",
            json!({"spec": {"kind": "exact", "columns": ["id"]}, "evidence": [ev], "by": by}),
        );
        assert_eq!(code(&r), "reserved_proposer");
        // The text is the facade's typed refusal, as the CLI prints it.
        assert_eq!(json(&r)["outcome"], "refused");
    }
    // A sketch has no receipt path: refused, typed, dry run or not, alone
    // or as a sidecar, before anything is written.
    for (spec, sidecars) in [("sketch:id", json!([])), ("exact:id", json!(["sketch:id"]))] {
        for dry in [true, false] {
            let r = srv.call(
                "propose.key",
                json!({"spec": spec, "sidecars": sidecars, "evidence": [ev], "by": "a",
                    "dry_run": dry}),
            );
            assert_eq!(code(&r), "sketch_not_proposable", "{spec} [dry={dry}]");
            assert_eq!(json(&r)["refusal"]["declaration"], "sketch:id");
        }
    }
    assert_eq!(objects(&f.index), before);

    let r = srv.call("score.why", json!({"wait": true}));
    assert_eq!(code(&r), "no_workload");

    srv.engine()
        .index(&[bellwether::spec::parse("exact:id").unwrap()])
        .unwrap();
    write_file(
        &f.data.join("part-777.parquet"),
        &[vec![(1, 1, Some(1), "late".into())]],
    );
    let r = srv.call("observe.locate", json!({"keys": s(&f.keys)}));
    assert_eq!(code(&r), "stale_index");
    let fields = &r.envelope.refusal.as_ref().unwrap().fields;
    assert!(fields["untracked"][0]
        .as_str()
        .unwrap()
        .ends_with("part-777.parquet"));
    assert!(fields["changed"].as_array().unwrap().is_empty());
    assert!(r
        .envelope
        .refusal
        .as_ref()
        .unwrap()
        .next
        .contains("bellwether index"));

    let r = srv.call("observe.nothing", json!({}));
    assert_eq!(code(&r), "unknown_tool");

    // A budget of one replay: score.why spends it, the next scoring call is
    // refused before anything runs.
    let g = folder();
    let tight = server_at(
        &g.data,
        Budget {
            replays: 1,
            ..Budget::default()
        },
        clock,
    );
    ok(
        &tight,
        "score.why",
        json!({"keys": s(&g.keys), "wait": true}),
    );
    let before = objects(&g.index);
    let r = tight.call("score.why", json!({"keys": s(&g.keys), "wait": true}));
    assert_eq!(code(&r), "budget_exceeded");
    let bf = &r.envelope.refusal.as_ref().unwrap().fields;
    assert_eq!(bf["dimension"], "replays");
    assert_eq!(bf["limit"], 1);
    assert_eq!(objects(&g.index), before);
}

/// Long calls run as jobs by a static rule: score and apply return a job id
/// unless `wait` is set, a repeated start returns the same job, and the
/// job's result is the tool's reply.
#[test]
fn score_and_apply_run_as_jobs() {
    let f = folder();
    let srv = server(&f.data);
    let r = srv.call("score.why", json!({"keys": s(&f.keys)}));
    assert_eq!(r.envelope.status, Status::Running);
    let job = r.envelope.job.clone().unwrap();
    assert_eq!(job.id.len(), 64);
    assert!(job.progress.contains("score.why"));
    // The same request (wait aside, any field order) is the same job.
    let again = srv.call(
        "jobs.start",
        json!({"arguments": {"wait": true, "keys": s(&f.keys)}, "tool": "score.why"}),
    );
    assert_eq!(again.envelope.job.as_ref().unwrap().id, job.id);
    let done = finish(&srv, &job.id);
    assert_eq!(done.envelope.status, Status::Ok);
    assert!(done
        .envelope
        .job
        .as_ref()
        .unwrap()
        .progress
        .contains("finished"));
    let sync = ok(&srv, "score.why", json!({"keys": s(&f.keys), "wait": true}));
    assert_eq!(
        done.text, sync.text,
        "under a fixed clock the job's result is the call's"
    );
    let r = srv.call("jobs.result", json!({"job": "00".repeat(32)}));
    assert_eq!(code(&r), "job_not_found");
    let r = srv.call(
        "jobs.start",
        json!({"tool": "jobs.status", "arguments": {}}),
    );
    assert_eq!(code(&r), "unknown_tool");
    let r = srv.call(
        "jobs.start",
        json!({"tool": "propose.key", "arguments": {"score": 1}}),
    );
    assert_eq!(code(&r), "caller_supplied_cost");
}
