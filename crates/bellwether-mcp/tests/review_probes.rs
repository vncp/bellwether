//! The PR #5 review's probes, kept as tests: concurrent jobs against the
//! budget, the rows and seconds limits, every refusal an agent meets in the
//! loop (typed, never `engine_error`, never a CLI flag), dry runs against an
//! index location that does not exist yet, the apply dry run agreeing with
//! the real apply on a key that is already complete (nothing written, with
//! and without a fixed clock), closed schemas, and a job id that always
//! names one result.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

mod common;

use std::path::{Path, PathBuf};

use bellwether_mcp::jobs::JobState;
use bellwether_mcp::{Budget, Reply, Server, Status};
use common::{
    delta, finish, json, ledger_corpus, objects, ok, scattered_corpus, server, server_at,
    server_with, write_file, write_keys, CLOCKS,
};
use serde_json::{json, Value};

struct F {
    dir: tempfile::TempDir,
    data: PathBuf,
    keys: PathBuf,
}

fn folder() -> F {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    scattered_corpus(&data, 3, 4, 25, 400);
    let keys = dir.path().join("k.parquet");
    write_keys(&keys, "id", &[5, 77, 123]);
    F { dir, data, keys }
}

fn s(p: &Path) -> &str {
    p.to_str().unwrap()
}

fn ev(srv: &Server) -> String {
    json(&ok(srv, "observe.why", json!({})))["evidence"]["hash"]
        .as_str()
        .unwrap()
        .to_string()
}

fn refusal(r: &Reply) -> &bellwether_mcp::Refusal {
    r.envelope.refusal.as_ref().unwrap()
}

/// Asserts `r` was refused with `expected`, and that neither its message
/// nor its next line tells the agent to pass a CLI flag.
fn refused_as(label: &str, r: &Reply, expected: &str) {
    assert!(r.is_error(), "{label}: not refused: {}", r.text);
    let f = refusal(r);
    assert_eq!(f.code.as_str(), expected, "{label}: {f:?}");
    assert_ne!(f.code.as_str(), "engine_error", "{label}: {f:?}");
    assert!(f.fields.is_object(), "{label}: {f:?}");
    assert!(!f.next.contains("--"), "{label}: next names a flag: {f:?}");
    assert!(
        !f.message.contains(" --"),
        "{label}: message names a flag: {f:?}"
    );
}

/// A looping agent starts eight distinct `score.why` jobs at once on a
/// one-replay budget: the first is admitted and reserves the budget, the
/// rest wait for it to settle and are then refused, as if run one at a
/// time. At most one call runs past the limit.
///
/// Only final outcomes are asserted: a call returns its job, still running
/// or (when the job thread already settled, as a quickly refused job can)
/// done; how far a job got when its call returned is scheduling, not
/// behaviour.
#[test]
fn concurrent_jobs_cannot_overrun_the_budget() {
    let one = Budget {
        replays: 1,
        ..Budget::default()
    };
    let f = folder();
    let srv = server_with(&f.data, one);
    let mut ids = Vec::new();
    for n in 1..=8u32 {
        let r = srv.call(
            "score.why",
            json!({"keys": s(&f.keys), "max_candidates": n}),
        );
        assert!(
            matches!(r.envelope.status, Status::Running | Status::Ok),
            "{:?}",
            r.envelope.status
        );
        let job = r.envelope.job.unwrap();
        assert_eq!(
            job.state == JobState::Done,
            r.envelope.status == Status::Ok,
            "{job:?}"
        );
        ids.push(job.id);
    }
    let mut ran = 0;
    for id in &ids {
        let r = finish(&srv, id);
        if r.is_error() {
            refused_as("concurrent score.why", &r, "budget_exceeded");
            assert_eq!(refusal(&r).fields["dimension"], "replays");
        } else {
            ran += 1;
        }
    }
    assert_eq!(ran, 1, "calls that ran past a one-replay budget");
    assert_eq!(
        ids.iter().collect::<std::collections::BTreeSet<_>>().len(),
        8,
        "one job per distinct call"
    );
    let state = srv.budget();
    assert_eq!(state.reserved, bellwether_mcp::budget::Spent::default());
    assert!(state.spent.replays >= 1);

    // The same calls one at a time: the same one call runs.
    let g = folder();
    let seq = server_with(&g.data, one);
    let ran = (1..=8u32)
        .filter(|n| {
            !seq.call(
                "score.why",
                json!({"keys": s(&g.keys), "max_candidates": n, "wait": true}),
            )
            .is_error()
        })
        .count();
    assert_eq!(ran, 1);
}

/// The rows limit: a verification is charged the rows both sides read, and
/// the next call is refused on `rows`. The seconds limit: a spent wall-time
/// budget refuses any call, observe included.
#[test]
fn the_rows_and_seconds_limits_refuse_the_next_call() {
    let f = folder();
    let srv = server_with(
        &f.data,
        Budget {
            rows: 1,
            ..Budget::default()
        },
    );
    let e = ev(&srv);
    let dir = f.dir.path();
    let (b, a) = (dir.join("before"), dir.join("after"));
    ledger_corpus(&b, 1, 1, 10, 10);
    ledger_corpus(&a, 1, 1, 10, 10);
    let rw = ok(
        &srv,
        "propose.rewrite",
        json!({"shape": {"before": "b", "after": "a", "invariant": "same"}, "evidence": [e], "by": "x"}),
    );
    let rp = json(&rw)["proposal"]["hash"].as_str().unwrap().to_string();
    let args = json!({"proposal": rp, "wait": true,
        "rewrite": {"before": s(&b), "after": s(&a), "key": ["id"]}});
    ok(&srv, "score.verify", args.clone());
    assert_eq!(srv.budget().spent.rows, 20, "10 rows on each side");
    let r = srv.call("score.verify", args);
    refused_as("second verify", &r, "budget_exceeded");
    let bf = &refusal(&r).fields;
    assert_eq!(
        (&bf["dimension"], &bf["spent"], &bf["limit"]),
        (&json!("rows"), &json!(20), &json!(1))
    );

    let g = folder();
    for (budget, dimension) in [
        (
            Budget {
                seconds: 0,
                ..Budget::default()
            },
            "seconds",
        ),
        (
            Budget {
                replays: 0,
                ..Budget::default()
            },
            "replays",
        ),
    ] {
        let tight = server_with(&g.data, budget);
        for tool in ["observe.why", "observe.profile"] {
            let r = tight.call(tool, json!({}));
            refused_as(tool, &r, "budget_exceeded");
            assert_eq!(refusal(&r).fields["dimension"], dimension);
        }
    }
}

/// Every refusal an agent meets in the loop has a typed code with facts and
/// a next line in MCP field names; none is `engine_error`.
#[test]
fn loop_refusals_are_typed_and_name_mcp_fields() {
    let f = folder();
    let srv = server(&f.data);
    let e = ev(&srv);
    let absent = "ab".repeat(32);
    let spec = json!({"kind": "exact", "columns": ["id"]});
    let p = json(&ok(
        &srv,
        "propose.key",
        json!({"spec": spec, "evidence": [e], "by": "a"}),
    ))["proposal"]["hash"]
        .as_str()
        .unwrap()
        .to_string();
    let rw = json(&ok(
        &srv,
        "propose.rewrite",
        json!({"shape": {"before": "b", "after": "a", "invariant": "i"}, "evidence": [e], "by": "a"}),
    ))["proposal"]["hash"]
        .as_str()
        .unwrap()
        .to_string();
    let lp = json(&ok(
        &srv,
        "propose.layout",
        json!({"cluster_by": ["batch"], "evidence": [e], "by": "a"}),
    ))["proposal"]["hash"]
        .as_str()
        .unwrap()
        .to_string();
    // (label, tool, arguments, code, the field `fields.field` names)
    let cases: Vec<(&str, &str, Value, &str, Option<&str>)> = vec![
        (
            "evidence not found",
            "propose.key",
            json!({"spec": spec, "evidence": [absent], "by": "a"}),
            "not_found",
            Some("evidence"),
        ),
        (
            "evidence not found, dry run",
            "propose.key",
            json!({"spec": spec, "evidence": [absent], "by": "a", "dry_run": true}),
            "not_found",
            Some("evidence"),
        ),
        (
            "no evidence",
            "propose.key",
            json!({"spec": spec, "evidence": [], "by": "a"}),
            "invalid_argument",
            Some("evidence"),
        ),
        (
            "empty by",
            "propose.key",
            json!({"spec": spec, "evidence": [e], "by": ""}),
            "invalid_argument",
            Some("by"),
        ),
        (
            "unknown column",
            "propose.key",
            json!({"spec": {"kind": "exact", "columns": ["nope"]}, "evidence": [e], "by": "a"}),
            "invalid_spec",
            None,
        ),
        (
            "layout unknown column",
            "propose.layout",
            json!({"cluster_by": ["nope"], "evidence": [e], "by": "a"}),
            "invalid_spec",
            None,
        ),
        (
            "layout with no column",
            "propose.layout",
            json!({"cluster_by": [], "evidence": [e], "by": "a"}),
            "invalid_argument",
            Some("cluster_by"),
        ),
        (
            "empty rewrite invariant",
            "propose.rewrite",
            json!({"shape": {"before": "b", "after": "a", "invariant": " "}, "evidence": [e], "by": "a"}),
            "invalid_argument",
            Some("shape"),
        ),
        (
            "score.verify unknown proposal",
            "score.verify",
            json!({"proposal": absent, "wait": true}),
            "not_found",
            Some("proposal"),
        ),
        (
            "score.verify evidence as proposal",
            "score.verify",
            json!({"proposal": e, "wait": true}),
            "wrong_kind",
            Some("proposal"),
        ),
        (
            "score.verify key without workload",
            "score.verify",
            json!({"proposal": p, "wait": true}),
            "no_workload",
            Some("workload"),
        ),
        (
            "score.verify rewrite without outputs",
            "score.verify",
            json!({"proposal": rw, "wait": true}),
            "no_workload",
            Some("rewrite"),
        ),
        (
            "score.verify rewrite missing outputs",
            "score.verify",
            json!({"proposal": rw, "wait": true, "rewrite": {"before": "/nonexistent/b", "after": "/nonexistent/a", "key": ["id"]}}),
            "invalid_argument",
            Some("rewrite"),
        ),
        (
            "score.verify layout",
            "score.verify",
            json!({"proposal": lp, "wait": true}),
            "layout_not_applicable",
            None,
        ),
        (
            "locate, no index",
            "observe.locate",
            json!({"keys": s(&f.keys)}),
            "not_indexed",
            None,
        ),
        (
            "asof, no index",
            "observe.asof",
            json!({"keys": s(&f.keys), "at": "2026-01-01"}),
            "not_indexed",
            None,
        ),
        (
            "score.why missing key file",
            "score.why",
            json!({"keys": "/nonexistent/k.parquet", "wait": true}),
            "invalid_argument",
            Some("keys"),
        ),
        (
            "score.why unknown workload",
            "score.why",
            json!({"workload": absent, "wait": true}),
            "not_found",
            Some("workload"),
        ),
        (
            "score.why keys and a workload",
            "score.why",
            json!({"keys": s(&f.keys), "workload": absent, "wait": true}),
            "invalid_argument",
            Some("workload"),
        ),
        (
            "score.why no workload",
            "score.why",
            json!({"wait": true}),
            "no_workload",
            None,
        ),
        (
            "observe.profile extra field",
            "observe.profile",
            json!({"score": 1}),
            "invalid_input",
            None,
        ),
        (
            "apply evidence",
            "apply.receipt",
            json!({"certificate": e, "wait": true}),
            "wrong_kind",
            Some("certificate"),
        ),
        (
            "apply unknown certificate",
            "apply.receipt",
            json!({"certificate": absent, "wait": true}),
            "not_found",
            Some("certificate"),
        ),
        (
            "apply unknown certificate, dry run",
            "apply.receipt",
            json!({"certificate": absent, "dry_run": true}),
            "not_found",
            Some("certificate"),
        ),
        (
            "jobs.start unknown tool",
            "jobs.start",
            json!({"tool": "nope", "arguments": {}}),
            "unknown_tool",
            None,
        ),
    ];
    for (label, tool, args, expected, field) in cases {
        let r = srv.call(tool, args);
        refused_as(label, &r, expected);
        if let Some(field) = field {
            let f = refusal(&r);
            assert_eq!(f.fields["field"], field, "{label}: {f:?}");
            assert!(f.next.contains(&format!("`{field}`")), "{label}: {f:?}");
        }
    }

    // Over an index: the key file and `at` are the arguments to fix.
    srv.engine()
        .index(&[bellwether::spec::parse("temporal:id@ts").unwrap()])
        .unwrap();
    let hosts = f.dir.path().join("hosts.txt");
    std::fs::write(&hosts, "not parquet").unwrap();
    for (label, tool, args, expected, field) in [
        (
            "locate missing key file",
            "observe.locate",
            json!({"keys": "/nonexistent/k.parquet"}),
            "invalid_argument",
            "keys",
        ),
        (
            "locate relative key file",
            "observe.locate",
            json!({"keys": "no-such-k.parquet"}),
            "invalid_argument",
            "keys",
        ),
        (
            "locate a file that is not Parquet",
            "observe.locate",
            json!({"keys": s(&hosts)}),
            "invalid_argument",
            "keys",
        ),
        (
            "locate a key column the file lacks",
            "observe.locate",
            json!({"keys": s(&f.keys), "key_columns": ["nope"]}),
            "invalid_argument",
            "key_columns",
        ),
        (
            "asof an instant that is not one",
            "observe.asof",
            json!({"keys": s(&f.keys), "at": "yesterday"}),
            "invalid_argument",
            "at",
        ),
    ] {
        let r = srv.call(tool, args);
        refused_as(label, &r, expected);
        assert_eq!(refusal(&r).fields["field"], field, "{label}");
    }
    // An index without a temporal spec names the spec asof needs.
    let g = folder();
    let other = server(&g.data);
    other
        .engine()
        .index(&[bellwether::spec::parse("exact:id").unwrap()])
        .unwrap();
    let r = other.call(
        "observe.asof",
        json!({"keys": s(&g.keys), "at": "2026-01-01"}),
    );
    refused_as("asof without a temporal spec", &r, "not_indexed");
    assert!(refusal(&r).fields["needs"]
        .as_str()
        .unwrap()
        .starts_with("temporal:id@"));
}

/// Dry runs write nothing anywhere: the whole temporary tree is unchanged,
/// and an index location that does not exist yet is never created.
#[test]
fn dry_runs_never_create_an_absent_index_location() {
    for clock in CLOCKS {
        let f = folder();
        let srv = server_at(&f.data, Budget::default(), clock);
        let e = ev(&srv);
        let spec = json!({"kind": "exact", "columns": ["id"]});
        let before = objects(f.dir.path());
        ok(
            &srv,
            "propose.key",
            json!({"spec": spec, "evidence": [e], "by": "a", "dry_run": true}),
        );
        assert_eq!(objects(f.dir.path()), before, "[clock={clock:?}]");

        let g = folder();
        let idx = g.dir.path().join("elsewhere").join("idx");
        let elsewhere = Server::new(&bellwether_mcp::Config {
            root: s(&g.data).to_string(),
            engine: bellwether::EngineOptions {
                index: Some(s(&idx).to_string()),
                clock: clock.map(str::to_string),
                ..bellwether::EngineOptions::default()
            },
            budget: Budget::default(),
        })
        .unwrap();
        let before = objects(g.dir.path());
        for (tool, args, expected) in [
            (
                "propose.key",
                json!({"spec": spec, "evidence": [e], "by": "a", "dry_run": true}),
                "not_found",
            ),
            (
                "apply.receipt",
                json!({"certificate": e, "dry_run": true}),
                "not_found",
            ),
        ] {
            let r = elsewhere.call(tool, args);
            refused_as(tool, &r, expected);
        }
        ok(&elsewhere, "observe.profile", json!({}));
        assert!(
            !idx.exists(),
            "[clock={clock:?}] a dry run created the index"
        );
        assert_eq!(objects(g.dir.path()), before, "[clock={clock:?}]");
    }
}

/// A certificate for a key that is already built: the apply dry run and
/// the real apply both refuse with `nothing_to_apply`, and neither writes,
/// under a fixed clock and under the system clock.
#[test]
fn apply_dry_run_and_apply_agree_on_a_built_key_and_write_nothing() {
    for clock in CLOCKS {
        let f = folder();
        let srv = server_at(&f.data, Budget::default(), clock);
        let e = ev(&srv);
        let sc = ok(&srv, "score.why", json!({"keys": s(&f.keys), "wait": true}));
        let wl = json(&sc)["workload"]["hash"].as_str().unwrap().to_string();
        srv.engine()
            .index(&[bellwether::spec::parse("exact:id").unwrap()])
            .unwrap();
        let p = json(&ok(
            &srv,
            "propose.key",
            json!({"spec": {"kind": "exact", "columns": ["id"]}, "evidence": [e], "by": "a"}),
        ))["proposal"]["hash"]
            .as_str()
            .unwrap()
            .to_string();
        let v = ok(
            &srv,
            "score.verify",
            json!({"proposal": p, "workload": wl, "wait": true}),
        );
        let cert = json(&v)["certificate"]["hash"]
            .as_str()
            .unwrap()
            .to_string();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let index = f.data.join(".bellwether");
        let before = objects(&index);
        let spent = srv.budget().spent.replays;
        let dry = srv.call(
            "apply.receipt",
            json!({"certificate": cert, "dry_run": true}),
        );
        refused_as("apply dry run", &dry, "nothing_to_apply");
        let real = srv.call("apply.receipt", json!({"certificate": cert, "wait": true}));
        refused_as("apply", &real, "nothing_to_apply");
        assert_eq!(refusal(&dry).fields, refusal(&real).fields);
        let (kinds, other) = delta(&before, &objects(&index));
        assert!(
            kinds.is_empty() && other.is_empty(),
            "[clock={clock:?}] a refused apply wrote {kinds:?} {other:?}"
        );
        assert_eq!(srv.budget().spent.replays, spent, "no replay was charged");
    }
}

/// The in-band apply refusals through the tool: a passing rewrite
/// certificate is `no_replay`, a key certificate whose data changed after
/// verification is `data_changed`, and a layout proposal is
/// `layout_not_applicable`. The applies write nothing, dry run or not,
/// under a fixed clock and the system clock.
#[test]
fn in_band_apply_refusals_through_the_tool_write_nothing() {
    for clock in CLOCKS {
        let f = folder();
        let srv = server_at(&f.data, Budget::default(), clock);
        let e = ev(&srv);
        let (b, a) = (f.dir.path().join("before"), f.dir.path().join("after"));
        ledger_corpus(&b, 1, 1, 10, 10);
        ledger_corpus(&a, 1, 1, 10, 10);
        let rw = json(&ok(
            &srv,
            "propose.rewrite",
            json!({"shape": {"before": "b", "after": "a", "invariant": "same"}, "evidence": [e], "by": "x"}),
        ))["proposal"]["hash"]
            .as_str()
            .unwrap()
            .to_string();
        let v = ok(
            &srv,
            "score.verify",
            json!({"proposal": rw, "wait": true, "rewrite": {"before": s(&b), "after": s(&a), "key": ["id"]}}),
        );
        let cert_rw = json(&v)["certificate"]["hash"]
            .as_str()
            .unwrap()
            .to_string();
        let sc = json(&ok(
            &srv,
            "score.why",
            json!({"keys": s(&f.keys), "wait": true}),
        ));
        let wl = sc["workload"]["hash"].as_str().unwrap().to_string();
        let cand = sc["candidates"][0]["proposal"]["hash"]
            .as_str()
            .unwrap()
            .to_string();
        let v = ok(
            &srv,
            "score.verify",
            json!({"proposal": cand, "workload": wl, "wait": true}),
        );
        let cert_key = json(&v)["certificate"]["hash"]
            .as_str()
            .unwrap()
            .to_string();
        write_file(
            &f.data.join("part-999.parquet"),
            &[vec![(1, 1, Some(1), "late".into())]],
        );
        let before = objects(f.dir.path());
        for dry in [true, false] {
            let r = srv.call(
                "apply.receipt",
                json!({"certificate": cert_rw, "dry_run": dry, "wait": true}),
            );
            refused_as("apply a passing rewrite", &r, "no_replay");
            assert_eq!(refusal(&r).fields["certificate"], cert_rw.as_str());
            let r = srv.call(
                "apply.receipt",
                json!({"certificate": cert_key, "dry_run": dry, "wait": true}),
            );
            refused_as("apply after the data changed", &r, "data_changed");
            assert_eq!(refusal(&r).fields["certificate"], cert_key.as_str());
        }
        assert_eq!(objects(f.dir.path()), before, "[clock={clock:?}]");
        let lp = json(&ok(
            &srv,
            "propose.layout",
            json!({"cluster_by": ["batch"], "evidence": [e], "by": "a"}),
        ))["proposal"]["hash"]
            .as_str()
            .unwrap()
            .to_string();
        let r = srv.call("score.verify", json!({"proposal": lp, "wait": true}));
        refused_as("verify a layout", &r, "layout_not_applicable");
    }
}

/// An agent that follows the `stale_manifest` next line literally ("call
/// score.verify again on the proposal, then apply.receipt with the new
/// certificate"), with the same arguments and as jobs, ends with a receipt:
/// the commit changed the state, so the repeated `score.verify` is a new
/// job with a new certificate, not the stored stale one.
#[test]
fn following_the_stale_manifest_next_line_ends_in_a_receipt() {
    for clock in CLOCKS {
        let f = folder();
        let srv = server_at(&f.data, Budget::default(), clock);
        let sc = json(&ok(
            &srv,
            "score.why",
            json!({"keys": s(&f.keys), "wait": true}),
        ));
        let wl = sc["workload"]["hash"].as_str().unwrap().to_string();
        let cand = sc["candidates"][0]["proposal"]["hash"]
            .as_str()
            .unwrap()
            .to_string();
        let verify = json!({"proposal": cand, "workload": wl});
        let job = |args: Value, tool: &str| {
            let r = srv.call(tool, args);
            assert_eq!(r.envelope.status, Status::Running, "{tool} is a job");
            finish(&srv, &r.envelope.job.unwrap().id)
        };
        let first = job(verify.clone(), "score.verify");
        let stale = json(&first)["certificate"]["hash"]
            .as_str()
            .unwrap()
            .to_string();
        srv.engine()
            .index(&[bellwether::spec::parse("exact:payload").unwrap()])
            .unwrap();
        let r = job(json!({"certificate": stale}), "apply.receipt");
        refused_as("apply a stale certificate", &r, "stale_manifest");
        let next = &refusal(&r).next;
        assert!(
            next.contains("score.verify") && next.contains("apply.receipt"),
            "{next}"
        );
        // Exactly as the next line says: the same score.verify call again.
        let again = job(verify, "score.verify");
        let fresh = json(&again)["certificate"]["hash"]
            .as_str()
            .unwrap()
            .to_string();
        assert_ne!(
            fresh, stale,
            "[clock={clock:?}] the stale certificate came back"
        );
        let applied = job(json!({"certificate": fresh}), "apply.receipt");
        assert!(!applied.is_error(), "{}", applied.text);
        let v = json(&applied);
        assert_eq!(v["outcome"]["kind"], "applied", "[clock={clock:?}]");
        assert!(v["outcome"]["receipt"]["hash"].is_string());
    }
}

/// The same for `data_changed`: a file lands without a commit, so the
/// manifest sequence is unchanged and only the data's catalog digest tells
/// the retry apart. Following the next line literally (the same
/// `score.verify` again, as a job) gives a new job and a new certificate,
/// and the apply ends in a receipt.
#[test]
fn following_the_data_changed_next_line_ends_in_a_receipt() {
    for clock in CLOCKS {
        let f = folder();
        let srv = server_at(&f.data, Budget::default(), clock);
        let sc = json(&ok(
            &srv,
            "score.why",
            json!({"keys": s(&f.keys), "wait": true}),
        ));
        let wl = sc["workload"]["hash"].as_str().unwrap().to_string();
        let cand = sc["candidates"][0]["proposal"]["hash"]
            .as_str()
            .unwrap()
            .to_string();
        let verify = json!({"proposal": cand, "workload": wl});
        let job = |args: Value, tool: &str| {
            let r = srv.call(tool, args);
            assert_eq!(r.envelope.status, Status::Running, "{tool} is a job");
            let id = r.envelope.job.unwrap().id;
            (id.clone(), finish(&srv, &id))
        };
        let (first_id, first) = job(verify.clone(), "score.verify");
        let stale = json(&first)["certificate"]["hash"]
            .as_str()
            .unwrap()
            .to_string();
        let sequence = srv.engine().state().unwrap().sequence;
        write_file(
            &f.data.join("part-777.parquet"),
            &[vec![(5, 1, Some(1), "late".into())]],
        );
        assert_eq!(
            srv.engine().state().unwrap().sequence,
            sequence,
            "no commit"
        );
        let (_, r) = job(json!({"certificate": stale}), "apply.receipt");
        refused_as("apply after a file landed", &r, "data_changed");
        let next = &refusal(&r).next;
        assert!(
            next.contains("score.verify") && next.contains("apply.receipt"),
            "{next}"
        );
        // Exactly as the next line says: the same score.verify call again.
        let (again_id, again) = job(verify, "score.verify");
        assert_ne!(
            again_id, first_id,
            "[clock={clock:?}] the stored job came back"
        );
        let fresh = json(&again)["certificate"]["hash"]
            .as_str()
            .unwrap()
            .to_string();
        assert_ne!(
            fresh, stale,
            "[clock={clock:?}] the stale certificate came back"
        );
        let (_, applied) = job(json!({"certificate": fresh}), "apply.receipt");
        assert!(!applied.is_error(), "{}", applied.text);
        let v = json(&applied);
        assert_eq!(v["outcome"]["kind"], "applied", "[clock={clock:?}]");
        assert!(v["outcome"]["receipt"]["hash"].is_string());
    }
}

/// `invalid_spec` for a column the dataset lacks carries the column and the
/// dataset's columns, from `observe.why` and the propose tools alike.
#[test]
fn invalid_spec_names_the_column_and_the_columns() {
    let f = folder();
    let srv = server(&f.data);
    let e = ev(&srv);
    for (tool, args) in [
        ("observe.why", json!({"columns": ["nope"]})),
        (
            "propose.key",
            json!({"spec": {"kind": "exact", "columns": ["nope"]}, "evidence": [e], "by": "a"}),
        ),
        (
            "propose.layout",
            json!({"cluster_by": ["nope"], "evidence": [e], "by": "a"}),
        ),
    ] {
        let r = srv.call(tool, args);
        refused_as(tool, &r, "invalid_spec");
        let fields = &refusal(&r).fields;
        assert_eq!(fields["column"], "nope", "{tool}");
        assert_eq!(
            fields["columns"],
            json!(["id", "batch", "ts", "payload"]),
            "{tool}"
        );
    }
}

/// Every object schema in every tool input is closed, and a score smuggled
/// through `jobs.start` or into any key-spec variant is `invalid_input`
/// and writes nothing.
#[test]
fn schemas_are_closed_everywhere() {
    fn open(v: &Value, path: &str, out: &mut Vec<String>) {
        match v {
            Value::Object(m) => {
                let is_obj =
                    m.get("type") == Some(&Value::from("object")) || m.contains_key("properties");
                if is_obj && m.get("additionalProperties") != Some(&Value::Bool(false)) {
                    out.push(path.to_string());
                }
                for (k, x) in m {
                    open(x, &format!("{path}/{k}"), out);
                }
            }
            Value::Array(a) => {
                for (i, x) in a.iter().enumerate() {
                    open(x, &format!("{path}/{i}"), out);
                }
            }
            _ => {}
        }
    }
    for t in bellwether_mcp::tools::catalog() {
        let mut out = Vec::new();
        open(&t.input, "", &mut out);
        // `jobs.start.arguments` is open by design: it is checked against
        // the target tool's own closed schema.
        out.retain(|p| !(t.name == "jobs.start" && p == "/properties/arguments"));
        assert!(out.is_empty(), "{}: open schemas at {out:?}", t.name);
    }
    for clock in CLOCKS {
        let f = folder();
        let srv = server_at(&f.data, Budget::default(), clock);
        let e = ev(&srv);
        let index = f.data.join(".bellwether");
        let before = objects(&index);
        let r = srv.call(
            "jobs.start",
            json!({"tool": "propose.key", "arguments": {"spec": {"kind": "exact", "columns": ["id"], "score": 9}, "evidence": [e], "by": "a"}}),
        );
        refused_as("jobs.start nested score", &r, "invalid_input");
        for spec in [
            json!({"kind": "bloom", "columns": ["id"], "fpr": 0.01, "score": 1}),
            json!({"kind": "temporal", "columns": ["id"], "ts_column": "ts", "score": 1}),
            json!({"kind": "range", "column": "ts", "score": 1}),
        ] {
            let r = srv.call(
                "propose.key",
                json!({"spec": spec, "evidence": [e], "by": "a"}),
            );
            refused_as("variant score", &r, "invalid_input");
        }
        assert_eq!(objects(&index), before, "[clock={clock:?}]");
    }
}

/// Unknown and malformed job ids are `job_not_found`; starting the same
/// request returns the same job, running or finished.
#[test]
fn job_ids_resolve_or_are_not_found() {
    let f = folder();
    let srv = server(&f.data);
    for tool in ["jobs.status", "jobs.result"] {
        for id in ["00".repeat(32), "nope".to_string(), String::new()] {
            let r = srv.call(tool, json!({"job": id}));
            refused_as(tool, &r, "job_not_found");
        }
    }
    let start = || {
        srv.call(
            "jobs.start",
            json!({"tool": "score.why", "arguments": {"keys": s(&f.keys)}}),
        )
    };
    let a = start();
    let b = start();
    let id = a.envelope.job.unwrap().id;
    assert_eq!(b.envelope.job.unwrap().id, id);
    finish(&srv, &id);
    let c = start().envelope.job.unwrap();
    assert_eq!((c.id.as_str(), c.state), (id.as_str(), JobState::Done));
}

/// A job id always names one result: calling the tool again directly with
/// a finished job's request returns the stored result and runs nothing.
/// Under the system clock a rerun would write records with a new time, so
/// equal text proves there was none.
#[test]
fn a_finished_job_is_returned_not_rerun() {
    let f = folder();
    let srv = server_at(&f.data, Budget::default(), None);
    let args = json!({"keys": s(&f.keys)});
    let id = srv.call("score.why", args.clone()).envelope.job.unwrap().id;
    let first = finish(&srv, &id);
    assert_eq!(first.envelope.status, Status::Ok);
    let spent = srv.budget().spent;
    std::thread::sleep(std::time::Duration::from_millis(5));
    let index = f.data.join(".bellwether");
    let before = objects(&index);
    let again = srv.call("score.why", args);
    assert_eq!(again.envelope.status, Status::Ok);
    assert_eq!(again.text, first.text);
    assert_eq!(again.envelope.job.as_ref().unwrap().id, id);
    assert_eq!(finish(&srv, &id).text, first.text);
    assert_eq!(objects(&index), before, "the request ran again");
    assert_eq!(srv.budget().spent.replays, spent.replays);
    // `wait: true` runs it again now, outside the job.
    let fresh = ok(&srv, "score.why", json!({"keys": s(&f.keys), "wait": true}));
    assert!(fresh.envelope.job.is_none());
    assert_eq!(finish(&srv, &id).text, first.text, "the job is unchanged");
}
