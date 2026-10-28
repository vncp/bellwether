//! Join keys (B3) over MCP: the server reads only the right dataset its
//! operator named with `--join`. A join key naming any other right side is
//! refused with `join_not_served` by `propose.key`, `score.verify`,
//! `apply.receipt`, and `score.why` (a workload's recorded semi-join runs)
//! before any folder is read, so the refusal is the same whether a folder
//! of that name exists and is indexed, exists unindexed, or is missing,
//! and nothing is written (review of PR #32, item 3).

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::path::{Path, PathBuf};

use bellwether::types::Hash;
use bellwether::{Engine, EngineOptions, FixOutcome, FixRequest, KeyFile, LookupMode, WhyRequest};
use bellwether_mcp::refusal::Refusal;
use bellwether_mcp::{Budget, Config, Server};
use common::{objects, scattered_corpus, write_keys, CLOCK};
use serde_json::{json, Value};

struct Lake {
    _dir: tempfile::TempDir,
    root: PathBuf,
    orders: PathBuf,
    keys: PathBuf,
}

fn s(p: &Path) -> &str {
    p.to_str().unwrap()
}

fn engine(root: &Path) -> Engine {
    Engine::open(
        s(root),
        EngineOptions {
            clock: Some(CLOCK.into()),
            ..EngineOptions::default()
        },
    )
    .unwrap()
}

fn exact_id() -> bellwether::types::KeySpec {
    bellwether::spec::parse("exact:id").unwrap()
}

/// `orders` (ids 0..400 scattered), and beside it `payables` (the right
/// side the server serves) and `secret` (another indexed folder), each of
/// ids 0..41 with `exact:id`.
fn lake() -> Lake {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let orders = root.join("orders");
    scattered_corpus(&orders, 3, 4, 25, 400);
    for right in ["payables", "secret"] {
        scattered_corpus(&root.join(right), 2, 4, 25, 41);
        engine(&root.join(right)).index(&[exact_id()]).unwrap();
    }
    engine(&orders).index(&[exact_id()]).unwrap();
    let keys = root.join("k.parquet");
    write_keys(&keys, "id", &[5, 7, 77, 123, 300]);
    Lake {
        _dir: dir,
        root,
        orders,
        keys,
    }
}

fn server(data: &Path, join: Option<&str>) -> Server {
    Server::new(&Config {
        root: s(data).to_string(),
        engine: EngineOptions {
            clock: Some(CLOCK.into()),
            ..EngineOptions::default()
        },
        budget: Budget::default(),
        join: join.map(str::to_string),
        workspace: false,
    })
    .unwrap()
}

fn join_spec(right: &str) -> Value {
    json!({"kind": "join", "dataset": "orders", "columns": ["id"], "right": right,
           "right_columns": ["id"]})
}

/// The records a human's CLI wrote naming `secret` as the right side: the
/// profile Evidence, a semi-join workload, the join key's proposal, and a
/// passing certificate of it.
struct Written {
    evidence: Hash,
    workload: Hash,
    proposal: Hash,
    certificate: Hash,
}

fn written_by_the_human(l: &Lake) -> Written {
    let left = engine(&l.orders);
    let w = left
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&l.keys)),
            mode: Some(LookupMode::SemiJoin),
            join: Some("secret.id".into()),
            ..WhyRequest::default()
        })
        .unwrap();
    let rec = w.recommended.as_ref().expect("the join key is recommended");
    assert_eq!(rec.spec.name(), "join:orders.id=secret.id");
    let workload = w.workload.as_ref().unwrap().hash;
    let fixed = left
        .fix(&FixRequest {
            proposal: Some(rec.proposal),
            workload: Some(workload),
            verify_only: true,
            ..FixRequest::default()
        })
        .unwrap();
    assert!(
        matches!(fixed.outcome, FixOutcome::Verified),
        "{:?}",
        fixed.outcome
    );
    Written {
        evidence: w.evidence.hash.unwrap(),
        workload,
        proposal: rec.proposal,
        certificate: fixed.certificate.unwrap().hash,
    }
}

/// Every tool that takes a join key naming `right`, or a record naming
/// `secret`, called once each: the refusals, in order.
fn refusals(srv: &Server, right: &str, w: &Written) -> Vec<Refusal> {
    let calls = [
        (
            "propose.key",
            json!({"spec": join_spec(right), "evidence": [w.evidence.to_hex()], "by": "agent",
                   "dry_run": true}),
        ),
        (
            "propose.key",
            json!({"spec": join_spec(right), "evidence": [w.evidence.to_hex()], "by": "agent"}),
        ),
        (
            "score.verify",
            json!({"proposal": w.proposal.to_hex(), "workload": w.workload.to_hex(),
                   "wait": true}),
        ),
        (
            "apply.receipt",
            json!({"certificate": w.certificate.to_hex(), "dry_run": true}),
        ),
        (
            "apply.receipt",
            json!({"certificate": w.certificate.to_hex(), "wait": true}),
        ),
        (
            "score.why",
            json!({"workload": w.workload.to_hex(), "wait": true}),
        ),
    ];
    calls
        .into_iter()
        .map(|(tool, args)| {
            let r = srv.call(tool, args.clone());
            let refusal = r
                .envelope
                .refusal
                .clone()
                .unwrap_or_else(|| panic!("{tool} {args} was not refused: {}", r.text));
            assert_eq!(
                refusal.code.as_str(),
                "join_not_served",
                "{tool}: {refusal:?}"
            );
            refusal
        })
        .collect()
}

#[test]
fn every_tool_refuses_a_right_side_other_than_join_identically_whatever_the_folder_is() {
    let l = lake();
    let w = written_by_the_human(&l);
    let srv = server(&l.orders, Some("payables.id"));
    let index = l.orders.join(".bellwether");
    let before = objects(&index);
    // 1. `secret` exists and is indexed.
    let indexed = refusals(&srv, "secret", &w);
    // 2. `secret` exists, its index deleted.
    std::fs::remove_dir_all(l.root.join("secret").join(".bellwether")).unwrap();
    let unindexed = refusals(&srv, "secret", &w);
    // 3. `secret` is gone.
    std::fs::rename(l.root.join("secret"), l.root.join("moved")).unwrap();
    let missing = refusals(&srv, "secret", &w);
    // A name that was never a folder.
    let never = refusals(&srv, "nosuch", &w);
    assert_eq!(indexed, unindexed, "an existing and an unindexed folder");
    assert_eq!(indexed, missing, "an existing and a missing folder");
    assert_eq!(indexed, never, "any other name");
    for r in &indexed {
        // It names the side served, never the folder asked or any path.
        assert_eq!(r.fields, json!({"served": "payables.id"}));
        assert!(r.message.contains("--join payables.id"), "{}", r.message);
        for leak in ["secret", "nosuch", s(&l.root), "no such", "not indexed"] {
            assert!(!r.message.contains(leak), "{leak}: {}", r.message);
        }
        assert!(!r.next.contains("--"), "{}", r.next);
    }
    assert_eq!(objects(&index), before, "a refusal writes nothing");
}

#[test]
fn without_join_every_join_key_is_refused_and_the_served_side_is_not() {
    let l = lake();
    let w = written_by_the_human(&l);
    // No --join: any right side, the would-be served one too.
    let srv = server(&l.orders, None);
    for right in ["payables", "secret"] {
        for r in refusals(&srv, right, &w) {
            assert_eq!(r.fields, json!({"served": null}));
            assert!(r.message.contains("without --join"), "{}", r.message);
        }
    }
    // With --join payables.id: other columns of the served dataset are
    // another right side; the served side itself is proposed.
    let srv = server(&l.orders, Some("payables.id"));
    let other_columns = json!({"kind": "join", "dataset": "orders", "columns": ["batch"],
        "right": "payables", "right_columns": ["batch"]});
    let r = srv.call(
        "propose.key",
        json!({"spec": other_columns, "evidence": [w.evidence.to_hex()], "by": "agent",
               "dry_run": true}),
    );
    assert_eq!(
        r.envelope.refusal.as_ref().map(|r| r.code.as_str()),
        Some("join_not_served")
    );
    let r = srv.call(
        "propose.key",
        json!({"spec": join_spec("payables"), "evidence": [w.evidence.to_hex()],
               "by": "agent", "dry_run": true}),
    );
    assert!(!r.is_error(), "{}", r.text);
    let v: Value = serde_json::from_str(&r.text).unwrap();
    assert_eq!(v["outcome"], "would_write");
}
