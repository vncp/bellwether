//! Threat model, deliverable 7: arbitrary JSON tool inputs never panic, and
//! never write anything beyond what the tool's tier allows. The store's
//! object list (read straight from the index directory) is compared before
//! and after every call.
//!
//! Per tier (`docs/threat-model.md`): a propose tool writes at most one
//! Proposal and nothing else; an observe tool at most Evidence and cache
//! entries; apply with an arbitrary hash nothing at all; a score tool at
//! most engine Proposals, Replays, Evidence, Certificates, and cache
//! entries; jobs whatever the tool they start may. Inputs are drawn both
//! fully at random and as mutations of valid arguments (an extra field, a
//! dropped field, a replaced value), so most cases reach the facade.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::match_same_arms,
    clippy::needless_pass_by_value
)]

mod common;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::OnceLock;

use bellwether_mcp::{Server, Status};
use common::{delta, finish, json, ledger_corpus, objects, ok, server, write_keys};
use proptest::prelude::*;
use serde_json::{json, Map, Value};

struct World {
    _dir: tempfile::TempDir,
    data: PathBuf,
    index: PathBuf,
    server: Server,
    /// Valid arguments per tool, to mutate.
    valid: BTreeMap<&'static str, Value>,
}

fn world() -> &'static World {
    static W: OnceLock<World> = OnceLock::new();
    W.get_or_init(|| {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        ledger_corpus(&data, 2, 2, 20, 100);
        let keys = dir.path().join("k.parquet");
        write_keys(&keys, "id", &[3, 41]);
        let server = server(&data);
        let ev = json(&ok(&server, "observe.why", json!({})))["evidence"]["hash"]
            .as_str()
            .unwrap()
            .to_string();
        let k = keys.to_str().unwrap().to_string();
        let mut valid = BTreeMap::new();
        valid.insert("observe.why", json!({"columns": ["id"]}));
        valid.insert("observe.profile", json!({}));
        valid.insert("observe.locate", json!({"keys": k}));
        valid.insert("observe.asof", json!({"keys": k, "at": "1970-01-01T00:00:10Z"}));
        valid.insert(
            "propose.key",
            json!({"spec": {"kind": "exact", "columns": ["id"]}, "evidence": [ev], "by": "p"}),
        );
        valid.insert(
            "propose.layout",
            json!({"cluster_by": ["batch"], "evidence": [ev], "by": "p"}),
        );
        valid.insert(
            "propose.rewrite",
            json!({"shape": {"before": "b", "after": "a", "invariant": "i"}, "evidence": [ev], "by": "p"}),
        );
        valid.insert("apply.receipt", json!({"certificate": ev, "wait": true}));
        valid.insert("score.why", json!({"keys": k, "max_candidates": 1, "wait": true}));
        valid.insert(
            "score.verify",
            json!({"proposal": ev, "workload": ev, "wait": true}),
        );
        valid.insert("jobs.status", json!({"job": "0".repeat(64)}));
        valid.insert("jobs.result", json!({"job": "0".repeat(64)}));
        valid.insert(
            "jobs.start",
            json!({"tool": "observe.profile", "arguments": {}}),
        );
        World {
            index: data.join(".bellwether"),
            data,
            _dir: dir,
            server,
            valid,
        }
    })
}

fn arb_json() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        any::<f64>()
            .prop_filter("finite", |f| f.is_finite())
            .prop_map(Value::from),
        "[a-z_]{0,8}".prop_map(Value::from),
        "[0-9a-f]{64}".prop_map(Value::from),
        Just(Value::from("engine")),
        Just(Value::from("../../etc/passwd")),
        Just(Value::from("rm -rf /; select 1")),
    ];
    leaf.prop_recursive(3, 24, 6, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Value::from),
            prop::collection::btree_map(
                prop_oneof![
                    "[a-z_]{1,8}".prop_map(String::from),
                    Just("score".to_string()),
                    Just("kind".to_string()),
                    Just("columns".to_string()),
                    Just("dry_run".to_string()),
                ],
                inner,
                0..4
            )
            .prop_map(|m| Value::Object(m.into_iter().collect::<Map<_, _>>())),
        ]
    })
}

const TOOLS: [&str; 13] = [
    "observe.why",
    "observe.profile",
    "observe.locate",
    "observe.asof",
    "score.why",
    "score.verify",
    "propose.key",
    "propose.layout",
    "propose.rewrite",
    "apply.receipt",
    "jobs.start",
    "jobs.status",
    "jobs.result",
];

/// A mutation of valid arguments: add a field, drop a field, or replace a
/// field's value (at the top level or one level down).
fn mutate(valid: &Value, op: u8, key: &str, value: Value, pick: usize) -> Value {
    let mut v = valid.clone();
    let Value::Object(m) = &mut v else { return v };
    let keys: Vec<String> = m.keys().cloned().collect();
    match op % 4 {
        0 => {
            m.insert(key.to_string(), value);
        }
        1 if !keys.is_empty() => {
            m.remove(&keys[pick % keys.len()]);
        }
        2 if !keys.is_empty() => {
            m.insert(keys[pick % keys.len()].clone(), value);
        }
        _ => {
            if let Some(Value::Object(inner)) = keys
                .get(pick % keys.len().max(1))
                .and_then(|k| m.get_mut(k))
            {
                inner.insert(key.to_string(), value);
            }
        }
    }
    v
}

/// The ledger kinds and other paths a tool may add.
fn allowed(tool: &str, kinds: &[String], other: &[String]) -> Result<(), String> {
    let only = |ok: &[&str]| kinds.iter().all(|k| ok.contains(&k.as_str()));
    let cache_only = other.iter().all(|p| p.starts_with("cache/"));
    let fine = match tool {
        "propose.key" | "propose.layout" | "propose.rewrite" => {
            kinds.len() <= 1 && only(&["proposal"]) && other.is_empty()
        }
        "observe.why" | "observe.profile" | "observe.locate" | "observe.asof" => {
            only(&["evidence"]) && cache_only
        }
        "apply.receipt" | "jobs.status" | "jobs.result" => kinds.is_empty() && other.is_empty(),
        "score.why" | "score.verify" | "jobs.start" => {
            only(&["evidence", "proposal", "replay", "certificate"]) && cache_only
        }
        _ => kinds.is_empty() && other.is_empty(),
    };
    if fine {
        Ok(())
    } else {
        Err(format!("{tool} wrote {kinds:?} and {other:?}"))
    }
}

/// The lock every call over the shared folder takes.
fn one() -> std::sync::MutexGuard<'static, ()> {
    static ONE: std::sync::Mutex<()> = std::sync::Mutex::new(());
    ONE.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn check(tool: &str, args: Value) -> Result<(), TestCaseError> {
    // One call at a time over the shared folder, so the object lists
    // compared belong to this call alone.
    let _one = one();
    let w = world();
    let before = objects(&w.index);
    let reply = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        w.server.call(tool, args.clone())
    }))
    .map_err(|_| TestCaseError::fail(format!("{tool} panicked on {args}")))?;
    if reply.envelope.status == Status::Running {
        let id = reply.envelope.job.as_ref().unwrap().id.clone();
        finish(&w.server, &id);
    }
    // Every refusal carries a code and a next line.
    if let Some(r) = &reply.envelope.refusal {
        prop_assert!(!r.next.is_empty());
        prop_assert!(!r.message.is_empty());
    }
    let (kinds, other) = delta(&before, &objects(&w.index));
    allowed(tool, &kinds, &other).map_err(TestCaseError::fail)?;
    let _ = &w.data;
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 96, ..ProptestConfig::default() })]

    #[test]
    fn arbitrary_inputs_never_panic_or_write_out_of_tier(
        t in 0usize..TOOLS.len(),
        args in arb_json(),
    ) {
        check(TOOLS[t], args)?;
    }

    #[test]
    fn mutated_valid_inputs_never_panic_or_write_out_of_tier(
        t in 0usize..TOOLS.len(),
        op in any::<u8>(),
        key in prop_oneof![Just("score".to_string()), "[a-z_]{1,6}".prop_map(String::from)],
        value in arb_json(),
        pick in any::<usize>(),
    ) {
        let tool = TOOLS[t];
        let args = mutate(&world().valid[tool], op, &key, value, pick);
        check(tool, args)?;
    }
}

/// The unmutated valid arguments pass, so the mutations start from inputs
/// that reach the facade.
#[test]
fn the_valid_arguments_reach_the_facade() {
    let _one = one();
    let w = world();
    for tool in TOOLS {
        let r = w.server.call(tool, w.valid[tool].clone());
        if let Some(r) = &r.envelope.refusal {
            assert_ne!(r.code.as_str(), "invalid_input", "{tool}: {}", r.message);
        }
    }
}
