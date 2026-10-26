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
//!
//! Signatures (ADR 029): without `--signing-key` no tool writes a
//! `.sig.` object; with one, only `apply.receipt` and `score.verify` (or a
//! job running them) may, and over the loop they sign exactly what they
//! wrote.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::match_same_arms,
    clippy::needless_pass_by_value
)]

mod common;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::LazyLock;

use bellwether_mcp::{Server, Status};
use common::{
    delta, finish, json, ledger_corpus, objects, ok, scattered_corpus, server, write_keys,
};
use proptest::prelude::*;
use serde_json::{json, Map, Value};

struct World {
    _dir: tempfile::TempDir,
    data: PathBuf,
    index: PathBuf,
    server: Server,
    /// Whether the server has a signing key.
    signing: bool,
    /// Valid arguments per tool, to mutate.
    valid: BTreeMap<&'static str, Value>,
}

static WORLD: LazyLock<World> = LazyLock::new(|| make_world(false));
/// The same world, served with a signing key.
static KEYED_WORLD: LazyLock<World> = LazyLock::new(|| make_world(true));

fn world() -> &'static World {
    &WORLD
}

fn keyed_world() -> &'static World {
    &KEYED_WORLD
}

/// A server over `data` under the fixed clock, with a signing key whose
/// seed lives in `dir` (outside the data folder and the index) when
/// `signing`, and trusting it.
fn signing_server(data: &std::path::Path, dir: &std::path::Path, signing: bool) -> Server {
    let mut engine = bellwether::EngineOptions {
        clock: Some(common::CLOCK.into()),
        ..bellwether::EngineOptions::default()
    };
    if signing {
        let path = dir.join("signer");
        if !path.exists() {
            bellwether::generate_key(&path, Some("ci"), None).unwrap();
            bellwether::trust_key(
                &dir.join("trusted.toml"),
                &bellwether::public_path(&path),
                None,
                None,
            )
            .unwrap();
        }
        // Through the server's own flags, as `--signing-key` and
        // `--trusted-keys` load them.
        bellwether_mcp::KeyArgs {
            signing_key: Some(path),
            trusted_keys: Some(dir.join("trusted.toml")),
        }
        .apply(data.to_str().unwrap(), &mut engine)
        .unwrap();
    }
    Server::new(&bellwether_mcp::Config {
        root: data.to_str().unwrap().to_string(),
        engine,
        budget: bellwether_mcp::Budget::default(),
    })
    .unwrap()
}

fn make_world(signing: bool) -> World {
    {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        ledger_corpus(&data, 2, 2, 20, 100);
        let keys = dir.path().join("k.parquet");
        write_keys(&keys, "id", &[3, 41]);
        let server = if signing {
            signing_server(&data, dir.path(), true)
        } else {
            server(&data)
        };
        let ev = json(&ok(&server, "observe.why", json!({})))["evidence"]["hash"]
            .as_str()
            .unwrap()
            .to_string();
        let k = keys.to_str().unwrap().to_string();
        let mut valid = BTreeMap::new();
        valid.insert("observe.why", json!({"columns": ["id"]}));
        valid.insert("observe.profile", json!({}));
        valid.insert(
            "observe.cardinality",
            json!({"items": ["id"], "transient": true}),
        );
        valid.insert("observe.overlap", json!({"left": "id", "right": "batch"}));
        valid.insert("observe.locate", json!({"keys": k}));
        valid.insert(
            "observe.asof",
            json!({"keys": k, "at": "1970-01-01T00:00:10Z"}),
        );
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
        valid.insert(
            "score.why",
            json!({"keys": k, "max_candidates": 1, "wait": true}),
        );
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
            signing,
            valid,
        }
    }
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

const TOOLS: [&str; 15] = [
    "observe.why",
    "observe.cardinality",
    "observe.overlap",
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

/// Whether a path is a signature object (or the local store's lock file
/// beside one).
fn is_signature(p: &str) -> bool {
    p.starts_with("ledger/") && p.contains(".sig.")
}

/// The ledger kinds and other paths a tool may add. Signature objects only
/// with a signing key, and only from the tools that are engine stages.
fn allowed(tool: &str, kinds: &[String], other: &[String], signing: bool) -> Result<(), String> {
    let (sigs, other): (Vec<&String>, Vec<&String>) = other.iter().partition(|p| is_signature(p));
    if !sigs.is_empty() {
        let may_sign = signing && matches!(tool, "score.verify" | "apply.receipt" | "jobs.start");
        if !may_sign {
            return Err(format!(
                "{tool} wrote signature objects {sigs:?} (signing key: {signing})"
            ));
        }
    }
    let only = |ok: &[&str]| kinds.iter().all(|k| ok.contains(&k.as_str()));
    let cache_only = other.iter().all(|p| p.starts_with("cache/"));
    let fine = match tool {
        "propose.key" | "propose.layout" | "propose.rewrite" => {
            kinds.len() <= 1 && only(&["proposal"]) && other.is_empty()
        }
        "observe.cardinality" | "observe.overlap" => kinds.is_empty() && cache_only,
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

fn check(w: &World, tool: &str, args: Value) -> Result<(), TestCaseError> {
    // One call at a time over the shared folder, so the object lists
    // compared belong to this call alone.
    let _one = one();
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
    allowed(tool, &kinds, &other, w.signing).map_err(TestCaseError::fail)?;
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
        check(world(), TOOLS[t], args)?;
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
        check(world(), tool, args)?;
    }

    #[test]
    fn with_a_signing_key_only_apply_and_verify_may_sign(
        t in 0usize..TOOLS.len(),
        op in any::<u8>(),
        key in prop_oneof![Just("score".to_string()), "[a-z_]{1,6}".prop_map(String::from)],
        value in arb_json(),
        pick in any::<usize>(),
    ) {
        let tool = TOOLS[t];
        let w = keyed_world();
        let args = mutate(&w.valid[tool], op, &key, value, pick);
        check(w, tool, args)?;
    }
}

/// The signature objects (not their lock files) under `index`.
fn signatures(index: &std::path::Path) -> Vec<String> {
    objects(index)
        .into_keys()
        .filter(|p| is_signature(p) && !p.contains(".lock#"))
        .collect()
}

/// Over the whole loop with a signing key: every tool but `score.verify`
/// and `apply.receipt` signs nothing; `score.verify` signs the certificate
/// it wrote, `apply.receipt` the receipt it wrote, and the receipt's chain
/// in the envelope carries both signatures and the verdict. Without a key
/// the same loop signs nothing.
#[test]
fn the_loop_signs_exactly_what_the_engine_stages_wrote() {
    for signing in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        scattered_corpus(&data, 2, 2, 20, 100);
        let keys = dir.path().join("k.parquet");
        write_keys(&keys, "id", &[3, 41]);
        let index = data.join(".bellwether");
        let srv = signing_server(&data, dir.path(), signing);
        let k = keys.to_str().unwrap();

        let ev = json(&ok(&srv, "observe.why", json!({})))["evidence"]["hash"]
            .as_str()
            .unwrap()
            .to_string();
        let scored = json(&ok(&srv, "score.why", json!({"keys": k, "wait": true})));
        let workload = scored["workload"]["hash"].as_str().unwrap().to_string();
        // Refused here (no index yet); it must sign nothing either way.
        let _ = srv.call("observe.locate", json!({"keys": k}));
        let p = json(&ok(
            &srv,
            "propose.key",
            json!({"spec": {"kind": "exact", "columns": ["id"]}, "evidence": [ev], "by": "agent"}),
        ))["proposal"]["hash"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(
            signatures(&index).is_empty(),
            "observe, score.why, propose signed"
        );

        let v = json(&ok(
            &srv,
            "score.verify",
            json!({"proposal": p, "workload": workload, "wait": true}),
        ));
        let cert = v["certificate"]["hash"].as_str().unwrap().to_string();
        let after_verify = signatures(&index);
        let dry = ok(
            &srv,
            "apply.receipt",
            json!({"certificate": cert, "dry_run": true}),
        );
        assert_eq!(json(&dry)["outcome"]["kind"], "would_apply");
        assert_eq!(signatures(&index), after_verify, "the apply dry run signed");
        let applied = ok(
            &srv,
            "apply.receipt",
            json!({"certificate": cert, "wait": true}),
        );
        let receipt = json(&applied)["outcome"]["receipt"]["hash"]
            .as_str()
            .unwrap()
            .to_string();
        let after_apply = signatures(&index);
        let chain = applied
            .envelope
            .chains
            .iter()
            .find(|c| c.receipt.to_hex() == receipt)
            .unwrap();
        if signing {
            assert_eq!(after_verify.len(), 1);
            assert!(after_verify[0].starts_with(&format!("ledger/{cert}.sig.")));
            assert_eq!(after_apply.len(), 2);
            assert!(after_apply
                .iter()
                .any(|s| s.starts_with(&format!("ledger/{receipt}.sig."))));
            assert_eq!(chain.signatures.len(), 2);
            assert!(chain.signatures.iter().all(|s| s.status == "valid"));
            assert_eq!(chain.verdict.as_deref(), Some("signed"));
        } else {
            assert_eq!(after_apply, Vec::<String>::new());
            assert_eq!(chain.signatures, vec![]);
            assert_eq!(chain.verdict, None);
        }
        // Reading back signs nothing.
        srv.resource(bellwether_mcp::RECEIPTS).unwrap();
        ok(&srv, "observe.profile", json!({}));
        assert_eq!(signatures(&index), after_apply);
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

/// The server's `--signing-key` and `--trusted-keys` are refused inside the
/// index location, as the CLI's are.
#[test]
fn key_args_refuse_paths_inside_the_index() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data, 1, 1, 10, 10);
    let index = data.join(".bellwether");
    std::fs::create_dir_all(&index).unwrap();
    let key = index.join("signer");
    bellwether::generate_key(&key, None, None).unwrap();
    let mut options = bellwether::EngineOptions::default();
    let e = bellwether_mcp::KeyArgs {
        signing_key: Some(key.clone()),
        trusted_keys: None,
    }
    .apply(data.to_str().unwrap(), &mut options)
    .unwrap_err();
    assert!(matches!(e, bellwether_mcp::args::OptionsError::Usage(_)));
    assert!(e.to_string().contains("inside the index location"), "{e}");
    let trust = index.join("t.toml");
    bellwether::trust_key(&trust, &bellwether::public_path(&key), None, None).unwrap();
    let e = bellwether_mcp::KeyArgs {
        signing_key: None,
        trusted_keys: Some(trust),
    }
    .apply(data.to_str().unwrap(), &mut options)
    .unwrap_err();
    assert!(e.to_string().contains("choose who is trusted"), "{e}");
}
