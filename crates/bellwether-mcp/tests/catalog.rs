//! The catalog the server publishes: the tools, their closed schemas from
//! the contract, the one prompt generated from `AGENTS.md`, the refusal
//! codes `AGENTS.md` documents, and the three resources.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeSet;

use bellwether_mcp::handler::{get_prompt, prompt_def, resource_list, tool_list};
use bellwether_mcp::prompt::{messages, AGENTS_MD, NAME};
use bellwether_mcp::tools::{catalog, output_schema, Tier};
use bellwether_mcp::RefusalCode;
use rmcp::model::{GetPromptRequestParams, GetPromptResponse};
use serde_json::Value;

/// Every object schema that declares properties is closed, at any depth.
fn assert_closed(v: &Value, at: &str) {
    match v {
        Value::Object(m) => {
            if m.contains_key("properties") {
                assert_eq!(
                    m.get("additionalProperties"),
                    Some(&Value::Bool(false)),
                    "open object at {at}"
                );
            }
            for (k, x) in m {
                assert_closed(x, &format!("{at}/{k}"));
            }
        }
        Value::Array(a) => {
            for (i, x) in a.iter().enumerate() {
                assert_closed(x, &format!("{at}/{i}"));
            }
        }
        _ => {}
    }
}

/// Every property name anywhere in a schema.
fn properties(v: &Value, out: &mut BTreeSet<String>) {
    match v {
        Value::Object(m) => {
            if let Some(Value::Object(p)) = m.get("properties") {
                out.extend(p.keys().cloned());
            }
            m.values().for_each(|x| properties(x, out));
        }
        Value::Array(a) => a.iter().for_each(|x| properties(x, out)),
        _ => {}
    }
}

#[test]
fn the_tools_are_the_tiers_and_nothing_else() {
    let names: Vec<(&str, Tier)> = catalog().iter().map(|t| (t.name, t.tier)).collect();
    assert_eq!(
        names,
        vec![
            ("observe.why", Tier::Observe),
            ("observe.profile", Tier::Observe),
            ("observe.locate", Tier::Observe),
            ("observe.asof", Tier::Observe),
            ("score.why", Tier::Score),
            ("score.verify", Tier::Score),
            ("propose.key", Tier::Propose),
            ("propose.layout", Tier::Propose),
            ("propose.rewrite", Tier::Propose),
            ("apply.receipt", Tier::Apply),
            ("jobs.start", Tier::Jobs),
            ("jobs.status", Tier::Jobs),
            ("jobs.result", Tier::Jobs),
        ]
    );
    assert_eq!(tool_list().len(), names.len());
}

/// Schemas come from the contract's derives, closed; no tool takes a path
/// to write, a command, a query, or a score; apply takes a certificate hash
/// only (and its delivery flags); observe.why takes no keys.
#[test]
fn schemas_are_closed_and_carry_no_dangerous_field() {
    for t in catalog() {
        assert_closed(&t.input, t.name);
        let mut props = BTreeSet::new();
        properties(&t.input, &mut props);
        for bad in [
            "score",
            "estimate",
            "confidence",
            "command",
            "shell",
            "sql",
            "query",
            "exec",
            "output",
            "out",
            "html",
            "write",
            "path",
        ] {
            assert!(!props.contains(bad), "{} has a {bad:?} field", t.name);
        }
        if t.name == "apply.receipt" {
            let top: BTreeSet<&str> = t.input["properties"]
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect();
            assert_eq!(top, BTreeSet::from(["certificate", "dry_run", "wait"]));
            assert_eq!(t.input["required"], serde_json::json!(["certificate"]));
        }
        if t.name == "observe.why" {
            assert!(!props.contains("keys") && !props.contains("workload"));
        }
    }
    // The key spec in propose.key is the contract's KeySpec: its tagged
    // variants, each closed.
    let key = catalog()
        .into_iter()
        .find(|t| t.name == "propose.key")
        .unwrap();
    let text = key.input.to_string();
    for kind in ["exact", "bloom", "range", "temporal"] {
        assert!(text.contains(&format!("\"const\":\"{kind}\"")), "{kind}");
    }
    // A hash is the contract's Hash: 64 lowercase hex characters.
    assert!(text.contains("^[0-9a-f]{64}$"));
    assert_closed(&output_schema(), "output");
}

/// The prompt is AGENTS.md, verbatim, and walks the loop in order.
#[test]
fn the_prompt_is_agents_md() {
    let disk =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../AGENTS.md")).unwrap();
    assert_eq!(AGENTS_MD, disk);
    assert_eq!(messages(None), vec![disk.clone()]);
    assert_eq!(prompt_def().name, NAME);
    let mut params = GetPromptRequestParams::new(NAME);
    params.arguments = Some(
        serde_json::json!({"symptom": "nightly job reads 40 GB"})
            .as_object()
            .unwrap()
            .clone(),
    );
    let GetPromptResponse::Complete(r) = get_prompt(&params).unwrap() else {
        panic!("not complete");
    };
    assert_eq!(r.messages.len(), 2);
    let first = serde_json::to_value(&r.messages[0]).unwrap();
    assert_eq!(first["content"]["text"], disk.as_str());
    assert!(get_prompt(&GetPromptRequestParams::new("other")).is_err());

    // The loop, in order.
    let at = |s: &str| {
        disk.find(s)
            .unwrap_or_else(|| panic!("AGENTS.md lacks {s}"))
    };
    let order = [
        "`observe.why`",
        "**Read the evidence.**",
        "`propose.key`",
        "`score.verify`",
        "`apply.receipt`",
        "Quote the receipt hash to the human",
    ];
    for w in order.windows(2) {
        assert!(at(w[0]) < at(w[1]), "{} before {}", w[0], w[1]);
    }
    assert!(disk.contains("The engine refuses unverified applies"));
}

/// Every refusal code the server can return is documented in AGENTS.md,
/// and AGENTS.md documents no other.
#[test]
fn agents_md_documents_every_refusal_code() {
    let table: BTreeSet<String> = AGENTS_MD
        .lines()
        .filter_map(|l| l.strip_prefix("| `"))
        .filter_map(|l| l.split_once("` |").map(|(c, _)| c.to_string()))
        .filter(|c| c.chars().all(|ch| ch.is_ascii_lowercase() || ch == '_'))
        .collect();
    let codes: BTreeSet<String> = RefusalCode::ALL
        .iter()
        .map(|c| c.as_str().to_string())
        .collect();
    assert_eq!(table, codes);
    for c in RefusalCode::ALL {
        assert_eq!(serde_json::to_value(c).unwrap(), c.as_str());
        assert_ne!(c.next(), "");
    }
}

#[test]
fn three_resources() {
    let uris: Vec<String> = resource_list().into_iter().map(|r| r.uri).collect();
    assert_eq!(
        uris,
        vec![
            bellwether_mcp::FINDINGS,
            bellwether_mcp::RECEIPTS,
            bellwether_mcp::SCORECARD
        ]
    );
}
