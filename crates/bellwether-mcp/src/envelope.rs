//! The MCP envelope around a facade result (ADR 010, additions 10 and 14).
//!
//! A tool call returns two things. The text content is the facade's typed
//! result serialized with `serde_json::to_string`, byte for byte the CLI's
//! `--json` line without its newline; it is never re-encoded through a JSON
//! value, which would reorder its fields. The structured content is this
//! envelope: the tool's status, the record hashes the result rests on
//! (`cites`), the `explain` chain of every receipt it mentions, a refusal
//! with its code, a job, and the session budget. Removing the envelope
//! leaves exactly the CLI's record.

use bellwether::types::Hash;
use bellwether::{LinkStatus, ReceiptResult};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::budget::BudgetState;
use crate::jobs::JobInfo;
use crate::refusal::Refusal;
use crate::tools::Tier;

/// What happened.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// The call completed; the text content is the facade's result.
    Ok,
    /// Refused: `refusal` says why and what to do. The text content is the
    /// facade's result when the facade returned a typed refusal, else the
    /// refusal itself.
    Refused,
    /// Still running as a job: `job` has its id; nothing to read yet.
    Running,
}

/// A record the result rests on.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Cited {
    /// Its content hash, to quote verbatim.
    pub hash: Hash,
    /// Its kind: evidence, proposal, replay, certificate, or receipt.
    pub kind: String,
}

/// One link of a receipt's chain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Link {
    /// The record's hash.
    pub hash: Hash,
    /// The kind the chain expects here.
    pub kind: String,
    /// `ok`, `missing`, `wrong_kind`, or `unreadable`.
    pub status: String,
}

/// A receipt's `explain` chain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Chain {
    /// The receipt.
    pub receipt: Hash,
    /// Whether every link resolved to its expected kind.
    pub intact: bool,
    /// Evidence, proposal, replay, certificate, receipt.
    pub links: Vec<Link>,
}

impl From<&ReceiptResult> for Chain {
    fn from(r: &ReceiptResult) -> Self {
        Self {
            receipt: r.receipt,
            intact: r.intact,
            links: r
                .chain
                .iter()
                .map(|l| Link {
                    hash: l.hash,
                    kind: l.kind.as_str().to_string(),
                    status: match &l.status {
                        LinkStatus::Ok => "ok",
                        LinkStatus::Missing => "missing",
                        LinkStatus::WrongKind { .. } => "wrong_kind",
                        LinkStatus::Unreadable { .. } => "unreadable",
                    }
                    .to_string(),
                })
                .collect(),
        }
    }
}

/// The structured content of every tool result.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Envelope {
    /// The tool called.
    pub tool: String,
    /// Its tier.
    pub tier: Tier,
    /// What happened.
    pub status: Status,
    /// Every record the result rests on, in the order it first appears.
    pub cites: Vec<Cited>,
    /// The `explain` chain of every receipt the result mentions.
    pub chains: Vec<Chain>,
    /// Why the call was refused, when it was.
    pub refusal: Option<Refusal>,
    /// The job, when the call runs (or ran) as one.
    pub job: Option<JobInfo>,
    /// The session budget after the call.
    pub budget: BudgetState,
}

/// Every record a serialized facade result rests on, and the receipts it
/// mentions. Walks the result's JSON value (for collection only; the bytes
/// returned are never re-encoded): a `{hash, record}` object is a record of
/// the record's kind; a recorded profile (`{recorded: true, hash,
/// evidence}`) is an Evidence; a resolved chain link is a record of its
/// kind; a `receipts` list of hashes names receipts.
#[must_use]
pub fn cites_of(v: &Value) -> (Vec<Cited>, Vec<Hash>) {
    let mut cites: Vec<Cited> = Vec::new();
    let mut receipts: Vec<Hash> = Vec::new();
    walk(v, &mut cites);
    for c in &cites {
        if c.kind == "receipt" && !receipts.contains(&c.hash) {
            receipts.push(c.hash);
        }
    }
    (cites, receipts)
}

fn push(cites: &mut Vec<Cited>, hash: Option<Hash>, kind: &str) {
    if let Some(hash) = hash {
        if !cites.iter().any(|c| c.hash == hash) {
            cites.push(Cited {
                hash,
                kind: kind.to_string(),
            });
        }
    }
}

fn hash_of(v: Option<&Value>) -> Option<Hash> {
    v.and_then(Value::as_str)
        .and_then(|s| Hash::from_hex(s).ok())
}

fn walk(v: &Value, cites: &mut Vec<Cited>) {
    match v {
        Value::Object(m) => {
            if let (Some(h), Some(kind)) = (
                hash_of(m.get("hash")),
                m.get("record")
                    .and_then(|r| r.get("record"))
                    .and_then(Value::as_str),
            ) {
                push(cites, Some(h), kind);
            }
            if m.get("recorded") == Some(&Value::Bool(true)) && m.contains_key("evidence") {
                push(cites, hash_of(m.get("hash")), "evidence");
            }
            if m.get("status").and_then(|s| s.get("status")) == Some(&Value::from("ok")) {
                if let Some(kind) = m.get("kind").and_then(Value::as_str) {
                    push(cites, hash_of(m.get("hash")), kind);
                }
            }
            if let Some(Value::Array(list)) = m.get("receipts") {
                for x in list {
                    if let Some(h) = hash_of(Some(x)) {
                        push(cites, Some(h), "receipt");
                    }
                }
            }
            for x in m.values() {
                walk(x, cites);
            }
        }
        Value::Array(a) => {
            for x in a {
                walk(x, cites);
            }
        }
        _ => {}
    }
}
