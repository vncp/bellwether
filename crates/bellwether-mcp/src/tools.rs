//! The tool catalog: names, tiers, descriptions, and input and output
//! schemas generated from the contract's derives.
//!
//! Every input struct denies unknown fields, and every object schema is
//! closed (`additionalProperties: false`) before it is published or used to
//! validate arguments, including the contract types nested inside (`KeySpec`,
//! `RewriteShape`), whose own serde accepts unknown fields for forward
//! compatibility. A `score` (or any other field) anywhere in a propose
//! tool's arguments is therefore rejected before anything is deserialized.
//! No input takes a path to write, a shell command, or a query string:
//! paths name files the engine reads (key files, rewrite outputs), and a
//! rewrite is `RewriteShape` data.

use bellwether::types::{Gate, Hash, KeySpec, RewriteShape};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::envelope::Envelope;

/// Which side of the trust boundary a tool is on (ADR 009).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Tier {
    /// Reads, and writes Evidence and cache entries only.
    Observe,
    /// The engine scores and verifies: engine Proposals, Replays, plan
    /// Evidence, Certificates. Charged against the session budget.
    Score,
    /// Writes one Proposal (or nothing, as a dry run).
    Propose,
    /// Applies a certificate the engine wrote and attests it.
    Apply,
    /// Starts, polls, and reads long-running calls.
    Jobs,
}

/// `observe.why`: the dataset and its profile, without a workload.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObserveWhy {
    /// Candidate key columns for the profile; default: every id-like column.
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    /// Diagnose from the index's manifest and complete sketches alone: no
    /// data file is opened, footers included, the profile measures nothing,
    /// and the `sketch` column and the `R-sketch-1` lint carry the
    /// (approximate) diagnosis. Refused with `columns`, without a complete
    /// sketch, and over files that changed since the index was built.
    #[serde(default)]
    pub sketches_only: bool,
}

/// `observe.cardinality`: approximate distinct counts from sketches.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObserveCardinality {
    /// Items to count: a column, or columns joined by `+` (`symbol+ts`);
    /// default: every item of every complete sketch.
    #[serde(default)]
    pub items: Vec<String>,
    /// Read the files now for items no complete sketch holds, instead of
    /// refusing with `not_indexed`; the result says how many were read.
    #[serde(default)]
    pub transient: bool,
}

/// `observe.overlap`: approximate Jaccard similarity of two sketched items.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObserveOverlap {
    /// The left item: a column, or columns joined by `+`.
    pub left: String,
    /// The right item, in the same data folder.
    pub right: String,
}

/// The comparison of a rewrite of engine queries' before and after
/// results (the key columns are the proposal's).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EngineCompare {
    /// Compare the newest version per key by this column.
    #[serde(default)]
    pub latest_by: Option<String>,
    /// Columns left out of the row hash.
    #[serde(default)]
    pub ignore: Vec<String>,
    /// Columns totalled exactly (`col` decimal, `col:dec(S)` float).
    #[serde(default)]
    pub totals: Vec<String>,
    /// Columns grouping the totals.
    #[serde(default)]
    pub group_by: Vec<String>,
}

/// `score.record`: the engine runs a query that produces keys in the query
/// engine the server was started with (`--twin-of`), read-only, and
/// records its result as a workload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScoreRecord {
    /// One query in the engine's language whose result rows are the keys.
    /// It runs read-only, under a tag the engine chooses.
    pub keys_query: String,
    /// Result columns to take as the key, in tuple order; default: all.
    #[serde(default)]
    pub key_columns: Option<Vec<String>>,
    /// Run in this call instead of returning a job id.
    #[serde(default)]
    pub wait: bool,
}

/// `observe.profile`: the newest recorded profile Evidence per root, with
/// its lints and the receipts citing it. Reads the ledger only.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObserveProfile {}

/// `observe.locate`: which files and row groups hold the keys.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObserveLocate {
    /// A Parquet file of keys (read, never written); an absolute path.
    pub keys: String,
    /// Key columns to take from the file, in tuple order; default: all.
    #[serde(default)]
    pub key_columns: Option<Vec<String>>,
    /// Plan against the committed manifest at this sequence (a plan's
    /// `sequence`) instead of the current one, so a lookup repeated over a
    /// grown folder reads exactly what it read then: files indexed since are
    /// ignored. Default: the current manifest. A sequence the index no
    /// longer retains is refused with `not_retained`.
    #[serde(default)]
    pub at_manifest: Option<u64>,
}

/// `observe.asof`: the version of each key current at an instant.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObserveAsof {
    /// A Parquet file of keys (read, never written); an absolute path.
    pub keys: String,
    /// Key columns to take from the file, in tuple order; default: all.
    #[serde(default)]
    pub key_columns: Option<Vec<String>>,
    /// The instant: RFC 3339, a date (YYYY-MM-DD), or an integer in the
    /// timestamp column's unit.
    pub at: String,
    /// Plan against the committed manifest at this sequence (a plan's
    /// `sequence`) instead of the current one, so a lookup repeated over a
    /// grown folder reads exactly what it read then: files indexed since are
    /// ignored. Default: the current manifest. A sequence the index no
    /// longer retains is refused with `not_retained`.
    #[serde(default)]
    pub at_manifest: Option<u64>,
}

/// `score.why`: replay a workload under every candidate key. Writes engine
/// Proposals and Replays.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScoreWhy {
    /// A Parquet file of the keys one run looked up (read, never written).
    #[serde(default)]
    pub keys: Option<String>,
    /// Key columns to take from `keys`, in tuple order; default: all.
    #[serde(default)]
    pub key_columns: Option<Vec<String>>,
    /// A workload recorded earlier, by its hash.
    #[serde(default)]
    pub workload: Option<Hash>,
    /// Candidate key columns for the profile; default: every id-like column.
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    /// Most profile candidates replayed beyond the workload's own key
    /// columns; default 8.
    #[serde(default)]
    pub max_candidates: Option<u32>,
    /// With a twin (the server's `--twin-of`): limits each candidate's
    /// keyset is compared against, such as `{"name": "in", "limit":
    /// 1000000}`.
    #[serde(default)]
    pub gates: Vec<Gate>,
    /// Run in this call instead of returning a job id.
    #[serde(default)]
    pub wait: bool,
}

/// The outputs a rewrite proposal is verified on (folders the engine reads).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RewriteOutputs {
    /// The output before the rewrite (a folder of files; never a table).
    pub before: String,
    /// The output after it.
    pub after: String,
    /// Key columns the outputs are compared by.
    pub key: Vec<String>,
    /// Compare the newest version per key by this column.
    #[serde(default)]
    pub latest_by: Option<String>,
    /// Columns left out of the row hash.
    #[serde(default)]
    pub ignore: Vec<String>,
    /// Decimal columns totalled exactly.
    #[serde(default)]
    pub totals: Vec<String>,
    /// Columns grouping the totals.
    #[serde(default)]
    pub group_by: Vec<String>,
}

/// `score.verify`: the engine scores and verifies a proposal and writes the
/// Certificate (`fix` with `verify_only`). Nothing is applied.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScoreVerify {
    /// The proposal to score and verify.
    pub proposal: Hash,
    /// For a key proposal: the recorded workload it is scored on.
    #[serde(default)]
    pub workload: Option<Hash>,
    /// For a rewrite proposal: the outputs it is verified on.
    #[serde(default)]
    pub rewrite: Option<RewriteOutputs>,
    /// For a rewrite of engine queries: how the exported results compare
    /// (default: by the proposal's key columns alone).
    #[serde(default)]
    pub engine: Option<EngineCompare>,
    /// Run in this call instead of returning a job id.
    #[serde(default)]
    pub wait: bool,
}

/// `propose.key`: propose an index key.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProposeKey {
    /// The key.
    pub spec: KeyArg,
    /// Evidence hashes the proposal cites; at least one.
    pub evidence: Vec<Hash>,
    /// Who proposes: your agent name. `engine` is reserved.
    pub by: String,
    /// Return what would be written, and write nothing.
    #[serde(default)]
    pub dry_run: bool,
}

/// The key `propose.key` proposes: the contract's `KeySpec` as JSON, or the
/// short form `bellwether index --key` and `bellwether propose --key` read.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum KeyArg {
    /// The short form: exact:a,b, bloom:a@0.01, range:ts, or temporal:id@ts.
    /// A sidecar declaration (temporal2:id@valid,known, or exact:a@ts with
    /// its per-key time bounds) is refused with `sidecar_not_proposable`:
    /// a proposal carries a key spec only.
    Short(String),
    /// The key spec's JSON form.
    Spec(KeySpec),
}

/// `propose.layout`: propose clustering files by columns.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProposeLayout {
    /// Columns to cluster by.
    pub cluster_by: Vec<String>,
    /// Evidence hashes the proposal cites; at least one.
    pub evidence: Vec<Hash>,
    /// Who proposes: your agent name. `engine` is reserved.
    pub by: String,
    /// Return what would be written, and write nothing.
    #[serde(default)]
    pub dry_run: bool,
}

/// `propose.rewrite`: propose a rewrite, as data. A rewrite of engine
/// queries carries the (before, after) query pairs in `shape.pairs`; the
/// engine submits them, read-only, only to the engine the server was
/// started with, and only in `score.verify`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProposeRewrite {
    /// Before, after, and the invariant the rewrite preserves.
    pub shape: RewriteShape,
    /// Evidence hashes the proposal cites; at least one.
    pub evidence: Vec<Hash>,
    /// Who proposes: your agent name. `engine` is reserved.
    pub by: String,
    /// Return what would be written, and write nothing.
    #[serde(default)]
    pub dry_run: bool,
}

/// `apply.receipt`: apply a certificate the engine wrote and attest it.
/// Takes a certificate hash only.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ApplyReceipt {
    /// The certificate (`score.verify` returned it).
    pub certificate: Hash,
    /// Run the gate only: return what would happen, and write nothing.
    #[serde(default)]
    pub dry_run: bool,
    /// Run in this call instead of returning a job id.
    #[serde(default)]
    pub wait: bool,
}

/// `jobs.start`: run any tool as a job. The job id is the hash of the
/// request, so starting the same request again returns the same job.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JobsStart {
    /// The tool to run.
    pub tool: String,
    /// Its arguments, validated against that tool's schema.
    #[serde(default)]
    pub arguments: Map<String, Value>,
}

/// `jobs.status` and `jobs.result`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JobRef {
    /// The job id.
    pub job: String,
}

/// One tool's catalog entry.
pub struct ToolDef {
    /// The tool's name.
    pub name: &'static str,
    /// Its tier.
    pub tier: Tier,
    /// What it does and what it writes.
    pub description: &'static str,
    /// Whether it only reads (writes at most Evidence and cache entries).
    pub read_only: bool,
    /// The closed input schema.
    pub input: Value,
}

/// Every tool, in listing order.
#[must_use]
#[allow(clippy::too_many_lines)] // one entry per tool
pub fn catalog() -> Vec<ToolDef> {
    vec![
        def::<ObserveWhy>(
            "observe.why",
            Tier::Observe,
            true,
            "Profile the data folder from footer statistics: files, row groups, rows, bytes, \
             fanout per column, and lints, as a profile Evidence (evidence.hash). No workload. \
             Writes the Evidence and cache entries only; with an unwritable index it writes \
             nothing (recorded: false).",
        ),
        def::<ObserveCardinality>(
            "observe.cardinality",
            Tier::Observe,
            true,
            "Approximate count(distinct) per item from the index's sketches (no data file read): \
             the estimate with its 99.7% interval, exact sketched and null rows, rows per value \
             (fanout), files per value, and files that repeat a value. Labelled approximate; \
             exact_distinct is set where an exact key on the same columns holds the count. With \
             transient, reads the files for items no sketch holds. Writes nothing.",
        ),
        def::<ObserveOverlap>(
            "observe.overlap",
            Tier::Observe,
            true,
            "Approximate Jaccard similarity of two sketched items' value sets (MinHash when both \
             sketches keep signatures, else HyperLogLog inclusion-exclusion), with its interval. \
             Labelled approximate. Writes nothing.",
        ),
        def::<ObserveProfile>(
            "observe.profile",
            Tier::Observe,
            true,
            "The newest recorded profile Evidence per data root, with its lints and the \
             receipts that cite it. Reads the ledger; writes nothing.",
        ),
        def::<ObserveLocate>(
            "observe.locate",
            Tier::Observe,
            true,
            "Plan which files and row groups hold the keys in a Parquet key file, against the \
             current manifest or a committed one (at_manifest). Writes the plan Evidence and \
             the key set to the cache only.",
        ),
        def::<ObserveAsof>(
            "observe.asof",
            Tier::Observe,
            true,
            "Plan the version of each key current at an instant, against the current manifest \
             or a committed one (at_manifest). Writes the plan Evidence and the key set to the \
             cache only.",
        ),
        def::<ScoreWhy>(
            "score.why",
            Tier::Score,
            false,
            "Engine-scored why: replay the keys (or a recorded workload) under the current \
             configuration and every candidate key; returns the funnel, the culprit, the \
             recommended key, and workload.hash. Writes engine Proposals and Replays. Charged \
             to the session budget. Returns a job id unless wait is true.",
        ),
        def::<ScoreVerify>(
            "score.verify",
            Tier::Score,
            false,
            "The engine scores a proposal (a Replay) and verifies it (a Certificate, written \
             passing or failing); nothing is applied. Pass the certificate hash to \
             apply.receipt. Charged to the session budget. Returns a job id unless wait is true.",
        ),
        def::<ScoreRecord>(
            "score.record",
            Tier::Score,
            false,
            "Run a query that produces keys in the query engine the server was started with \
             (--twin-of), read-only, and record its rows as a workload (workload.hash) for \
             score.why. Writes the workload to the cache. Charged to the session budget. Returns \
             a job id unless wait is true.",
        ),
        def::<ProposeKey>(
            "propose.key",
            Tier::Propose,
            false,
            "Propose an index key (JSON, or the short form such as exact:id) citing Evidence. \
             Writes one Proposal (no score field exists), or nothing with dry_run.",
        ),
        def::<ProposeLayout>(
            "propose.layout",
            Tier::Propose,
            false,
            "Propose clustering files by columns, citing Evidence. Writes one Proposal, or \
             nothing with dry_run.",
        ),
        def::<ProposeRewrite>(
            "propose.rewrite",
            Tier::Propose,
            false,
            "Propose a rewrite as data (before, after, invariant), citing Evidence. A rewrite of \
             queries in the server's query engine carries them as shape.pairs (before and after \
             query text) and shape.key (the columns their results are compared by), citing the \
             twin Evidence observe.why or score.why returned. Writes one Proposal, or nothing \
             with dry_run.",
        ),
        def::<ApplyReceipt>(
            "apply.receipt",
            Tier::Apply,
            false,
            "Apply a certificate the engine wrote and attest it: refused unless it passed, was \
             computed against the current manifest, and the data is unchanged. Returns the \
             Receipt. With dry_run, runs the gate and writes nothing. Returns a job id unless \
             wait or dry_run is true.",
        ),
        def::<JobsStart>(
            "jobs.start",
            Tier::Jobs,
            false,
            "Run any tool as a job and return its id (the hash of the request; starting the \
             same request again returns the same job).",
        ),
        def::<JobRef>(
            "jobs.status",
            Tier::Jobs,
            true,
            "A job's state and progress text.",
        ),
        def::<JobRef>(
            "jobs.result",
            Tier::Jobs,
            true,
            "A finished job's result, exactly as the tool would have returned it.",
        ),
    ]
}

/// The tool named `name`.
#[must_use]
pub fn find(name: &str) -> Option<ToolDef> {
    catalog().into_iter().find(|t| t.name == name)
}

/// The output schema every tool shares: the envelope.
#[must_use]
pub fn output_schema() -> Value {
    let mut v = serde_json::to_value(schemars::schema_for!(Envelope)).unwrap_or(Value::Null);
    close(&mut v);
    v
}

fn def<T: JsonSchema>(
    name: &'static str,
    tier: Tier,
    read_only: bool,
    description: &'static str,
) -> ToolDef {
    let mut input = serde_json::to_value(schemars::schema_for!(T)).unwrap_or(Value::Null);
    close(&mut input);
    ToolDef {
        name,
        tier,
        description,
        read_only,
        input,
    }
}

/// Closes every object schema that declares properties: no field beyond
/// the declared ones is accepted, at any depth (`$defs` included).
pub fn close(v: &mut Value) {
    match v {
        Value::Object(m) => {
            if m.contains_key("properties") && !m.contains_key("additionalProperties") {
                m.insert("additionalProperties".into(), Value::Bool(false));
            }
            for x in m.values_mut() {
                close(x);
            }
        }
        Value::Array(a) => a.iter_mut().for_each(close),
        _ => {}
    }
}

/// Validates `args` against `schema`; the errors, if any, one line each.
pub fn validate(schema: &Value, args: &Value) -> Result<(), Vec<String>> {
    let validator = jsonschema::validator_for(schema).map_err(|e| vec![e.to_string()])?;
    let errors: Vec<String> = validator
        .iter_errors(args)
        .map(|e| {
            let at = e.instance_path().to_string();
            if at.is_empty() {
                e.to_string()
            } else {
                format!("{at}: {e}")
            }
        })
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}
