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

use bellwether::types::{AggregateFn, Gate, Hash, KeySpec, RewriteShape, TextOp};
use bellwether::TextQuery;
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
    /// Target dataset: a `--dataset NAME=ROOT` binding or, when the server runs
    /// with `--workspace`, a relative folder under the workspace root; default:
    /// the server's primary root.
    #[serde(default)]
    pub dataset: Option<String>,
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
    /// Target dataset: a `--dataset NAME=ROOT` binding or, when the server runs
    /// with `--workspace`, a relative folder under the workspace root; default:
    /// the server's primary root.
    #[serde(default)]
    pub dataset: Option<String>,
}

/// `observe.overlap`: approximate Jaccard similarity of two sketched items.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObserveOverlap {
    /// The left item: a column, or columns joined by `+`.
    pub left: String,
    /// The right item, in the same data folder.
    pub right: String,
    /// Target dataset: a `--dataset NAME=ROOT` binding or, when the server runs
    /// with `--workspace`, a relative folder under the workspace root; default:
    /// the server's primary root.
    #[serde(default)]
    pub dataset: Option<String>,
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
    /// Target dataset: a `--dataset NAME=ROOT` binding or, when the server runs
    /// with `--workspace`, a relative folder under the workspace root; default:
    /// the server's primary root.
    #[serde(default)]
    pub dataset: Option<String>,
}

/// `observe.profile`: the newest recorded profile Evidence per root, with
/// its lints and the receipts citing it. Reads the ledger only.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObserveProfile {
    /// Target dataset: a `--dataset NAME=ROOT` binding or, when the server runs
    /// with `--workspace`, a relative folder under the workspace root; default:
    /// the server's primary root.
    #[serde(default)]
    pub dataset: Option<String>,
}

/// The query engine an `emit` argument translates a plan for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EmitEngine {
    /// Apache `DataFusion`: a Parquet scan pinned to the planned row groups
    /// (JSON).
    Datafusion,
    /// `DuckDB`: one SQL statement over the planned files.
    Duckdb,
    /// Apache Spark: a manifest for `shims/spark/bellwether_plan.py` (JSON).
    Spark,
}

impl From<EmitEngine> for bellwether::EmitEngine {
    fn from(e: EmitEngine) -> Self {
        match e {
            EmitEngine::Datafusion => Self::Datafusion,
            EmitEngine::Duckdb => Self::Duckdb,
            EmitEngine::Spark => Self::Spark,
        }
    }
}

/// Which part of the plan's residual `emit` applies.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EmitResidual {
    /// All of it, in order: exactly the lookup's rows.
    #[default]
    Full,
    /// Only its key membership (`In`): a superset, labeled in the artifact.
    KeysOnly,
    /// None: every row of the planned row groups, labeled in the artifact.
    None,
}

impl From<EmitResidual> for bellwether::EmitResidual {
    fn from(r: EmitResidual) -> Self {
        match r {
            EmitResidual::Full => Self::Full,
            EmitResidual::KeysOnly => Self::KeysOnly,
            EmitResidual::None => Self::None,
        }
    }
}

/// `observe.locate`: which files and row groups hold the keys.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObserveLocate {
    /// A Parquet file of keys (read, never written); an absolute path. Give
    /// `keys` or `key_values` (with `key_columns`), not both.
    #[serde(default)]
    pub keys: Option<String>,
    /// Key columns to take from `keys`, or the columns of `key_values`, in
    /// tuple order; default for `keys`: all. Required with `key_values`.
    #[serde(default)]
    pub key_columns: Option<Vec<String>>,
    /// Inline key tuples (one array of JSON scalars per key tuple, in
    /// `key_columns` order) instead of a `keys` file, encoded at the
    /// dataset's key column types and stored in the key-set cache.
    #[serde(default)]
    pub key_values: Option<Vec<Vec<Value>>>,
    /// Plan against the committed manifest at this sequence (a plan's
    /// `sequence`) instead of the current one, so a lookup repeated over a
    /// grown folder reads exactly what it read then: files indexed since are
    /// ignored. Default: the current manifest. A sequence the index no
    /// longer retains is refused with `not_retained`.
    #[serde(default)]
    pub at_manifest: Option<u64>,
    /// The columns the reader reads: each planned file also reports those
    /// columns' compressed bytes, and the plan their sum
    /// (`projected_bytes`), from the index. Default: no projection.
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    /// List membership (B4; `locate --any` / `--all`): the key file holds
    /// one column, named after a list (or map) column, of its element
    /// type, and the plan is the rows whose list holds `any` of the values
    /// or `all` of them, through the list key on that column. Default: a
    /// point lookup of the key tuples.
    #[serde(default)]
    pub contains: Option<Membership>,
    /// Translate the plan for a query engine (`datafusion` JSON, `duckdb`
    /// SQL, `spark` manifest JSON) and return it in `emitted`; nothing is
    /// executed.
    #[serde(default)]
    pub emit: Option<EmitEngine>,
    /// With `emit`: refuse unless the plan has a read order, and have the
    /// engine return rows in it.
    #[serde(default)]
    pub ordered: bool,
    /// With `emit`: which part of the residual the engine applies (`full`,
    /// `keys_only`, `none`; default `full`).
    #[serde(default)]
    pub residual: Option<EmitResidual>,
    /// With `emit`: the engine reads every planned file whose path starts
    /// with `[0]` (`from`) at `[1]` (`to`) followed by the rest of the path.
    #[serde(default)]
    pub relocate: Option<[String; 2]>,
    /// Target dataset: a `--dataset NAME=ROOT` binding or, when the server runs
    /// with `--workspace`, a relative folder under the workspace root; default:
    /// the server's primary root.
    #[serde(default)]
    pub dataset: Option<String>,
}

/// Which rows a list-membership lookup keeps.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Membership {
    /// Rows whose list holds at least one of the values.
    Any,
    /// Rows whose list holds every one of the values.
    All,
}

/// `observe.count`: the rows holding the keys, exact, from the index's
/// per-key counts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObserveCount {
    /// A Parquet file of keys (read, never written); an absolute path. Give
    /// `keys` or `key_values` (with `key_columns`), not both.
    #[serde(default)]
    pub keys: Option<String>,
    /// Key columns to take from `keys`, or the columns of `key_values`, in
    /// tuple order; default for `keys`: all. Required with `key_values`.
    #[serde(default)]
    pub key_columns: Option<Vec<String>>,
    /// Inline key tuples (one array of JSON scalars per key tuple, in
    /// `key_columns` order) instead of a `keys` file.
    #[serde(default)]
    pub key_values: Option<Vec<Vec<Value>>>,
    /// Count against the committed manifest at this sequence instead of the
    /// current one. Default: the current manifest.
    #[serde(default)]
    pub at_manifest: Option<u64>,
    /// Target dataset: a `--dataset NAME=ROOT` binding or, when the server runs
    /// with `--workspace`, a relative folder under the workspace root; default:
    /// the server's primary root.
    #[serde(default)]
    pub dataset: Option<String>,
}

/// `observe.aggregate` (B7): per key, `sum`, `count`, `min`, `max` of one
/// column over the keys in a Parquet key file or inline `key_values`, from
/// the index's agg key (`bellwether aggregate`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObserveAggregate {
    /// A Parquet file of keys (read, never written); an absolute path. Give
    /// `keys` or `key_values` (with `key_columns`), not both.
    #[serde(default)]
    pub keys: Option<String>,
    /// Key columns to take from `keys`, or the columns of `key_values`, in
    /// tuple order; default for `keys`: all. Required with `key_values`.
    #[serde(default)]
    pub key_columns: Option<Vec<String>>,
    /// Inline key tuples (one array of JSON scalars per key tuple, in
    /// `key_columns` order) instead of a `keys` file.
    #[serde(default)]
    pub key_values: Option<Vec<Vec<Value>>>,
    /// The column to aggregate.
    pub value: String,
    /// The functions (`sum`, `count`, `min`, `max`); `count` is always
    /// answered. Default: `sum`, `count` (as `aggregate --fn`).
    #[serde(default)]
    pub functions: Option<Vec<AggregateFn>>,
    /// Answer against the committed manifest at this sequence instead of
    /// the current one. Default: the current manifest.
    #[serde(default)]
    pub at_manifest: Option<u64>,
    /// Target dataset: a `--dataset NAME=ROOT` binding or, when the server runs
    /// with `--workspace`, a relative folder under the workspace root; default:
    /// the server's primary root.
    #[serde(default)]
    pub dataset: Option<String>,
}

/// `observe.asof`: the version of each key current at an instant, or, with
/// `valid` and/or `known` instead of `at`, the bitemporal as-of through a
/// `temporal2` key (`asof --valid --known`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObserveAsof {
    /// A Parquet file of keys (read, never written); an absolute path. Give
    /// `keys` or `key_values` (with `key_columns`), not both.
    #[serde(default)]
    pub keys: Option<String>,
    /// Key columns to take from `keys`, or the columns of `key_values`, in
    /// tuple order; default for `keys`: all. Required with `key_values`.
    #[serde(default)]
    pub key_columns: Option<Vec<String>>,
    /// Inline key tuples (one array of JSON scalars per key tuple, in
    /// `key_columns` order) instead of a `keys` file.
    #[serde(default)]
    pub key_values: Option<Vec<Vec<Value>>>,
    /// The instant: RFC 3339, a date (YYYY-MM-DD), or an integer in the
    /// timestamp column's unit. Give `at`, or `valid` and/or `known`.
    #[serde(default)]
    pub at: Option<String>,
    /// Bitemporal: the valid-time instant, in the same forms as `at`
    /// (needs a `temporal2` key); absent: unbounded.
    #[serde(default)]
    pub valid: Option<String>,
    /// Bitemporal: the known-time instant; rows learned later are ignored.
    /// Absent: unbounded.
    #[serde(default)]
    pub known: Option<String>,
    /// Bitemporal: the valid-time column of the bitemporal key to plan
    /// through (with `known_by`), when several serve the key columns.
    /// Absent: the one that serves them.
    #[serde(default)]
    pub valid_by: Option<String>,
    /// Bitemporal: the known-time column of that key (with `valid_by`).
    #[serde(default)]
    pub known_by: Option<String>,
    /// Keep only live rows (B8; the CLI's `--live`): each key's newest
    /// version at the instant, without its tombstone rows, through the
    /// tombstone the serving temporal or temporal2 key declares
    /// (`temporal:id@ts,tombstone=void`). A key whose newest row is a
    /// tombstone answers no row. Default `false`: every row of the newest
    /// version.
    #[serde(default)]
    pub live: bool,
    /// Plan against the committed manifest at this sequence (a plan's
    /// `sequence`) instead of the current one, so a lookup repeated over a
    /// grown folder reads exactly what it read then: files indexed since are
    /// ignored. Default: the current manifest. A sequence the index no
    /// longer retains is refused with `not_retained`.
    #[serde(default)]
    pub at_manifest: Option<u64>,
    /// Translate the plan for a query engine (`datafusion` JSON, `duckdb`
    /// SQL, `spark` manifest JSON) and return it in `emitted`; nothing is
    /// executed.
    #[serde(default)]
    pub emit: Option<EmitEngine>,
    /// With `emit`: refuse unless the plan has a read order, and have the
    /// engine return rows in it.
    #[serde(default)]
    pub ordered: bool,
    /// With `emit`: which part of the residual the engine applies (`full`,
    /// `keys_only`, `none`; default `full`).
    #[serde(default)]
    pub residual: Option<EmitResidual>,
    /// With `emit`: the engine reads every planned file whose path starts
    /// with `[0]` (`from`) at `[1]` (`to`) followed by the rest of the path.
    #[serde(default)]
    pub relocate: Option<[String; 2]>,
    /// Target dataset: a `--dataset NAME=ROOT` binding or, when the server runs
    /// with `--workspace`, a relative folder under the workspace root; default:
    /// the server's primary root.
    #[serde(default)]
    pub dataset: Option<String>,
}

/// `observe.semijoin` (B3): `semijoin(keys)` through the join key the
/// server was started with (`--join`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObserveSemijoin {
    /// A Parquet file of keys on the join key's left columns (read, never
    /// written); an absolute path.
    pub keys: String,
    /// Key columns to take from the file, in tuple order; default: all.
    #[serde(default)]
    pub key_columns: Option<Vec<String>>,
    /// Target dataset: a `--dataset NAME=ROOT` binding or, when the server runs
    /// with `--workspace`, a relative folder under the workspace root; default:
    /// the server's primary root.
    #[serde(default)]
    pub dataset: Option<String>,
}

/// `observe.join_overlap` (B3): the key overlap of the server's join key.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObserveJoinOverlap {
    /// Target dataset: a `--dataset NAME=ROOT` binding or, when the server runs
    /// with `--workspace`, a relative folder under the workspace root; default:
    /// the server's primary root.
    #[serde(default)]
    pub dataset: Option<String>,
}

/// `observe.join_verify` (B3): whether the folder's join keys are fresh.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObserveJoinVerify {
    /// Target dataset: a `--dataset NAME=ROOT` binding or, when the server runs
    /// with `--workspace`, a relative folder under the workspace root; default:
    /// the server's primary root.
    #[serde(default)]
    pub dataset: Option<String>,
}

/// `observe.plan_from_sql` (WP15b): parse a SQL `SELECT` query and translate
/// it deterministically into a [`bellwether::types::ConsumerPlan`] DAG,
/// validated against the server's bound datasets and stored in the cache.
/// Never executes the SQL string.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObservePlanFromSql {
    /// One read-only SQL `SELECT` query (with optional `WITH` CTEs).
    /// Parsed and translated into a `ConsumerPlan` DAG; never executed.
    pub sql: String,
    /// Table name in `sql` that maps to the `"main"` dataset; default:
    /// inferred when the query scans a single table.
    #[serde(default)]
    pub main_table: Option<String>,
    /// Translate latest-per-key (`QUALIFY row_number() ... = 1` or `arg_max`)
    /// into `as_of` instead of `latest`.
    #[serde(default)]
    pub as_of: bool,
    /// Target dataset: a `--dataset NAME=ROOT` binding or, when the server runs
    /// with `--workspace`, a relative folder under the workspace root; default:
    /// the server's primary root.
    #[serde(default)]
    pub dataset: Option<String>,
}
/// One text lookup (B5): rows whose string `column` matches `value` under
/// `op` (`has_token`, `contains`, or `has_prefix`), both case-folded.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TextLookup {
    /// The string column.
    pub column: String,
    /// `has_token` (one word), `contains` (a substring), or `has_prefix`
    /// (a word starting with `value`).
    pub op: TextOp,
    /// The word, substring, or prefix.
    pub value: String,
}

impl From<TextLookup> for TextQuery {
    fn from(t: TextLookup) -> Self {
        Self {
            column: t.column,
            op: t.op,
            value: t.value,
        }
    }
}

/// `observe.text`: which files and row groups hold rows matching a text
/// lookup, through the folder's text keys.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObserveText {
    /// The string column.
    pub column: String,
    /// `has_token` (one word), `contains` (a substring), or `has_prefix`
    /// (a word starting with `value`).
    pub op: TextOp,
    /// The word, substring, or prefix.
    pub value: String,
    /// Plan against the committed manifest at this sequence (a plan's
    /// `sequence`) instead of the current one. Default: the current
    /// manifest. A sequence the index no longer retains is refused with
    /// `not_retained`.
    #[serde(default)]
    pub at_manifest: Option<u64>,
    /// Target dataset: a `--dataset NAME=ROOT` binding or, when the server runs
    /// with `--workspace`, a relative folder under the workspace root; default:
    /// the server's primary root.
    #[serde(default)]
    pub dataset: Option<String>,
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
    /// Inline key tuples (one array of JSON scalars per key tuple, in
    /// `key_columns` order) instead of `keys`, recorded as one run.
    #[serde(default)]
    pub key_values: Option<Vec<Vec<Value>>>,
    /// Further key files (read, never written), each recorded as one more
    /// run of the same lookup as `keys` (`why --from` repeated); default:
    /// none.
    #[serde(default)]
    pub more_keys: Vec<String>,
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
    /// Price `keys` as list membership (B4; `why --mode contains-any` /
    /// `contains-all`): the key file holds one column, named after a list
    /// (or map) column, of its element type, and the floor candidate is
    /// the list key on it. With `workload`, its runs must already be
    /// membership runs of that kind.
    #[serde(default)]
    pub contains: Option<Membership>,
    /// Price `keys` (or `workload`'s semi-join runs) as a semi-join (B3;
    /// `why --mode semijoin`) with the right side of the join key the server
    /// was started with (`--join`): the floor candidate is that join key,
    /// priced from both datasets' complete exact keys.
    #[serde(default)]
    pub join: bool,
    /// Price `keys` as one aggregate lookup (B7; `why --mode aggregate
    /// --value V --fn F`): per key, `functions` of `value`, with the agg
    /// key on the key columns as the candidate once a key counting rows on
    /// exactly those columns is complete (else that counted key first).
    /// With `workload`, its runs must already be these aggregate runs.
    #[serde(default)]
    pub aggregate: Option<AggregateArg>,
    /// Run in this call instead of returning a job id.
    #[serde(default)]
    pub wait: bool,
    /// Text lookups (B5), recorded as the workload instead of `keys` or
    /// `workload`: one run each, priced under the current configuration and
    /// each candidate text key (`text:<column>,tokenizer=words` for words
    /// and word prefixes, `tokenizer=trigram` for substrings); default:
    /// none.
    #[serde(default)]
    pub text: Vec<TextLookup>,
    /// Price `keys` under a bitemporal lookup (`why --valid-by`; `asof
    /// --valid --known`): the valid-time column. Needs `known_by`; the floor
    /// candidate is `temporal2:<key columns>@<valid_by>,<known_by>`. A
    /// bitemporal run is priced over an index.
    #[serde(default)]
    pub valid_by: Option<String>,
    /// The known-time column of a bitemporal lookup (`why --known-by`; with
    /// `valid_by`).
    #[serde(default)]
    pub known_by: Option<String>,
    /// The valid-time bound of a bitemporal lookup (`why --valid`):
    /// RFC 3339, a date, or an integer in `valid_by`'s unit; default:
    /// unbounded.
    #[serde(default)]
    pub valid: Option<String>,
    /// The known-time bound of a bitemporal lookup (`why --known`), in
    /// `known_by`'s unit likewise; default: unbounded.
    #[serde(default)]
    pub known: Option<String>,
    /// Price `keys` as `latest` lookups (`why --latest-by`): each key's
    /// newest version by this time column, the floor candidate
    /// `temporal:<key columns>@<latest_by>` (with `tombstone`, the live
    /// rows of it, through `temporal:<key columns>@<latest_by>,tombstone=
    /// <column>`). Takes no `valid_by` or `known_by`.
    #[serde(default)]
    pub latest_by: Option<String>,
    /// With `latest_by` or a bitemporal lookup (B8; `why --tombstone`): the
    /// runs keep only live rows, `<column>` (a boolean column whose true
    /// marks a retracted row) or `<column>=<value>`; the floor candidate is
    /// `temporal:<key columns>@<latest_by>,tombstone=<column>` (or
    /// `temporal2:<key columns>@<valid_by>,<known_by>,tombstone=<column>`).
    /// Default: none (every row of the newest version).
    #[serde(default)]
    pub tombstone: Option<String>,
    /// A consumer plan (relational replay; `why --plan`): a JSON file of a
    /// typed plan DAG (read, never written). Each run of `keys` is priced
    /// through it under the current configuration and each candidate key
    /// drawn from the columns its filters, joins, windows, and aggregates
    /// name; the result adds its operator table. Takes `keys` and no
    /// lookup option but `at`.
    #[serde(default)]
    pub plan: Option<String>,
    /// The datasets `plan` scans besides the data folder, by the names the
    /// human bound when starting the server (`--dataset NAME=ROOT`): names
    /// only, never a folder; default: every dataset the plan names.
    #[serde(default)]
    pub datasets: Vec<String>,
    /// The instant of `plan`'s `as_of` nodes (`why --plan --at`), or with
    /// `latest_by` the instant of an `asof` lookup: RFC 3339, a date, or an
    /// integer in the version column's unit; default: none.
    #[serde(default)]
    pub at: Option<String>,
    /// Target dataset: a `--dataset NAME=ROOT` binding or, when the server runs
    /// with `--workspace`, a relative folder under the workspace root; default:
    /// the server's primary root.
    #[serde(default)]
    pub dataset: Option<String>,
}

/// What an aggregate lookup computes per key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AggregateArg {
    /// The column to aggregate.
    pub value: String,
    /// The functions (`sum`, `count`, `min`, `max`); `count` is always
    /// computed. Default: `sum`, `count` (as `why --fn`).
    #[serde(default)]
    pub functions: Option<Vec<AggregateFn>>,
}

/// The functions an aggregate computes when none are given: `sum` and
/// `count`, as the CLI's `--fn` defaults.
#[must_use]
pub fn default_functions() -> Vec<AggregateFn> {
    vec![AggregateFn::Sum, AggregateFn::Count]
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
    /// Target dataset: a `--dataset NAME=ROOT` binding or, when the server runs
    /// with `--workspace`, a relative folder under the workspace root; default:
    /// the server's primary root.
    #[serde(default)]
    pub dataset: Option<String>,
}

/// `score.advise`: workload-level physical design advisor (milestone M6).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScoreAdvise {
    /// A single recorded workload hash.
    #[serde(default)]
    pub workload: Option<Hash>,
    /// Multiple recorded workload hashes.
    #[serde(default)]
    pub workloads: Vec<Hash>,
    /// A Parquet file of keys to record as a workload (read, never written).
    #[serde(default)]
    pub keys: Option<String>,
    /// Key columns to take from `keys`, in tuple order; default: all.
    #[serde(default)]
    pub key_columns: Option<Vec<String>>,
    /// Further key files (read, never written), each recorded as one more run.
    #[serde(default)]
    pub more_keys: Vec<String>,
    /// Maximum number of key specs in a joint configuration D; default 3.
    #[serde(default)]
    pub max_keys: Option<u32>,
    /// Optional cap on index segment bytes m(D).
    #[serde(default)]
    pub max_index_bytes: Option<u64>,
    /// Preserve-and-extend per-run regression allowance δ on anchor runs; default 0.05.
    #[serde(default)]
    pub delta: Option<f64>,
    /// Optional key-count gates to evaluate and tune.
    #[serde(default)]
    pub gates: Vec<Gate>,
    /// Run in this call instead of returning a job id.
    #[serde(default)]
    pub wait: bool,
    /// Target dataset: a `--dataset NAME=ROOT` binding or, when the server runs
    /// with `--workspace`, a relative folder under the workspace root; default:
    /// the server's primary root.
    #[serde(default)]
    pub dataset: Option<String>,
}

/// `propose.key`: propose an index key.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProposeKey {
    /// The key.
    pub spec: KeyArg,
    /// Further keys applied in the same commit as `spec` (the proposal's
    /// `sidecars`), each in either form; every name distinct.
    #[serde(default)]
    pub sidecars: Vec<KeyArg>,
    /// Evidence hashes the proposal cites; at least one.
    pub evidence: Vec<Hash>,
    /// Who proposes: your agent name. `engine` is reserved.
    pub by: String,
    /// Return what would be written, and write nothing.
    #[serde(default)]
    pub dry_run: bool,
    /// Target dataset: a `--dataset NAME=ROOT` binding or, when the server runs
    /// with `--workspace`, a relative folder under the workspace root; default:
    /// the server's primary root.
    #[serde(default)]
    pub dataset: Option<String>,
}

/// The key `propose.key` proposes: the contract's `KeySpec` as JSON, or the
/// short form `bellwether index --key` and `bellwether propose --key` read.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum KeyArg {
    /// The short form: exact:a,b, exact:a@ts (per-key time bounds),
    /// bloom:a@0.01, range:ts, temporal:id@ts, temporal2:id@valid,known, or
    /// any other `index --key` spelling. A sketch (sketch:a) is refused with
    /// `sketch_not_proposable`: it is approximate and never planned with.
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
    /// Target dataset: a `--dataset NAME=ROOT` binding or, when the server runs
    /// with `--workspace`, a relative folder under the workspace root; default:
    /// the server's primary root.
    #[serde(default)]
    pub dataset: Option<String>,
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
    /// Target dataset: a `--dataset NAME=ROOT` binding or, when the server runs
    /// with `--workspace`, a relative folder under the workspace root; default:
    /// the server's primary root.
    #[serde(default)]
    pub dataset: Option<String>,
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
    /// Target dataset: a `--dataset NAME=ROOT` binding or, when the server runs
    /// with `--workspace`, a relative folder under the workspace root; default:
    /// the server's primary root.
    #[serde(default)]
    pub dataset: Option<String>,
}

/// `apply.refresh`: rebuild stale index specs and stale join keys on the
/// server's data folder (`bellwether index` with no new declarations).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ApplyRefresh {
    /// Check folder changes and join-key freshness and return what would be
    /// refreshed, without writing anything.
    #[serde(default)]
    pub dry_run: bool,
    /// Run in this call instead of returning a job id.
    #[serde(default)]
    pub wait: bool,
    /// Target dataset: a `--dataset NAME=ROOT` binding or, when the server runs
    /// with `--workspace`, a relative folder under the workspace root; default:
    /// the server's primary root.
    #[serde(default)]
    pub dataset: Option<String>,
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
            "Plan which files and row groups hold the keys in a Parquet key file (or, with \
             contains, the rows whose list holds them), against the current or a committed \
             manifest (at_manifest). Writes the plan Evidence and the key set to the cache only.",
        ),
        def::<ObserveCount>(
            "observe.count",
            Tier::Observe,
            true,
            "Count the rows holding the keys in a Parquet key file (count(*) where key in ...), \
             exact, from the per-key counts of a key spec with counts on exactly those columns \
             (exact:<cols>,counts): rows, row groups, files. No data file is read. Writes \
             nothing; refused with not_indexed when no such spec is complete.",
        ),
        def::<ObserveAggregate>(
            "observe.aggregate",
            Tier::Observe,
            true,
            "Per key, sum, count, min, and max of one column over the keys in a Parquet key file \
             (fn(value) ... group by key), from the index's agg key beside a key counting rows \
             on exactly the key columns (agg:<cols>@<value>{...} and exact:<cols>,counts). \
             Exact for counts, integer and decimal sums, min and max; a float sum is labelled \
             measured (agg.kahan_f64). Where the counts do not prove a row group answered, the \
             result names the row groups still to read. No data file is read. Writes nothing; \
             refused with not_indexed naming the key to declare when none serves.",
        ),
        def::<ObserveAsof>(
            "observe.asof",
            Tier::Observe,
            true,
            "Plan the version of each key current at an instant (at), or its bitemporal version \
             valid at valid as known at known (a temporal2 key), against the current manifest or \
             a committed one (at_manifest); with live, only its rows that are not tombstones (the \
             key's declared tombstone). Writes the plan Evidence and the key set to the cache \
             only.",
        ),
        def::<ObserveText>(
            "observe.text",
            Tier::Observe,
            true,
            "Plan which files and row groups hold rows whose string column matches a text \
             lookup (has_token, contains, has_prefix; case-folded), through the folder's text \
             keys. The result says when the plan is a candidate set and the residual is \
             mandatory. Writes the plan Evidence and the lookup to the cache only.",
        ),
        def::<ObserveSemijoin>(
            "observe.semijoin",
            Tier::Observe,
            true,
            "Plan semijoin(keys) through the join key the server was started with (--join): step \
             1, this folder's row groups holding a key the right dataset also holds; step 2, the \
             right dataset's; each with its residual and, with counts, exact rows. Refused with \
             stale_join when either dataset changed since the key was built. Writes one plan \
             Evidence per step and the key set to the cache only.",
        ),
        def::<ObserveJoinOverlap>(
            "observe.join_overlap",
            Tier::Observe,
            true,
            "The key overlap of the server's join key, exact, from its pair index: keys on both \
             datasets, on one only, and row groups (with counts, rows) per shared key. Refused \
             with stale_join when either dataset changed since it was built. Writes nothing.",
        ),
        def::<ObserveJoinVerify>(
            "observe.join_verify",
            Tier::Observe,
            true,
            "Whether each join key of the folder (or the server's --join key) is fresh, or stale \
             on the left or right, with the command that rebuilds it. Writes nothing.",
        ),
        def::<ObservePlanFromSql>(
            "observe.plan_from_sql",
            Tier::Observe,
            true,
            "Parse a SQL SELECT query into a typed ConsumerPlan DAG (plan, hash, datasets, \
             cached), validated against the bound datasets and stored in the cache. Never \
             executes the SQL string; refuses unsupported SQL constructs by name.",
        ),
        def::<ScoreWhy>(
            "score.why",
            Tier::Score,
            false,
            "Engine-scored why: replay the keys (or a recorded workload) under the current \
             configuration and every candidate key, as point lookups or, with valid_by and \
             known_by, bitemporal ones, or with latest_by, latest ones (with tombstone, only their live \
             rows); returns the funnel, the culprit, the recommended key, and \
             workload.hash. Writes engine Proposals and Replays. Charged to the session budget. \
             Returns a job id unless wait is true.",
        ),
        def::<ScoreVerify>(
            "score.verify",
            Tier::Score,
            false,
            "The engine scores a proposal (a Replay) and verifies it (a Certificate, written \
             passing or failing); nothing is applied. Pass the certificate hash to \
             apply.receipt. Charged to the session budget. Returns a job id unless wait is true.",
        ),
        def::<ScoreAdvise>(
            "score.advise",
            Tier::Score,
            false,
            "Workload-level physical design advisor (M6): rule-driven key enumeration, \
             subsumption, exact bloom FPR and gate descent, and Pareto frontier with anchors \
             and delta. Writes engine Proposals and Replays for every frontier member so you \
             can call score.verify and apply.receipt on any frontier member. Charged to the \
             session budget. Returns a job id unless wait is true.",
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
        def::<ApplyRefresh>(
            "apply.refresh",
            Tier::Apply,
            false,
            "Rebuild stale declared key specs, sidecars, and join keys over the data folder's \
             new, changed, and removed files in one manifest commit (`bellwether index`). \
             Charged to the session budget. With dry_run, checks folder changes and join \
             freshness and writes nothing. Returns a job id unless wait or dry_run is true.",
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
