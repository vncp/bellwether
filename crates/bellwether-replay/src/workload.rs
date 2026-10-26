//! The workload recorder: recorded runs' key sets in the cache, named by
//! their BLAKE3, and a `workload.json` manifest naming them (ADR 005).
//!
//! # Identity
//!
//! A run's key set is stored under the BLAKE3 of its payload
//! ([`RunKeys::to_payload`]). The workload's identity is the BLAKE3 of the
//! canonical JSON of its [`WorkloadManifest`]: the format, the encoding
//! version, and every run's id, template, query kind, key columns, key
//! types, key count, dropped-null count, and key-set hash, with runs sorted
//! by id. Two recordings of the same keys therefore have the same hash
//! however they were fed in (an Arrow batch or a Parquet file, in any run
//! order), and a changed key, column, type, kind, or run id changes it. The
//! manifest bytes are stored in the cache under that hash too, so a `Replay`
//! record's `workload` hash resolves to everything needed to re-run it.
//! Every read checks the BLAKE3 of the bytes it got against the hash it
//! asked for, whatever [`KeySetSource`] served them.
//!
//! `workload.json` is the same manifest plus two derived fields for humans
//! and tools: the hash and the nearest-rank key-set sizes at p50, p95, and
//! max. Reading it back recomputes both and refuses a file whose recorded
//! values disagree.
//!
//! # Templates and query kinds
//!
//! A run's template groups it in reports. Without one, the template is the
//! run's key-column set, `columns(<key columns>)`: a grouping by columns, not
//! by query; pass the query or pipeline step a run came from to group by
//! that. Every run records a query kind: `locate`, `latest`, or `asof`
//! (contract v3 backlog #14), the latter two optionally with a lookback
//! window, or `window` (the rows of its keys whose time column lies in the
//! run's [`RunWindow`], which only a window run records). A workload naming
//! another kind is refused. A recorder made [`WorkloadRecorder::in_window`]
//! records every `locate` run as a window run.
//!
//! # The bench workload format
//!
//! [`record_bench_workload`] reads `bellwether-bench`'s `workload.json`
//! (`format: "bellwether-bench-workload/1"`) and its run files directly, in
//! two passes. The first verifies everything the bench recorded and writes
//! nothing: each run's hash (BLAKE3 over its sorted `u64` keys as 8
//! big-endian bytes), its key count, the workload hash (BLAKE3 over the run
//! hashes in order), each run file's location inside the corpus, and, when
//! `corpus.json` sits next to it, the corpus id. Only then does the second
//! pass record the runs, re-verifying each run's hash as it reads it again.
//! A mismatch is a [`ReplayError::BenchMismatch`]. The bench hashes are
//! type-specific (`u64` only); the replay hashes above are over canonical
//! encodings of any key type, so the two differ by design and both are
//! checked.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatchReader;
use bellwether_core::segment::ColumnType;
use bellwether_store::{Cache, StoreExt};
use bellwether_types::format::ENCODING_VERSION;
use bellwether_types::{AggregateFn, Error, Hash, Result, TextOp};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};

use crate::error::{ReplayError, ReplayResult};
use crate::keys::{check_distinct_columns, RunKeys, RunKeysBuilder};
use crate::report::p50_p95_max;

/// Format tag of a replay workload manifest.
pub const WORKLOAD_FORMAT: &str = "bellwether-replay-workload/1";
const WORKLOAD_FORMAT_PREFIX: &str = "bellwether-replay-workload/";
/// Format tag of `bellwether-bench`'s `workload.json`.
pub const BENCH_WORKLOAD_FORMAT: &str = "bellwether-bench-workload/1";
const BENCH_WORKLOAD_FORMAT_PREFIX: &str = "bellwether-bench-workload/";
/// The query kind of a recorded run that looks up every row holding a key
/// (`locate`).
pub const QUERY_KIND_LOCATE: &str = "locate";
/// The query kind of a recorded run that looks up the newest version of each
/// key (`latest`).
pub const QUERY_KIND_LATEST: &str = "latest";
/// The query kind of a recorded run that looks up the version of each key
/// current at an instant (`asof`).
pub const QUERY_KIND_ASOF: &str = "asof";
/// The query kind of a recorded run that looks keys up in a time window
/// (`window`, [`RunWindow`]).
pub const QUERY_KIND_WINDOW: &str = "window";
/// The query kind of a recorded run that counts the rows holding its keys
/// (`count(*) where key in ...`, `bellwether count`).
pub const QUERY_KIND_COUNT: &str = "count";
/// The query kind of a recorded run that looks up the rows of a string
/// column holding one of its words (`has_token`; B5, `has`).
pub const QUERY_KIND_HAS_TOKEN: &str = "has_token";
/// The query kind of a recorded run that looks up the rows of a string
/// column containing one of its substrings (`contains`, `like '%x%'`; B5).
pub const QUERY_KIND_CONTAINS: &str = "contains";
/// The query kind of a recorded run that looks up the rows of a string
/// column holding a word that starts with one of its prefixes (`has_prefix`;
/// B5).
pub const QUERY_KIND_HAS_PREFIX: &str = "has_prefix";
/// The query kind of a recorded run that looks up the rows whose list
/// column holds any of its values (`contains_any`: `array_contains`,
/// `has_any`, `exists(unnest)`; B4).
pub const QUERY_KIND_CONTAINS_ANY: &str = "contains_any";
/// The query kind of a recorded run that looks up the rows whose list
/// column holds every one of its values (`contains_all`; B4).
pub const QUERY_KIND_CONTAINS_ALL: &str = "contains_all";
/// The query kind of a recorded run that semi-joins its keys with a right
/// dataset (`semijoin`: `in (subquery)`, `exists`, a semi-join; B3): its
/// keys are values of the run's columns in the left dataset (the data
/// root), and the run asks for the rows of both datasets holding a key the
/// other also holds. The right side is [`RecordedRun::join`].
pub const QUERY_KIND_SEMIJOIN: &str = "semijoin";

/// The right side of a semi-join run (B3): the right dataset (the folder of
/// that name beside the data root) and its join columns, pairwise with the
/// run's columns.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RunJoin {
    /// The right dataset's name.
    pub right: String,
    /// Its join columns, pairwise with the run's key columns.
    pub right_columns: Vec<String>,
}

/// The query kind of a recorded run that aggregates one value column per
/// key over its keys (`sum/count/min/max(value) ... where key in (...)
/// group by key`; B7), with the value and functions in
/// [`RecordedRun::aggregate`].
pub const QUERY_KIND_AGGREGATE: &str = "aggregate";

/// What an `aggregate` run computes per key: `functions` of `value`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunAggregate {
    /// The aggregated column.
    pub value: String,
    /// The functions, in the order sum, count, min, max, `count` always
    /// among them (`AggregateFn::canonical`).
    pub functions: Vec<AggregateFn>,
}

impl RunAggregate {
    /// `functions` of `value`, the functions put in canonical order with
    /// `count` among them.
    #[must_use]
    pub fn new(value: &str, functions: &[AggregateFn]) -> Self {
        Self {
            value: value.to_string(),
            functions: AggregateFn::canonical(functions),
        }
    }
}

/// The time window of a `window` run: the rows of its keys whose
/// `ts_column` lies in `[lo, hi]`, both inclusive and in the column's
/// dataset unit (days for `Date32`, milliseconds for `Date64`, the
/// timestamp's own unit, an integer column's value itself).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunWindow {
    /// The time column.
    pub ts_column: String,
    /// Inclusive lower bound.
    pub lo: i64,
    /// Inclusive upper bound.
    pub hi: i64,
}

/// What a recorded run asked of the index.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum RunQuery {
    /// Every row holding a key ([`QUERY_KIND_LOCATE`]).
    #[default]
    Locate,
    /// The newest version of each key ([`QUERY_KIND_LATEST`]).
    Latest,
    /// The version of each key current at `ts`, in the timestamp column's
    /// dataset unit ([`QUERY_KIND_ASOF`]).
    Asof {
        /// The instant, in the timestamp column's dataset unit.
        ts: i64,
    },
    /// Every version of each key in a lookback window: `[asof − lookback,
    /// asof]` when `asof` is set ([`QUERY_KIND_ASOF`]), else `[newest −
    /// lookback, newest]` per key, `newest` being the key's newest version
    /// ([`QUERY_KIND_LATEST`]). Both bounds inclusive, in the timestamp
    /// column's dataset unit. With `asof`, it is the window
    /// `[asof − lookback, asof]` on the serving temporal spec's timestamp
    /// column, planned as a [`RunQuery::Window`] is.
    Lookback {
        /// The asof instant, or `None` to end each key's window at its
        /// newest version.
        asof: Option<i64>,
        /// The window's length, `>= 0`.
        lookback: i64,
    },
    /// The rows of the keys whose time column lies in a fixed window
    /// ([`QUERY_KIND_WINDOW`]).
    Window(RunWindow),
    /// The number of rows holding the keys ([`QUERY_KIND_COUNT`]): planned
    /// as a `locate`, and read from the index at no file cost when a
    /// serving spec stores counts on exactly the run's key columns.
    Count,
    /// The rows whose string column (the run's one key column) matches one
    /// of the run's values under the text match ([`QUERY_KIND_HAS_TOKEN`],
    /// [`QUERY_KIND_CONTAINS`], [`QUERY_KIND_HAS_PREFIX`]; B5): the values
    /// are the words, substrings, or prefixes, both sides case-folded. Only
    /// a text key can plan it.
    Text(TextOp),
    /// The rows whose list column (the run's one key column, a list or map
    /// in the dataset) holds at least one of the run's values
    /// ([`QUERY_KIND_CONTAINS_ANY`]).
    ContainsAny,
    /// The rows whose list column holds every one of the run's values
    /// ([`QUERY_KIND_CONTAINS_ALL`]).
    ContainsAll,
    /// The rows of both datasets whose key the other holds, for the run's
    /// keys ([`QUERY_KIND_SEMIJOIN`]; B3): a two-step plan, the left
    /// dataset then the right. Only a join key on the pair answers it
    /// alone; otherwise each side is planned by its own keys.
    SemiJoin(RunJoin),
    /// Per key, `functions` of one value column over the key's rows
    /// ([`QUERY_KIND_AGGREGATE`]; B7): answered from an agg key on the value
    /// beside the key counting rows on exactly the run's columns, reading
    /// only the row groups their counts do not prove answered; else
    /// planned, and read, as a `locate`.
    Aggregate(RunAggregate),
}

impl RunQuery {
    /// The `query_kind` tag (`locate`, `latest`, `asof`, `window`,
    /// `contains_any`, `contains_all`, or `aggregate`).
    #[must_use]
    pub fn kind_str(&self) -> &'static str {
        match self {
            Self::Locate => QUERY_KIND_LOCATE,
            Self::Latest | Self::Lookback { asof: None, .. } => QUERY_KIND_LATEST,
            Self::Asof { .. } | Self::Lookback { asof: Some(_), .. } => QUERY_KIND_ASOF,
            Self::Window(_) => QUERY_KIND_WINDOW,
            Self::Count => QUERY_KIND_COUNT,
            Self::Text(TextOp::HasToken) => QUERY_KIND_HAS_TOKEN,
            Self::Text(TextOp::Contains) => QUERY_KIND_CONTAINS,
            Self::Text(TextOp::HasPrefix) => QUERY_KIND_HAS_PREFIX,
            Self::ContainsAny => QUERY_KIND_CONTAINS_ANY,
            Self::ContainsAll => QUERY_KIND_CONTAINS_ALL,
            Self::SemiJoin(_) => QUERY_KIND_SEMIJOIN,
            Self::Aggregate(_) => QUERY_KIND_AGGREGATE,
        }
    }

    /// The `asof` instant when this is [`RunQuery::Asof`] or an asof
    /// [`RunQuery::Lookback`].
    #[must_use]
    pub fn asof_ts(&self) -> Option<i64> {
        match self {
            Self::Asof { ts } => Some(*ts),
            Self::Lookback { asof, .. } => *asof,
            Self::Locate
            | Self::Latest
            | Self::Window(_)
            | Self::Count
            | Self::Text(_)
            | Self::ContainsAny
            | Self::ContainsAll
            | Self::SemiJoin(_)
            | Self::Aggregate(_) => None,
        }
    }

    /// The lookback window's length when this is a [`RunQuery::Lookback`].
    #[must_use]
    pub fn lookback(&self) -> Option<i64> {
        match self {
            Self::Lookback { lookback, .. } => Some(*lookback),
            Self::Locate
            | Self::Latest
            | Self::Asof { .. }
            | Self::Window(_)
            | Self::Count
            | Self::Text(_)
            | Self::ContainsAny
            | Self::ContainsAll
            | Self::SemiJoin(_)
            | Self::Aggregate(_) => None,
        }
    }

    /// The fixed window of a [`RunQuery::Window`].
    #[must_use]
    pub fn window(&self) -> Option<&RunWindow> {
        match self {
            Self::Window(w) => Some(w),
            _ => None,
        }
    }

    /// The text match of a [`RunQuery::Text`] run.
    #[must_use]
    pub fn text_op(&self) -> Option<TextOp> {
        match self {
            Self::Text(op) => Some(*op),
            _ => None,
        }
    }

    /// What an [`RunQuery::Aggregate`] run computes.
    #[must_use]
    pub fn aggregate(&self) -> Option<&RunAggregate> {
        match self {
            Self::Aggregate(a) => Some(a),
            _ => None,
        }
    }

    /// Whether this asks for versions (`latest`, `asof`, or a lookback), so
    /// only a temporal key spec can plan it.
    #[must_use]
    pub fn is_temporal(&self) -> bool {
        matches!(
            self,
            Self::Latest | Self::Asof { .. } | Self::Lookback { .. }
        )
    }

    /// Whether this asks which rows' list column holds the run's values
    /// (`contains_any` or `contains_all`), so only a list key can plan it.
    #[must_use]
    pub fn is_membership(&self) -> bool {
        matches!(self, Self::ContainsAny | Self::ContainsAll)
    }

    /// The right side of a semi-join run (B3).
    #[must_use]
    pub fn join(&self) -> Option<&RunJoin> {
        match self {
            Self::SemiJoin(j) => Some(j),
            _ => None,
        }
    }
}

/// One recorded run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedRun {
    /// Identifier of the run, unique within the workload.
    pub run_id: String,
    /// The group the run is reported in. Defaults to the run's key-column
    /// set, `columns(<key columns>)`: a grouping by columns, not by query;
    /// recorders that know the query or pipeline step pass it instead.
    pub template: String,
    /// What the run asked of the index: [`QUERY_KIND_LOCATE`],
    /// [`QUERY_KIND_LATEST`], [`QUERY_KIND_ASOF`], [`QUERY_KIND_WINDOW`],
    /// [`QUERY_KIND_AGGREGATE`], or another `QUERY_KIND_*`.
    pub query_kind: String,
    /// For [`QUERY_KIND_ASOF`] runs: the instant in the timestamp column's
    /// dataset unit. Absent (`None`, omitted in JSON) for `locate` and
    /// `latest` runs, so existing workload manifests serialize and hash to
    /// identical bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub asof: Option<i64>,
    /// For a [`RunQuery::Lookback`] run: the lookback window's length in the
    /// timestamp column's dataset unit. Absent (`None`, omitted in JSON)
    /// otherwise, so existing workload manifests serialize and hash to
    /// identical bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lookback: Option<i64>,
    /// Key columns, in tuple order.
    pub columns: Vec<String>,
    /// The dataset type each key column was encoded at.
    pub key_types: Vec<ColumnType>,
    /// Distinct non-null key tuples.
    pub keys: u64,
    /// Input rows dropped because their tuple held a null (ADR 014).
    pub null_tuples: u64,
    /// BLAKE3 of the key-set payload stored in the cache.
    pub key_set: Hash,
    /// The window of a `window` run; absent (and not serialized, so a
    /// `locate` run hashes as before) otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<RunWindow>,
    /// The right side of a semi-join run ([`QUERY_KIND_SEMIJOIN`]; B3);
    /// absent (and not serialized, so every other run hashes as before)
    /// otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub join: Option<RunJoin>,
    /// The value and functions of an `aggregate` run; absent (and not
    /// serialized, so other runs hash as before) otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregate: Option<RunAggregate>,
}

impl RecordedRun {
    /// The typed query kind of this run, or an error if `query_kind`,
    /// `asof`, `lookback`, and `window` disagree or name an unknown kind.
    #[allow(clippy::too_many_lines)] // one arm per query kind
    pub fn query(&self) -> Result<RunQuery> {
        if self.query_kind == QUERY_KIND_WINDOW {
            return match (&self.window, self.asof, self.lookback) {
                (Some(w), None, None) if !w.ts_column.is_empty() && w.lo <= w.hi => {
                    Ok(RunQuery::Window(w.clone()))
                }
                (Some(w), None, None) => Err(Error::Other(format!(
                    "run {:?} records the window {}..{} of {:?}; a window names its time \
                     column and starts at or before its end",
                    self.run_id, w.lo, w.hi, w.ts_column
                ))),
                _ => Err(Error::Other(format!(
                    "run {:?} is a window run; it records exactly one window and no asof or \
                     lookback",
                    self.run_id
                ))),
            };
        }
        if self.window.is_some() {
            return Err(Error::Other(format!(
                "run {:?} has query_kind {:?} but records a window; only a window run records \
                 one",
                self.run_id, self.query_kind
            )));
        }
        if self.query_kind == QUERY_KIND_SEMIJOIN {
            return match &self.join {
                Some(j)
                    if self.asof.is_none()
                        && self.lookback.is_none()
                        && j.right_columns.len() == self.columns.len() =>
                {
                    Ok(RunQuery::SemiJoin(j.clone()))
                }
                _ => Err(Error::Other(format!(
                    "run {:?} is a semijoin run; it records its right dataset and one right \
                     column per key column, and no asof, lookback, or window",
                    self.run_id
                ))),
            };
        }
        if self.query_kind == QUERY_KIND_AGGREGATE {
            return match (&self.aggregate, self.asof, self.lookback) {
                (Some(a), None, None)
                    if !a.value.is_empty()
                        && a.functions == AggregateFn::canonical(&a.functions)
                        && !self.columns.contains(&a.value) =>
                {
                    Ok(RunQuery::Aggregate(a.clone()))
                }
                (Some(a), None, None) => Err(Error::Other(format!(
                    "run {:?} aggregates {:?} with {:?}; an aggregate run names a value column \
                     that is not a key column and lists its functions once each in the order \
                     sum, count, min, max, count among them",
                    self.run_id, a.value, a.functions
                ))),
                _ => Err(Error::Other(format!(
                    "run {:?} is an aggregate run; it records its value and functions and no \
                     asof, lookback, or window",
                    self.run_id
                ))),
            };
        }
        if self.join.is_some() {
            return Err(Error::Other(format!(
                "run {:?} has query_kind {:?} but records a right dataset; only a semijoin run \
                 records one",
                self.run_id, self.query_kind
            )));
        }
        if self.aggregate.is_some() {
            return Err(Error::Other(format!(
                "run {:?} has query_kind {:?} but records an aggregate; only an aggregate run \
                 records one",
                self.run_id, self.query_kind
            )));
        }
        if matches!(
            self.query_kind.as_str(),
            QUERY_KIND_CONTAINS_ANY | QUERY_KIND_CONTAINS_ALL
        ) {
            if self.asof.is_some() || self.lookback.is_some() || self.columns.len() != 1 {
                return Err(Error::Other(format!(
                    "run {:?} is a {} run; it looks up one list column and records no asof, \
                     lookback, or window",
                    self.run_id, self.query_kind
                )));
            }
            return Ok(if self.query_kind == QUERY_KIND_CONTAINS_ANY {
                RunQuery::ContainsAny
            } else {
                RunQuery::ContainsAll
            });
        }
        let text = match self.query_kind.as_str() {
            QUERY_KIND_HAS_TOKEN => Some(TextOp::HasToken),
            QUERY_KIND_CONTAINS => Some(TextOp::Contains),
            QUERY_KIND_HAS_PREFIX => Some(TextOp::HasPrefix),
            _ => None,
        };
        if let Some(op) = text {
            if self.asof.is_some() || self.lookback.is_some() || self.columns.len() != 1 {
                return Err(Error::Other(format!(
                    "run {:?} is a {} run; it looks up one string column and records no asof, \
                     lookback, or window",
                    self.run_id, self.query_kind
                )));
            }
            return Ok(RunQuery::Text(op));
        }
        let point = match (self.query_kind.as_str(), self.asof) {
            (QUERY_KIND_LOCATE, None) => RunQuery::Locate,
            (QUERY_KIND_COUNT, None) => RunQuery::Count,
            (QUERY_KIND_LATEST, None) => RunQuery::Latest,
            (QUERY_KIND_ASOF, Some(ts)) => RunQuery::Asof { ts },
            (QUERY_KIND_LOCATE | QUERY_KIND_LATEST | QUERY_KIND_COUNT, Some(_)) => {
                return Err(Error::Other(format!(
                    "run {:?} has query_kind {:?} but sets asof",
                    self.run_id, self.query_kind
                )))
            }
            (QUERY_KIND_ASOF, None) => {
                return Err(Error::Other(format!(
                    "run {:?} has query_kind \"asof\" without an asof timestamp",
                    self.run_id
                )))
            }
            (other, _) => {
                return Err(Error::UnknownKind {
                    what: "query kind",
                    found: other.to_string(),
                })
            }
        };
        match (point, self.lookback) {
            (q, None) => Ok(q),
            (q @ (RunQuery::Locate | RunQuery::Count), Some(_)) => Err(Error::Other(format!(
                "run {:?} has query_kind {:?} but sets a lookback; a lookback window applies \
                 to latest and asof runs",
                self.run_id,
                q.kind_str()
            ))),
            (_, Some(l)) if l < 0 => Err(Error::Other(format!(
                "run {:?} has a negative lookback {l}",
                self.run_id
            ))),
            (q, Some(lookback)) => Ok(RunQuery::Lookback {
                asof: q.asof_ts(),
                lookback,
            }),
        }
    }
}

/// The canonical workload manifest; its JSON's BLAKE3 is the workload hash.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadManifest {
    /// [`WORKLOAD_FORMAT`].
    pub format: String,
    /// Canonical encoding version the keys were encoded with.
    pub encoding_version: u32,
    /// Runs, sorted by id.
    pub runs: Vec<RecordedRun>,
}

/// `workload.json`: the manifest plus its hash and key-set sizes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadFile {
    /// [`WORKLOAD_FORMAT`].
    pub format: String,
    /// The workload hash (BLAKE3 of the canonical manifest JSON).
    pub hash: Hash,
    /// Canonical encoding version.
    pub encoding_version: u32,
    /// Nearest-rank key-set sizes at p50, p95, max.
    pub keys_p50_p95_max: [u64; 3],
    /// Runs, sorted by id.
    pub runs: Vec<RecordedRun>,
}

/// A validated workload and its hash.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Workload {
    manifest: WorkloadManifest,
    hash: Hash,
}

impl Workload {
    /// Validates `manifest` and computes its hash. Runs must be sorted by
    /// id with no duplicates, every run must name distinct key columns with
    /// one recorded type each, and every query kind must be one this build
    /// knows ([`Error::UnknownKind`] otherwise).
    pub fn new(manifest: WorkloadManifest) -> Result<Self> {
        check_format(
            &manifest.format,
            WORKLOAD_FORMAT_PREFIX,
            WORKLOAD_FORMAT,
            "workload",
        )?;
        if manifest.encoding_version > ENCODING_VERSION {
            return Err(Error::UnsupportedVersion {
                what: "encoding",
                found: manifest.encoding_version,
                supported: ENCODING_VERSION,
            });
        }
        if manifest.encoding_version != ENCODING_VERSION {
            return Err(Error::Other(format!(
                "workload encoding version {} is not {ENCODING_VERSION}",
                manifest.encoding_version
            )));
        }
        for (i, run) in manifest.runs.iter().enumerate() {
            if run.run_id.is_empty() || run.template.is_empty() {
                return Err(Error::Other(
                    "a recorded run needs an id and a template".into(),
                ));
            }
            run.query()?;
            if i > 0 && manifest.runs[i - 1].run_id >= run.run_id {
                return Err(Error::Other(format!(
                    "workload runs must be sorted by id without duplicates; {:?} follows {:?}",
                    run.run_id,
                    manifest.runs[i - 1].run_id
                )));
            }
            if run.columns.is_empty() || run.columns.len() != run.key_types.len() {
                return Err(Error::Other(format!(
                    "run {:?} records {} columns and {} key types",
                    run.run_id,
                    run.columns.len(),
                    run.key_types.len()
                )));
            }
            check_distinct_columns(&run.columns)?;
        }
        let hash = Hash::of_record(&manifest)?;
        Ok(Self { manifest, hash })
    }

    /// The manifest.
    #[must_use]
    pub fn manifest(&self) -> &WorkloadManifest {
        &self.manifest
    }

    /// The workload hash.
    #[must_use]
    pub fn hash(&self) -> Hash {
        self.hash
    }

    /// The runs, sorted by id.
    #[must_use]
    pub fn runs(&self) -> &[RecordedRun] {
        &self.manifest.runs
    }

    /// The run with id `run_id`.
    #[must_use]
    pub fn run(&self, run_id: &str) -> Option<&RecordedRun> {
        self.manifest
            .runs
            .binary_search_by(|r| r.run_id.as_str().cmp(run_id))
            .ok()
            .map(|i| &self.manifest.runs[i])
    }

    /// Nearest-rank key-set sizes at p50, p95, max.
    #[must_use]
    pub fn keys_p50_p95_max(&self) -> [u64; 3] {
        let mut sizes: Vec<u64> = self.manifest.runs.iter().map(|r| r.keys).collect();
        sizes.sort_unstable();
        p50_p95_max(&sizes)
    }

    /// The `workload.json` form.
    #[must_use]
    pub fn to_file(&self) -> WorkloadFile {
        WorkloadFile {
            format: self.manifest.format.clone(),
            hash: self.hash,
            encoding_version: self.manifest.encoding_version,
            keys_p50_p95_max: self.keys_p50_p95_max(),
            runs: self.manifest.runs.clone(),
        }
    }

    /// Validates a `workload.json`: its hash and sizes must equal the ones
    /// recomputed from its runs.
    pub fn from_file(file: WorkloadFile) -> Result<Self> {
        let w = Self::new(WorkloadManifest {
            format: file.format,
            encoding_version: file.encoding_version,
            runs: file.runs,
        })?;
        if w.hash != file.hash {
            return Err(Error::Other(format!(
                "workload.json records hash {} but its runs hash to {}",
                file.hash, w.hash
            )));
        }
        if w.keys_p50_p95_max() != file.keys_p50_p95_max {
            return Err(Error::Other(
                "workload.json key-set sizes disagree with its runs".into(),
            ));
        }
        Ok(w)
    }

    /// Writes `workload.json` (pretty, with a trailing newline).
    pub fn write_json(&self, path: &Path) -> Result<()> {
        let mut bytes = serde_json::to_vec_pretty(&self.to_file())?;
        bytes.push(b'\n');
        std::fs::write(path, bytes).map_err(|e| Error::Other(format!("{}: {e}", path.display())))
    }

    /// Reads and validates `workload.json`.
    pub fn read_json(path: &Path) -> Result<Self> {
        let bytes =
            std::fs::read(path).map_err(|e| Error::Other(format!("{}: {e}", path.display())))?;
        Self::from_file(serde_json::from_slice(&bytes)?)
    }

    /// Loads the workload stored under `hash` (by
    /// [`WorkloadRecorder::finish`]). The stored bytes must hash to `hash`
    /// and be exactly the canonical JSON of a valid manifest.
    pub fn load(keys: &dyn KeySetSource, hash: &Hash) -> Result<Self> {
        let bytes = keys.get(hash)?;
        check_content(&bytes, hash, "workload")?;
        let manifest: WorkloadManifest = serde_json::from_slice(&bytes)?;
        if serde_json::to_vec(&manifest)? != bytes.as_ref() {
            return Err(Error::Other(format!(
                "workload {hash} is not stored in canonical form"
            )));
        }
        let w = Self::new(manifest)?;
        if w.hash != *hash {
            return Err(Error::Other(format!(
                "workload {hash} hashes to {}",
                w.hash
            )));
        }
        Ok(w)
    }

    /// Loads one run's key set and checks it against the run's record: the
    /// BLAKE3 of the bytes (checked here, whatever source served them), then
    /// columns, key types, and key count.
    pub fn run_keys(&self, keys: &dyn KeySetSource, run: &RecordedRun) -> Result<RunKeys> {
        let bytes = keys.get(&run.key_set)?;
        check_content(&bytes, &run.key_set, "key set")?;
        let rk = RunKeys::from_payload(&bytes)?;
        if rk.columns() != run.columns.as_slice()
            || rk.key_types() != run.key_types.as_slice()
            || rk.len() as u64 != run.keys
        {
            return Err(Error::Other(format!(
                "key set {} does not match run {:?}'s record",
                run.key_set, run.run_id
            )));
        }
        Ok(rk)
    }
}

fn check_content(bytes: &[u8], hash: &Hash, what: &str) -> Result<()> {
    let got = Hash::of(bytes);
    if got == *hash {
        Ok(())
    } else {
        Err(Error::Other(format!(
            "{what} {hash} was served bytes that hash to {got}"
        )))
    }
}

/// Where key sets and workload manifests are read from, by hash.
pub trait KeySetSource {
    /// The bytes stored under `hash`; [`Error::NotFound`] if absent. Readers
    /// check the bytes' BLAKE3 against `hash` themselves.
    fn get(&self, hash: &Hash) -> Result<Bytes>;
}

/// Where key sets and workload manifests are written, by hash.
pub trait KeySetSink {
    /// Stores `bytes` under `hash` (their BLAKE3). Idempotent.
    fn put(&self, hash: &Hash, bytes: Bytes) -> Result<()>;
}

impl<S: StoreExt + ?Sized> KeySetSource for Cache<S> {
    fn get(&self, hash: &Hash) -> Result<Bytes> {
        let (bytes, _age) = self.get_cached(hash)?.ok_or(Error::NotFound(*hash))?;
        Ok(bytes)
    }
}

impl<S: StoreExt + ?Sized> KeySetSink for Cache<S> {
    fn put(&self, hash: &Hash, bytes: Bytes) -> Result<()> {
        if Hash::of(&bytes) != *hash {
            return Err(Error::Other(format!("bytes do not hash to {hash}")));
        }
        self.put_cached(hash, bytes)
    }
}

/// An in-process key-set store, for callers without an index root and for
/// tests.
#[derive(Debug, Default, Clone)]
pub struct MemoryKeySets {
    entries: Arc<Mutex<BTreeMap<Hash, Bytes>>>,
}

impl MemoryKeySets {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Entries held.
    pub fn len(&self) -> Result<usize> {
        Ok(self.lock()?.len())
    }

    /// Whether nothing is held.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.lock()?.is_empty())
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, BTreeMap<Hash, Bytes>>> {
        self.entries
            .lock()
            .map_err(|_| Error::Other("key-set store lock poisoned".into()))
    }
}

impl KeySetSource for MemoryKeySets {
    fn get(&self, hash: &Hash) -> Result<Bytes> {
        self.lock()?
            .get(hash)
            .cloned()
            .ok_or(Error::NotFound(*hash))
    }
}

impl KeySetSink for MemoryKeySets {
    fn put(&self, hash: &Hash, bytes: Bytes) -> Result<()> {
        if Hash::of(&bytes) != *hash {
            return Err(Error::Other(format!("bytes do not hash to {hash}")));
        }
        self.lock()?.insert(*hash, bytes);
        Ok(())
    }
}

/// A run's keys as the recorder accepts them: an Arrow batch, or a Parquet
/// file read one batch at a time. Either way every column is a key column.
#[derive(Clone, Copy, Debug)]
pub enum KeyInput<'a> {
    /// A batch whose columns are the key columns.
    Batch(&'a RecordBatch),
    /// A Parquet file whose columns are the key columns.
    Parquet(&'a Path),
}

/// Records runs' key sets into a [`KeySetSink`] and builds the workload.
///
/// Memory (ADR 018): one run's key set at a time, plus one input batch.
pub struct WorkloadRecorder<'a> {
    sink: &'a dyn KeySetSink,
    schema: SchemaRef,
    runs: BTreeMap<String, RecordedRun>,
    window: Option<RunWindow>,
}

impl std::fmt::Debug for WorkloadRecorder<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkloadRecorder")
            .field("runs", &self.runs.len())
            .finish_non_exhaustive()
    }
}

/// A run encoded but not yet stored.
struct Prepared {
    run: RecordedRun,
    payload: Vec<u8>,
}

impl<'a> WorkloadRecorder<'a> {
    /// A recorder writing to `sink`, encoding keys at the types `schema`
    /// (the dataset schema, `Source::schema`) declares.
    pub fn new(sink: &'a dyn KeySetSink, schema: SchemaRef) -> Self {
        Self {
            sink,
            schema,
            runs: BTreeMap::new(),
            window: None,
        }
    }

    /// This recorder, recording every `locate` run as a `window` run over
    /// `window` ([`QUERY_KIND_WINDOW`]). The time column must be in the
    /// dataset schema and the window must not end before it starts.
    pub fn in_window(mut self, window: RunWindow) -> Result<Self> {
        if self.schema.field_with_name(&window.ts_column).is_err() {
            return Err(Error::KeySpec(format!(
                "the window's time column {:?} is not in the dataset schema",
                window.ts_column
            )));
        }
        if window.lo > window.hi {
            return Err(Error::Other(format!(
                "the window {}..{} of {} ends before it starts",
                window.lo, window.hi, window.ts_column
            )));
        }
        self.window = Some(window);
        Ok(self)
    }

    /// Records a [`RunQuery::Locate`] run `run_id` (a [`RunQuery::Window`]
    /// run in a recorder made [`WorkloadRecorder::in_window`]) with the
    /// default template, the run's key-column set.
    pub fn record(&mut self, run_id: &str, keys: KeyInput<'_>) -> Result<&RecordedRun> {
        self.record_with_template(run_id, keys, None)
    }

    /// Records a [`RunQuery::Locate`] run `run_id`: encodes its keys at the
    /// dataset types, drops null tuples (counting them), sorts and
    /// deduplicates, stores the payload in the cache under its BLAKE3, and
    /// returns the run's record. A repeated run id is refused before anything
    /// is written.
    pub fn record_with_template(
        &mut self,
        run_id: &str,
        keys: KeyInput<'_>,
        template: Option<&str>,
    ) -> Result<&RecordedRun> {
        self.record_query(run_id, keys, RunQuery::Locate, template)
    }

    /// Records run `run_id` with query kind `query` (`locate`, `latest`, or
    /// `asof`) and optional `template`.
    pub fn record_query(
        &mut self,
        run_id: &str,
        keys: KeyInput<'_>,
        query: RunQuery,
        template: Option<&str>,
    ) -> Result<&RecordedRun> {
        self.check_new(run_id)?;
        let prepared = self.prepare(run_id, keys, query, template)?;
        self.sink
            .put(&prepared.run.key_set, Bytes::from(prepared.payload))?;
        Ok(self.runs.entry(run_id.to_string()).or_insert(prepared.run))
    }

    fn check_new(&self, run_id: &str) -> Result<()> {
        if run_id.is_empty() {
            return Err(Error::Other("a run id must not be empty".into()));
        }
        if self.runs.contains_key(run_id) {
            return Err(Error::Other(format!("run {run_id:?} is already recorded")));
        }
        Ok(())
    }

    /// Encodes a run without storing anything.
    fn prepare(
        &self,
        run_id: &str,
        keys: KeyInput<'_>,
        query: RunQuery,
        template: Option<&str>,
    ) -> Result<Prepared> {
        let (built, null_tuples) = match keys {
            KeyInput::Batch(batch) => {
                let columns = field_names(&batch.schema());
                let mut b = RunKeysBuilder::new(&columns, &self.schema)?;
                b.push_batch(batch)?;
                let nulls = b.null_tuples();
                (b.finish(), nulls)
            }
            KeyInput::Parquet(path) => read_parquet_keys(path, &self.schema)?,
        };
        if query.lookback().is_some_and(|l| l < 0) {
            return Err(Error::Other(format!(
                "run {run_id:?}: a lookback window cannot be negative"
            )));
        }
        // A recorder made `in_window` records its locate runs as window runs.
        let query = match (query, &self.window) {
            (RunQuery::Locate, Some(w)) => RunQuery::Window(w.clone()),
            (q, None) => q,
            (q, Some(_)) => {
                return Err(Error::Other(format!(
                    "run {run_id:?}: a recorder in a window records locate runs only, not {}",
                    q.kind_str()
                )))
            }
        };
        check_text(run_id, &built, &query, &self.schema)?;
        check_membership(run_id, &built, &query, &self.schema)?;
        if let RunQuery::SemiJoin(j) = &query {
            if j.right_columns.len() != built.columns().len() {
                return Err(Error::KeySpec(format!(
                    "run {run_id:?} semi-joins {} key column(s) ({}) with {} column(s) of {} \
                     ({}); pair one right column with each key column",
                    built.columns().len(),
                    built.columns().join(", "),
                    j.right_columns.len(),
                    j.right,
                    j.right_columns.join(", ")
                )));
            }
        }
        check_aggregate(run_id, &built, &query, &self.schema)?;
        if matches!(query, RunQuery::ContainsAll) && null_tuples > 0 {
            // A null value is in no list (ADR 014), so no row holds every
            // value; dropping it, as a key lookup drops a null key, would
            // widen the run to the remaining values.
            return Err(Error::KeySpec(format!(
                "run {run_id:?} is a contains_all run with {null_tuples} null value(s); no list \
                 holds a null (ADR 014), so no row would match: remove the null values from the \
                 key file, or record a contains_any run"
            )));
        }
        let payload = built.to_payload()?;
        let key_set = Hash::of(&payload);
        let columns = built.columns().to_vec();
        Ok(Prepared {
            run: RecordedRun {
                run_id: run_id.to_string(),
                template: template.map_or_else(
                    || format!("columns({})", columns.join(",")),
                    ToString::to_string,
                ),
                query_kind: query.kind_str().to_string(),
                asof: query.asof_ts(),
                lookback: query.lookback(),
                key_types: built.key_types().to_vec(),
                columns,
                keys: built.len() as u64,
                null_tuples,
                key_set,
                window: query.window().cloned(),
                join: query.join().cloned(),
                aggregate: query.aggregate().cloned(),
            },
            payload,
        })
    }

    /// Runs recorded so far.
    #[must_use]
    pub fn len(&self) -> usize {
        self.runs.len()
    }

    /// Whether no run is recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    /// Builds the workload, stores its manifest in the sink under the
    /// workload hash, and returns it.
    pub fn finish(self) -> Result<Workload> {
        let w = Workload::new(WorkloadManifest {
            format: WORKLOAD_FORMAT.to_string(),
            encoding_version: ENCODING_VERSION,
            runs: self.runs.into_values().collect(),
        })?;
        let bytes = serde_json::to_vec(w.manifest())?;
        self.sink.put(&w.hash(), Bytes::from(bytes))?;
        Ok(w)
    }
}

fn field_names(schema: &Schema) -> Vec<String> {
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

/// A text run (`has_token`, `contains`, `has_prefix`) looks up exactly one
/// string column, and each of its values is an argument its match takes
/// (core's `text::check_arg`: `has_token` one word, `has_prefix` a
/// non-empty prefix). Refused with what to record instead.
fn check_text(run_id: &str, keys: &RunKeys, query: &RunQuery, schema: &Schema) -> Result<()> {
    let Some(op) = query.text_op() else {
        return Ok(());
    };
    let [column] = keys.columns() else {
        return Err(Error::KeySpec(format!(
            "run {run_id:?} is a {} run on {} columns ({}); a text run looks up one string \
             column, so give a key file with that one column",
            query.kind_str(),
            keys.columns().len(),
            keys.columns().join(", ")
        )));
    };
    let dt = schema
        .field_with_name(column)
        .map_err(|_| Error::KeySpec(format!("column {column:?} is not in the dataset schema")))?
        .data_type();
    bellwether_core::text::check_column_type(column, dt)?;
    for key in keys.tuples() {
        let value = bellwether_core::text::arg_of_key(key)?;
        bellwether_core::text::check_arg(op, &value).map_err(|e| match e {
            Error::InvalidArgument { field, reason } => Error::InvalidArgument {
                field,
                reason: format!("run {run_id:?}: {reason}"),
            },
            other => other,
        })?;
    }
    Ok(())
}

/// A membership run (`contains_any`, `contains_all`) looks up exactly one
/// column, a list (or map) in the dataset; any other run never looks up a
/// list column, whose rows have no single value to equal a key. Refused
/// with what to record instead.
fn check_membership(run_id: &str, keys: &RunKeys, query: &RunQuery, schema: &Schema) -> Result<()> {
    let is_list = |c: &String| {
        schema
            .field_with_name(c)
            .is_ok_and(|f| bellwether_core::list::element_type(f.data_type()).is_some())
    };
    let columns = keys.columns();
    if query.is_membership() {
        return match columns {
            [c] if is_list(c) => Ok(()),
            [c] => Err(Error::KeySpec(format!(
                "run {run_id:?} is a {} run on {c:?}, which is not a list column; a membership \
                 run looks up the elements of one List, LargeList, or Map column (record a \
                 locate run for a flat column)",
                query.kind_str()
            ))),
            _ => Err(Error::KeySpec(format!(
                "run {run_id:?} is a {} run on {} columns ({}); a membership run looks up the \
                 elements of exactly one list column, so give a key file with that one column",
                query.kind_str(),
                columns.len(),
                columns.join(", ")
            ))),
        };
    }
    if let Some(c) = columns.iter().find(|c| is_list(c)) {
        return Err(Error::KeySpec(format!(
            "run {run_id:?} looks up {c:?}, a list column, as a {} run; a list column's rows hold \
             many values, so record its values as a membership run: contains_any (rows holding \
             any of them; `--any` on the command line) or contains_all (`--all`)",
            query.kind_str()
        )));
    }
    Ok(())
}

/// An aggregate run's value column is in the dataset, is no key column, has
/// one value per row, and can be summed when the run asks for `sum`, by
/// the rules an agg key on the run's columns would be declared under
/// (core's `agg::value_type`). Refused naming what to change.
fn check_aggregate(run_id: &str, keys: &RunKeys, query: &RunQuery, schema: &Schema) -> Result<()> {
    let Some(a) = query.aggregate() else {
        return Ok(());
    };
    let spec = bellwether_types::KeySpec::Agg {
        columns: keys.columns().to_vec(),
        value: a.value.clone(),
        functions: a.functions.clone(),
    };
    bellwether_core::agg::value_type(&spec, schema).map_err(|e| match e {
        Error::KeySpec(m) => Error::KeySpec(format!("run {run_id:?}: {m}")),
        other => other,
    })?;
    Ok(())
}

fn read_parquet_keys(path: &Path, schema: &Schema) -> Result<(RunKeys, u64)> {
    let file = File::open(path).map_err(|e| Error::Source(format!("{}: {e}", path.display())))?;
    let meta = bellwether_source::load_reader_metadata(&file)
        .map_err(|e| Error::Source(format!("{}: {e}", path.display())))?;
    let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(file, meta)
        .build()
        .map_err(|e| Error::Source(format!("{}: {e}", path.display())))?;
    let columns = field_names(&reader.schema());
    let mut b = RunKeysBuilder::new(&columns, schema)?;
    for batch in reader {
        let batch = batch.map_err(|e| Error::Source(format!("{}: {e}", path.display())))?;
        b.push_batch(&batch)?;
    }
    let nulls = b.null_tuples();
    Ok((b.finish(), nulls))
}

fn check_format(found: &str, prefix: &str, expected: &str, what: &'static str) -> Result<()> {
    if found == expected {
        return Ok(());
    }
    let supported = expected
        .strip_prefix(prefix)
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(1);
    if let Some(v) = found
        .strip_prefix(prefix)
        .and_then(|v| v.parse::<u32>().ok())
    {
        if v > supported {
            return Err(Error::UnsupportedVersion {
                what,
                found: v,
                supported,
            });
        }
    }
    Err(Error::Other(format!(
        "{what} format {found:?} is not {expected:?}"
    )))
}

#[derive(Deserialize)]
struct BenchWorkload {
    format: String,
    corpus_id: Hash,
    runs: Vec<BenchRun>,
    hash: Hash,
}

#[derive(Deserialize)]
struct BenchRun {
    run_id: String,
    file: String,
    keys: u64,
    hash: Hash,
    /// `locate` (the default when absent) or `count`: a gated workload
    /// counts a key set before it looks the same keys up.
    #[serde(default)]
    query: Option<String>,
}

impl BenchRun {
    /// The run's query kind: [`RunQuery::Locate`] unless it records
    /// `count`.
    fn query(&self) -> ReplayResult<RunQuery> {
        match self.query.as_deref() {
            None | Some(QUERY_KIND_LOCATE) => Ok(RunQuery::Locate),
            Some(QUERY_KIND_COUNT) => Ok(RunQuery::Count),
            Some(other) => Err(bench_err(
                Some(&self.run_id),
                format!("query {other:?} is not one a bench run records (locate or count)"),
            )),
        }
    }
}

/// What [`record_bench_workload`] verified and recorded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BenchWorkloadSummary {
    /// The bench corpus the keys were drawn from.
    pub corpus_id: Hash,
    /// The bench's own workload hash (BLAKE3 over its run hashes).
    pub bench_hash: Hash,
    /// Runs recorded.
    pub runs: usize,
}

fn bench_err(run_id: Option<&str>, detail: impl Into<String>) -> ReplayError {
    ReplayError::BenchMismatch {
        run_id: run_id.map(ToString::to_string),
        detail: detail.into(),
    }
}

/// Reads `bellwether-bench`'s `workload.json` and records every run into
/// `recorder` (module docs). Run files are resolved relative to the
/// directory holding `workload_json` and must stay inside it.
///
/// Nothing is written and the recorder is untouched unless every run
/// verifies: the first pass only reads and checks, holding one run's keys at
/// a time. The second pass records; if a run file changed between the
/// passes, its re-verification fails, the recorder is still untouched, and
/// the key sets already stored for earlier runs remain in the sink as
/// unreferenced, content-addressed cache entries.
pub fn record_bench_workload(
    recorder: &mut WorkloadRecorder<'_>,
    workload_json: &Path,
) -> ReplayResult<BenchWorkloadSummary> {
    let bytes = std::fs::read(workload_json)
        .map_err(|e| Error::Source(format!("{}: {e}", workload_json.display())))?;
    let bench: BenchWorkload = serde_json::from_slice(&bytes).map_err(Error::from)?;
    match check_format(
        &bench.format,
        BENCH_WORKLOAD_FORMAT_PREFIX,
        BENCH_WORKLOAD_FORMAT,
        "bench workload",
    ) {
        Ok(()) => {}
        Err(e @ Error::UnsupportedVersion { .. }) => return Err(e.into()),
        Err(e) => return Err(bench_err(None, e.to_string())),
    }
    let root = workload_json.parent().unwrap_or_else(|| Path::new("."));
    let corpus_json = root.join("corpus.json");
    if corpus_json.exists() {
        let corpus = std::fs::read(&corpus_json)
            .map_err(|e| Error::Source(format!("{}: {e}", corpus_json.display())))?;
        if Hash::of(&corpus) != bench.corpus_id {
            return Err(bench_err(
                None,
                format!(
                    "{} was generated for corpus {}, but {} hashes to {}",
                    workload_json.display(),
                    bench.corpus_id,
                    corpus_json.display(),
                    Hash::of(&corpus)
                ),
            ));
        }
    }
    let mut all = Vec::with_capacity(32 * bench.runs.len());
    for r in &bench.runs {
        all.extend_from_slice(&r.hash.0);
    }
    if Hash::of(&all) != bench.hash {
        return Err(bench_err(
            None,
            format!(
                "workload hash {} does not match its runs ({})",
                bench.hash,
                Hash::of(&all)
            ),
        ));
    }
    // Pass 1: verify every run and every id, writing nothing.
    let mut ids = BTreeSet::new();
    let mut paths = Vec::with_capacity(bench.runs.len());
    for run in &bench.runs {
        if !ids.insert(run.run_id.as_str()) {
            return Err(bench_err(Some(&run.run_id), "the run id appears twice"));
        }
        recorder.check_new(&run.run_id)?;
        run.query()?;
        let path = run_path(root, run)?;
        verified_keys(&path, run)?;
        paths.push(path);
    }
    // Pass 2: re-read, re-verify, encode, and store each run; insert into
    // the recorder only when every run is stored.
    let mut prepared = Vec::with_capacity(bench.runs.len());
    for (run, path) in bench.runs.iter().zip(&paths) {
        let (column, keys) = verified_keys(path, run)?;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                column,
                DataType::UInt64,
                false,
            )])),
            vec![Arc::new(UInt64Array::from(keys)) as ArrayRef],
        )
        .map_err(|e| bench_err(Some(&run.run_id), e.to_string()))?;
        let p = recorder.prepare(&run.run_id, KeyInput::Batch(&batch), run.query()?, None)?;
        recorder.sink.put(&p.run.key_set, Bytes::from(p.payload))?;
        prepared.push(p.run);
    }
    for run in prepared {
        recorder.runs.insert(run.run_id.clone(), run);
    }
    Ok(BenchWorkloadSummary {
        corpus_id: bench.corpus_id,
        bench_hash: bench.hash,
        runs: bench.runs.len(),
    })
}

/// A bench run file's path, which must stay inside the corpus directory.
fn run_path(root: &Path, run: &BenchRun) -> ReplayResult<PathBuf> {
    let rel = Path::new(&run.file);
    if rel.is_absolute()
        || rel
            .components()
            .any(|c| !matches!(c, Component::Normal(_) | Component::CurDir))
    {
        return Err(bench_err(
            Some(&run.run_id),
            format!("the run file {:?} is not inside the corpus", run.file),
        ));
    }
    Ok(root.join(rel))
}

/// A bench run file's keys, verified against the run's recorded hash and
/// count.
fn verified_keys(path: &Path, run: &BenchRun) -> ReplayResult<(String, Vec<u64>)> {
    let (column, keys) = read_bench_keys(path, &run.run_id)?;
    let mut h = Vec::with_capacity(keys.len() * 8);
    for k in &keys {
        h.extend_from_slice(&k.to_be_bytes());
    }
    if Hash::of(&h) != run.hash || keys.len() as u64 != run.keys {
        return Err(bench_err(
            Some(&run.run_id),
            format!(
                "{} distinct keys hashing to {}, but workload.json records {} keys hashing to {}",
                keys.len(),
                Hash::of(&h),
                run.keys,
                run.hash
            ),
        ));
    }
    Ok((column, keys))
}

/// A bench run file's keys: one non-null `UInt64` column, sorted and
/// deduplicated (how the bench hashes them).
fn read_bench_keys(path: &Path, run_id: &str) -> ReplayResult<(String, Vec<u64>)> {
    let src = |e: &dyn std::fmt::Display| Error::Source(format!("{}: {e}", path.display()));
    let file = File::open(path).map_err(|e| src(&e))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| src(&e))?
        .build()
        .map_err(|e| src(&e))?;
    let schema = reader.schema();
    if schema.fields().len() != 1 || schema.field(0).data_type() != &DataType::UInt64 {
        return Err(bench_err(
            Some(run_id),
            format!(
                "{}: a bench run file holds one UInt64 key column, found {}",
                path.display(),
                schema
                    .fields()
                    .iter()
                    .map(|f| format!("{:?} ({})", f.name(), f.data_type()))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        ));
    }
    let column = schema.field(0).name().clone();
    let mut keys = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| src(&e))?;
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| bench_err(Some(run_id), "keys are not UInt64"))?;
        if col.null_count() > 0 {
            return Err(bench_err(Some(run_id), "a bench run file has null keys"));
        }
        keys.extend(col.values().iter().copied());
    }
    keys.sort_unstable();
    keys.dedup();
    Ok((column, keys))
}
