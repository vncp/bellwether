//! Bellwether plans executed inside query engines (WP16; ADR 010's second
//! half: a plan is the same plan in every surface, including an engine).
//!
//! An adapter turns the [`ScanPlan`] that `locate`, `latest`, `asof`, and
//! `why` emit, and the dataset schema it was planned against, into an
//! artifact a query engine runs:
//!
//! - [`datafusion`]: a scan description with one `ParquetAccessPlan` per
//!   file (planned row groups `Scan`, the rest `Skip`), DataFusion's own
//!   pruning off by default, and the residual as typed expressions; with the
//!   `datafusion` feature, the executable `FileScanConfig`, the residual's
//!   logical plan, and the byte-counting [`datafusion::CountingStore`].
//! - [`duckdb`]: one SQL statement over `read_parquet` of the planned files,
//!   restricted to the planned row groups' rows by `file_row_number`, with
//!   the residual as typed SQL.
//! - [`spark`]: a JSON manifest for `shims/spark/bellwether_plan.py`, which
//!   reads the planned files with the residual as DataFrame operations.
//!
//! Adapters never open an index, read a manifest, or see a `Store`: what
//! the plan does not carry (row-group row counts, partition values) comes
//! in [`EmitOptions`], which `bellwether locate|latest|asof --emit` fills
//! from the manifest the plan was computed against.
//!
//! The residual is translated, never dropped ([`residual`]): canonical
//! bytes are decoded to typed values at the dataset type with
//! `bellwether_core::encoding::decode`, and every operation runs in the
//! order the plan lists it (filters, then `NotOlderThan`, then
//! `LatestPerKey`, then `Live`). `LatestPerKey` keeps every row at a key's
//! newest timestamp (ties are all winners), and a null key or timestamp
//! never wins. A residual an engine cannot evaluate exactly is a typed
//! [`AdapterError::Unsupported`] naming the engine that can, never a
//! looser query.

#![forbid(unsafe_code)]
#![allow(clippy::doc_markdown)]

pub mod datafusion;
pub mod duckdb;
mod error;
mod keys;
pub mod layout;
pub mod measure;
pub mod residual;
pub mod spark;
mod sql;

use std::collections::BTreeMap;
use std::path::PathBuf;

use arrow::datatypes::DataType;
use bellwether_types::ScanPlan;
use serde::{Deserialize, Serialize};

pub use error::{AdapterError, Result};

/// One column of the dataset schema a plan was computed against, at its
/// dataset type (`arrow` spells it; nested list and map columns included).
/// Partition columns are listed too; [`EmitOptions::partitions`] says which
/// they are.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnDef {
    /// The column's name.
    pub name: String,
    /// Its dataset type.
    pub data_type: DataType,
}

impl ColumnDef {
    /// A column.
    #[must_use]
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
        }
    }
}

/// Which part of the plan's residual the engine applies.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResidualMode {
    /// All of it, in order: the rows are exactly the lookup's.
    #[default]
    Full,
    /// Only its key membership (`In`): every row of the planned row groups
    /// holding a queried key, without time filters, ranking, or the live
    /// test. Labeled in every artifact; the rows are a superset.
    KeysOnly,
    /// None: every row of the planned row groups. Labeled in every artifact.
    None,
}

impl ResidualMode {
    /// The spelling used in artifacts and on the command line.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::KeysOnly => "keys_only",
            Self::None => "none",
        }
    }
}

/// Rows per row group of each planned file, by path as the plan spells it,
/// in `index_in_file` order (manifest storage version 2's
/// `rowgroup_rows`). Their prefix sums are the row offsets DuckDB and
/// Spark restrict a strict subset of a file's row groups by.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowOffsets(pub BTreeMap<String, Vec<u64>>);

impl RowOffsets {
    /// The rows of each row group of `path`, if recorded.
    #[must_use]
    pub fn rows(&self, path: &str) -> Option<&[u64]> {
        self.0.get(path).map(Vec::as_slice)
    }
}

/// The partition columns of a partitioned dataset and each planned file's
/// values, as the file's path spells them (percent-decoded; `None` for
/// Hive's null), from the partitioning the index records.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Partitions {
    /// The recorded scheme (`hive` or `template:<pattern>`), for the
    /// artifact's label.
    pub scheme: String,
    /// The partition columns, in path order; each is a dataset column.
    pub columns: Vec<String>,
    /// Per planned file (by path as the plan spells it), one value per
    /// column.
    pub files: BTreeMap<String, Vec<Option<String>>>,
}

/// A rewrite of the plan's file paths for the engine: every path that
/// starts with `from` is read at `to` followed by the rest (a local folder
/// served over HTTP, for example). The plan's identity is unchanged; only
/// where the engine reads it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Relocate {
    /// The prefix the plan's paths start with.
    pub from: String,
    /// What the engine reads instead.
    pub to: String,
}

impl Relocate {
    /// `path` as the engine reads it.
    #[must_use]
    pub fn apply(&self, path: &str) -> String {
        match path.strip_prefix(&self.from) {
            Some(rest) => format!("{}{rest}", self.to),
            None => path.to_string(),
        }
    }
}

/// How an adapter translates a plan. Serialized into a DataFusion scan
/// description (its `source.options`), so [`datafusion::from_json`] builds
/// the same scan.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmitOptions {
    /// A Parquet file of the queried keys, whose columns are the residual's
    /// `In` columns at their dataset types: the engine semi-joins against
    /// it instead of an inline list. The adapter reads it once and refuses
    /// it unless its key set is exactly the `In` list's.
    pub keys_file: Option<PathBuf>,
    /// Which part of the residual the engine applies.
    pub residual: ResidualMode,
    /// Rows per row group of the planned files (from the manifest).
    pub row_offsets: Option<RowOffsets>,
    /// DataFusion: read each planned file's footer trailer once to set
    /// `metadata_size_hint` to the footer's exact length, and check the
    /// plan's row groups against the footer.
    pub footer_hint: bool,
    /// DataFusion: turn its statistics, page-index, and bloom-filter
    /// pruning back on (realized bytes are then at most the plan's).
    pub engine_pruning: bool,
    /// The dataset's partition columns and the planned files' values.
    pub partitions: Option<Partitions>,
    /// Return rows in the plan's read order (`ScanPlan::order`, else entry
    /// order with row groups ascending), row by row.
    pub ordered: bool,
    /// Where the engine reads the plan's files.
    pub relocate: Option<Relocate>,
}

/// What an adapter produces.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)] // one per emit, returned by value and printed
pub enum Artifact {
    /// DataFusion: the scan description, executable with the `datafusion`
    /// feature.
    DataFusion(datafusion::ScanConfig),
    /// DuckDB: one SQL statement.
    Sql(String),
    /// Spark: the manifest `shims/spark/bellwether_plan.py` reads.
    Manifest(serde_json::Value),
}

impl Artifact {
    /// The artifact as the CLI prints it: JSON for DataFusion and Spark,
    /// SQL for DuckDB.
    pub fn to_text(&self) -> Result<String> {
        match self {
            Self::DataFusion(c) => c.to_json(),
            Self::Sql(s) => Ok(s.clone()),
            Self::Manifest(v) => serde_json::to_string_pretty(v)
                .map_err(|e| AdapterError::Plan(format!("cannot serialize the manifest: {e}"))),
        }
    }
}

/// One engine's translation of a plan.
pub trait Adapter {
    /// The engine's name (`datafusion`, `duckdb`, `spark`).
    fn engine(&self) -> &'static str;
    /// Translates `plan` over `schema`.
    fn emit(&self, plan: &ScanPlan, schema: &[ColumnDef], opts: &EmitOptions) -> Result<Artifact>;
}

/// The DataFusion adapter ([`datafusion::scan_config`]).
#[derive(Clone, Copy, Debug, Default)]
pub struct DataFusion;

/// The DuckDB adapter ([`duckdb::sql`]).
#[derive(Clone, Copy, Debug, Default)]
pub struct DuckDb;

/// The Spark adapter ([`spark::manifest`]).
#[derive(Clone, Copy, Debug, Default)]
pub struct Spark;

impl Adapter for DataFusion {
    fn engine(&self) -> &'static str {
        "datafusion"
    }
    fn emit(&self, plan: &ScanPlan, schema: &[ColumnDef], opts: &EmitOptions) -> Result<Artifact> {
        datafusion::scan_config(plan, schema, opts).map(Artifact::DataFusion)
    }
}

impl Adapter for DuckDb {
    fn engine(&self) -> &'static str {
        "duckdb"
    }
    fn emit(&self, plan: &ScanPlan, schema: &[ColumnDef], opts: &EmitOptions) -> Result<Artifact> {
        duckdb::sql(plan, schema, opts).map(Artifact::Sql)
    }
}

impl Adapter for Spark {
    fn engine(&self) -> &'static str {
        "spark"
    }
    fn emit(&self, plan: &ScanPlan, schema: &[ColumnDef], opts: &EmitOptions) -> Result<Artifact> {
        spark::manifest(plan, schema, opts).map(Artifact::Manifest)
    }
}

/// The adapter for an engine name (`datafusion`, `duckdb`, `spark`).
#[must_use]
pub fn adapter(engine: &str) -> Option<&'static dyn Adapter> {
    match engine {
        "datafusion" => Some(&DataFusion),
        "duckdb" => Some(&DuckDb),
        "spark" => Some(&Spark),
        _ => None,
    }
}

/// The engines with an adapter.
pub const ENGINES: [&str; 3] = ["datafusion", "duckdb", "spark"];
