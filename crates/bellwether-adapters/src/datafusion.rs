//! DataFusion: the plan as a Parquet scan pinned to the planned row groups.
//!
//! [`scan_config`] lays the plan out as [`ScanConfig`]: one file per entry,
//! each with a row-group access vector (the planned row groups `scan`, the
//! rest `skip`) for DataFusion's `ParquetAccessPlan`, `metadata_size_hint`
//! set to the footer's exact length when [`crate::EmitOptions::footer_hint`]
//! read it, DataFusion's statistics, page-index, and bloom-filter pruning
//! off unless [`crate::EmitOptions::engine_pruning`], and the residual as
//! typed stages. Its JSON form is what `--emit datafusion` prints; it is
//! always built. The JSON carries the plan, the dataset schema, and the
//! options it was built from, and [`from_json`] loads it back: it rebuilds
//! the scan from those inputs and refuses a description that differs from
//! the rebuilt one, so a description edited after emission never runs.
//!
//! With the `datafusion` feature, [`ScanConfig::file_scan_config`] builds
//! the executable `FileScanConfig` (DataFusion 55), [`logical_plan`] the
//! residual over it (filters, a `VALUES` relation left-joined on the key
//! for `NotOlderThan`, `rank() over (partition by key order by ts desc) =
//! 1` for `LatestPerKey`), [`execute`] runs it, and [`CountingStore`]
//! counts every byte DataFusion asks the object store for, split into
//! footer, data, and other bytes, so a caller measures its own run.
//!
//! Pinned means pinned: with engine pruning off the scan is a leaf no
//! optimizer pushes a filter into, so neither a row-group statistic nor a
//! partition value prunes what the plan names, and the bytes DataFusion
//! reads are the plan's.

use std::path::PathBuf;

use arrow::datatypes::DataType;
use bellwether_types::ScanPlan;
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;

use crate::layout::layout;
use crate::residual::{compile, Residual};
use crate::{AdapterError, ColumnDef, EmitOptions, Result};

#[cfg(feature = "datafusion")]
mod counting;
#[cfg(feature = "datafusion")]
mod exec;
#[cfg(feature = "datafusion")]
mod udf;

/// The DataFusion crate the executable scan is built with (feature
/// `datafusion`), for callers that run it.
#[cfg(feature = "datafusion")]
pub use ::datafusion as engine;
#[cfg(feature = "datafusion")]
pub use counting::CountingStore;
#[cfg(feature = "datafusion")]
pub use exec::{execute, logical_plan, register};

const E: &str = "datafusion";

/// The scan description's format name.
pub const FORMAT: &str = "bellwether.datafusion/1";

/// Hidden partition column holding each scanned file's read position (an
/// ordered plan only).
pub const POSITION_COLUMN: &str = "__bellwether_pos";
/// Hidden virtual column holding each row's number in its file (an ordered
/// plan only).
pub const ROW_COLUMN: &str = "__bellwether_row";

/// One file DataFusion scans: a planned file, or, for a plan whose read
/// order interleaves files, one of its row groups.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ScanFile {
    /// The path as the plan spells it.
    pub plan_path: String,
    /// Where DataFusion reads it.
    pub location: String,
    /// The file's size the plan's fingerprint records.
    pub size: u64,
    /// `scan` or `skip` for every row group of the file, in order: the
    /// `ParquetAccessPlan`.
    pub access: Vec<&'static str>,
    /// The footer's exact length including its trailer, the
    /// `metadata_size_hint` (when the footer was read).
    pub metadata_size_hint: Option<u64>,
    /// The file's partition values, one per partition column.
    pub partition_values: Vec<Option<String>>,
    /// The scan's read position (an ordered plan only).
    pub position: Option<u32>,
}

/// The Parquet options of the scan.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[allow(clippy::struct_excessive_bools)] // DataFusion's own four independent switches, by name
pub struct ParquetOptions {
    /// Row-group statistics pruning.
    pub pruning: bool,
    /// Page-index reads and pruning.
    pub enable_page_index: bool,
    /// Bloom-filter reads and pruning.
    pub bloom_filter_on_read: bool,
    /// Row filters inside the scan.
    pub pushdown_filters: bool,
}

/// One dataset column in a scan description: its name and its Arrow type
/// as Arrow spells it (`DataType`'s `Display`, which `FromStr` reads back
/// exactly).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceColumn {
    /// The column's name.
    pub name: String,
    /// Its dataset type.
    #[serde(rename = "type")]
    pub data_type: String,
}

/// What a scan was built from: the plan, the dataset schema, and the
/// options. [`from_json`] rebuilds the scan from these.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Source {
    /// The plan, exactly as the lookup emitted it.
    pub plan: ScanPlan,
    /// The dataset schema it was planned against.
    pub schema: Vec<SourceColumn>,
    /// The options it was translated with.
    pub options: EmitOptions,
}

/// The plan as DataFusion runs it (see the module docs). Serializes to the
/// JSON `--emit datafusion` prints.
#[derive(Clone, Debug, Serialize)]
pub struct ScanConfig {
    /// [`FORMAT`].
    pub format: &'static str,
    /// The manifest the plan was computed against.
    pub manifest_version: String,
    /// The files, in scan order.
    pub files: Vec<ScanFile>,
    /// File groups (indices into `files`): one per file, or one group in
    /// read order for an ordered plan (DataFusion reads a group's files in
    /// order).
    pub file_groups: Vec<Vec<usize>>,
    /// The partition columns, synthesized from each file's values.
    pub table_partition_cols: Vec<String>,
    /// The columns returned, in order.
    pub projection: Vec<String>,
    /// The Parquet options.
    pub parquet: ParquetOptions,
    /// What the realized bytes are: `equal to the plan's` with engine
    /// pruning off, `at most the plan's` with it on.
    pub realized: &'static str,
    /// Planned bytes, row groups, and files.
    pub planned: Json,
    /// The residual, typed, in order.
    pub residual: Json,
    /// The keys file the `In` stage semi-joins against, if any.
    pub keys_file: Option<PathBuf>,
    /// Whether rows come back in the plan's read order.
    pub ordered: bool,
    /// The plan, schema, and options the scan was built from.
    pub source: Source,
    #[serde(skip)]
    pub(crate) compiled: Residual,
    #[serde(skip)]
    pub(crate) schema: Vec<ColumnDef>,
}

impl ScanConfig {
    /// The description as pretty JSON.
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self)
            .map_err(|e| AdapterError::Plan(format!("cannot serialize the scan: {e}")))
    }

    /// The compiled residual.
    #[must_use]
    pub fn residual(&self) -> &Residual {
        &self.compiled
    }

    /// The dataset schema the scan was built against.
    #[must_use]
    pub fn schema(&self) -> &[ColumnDef] {
        &self.schema
    }
}

/// Loads a scan description `--emit datafusion` printed: rebuilds the scan
/// from the plan, schema, and options it carries ([`scan_config`], which
/// checks the keys file and, with the footer hint, each planned file's size
/// and footer again) and refuses it unless the rebuilt description is the
/// given one, field for field. What runs is what was printed.
pub fn from_json(text: &str) -> Result<ScanConfig> {
    let bad = |m: String| AdapterError::Description(m);
    let doc: Json = serde_json::from_str(text).map_err(|e| bad(format!("it is not JSON ({e})")))?;
    match doc.get("format").and_then(Json::as_str) {
        Some(FORMAT) => {}
        Some(other) => return Err(bad(format!("its format is {other:?}, not {FORMAT:?}"))),
        None => return Err(bad(format!("it has no format field (expected {FORMAT:?})"))),
    }
    let source = doc
        .get("source")
        .ok_or_else(|| bad("it has no source field (the plan it was built from)".into()))?;
    let source: Source = serde_json::from_value(source.clone())
        .map_err(|e| bad(format!("its source does not parse ({e})")))?;
    let schema = source
        .schema
        .iter()
        .map(|c| {
            c.data_type
                .parse::<DataType>()
                .map(|dt| ColumnDef::new(&c.name, dt))
                .map_err(|e| {
                    bad(format!(
                        "column {:?} has type {:?}, which Arrow does not read ({e})",
                        c.name, c.data_type
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    let rebuilt = scan_config(&source.plan, &schema, &source.options)?;
    let again = serde_json::to_value(&rebuilt)
        .map_err(|e| bad(format!("the rebuilt scan does not serialize ({e})")))?;
    if again != doc {
        let field = match (&again, &doc) {
            (Json::Object(a), Json::Object(d)) => a
                .iter()
                .find(|(k, v)| d.get(*k) != Some(*v))
                .map(|(k, _)| k.clone())
                .or_else(|| d.keys().find(|k| !a.contains_key(*k)).cloned())
                .unwrap_or_default(),
            _ => String::new(),
        };
        return Err(bad(format!(
            "its {field:?} field differs from the scan its own plan, schema, and options build \
             (it was edited after it was emitted)"
        )));
    }
    Ok(rebuilt)
}

/// The plan as a DataFusion scan (see the module docs).
#[allow(clippy::too_many_lines)] // one linear pass: files, access plans, residual, labels
pub fn scan_config(
    plan: &ScanPlan,
    schema: &[ColumnDef],
    opts: &EmitOptions,
) -> Result<ScanConfig> {
    let compiled = compile(plan.residual.as_ref(), schema, opts.residual, E)?;
    let lay = layout(plan, schema, opts)?;
    for c in schema {
        if c.name.starts_with("__bellwether_") {
            return Err(AdapterError::Unsupported {
                engine: E,
                what: format!("column {:?}", c.name),
                reason: "uses the __bellwether_ prefix the scan's helper columns use".into(),
                next: "rename the column".into(),
            });
        }
    }
    let mut source_schema = Vec::with_capacity(schema.len());
    for c in schema {
        let spelled = c.data_type.to_string();
        if spelled.parse::<DataType>().ok().as_ref() != Some(&c.data_type) {
            return Err(AdapterError::Unsupported {
                engine: E,
                what: format!("column {:?} of type {spelled}", c.name),
                reason: "has no exact text form in Arrow, so the scan description could not be \
                         loaded back"
                    .into(),
                next: "emit for DuckDB, or build the scan in process with \
                       datafusion::scan_config"
                    .into(),
            });
        }
        source_schema.push(SourceColumn {
            name: c.name.clone(),
            data_type: spelled,
        });
    }
    let keys_file = match &opts.keys_file {
        Some(p) => {
            crate::keys::check(p, &compiled, crate::keys::KeyEquality::Canonical)?;
            Some(p.clone())
        }
        None => None,
    };
    let access = |f: &crate::layout::PlannedFile, only: Option<u32>| -> Result<Vec<&'static str>> {
        let n = f
            .row_group_count()
            .ok_or_else(|| AdapterError::RowOffsetsNeeded {
                engine: E,
                path: f.path.clone(),
                why: "to give the file a row-group access plan, which names every row group",
            })?;
        Ok((0..n)
            .map(|i| {
                let i = u32::try_from(i).unwrap_or(u32::MAX);
                let planned = match only {
                    Some(rg) => rg == i,
                    None => f.row_groups.contains(&i),
                };
                if planned {
                    "scan"
                } else {
                    "skip"
                }
            })
            .collect())
    };
    let scan_file = |f: &crate::layout::PlannedFile, only: Option<u32>, position: Option<u32>| {
        Ok::<_, AdapterError>(ScanFile {
            plan_path: f.path.clone(),
            location: f.location.clone(),
            size: f.size,
            access: access(f, only)?,
            metadata_size_hint: f.footer_len,
            partition_values: f.partition_values.clone(),
            position,
        })
    };
    let mut files = Vec::new();
    if opts.ordered && lay.interleaved {
        for (pos, (fi, rg)) in lay.read_order.iter().enumerate() {
            files.push(scan_file(
                &lay.files[*fi],
                Some(*rg),
                Some(u32::try_from(pos).unwrap_or(u32::MAX)),
            )?);
        }
    } else {
        for (pos, f) in lay.files.iter().enumerate() {
            let position = opts.ordered.then(|| u32::try_from(pos).unwrap_or(u32::MAX));
            files.push(scan_file(f, None, position)?);
        }
    }
    let file_groups = if opts.ordered {
        if files.is_empty() {
            Vec::new()
        } else {
            vec![(0..files.len()).collect()]
        }
    } else {
        (0..files.len()).map(|i| vec![i]).collect()
    };
    let pruning = opts.engine_pruning;
    Ok(ScanConfig {
        format: FORMAT,
        manifest_version: plan.manifest_version.clone(),
        files,
        file_groups,
        table_partition_cols: lay.partition_columns.clone(),
        projection: lay.output.clone(),
        parquet: ParquetOptions {
            pruning,
            enable_page_index: pruning,
            bloom_filter_on_read: pruning,
            pushdown_filters: false,
        },
        realized: if pruning {
            "at most the plan's bytes: DataFusion's own pruning may skip planned row groups"
        } else {
            "the plan's bytes: every planned row group is read and no other"
        },
        planned: serde_json::json!({
            "bytes": plan.bytes,
            "row_groups": plan.row_groups_selected,
            "files": lay.files.len(),
        }),
        residual: compiled.to_json(),
        keys_file,
        ordered: opts.ordered,
        source: Source {
            plan: plan.clone(),
            schema: source_schema,
            options: opts.clone(),
        },
        compiled,
        schema: schema.to_vec(),
    })
}
