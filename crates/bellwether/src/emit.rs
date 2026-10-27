//! What an engine adapter needs beside a plan (WP16, `--emit`): the
//! dataset schema the plan was computed against, the row counts of the
//! planned files' row groups, and their partition values, all read from
//! the manifest at the plan's own sequence. Adapters never read a
//! manifest; the facade hands them this.

use std::collections::BTreeMap;
use std::path::PathBuf;

use arrow::datatypes::SchemaRef;
use bellwether_adapters::{
    AdapterError, ColumnDef, EmitOptions, Partitions, Relocate, ResidualMode, RowOffsets,
};
use bellwether_source::Partitioning;
use bellwether_types::{Error, EvidenceBody, Record, Result, ScanPlan};
use serde::{Deserialize, Serialize};

use crate::engine::{guard, Engine};
use crate::results::PlanResult;
/// The planned files' partition values (`hive` or a path template).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmitPartitions {
    /// The recorded scheme, as the manifest spells it.
    pub scheme: String,
    /// The partition columns, in path order.
    pub columns: Vec<String>,
    /// Per planned file (by the plan's path), one value per column
    /// (`None` for Hive's null).
    pub files: BTreeMap<String, Vec<Option<String>>>,
}

/// The inputs an adapter takes from the manifest a plan was computed
/// against.
#[derive(Clone, Debug)]
pub struct EmitInputs {
    /// The dataset schema, partition columns included.
    pub schema: SchemaRef,
    /// Rows of every row group of each planned file, in `index_in_file`
    /// order; `None` when the manifest does not record them (storage
    /// version 1), and the adapter then reads them from the footers.
    pub row_counts: Option<BTreeMap<String, Vec<u64>>>,
    /// The partition values, for a partitioned dataset.
    pub partitions: Option<EmitPartitions>,
}

/// The engine an emitted plan is translated for (`--emit`, MCP `emit`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

impl EmitEngine {
    /// The engine's adapter name (`datafusion`, `duckdb`, `spark`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Datafusion => "datafusion",
            Self::Duckdb => "duckdb",
            Self::Spark => "spark",
        }
    }
}

/// Which part of the plan's residual the emitted artifact applies.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
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

impl From<EmitResidual> for ResidualMode {
    fn from(r: EmitResidual) -> Self {
        match r {
            EmitResidual::Full => Self::Full,
            EmitResidual::KeysOnly => Self::KeysOnly,
            EmitResidual::None => Self::None,
        }
    }
}

/// How [`Engine::emit_plan`] translates a plan for a query engine.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct EmitRequest {
    /// The target engine (`datafusion`, `duckdb`, `spark`).
    pub engine: EmitEngine,
    /// A Parquet file of the queried keys for the engine to semi-join
    /// against instead of an inline key list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keys_file: Option<PathBuf>,
    /// Which part of the residual the engine applies.
    #[serde(default)]
    pub residual: EmitResidual,
    /// Return rows in the plan's read order; refused when the plan has none.
    #[serde(default)]
    pub ordered: bool,
    /// `DataFusion`: read each planned file's footer once to set its exact
    /// `metadata_size_hint`.
    #[serde(default)]
    pub footer_hint: bool,
    /// `DataFusion`: leave `DataFusion`'s own statistics, page-index, and
    /// bloom-filter pruning on.
    #[serde(default)]
    pub engine_pruning: bool,
    /// Rewrite planned file paths starting with `[0]` (`from`) to `[1]`
    /// (`to`) followed by the rest of the path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relocate: Option<[String; 2]>,
}

/// What [`Engine::emit_plan`] produced (`PlanResult::emitted`). Nothing is
/// executed: `text` is the artifact for the caller's engine (JSON for
/// `DataFusion` and Spark, SQL for `DuckDB`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmittedArtifact {
    /// The engine (`datafusion`, `duckdb`, `spark`).
    pub engine: EmitEngine,
    /// Which part of the residual the artifact applies.
    pub residual: EmitResidual,
    /// Whether rows come back in the plan's read order.
    pub ordered: bool,
    /// The path rewrite applied, `[from, to]`, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relocate: Option<[String; 2]>,
    /// The artifact text (JSON for `DataFusion` and Spark, SQL for `DuckDB`).
    pub text: String,
}

impl Engine {
    /// The schema, row counts, and partition values of `plan`'s files, from
    /// the manifest at the plan's sequence (`plan.manifest_version`). A
    /// plan over a table or a text source has no Parquet files to hand an
    /// engine and is refused.
    pub fn emit_inputs(&self, plan: &ScanPlan) -> Result<EmitInputs> {
        let seq: u64 = plan.manifest_version.parse().map_err(|_| {
            Error::Other(format!(
                "the plan's manifest version {:?} is not a sequence; emit a plan this index \
                 computed",
                plan.manifest_version
            ))
        })?;
        let pinned = self.pin_sequence(seq)?.ok_or_else(|| Error::NotIndexed {
            root: self.root().to_string(),
            operation: "emit",
            needs: None,
        })?;
        let m = pinned.manifest();
        if !m.source.kind.is_empty() && m.source.kind != "parquet" {
            return Err(Error::Other(format!(
                "--emit hands Parquet files to an engine, and this index is over a {} source; \
                 query it through bellwether instead",
                m.source.kind
            )));
        }
        let planned: std::collections::BTreeSet<&str> =
            plan.entries.iter().map(|e| e.file.path.as_str()).collect();
        let mut rows = BTreeMap::new();
        let mut complete = true;
        for f in &m.rowgroup_space.files {
            if !planned.contains(f.fingerprint.path.as_str()) {
                continue;
            }
            match &f.rowgroup_rows {
                Some(r) => {
                    rows.insert(f.fingerprint.path.clone(), r.clone());
                }
                None => complete = false,
            }
        }
        let partitioning = Partitioning::parse(m.source.partitioning.as_deref())?;
        let partitions = match partitioning.manifest_name() {
            None => None,
            Some(scheme) => {
                let mut columns: Option<Vec<String>> = None;
                let mut files = BTreeMap::new();
                for e in &plan.entries {
                    let values = partitioning.values(self.root(), &e.file.path)?;
                    let names: Vec<String> = values.iter().map(|v| v.name.clone()).collect();
                    if columns.get_or_insert_with(|| names.clone()) != &names {
                        return Err(Error::Other(format!(
                            "{} has partition columns {names:?}, unlike the other planned files; \
                             run `bellwether index` on the folder",
                            e.file.path
                        )));
                    }
                    files.insert(
                        e.file.path.clone(),
                        values.into_iter().map(|v| v.value).collect(),
                    );
                }
                let columns = match columns {
                    Some(c) => c,
                    None => partitioning.column_names(self.root()).unwrap_or_default(),
                };
                Some(EmitPartitions {
                    scheme,
                    columns,
                    files,
                })
            }
        };
        Ok(EmitInputs {
            schema: SchemaRef::clone(pinned.planner.schema()),
            row_counts: complete.then_some(rows),
            partitions,
        })
    }

    /// Translates `plan` for the query engine `req` names (`datafusion`
    /// JSON, `duckdb` SQL, `spark` manifest JSON). Nothing is executed.
    pub fn emit_plan(&self, plan: &ScanPlan, req: &EmitRequest) -> Result<EmittedArtifact> {
        guard(|| self.emit_plan_inner(plan, req))
    }

    /// Translates `result`'s plan for `req`'s engine, checking `--ordered`
    /// against `result.ordered_by` first so an unordered plan is refused
    /// with `result.unordered_reason`.
    pub fn emit_plan_result(
        &self,
        result: &PlanResult,
        req: &EmitRequest,
    ) -> Result<EmittedArtifact> {
        guard(|| {
            if req.ordered && result.ordered_by.is_none() {
                let reason = result
                    .unordered_reason
                    .clone()
                    .unwrap_or_else(|| "the plan has no read order".into());
                return Err(Error::InvalidArgument {
                    field: "ordered",
                    reason: format!(
                        "--ordered: this plan has no read order ({reason}); emit without \
                         --ordered, or declare the range key the reason names and re-plan"
                    ),
                });
            }
            let plan = match &result.evidence.record {
                Record::Evidence(e) => match &e.body {
                    EvidenceBody::Plan { plan } => plan,
                    EvidenceBody::Profile { .. } => {
                        return Err(Error::Other(
                            "the lookup's result holds no plan evidence".into(),
                        ))
                    }
                },
                _ => {
                    return Err(Error::Other(
                        "the lookup's result holds no plan evidence".into(),
                    ))
                }
            };
            self.emit_plan_inner(plan, req)
        })
    }

    fn emit_plan_inner(&self, plan: &ScanPlan, req: &EmitRequest) -> Result<EmittedArtifact> {
        if (req.footer_hint || req.engine_pruning) && req.engine != EmitEngine::Datafusion {
            return Err(Error::InvalidArgument {
                field: if req.footer_hint {
                    "footer_hint"
                } else {
                    "engine_pruning"
                },
                reason: format!(
                    "--footer-hint and --engine-pruning set DataFusion's scan options; {} has \
                     neither, so drop them or use --emit datafusion",
                    req.engine.as_str()
                ),
            });
        }
        let inputs = self.emit_inputs(plan)?;
        let schema: Vec<ColumnDef> = inputs
            .schema
            .fields()
            .iter()
            .map(|f| ColumnDef::new(f.name(), f.data_type().clone()))
            .collect();
        let opts = EmitOptions {
            keys_file: req.keys_file.clone(),
            residual: req.residual.into(),
            // A storage version 1 manifest records no row counts: read them
            // from the planned files' footers instead.
            footer_hint: req.footer_hint || inputs.row_counts.is_none(),
            row_offsets: inputs.row_counts.map(RowOffsets),
            engine_pruning: req.engine_pruning,
            partitions: inputs.partitions.map(|p| Partitions {
                scheme: p.scheme,
                columns: p.columns,
                files: p.files,
            }),
            ordered: req.ordered,
            relocate: req.relocate.as_ref().map(|[from, to]| Relocate {
                from: from.clone(),
                to: to.clone(),
            }),
        };
        let adapter = bellwether_adapters::adapter(req.engine.as_str()).ok_or_else(|| {
            Error::InvalidArgument {
                field: "emit",
                reason: format!("no adapter for {}", req.engine.as_str()),
            }
        })?;
        let text = adapter
            .emit(plan, &schema, &opts)
            .and_then(|a| a.to_text())
            .map_err(adapter_error)?;
        Ok(EmittedArtifact {
            engine: req.engine,
            residual: req.residual,
            ordered: req.ordered,
            relocate: req.relocate.clone(),
            text,
        })
    }
}

fn adapter_error(e: AdapterError) -> Error {
    match e {
        AdapterError::Unsupported { .. }
        | AdapterError::RowOffsetsNeeded { .. }
        | AdapterError::KeysFile { .. } => Error::InvalidArgument {
            field: "emit",
            reason: e.to_string(),
        },
        other => Error::Other(other.to_string()),
    }
}
