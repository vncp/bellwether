//! `--emit` on `locate`, `latest`, and `asof` (WP16): the plan the lookup
//! printed, translated for a query engine by `bellwether-adapters` and
//! printed after the plan line (or written to `--out`). Nothing is
//! executed: the artifact is text for the user's engine (threat model, "no
//! surface executes a query string").

use std::path::{Path, PathBuf};

use bellwether::types::{Error, EvidenceBody, Record, ScanPlan};
use bellwether::{Engine, PlanResult};
use bellwether_adapters::{
    ColumnDef, EmitOptions, Partitions, ResidualMode, RowOffsets,
};
use clap::Args;

use crate::{Failure, Outcome};

/// The engines `--emit` translates for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum EngineArg {
    /// Apache DataFusion: a Parquet scan pinned to the planned row groups
    /// (JSON).
    Datafusion,
    /// DuckDB: one SQL statement over the planned files.
    Duckdb,
    /// Apache Spark: a manifest for shims/spark/bellwether_plan.py (JSON).
    Spark,
}

impl EngineArg {
    fn name(self) -> &'static str {
        match self {
            Self::Datafusion => "datafusion",
            Self::Duckdb => "duckdb",
            Self::Spark => "spark",
        }
    }
}

/// Which part of the residual the engine applies.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum ResidualArg {
    /// All of it: exactly the lookup's rows.
    Full,
    /// Only the key membership (a superset; labeled in the artifact).
    KeysOnly,
    /// None: every row of the planned row groups (labeled).
    None,
}

/// The `--emit` flags.
#[derive(Args, Debug, Default)]
pub(crate) struct EmitArgs {
    /// Translate the plan for a query engine and print the artifact after
    /// the plan line: datafusion (JSON), duckdb (SQL), or spark (JSON for
    /// shims/spark/bellwether_plan.py). Nothing is executed.
    #[arg(long, value_enum, value_name = "ENGINE")]
    pub emit: Option<EngineArg>,
    /// With --emit: the engine semi-joins against the --keys file instead of
    /// an inline key list (the file must hold exactly the plan's keys at
    /// the dataset types; it is checked).
    #[arg(long, requires = "emit")]
    pub keys_file_in_query: bool,
    /// With --emit: write the artifact to this file instead of stdout.
    #[arg(long, value_name = "PATH", requires = "emit")]
    pub out: Option<PathBuf>,
    /// With --emit: refuse unless the plan has a read order, and have the
    /// engine return rows in it.
    #[arg(long, requires = "emit")]
    pub ordered: bool,
    /// With --emit: the part of the residual the engine applies [default:
    /// full].
    #[arg(long, value_enum, value_name = "PART", requires = "emit")]
    pub residual: Option<ResidualArg>,
    /// With --emit datafusion: read each planned file's footer once, to set
    /// DataFusion's metadata size hint to its exact length.
    #[arg(long, requires = "emit")]
    pub footer_hint: bool,
    /// With --emit datafusion: leave DataFusion's own statistics, page
    /// index, and bloom filter pruning on (it then reads at most the
    /// plan's bytes, not exactly them).
    #[arg(long, requires = "emit")]
    pub engine_pruning: bool,
}

/// The plan a lookup's result carries.
fn plan_of(result: &PlanResult) -> Result<&ScanPlan, Failure> {
    match &result.evidence.record {
        Record::Evidence(e) => match &e.body {
            EvidenceBody::Plan { plan } => Ok(plan),
            _ => Err(Failure::Engine(Error::Other(
                "the lookup's evidence holds no plan".into(),
            ))),
        },
        _ => Err(Failure::Engine(Error::Other(
            "the lookup's record is not an evidence".into(),
        ))),
    }
}

fn adapter_failure(e: bellwether_adapters::AdapterError) -> Failure {
    use bellwether_adapters::AdapterError as A;
    match e {
        // The caller chose an engine, a flag, or a keys file the plan
        // cannot be run with: theirs to change, as the message says.
        A::Unsupported { .. } | A::RowOffsetsNeeded { .. } | A::KeysFile { .. } => {
            Failure::Usage(e.to_string())
        }
        other => Failure::Engine(Error::Other(other.to_string())),
    }
}

/// Appends the artifact `args` asks for to a lookup's outcome: printed
/// after the plan line (human or `--json`), or written to `--out` with a
/// line saying where.
pub(crate) fn with_emit(
    engine: &Engine,
    result: &PlanResult,
    keys: Option<&Path>,
    args: &EmitArgs,
    outcome: Outcome,
) -> Result<Outcome, Failure> {
    let Some(target) = args.emit else {
        return Ok(outcome);
    };
    if args.ordered && result.ordered_by.is_none() {
        let reason = result
            .unordered_reason
            .clone()
            .unwrap_or_else(|| "the plan has no read order".into());
        return Err(Failure::Usage(format!(
            "--ordered: this plan has no read order ({reason}); emit without --ordered, or \
             declare the range key the reason names and re-plan"
        )));
    }
    if (args.footer_hint || args.engine_pruning) && target != EngineArg::Datafusion {
        return Err(Failure::Usage(format!(
            "--footer-hint and --engine-pruning set DataFusion's scan options; {} has neither, \
             so drop them or use --emit datafusion",
            target.name()
        )));
    }
    let keys_file = if args.keys_file_in_query {
        Some(keys.map(Path::to_path_buf).ok_or_else(|| {
            Failure::Usage(
                "--keys-file-in-query puts the --keys file in the query; this lookup has no \
                 key file (a text lookup), so drop the flag"
                    .into(),
            )
        })?)
    } else {
        None
    };
    let plan = plan_of(result)?;
    let inputs = engine.emit_inputs(plan)?;
    let schema: Vec<ColumnDef> = inputs
        .schema
        .fields()
        .iter()
        .map(|f| ColumnDef::new(f.name(), f.data_type().clone()))
        .collect();
    let opts = EmitOptions {
        keys_file,
        residual: match args.residual.unwrap_or(ResidualArg::Full) {
            ResidualArg::Full => ResidualMode::Full,
            ResidualArg::KeysOnly => ResidualMode::KeysOnly,
            ResidualArg::None => ResidualMode::None,
        },
        // A storage version 1 manifest records no row counts: read them
        // from the planned files' footers instead.
        footer_hint: args.footer_hint || inputs.row_counts.is_none(),
        row_offsets: inputs.row_counts.map(RowOffsets),
        engine_pruning: args.engine_pruning,
        partitions: inputs.partitions.map(|p| Partitions {
            scheme: p.scheme,
            columns: p.columns,
            files: p.files,
        }),
        ordered: args.ordered,
        relocate: None,
    };
    let adapter = bellwether_adapters::adapter(target.name()).ok_or_else(|| {
        Failure::Usage(format!("no adapter for {}", target.name()))
    })?;
    let text = adapter
        .emit(plan, &schema, &opts)
        .and_then(|a| a.to_text())
        .map_err(adapter_failure)?;
    let Outcome::Done(mut json, mut human, code) = outcome;
    match &args.out {
        Some(path) => {
            std::fs::write(path, format!("{text}\n")).map_err(|e| {
                Failure::Usage(format!(
                    "--out: cannot write {} ({e}); give a writable path",
                    path.display()
                ))
            })?;
            human.push_str(&format!(
                "wrote the {} artifact to {} (nothing was executed)\n",
                target.name(),
                path.display()
            ));
        }
        None => {
            json.push_str(&text);
            json.push('\n');
            human.push_str(&text);
            human.push('\n');
        }
    }
    Ok(Outcome::Done(json, human, code))
}
