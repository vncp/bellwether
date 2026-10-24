//! `bellwether bench`: the benchmark (WP10) through its public library
//! functions. Nothing here measures anything itself.

use std::path::{Path, PathBuf};

use bellwether::types::{Error, Hash};
use bellwether_bench::corpus::{GenParams, Rung};
use bellwether_bench::harness::{run_all, DuckDbMode, RunOptions, ScanFault};
use bellwether_bench::report::write_report;
use bellwether_bench::results::Results;
use bellwether_bench::slo::{assert_slo, Mode};
use bellwether_bench::truth::write_ground_truth;
use bellwether_bench::workload::{generate_workload, WorkloadParams};
use clap::{Args, Subcommand, ValueEnum};
use serde::Serialize;

/// Benchmark subcommands.
#[derive(Subcommand)]
pub(crate) enum BenchCommand {
    /// Write the synthetic versioned ledger.
    Generate(GenerateArgs),
    /// Write recorded key sets for a corpus.
    Workload(WorkloadArgs),
    /// Scan a corpus and write expected.json (planted anti-patterns).
    GroundTruth {
        /// Corpus directory.
        #[arg(long)]
        corpus: PathBuf,
    },
    /// Run every recorded key set through every baseline strategy.
    Run(RunArgs),
    /// Render results.json as a self-contained HTML report.
    Report {
        /// Results file.
        #[arg(long)]
        results: PathBuf,
        /// HTML output.
        #[arg(long)]
        out: PathBuf,
    },
    /// Assert the SLOs on a results file (exit 1 if any check fails).
    AssertSlo {
        /// Results file.
        #[arg(long)]
        results: PathBuf,
    },
}

#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum RungArg {
    /// 10 days, 10 files, 1,000,000 entities.
    Small,
    /// 30 days, 120 files, 1,000,000 entities.
    Medium,
    /// 30 days, 960 files, 10,000,000 entities.
    Large,
}

#[derive(Args)]
pub(crate) struct GenerateArgs {
    /// Output corpus directory.
    #[arg(long)]
    out: PathBuf,
    /// A rung of the pre-registered ladder (docs/benchmark.md); sets every
    /// parameter.
    #[arg(long, value_enum, conflicts_with_all = ["seed", "files", "row_groups_per_file",
        "rows_per_row_group", "entities", "churn", "days", "payload_bytes", "late_fraction"])]
    rung: Option<RungArg>,
    /// RNG seed.
    #[arg(long, default_value_t = 42)]
    seed: u64,
    /// Total files (at least `days`).
    #[arg(long, default_value_t = 256)]
    files: u32,
    /// Row groups per file.
    #[arg(long, default_value_t = 2)]
    row_groups_per_file: u32,
    /// Rows per row group.
    #[arg(long, default_value_t = 2000)]
    rows_per_row_group: u32,
    /// Distinct entities.
    #[arg(long, default_value_t = 640_000)]
    entities: u64,
    /// Fraction of entities re-versioned per day.
    #[arg(long, default_value_t = 0.1)]
    churn: f64,
    /// Days, one partition each.
    #[arg(long, default_value_t = 16)]
    days: u32,
    /// Bytes of the wide payload column per row.
    #[arg(long, default_value_t = 128)]
    payload_bytes: u32,
    /// Fraction of rows with a back-dated `version_ts`.
    #[arg(long, default_value_t = 0.02)]
    late_fraction: f64,
}

#[derive(Args)]
pub(crate) struct WorkloadArgs {
    /// Corpus directory.
    #[arg(long)]
    corpus: PathBuf,
    /// The pre-registered workload (200 runs at 50 / 10,000 / 100,000 keys,
    /// seed 11).
    #[arg(long, conflicts_with_all = ["seed", "runs", "p50", "p95", "max"])]
    preregistered: bool,
    /// RNG seed.
    #[arg(long, default_value_t = 7)]
    seed: u64,
    /// Recorded runs.
    #[arg(long, default_value_t = 48)]
    runs: u32,
    /// Median keys per run.
    #[arg(long, default_value_t = 40)]
    p50: u64,
    /// p95 keys per run.
    #[arg(long, default_value_t = 120_000)]
    p95: u64,
    /// Largest run.
    #[arg(long, default_value_t = 415_000)]
    max: u64,
}

#[derive(Args)]
pub(crate) struct RunArgs {
    /// Corpus directories (repeat for latency against corpus size).
    #[arg(long, required = true)]
    corpus: Vec<PathBuf>,
    /// Output results file.
    #[arg(long)]
    out: PathBuf,
    /// Plan only: do not read the planned row groups.
    #[arg(long)]
    no_scan: bool,
    /// Skip the `DuckDB` baseline.
    #[arg(long)]
    no_duckdb: bool,
}

/// A file the benchmark wrote.
#[derive(Serialize)]
pub(crate) struct Wrote {
    /// What was written.
    pub what: &'static str,
    /// Where.
    pub path: String,
    /// BLAKE3 of the file, when it is one file.
    pub blake3: Option<Hash>,
    /// SLO checks that failed (`assert-slo` only).
    pub failed: Vec<String>,
}

fn bench_err(e: &bellwether_bench::BenchError) -> Error {
    Error::Other(format!("benchmark: {e}"))
}

fn file_hash(path: &Path) -> Result<Option<Hash>, Error> {
    if path.is_file() {
        let bytes = std::fs::read(path)
            .map_err(|e| Error::Other(format!("cannot read {} ({e})", path.display())))?;
        Ok(Some(Hash::of(&bytes)))
    } else {
        Ok(None)
    }
}

/// Runs one benchmark subcommand. Returns what it wrote.
pub(crate) fn run(cmd: &BenchCommand) -> Result<Wrote, Error> {
    let (what, path, failed) = match cmd {
        BenchCommand::Generate(a) => {
            let params = match a.rung {
                Some(RungArg::Small) => GenParams::rung(Rung::Small),
                Some(RungArg::Medium) => GenParams::rung(Rung::Medium),
                Some(RungArg::Large) => GenParams::rung(Rung::Large),
                None => GenParams {
                    seed: a.seed,
                    files: a.files,
                    row_groups_per_file: a.row_groups_per_file,
                    rows_per_row_group: a.rows_per_row_group,
                    entities: a.entities,
                    churn: a.churn,
                    days: a.days,
                    payload_bytes: a.payload_bytes,
                    late_fraction: a.late_fraction,
                },
            };
            params.validate().map_err(|e| bench_err(&e))?;
            bellwether_bench::generate::generate(&params, &a.out).map_err(|e| bench_err(&e))?;
            ("corpus", a.out.join("corpus.json"), Vec::new())
        }
        BenchCommand::Workload(a) => {
            let params = if a.preregistered {
                WorkloadParams::preregistered()
            } else {
                WorkloadParams {
                    seed: a.seed,
                    runs: a.runs,
                    p50: a.p50,
                    p95: a.p95,
                    max: a.max,
                }
            };
            generate_workload(&a.corpus, &params).map_err(|e| bench_err(&e))?;
            ("workload", a.corpus.join("workload.json"), Vec::new())
        }
        BenchCommand::GroundTruth { corpus } => {
            write_ground_truth(corpus).map_err(|e| bench_err(&e))?;
            ("ground truth", corpus.join("expected.json"), Vec::new())
        }
        BenchCommand::Run(a) => {
            let roots: Vec<&Path> = a.corpus.iter().map(PathBuf::as_path).collect();
            let options = RunOptions {
                scan: !a.no_scan,
                duckdb: if a.no_duckdb {
                    DuckDbMode::Off
                } else {
                    DuckDbMode::Auto
                },
                fault: ScanFault::None,
            };
            run_all(&roots, &a.out, &options).map_err(|e| bench_err(&e))?;
            ("results", a.out.clone(), Vec::new())
        }
        BenchCommand::Report { results, out } => {
            write_report(results, out).map_err(|e| bench_err(&e))?;
            ("report", out.clone(), Vec::new())
        }
        BenchCommand::AssertSlo { results } => {
            let bytes = std::fs::read(results)
                .map_err(|e| Error::Other(format!("cannot read {} ({e})", results.display())))?;
            let parsed: Results = serde_json::from_slice(&bytes)?;
            let failed = match assert_slo(&parsed, Mode::Lenient) {
                Ok(_) => Vec::new(),
                Err(e) => vec![e.to_string()],
            };
            ("slo", results.clone(), failed)
        }
    };
    Ok(Wrote {
        what,
        path: path.display().to_string(),
        blake3: file_hash(&path)?,
        failed,
    })
}
