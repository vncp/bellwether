//! `bellwether-bench`: command-line entry point. See the crate README.

#![forbid(unsafe_code)]
#![allow(clippy::doc_markdown, clippy::cast_precision_loss)]

use std::path::PathBuf;
use std::process::ExitCode;

use bellwether_bench::corpus::{GenParams, Rung};
use bellwether_bench::generate::generate;
use bellwether_bench::harness::{run_all, DuckDbMode, RunOptions, ScanFault};
use bellwether_bench::report::write_report;
use bellwether_bench::results::Results;
use bellwether_bench::slo::{assert_slo, describe_generator, run_smoke, Mode};
use bellwether_bench::truth::write_ground_truth;
use bellwether_bench::workload::{generate_workload, WorkloadParams};
use bellwether_bench::{BenchError, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(
    name = "bellwether-bench",
    version,
    about = "Bellwether benchmark (WP10, ADR 019)",
    args_conflicts_with_subcommands = true
)]
struct Cli {
    /// Assert SLOs with no subcommand: generate a small smoke corpus, run the
    /// harness, and assert strictly on its results (what CI runs). With
    /// `--results`, assert on that file instead. To assert after a run, use
    /// `run --assert-slo`.
    #[arg(long)]
    assert_slo: bool,
    /// Results file to assert on (with `--assert-slo`).
    #[arg(long, requires = "assert_slo")]
    results: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Write the synthetic versioned ledger.
    Generate(GenerateArgs),
    /// Write recorded incremental key sets for a corpus.
    Workload(WorkloadArgs),
    /// Scan a corpus and write expected.json (exact planted anti-patterns).
    GroundTruth {
        /// Corpus directory.
        #[arg(long)]
        corpus: PathBuf,
    },
    /// Run every recorded key set through every strategy.
    Run(RunArgs),
    /// Render results.json to a self-contained HTML report.
    Report {
        /// Results file.
        #[arg(long, default_value = "benches/out/results.json")]
        results: PathBuf,
        /// HTML output.
        #[arg(long, default_value = "benches/out/report.html")]
        out: PathBuf,
    },
}

/// A rung of the pre-registered corpus ladder (docs/benchmark.md).
#[derive(Clone, Copy, Debug, ValueEnum)]
enum RungArg {
    /// 10 days × 1 file per day, 1,000,000 entities.
    Small,
    /// 30 days × 4 files per day, 1,000,000 entities.
    Medium,
    /// 30 days × 32 files per day, 10,000,000 entities.
    Large,
}

impl RungArg {
    fn rung(self) -> Rung {
        match self {
            Self::Small => Rung::Small,
            Self::Medium => Rung::Medium,
            Self::Large => Rung::Large,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Small => "small",
            Self::Medium => "medium",
            Self::Large => "large",
        }
    }
}

#[derive(Args)]
struct GenerateArgs {
    /// A rung of the pre-registered ladder; sets every corpus parameter
    /// exactly (and the output directory to `benches/out/<rung>` unless
    /// `--out` is given).
    #[arg(long, value_enum, conflicts_with_all = [
        "seed", "files", "row_groups_per_file", "rows_per_row_group", "entities",
        "churn", "days", "payload_bytes", "late_fraction",
    ])]
    rung: Option<RungArg>,
    /// Generate the time-series `market` corpus (symbols × days × intraday
    /// rows, with restatements and late prints, Hive `day=` directories).
    #[arg(long, conflicts_with = "rung")]
    market: bool,
    /// Print what the corpus would contain and exit without writing.
    #[arg(long)]
    dry_run: bool,
    /// Output corpus directory [default: benches/out/corpus, or
    /// benches/out/<rung> with --rung].
    #[arg(long)]
    out: Option<PathBuf>,
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
    /// Distinct entities. The defaults make rows per day equal
    /// `churn * entities`, so each entity has at most one version per day.
    #[arg(long, default_value_t = 640_000)]
    entities: u64,
    /// Fraction of entities re-versioned per day.
    #[arg(long, default_value_t = 0.1)]
    churn: f64,
    /// Days (one partition each).
    #[arg(long, default_value_t = 16)]
    days: u32,
    /// Bytes of the wide payload column per row.
    #[arg(long, default_value_t = 128)]
    payload_bytes: u32,
    /// Fraction of rows with a back-dated version_ts.
    #[arg(long, default_value_t = 0.02)]
    late_fraction: f64,
}

#[derive(Args)]
struct WorkloadArgs {
    /// Corpus directory.
    #[arg(long, default_value = "benches/out/corpus")]
    corpus: PathBuf,
    /// The pre-registered workload: 200 runs, key sets at p50 / p95 / max of
    /// 50 / 10,000 / 100,000, seed 11.
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
#[allow(clippy::struct_excessive_bools)] // independent CLI flags
struct RunArgs {
    /// Corpus directories (repeat for latency versus corpus size).
    #[arg(long, required = true)]
    corpus: Vec<PathBuf>,
    /// Output results file.
    #[arg(long, default_value = "benches/out/results.json")]
    out: PathBuf,
    /// Plan only: do not read the planned row groups.
    #[arg(long)]
    no_scan: bool,
    /// Skip the DuckDB baseline even when compiled in.
    #[arg(long, conflicts_with = "require_duckdb")]
    no_duckdb: bool,
    /// Fail unless the DuckDB baseline runs (needs `--features duckdb` and a
    /// `duckdb` CLI on PATH or in $BELLWETHER_DUCKDB).
    #[arg(long)]
    require_duckdb: bool,
    /// Assert SLOs on the results this run writes.
    #[arg(long)]
    assert_slo: bool,
}

fn finish_slo(results: &Results, mode: Mode) -> Result<()> {
    let report = assert_slo(results, mode)?;
    print!("{}", report.render());
    println!("all SLO checks passed");
    Ok(())
}

fn cmd_generate(a: GenerateArgs) -> Result<()> {
    if a.market {
        let mparams = bellwether_bench::market::MarketParams {
            seed: a.seed,
            days: a.days,
            files_per_day: (a.files / a.days.max(1)).max(1),
            row_groups_per_file: a.row_groups_per_file,
            rows_per_row_group: a.rows_per_row_group,
            symbols: u32::try_from(a.entities).unwrap_or(200).clamp(10, 50_000),
            restatement_fraction: a.late_fraction,
        };
        let out = a
            .out
            .unwrap_or_else(|| PathBuf::from("benches/out/market"));
        let manifest = bellwether_bench::market::generate_market(&mparams, &out)?;
        println!(
            "wrote market corpus ({} rows in {} row groups, {} files) to {}",
            manifest.rows_total,
            manifest.row_groups_total,
            manifest.files_total,
            out.display()
        );
        return Ok(());
    }
    let params = match a.rung {
        Some(r) => GenParams::rung(r.rung()),
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
    params.validate()?;
    let out = a.out.unwrap_or_else(|| {
        PathBuf::from("benches/out").join(a.rung.map_or("corpus", RungArg::name))
    });
    if a.dry_run {
        let summary = params.summary();
        println!("{}", serde_json::to_string_pretty(&(&params, &summary))?);
        println!(
            "would write {} rows in {} row groups ({} files) to {}; Arrow size in memory \
                 (estimate) about {:.1} GB; \
                 {:.1} versions per re-versioned entity per day; generator RSS bound {} B",
            summary.rows,
            summary.row_groups,
            summary.files,
            out.display(),
            summary.estimated_arrow_bytes as f64 / 1e9,
            summary.versions_per_active_entity_per_day,
            bellwether_bench::generate::rss_growth_bound_bytes(&params)
        );
        return Ok(());
    }
    let stats = generate(&params, &out)?;
    println!(
        "wrote {} rows in {} row groups to {} ({:.0} ms)",
        stats.rows_written,
        stats.row_groups_written,
        out.display(),
        stats.elapsed_ms,
    );
    println!("{}", describe_generator(&params, &stats));
    Ok(())
}

fn real_main(cli: Cli) -> Result<()> {
    match cli.command {
        None => {
            if !cli.assert_slo {
                return Err(BenchError::Invalid(
                    "no subcommand; see --help (or pass --assert-slo)".into(),
                ));
            }
            // A results file is judged leniently (a --no-scan run may skip
            // row checks); the smoke run CI uses is strict: skips fail.
            let (results, mode) = match cli.results {
                Some(path) => (
                    serde_json::from_slice(
                        &std::fs::read(&path).map_err(|source| BenchError::Io { path, source })?,
                    )?,
                    Mode::Lenient,
                ),
                None => (
                    run_smoke(&PathBuf::from("benches/out/slo"), &RunOptions::default())?,
                    Mode::Strict,
                ),
            };
            finish_slo(&results, mode)
        }
        Some(Command::Generate(a)) => cmd_generate(a),
        Some(Command::Workload(a)) => {
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
            let m = generate_workload(&a.corpus, &params)?;
            println!(
                "wrote {} runs (keys p50/p95/max {:?}{}) hash {}",
                m.runs.len(),
                m.sizes_p50_p95_max,
                if m.clamped {
                    ", clamped to available entities"
                } else {
                    ""
                },
                m.hash
            );
            Ok(())
        }
        Some(Command::GroundTruth { corpus }) => {
            let e = write_ground_truth(&corpus)?;
            for ap in &e.anti_patterns {
                println!("{:<26} {}", ap.rule, ap.finding);
            }
            Ok(())
        }
        Some(Command::Run(a)) => {
            let roots: Vec<&std::path::Path> = a.corpus.iter().map(PathBuf::as_path).collect();
            let options = RunOptions {
                scan: !a.no_scan,
                duckdb: if a.no_duckdb {
                    DuckDbMode::Off
                } else if a.require_duckdb {
                    DuckDbMode::Required
                } else {
                    DuckDbMode::Auto
                },
                fault: ScanFault::None,
            };
            let results = run_all(&roots, &a.out, &options)?;
            println!("wrote {}", a.out.display());
            if a.assert_slo {
                finish_slo(&results, Mode::Lenient)?;
            }
            Ok(())
        }
        Some(Command::Report { results, out }) => {
            write_report(&results, &out)?;
            println!("wrote {}", out.display());
            Ok(())
        }
    }
}

fn main() -> ExitCode {
    match real_main(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
