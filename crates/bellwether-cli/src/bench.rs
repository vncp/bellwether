//! `bellwether bench`: the benchmark (WP10) through its public library
//! functions. Nothing here measures anything itself.

use std::path::{Path, PathBuf};

use bellwether::types::{Error, Hash};
use bellwether_bench::corpus::{GenParams, Rung};
use bellwether_bench::expr::{generate_expr, ExprParams, ExprShape, ExprSummary};
use bellwether_bench::harness::{run_all, DuckDbMode, RunOptions, ScanFault};
use bellwether_bench::market::{
    generate_market, parse_ymd, MarketParams, MarketRung, MarketSummary,
};
use bellwether_bench::perf::{all_cells, plan_cells, read_row_groups, ReadReport};
use bellwether_bench::report::write_report;
use bellwether_bench::results::Results;
use bellwether_bench::slo::{assert_slo, Mode};
use bellwether_bench::text::{generate_text, TextParams, TextShape, TextSummary};
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
    /// Write the synthetic bitemporal market trade corpus, workload, and ground truth.
    Market(MarketArgs),
    /// Write a text corpus (1–4 KB documents with planted terms at 1% and
    /// 0.01% of rows) in the ledger or market shape, with ground truth, for
    /// text keys (`text:body,tokenizer=words|trigram`).
    Text(TextArgs),
    /// Read row groups through the benchmark's byte-counting reader and
    /// report the time and bytes: the planned row groups of a lookup's
    /// --json output (--plan), or every row group under a folder (--all).
    Read(ReadArgs),
    /// Write a corpus with a JSON attribute column (ledger or market shape),
    /// key files for equality on a JSON path at three selectivities, and
    /// exact ground truth: the `expr` key benchmark.
    Expr(ExprArgs),
}

#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum ExprShapeArg {
    /// A versioned ledger whose `attrs` JSON holds an `account` join key.
    Ledger,
    /// Market prints whose vendor `payload` JSON holds the `symbol`.
    Market,
}

#[derive(Args)]
pub(crate) struct ExprArgs {
    /// Output directory (`data/` and `workload/`).
    #[arg(long)]
    out: PathBuf,
    /// The shape.
    #[arg(long, value_enum)]
    shape: ExprShapeArg,
    /// RNG seed.
    #[arg(long)]
    seed: Option<u64>,
    /// Files [default: 10].
    #[arg(long)]
    files: Option<u64>,
    /// Row groups per file [default: 20].
    #[arg(long)]
    row_groups_per_file: Option<u64>,
    /// Rows per row group, a multiple of 4 [default: 1000].
    #[arg(long)]
    rows_per_row_group: Option<u64>,
    /// Keys per rare or medium query [default: 3].
    #[arg(long)]
    keys_per_query: Option<u64>,
}

#[derive(Args)]
#[command(group(clap::ArgGroup::new("what").required(true).args(["plan", "all"])))]
pub(crate) struct ReadArgs {
    /// The --json output of `locate`, `latest`, or `asof`, saved to a file;
    /// its plan's row groups are read.
    #[arg(long, value_name = "FILE")]
    plan: Option<PathBuf>,
    /// A data folder: every row group of every .parquet file under it (a
    /// full scan; directories starting with `.` are skipped).
    #[arg(long, value_name = "DIR")]
    all: Option<PathBuf>,
}

#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum TextShapeArg {
    /// `entity_id`, `version_ts`, `body`: attribute blobs on a ledger.
    Ledger,
    /// `symbol`, `ts`, `body`: documents keyed by symbol and date.
    Market,
}

#[derive(Args)]
pub(crate) struct TextArgs {
    /// Output directory (`data/` and `workload/text-truth.json`).
    #[arg(long)]
    out: PathBuf,
    /// The shape the documents ride on.
    #[arg(long, value_enum)]
    shape: TextShapeArg,
    /// RNG seed [default: 5].
    #[arg(long)]
    seed: Option<u64>,
    /// Day partitions [default: 3].
    #[arg(long)]
    days: Option<u32>,
    /// Files per day [default: 2].
    #[arg(long)]
    files_per_day: Option<u32>,
    /// Row groups per file [default: 4].
    #[arg(long)]
    row_groups_per_file: Option<u32>,
    /// Rows per row group [default: 500].
    #[arg(long)]
    rows_per_row_group: Option<u32>,
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

#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum MarketRungArg {
    /// 50 symbols × 10 days, 2 files/day, 500 rows/row group, 200 prints/symbol/day.
    Small,
    /// 200 symbols × 30 days, 4 files/day, 2,000 rows/row group, 300 prints/symbol/day.
    Medium,
}

#[derive(Args)]
pub(crate) struct MarketArgs {
    /// Output directory for the market corpus (`data/` and `workload/`).
    #[arg(long)]
    out: PathBuf,
    /// A preset rung (`small` or `medium`); sets default corpus parameters
    /// (individual flags still override when given).
    #[arg(long, value_enum)]
    rung: Option<MarketRungArg>,
    /// RNG seed.
    #[arg(long)]
    seed: Option<u64>,
    /// Number of symbols (`S0001..`).
    #[arg(long)]
    symbols: Option<u32>,
    /// Number of partition days.
    #[arg(long)]
    days: Option<u32>,
    /// First partition date (`YYYY-MM-DD`).
    #[arg(long)]
    start: Option<String>,
    /// Parquet files per day partition.
    #[arg(long)]
    files_per_day: Option<u32>,
    /// Target rows per row group.
    #[arg(long)]
    rows_per_row_group: Option<u32>,
    /// Base prints per symbol per day.
    #[arg(long)]
    prints_per_symbol_per_day: Option<u32>,
    /// Fraction of on-time prints that also produce a corrected restatement row 1–3 days later.
    #[arg(long)]
    restatement_fraction: Option<f64>,
    /// Fraction of base prints that arrive 1–3 days late.
    #[arg(long)]
    late_fraction: Option<f64>,
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
    /// Market corpus summary (`market` only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub market: Option<MarketSummary>,
    /// What the read cost (`read` only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read: Option<ReadReport>,
    /// Text corpus summary and ground truth (`text` only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<TextSummary>,
    /// Expr corpus summary (`expr` only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expr: Option<ExprSummary>,
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
    let (what, path, failed, market) = match cmd {
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
            ("corpus", a.out.join("corpus.json"), Vec::new(), None)
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
            ("workload", a.corpus.join("workload.json"), Vec::new(), None)
        }
        BenchCommand::GroundTruth { corpus } => {
            write_ground_truth(corpus).map_err(|e| bench_err(&e))?;
            (
                "ground truth",
                corpus.join("expected.json"),
                Vec::new(),
                None,
            )
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
            ("results", a.out.clone(), Vec::new(), None)
        }
        BenchCommand::Report { results, out } => {
            write_report(results, out).map_err(|e| bench_err(&e))?;
            ("report", out.clone(), Vec::new(), None)
        }
        BenchCommand::AssertSlo { results } => {
            let bytes = std::fs::read(results)
                .map_err(|e| Error::Other(format!("cannot read {} ({e})", results.display())))?;
            let parsed: Results = serde_json::from_slice(&bytes)?;
            let failed = match assert_slo(&parsed, Mode::Lenient) {
                Ok(_) => Vec::new(),
                Err(e) => vec![e.to_string()],
            };
            ("slo", results.clone(), failed, None)
        }
        BenchCommand::Market(a) => return market(a),
        BenchCommand::Text(a) => return text(a),
        BenchCommand::Read(a) => return read(a),
        BenchCommand::Expr(a) => return expr(a),
    };
    Ok(Wrote {
        what,
        path: path.display().to_string(),
        blake3: file_hash(&path)?,
        failed,
        market,
        read: None,
        text: None,
        expr: None,
    })
}

/// `bench market`: the rung's parameters, each flag given overriding one.
fn text(a: &TextArgs) -> Result<Wrote, Error> {
    let mut params = TextParams::small(match a.shape {
        TextShapeArg::Ledger => TextShape::Ledger,
        TextShapeArg::Market => TextShape::Market,
    });
    if let Some(v) = a.seed {
        params.seed = v;
    }
    if let Some(v) = a.days {
        params.days = v;
    }
    if let Some(v) = a.files_per_day {
        params.files_per_day = v;
    }
    if let Some(v) = a.row_groups_per_file {
        params.row_groups_per_file = v;
    }
    if let Some(v) = a.rows_per_row_group {
        params.rows_per_row_group = v;
    }
    let summary = generate_text(&a.out, &params).map_err(|e| bench_err(&e))?;
    let truth = a.out.join("workload").join("text-truth.json");
    Ok(Wrote {
        what: "text corpus",
        path: format!(
            "{} ({} files, {} row groups, {} rows, {} bytes of text)",
            a.out.display(),
            summary.files,
            summary.row_groups,
            summary.rows,
            summary.body_bytes
        ),
        blake3: file_hash(&truth)?,
        failed: Vec::new(),
        market: None,
        read: None,
        expr: None,
        text: Some(summary),
    })
}

fn market(a: &MarketArgs) -> Result<Wrote, Error> {
    let mut params = match a.rung.unwrap_or(MarketRungArg::Small) {
        MarketRungArg::Small => MarketParams::rung(MarketRung::Small),
        MarketRungArg::Medium => MarketParams::rung(MarketRung::Medium),
    };
    if let Some(v) = a.seed {
        params.seed = v;
    }
    if let Some(v) = a.symbols {
        params.symbols = v;
    }
    if let Some(v) = a.days {
        params.days = v;
    }
    if let Some(s) = &a.start {
        params.start = parse_ymd(s).map_err(|e| bench_err(&e))?;
    }
    if let Some(v) = a.files_per_day {
        params.files_per_day = v;
    }
    if let Some(v) = a.rows_per_row_group {
        params.rows_per_row_group = v;
    }
    if let Some(v) = a.prints_per_symbol_per_day {
        params.prints_per_symbol_per_day = v;
    }
    if let Some(v) = a.restatement_fraction {
        params.restatement_fraction = v;
    }
    if let Some(v) = a.late_fraction {
        params.late_fraction = v;
    }
    params.validate().map_err(|e| bench_err(&e))?;
    let summary = generate_market(&a.out, &params).map_err(|e| bench_err(&e))?;
    let where_summary = format!(
        "{} ({} files, {} row groups, {} rows, {} restatements, {} late prints, {} bytes)",
        a.out.display(),
        summary.files,
        summary.row_groups,
        summary.rows,
        summary.restatements,
        summary.late_prints,
        summary.bytes,
    );
    let truth_path = a.out.join("workload").join("truth.json");
    Ok(Wrote {
        what: "market corpus",
        path: where_summary,
        blake3: file_hash(&truth_path)?,
        failed: Vec::new(),
        market: Some(summary),
        read: None,
        expr: None,
        text: None,
    })
}

/// `bench expr`: the preset for the shape, each flag given overriding one.
fn expr(a: &ExprArgs) -> Result<Wrote, Error> {
    let shape = match a.shape {
        ExprShapeArg::Ledger => ExprShape::Ledger,
        ExprShapeArg::Market => ExprShape::Market,
    };
    let mut params = ExprParams::small(shape);
    if let Some(v) = a.seed {
        params.seed = v;
    }
    if let Some(v) = a.files {
        params.files = v;
    }
    if let Some(v) = a.row_groups_per_file {
        params.row_groups_per_file = v;
    }
    if let Some(v) = a.rows_per_row_group {
        params.rows_per_row_group = v;
    }
    if let Some(v) = a.keys_per_query {
        params.keys_per_query = v;
    }
    let summary = generate_expr(&a.out, &params).map_err(|e| bench_err(&e))?;
    let truth_path = a.out.join("workload").join("truth.json");
    Ok(Wrote {
        what: "expr corpus",
        path: format!(
            "{} ({} shape, {} files, {} row groups, {} rows, {} rows without the keyed member, \
             {} bytes, {} queries)",
            a.out.display(),
            shape.as_str(),
            summary.files,
            summary.row_groups,
            summary.rows,
            summary.null_rows,
            summary.bytes,
            summary.queries
        ),
        blake3: file_hash(&truth_path)?,
        failed: Vec::new(),
        market: None,
        read: None,
        expr: Some(summary),
        text: None,
    })
}

/// `bench read`: the row groups of a plan, or of a whole folder, through the
/// byte-counting reader.
fn read(a: &ReadArgs) -> Result<Wrote, Error> {
    let (source, cells) = match (&a.plan, &a.all) {
        (Some(plan), _) => {
            let bytes = std::fs::read(plan)
                .map_err(|e| Error::Other(format!("cannot read {} ({e})", plan.display())))?;
            let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(|e| {
                Error::Other(format!(
                    "{} is not JSON ({e}); save the --json output of locate, latest, or asof",
                    plan.display()
                ))
            })?;
            (plan, plan_cells(&value).map_err(|e| bench_err(&e))?)
        }
        (None, Some(dir)) => (dir, all_cells(dir).map_err(|e| bench_err(&e))?),
        (None, None) => {
            return Err(Error::Other(
                "bench read needs --plan FILE or --all DIR".into(),
            ))
        }
    };
    let report = read_row_groups(&cells).map_err(|e| bench_err(&e))?;
    Ok(Wrote {
        what: "read",
        path: format!(
            "{} ({} files, {} row groups, {} rows, {} bytes fetched, {:.1} ms)",
            source.display(),
            report.files,
            report.row_groups,
            report.rows,
            report.bytes_fetched,
            report.total_ms
        ),
        blake3: None,
        failed: Vec::new(),
        market: None,
        read: Some(report),
        text: None,
        expr: None,
    })
}
