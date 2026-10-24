//! `bellwether`: the command line over the facade (ADR 010). This crate
//! parses arguments and formats results; every operation is one call into
//! `bellwether::Engine`, and `--json` prints that call's typed result,
//! serialized, one JSON object per line.
//!
//! Exit codes: 0 success; 1 a certificate failed or mismatches were found;
//! 2 usage; 3 refused at the trust boundary; 4 engine error.

#![forbid(unsafe_code)]

mod bench;
mod human;
mod verify_cmds;

use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use bellwether::types::{Error, Hash, ProposalBody, RewriteShape};
use bellwether::{Engine, EngineOptions, KeyFile, SourceFormat};
use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::Serialize;

/// Exit code for success.
pub(crate) const EXIT_OK: u8 = 0;
/// A certificate failed, mismatches were found, or a check failed.
pub(crate) const EXIT_FAILED: u8 = 1;
/// Refused at the trust boundary.
pub(crate) const EXIT_REFUSED: u8 = 3;
/// A usage error (clap uses the same code for its own).
const EXIT_USAGE: u8 = 2;
/// An engine error.
const EXIT_ENGINE: u8 = 4;

#[derive(Parser)]
#[command(
    name = "bellwether",
    version,
    about = "Find out why lake queries read more than they return, build the index that fixes \
             it, and prove the fix returned the same rows."
)]
struct Cli {
    #[command(flatten)]
    global: Global,
    #[command(subcommand)]
    command: Command,
}

#[derive(Args)]
pub(crate) struct Global {
    /// Print the typed result as JSON, one object per line (the same value
    /// the MCP server returns).
    #[arg(long, global = true)]
    json: bool,
    /// Print nothing on success; errors still go to stderr.
    #[arg(long, short, global = true, conflicts_with = "json")]
    quiet: bool,
    /// Index location: a path or a store URL [default: <ROOT>/.bellwether].
    #[arg(long, global = true, value_name = "PATH|URL")]
    index: Option<String>,
    /// Kind of files under the root.
    #[arg(long, global = true, value_enum, default_value_t = FormatArg::Parquet)]
    format: FormatArg,
    /// Chunk size in bytes for CSV and JSON lines files seen for the first
    /// time.
    #[arg(long, global = true, value_name = "BYTES")]
    chunk_size: Option<u64>,
    /// The dataset schema as a JSON file (a list of {"name", "type",
    /// "nullable"} with Arrow type spellings), instead of inferring it from
    /// the first file; needed when files differ in decimal scale or time
    /// unit.
    #[arg(long, global = true, value_name = "FILE")]
    schema: Option<PathBuf>,
    /// Which bytes a file fingerprint hashes when a new index is built:
    /// the last 64 KiB (default) or the whole file, which also catches an
    /// in-place patch that keeps size and mtime.
    #[arg(long, global = true, value_enum)]
    fingerprint_policy: Option<PolicyArg>,
    /// A fixed RFC 3339 time for every record's `created_at` and for ages, so
    /// runs are byte-reproducible (testing and audits).
    #[arg(long, global = true, value_name = "RFC3339", hide = true)]
    clock: Option<String>,
}

#[derive(Clone, Copy, ValueEnum)]
enum PolicyArg {
    /// BLAKE3 of the last 64 KiB.
    Tail64k,
    /// BLAKE3 of the whole file.
    Full,
}

#[derive(Clone, Copy, ValueEnum)]
enum FormatArg {
    /// Parquet files.
    Parquet,
    /// Comma-separated files with a header row.
    Csv,
    /// JSON lines.
    Jsonl,
}

#[derive(Subcommand)]
enum Command {
    /// Profile the folder and, with keys, replay them under every candidate
    /// key: the funnel, the culprit column, and the recommended key.
    Why(WhyArgs),
    /// Build or update the index for one or more key specs.
    Index(IndexArgs),
    /// Plan which files and row groups hold the keys.
    Locate(LookupArgs),
    /// Plan the newest version of each key.
    Latest(LookupArgs),
    /// Plan the version of each key current at an instant.
    Asof(AsofArgs),
    /// Record a proposal (a key, a layout, or a rewrite) citing evidence.
    Propose(ProposeArgs),
    /// Show a receipt and the chain behind it.
    Receipt {
        #[command(subcommand)]
        command: ReceiptCommand,
    },
    /// Render every receipt and profile from the ledger as one HTML file.
    Report(ReportArgs),
    #[command(flatten)]
    Verify(verify_cmds::VerifyCommand),
    /// The benchmark (WP10): generate corpora and workloads, run the
    /// baselines, report, assert SLOs.
    Bench {
        #[command(subcommand)]
        command: bench::BenchCommand,
    },
    /// Serve the engine over MCP (arrives with WP12).
    Mcp,
}

#[derive(Args)]
pub(crate) struct RootArg {
    /// The folder of data files.
    pub(crate) root: String,
}

#[derive(Args)]
struct WhyArgs {
    #[command(flatten)]
    root: RootArg,
    /// A Parquet file of keys one run looked up.
    #[arg(long, value_name = "FILE")]
    from: Option<PathBuf>,
    /// Key columns to take from --from, comma-separated [default: all].
    #[arg(long, value_name = "COLS", requires = "from", value_delimiter = ',')]
    key: Option<Vec<String>>,
    /// A recorded workload in the benchmark's format (directory with
    /// workload.json).
    #[arg(long, value_name = "DIR", conflicts_with_all = ["from", "workload"])]
    bench_workload: Option<PathBuf>,
    /// A workload recorded earlier, by its hash.
    #[arg(long, value_name = "HASH", conflicts_with = "from")]
    workload: Option<String>,
    /// Timestamp (or date, time, or integer version) column for temporal
    /// pricing (`latest`, or `asof` when `--at` is also given).
    #[arg(long, value_name = "COL", conflicts_with = "bench_workload")]
    latest_by: Option<String>,
    /// The instant for `asof` pricing (with `--latest-by`): RFC 3339, a date
    /// (YYYY-MM-DD), or an integer in the timestamp column's unit.
    #[arg(long, value_name = "INSTANT", requires = "latest_by")]
    at: Option<String>,
    /// Candidate key columns for the profile, comma-separated [default:
    /// every id-like column].
    #[arg(long, value_name = "COLS", value_delimiter = ',')]
    columns: Option<Vec<String>>,
    /// Most profile candidates replayed beyond the workload's own key
    /// columns.
    #[arg(long, value_name = "N", default_value_t = bellwether::DEFAULT_MAX_CANDIDATES)]
    max_candidates: usize,
}

#[derive(Args)]
struct IndexArgs {
    #[command(flatten)]
    root: RootArg,
    /// A key spec: exact:a,b  temporal:id@ts  bloom:a@0.01  range:ts.
    /// Repeat for several.
    #[arg(long = "key", value_name = "SPEC")]
    keys: Vec<String>,
}

#[derive(Args)]
struct LookupArgs {
    #[command(flatten)]
    root: RootArg,
    /// A Parquet file of keys.
    #[arg(long, value_name = "FILE")]
    keys: PathBuf,
    /// Key columns to take from the file, comma-separated [default: all].
    #[arg(long, value_name = "COLS", value_delimiter = ',')]
    key: Option<Vec<String>>,
}

#[derive(Args)]
struct AsofArgs {
    #[command(flatten)]
    lookup: LookupArgs,
    /// The instant: RFC 3339, a date (YYYY-MM-DD), or an integer in the
    /// timestamp column's unit.
    #[arg(long, value_name = "INSTANT")]
    at: String,
}

#[derive(Args)]
struct ProposeArgs {
    #[command(flatten)]
    root: RootArg,
    /// Evidence hash the proposal cites (repeat for several).
    #[arg(long = "evidence", value_name = "HASH", required = true)]
    evidence: Vec<String>,
    /// Who proposes: an agent name or a user.
    #[arg(long, value_name = "NAME", default_value = "cli")]
    by: String,
    /// Propose an index key (a key spec, as for `index`).
    #[arg(long, value_name = "SPEC", group = "body")]
    key: Option<String>,
    /// Propose clustering files by these columns, comma-separated.
    #[arg(long, value_name = "COLS", value_delimiter = ',', group = "body")]
    layout: Option<Vec<String>>,
    /// Propose a rewrite: the current shape (with --after and --invariant).
    #[arg(long, value_name = "TEXT", group = "body", requires_all = ["after", "invariant"])]
    before: Option<String>,
    /// The rewrite's target shape.
    #[arg(long, value_name = "TEXT", requires = "before")]
    after: Option<String>,
    /// What the rewrite must preserve.
    #[arg(long, value_name = "TEXT", requires = "before")]
    invariant: Option<String>,
}

#[derive(Subcommand)]
enum ReceiptCommand {
    /// Walk a receipt back through its certificate, replay, proposal, and
    /// evidence.
    Show {
        #[command(flatten)]
        root: RootArg,
        /// The receipt hash.
        hash: String,
    },
}

#[derive(Args)]
struct ReportArgs {
    #[command(flatten)]
    root: RootArg,
    /// Output file, or an existing directory (gets report.html).
    #[arg(long, value_name = "PATH")]
    html: PathBuf,
}

/// How output is shaped.
pub(crate) struct Out {
    pub json: bool,
    pub quiet: bool,
    pub color: bool,
}

/// What a command produced: what to print and how it exits.
pub(crate) enum Outcome {
    Done(String, String, u8),
}

fn main() -> ExitCode {
    std::panic::set_hook(Box::new(|info| {
        let what = info
            .payload()
            .downcast_ref::<&str>()
            .map(ToString::to_string)
            .or_else(|| info.payload().downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "unknown panic".to_string());
        eprintln!(
            "bellwether: internal error ({what}); please report it at {}",
            bellwether::ISSUE_TRACKER
        );
        std::process::exit(i32::from(EXIT_ENGINE));
    }));
    // Debug builds only: lets the tests prove the hook is installed.
    assert!(
        !(cfg!(debug_assertions) && std::env::var_os("BELLWETHER_PANIC_PROBE").is_some()),
        "panic probe"
    );
    let cli = Cli::parse();
    let out = Out {
        json: cli.global.json,
        quiet: cli.global.quiet,
        color: std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none(),
    };
    match run(&cli, &out) {
        Ok(Outcome::Done(json, human, code)) => {
            let text = if out.json { json } else { human };
            if !out.quiet && !text.is_empty() {
                let mut stdout = std::io::stdout().lock();
                // A closed pipe is not an error worth a panic.
                let _ = stdout.write_all(text.as_bytes());
                let _ = stdout.flush();
            }
            ExitCode::from(code)
        }
        Err(Failure::Usage(msg)) => {
            eprintln!("bellwether: {msg}");
            ExitCode::from(EXIT_USAGE)
        }
        Err(Failure::Engine(e)) => {
            eprintln!("bellwether: {e}");
            ExitCode::from(EXIT_ENGINE)
        }
    }
}

pub(crate) enum Failure {
    Usage(String),
    Engine(Error),
}

impl From<Error> for Failure {
    fn from(e: Error) -> Self {
        Self::Engine(e)
    }
}

/// The JSON line of a result: exactly `serde_json::to_string(result)` and a
/// newline. The ADR 010 test holds the CLI to this.
fn json_line<T: Serialize>(result: &T) -> Result<String, Failure> {
    let mut s = serde_json::to_string(result).map_err(Error::from)?;
    s.push('\n');
    Ok(s)
}

pub(crate) fn done<T: Serialize>(result: &T, human: String, code: u8) -> Result<Outcome, Failure> {
    Ok(Outcome::Done(json_line(result)?, human, code))
}

pub(crate) fn engine(global: &Global, root: &str) -> Result<Engine, Failure> {
    let format = match global.format {
        FormatArg::Parquet => SourceFormat::Parquet,
        FormatArg::Csv => SourceFormat::Csv {
            delimiter: b',',
            has_header: true,
        },
        FormatArg::Jsonl => SourceFormat::JsonLines,
    };
    let schema = match &global.schema {
        None => None,
        Some(p) => {
            let text = std::fs::read_to_string(p).map_err(|e| {
                Failure::Usage(format!("cannot read the schema file {} ({e})", p.display()))
            })?;
            Some(bellwether::schema_from_json(&text)?)
        }
    };
    Ok(Engine::open(
        root,
        EngineOptions {
            index: global.index.clone(),
            format,
            chunk_size: global.chunk_size,
            schema,
            fingerprint_policy: global.fingerprint_policy.map(|p| match p {
                PolicyArg::Tail64k => bellwether::FingerprintPolicy::Tail64K,
                PolicyArg::Full => bellwether::FingerprintPolicy::Full,
            }),
            clock: global.clock.clone(),
        },
    )?)
}

pub(crate) fn hash(s: &str, what: &str) -> Result<Hash, Failure> {
    Hash::from_hex(s.trim()).map_err(|_| {
        Failure::Usage(format!(
            "{what} {s:?} is not a 64-character hex hash; pass the hash bellwether printed"
        ))
    })
}

fn key_file(path: &std::path::Path, key: Option<&Vec<String>>) -> KeyFile {
    let f = KeyFile::new(path);
    match key {
        Some(c) => f.with_columns(c.clone()),
        None => f,
    }
}

fn run(cli: &Cli, out: &Out) -> Result<Outcome, Failure> {
    let g = &cli.global;
    match &cli.command {
        Command::Why(a) => why(g, out, a),
        Command::Index(a) => {
            let specs = a
                .keys
                .iter()
                .map(|s| bellwether::spec::parse(s))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| Failure::Usage(e.to_string()))?;
            let r = engine(g, &a.root.root)?.index(&specs)?;
            done(&r, human::index(&r, out), EXIT_OK)
        }
        Command::Locate(a) => {
            let r = engine(g, &a.root.root)?.locate(&key_file(&a.keys, a.key.as_ref()))?;
            done(&r, human::plan(&r, out), EXIT_OK)
        }
        Command::Latest(a) => {
            let r = engine(g, &a.root.root)?.latest(&key_file(&a.keys, a.key.as_ref()))?;
            done(&r, human::plan(&r, out), EXIT_OK)
        }
        Command::Asof(a) => {
            let l = &a.lookup;
            let r = engine(g, &l.root.root)?.asof(&key_file(&l.keys, l.key.as_ref()), &a.at)?;
            done(&r, human::plan(&r, out), EXIT_OK)
        }
        Command::Propose(a) => propose(g, out, a),
        Command::Receipt {
            command: ReceiptCommand::Show { root, hash: h },
        } => {
            let h = hash(h, "receipt")?;
            let r = engine(g, &root.root)?.receipt(&h)?;
            done(&r, human::receipt(&r, out), EXIT_OK)
        }
        Command::Report(a) => {
            let r = engine(g, &a.root.root)?.report(&a.html)?;
            done(&r, human::report(&r, out), EXIT_OK)
        }
        Command::Verify(c) => verify_cmds::run(g, out, c),
        Command::Bench { command } => {
            let w = bench::run(command)?;
            let code = if w.failed.is_empty() {
                EXIT_OK
            } else {
                EXIT_FAILED
            };
            let human = if w.failed.is_empty() {
                format!(
                    "wrote {} to {}
",
                    w.what, w.path
                )
            } else {
                format!(
                    "SLO checks failed on {}:
{}
",
                    w.path,
                    w.failed.join(
                        "
"
                    )
                )
            };
            done(&w, human, code)
        }
        Command::Mcp => Err(Failure::Usage(
            "the MCP server arrives with WP12 (bellwether-mcp); until then use the CLI".into(),
        )),
    }
}

fn propose(g: &Global, out: &Out, a: &ProposeArgs) -> Result<Outcome, Failure> {
    let body = if let Some(k) = &a.key {
        ProposalBody::Key {
            spec: bellwether::spec::parse(k).map_err(|e| Failure::Usage(e.to_string()))?,
        }
    } else if let Some(cols) = &a.layout {
        ProposalBody::Layout {
            cluster_by: cols.clone(),
        }
    } else if let (Some(before), Some(after), Some(invariant)) = (&a.before, &a.after, &a.invariant)
    {
        ProposalBody::Rewrite {
            shape: RewriteShape {
                before: before.clone(),
                after: after.clone(),
                invariant: invariant.clone(),
            },
        }
    } else {
        return Err(Failure::Usage(
            "say what to propose: --key <spec>, --layout <cols>, or --before/--after/--invariant"
                .into(),
        ));
    };
    let evidence = a
        .evidence
        .iter()
        .map(|e| hash(e, "evidence"))
        .collect::<Result<Vec<_>, _>>()?;
    let r = engine(g, &a.root.root)?.propose(body, &evidence, &a.by)?;
    let code = match r {
        bellwether::ProposeResult::Written { .. } => EXIT_OK,
        bellwether::ProposeResult::Refused { .. } => EXIT_REFUSED,
    };
    done(&r, human::propose(&r, out), code)
}

fn why(g: &Global, out: &Out, a: &WhyArgs) -> Result<Outcome, Failure> {
    if a.latest_by.is_some() && a.from.is_none() && a.workload.is_none() {
        return Err(Failure::Usage(
            "--latest-by requires a workload: pass --from <keys.parquet> (or --workload <hash>)"
                .into(),
        ));
    }
    let request = bellwether::WhyRequest {
        keys: a.from.as_ref().map(|p| key_file(p, a.key.as_ref())),
        bench_workload: a.bench_workload.clone(),
        workload: a
            .workload
            .as_deref()
            .map(|w| hash(w, "workload"))
            .transpose()?,
        latest_by: a.latest_by.clone(),
        at: a.at.clone(),
        columns: a.columns.clone(),
        max_candidates: a.max_candidates,
    };
    let r = engine(g, &a.root.root)?.why(&request)?;
    done(&r, human::why(&r, out), EXIT_OK)
}
