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

use bellwether::types::{Error, Hash, HashKind, ProposalBody, RewriteShape};
use bellwether::{Engine, EngineOptions, KeyFile};
use bellwether_mcp::args::OptionsError;
use clap::{Args, Parser, Subcommand};
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
    /// How the engine opens the data folder: the same flags as
    /// `bellwether-mcp`, from one definition.
    #[command(flatten)]
    engine: bellwether_mcp::EngineArgs,
}

#[derive(Subcommand)]
enum Command {
    /// Profile the folder and, with keys, replay them under every candidate
    /// key: the funnel, the culprit column, and the recommended key.
    Why(WhyArgs),
    /// Build or update the index for one or more key specs.
    Index(IndexArgs),
    /// Plan which files and row groups hold the keys (in a time window with
    /// --from and --to).
    Locate(LocateArgs),
    /// Plan the newest version of each key (newest valid version known by
    /// an instant with --known).
    Latest(LatestArgs),
    /// Plan the version of each key current at an instant (--at), or the
    /// bitemporal version valid at --valid as known at --known.
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
    /// Read the ledger: the newest findings per root, every receipt with its
    /// chain status, or the scorecard per rule id (the MCP resources).
    Ledger {
        #[command(subcommand)]
        command: LedgerCommand,
    },
    #[command(flatten)]
    Verify(verify_cmds::VerifyCommand),
    /// The benchmark (WP10): generate corpora and workloads, run the
    /// baselines, report, assert SLOs.
    Bench {
        #[command(subcommand)]
        command: bench::BenchCommand,
    },
    /// Serve the engine over MCP on stdin and stdout, bound to one data
    /// folder (the same server as `bellwether-mcp`).
    Mcp(McpArgs),
}

#[derive(Args)]
pub(crate) struct RootArg {
    /// The folder of data files.
    pub(crate) root: String,
}

#[derive(Args)]
struct McpArgs {
    #[command(flatten)]
    root: RootArg,
    #[command(flatten)]
    budget: bellwether_mcp::BudgetArgs,
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
    /// The lookup the keys are priced under: `locate` (rows holding a key),
    /// `latest` (each key's newest version), or `asof` (the version current
    /// at --at) [default: asof with --at, latest with --latest-by, else
    /// locate].
    #[arg(long, value_enum, value_name = "MODE")]
    mode: Option<ModeArg>,
    /// Timestamp (or date, time, or integer version) column that orders
    /// versions, for `latest` and `asof` pricing.
    #[arg(long, value_name = "COL", conflicts_with = "bench_workload")]
    latest_by: Option<String>,
    /// The instant for `asof` pricing (with `--latest-by`): RFC 3339, a date
    /// (YYYY-MM-DD), or an integer in the timestamp column's unit.
    #[arg(long, value_name = "INSTANT", requires = "latest_by")]
    at: Option<String>,
    /// Price every version in a window instead of one per key: the
    /// versions in [at − N, at] (asof) or [newest − N, newest] (latest). A
    /// whole number with a calendar unit: s, m, h, d, or w (252d, 36h;
    /// trading-day calendars are out of scope); a bare integer for an
    /// integer timestamp column.
    #[arg(long, value_name = "N", requires = "latest_by")]
    lookback: Option<String>,
    /// Candidate key columns for the profile, comma-separated [default:
    /// every id-like column].
    #[arg(long, value_name = "COLS", value_delimiter = ',')]
    columns: Option<Vec<String>>,
    /// Most profile candidates replayed beyond the workload's own key
    /// columns.
    #[arg(long, value_name = "N", default_value_t = bellwether::DEFAULT_MAX_CANDIDATES)]
    max_candidates: usize,
    /// Profile and replay the committed manifest at this sequence instead
    /// of the current one (the files it recorded, its key specs).
    #[arg(long, value_name = "SEQUENCE")]
    at_manifest: Option<u64>,
    /// Price the runs as point lookups in this time window, START..END,
    /// both inclusive (as `locate --window`). A latest or asof lookup takes
    /// --lookback instead.
    #[arg(
        long,
        value_name = "START..END",
        conflicts_with_all = ["latest_by", "workload"]
    )]
    window: Option<String>,
    /// The window's time column [default: the one a window lookup would
    /// take].
    #[arg(long = "ts", value_name = "COLUMN")]
    ts: Option<String>,
}

/// `why --mode`.
#[derive(Clone, Copy, clap::ValueEnum)]
enum ModeArg {
    Locate,
    Latest,
    Asof,
}

impl From<ModeArg> for bellwether::LookupMode {
    fn from(m: ModeArg) -> Self {
        match m {
            ModeArg::Locate => Self::Locate,
            ModeArg::Latest => Self::Latest,
            ModeArg::Asof => Self::Asof,
        }
    }
}

#[derive(Args)]
struct IndexArgs {
    #[command(flatten)]
    root: RootArg,
    /// A key spec: exact:a,b  temporal:id@ts  bloom:a@0.01  range:ts; with
    /// per-key time bounds exact:a@ts or bloom:a@0.01@ts; or a bitemporal
    /// key temporal2:id@valid,known. Repeat for several.
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
    /// Plan against the committed manifest at this sequence instead of the
    /// current one, so a lookup run later over a grown folder reads exactly
    /// what it read then. Files added since are ignored; the retained
    /// sequences are listed when one is not.
    #[arg(long, value_name = "SEQUENCE")]
    at_manifest: Option<u64>,
}

#[derive(Args)]
#[command(group(clap::ArgGroup::new("range").multiple(false).args(["from", "window"])))]
struct LocateArgs {
    #[command(flatten)]
    lookup: LookupArgs,
    /// Window start (inclusive): RFC 3339, a date (YYYY-MM-DD), or an
    /// integer in the time column's unit.
    #[arg(long, value_name = "INSTANT", requires = "to")]
    from: Option<String>,
    /// Window end (inclusive), as --from.
    #[arg(long, value_name = "INSTANT", requires = "from")]
    to: Option<String>,
    /// The window as START..END, both inclusive (the same as --from START
    /// --to END), for example 2026-01-01..2026-03-01.
    #[arg(long, value_name = "START..END")]
    window: Option<String>,
    /// The window's time column [default: the one per-key bounds, a
    /// timestamp range key, a temporal key, or a date partition column
    /// names].
    #[arg(long = "ts", value_name = "COLUMN")]
    ts: Option<String>,
}

#[derive(Args)]
struct LatestArgs {
    #[command(flatten)]
    lookup: LookupArgs,
    /// Bitemporal: the newest valid version among rows known at or before
    /// this instant (needs a temporal2 key).
    #[arg(long, value_name = "INSTANT", conflicts_with = "lookback")]
    known: Option<String>,
    /// Every row of the keys in the N before their newest version (one
    /// window ending at the newest timestamp among the keys, through their
    /// temporal key): a whole number and a calendar unit, s, m, h, d (24
    /// hours of UTC), or w (7 days), for example 252d; no trading days.
    #[arg(long, value_name = "N")]
    lookback: Option<String>,
}

#[derive(Args)]
#[command(group(clap::ArgGroup::new("instant").required(true).multiple(true).args(["at", "valid", "known"])))]
struct AsofArgs {
    #[command(flatten)]
    lookup: LookupArgs,
    /// The instant: RFC 3339, a date (YYYY-MM-DD), or an integer in the
    /// timestamp column's unit.
    #[arg(long, value_name = "INSTANT", conflicts_with_all = ["valid", "known"])]
    at: Option<String>,
    /// Every row of the keys from --at minus N to --at (instead of the one
    /// version current at --at): a whole number and a calendar unit, s, m,
    /// h, d (24 hours of UTC), or w (7 days), for example 252d; no trading
    /// days.
    #[arg(long, value_name = "N", requires = "at")]
    lookback: Option<String>,
    /// The lookback's time column [default: the temporal key's timestamp,
    /// else the one a window would take].
    #[arg(long = "ts", value_name = "COLUMN", requires = "lookback")]
    ts: Option<String>,
    /// Bitemporal: the valid-time instant (needs a temporal2 key).
    #[arg(long, value_name = "INSTANT")]
    valid: Option<String>,
    /// Bitemporal: the known-time instant; rows learned later are ignored.
    #[arg(long, value_name = "INSTANT")]
    known: Option<String>,
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
enum LedgerCommand {
    /// The newest profile Evidence per data root, with its lints and the
    /// receipts citing it.
    Findings(RootArg),
    /// Every receipt, newest first, with its chain status.
    Receipts(RootArg),
    /// Per rule id: proposals, certificates passed and failed, receipts.
    Scorecard(RootArg),
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
            match hint(&e) {
                Some(h) => eprintln!("bellwether: {e}; {h}"),
                None => eprintln!("bellwether: {e}"),
            }
            ExitCode::from(EXIT_ENGINE)
        }
        Err(Failure::Server(msg)) => {
            eprintln!("bellwether: {msg}");
            ExitCode::from(EXIT_ENGINE)
        }
    }
}

pub(crate) enum Failure {
    Usage(String),
    Engine(Error),
    /// The MCP server's transport failed after the engine opened.
    Server(String),
}

impl From<Error> for Failure {
    fn from(e: Error) -> Self {
        Self::Engine(e)
    }
}

/// The next step for a typed engine error, in this CLI's flags and
/// commands. The facade's message says what happened in words that name no
/// surface; the MCP server adds its own next step in its field names.
fn hint(e: &Error) -> Option<String> {
    let hash_arg = |k: &HashKind| match k {
        HashKind::Evidence => {
            "cite an evidence hash `bellwether why` printed for this index \
                               (--evidence)"
        }
        HashKind::Proposal => {
            "pass a proposal hash `bellwether propose` or `bellwether why` printed (--proposal)"
        }
        HashKind::Certificate => {
            "pass the certificate hash `bellwether fix --verify-only` printed (--certificate)"
        }
        HashKind::Receipt => "pass a receipt hash `bellwether fix` printed",
        HashKind::Workload => {
            "pass the workload hash `bellwether why --from <keys.parquet>` printed (--workload)"
        }
    };
    match e {
        Error::RecordNotFound { kind, .. } | Error::WrongKind { expected: kind, .. } => {
            Some(hash_arg(kind).to_string())
        }
        Error::NoWorkload { field, .. } => Some(
            if *field == "rewrite" {
                "pass --before-output <dir> --after-output <dir> --key <cols>"
            } else {
                "pass --workload <hash> (bellwether why prints it)"
            }
            .to_string(),
        ),
        Error::NotIndexed { root, needs, .. } => Some(format!(
            "run `bellwether index {root} --key {}` first",
            needs.as_deref().unwrap_or("<spec>")
        )),
        Error::InvalidArgument { field, .. } => {
            let flag = match *field {
                "by" => "--by",
                "evidence" => "--evidence",
                "at" => "--at",
                "window" => "--window (or --from and --to)",
                "valid" => "--valid",
                "known" => "--known",
                "keys" => "--from or --keys",
                "key_columns" => "--key",
                "cluster_by" => "--layout",
                "shape" => "--before, --after, and --invariant",
                "workload" => "--from, --bench-workload, or --workload",
                "rewrite" => "--before-output and --after-output",
                _ => return None,
            };
            Some(format!("check {flag}"))
        }
        Error::NotRetained { retained, .. } => match (retained.first(), retained.last()) {
            (Some(oldest), Some(current)) => {
                Some(format!("pass one of {oldest}..={current} to --at-manifest"))
            }
            _ => Some("run `bellwether index` first".to_string()),
        },
        _ => None,
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
    engine_at(global, root, None)
}

/// [`engine`], pinned to the committed manifest at `at_manifest` when given.
pub(crate) fn engine_at(
    global: &Global,
    root: &str,
    at_manifest: Option<u64>,
) -> Result<Engine, Failure> {
    let options = EngineOptions {
        at_manifest,
        ..engine_options(global)?
    };
    let engine = Engine::open(root, options)?;
    // A sequence the index never committed (or pruned) is a usage error
    // naming the committed range, before any lookup runs.
    if let Some(seq) = at_manifest {
        let retained = engine.retained_manifests()?;
        if !retained.contains(&seq) {
            let e = Error::NotRetained {
                index: engine.index_location().to_string(),
                requested: seq,
                retained,
            };
            return Err(Failure::Usage(match hint(&e) {
                Some(h) => format!("{e}; {h}"),
                None => e.to_string(),
            }));
        }
    }
    Ok(engine)
}

/// The engine options the global flags name.
fn engine_options(global: &Global) -> Result<EngineOptions, Failure> {
    global.engine.options().map_err(|e| match e {
        OptionsError::Usage(m) => Failure::Usage(m),
        OptionsError::Engine(e) => Failure::Engine(e),
    })
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
            let declared = a
                .keys
                .iter()
                .map(|s| bellwether::spec::parse_declaration(s))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| Failure::Usage(e.to_string()))?;
            let r = engine(g, &a.root.root)?.index_declared(&declared)?;
            done(&r, human::index(&r, out), EXIT_OK)
        }
        Command::Locate(a) => locate(g, out, a),
        Command::Latest(a) => {
            check_lookback(a.lookback.as_deref())?;
            lookup(g, out, &a.lookup, |e, keys| match (&a.known, &a.lookback) {
                (Some(known), _) => e.asof_bitemporal(keys, None, Some(known)),
                (None, Some(n)) => e.latest_lookback(keys, n),
                (None, None) => e.latest(keys),
            })
        }
        Command::Asof(a) => {
            check_lookback(a.lookback.as_deref())?;
            lookup(g, out, &a.lookup, |e, keys| match (&a.at, &a.lookback) {
                (Some(at), Some(n)) => e.asof_lookback(keys, at, n, a.ts.as_deref()),
                (Some(at), None) => e.asof(keys, at),
                (None, _) => e.asof_bitemporal(keys, a.valid.as_deref(), a.known.as_deref()),
            })
        }
        Command::Propose(a) => propose(g, out, a),
        Command::Receipt {
            command: ReceiptCommand::Show { root, hash: h },
        } => {
            let h = hash(h, "receipt")?;
            let r = engine(g, &root.root)?.receipt(&h)?;
            done(&r, human::receipt(&r, out), EXIT_OK)
        }
        Command::Ledger { command } => match command {
            LedgerCommand::Findings(root) => {
                let r = engine(g, &root.root)?.findings()?;
                done(&r, human::findings(&r, out), EXIT_OK)
            }
            LedgerCommand::Receipts(root) => {
                let r = engine(g, &root.root)?.receipts()?;
                done(&r, human::receipts(&r, out), EXIT_OK)
            }
            LedgerCommand::Scorecard(root) => {
                let r = engine(g, &root.root)?.scorecard()?;
                done(&r, human::scorecard(&r, out), EXIT_OK)
            }
        },
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
        Command::Mcp(a) => {
            let config = bellwether_mcp::Config {
                root: a.root.root.clone(),
                engine: engine_options(g)?,
                budget: a.budget.into(),
            };
            // stdout is the protocol channel: nothing else is printed there.
            match bellwether_mcp::serve_stdio(&config) {
                Ok(()) => Ok(Outcome::Done(String::new(), String::new(), EXIT_OK)),
                Err(bellwether_mcp::ServeError::Open(e)) => Err(Failure::Engine(e)),
                Err(bellwether_mcp::ServeError::Transport(m)) => Err(Failure::Server(m)),
            }
        }
    }
}

/// A plan lookup (`locate`, `latest`, `asof`): the engine, pinned to
/// `--at-manifest` when given, and the key file, handed to `plan`.
fn lookup(
    g: &Global,
    out: &Out,
    args: &LookupArgs,
    plan: impl FnOnce(&Engine, &KeyFile) -> Result<bellwether::PlanResult, Error>,
) -> Result<Outcome, Failure> {
    let engine = engine_at(g, &args.root.root, args.at_manifest)?;
    let result = plan(&engine, &key_file(&args.keys, args.key.as_ref()))?;
    done(&result, human::plan(&result, out), EXIT_OK)
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
        bellwether::ProposeResult::Written { .. }
        | bellwether::ProposeResult::WouldWrite { .. } => EXIT_OK,
        bellwether::ProposeResult::Refused { .. } => EXIT_REFUSED,
    };
    done(&r, human::propose(&r, out), code)
}

/// `locate`, in a window when one is given; a reversed window is a usage
/// error naming the flag that gave it.
fn locate(g: &Global, out: &Out, a: &LocateArgs) -> Result<Outcome, Failure> {
    let window = match (&a.window, &a.from, &a.to) {
        (Some(w), _, _) => {
            Some(bellwether::split_window(w).map_err(|e| Failure::Usage(e.to_string()))?)
        }
        (None, Some(from), Some(to)) => Some((from.as_str(), to.as_str())),
        _ => None,
    };
    if let Some((from, to)) = window {
        if bellwether::window_reversed(from, to) {
            let given = if a.window.is_some() {
                format!("--window {from}..{to}")
            } else {
                format!("--from {from} --to {to}")
            };
            return Err(Failure::Usage(format!(
                "{given} starts after it ends; give the start first, START..END"
            )));
        }
    }
    if window.is_none() && a.ts.is_some() {
        return Err(Failure::Usage(
            "--ts names a window's time column; give the window with --window START..END (or \
             --from and --to)"
                .into(),
        ));
    }
    lookup(g, out, &a.lookup, |e, keys| match window {
        Some((from, to)) => e.locate_window(keys, from, to, a.ts.as_deref()),
        None => e.locate(keys),
    })
}

/// A malformed `--lookback` is a usage error (exit 2), refused before any
/// engine work.
fn check_lookback(lookback: Option<&str>) -> Result<(), Failure> {
    match lookback {
        Some(n) => bellwether::Lookback::parse(n)
            .map(|_| ())
            .map_err(|e| Failure::Usage(e.to_string())),
        None => Ok(()),
    }
}

fn why(g: &Global, out: &Out, a: &WhyArgs) -> Result<Outcome, Failure> {
    check_lookback(a.lookback.as_deref())?;
    let window = a
        .window
        .as_deref()
        .map(|w| {
            let (from, to) =
                bellwether::split_window(w).map_err(|e| Failure::Usage(e.to_string()))?;
            if bellwether::window_reversed(from, to) {
                return Err(Failure::Usage(format!(
                    "--window {w} starts after it ends; give the start first, START..END"
                )));
            }
            Ok(bellwether::TimeWindow {
                from: from.to_string(),
                to: to.to_string(),
            })
        })
        .transpose()?;
    if window.is_none() && a.ts.is_some() {
        return Err(Failure::Usage(
            "--ts names a window's time column; give the window with --window START..END".into(),
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
        mode: a.mode.map(Into::into),
        latest_by: a.latest_by.clone(),
        at: a.at.clone(),
        lookback: a.lookback.clone(),
        window,
        ts_column: a.ts.clone(),
        columns: a.columns.clone(),
        max_candidates: a.max_candidates,
    };
    let mode = request.lookup_mode().map_err(Failure::Usage)?;
    if mode != bellwether::LookupMode::Locate && a.from.is_none() && a.workload.is_none() {
        return Err(Failure::Usage(format!(
            "--mode {} prices a workload's keys; pass --from <keys.parquet> (or --workload <hash>)",
            mode.as_str()
        )));
    }
    if request.window.is_some() && mode != bellwether::LookupMode::Locate {
        return Err(Failure::Usage(
            "--window prices point lookups in a time window; a latest or asof lookup takes \
             --lookback instead (--latest-by <column> --lookback <length>, and --at <instant> \
             for asof)"
                .into(),
        ));
    }
    let r = engine_at(g, &a.root.root, a.at_manifest)?.why(&request)?;
    done(&r, human::why(&r, out), EXIT_OK)
}
