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
mod join_cmds;
mod keys_cmds;
mod sketch_cmds;
mod verify_cmds;

use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use bellwether::types::{Error, Hash, HashKind, ProposalBody, RewriteShape, TextOp};
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

// Parsed once per process: the size of `why`'s arguments costs nothing.
#[allow(clippy::large_enum_variant)]
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
    /// Count the rows holding the keys (count(*) where key in ...), exact,
    /// from the index's per-key counts (a key spec exact:COLS,counts); no
    /// data file is read.
    Count(CountArgs),
    /// Aggregate a column per key over the keys (sum, count, min, max ...
    /// group by key where key in ...), from the index's agg keys (a key spec
    /// agg:COLS@VALUE{sum,count,min,max} beside exact:COLS,counts); no data
    /// file is read, and the row groups still to read are named when the
    /// index answers only part.
    Aggregate(AggregateArgs),
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
    /// Join keys (B3) between this folder and a dataset beside it: plan a
    /// semi-join, read the key overlap, or check that each join key is
    /// fresh.
    Join {
        #[command(subcommand)]
        command: join_cmds::JoinCommand,
    },
    /// Approximate distinct counts and overlaps from `sketch` sidecars
    /// (never used to plan): cardinality per column, overlap of two.
    Sketch {
        #[command(subcommand)]
        command: sketch_cmds::SketchCommand,
    },
    /// Signing keys: generate one, show a public key's id, or add a public
    /// key to a trust file (ADR 029).
    Keys {
        #[command(subcommand)]
        command: keys_cmds::KeysCommand,
    },
    /// Serve the engine over MCP on stdin and stdout, bound to one data
    /// folder (the same server as `bellwether-mcp`).
    Mcp(McpArgs),
    /// Record workloads: the keys a diagnosis replays.
    Workload {
        #[command(subcommand)]
        command: WorkloadCommand,
    },
}

#[derive(Subcommand)]
enum WorkloadCommand {
    /// Run a query that produces keys in a query engine (read-only) and
    /// record its rows as a workload over the folder that is the engine's
    /// twin; prints the workload hash `why --workload` takes.
    Record(RecordArgs),
}

#[derive(Args)]
struct RecordArgs {
    #[command(flatten)]
    root: RootArg,
    /// The engine: clickhouse://[user@]host[:port]/<db>[/<table>] or
    /// kusto://<cluster>/<db>[/<table>] (a secret only as password=env:NAME).
    #[arg(long, value_name = "URI")]
    from_engine: String,
    /// A file holding one query, in the engine's language, whose result
    /// rows are the keys.
    #[arg(long, value_name = "FILE")]
    keys_query: PathBuf,
    /// Result columns to take as the key, comma-separated [default: all].
    #[arg(long, value_name = "COLS", value_delimiter = ',')]
    key: Option<Vec<String>>,
}

#[derive(Args)]
pub(crate) struct RootArg {
    /// The folder of data files, or a table:
    /// sql://<engine>/<schema>.<table>?db=<connection> (needs --index;
    /// lookups and index builds take a folder only).
    pub(crate) root: String,
}

#[derive(Args)]
struct McpArgs {
    #[command(flatten)]
    root: RootArg,
    #[command(flatten)]
    budget: bellwether_mcp::BudgetArgs,
    #[command(flatten)]
    keys: bellwether_mcp::KeyArgs,
    #[command(flatten)]
    join: bellwether_mcp::JoinArgs,
}

#[derive(Args)]
struct WhyArgs {
    #[command(flatten)]
    root: RootArg,
    /// A Parquet file of keys one run looked up; repeat for several runs
    /// (recorded as runs keys, keys-2, keys-3, ...), each priced under the
    /// same lookup.
    #[arg(long, value_name = "FILE")]
    from: Vec<PathBuf>,
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
    /// `latest` (each key's newest version), `asof` (the version current
    /// at --at), a text lookup over a key file of one string column's
    /// values: `contains` (substrings), `has-token` (words), `has-prefix`
    /// (word prefixes), case-folded, a list-membership lookup over a key
    /// file of one list column's values (named after the column):
    /// `contains-any` (rows whose list holds any of them) or `contains-all`
    /// (every one), priced against list:<column>, `bitemporal` (with
    /// --valid-by and --known-by), `semijoin` (the rows of this folder and
    /// of the --join dataset whose key the other holds), priced against the
    /// join key on the pair, or `aggregate` (per-key aggregates of --value)
    /// [default: aggregate with --value, asof with --at, latest with
    /// --latest-by, bitemporal with --valid-by, else locate].
    #[arg(long, value_enum, value_name = "MODE")]
    mode: Option<ModeArg>,
    /// With --mode semijoin: the right dataset and its join columns,
    /// RIGHT.COL[,COL...], pairwise with the key file's columns; RIGHT is
    /// the folder of that name beside the data root, read through its own
    /// index.
    #[arg(long, value_name = "RIGHT.COLS")]
    join: Option<String>,
    /// Timestamp (or date, time, or integer version) column that orders
    /// versions, for `latest` and `asof` pricing.
    #[arg(long, value_name = "COL", conflicts_with = "bench_workload")]
    latest_by: Option<String>,
    /// Price latest or asof runs (or bitemporal ones) that keep only live
    /// rows (B8): each key's newest version, then `where not COL` (a
    /// boolean column) or `where COL <> VALUE`; the floor candidate is the
    /// temporal key with this tombstone, and its column the fraction of
    /// keys whose newest row is a tombstone.
    #[arg(long, value_name = "COL[=VALUE]", conflicts_with = "lookback")]
    tombstone: Option<String>,
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
    /// Price a bitemporal lookup (`asof --valid --known`): the column a
    /// version is true from. With --known-by; the candidate is
    /// temporal2:<key>@<valid-by>,<known-by>, priced over an index.
    #[arg(
        long,
        value_name = "COL",
        requires = "known_by",
        conflicts_with_all = ["latest_by", "window", "bench_workload"]
    )]
    valid_by: Option<String>,
    /// The column a version became known at (with --valid-by).
    #[arg(long, value_name = "COL", requires = "valid_by")]
    known_by: Option<String>,
    /// The valid-time bound of the bitemporal lookup (with --valid-by): RFC
    /// 3339, a date, or an integer in the column's unit [default: none].
    #[arg(long, value_name = "INSTANT", requires = "valid_by")]
    valid: Option<String>,
    /// The known-time bound of the bitemporal lookup (with --known-by);
    /// rows learned later are ignored [default: none].
    #[arg(long, value_name = "INSTANT", requires = "known_by")]
    known: Option<String>,
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
    /// take]. With --at and --lookback instead of --window, the column
    /// `asof --at --lookback --ts` plans on: the run is priced as that
    /// window (pass the same column to --latest-by).
    #[arg(long = "ts", value_name = "COLUMN")]
    ts: Option<String>,
    /// Record a text lookup as a run: rows whose string column contains
    /// TEXT, case-folded, for example body=quarterly (repeat for several
    /// runs; instead of --from, --bench-workload, or --workload).
    #[arg(long, value_name = "COL=TEXT", conflicts_with_all = ["from", "bench_workload", "workload", "mode"])]
    contains: Vec<String>,
    /// Record a text lookup as a run: rows whose string column has the
    /// word WORD, case-folded (repeat for several runs).
    #[arg(long, value_name = "COL=WORD", conflicts_with_all = ["from", "bench_workload", "workload", "mode"])]
    has: Vec<String>,
    /// Record a text lookup as a run: rows whose string column has a word
    /// starting with PREFIX, case-folded (repeat for several runs).
    #[arg(long, value_name = "COL=PREFIX", conflicts_with_all = ["from", "bench_workload", "workload", "mode"])]
    prefix: Vec<String>,
    /// With --twin-of: a limit to compare each candidate's keyset with,
    /// NAME:N (repeat for several), for example in:1000000 (a membership
    /// list the engine accepts) or join:200000 (a join's broadcast size).
    #[arg(long = "gate", value_name = "NAME:N", value_parser = parse_gate)]
    gates: Vec<bellwether::types::Gate>,
    /// Diagnose from the index's manifest and complete sketches alone
    /// (`index --key sketch:...`): no data file is opened, footers
    /// included; the sketch column and the low-cardinality-key lint carry
    /// the diagnosis, approximate.
    #[arg(long, conflicts_with_all = ["from", "bench_workload", "workload", "columns"])]
    sketches_only: bool,
    /// Price the keys as one aggregate lookup of this column per key (B7,
    /// `--mode aggregate`, as `bellwether aggregate`): the candidate is the
    /// agg key on the key columns beside exact:<cols>,counts.
    #[arg(long, value_name = "COLUMN", conflicts_with_all = ["bench_workload", "latest_by", "window"])]
    value: Option<String>,
    /// With --value: the functions, comma-separated: sum, count, min, max
    /// [default: sum,count].
    #[arg(
        long = "fn",
        value_name = "FNS",
        value_delimiter = ',',
        requires = "value"
    )]
    functions: Vec<String>,
}

/// `--gate NAME:N`.
fn parse_gate(given: &str) -> Result<bellwether::types::Gate, String> {
    let (name, n) = given
        .rsplit_once(':')
        .ok_or_else(|| format!("{given:?} is not a gate; write NAME:N, for example in:1000000"))?;
    let name = name.trim();
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(format!(
            "{given:?} names no gate; write NAME:N with a name of letters, digits, '_', or '-'"
        ));
    }
    let limit = n.trim().parse::<u64>().map_err(|_| {
        format!("{given:?}: {n:?} is not a whole number; write NAME:N, for example in:1000000")
    })?;
    Ok(bellwether::types::Gate {
        name: name.to_string(),
        limit,
    })
}

/// `why --mode`.
#[derive(Clone, Copy, clap::ValueEnum)]
enum ModeArg {
    Locate,
    Latest,
    Asof,
    Contains,
    HasToken,
    HasPrefix,
    ContainsAny,
    ContainsAll,
    Bitemporal,
    Semijoin,
    Aggregate,
}

impl From<ModeArg> for bellwether::LookupMode {
    fn from(m: ModeArg) -> Self {
        match m {
            ModeArg::Locate => Self::Locate,
            ModeArg::Latest => Self::Latest,
            ModeArg::Asof => Self::Asof,
            ModeArg::Contains => Self::Contains,
            ModeArg::HasToken => Self::HasToken,
            ModeArg::HasPrefix => Self::HasPrefix,
            ModeArg::ContainsAny => Self::ContainsAny,
            ModeArg::ContainsAll => Self::ContainsAll,
            ModeArg::Bitemporal => Self::Bitemporal,
            ModeArg::Semijoin => Self::SemiJoin,
            ModeArg::Aggregate => Self::Aggregate,
        }
    }
}

#[derive(Args)]
struct IndexArgs {
    #[command(flatten)]
    root: RootArg,
    /// A key spec: exact:a,b  temporal:id@ts  bloom:a@0.01  range:ts; with
    /// per-key row counts exact:a,b,counts or temporal:id@ts,counts; with
    /// per-key time bounds exact:a@ts or bloom:a@0.01@ts; or a bitemporal
    /// key temporal2:id@valid,known. Repeat for several.
    #[arg(long = "key", value_name = "SPEC")]
    keys: Vec<String>,
    /// Worker threads for the build [default: the available parallelism].
    /// The segments written do not depend on it.
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(u32).range(1..))]
    threads: Option<u32>,
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
#[command(group(clap::ArgGroup::new("text").multiple(false).args(["contains", "has", "prefix"])))]
#[command(group(clap::ArgGroup::new("what").required(true).multiple(false).args(["keys", "contains", "has", "prefix"])))]
struct LocateArgs {
    #[command(flatten)]
    root: RootArg,
    /// A Parquet file of keys.
    #[arg(long, value_name = "FILE")]
    keys: Option<PathBuf>,
    /// Key columns to take from the file, comma-separated [default: all].
    #[arg(long, value_name = "COLS", value_delimiter = ',', requires = "keys")]
    key: Option<Vec<String>>,
    /// Plan against the committed manifest at this sequence instead of the
    /// current one, so a lookup run later over a grown folder reads exactly
    /// what it read then. Files added since are ignored; the retained
    /// sequences are listed when one is not.
    #[arg(long, value_name = "SEQUENCE")]
    at_manifest: Option<u64>,
    /// Rows whose string column contains TEXT, case-folded (a text key,
    /// text:COL,tokenizer=trigram), for example body=quarterly.
    #[arg(long, value_name = "COL=TEXT", conflicts_with_all = ["range", "to", "ts", "columns"])]
    contains: Option<String>,
    /// Rows whose string column has the word WORD, case-folded (a text key,
    /// text:COL,tokenizer=words or trigram), for example body=guidance.
    #[arg(long, value_name = "COL=WORD", conflicts_with_all = ["range", "to", "ts", "columns"])]
    has: Option<String>,
    /// Rows whose string column has a word starting with PREFIX,
    /// case-folded (a text key), for example body=restat.
    #[arg(long, value_name = "COL=PREFIX", conflicts_with_all = ["range", "to", "ts", "columns"])]
    prefix: Option<String>,
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
    /// The columns the reader reads, comma-separated: each planned file also
    /// reports those columns' compressed bytes, and the plan their sum
    /// (from the index, no footer read; a storage version 1 index's are
    /// read from the planned files' footers, and the output says so).
    #[arg(long, value_name = "COLS", value_delimiter = ',')]
    columns: Option<Vec<String>>,
    /// List membership: the rows whose list (or map) column holds ANY of
    /// the values in --keys (`array_contains`, `has_any`), through the list
    /// key on that column (list:<column>). The key file has one column,
    /// named after the list column, of its element type.
    #[arg(long, conflicts_with_all = ["all", "from", "to", "window", "ts", "columns", "contains", "has", "prefix"])]
    any: bool,
    /// List membership: the rows whose list column holds EVERY one of the
    /// values in --keys, as --any.
    #[arg(long, conflicts_with_all = ["from", "to", "window", "ts", "columns", "contains", "has", "prefix"])]
    all: bool,
}

#[derive(Args)]
struct CountArgs {
    #[command(flatten)]
    lookup: LookupArgs,
}

#[derive(Args)]
struct AggregateArgs {
    #[command(flatten)]
    lookup: LookupArgs,
    /// The column to aggregate.
    #[arg(long, value_name = "COLUMN")]
    value: String,
    /// The functions, comma-separated: sum, count, min, max [default: sum,count].
    #[arg(
        long = "fn",
        value_name = "FNS",
        value_delimiter = ',',
        default_value = "sum,count"
    )]
    functions: Vec<String>,
}

#[derive(Args)]
struct LatestArgs {
    #[command(flatten)]
    lookup: LookupArgs,
    /// Bitemporal: the newest valid version among rows known at or before
    /// this instant (needs a temporal2 key).
    #[arg(long, value_name = "INSTANT", conflicts_with = "lookback")]
    known: Option<String>,
    /// Bitemporal: the valid-time column of the temporal2 key to plan
    /// through, when several serve the key columns (with --known-by).
    #[arg(long, value_name = "COL", requires_all = ["known_by", "known"])]
    valid_by: Option<String>,
    /// Bitemporal: the known-time column of that key (with --valid-by).
    #[arg(long, value_name = "COL", requires = "valid_by")]
    known_by: Option<String>,
    /// Every row of the keys in the N before their newest version (one
    /// window ending at the newest timestamp among the keys, through their
    /// temporal key): a whole number and a calendar unit, s, m, h, d (24
    /// hours of UTC), or w (7 days), for example 252d; no trading days.
    #[arg(long, value_name = "N")]
    lookback: Option<String>,
    /// Only live rows (B8): each key's newest version without its tombstone
    /// rows, by the tombstone its temporal or temporal2 key declares
    /// (temporal:id@ts,tombstone=void); a key whose newest row is a
    /// tombstone answers no row.
    #[arg(long, conflicts_with = "lookback")]
    live: bool,
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
    /// Bitemporal: the valid-time column of the temporal2 key to plan
    /// through, when several serve the key columns (with --known-by).
    #[arg(long, value_name = "COL", requires = "known_by", conflicts_with = "at")]
    valid_by: Option<String>,
    /// Bitemporal: the known-time column of that key (with --valid-by).
    #[arg(long, value_name = "COL", requires = "valid_by")]
    known_by: Option<String>,
    /// Only live rows (B8): each key's version current at the instant
    /// without its tombstone rows, by the tombstone its temporal or
    /// temporal2 key declares (temporal:id@ts,tombstone=void); a key whose
    /// newest row then is a tombstone answers no row.
    #[arg(long, conflicts_with_all = ["lookback", "ts"])]
    live: bool,
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
    /// Propose an index key (a key spec, as for `index`): exact:a@ts (per-key
    /// time bounds), temporal2:id@valid,known (bitemporal versions) and
    /// every other spec. Repeat to apply more specs in the same commit: the
    /// first is the proposal's spec, the rest its sidecars. A sketch is
    /// refused: it is approximate and never planned with.
    #[arg(long, value_name = "SPEC", group = "body")]
    key: Vec<String>,
    /// Propose clustering files by these columns, comma-separated.
    #[arg(long, value_name = "COLS", value_delimiter = ',', group = "body")]
    layout: Option<Vec<String>>,
    /// Propose a rewrite: the current shape (with --after and --invariant).
    #[arg(long, value_name = "TEXT", group = "body", requires_all = ["after", "invariant"])]
    before: Option<String>,
    /// The rewrite's target shape.
    #[arg(long, value_name = "TEXT", requires = "before")]
    after: Option<String>,
    /// What the rewrite must preserve (with --rewrite: default, each pair
    /// returns the same rows by the key columns).
    #[arg(long, value_name = "TEXT")]
    invariant: Option<String>,
    /// Propose a rewrite of queries in the engine the folder is a twin of:
    /// BEFORE=AFTER, two files of one query each (repeat for several pairs,
    /// scored and verified as one set). Cite the evidence `why --twin-of`
    /// printed.
    #[arg(long = "rewrite", value_name = "BEFORE=AFTER", group = "body")]
    rewrite: Vec<String>,
    /// With --rewrite: the columns each pair's before and after results are
    /// compared by [default: the key sample's columns].
    #[arg(long, value_name = "COLS", value_delimiter = ',', requires = "rewrite")]
    key_columns: Option<Vec<String>>,
}

#[derive(Subcommand)]
enum LedgerCommand {
    /// The newest profile Evidence per data root, with its lints and the
    /// receipts citing it.
    Findings(RootArg),
    /// Every receipt, newest first, with its chain status (and, with
    /// --trusted-keys, whether it is signed).
    Receipts {
        #[command(flatten)]
        root: RootArg,
        #[command(flatten)]
        trust: keys_cmds::TrustArg,
    },
    /// Per rule id: proposals, certificates passed and failed, receipts.
    Scorecard(RootArg),
}

#[derive(Subcommand)]
enum ReceiptCommand {
    /// Walk a receipt back through its certificate, replay, proposal, and
    /// evidence, with the signatures beside the receipt and the certificate.
    Show {
        #[command(flatten)]
        root: RootArg,
        /// The receipt hash.
        hash: String,
        #[command(flatten)]
        trust: keys_cmds::TrustArg,
        /// Exit 1 unless the receipt and its certificate are each signed by
        /// a key in --trusted-keys, within the key's window.
        #[arg(long)]
        require_signature: bool,
    },
}

#[derive(Args)]
struct ReportArgs {
    #[command(flatten)]
    root: RootArg,
    /// Output file, or an existing directory (gets report.html).
    #[arg(long, value_name = "PATH")]
    html: PathBuf,
    #[command(flatten)]
    trust: keys_cmds::TrustArg,
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
        match e {
            // An argument only the data can judge (a lookback against its
            // time column's unit, `--latest-by` against the column the
            // index plans on) is still the caller's to fix: a usage error,
            // whose message names the value to pass.
            Error::InvalidArgument {
                field:
                    "lookback" | "latest_by" | "signing_key" | "trusted_keys" | "public_key" | "out"
                    | "name" | "valid_from" | "revoked_at" | "index" | "root" | "scratch" | "items"
                    | "columns" | "text" | "join" | "functions",
                ..
            } => Self::Usage(match hint(&e) {
                Some(h) => format!("{e}; {h}"),
                None => e.to_string(),
            }),
            // A table given where a folder is needed: the caller's to fix.
            Error::NotAFolder { .. } => Self::Usage(e.to_string()),
            e => Self::Engine(e),
        }
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
        Error::NotIndexed {
            root,
            needs: Some(sketch),
            operation,
        } if sketch.starts_with("sketch:") => Some(format!(
            "run `bellwether index {root} --key {sketch}` first{}",
            if *operation == "cardinality" {
                ", or pass --transient to read the files now"
            } else {
                ""
            }
        )),
        Error::NotIndexed {
            root,
            needs: Some(agg),
            ..
        } if agg.starts_with("agg:") => Some(format!(
            "run `bellwether index {root} --key 'exact:{},counts' --key '{agg}'` first (an agg \
             key is declared beside the counted key on its columns)",
            agg.trim_start_matches("agg:")
                .split('@')
                .next()
                .unwrap_or_default()
        )),
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
                "keys" => {
                    "--from or --keys (for a rewrite of engine queries: the key sample the cited \
                     twin diagnosis recorded)"
                }
                "scratch" => "--scratch",
                "key_columns" => "--key",
                "cluster_by" => "--layout",
                "shape" => "--before, --after, and --invariant",
                "workload" => "--from, --bench-workload, or --workload",
                "rewrite" => "--before-output and --after-output",
                "index" => "--index <dir>",
                "items" => "--column",
                "columns" => "--columns",
                "text" => "--contains, --has, or --prefix",
                "functions" => "--fn",
                "join" => {
                    "the right side the join key names (--join <right>.<column>[,<column>...], or \
                     the right side of a join: key spec): a folder beside the data root, one \
                     column per key column; why takes --join with --mode semijoin"
                }
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

/// [`engine`] with a signing key (`fix`) or trusted keys (`receipt show`,
/// `ledger receipts`, `report`), loaded by the caller before the engine
/// opens.
pub(crate) fn engine_keyed(
    global: &Global,
    root: &str,
    signing_key: Option<bellwether::SigningKey>,
    trusted_keys: Option<bellwether::TrustedKeys>,
) -> Result<Engine, Failure> {
    let options = EngineOptions {
        signing_key,
        trusted_keys,
        ..engine_options(global)?
    };
    Ok(Engine::open(root, options)?)
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

pub(crate) fn key_file(path: &std::path::Path, key: Option<&Vec<String>>) -> KeyFile {
    let f = KeyFile::new(path);
    match key {
        Some(c) => f.with_columns(c.clone()),
        None => f,
    }
}

/// `index`: the declared specs, built with `--threads` workers when given.
/// The build committed and a join key could not be rebuilt (B3): the
/// commit is reported, and the run exits 1 (a check failed).
fn index(g: &Global, out: &Out, a: &IndexArgs) -> Result<Outcome, Failure> {
    let declared = a
        .keys
        .iter()
        .map(|s| bellwether::spec::parse_declaration(s))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| Failure::Usage(e.to_string()))?;
    let options = EngineOptions {
        threads: a.threads.map(|n| n as usize),
        ..engine_options(g)?
    };
    let r = Engine::open(&a.root.root, options)?.index_declared(&declared)?;
    let code = if r
        .joins
        .iter()
        .any(|j| j.state == bellwether::JoinFreshness::NotRebuilt)
    {
        EXIT_FAILED
    } else {
        EXIT_OK
    };
    done(&r, human::index(&r, out), code)
}

#[allow(clippy::too_many_lines)]
fn run(cli: &Cli, out: &Out) -> Result<Outcome, Failure> {
    let g = &cli.global;
    match &cli.command {
        Command::Why(a) => why(g, out, a),
        Command::Index(a) => index(g, out, a),
        Command::Locate(a) => locate(g, out, a),
        Command::Count(a) => {
            let engine = engine_at(g, &a.lookup.root.root, a.lookup.at_manifest)?;
            let r = engine.count(&key_file(&a.lookup.keys, a.lookup.key.as_ref()))?;
            done(&r, human::count_result(&r, out), EXIT_OK)
        }
        Command::Aggregate(a) => {
            let functions = aggregate_functions(&a.functions)?;
            let engine = engine_at(g, &a.lookup.root.root, a.lookup.at_manifest)?;
            let r = engine.aggregate(
                &key_file(&a.lookup.keys, a.lookup.key.as_ref()),
                &a.value,
                &functions,
            )?;
            done(&r, human::aggregate_result(&r, engine.root(), out), EXIT_OK)
        }
        Command::Latest(a) => {
            check_lookback(a.lookback.as_deref())?;
            lookup(g, out, &a.lookup, |e, keys| match (&a.known, &a.lookback) {
                (Some(known), _) => e.asof_bitemporal(
                    keys,
                    None,
                    Some(known),
                    a.valid_by.as_deref().zip(a.known_by.as_deref()),
                    a.live,
                ),
                (None, Some(n)) => e.latest_lookback(keys, n),
                (None, None) if a.live => e.latest_live(keys),
                (None, None) => e.latest(keys),
            })
        }
        Command::Asof(a) => {
            check_lookback(a.lookback.as_deref())?;
            lookup(g, out, &a.lookup, |e, keys| match (&a.at, &a.lookback) {
                (Some(at), Some(n)) => e.asof_lookback(keys, at, n, a.ts.as_deref()),
                (Some(at), None) if a.live => e.asof_live(keys, at),
                (Some(at), None) => e.asof(keys, at),
                (None, _) => e.asof_bitemporal(
                    keys,
                    a.valid.as_deref(),
                    a.known.as_deref(),
                    a.valid_by.as_deref().zip(a.known_by.as_deref()),
                    a.live,
                ),
            })
        }
        Command::Propose(a) => propose(g, out, a),
        Command::Receipt {
            command:
                ReceiptCommand::Show {
                    root,
                    hash: h,
                    trust,
                    require_signature,
                },
        } => keys_cmds::receipt_show(g, out, &root.root, h, trust, *require_signature),
        Command::Ledger { command } => match command {
            LedgerCommand::Findings(root) => {
                let r = engine(g, &root.root)?.findings()?;
                done(&r, human::findings(&r, out), EXIT_OK)
            }
            LedgerCommand::Receipts { root, trust } => {
                let r =
                    engine_keyed(g, &root.root, None, trust.load(g, &root.root)?)?.receipts()?;
                done(&r, human::receipts(&r, out), EXIT_OK)
            }
            LedgerCommand::Scorecard(root) => {
                let r = engine(g, &root.root)?.scorecard()?;
                done(&r, human::scorecard(&r, out), EXIT_OK)
            }
        },
        Command::Report(a) => {
            let r = engine_keyed(g, &a.root.root, None, a.trust.load(g, &a.root.root)?)?
                .report(&a.html)?;
            done(&r, human::report(&r, out), EXIT_OK)
        }
        Command::Verify(c) => verify_cmds::run(g, out, c),
        Command::Bench {
            command: bench::BenchCommand::PerfCheck(a),
        } => bench::perf_check(a),
        Command::Bench { command } => {
            let w = bench::run(command)?;
            let code = if w.failed.is_empty() {
                EXIT_OK
            } else {
                EXIT_FAILED
            };
            let human = if w.read.is_some() {
                format!(
                    "read {}
",
                    w.path
                )
            } else if w.failed.is_empty() {
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
        Command::Join { command } => join_cmds::run(g, out, command),
        Command::Sketch { command } => sketch_cmds::run(g, out, command),
        Command::Keys { command } => keys_cmds::run(g, command),
        Command::Mcp(a) => mcp(g, a),
        Command::Workload {
            command: WorkloadCommand::Record(a),
        } => {
            let query = std::fs::read_to_string(&a.keys_query).map_err(|e| {
                Failure::Usage(format!(
                    "cannot read the keys query {} ({e}); pass a readable file holding one query",
                    a.keys_query.display()
                ))
            })?;
            let options = EngineOptions {
                twin_of: Some(a.from_engine.clone()),
                ..engine_options(g)?
            };
            let r = Engine::open(&a.root.root, options)?
                .record_from_engine(&query, a.key.as_deref())?;
            done(&r, human::engine_workload(&r, out), EXIT_OK)
        }
    }
}

/// `bellwether mcp`: the same server as `bellwether-mcp`, with the same
/// flags, signing and trust included.
fn mcp(g: &Global, a: &McpArgs) -> Result<Outcome, Failure> {
    let mut engine = engine_options(g)?;
    a.keys
        .apply(&a.root.root, &mut engine)
        .map_err(|e| match e {
            OptionsError::Usage(m) => Failure::Usage(m),
            OptionsError::Engine(e) => Failure::Engine(e),
        })?;
    let config = bellwether_mcp::Config {
        root: a.root.root.clone(),
        engine,
        budget: a.budget.into(),
        join: a.join.join.clone(),
    };
    // stdout is the protocol channel: nothing else is printed there.
    match bellwether_mcp::serve_stdio(&config) {
        Ok(()) => Ok(Outcome::Done(String::new(), String::new(), EXIT_OK)),
        Err(bellwether_mcp::ServeError::Open(e)) => Err(Failure::Engine(e)),
        Err(bellwether_mcp::ServeError::Transport(m)) => Err(Failure::Server(m)),
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
    enum What {
        Key(Vec<bellwether::spec::Declaration>),
        Body(ProposalBody),
    }
    // A key is read as `index --key` reads it, so a sketch reaches the
    // facade's typed refusal rather than an unknown-kind usage error.
    let what = if !a.key.is_empty() {
        What::Key(
            a.key
                .iter()
                .map(|k| bellwether::spec::parse_declaration(k))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| Failure::Usage(e.to_string()))?,
        )
    } else if let Some(cols) = &a.layout {
        What::Body(ProposalBody::Layout {
            cluster_by: cols.clone(),
        })
    } else if let (Some(before), Some(after), Some(invariant)) = (&a.before, &a.after, &a.invariant)
    {
        What::Body(ProposalBody::Rewrite {
            shape: RewriteShape {
                before: before.clone(),
                after: after.clone(),
                invariant: invariant.clone(),
                pairs: Vec::new(),
                key: Vec::new(),
                keys: None,
                before_plan: None,
                after_plan: None,
            },
        })
    } else if !a.rewrite.is_empty() {
        let pairs = a
            .rewrite
            .iter()
            .map(|p| query_pair(p))
            .collect::<Result<Vec<_>, _>>()?;
        What::Body(ProposalBody::Rewrite {
            shape: bellwether::engine_rewrite(
                pairs,
                a.key_columns.clone().unwrap_or_default(),
                a.invariant.clone(),
            ),
        })
    } else {
        return Err(Failure::Usage(
            "say what to propose: --key <spec>, --layout <cols>, --before/--after/--invariant, or \
             --rewrite <before.sql>=<after.sql>"
                .into(),
        ));
    };
    let evidence = a
        .evidence
        .iter()
        .map(|e| hash(e, "evidence"))
        .collect::<Result<Vec<_>, _>>()?;
    let e = engine(g, &a.root.root)?;
    let r = match what {
        What::Key(declared) => e.propose_key(&declared, &evidence, &a.by)?,
        What::Body(body) => e.propose(body, &evidence, &a.by)?,
    };
    let code = match r {
        bellwether::ProposeResult::Written { .. }
        | bellwether::ProposeResult::WouldWrite { .. } => EXIT_OK,
        bellwether::ProposeResult::Refused { .. } => EXIT_REFUSED,
    };
    done(&r, human::propose(&r, out), code)
}

/// `--rewrite BEFORE=AFTER`: two files, each read whole as one query.
fn query_pair(given: &str) -> Result<bellwether::types::QueryPair, Failure> {
    let (b, a) = given.split_once('=').ok_or_else(|| {
        Failure::Usage(format!(
            "--rewrite {given:?} is not BEFORE=AFTER; give two files, the query as it runs and \
             its rewrite, for example --rewrite before.sql=after.sql"
        ))
    })?;
    let read = |p: &str| {
        std::fs::read_to_string(p.trim()).map_err(|e| {
            Failure::Usage(format!(
                "--rewrite: cannot read the query file {p:?} ({e}); pass a readable file holding \
                 one query"
            ))
        })
    };
    Ok(bellwether::types::QueryPair {
        before: read(b)?,
        after: read(a)?,
    })
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
    let engine = engine_at(g, &a.root.root, a.at_manifest)?;
    if let Some(query) = text_query(a.contains.as_deref(), a.has.as_deref(), a.prefix.as_deref())? {
        let result = engine.locate_text(&query)?;
        return done(&result, human::plan(&result, out), EXIT_OK);
    }
    let Some(keys) = &a.keys else {
        return Err(Failure::Usage(
            "say what to locate: --keys <file> or --contains, --has, or --prefix COL=TEXT".into(),
        ));
    };
    let keys = key_file(keys, a.key.as_ref());
    let result = match (window, &a.columns) {
        (w, Some(columns)) => {
            engine.locate_columns(&keys, w.map(|(f, t)| (f, t, a.ts.as_deref())), columns)?
        }
        (Some((from, to)), None) => engine.locate_window(&keys, from, to, a.ts.as_deref())?,
        (None, None) if a.any || a.all => engine.locate_contains(&keys, a.all)?,
        (None, None) => engine.locate(&keys)?,
    };
    done(&result, human::plan(&result, out), EXIT_OK)
}

/// The text lookup `--contains`, `--has`, or `--prefix` gives (at most one;
/// clap refuses several), each `COL=TEXT`, split at the first `=`.
fn text_query(
    contains: Option<&str>,
    has: Option<&str>,
    prefix: Option<&str>,
) -> Result<Option<bellwether::TextQuery>, Failure> {
    let (flag, op, given) = match (contains, has, prefix) {
        (Some(c), _, _) => ("--contains", TextOp::Contains, c),
        (None, Some(h), _) => ("--has", TextOp::HasToken, h),
        (None, None, Some(p)) => ("--prefix", TextOp::HasPrefix, p),
        (None, None, None) => return Ok(None),
    };
    text_arg(flag, op, given).map(Some)
}

/// One `COL=TEXT` argument of `flag` as a text lookup.
fn text_arg(flag: &str, op: TextOp, given: &str) -> Result<bellwether::TextQuery, Failure> {
    match given.split_once('=') {
        Some((column, value)) if !column.trim().is_empty() => Ok(bellwether::TextQuery {
            column: column.trim().to_string(),
            op,
            value: value.to_string(),
        }),
        _ => Err(Failure::Usage(format!(
            "{flag} takes COL=TEXT, the string column and the text, for example {flag} \
             body=quarterly; {given:?} names no column"
        ))),
    }
}

/// `why`'s text lookups, in flag order: every --contains, then --has,
/// then --prefix.
fn why_text(a: &WhyArgs) -> Result<Vec<bellwether::TextQuery>, Failure> {
    let mut out = Vec::new();
    for (flag, op, given) in [
        ("--contains", TextOp::Contains, &a.contains),
        ("--has", TextOp::HasToken, &a.has),
        ("--prefix", TextOp::HasPrefix, &a.prefix),
    ] {
        for g in given {
            out.push(text_arg(flag, op, g)?);
        }
    }
    Ok(out)
}

/// `--fn sum,count,min,max` as aggregate functions; an unknown name is a
/// usage error (exit 2).
fn aggregate_functions(given: &[String]) -> Result<Vec<bellwether::types::AggregateFn>, Failure> {
    given
        .iter()
        .map(|f| {
            bellwether::types::AggregateFn::parse(f.trim()).ok_or_else(|| {
                Failure::Usage(format!(
                    "--fn {f:?} is not an aggregate function; pass a comma-separated list of \
                     sum, count, min, max, for example --fn sum,count"
                ))
            })
        })
        .collect()
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

#[allow(clippy::too_many_lines)] // one branch per lookup mode
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
    if let (None, Some(ts)) = (&window, &a.ts) {
        if a.at.is_none() || a.lookback.is_none() {
            return Err(Failure::Usage(format!(
                "--ts names a window's time column; give the window with --window START..END, or \
                 price `asof --at --lookback --ts {ts}` with --latest-by {ts} --at <instant> \
                 --lookback <length>"
            )));
        }
        if a.from.is_empty() {
            return Err(Failure::Usage(
                "--ts with --at and --lookback records the key file's run as that window; pass \
                 --from <keys.parquet>"
                    .into(),
            ));
        }
    }
    let request = bellwether::WhyRequest {
        keys: a.from.first().map(|p| key_file(p, a.key.as_ref())),
        more_keys: a
            .from
            .iter()
            .skip(1)
            .map(|p| key_file(p, a.key.as_ref()))
            .collect(),
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
        valid_by: a.valid_by.clone(),
        known_by: a.known_by.clone(),
        valid: a.valid.clone(),
        known: a.known.clone(),
        window,
        ts_column: a.ts.clone(),
        columns: a.columns.clone(),
        max_candidates: a.max_candidates,
        gates: a.gates.clone(),
        sketches_only: a.sketches_only,
        text: why_text(a)?,
        join: a.join.clone(),
        tombstone: a
            .tombstone
            .as_deref()
            .map(bellwether::spec::parse_tombstone)
            .transpose()
            .map_err(|e| Failure::Usage(e.to_string()))?,
        aggregate: a
            .value
            .as_ref()
            .map(|value| -> Result<_, Failure> {
                Ok(bellwether::AggregateQuery {
                    value: value.clone(),
                    functions: if a.functions.is_empty() {
                        vec![
                            bellwether::types::AggregateFn::Sum,
                            bellwether::types::AggregateFn::Count,
                        ]
                    } else {
                        aggregate_functions(&a.functions)?
                    },
                })
            })
            .transpose()?,
    };
    // Text lookups take none of the lookup-mode flags; the facade refuses
    // them first, with that cause.
    let mode = if request.text.is_empty() {
        request.lookup_mode().map_err(Failure::Usage)?
    } else {
        bellwether::LookupMode::Locate
    };
    if mode != bellwether::LookupMode::Locate && a.from.is_empty() && a.workload.is_none() {
        return Err(Failure::Usage(if mode.text_op().is_some() {
            format!(
                "--mode {} prices a key file of one string column's values as a text lookup; \
                 pass --from <values.parquet> (or --workload <hash>), or give the lookups with \
                 --contains, --has, or --prefix COL=TEXT",
                mode.as_str()
            )
        } else {
            format!(
                "--mode {} prices a workload's keys; pass --from <keys.parquet> (or --workload \
                 <hash>)",
                mode.as_str()
            )
        }));
    }
    if request.window.is_some() && mode != bellwether::LookupMode::Locate {
        return Err(Failure::Usage(
            "--window prices point lookups in a time window; a latest or asof lookup takes \
             --lookback instead (--latest-by <column> --lookback <length>, and --at <instant> \
             for asof)"
                .into(),
        ));
    }
    let e = engine_at(g, &a.root.root, a.at_manifest)?;
    let r = e.why(&request)?;
    let fix_command = if e.is_table() {
        format!(
            "bellwether fix '{}' --index {}",
            bellwether::redact_db(&a.root.root),
            e.index_location()
        )
    } else {
        format!("bellwether fix {}", r.root)
    };
    done(&r, human::why(&r, out, &fix_command), EXIT_OK)
}
