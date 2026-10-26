//! The command-line flags `bellwether-mcp` and `bellwether mcp` share: how
//! the engine opens the data folder, and the session budget. One definition,
//! so the two binaries cannot drift apart: on an unindexed CSV or JSON lines
//! folder the engine falls back to `--schema`, `--chunk-size`, and
//! `--fingerprint-policy`, and a flag one binary lacked would give a
//! different result for the same folder.

use std::path::PathBuf;

use bellwether::{EngineOptions, FingerprintPolicy, Partitioning, PathTemplate, SourceFormat};
use clap::ValueEnum;

use crate::budget::Budget;

/// Kind of files under the root.
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum FormatArg {
    /// Parquet files.
    Parquet,
    /// Comma-separated files with a header row.
    Csv,
    /// JSON lines.
    Jsonl,
}

/// Which bytes a file fingerprint hashes.
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum PolicyArg {
    /// BLAKE3 of the last 64 KiB.
    Tail64k,
    /// BLAKE3 of the whole file.
    Full,
}

/// How directories carry data.
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum PartitioningArg {
    /// Paths carry no data.
    None,
    /// Hive-style name=value directories are columns.
    Hive,
}

/// How the engine opens the data folder. Every flag is global, so the CLI
/// accepts it before or after any subcommand.
#[derive(Clone, Debug, clap::Args)]
pub struct EngineArgs {
    /// Index location: a path or a store URL (default: `<ROOT>/.bellwether`).
    #[arg(
        long,
        global = true,
        value_name = "PATH|URL",
        help = "Index location: a path or a store URL [default: <ROOT>/.bellwether]"
    )]
    pub index: Option<String>,
    /// Kind of files under the root.
    #[arg(long, global = true, value_enum, default_value_t = FormatArg::Parquet)]
    pub format: FormatArg,
    /// Chunk size in bytes for CSV and JSON lines files seen for the first
    /// time.
    #[arg(long, global = true, value_name = "BYTES")]
    pub chunk_size: Option<u64>,
    /// The dataset schema as a JSON file (a list of {"name", "type",
    /// "nullable"} with Arrow type spellings), instead of inferring it from
    /// the first file; needed when files differ in decimal scale or time
    /// unit.
    #[arg(long, global = true, value_name = "FILE")]
    pub schema: Option<PathBuf>,
    /// Which bytes a file fingerprint hashes when a new index is built:
    /// the last 64 KiB (default) or the whole file, which also catches an
    /// in-place patch that keeps size and mtime.
    #[arg(long, global = true, value_enum)]
    pub fingerprint_policy: Option<PolicyArg>,
    /// How directories carry data: `hive` makes every `name=value`
    /// directory below the root a column (for example day=2026-01-02), when
    /// a new index is built or an unindexed folder is read. An existing
    /// index keeps its recorded scheme.
    #[arg(long, global = true, value_enum)]
    pub partitioning: Option<PartitioningArg>,
    /// File paths carry data: the path of every file below the root, with
    /// {name} or {name:type} placeholders (type str, int, or date) whose
    /// values become columns, for example `{day:date}.parquet` or
    /// `grouped_daily_us/{symbol:str}/{day:date}.parquet`. Every file must
    /// match (up to 10 that do not are named). Recorded with a new index
    /// like --partitioning, which it replaces; an existing index refuses
    /// another template.
    #[arg(
        long,
        global = true,
        value_name = "PATTERN",
        conflicts_with = "partitioning"
    )]
    pub path_template: Option<String>,
    /// A fixed RFC 3339 time for every record's `created_at` and for ages, so
    /// runs are byte-reproducible (testing and audits).
    #[arg(long, global = true, value_name = "RFC3339", hide = true)]
    pub clock: Option<String>,
}

/// Why [`EngineArgs::options`] failed.
#[derive(Debug)]
pub enum OptionsError {
    /// A flag's value cannot be used: the `--schema` file could not be
    /// read, or the `--path-template` is malformed (a usage error).
    Usage(String),
    /// The schema file was read but is not a dataset schema.
    Engine(bellwether::types::Error),
}

impl std::fmt::Display for OptionsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Usage(m) => f.write_str(m),
            Self::Engine(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for OptionsError {}

impl EngineArgs {
    /// The engine options these flags name; reads the `--schema` file and
    /// parses the `--path-template`.
    pub fn options(&self) -> Result<EngineOptions, OptionsError> {
        let format = match self.format {
            FormatArg::Parquet => SourceFormat::Parquet,
            FormatArg::Csv => SourceFormat::Csv {
                delimiter: b',',
                has_header: true,
            },
            FormatArg::Jsonl => SourceFormat::JsonLines,
        };
        let schema = match &self.schema {
            None => None,
            Some(p) => {
                let text = std::fs::read_to_string(p).map_err(|e| {
                    OptionsError::Usage(format!(
                        "cannot read the schema file {} ({e})",
                        p.display()
                    ))
                })?;
                Some(bellwether::schema_from_json(&text).map_err(OptionsError::Engine)?)
            }
        };
        Ok(EngineOptions {
            index: self.index.clone(),
            format,
            chunk_size: self.chunk_size,
            schema,
            fingerprint_policy: self.fingerprint_policy.map(|p| match p {
                PolicyArg::Tail64k => FingerprintPolicy::Tail64K,
                PolicyArg::Full => FingerprintPolicy::Full,
            }),
            partitioning: match (&self.path_template, self.partitioning) {
                (Some(t), _) => Some(Partitioning::Template(
                    PathTemplate::parse(t).map_err(|e| OptionsError::Usage(e.to_string()))?,
                )),
                (None, Some(PartitioningArg::None)) => Some(Partitioning::None),
                (None, Some(PartitioningArg::Hive)) => Some(Partitioning::Hive),
                (None, None) => None,
            },
            // Pinning is per lookup (`at_manifest` on the observe tools,
            // `--at-manifest` on the CLI's lookups), never per session.
            at_manifest: None,
            clock: self.clock.clone(),
            // Signing and trust are the server's own flags ([`KeyArgs`]);
            // the CLI gives them per command.
            signing_key: None,
            trusted_keys: None,
        })
    }
}

/// The server's signing and trust flags (ADR 029). Both are paths, read
/// once at startup; nothing comes from the environment. A key or trust file
/// that cannot be used stops the server before the handshake (exit 2).
#[derive(Clone, Debug, Default, clap::Args)]
pub struct KeyArgs {
    /// Sign what `apply.receipt` and `score.verify` write (the engine's
    /// attest stage): the file of the 32-byte seed `bellwether keys
    /// generate` wrote, readable by its owner only. No other tool signs.
    #[arg(long, value_name = "FILE")]
    pub signing_key: Option<PathBuf>,
    /// The trust file (`bellwether keys trust`): with it, every receipt
    /// chain in a result's envelope carries a verdict, `signed`,
    /// `untrusted`, `unsigned`, or `invalid`.
    #[arg(long, value_name = "FILE")]
    pub trusted_keys: Option<PathBuf>,
}

impl KeyArgs {
    /// Loads the key and the trust file for the data at `root` into
    /// `options`. Either one inside the index location is refused: a store
    /// writer could read the key there, or choose who is trusted.
    pub fn apply(&self, root: &str, options: &mut EngineOptions) -> Result<(), OptionsError> {
        let usage = |e: bellwether::types::Error| OptionsError::Usage(e.to_string());
        let index = options.index.clone();
        if let Some(p) = &self.signing_key {
            bellwether::refuse_inside_index(root, index.as_deref(), p, "signing_key")
                .map_err(usage)?;
            options.signing_key = Some(bellwether::load_signing_key(p).map_err(usage)?);
        }
        if let Some(p) = &self.trusted_keys {
            bellwether::refuse_inside_index(root, index.as_deref(), p, "trusted_keys")
                .map_err(usage)?;
            options.trusted_keys = Some(bellwether::load_trusted_keys(p).map_err(usage)?);
        }
        Ok(())
    }
}

/// The per-session budget flags.
#[derive(Clone, Copy, Debug, clap::Args)]
pub struct BudgetArgs {
    /// Replay records this session may cause the engine to write.
    #[arg(long, value_name = "N", default_value_t = Budget::default().replays)]
    pub budget_replays: u64,
    /// Rows the verifier may read in this session, both sides summed.
    #[arg(long, value_name = "N", default_value_t = Budget::default().rows)]
    pub budget_rows: u64,
    /// Wall time this session's tool calls may take, in seconds.
    #[arg(long, value_name = "SECONDS", default_value_t = Budget::default().seconds)]
    pub budget_seconds: u64,
}

impl From<BudgetArgs> for Budget {
    fn from(a: BudgetArgs) -> Self {
        Self {
            replays: a.budget_replays,
            rows: a.budget_rows,
            seconds: a.budget_seconds,
        }
    }
}
