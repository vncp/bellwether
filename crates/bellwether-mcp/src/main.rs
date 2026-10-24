//! `bellwether-mcp`: the MCP server over stdio, bound to one data folder.
//! The same server is `bellwether mcp`. Exit codes: 0 when the client
//! disconnects, 2 usage, 4 the server could not start.

#![forbid(unsafe_code)]

use std::process::ExitCode;

use bellwether::{EngineOptions, SourceFormat};
use bellwether_mcp::{serve_stdio, BudgetArgs, Config};
use clap::{Parser, ValueEnum};

#[derive(Clone, Copy, ValueEnum)]
enum FormatArg {
    /// Parquet files.
    Parquet,
    /// Comma-separated files with a header row.
    Csv,
    /// JSON lines.
    Jsonl,
}

/// Serve Bellwether over MCP (stdio) for one data folder.
#[derive(Parser)]
#[command(name = "bellwether-mcp", version)]
struct Args {
    /// The folder of data files the server is bound to.
    root: String,
    /// Index location: a path or a store URL [default: <ROOT>/.bellwether].
    #[arg(long, value_name = "PATH|URL")]
    index: Option<String>,
    /// Kind of files under the root.
    #[arg(long, value_enum, default_value_t = FormatArg::Parquet)]
    format: FormatArg,
    /// A fixed RFC 3339 time for every record's `created_at` (testing and
    /// audits).
    #[arg(long, value_name = "RFC3339", hide = true)]
    clock: Option<String>,
    #[command(flatten)]
    budget: BudgetArgs,
}

fn main() -> ExitCode {
    let args = Args::parse();
    let format = match args.format {
        FormatArg::Parquet => SourceFormat::Parquet,
        FormatArg::Csv => SourceFormat::Csv {
            delimiter: b',',
            has_header: true,
        },
        FormatArg::Jsonl => SourceFormat::JsonLines,
    };
    let config = Config {
        root: args.root,
        engine: EngineOptions {
            index: args.index,
            format,
            clock: args.clock,
            ..EngineOptions::default()
        },
        budget: args.budget.into(),
    };
    match serve_stdio(&config) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("bellwether-mcp: {e}");
            ExitCode::from(4)
        }
    }
}
