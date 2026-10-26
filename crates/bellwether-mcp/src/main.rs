//! `bellwether-mcp`: the MCP server over stdio, bound to one data folder.
//! The same server is `bellwether mcp`, with the same flags
//! ([`EngineArgs`], [`BudgetArgs`], [`KeyArgs`]). Exit codes: 0 when the
//! client disconnects, 2 usage (a key or trust file that cannot be used
//! included), 4 the server could not start.

#![forbid(unsafe_code)]

use std::process::ExitCode;

use bellwether_mcp::args::OptionsError;
use bellwether_mcp::{serve_stdio, BudgetArgs, Config, EngineArgs, KeyArgs};
use clap::Parser;

/// Serve Bellwether over MCP (stdio) for one data folder.
#[derive(Parser)]
#[command(name = "bellwether-mcp", version)]
struct Args {
    /// The folder of data files the server is bound to.
    root: String,
    #[command(flatten)]
    engine: EngineArgs,
    #[command(flatten)]
    budget: BudgetArgs,
    #[command(flatten)]
    keys: KeyArgs,
}

fn main() -> ExitCode {
    let args = Args::parse();
    let engine = match args.engine.options().and_then(|mut o| {
        args.keys.apply(&args.root, &mut o)?;
        Ok(o)
    }) {
        Ok(o) => o,
        Err(e @ OptionsError::Usage(_)) => {
            eprintln!("bellwether-mcp: {e}");
            return ExitCode::from(2);
        }
        Err(e @ OptionsError::Engine(_)) => {
            eprintln!("bellwether-mcp: {e}");
            return ExitCode::from(4);
        }
    };
    let config = Config {
        root: args.root,
        engine,
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
