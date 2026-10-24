//! `bellwether-profile`: a thin wrapper over `bellwether_profile::profile`
//! for running a profile by hand before WP9's `bellwether why` exists.
//!
//! ```text
//! bellwether-profile <dataset-root> --index <dir> [--workload <dir>]
//!     [--columns a,b,c] [--time-columns t] [--k 2] [--max-tuples 64]
//!     [--memory-mib 256] [--max-tracked N] [--source-config parquet]
//!     [--no-reuse] [--no-record] [--json]
//! ```
//!
//! Parquet sources only. `--index` is the index root holding `cache/`,
//! `ledger/`, and, if the folder is indexed, `manifest.json`.

#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use bellwether_ledger::Ledger;
use bellwether_profile::{profile, record, render, ProfileOptions};
use bellwether_source::ParquetSource;
use bellwether_store::ObjectStoreBackend;

const USAGE: &str = "usage: bellwether-profile <dataset-root> --index <dir> [--workload <dir>] \
[--columns a,b] [--time-columns t] [--k N] [--max-tuples N] [--memory-mib N] [--max-tracked N] \
[--source-config S] [--no-reuse] [--no-record] [--json]";

struct Args {
    root: String,
    index: String,
    options: ProfileOptions,
    record: bool,
    json: bool,
}

fn list(v: &str) -> Vec<String> {
    v.split(',')
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

fn parse(argv: &[String]) -> Result<Args, String> {
    let mut root = None;
    let mut index = None;
    // This binary reads Parquet only; `--source-config` may refine it.
    let mut options = ProfileOptions::for_source("parquet");
    let mut record = true;
    let mut json = false;
    let mut it = argv.iter();
    while let Some(a) = it.next() {
        let mut value = |name: &str| it.next().cloned().ok_or(format!("{name} needs a value"));
        let number = |name: &str, v: String| v.parse::<usize>().map_err(|e| format!("{name}: {e}"));
        match a.as_str() {
            "--index" => index = Some(value("--index")?),
            "--workload" => options.workload = Some(PathBuf::from(value("--workload")?)),
            "--columns" => options.columns = Some(list(&value("--columns")?)),
            "--time-columns" => options.time_columns = Some(list(&value("--time-columns")?)),
            "--k" => options.max_tuple_len = number("--k", value("--k")?)?,
            "--max-tuples" => options.max_tuples = number("--max-tuples", value("--max-tuples")?)?,
            "--memory-mib" => {
                options.memory_budget = number("--memory-mib", value("--memory-mib")?)? << 20;
            }
            "--max-tracked" => {
                options.max_tracked_per_key =
                    Some(number("--max-tracked", value("--max-tracked")?)?);
            }
            "--source-config" => options.source_config = Some(value("--source-config")?),
            "--no-reuse" => options.reuse_cache = false,
            "--no-record" => record = false,
            "--json" => json = true,
            "-h" | "--help" => return Err(USAGE.to_string()),
            other if other.starts_with('-') => {
                return Err(format!("unknown flag {other}\n{USAGE}"))
            }
            other => {
                if root.replace(other.to_string()).is_some() {
                    return Err(format!("one dataset root, please\n{USAGE}"));
                }
            }
        }
    }
    Ok(Args {
        root: root.ok_or(USAGE)?,
        index: index.ok_or(format!("--index is required\n{USAGE}"))?,
        options,
        record,
        json,
    })
}

fn run(args: &Args) -> Result<String, String> {
    let store = Arc::new(ObjectStoreBackend::from_url(&args.index).map_err(|e| e.to_string())?);
    let source = ParquetSource::new();
    let report = profile(&source, &args.root, &store, &args.options).map_err(|e| e.to_string())?;
    let hash = if args.record {
        let ledger = Ledger::new(Arc::clone(&store));
        Some(record(&ledger, &report).map_err(|e| e.to_string())?.1)
    } else {
        None
    };
    if args.json {
        let value = serde_json::json!({ "evidence": hash, "report": report });
        serde_json::to_string_pretty(&value).map_err(|e| e.to_string())
    } else {
        Ok(render(&report, hash.as_ref()))
    }
}

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    match parse(&argv).and_then(|a| run(&a)) {
        Ok(text) => {
            println!("{text}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
