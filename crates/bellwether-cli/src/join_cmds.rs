//! `join`: semi-joins, key overlap, and freshness of join keys (B3).
//! Argument parsing and formatting only; the facade computes.
//!
//! The data root is the left dataset. `--join` names a join key whole
//! (`join:orders.order_id=payables.order_id`) or by its right side
//! (`payables.order_id`): the right dataset is the folder of that name
//! beside the data root, read through its own index.

use std::path::PathBuf;

use clap::Subcommand;

use crate::{
    done, engine, human, key_file, Failure, Global, Out, Outcome, RootArg, EXIT_FAILED, EXIT_OK,
};

/// `join` subcommands.
#[derive(Subcommand)]
pub(crate) enum JoinCommand {
    /// Plan semijoin(keys) through the join key: step 1, the row groups of
    /// this folder holding a key the right dataset also holds; step 2, the
    /// right dataset's row groups holding one; each with its residual.
    /// Writes one plan Evidence per step.
    Semijoin {
        #[command(flatten)]
        root: RootArg,
        /// The join key: RIGHT.COLS (the right dataset and its columns), or
        /// the whole key, join:LEFT.COLS=RIGHT.COLS.
        #[arg(long, value_name = "RIGHT.COLS")]
        join: String,
        /// A Parquet file of keys on the join key's left columns.
        #[arg(long, value_name = "FILE")]
        keys: PathBuf,
        /// Key columns to take from the file, comma-separated [default: all].
        #[arg(long, value_name = "COLS", value_delimiter = ',')]
        key: Option<Vec<String>>,
    },
    /// The two key sets' overlap, exact, from the join key alone: keys on
    /// both sides, on one side only, and row groups (with counts, rows) per
    /// shared key. Writes nothing.
    Overlap {
        #[command(flatten)]
        root: RootArg,
        /// The join key: RIGHT.COLS or join:LEFT.COLS=RIGHT.COLS.
        #[arg(long, value_name = "RIGHT.COLS")]
        join: String,
    },
    /// Whether each join key is fresh: stale when this folder's row groups
    /// changed or the right dataset committed since it was built. Exits 1
    /// when one is stale, naming the command that rebuilds it.
    Verify {
        #[command(flatten)]
        root: RootArg,
        /// Only this join key [default: every declared one].
        #[arg(long, value_name = "RIGHT.COLS")]
        join: Option<String>,
    },
}

pub(crate) fn run(g: &Global, out: &Out, cmd: &JoinCommand) -> Result<Outcome, Failure> {
    match cmd {
        JoinCommand::Semijoin {
            root,
            join,
            keys,
            key,
        } => {
            let r = engine(g, &root.root)?.semijoin(join, &key_file(keys, key.as_ref()))?;
            done(&r, human::semijoin(&r, out), EXIT_OK)
        }
        JoinCommand::Overlap { root, join } => {
            let r = engine(g, &root.root)?.join_overlap(join)?;
            done(&r, human::join_overlap(&r, out), EXIT_OK)
        }
        JoinCommand::Verify { root, join } => {
            let r = engine(g, &root.root)?.join_verify(join.as_deref())?;
            let code = if r.pass { EXIT_OK } else { EXIT_FAILED };
            done(&r, human::join_verify(&r, out), code)
        }
    }
}
