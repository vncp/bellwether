//! `sketch`: approximate distinct counts and overlaps from `sketch`
//! sidecars. Argument parsing and formatting only; the facade computes.

use clap::Subcommand;

use crate::{done, engine, human, Failure, Global, Out, Outcome, RootArg, EXIT_OK};

/// `sketch` subcommands.
#[derive(Subcommand)]
pub(crate) enum SketchCommand {
    /// Approximate count(distinct) per column (or `a+b` tuple) from the
    /// index's sketches, with its error bound, rows per value, and files
    /// that repeat a value. No data file is read unless --transient.
    Cardinality {
        #[command(flatten)]
        root: RootArg,
        /// A column, or columns joined by `+`; repeat for several [default:
        /// every item of every complete sketch].
        #[arg(long = "column", value_name = "ITEM")]
        items: Vec<String>,
        /// Read the files now for items no complete sketch holds (and say
        /// how many were read) instead of refusing.
        #[arg(long)]
        transient: bool,
    },
    /// Approximate Jaccard similarity of two sketched items' value sets.
    Overlap {
        #[command(flatten)]
        root: RootArg,
        /// The left item: a column, or columns joined by `+`.
        left: String,
        /// The right item.
        right: String,
        /// The data folder the right item is in, read through its own index
        /// at its default location [default: the same folder].
        #[arg(long = "with", value_name = "ROOT")]
        with: Option<String>,
    },
}

pub(crate) fn run(g: &Global, out: &Out, cmd: &SketchCommand) -> Result<Outcome, Failure> {
    match cmd {
        SketchCommand::Cardinality {
            root,
            items,
            transient,
        } => {
            let r = engine(g, &root.root)?.cardinality(&bellwether::CardinalityRequest {
                items: items.clone(),
                transient: *transient,
            })?;
            done(&r, human::cardinality(&r, out), EXIT_OK)
        }
        SketchCommand::Overlap {
            root,
            left,
            right,
            with,
        } => {
            let e = engine(g, &root.root)?;
            let other = match with {
                Some(_) if g.engine.index.is_some() => {
                    return Err(Failure::Usage(
                        "--with reads the other folder through its own index at its default \
                         location, and --index names one location; drop --index, or compare \
                         two items of one folder"
                            .into(),
                    ))
                }
                Some(w) => Some(engine(g, w)?),
                None => None,
            };
            let r = e.overlap(left, right, other.as_ref())?;
            done(&r, human::overlap(&r, out), EXIT_OK)
        }
    }
}
