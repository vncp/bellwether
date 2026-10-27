//! The adapters' typed refusals. Every message says what is wrong and what
//! to do next; none is an engine's error string passed through.

/// Why a plan could not be translated (or, with the `datafusion` feature,
/// executed).
#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    /// The plan or residual names a column the dataset schema lacks.
    #[error(
        "the plan's residual names column {column:?} ({context}), which is not in the dataset \
         schema; emit the plan against the schema of the index that planned it"
    )]
    UnknownColumn {
        /// The column.
        column: String,
        /// Where it was named.
        context: String,
    },
    /// The plan names a file the adapter's inputs do not know.
    #[error(
        "the plan names file {path} that {why}; re-plan against the current index \
         (`bellwether locate ... --emit`) so the plan and the files agree"
    )]
    UnknownFile {
        /// The file.
        path: String,
        /// Why it is unknown.
        why: String,
    },
    /// The plan names a row group the file does not have.
    #[error(
        "the plan reads row group {row_group} of {path}, which has {row_groups} row groups; the \
         plan does not belong to this file, so re-plan against the current index"
    )]
    RowGroupOutOfRange {
        /// The file.
        path: String,
        /// The row group asked.
        row_group: u32,
        /// The file's row groups.
        row_groups: usize,
    },
    /// Canonical bytes in the residual that do not decode at the column's
    /// dataset type.
    #[error(
        "the residual's {what} on column {column:?} holds canonical bytes that are not a value \
         of its dataset type ({reason}); the plan was not computed against this schema, so \
         re-plan against the index that holds it"
    )]
    Canonical {
        /// The column (or expression).
        column: String,
        /// Which part of the residual.
        what: String,
        /// The decoder's reason.
        reason: String,
    },
    /// A residual (or plan feature) the engine cannot evaluate exactly.
    #[error("{engine} cannot run this plan exactly: {what} {reason}; {next}")]
    Unsupported {
        /// The engine.
        engine: &'static str,
        /// What it cannot run.
        what: String,
        /// Why.
        reason: String,
        /// What to do instead.
        next: String,
    },
    /// The adapter needs a planned file's row-group row counts and was not
    /// given them.
    #[error(
        "{engine} needs the row counts of the row groups of {path} ({why}), which were not \
         given; emit through `bellwether locate|latest|asof --emit {engine}` (which takes them \
         from the manifest), pass EmitOptions::row_offsets, or set footer_hint to read them \
         from the file's footer"
    )]
    RowOffsetsNeeded {
        /// The engine.
        engine: &'static str,
        /// The file.
        path: String,
        /// What they are for.
        why: &'static str,
    },
    /// The keys file cannot stand in for the residual's key list.
    #[error("the keys file {path} cannot replace the residual's key list: {reason}")]
    KeysFile {
        /// The keys file.
        path: String,
        /// Why.
        reason: String,
    },
    /// A planned file's footer could not be read for `footer_hint`.
    #[error(
        "cannot read the footer of {path} for the footer hint ({reason}); check the file is the \
         one the plan was made over, or emit without the footer hint"
    )]
    Footer {
        /// The file.
        path: String,
        /// Why.
        reason: String,
    },
    /// The plan itself is malformed.
    #[error("the plan is malformed: {0}; emit the plan bellwether printed, unchanged")]
    Plan(String),
    /// Execution in DataFusion failed (`datafusion` feature).
    #[error("DataFusion could not run the plan: {0}")]
    Execution(String),
}

/// Result alias for the adapters.
pub type Result<T, E = AdapterError> = std::result::Result<T, E>;
