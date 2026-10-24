//! Typed errors. Every ambiguity at a boundary is one of these, never a
//! best guess and never a pass.

/// Why a reconciliation could not run or could not be certified.
///
/// Engine errors from the source, the ledger, or the canonical encoder are
/// wrapped in [`VerifyError::Engine`]; everything this crate decides itself
/// has its own variant so a caller can match on it.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum VerifyError {
    /// `StaticProof` coverage was requested. Reconciliation compares data;
    /// a static algebraic proof belongs to the rewrite verifier (WP13), and
    /// this crate never produces one.
    #[error(
        "static-proof coverage ({identity:?}) is not produced by reconciliation; \
         it belongs to the rewrite verifier"
    )]
    StaticProofUnsupported {
        /// The identity the caller asked for.
        identity: String,
    },
    /// The spec itself is malformed: duplicate or missing columns, a
    /// non-decimal total column, a sampling fraction outside `(0, 1]`, a
    /// malformed or null workload key, and so on.
    #[error("invalid reconcile spec: {0}")]
    InvalidSpec(String),
    /// The two sides do not share a dataset schema: a column present on one
    /// side only, types without a common canonical encoding, or a type with
    /// no canonical encoding at all.
    #[error("schema: {0}")]
    Schema(String),
    /// A value could not be converted to its dataset type exactly (for
    /// example a decimal not representable at the dataset scale). Located to
    /// the side, file, row group, and column.
    #[error("{side} side, {file} row group {row_group}, column {column:?}: {detail}")]
    Unrepresentable {
        /// `"left"` or `"right"`.
        side: &'static str,
        /// File path.
        file: String,
        /// Row group index within the file.
        row_group: u32,
        /// Column name.
        column: String,
        /// What the encoder reported.
        detail: String,
    },
    /// With `latest_by`, a key element is null. Rows whose keys are null
    /// cannot be told apart, so keeping only the newest of them could hide
    /// a row missing on the other side; they are refused rather than
    /// collapsed. Without `latest_by` the multiset comparison keeps them.
    #[error(
        "{side} side, {file} row group {row_group}, key column {column:?} is null; with \
         latest_by, rows with a null key cannot be told apart and are refused (reconcile \
         without latest_by, or exclude them)"
    )]
    NullKey {
        /// `"left"` or `"right"`.
        side: &'static str,
        /// File path.
        file: String,
        /// Row group index within the file.
        row_group: u32,
        /// Key column name.
        column: String,
    },
    /// The coverage compared no key although there was something to
    /// compare: every supplied workload key is absent from both sides (a
    /// key encoded at another decimal scale looks like this), or a sample
    /// selected none of the keys present. A certificate over nothing would
    /// pass vacuously, so this is an error.
    #[error("nothing compared: {0}")]
    NothingCompared(String),
    /// An exact decimal total does not fit the integer type of its column
    /// (`i128` for Decimal128, `i256` for Decimal256).
    #[error("decimal total of {column:?} overflows {width}{group}")]
    Overflow {
        /// Total column.
        column: String,
        /// `"i128"` or `"i256"`.
        width: &'static str,
        /// The group, rendered, or empty.
        group: String,
    },
    /// The memory budget cannot hold the state the comparison needs, even
    /// at the largest pass count allowed.
    #[error("memory budget: {0}")]
    Budget(String),
    /// A certificate must name the manifest version it was computed against
    /// (invariant 6), and none is known: no side is indexed and the caller
    /// supplied none, or the candidates disagree.
    #[error("certificate manifest: {0}")]
    Manifest(String),
    /// An indexed side's manifest does not describe the files the source
    /// discovers, or its plan does not resolve against them.
    #[error("index: {0}")]
    Index(String),
    /// A source returned data that disagrees with its own metadata.
    #[error("source contract: {0}")]
    SourceContract(String),
    /// An error from another engine crate (source, ledger, encoder).
    #[error(transparent)]
    Engine(#[from] bellwether_types::Error),
}

impl From<VerifyError> for bellwether_types::Error {
    /// Folds a verify error into the engine-wide error type for callers that
    /// do not match on it. Engine errors come back unchanged; spec and
    /// schema errors become `KeySpec`; the rest become `Other`.
    fn from(e: VerifyError) -> Self {
        match e {
            VerifyError::Engine(inner) => inner,
            VerifyError::InvalidSpec(_) | VerifyError::Schema(_) => Self::KeySpec(e.to_string()),
            other => Self::Other(other.to_string()),
        }
    }
}

/// Result alias for this crate.
pub type Result<T> = std::result::Result<T, VerifyError>;
