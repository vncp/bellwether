//! [`ReplayError`]: the refusals a caller must be able to tell apart.
//!
//! Every refusal happens before anything is written: nothing reaches the
//! ledger, the key-set sink, or a recorder before the check that refuses it.
//! A `ReplayError` converts into the shared [`Error`] (for `?` in other
//! crates) as the variant that fits: a refusal the ledger's rules would also
//! make becomes [`Error::LedgerRule`], a spec problem [`Error::KeySpec`], a
//! source integrity problem [`Error::Source`].

use bellwether_types::{Error, Hash};

/// A refusal or failure of a replay operation.
#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    /// A failure from the engine crates (store, source, ledger, core).
    #[error(transparent)]
    Engine(#[from] Error),
    /// A `Rewrite` proposal has no replay: its read set depends on the
    /// rewritten plan, which a `RewriteShape` states in prose.
    #[error(
        "this crate has no replay for a Rewrite proposal: its read set depends on the rewritten \
         plan, which a RewriteShape states in prose (contract v3 backlog #2)"
    )]
    RewriteNotScorable,
    /// A proposed key spec serves none of the workload's runs, so scoring it
    /// would report "no change" for a spec that was never applicable.
    #[error("proposed spec {spec} serves none of the workload's runs (their key columns: {run_columns:?})")]
    SpecServesNoRun {
        /// The proposed spec's name.
        spec: String,
        /// The distinct key-column sets of the workload's runs.
        run_columns: Vec<Vec<String>>,
    },
    /// A scored replay does not match the proposal it cites: another body,
    /// no inputs, costs whose exactness contradicts the proposal's kind, or
    /// a cost model named although every cost is exact (or missing although
    /// one is not).
    #[error("replay does not match proposal {proposal}: {detail}")]
    ProposalMismatch {
        /// The proposal the replay cites.
        proposal: Hash,
        /// What disagrees.
        detail: String,
    },
    /// The hash a replay was asked to score resolves to another record kind.
    #[error("{0} is not a proposal; a replay scores a proposal")]
    NotAProposal(Hash),
    /// A bench workload does not verify: a run's keys, a run's count, the
    /// workload hash, the corpus id, the format, or a run file's location.
    #[error("bench workload does not verify{}: {detail}", run_id.as_ref().map(|r| format!(" (run {r})")).unwrap_or_default())]
    BenchMismatch {
        /// The run that failed, when one did.
        run_id: Option<String>,
        /// What failed.
        detail: String,
    },
    /// The layout model's map of distinct cluster keys would exceed its
    /// stated bound (`TransientLimits::max_cluster_key_bytes`).
    #[error(
        "the layout model holds {keys} distinct cluster keys ({bytes} accounted bytes), past its \
         cap of {cap} bytes; raise TransientLimits::max_cluster_key_bytes or cluster by fewer \
         distinct values"
    )]
    ClusterKeysExceeded {
        /// Distinct cluster keys seen when the cap was hit.
        keys: u64,
        /// Accounted bytes when the cap was hit.
        bytes: u64,
        /// The cap.
        cap: u64,
    },
    /// A run column carries an embedded bloom filter the statistics
    /// baseline cannot probe exactly, so the baseline would be weaker than
    /// its definition; it refuses instead.
    #[error(
        "{file}: column {column} carries an embedded bloom filter the baseline cannot probe: {why}"
    )]
    UnprobeableFilter {
        /// The file.
        file: String,
        /// The column.
        column: String,
        /// Why it cannot be probed.
        why: String,
    },
}

/// Result alias for operations that return [`ReplayError`].
pub type ReplayResult<T> = std::result::Result<T, ReplayError>;

impl From<ReplayError> for Error {
    fn from(e: ReplayError) -> Self {
        let msg = e.to_string();
        match e {
            ReplayError::Engine(inner) => inner,
            ReplayError::RewriteNotScorable
            | ReplayError::ProposalMismatch { .. }
            | ReplayError::NotAProposal(_) => Error::LedgerRule(msg),
            ReplayError::SpecServesNoRun { .. } => Error::KeySpec(msg),
            ReplayError::BenchMismatch { .. } | ReplayError::UnprobeableFilter { .. } => {
                Error::Source(msg)
            }
            ReplayError::ClusterKeysExceeded { .. } => Error::Other(msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refusals_convert_into_the_shared_variant_that_fits() {
        assert!(matches!(
            Error::from(ReplayError::RewriteNotScorable),
            Error::LedgerRule(_)
        ));
        assert!(matches!(
            Error::from(ReplayError::SpecServesNoRun {
                spec: "exact:x".into(),
                run_columns: vec![]
            }),
            Error::KeySpec(_)
        ));
        assert!(matches!(
            Error::from(ReplayError::BenchMismatch {
                run_id: Some("r".into()),
                detail: "d".into()
            }),
            Error::Source(m) if m.contains("run r")
        ));
        assert!(matches!(
            Error::from(ReplayError::Engine(Error::NotFound(Hash::of(b"x")))),
            Error::NotFound(_)
        ));
    }
}
