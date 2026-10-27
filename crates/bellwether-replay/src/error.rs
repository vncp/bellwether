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
    /// A `Rewrite` proposal whose shape carries no typed plans has no
    /// replay: its read set depends on the rewritten plan, which the shape
    /// then states only in prose. A shape with `before_plan` and
    /// `after_plan` is replayed (relational replay, WP15).
    #[error(
        "this rewrite has no replay: its shape states the rewritten plan only in prose; give the \
         rewrite a before_plan and an after_plan (typed consumer plans, docs/evolve.md section \
         14) so the engine can price both"
    )]
    RewriteNotScorable,
    /// A proposed key spec serves none of the workload's runs, so scoring it
    /// would report "no change" for a spec that was never applicable.
    #[error(
        "proposed spec {spec} serves none of the workload's runs, which look up {}; propose a \
         key on the columns a run looks up",
        column_sets(run_columns)
    )]
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
    /// A consumer plan is malformed or does not fit its datasets: another
    /// version, an input out of range, a cycle, a node feeding two nodes, a
    /// node the root does not read, a dataset not bound, a column not in
    /// its input, an ambiguous column, or join or key columns whose
    /// encodings do not compare. Refused before anything is priced.
    #[error("consumer plan{}: {reason}", node.map(|n| format!(" node {n}")).unwrap_or_default())]
    InvalidPlan {
        /// The node at fault, when one is.
        node: Option<u32>,
        /// What is wrong, and what to do.
        reason: String,
    },
    /// A two-sided join (or a semi-join keyed by another node's output)
    /// whose side has no complete exact or temporal key on exactly its
    /// join columns, or is no single dataset's rows: the common keys are an
    /// intersection of two indexes, so without one the node is refused,
    /// never priced as a scan.
    #[error("{}", join_not_indexed(*node, side, dataset, columns, derived.as_deref()))]
    JoinNotIndexed {
        /// The join node.
        node: u32,
        /// `left`, `right`, or `keys` (a semi-join's key source).
        side: &'static str,
        /// The dataset of that side (`node <n>` for derived rows).
        dataset: String,
        /// That side's join columns.
        columns: Vec<String>,
        /// Why no index can hold that side's rows, when it is rows derived
        /// from several scans; `None` when the dataset only lacks the key.
        derived: Option<String>,
    },
}

fn join_not_indexed(
    node: u32,
    side: &str,
    dataset: &str,
    columns: &[String],
    derived: Option<&str>,
) -> String {
    match derived {
        Some(why) => format!(
            "consumer plan node {node} joins on its {side} side ({dataset}: {}), and {why}; \
             join the scans' rows first and aggregate after, or price the two steps as two plans",
            columns.join(", ")
        ),
        None => format!(
            "consumer plan node {node} joins on its {side} side {dataset}({}), and {dataset} has \
             no complete exact or temporal key on exactly those columns, so the common keys \
             cannot be read from two indexes and the join is not priced as a scan; declare \
             `exact:{}` (add `,counts` for an exact output cardinality) on {dataset} and index \
             it, or price a configuration that has it",
            columns.join(", "),
            columns.join(",")
        ),
    }
}

/// Result alias for operations that return [`ReplayError`].
pub type ReplayResult<T> = std::result::Result<T, ReplayError>;

/// Key-column sets in words: `(order_id)` or `(a, b) and (c)`.
fn column_sets(sets: &[Vec<String>]) -> String {
    if sets.is_empty() {
        return "no key columns".into();
    }
    sets.iter()
        .map(|s| format!("({})", s.join(", ")))
        .collect::<Vec<_>>()
        .join(" and ")
}

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
            ReplayError::InvalidPlan { .. } => Error::InvalidArgument {
                field: "plan",
                reason: msg,
            },
            ReplayError::JoinNotIndexed { .. } => Error::KeySpec(msg),
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
        // Review of PR #32, item 7h: the run columns in words, never Debug.
        let e = ReplayError::SpecServesNoRun {
            spec: "exact:x".into(),
            run_columns: vec![vec!["order_id".into()], vec!["a".into(), "b".into()]],
        };
        assert_eq!(
            e.to_string(),
            "proposed spec exact:x serves none of the workload's runs, which look up (order_id) \
             and (a, b); propose a key on the columns a run looks up"
        );
    }
}
