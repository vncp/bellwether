//! What to reconcile: the key, the version rule, the columns compared and
//! summed, and how much of the key space to cover.

use std::collections::BTreeSet;
use std::time::Duration;

use bellwether_types::Hash;

use crate::error::{Result, VerifyError};

/// Default memory budget for the comparison state: 256 MiB. See
/// [`Options::memory_budget`] and the crate README for what it bounds.
pub const DEFAULT_MEMORY_BUDGET: u64 = 256 * 1024 * 1024;

/// Default number of per-key mismatches recorded as examples.
pub const DEFAULT_MAX_EXAMPLES: usize = 100;

/// The largest pass count a run may use before it gives up with
/// [`VerifyError::Budget`]. Every pass reads both sides once.
pub const MAX_PASSES: u32 = 1024;

/// How much of the key space to compare.
#[derive(Clone, Debug, PartialEq)]
pub enum CoverageRequest {
    /// Every key present on either side.
    Full,
    /// Only the keys a recorded workload touched. The caller supplies the
    /// key tuples; this crate never reads the workload format itself.
    Workload(WorkloadKeys),
    /// A deterministic sample of distinct keys: a key is in the sample when
    /// `xxh3_64(key, seed) < floor(fraction × 2^64)`, so both sides sample
    /// the same keys and the same seed always selects the same sample.
    Sampled {
        /// Requested fraction of distinct keys, in `(0, 1]`. The
        /// certificate records the realized fraction.
        fraction: f64,
        /// Sampling seed.
        seed: u64,
    },
    /// Never produced here: always refused with
    /// [`VerifyError::StaticProofUnsupported`]. A static algebraic proof is
    /// the rewrite verifier's (WP13).
    StaticProof {
        /// Identifier of the identity in the rule catalog.
        identity: String,
    },
}

/// The key tuples of a recorded workload, as the replay crate records them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkloadKeys {
    /// Hash of the recorded workload the keys come from; it names the
    /// coverage on the certificate and is one of its provenance inputs.
    pub workload: Hash,
    /// Age of the recorded workload when the reconcile starts, for the
    /// certificate's `oldest_input_age`.
    pub age: Duration,
    /// Canonical key tuples over [`ReconcileSpec::key_columns`], each value
    /// encoded at its column's **dataset** type (ADR 016), with no null
    /// element (a probe containing a null matches nothing, ADR 014).
    /// Duplicates are allowed and collapse.
    pub keys: Vec<Vec<u8>>,
}

/// A reconciliation request.
#[derive(Clone, Debug, PartialEq)]
pub struct ReconcileSpec {
    /// Key columns, in tuple order. A composite key is encoded as the
    /// concatenation of its canonical element encodings (`docs/format.md`).
    pub key_columns: Vec<String>,
    /// When set, the version compared per key is the newest by this column
    /// (a timestamp, date, time, or integer version). A null value never
    /// wins (ADR 014); a key whose rows on a side all have a null value has
    /// no comparable version there. When unset, the multiset of row hashes
    /// per key must match.
    pub latest_by: Option<String>,
    /// Columns left out of the row hash. Recorded on the certificate.
    pub ignored_columns: Vec<String>,
    /// Decimal columns summed exactly, per group.
    pub total_columns: Vec<String>,
    /// Columns that group the totals. Empty means one group.
    pub group_by: Vec<String>,
    /// How much of the key space to compare.
    pub coverage: CoverageRequest,
}

impl ReconcileSpec {
    /// A full-coverage reconcile by `key_columns`, with no version rule, no
    /// ignored columns, and no totals.
    #[must_use]
    pub fn new<I, S>(key_columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            key_columns: key_columns.into_iter().map(Into::into).collect(),
            latest_by: None,
            ignored_columns: Vec::new(),
            total_columns: Vec::new(),
            group_by: Vec::new(),
            coverage: CoverageRequest::Full,
        }
    }

    /// Compares the newest version per key by `column`.
    #[must_use]
    pub fn latest_by(mut self, column: impl Into<String>) -> Self {
        self.latest_by = Some(column.into());
        self
    }

    /// Leaves `columns` out of the row hash.
    #[must_use]
    pub fn ignore<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.ignored_columns
            .extend(columns.into_iter().map(Into::into));
        self
    }

    /// Sums `columns` exactly.
    #[must_use]
    pub fn totals<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.total_columns
            .extend(columns.into_iter().map(Into::into));
        self
    }

    /// Groups the totals by `columns`.
    #[must_use]
    pub fn group_by<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.group_by.extend(columns.into_iter().map(Into::into));
        self
    }

    /// Sets the coverage request.
    #[must_use]
    pub fn coverage(mut self, coverage: CoverageRequest) -> Self {
        self.coverage = coverage;
        self
    }

    /// Checks the rules that need no schema: non-empty and distinct column
    /// lists, a key column is never ignored, the version column is not a key
    /// column, and the coverage request is one this crate runs.
    pub(crate) fn validate(&self) -> Result<()> {
        if self.key_columns.is_empty() {
            return Err(VerifyError::InvalidSpec(
                "a reconcile needs at least one key column".into(),
            ));
        }
        distinct("key_columns", &self.key_columns)?;
        distinct("ignored_columns", &self.ignored_columns)?;
        distinct("total_columns", &self.total_columns)?;
        distinct("group_by", &self.group_by)?;
        for k in &self.key_columns {
            if self.ignored_columns.contains(k) {
                return Err(VerifyError::InvalidSpec(format!(
                    "key column {k:?} cannot be ignored: the key identifies the rows compared"
                )));
            }
        }
        if let Some(ts) = &self.latest_by {
            if self.key_columns.contains(ts) {
                return Err(VerifyError::InvalidSpec(format!(
                    "latest_by column {ts:?} is also a key column"
                )));
            }
        }
        match &self.coverage {
            CoverageRequest::Full => {}
            CoverageRequest::StaticProof { identity } => {
                return Err(VerifyError::StaticProofUnsupported {
                    identity: identity.clone(),
                })
            }
            CoverageRequest::Sampled { fraction, .. } => {
                if !(fraction.is_finite() && *fraction > 0.0 && *fraction <= 1.0) {
                    return Err(VerifyError::InvalidSpec(format!(
                        "sampling fraction {fraction} is not in (0, 1]"
                    )));
                }
            }
            CoverageRequest::Workload(w) => {
                if w.keys.is_empty() {
                    return Err(VerifyError::InvalidSpec(
                        "workload coverage with no keys would compare nothing".into(),
                    ));
                }
            }
        }
        Ok(())
    }
}

fn distinct(what: &str, columns: &[String]) -> Result<()> {
    let mut seen = BTreeSet::new();
    for c in columns {
        if !seen.insert(c) {
            return Err(VerifyError::InvalidSpec(format!(
                "{what} lists {c:?} twice"
            )));
        }
    }
    Ok(())
}

/// Tuning that does not change what is compared, only how.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Options {
    /// Bytes the comparison state may occupy at once: one pass's per-key
    /// state for both sides, the group totals, and the recorded examples,
    /// as accounted by the crate's meter (README, "Memory bound"). The pass
    /// count is chosen from row counts in the sources' statistics so the
    /// state fits; a pass that would exceed it is abandoned and the run
    /// restarts with twice the passes.
    pub memory_budget: u64,
    /// Per-key mismatches kept as examples, the smallest keys first. The
    /// counts are always exact.
    pub max_examples: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            memory_budget: DEFAULT_MEMORY_BUDGET,
            max_examples: DEFAULT_MAX_EXAMPLES,
        }
    }
}
