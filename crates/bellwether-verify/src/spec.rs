//! What to reconcile: the key, the version rule, the columns compared and
//! summed, and how much of the key space to cover.

use std::collections::BTreeSet;
use std::time::Duration;

use bellwether_types::{Hash, Predicate};

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
    /// The rows a recorded workload asks for, chosen by the row filter
    /// ([`ReconcileSpec::rows_where`], required) rather than by key tuples:
    /// a workload of list-membership lookups (B4), whose runs name element
    /// values, not reconcile keys. Every key with a kept row on either side
    /// is compared; the certificate's coverage is the workload with the
    /// number of keys compared.
    WorkloadRows {
        /// Hash of the recorded workload; it names the coverage and is one
        /// of the certificate's provenance inputs.
        workload: Hash,
        /// Age of the recorded workload when the reconcile starts.
        age: Duration,
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

/// The largest scale a float total may be quantized at: 38 digits after
/// the point, the most a 128-bit decimal holds.
pub const MAX_QUANTIZE_SCALE: u8 = 38;

/// One total column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TotalColumn {
    /// The column.
    pub column: String,
    /// `None` for a decimal column, summed at its dataset scale. `Some(s)`
    /// for a float column: each value's exact binary value is rounded half
    /// to even at scale `s` (`round(v × 10^s)`), then summed exactly as an
    /// `i128` at scale `s`. A NaN or infinite value is an error naming its
    /// key; a value whose quantized integer does not fit an `i128` is an
    /// error too.
    pub quantize: Option<u8>,
}

impl TotalColumn {
    /// A decimal total column.
    #[must_use]
    pub fn decimal(column: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            quantize: None,
        }
    }

    /// A float total column quantized at `scale`.
    #[must_use]
    pub fn quantized(column: impl Into<String>, scale: u8) -> Self {
        Self {
            column: column.into(),
            quantize: Some(scale),
        }
    }

    /// Parses `col` or `col:dec(S)`, the CLI's `--totals` spelling. The
    /// column is everything before the last `:dec(`; `S` is a whole number
    /// from 0 to [`MAX_QUANTIZE_SCALE`].
    pub fn parse(given: &str) -> std::result::Result<Self, String> {
        let given = given.trim();
        let Some((column, rest)) = given.rsplit_once(":dec(") else {
            if given.is_empty() {
                return Err("--totals needs a column name".into());
            }
            return Ok(Self::decimal(given));
        };
        let scale = rest
            .strip_suffix(')')
            .and_then(|s| s.trim().parse::<u8>().ok())
            .filter(|s| *s <= MAX_QUANTIZE_SCALE);
        match (column.is_empty(), scale) {
            (false, Some(s)) => Ok(Self::quantized(column, s)),
            _ => Err(format!(
                "--totals {given:?} is not `<column>` or `<column>:dec(<scale>)` with a scale \
                 from 0 to {MAX_QUANTIZE_SCALE}; write for example volume:dec(4)"
            )),
        }
    }

    /// The spelling `parse` reads: `col`, or `col:dec(S)`. Totals and
    /// certificate labels name a quantized column this way, so the
    /// quantization is recorded with every total.
    #[must_use]
    pub fn label(&self) -> String {
        match self.quantize {
            None => self.column.clone(),
            Some(s) => format!("{}:dec({s})", self.column),
        }
    }
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
    /// Columns summed exactly, per group: decimal columns at their dataset
    /// scale, and float columns quantized at an explicit scale
    /// ([`TotalColumn::quantize`]).
    pub total_columns: Vec<TotalColumn>,
    /// Columns that group the totals. Empty means one group.
    pub group_by: Vec<String>,
    /// How much of the key space to compare.
    pub coverage: CoverageRequest,
    /// Which rows of the covered keys are compared, when not all of them:
    /// a row is compared on either side when any of these predicates keeps
    /// it. Each has the form of a lookup scan plan's residual (ADR 011),
    /// evaluated on canonical bytes at the dataset types with SQL nulls (a
    /// null never satisfies `In`, `Between`, or a listed key's
    /// `NotOlderThan`). `LatestPerKey` keeps every row here: it is the
    /// version rule, which is [`ReconcileSpec::latest_by`]. Rows no
    /// predicate keeps are read and skipped on both sides. `None` compares
    /// every row.
    pub row_filter: Option<Vec<Predicate>>,
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
            row_filter: None,
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

    /// Sums the decimal `columns` exactly, at their dataset scale.
    #[must_use]
    pub fn totals<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.total_columns
            .extend(columns.into_iter().map(|c| TotalColumn::decimal(c.into())));
        self
    }

    /// Sums total columns given as [`TotalColumn`]s: decimal, or float
    /// quantized at a scale.
    #[must_use]
    pub fn with_totals<I>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = TotalColumn>,
    {
        self.total_columns.extend(columns);
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

    /// Compares only the rows that at least one of `residuals` keeps
    /// ([`ReconcileSpec::row_filter`]).
    #[must_use]
    pub fn rows_where(mut self, residuals: Vec<Predicate>) -> Self {
        self.row_filter = Some(residuals);
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
        let names: Vec<String> = self
            .total_columns
            .iter()
            .map(|t| t.column.clone())
            .collect();
        distinct("total_columns", &names)?;
        for t in &self.total_columns {
            if t.quantize.is_some_and(|s| s > MAX_QUANTIZE_SCALE) {
                return Err(VerifyError::InvalidSpec(format!(
                    "{}: the scale must be at most {MAX_QUANTIZE_SCALE}",
                    t.label()
                )));
            }
        }
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
        if self.row_filter.as_ref().is_some_and(Vec::is_empty) {
            return Err(VerifyError::InvalidSpec(
                "a row filter with no predicate would compare nothing".into(),
            ));
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
            CoverageRequest::WorkloadRows { .. } => {
                if self.row_filter.is_none() {
                    return Err(VerifyError::InvalidSpec(
                        "workload-rows coverage chooses rows by the row filter; give one \
                         (rows_where) or the whole data would be compared under a workload's name"
                            .into(),
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
