//! `reconcile`: compare two folders by key (WP8) and write the Certificate.

use bellwether_types::{Error, Result};
use bellwether_verify::{
    CertificateContext, CoverageRequest, ManifestAnchor, Options, ReconcileSpec, Reconciliation,
    Side,
};
use serde::{Deserialize, Serialize};

use crate::engine::{guard, Engine};
use crate::results::Written;

/// How to reconcile two folders.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ReconcileRequest {
    /// Key columns, in tuple order.
    pub key: Vec<String>,
    /// Compare the newest version per key by this column (a null never
    /// wins, ADR 014).
    pub latest_by: Option<String>,
    /// Columns left out of the row hash; listed on the certificate.
    pub ignore: Vec<String>,
    /// Decimal columns totalled exactly.
    pub totals: Vec<String>,
    /// Columns grouping the totals.
    pub group_by: Vec<String>,
    /// Compare a deterministic sample of keys: `(fraction, seed)`.
    pub sample: Option<(f64, u64)>,
    /// Bytes the comparison state may occupy (the verifier's
    /// `Options::memory_budget`); `None` keeps its default, 256 MiB. A
    /// larger budget means fewer passes over both sides: the result's
    /// `outcome.memory` reports the passes and the rows read per side.
    #[serde(default)]
    pub memory_budget: Option<u64>,
}

/// What [`Engine::reconcile`] found.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReconcileResult {
    /// The Certificate (written even when it fails).
    pub certificate: Written,
    /// The typed outcome: counts, the first mismatches, totals, sides.
    pub outcome: Reconciliation,
}

/// A reconcile request the engine would refuse for what it asks, not for
/// what the data holds: a usage problem a caller can fix by changing the
/// request. Found by [`Engine::reconcile_problems`] before any row is read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReconcileProblem {
    /// A `totals` column is not a decimal. Totals are exact decimal sums and
    /// never use floats, so the column must be written as a decimal first.
    TotalNotDecimal {
        /// The column.
        column: String,
        /// Its dataset type, as Arrow prints it.
        data_type: String,
        /// Whether it is a floating-point column (the usual case: cast it to
        /// a decimal when writing).
        float: bool,
    },
    /// A `totals` column is not in the left side's dataset schema.
    TotalNotFound {
        /// The column.
        column: String,
    },
    /// The memory budget is zero.
    ZeroMemoryBudget,
}

impl Engine {
    /// The usage problems in `request`, checked against the data root's
    /// dataset schema before any row is read: every `totals` column must be
    /// a decimal column of the left side, and a memory budget must be
    /// positive. Empty when the request is well formed. `reconcile` refuses
    /// the same requests (as engine errors); a caller that tells usage from
    /// engine failures (the CLI's exit 2 against 4) asks here first.
    pub fn reconcile_problems(&self, request: &ReconcileRequest) -> Result<Vec<ReconcileProblem>> {
        guard(|| {
            let mut out = Vec::new();
            if request.memory_budget == Some(0) {
                out.push(ReconcileProblem::ZeroMemoryBudget);
            }
            if request.totals.is_empty() {
                return Ok(out);
            }
            let pinned = self.pin()?;
            let schema = self.source(pinned.as_ref())?.schema(self.root())?;
            for c in &request.totals {
                match schema.field_with_name(c) {
                    Err(_) => out.push(ReconcileProblem::TotalNotFound { column: c.clone() }),
                    Ok(f) => {
                        let dt = f.data_type();
                        if !totalable(dt) {
                            out.push(ReconcileProblem::TotalNotDecimal {
                                column: c.clone(),
                                data_type: dt.to_string(),
                                float: dt.is_floating(),
                            });
                        }
                    }
                }
            }
            Ok(out)
        })
    }

    /// Reconciles the engine's data root (left) against `right`, a folder
    /// of the same format, and writes the Certificate to this ledger. The
    /// certificate is anchored to this index's current manifest when one
    /// exists; otherwise the verifier records manifest version "0" and
    /// cites both sides' catalog digests (contract v3 entry 29).
    pub fn reconcile(&self, right: &str, request: &ReconcileRequest) -> Result<ReconcileResult> {
        guard(|| self.reconcile_inner(right, request))
    }

    fn reconcile_inner(&self, right: &str, req: &ReconcileRequest) -> Result<ReconcileResult> {
        if req.key.is_empty() {
            return Err(Error::KeySpec(
                "a reconcile compares rows by key; pass --key <columns>".into(),
            ));
        }
        let right = std::fs::canonicalize(right)
            .map_err(|e| {
                Error::Source(format!(
                    "cannot open the right side {right:?} ({e}); pass an existing folder"
                ))
            })?
            .to_string_lossy()
            .into_owned();
        let pinned = self.pin()?;
        let left_source = self.source(pinned.as_ref())?;
        let right_source = self.output_source()?;
        let mut spec = ReconcileSpec::new(req.key.clone())
            .ignore(req.ignore.clone())
            .totals(req.totals.clone())
            .group_by(req.group_by.clone());
        if let Some(l) = &req.latest_by {
            spec = spec.latest_by(l.clone());
        }
        if let Some((fraction, seed)) = req.sample {
            spec = spec.coverage(CoverageRequest::Sampled { fraction, seed });
        }
        let ctx = CertificateContext {
            proposal: None,
            manifest: pinned.as_ref().map(|p| ManifestAnchor {
                sequence: p.sequence(),
                hash: p.hash,
                age: p.age,
            }),
            inputs: Vec::new(),
        };
        let mut options = Options::default();
        if let Some(budget) = req.memory_budget {
            options.memory_budget = budget;
        }
        let certified = self.reconciler(spec, options)?.certify(
            &Side::new(&*left_source, self.root()),
            &Side::new(&*right_source, &right),
            &ctx,
        )?;
        let certificate = self.record_certified(&certified)?;
        Ok(ReconcileResult {
            certificate,
            outcome: certified.outcome,
        })
    }
}

/// Whether the verifier totals a column of type `dt`: a 128- or 256-bit
/// decimal, possibly dictionary-encoded.
fn totalable(dt: &arrow::datatypes::DataType) -> bool {
    use arrow::datatypes::DataType;
    match dt {
        DataType::Decimal128(..) | DataType::Decimal256(..) => true,
        DataType::Dictionary(_, v) => totalable(v),
        _ => false,
    }
}
