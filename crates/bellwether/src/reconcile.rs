//! `reconcile`: compare two folders by key (WP8) and write the Certificate.

use bellwether_types::{Error, Result};
use bellwether_verify::{
    CertificateContext, CoverageRequest, ManifestAnchor, Options, ReconcileSpec, Reconciler,
    Reconciliation, Side,
};
use serde::{Deserialize, Serialize};

use crate::engine::{guard, Engine};
use crate::results::Written;

/// How many per-key mismatches the CLI's human `reconcile` output prints.
pub const DEFAULT_DISPLAY_MISMATCHES: usize = 20;

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
    /// Memory budget for the comparison state, in bytes (`None` uses the
    /// verifier's default, 256 MiB).
    #[serde(default, skip_serializing_if = "Option::is_none")]
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

impl Engine {
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
            created_at: Some(self.now()),
        };
        let mut opts = Options::default();
        if let Some(b) = req.memory_budget {
            opts.memory_budget = b;
        }
        let certified = Reconciler::new(spec, opts)?.certify(
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
