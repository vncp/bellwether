//! The Certificate record (ADR 004) and the only write path for it.

use std::ops::Deref;
use std::time::{Duration, Instant};

use bellwether_ledger::{Ledger, RecordKind};
use bellwether_types::{format, CertBody, Certificate, Evidence, Hash, Provenance, Record, Store};

use crate::error::{Result, VerifyError};
use crate::outcome::{Mismatch, Reconciliation};
use crate::reconcile::{now_rfc3339, Execution, Reconciler, Side};

/// The manifest a certificate is anchored to when no side is indexed (for
/// example a Key proposal verified by reconciling a full scan against a
/// source over the hypothetical plan): the index the proposal would change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestAnchor {
    /// The manifest sequence (ADR 020).
    pub sequence: u64,
    /// The manifest's content hash, cited as a provenance input.
    pub hash: Hash,
    /// The manifest's age when handed over (from its commit time).
    pub age: Duration,
}

/// Another input the certificate depends on, with its age when handed over.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AgedInput {
    /// Content hash of the input.
    pub hash: Hash,
    /// Its age when handed over.
    pub age: Duration,
}

/// What the caller adds to a certificate.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CertificateContext {
    /// The proposal verified, or `None` for a standalone reconcile. The
    /// ledger refuses a certificate whose proposal does not resolve.
    pub proposal: Option<Hash>,
    /// The manifest to anchor to. When a side is indexed it must name that
    /// side's sequence. Without one, and with no side indexed, the
    /// certificate is anchored at manifest `"0"` (contract-v3 #29).
    pub manifest: Option<ManifestAnchor>,
    /// Further inputs, cited in order after this crate's own.
    pub inputs: Vec<AgedInput>,
    /// Optional RFC 3339 `created_at` timestamp (for example from the facade's
    /// clock); when `None`, the current UTC time at millisecond precision is
    /// used.
    pub created_at: Option<String>,
}

/// A reconciliation with its certificate, ready to record.
#[derive(Clone, Debug, PartialEq)]
pub struct Certified {
    /// The typed result.
    pub outcome: Reconciliation,
    /// The certificate. Not yet in any ledger; see [`record`].
    pub certificate: Certificate,
    /// `plan` Evidence for each planned side, cited by the certificate's
    /// provenance; [`record`] writes them before the certificate.
    pub evidence: Vec<Evidence>,
}

impl Reconciler {
    /// Runs the reconcile and builds its certificate.
    ///
    /// The certificate's `manifest_version` is the indexed side's manifest
    /// sequence, or the context's anchor; with neither, it is `"0"`, the
    /// sequence of a manifest never committed, matching the replay crate
    /// (contract-v3 #29). Two indexed sides at different sequences with no
    /// anchor are refused before any data is read. Its provenance cites, in
    /// order and without repeats: each side's catalog digest; each indexed
    /// side's manifest hash and, for a planned side, the hash of the `plan`
    /// Evidence holding the exact plan read; the workload hash; the anchor's
    /// hash; then the context's inputs. `oldest_input_age` is the largest
    /// age among them at the moment the certificate is made, in exact
    /// nanoseconds.
    pub fn certify(
        &self,
        left: &Side<'_>,
        right: &Side<'_>,
        ctx: &CertificateContext,
    ) -> Result<Certified> {
        let handed = Instant::now();
        // Fail before reading data if two indexes disagree on the anchor.
        if ctx.manifest.is_none() {
            if let (Some(a), Some(b)) = (left.planner(), right.planner()) {
                let seqs = [a.manifest()?.sequence, b.manifest()?.sequence];
                if seqs[0] != seqs[1] {
                    return Err(two_indexes(&seqs));
                }
            }
        }
        let mut exec = self.execute(left, right)?;
        let created_at = match &ctx.created_at {
            Some(c) => c.clone(),
            None => now_rfc3339()?,
        };
        if ctx.created_at.is_some() {
            for ev in &mut exec.evidence {
                let old_hash = Record::Evidence(ev.clone()).hash()?;
                ev.provenance.created_at.clone_from(&created_at);
                let new_hash = Record::Evidence(ev.clone()).hash()?;
                for (h, _, _) in &mut exec.inputs {
                    if *h == old_hash {
                        *h = new_hash;
                    }
                }
                for s in &mut exec.outcome.sides {
                    if let crate::outcome::SidePath::Planned { plan_evidence, .. } = &mut s.path {
                        if *plan_evidence == old_hash {
                            *plan_evidence = new_hash;
                        }
                    }
                }
            }
        }
        let now = Instant::now();
        build(exec, ctx, handed, now, created_at)
    }
}

fn two_indexes(seqs: &[u64]) -> VerifyError {
    VerifyError::Manifest(format!(
        "both sides are indexed, at manifests {seqs:?}; name the one the certificate is for \
         in CertificateContext::manifest"
    ))
}

/// Builds the certificate from a finished run. `handed` is when the caller
/// handed over the context, `now` when the certificate is made.
pub(crate) fn build(
    exec: Execution,
    ctx: &CertificateContext,
    handed: Instant,
    now: Instant,
    created_at: String,
) -> Result<Certified> {
    let sequence = manifest_version(&exec.manifests, ctx.manifest.as_ref())?;
    let mut inputs: Vec<Hash> = Vec::new();
    let mut oldest = Duration::ZERO;
    let mut cite = |hash: Hash, age: Duration, at: Instant| {
        let age = age + now.saturating_duration_since(at);
        oldest = oldest.max(age);
        if !inputs.contains(&hash) {
            inputs.push(hash);
        }
    };
    for (hash, age, at) in &exec.inputs {
        cite(*hash, *age, *at);
    }
    if let Some(a) = &ctx.manifest {
        cite(a.hash, a.age, handed);
    }
    for i in &ctx.inputs {
        cite(i.hash, i.age, handed);
    }
    let o = &exec.outcome;
    let totals_delta = o
        .mismatches
        .iter()
        .filter_map(|m| match m {
            Mismatch::TotalDiffers {
                group,
                column,
                left,
                right,
            } => Some((group.label(column), left.render(), right.render())),
            _ => None,
        })
        .collect();
    let certificate = Certificate {
        record_version: format::RECORD_VERSION,
        proposal: ctx.proposal,
        coverage: o.coverage.clone(),
        pass: o.pass,
        body: CertBody {
            missing_right: o.counts.missing_right,
            missing_left: o.counts.missing_left,
            mismatched: o.counts.mismatched,
            ignored_columns: o.ignored_columns.clone(),
            totals_delta,
        },
        provenance: Provenance {
            inputs,
            oldest_input_age: oldest,
            manifest_version: Some(sequence.to_string()),
            created_at,
        },
    };
    Ok(Certified {
        outcome: exec.outcome,
        certificate,
        evidence: exec.evidence,
    })
}

/// The manifest sequence the certificate names.
fn manifest_version(indexed: &[u64], anchor: Option<&ManifestAnchor>) -> Result<u64> {
    match (anchor, indexed) {
        (Some(a), []) => Ok(a.sequence),
        (Some(a), seqs) if seqs.contains(&a.sequence) => Ok(a.sequence),
        (Some(a), seqs) => Err(VerifyError::Manifest(format!(
            "the context names manifest {} but the indexed sides are at {seqs:?}",
            a.sequence
        ))),
        // Neither side indexed: the never-committed manifest (#29).
        (None, []) => Ok(0),
        (None, [one]) => Ok(*one),
        (None, [a, b]) if a == b => Ok(*a),
        (None, seqs) => Err(two_indexes(seqs)),
    }
}

/// Writes a certified reconcile: its `plan` Evidence first, then the
/// certificate, each through [`Ledger::put`], which checks the write-time
/// rules (invariant 6). The certificate's proposal is checked to resolve
/// before anything is written, so a refusal leaves nothing behind. Returns the certificate's hash. This crate never
/// writes a Receipt; applying and attesting is the facade's.
pub fn record<S>(ledger: &Ledger<S>, certified: &Certified) -> Result<Hash>
where
    S: Deref,
    S::Target: Store,
{
    // The ledger checks the certificate's proposal only when the
    // certificate is written, after its plan Evidence; check it first so a
    // refusal writes nothing.
    if let Some(proposal) = &certified.certificate.proposal {
        match ledger.kind(proposal) {
            Ok(RecordKind::Proposal) => {}
            Ok(other) => {
                return Err(VerifyError::Engine(bellwether_types::Error::LedgerRule(
                    format!(
                        "certificate's proposal {proposal} resolves to {}, not a proposal",
                        other.with_article()
                    ),
                )))
            }
            Err(bellwether_types::Error::NotFound(_)) => {
                return Err(VerifyError::Engine(bellwether_types::Error::LedgerRule(
                    format!("certificate's proposal {proposal} does not resolve"),
                )))
            }
            Err(e) => return Err(e.into()),
        }
    }
    for ev in &certified.evidence {
        let record = Record::Evidence(ev.clone());
        let expected = record.hash()?;
        let written = ledger.put(&record)?;
        if written != expected {
            return Err(VerifyError::Engine(bellwether_types::Error::LedgerRule(
                format!("plan evidence was stored as {written}, not the cited {expected}"),
            )));
        }
    }
    Ok(ledger.put(&Record::Certificate(certified.certificate.clone()))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_version_needs_one_unambiguous_anchor() {
        let a = |sequence| ManifestAnchor {
            sequence,
            hash: Hash::of(b"m"),
            age: Duration::ZERO,
        };
        assert_eq!(manifest_version(&[], Some(&a(7))).unwrap(), 7);
        assert_eq!(manifest_version(&[7], None).unwrap(), 7);
        assert_eq!(manifest_version(&[7, 7], None).unwrap(), 7);
        assert_eq!(manifest_version(&[3, 7], Some(&a(7))).unwrap(), 7);
        assert_eq!(manifest_version(&[], None).unwrap(), 0);
        assert!(manifest_version(&[3, 7], None).is_err());
        assert!(manifest_version(&[3], Some(&a(7))).is_err());
    }
}
