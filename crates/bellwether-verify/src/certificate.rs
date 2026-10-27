//! The Certificate record (ADR 004) and the only write path for it.

use std::ops::Deref;
use std::time::{Duration, Instant};

use bellwether_ledger::{Ledger, RecordKind};
use bellwether_types::{
    format, AggServed, CertBody, Certificate, Evidence, Hash, MismatchExample, MismatchKind,
    Provenance, Record, Store, MAX_MISMATCH_EXAMPLES,
};

use crate::error::{Result, VerifyError};
use crate::outcome::{Mismatch, Reconciliation};
use crate::reconcile::{Execution, Reconciler, Side};

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
    /// The manifest to anchor to: the left side's. When the left side is
    /// indexed it must name that side's sequence, or, when both sides are
    /// indexed, the right's (naming the side the certificate is for). Without
    /// one, and with no side indexed, the certificate is anchored at
    /// manifest `"0"` (contract-v3 #29).
    pub manifest: Option<ManifestAnchor>,
    /// Further inputs, cited in order after this crate's own.
    pub inputs: Vec<AgedInput>,
    /// A fixed RFC 3339 time (the facade's `--clock`): the certificate's
    /// `created_at`, and "now" for its ages, so no time elapsed during the
    /// run is added to an input's age. Two runs over the same inputs then
    /// make byte-identical certificates. `None` uses the system clock.
    pub clock: Option<String>,
    /// Agg cells (B7) the caller checked against the decoded rows
    /// ([`crate::check_agg_cells`]), each with [`AggServed::checked`] set:
    /// appended to the reconciliation's and the certificate's `agg`; both
    /// pass only when no checked cell differs.
    pub agg: Vec<AggServed>,
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
        let exec = self.execute(left, right)?;
        let created_at = match &ctx.clock {
            Some(c) => c.clone(),
            None => self.stamp()?,
        };
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
    let sequence = manifest_version(exec.manifests, ctx.manifest.as_ref())?;
    let mut inputs: Vec<Hash> = Vec::new();
    let mut oldest = Duration::ZERO;
    // Under a fixed clock "now" does not advance, so nothing elapses.
    let fixed = ctx.clock.is_some();
    let mut cite = |hash: Hash, age: Duration, at: Instant| {
        let age = if fixed {
            age
        } else {
            age + now.saturating_duration_since(at)
        };
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
    let mut outcome = exec.outcome;
    outcome.pass &= ctx
        .agg
        .iter()
        .all(|a| a.checked.as_ref().is_none_or(|c| c.differ == 0));
    outcome.agg.extend(ctx.agg.iter().cloned());
    let o = &outcome;
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
            // A key that failed to compare is counted in `uncomparable`,
            // never in `mismatched` (contract v3 #26).
            mismatched: o.counts.mismatched - o.counts.no_comparable_version,
            ignored_columns: o.ignored_columns.clone(),
            totals_delta,
            examples: examples(&o.mismatches),
            uncomparable: o.counts.no_comparable_version,
            totals: exec.totals.clone(),
            row_filter: o.row_filter.clone(),
            agg: o.agg.clone(),
        },
        provenance: Provenance {
            inputs,
            oldest_input_age: oldest,
            manifest_version: Some(sequence.to_string()),
            created_at,
        },
    };
    Ok(Certified {
        outcome,
        certificate,
        evidence: exec.evidence,
    })
}

/// The certificate's examples: the per-key differences of `mismatches` in
/// their (canonical key) order, at most [`MAX_MISMATCH_EXAMPLES`] (a run with a larger
/// `max_examples` still certifies only that many); totals are in
/// `totals_delta`, not here.
fn examples(mismatches: &[Mismatch]) -> Vec<MismatchExample> {
    mismatches
        .iter()
        .filter_map(|m| {
            let (kind, key) = match m {
                Mismatch::MissingOnLeft { key, .. } => (MismatchKind::MissingLeft, key),
                Mismatch::MissingOnRight { key, .. } => (MismatchKind::MissingRight, key),
                Mismatch::RowHashDiffers { key, .. } => (MismatchKind::Mismatched, key),
                Mismatch::NoComparableVersion { key, .. } => (MismatchKind::Uncomparable, key),
                Mismatch::TotalDiffers { .. } => return None,
            };
            Some(MismatchExample {
                kind,
                key: key.canonical.clone(),
                values: key.values.clone(),
            })
        })
        .take(MAX_MISMATCH_EXAMPLES)
        .collect()
}

/// The manifest sequence the certificate names. `indexed` holds each
/// side's manifest sequence when it is indexed. The anchor is the left
/// side's (the caller's folder): an indexed left must be at it. With the
/// left unindexed the anchor stands; the right side's index, handed for its
/// agg key (B7), is cited as an input and anchors nothing.
fn manifest_version(indexed: [Option<u64>; 2], anchor: Option<&ManifestAnchor>) -> Result<u64> {
    match (anchor, indexed) {
        (Some(a), [None, _]) => Ok(a.sequence),
        (Some(a), [Some(l), r]) if l == a.sequence || r == Some(a.sequence) => Ok(a.sequence),
        (Some(a), seqs) => Err(VerifyError::Manifest(format!(
            "the context names manifest {} but the indexed sides are at {:?}",
            a.sequence,
            seqs.iter().flatten().collect::<Vec<_>>()
        ))),
        // Neither side indexed: the never-committed manifest (#29).
        (None, [None, None]) => Ok(0),
        (None, [Some(one), None] | [None, Some(one)]) => Ok(one),
        (None, [Some(a), Some(b)]) if a == b => Ok(a),
        (None, [Some(a), Some(b)]) => Err(two_indexes(&[a, b])),
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
        assert_eq!(manifest_version([None, None], Some(&a(7))).unwrap(), 7);
        assert_eq!(manifest_version([Some(7), None], None).unwrap(), 7);
        assert_eq!(manifest_version([None, Some(7)], None).unwrap(), 7);
        assert_eq!(manifest_version([Some(7), Some(7)], None).unwrap(), 7);
        assert_eq!(
            manifest_version([Some(7), Some(3)], Some(&a(7))).unwrap(),
            7
        );
        // Two indexed sides at different manifests: the anchor names the
        // one the certificate is for (`tests/planner.rs`).
        assert_eq!(
            manifest_version([Some(3), Some(7)], Some(&a(7))).unwrap(),
            7
        );
        assert_eq!(manifest_version([None, None], None).unwrap(), 0);
        assert!(manifest_version([Some(3), Some(7)], None).is_err());
        assert!(manifest_version([Some(3), None], Some(&a(7))).is_err());
        // An unindexed left side's anchor stands beside the right side's
        // index (an agg key on the right only, B7).
        assert_eq!(manifest_version([None, Some(3)], Some(&a(7))).unwrap(), 7);
    }
}
