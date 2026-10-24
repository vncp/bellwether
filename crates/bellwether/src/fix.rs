//! `fix`: the engine side of the loop, in this order and no other.
//!
//! 1. **Score.** Replay the recorded workload under the proposal (WP6),
//!    computed here immediately before the Replay is written; the facade
//!    never accepts a caller's cost.
//! 2. **Verify**, against the current manifest (`manifest_before`). A Key
//!    proposal: reconcile (WP8) the full scan, restricted to the workload's
//!    keys, against a source exposing only the row groups the hypothetical
//!    plan selects (coverage `Workload`). A Rewrite: reconcile the before and
//!    after outputs the caller names (coverage `Full`). A Layout proposal is
//!    refused: layout apply arrives with evolve.
//! 3. **Apply**, only if the certificate passed and was computed against the
//!    current manifest sequence: build the key (WP4) in one commit
//!    (`manifest_after`).
//! 4. **Attest**: `Ledger::put_receipt` links evidence, proposal, replay,
//!    and certificate, with `manifest_before < manifest_after`.
//!
//! A failing certificate writes no Receipt and returns a typed refusal
//! naming the mismatch counts.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use bellwether_build::{BuildOptions, Builder};
use bellwether_replay::{proposed_specs, Replayer, Workload};
use bellwether_store::{Cache, ObjectStoreBackend};
use bellwether_types::{
    format, Certificate, Coverage, Error, Evidence, EvidenceBody, FileFingerprint, Hash, KeySpec,
    Proposal, ProposalBody, Provenance, Receipt, Record, Result, RowGroupMeta, ScanEntry, ScanPlan,
    Source,
};
use bellwether_verify::{
    AgedInput, CertificateContext, CoverageRequest, ManifestAnchor, Options, ReconcileSpec,
    Reconciler, Reconciliation, Side, WorkloadKeys,
};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use crate::engine::{folder_changes, guard, Engine, Pinned};
use crate::index::stale_index;
use crate::results::{total, Cost, Written};

/// The two outputs a Rewrite proposal is verified on, and how to compare
/// them.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RewriteOutputs {
    /// The output before the rewrite (a folder of files).
    pub before: String,
    /// The output after it.
    pub after: String,
    /// Key columns the outputs are compared by.
    pub key: Vec<String>,
    /// Compare the newest version per key by this column.
    pub latest_by: Option<String>,
    /// Columns left out of the row hash (listed on the certificate).
    pub ignore: Vec<String>,
    /// Decimal columns totalled exactly.
    pub totals: Vec<String>,
    /// Columns grouping the totals.
    pub group_by: Vec<String>,
}

/// What `fix` is asked to do.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixRequest {
    /// Score, verify, apply, and attest this proposal.
    pub proposal: Option<Hash>,
    /// Or: apply and attest a certificate the engine wrote earlier (the
    /// `apply.*` tier, which takes a certificate id only). It is re-checked
    /// (passing, current manifest) before anything is written, and the
    /// proposal is re-scored immediately before its Replay is written.
    pub certificate: Option<Hash>,
    /// The recorded workload a Key proposal is scored on (`why` prints it).
    pub workload: Option<Hash>,
    /// The outputs a Rewrite proposal is verified on.
    pub rewrite: Option<RewriteOutputs>,
    /// Stop after the certificate: score and verify, apply nothing.
    pub verify_only: bool,
}

/// The score stage's outcome.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Score {
    /// A Replay computed and written now.
    Replayed {
        /// The Replay.
        replay: Box<Written>,
        /// Summed over runs, under the current configuration.
        current: Cost,
        /// Summed over runs, under the proposal.
        proposed: Cost,
    },
    /// The proposal kind has no defined replay.
    NotScorable {
        /// Why.
        reason: String,
    },
}

/// Why `fix` stopped without a receipt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Refusal {
    /// The certificate found differences (exit 1).
    CertificateFailed {
        /// The failing certificate (stored; a failing certificate is a
        /// legitimate record).
        certificate: Hash,
        /// Keys on both sides whose compared versions differ, or that could
        /// not be compared.
        mismatched: u64,
        /// Keys on the right only.
        missing_left: u64,
        /// Keys on the left only.
        missing_right: u64,
        /// Group totals that differ.
        totals_differ: u64,
    },
    /// The certificate was computed against another manifest than the
    /// current one; nothing was applied (ADR 020).
    StaleManifest {
        /// The certificate.
        certificate: Hash,
        /// The sequence it was computed against.
        certified: String,
        /// The current sequence.
        current: String,
    },
    /// Another commit landed between the manifest check and the apply's
    /// commit, so the applied index is not the one the certificate
    /// verified; the receipt is withheld. The build crate has no
    /// expected-base precondition to prevent this.
    ConcurrentCommit {
        /// The sequence the apply should have produced.
        expected: u64,
        /// The sequence it produced.
        committed: u64,
    },
    /// The data root changed after the certificate was computed: its
    /// catalog digest is not the certificate's left-side digest, so the
    /// certificate no longer describes the data. Run `fix` again to
    /// re-verify.
    DataChanged {
        /// The certificate.
        certificate: Hash,
        /// The left-side catalog digest the certificate cites.
        certified: Option<Hash>,
        /// The data root's catalog digest now.
        current: Hash,
    },
    /// Applying the key changed nothing (it is already complete), and a
    /// Key proposal must advance the manifest to be attested.
    NothingToApply {
        /// The unchanged sequence.
        sequence: String,
    },
    /// A Layout proposal: layout apply arrives with evolve.
    LayoutNotApplicable,
    /// A passing Rewrite cannot be attested yet: a receipt cites a Replay
    /// and the replay crate defines none for rewrites (contract v3
    /// backlog #2).
    NoReplay {
        /// The passing certificate.
        certificate: Hash,
    },
}

/// The apply stage's outcome.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FixOutcome {
    /// Applied and attested.
    Applied {
        /// The Receipt.
        receipt: Box<Written>,
        /// The proposal replayed at `manifest_after` right after the apply:
        /// the realized cost, which `report` pairs with the receipt's
        /// predicted one. `None` for a proposal kind with no replay.
        realized: Option<Box<Written>>,
        /// Set when the realized replay could not be computed after the
        /// receipt was written; the receipt stands.
        warning: Option<String>,
    },
    /// Verified; `verify_only` was set, nothing applied.
    Verified,
    /// Stopped without a receipt.
    Refused {
        /// Why.
        refusal: Refusal,
    },
}

/// What [`Engine::fix`] did, stage by stage.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FixResult {
    /// The proposal.
    pub proposal: Written,
    /// The manifest sequence scored and verified against.
    pub manifest_before: String,
    /// Stage 1.
    pub score: Option<Score>,
    /// The hypothetical plan's row groups (a `plan` Evidence), which the
    /// certificate's right side read, for a Key proposal.
    pub plan: Option<Written>,
    /// Stage 2: the certificate (stored even when it fails).
    pub certificate: Option<Written>,
    /// For a Rewrite: the invariant the reconcile was run to check.
    pub invariant: Option<String>,
    /// The verify stage's typed outcome: exact counts, the first
    /// mismatches, and what each side read (its files, row groups, and
    /// catalog digest).
    pub verification: Option<Reconciliation>,
    /// Stages 3 and 4.
    pub outcome: FixOutcome,
}

impl Engine {
    /// Runs score → verify → apply → attest for `request.proposal`, or
    /// apply → attest for an earlier `request.certificate` (module docs).
    pub fn fix(&self, request: &FixRequest) -> Result<FixResult> {
        guard(|| match (&request.proposal, &request.certificate) {
            (Some(p), None) => self.fix_proposal(p, request),
            (None, Some(c)) => self.apply_certificate(c),
            _ => Err(Error::Other(
                "fix takes a proposal (--proposal) or a certificate to apply (--certificate), \
                 not both"
                    .into(),
            )),
        })
    }

    fn proposal(&self, hash: &Hash) -> Result<(Written, Proposal)> {
        let w = self.get(hash).map_err(|e| match e {
            Error::NotFound(h) => Error::Other(format!(
                "proposal {h} is not in the ledger at {}; pass a hash `bellwether propose` or \
                 `bellwether why` printed",
                self.index_location()
            )),
            other => other,
        })?;
        match &w.record {
            Record::Proposal(p) => {
                let p = p.clone();
                Ok((w, p))
            }
            _ => Err(Error::LedgerRule(format!(
                "{hash} is {}, not a proposal; pass a proposal hash",
                w.kind().with_article()
            ))),
        }
    }

    // The three proposal kinds, each a short linear sequence of stages.
    #[allow(clippy::too_many_lines)]
    fn fix_proposal(&self, hash: &Hash, req: &FixRequest) -> Result<FixResult> {
        let (written, proposal) = self.proposal(hash)?;
        let pinned = self.pin()?;
        if let Some(p) = &pinned {
            // A stale index is refused with what to do, before any work.
            let changes = folder_changes(self.root(), p, self.format())?;
            if !changes.is_empty() {
                return Err(stale_index(self.root(), "fix", &changes));
            }
        }
        let before = pinned.as_ref().map_or(0, Pinned::sequence);
        let mut result = FixResult {
            proposal: written,
            manifest_before: before.to_string(),
            score: None,
            plan: None,
            certificate: None,
            invariant: None,
            verification: None,
            outcome: FixOutcome::Verified,
        };
        match &proposal.body {
            ProposalBody::Layout { .. } => {
                result.outcome = refused(Refusal::LayoutNotApplicable);
                Ok(result)
            }
            ProposalBody::Key { spec } => {
                let workload_hash = req.workload.ok_or_else(|| {
                    Error::Other(
                        "a key proposal is scored on a recorded workload; pass --workload <hash> \
                         (bellwether why prints it)"
                            .into(),
                    )
                })?;
                let source = self.source(pinned.as_ref())?;
                let replayer = replayer(self, &*source, pinned.as_ref())?;
                let cache = self.cache()?;
                let workload = load_workload(self, &cache, &workload_hash)?;
                // 1. Score.
                let replay = self.score(&replayer, hash, &workload, &cache)?;
                let replay_hash = replay.0.hash;
                result.score = Some(Score::Replayed {
                    replay: Box::new(replay.0),
                    current: replay.1,
                    proposed: replay.2,
                });
                // 2. Verify.
                let (plan, cert, verification) = self.verify_key(
                    hash,
                    spec,
                    &replayer,
                    &*source,
                    &workload,
                    &cache,
                    pinned.as_ref(),
                )?;
                result.plan = Some(plan);
                result.verification = Some(verification);
                let certificate = cert_of(&cert)?.clone();
                result.certificate = Some(cert.clone());
                if let Some(r) = failed(&cert.hash, &certificate) {
                    result.outcome = refused(r);
                    return Ok(result);
                }
                if req.verify_only {
                    return Ok(result);
                }
                // 3 and 4, behind the trust-boundary gate.
                result.outcome = match self.gate(&cert.hash, &certificate, true)? {
                    Err(r) => refused(r),
                    Ok(before) => self.apply_and_attest(
                        &proposal,
                        hash,
                        spec,
                        &replay_hash,
                        &cert.hash,
                        before,
                        &workload,
                    )?,
                };
                Ok(result)
            }
            ProposalBody::Rewrite { shape } => {
                result.score = Some(Score::NotScorable {
                    reason: "the replay crate defines no replay for rewrite proposals (contract \
                             v3 backlog #2)"
                        .into(),
                });
                result.invariant = Some(shape.invariant.clone());
                let outputs = req.rewrite.as_ref().ok_or_else(|| {
                    Error::Other(
                        "a rewrite proposal is verified on the outputs before and after it; pass \
                         --before-output <dir> --after-output <dir> --key <cols>"
                            .into(),
                    )
                })?;
                let (cert, verification) = self.verify_rewrite(hash, outputs, pinned.as_ref())?;
                result.verification = Some(verification);
                let certificate = cert_of(&cert)?.clone();
                result.certificate = Some(cert.clone());
                result.outcome = match failed(&cert.hash, &certificate) {
                    Some(r) => refused(r),
                    None if req.verify_only => FixOutcome::Verified,
                    None => refused(Refusal::NoReplay {
                        certificate: cert.hash,
                    }),
                };
                Ok(result)
            }
        }
    }

    /// Apply and attest a certificate written earlier: every check runs
    /// before anything is written, and the replay is recomputed now.
    fn apply_certificate(&self, cert_hash: &Hash) -> Result<FixResult> {
        let w = self.get(cert_hash)?;
        let Record::Certificate(certificate) = &w.record else {
            return Err(Error::LedgerRule(format!(
                "{cert_hash} is {}, not a certificate; pass the certificate hash `bellwether fix \
                 --verify-only` printed",
                w.kind().with_article()
            )));
        };
        let certificate = certificate.clone();
        let proposal_hash = certificate.proposal.ok_or_else(|| {
            Error::LedgerRule(format!(
                "certificate {cert_hash} verified no proposal (a standalone reconcile), so there \
                 is nothing to apply"
            ))
        })?;
        let (written, proposal) = self.proposal(&proposal_hash)?;
        let certified = certificate
            .provenance
            .manifest_version
            .clone()
            .unwrap_or_default();
        let mut result = FixResult {
            proposal: written,
            manifest_before: certified.clone(),
            score: None,
            plan: None,
            certificate: Some(w.clone()),
            invariant: None,
            verification: None,
            outcome: FixOutcome::Verified,
        };
        // The gate runs before anything is written; for a key proposal it
        // also checks that the data is still the data certified.
        let is_key = matches!(proposal.body, ProposalBody::Key { .. });
        let before = match self.gate(cert_hash, &certificate, is_key)? {
            Err(r) => {
                result.outcome = refused(r);
                return Ok(result);
            }
            Ok(before) => before,
        };
        match &proposal.body {
            ProposalBody::Layout { .. } => {
                result.outcome = refused(Refusal::LayoutNotApplicable);
            }
            ProposalBody::Rewrite { shape } => {
                result.invariant = Some(shape.invariant.clone());
                result.outcome = refused(Refusal::NoReplay {
                    certificate: *cert_hash,
                });
            }
            ProposalBody::Key { spec } => {
                let Coverage::Workload { workload, .. } = &certificate.coverage else {
                    return Err(Error::LedgerRule(format!(
                        "certificate {cert_hash} does not cover a recorded workload, so the key \
                         proposal it verified cannot be re-scored on the keys it checked"
                    )));
                };
                let pinned = self.pin()?;
                let source = self.source(pinned.as_ref())?;
                let replayer = replayer(self, &*source, pinned.as_ref())?;
                let cache = self.cache()?;
                let workload = load_workload(self, &cache, workload)?;
                let (replay, current_cost, proposed_cost) =
                    self.score(&replayer, &proposal_hash, &workload, &cache)?;
                let replay_hash = replay.hash;
                result.score = Some(Score::Replayed {
                    replay: Box::new(replay),
                    current: current_cost,
                    proposed: proposed_cost,
                });
                result.outcome = self.apply_and_attest(
                    &proposal,
                    &proposal_hash,
                    spec,
                    &replay_hash,
                    cert_hash,
                    before,
                    &workload,
                )?;
            }
        }
        Ok(result)
    }

    /// Stage 1: the replay, computed now and written now.
    fn score(
        &self,
        replayer: &Replayer<'_>,
        proposal: &Hash,
        workload: &Workload,
        cache: &Cache<ObjectStoreBackend>,
    ) -> Result<(Written, Cost, Cost)> {
        let (hash, scored) = replayer
            .record(&self.ledger()?, proposal, workload, cache, &self.now())
            .map_err(Error::from)?;
        let current = total(&scored.replay.current);
        let proposed = total(&scored.replay.proposed);
        Ok((
            Written {
                hash,
                record: Record::Replay(scored.replay),
            },
            current,
            proposed,
        ))
    }

    /// Stage 2 for a Key proposal.
    #[allow(clippy::too_many_arguments)]
    fn verify_key(
        &self,
        proposal: &Hash,
        spec: &KeySpec,
        replayer: &Replayer<'_>,
        source: &dyn Source,
        workload: &Workload,
        cache: &Cache<ObjectStoreBackend>,
        pinned: Option<&Pinned>,
    ) -> Result<(Written, Written, Reconciliation)> {
        let specs = proposed_specs(replayer.current_specs(), spec);
        let runs = workload.runs();
        let columns = runs
            .first()
            .map(|r| r.columns.clone())
            .ok_or_else(|| Error::Other("the workload records no run, so there is nothing to verify; record keys with `bellwether why --from`".into()))?;
        if runs.iter().any(|r| r.columns != columns) {
            return Err(Error::Other(
                "the workload's runs look up different key columns; fix verifies one key-column \
                 set at a time, so record a workload whose runs share their key columns"
                    .into(),
            ));
        }
        // The hypothetical plan: per run, the plan under the current specs
        // plus the proposed one (transient segments for the latter), united.
        let mut selected: BTreeMap<String, (FileFingerprint, RoaringBitmap)> = BTreeMap::new();
        let mut keys: Vec<Vec<u8>> = Vec::new();
        for run in runs {
            let rk = workload.run_keys(cache, run)?;
            let plan = replayer.plan_of_keys(&specs, &rk)?;
            for e in plan.entries {
                selected
                    .entry(e.file.path.clone())
                    .or_insert_with(|| (e.file.clone(), RoaringBitmap::new()))
                    .1 |= &e.row_groups;
            }
            // Both encodings of a float zero, as the planner probes them:
            // SQL equality matches a row stored under either zero.
            let all: Vec<usize> = (0..rk.columns().len()).collect();
            keys.extend(rk.all_probes(&all));
        }
        keys.sort_unstable();
        keys.dedup();
        #[cfg(test)]
        test_seam::apply(&mut selected);
        let union = union_plan(replayer, &selected)?;
        let plan_evidence = self.put(Record::Evidence(Evidence {
            record_version: format::RECORD_VERSION,
            provenance: Provenance {
                inputs: vec![replayer.catalog().digest(), workload.hash()],
                oldest_input_age: Duration::ZERO,
                manifest_version: Some(replayer.manifest_version()),
                created_at: self.now(),
            },
            body: EvidenceBody::Plan { plan: union },
        }))?;
        let planned = PlannedSource {
            inner: source,
            selected,
            schema: SchemaRef::clone(replayer.schema()),
        };
        let workload_age = cache
            .get_cached(&workload.hash())?
            .map_or(Duration::ZERO, |(_, age)| age);
        let spec = ReconcileSpec::new(columns).coverage(CoverageRequest::Workload(WorkloadKeys {
            workload: workload.hash(),
            age: workload_age,
            keys,
        }));
        let ctx = CertificateContext {
            proposal: Some(*proposal),
            manifest: pinned.map(anchor),
            inputs: vec![AgedInput {
                hash: plan_evidence.hash,
                age: Duration::ZERO,
            }],
        };
        let root = self.root();
        let left = Side::new(source, root);
        let right = Side::new(&planned, root);
        let certified = Reconciler::new(spec, Options::default())?.certify(&left, &right, &ctx)?;
        let cert = self.record_certified(&certified)?;
        Ok((plan_evidence, cert, certified.outcome))
    }

    /// Stage 2 for a Rewrite proposal: the caller's before and after outputs.
    fn verify_rewrite(
        &self,
        proposal: &Hash,
        outputs: &RewriteOutputs,
        pinned: Option<&Pinned>,
    ) -> Result<(Written, Reconciliation)> {
        let before = canonical_dir(&outputs.before)?;
        let after = canonical_dir(&outputs.after)?;
        let left_source = self.output_source()?;
        let right_source = self.output_source()?;
        let mut spec = ReconcileSpec::new(outputs.key.clone())
            .ignore(outputs.ignore.clone())
            .totals(outputs.totals.clone())
            .group_by(outputs.group_by.clone());
        if let Some(l) = &outputs.latest_by {
            spec = spec.latest_by(l.clone());
        }
        let ctx = CertificateContext {
            proposal: Some(*proposal),
            manifest: pinned.map(anchor),
            inputs: Vec::new(),
        };
        let certified = Reconciler::new(spec, Options::default())?.certify(
            &Side::new(&*left_source, &before),
            &Side::new(&*right_source, &after),
            &ctx,
        )?;
        let cert = self.record_certified(&certified)?;
        Ok((cert, certified.outcome))
    }

    /// A source for an output folder: the engine's format, schema inferred.
    pub(crate) fn output_source(&self) -> Result<Box<dyn Source>> {
        self.source(None)
    }

    pub(crate) fn record_certified(
        &self,
        certified: &bellwether_verify::Certified,
    ) -> Result<Written> {
        let hash = bellwether_verify::record(&self.ledger()?, certified)?;
        Ok(Written {
            hash,
            record: Record::Certificate(certified.certificate.clone()),
        })
    }

    /// The trust-boundary gate (threat model, ADR 020): the certificate
    /// passed and was computed against the current manifest sequence.
    /// Returns that sequence (`manifest_before`), or the refusal. Called
    /// once per path, before any write of the apply stage.
    fn gate(
        &self,
        cert_hash: &Hash,
        certificate: &Certificate,
        check_data: bool,
    ) -> Result<std::result::Result<u64, Refusal>> {
        if let Some(r) = failed(cert_hash, certificate) {
            return Ok(Err(r));
        }
        let certified = certificate
            .provenance
            .manifest_version
            .clone()
            .unwrap_or_default();
        let current = self.manifest()?.map_or(0, |m| m.sequence);
        if certified != current.to_string() {
            return Ok(Err(Refusal::StaleManifest {
                certificate: *cert_hash,
                certified,
                current: current.to_string(),
            }));
        }
        if check_data {
            // The certificate describes the data it read; a sequence check
            // alone misses a file that landed or changed without a commit.
            let pinned = self.pin()?;
            let now = data_digest(self.root(), &*self.source(pinned.as_ref())?)?;
            let certified = certificate.provenance.inputs.first().copied();
            if certified != Some(now) {
                return Ok(Err(Refusal::DataChanged {
                    certificate: *cert_hash,
                    certified,
                    current: now,
                }));
            }
        }
        Ok(Ok(current))
    }

    /// The proposal replayed at the current manifest (`manifest_after`, just
    /// committed), computed and written now: the realized cost. The current
    /// configuration now includes the applied key, so its `current` costs
    /// are what the receipt's replay predicted as `proposed`.
    fn realize(&self, proposal: &Hash, workload: &Workload) -> Result<Option<Box<Written>>> {
        let pinned = self.pin()?;
        let source = self.source(pinned.as_ref())?;
        let replayer = replayer(self, &*source, pinned.as_ref())?;
        let cache = self.cache()?;
        let (replay, _, _) = self.score(&replayer, proposal, workload, &cache)?;
        Ok(Some(Box::new(replay)))
    }

    /// Stages 3 and 4, after the gate: build the key in one commit, which
    /// must be exactly `before + 1`, then write the receipt.
    #[allow(clippy::too_many_arguments)]
    fn apply_and_attest(
        &self,
        proposal: &Proposal,
        proposal_hash: &Hash,
        spec: &KeySpec,
        replay: &Hash,
        cert_hash: &Hash,
        before: u64,
        workload: &Workload,
    ) -> Result<FixOutcome> {
        let mut options = BuildOptions::new(self.format().kind());
        options.chunk_size = self.options().chunk_size;
        options.schema.clone_from(&self.options().schema);
        options.fingerprint_policy = self.options().fingerprint_policy;
        let report = Builder::new(Arc::clone(self.store()?), "").build(
            self.root(),
            std::slice::from_ref(spec),
            &options,
        )?;
        let after = match advance(before, report.sequence) {
            Ok(after) => after,
            Err(refusal) => return Ok(refused(refusal)),
        };
        let receipt = Receipt {
            record_version: format::RECORD_VERSION,
            evidence: proposal.evidence.clone(),
            proposal: *proposal_hash,
            replay: *replay,
            certificate: *cert_hash,
            manifest_before: before.to_string(),
            manifest_after: after.to_string(),
            created_at: self.now(),
        };
        let now = self.manifest()?.map_or(0, |m| m.sequence);
        let hash = self.ledger()?.put_receipt(&receipt, &now.to_string())?;
        // After the apply: the same proposal replayed at manifest_after, the
        // realized cost `report` sets beside the predicted one.
        let (realized, warning) = match self.realize(proposal_hash, workload) {
            Ok(r) => (r, None),
            Err(e) => (
                None,
                Some(format!(
                    "the receipt stands, but its realized replay could not be computed ({e}); run \
                     `bellwether why --workload <hash>` to compute the realized cost"
                )),
            ),
        };
        Ok(FixOutcome::Applied {
            receipt: Box::new(Written {
                hash,
                record: Record::Receipt(receipt),
            }),
            realized,
            warning,
        })
    }
}

/// The data root's catalog digest as the verifier computes a scanned side's
/// (`bellwether_source::catalog::unindexed` over every discovered file and
/// row group, with core's type spelling): what a certificate's left side
/// cites.
fn data_digest(root: &str, source: &dyn Source) -> Result<Hash> {
    let schema = source.schema(root)?;
    let spelled = bellwether_source::catalog::schema_identity(&schema, |t| {
        bellwether_core::segment::ColumnType::of(t)
            .ok()
            .and_then(|c| serde_json::to_string(&c).ok())
    });
    let files = source
        .discover(root)?
        .into_iter()
        .map(|fp| {
            let metas = source.row_groups(&fp)?;
            Ok((fp, metas))
        })
        .collect::<Result<Vec<_>>>()?;
    bellwether_source::catalog::unindexed(&spelled, &files)
}

/// A test-only seam: lets the crate's own tests make the hypothetical plan
/// incomplete, to prove the verify stage fails then.
#[cfg(test)]
pub(crate) mod test_seam {
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    use bellwether_types::FileFingerprint;
    use roaring::RoaringBitmap;

    type Selected = BTreeMap<String, (FileFingerprint, RoaringBitmap)>;
    type Hook = Box<dyn Fn(&mut Selected)>;

    thread_local! {
        static HOOK: RefCell<Option<Hook>> = RefCell::new(None);
    }

    /// Installs `hook` for this thread until `clear`.
    pub(crate) fn set(hook: impl Fn(&mut Selected) + 'static) {
        HOOK.with(|h| *h.borrow_mut() = Some(Box::new(hook)));
    }

    pub(crate) fn clear() {
        HOOK.with(|h| *h.borrow_mut() = None);
    }

    pub(crate) fn apply(selected: &mut Selected) {
        HOOK.with(|h| {
            if let Some(f) = h.borrow().as_ref() {
                f(selected);
            }
        });
    }
}

/// The sequence a Key apply produced, or the refusal: nothing committed (the
/// key was already complete), or another commit landed first. A Key
/// proposal must advance the manifest by exactly the apply's own commit.
fn advance(before: u64, committed: Option<u64>) -> std::result::Result<u64, Refusal> {
    let after = committed.unwrap_or(before);
    if after <= before {
        return Err(Refusal::NothingToApply {
            sequence: before.to_string(),
        });
    }
    if after != before + 1 {
        return Err(Refusal::ConcurrentCommit {
            expected: before + 1,
            committed: after,
        });
    }
    Ok(after)
}

fn refused(refusal: Refusal) -> FixOutcome {
    FixOutcome::Refused { refusal }
}

fn cert_of(w: &Written) -> Result<&Certificate> {
    match &w.record {
        Record::Certificate(c) => Ok(c),
        _ => Err(Error::Other(
            "verify returned a record that is not a certificate".into(),
        )),
    }
}

/// The refusal for a certificate that did not pass, naming its counts.
fn failed(hash: &Hash, c: &Certificate) -> Option<Refusal> {
    (!c.pass).then_some(Refusal::CertificateFailed {
        certificate: *hash,
        mismatched: c.body.mismatched,
        missing_left: c.body.missing_left,
        missing_right: c.body.missing_right,
        totals_differ: c.body.totals_delta.len() as u64,
    })
}

/// The manifest anchor for a certificate when an index exists: its sequence,
/// the BLAKE3 of its bytes as stored, and its age. With no index, no anchor:
/// the verifier then records manifest version "0" and cites both sides'
/// catalog digests (contract v3 entry 29).
fn anchor(p: &Pinned) -> ManifestAnchor {
    ManifestAnchor {
        sequence: p.sequence(),
        hash: p.hash,
        age: p.age,
    }
}

fn replayer<'a>(
    engine: &Engine,
    source: &'a dyn Source,
    pinned: Option<&Pinned>,
) -> Result<Replayer<'a>> {
    match pinned {
        Some(p) => Replayer::for_snapshot(source, &p.snapshot),
        None => Replayer::unindexed(source, engine.root()),
    }
}

fn load_workload(
    engine: &Engine,
    cache: &Cache<ObjectStoreBackend>,
    hash: &Hash,
) -> Result<Workload> {
    Workload::load(cache, hash).map_err(|e| {
        Error::Other(format!(
            "workload {hash} is not in the cache at {} ({e}); record it with `bellwether why \
             --from <keys.parquet>`",
            engine.index_location()
        ))
    })
}

fn canonical_dir(p: &str) -> Result<String> {
    let c = std::fs::canonicalize(p).map_err(|e| {
        Error::Source(format!(
            "cannot open the output {p:?} ({e}); pass an existing folder"
        ))
    })?;
    c.to_str()
        .map(ToString::to_string)
        .ok_or_else(|| Error::Source(format!("{} is not valid UTF-8", c.display())))
}

/// The united hypothetical plan as a `ScanPlan`, with each entry's bytes
/// summed from the catalog and checked by it.
fn union_plan(
    replayer: &Replayer<'_>,
    selected: &BTreeMap<String, (FileFingerprint, RoaringBitmap)>,
) -> Result<ScanPlan> {
    let catalog = replayer.catalog();
    let mut bytes_of: BTreeMap<(&str, u32), u64> = BTreeMap::new();
    for rg in catalog.row_groups() {
        bytes_of.insert(
            (rg.meta.file.path.as_str(), rg.meta.index_in_file),
            rg.meta.bytes,
        );
    }
    let mut entries = Vec::with_capacity(selected.len());
    let (mut bytes, mut count) = (0u64, 0u64);
    for (path, (fp, rgs)) in selected {
        let mut b = 0u64;
        for i in rgs {
            b = b
                .checked_add(*bytes_of.get(&(path.as_str(), i)).ok_or_else(|| {
                    Error::Other(format!(
                        "the plan names row group {i} of {path}, which is not live"
                    ))
                })?)
                .ok_or_else(|| Error::Other("plan bytes overflow u64".into()))?;
        }
        bytes = bytes.saturating_add(b);
        count += rgs.len();
        entries.push(ScanEntry {
            file: fp.clone(),
            row_groups: rgs.clone(),
            bytes: b,
        });
    }
    let plan = ScanPlan {
        manifest_version: replayer.manifest_version(),
        entries,
        residual: None,
        bytes,
        row_groups_total: catalog.len() as u64,
        row_groups_selected: count,
    };
    catalog.verify_plan(&plan)?;
    Ok(plan)
}

/// A source exposing only the row groups a plan selects: the right side of
/// a Key proposal's reconcile. Schema, fingerprints, row counts, and
/// positions are the inner source's own, so the verifier sees the same data
/// restricted to the plan.
///
/// The verifier's source contract lists a file's row groups densely by
/// position, so a row group the plan does not select is still listed, at
/// its position, with zero rows, zero bytes, and no statistics, and reads
/// as an empty batch: exactly what the planned subset holds there.
struct PlannedSource<'a> {
    inner: &'a dyn Source,
    selected: BTreeMap<String, (FileFingerprint, RoaringBitmap)>,
    /// The dataset schema, for the empty batches of unselected row groups.
    schema: SchemaRef,
}

impl PlannedSource<'_> {
    fn selection(&self, file: &FileFingerprint) -> Result<&RoaringBitmap> {
        match self.selected.get(&file.path) {
            Some((fp, rgs)) if fp == file => Ok(rgs),
            _ => Err(Error::Source(format!(
                "{} is not in the hypothetical plan",
                file.path
            ))),
        }
    }
}

impl Source for PlannedSource<'_> {
    fn discover(&self, root: &str) -> Result<Vec<FileFingerprint>> {
        let found = self.inner.discover(root)?;
        let mut out = Vec::with_capacity(self.selected.len());
        for (path, (fp, _)) in &self.selected {
            if !found.contains(fp) {
                return Err(Error::Source(format!(
                    "{path} changed since the plan was made (fingerprint drift, ADR 013); run fix \
                     again"
                )));
            }
            out.push(fp.clone());
        }
        Ok(out)
    }

    fn row_groups(&self, file: &FileFingerprint) -> Result<Vec<RowGroupMeta>> {
        let rgs = self.selection(file)?;
        Ok(self
            .inner
            .row_groups(file)?
            .into_iter()
            .map(|mut m| {
                if !rgs.contains(m.index_in_file) {
                    m.rows = 0;
                    m.bytes = 0;
                    for c in &mut m.stats {
                        c.min = None;
                        c.max = None;
                        c.null_count = Some(0);
                        c.distinct_count = Some(0);
                        c.has_bloom = false;
                        c.bytes = Some(0);
                    }
                }
                m
            })
            .collect())
    }

    fn read_columns(&self, rg: &RowGroupMeta, columns: &[String]) -> Result<RecordBatch> {
        if self.selection(&rg.file)?.contains(rg.index_in_file) {
            return self.inner.read_columns(rg, columns);
        }
        let fields = columns
            .iter()
            .map(|c| {
                self.schema
                    .field_with_name(c)
                    .cloned()
                    .map_err(|_| Error::KeySpec(format!("column {c:?} is not in the dataset")))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(RecordBatch::new_empty(std::sync::Arc::new(
            arrow::datatypes::Schema::new(fields),
        )))
    }

    fn schema(&self, root: &str) -> Result<SchemaRef> {
        self.inner.schema(root)
    }
}

/// The integration tests' corpus helpers, shared with the unit tests here.
#[cfg(test)]
#[path = "../tests/common/mod.rs"]
mod test_common;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    use super::test_common as common;

    use std::path::Path;

    use crate::{EngineOptions, KeyFile, WhyRequest};

    fn receipts(index: &Path) -> usize {
        let Ok(rd) = std::fs::read_dir(index.join("ledger")) else {
            return 0;
        };
        rd.filter(|e| {
            std::fs::read(e.as_ref().unwrap().path())
                .unwrap()
                .starts_with(br#"{"record":"receipt""#)
        })
        .count()
    }

    /// `why` on `keys` over `data`: the engine, its workload, and the floor
    /// candidate's proposal.
    fn setup(data: &Path, keys: &Path) -> (Engine, Hash, Hash) {
        let e = Engine::open(data.to_str().unwrap(), EngineOptions::default()).unwrap();
        let w = e
            .why(&WhyRequest {
                keys: Some(KeyFile::new(keys)),
                ..WhyRequest::default()
            })
            .unwrap();
        let wl = w.workload.as_ref().unwrap().hash;
        let p = w.candidates[0].proposal.hash;
        (e, wl, p)
    }

    fn fix_key(e: &Engine, p: Hash, wl: Hash, verify_only: bool) -> FixResult {
        e.fix(&FixRequest {
            proposal: Some(p),
            workload: Some(wl),
            verify_only,
            ..FixRequest::default()
        })
        .unwrap()
    }

    fn file_name(path: &str) -> String {
        Path::new(path)
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned()
    }

    /// The verify stage has teeth: a hypothetical plan that misses a row
    /// group holding a workload key fails the certificate, and no receipt
    /// is written.
    #[test]
    fn an_incomplete_plan_fails_verify_and_writes_no_receipt() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        // ids 0..299 once each: 5 and 77 in part-000, 123 in part-001.
        common::ledger_corpus(&data, 3, 4, 25, 400);
        let kf = dir.path().join("k.parquet");
        common::write_keys(&kf, "id", &[5, 77, 123]);
        let (e, wl, p) = setup(&data, &kf);
        test_seam::set(|sel| {
            sel.retain(|path, _| !path.ends_with("part-001.parquet"));
        });
        let r = fix_key(&e, p, wl, false);
        test_seam::clear();
        let FixOutcome::Refused {
            refusal:
                Refusal::CertificateFailed {
                    missing_right,
                    mismatched,
                    ..
                },
        } = r.outcome
        else {
            panic!("{:?}", r.outcome)
        };
        assert!(missing_right + mismatched >= 1);
        let v = r.verification.unwrap();
        assert!(!v.pass);
        assert_eq!(receipts(&data.join(".bellwether")), 0);
    }

    /// The left side is the whole dataset: it read every row group, and its
    /// catalog digest is the data root's (what the gate checks). The right
    /// side is the plan: a file with no planned row group is not on it.
    #[test]
    fn the_left_side_reads_every_row_group_and_unplanned_files_are_absent_on_the_right() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        common::ledger_corpus(&data, 3, 4, 25, 400);
        let kf = dir.path().join("k.parquet");
        common::write_keys(&kf, "id", &[5, 123]);
        let (e, wl, p) = setup(&data, &kf);
        let r = fix_key(&e, p, wl, true);
        assert!(matches!(r.outcome, FixOutcome::Verified), "{:?}", r.outcome);
        let v = r.verification.unwrap();
        assert!(v.pass);
        let [left, right] = &v.sides;
        assert_eq!((left.files, left.row_groups, left.rows), (3, 12, 300));
        assert_eq!(
            right.files, 2,
            "part-002 holds no workload key and is not planned"
        );
        let source = e.source(None).unwrap();
        assert_eq!(left.catalog, data_digest(e.root(), &*source).unwrap());
        // The plan Evidence the certificate cites names exactly the planned
        // row groups.
        let Record::Evidence(ev) = &r.plan.unwrap().record else {
            panic!()
        };
        let bellwether_types::EvidenceBody::Plan { plan } = &ev.body else {
            panic!()
        };
        let files: Vec<String> = plan
            .entries
            .iter()
            .map(|e| file_name(&e.file.path))
            .collect();
        assert_eq!(files, vec!["part-000.parquet", "part-001.parquet"]);
        assert_eq!(plan.row_groups_selected, 2);
    }

    /// Signed zero: a workload key 0.0 matches rows stored under -0.0 too.
    /// A plan missing the -0.0 file fails the certificate; one that has it
    /// passes.
    #[test]
    fn verify_compares_rows_under_both_float_zeros() {
        use std::sync::Arc;

        use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
        use arrow::datatypes::{DataType, Field, Schema};
        use parquet::arrow::ArrowWriter;

        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        std::fs::create_dir_all(&data).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        for (name, xs, vs) in [
            ("a.parquet", vec![0.0, 1.0], vec![1i64, 2]),
            ("b.parquet", vec![-0.0, 2.0], vec![3, 4]),
            ("c.parquet", vec![5.0, 6.0], vec![5, 6]),
        ] {
            let b = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Float64Array::from(xs)) as ArrayRef,
                    Arc::new(Int64Array::from(vs)),
                ],
            )
            .unwrap();
            let f = std::fs::File::create(data.join(name)).unwrap();
            let mut w = ArrowWriter::try_new(f, Arc::clone(&schema), None).unwrap();
            w.write(&b).unwrap();
            w.close().unwrap();
        }
        let kf = dir.path().join("k.parquet");
        let ks = Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, false)]));
        let kb = RecordBatch::try_new(
            Arc::clone(&ks),
            vec![Arc::new(Float64Array::from(vec![0.0])) as ArrayRef],
        )
        .unwrap();
        let mut w = ArrowWriter::try_new(std::fs::File::create(&kf).unwrap(), ks, None).unwrap();
        w.write(&kb).unwrap();
        w.close().unwrap();
        let (e, wl, p) = setup(&data, &kf);
        // The real plan holds both zeros (a and b) and passes.
        let ok = fix_key(&e, p, wl, true);
        assert!(
            matches!(ok.outcome, FixOutcome::Verified),
            "{:?}",
            ok.outcome
        );
        assert_eq!(ok.verification.as_ref().unwrap().sides[1].files, 2);
        // A plan without b misses the -0.0 row: the certificate fails.
        test_seam::set(|sel| {
            sel.retain(|path, _| !path.ends_with("b.parquet"));
        });
        let bad = fix_key(&e, p, wl, true);
        test_seam::clear();
        assert!(
            matches!(
                bad.outcome,
                FixOutcome::Refused {
                    refusal: Refusal::CertificateFailed { .. }
                }
            ),
            "{:?}",
            bad.outcome
        );
    }

    #[test]
    fn a_key_apply_must_advance_by_its_own_commit() {
        assert_eq!(advance(3, Some(4)), Ok(4));
        assert_eq!(advance(0, Some(1)), Ok(1));
        assert_eq!(
            advance(3, None),
            Err(Refusal::NothingToApply {
                sequence: "3".into()
            })
        );
        assert_eq!(
            advance(3, Some(3)),
            Err(Refusal::NothingToApply {
                sequence: "3".into()
            })
        );
        assert_eq!(
            advance(3, Some(6)),
            Err(Refusal::ConcurrentCommit {
                expected: 4,
                committed: 6
            })
        );
    }
}
