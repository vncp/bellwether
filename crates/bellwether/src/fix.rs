//! `fix`: the engine side of the loop, in this order and no other.
//!
//! 1. **Score.** Replay the recorded workload under the proposal (WP6),
//!    computed here immediately before the Replay is written; the facade
//!    never accepts a caller's cost.
//! 2. **Verify**, against the current manifest (`manifest_before`). A Key
//!    proposal: reconcile (WP8) the full scan, restricted to the workload's
//!    keys (and, for an `asof`, lookback, or window run, to the rows it asks
//!    for, bounded by the workload and the data, never by the index), against a source exposing only the row groups
//!    the hypothetical plan selects (coverage `Workload`). A Rewrite: reconcile the before and
//!    after outputs the caller names (coverage `Full`). A Layout proposal is
//!    refused: layout apply arrives with evolve.
//! 3. **Apply**, only if the certificate passed and was computed against the
//!    current manifest sequence: build the key (WP4) in one commit
//!    (`manifest_after`).
//! 4. **Attest**: `Ledger::put_receipt` links evidence, proposal, replay,
//!    and certificate, with `manifest_before < manifest_after`; with a
//!    signing key, the receipt and the certificate this call wrote are then
//!    signed (ADR 029). A verify-only run signs its passing certificate.
//!
//! A failing certificate writes no Receipt and returns a typed refusal
//! naming the mismatch counts.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use bellwether_build::encode_ts;
use bellwether_build::Builder;
use bellwether_ledger::RecordKind;
use bellwether_replay::{
    proposed_specs, Baseline, Catalog, RecordedRun, Replayer, RunKeys, RunQuery, Workload,
};
use bellwether_source_sql::{is_sql_root, SqlUrl, MEASURED_AGGREGATES};
use bellwether_store::{parse_rfc3339, Cache, ObjectStoreBackend};
use bellwether_types::{
    format, Certificate, Coverage, Error, Evidence, EvidenceBody, FileFingerprint, Hash, HashKind,
    KeySpec, Predicate, Proposal, ProposalBody, Provenance, Receipt, Record, Result, RowGroupMeta,
    ScanEntry, ScanPlan, Source,
};
use bellwether_verify::{
    AgedInput, CertificateContext, CoverageRequest, ManifestAnchor, Options, ReconcileSpec,
    Reconciler, Reconciliation, Side, WorkloadKeys,
};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use crate::engine::{folder_changes, guard, Engine, Pinned};
use crate::engine_fix::{engine_shape, EngineCompare, EngineRun};
use crate::index::stale_index;
use crate::join::{JoinFreshness, WhyPair};
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
    /// Columns totalled exactly: `col` (decimal) or `col:dec(S)` (float
    /// quantized at scale `S`), as `ReconcileRequest::totals`.
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
    /// Go on with a Key proposal whose replay predicts no saving on the
    /// workload (proposed bytes not below current bytes). Without it such a
    /// proposal is refused right after the score ([`Refusal::NothingToGain`]).
    #[serde(default)]
    pub allow_no_gain: bool,
    /// For a rewrite of engine queries (ADR 033): how its before and after
    /// results are compared (default: by the proposal's key columns alone).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine: Option<EngineCompare>,
}

/// The score stage's outcome.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Score {
    /// A Replay computed and written now.
    Replayed {
        /// The Replay.
        replay: Box<Written>,
        /// Summed over runs, under the current configuration. A run no
        /// current spec serves is priced, over Parquet files, as their
        /// min/max statistics read it (the Replay's `unserved` says so).
        current: Cost,
        /// Summed over runs, under the proposal.
        proposed: Cost,
    },
    /// Every cost measured (`exact: false`, `model` named). Over a table
    /// (ADR 027): a Replay computed now and not recorded, since the ledger
    /// keeps exact current costs only for anything but a measured rewrite
    /// (ADR 005); nothing is applied to a table, so no gain gate runs. In a
    /// query engine (ADR 033): the Replay of a rewrite measured from the
    /// engine's own query log, recorded before anything else, with the
    /// twin's prediction.
    Measured {
        /// The Replay, recorded (engine) or as it would be stored (table),
        /// with its hash.
        replay: Box<Written>,
        /// Summed over runs, under the current configuration (measured).
        current: Cost,
        /// Summed over runs, under the proposal (measured).
        proposed: Cost,
        /// The cost model: `measured: logical bytes from per-unit
        /// aggregates` over a table, `<engine>.query_log` in an engine.
        model: String,
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
        /// Keys on both sides whose compared versions differ (the
        /// certificate's `mismatched`).
        mismatched: u64,
        /// Keys that could not be compared: every row of the key on some
        /// side has a null version time, and a null never wins (ADR 014;
        /// the certificate's `uncomparable`).
        #[serde(default)]
        uncomparable: u64,
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
    /// expected-base precondition to prevent this. The key is built and
    /// serves lookups (no re-index is needed), but it cannot be attested:
    /// a retry finds it already complete ([`Refusal::NothingToApply`]).
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
    /// The proposal verified, and its target is a table (ADR 027): the
    /// engine creates no index inside a database and deploys no job, so
    /// apply is attest-only. The passing certificate is written; no
    /// Receipt is, because a Receipt attests an apply the engine made.
    /// `run` is what a human (or a CI gate) runs to apply a key. CLI
    /// exit 3.
    AttestOnly {
        /// The passing certificate.
        certificate: Hash,
        /// The table root the proposal targets (the data root).
        target: String,
        /// For a key: one `CREATE INDEX` per table, written by the engine.
        /// Empty for a rewrite.
        run: Vec<String>,
        /// For a rewrite: the proposal's `after` text as the agent wrote it,
        /// verified on the outputs given and never run by the engine. Not a
        /// statement the engine vouches for: a human reviews it before
        /// deploying it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        proposed_rewrite: Option<String>,
    },
    /// A query engine refused to export a rewrite's results (ADR 033): the
    /// export is the measured run, so nothing was measured, and there is no
    /// certificate and no receipt. CLI exit 3.
    ExportDenied {
        /// The engine root.
        engine: String,
        /// The permission the export needs.
        permission: String,
        /// The whole message, naming what to grant.
        message: String,
    },
    /// The proposal's replay predicts no saving on the workload: summed
    /// over its runs, the proposal reads at least as many bytes as the
    /// current configuration (the measure `why` recommends by; for a
    /// rewrite of engine queries, the bytes the engine's log says each
    /// query read). Refused right after the score, before the verify, so
    /// nothing but the Replay is written; `allow_no_gain` goes on anyway. A
    /// verify-only run is not gated (it applies nothing); applying its
    /// certificate is, on the apply tier's own measurement. CLI exit 3.
    NothingToGain {
        /// The Replay that predicts it.
        replay: Hash,
        /// Bytes under the current configuration, summed over runs.
        current_bytes: u64,
        /// Bytes with the proposal, summed over runs.
        proposed_bytes: u64,
    },
    /// The apply tier ran a rewrite of engine queries again, in the state
    /// the certificate verified, and one query's measurement disagrees with
    /// the certified Replay's on rows read, bytes read, or result rows: the
    /// numbers a receipt would attest are not the numbers verified. Nothing
    /// is written. CLI exit 3.
    MeasurementChanged {
        /// The certificate.
        certificate: Hash,
        /// The measured Replay it cites.
        replay: Hash,
        /// The pair, in the proposal's order.
        pair: u64,
        /// `before` or `after`.
        side: String,
        /// Rows read, as certified.
        certified_read_rows: u64,
        /// Rows read now.
        measured_read_rows: u64,
        /// Bytes read, as certified.
        certified_read_bytes: u64,
        /// Bytes read now.
        measured_read_bytes: u64,
        /// Result rows, as certified.
        certified_result_rows: u64,
        /// Result rows now.
        measured_result_rows: u64,
    },
}

/// The gain gate: a refusal when the proposal reads no fewer bytes than the
/// current configuration and the caller did not allow it.
pub(crate) fn no_gain(
    replay: &Hash,
    current: &Cost,
    proposed: &Cost,
    allow: bool,
) -> Option<Refusal> {
    (!allow && proposed.bytes >= current.bytes).then_some(Refusal::NothingToGain {
        replay: *replay,
        current_bytes: current.bytes,
        proposed_bytes: proposed.bytes,
    })
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
    /// A rewrite of engine queries verified and attested (ADR 033): an
    /// attest-only Receipt (nothing applied; `manifest_after ==
    /// manifest_before`). A human or a CI gate deploys `deploy` in
    /// `target`; the engine changes nothing there.
    Attested {
        /// The attest-only Receipt.
        receipt: Box<Written>,
        /// The engine the queries run in.
        target: String,
        /// The `after` query of each pair, as the agent wrote them, in the
        /// proposal's order: what a human deploys after review.
        deploy: Vec<String>,
    },
    /// Verified; `verify_only` was set, nothing applied.
    Verified,
    /// A dry run of the apply tier ([`Engine::apply_dry_run`]): the gate
    /// passed and the proposal kind can be applied; nothing was written.
    /// The real apply may still be refused if the data or the manifest
    /// changes first, or if its commit is not `expected`.
    WouldApply {
        /// The current manifest sequence, which the certificate was
        /// computed against.
        sequence: u64,
        /// The sequence the apply's one commit must produce.
        expected: u64,
    },
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
    /// The signatures this call wrote (ADR 029): with a signing key, the
    /// Receipt and the Certificate this call wrote, signed right after the
    /// Receipt was written, or the passing Certificate of a verify-only run.
    /// Empty without a signing key, and whenever nothing was attested or
    /// verified.
    pub signatures: Vec<SignatureWritten>,
    /// For a rewrite of engine queries (ADR 033): what each query measured
    /// in the engine, the exports, and the engine's identity before and
    /// after. Omitted otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine: Option<EngineRun>,
    /// For a join key (B3): the right dataset's side of the verification,
    /// whose certificate the left certificate (`certificate`, the one a
    /// receipt cites) cites as an input. Omitted otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub join: Option<JoinCheck>,
}

/// The right dataset's side of a join key's verification (B3): the rows of
/// the semi-join's keys there, all of them against those in the right
/// step's planned row groups.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JoinCheck {
    /// The right dataset's canonical root.
    pub right_root: String,
    /// Distinct keys the workload asks.
    pub keys: u64,
    /// Those both datasets hold, read from their files: the keys both
    /// certificates compare rows of.
    pub matched: u64,
    /// The right step's planned row groups (a `plan` Evidence).
    pub plan: Written,
    /// The right certificate (stored even when it fails).
    pub certificate: Written,
    /// Its typed outcome.
    pub verification: Reconciliation,
}

/// One signature `fix` wrote.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignatureWritten {
    /// The record signed.
    pub record: Hash,
    /// Its kind: a receipt or a certificate.
    pub kind: RecordKind,
    /// The signing key's id (BLAKE3 of its public key).
    pub key_id: Hash,
    /// When it was signed: the engine's clock, RFC 3339 UTC with
    /// milliseconds.
    pub signed_at: String,
    /// The signature object's store path,
    /// `ledger/<record>.sig.<key id>.json`.
    pub path: String,
}

/// What scoring, verifying, and applying depend on now: the current
/// manifest sequence and the data root's catalog digest. Two calls with the
/// same request in the same state compute the same thing; a commit or a
/// file that lands, changes, or goes changes the state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataState {
    /// The current manifest sequence (0 with no index).
    pub sequence: u64,
    /// The data root's catalog digest, as a certificate's left side cites
    /// it.
    pub data: Hash,
}

impl Engine {
    /// Runs score → verify → apply → attest for `request.proposal`, or
    /// apply → attest for an earlier `request.certificate` (module docs).
    pub fn fix(&self, request: &FixRequest) -> Result<FixResult> {
        guard(|| {
            self.require_current("fix")?;
            match (&request.proposal, &request.certificate) {
                (Some(p), None) => self.fix_proposal(p, request),
                (None, Some(c)) => self.apply_certificate(c, false, request.allow_no_gain),
                _ => Err(Error::Other(
                    "fix takes a proposal (--proposal) or a certificate to apply (--certificate), \
                 not both"
                        .into(),
                )),
            }
        })
        .map_err(|e| crate::engine::root_hint(self.root(), e))
    }

    /// The apply tier as a dry run: resolves `certificate` and its proposal
    /// and runs the gate `fix --certificate` runs before anything is written
    /// (passing, current manifest sequence, and for a key the data still the
    /// data certified), then the proposal-kind refusals. Returns
    /// [`FixOutcome::WouldApply`] or the same refusal or error, and writes
    /// nothing anywhere: no replay, no build, no receipt, and it never
    /// creates the index location.
    pub fn apply_dry_run(&self, certificate: &Hash) -> Result<FixResult> {
        guard(|| {
            self.require_current("fix")?;
            self.apply_certificate(certificate, true, false)
        })
    }

    /// The current [`DataState`]. Reads footers; writes nothing and never
    /// creates the index location.
    pub fn state(&self) -> Result<DataState> {
        guard(|| {
            let pinned = self.pin()?;
            let data = data_digest(self.root(), &*self.source(pinned.as_ref())?)?;
            Ok(DataState {
                sequence: pinned.as_ref().map_or(0, Pinned::sequence),
                data,
            })
        })
    }

    /// Reads the record `hash` names, given as `kind`. With
    /// `existing_only`, never creates the index location: an absent one
    /// holds no record.
    fn read(&self, hash: &Hash, kind: HashKind, existing_only: bool) -> Result<Written> {
        let not_found = || Error::RecordNotFound {
            kind,
            hash: *hash,
            place: format!("the ledger at {}", self.index_location()),
        };
        let read = if existing_only {
            let Some(store) = self.existing_store()? else {
                return Err(not_found());
            };
            bellwether_ledger::Ledger::new(Arc::clone(store))
                .get(hash)
                .map(|record| Written {
                    hash: *hash,
                    record,
                })
        } else {
            self.get(hash)
        };
        read.map_err(|e| match e {
            Error::NotFound(h) if h == *hash => not_found(),
            other => other,
        })
    }

    fn proposal(&self, hash: &Hash) -> Result<(Written, Proposal)> {
        self.proposal_read(hash, false)
    }

    fn proposal_read(&self, hash: &Hash, existing_only: bool) -> Result<(Written, Proposal)> {
        let w = self.read(hash, HashKind::Proposal, existing_only)?;
        match &w.record {
            Record::Proposal(p) => {
                // A join key naming a right dataset this engine does not
                // serve is refused before anything else is read (B3,
                // `EngineOptions::join_scope`).
                if let ProposalBody::Key { spec, .. } = &p.body {
                    self.check_join_scope(spec)?;
                }
                let p = p.clone();
                Ok((w, p))
            }
            _ => Err(Error::WrongKind {
                hash: *hash,
                expected: HashKind::Proposal,
                found: w.kind().with_article().into(),
            }),
        }
    }

    // The three proposal kinds, each a short linear sequence of stages.
    #[allow(clippy::too_many_lines)]
    fn fix_proposal(&self, hash: &Hash, req: &FixRequest) -> Result<FixResult> {
        let (written, proposal) = self.proposal(hash)?;
        let pinned = self.pin()?;
        if let Some(p) = &pinned {
            // A stale index is refused with what to do, before any work.
            let changes = folder_changes(self.root(), p, self.format(), &self.excluded())?;
            if !changes.is_empty() {
                return Err(stale_index(self.root(), "fix", &changes));
            }
        }
        let before = pinned.as_ref().map_or(0, Pinned::sequence);
        let mut result = FixResult {
            engine: None,
            proposal: written,
            manifest_before: before.to_string(),
            score: None,
            plan: None,
            certificate: None,
            invariant: None,
            verification: None,
            outcome: FixOutcome::Verified,
            signatures: Vec::new(),
            join: None,
        };
        match &proposal.body {
            ProposalBody::Layout { .. } => {
                result.outcome = refused(Refusal::LayoutNotApplicable);
                Ok(result)
            }
            ProposalBody::Key { spec, sidecars } => {
                // A join key (B3) declared and stale is rebuilt by `index`,
                // not proposed again: refused with that command.
                self.refuse_stale_join(pinned.as_ref(), spec)?;
                let specs = proposed_keys(spec, sidecars);
                // A key already complete has nothing to apply: refused
                // before anything is scored, so the refusal writes nothing.
                if !req.verify_only && complete(pinned.as_ref(), &specs) {
                    result.outcome = refused(Refusal::NothingToApply {
                        sequence: before.to_string(),
                    });
                    return Ok(result);
                }
                let workload_hash = req.workload.ok_or(Error::NoWorkload {
                    proposal: "key",
                    field: "workload",
                    needs: "a recorded workload (the keys a slow lookup ran)",
                })?;
                let source = self.source(pinned.as_ref())?;
                // A join key is priced and verified on both datasets.
                let pair = self.pair_of(pinned.as_ref(), spec)?;
                let replayer =
                    with_pair(replayer(self, &*source, pinned.as_ref())?, pair.as_ref())?;
                let cache = self.cache()?;
                let workload = load_workload(self, &cache, &workload_hash)?;
                // 1. Score.
                let mut replay_hash = None;
                if self.is_table() {
                    let (h, scored) = measured_score(self, &replayer, hash, &workload, &cache)?;
                    // The same gain gate as over files, on measured costs.
                    let gate = no_gain(
                        &h,
                        &total(&scored.replay.current),
                        &total(&scored.replay.proposed),
                        req.allow_no_gain || req.verify_only,
                    );
                    result.score = Some(Score::Measured {
                        current: total(&scored.replay.current),
                        proposed: total(&scored.replay.proposed),
                        model: MEASURED_AGGREGATES.to_string(),
                        replay: Box::new(Written {
                            hash: h,
                            record: Record::Replay(scored.replay),
                        }),
                    });
                    if let Some(r) = gate {
                        result.outcome = refused(r);
                        return Ok(result);
                    }
                } else {
                    let replay = self.score(&replayer, hash, &workload, &cache)?;
                    replay_hash = Some(replay.0.hash);
                    // A verify-only run applies nothing, so it is not gated;
                    // applying its certificate later re-scores and is.
                    let gate = no_gain(
                        &replay.0.hash,
                        &replay.1,
                        &replay.2,
                        req.allow_no_gain || req.verify_only,
                    );
                    result.score = Some(Score::Replayed {
                        replay: Box::new(replay.0),
                        current: replay.1,
                        proposed: replay.2,
                    });
                    if let Some(r) = gate {
                        result.outcome = refused(r);
                        return Ok(result);
                    }
                }
                // 2. Verify.
                let (plan, cert, verification) = match &pair {
                    Some(p) => {
                        let (plan, cert, verification, join) = self.verify_join(
                            hash,
                            spec,
                            &replayer,
                            &*source,
                            &workload,
                            &cache,
                            pinned.as_ref(),
                            p,
                        )?;
                        // The right side first: a right certificate that
                        // fails withholds the receipt.
                        let right = cert_of(&join.certificate)?.clone();
                        let right_hash = join.certificate.hash;
                        result.join = Some(join);
                        if let Some(r) = failed(&right_hash, &right) {
                            result.plan = Some(plan);
                            result.verification = Some(verification);
                            result.certificate = Some(cert);
                            result.outcome = refused(r);
                            return Ok(result);
                        }
                        (plan, cert, verification)
                    }
                    None => self.verify_key(
                        hash,
                        &specs,
                        &replayer,
                        &*source,
                        &workload,
                        &cache,
                        pinned.as_ref(),
                    )?,
                };
                result.plan = Some(plan);
                result.verification = Some(verification);
                let certificate = cert_of(&cert)?.clone();
                result.certificate = Some(cert.clone());
                if let Some(r) = failed(&cert.hash, &certificate) {
                    result.outcome = refused(r);
                    return Ok(result);
                }
                if req.verify_only {
                    // A verify-only run's attestation is its passing
                    // certificate, written by this call.
                    result.signatures =
                        self.sign_written(&[(cert.hash, RecordKind::Certificate)], None)?;
                    return Ok(result);
                }
                let Some(replay_hash) = replay_hash else {
                    // A table: attest-only (ADR 027).
                    result.outcome = refused(self.attest_only(&cert.hash, &proposal.body)?);
                    return Ok(result);
                };
                // 3 and 4, behind the trust-boundary gate.
                match self.gate(&cert.hash, &certificate, true)? {
                    Err(r) => result.outcome = refused(r),
                    Ok(before) => {
                        (result.outcome, result.signatures) = self.apply_and_attest(
                            &proposal,
                            hash,
                            &specs,
                            &replay_hash,
                            &cert.hash,
                            true,
                            before,
                            &workload,
                            pair.as_ref()
                                .zip(result.join.as_ref().map(|j| j.certificate.hash)),
                        )?;
                    }
                }
                Ok(result)
            }
            ProposalBody::Rewrite { shape } if engine_shape(&proposal.body).is_some() => self
                .fix_engine_rewrite(
                    result,
                    hash,
                    &proposal,
                    shape,
                    req,
                    before,
                    pinned.as_ref().map(anchor).as_ref(),
                ),
            ProposalBody::Rewrite { shape } => {
                result.score = Some(Score::NotScorable {
                    reason: "the replay crate defines no replay for rewrite proposals (contract \
                             v3 backlog #2)"
                        .into(),
                });
                result.invariant = Some(shape.invariant.clone());
                let outputs = req.rewrite.as_ref().ok_or(Error::NoWorkload {
                    proposal: "rewrite",
                    field: "rewrite",
                    needs: "the outputs before and after it",
                })?;
                let (cert, verification) = self.verify_rewrite(hash, outputs, pinned.as_ref())?;
                result.verification = Some(verification);
                let certificate = cert_of(&cert)?.clone();
                result.certificate = Some(cert.clone());
                // Attest-only when the data root is a table; a rewrite over
                // a folder has no replay to attest, whatever its outputs are,
                // so `fix --certificate` (which sees no outputs) agrees.
                let table = self.is_table();
                result.outcome = match failed(&cert.hash, &certificate) {
                    Some(r) => refused(r),
                    None if req.verify_only => {
                        result.signatures =
                            self.sign_written(&[(cert.hash, RecordKind::Certificate)], None)?;
                        FixOutcome::Verified
                    }
                    None if table => refused(self.attest_only(&cert.hash, &proposal.body)?),
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
    #[allow(clippy::too_many_lines)]
    fn apply_certificate(
        &self,
        cert_hash: &Hash,
        dry_run: bool,
        allow_no_gain: bool,
    ) -> Result<FixResult> {
        let w = self.read(cert_hash, HashKind::Certificate, dry_run)?;
        let Record::Certificate(certificate) = &w.record else {
            return Err(Error::WrongKind {
                hash: *cert_hash,
                expected: HashKind::Certificate,
                found: w.kind().with_article().into(),
            });
        };
        let certificate = certificate.clone();
        let proposal_hash = certificate.proposal.ok_or_else(|| {
            Error::LedgerRule(format!(
                "certificate {cert_hash} verified no proposal (a standalone reconcile), so there \
                 is nothing to apply"
            ))
        })?;
        let (written, proposal) = self.proposal_read(&proposal_hash, dry_run)?;
        let certified = certificate
            .provenance
            .manifest_version
            .clone()
            .unwrap_or_default();
        let mut result = FixResult {
            engine: None,
            proposal: written,
            manifest_before: certified.clone(),
            score: None,
            plan: None,
            certificate: Some(w.clone()),
            invariant: None,
            verification: None,
            outcome: FixOutcome::Verified,
            signatures: Vec::new(),
            join: None,
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
        // A join key (B3) declared and stale is rebuilt by `index`: refused
        // with that command before any other check reads the key, so a
        // stale key is never reported as complete (`nothing_to_apply`), and
        // the dry run and the real apply agree.
        if let ProposalBody::Key { spec, .. } = &proposal.body {
            if !self.is_table() {
                self.refuse_stale_join(self.pin()?.as_ref(), spec)?;
            }
        }
        match &proposal.body {
            ProposalBody::Layout { .. } => {
                result.outcome = refused(Refusal::LayoutNotApplicable);
            }
            ProposalBody::Rewrite { shape } if engine_shape(&proposal.body).is_some() => {
                return self.apply_engine_rewrite(
                    result,
                    cert_hash,
                    &certificate,
                    &proposal_hash,
                    &proposal,
                    shape,
                    before,
                    dry_run,
                    allow_no_gain,
                );
            }
            // A table: the gate passed, and the engine applies nothing to a
            // table (ADR 027).
            body if self.is_table() => {
                if let ProposalBody::Rewrite { shape } = body {
                    result.invariant = Some(shape.invariant.clone());
                }
                if let ProposalBody::Key { .. } = body {
                    // Re-scored now, and gated like `fix --proposal`. The
                    // dry run scores too (a measured score is never
                    // recorded, so it writes nothing) and so agrees with
                    // the real apply.
                    let Coverage::Workload { workload, .. } = &certificate.coverage else {
                        return Err(not_workload_covered(cert_hash));
                    };
                    let source = self.source(None)?;
                    let replayer = replayer(self, &*source, None)?;
                    let cache = self.cache()?;
                    let workload = load_workload(self, &cache, workload)?;
                    let (h, scored) =
                        measured_score(self, &replayer, &proposal_hash, &workload, &cache)?;
                    let (current, proposed) = (
                        total(&scored.replay.current),
                        total(&scored.replay.proposed),
                    );
                    let gate = no_gain(&h, &current, &proposed, allow_no_gain);
                    result.score = Some(Score::Measured {
                        current,
                        proposed,
                        model: MEASURED_AGGREGATES.to_string(),
                        replay: Box::new(Written {
                            hash: h,
                            record: Record::Replay(scored.replay),
                        }),
                    });
                    if let Some(r) = gate {
                        result.outcome = refused(r);
                        return Ok(result);
                    }
                }
                result.outcome = refused(self.attest_only(cert_hash, body)?);
            }
            ProposalBody::Rewrite { shape } => {
                result.invariant = Some(shape.invariant.clone());
                result.outcome = refused(Refusal::NoReplay {
                    certificate: *cert_hash,
                });
            }
            // Already complete: the apply would commit nothing. Refused
            // here, before the replay is scored, so the dry run and the
            // real apply agree and the refusal writes nothing.
            ProposalBody::Key { spec, sidecars }
                if complete(self.pin()?.as_ref(), &proposed_keys(spec, sidecars)) =>
            {
                result.outcome = refused(Refusal::NothingToApply {
                    sequence: before.to_string(),
                });
            }
            ProposalBody::Key { spec, .. } if dry_run => {
                if !matches!(certificate.coverage, Coverage::Workload { .. }) {
                    return Err(not_workload_covered(cert_hash));
                }
                // A join key's right certificate is checked as the real
                // apply checks it, so the two agree.
                if matches!(spec, KeySpec::Join { .. }) {
                    let r = self.right_engine(spec)?;
                    let rpin = r.pin_required("a join key")?;
                    if let Err(refusal) =
                        self.right_certificate(cert_hash, &certificate, r.root(), &rpin)?
                    {
                        result.outcome = refused(refusal);
                        return Ok(result);
                    }
                }
                result.outcome = FixOutcome::WouldApply {
                    sequence: before,
                    expected: before.saturating_add(1),
                };
            }
            ProposalBody::Key { spec, sidecars } => {
                let specs = proposed_keys(spec, sidecars);
                let Coverage::Workload { workload, .. } = &certificate.coverage else {
                    return Err(not_workload_covered(cert_hash));
                };
                let pinned = self.pin()?;
                let source = self.source(pinned.as_ref())?;
                let pair = self.pair_of(pinned.as_ref(), spec)?;
                let right_cert = match &pair {
                    Some(p) => match self.right_certificate(
                        cert_hash,
                        &certificate,
                        p.right().root(),
                        p.rpin(),
                    )? {
                        Ok(h) => Some(h),
                        Err(r) => {
                            result.outcome = refused(r);
                            return Ok(result);
                        }
                    },
                    None => None,
                };
                let replayer =
                    with_pair(replayer(self, &*source, pinned.as_ref())?, pair.as_ref())?;
                let cache = self.cache()?;
                let workload = load_workload(self, &cache, workload)?;
                let (replay, current_cost, proposed_cost) =
                    self.score(&replayer, &proposal_hash, &workload, &cache)?;
                let replay_hash = replay.hash;
                let gate = no_gain(&replay_hash, &current_cost, &proposed_cost, allow_no_gain);
                result.score = Some(Score::Replayed {
                    replay: Box::new(replay),
                    current: current_cost,
                    proposed: proposed_cost,
                });
                if let Some(r) = gate {
                    result.outcome = refused(r);
                    return Ok(result);
                }
                // The certificate was written by an earlier call (which
                // signed it then, if it had a key); this call signs only
                // the receipt it writes.
                (result.outcome, result.signatures) = self.apply_and_attest(
                    &proposal,
                    &proposal_hash,
                    &specs,
                    &replay_hash,
                    cert_hash,
                    false,
                    before,
                    &workload,
                    pair.as_ref().zip(right_cert),
                )?;
            }
        }
        Ok(result)
    }

    /// The attest-only refusal for a proposal over a table (ADR 027): what
    /// a human runs to apply it.
    fn attest_only(&self, certificate: &Hash, body: &ProposalBody) -> Result<Refusal> {
        let target = self.root().to_string();
        let (run, proposed_rewrite) = match body {
            ProposalBody::Key { spec, .. } => match self.table_url() {
                Some(url) => (
                    SqlUrl::parse(url)?.create_index_statements(spec.columns()),
                    None,
                ),
                None => (Vec::new(), None),
            },
            ProposalBody::Rewrite { shape } => (Vec::new(), Some(shape.after.clone())),
            ProposalBody::Layout { .. } => (Vec::new(), None),
        };
        Ok(Refusal::AttestOnly {
            certificate: *certificate,
            target,
            run,
            proposed_rewrite,
        })
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
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn verify_key(
        &self,
        proposal: &Hash,
        proposed: &[KeySpec],
        replayer: &Replayer<'_>,
        source: &dyn Source,
        workload: &Workload,
        cache: &Cache<ObjectStoreBackend>,
        pinned: Option<&Pinned>,
    ) -> Result<(Written, Written, Reconciliation)> {
        // The configuration the apply commits: each proposed spec replaces
        // the current spec of its name.
        let specs = proposed
            .iter()
            .fold(replayer.current_specs().to_vec(), |c, s| {
                proposed_specs(&c, s)
            });
        let named = proposed
            .iter()
            .map(crate::spec::label)
            .collect::<Vec<_>>()
            .join(" --key ");
        let runs = workload.runs();
        let columns =
            runs.first()
                .map(|r| r.columns.clone())
                .ok_or_else(|| Error::InvalidArgument {
                    field: "workload",
                    reason:
                        "the workload records no run, so there is nothing to verify; record one \
                         from the keys a slow lookup ran"
                            .into(),
                })?;
        let text = runs
            .iter()
            .map(|r| r.query().map(|q| q.text_op().is_some()))
            .collect::<Result<Vec<bool>>>()?;
        if text.iter().any(|t| *t) && !text.iter().all(|t| *t) {
            return Err(Error::InvalidArgument {
                field: "workload",
                reason: "the workload mixes text lookups (contains, has_token, has_prefix) with \
                         key lookups; verify compares one kind at a time, so record each as its \
                         own workload"
                    .into(),
            });
        }
        let text = text.first().copied().unwrap_or(false);
        let membership = membership_runs(runs)?;
        if runs.iter().any(|r| r.columns != columns) {
            return Err(Error::InvalidArgument {
                field: "workload",
                reason: "the workload's runs look up different key columns; verify checks one \
                         key-column set at a time, so record a workload whose runs share their \
                         key columns"
                    .into(),
            });
        }
        // The hypothetical plan: per run, the plan under the current specs
        // plus the proposed one (transient segments for the latter), united.
        let mut selected: BTreeMap<String, (FileFingerprint, RoaringBitmap)> = BTreeMap::new();
        let mut keys: Vec<Vec<u8>> = Vec::new();
        let mut asked: Vec<AskedRun> = Vec::with_capacity(runs.len());
        for run in runs {
            let rk = workload.run_keys(cache, run)?;
            let query = run.query()?;
            let plan = replayer.plan_of_query(&specs, &rk, &query)?;
            for e in &plan.entries {
                selected
                    .entry(e.file.path.clone())
                    .or_insert_with(|| (e.file.clone(), RoaringBitmap::new()))
                    .1 |= &e.row_groups;
            }
            // Both encodings of a float zero, as the planner probes them:
            // SQL equality matches a row stored under either zero. A text
            // run's values are words, substrings, or prefixes, and a
            // membership run's (B4) list elements, not reconcile keys: their
            // rows are chosen by the row filter (`rows_asked`).
            if !text && !membership {
                let all: Vec<usize> = (0..rk.columns().len()).collect();
                keys.extend(rk.all_probes(&all));
            }
            let column = replayer.planned_time_column(&specs, rk.columns(), &query)?;
            let window = replayer.planned_lookback_window(&specs, rk.columns(), &query)?;
            asked.push(AskedRun {
                keys: rk,
                query,
                column,
                window,
                residual: plan.residual,
            });
        }
        keys.sort_unstable();
        keys.dedup();
        #[cfg(test)]
        test_seam::apply(&mut selected);
        let union = union_plan(replayer, &selected)?;
        let plan_evidence = self.plan_evidence(replayer, workload, union)?;
        let planned = PlannedSource {
            inner: source,
            selected,
            schema: SchemaRef::clone(replayer.schema()),
        };
        // Under a fixed clock the workload's age is measured to that clock,
        // so two runs over the same inputs certify the same bytes.
        let cached = match &self.options().clock {
            Some(c) => cache.get_cached_at(&workload.hash(), parse_rfc3339(c)?)?,
            None => cache.get_cached(&workload.hash())?,
        };
        let workload_age = cached.map_or(Duration::ZERO, |(_, age)| age);
        // A text workload (B5) names words and substrings, and a membership
        // workload (B4) list elements, not keys: rows are compared keyed by
        // every hashable column, and chosen by the runs' text matches or
        // membership tests. A workload keyed on an expression (an expr
        // key's runs) compares the rows whose expression value is one of its
        // keys, each row identified by the expression's input columns.
        let (key_columns, coverage, keys_of) = if text || membership {
            let hashable: Vec<String> = replayer
                .schema()
                .fields()
                .iter()
                .filter(|f| bellwether_core::encoding::check_supported(f.data_type()).is_ok())
                .map(|f| f.name().clone())
                .collect();
            if hashable.is_empty() {
                return Err(Error::Other(format!(
                    "every column of {} is nested (a list, map, or struct), so verify has no \
                     column to compare rows by and cannot certify {}; build it without a \
                     receipt with `bellwether index {} --key {}`",
                    self.root(),
                    named,
                    self.root(),
                    named
                )));
            }
            (
                hashable,
                CoverageRequest::WorkloadRows {
                    workload: workload.hash(),
                    age: workload_age,
                },
                None,
            )
        } else {
            let keys_of = match columns.as_slice() {
                [c] if bellwether_core::expr::is_expression(c, replayer.schema()) => {
                    Some(c.clone())
                }
                _ => None,
            };
            let key_columns = match &keys_of {
                Some(e) => bellwether_core::expr::Expr::parse(e)?.columns(),
                None => columns,
            };
            (
                key_columns,
                CoverageRequest::Workload(WorkloadKeys {
                    workload: workload.hash(),
                    age: workload_age,
                    keys,
                }),
                keys_of,
            )
        };
        let ignored = unhashable(replayer.schema(), &key_columns);
        let mut rec_spec = ReconcileSpec::new(key_columns)
            .ignore(ignored)
            .coverage(coverage);
        if let Some(e) = keys_of {
            rec_spec = rec_spec.keys_of(e);
        }
        let rec_spec = rows_asked(source, replayer.catalog(), &asked, rec_spec)?;
        let ctx = CertificateContext {
            proposal: Some(*proposal),
            manifest: pinned.map(anchor),
            inputs: vec![AgedInput {
                hash: plan_evidence.hash,
                age: Duration::ZERO,
            }],
            clock: self.options().clock.clone(),
        };
        let root = self.root();
        let left = Side::new(source, root);
        let right = Side::new(&planned, root);
        let certified = self
            .reconciler(rec_spec, Options::default())?
            .certify(&left, &right, &ctx)?;
        let cert = self.record_certified(&certified)?;
        Ok((plan_evidence, cert, certified.outcome))
    }

    /// Refuses a fix of the join key `spec` (B3) when the index declares
    /// it and it is stale: `bellwether index` rebuilds it, with no
    /// proposal; and the current configuration could not plan with it, so
    /// no replay of it would be the plan.
    fn refuse_stale_join(&self, pinned: Option<&Pinned>, spec: &KeySpec) -> Result<()> {
        let (Some(p), KeySpec::Join { .. }) = (pinned, spec) else {
            return Ok(());
        };
        if !p.planner.declared_specs().contains(spec) {
            return Ok(());
        }
        let right = self.right_engine(spec)?;
        let rpin = right.pin_required("a join key")?;
        let state = self.join_key_state(p, spec, &right, &rpin)?;
        match state.state {
            JoinFreshness::Fresh { .. } => Ok(()),
            _ => Err(self.stale_join(&state)),
        }
    }

    /// For a join key (B3), what it is priced and verified on: its right
    /// dataset and transient pair index ([`Engine::pair_for`]). `None` for
    /// another key.
    fn pair_of(&self, pinned: Option<&Pinned>, spec: &KeySpec) -> Result<Option<WhyPair>> {
        match spec {
            KeySpec::Join { .. } => self.pair_for(pinned, spec).map(Some),
            _ => Ok(None),
        }
    }

    /// The right certificate a join key's (left) certificate cites (B3):
    /// it must be in the ledger, pass, and be of the right dataset's
    /// current manifest (at `right_root`, pinned by `rpin`), by sequence and
    /// by content (a right index deleted and rebuilt to the same sequence
    /// is another manifest); or the refusal.
    #[allow(clippy::result_large_err)]
    fn right_certificate(
        &self,
        cert_hash: &Hash,
        certificate: &Certificate,
        right_root: &str,
        rpin: &Pinned,
    ) -> Result<std::result::Result<Hash, Refusal>> {
        let ledger = self.ledger()?;
        for input in &certificate.provenance.inputs {
            let Ok(Record::Certificate(right)) = ledger.get(input) else {
                continue;
            };
            if let Some(r) = failed(input, &right) {
                return Ok(Err(r));
            }
            let certified = right
                .provenance
                .manifest_version
                .clone()
                .unwrap_or_default();
            // The certificate cites the manifest it verified by content.
            if certified != rpin.sequence().to_string()
                || !right.provenance.inputs.contains(&rpin.hash)
            {
                return Ok(Err(stale_right(
                    *input,
                    right_root,
                    &certified,
                    rpin.sequence(),
                )));
            }
            return Ok(Ok(*input));
        }
        Err(Error::LedgerRule(format!(
            "certificate {cert_hash} verifies a join key but cites no certificate of its right \
             dataset; verify the proposal again with `bellwether fix --verify-only`"
        )))
    }

    /// Stage 2 for a join key (B3): one certificate per dataset. Each
    /// compares, for the keys the workload asks that **both datasets
    /// hold** (read from their files, never from an index), every row of
    /// those keys in the dataset against the rows in its step's planned row
    /// groups (the union over runs of the two-step plan under the current
    /// keys plus the join key). The right certificate is written first;
    /// the left one, which a receipt cites, cites it.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn verify_join(
        &self,
        proposal: &Hash,
        spec: &KeySpec,
        replayer: &Replayer<'_>,
        source: &dyn Source,
        workload: &Workload,
        cache: &Cache<ObjectStoreBackend>,
        pinned: Option<&Pinned>,
        pair: &WhyPair,
    ) -> Result<(Written, Written, Reconciliation, JoinCheck)> {
        type Selected = BTreeMap<String, (FileFingerprint, RoaringBitmap)>;
        let KeySpec::Join {
            columns,
            right,
            right_columns,
            ..
        } = spec
        else {
            return Err(Error::KeySpec(format!("{} is not a join key", spec.name())));
        };
        let specs = proposed_specs(replayer.current_specs(), spec);
        let right_replayer = replayer_of(pair)?;
        let (mut lsel, mut rsel): (Selected, Selected) = (BTreeMap::new(), BTreeMap::new());
        let mut asked: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
        for run in workload.runs() {
            let query = run.query()?;
            let j = match query.join() {
                Some(j)
                    if run.columns == *columns
                        && j.right == *right
                        && j.right_columns == *right_columns =>
                {
                    j
                }
                _ => {
                    return Err(Error::InvalidArgument {
                        field: "workload",
                        reason: format!(
                            "run {:?} is not a semi-join of {} with {right}.{}, so {} cannot be \
                             verified on it; record the keys with `bellwether why --from \
                             <keys.parquet> --mode semijoin --join {right}.{}`",
                            run.run_id,
                            columns.join(","),
                            right_columns.join(","),
                            spec.name(),
                            right_columns.join(",")
                        ),
                    })
                }
            };
            let rk = workload.run_keys(cache, run)?;
            let (l, r) = replayer.semijoin_plans(&specs, &rk, j)?;
            for (sel, plan) in [(&mut lsel, &l), (&mut rsel, &r)] {
                for e in &plan.entries {
                    sel.entry(e.file.path.clone())
                        .or_insert_with(|| (e.file.clone(), RoaringBitmap::new()))
                        .1 |= &e.row_groups;
                }
            }
            let all: Vec<usize> = (0..rk.columns().len()).collect();
            asked.extend(rk.all_probes(&all));
        }
        #[cfg(test)]
        test_seam::apply(&mut lsel);
        // The ground truth both certificates compare by: the asked keys
        // each dataset's files hold.
        let lhas = present_keys(
            source,
            replayer.catalog(),
            replayer.schema(),
            columns,
            &asked,
        )?;
        let rhas = present_keys(
            pair.source(),
            right_replayer.catalog(),
            right_replayer.schema(),
            right_columns,
            &asked,
        )?;
        let matched: Vec<Vec<u8>> = asked
            .iter()
            .filter(|k| lhas.contains(*k) && rhas.contains(*k))
            .cloned()
            .collect();
        let cached = match &self.options().clock {
            Some(c) => cache.get_cached_at(&workload.hash(), parse_rfc3339(c)?)?,
            None => cache.get_cached(&workload.hash())?,
        };
        let age = cached.map_or(Duration::ZERO, |(_, age)| age);
        let right_plan = self.plan_evidence(
            &right_replayer,
            workload,
            union_plan(&right_replayer, &rsel)?,
        )?;
        let (right_cert, right_outcome) = self.certify_side(
            proposal,
            right_columns,
            &matched,
            (&right_replayer, pair.source(), pair.right().root()),
            rsel,
            Some(anchor(pair.rpin())),
            vec![right_plan.hash],
            (workload.hash(), age),
        )?;
        let left_plan = self.plan_evidence(replayer, workload, union_plan(replayer, &lsel)?)?;
        let (left_cert, left_outcome) = self.certify_side(
            proposal,
            columns,
            &matched,
            (replayer, source, self.root()),
            lsel,
            pinned.map(anchor),
            vec![left_plan.hash, right_cert.hash],
            (workload.hash(), age),
        )?;
        Ok((
            left_plan,
            left_cert,
            left_outcome,
            JoinCheck {
                right_root: pair.right().root().to_string(),
                keys: asked.len() as u64,
                matched: matched.len() as u64,
                plan: right_plan,
                certificate: right_cert,
                verification: right_outcome,
            },
        ))
    }

    /// One dataset's certificate of a join key: every row of `keys` in the
    /// dataset against the rows in `selected` (its step's planned row
    /// groups), keyed by `columns`.
    #[allow(clippy::too_many_arguments)]
    fn certify_side(
        &self,
        proposal: &Hash,
        columns: &[String],
        keys: &[Vec<u8>],
        (replayer, source, root): (&Replayer<'_>, &dyn Source, &str),
        selected: BTreeMap<String, (FileFingerprint, RoaringBitmap)>,
        manifest: Option<ManifestAnchor>,
        inputs: Vec<Hash>,
        (workload, age): (Hash, Duration),
    ) -> Result<(Written, Reconciliation)> {
        let planned = PlannedSource {
            inner: source,
            selected,
            schema: SchemaRef::clone(replayer.schema()),
        };
        let rec_spec = ReconcileSpec::new(columns.to_vec())
            .ignore(unhashable(replayer.schema(), columns))
            .coverage(CoverageRequest::Workload(WorkloadKeys {
                workload,
                age,
                keys: keys.to_vec(),
            }));
        let ctx = CertificateContext {
            proposal: Some(*proposal),
            manifest,
            inputs: inputs
                .into_iter()
                .map(|hash| AgedInput {
                    hash,
                    age: Duration::ZERO,
                })
                .collect(),
            clock: self.options().clock.clone(),
        };
        let certified = self.reconciler(rec_spec, Options::default())?.certify(
            &Side::new(source, root),
            &Side::new(&planned, root),
            &ctx,
        )?;
        let cert = self.record_certified(&certified)?;
        Ok((cert, certified.outcome))
    }

    /// Writes `plan` as the `plan` Evidence verify reads under: it cites the
    /// catalog digest and the workload.
    fn plan_evidence(
        &self,
        replayer: &Replayer<'_>,
        workload: &Workload,
        plan: ScanPlan,
    ) -> Result<Written> {
        self.put(Record::Evidence(Evidence {
            record_version: format::RECORD_VERSION,
            provenance: Provenance {
                inputs: vec![replayer.catalog().digest(), workload.hash()],
                oldest_input_age: Duration::ZERO,
                manifest_version: Some(replayer.manifest_version()),
                created_at: self.now(),
            },
            body: EvidenceBody::Plan { plan },
        }))
    }

    /// Stage 2 for a Rewrite proposal: the caller's before and after outputs.
    fn verify_rewrite(
        &self,
        proposal: &Hash,
        outputs: &RewriteOutputs,
        pinned: Option<&Pinned>,
    ) -> Result<(Written, Reconciliation)> {
        let side = |p: &str| -> Result<(String, String)> {
            if is_sql_root(p) {
                Ok((p.to_string(), SqlUrl::parse(p)?.root()))
            } else {
                let c = canonical_dir(p)?;
                Ok((c.clone(), c))
            }
        };
        let (before, before_root) = side(&outputs.before)?;
        let (after, after_root) = side(&outputs.after)?;
        let left_source = self.output_source(&before)?;
        let right_source = self.output_source(&after)?;
        let mut spec = ReconcileSpec::new(outputs.key.clone())
            .ignore(outputs.ignore.clone())
            .with_totals(crate::reconcile::total_columns(&outputs.totals)?)
            .group_by(outputs.group_by.clone());
        if let Some(l) = &outputs.latest_by {
            spec = spec.latest_by(l.clone());
        }
        let ctx = CertificateContext {
            proposal: Some(*proposal),
            manifest: pinned.map(anchor),
            inputs: Vec::new(),
            clock: self.options().clock.clone(),
        };
        let certified = self.reconciler(spec, Options::default())?.certify(
            &Side::new(&*left_source, &before_root),
            &Side::new(&*right_source, &after_root),
            &ctx,
        )?;
        let cert = self.record_certified(&certified)?;
        Ok((cert, certified.outcome))
    }

    /// A reconciler whose records carry this engine's clock, like every
    /// other record the engine writes.
    pub(crate) fn reconciler(&self, spec: ReconcileSpec, options: Options) -> Result<Reconciler> {
        Ok(Reconciler::new(spec, options)?.created_at(self.now()))
    }

    /// A source for an output folder `root`: the engine's format, schema
    /// inferred, partitioning as the options say, rooted at `root`.
    pub(crate) fn output_source(&self, root: &str) -> Result<Box<dyn Source>> {
        self.source_at(None, root)
    }

    pub(crate) fn record_certified(
        &self,
        certified: &bellwether_verify::Certified,
    ) -> Result<Written> {
        // A run over a table ends before its certificate is written: a
        // table that changed without a snapshot is refused, never certified.
        self.finish_tables()?;
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
    fn realize(
        &self,
        proposal: &Hash,
        specs: &[KeySpec],
        workload: &Workload,
    ) -> Result<Option<Box<Written>>> {
        let pinned = self.pin()?;
        let source = self.source(pinned.as_ref())?;
        let pair = match specs {
            [spec] => self.pair_of(pinned.as_ref(), spec)?,
            _ => None,
        };
        let replayer = with_pair(replayer(self, &*source, pinned.as_ref())?, pair.as_ref())?;
        let cache = self.cache()?;
        let (replay, _, _) = self.score(&replayer, proposal, workload, &cache)?;
        Ok(Some(Box::new(replay)))
    }

    /// Stages 3 and 4, after the gate: build the key in one commit, which
    /// must be exactly `before + 1`, then write the receipt and, with a
    /// signing key, sign it (and the certificate, when `certificate_written`
    /// says this call wrote it) right after `put_receipt` returns.
    #[allow(clippy::too_many_arguments)]
    fn apply_and_attest(
        &self,
        proposal: &Proposal,
        proposal_hash: &Hash,
        specs: &[KeySpec],
        replay: &Hash,
        cert_hash: &Hash,
        certificate_written: bool,
        before: u64,
        workload: &Workload,
        join: Option<(&WhyPair, Hash)>,
    ) -> Result<(FixOutcome, Vec<SignatureWritten>)> {
        #[cfg(test)]
        test_seam::before_apply(self);
        // A join key (B3) is declared and built whole in one commit from
        // both datasets' exact keys, pinned to the right manifest the right
        // certificate verified: `refresh_join` is given its content hash and
        // refuses, before building or committing anything, a right dataset
        // whose current manifest is another (a commit, or an index deleted
        // and rebuilt, since the certificate).
        let committed = if let Some((pair, right_cert)) = join {
            let [spec] = specs else {
                return Err(Error::KeySpec(
                    "a join key is applied alone, built whole from both datasets; propose the \
                     specs beside it in a proposal of their own"
                        .into(),
                ));
            };
            let rpin = pair.rpin();
            match Builder::new(Arc::clone(self.store()?), "").refresh_join(
                spec,
                pair.right().store()?,
                "",
                Some(rpin.hash),
                self.build_options().pair_caps(),
            )? {
                bellwether_build::JoinRefresh::Built { sequence, .. } => Some(sequence),
                bellwether_build::JoinRefresh::RightChanged { found, .. } => {
                    return Ok((
                        refused(stale_right(
                            right_cert,
                            pair.right().root(),
                            &rpin.sequence().to_string(),
                            found.sequence,
                        )),
                        Vec::new(),
                    ));
                }
                bellwether_build::JoinRefresh::Fresh { .. } => None,
            }
        } else {
            let options = self.build_options();
            Builder::new(Arc::clone(self.store()?), "")
                .build(self.root(), specs, &options)?
                .sequence
        };
        let after = match advance(before, committed) {
            Ok(after) => after,
            Err(refusal) => return Ok((refused(refusal), Vec::new())),
        };
        let receipt = Receipt {
            attest_only: false,
            prediction: None,
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
        // Attest: sign only what this call wrote, and only now that the
        // ledger has accepted the receipt.
        let mut signed = vec![(hash, RecordKind::Receipt)];
        if certificate_written {
            signed.push((*cert_hash, RecordKind::Certificate));
        }
        let applied = format!("manifest {before} -> {after}");
        let signatures = self.sign_written(&signed, Some(&applied))?;
        // After the apply: the same proposal replayed at manifest_after, the
        // realized cost `report` sets beside the predicted one.
        let (realized, warning) = match self.realize(proposal_hash, specs, workload) {
            Ok(r) => (r, None),
            Err(e) => (
                None,
                Some(format!(
                    "the receipt stands, but its realized replay could not be computed ({e}); run \
                     `bellwether why --workload <hash>` to compute the realized cost"
                )),
            ),
        };
        Ok((
            FixOutcome::Applied {
                receipt: Box::new(Written {
                    hash,
                    record: Record::Receipt(receipt),
                }),
                realized,
                warning,
            },
            signatures,
        ))
    }

    /// Signs records this call wrote with the configured key, at the
    /// engine's clock; nothing without a key. The hashes come from this
    /// call's own writes, never from a request.
    ///
    /// `applied` names the change already committed (`manifest 3 -> 4`),
    /// so a signing failure after the apply says the change stands.
    pub(crate) fn sign_written(
        &self,
        records: &[(Hash, RecordKind)],
        applied: Option<&str>,
    ) -> Result<Vec<SignatureWritten>> {
        let Some(key) = &self.options().signing_key else {
            return Ok(Vec::new());
        };
        let ledger = self.ledger()?;
        let signed_at = self.now();
        let mut out = Vec::with_capacity(records.len());
        for (i, (record, kind)) in records.iter().enumerate() {
            let at = ledger.sign(record, key, &signed_at).map_err(|e| {
                let applied = applied
                    .map_or_else(String::new, |a| format!("the change is applied ({a}) and "));
                // Signing stops at the first failure: name what else this
                // call wrote that is left unsigned with it.
                let rest: Vec<String> = records[i + 1..]
                    .iter()
                    .map(|(h, k)| format!("the {} {h} this call wrote", k.as_str()))
                    .collect();
                let also = if rest.is_empty() {
                    String::new()
                } else {
                    format!(" (and so is {})", rest.join(" and "))
                };
                Error::Other(format!(
                    "{applied}{} {record} is written, but it could not be signed ({e}); it \
                     stands unsigned{also}, since only the call that writes a record signs it. \
                     Check that the index location is writable for `ledger/` (each signature \
                     is a new file there), then sign the next change; `bellwether receipt \
                     show` shows this one as unsigned",
                    kind.as_str()
                ))
            })?;
            out.push(SignatureWritten {
                record: *record,
                kind: *kind,
                key_id: at.key_id,
                signed_at: signed_at.clone(),
                path: at.path(),
            });
        }
        Ok(out)
    }
}

/// Scores `proposal` on `workload` without recording the Replay: over a
/// table (ADR 027) every cost, current and proposed, is measured, so each
/// is marked inexact and the Replay names [`MEASURED_AGGREGATES`]. The ledger
/// keeps only replays whose current costs are exact (ADR 005), so a
/// measured one is returned, hashed as it would be stored, and never
/// stored: nothing measured is ever recorded as exact.
pub(crate) fn measured_score(
    engine: &Engine,
    replayer: &Replayer<'_>,
    proposal: &Hash,
    workload: &Workload,
    cache: &Cache<ObjectStoreBackend>,
) -> Result<(Hash, bellwether_replay::Scored)> {
    let body = match engine.get(proposal)?.record {
        Record::Proposal(p) => p.body,
        _ => {
            return Err(Error::WrongKind {
                hash: *proposal,
                expected: HashKind::Proposal,
                found: "another record".into(),
            })
        }
    };
    let mut scored = replayer
        .score(proposal, &body, workload, cache, &engine.now())
        .map_err(Error::from)?;
    mark_measured(&mut scored.replay);
    let hash = Record::Replay(scored.replay.clone()).hash()?;
    Ok((hash, scored))
}

/// Marks every cost of `replay` measured and names the model, on both
/// `exact` and `exactness` (record version 3: they agree, ledger rule 7b).
/// Runs no spec serves stay priced as a raw scan (`unserved` absent).
pub(crate) fn mark_measured(replay: &mut bellwether_types::Replay) {
    for c in replay.current.iter_mut().chain(replay.proposed.iter_mut()) {
        c.exact = false;
        c.exactness = bellwether_types::Exactness::Measured {
            model: MEASURED_AGGREGATES.to_string(),
        };
    }
    replay.model = Some(MEASURED_AGGREGATES.to_string());
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

    type ApplyHook = Box<dyn Fn(&crate::Engine)>;

    thread_local! {
        static BEFORE_APPLY: RefCell<Option<ApplyHook>> = RefCell::new(None);
    }

    /// Installs `hook`, run after the gate and before the apply's build, for
    /// this thread until `clear_before_apply`: a commit it makes lands
    /// between the manifest check and the apply.
    pub(crate) fn set_before_apply(hook: impl Fn(&crate::Engine) + 'static) {
        BEFORE_APPLY.with(|h| *h.borrow_mut() = Some(Box::new(hook)));
    }

    pub(crate) fn clear_before_apply() {
        BEFORE_APPLY.with(|h| *h.borrow_mut() = None);
    }

    pub(crate) fn before_apply(engine: &crate::Engine) {
        BEFORE_APPLY.with(|h| {
            if let Some(f) = h.borrow().as_ref() {
                f(engine);
            }
        });
    }
}

/// The sequence a Key apply produced, or the refusal: nothing committed (the
/// key was already complete), or another commit landed first. A Key
/// proposal must advance the manifest by exactly the apply's own commit.
// The refusal is returned as is; it is built once per apply.
#[allow(clippy::result_large_err)]
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

/// Whether every one of `specs` is complete at the pinned manifest:
/// applying them would commit nothing.
fn complete(pinned: Option<&Pinned>, specs: &[KeySpec]) -> bool {
    pinned.is_some_and(|p| specs.iter().all(|s| p.planner.completeness(s).is_some()))
}

/// The specs a key proposal applies in one commit: its spec, then the specs
/// proposed beside it (`ProposalBody::Key.sidecars`).
fn proposed_keys(spec: &KeySpec, sidecars: &[KeySpec]) -> Vec<KeySpec> {
    std::iter::once(spec).chain(sidecars).cloned().collect()
}

/// The refusal for a join key's right certificate (B3) whose right
/// manifest is no longer the right dataset's (at `root`) current one:
/// `certified` is the sequence it verified, `current` the current one; at
/// one sequence, the current manifest is another (the index was deleted
/// and rebuilt), and the refusal says so.
fn stale_right(certificate: Hash, root: &str, certified: &str, current: u64) -> Refusal {
    let current = if certified == current.to_string() {
        format!(
            "{current} of {root}, another manifest at that sequence (its index was deleted and \
             rebuilt since)"
        )
    } else {
        format!("{current} of {root}")
    };
    Refusal::StaleManifest {
        certificate,
        certified: format!("{certified} of {root}"),
        current,
    }
}

fn not_workload_covered(cert_hash: &Hash) -> Error {
    Error::LedgerRule(format!(
        "certificate {cert_hash} does not cover a recorded workload, so the key proposal it \
         verified cannot be re-scored on the keys it checked"
    ))
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
        uncomparable: c.body.uncomparable,
        missing_left: c.body.missing_left,
        missing_right: c.body.missing_right,
        totals_differ: c.body.totals_delta.len() as u64,
    })
}

/// Why `n` keys could not be compared and what to do, in the words `fix`,
/// `reconcile` and the MCP refusal use.
#[must_use]
pub fn uncomparable_cause(n: u64) -> String {
    let keys = if n == 1 {
        "1 key".to_string()
    } else {
        format!("{n} keys")
    };
    format!(
        "{keys} could not be compared (uncomparable): every row of such a key on one side has a \
         null version time, and a null never wins (ADR 014), so the key has no version to compare \
         there. Give those rows a time in the data, or compare without a version column (record \
         the workload without --latest-by, or reconcile without --latest-by) so every row of the \
         key is compared"
    )
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

/// `replayer` with a join key's right dataset attached (B3), when there is
/// one.
pub(crate) fn with_pair<'a>(
    replayer: Replayer<'a>,
    pair: Option<&'a WhyPair>,
) -> Result<Replayer<'a>> {
    Ok(match pair {
        Some(p) => replayer.with_pair(p.input()?),
        None => replayer,
    })
}

/// The right dataset's own replayer (B3).
fn replayer_of(pair: &WhyPair) -> Result<Replayer<'_>> {
    replayer(pair.right(), pair.source(), Some(pair.rpin()))
}

/// The tuples of `asked` some row of the dataset holds on `columns`, read
/// from every live row group's key columns (one row group at a time; only
/// asked tuples are kept, ADR 018). A tuple holding a null equals nothing
/// (ADR 014).
fn present_keys(
    source: &dyn Source,
    catalog: &Catalog,
    schema: &SchemaRef,
    columns: &[String],
    asked: &std::collections::BTreeSet<Vec<u8>>,
) -> Result<std::collections::BTreeSet<Vec<u8>>> {
    let types = columns
        .iter()
        .map(|c| {
            schema
                .field_with_name(c)
                .map(|f| f.data_type().clone())
                .map_err(|_| Error::KeySpec(format!("join column {c:?} is not in the dataset")))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut found = std::collections::BTreeSet::new();
    if asked.is_empty() {
        return Ok(found);
    }
    for rg in catalog.row_groups() {
        let batch = source.read_columns(&rg.meta, columns)?;
        let arrays = columns
            .iter()
            .map(|c| {
                batch
                    .column_by_name(c)
                    .cloned()
                    .ok_or_else(|| Error::Source(format!("the source returned no column {c:?}")))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut tuple = Vec::new();
        'rows: for row in 0..batch.num_rows() {
            tuple.clear();
            for (a, t) in arrays.iter().zip(&types) {
                if !bellwether_core::encoding::encode_array_value_as(
                    a.as_ref(),
                    row,
                    t,
                    &mut tuple,
                )? {
                    continue 'rows;
                }
            }
            if asked.contains(&tuple) {
                found.insert(tuple.clone());
            }
        }
    }
    Ok(found)
}

/// The replayer every `why` and `fix` replay is computed by, pinned to
/// `pinned` or over the unindexed folder: over Parquet files, a run no spec
/// of a configuration serves is priced as the files' own min/max statistics
/// read it (`Baseline::ParquetStats`), so `why`'s numbers, the score `fix`
/// gates on, and the realized replay after the apply are one measure, the
/// Replay a receipt cites. CSV and JSON lines have no footer statistics,
/// and a table's costs are measured (ADR 027): such a run reads every row
/// group.
pub(crate) fn replayer<'a>(
    engine: &Engine,
    source: &'a dyn Source,
    pinned: Option<&Pinned>,
) -> Result<Replayer<'a>> {
    let replayer = match pinned {
        Some(p) => Replayer::for_snapshot(source, &p.snapshot)?,
        None => Replayer::unindexed(source, engine.root(), &engine.partitioning(None)?)?
            .with_fingerprint_policy(engine.options().fingerprint_policy.unwrap_or_default()),
    };
    Ok(match engine.format() {
        crate::SourceFormat::Parquet if !engine.is_table() => {
            replayer.with_unserved(Baseline::ParquetStats)
        }
        _ => replayer,
    })
}

/// The recorded workload `hash`: absent is [`Error::RecordNotFound`];
/// present but unreadable names the workload and the cause.
/// Columns with no canonical encoding (nested struct, list, map, ...),
/// minus `keys`: they cannot be row-hashed, and a key fix changes which row
/// groups are read, not any value, so the key verification leaves them out
/// (listed on the certificate) rather than refusing every lake that holds
/// one. Ported from the Argon defects implementation.
fn unhashable(schema: &arrow::datatypes::Schema, keys: &[String]) -> Vec<String> {
    schema
        .fields()
        .iter()
        .filter(|f| bellwether_core::encoding::check_supported(f.data_type()).is_err())
        .map(|f| f.name().clone())
        .filter(|n| !keys.contains(n))
        .collect()
}

/// Whether a workload's runs are list-membership runs (B4); a workload
/// mixing them with key lookups is refused, since verify compares one kind
/// at a time.
fn membership_runs(runs: &[RecordedRun]) -> Result<bool> {
    let membership = runs
        .iter()
        .map(|r| r.query().map(|q| q.is_membership()))
        .collect::<Result<Vec<bool>>>()?;
    if membership.iter().any(|m| *m) && !membership.iter().all(|m| *m) {
        return Err(Error::InvalidArgument {
            field: "workload",
            reason: "the workload mixes list-membership runs with key lookups; verify compares \
                     one kind at a time, so record each as its own workload"
                .into(),
        });
    }
    Ok(membership.first().copied().unwrap_or(false))
}

/// One run as verify compares it: its keys, its lookup, the time column the
/// lookup is planned on (`Replayer::planned_time_column`; `None` when no
/// index can plan it, so the run reads every row group), and its plan's
/// residual, read only for which columns key a lookback's windows.
struct AskedRun {
    keys: RunKeys,
    query: RunQuery,
    column: Option<String>,
    /// An `asof` lookback run's window as the plan was priced on: on
    /// `column`, converted from the run's own unit rounding outward
    /// (`Replayer::planned_lookback_window`).
    window: Option<bellwether_replay::RunWindow>,
    residual: Option<Predicate>,
}

/// `spec` comparing only the rows the runs ask for, every bound taken from
/// the workload and the data on the left side, never from the index under
/// test: a window's ends and an `asof`'s instant (and lookback) are the
/// run's own, and a `latest` lookback's window `[newest − N, newest]` is
/// built from each key's newest time in `source` (the left side, every
/// live row group of `catalog`). A row is compared, on both sides, when
/// some run keeps it: one of the run's keys, within its window, or for a
/// text run (B5) a row whose string matches one of the run's values, and for
/// a membership run (B4) a row whose list holds any (or every one) of the
/// run's values. Locate and
/// plain `latest` runs keep every row of their keys, and `latest` and
/// `asof` compare the newest kept version per key (`latest_by`). An index
/// whose bounds disagree with the data plans other row groups than these
/// windows need, so its certificate fails.
fn rows_asked(
    source: &dyn Source,
    catalog: &Catalog,
    runs: &[AskedRun],
    mut spec: ReconcileSpec,
) -> Result<ReconcileSpec> {
    let schema = catalog.schema();
    let ty = |c: &str| -> Result<arrow::datatypes::DataType> {
        Ok(schema
            .field_with_name(c)
            .map_err(|_| Error::KeySpec(format!("column {c:?} is not in the dataset")))?
            .data_type()
            .clone())
    };
    let mut narrowed = false;
    let mut filter = Vec::with_capacity(runs.len());
    for run in runs {
        if run.query.is_membership() {
            // The rows whose list holds the run's values, from the
            // workload alone.
            let [column] = run.keys.columns() else {
                return Err(Error::KeySpec(format!(
                    "a membership run looks up one list column, not {:?}",
                    run.keys.columns()
                )));
            };
            filter.push(bellwether_replay::membership_residual(
                column,
                &run.keys,
                matches!(run.query, RunQuery::ContainsAll),
            ));
            narrowed = true;
            continue;
        }
        if let Some(text) = bellwether_replay::text_residual(&run.keys, &run.query)? {
            // The rows whose string matches the run's values, from the
            // workload alone.
            filter.push(text);
            narrowed = true;
            continue;
        }
        let all: Vec<usize> = (0..run.keys.columns().len()).collect();
        let keyed = match run.keys.columns() {
            [c] if bellwether_core::expr::is_expression(c, schema) => Predicate::ExprIn {
                expr: c.clone(),
                values: run.keys.all_probes(&all),
            },
            columns => Predicate::In {
                columns: columns.to_vec(),
                values: run.keys.all_probes(&all),
            },
        };
        let mut tests = vec![keyed];
        let between = |c: &str, lo: i64, hi: i64| -> Result<Predicate> {
            let t = ty(c)?;
            let floor = bellwether_core::encoding::ts_min(&t)?;
            Ok(Predicate::Between {
                column: c.to_string(),
                lo: encode_ts(lo.max(floor), &t)?,
                hi: encode_ts(hi, &t)?,
            })
        };
        match (&run.query, &run.column) {
            (RunQuery::Window(w), _) => tests.push(between(&w.ts_column, w.lo, w.hi)?),
            (RunQuery::Asof { ts }, Some(c)) => tests.push(between(c, i64::MIN, *ts)?),
            (RunQuery::Lookback { asof: Some(_), .. }, Some(_)) => {
                // Without a window, the run's units do not convert to the
                // planned column's: it was planned without one, so every
                // row of its keys is compared.
                let Some(w) = &run.window else {
                    filter.push(Predicate::And { all: tests });
                    continue;
                };
                tests.push(between(&w.ts_column, w.lo, w.hi)?);
            }
            (
                RunQuery::Lookback {
                    asof: None,
                    lookback,
                },
                Some(c),
            ) => tests.extend(newest_windows(source, catalog, run, c, *lookback)?),
            // Each key's winning version, from the data: the rows at its
            // greatest valid time within the bound, and among those its
            // greatest known time within the bound.
            (RunQuery::Bitemporal(b), _) => {
                tests = bitemporal_winners(source, catalog, run, b)?;
            }
            _ => {
                filter.push(Predicate::And { all: tests });
                continue;
            }
        }
        narrowed = true;
        filter.push(Predicate::And { all: tests });
    }
    if narrowed {
        spec = spec.rows_where(filter);
    }
    // One version per key, by the one column every run ranks versions on.
    let mut versioned = runs.iter().map(|r| match (&r.query, &r.column) {
        (RunQuery::Latest | RunQuery::Asof { .. }, Some(c)) => Some(c),
        _ => None,
    });
    if let Some(Some(first)) = versioned.next() {
        if versioned.all(|c| c == Some(first)) {
            spec = spec.latest_by(first.clone());
        }
    }
    Ok(spec)
}

/// A `latest` lookback run's per-key windows `[newest − lookback, newest]`
/// on `ts_column`, keyed by the columns the planner keys its windows by
/// (the residual's `NotOlderThan` columns; the run's columns otherwise),
/// with each key's newest time read from every live row group of `source`:
/// `In` over the keys that have a non-null time and `NotOlderThan` at each
/// window's start. Memory: one time per looked-up key (ADR 018).
fn newest_windows(
    source: &dyn Source,
    catalog: &Catalog,
    run: &AskedRun,
    ts_column: &str,
    lookback: i64,
) -> Result<Vec<Predicate>> {
    fn key_columns(p: &Predicate) -> Option<&Vec<String>> {
        match p {
            Predicate::NotOlderThan { columns, .. } => Some(columns),
            Predicate::And { all } => all.iter().find_map(key_columns),
            _ => None,
        }
    }
    let columns = run
        .residual
        .as_ref()
        .and_then(key_columns)
        .cloned()
        .unwrap_or_else(|| run.keys.columns().to_vec());
    let positions = run.keys.positions(&columns).ok_or_else(|| {
        Error::Other(format!(
            "the lookback's key columns {columns:?} are not all among the run's {:?}",
            run.keys.columns()
        ))
    })?;
    let wanted: std::collections::HashSet<Vec<u8>> =
        run.keys.all_probes(&positions).into_iter().collect();
    let schema = catalog.schema();
    let types: Vec<arrow::datatypes::DataType> = columns
        .iter()
        .chain(std::iter::once(&ts_column.to_string()))
        .map(|c| {
            schema
                .field_with_name(c)
                .map(|f| f.data_type().clone())
                .map_err(|_| Error::KeySpec(format!("column {c:?} is not in the dataset")))
        })
        .collect::<Result<_>>()?;
    let mut read: Vec<String> = columns.clone();
    read.push(ts_column.to_string());
    let n = columns.len();
    let mut newest: BTreeMap<Vec<u8>, i64> = BTreeMap::new();
    let mut key = Vec::new();
    for rg in catalog.row_groups() {
        let batch = source.read_columns(&rg.meta, &read)?;
        'rows: for row in 0..batch.num_rows() {
            key.clear();
            for (i, t) in types[..n].iter().enumerate() {
                if !bellwether_core::encoding::encode_array_value_as(
                    batch.column(i).as_ref(),
                    row,
                    t,
                    &mut key,
                )? {
                    continue 'rows;
                }
            }
            if !wanted.contains(&key) {
                continue;
            }
            if let Some(t) =
                bellwether_core::encoding::ts_value_as(batch.column(n).as_ref(), row, &types[n])?
            {
                let e = newest.entry(key.clone()).or_insert(t);
                *e = (*e).max(t);
            }
        }
    }
    let ts_type = &types[n];
    let floor = bellwether_core::encoding::ts_min(ts_type)?;
    let bounds = newest
        .iter()
        .map(|(k, t)| {
            Ok((
                k.clone(),
                encode_ts(t.saturating_sub(lookback).max(floor), ts_type)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(vec![
        Predicate::In {
            columns: columns.clone(),
            values: newest.into_keys().collect(),
        },
        Predicate::NotOlderThan {
            columns,
            ts_column: ts_column.to_string(),
            bounds,
        },
    ])
}

/// A bitemporal run's winning rows, from every live row group of `source`
/// (the left side, never the index under test): per key (the columns the
/// plan's residual keys its versions by, the run's columns otherwise), the
/// greatest valid time at or before the run's valid bound among rows known
/// at or before its known bound, then the greatest known time at that valid
/// time. As predicates: `In` over the keys with a winner, each clock within
/// its bound, and `NotOlderThan` at each key's winning valid and known
/// times, which together keep exactly the winning rows. Memory: one winner
/// per looked-up key (ADR 018).
#[allow(clippy::many_single_char_names)]
fn bitemporal_winners(
    source: &dyn Source,
    catalog: &Catalog,
    run: &AskedRun,
    b: &bellwether_replay::RunBitemporal,
) -> Result<Vec<Predicate>> {
    fn key_columns(p: &Predicate) -> Option<&Vec<String>> {
        match p {
            Predicate::NotOlderThan { columns, .. } => Some(columns),
            Predicate::And { all } => all.iter().find_map(key_columns),
            _ => None,
        }
    }
    let columns = run
        .residual
        .as_ref()
        .and_then(key_columns)
        .cloned()
        .unwrap_or_else(|| run.keys.columns().to_vec());
    let positions = run.keys.positions(&columns).ok_or_else(|| {
        Error::Other(format!(
            "the bitemporal key columns {columns:?} are not all among the run's {:?}",
            run.keys.columns()
        ))
    })?;
    let wanted: std::collections::HashSet<Vec<u8>> =
        run.keys.all_probes(&positions).into_iter().collect();
    let schema = catalog.schema();
    let mut read: Vec<String> = columns.clone();
    read.push(b.valid_column.clone());
    read.push(b.known_column.clone());
    let types: Vec<arrow::datatypes::DataType> = read
        .iter()
        .map(|c| {
            schema
                .field_with_name(c)
                .map(|f| f.data_type().clone())
                .map_err(|_| Error::KeySpec(format!("column {c:?} is not in the dataset")))
        })
        .collect::<Result<_>>()?;
    let n = columns.len();
    let mut winners: BTreeMap<Vec<u8>, (i64, i64)> = BTreeMap::new();
    let mut key = Vec::new();
    for rg in catalog.row_groups() {
        let batch = source.read_columns(&rg.meta, &read)?;
        'rows: for row in 0..batch.num_rows() {
            key.clear();
            for (i, t) in types[..n].iter().enumerate() {
                if !bellwether_core::encoding::encode_array_value_as(
                    batch.column(i).as_ref(),
                    row,
                    t,
                    &mut key,
                )? {
                    continue 'rows;
                }
            }
            if !wanted.contains(&key) {
                continue;
            }
            let clock = |i: usize| {
                bellwether_core::encoding::ts_value_as(batch.column(i).as_ref(), row, &types[i])
            };
            let (Some(v), Some(k)) = (clock(n)?, clock(n + 1)?) else {
                continue;
            };
            if b.valid.is_some_and(|at| v > at) || b.known.is_some_and(|at| k > at) {
                continue;
            }
            let e = winners.entry(key.clone()).or_insert((v, k));
            *e = (*e).max((v, k));
        }
    }
    let (valid_type, known_type) = (&types[n], &types[n + 1]);
    let mut tests = vec![Predicate::In {
        columns: columns.clone(),
        values: winners.keys().cloned().collect(),
    }];
    let bound = |c: &str, t: &arrow::datatypes::DataType, at: i64| -> Result<Predicate> {
        Ok(Predicate::Between {
            column: c.to_string(),
            lo: encode_ts(bellwether_core::encoding::ts_min(t)?, t)?,
            hi: encode_ts(at, t)?,
        })
    };
    if let Some(at) = b.valid {
        tests.push(bound(&b.valid_column, valid_type, at)?);
    }
    if let Some(at) = b.known {
        tests.push(bound(&b.known_column, known_type, at)?);
    }
    let per_key = |clock: &str, t: &arrow::datatypes::DataType, pick: fn(&(i64, i64)) -> i64| {
        Ok::<_, Error>(Predicate::NotOlderThan {
            columns: columns.clone(),
            ts_column: clock.to_string(),
            bounds: winners
                .iter()
                .map(|(k, w)| Ok((k.clone(), encode_ts(pick(w), t)?)))
                .collect::<Result<Vec<_>>>()?,
        })
    };
    tests.push(per_key(&b.valid_column, valid_type, |w| w.0)?);
    tests.push(per_key(&b.known_column, known_type, |w| w.1)?);
    Ok(tests)
}

pub(crate) fn load_workload(
    engine: &Engine,
    cache: &Cache<ObjectStoreBackend>,
    hash: &Hash,
) -> Result<Workload> {
    Workload::load(cache, hash).map_err(|e| match e {
        Error::NotFound(h) if h == *hash => Error::RecordNotFound {
            kind: HashKind::Workload,
            hash: *hash,
            place: format!("the cache at {}", engine.index_location()),
        },
        other => Error::Other(format!(
            "workload {hash} in the cache at {} cannot be read ({other}); record it again from \
             the keys a slow lookup ran",
            engine.index_location()
        )),
    })
}

/// A rewrite output folder the caller named, canonical.
fn canonical_dir(p: &str) -> Result<String> {
    let invalid = |reason: String| Error::InvalidArgument {
        field: "rewrite",
        reason,
    };
    let c = std::fs::canonicalize(p).map_err(|e| {
        invalid(format!(
            "cannot open the output {p:?} ({e}); pass an existing folder"
        ))
    })?;
    c.to_str()
        .map(ToString::to_string)
        .ok_or_else(|| invalid(format!("{} is not valid UTF-8", c.display())))
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
            columns: None,
        });
    }
    let plan = ScanPlan {
        manifest_version: replayer.manifest_version(),
        entries,
        residual: None,
        bytes,
        row_groups_total: catalog.len() as u64,
        row_groups_selected: count,
        projected_bytes: None,
        order: None,
        rows: None,
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
        // id 37k mod 400 for row k in 0..299: 5 (k 65) and 123 (k 79) in
        // part-000, 77 (k 121) in part-001; min/max prunes nothing.
        common::scattered_corpus(&data, 3, 4, 25, 400);
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

    /// The before+1 gate end to end: a commit that lands between the gate
    /// and the apply makes the apply's commit `before + 2`, so the receipt is
    /// withheld (`concurrent_commit`); the key is built, and a retry finds it
    /// already complete and attests nothing.
    #[test]
    fn a_commit_between_the_gate_and_the_apply_withholds_the_receipt() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        // Ids scattered over row groups (37k mod 400): the files' min/max
        // statistics skip nothing, so the key has a gain to apply.
        common::scattered_corpus(&data, 3, 4, 25, 400);
        let kf = dir.path().join("k.parquet");
        common::write_keys(&kf, "id", &[5, 77, 123]);
        Engine::open(data.to_str().unwrap(), EngineOptions::default())
            .unwrap()
            .index(&[crate::spec::parse("exact:batch").unwrap()])
            .unwrap();
        let (e, wl, p) = setup(&data, &kf);
        test_seam::set_before_apply(|engine| {
            engine
                .index(&[crate::spec::parse("range:ts").unwrap()])
                .unwrap();
        });
        let r = fix_key(&e, p, wl, false);
        test_seam::clear_before_apply();
        assert_eq!(r.manifest_before, "1");
        assert_eq!(
            r.outcome,
            FixOutcome::Refused {
                refusal: Refusal::ConcurrentCommit {
                    expected: 2,
                    committed: 3
                }
            }
        );
        assert_eq!(receipts(&data.join(".bellwether")), 0);
        // The key is built: the current configuration serves it.
        let specs = e.pin().unwrap().unwrap().specs().0;
        assert!(specs.iter().any(|s| s.name() == "exact:id"), "{specs:?}");
        // A retry cannot attest it: the key is complete now, which is
        // checked before the gain gate ...
        let again = fix_key(&e, p, wl, false);
        assert!(
            matches!(
                again.outcome,
                FixOutcome::Refused {
                    refusal: Refusal::NothingToApply { .. }
                }
            ),
            "{:?}",
            again.outcome
        );
        // ... and with the gain gate off the apply finds nothing to do.
        let again = e
            .fix(&FixRequest {
                proposal: Some(p),
                workload: Some(wl),
                allow_no_gain: true,
                ..FixRequest::default()
            })
            .unwrap();
        assert_eq!(
            again.outcome,
            FixOutcome::Refused {
                refusal: Refusal::NothingToApply {
                    sequence: "3".into()
                }
            }
        );
        assert_eq!(receipts(&data.join(".bellwether")), 0);
    }

    /// Review of PR #32, mutant MC and item 6: a commit on a join key's
    /// right dataset that lands between the gate and the apply (the right
    /// certificate verified the manifest before it) is refused
    /// `stale_manifest` before the left commits anything: the key is not
    /// declared, no pair is built, and no receipt is written. The same
    /// proposal verified again then applies.
    #[test]
    fn a_right_commit_between_the_gate_and_the_apply_refuses_before_committing() {
        let dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        let (orders, payables) = (root.join("orders"), root.join("payables"));
        common::scattered_corpus(&orders, 3, 4, 25, 400);
        common::scattered_corpus(&payables, 2, 4, 25, 41);
        let exact = crate::spec::parse("exact:id").unwrap();
        let open = |p: &Path| Engine::open(p.to_str().unwrap(), EngineOptions::default()).unwrap();
        open(&payables).index(std::slice::from_ref(&exact)).unwrap();
        let e = open(&orders);
        e.index(std::slice::from_ref(&exact)).unwrap();
        let kf = root.join("k.parquet");
        common::write_keys(&kf, "id", &[5, 7, 77, 123, 300]);
        let w = e
            .why(&WhyRequest {
                keys: Some(KeyFile::new(&kf)),
                mode: Some(crate::LookupMode::SemiJoin),
                join: Some("payables.id".into()),
                ..WhyRequest::default()
            })
            .unwrap();
        let p = w.recommended.as_ref().unwrap().proposal;
        let wl = w.workload.as_ref().unwrap().hash;
        let right = payables.clone();
        test_seam::set_before_apply(move |_| {
            Engine::open(right.to_str().unwrap(), EngineOptions::default())
                .unwrap()
                .index(&[crate::spec::parse("exact:batch").unwrap()])
                .unwrap();
        });
        let r = fix_key(&e, p, wl, false);
        test_seam::clear_before_apply();
        let root_of = payables.to_str().unwrap();
        assert_eq!(
            r.outcome,
            FixOutcome::Refused {
                refusal: Refusal::StaleManifest {
                    certificate: r.join.as_ref().unwrap().certificate.hash,
                    certified: format!("1 of {root_of}"),
                    current: format!("2 of {root_of}"),
                }
            }
        );
        let index = orders.join(".bellwether");
        assert_eq!(receipts(&index), 0);
        let pin = e.pin().unwrap().unwrap();
        assert_eq!(pin.sequence(), 1, "the left committed nothing");
        assert!(pin
            .planner
            .declared_specs()
            .iter()
            .all(|s| !matches!(s, KeySpec::Join { .. })));
        // Verified again against the right as it is now, it applies.
        let again = fix_key(&e, p, wl, false);
        assert!(
            matches!(again.outcome, FixOutcome::Applied { .. }),
            "{:?}",
            again.outcome
        );
        assert_eq!(receipts(&index), 1);
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

    /// Nine one-day row groups (2026-01-01 to 2026-01-09; three files of
    /// three), four prints each. `A` trades on even days only, so a key or
    /// time index prunes; `B` every day.
    fn daily(data: &Path) {
        use std::sync::Arc;

        use arrow::array::{ArrayRef, Date32Array, Int64Array, StringArray};
        use arrow::array::{RecordBatch, TimestampMicrosecondArray};
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;

        const FIRST: i32 = 20_454; // 2026-01-01
        std::fs::create_dir_all(data).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("day", DataType::Date32, false),
            Field::new("v", DataType::Int64, false),
        ]));
        for f in 0..3 {
            let props = WriterProperties::builder()
                .set_max_row_group_row_count(Some(4))
                .build();
            let file = std::fs::File::create(data.join(format!("part-{f}.parquet"))).unwrap();
            let mut w = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
            for r in 0..3 {
                let d = FIRST + f * 3 + r;
                // A day without A still spans it in its footer min/max
                // ("0" to "B"), so a reader's own statistics read it and a
                // key on symbol is worth building.
                let syms = if (d - FIRST) % 2 == 0 {
                    ["A", "B", "A", "B"]
                } else {
                    ["0", "B", "B", "B"]
                };
                let ts: Vec<i64> = (0..4)
                    .map(|i| i64::from(d) * 86_400_000_000 + i * 1_000_000 + 1)
                    .collect();
                let b = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(StringArray::from(syms.to_vec())) as ArrayRef,
                        Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
                        Arc::new(Date32Array::from(vec![d; 4])),
                        Arc::new(Int64Array::from(vec![i64::from(d); 4])),
                    ],
                )
                .unwrap();
                w.write(&b).unwrap();
                w.flush().unwrap();
            }
            w.close().unwrap();
        }
    }

    /// `fix` on the key `why` recommends verifies and applies under every
    /// lookup kind: verification compares the rows each run asks for (its
    /// plan's residual on both sides), not every row of its keys. A plan
    /// that misses a row group holding a row inside the window still fails.
    #[test]
    #[allow(clippy::too_many_lines, clippy::many_single_char_names)]
    fn fix_applies_why_s_recommendation_under_every_lookup_kind() {
        use crate::TimeWindow;

        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base");
        daily(&base);
        let kf = dir.path().join("k.parquet");
        {
            use std::sync::Arc;

            use arrow::array::{ArrayRef, RecordBatch, StringArray};
            use arrow::datatypes::{DataType, Field, Schema};
            use parquet::arrow::ArrowWriter;
            let ks = Arc::new(Schema::new(vec![Field::new(
                "symbol",
                DataType::Utf8,
                false,
            )]));
            let kb = RecordBatch::try_new(
                Arc::clone(&ks),
                vec![Arc::new(StringArray::from(vec!["A"])) as ArrayRef],
            )
            .unwrap();
            let mut w =
                ArrowWriter::try_new(std::fs::File::create(&kf).unwrap(), ks, None).unwrap();
            w.write(&kb).unwrap();
            w.close().unwrap();
        }
        let s = |v: &str| Some(v.to_string());
        let window = |from: &str, to: &str| {
            Some(TimeWindow {
                from: from.into(),
                to: to.into(),
            })
        };
        let cases: Vec<(&str, WhyRequest, bool)> = vec![
            ("locate", WhyRequest::default(), false),
            (
                "latest",
                WhyRequest {
                    latest_by: s("ts"),
                    ..WhyRequest::default()
                },
                false,
            ),
            (
                "asof",
                WhyRequest {
                    latest_by: s("ts"),
                    at: s("2026-01-06"),
                    ..WhyRequest::default()
                },
                true,
            ),
            (
                "latest lookback",
                WhyRequest {
                    latest_by: s("ts"),
                    lookback: s("2d"),
                    ..WhyRequest::default()
                },
                true,
            ),
            (
                "asof lookback",
                WhyRequest {
                    latest_by: s("ts"),
                    at: s("2026-01-06"),
                    lookback: s("2d"),
                    ..WhyRequest::default()
                },
                true,
            ),
            (
                "window --ts day",
                WhyRequest {
                    window: window("2026-01-03", "2026-01-05"),
                    ts_column: s("day"),
                    ..WhyRequest::default()
                },
                true,
            ),
            (
                "asof lookback --ts ts",
                WhyRequest {
                    latest_by: s("ts"),
                    at: s("2026-01-06"),
                    lookback: s("2d"),
                    ts_column: s("ts"),
                    ..WhyRequest::default()
                },
                true,
            ),
        ];
        for (i, (name, req, filtered)) in cases.into_iter().enumerate() {
            let data = dir.path().join(format!("d{i}"));
            std::fs::create_dir_all(&data).unwrap();
            for f in 0..3 {
                let n = format!("part-{f}.parquet");
                std::fs::copy(base.join(&n), data.join(&n)).unwrap();
            }
            let e = Engine::open(data.to_str().unwrap(), EngineOptions::default()).unwrap();
            let w = e
                .why(&WhyRequest {
                    keys: Some(KeyFile::new(&kf)),
                    ..req
                })
                .unwrap();
            let rec = w
                .recommended
                .as_ref()
                .unwrap_or_else(|| panic!("{name}: no rec"));
            let wl = w.workload.as_ref().unwrap().hash;
            if name == "window --ts day" {
                // A plan missing the 2026-01-03 row group (part-0's only
                // one in the window), which holds A's rows inside the
                // window, fails the certificate.
                test_seam::set(|sel| {
                    sel.retain(|p, _| !p.ends_with("part-0.parquet"));
                });
                let bad = fix_key(&e, rec.proposal, wl, true);
                test_seam::clear();
                assert!(
                    matches!(
                        bad.outcome,
                        FixOutcome::Refused {
                            refusal: Refusal::CertificateFailed { .. }
                        }
                    ),
                    "{name}: {:?}",
                    bad.outcome
                );
            }
            let r = fix_key(&e, rec.proposal, wl, false);
            assert!(
                matches!(r.outcome, FixOutcome::Applied { .. }),
                "{name}: {:?} {:?}",
                r.outcome,
                r.verification
            );
            let v = r.verification.unwrap();
            assert!(v.pass, "{name}");
            assert_eq!(v.row_filter.is_some(), filtered, "{name}");
            // The certificate cites the union plan; the filter comes from
            // the workload (cited by its coverage) and the left side's data.
            let Some(Written {
                record: Record::Certificate(c),
                ..
            }) = &r.certificate
            else {
                panic!("{name}: no certificate")
            };
            let plans = c
                .provenance
                .inputs
                .iter()
                .filter(|h| {
                    matches!(
                        e.ledger().unwrap().get(h),
                        Ok(Record::Evidence(Evidence {
                            body: EvidenceBody::Plan { .. },
                            ..
                        }))
                    )
                })
                .count();
            assert_eq!(plans, 1, "{name}");
        }
    }

    /// How [`plant_bounds`] corrupts `A`'s bounds in its newest row group
    /// (2026-01-09, part-2 row group 2).
    #[derive(Clone, Copy, Debug)]
    enum Plant {
        /// Its time dropped (the all-null sentinel): the index's newest
        /// time for `A` is two days too old.
        TooOld,
        /// Its maximum raised five days: the index's newest is too new.
        TooNew,
    }

    /// Commits the `exact:symbol@ts` segment for the [`daily`] corpus over
    /// `e`'s index in place of the one `index` built, `A`'s per-key bounds
    /// corrupted by `plant` (its membership kept).
    #[allow(
        clippy::many_single_char_names,
        clippy::cast_possible_wrap,
        clippy::default_trait_access
    )]
    fn plant_bounds(e: &Engine, plant: Option<Plant>) {
        use bellwether_core::encoding::{encode_value_as, Value};
        use bellwether_core::segment::{SegmentWriter, WriterOptions};
        use bellwether_store::{commit, RowGroupRange, SegmentRef, StagedSegments};
        use bellwether_types::RowGroupId;

        const FIRST: i64 = 20_454;
        const DAY: i64 = 86_400_000_000;
        let store = e.store().unwrap();
        let m = bellwether_store::Snapshot::open(Arc::clone(store), "")
            .unwrap()
            .manifest()
            .clone();
        let spec = crate::spec::parse("exact:symbol@ts").unwrap();
        let schema = e.pin().unwrap().unwrap().planner.schema().clone();
        let mut w =
            SegmentWriter::for_dataset(spec.clone(), &schema, WriterOptions::default()).unwrap();
        let enc = |s: &str| {
            let mut out = Vec::new();
            encode_value_as(&Value::Utf8(s), &arrow::datatypes::DataType::Utf8, &mut out).unwrap();
            out
        };
        let mut timed = |key: &str, rg: RowGroupId, lo: Option<i64>, hi: Option<i64>| {
            w.insert_timed(&enc(key), rg, lo).unwrap();
            w.insert_timed(&enc(key), rg, hi).unwrap();
        };
        let (mut lo_id, mut hi_id) = (u32::MAX, 0);
        for f in &m.rowgroup_space.files {
            let name = file_name(&f.fingerprint.path);
            let fi: i64 = name["part-".len()..name.len() - ".parquet".len()]
                .parse()
                .unwrap();
            for (r, id) in f.rowgroup_ids.iter().enumerate() {
                lo_id = lo_id.min(id.0);
                hi_id = hi_id.max(id.0);
                let d = fi * 3 + r as i64;
                let t = |i: i64| (FIRST + d) * DAY + i * 1_000_000 + 1;
                if d % 2 == 0 {
                    let (lo, hi) = (t(0), t(2));
                    match (plant, d) {
                        (Some(Plant::TooOld), 8) => timed("A", *id, None, None),
                        (Some(Plant::TooNew), 8) => {
                            timed("A", *id, Some(lo), Some(hi + 5 * DAY));
                        }
                        _ => timed("A", *id, Some(lo), Some(hi)),
                    }
                    timed("B", *id, Some(t(1)), Some(t(3)));
                } else {
                    timed("B", *id, Some(t(0)), Some(t(3)));
                }
            }
        }
        w.set_rowgroup_range(RowGroupId(lo_id)..RowGroupId(hi_id + 1))
            .unwrap();
        let seg = w.finish().unwrap();
        let mut staged = StagedSegments::new();
        staged.stage(bytes::Bytes::from(seg.bytes.clone()));
        let identity = spec.identity();
        commit(&**store, "", &staged, |cur| {
            let mut next = cur.clone();
            next.segments
                .retain(|s| !(s.kind.is_none() && s.key_spec == identity));
            next.segments.push(SegmentRef {
                id: seg.id,
                kind: None,
                key_spec: identity.clone(),
                rowgroup_range: RowGroupRange {
                    start: RowGroupId(lo_id),
                    end: RowGroupId(hi_id + 1),
                    extra: Default::default(),
                },
                bytes: seg.bytes.len() as u64,
                join: None,
                extra: Default::default(),
            });
            Ok(next)
        })
        .unwrap();
    }

    /// Verify takes a `latest` lookback's per-key windows from the data,
    /// not from the index under test: a bounds sidecar whose newest time
    /// for `A` is too old, or too new, plans other row groups than A's
    /// window in the data needs, and the certificate fails. With correct
    /// bounds the same steps pass.
    #[test]
    #[allow(clippy::cast_possible_wrap)]
    fn a_bounds_sidecar_that_disagrees_with_the_data_fails_verification() {
        use std::sync::Arc;

        use arrow::array::{ArrayRef, RecordBatch, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use parquet::arrow::ArrowWriter;

        let dir = tempfile::tempdir().unwrap();
        let kf = dir.path().join("k.parquet");
        let ks = Arc::new(Schema::new(vec![Field::new(
            "symbol",
            DataType::Utf8,
            false,
        )]));
        let kb = RecordBatch::try_new(
            Arc::clone(&ks),
            vec![Arc::new(StringArray::from(vec!["A"])) as ArrayRef],
        )
        .unwrap();
        let mut w = ArrowWriter::try_new(std::fs::File::create(&kf).unwrap(), ks, None).unwrap();
        w.write(&kb).unwrap();
        w.close().unwrap();
        for plant in [None, Some(Plant::TooOld), Some(Plant::TooNew)] {
            let data = dir.path().join(format!("{plant:?}"));
            daily(&data);
            let e = Engine::open(data.to_str().unwrap(), EngineOptions::default()).unwrap();
            e.index_declared(&[crate::spec::parse_declaration("exact:symbol@ts").unwrap()])
                .unwrap();
            plant_bounds(&e, plant);
            let why = e
                .why(&WhyRequest {
                    keys: Some(KeyFile::new(&kf)),
                    latest_by: Some("ts".into()),
                    lookback: Some("2d".into()),
                    ..WhyRequest::default()
                })
                .unwrap();
            let wl = why.workload.as_ref().unwrap().hash;
            let ev = why.evidence.hash.unwrap();
            // A bloom key beside the exact one: the lookback still plans
            // through the (planted) bounds sidecar.
            let crate::ProposeResult::Written { proposal } = e
                .propose(
                    ProposalBody::Key {
                        spec: crate::spec::parse("bloom:symbol@0.01").unwrap(),
                        sidecars: Vec::new(),
                    },
                    &[ev],
                    "test",
                )
                .unwrap()
            else {
                panic!("not written")
            };
            let r = fix_key(&e, proposal.hash, wl, true);
            let v = r.verification.as_ref().unwrap();
            match plant {
                None => assert!(
                    matches!(r.outcome, FixOutcome::Verified) && v.pass,
                    "{:?}",
                    r.outcome
                ),
                Some(_) => assert!(
                    matches!(
                        r.outcome,
                        FixOutcome::Refused {
                            refusal: Refusal::CertificateFailed { mismatched: 1, .. }
                        }
                    ),
                    "{plant:?}: {:?} {v:?}",
                    r.outcome
                ),
            }
        }
    }

    /// The boundary of an `asof` lookback: `A` has rows at exactly
    /// `T − 2d`, at `T − 1d`, and at `T + 1d`, each in its own file. Priced
    /// with `--at T --lookback 2d`, the recommended key's plan holds the
    /// first two; a plan missing the row at exactly `T − 2d` (the window's
    /// inclusive start) fails the certificate, and the real plan passes.
    #[test]
    fn a_plan_missing_the_row_at_the_lookback_start_fails_verification() {
        use std::sync::Arc;

        use arrow::array::TimestampMicrosecondArray;
        use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use parquet::arrow::ArrowWriter;

        const DAY: i64 = 86_400_000_000;
        // 2026-01-05T00:00:00Z in microseconds.
        const T: i64 = 20_458 * DAY;
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        std::fs::create_dir_all(&data).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("v", DataType::Int64, false),
        ]));
        for (name, sym, ts) in [
            ("a-start.parquet", "A", T - 2 * DAY),
            ("b-inside.parquet", "A", T - DAY),
            ("c-after.parquet", "A", T + DAY),
            ("d-other.parquet", "B", T - DAY),
        ] {
            let b = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(vec![sym])) as ArrayRef,
                    Arc::new(TimestampMicrosecondArray::from(vec![ts]).with_timezone("UTC")),
                    Arc::new(Int64Array::from(vec![ts])),
                ],
            )
            .unwrap();
            let f = std::fs::File::create(data.join(name)).unwrap();
            let mut w = ArrowWriter::try_new(f, Arc::clone(&schema), None).unwrap();
            w.write(&b).unwrap();
            w.close().unwrap();
        }
        let kf = dir.path().join("k.parquet");
        let ks = Arc::new(Schema::new(vec![Field::new(
            "symbol",
            DataType::Utf8,
            false,
        )]));
        let kb = RecordBatch::try_new(
            Arc::clone(&ks),
            vec![Arc::new(StringArray::from(vec!["A"])) as ArrayRef],
        )
        .unwrap();
        let mut w = ArrowWriter::try_new(std::fs::File::create(&kf).unwrap(), ks, None).unwrap();
        w.write(&kb).unwrap();
        w.close().unwrap();
        let e = Engine::open(data.to_str().unwrap(), EngineOptions::default()).unwrap();
        let why = e
            .why(&WhyRequest {
                keys: Some(KeyFile::new(&kf)),
                latest_by: Some("ts".into()),
                at: Some("2026-01-05T00:00:00Z".into()),
                lookback: Some("2d".into()),
                ..WhyRequest::default()
            })
            .unwrap();
        // An unserved asof run is bounded by its keys only, never by an
        // instant (A is in every file), so the temporal floor is recommended.
        let rec = why.recommended.as_ref().expect("a recommendation");
        let floor = rec.proposal;
        let wl = why.workload.as_ref().unwrap().hash;
        test_seam::set(|sel| {
            assert!(
                sel.keys().any(|p| p.ends_with("a-start.parquet")),
                "{sel:?}"
            );
            sel.retain(|p, _| !p.ends_with("a-start.parquet"));
        });
        let bad = fix_key(&e, floor, wl, true);
        test_seam::clear();
        assert!(
            matches!(
                bad.outcome,
                FixOutcome::Refused {
                    refusal: Refusal::CertificateFailed { .. }
                }
            ),
            "{:?} {:?}",
            bad.outcome,
            bad.verification
        );
        let ok = fix_key(&e, floor, wl, true);
        assert!(
            matches!(ok.outcome, FixOutcome::Verified),
            "{:?}",
            ok.outcome
        );
    }
}
