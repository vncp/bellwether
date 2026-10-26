//! `fix` on a rewrite of engine queries (ADR 033): score in the engine,
//! verify on exports, attest only.
//!
//! 1. **Score.** Every query of every pair is submitted to the engine under
//!    a tag this call chose, read-only; each one's statistics are read
//!    from the engine's own query log by that tag. The Replay holds them
//!    (`exact: false`, `model: <engine>.query_log`) beside the twin's
//!    prediction for the key sample, and is written before anything else,
//!    so a failing certificate keeps the prediction error.
//! 2. **Verify.** Each pair's before and after results are exported as
//!    Parquet into a fresh folder of the caller's scratch folder and
//!    reconciled by the proposal's key columns (latest-by and exact totals
//!    as for any reconcile). One Certificate covers every pair and cites
//!    the engine's identity before scoring and after the exports. No
//!    export, no certificate, no receipt: an export the engine refuses is
//!    [`Refusal::ExportDenied`], naming the permission. The exports are
//!    deleted once the certificate is written, unless `keep`.
//! 3. **Attest.** A passing certificate against the current manifest gets
//!    an attest-only Receipt (`attest_only`, `manifest_after ==
//!    manifest_before`) binding the twin Evidence, the proposal, the
//!    measured Replay, the certificate, and the prediction. Nothing is
//!    deployed: [`FixOutcome::Attested`] names the queries a human deploys.

use std::path::{Path, PathBuf};
use std::time::Duration;

use bellwether_ledger::RecordKind;
use bellwether_source::ParquetSource;
use bellwether_source_engine::{
    check_query, ratio, EngineAdapter, EngineIdentity, Exported, ResultHandle, Tag,
};
use bellwether_types::{
    format, CertBody, Certificate, Coverage, Error, Evidence, EvidenceBody, Hash, Prediction,
    Proposal, ProposalBody, Provenance, QueryStats, Receipt, Record, Replay, Result, RewriteShape,
    RunCost, TwinOf,
};
use bellwether_verify::{CertificateContext, Options, ReconcileSpec, Reconciliation, Side};
use serde::{Deserialize, Serialize};

use crate::engine::Engine;
use crate::fix::{FixOutcome, FixRequest, FixResult, Refusal, Score};
use crate::results::{total, Written};

/// How `fix` compares the before and after results of a rewrite of engine
/// queries; the key columns are the proposal's. Where the exports go is the
/// engine's configuration (`EngineOptions::scratch`), never a request's.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineCompare {
    /// Compare the newest version per key by this column.
    #[serde(default)]
    pub latest_by: Option<String>,
    /// Columns left out of the row hash.
    #[serde(default)]
    pub ignore: Vec<String>,
    /// Columns totalled exactly (`col` decimal, `col:dec(S)` float).
    #[serde(default)]
    pub totals: Vec<String>,
    /// Columns grouping the totals.
    #[serde(default)]
    pub group_by: Vec<String>,
}

/// What a fix in a query engine did there, pair by pair.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EngineRun {
    /// The engine root (credentials removed).
    pub engine: String,
    /// The cost model of every measured number: `<engine>.query_log`.
    pub model: String,
    /// The engine's identity before scoring.
    pub identity_before: EngineIdentity,
    /// Its identity after the exports, when they ran.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_after: Option<EngineIdentity>,
    /// Each pair, in the proposal's order.
    pub pairs: Vec<PairRun>,
    /// The twin's prediction beside the measurement (also on the Replay).
    pub prediction: Prediction,
    /// The folder of this run's exports, when exports ran.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scratch: Option<String>,
    /// Whether the exports were kept (`keep`); otherwise they are deleted
    /// once the certificate is written.
    pub kept: bool,
}

/// One pair's measurement and verification.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PairRun {
    /// The before query's statistics from the engine's log.
    pub before: QueryStats,
    /// The after query's.
    pub after: QueryStats,
    /// The exports, when they ran: files and bytes per side.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exported: Option<[Exported; 2]>,
    /// The pair's reconciliation, when it ran.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification: Option<Reconciliation>,
}

/// The twin Evidence a rewrite of engine queries cites: the first cited
/// Evidence whose profile records a twin.
pub(crate) fn cited_twin(engine: &Engine, proposal: &Proposal) -> Result<(Hash, TwinOf)> {
    for h in &proposal.evidence {
        if let Ok(w) = engine.get(h) {
            if let Record::Evidence(Evidence {
                body: EvidenceBody::Profile { twin: Some(t), .. },
                ..
            }) = w.record
            {
                return Ok((*h, t));
            }
        }
    }
    Err(Error::InvalidArgument {
        field: "evidence",
        reason: "a rewrite of engine queries cites the twin diagnosis it rests on: run `why` \
                 with the engine named (--twin-of) and cite the evidence it prints"
            .into(),
    })
}

/// Checks a rewrite of engine queries as data: pairs of non-empty queries
/// and the key columns their results are reconciled by.
pub(crate) fn check_pairs(shape: &RewriteShape) -> Result<()> {
    if shape.key.is_empty() {
        return Err(Error::InvalidArgument {
            field: "key_columns",
            reason: "a rewrite of engine queries names the key columns its before and after \
                     results are reconciled by"
                .into(),
        });
    }
    for p in &shape.pairs {
        check_query(&p.before)?;
        check_query(&p.after)?;
    }
    Ok(())
}

fn stats_cost(s: &QueryStats, keys: u64) -> RunCost {
    let units = s.units_read.unwrap_or(0);
    RunCost {
        run_id: s.tag.clone(),
        keys,
        files: units,
        row_groups: units,
        bytes: s.read_bytes,
        exact: false,
        measured: Some(s.clone()),
    }
}

fn prediction(
    evidence: Hash,
    twin: &TwinOf,
    before: &[QueryStats],
    after: &[QueryStats],
) -> Prediction {
    let sum = |v: &[QueryStats], f: fn(&QueryStats) -> u64| v.iter().map(f).sum::<u64>();
    let (br, ar) = (sum(before, |s| s.read_rows), sum(after, |s| s.read_rows));
    Prediction {
        evidence,
        predicted_rows: twin.predicted_rows,
        predicted_row_groups: twin.predicted_row_groups,
        before_read_rows: br,
        after_read_rows: ar,
        before_read_bytes: sum(before, |s| s.read_bytes),
        after_read_bytes: sum(after, |s| s.read_bytes),
        before_ratio: ratio(br, twin.predicted_rows),
        after_ratio: ratio(ar, twin.predicted_rows),
    }
}

/// The tag of pair `i`'s `side` in run `run`.
fn tag(run: &str, i: usize, side: &str) -> Result<Tag> {
    Tag::new(run, &format!("p{i}-{side}"))
}

/// What stage 1 hands on: the written Replay, each pair's statistics, the
/// result handles to export, and the prediction error.
type Scored = (
    Written,
    Vec<PairRun>,
    Vec<(ResultHandle, ResultHandle)>,
    Prediction,
);

impl Engine {
    /// The engine adapter for a proposal's twin: `twin_of` must name the
    /// engine the twin Evidence was made against.
    fn engine_for(&self, twin: &TwinOf, scratch: Option<&Path>) -> Result<Box<dyn EngineAdapter>> {
        let named = self
            .options()
            .twin_of
            .as_deref()
            .map(bellwether_source_engine::redact);
        match named {
            Some(n) if n == twin.engine => self.twin_adapter(scratch),
            Some(n) => Err(Error::InvalidArgument {
                field: "twin_of",
                reason: format!(
                    "the proposal's twin evidence is about {}, and this run names {n}; pass \
                     --twin-of {} (with its credentials)",
                    twin.engine, twin.engine
                ),
            }),
            None => Err(Error::InvalidArgument {
                field: "twin_of",
                reason: format!(
                    "this proposal rewrites queries in {}; name that engine with --twin-of {}",
                    twin.engine, twin.engine
                ),
            }),
        }
    }

    /// Stage 1 (module docs): submits every query, reads every tag's
    /// statistics, and writes the Replay.
    #[allow(clippy::too_many_arguments)]
    fn score_in_engine(
        &self,
        engine: &dyn EngineAdapter,
        proposal_hash: &Hash,
        shape: &RewriteShape,
        evidence: Hash,
        twin: &TwinOf,
        identity: &EngineIdentity,
        manifest: u64,
        run: &str,
    ) -> Result<Scored> {
        let mut handles = Vec::with_capacity(shape.pairs.len());
        for (i, p) in shape.pairs.iter().enumerate() {
            let b = engine.submit(&p.before, &tag(run, i, "before")?)?;
            let a = engine.submit(&p.after, &tag(run, i, "after")?)?;
            handles.push((b, a));
        }
        let mut pairs = Vec::with_capacity(handles.len());
        for (b, a) in &handles {
            pairs.push(PairRun {
                before: engine.stats(&b.tag)?,
                after: engine.stats(&a.tag)?,
                exported: None,
                verification: None,
            });
        }
        let before: Vec<QueryStats> = pairs.iter().map(|p| p.before.clone()).collect();
        let after: Vec<QueryStats> = pairs.iter().map(|p| p.after.clone()).collect();
        let prediction = prediction(evidence, twin, &before, &after);
        let mut inputs = vec![identity.hash, evidence];
        for p in &shape.pairs {
            inputs.push(Hash::of(p.before.as_bytes()));
            inputs.push(Hash::of(p.after.as_bytes()));
        }
        let replay = Replay {
            record_version: format::RECORD_VERSION,
            proposal: *proposal_hash,
            manifest_version: manifest.to_string(),
            workload: shape.keys.or(twin.workload).unwrap_or(evidence),
            inputs,
            current: before.iter().map(|s| stats_cost(s, twin.keys)).collect(),
            proposed: after.iter().map(|s| stats_cost(s, twin.keys)).collect(),
            model: Some(engine.model().to_string()),
            prediction: Some(prediction.clone()),
            created_at: self.now(),
        };
        let written = self.put(Record::Replay(replay))?;
        Ok((written, pairs, handles, prediction))
    }

    /// `fix` on a rewrite of engine queries (module docs).
    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
    pub(crate) fn fix_engine_rewrite(
        &self,
        mut result: FixResult,
        proposal_hash: &Hash,
        proposal: &Proposal,
        shape: &RewriteShape,
        req: &FixRequest,
        manifest: u64,
        anchor: Option<&bellwether_verify::ManifestAnchor>,
    ) -> Result<FixResult> {
        check_pairs(shape)?;
        result.invariant = Some(shape.invariant.clone());
        let (evidence, twin) = cited_twin(self, proposal)?;
        let verify = req.engine.clone().unwrap_or_default();
        let given = self.options().scratch.as_deref().ok_or(Error::NoWorkload {
            proposal: "rewrite",
            field: "scratch",
            needs: "a scratch folder the engine's exports are written to",
        })?;
        let keep = self.options().keep_exports;
        let scratch = scratch_root(&twin.engine, given)?;
        let engine = self.engine_for(&twin, Some(&scratch))?;
        let identity_before = Engine::twin_identity(&*engine)?;
        let run = Tag::run();
        let (replay, mut pairs, handles, prediction) = self.score_in_engine(
            &*engine,
            proposal_hash,
            shape,
            evidence,
            &twin,
            &identity_before,
            manifest,
            &run,
        )?;
        let replay_hash = replay.hash;
        if let Record::Replay(r) = &replay.record {
            result.score = Some(Score::Measured {
                current: total(&r.current),
                proposed: total(&r.proposed),
                model: engine.model().to_string(),
                replay: Box::new(replay.clone()),
            });
        }
        let mut run_info = EngineRun {
            engine: engine.root().to_string(),
            model: engine.model().to_string(),
            identity_before,
            identity_after: None,
            pairs: Vec::new(),
            prediction,
            scratch: None,
            kept: keep,
        };
        // The gain gate, on the bytes the engine read, the measure `why`
        // recommends by and the one a rerun of the same query on the same
        // tables reproduces (peak memory, CPU, and duration are one noisy
        // sample each and never pass a rewrite).
        let sum = |f: fn(&PairRun) -> u64| pairs.iter().map(f).sum::<u64>();
        let (current_bytes, proposed_bytes) =
            (sum(|p| p.before.read_bytes), sum(|p| p.after.read_bytes));
        let gain = proposed_bytes < current_bytes;
        if !gain && !req.allow_no_gain && !req.verify_only {
            run_info.pairs = pairs;
            result.engine = Some(run_info);
            result.outcome = FixOutcome::Refused {
                refusal: Refusal::NothingToGain {
                    replay: replay_hash,
                    current_bytes,
                    proposed_bytes,
                },
            };
            return Ok(result);
        }
        // 2. Verify, on exports.
        let dir = scratch.join(format!("bellwether-{run}"));
        run_info.scratch = Some(dir.display().to_string());
        let verified = self.export_and_reconcile(
            &*engine,
            shape,
            &verify,
            &handles,
            &mut pairs,
            &dir,
            proposal_hash,
            anchor,
        );
        if !keep {
            let _ = std::fs::remove_dir_all(&dir);
        }
        let certified = match verified {
            Ok(c) => c,
            Err(Error::ExportDenied {
                engine: e,
                permission,
                message,
            }) => {
                run_info.pairs = pairs;
                run_info.scratch = None;
                result.engine = Some(run_info);
                result.outcome = FixOutcome::Refused {
                    refusal: Refusal::ExportDenied {
                        engine: e,
                        permission,
                        message,
                    },
                };
                return Ok(result);
            }
            Err(e) => return Err(e),
        };
        // The engine's identity after the exports: a write that landed
        // during the run would mix two states of its tables.
        let identity_after = {
            let fresh = self.engine_for(&twin, Some(&scratch))?;
            Engine::twin_identity(&*fresh)?
        };
        run_info.identity_after = Some(identity_after);
        if identity_after.hash != identity_before.hash {
            return Err(Error::SourceChanged {
                root: engine.root().to_string(),
                units: Vec::new(),
                message: format!(
                    "{} changed during the run (its identity went from {} to {}: {} to {} \
                     units, {} to {} rows), so the before and after results may come from two \
                     states of its tables and are not certified; run fix again when the tables \
                     are quiet. The measured replay {replay_hash} stands",
                    engine.root(),
                    identity_before.hash,
                    identity_after.hash,
                    identity_before.units,
                    identity_after.units,
                    identity_before.rows,
                    identity_after.rows
                ),
            });
        }
        let cert = self.compose_certificate(
            proposal_hash,
            &certified,
            &identity_before,
            &identity_after,
            replay_hash,
        )?;
        result.verification = certified.first().map(|c| c.outcome.clone());
        run_info.pairs = pairs;
        result.engine = Some(run_info);
        let Record::Certificate(certificate) = &cert.record else {
            return Err(Error::Other(
                "a composed certificate is not a certificate".into(),
            ));
        };
        let certificate = certificate.clone();
        result.certificate = Some(cert.clone());
        if !certificate.pass {
            result.outcome = FixOutcome::Refused {
                refusal: Refusal::CertificateFailed {
                    certificate: cert.hash,
                    mismatched: certificate.body.mismatched,
                    missing_left: certificate.body.missing_left,
                    missing_right: certificate.body.missing_right,
                    totals_differ: certificate.body.totals_delta.len() as u64,
                },
            };
            return Ok(result);
        }
        if req.verify_only {
            result.signatures = self.sign_written(&[(cert.hash, RecordKind::Certificate)], None)?;
            result.outcome = FixOutcome::Verified;
            return Ok(result);
        }
        (result.outcome, result.signatures) = self.attest(
            proposal,
            proposal_hash,
            shape,
            &replay,
            &cert.hash,
            true,
            manifest,
        )?;
        Ok(result)
    }

    /// Exports and reconciles every pair; the certified outcomes, one per
    /// pair.
    #[allow(clippy::too_many_arguments)]
    fn export_and_reconcile(
        &self,
        engine: &dyn EngineAdapter,
        shape: &RewriteShape,
        verify: &EngineCompare,
        handles: &[(ResultHandle, ResultHandle)],
        pairs: &mut [PairRun],
        dir: &Path,
        proposal: &Hash,
        anchor: Option<&bellwether_verify::ManifestAnchor>,
    ) -> Result<Vec<bellwether_verify::Certified>> {
        let mut out = Vec::with_capacity(handles.len());
        for (i, ((b, a), pair)) in handles.iter().zip(pairs.iter_mut()).enumerate() {
            let (db, da) = (
                dir.join(format!("p{i}")).join("before"),
                dir.join(format!("p{i}")).join("after"),
            );
            let eb = engine.export(b, &db)?;
            let ea = engine.export(a, &da)?;
            pair.exported = Some([eb, ea]);
            let opts = compare_spec(shape, verify)?;
            let ctx = CertificateContext {
                proposal: Some(*proposal),
                manifest: anchor.cloned(),
                inputs: Vec::new(),
                clock: self.options().clock.clone(),
            };
            let (rb, ra) = (path_str(&db)?, path_str(&da)?);
            let (sb, sa) = (ParquetSource::new(), ParquetSource::new());
            let certified = self
                .reconciler(opts, Options::default())
                .and_then(|r| {
                    r.certify(&Side::new(&sb, &rb), &Side::new(&sa, &ra), &ctx)
                        .map_err(Error::from)
                })
                .map_err(|e| {
                    // The comparison names "left" and "right"; say which
                    // results those are and what columns they hold.
                    let cols = bellwether_types::Source::schema(&sb, &rb).map_or_else(
                        |_| "unreadable".to_string(),
                        |s| {
                            s.fields()
                                .iter()
                                .map(|f| f.name().as_str())
                                .collect::<Vec<_>>()
                                .join(", ")
                        },
                    );
                    let context = |m: String| {
                        format!(
                            "comparing pair p{i}'s exported results (left: before, right: after; \
                             columns {cols}): {m}"
                        )
                    };
                    match e {
                        Error::KeySpec(m) => Error::KeySpec(context(m)),
                        Error::Other(m) => Error::Other(context(m)),
                        other => other,
                    }
                })?;
            pair.verification = Some(certified.outcome.clone());
            out.push(certified);
        }
        Ok(out)
    }

    /// One Certificate over every pair (module docs).
    fn compose_certificate(
        &self,
        proposal: &Hash,
        certified: &[bellwether_verify::Certified],
        before: &EngineIdentity,
        after: &EngineIdentity,
        replay: Hash,
    ) -> Result<Written> {
        let first = certified
            .first()
            .ok_or_else(|| Error::Other("a rewrite of engine queries has no pair".into()))?;
        let mut body = CertBody {
            missing_right: 0,
            missing_left: 0,
            mismatched: 0,
            ignored_columns: Vec::new(),
            totals_delta: Vec::new(),
        };
        let mut inputs = vec![before.hash, after.hash, replay];
        let mut age = Duration::ZERO;
        for (i, c) in certified.iter().enumerate() {
            let b = &c.certificate.body;
            body.missing_right += b.missing_right;
            body.missing_left += b.missing_left;
            body.mismatched += b.mismatched;
            for col in &b.ignored_columns {
                if !body.ignored_columns.contains(col) {
                    body.ignored_columns.push(col.clone());
                }
            }
            for (g, l, r) in &b.totals_delta {
                let g = if certified.len() > 1 {
                    format!("pair {i}: {g}")
                } else {
                    g.clone()
                };
                body.totals_delta.push((g, l.clone(), r.clone()));
            }
            inputs.extend(c.certificate.provenance.inputs.iter().copied());
            age = age.max(c.certificate.provenance.oldest_input_age);
        }
        let pass = certified.iter().all(|c| c.certificate.pass);
        let certificate = Certificate {
            record_version: format::RECORD_VERSION,
            proposal: Some(*proposal),
            coverage: Coverage::Full,
            pass,
            body,
            provenance: Provenance {
                inputs,
                oldest_input_age: age,
                manifest_version: first.certificate.provenance.manifest_version.clone(),
                created_at: self.now(),
            },
        };
        self.put(Record::Certificate(certificate))
    }

    /// The attest-only Receipt, and the signatures over what this call
    /// wrote.
    #[allow(clippy::too_many_arguments)]
    fn attest(
        &self,
        proposal: &Proposal,
        proposal_hash: &Hash,
        shape: &RewriteShape,
        replay: &Written,
        cert: &Hash,
        certificate_written: bool,
        manifest: u64,
    ) -> Result<(FixOutcome, Vec<crate::fix::SignatureWritten>)> {
        let Record::Replay(r) = &replay.record else {
            return Err(Error::Other("a replay written is not a replay".into()));
        };
        let receipt = Receipt {
            record_version: format::RECORD_VERSION,
            evidence: proposal.evidence.clone(),
            proposal: *proposal_hash,
            replay: replay.hash,
            certificate: *cert,
            manifest_before: manifest.to_string(),
            manifest_after: manifest.to_string(),
            created_at: self.now(),
            attest_only: true,
            prediction: r.prediction.clone(),
        };
        let now = self.manifest()?.map_or(0, |m| m.sequence);
        let hash = self.ledger()?.put_receipt(&receipt, &now.to_string())?;
        let mut signed = vec![(hash, RecordKind::Receipt)];
        if certificate_written {
            signed.push((*cert, RecordKind::Certificate));
        }
        let signatures = self.sign_written(&signed, None)?;
        let target = cited_twin(self, proposal)
            .map(|(_, t)| t.engine)
            .unwrap_or_default();
        Ok((
            FixOutcome::Attested {
                receipt: Box::new(Written {
                    hash,
                    record: Record::Receipt(receipt),
                }),
                target,
                deploy: shape.pairs.iter().map(|p| p.after.clone()).collect(),
            },
            signatures,
        ))
    }

    /// The apply tier on a rewrite of engine queries: the certificate is
    /// gated as any other, the queries are scored again now (a Replay is
    /// written only by the call that measured it), the engine must still be
    /// in the state the certificate verified, and the receipt attests.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn apply_engine_rewrite(
        &self,
        mut result: FixResult,
        cert_hash: &Hash,
        certificate: &Certificate,
        proposal_hash: &Hash,
        proposal: &Proposal,
        shape: &RewriteShape,
        manifest: u64,
        dry_run: bool,
    ) -> Result<FixResult> {
        result.invariant = Some(shape.invariant.clone());
        let (evidence, twin) = cited_twin(self, proposal)?;
        let engine = self.engine_for(&twin, None)?;
        let identity = Engine::twin_identity(&*engine)?;
        let certified = certificate.provenance.inputs.get(1).copied();
        if certified != Some(identity.hash) {
            result.outcome = FixOutcome::Refused {
                refusal: Refusal::DataChanged {
                    certificate: *cert_hash,
                    certified,
                    current: identity.hash,
                },
            };
            return Ok(result);
        }
        if dry_run {
            result.outcome = FixOutcome::WouldApply {
                sequence: manifest,
                expected: manifest,
            };
            return Ok(result);
        }
        let (replay, pairs, _, prediction) = self.score_in_engine(
            &*engine,
            proposal_hash,
            shape,
            evidence,
            &twin,
            &identity,
            manifest,
            &Tag::run(),
        )?;
        if let Record::Replay(r) = &replay.record {
            result.score = Some(Score::Measured {
                current: total(&r.current),
                proposed: total(&r.proposed),
                model: engine.model().to_string(),
                replay: Box::new(replay.clone()),
            });
        }
        result.engine = Some(EngineRun {
            engine: engine.root().to_string(),
            model: engine.model().to_string(),
            identity_before: identity,
            identity_after: None,
            pairs,
            prediction,
            scratch: None,
            kept: false,
        });
        (result.outcome, result.signatures) = self.attest(
            proposal,
            proposal_hash,
            shape,
            &replay,
            cert_hash,
            false,
            manifest,
        )?;
        Ok(result)
    }
}

/// Whether a proposal body is a rewrite of engine queries.
pub(crate) fn engine_shape(body: &ProposalBody) -> Option<&RewriteShape> {
    match body {
        ProposalBody::Rewrite { shape } if !shape.pairs.is_empty() => Some(shape),
        _ => None,
    }
}

fn path_str(p: &Path) -> Result<String> {
    p.to_str()
        .map(str::to_string)
        .ok_or_else(|| Error::Other(format!("{} is not valid UTF-8", p.display())))
}

/// The reconcile spec of an engine rewrite: the proposal's key columns,
/// with the request's comparison options.
fn compare_spec(shape: &RewriteShape, verify: &EngineCompare) -> Result<ReconcileSpec> {
    let mut spec = ReconcileSpec::new(shape.key.clone())
        .ignore(verify.ignore.clone())
        .with_totals(crate::reconcile::total_columns(&verify.totals)?)
        .group_by(verify.group_by.clone());
    if let Some(l) = &verify.latest_by {
        spec = spec.latest_by(l.clone());
    }
    Ok(spec)
}

/// The scratch folder, created if missing and canonical.
fn scratch_root(engine: &str, given: &str) -> Result<PathBuf> {
    let p = PathBuf::from(given);
    std::fs::create_dir_all(&p).map_err(|e| Error::ExportDenied {
        engine: engine.to_string(),
        permission: format!("write access to the scratch folder {given}"),
        message: format!(
            "the scratch folder {given} cannot be created ({e}); grant write access to it, or \
             name another with --scratch"
        ),
    })?;
    std::fs::canonicalize(&p).map_err(|e| Error::InvalidArgument {
        field: "scratch",
        reason: format!("cannot open the scratch folder {given} ({e})"),
    })
}
