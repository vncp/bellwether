//! `fix` on a rewrite of engine queries (ADR 033): measure in the engine,
//! verify on what those very runs exported, attest only.
//!
//! 1. **Measure.** Every query of every pair runs once in the engine,
//!    under a tag this call chose, read-only, and that one run writes its
//!    rows as Parquet into a fresh folder of the scratch folder: the run
//!    measured is the run whose rows are verified, so a query that tells
//!    its runs apart (by its own query id) gains nothing. Each run's
//!    statistics are read from the engine's own query log, from the entry
//!    of that run alone; the statistics must name the run's tag, and the
//!    rows the log says the run returned must be the rows it exported, or
//!    the measurement is refused ([`Error::MeasurementRefused`]). The
//!    Replay holds the statistics (`exact: false`, `model:
//!    <engine>.query_log`) beside the twin's prediction for the key sample,
//!    pair by pair, and is written before anything else, so a failing
//!    certificate keeps the prediction error. A refused export is
//!    [`Refusal::ExportDenied`]: nothing measured, nothing written.
//! 2. **Gate.** A rewrite that reads no fewer bytes in the engine is
//!    refused ([`Refusal::NothingToGain`]), before anything is verified; a
//!    verify-only run goes on (it applies nothing) and applying its
//!    certificate is gated again on the apply tier's own measurement.
//! 3. **Verify.** Each pair's before and after exports are reconciled by
//!    the proposal's key columns (latest-by and exact totals as for any
//!    reconcile). One Certificate covers every pair and cites the engine's
//!    identity before the runs, after them (on a fresh connection), and
//!    the Replay. The exports are deleted afterwards, unless `keep`.
//! 4. **Attest.** A passing certificate against the current manifest gets
//!    an attest-only Receipt (`attest_only`, `manifest_after ==
//!    manifest_before`) binding the twin Evidence, the proposal, the
//!    measured Replay, the certificate, and the prediction. Nothing is
//!    deployed: [`FixOutcome::Attested`] names the queries a human deploys.
//!
//! The scratch folder is governed: it is a hidden folder (`_` or `.`)
//! under the lake root, checked on its canonical path, so the exports
//! stay under the lake's account and are never read as lake data, and
//! each run's folder is created with the lake root's permissions.

use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bellwether_ledger::RecordKind;
use bellwether_source::ParquetSource;
use bellwether_source_engine::{
    check_query, ratio, refused, EngineAdapter, EngineIdentity, Exported, Tag,
};
use bellwether_types::{
    format, CertBody, Certificate, Coverage, Error, Evidence, EvidenceBody, Exactness, Hash,
    PairPrediction, Prediction, Proposal, ProposalBody, Provenance, QueryStats, Receipt, Record,
    Replay, Result, RewriteShape, RunCost, TwinOf, MAX_MISMATCH_EXAMPLES,
};
use bellwether_verify::{CertificateContext, Options, ReconcileSpec, Reconciliation, Side};
use serde::{Deserialize, Serialize};

use crate::engine::{Engine, DEFAULT_INDEX_DIR};
use crate::fix::{no_gain, FixOutcome, FixRequest, FixResult, Refusal, Score};
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
    /// The engine's identity when the cited twin diagnosis ran (its
    /// Evidence), so a reader sees whether the copy the prediction came
    /// from was current.
    pub identity_at_twin: EngineIdentity,
    /// The engine's identity before the runs.
    pub identity_before: EngineIdentity,
    /// Its identity after the runs, when it was read again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_after: Option<EngineIdentity>,
    /// Each pair, in the proposal's order (those measured, when a run
    /// stopped partway).
    pub pairs: Vec<PairRun>,
    /// The twin's prediction beside the measurement (also on the Replay),
    /// when every pair was measured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prediction: Option<Prediction>,
    /// The folder of this run's exports, when exports ran.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scratch: Option<String>,
    /// Whether the exports were kept (`keep`); otherwise they are deleted
    /// when the run ends, whatever its outcome.
    pub kept: bool,
}

/// One pair's measurement and verification.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PairRun {
    /// The before query's statistics from the engine's log.
    pub before: QueryStats,
    /// The after query's.
    pub after: QueryStats,
    /// What the measured runs exported, when they exported: files and
    /// bytes per side.
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

/// The key sample a rewrite of engine queries rests on: the proposal's
/// `keys`, which must be the sample the cited twin diagnosis recorded. A
/// diagnosis without a sample predicts nothing to compare the engine with,
/// so it backs no rewrite.
pub(crate) fn key_sample(shape: &RewriteShape, twin: &TwinOf) -> Result<Hash> {
    match (shape.keys, twin.workload) {
        (_, None) => Err(Error::InvalidArgument {
            field: "keys",
            reason: format!(
                "the cited twin diagnosis of {e} ran on no key sample, so there is no prediction \
                 to compare the engine's reads with; record the sample (`workload record \
                 <folder> --from-engine {e} --keys-query <file>`), run `why --workload <hash> \
                 --twin-of {e}` with the hash it prints, and cite the evidence that prints",
                e = twin.engine
            ),
        }),
        (Some(k), Some(w)) if k == w => Ok(k),
        (Some(k), Some(w)) => Err(Error::InvalidArgument {
            field: "keys",
            reason: format!(
                "the key sample {k} is not the one the cited twin diagnosis ran on ({w}); cite \
                 the evidence of a diagnosis on this sample (`why --workload {k} --twin-of {}`)",
                twin.engine
            ),
        }),
        (None, Some(w)) => Err(Error::InvalidArgument {
            field: "keys",
            reason: format!(
                "the proposal names no key sample; propose it again citing the twin diagnosis, \
                 which records the sample {w}"
            ),
        }),
    }
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

fn stats_cost(s: &QueryStats, keys: u64, model: &str) -> RunCost {
    let units = s.units_read.unwrap_or(0);
    RunCost {
        run_id: s.tag.clone(),
        keys,
        files: units,
        row_groups: units,
        bytes: s.read_bytes,
        // Measured by the engine's own log, never exact; `exact` and
        // `exactness` agree (record version 3, ledger rule 7b).
        exact: false,
        exactness: Exactness::Measured {
            model: model.to_string(),
        },
        // `rows` counts the rows holding the run's keys; the log records
        // the rows the engine read (`measured.read_rows`), a different
        // number, so it is absent.
        rows: None,
        measured: Some(s.clone()),
    }
}

/// The twin's prediction beside each pair's measurement: each pair's
/// queries against the twin's predicted rows on their own.
fn prediction(evidence: Hash, twin: &TwinOf, pairs: &[PairRun]) -> Prediction {
    Prediction {
        evidence,
        predicted_rows: twin.predicted_rows,
        predicted_row_groups: twin.predicted_row_groups,
        pairs: pairs
            .iter()
            .map(|p| PairPrediction {
                before_read_rows: p.before.read_rows,
                after_read_rows: p.after.read_rows,
                before_read_bytes: p.before.read_bytes,
                after_read_bytes: p.after.read_bytes,
                before_ratio: ratio(p.before.read_rows, twin.predicted_rows),
                after_ratio: ratio(p.after.read_rows, twin.predicted_rows),
            })
            .collect(),
    }
}

/// The tag of pair `i`'s `side` in run `run`.
fn tag(run: &str, i: usize, side: &str) -> Result<Tag> {
    Tag::new(run, &format!("p{i}-{side}"))
}

/// The rows of the Parquet files `x` holds, from their footers.
fn exported_rows(x: &Exported) -> Result<u64> {
    let mut rows = 0u64;
    for f in &x.files {
        let file = std::fs::File::open(f)
            .map_err(|e| Error::Source(format!("cannot open the export {} ({e})", f.display())))?;
        let reader = parquet::file::reader::SerializedFileReader::new(file).map_err(|e| {
            Error::Source(format!("the export {} is not Parquet ({e})", f.display()))
        })?;
        let n = parquet::file::reader::FileReader::metadata(&reader)
            .file_metadata()
            .num_rows();
        rows = rows.saturating_add(u64::try_from(n).unwrap_or(0));
    }
    Ok(rows)
}

/// One query run once: exported into `dir` when given, its rows discarded
/// otherwise; its statistics read from that run's log entry and checked
/// to be that run's (module docs).
fn run_once(
    engine: &dyn EngineAdapter,
    query: &str,
    tag: &Tag,
    dir: Option<&Path>,
) -> Result<(QueryStats, Option<Exported>)> {
    let run = match dir {
        Some(d) => engine.export(query, tag, d)?,
        None => engine.submit(query, tag)?,
    };
    let stats = engine.stats(&run)?;
    if stats.tag != tag.as_str() {
        return Err(refused(
            engine.root(),
            tag,
            "tag_mismatch",
            &format!(
                "the adapter returned the statistics of query {}, not of this run; nothing it \
                 returned is used",
                stats.tag
            ),
        ));
    }
    if let Some(x) = &run.exported {
        let rows = exported_rows(x)?;
        if rows != stats.result_rows {
            return Err(refused(
                engine.root(),
                tag,
                "result_rows_differ",
                &format!(
                    "the engine's log says the run returned {} rows and the run exported {rows}, \
                     so the rows verified would not be the rows measured",
                    stats.result_rows
                ),
            ));
        }
    }
    Ok((stats, run.exported))
}

/// Every pair's two queries, each run once (module docs); with `dir`, each
/// run exports into `dir/p<i>/<side>`. `pairs` receives each pair as it is
/// measured, so a run that stops partway still shows what it measured.
fn measure(
    engine: &dyn EngineAdapter,
    shape: &RewriteShape,
    run: &str,
    dir: Option<&Path>,
    pairs: &mut Vec<PairRun>,
) -> Result<()> {
    for (i, p) in shape.pairs.iter().enumerate() {
        let side = |s: &str| dir.map(|d| d.join(format!("p{i}")).join(s));
        let (db, da) = (side("before"), side("after"));
        let (before, eb) = run_once(engine, &p.before, &tag(run, i, "before")?, db.as_deref())?;
        let (after, ea) = run_once(engine, &p.after, &tag(run, i, "after")?, da.as_deref())?;
        pairs.push(PairRun {
            before,
            after,
            exported: eb.zip(ea).map(|(b, a)| [b, a]),
            verification: None,
        });
    }
    Ok(())
}

/// `e`, saying where the exports so far are left when they are kept.
fn left_in(e: Error, dir: &Path) -> Error {
    let has = std::fs::read_dir(dir).is_ok_and(|mut d| d.next().is_some());
    if !has {
        return e;
    }
    let note = format!(
        " The exports so far are left in {} (--keep); delete them when done",
        dir.display()
    );
    match e {
        Error::SourceChanged {
            root,
            units,
            message,
        } => Error::SourceChanged {
            root,
            units,
            message: message + &note,
        },
        Error::MeasurementRefused {
            engine,
            tag,
            reason,
            message,
        } => Error::MeasurementRefused {
            engine,
            tag,
            reason,
            message: message + &note,
        },
        Error::Source(m) => Error::Source(m + &note),
        Error::Other(m) => Error::Other(m + &note),
        other => other,
    }
}

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

    /// Writes the measured Replay of `pairs` and returns it with the
    /// prediction it holds.
    #[allow(clippy::too_many_arguments)]
    fn put_measured_replay(
        &self,
        engine: &dyn EngineAdapter,
        proposal_hash: &Hash,
        shape: &RewriteShape,
        keys: Hash,
        evidence: Hash,
        twin: &TwinOf,
        identity: &EngineIdentity,
        manifest: u64,
        pairs: &[PairRun],
    ) -> Result<(Written, Prediction)> {
        let prediction = prediction(evidence, twin, pairs);
        let mut inputs = vec![identity.hash, evidence];
        for p in &shape.pairs {
            inputs.push(Hash::of(p.before.as_bytes()));
            inputs.push(Hash::of(p.after.as_bytes()));
        }
        let replay = Replay {
            record_version: format::RECORD_VERSION,
            proposal: *proposal_hash,
            manifest_version: manifest.to_string(),
            workload: keys,
            inputs,
            current: pairs
                .iter()
                .map(|p| stats_cost(&p.before, twin.keys, engine.model()))
                .collect(),
            proposed: pairs
                .iter()
                .map(|p| stats_cost(&p.after, twin.keys, engine.model()))
                .collect(),
            configuration: Vec::new(),
            operators: Vec::new(),
            model: Some(engine.model().to_string()),
            unserved: None,
            prediction: Some(prediction.clone()),
            created_at: self.now(),
        };
        Ok((self.put(Record::Replay(replay))?, prediction))
    }

    /// The scratch folder (module docs): `given`, which must resolve to a
    /// hidden folder under the lake root; the folders missing on its way
    /// are created with the lake root's permissions.
    fn scratch_root(&self, engine: &str, given: &str) -> Result<PathBuf> {
        let lake = PathBuf::from(self.root());
        let refuse = |why: &str| Error::InvalidArgument {
            field: "scratch",
            reason: format!(
                "the scratch folder {given} {why}: the exports hold the engine's rows, so they \
                 stay under the lake root, with its account and permissions, in a folder whose \
                 name starts with `_` or `.`, which the lake's discovery never reads as data; \
                 name one such as {}",
                lake.join("_bellwether-scratch").display()
            ),
        };
        let p = PathBuf::from(given);
        let p = if p.is_absolute() {
            p
        } else {
            std::env::current_dir()
                .map_err(|e| Error::Other(format!("cannot read the working folder ({e})")))?
                .join(p)
        };
        // The nearest folder that exists, canonical, then the names below
        // it (a name that climbs, `..`, ends the walk and is refused).
        let mut existing = p.as_path();
        let mut missing = Vec::new();
        while !existing.exists() {
            match (existing.parent(), existing.file_name()) {
                (Some(parent), Some(name)) => {
                    missing.push(name.to_os_string());
                    existing = parent;
                }
                _ => return Err(refuse("does not name a folder")),
            }
        }
        let mut canonical =
            std::fs::canonicalize(existing).map_err(|e| Error::InvalidArgument {
                field: "scratch",
                reason: format!("cannot open the scratch folder {given} ({e})"),
            })?;
        let first_new = canonical.components().count();
        for name in missing.iter().rev() {
            canonical.push(name);
        }
        let Ok(below) = canonical.strip_prefix(&lake) else {
            return Err(refuse(&format!(
                "is {}, outside the lake root {}",
                canonical.display(),
                lake.display()
            )));
        };
        let first = match below.components().next() {
            Some(Component::Normal(n)) => n.to_string_lossy().to_string(),
            _ => return Err(refuse("is the lake root itself")),
        };
        if first == DEFAULT_INDEX_DIR {
            return Err(refuse("is inside the index folder"));
        }
        if !(first.starts_with('_') || first.starts_with('.')) {
            return Err(refuse(&format!(
                "is under {}, a folder the lake's discovery reads",
                lake.join(&first).display()
            )));
        }
        // Create what is missing, one folder at a time, each with the lake
        // root's permissions.
        let mut at: PathBuf = canonical.components().take(first_new).collect();
        for name in missing.iter().rev() {
            at.push(name);
            self.private_dir(engine, &at)?;
        }
        Ok(canonical)
    }

    /// Creates the folder `dir` (its parent exists) with the lake root's
    /// permissions, or refuses naming the permission it needs.
    pub(crate) fn private_dir(&self, engine: &str, dir: &Path) -> Result<()> {
        let denied = |e: &dyn std::fmt::Display| Error::ExportDenied {
            engine: engine.to_string(),
            permission: format!("write access to the scratch folder {}", dir.display()),
            message: format!(
                "the scratch folder {} cannot be created with the lake root's permissions ({e}); \
                 grant write access to it, or name another hidden folder under the lake root \
                 with --scratch",
                dir.display()
            ),
        };
        std::fs::create_dir(dir).map_err(|e| denied(&e))?;
        let mode = std::fs::metadata(self.root())
            .map_err(|e| denied(&e))?
            .permissions();
        std::fs::set_permissions(dir, mode).map_err(|e| denied(&e))
    }

    /// The scratch folder the engine is configured with, governed (module
    /// docs), and this run's fresh folder in it.
    pub(crate) fn run_folder(&self, engine: &str, scratch: &str, name: &str) -> Result<PathBuf> {
        let dir = self.scratch_root(engine, scratch)?.join(name);
        self.private_dir(engine, &dir)?;
        Ok(dir)
    }

    /// `fix` on a rewrite of engine queries (module docs).
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
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
        let keys = key_sample(shape, &twin)?;
        let given = self.options().scratch.as_deref().ok_or(Error::NoWorkload {
            proposal: "rewrite",
            field: "scratch",
            needs: "a scratch folder the engine's exports are written to",
        })?;
        let keep = self.options().keep_exports;
        let run = Tag::run();
        let dir = self.run_folder(&twin.engine, given, &format!("bellwether-{run}"))?;
        let scratch = dir.parent().map(Path::to_path_buf).unwrap_or_default();
        let stages = (|| {
            let engine = self.engine_for(&twin, Some(&scratch))?;
            let identity_before = Engine::twin_identity(&*engine)?;
            let mut run_info = EngineRun {
                engine: engine.root().to_string(),
                model: engine.model().to_string(),
                identity_at_twin: EngineIdentity {
                    hash: twin.identity,
                    units: twin.units,
                    rows: twin.engine_rows,
                },
                identity_before,
                identity_after: None,
                pairs: Vec::new(),
                prediction: None,
                scratch: Some(dir.display().to_string()),
                kept: keep,
            };
            let measured = measure(&*engine, shape, &run, Some(&dir), &mut run_info.pairs);
            match measured {
                Ok(()) => {}
                Err(Error::ExportDenied {
                    engine: e,
                    permission,
                    message,
                }) => {
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
            }
            let (replay, prediction) = self.put_measured_replay(
                &*engine,
                proposal_hash,
                shape,
                keys,
                evidence,
                &twin,
                &identity_before,
                manifest,
                &run_info.pairs,
            )?;
            run_info.prediction = Some(prediction);
            let replay_hash = replay.hash;
            let Record::Replay(r) = &replay.record else {
                return Err(Error::Other("a replay written is not a replay".into()));
            };
            let (current, proposed) = (total(&r.current), total(&r.proposed));
            result.score = Some(Score::Measured {
                current,
                proposed,
                model: engine.model().to_string(),
                replay: Box::new(replay.clone()),
            });
            // The gain gate, on the bytes the engine read, the measure `why`
            // recommends by and the one a rerun of the same query on the same
            // tables reproduces (peak memory, CPU, and duration are one noisy
            // sample each and never pass a rewrite). A verify-only run is
            // not gated here; the apply tier gates its own measurement.
            if let Some(refusal) = no_gain(
                &replay_hash,
                &current,
                &proposed,
                req.allow_no_gain || req.verify_only,
            ) {
                result.engine = Some(run_info);
                result.outcome = FixOutcome::Refused { refusal };
                return Ok(result);
            }
            let certified = self.reconcile_exports(
                shape,
                &req.engine.clone().unwrap_or_default(),
                &mut run_info.pairs,
                &dir,
                proposal_hash,
                anchor,
            )?;
            // The engine's identity after the runs, on a fresh connection: a
            // write that landed during the run would mix two states of its
            // tables.
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
                         units, {} to {} rows), so the before and after results may come from \
                         two states of its tables and are not certified; run fix again when the \
                         tables are quiet. The measured replay {replay_hash} stands",
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
                        uncomparable: certificate.body.uncomparable,
                        missing_left: certificate.body.missing_left,
                        missing_right: certificate.body.missing_right,
                        totals_differ: certificate.body.totals_delta.len() as u64,
                    },
                };
                return Ok(result);
            }
            if req.verify_only {
                result.signatures =
                    self.sign_written(&[(cert.hash, RecordKind::Certificate)], None)?;
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
        })();
        if keep {
            stages.map_err(|e| left_in(e, &dir))
        } else {
            let _ = std::fs::remove_dir_all(&dir);
            stages
        }
    }

    /// Reconciles every pair's exports; the certified outcomes, one per
    /// pair.
    fn reconcile_exports(
        &self,
        shape: &RewriteShape,
        verify: &EngineCompare,
        pairs: &mut [PairRun],
        dir: &Path,
        proposal: &Hash,
        anchor: Option<&bellwether_verify::ManifestAnchor>,
    ) -> Result<Vec<bellwether_verify::Certified>> {
        let mut out = Vec::with_capacity(pairs.len());
        for (i, pair) in pairs.iter_mut().enumerate() {
            let (db, da) = (
                dir.join(format!("p{i}")).join("before"),
                dir.join(format!("p{i}")).join("after"),
            );
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
            examples: Vec::new(),
            uncomparable: 0,
            totals: Vec::new(),
            // Each pair's exports are reconciled whole: every row of every
            // key is compared.
            row_filter: None,
        };
        let mut inputs = vec![before.hash, after.hash, replay];
        let mut age = Duration::ZERO;
        for (i, c) in certified.iter().enumerate() {
            let b = &c.certificate.body;
            body.missing_right += b.missing_right;
            body.missing_left += b.missing_left;
            body.mismatched += b.mismatched;
            body.uncomparable += b.uncomparable;
            let room = MAX_MISMATCH_EXAMPLES - body.examples.len();
            body.examples.extend(b.examples.iter().take(room).cloned());
            for t in &b.totals {
                if !body.totals.contains(t) {
                    body.totals.push(t.clone());
                }
            }
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

    /// The measured Replay `certificate` cites (its third input): the
    /// measurement that was verified.
    fn certified_replay(
        &self,
        cert_hash: &Hash,
        certificate: &Certificate,
        proposal_hash: &Hash,
    ) -> Result<(Hash, Replay)> {
        let wrong = || {
            Error::LedgerRule(format!(
                "certificate {cert_hash} cites no measured replay of proposal {proposal_hash}, so \
                 there is no verified measurement to apply; run fix on the proposal again"
            ))
        };
        let h = certificate
            .provenance
            .inputs
            .get(2)
            .copied()
            .ok_or_else(wrong)?;
        let store = self.existing_store()?.ok_or_else(wrong)?;
        match bellwether_ledger::Ledger::new(Arc::clone(store)).get(&h) {
            Ok(Record::Replay(r))
                if r.proposal == *proposal_hash
                    && !r.current.is_empty()
                    && r.current
                        .iter()
                        .chain(&r.proposed)
                        .all(|c| c.measured.is_some()) =>
            {
                Ok((h, r))
            }
            _ => Err(wrong()),
        }
    }

    /// The apply tier on a rewrite of engine queries: the certificate is
    /// gated as any other; the engine must be in the state the certificate
    /// verified before the queries run again and after; the new
    /// measurement must agree with the certified Replay's (rows read,
    /// bytes read, result rows), and pass the gain gate; then the receipt
    /// attests the new Replay (a Replay is written only by the call that
    /// measured it).
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
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
        allow_no_gain: bool,
    ) -> Result<FixResult> {
        result.invariant = Some(shape.invariant.clone());
        let (evidence, twin) = cited_twin(self, proposal)?;
        let keys = key_sample(shape, &twin)?;
        let (replay_hash, certified_replay) =
            self.certified_replay(cert_hash, certificate, proposal_hash)?;
        let engine = self.engine_for(&twin, None)?;
        let identity = Engine::twin_identity(&*engine)?;
        let certified = certificate.provenance.inputs.get(1).copied();
        let changed = |current: Hash| FixOutcome::Refused {
            refusal: Refusal::DataChanged {
                certificate: *cert_hash,
                certified,
                current,
            },
        };
        if certified != Some(identity.hash) {
            result.outcome = changed(identity.hash);
            return Ok(result);
        }
        if dry_run {
            result.outcome = FixOutcome::WouldApply {
                sequence: manifest,
                expected: manifest,
            };
            return Ok(result);
        }
        let mut run_info = EngineRun {
            engine: engine.root().to_string(),
            model: engine.model().to_string(),
            identity_at_twin: EngineIdentity {
                hash: twin.identity,
                units: twin.units,
                rows: twin.engine_rows,
            },
            identity_before: identity,
            identity_after: None,
            pairs: Vec::new(),
            prediction: None,
            scratch: None,
            kept: false,
        };
        let measured = measure(&*engine, shape, &Tag::run(), None, &mut run_info.pairs);
        // The engine's identity after the measurement, on a fresh
        // connection: the numbers must describe the state certified.
        let after = Engine::twin_identity(&*self.engine_for(&twin, None)?)?;
        run_info.identity_after = Some(after);
        measured?;
        if after.hash != identity.hash {
            result.engine = Some(run_info);
            result.outcome = changed(after.hash);
            return Ok(result);
        }
        if let Some(refusal) =
            measurement_changed(cert_hash, replay_hash, &certified_replay, &run_info.pairs)
        {
            result.engine = Some(run_info);
            result.outcome = FixOutcome::Refused { refusal };
            return Ok(result);
        }
        let (replay, prediction) = self.put_measured_replay(
            &*engine,
            proposal_hash,
            shape,
            keys,
            evidence,
            &twin,
            &identity,
            manifest,
            &run_info.pairs,
        )?;
        run_info.prediction = Some(prediction);
        result.engine = Some(run_info);
        let Record::Replay(r) = &replay.record else {
            return Err(Error::Other("a replay written is not a replay".into()));
        };
        let (current, proposed) = (total(&r.current), total(&r.proposed));
        result.score = Some(Score::Measured {
            current,
            proposed,
            model: engine.model().to_string(),
            replay: Box::new(replay.clone()),
        });
        if let Some(refusal) = no_gain(&replay.hash, &current, &proposed, allow_no_gain) {
            result.outcome = FixOutcome::Refused { refusal };
            return Ok(result);
        }
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

/// The first query whose new measurement disagrees with the certified
/// Replay's on rows read, bytes read, or result rows.
fn measurement_changed(
    cert: &Hash,
    replay: Hash,
    certified: &Replay,
    pairs: &[PairRun],
) -> Option<Refusal> {
    let refusal = |pair: usize, side: &str, was: Option<&QueryStats>, now: &QueryStats| {
        Refusal::MeasurementChanged {
            certificate: *cert,
            replay,
            pair: pair as u64,
            side: side.to_string(),
            certified_read_rows: was.map_or(0, |s| s.read_rows),
            measured_read_rows: now.read_rows,
            certified_read_bytes: was.map_or(0, |s| s.read_bytes),
            measured_read_bytes: now.read_bytes,
            certified_result_rows: was.map_or(0, |s| s.result_rows),
            measured_result_rows: now.result_rows,
        }
    };
    let same = |a: &QueryStats, b: &QueryStats| {
        (a.read_rows, a.read_bytes, a.result_rows) == (b.read_rows, b.read_bytes, b.result_rows)
    };
    for (i, p) in pairs.iter().enumerate() {
        for (side, now, was) in [
            ("before", &p.before, certified.current.get(i)),
            ("after", &p.after, certified.proposed.get(i)),
        ] {
            let was = was.and_then(|c| c.measured.as_ref());
            if !was.is_some_and(|w| same(w, now)) {
                return Some(refusal(i, side, was, now));
            }
        }
    }
    None
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
