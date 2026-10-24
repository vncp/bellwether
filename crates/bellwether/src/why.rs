//! `why`: the dataset from footer statistics, then, when a workload is
//! available, a replay of it (WP6) under the current configuration and
//! under each candidate key, one engine Proposal and one Replay record per
//! candidate.
//!
//! The profile (WP5: fanout per column and composite, the statistics-only
//! lints, a `profile` Evidence, and the profile's measured candidates) is
//! wired in when its crate merges; until then the candidates are the exact
//! keys on the workload's own key columns, and the culprit carries no
//! fanout.

use std::collections::BTreeSet;
use std::path::PathBuf;

use bellwether_replay::{
    record_bench_workload, servable, KeyInput, Replayer, Workload, WorkloadRecorder,
};
use bellwether_store::{Cache, ObjectStoreBackend};
use bellwether_types::{format, Error, Hash, KeySpec, Proposal, ProposalBody, Record, Result};
use serde::{Deserialize, Serialize};

use crate::engine::{folder_changes, guard, schema_hint, Engine, Pinned};
use crate::keys::KeyFile;
use crate::results::{total, Cost, Written};

/// Who the engine's own candidate proposals are by.
pub const ENGINE_PROPOSER: &str = "engine";

/// Proposer names only the engine writes. `propose` refuses them (compared
/// trimmed and ignoring ASCII case), so a caller cannot pass a proposal off
/// as one of the engine's own candidates.
pub const RESERVED_PROPOSERS: &[&str] = &[ENGINE_PROPOSER];

/// What `why` looks at. With no workload, `why` reports the dataset only.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WhyRequest {
    /// The keys one run looked up, as a key file. Recorded as a one-run
    /// workload (run id `keys`).
    pub keys: Option<KeyFile>,
    /// A recorded workload in the benchmark's format (a directory holding
    /// `workload.json`, or the file itself).
    pub bench_workload: Option<PathBuf>,
    /// A workload recorded earlier, by its hash (it must be in the cache).
    pub workload: Option<Hash>,
}

/// The recorded workload `why` replayed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadInfo {
    /// The workload hash; `fix` takes it.
    pub hash: Hash,
    /// Recorded runs.
    pub runs: u64,
    /// The distinct key-column sets the runs look up by.
    pub columns: Vec<Vec<String>>,
    /// Distinct keys per run at nearest-rank p50, p95, max.
    pub keys_p50_p95_max: [u64; 3],
    /// Input rows dropped because their tuple held a null (ADR 014).
    pub null_tuples: u64,
}

/// The whole dataset, from footer statistics: the replay crate's catalog
/// plus, in an indexed folder, any file the index does not track yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Dataset {
    /// Files.
    pub files: u64,
    /// Live row groups.
    pub row_groups: u64,
    /// Rows.
    pub rows: u64,
    /// Compressed bytes of every live row group.
    pub bytes: u64,
}

/// Where a candidate came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateOrigin {
    /// An exact key on the workload's own key columns: it selects exactly
    /// the row groups that hold a queried key, the least any row-group
    /// index can read for these runs (the floor).
    Workload,
}

/// One candidate key, replayed over the workload.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Candidate {
    /// The key.
    pub spec: KeySpec,
    /// Where it came from.
    pub origin: CandidateOrigin,
    /// Summed over runs: what the workload would read under the current
    /// configuration with this key added.
    pub proposed: Cost,
    /// The engine's Proposal for it. It cites no evidence until the
    /// profile's Evidence is wired in (WP5).
    pub proposal: Written,
    /// The Replay scoring that Proposal (`inputs`: the catalog digest; a
    /// key replay is exact, so no `model`; contract v2.1).
    pub replay: Written,
}

/// The candidate that reads the least.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Recommendation {
    /// The key.
    pub spec: KeySpec,
    /// Its Proposal; pass it to `fix`.
    pub proposal: Hash,
    /// Its Replay.
    pub replay: Hash,
    /// Summed over runs, under the current configuration.
    pub current: Cost,
    /// Summed over runs, with this key.
    pub proposed: Cost,
}

/// Why columns were named the culprit.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CulpritReason {
    /// No current key spec serves the workload's key columns, so every run
    /// reads every row group.
    NoIndexServes,
    /// A current key spec serves the workload; its columns bound what it
    /// prunes.
    ServingSpec {
        /// The serving spec.
        spec: KeySpec,
    },
}

/// The columns behind the read.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Culprit {
    /// Columns.
    pub columns: Vec<String>,
    /// Why.
    pub reason: CulpritReason,
}

/// What [`Engine::why`] found.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WhyReport {
    /// The canonical data root.
    pub root: String,
    /// The manifest sequence, `0` for a folder never indexed.
    pub sequence: u64,
    /// The current configuration: declared specs complete at `sequence`.
    pub current_specs: Vec<KeySpec>,
    /// The workload replayed, if any.
    pub workload: Option<WorkloadInfo>,
    /// The dataset, untracked files included.
    pub dataset: Dataset,
    /// Files under the root that the index does not track yet (they landed
    /// after the last build). They are counted in `dataset`; the costs below
    /// cover the files the index tracks, so run `bellwether index` first.
    pub untracked: Vec<String>,
    /// Summed over runs, under the current configuration (exact). Present
    /// with a workload.
    pub current: Option<Cost>,
    /// Candidates replayed: the exact key on each run's key columns, unless
    /// it is already current.
    pub candidates: Vec<Candidate>,
    /// The candidate reading the fewest bytes, if any reads fewer than the
    /// current configuration.
    pub recommended: Option<Recommendation>,
    /// The culprit, with a workload.
    pub culprit: Option<Culprit>,
}

impl Engine {
    /// Explains what the data's layout costs a lookup.
    ///
    /// 1. **Footer statistics**: files, live row groups, rows, and bytes,
    ///    through the replay crate's catalog (the index's manifest when one
    ///    exists, else discovery). Works on a folder with no index and no
    ///    workload.
    /// 2. **Replay** (WP6), when a workload is given (a key file, a bench
    ///    workload directory, or a recorded workload hash): every run under
    ///    the current configuration and under each candidate exact key,
    ///    each candidate written as an engine Proposal and scored by a Replay
    ///    record computed immediately before it is written. The candidate is
    ///    the exact key on each run's own key columns (the floor), unless it
    ///    is already current.
    ///
    /// The **culprit**, with a workload: its key columns when no current
    /// spec serves them, or the serving current spec's columns. The
    /// **recommended** key is the candidate with the fewest bytes over all
    /// runs (ties: fewer columns, then candidate order), only if it reads
    /// fewer bytes than the current configuration.
    pub fn why(&self, request: &WhyRequest) -> Result<WhyReport> {
        guard(|| self.why_inner(request)).map_err(schema_hint)
    }

    fn why_inner(&self, req: &WhyRequest) -> Result<WhyReport> {
        let given = [
            req.keys.is_some(),
            req.bench_workload.is_some(),
            req.workload.is_some(),
        ];
        if given.iter().filter(|g| **g).count() > 1 {
            return Err(Error::Other(
                "give one workload: a key file (--from), a bench workload directory \
                 (--bench-workload), or a recorded workload hash (--workload)"
                    .into(),
            ));
        }
        let pinned = self.pin()?;
        let source = self.source(pinned.as_ref())?;
        let sequence = pinned.as_ref().map_or(0, Pinned::sequence);
        let current_specs = pinned.as_ref().map(|p| p.specs().0).unwrap_or_default();
        let replayer = match &pinned {
            Some(p) => Replayer::for_snapshot(&*source, &p.snapshot)?,
            None => Replayer::unindexed(&*source, self.root())?,
        };
        let catalog = replayer.catalog();
        let mut dataset = Dataset {
            files: catalog.files().len() as u64,
            row_groups: catalog.len() as u64,
            rows: catalog.rows_total(),
            bytes: catalog.bytes_total(),
        };
        let mut untracked = Vec::new();
        if let Some(p) = &pinned {
            for fp in folder_changes(self.root(), p, self.format())?.untracked {
                for rg in source.row_groups(&fp)? {
                    dataset.row_groups += 1;
                    dataset.rows += rg.rows;
                    dataset.bytes += rg.bytes;
                }
                dataset.files += 1;
                untracked.push(fp.path);
            }
        }
        let mut report = WhyReport {
            root: self.root().to_string(),
            sequence,
            current_specs,
            workload: None,
            dataset,
            untracked,
            current: None,
            candidates: Vec::new(),
            recommended: None,
            culprit: None,
        };
        if given.iter().any(|g| *g) {
            let cache = self.cache()?;
            let workload = self.record_workload(req, &replayer, &cache)?;
            self.replay_candidates(&mut report, &replayer, &workload, &cache)?;
            report.culprit = culprit(&report.current_specs, &workload);
        }
        Ok(report)
    }

    fn record_workload(
        &self,
        req: &WhyRequest,
        replayer: &Replayer<'_>,
        cache: &Cache<ObjectStoreBackend>,
    ) -> Result<Workload> {
        if let Some(h) = &req.workload {
            return Workload::load(cache, h).map_err(|e| {
                Error::Other(format!(
                    "workload {h} is not in the cache at {} ({e}); record it again with \
                     `bellwether why --from <keys.parquet>`",
                    self.index_location()
                ))
            });
        }
        let mut recorder = WorkloadRecorder::new(cache, std::sync::Arc::clone(replayer.schema()));
        if let Some(keys) = &req.keys {
            let batch = keys.read()?;
            recorder.record("keys", KeyInput::Batch(&batch))?;
        } else if let Some(dir) = &req.bench_workload {
            let file = if dir.is_dir() {
                dir.join("workload.json")
            } else {
                dir.clone()
            };
            record_bench_workload(&mut recorder, &file).map_err(Error::from)?;
        }
        recorder.finish()
    }

    fn replay_candidates(
        &self,
        report: &mut WhyReport,
        replayer: &Replayer<'_>,
        workload: &Workload,
        cache: &Cache<ObjectStoreBackend>,
    ) -> Result<()> {
        let run_columns: Vec<Vec<String>> = {
            let set: BTreeSet<Vec<String>> =
                workload.runs().iter().map(|r| r.columns.clone()).collect();
            set.into_iter().collect()
        };
        report.workload = Some(WorkloadInfo {
            hash: workload.hash(),
            runs: workload.runs().len() as u64,
            columns: run_columns.clone(),
            keys_p50_p95_max: workload.keys_p50_p95_max(),
            null_tuples: workload.runs().iter().map(|r| r.null_tuples).sum(),
        });
        let current_names: BTreeSet<String> =
            report.current_specs.iter().map(KeySpec::name).collect();
        let mut chosen: Vec<(KeySpec, CandidateOrigin)> = Vec::new();
        for c in &run_columns {
            let spec = KeySpec::Exact { columns: c.clone() };
            if !current_names.contains(&spec.name()) {
                chosen.push((spec, CandidateOrigin::Workload));
            }
        }
        let ledger = self.ledger()?;
        for (spec, origin) in chosen {
            let proposal = self.put(Record::Proposal(Proposal {
                record_version: format::RECORD_VERSION,
                evidence: Vec::new(),
                proposed_by: ENGINE_PROPOSER.to_string(),
                created_at: self.now(),
                body: ProposalBody::Key { spec: spec.clone() },
            }))?;
            let (replay_hash, scored) = replayer
                .record(&ledger, &proposal.hash, workload, cache, &self.now())
                .map_err(Error::from)?;
            report.candidates.push(Candidate {
                spec,
                origin,
                proposed: total(&scored.replay.proposed),
                proposal,
                replay: Written {
                    hash: replay_hash,
                    record: Record::Replay(scored.replay),
                },
            });
        }
        let current = match report.candidates.first() {
            Some(Candidate {
                replay:
                    Written {
                        record: Record::Replay(r),
                        ..
                    },
                ..
            }) => total(&r.current),
            _ => total(&replayer.replay_current(workload, cache)?),
        };
        report.current = Some(current);
        report.recommended = report
            .candidates
            .iter()
            .enumerate()
            .min_by_key(|(i, c)| (c.proposed.bytes, c.spec.columns().len(), *i))
            .filter(|(_, c)| c.proposed.bytes < current.bytes)
            .map(|(_, c)| Recommendation {
                spec: c.spec.clone(),
                proposal: c.proposal.hash,
                replay: c.replay.hash,
                current,
                proposed: c.proposed,
            });
        Ok(())
    }
}

/// The workload's first run's key columns when no current spec serves them,
/// else the serving spec's columns.
fn culprit(current: &[KeySpec], workload: &Workload) -> Option<Culprit> {
    let columns = workload.runs().first()?.columns.clone();
    Some(
        match current.iter().find(|s| servable(s, &columns).is_some()) {
            None => Culprit {
                columns,
                reason: CulpritReason::NoIndexServes,
            },
            Some(spec) => Culprit {
                columns: spec.columns().to_vec(),
                reason: CulpritReason::ServingSpec { spec: spec.clone() },
            },
        },
    )
}
