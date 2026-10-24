//! `why`: the dataset from footer statistics and its profile (WP5: fanout
//! per column and composite, the statistics-only lints, a `profile`
//! Evidence), then, when a workload is available, a replay of it (WP6) under
//! the current configuration and under each candidate key, one engine
//! Proposal citing the Evidence and one Replay record per candidate.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use bellwether_profile::{ProfileOptions, ProfileReport};

use bellwether_replay::{
    record_bench_workload, servable, KeyInput, Lookup, Replayer, Workload, WorkloadRecorder,
};
use bellwether_store::{Cache, ObjectStoreBackend, StoreExt};
use bellwether_types::{
    format, Error, Evidence, Fanout, Hash, KeySpec, Proposal, ProposalBody, Record, Result, Store,
};
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

/// Default cap on the profile's candidates `why` replays beyond the
/// workload's own key columns.
pub const DEFAULT_MAX_CANDIDATES: usize = 8;

/// What `why` looks at. With no workload, `why` reports the dataset and its
/// profile.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WhyRequest {
    /// The keys one run looked up, as a key file. Recorded as a one-run
    /// workload (run id `keys`).
    pub keys: Option<KeyFile>,
    /// A recorded workload in the benchmark's format (a directory holding
    /// `workload.json`, or the file itself).
    pub bench_workload: Option<PathBuf>,
    /// A workload recorded earlier, by its hash (it must be in the cache).
    pub workload: Option<Hash>,
    /// Candidate key columns for the profile; `None` selects every id-like
    /// column.
    pub columns: Option<Vec<String>>,
    /// Optional manifest sequence to pin `why` against (`--at-manifest`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at_manifest: Option<u64>,
    /// Optional time window `A..B` (`--window`) to intersect with the workload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<String>,
    /// Optional upper instant `T` (`--at`) for a lookback window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<String>,
    /// Optional lookback duration (`--lookback`, e.g. `252d`) for `[T - N, T]`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lookback: Option<String>,
    /// Most profile candidates replayed beyond the workload's own key
    /// columns.
    pub max_candidates: usize,
}

impl Default for WhyRequest {
    fn default() -> Self {
        Self {
            keys: None,
            bench_workload: None,
            workload: None,
            columns: None,
            at_manifest: None,
            window: None,
            at: None,
            lookback: None,
            max_candidates: DEFAULT_MAX_CANDIDATES,
        }
    }
}

/// The `profile` Evidence `why` computed. It is written to the ledger when
/// the index location exists or can be created; otherwise it is returned
/// unrecorded, with how to record it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProfileEvidence {
    /// Whether the Evidence is in the ledger.
    pub recorded: bool,
    /// Its hash in the ledger, when recorded.
    pub hash: Option<Hash>,
    /// The Evidence, exactly as written when recorded.
    pub evidence: Evidence,
    /// When not recorded: one line saying how to record it.
    pub note: Option<String>,
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
    /// A column or composite the profile measured that serves the workload.
    Profile,
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
    /// The engine's Proposal for it, citing the profile Evidence.
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
    /// The profile raised a lint on a column the workload or the current
    /// index relies on.
    Lint {
        /// Rule id, for example `batch_stamp_key`.
        rule: String,
        /// Confidence in `[0, 1]` per the rule catalog.
        confidence: f64,
        /// The lint's one-sentence message.
        message: String,
    },
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

/// The columns behind the read, with their measured fanout.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Culprit {
    /// Columns.
    pub columns: Vec<String>,
    /// Their fanout, when the profile measured this tuple.
    pub fanout: Option<Fanout>,
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
    /// The `profile` Evidence.
    pub evidence: ProfileEvidence,
    /// Everything the profile measured, including what the Evidence's types
    /// cannot carry (sampling detail, ranges, time scatter, memory).
    pub profile: ProfileReport,
    /// The dataset, untracked files included.
    pub dataset: Dataset,
    /// Files under the root that the index does not track yet (they landed
    /// after the last build). They are counted in `dataset`; the costs below
    /// cover the files the index tracks, so run `bellwether index` first.
    pub untracked: Vec<String>,
    /// Summed over runs, under the current configuration (exact). Present
    /// with a workload.
    pub current: Option<Cost>,
    /// Candidates replayed: the exact key on each run's key columns, then
    /// the profile's measured candidates that serve a run in the profile's
    /// rank order, minus specs already current.
    pub candidates: Vec<Candidate>,
    /// Candidate keys the profile measured that serve no run of the
    /// workload, so they cannot be replayed against it.
    pub not_serving: Vec<KeySpec>,
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
    /// 2. **Profile** (WP5), always: fanout per candidate column and
    ///    composite, range statistics, and the statistics-only lints, as a
    ///    `profile` Evidence (invariant 6). It is written to the ledger when
    ///    the index location exists or can be created; otherwise (a
    ///    read-only folder and no `--index`) the profile is computed in
    ///    memory and the Evidence is returned with `recorded: false` and one
    ///    line naming `--index`. A bench workload's key columns are the
    ///    profile's supplied columns.
    /// 3. **Replay** (WP6), when a workload is given (a key file, a bench
    ///    workload directory, or a recorded workload hash): every run under
    ///    the current configuration and under each candidate exact key,
    ///    each candidate written as an engine Proposal citing the Evidence
    ///    and scored by a Replay record computed immediately before it is
    ///    written. Candidates are the exact key on each run's own key
    ///    columns (the floor), then the profile's measured candidates that
    ///    serve a run, at most `max_candidates`, minus specs already
    ///    current. A workload needs a writable index location: Proposals
    ///    cite a recorded Evidence.
    ///
    /// The **culprit** is, in order: the highest-confidence
    /// `batch_stamp_key` or `low_selectivity_single_column` lint on a column
    /// the workload or a current spec uses; else, with a workload, its key
    /// columns when no current spec serves them, or the serving current
    /// spec's columns; each with its measured fanout. The **recommended**
    /// key is the candidate with the fewest bytes over all runs (ties: fewer
    /// columns, then candidate order), only if it reads fewer bytes than the
    /// current configuration.
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
        let pinned = match req.at_manifest.or(self.options().at_manifest) {
            Some(seq) => Some(self.pin_at(seq)?),
            None => self.pin()?,
        };
        let source = self.source(pinned.as_ref())?;
        let sequence = pinned.as_ref().map_or(0, Pinned::sequence);
        let current_specs = pinned.as_ref().map(|p| p.specs().0).unwrap_or_default();
        let mut replayer = match &pinned {
            Some(p) => Replayer::for_snapshot(&*source, &p.snapshot)?,
            None => Replayer::unindexed_with_template(
                &*source,
                self.root(),
                self.effective_path_template(None)?,
            )?,
        };
        let resolved_window = self.resolve_why_window(req, &replayer, &current_specs)?;
        if let Some((ref ts_col, lo, hi)) = resolved_window {
            replayer = replayer.with_window(ts_col.clone(), lo, hi);
        }
        let catalog = replayer.catalog();
        let mut dataset = Dataset {
            files: catalog.files().len() as u64,
            row_groups: catalog.len() as u64,
            rows: catalog.rows_total(),
            bytes: catalog.bytes_total(),
        };
        let mut untracked = Vec::new();
        if let Some(p) = &pinned {
            if req.at_manifest.is_none() && self.options().at_manifest.is_none() {
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
        }
        let has_workload = given.iter().any(|g| *g);
        let (profile, evidence) = self.profile(req, &*source, pinned.as_ref(), has_workload)?;
        let mut report = WhyReport {
            root: self.root().to_string(),
            sequence,
            current_specs,
            workload: None,
            evidence,
            profile,
            dataset,
            untracked,
            current: None,
            candidates: Vec::new(),
            not_serving: Vec::new(),
            recommended: None,
            culprit: None,
        };
        let _ = resolved_window;
        let workload = if has_workload {
            let cache = self.cache()?;
            let workload = self.record_workload(req, &replayer, &cache)?;
            self.replay_candidates(&mut report, &replayer, &workload, &cache, req)?;
            Some(workload)
        } else {
            None
        };
        report.culprit = culprit(&report.current_specs, &report.profile, workload.as_ref());
        Ok(report)
    }

    /// Profiles the data root and builds its Evidence, recording it when the
    /// index location is writable. With a workload, an unwritable location
    /// is refused (the store's one-line error naming `--index`), since the
    /// Proposals must cite a recorded Evidence.
    fn profile(
        &self,
        req: &WhyRequest,
        source: &dyn bellwether_types::Source,
        pinned: Option<&Pinned>,
        has_workload: bool,
    ) -> Result<(ProfileReport, ProfileEvidence)> {
        let writable = if has_workload {
            Some(Arc::clone(self.store()?))
        } else {
            self.writable_store().map(Arc::clone)
        };
        let store = if let Some(s) = &writable {
            Arc::clone(s)
        } else {
            // Profile in memory, with the index's manifest when it can be
            // read, so a read-only index still informs the lints.
            let memory = Arc::new(ObjectStoreBackend::memory()?);
            if let Some(existing) = self.existing_store().ok().flatten() {
                if let Some((bytes, _)) = existing.get_opt("manifest.json")? {
                    memory.put_if_absent("manifest.json", bytes)?;
                }
            }
            memory
        };
        let mut options = ProfileOptions::for_source(&self.source_config(pinned)?);
        options.columns.clone_from(&req.columns);
        // A fixed clock asks for reproducible output: recompute the cached
        // artifacts rather than reuse them, so no input carries a wall-clock
        // age or a cache hit.
        options.reuse_cache = self.options().clock.is_none();
        options.workload = req.bench_workload.as_ref().map(|p| {
            if p.is_dir() {
                p.clone()
            } else {
                p.parent().map(PathBuf::from).unwrap_or_default()
            }
        });
        let mut profile = if let (true, Some(p)) = (
            req.at_manifest.is_some() || self.options().at_manifest.is_some(),
            pinned,
        ) {
            let pinned_source = PinnedFilesSource {
                inner: source,
                files: p
                    .manifest()
                    .rowgroup_space
                    .files
                    .iter()
                    .map(|f| f.fingerprint.clone())
                    .collect(),
            };
            let memory = Arc::new(ObjectStoreBackend::memory()?);
            if let Ok(bytes) = p.manifest().to_vec() {
                let _ = memory.put_if_absent("manifest.json", bytes::Bytes::from(bytes));
            }
            options.reuse_cache = false;
            bellwether_profile::profile(&pinned_source, self.root(), &memory, &options)?
        } else {
            bellwether_profile::profile(source, self.root(), &store, &options)?
        };
        let mut evidence = bellwether_profile::evidence(&profile)?;
        if let Some(p) = pinned {
            for input in &mut profile.inputs {
                if input.kind == bellwether_profile::evidence::InputKind::Manifest {
                    input.age = p.age;
                    input.hash = p.hash;
                }
            }
            if let Some(m) = &mut profile.manifest {
                m.sequence = p.sequence();
                m.hash = p.hash;
            }
            evidence = bellwether_profile::evidence(&profile)?;
        }
        // Every record this engine writes carries the engine's clock.
        evidence.provenance.created_at = self.now();
        let out = if writable.is_some() {
            let hash = self.ledger()?.put(&Record::Evidence(evidence.clone()))?;
            ProfileEvidence {
                recorded: true,
                hash: Some(hash),
                evidence,
                note: None,
            }
        } else {
            ProfileEvidence {
                recorded: false,
                hash: None,
                evidence,
                note: Some(format!(
                    "not recorded: the index location {} is not writable; pass a writable \
                     location with --index to record it",
                    self.index_location()
                )),
            }
        };
        Ok((profile, out))
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

    // One linear pass: choose the candidates, then write and score each.
    #[allow(clippy::too_many_lines)]
    fn replay_candidates(
        &self,
        report: &mut WhyReport,
        replayer: &Replayer<'_>,
        workload: &Workload,
        cache: &Cache<ObjectStoreBackend>,
        req: &WhyRequest,
    ) -> Result<()> {
        let evidence = report.evidence.hash.ok_or_else(|| {
            Error::Store(format!(
                "the profile Evidence could not be recorded at {}; pass a writable location \
                 with --index",
                self.index_location()
            ))
        })?;
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
        // How a measured key serves the workload: by point lookup on some
        // run, only by prefix of a run's columns, or not at all. A key that
        // serves only by prefix is a composite led by a run's whole key, so
        // it selects at least the row groups the exact key on that run's
        // columns (the floor, a candidate or current) selects: it can never
        // read less, and is not replayed.
        let point = |spec: &KeySpec| {
            run_columns
                .iter()
                .any(|c| matches!(servable(spec, c), Some(Lookup::Point { .. })))
        };
        let serves = |spec: &KeySpec| run_columns.iter().any(|c| servable(spec, c).is_some());
        let mut chosen: Vec<(KeySpec, CandidateOrigin)> = Vec::new();
        let mut names = BTreeSet::new();
        let win_ts_col = self
            .resolve_why_window(req, replayer, &report.current_specs)?
            .map(|(col, _, _)| col);
        for c in &run_columns {
            let spec = KeySpec::Exact { columns: c.clone() };
            if !current_names.contains(&spec.name()) && names.insert(spec.name()) {
                chosen.push((spec, CandidateOrigin::Workload));
            }
            if let Some(ref ts_col) = win_ts_col {
                let bspec = KeySpec::Bounds {
                    columns: c.clone(),
                    ts_column: ts_col.clone(),
                };
                if !current_names.contains(&bspec.name()) && names.insert(bspec.name()) {
                    chosen.push((bspec, CandidateOrigin::Workload));
                }
            }
        }
        if let Some(ref ts_col) = win_ts_col {
            let rspec = KeySpec::Range {
                column: ts_col.clone(),
            };
            if !current_names.contains(&rspec.name()) && names.insert(rspec.name()) {
                chosen.push((rspec, CandidateOrigin::Workload));
            }
        }
        let mut from_profile = 0usize;
        for entry in &report.profile.fanout {
            let spec = KeySpec::Exact {
                columns: entry.fanout.columns.clone(),
            };
            if current_names.contains(&spec.name()) || names.contains(&spec.name()) {
                continue;
            }
            if !serves(&spec) {
                report.not_serving.push(spec);
                continue;
            }
            if !point(&spec) {
                continue;
            }
            if from_profile < req.max_candidates {
                names.insert(spec.name());
                chosen.push((spec, CandidateOrigin::Profile));
                from_profile += 1;
            }
        }
        let ledger = self.ledger()?;
        for (spec, origin) in chosen {
            let proposal = self.put(Record::Proposal(Proposal {
                record_version: format::RECORD_VERSION,
                evidence: vec![evidence],
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

    fn resolve_why_window(
        &self,
        req: &WhyRequest,
        replayer: &Replayer<'_>,
        current_specs: &[KeySpec],
    ) -> Result<Option<(String, i64, i64)>> {
        if req.window.is_none() && req.lookback.is_none() {
            return Ok(None);
        }
        if req.window.is_some() && req.lookback.is_some() {
            return Err(Error::KeySpec(
                "why takes either --window or --lookback, not both".into(),
            ));
        }
        let schema = replayer.schema();
        let dummy_manifest = bellwether_store::Manifest::default();
        let (ts_column, ts_type) = crate::index::resolve_window_ts_column(
            current_specs,
            schema,
            &[],
            &dummy_manifest,
        )?;
        if let Some(win) = &req.window {
            let (lo_str, hi_str) = win.split_once("..").ok_or_else(|| {
                Error::KeySpec(format!(
                    "--window {win:?} must be written as A..B (inclusive bounds)"
                ))
            })?;
            let hi_str = hi_str.strip_prefix('=').unwrap_or(hi_str);
            let lo = crate::index::instant_in_unit(lo_str, &ts_column, &ts_type)?;
            let hi = crate::index::instant_in_unit(hi_str, &ts_column, &ts_type)?;
            return Ok(Some((ts_column, lo, hi)));
        }
        if let Some(lb) = &req.lookback {
            let delta = crate::index::parse_lookback(lb, &ts_column, &ts_type)?;
            let hi = if let Some(at) = &req.at {
                crate::index::instant_in_unit(at, &ts_column, &ts_type)?
            } else {
                let mut max_enc: Option<Vec<u8>> = None;
                for rg in replayer.catalog().row_groups() {
                    if let Some(st) = rg.meta.stats.iter().find(|s| s.column == ts_column) {
                        if let Some(m) = &st.max {
                            if max_enc.as_ref().is_none_or(|cur| m > cur) {
                                max_enc = Some(m.clone());
                            }
                        }
                    }
                }
                let enc = max_enc.ok_or_else(|| {
                    Error::KeySpec(format!(
                        "cannot determine latest timestamp for {ts_column:?} from dataset statistics"
                    ))
                })?;
                crate::index::decode_ts_from_canonical(&enc, &ts_type).ok_or_else(|| {
                    Error::KeySpec(format!(
                        "cannot decode maximum timestamp of {ts_column:?} ({ts_type})"
                    ))
                })?
            };
            let lo = hi.saturating_sub(delta);
            return Ok(Some((ts_column, lo, hi)));
        }
        Ok(None)
    }
}

/// Wraps a `Source` so `discover` returns only the files tracked by a pinned
/// manifest snapshot.
struct PinnedFilesSource<'a> {
    inner: &'a dyn bellwether_types::Source,
    files: Vec<bellwether_types::FileFingerprint>,
}

impl bellwether_types::Source for PinnedFilesSource<'_> {
    fn discover(&self, _root: &str) -> Result<Vec<bellwether_types::FileFingerprint>> {
        Ok(self.files.clone())
    }

    fn row_groups(
        &self,
        file: &bellwether_types::FileFingerprint,
    ) -> Result<Vec<bellwether_types::RowGroupMeta>> {
        self.inner.row_groups(file)
    }

    fn read_columns(
        &self,
        rg: &bellwether_types::RowGroupMeta,
        columns: &[String],
    ) -> Result<arrow::array::RecordBatch> {
        self.inner.read_columns(rg, columns)
    }

    fn schema(&self, root: &str) -> Result<arrow::datatypes::SchemaRef> {
        self.inner.schema(root)
    }
}

/// The lint rules that name a column a lookup should not rely on, in rule
/// catalog order.
const CULPRIT_RULES: [&str; 2] = ["batch_stamp_key", "low_selectivity_single_column"];

/// The profile's fanout of exactly `columns`, in that order or another.
fn fanout_of(profile: &ProfileReport, columns: &[String]) -> Option<Fanout> {
    let exact = profile
        .fanout
        .iter()
        .find(|e| e.fanout.columns == columns)
        .map(|e| e.fanout.clone());
    exact.or_else(|| {
        let want: BTreeSet<&String> = columns.iter().collect();
        profile
            .fanout
            .iter()
            .find(|e| e.fanout.columns.iter().collect::<BTreeSet<_>>() == want)
            .map(|e| e.fanout.clone())
    })
}

/// The culprit rule documented on [`Engine::why`].
fn culprit(
    current_specs: &[KeySpec],
    profile: &ProfileReport,
    workload: Option<&Workload>,
) -> Option<Culprit> {
    let mut used: BTreeSet<&String> = current_specs
        .iter()
        .flat_map(|s| s.columns().iter())
        .collect();
    if let Some(w) = workload {
        used.extend(w.runs().iter().flat_map(|r| r.columns.iter()));
    }
    let lint = profile
        .lints
        .iter()
        .filter_map(|l| {
            let rank = CULPRIT_RULES.iter().position(|r| *r == l.rule)?;
            l.columns
                .iter()
                .any(|c| used.contains(c))
                .then_some((rank, l))
        })
        .min_by(|(ra, a), (rb, b)| b.confidence.total_cmp(&a.confidence).then(ra.cmp(rb)));
    if let Some((_, l)) = lint {
        return Some(Culprit {
            columns: l.columns.clone(),
            fanout: fanout_of(profile, &l.columns),
            reason: CulpritReason::Lint {
                rule: l.rule.clone(),
                confidence: l.confidence,
                message: l.message.clone(),
            },
        });
    }
    let columns = workload?.runs().first()?.columns.clone();
    let serving = current_specs
        .iter()
        .find(|s| servable(s, &columns).is_some());
    Some(match serving {
        None => Culprit {
            fanout: fanout_of(profile, &columns),
            columns,
            reason: CulpritReason::NoIndexServes,
        },
        Some(spec) => Culprit {
            columns: spec.columns().to_vec(),
            fanout: fanout_of(profile, spec.columns()),
            reason: CulpritReason::ServingSpec { spec: spec.clone() },
        },
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use bellwether_types::Lint;

    fn lint(rule: &str, column: &str, confidence: f64) -> Lint {
        Lint {
            rule: rule.into(),
            columns: vec![column.into()],
            message: format!("{rule} on {column}"),
            confidence,
            site: None,
        }
    }

    fn profile(lints: Vec<Lint>) -> ProfileReport {
        ProfileReport {
            root: "/data".into(),
            files_total: 1,
            row_groups_total: 1,
            rows_total: 1,
            key_columns: Vec::new(),
            skipped: Vec::new(),
            time_columns: Vec::new(),
            fanout: Vec::new(),
            ranges: Vec::new(),
            time_scatter: Vec::new(),
            lints,
            workload: None,
            manifest: None,
            inputs: Vec::new(),
            memory: bellwether_profile::MemoryReport::default(),
        }
    }

    fn exact(c: &str) -> KeySpec {
        KeySpec::Exact {
            columns: vec![c.into()],
        }
    }

    fn named(c: &Culprit) -> (String, String) {
        let CulpritReason::Lint { rule, .. } = &c.reason else {
            panic!("{c:?}")
        };
        (rule.clone(), c.columns[0].clone())
    }

    /// The highest-confidence culprit lint wins; a lint on a column nothing
    /// uses, or of another rule, never does.
    #[test]
    fn the_highest_confidence_culprit_lint_on_a_used_column_wins() {
        let used = [exact("stamp"), exact("tenant")];
        let p = profile(vec![
            lint("batch_stamp_key", "stamp", 0.6),
            lint("low_selectivity_single_column", "tenant", 0.9),
            lint("batch_stamp_key", "unused", 1.0),
            lint("scattered_time_column", "tenant", 1.0),
        ]);
        let c = culprit(&used, &p, None).unwrap();
        assert_eq!(
            named(&c),
            ("low_selectivity_single_column".into(), "tenant".into())
        );
        let p = profile(vec![
            lint("batch_stamp_key", "stamp", 0.9),
            lint("low_selectivity_single_column", "tenant", 0.6),
        ]);
        assert_eq!(
            named(&culprit(&used, &p, None).unwrap()),
            ("batch_stamp_key".into(), "stamp".into())
        );
    }

    /// At equal confidence the rule catalog order decides: `batch_stamp_key`
    /// before `low_selectivity_single_column`, whichever is listed first.
    #[test]
    fn equal_confidence_culprit_lints_follow_the_rule_catalog_order() {
        let used = [exact("stamp"), exact("tenant")];
        for lints in [
            vec![
                lint("low_selectivity_single_column", "tenant", 0.8),
                lint("batch_stamp_key", "stamp", 0.8),
            ],
            vec![
                lint("batch_stamp_key", "stamp", 0.8),
                lint("low_selectivity_single_column", "tenant", 0.8),
            ],
        ] {
            let c = culprit(&used, &profile(lints), None).unwrap();
            assert_eq!(named(&c), ("batch_stamp_key".into(), "stamp".into()));
        }
    }
}
