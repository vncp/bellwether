//! `advise`: workload-level physical design advisor (milestone M6; ADR 032).
//!
//! Takes one or more recorded workloads (`workloads: Vec<Hash>`, or key files
//! `from: Vec<KeyFile>`, or `all_workloads: true`), profiles the dataset
//! (recording a `profile` [`Evidence`](bellwether_types::Evidence)), and calls
//! [`bellwether_evolve::search_configurations`] to run:
//! - deterministic rule-driven key enumeration (`docs/evolve.md` §4.4),
//! - key subsumption and multi-key set search up to `max_keys` (§5.2),
//! - exact numeric knob descent over bloom false-positive rates and key-count
//!   gates (§4.3),
//! - preserve-and-extend admission with anchor runs and regression allowance
//!   `δ` (`delta`, ADR 021) and maintenance cost `m(D)`, and
//! - Pareto frontier selection with crowding distance and knee-point
//!   recommendation (§6).
//!
//! For every non-baseline configuration on the Pareto frontier, writes an
//! engine [`Proposal`](bellwether_types::Proposal) citing the profile Evidence
//! and one [`Replay`](bellwether_types::Replay) per workload so every number on
//! the frontier is backed by a content-addressed `Replay` hash.
//!
//! When `apply: true` (`bellwether advise --apply`), verifies and applies the
//! recommended frontier configuration through [`Engine::fix`], emitting a
//! [`Receipt`](bellwether_types::Receipt) (and detached ed25519 signatures when
//! a signing key is configured).

use std::collections::BTreeSet;
use std::sync::Arc;

use bellwether_evolve::{
    search_configurations, AnchorRun, BloomSweep, CandidateEval, DiscardedCandidate,
    EnumeratedSpec, FrontierEntry, GateEvaluation, SearchConfig, SubsumptionPair, WorkloadSlice,
    DEFAULT_DELTA, DEFAULT_MAX_KEYS,
};
use bellwether_profile::{ProfileOptions, ProfileReport};
use bellwether_replay::{proposed_specs, KeyInput, Replayer, RunQuery, Workload, WorkloadRecorder};
use bellwether_store::ObjectStoreBackend;
use bellwether_types::{
    format, Error, Gate, Hash, KeySpec, Proposal, ProposalBody, Record, Result, Store,
};
use serde::{Deserialize, Serialize};

use crate::engine::{folder_changes, guard, root_hint, schema_hint, Engine, Pinned};
use crate::fix::{FixOutcome, FixRequest, FixResult, SignatureWritten};
use crate::index::stale_index;
use crate::keys::KeyFile;
use crate::results::Written;
use crate::sketch::CountingSource;
use crate::why::{Dataset, ProfileEvidence, ENGINE_PROPOSER};

/// What `advise` is asked to optimize (`Engine::advise`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AdviseRequest {
    /// Recorded workload hashes to optimize across.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub workloads: Vec<Hash>,
    /// Key files to record as a workload when `workloads` is empty (or alongside them).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub from: Vec<KeyFile>,
    /// When `true` and `workloads` and `from` are empty, discovers all recorded
    /// workloads in the index's cache.
    #[serde(default)]
    pub all_workloads: bool,
    /// Maximum number of key specs in a joint configuration `D` (default 3).
    pub max_keys: usize,
    /// Optional cap on index segment bytes `m(D)`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_index_bytes: Option<u64>,
    /// Preserve-and-extend per-run regression allowance `δ` on anchor runs (default `0.05`, ADR 021).
    pub delta: f64,
    /// When `true`, verifies and applies the recommended frontier configuration
    /// through `fix` and emits a `Receipt` per applied key spec.
    #[serde(default)]
    pub apply: bool,
    /// Optional key-count gates (`--gate <name>:<n>`) to evaluate and tune.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub gates: Vec<Gate>,
}

impl Default for AdviseRequest {
    fn default() -> Self {
        Self {
            workloads: Vec::new(),
            from: Vec::new(),
            all_workloads: false,
            max_keys: DEFAULT_MAX_KEYS,
            max_index_bytes: None,
            delta: DEFAULT_DELTA,
            apply: false,
            gates: Vec::new(),
        }
    }
}

/// The recommended configuration on the Pareto frontier.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AdviseRecommendation {
    /// Rank on the Pareto frontier.
    pub frontier_rank: usize,
    /// Recommended key specs in `D`.
    pub specs: Vec<KeySpec>,
    /// Canonical short identities ([`KeySpec::identity`]).
    pub labels: Vec<String>,
    /// Engine `Proposal` hash for this configuration.
    pub proposal: Hash,
    /// Content-addressed `Replay` hashes (one per workload) backing this configuration.
    pub replays: Vec<Hash>,
    /// Baseline total bytes read across all runs.
    pub baseline_bytes: u64,
    /// Proposed total bytes read across all runs.
    pub proposed_bytes: u64,
    /// Proposed p95 run bytes read.
    pub p95_run_bytes: u64,
    /// Estimated/realized index segment bytes (`m(D)`).
    pub estimated_index_bytes: u64,
}

/// Rich typed result of [`Engine::advise`] (ADR 010).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AdviseResult {
    /// Canonical data root.
    pub root: String,
    /// Manifest sequence before advise (and before apply).
    pub sequence: u64,
    /// Manifest sequence after `--apply` (`sequence` when `apply` is false or nothing applied).
    pub sequence_after: u64,
    /// Complete key specs in the baseline configuration `a_0`.
    pub current_specs: Vec<KeySpec>,
    /// Dataset summary.
    pub dataset: Dataset,
    /// Recorded profile Evidence.
    pub evidence: ProfileEvidence,
    /// Profile report.
    pub profile: ProfileReport,
    /// Workloads evaluated.
    pub workloads: Vec<WorkloadSlice>,
    /// Baseline `a_0` evaluation.
    pub baseline: CandidateEval,
    /// Anchor runs selected across templates (ADR 021).
    pub anchors: Vec<AnchorRun>,
    /// Rule-driven enumerated singleton candidates.
    pub enumerated: Vec<EnumeratedSpec>,
    /// Subsumption pairs identified during search.
    pub subsumptions: Vec<SubsumptionPair>,
    /// Exact numeric knob descent over bloom false-positive rates.
    pub bloom_sweeps: Vec<BloomSweep>,
    /// Key-count gate evaluation and threshold descent.
    pub gate_evaluation: GateEvaluation,
    /// Non-dominated Pareto frontier (index 0 is baseline `a_0`), with `Proposal` and `Replay` hashes.
    pub frontier: Vec<FrontierEntry>,
    /// The recommended knee-point configuration on the frontier, if one improved on `a_0`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recommended: Option<AdviseRecommendation>,
    /// Discarded candidate configurations with their exact [`bellwether_evolve::DiscardReason`].
    pub discarded: Vec<DiscardedCandidate>,
    /// Engine Proposals written for frontier members (embedded so MCP `cites_of` and `budget` track them).
    pub proposals: Vec<Written>,
    /// Engine Replays written for frontier members (embedded so MCP `cites_of` and `budget` track them).
    pub replays: Vec<Written>,
    /// Total exact replay evaluations performed during search.
    pub replays_evaluated: u64,
    /// Verification and apply results when `apply: true` (one `FixResult` per applied step).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub applied: Vec<FixResult>,
    /// Receipt hashes emitted when `apply: true`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub receipts: Vec<Hash>,
    /// Detached signatures written when `apply: true` and a signing key is configured.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub signatures: Vec<SignatureWritten>,
}

impl Engine {
    /// Runs the deterministic stage-1 physical design advisor (`bellwether advise`,
    /// MCP `score.advise`, milestone M6).
    pub fn advise(&self, request: &AdviseRequest) -> Result<AdviseResult> {
        guard(|| self.advise_inner(request)).map_err(|e| root_hint(self.root(), schema_hint(e)))
    }

    #[allow(clippy::too_many_lines)]
    fn advise_inner(&self, req: &AdviseRequest) -> Result<AdviseResult> {
        self.require_folder("advise")?;
        if req.apply {
            self.require_current("advise")?;
        }
        if req.max_keys == 0 {
            return Err(Error::InvalidArgument {
                field: "max_keys",
                reason: "max_keys must be at least 1".into(),
            });
        }
        if !req.delta.is_finite() || req.delta < 0.0 {
            return Err(Error::InvalidArgument {
                field: "delta",
                reason: "delta must be a non-negative finite number (for example 0.05)".into(),
            });
        }

        let pinned = self.pin()?;
        if let Some(p) = &pinned {
            if req.apply {
                let changes = folder_changes(self.root(), p, self.format(), &self.excluded())?;
                if !changes.is_empty() {
                    return Err(stale_index(self.root(), "advise", &changes));
                }
            }
        }

        let folder = self.source(pinned.as_ref())?;
        let source: Box<dyn bellwether_types::Source> = match (&pinned, self.options().at_manifest)
        {
            (Some(p), Some(_)) => Box::new(crate::engine::SnapshotFiles::new(folder, p.manifest())),
            _ => folder,
        };
        let counting = CountingSource::new(&*source);
        let source: &dyn bellwether_types::Source = &counting;
        let sequence = pinned.as_ref().map_or(0, Pinned::sequence);
        let current_specs = pinned.as_ref().map(|p| p.specs().0).unwrap_or_default();

        // Ensure index store is writable (Proposals and Replays cite recorded Evidence)
        let store = Arc::clone(self.store()?);
        let cache = self.cache()?;

        let base_replayer = crate::fix::replayer(self, source, pinned.as_ref())?;

        // Resolve workloads: explicit hashes, recorded from `from` key files, or `all_workloads`
        let mut workloads: Vec<Workload> = Vec::new();
        let mut seen_hashes: BTreeSet<Hash> = BTreeSet::new();

        for h in &req.workloads {
            let w = crate::fix::load_workload(self, &cache, h)?;
            if seen_hashes.insert(w.hash()) {
                workloads.push(w);
            }
        }

        if !req.from.is_empty() {
            let mut recorder = WorkloadRecorder::new(&cache, Arc::clone(base_replayer.schema()));
            for (i, k) in req.from.iter().enumerate() {
                let id = if i == 0 {
                    "keys".to_string()
                } else {
                    format!("keys-{}", i + 1)
                };
                let batch = k.read()?;
                recorder.record_query(&id, KeyInput::Batch(&batch), RunQuery::Locate, None)?;
            }
            let w = recorder.finish()?;
            if seen_hashes.insert(w.hash()) {
                workloads.push(w);
            }
        }

        if workloads.is_empty() && req.all_workloads {
            for w in discover_cached_workloads(&store, &cache)? {
                if seen_hashes.insert(w.hash()) {
                    workloads.push(w);
                }
            }
        }

        if workloads.is_empty() {
            return Err(Error::NoWorkload {
                proposal: "key",
                field: "workload",
                needs:
                    "at least one recorded workload (--workload <hash> or --from <keys.parquet>)",
            });
        }

        // Attach any consumer-plan datasets referenced across all workloads
        let mut all_plans = Vec::new();
        for w in &workloads {
            for (_, p) in crate::plans::workload_plans(w, &cache)? {
                all_plans.push(p);
            }
        }
        let plan_datasets = self.datasets_for(&[], &all_plans)?;
        let replayer = plan_datasets.attach(base_replayer)?;

        // Profile the dataset and record Profile Evidence
        let (profile, evidence) =
            self.advise_profile(source, pinned.as_ref(), &store, &workloads)?;
        let evidence_hash = evidence.hash.ok_or_else(|| {
            Error::Store(format!(
                "the profile Evidence could not be recorded at {}; pass a writable location \
                 with --index",
                self.index_location()
            ))
        })?;

        let catalog = replayer.catalog();
        let dataset = Dataset {
            files: catalog.files().len() as u64,
            row_groups: catalog.len() as u64,
            rows: catalog.rows_total(),
            bytes: catalog.bytes_total(),
        };

        // Run bellwether-evolve search
        let search_cfg = SearchConfig {
            max_keys: req.max_keys,
            max_index_bytes: req.max_index_bytes,
            delta: req.delta,
            gates: req.gates.clone(),
        };
        let mut outcome =
            search_configurations(&profile, &replayer, &workloads, &cache, &search_cfg)?;

        // Write Engine Proposal and Replay records for every non-baseline frontier member
        let ledger = self.ledger()?;
        let mut proposals_written: Vec<Written> = Vec::new();
        let mut replays_written: Vec<Written> = Vec::new();

        for entry in &mut outcome.frontier {
            if entry.is_baseline || entry.specs.is_empty() {
                continue;
            }
            let first_spec = entry.specs[0].clone();
            let sidecars = entry.specs[1..].to_vec();
            let prop_written = self.put(Record::Proposal(Proposal {
                record_version: format::RECORD_VERSION,
                evidence: vec![evidence_hash],
                proposed_by: ENGINE_PROPOSER.to_string(),
                created_at: self.now(),
                body: ProposalBody::Key {
                    spec: first_spec,
                    sidecars,
                },
            }))?;
            entry.proposal = Some(prop_written.hash);

            let mut entry_replays = Vec::with_capacity(workloads.len());
            for w in &workloads {
                // Score and write Replay for each workload that at least one spec in `entry.specs` serves
                if let Ok((r_hash, scored)) =
                    replayer.record(&ledger, &prop_written.hash, w, &cache, &self.now())
                {
                    entry_replays.push(r_hash);
                    replays_written.push(Written {
                        hash: r_hash,
                        record: Record::Replay(scored.replay),
                    });
                } else {
                    // When a multi-workload configuration has specs where one spec serves w1
                    // and another serves w2, `Replayer::score` checks `serves_some_run` for
                    // EVERY spec in `ProposalBody::Key` on `w`. If a spec in `sidecars` only
                    // serves `w2` and not `w1`, we also write a per-serving-subset proposal/replay
                    // or score the serving subset on `w` so `w` has a Replay record!
                    let serving: Vec<KeySpec> = entry
                        .specs
                        .iter()
                        .filter(|s| spec_serves_workload(&replayer, s, w, &cache))
                        .cloned()
                        .collect();
                    if let Some((s_first, s_rest)) = serving.split_first() {
                        let sub_prop = self.put(Record::Proposal(Proposal {
                            record_version: format::RECORD_VERSION,
                            evidence: vec![evidence_hash],
                            proposed_by: ENGINE_PROPOSER.to_string(),
                            created_at: self.now(),
                            body: ProposalBody::Key {
                                spec: s_first.clone(),
                                sidecars: s_rest.to_vec(),
                            },
                        }))?;
                        if let Ok((r_hash, scored)) =
                            replayer.record(&ledger, &sub_prop.hash, w, &cache, &self.now())
                        {
                            entry_replays.push(r_hash);
                            proposals_written.push(sub_prop);
                            replays_written.push(Written {
                                hash: r_hash,
                                record: Record::Replay(scored.replay),
                            });
                        }
                    }
                }
            }
            entry.replays = entry_replays;
            proposals_written.push(prop_written);
        }

        let recommended = outcome.recommended_index.and_then(|idx| {
            let e = outcome.frontier.get(idx)?;
            let proposal = e.proposal?;
            Some(AdviseRecommendation {
                frontier_rank: e.rank,
                specs: e.specs.clone(),
                labels: e.labels.clone(),
                proposal,
                replays: e.replays.clone(),
                baseline_bytes: outcome.baseline.objectives.total_bytes_read,
                proposed_bytes: e.objectives.total_bytes_read,
                p95_run_bytes: e.objectives.p95_run_bytes_read,
                estimated_index_bytes: e.objectives.estimated_index_bytes,
            })
        });

        // 4. Verification & Receipts (`apply: true`)
        let mut applied = Vec::new();
        let mut receipts = Vec::new();
        let mut signatures = Vec::new();

        if req.apply {
            if let Some(rec) = &recommended {
                applied = self.apply_recommended_specs(
                    &replayer,
                    &rec.specs,
                    &workloads,
                    &cache,
                    evidence_hash,
                )?;
                for fix_res in &applied {
                    if let FixOutcome::Applied { receipt, .. }
                    | FixOutcome::Attested { receipt, .. } = &fix_res.outcome
                    {
                        receipts.push(receipt.hash);
                    }
                    signatures.extend(fix_res.signatures.clone());
                }
            }
        }

        let sequence_after = self.manifest()?.map_or(sequence, |m| m.sequence);

        Ok(AdviseResult {
            root: self.root().to_string(),
            sequence,
            sequence_after,
            current_specs,
            dataset,
            evidence,
            profile,
            workloads: outcome.workloads,
            baseline: outcome.baseline,
            anchors: outcome.anchors,
            enumerated: outcome.enumerated,
            subsumptions: outcome.subsumptions,
            bloom_sweeps: outcome.bloom_sweeps,
            gate_evaluation: outcome.gate_evaluation,
            frontier: outcome.frontier,
            recommended,
            discarded: outcome.discarded,
            proposals: proposals_written,
            replays: replays_written,
            replays_evaluated: outcome.replays_evaluated,
            applied,
            receipts,
            signatures,
        })
    }

    /// Profiles the dataset for `advise` and writes the `profile` Evidence.
    fn advise_profile(
        &self,
        source: &dyn bellwether_types::Source,
        pinned: Option<&Pinned>,
        store: &Arc<ObjectStoreBackend>,
        workloads: &[Workload],
    ) -> Result<(ProfileReport, ProfileEvidence)> {
        let mut options = ProfileOptions::for_source(&self.source_config(pinned)?);
        options.reuse_cache = self.options().clock.is_none();
        if self.options().clock.is_some() {
            options.now_millis = Some(self.now_nanos()?.div_euclid(1_000_000));
        }
        for w in workloads {
            for r in w.runs() {
                if r.query().is_ok_and(|q| q.text_op().is_some()) {
                    continue;
                }
                for c in &r.columns {
                    if !options.supplied_columns.contains(c) {
                        options.supplied_columns.push(c.clone());
                    }
                }
            }
        }
        let profile = bellwether_profile::profile(source, self.root(), store, &options)?;
        let mut evidence = bellwether_profile::evidence(&profile)?;
        for w in workloads {
            let h = w.hash();
            if !evidence.provenance.inputs.contains(&h) {
                evidence.provenance.inputs.push(h);
            }
        }
        evidence.provenance.created_at = self.now();
        let hash = self.ledger()?.put(&Record::Evidence(evidence.clone()))?;
        Ok((
            profile,
            ProfileEvidence {
                recorded: true,
                hash: Some(hash),
                evidence,
                note: None,
            },
        ))
    }

    /// Verifies and applies the recommended key specs through `Engine::fix`.
    ///
    /// - When a single workload is given and all specs serve it, applies via one
    ///   joint proposal (or per-spec when specs serve distinct workloads or an `agg`
    ///   key requires its counted companion to be committed first).
    fn apply_recommended_specs(
        &self,
        replayer: &Replayer<'_>,
        specs: &[KeySpec],
        workloads: &[Workload],
        cache: &bellwether_store::Cache<ObjectStoreBackend>,
        evidence_hash: Hash,
    ) -> Result<Vec<FixResult>> {
        // Order specs so counted Exact specs come before Agg specs that depend on them
        let mut ordered = specs.to_vec();
        ordered.sort_by_key(|s| usize::from(matches!(s, KeySpec::Agg { .. })));

        // Check if all specs can be verified together on `workloads[0]` (no Agg key
        // and all specs serve `workloads[0]` and `workloads[0]` has uniform columns).
        let has_agg = ordered.iter().any(|s| matches!(s, KeySpec::Agg { .. }));
        let single_workload_uniform = workloads.len() == 1 && {
            let runs = workloads[0].runs();
            let first_cols = runs.first().map(|r| &r.columns);
            runs.iter().all(|r| Some(&r.columns) == first_cols)
        };
        let all_serve_first = workloads.first().is_some_and(|w| {
            ordered
                .iter()
                .all(|s| spec_serves_workload(replayer, s, w, cache))
        });

        if !has_agg && single_workload_uniform && all_serve_first && !ordered.is_empty() {
            let prop = self.put(Record::Proposal(Proposal {
                record_version: format::RECORD_VERSION,
                evidence: vec![evidence_hash],
                proposed_by: ENGINE_PROPOSER.to_string(),
                created_at: self.now(),
                body: ProposalBody::Key {
                    spec: ordered[0].clone(),
                    sidecars: ordered[1..].to_vec(),
                },
            }))?;
            let res = self.fix(&FixRequest {
                proposal: Some(prop.hash),
                workload: Some(workloads[0].hash()),
                ..FixRequest::default()
            })?;
            return Ok(vec![res]);
        }

        // Otherwise, verify and apply each spec in order against the workload it best serves,
        // emitting a Certificate and Receipt per applied key spec!
        let mut results = Vec::with_capacity(ordered.len());
        let mut current = replayer.current_specs().to_vec();

        for spec in ordered {
            // Find the workload where adding `spec` to `current` gives the largest byte reduction
            // (or any workload that `spec` serves).
            let mut best_workload: Option<Hash> = None;
            let mut best_saving = 0u64;
            for w in workloads {
                if !spec_serves_workload(replayer, &spec, w, cache) {
                    continue;
                }
                let next_specs = proposed_specs(&current, &spec);
                if let (Ok(cur_c), Ok(next_c)) = (
                    replayer.replay_specs(&current, w, cache),
                    replayer.replay_specs(&next_specs, w, cache),
                ) {
                    let cur_b: u64 = cur_c.iter().map(|c| c.bytes).sum();
                    let next_b: u64 = next_c.iter().map(|c| c.bytes).sum();
                    let saving = cur_b.saturating_sub(next_b);
                    if best_workload.is_none() || saving > best_saving {
                        best_saving = saving;
                        best_workload = Some(w.hash());
                    }
                }
            }
            let Some(w_hash) = best_workload.or_else(|| workloads.first().map(Workload::hash))
            else {
                continue;
            };
            let prop = self.put(Record::Proposal(Proposal {
                record_version: format::RECORD_VERSION,
                evidence: vec![evidence_hash],
                proposed_by: ENGINE_PROPOSER.to_string(),
                created_at: self.now(),
                body: ProposalBody::Key {
                    spec: spec.clone(),
                    sidecars: Vec::new(),
                },
            }))?;
            let res = self.fix(&FixRequest {
                proposal: Some(prop.hash),
                workload: Some(w_hash),
                allow_no_gain: best_saving == 0,
                ..FixRequest::default()
            })?;
            current = proposed_specs(&current, &spec);
            results.push(res);
        }

        Ok(results)
    }
}

fn spec_serves_workload(
    replayer: &Replayer<'_>,
    spec: &KeySpec,
    workload: &Workload,
    cache: &bellwether_store::Cache<ObjectStoreBackend>,
) -> bool {
    for r in workload.runs() {
        if let Some(h) = &r.plan {
            if let Ok(plan) = bellwether_replay::load_plan(cache, h) {
                if let Ok(checked) = replayer.check_plan(&plan) {
                    if checked.serves(spec) {
                        return true;
                    }
                }
            }
        } else if let Ok(q) = r.query() {
            if bellwether_replay::serves_query(spec, &r.columns, &q) {
                return true;
            }
        }
    }
    false
}

/// Discovers all recorded [`Workload`]s in `<index>/cache/*.bin`.
fn discover_cached_workloads(
    store: &Arc<ObjectStoreBackend>,
    cache: &bellwether_store::Cache<ObjectStoreBackend>,
) -> Result<Vec<Workload>> {
    let mut paths = store.list(bellwether_store::CACHE_DIR)?;
    paths.sort();
    let mut out = Vec::new();
    for p in paths {
        let Some(name) = p.rsplit('/').next() else {
            continue;
        };
        let Some(hex) = name.strip_suffix(".bin") else {
            continue;
        };
        let Ok(h) = Hash::from_hex(hex) else {
            continue;
        };
        if let Ok(w) = Workload::load(cache, &h) {
            out.push(w);
        }
    }
    Ok(out)
}
