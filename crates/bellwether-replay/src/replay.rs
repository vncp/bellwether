//! [`Replayer`]: exact replay of recorded runs against the current index, a
//! hypothetical key spec, a baseline, or a proposal, pinned to one manifest
//! version, and [`Replay`] records written through the ledger.
//!
//! # Configurations
//!
//! A configuration is a set of key specs. A run's cost under it is the cost
//! of the plan over the specs that serve the run's key columns
//! ([`crate::servable`]), intersected per key tuple (ADR 001). A run that no
//! spec of the configuration serves is priced under the replayer's
//! unserved baseline ([`Replayer::with_unserved`]): by default
//! [`Baseline::RawScan`], every live row group the planner leaves (a
//! window run's partitions), since the index cannot narrow it and the plan
//! says so rather than guessing at what a consumer would do instead; with
//! [`Baseline::ParquetStats`], of those, the row groups the files' own
//! footer min/max and embedded bloom filters keep for the run's keys (and
//! window), what any Parquet reader skips to without an index. An `asof`
//! or `latest` run that no temporal spec serves is planned on its keys
//! alone, never bounded by an instant: a recorded run does not name a time
//! column, so two callers could not agree on one. The [`Replay`] names that
//! baseline in `unserved` when it priced some run, so a record's number is
//! never read as a raw scan's.
//!
//! - **Current** ([`Replayer::replay_current`]): the manifest's declared key
//!   specs, each complete at the pinned sequence (ADR 012). With none, every
//!   run reads every row group.
//! - **Hypothetical** ([`Replayer::replay_specs`]): any specs; those the
//!   index does not have are built as transient segments over key reads and
//!   planned through the same [`PlanSource`] ([`PlanSource::with_transient`]),
//!   cached by (full spec, key and timestamp types, manifest sequence,
//!   catalog digest). A configuration holds every hypothetical index it
//!   needs at once.
//! - **A key proposal** ([`Replayer::score`]): the current specs with the
//!   proposed one added, replacing a current spec of the same
//!   [`KeySpec::name`]. A proposed spec that serves none of the workload's
//!   runs is a [`ReplayError::SpecServesNoRun`], so an irrelevant spec is
//!   never reported as "no change".
//! - **A layout proposal**: costed under the stated layout model
//!   ([`crate::layout`]), with `RunCost::exact == false`, and written with
//!   `Replay::model` naming that model (contract v2.1).
//! - **A rewrite proposal**: a [`ReplayError::RewriteNotScorable`]. Its cost
//!   depends on the rewritten plan, which a `RewriteShape` states in prose;
//!   the counterfactual hooks ([`crate::attribution`]) are where a plan
//!   model plugs in.
//!
//! Every refusal happens before anything is written.
//!
//! # Determinism (invariant 3)
//!
//! A [`Replay`] is a function of the manifest version, the data the catalog
//! digest names, the workload hash, the proposal, and the caller-supplied
//! `created_at`: runs are replayed in workload order, one at a time, through
//! ordered maps only, so the record's canonical JSON is byte-identical
//! across processes, machines, segment limits, and cache states
//! (`tests/determinism.rs`).
//!
//! `Replay::inputs` is the catalog digest. Everything else a replay reads
//! is named elsewhere in the record (the key sets by the workload hash, the
//! proposal by its hash) or derived from what the digest names (transient
//! segments). Committed segments are not listed: a manifest version names
//! them within a store, and how an index is split into segments never
//! changes a cost, so listing them would make two equal answers differ.
//! An unindexed folder keeps manifest version `"0"`; its digest tells two
//! folders, or one folder before and after a file lands, apart.
//!
//! # Memory (ADR 018)
//!
//! One run's key set at a time, the planner's block indexes and the blocks a
//! lookup touches, and the cached transient indexes (bounded by
//! [`TransientCache`]'s byte cap).

use std::borrow::Cow;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use bellwether_build::Planner;
use bellwether_core::segment::validate_spec;
use bellwether_ledger::{rfc3339::is_rfc3339, Ledger};
use bellwether_source::{FingerprintPolicy, Partitioning};
use bellwether_store::{Snapshot, StoreExt};
use bellwether_types::format::RECORD_VERSION;
use bellwether_types::{
    Error, Hash, KeySpec, ProposalBody, Record, Replay, Result, RunCost, ScanPlan, Source, Store,
};

use crate::baseline::{unserved_selection, Baseline, BaselinePlanner};
use crate::catalog::Catalog;
use crate::error::{ReplayError, ReplayResult};
use crate::keys::RunKeys;
use crate::layout::{replay_layout, LayoutModel, LayoutReplay};
use crate::plan::{
    count_cost, lookback_window, planned_time_column, run_cost, serves_query, PlanSource,
    SegmentPlanner,
};
use crate::transient::{CacheStats, TransientCache, TransientIndex, TransientLimits};
use crate::workload::{KeySetSource, RunQuery, Workload};

/// Default byte cap of a replayer's transient cache: 2 GiB.
pub const DEFAULT_CACHE_BYTES: u64 = 2 << 30;

/// A computed replay, the hash of the proposal body it scored, and, for a
/// layout proposal, the model it was costed under.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Scored {
    /// The record.
    pub replay: Replay,
    /// BLAKE3 of the canonical JSON of the [`ProposalBody`] these costs are
    /// for; [`write_replay`] refuses a replay whose cited proposal has
    /// another body.
    pub body: Hash,
    /// The layout model and its statement, for a layout proposal.
    pub layout: Option<LayoutReplay>,
}

/// Replays workloads against one pinned manifest version (module docs).
pub struct Replayer<'a> {
    source: &'a dyn Source,
    catalog: Arc<Catalog>,
    planner: Box<dyn PlanSource + 'a>,
    current: Vec<KeySpec>,
    policy: FingerprintPolicy,
    limits: TransientLimits,
    cache: TransientCache,
    unserved: Baseline,
    /// Embedded bloom filters read pricing unserved runs, for
    /// [`Replayer::bloom_filters_read`].
    filters_read: AtomicU64,
}

impl std::fmt::Debug for Replayer<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Replayer")
            .field("manifest_version", &self.catalog.manifest_version())
            .field("row_groups", &self.catalog.len())
            .field("current", &self.current)
            .finish_non_exhaustive()
    }
}

impl<'a> Replayer<'a> {
    /// A replayer over `catalog` that plans through `planner`, with
    /// `current` as the current configuration. The planner must plan
    /// against the catalog's manifest version and serve every current spec.
    pub fn new(
        source: &'a dyn Source,
        catalog: Arc<Catalog>,
        planner: Box<dyn PlanSource + 'a>,
        current: Vec<KeySpec>,
    ) -> Result<Self> {
        if planner.manifest_version() != catalog.manifest_version() {
            return Err(Error::Other(format!(
                "planner plans against manifest {} but the catalog is of {}",
                planner.manifest_version(),
                catalog.manifest_version()
            )));
        }
        check_configuration(&current)?;
        let have = planner.specs();
        for spec in &current {
            if !have.contains(spec) {
                return Err(Error::KeySpec(format!(
                    "the planner has no segments for current spec {}",
                    spec.identity()
                )));
            }
        }
        Ok(Self {
            source,
            catalog,
            planner,
            current,
            policy: FingerprintPolicy::default(),
            limits: TransientLimits::default(),
            cache: TransientCache::new(DEFAULT_CACHE_BYTES),
            unserved: Baseline::RawScan,
            filters_read: AtomicU64::new(0),
        })
    }

    /// A replayer pinned to `snapshot`'s manifest, planning through the
    /// build crate's planner (the one the index answers queries with,
    /// [`crate::build_plan`]): its catalog under the dataset schema
    /// `source` reports, which must be the index's; and as the current
    /// configuration, the declared specs complete at the pinned sequence
    /// (ADR 012). A declared spec that is not complete is not current; a
    /// configuration that names it builds it as transient segments, like
    /// any hypothetical spec.
    pub fn for_snapshot<S>(source: &'a dyn Source, snapshot: &Snapshot<S>) -> Result<Self>
    where
        S: StoreExt + ?Sized + Send + Sync + 'static,
        Snapshot<S>: Clone,
    {
        let manifest = snapshot.manifest();
        let planner = Planner::from_snapshot(snapshot.clone())?;
        let schema = source.schema(&manifest.source.root)?;
        check_same_columns(&schema, planner.schema())?;
        let catalog = Arc::new(Catalog::from_manifest(manifest, source, schema)?);
        let current = PlanSource::specs(&planner);
        let policy = match manifest.source.fingerprint_policy.as_deref() {
            None | Some("tail64k") => FingerprintPolicy::Tail64K,
            Some("full") => FingerprintPolicy::Full,
            Some(other) => {
                return Err(Error::Other(format!(
                    "unknown fingerprint policy {other:?} in the manifest"
                )))
            }
        };
        let mut r = Self::new(source, catalog, Box::new(planner), current)?;
        r.policy = policy;
        Ok(r)
    }

    /// A replayer over an unindexed folder (`Catalog::discover`, manifest
    /// version `"0"`, data named by the catalog digest): no current specs,
    /// so every run currently reads every row group, and hypothetical specs
    /// are built as transient segments. `partitioning` is the scheme
    /// `source` reads `root`'s paths under, so a window on a partition
    /// column prunes by path as an index would.
    pub fn unindexed(
        source: &'a dyn Source,
        root: &str,
        partitioning: &Partitioning,
    ) -> Result<Self> {
        let schema = source.schema(root)?;
        let catalog = Arc::new(Catalog::discover(source, root, schema)?);
        let planner =
            SegmentPlanner::new(Arc::clone(&catalog)).with_partitioning(root, partitioning.clone());
        Self::new(source, catalog, Box::new(planner), Vec::new())
    }

    /// Sets the caps on each transient build and on the layout model.
    #[must_use]
    pub fn with_limits(mut self, limits: TransientLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Replaces the transient cache with one of `max_bytes`.
    #[must_use]
    pub fn with_cache_bytes(mut self, max_bytes: u64) -> Self {
        self.cache = TransientCache::new(max_bytes);
        self
    }

    /// Sets the fingerprint policy embedded filters are verified under
    /// ([`Baseline::ParquetStats`]); it must be the one the files were
    /// fingerprinted with.
    #[must_use]
    pub fn with_fingerprint_policy(mut self, policy: FingerprintPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Sets the baseline a run that no spec of a configuration serves is
    /// priced under (module docs); [`Baseline::RawScan`] by default.
    /// [`Baseline::ParquetStats`] reads a row group's embedded bloom filters
    /// only when its footer min/max keeps it, one file at a time, and drops
    /// them after the run (ADR 018).
    #[must_use]
    pub fn with_unserved(mut self, baseline: Baseline) -> Self {
        self.unserved = baseline;
        self
    }

    /// The baseline unserved runs are priced under.
    #[must_use]
    pub fn unserved(&self) -> Baseline {
        self.unserved
    }

    /// How many embedded bloom filters pricing unserved runs has read so
    /// far: never more than the row groups the footer min/max kept times
    /// the run's key columns (ADR 018).
    #[must_use]
    pub fn bloom_filters_read(&self) -> u64 {
        self.filters_read.load(Ordering::Relaxed)
    }

    /// The manifest sequence every replay is pinned to, as a string.
    #[must_use]
    pub fn manifest_version(&self) -> String {
        self.catalog.manifest_version().to_string()
    }

    /// The pinned catalog.
    #[must_use]
    pub fn catalog(&self) -> &Arc<Catalog> {
        &self.catalog
    }

    /// The dataset schema keys are encoded at.
    #[must_use]
    pub fn schema(&self) -> &SchemaRef {
        self.catalog.schema()
    }

    /// The current configuration.
    #[must_use]
    pub fn current_specs(&self) -> &[KeySpec] {
        &self.current
    }

    /// The transient cache's counters.
    pub fn cache_stats(&self) -> Result<CacheStats> {
        self.cache.stats()
    }

    /// The transient index for `spec` over the pinned catalog, from the
    /// cache or built now.
    pub fn transient(&self, spec: &KeySpec) -> Result<Arc<TransientIndex>> {
        self.cache.get_or_build(&self.catalog, spec, || {
            TransientIndex::build(self.source, &self.catalog, spec, &self.limits)
        })
    }

    /// The planner for `specs`: the base planner plus transient segments for
    /// every spec it does not have.
    fn planner_for(&self, specs: &[KeySpec]) -> Result<Box<dyn PlanSource + '_>> {
        check_configuration(specs)?;
        let have = self.planner.specs();
        let mut extra: Vec<Arc<TransientIndex>> = Vec::new();
        for spec in specs {
            if !have.contains(spec) && !extra.iter().any(|t| t.spec() == spec) {
                extra.push(self.transient(spec)?);
            }
        }
        self.planner.with_transient(&extra)
    }

    /// The specs of `specs` that serve `keys` under `query`, and the verified
    /// plan over them, with whether it is an unserved run's plan under
    /// [`Baseline::ParquetStats`]. When none serves, or when a temporal run
    /// has no time column to plan on ([`planned_time_column`]: the serving
    /// temporal spec's, else, for a lookback, the planner's per-key bounds
    /// or window column, as the lookup verbs take it), the planner's plan
    /// reads every row group it cannot exclude (a window run's partitions),
    /// and under `ParquetStats` only those of them the files' statistics
    /// keep for the keys and the window, if any: both keep every row group
    /// holding a key, so their intersection does (invariant 1). An `asof`
    /// lookback run is planned as its window on that column.
    fn verified_plan(
        &self,
        planner: &dyn PlanSource,
        specs: &[KeySpec],
        keys: &RunKeys,
        query: &RunQuery,
    ) -> Result<(ScanPlan, bool)> {
        check_types(&self.catalog, keys)?;
        // A count is planned as the locate of its keys; [`cost_of`] prices it.
        let query = match query {
            RunQuery::Count => &RunQuery::Locate,
            q => q,
        };
        let column = planned_time_column(planner, specs, keys.columns(), query)?;
        let query = match (query, column) {
            (
                RunQuery::Lookback {
                    asof: Some(at),
                    lookback,
                },
                Some(ts),
            ) => Some(Cow::Owned(RunQuery::Window(lookback_window(
                self.catalog.schema(),
                ts,
                *at,
                *lookback,
            )?))),
            (q, column) if !q.is_temporal() || column.is_some() => Some(Cow::Borrowed(q)),
            _ => None,
        };
        // The window an unserved run's plan keeps, when it is unserved.
        let (plan, unserved) = match query {
            Some(q) => {
                let serving: Vec<KeySpec> = specs
                    .iter()
                    .filter(|s| serves_query(s, keys.columns(), &q))
                    .cloned()
                    .collect();
                let plan = planner.plan_query(&serving, keys, &q)?;
                (plan, serving.is_empty().then(|| q.window().cloned()))
            }
            None => (
                planner.plan_query(&[], keys, &RunQuery::Locate)?,
                Some(None),
            ),
        };
        self.catalog.verify_plan(&plan)?;
        let (Some(window), Baseline::ParquetStats) = (unserved, self.unserved) else {
            return Ok((plan, false));
        };
        let (kept, filters) =
            unserved_selection(&self.catalog, keys, window.as_ref(), self.policy)?;
        self.filters_read.fetch_add(filters, Ordering::Relaxed);
        let read = if plan.row_groups_selected == plan.row_groups_total {
            kept
        } else {
            self.catalog.ids_of(&plan) & kept
        };
        let mut narrowed = self.catalog.plan_for(&read)?;
        narrowed.residual = plan.residual;
        Ok((narrowed, true))
    }

    /// The time column `query` is planned on for a run keyed on
    /// `run_columns` under `specs` ([`planned_time_column`]), so a caller
    /// can check that it measures the run's instants and lengths on the
    /// column the plan uses.
    pub fn planned_time_column(
        &self,
        specs: &[KeySpec],
        run_columns: &[String],
        query: &RunQuery,
    ) -> Result<Option<String>> {
        let planner = self.planner_for(specs)?;
        planned_time_column(planner.as_ref(), specs, run_columns, query)
    }

    /// The exact cost of one ad hoc `locate` key set under `specs`.
    pub fn cost_of_keys(&self, specs: &[KeySpec], run_id: &str, keys: &RunKeys) -> Result<RunCost> {
        self.cost_of_query(specs, run_id, keys, &RunQuery::Locate)
    }

    /// The exact cost of one ad hoc key set under `specs` and `query`.
    pub fn cost_of_query(
        &self,
        specs: &[KeySpec],
        run_id: &str,
        keys: &RunKeys,
        query: &RunQuery,
    ) -> Result<RunCost> {
        let planner = self.planner_for(specs)?;
        let (plan, _) = self.verified_plan(planner.as_ref(), specs, keys, query)?;
        Ok(cost_of(query, run_id, keys.len() as u64, &plan))
    }

    /// The plan for one ad hoc `locate` key set under `specs`, as costed.
    pub fn plan_of_keys(&self, specs: &[KeySpec], keys: &RunKeys) -> Result<ScanPlan> {
        self.plan_of_query(specs, keys, &RunQuery::Locate)
    }

    /// The plan for one ad hoc key set under `specs` and `query`, as costed.
    pub fn plan_of_query(
        &self,
        specs: &[KeySpec],
        keys: &RunKeys,
        query: &RunQuery,
    ) -> Result<ScanPlan> {
        let planner = self.planner_for(specs)?;
        Ok(self.verified_plan(planner.as_ref(), specs, keys, query)?.0)
    }

    /// Every run's exact cost under `specs`, in workload order, each run
    /// planned under its recorded query kind (`locate`, `latest`, `asof`, a
    /// lookback, or `window`).
    pub fn replay_specs(
        &self,
        specs: &[KeySpec],
        workload: &Workload,
        keys: &dyn KeySetSource,
    ) -> Result<Vec<RunCost>> {
        Ok(self.costs(specs, workload, keys, None)?.0)
    }

    /// Every run's exact cost under `specs` when every run is planned with
    /// `query` (for example `RunQuery::Locate` to price point lookups on a
    /// temporal workload), in workload order.
    pub fn replay_specs_as(
        &self,
        specs: &[KeySpec],
        workload: &Workload,
        keys: &dyn KeySetSource,
        query: &RunQuery,
    ) -> Result<Vec<RunCost>> {
        Ok(self.costs(specs, workload, keys, Some(query))?.0)
    }

    /// Every run's exact cost under `specs`, each planned with `query` or
    /// else its recorded query kind, and whether some run was priced under
    /// [`Baseline::ParquetStats`] as unserved.
    fn costs(
        &self,
        specs: &[KeySpec],
        workload: &Workload,
        keys: &dyn KeySetSource,
        query: Option<&RunQuery>,
    ) -> Result<(Vec<RunCost>, bool)> {
        let planner = self.planner_for(specs)?;
        let mut stats = false;
        let costs = workload
            .runs()
            .iter()
            .map(|run| {
                let rk = workload.run_keys(keys, run)?;
                let recorded;
                let query = if let Some(q) = query {
                    q
                } else {
                    recorded = run.query()?;
                    &recorded
                };
                let (plan, unserved) = self.verified_plan(planner.as_ref(), specs, &rk, query)?;
                stats |= unserved;
                Ok(cost_of(query, &run.run_id, rk.len() as u64, &plan))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((costs, stats))
    }

    /// Every run's exact cost under the current configuration.
    pub fn replay_current(
        &self,
        workload: &Workload,
        keys: &dyn KeySetSource,
    ) -> Result<Vec<RunCost>> {
        self.replay_specs(&self.current, workload, keys)
    }

    /// Every run's exact cost under a baseline without an index.
    pub fn replay_baseline(
        &self,
        baseline: Baseline,
        workload: &Workload,
        keys: &dyn KeySetSource,
    ) -> ReplayResult<Vec<RunCost>> {
        let planner = match baseline {
            Baseline::RawScan => BaselinePlanner::raw_scan(&self.catalog),
            Baseline::ParquetStats => {
                let mut columns: Vec<String> = workload
                    .runs()
                    .iter()
                    .flat_map(|r| r.columns.iter().cloned())
                    .collect();
                columns.sort();
                columns.dedup();
                BaselinePlanner::parquet_stats(&self.catalog, &columns, self.policy)?
            }
        };
        workload
            .runs()
            .iter()
            .map(|run| {
                let rk = workload.run_keys(keys, run)?;
                check_types(&self.catalog, &rk)?;
                let plan = planner.plan(&rk, run.window.as_ref())?;
                self.catalog.verify_plan(&plan)?;
                Ok(run_cost(&run.run_id, rk.len() as u64, &plan))
            })
            .collect()
    }

    /// Scores a proposal body against the workload (module docs); the
    /// replay cites `proposal`, and [`Scored::body`] records which body the
    /// costs are for. `created_at` (RFC 3339) is supplied by the caller so
    /// the record is a function of its inputs (invariant 3).
    pub fn score(
        &self,
        proposal: &Hash,
        body: &ProposalBody,
        workload: &Workload,
        keys: &dyn KeySetSource,
        created_at: &str,
    ) -> ReplayResult<Scored> {
        if !is_rfc3339(created_at) {
            return Err(Error::Other(format!(
                "created_at {created_at:?} is not an RFC 3339 date-time"
            ))
            .into());
        }
        let (proposed, layout, proposed_stats) = match body {
            ProposalBody::Key { spec, .. } => {
                validate_spec(spec)?;
                if !workload
                    .runs()
                    .iter()
                    .any(|r| r.query().is_ok_and(|q| serves_query(spec, &r.columns, &q)))
                {
                    return Err(ReplayError::SpecServesNoRun {
                        spec: spec.name(),
                        run_columns: distinct_columns(workload),
                    });
                }
                let specs = proposed_specs(&self.current, spec);
                let (costs, stats) = self.costs(&specs, workload, keys, None)?;
                (costs, None, stats)
            }
            ProposalBody::Layout { cluster_by } => {
                if let Some(run) = workload.runs().iter().find(|r| r.window.is_some()) {
                    return Err(Error::Other(format!(
                        "a layout proposal is priced by a model of key lookups, and run {:?} \
                         looks its keys up in a time window; score it against a workload of \
                         locate runs (record one with `bellwether why --from` and no window)",
                        run.run_id
                    ))
                    .into());
                }
                let model = LayoutModel::defaults(cluster_by, &self.catalog);
                let lr = replay_layout(
                    self.source,
                    &self.catalog,
                    &model,
                    workload,
                    keys,
                    &self.limits,
                )?;
                (lr.costs.clone(), Some(lr), false)
            }
            ProposalBody::Rewrite { .. } => return Err(ReplayError::RewriteNotScorable),
        };
        let (current, current_stats) = self.costs(&self.current, workload, keys, None)?;
        // A cost model is named exactly when some proposed cost comes from
        // it (contract v2.1): a layout replay of a workload with runs.
        let model = layout
            .as_ref()
            .filter(|l| l.costs.iter().any(|c| !c.exact))
            .map(|l| l.description.clone());
        Ok(Scored {
            replay: Replay {
                prediction: None,
                record_version: RECORD_VERSION,
                proposal: *proposal,
                manifest_version: self.manifest_version(),
                workload: workload.hash(),
                current,
                proposed,
                created_at: created_at.to_string(),
                inputs: vec![self.catalog.digest()],
                model,
                configuration: Vec::new(),
                operators: Vec::new(),
                unserved: (current_stats || proposed_stats)
                    .then(|| Baseline::ParquetStats.name().to_string()),
            },
            body: Hash::of_record(body)?,
            layout,
        })
    }

    /// Resolves `proposal` in the ledger, scores its body, and writes the
    /// [`Replay`] through the ledger ([`write_replay`]). Nothing is written
    /// unless every check passes.
    pub fn record<L>(
        &self,
        ledger: &Ledger<L>,
        proposal: &Hash,
        workload: &Workload,
        keys: &dyn KeySetSource,
        created_at: &str,
    ) -> ReplayResult<(Hash, Scored)>
    where
        L: Deref,
        L::Target: Store,
    {
        let body = resolve_body(ledger, proposal)?;
        let scored = self.score(proposal, &body, workload, keys, created_at)?;
        let hash = write_replay(ledger, &scored)?;
        Ok((hash, scored))
    }
}

fn resolve_body<L>(ledger: &Ledger<L>, proposal: &Hash) -> ReplayResult<ProposalBody>
where
    L: Deref,
    L::Target: Store,
{
    match ledger.get(proposal)? {
        Record::Proposal(p) => Ok(p.body),
        _ => Err(ReplayError::NotAProposal(*proposal)),
    }
}

/// Writes a scored replay through the ledger after checking it against the
/// proposal it cites, which it resolves itself (the ledger checks again on
/// `put`). The proposal's body must be the body the costs were computed for
/// ([`Scored::body`]); the replay must name its inputs; current costs are
/// exact; and the costs must be the proposal kind's:
///
/// - a key proposal's costs are all exact and name no model;
/// - a layout proposal's costs are the layout model's, each inexact, for
///   the body's `cluster_by`, and `Replay::model` is that model's statement
///   (absent only for a workload with no runs, which has no costs);
/// - a rewrite has no replay ([`ReplayError::RewriteNotScorable`]).
///
/// Nothing is written unless every check passes.
pub fn write_replay<L>(ledger: &Ledger<L>, scored: &Scored) -> ReplayResult<Hash>
where
    L: Deref,
    L::Target: Store,
{
    let r = &scored.replay;
    let proposal = r.proposal;
    let mismatch = |detail: &str| ReplayError::ProposalMismatch {
        proposal,
        detail: detail.to_string(),
    };
    let body = resolve_body(ledger, &proposal)?;
    if Hash::of_record(&body)? != scored.body {
        return Err(mismatch(
            "the costs were computed for another proposal body",
        ));
    }
    if r.inputs.is_empty() {
        return Err(mismatch(
            "the replay names no inputs; it must cite the catalog digest of the data it read",
        ));
    }
    if r.current.iter().any(|c| !c.exact) {
        return Err(mismatch("current costs are always exact (ADR 005)"));
    }
    match &body {
        ProposalBody::Rewrite { .. } => return Err(ReplayError::RewriteNotScorable),
        ProposalBody::Key { .. } => {
            if r.proposed.iter().any(|c| !c.exact) || r.model.is_some() || scored.layout.is_some() {
                return Err(mismatch(
                    "a key proposal's costs are exact and come from no model",
                ));
            }
        }
        ProposalBody::Layout { cluster_by } => {
            let Some(layout) = &scored.layout else {
                return Err(mismatch(
                    "a layout proposal's costs come from the layout model, which is missing",
                ));
            };
            if &layout.model.cluster_by != cluster_by {
                return Err(mismatch("the layout model clusters by other columns"));
            }
            if r.proposed != layout.costs || r.proposed.iter().any(|c| c.exact) {
                return Err(mismatch(
                    "the proposed costs are not the layout model's, each inexact",
                ));
            }
            let named = (!r.proposed.is_empty()).then_some(layout.description.as_str());
            if r.model.as_deref() != named {
                return Err(mismatch(
                    "the replay must name the layout model its costs come from",
                ));
            }
        }
    }
    Ok(ledger.put(&Record::Replay(r.clone()))?)
}

/// The current specs with `candidate` added, replacing a spec of the same
/// [`KeySpec::name`].
#[must_use]
pub fn proposed_specs(current: &[KeySpec], candidate: &KeySpec) -> Vec<KeySpec> {
    let mut out: Vec<KeySpec> = current
        .iter()
        .filter(|s| s.name() != candidate.name())
        .cloned()
        .collect();
    out.push(candidate.clone());
    out
}

/// A configuration must hold valid specs with distinct names: segments are
/// filed by name, so two specs of one name could not both be committed.
fn check_configuration(specs: &[KeySpec]) -> Result<()> {
    for (i, s) in specs.iter().enumerate() {
        validate_spec(s)?;
        if specs[..i].iter().any(|o| o.name() == s.name()) {
            return Err(Error::KeySpec(format!(
                "a configuration holds two specs named {}",
                s.name()
            )));
        }
    }
    Ok(())
}

/// Refuses a source schema other than the index's (names, types,
/// nullability): keys and transient segments must encode at the index's
/// types.
fn check_same_columns(source: &Schema, index: &Schema) -> Result<()> {
    let describe = |f: Option<&std::sync::Arc<arrow::datatypes::Field>>| match f {
        None => "no column".to_string(),
        Some(f) => format!(
            "{:?}, {} `{}`",
            f.name(),
            if f.is_nullable() {
                "nullable"
            } else {
                "non-null"
            },
            f.data_type()
        ),
    };
    let n = source.fields().len().max(index.fields().len());
    for i in 0..n {
        let (s, x) = (source.fields().get(i), index.fields().get(i));
        let same = match (s, x) {
            (Some(s), Some(x)) => {
                s.name() == x.name()
                    && s.data_type() == x.data_type()
                    && s.is_nullable() == x.is_nullable()
            }
            _ => false,
        };
        if !same {
            return Err(Error::Source(format!(
                "the dataset schema changed since the index was built: column {} is {} in \
                 the files and {} in the index; replay encodes keys at the index's types, so \
                 re-index the folder",
                i + 1,
                describe(s),
                describe(x),
            )));
        }
    }
    Ok(())
}

/// Checks that a run's keys were encoded at the dataset's types.
fn check_types(catalog: &Catalog, keys: &RunKeys) -> Result<()> {
    for (c, recorded) in keys.columns().iter().zip(keys.key_types()) {
        let declared = crate::keys::declared_type(catalog.schema(), c)?;
        if bellwether_core::segment::ColumnType::of(&declared)? != *recorded {
            return Err(Error::KeySpec(format!(
                "keys on {c:?} were recorded as {recorded}, but the dataset declares \
                 {declared}; re-record the workload"
            )));
        }
    }
    Ok(())
}

fn distinct_columns(workload: &Workload) -> Vec<Vec<String>> {
    let mut out: Vec<Vec<String>> = workload.runs().iter().map(|r| r.columns.clone()).collect();
    out.sort();
    out.dedup();
    out
}

/// A run's cost under its verified plan: [`count_cost`] for a `count` run,
/// [`run_cost`] for every other kind.
fn cost_of(query: &RunQuery, run_id: &str, keys: u64, plan: &ScanPlan) -> RunCost {
    match query {
        RunQuery::Count => count_cost(run_id, keys, plan),
        _ => run_cost(run_id, keys, plan),
    }
}
