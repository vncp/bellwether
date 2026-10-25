//! `why`: the dataset from footer statistics and its profile (WP5: fanout
//! per column and composite, the statistics-only lints, a `profile`
//! Evidence), then, when a workload is available, a replay of it (WP6) under
//! the current configuration and under each candidate key, one engine
//! Proposal citing the Evidence and one Replay record per candidate.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::Schema;
use bellwether_core::encoding::check_ts_type;
use bellwether_profile::{ProfileOptions, ProfileReport};
use bellwether_replay::{
    proposed_specs, record_bench_workload, servable, serves_query, KeyInput, Lookup, Replayer,
    RunQuery, RunWindow, Workload, WorkloadRecorder, QUERY_KIND_LOCATE,
};
use bellwether_store::{Cache, ObjectStoreBackend, StoreExt};
use bellwether_types::{
    format, Error, Evidence, Fanout, Hash, KeySpec, Proposal, ProposalBody, Record, Result, Store,
};
use serde::{Deserialize, Serialize};

use crate::engine::{folder_changes, guard, schema_hint, Engine, Pinned, SnapshotFiles};
use crate::index::{
    bounds_time_columns, default_window_column, instant_in_unit, partition_columns,
};
use crate::keys::KeyFile;
use crate::lookback::Lookback;
use crate::results::{total, Cost, LookbackWindow, LookupKind, Written};

/// Who the engine's own candidate proposals are by.
pub const ENGINE_PROPOSER: &str = "engine";

/// Proposer names only the engine writes. `propose` refuses them (compared
/// trimmed and ignoring ASCII case), so a caller cannot pass a proposal off
/// as one of the engine's own candidates.
pub const RESERVED_PROPOSERS: &[&str] = &[ENGINE_PROPOSER];

/// Default cap on the profile's candidates `why` replays beyond the
/// workload's own key columns.
pub const DEFAULT_MAX_CANDIDATES: usize = 8;

/// A time window the runs `why` records look their keys up in
/// (`--window FROM..TO`): both ends inclusive, each an RFC 3339 instant, a
/// date, or an integer in the time column's unit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeWindow {
    /// The start as given.
    pub from: String,
    /// The end as given.
    pub to: String,
}

/// The share of the row groups a workload's runs span that every priced
/// configuration, the floor included, must read for `why` to name the
/// layout as the culprit ([`CulpritReason::Layout`]). The same threshold as
/// the profile's `no_discriminating_column` lint.
pub const LAYOUT_BOUND_FRACTION: f64 = bellwether_profile::lints::NO_DISCRIMINATING_MIN_FRACTION;

/// Which lookup `why` prices a workload's keys under (`--mode`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LookupMode {
    /// Every row holding a key (`bellwether locate`).
    Locate,
    /// The newest version of each key (`bellwether latest`).
    Latest,
    /// The version of each key current at an instant (`bellwether asof`).
    Asof,
}

impl LookupMode {
    /// The CLI spelling: `locate`, `latest`, or `asof`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Locate => "locate",
            Self::Latest => "latest",
            Self::Asof => "asof",
        }
    }
}

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
    /// The lookup the workload's keys are priced under. `None` infers it:
    /// `asof` when `at` is given, else `latest` when `latest_by` is given,
    /// else `locate`. `latest` needs `latest_by`; `asof` needs `latest_by`
    /// and `at`; `locate` takes neither, nor `lookback`.
    #[serde(default)]
    pub mode: Option<LookupMode>,
    /// Timestamp (or date, time, or integer version) column for temporal
    /// pricing (`latest`, or `asof` when `at` is also given). Records the
    /// key file run with that query kind, adds a temporal floor candidate
    /// `temporal:<run columns>@<latest_by>`, and prices candidates under
    /// both the temporal lookup and point (`locate`) lookups, choosing the
    /// recommendation by the requested temporal lookup.
    #[serde(default)]
    pub latest_by: Option<String>,
    /// Instant for `asof` pricing (with `latest_by`): RFC 3339, a date
    /// (`YYYY-MM-DD`), or an integer in `latest_by`'s unit.
    #[serde(default)]
    pub at: Option<String>,
    /// A lookback window on a `latest` or `asof` lookup: every version of
    /// each key in `[end − lookback, end]`, `end` being `at` (asof) or the
    /// key's newest version (latest). A whole number with a unit, `s`, `m`,
    /// `h`, `d`, or `w` (calendar seconds, minutes, hours, days, weeks;
    /// trading-day calendars are out of scope), converted exactly to
    /// `latest_by`'s unit; a bare integer in that unit for an integer
    /// column.
    #[serde(default)]
    pub lookback: Option<String>,
    /// Record the runs of `keys` or `bench_workload` as window runs: each
    /// looks its keys up in this window, and every cost is the window's
    /// plan (pruned by the time column as `locate --window` is). Prices
    /// `locate` lookups only; a lookback on `latest` or `asof` is
    /// `lookback`.
    pub window: Option<TimeWindow>,
    /// The window's time column; `None` takes the one a window lookup
    /// would ([`crate::Engine::locate_window`]).
    pub ts_column: Option<String>,
    /// Candidate key columns for the profile; `None` selects every id-like
    /// column.
    pub columns: Option<Vec<String>>,
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
            mode: None,
            latest_by: None,
            at: None,
            lookback: None,
            window: None,
            ts_column: None,
            columns: None,
            max_candidates: DEFAULT_MAX_CANDIDATES,
        }
    }
}

impl WhyRequest {
    /// The lookup this request prices: `mode`, or the one `at` and
    /// `latest_by` imply. An inconsistent combination (`latest` or `asof`
    /// without `latest_by`, `asof` without `at`, `latest` with `at`,
    /// `locate` with `latest_by`, `at`, or `lookback`, or a `lookback` that
    /// is not a whole number with an optional unit) is refused with one
    /// sentence saying what to pass instead. Needs no schema, so a caller
    /// can tell a usage error from an engine failure before any work.
    pub fn lookup_mode(&self) -> std::result::Result<LookupMode, String> {
        if let Some(l) = &self.lookback {
            Lookback::parse(l).map_err(|e| e.to_string())?;
        }
        let inferred = if self.at.is_some() {
            LookupMode::Asof
        } else if self.latest_by.is_some() {
            LookupMode::Latest
        } else {
            LookupMode::Locate
        };
        let Some(mode) = self.mode else {
            return match inferred {
                LookupMode::Asof if self.latest_by.is_none() => Err(
                    "--at requires --latest-by <timestamp column>; pass --latest-by <column> --at \
                     <instant>"
                        .into(),
                ),
                LookupMode::Locate if self.lookback.is_some() => Err(
                    "--lookback windows a latest or asof lookup; pass --latest-by <timestamp \
                     column> (and --at <instant> for asof)"
                        .into(),
                ),
                m => Ok(m),
            };
        };
        match mode {
            LookupMode::Locate
                if self.latest_by.is_some() || self.at.is_some() || self.lookback.is_some() =>
            {
                Err(
                    "--mode locate prices point lookups and takes no --latest-by, --at, or \
                     --lookback; pass --mode latest or --mode asof to price versions"
                        .into(),
                )
            }
            LookupMode::Latest | LookupMode::Asof if self.latest_by.is_none() => Err(format!(
                "--mode {} needs the timestamp column that orders versions; pass --latest-by \
                 <column>",
                mode.as_str()
            )),
            LookupMode::Latest if self.at.is_some() => Err(
                "--at prices the version current at an instant; pass --mode asof (or drop \
                 --mode)"
                    .into(),
            ),
            LookupMode::Asof if self.at.is_none() => Err(
                "--mode asof prices the version current at an instant; pass --at <instant>".into(),
            ),
            m => Ok(m),
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
    /// Query kind the workload's runs were priced under (`locate`, `latest`,
    /// or `asof`).
    pub query_kind: String,
    /// When `why` priced a temporal lookup (`--latest-by` or a recorded
    /// `latest`/`asof` workload): [`LookupKind::Latest`] or
    /// [`LookupKind::Asof`]. Absent (`None`, omitted in JSON) for point-only
    /// (`locate`) workloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lookup: Option<LookupKind>,
    /// The lookback window the runs were priced with (`--lookback`, or a
    /// recorded window workload). Absent (`None`, omitted in JSON) for
    /// lookups of one version per key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lookback: Option<LookbackWindow>,
    /// The distinct key-column sets the runs look up by.
    pub columns: Vec<Vec<String>>,
    /// Distinct keys per run at nearest-rank p50, p95, max.
    pub keys_p50_p95_max: [u64; 3],
    /// Input rows dropped because their tuple held a null (ADR 014).
    pub null_tuples: u64,
    /// The distinct windows of its window runs, in the time column's unit
    /// (empty when every run is a plain key lookup).
    pub windows: Vec<RunWindow>,
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
    /// A temporal key on a window run's key columns and its window's time
    /// column: its version chains keep only the row groups holding a
    /// version of a key inside the window.
    Window,
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
    /// Summed over runs under the workload's recorded query kind (`locate`
    /// by default, or `latest`/`asof` when `--latest-by` is given): what the
    /// workload would read under the current configuration with this key
    /// added.
    pub proposed: Cost,
    /// When the workload is temporal (`latest` or `asof`): what the same
    /// key configuration would read for point (`locate`) lookups over the
    /// same runs. Absent (`None`, omitted in JSON) when `proposed` is
    /// already `locate`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locate: Option<Cost>,
    /// The engine's Proposal for it, citing the profile Evidence.
    pub proposal: Written,
    /// The Replay scoring that Proposal (`inputs`: the catalog digest; a
    /// key replay is exact, so no `model`; contract v2.1).
    pub replay: Written,
}

/// The candidate that reads the least under the requested lookup kind
/// (`locate` by default, or `latest`/`asof` when `--latest-by` is given).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Recommendation {
    /// The key.
    pub spec: KeySpec,
    /// Its Proposal; pass it to `fix`.
    pub proposal: Hash,
    /// Its Replay.
    pub replay: Hash,
    /// Which lookup kind (`locate`, `latest`, or `asof`) `current` and
    /// `proposed` are priced for and by which this recommendation was chosen.
    pub lookup: LookupKind,
    /// The lookback window of a `latest` or `asof` lookup, when priced with
    /// `--lookback`. Absent (`None`, omitted in JSON) otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lookback: Option<LookbackWindow>,
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
    /// Every priced configuration, the floor included (an exact key on the
    /// workload's own columns), reads at least
    /// [`LAYOUT_BOUND_FRACTION`] of the row groups the runs span: each
    /// value is spread over nearly every row group, so the layout, not a
    /// missing index, is the cause, and no index helps.
    Layout {
        /// The fewest row groups any priced configuration reads, summed
        /// over runs.
        least_row_groups: u64,
        /// The row groups the runs span: runs times the indexed row
        /// groups.
        row_groups: u64,
        /// The measured single column with the lowest median row groups
        /// per value (ties: lower p95), and its fanout; `None` when the
        /// profile measured no single column.
        lowest_fanout: Option<Fanout>,
    },
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
    /// Summed over runs under the workload's query kind, under the current
    /// configuration (exact). Present with a workload.
    pub current: Option<Cost>,
    /// When the workload is temporal (`latest` or `asof`): summed over runs
    /// for point (`locate`) lookups under the current configuration. Absent
    /// (`None`, omitted in JSON) when `current` is already `locate`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_locate: Option<Cost>,
    /// Candidates replayed: the floor key(s) on each run's key columns, then
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
    /// The **culprit** is, in order: with a workload, the layout, when
    /// every priced configuration, the floor included, reads at least
    /// [`LAYOUT_BOUND_FRACTION`] of the row groups the runs span (it names
    /// the lowest-fanout measured column); else the highest-confidence
    /// `batch_stamp_key` or `low_selectivity_single_column` lint on a column
    /// the workload or a current spec uses; else, with a workload, its key
    /// columns when no current spec serves them, or the serving current
    /// spec's columns; each with its measured fanout.
    ///
    /// **Recommendation rule.** The `recommended` key is chosen by the
    /// workload's requested lookup kind: `locate` by default, or `latest` /
    /// `asof` when `--latest-by` is given (point-lookup costs are still
    /// reported in [`WhyReport::current_locate`] and [`Candidate::locate`]).
    /// Among all candidates it selects the one reading the fewest bytes over
    /// all runs under that lookup kind (ties broken by fewer key columns,
    /// then candidate order), and recommends it only if it reads strictly
    /// fewer bytes than the current configuration under that lookup kind.
    pub fn why(&self, request: &WhyRequest) -> Result<WhyReport> {
        guard(|| self.why_inner(request)).map_err(schema_hint)
    }

    fn why_inner(&self, req: &WhyRequest) -> Result<WhyReport> {
        let has_workload = self.check_why_request(req)?;
        let pinned = self.pin()?;
        let folder = self.source(pinned.as_ref())?;
        // Pinned to an earlier manifest: the dataset is exactly the files
        // that manifest recorded, for the profile as for the replay.
        let source: Box<dyn bellwether_types::Source> = match (&pinned, self.options().at_manifest)
        {
            (Some(p), Some(_)) => Box::new(SnapshotFiles::new(folder, p.manifest())),
            _ => folder,
        };
        let sequence = pinned.as_ref().map_or(0, Pinned::sequence);
        let current_specs = pinned.as_ref().map(|p| p.specs().0).unwrap_or_default();
        let replayer = match &pinned {
            Some(p) => Replayer::for_snapshot(&*source, &p.snapshot)?,
            None => Replayer::unindexed(&*source, self.root(), &self.partitioning(None)?)?,
        };
        let resolved_temporal = resolve_temporal(req, replayer.schema())?;
        let catalog = replayer.catalog();
        let indexed_row_groups = catalog.len() as u64;
        let mut dataset = Dataset {
            files: catalog.files().len() as u64,
            row_groups: catalog.len() as u64,
            rows: catalog.rows_total(),
            bytes: catalog.bytes_total(),
        };
        let mut untracked = Vec::new();
        if let Some(p) = pinned
            .as_ref()
            .filter(|_| self.options().at_manifest.is_none())
        {
            for fp in folder_changes(self.root(), p, self.format(), &self.excluded())?.untracked {
                for rg in source.row_groups(&fp)? {
                    dataset.row_groups += 1;
                    dataset.rows += rg.rows;
                    dataset.bytes += rg.bytes;
                }
                dataset.files += 1;
                untracked.push(fp.path);
            }
        }
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
            current_locate: None,
            candidates: Vec::new(),
            not_serving: Vec::new(),
            recommended: None,
            culprit: None,
        };
        let workload = if has_workload {
            let cache = self.cache()?;
            let (workload, temporal) =
                self.record_workload(req, pinned.as_ref(), &replayer, &cache, resolved_temporal)?;
            self.replay_candidates(
                &mut report,
                &replayer,
                &workload,
                &cache,
                req,
                temporal.as_ref(),
            )?;
            Some(workload)
        } else {
            None
        };
        report.culprit = workload
            .as_ref()
            .and_then(|w| layout_culprit(&report, w, indexed_row_groups))
            .or_else(|| culprit(&report.current_specs, &report.profile, workload.as_ref()));
        Ok(report)
    }

    /// Refuses a `why` request whose flags contradict each other, before any
    /// work. Returns whether it carries a workload.
    fn check_why_request(&self, req: &WhyRequest) -> Result<bool> {
        let given = [
            req.keys.is_some(),
            req.bench_workload.is_some(),
            req.workload.is_some(),
        ];
        if given.iter().filter(|g| **g).count() > 1 {
            return Err(Error::InvalidArgument {
                field: "workload",
                reason: "give one workload: a key file, a bench workload directory, or a \
                         recorded workload hash, not several"
                    .into(),
            });
        }
        if let Some(cols) = &req.columns {
            self.check_columns(&cols.iter().collect::<Vec<_>>())?;
        }
        let mode = req.lookup_mode().map_err(Error::KeySpec)?;
        if req.window.is_some() && !(given[0] || given[1]) {
            return Err(Error::Other(
                "a window (--window) applies to the runs why records: give a key file (--from) or \
                 a bench workload (--bench-workload); a recorded workload (--workload) keeps the \
                 windows it was recorded with"
                    .into(),
            ));
        }
        if req.window.is_some() && mode != LookupMode::Locate {
            return Err(Error::KeySpec(
                "--window prices point lookups in a time window; a latest or asof lookup takes a \
                 lookback instead: pass --latest-by <column> --lookback <length> (and --at \
                 <instant> for asof) without --window"
                    .into(),
            ));
        }
        if req.window.is_none() && req.ts_column.is_some() {
            return Err(Error::Other(
                "--ts names a window's time column; give the window too (--window)".into(),
            ));
        }
        if mode != LookupMode::Locate && req.keys.is_none() && req.workload.is_none() {
            return Err(Error::KeySpec(
                "--latest-by prices a workload's keys under latest or asof lookups; pass --from \
                 <keys.parquet> (or --workload <hash>)"
                    .into(),
            ));
        }
        Ok(given.iter().any(|g| *g))
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
        let store = match (
            &writable,
            pinned.filter(|_| self.options().at_manifest.is_some()),
        ) {
            // Pinned to an earlier manifest: the profile reads that
            // manifest, in memory, so its lints see the specs it declared.
            (_, Some(p)) => {
                let memory = Arc::new(ObjectStoreBackend::memory()?);
                memory.put_if_absent("manifest.json", p.bytes.clone())?;
                memory
            }
            (Some(s), None) => Arc::clone(s),
            (None, None) => {
                // Profile in memory, with the index's manifest when it can
                // be read, so a read-only index still informs the lints.
                let memory = Arc::new(ObjectStoreBackend::memory()?);
                if let Some(existing) = self.existing_store().ok().flatten() {
                    if let Some((bytes, _)) = existing.get_opt("manifest.json")? {
                        memory.put_if_absent("manifest.json", bytes)?;
                    }
                }
                memory
            }
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
        // A key file or a recorded workload says which columns lookups
        // supply, as a bench workload does (contract v3 backlog #35): the
        // lints then read them instead of assuming an order.
        if let Some(keys) = &req.keys {
            options.supplied_columns = keys.column_names()?;
        } else if let Some(h) = &req.workload {
            // An unreadable workload is refused by `record_workload`, with
            // its own message; here it only informs no column.
            if let Some(w) = self.cache().ok().and_then(|c| Workload::load(&c, h).ok()) {
                for r in w.runs() {
                    for c in &r.columns {
                        if !options.supplied_columns.contains(c) {
                            options.supplied_columns.push(c.clone());
                        }
                    }
                }
            }
        }
        let profile = bellwether_profile::profile(source, self.root(), &store, &options)?;
        let mut evidence = bellwether_profile::evidence(&profile)?;
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
        pinned: Option<&Pinned>,
        replayer: &Replayer<'_>,
        cache: &Cache<ObjectStoreBackend>,
        resolved: Option<ResolvedTemporal>,
    ) -> Result<(Workload, Option<ResolvedTemporal>)> {
        if let Some(h) = &req.workload {
            let w = crate::fix::load_workload(self, cache, h)?;
            let temporal = match resolved {
                Some(t) => {
                    // The runs are priced as recorded: a `--latest-by` that
                    // asks for another lookup would label their costs as
                    // the requested lookup's.
                    for r in w.runs() {
                        let q = r.query()?;
                        if q != t.query {
                            return Err(Error::KeySpec(format!(
                                "workload {h} records its runs as {} lookups, and --mode, \
                                 --latest-by, --at, and --lookback ask for {} lookups; record \
                                 the keys again with `bellwether why --from <keys.parquet> \
                                 --latest-by {}` and the same options",
                                describe(&q),
                                describe(&t.query),
                                t.ts_column
                            )));
                        }
                    }
                    Some(t)
                }
                None if req.mode == Some(LookupMode::Locate) => {
                    if let Some(r) = w
                        .runs()
                        .iter()
                        .find(|r| r.query().ok() != Some(RunQuery::Locate))
                    {
                        return Err(Error::KeySpec(format!(
                            "workload {h} records its runs as {} lookups, and --mode locate asks \
                             for point lookups; drop --mode to price them as recorded, or record \
                             the keys again with `bellwether why --from <keys.parquet>`",
                            describe(&r.query()?)
                        )));
                    }
                    None
                }
                None => infer_workload_temporal(&w, replayer.current_specs())?,
            };
            return Ok((w, temporal));
        }
        let recorder = WorkloadRecorder::new(cache, std::sync::Arc::clone(replayer.schema()));
        if let Some(keys) = &req.keys {
            let batch = keys.read()?;
            if let Some(t) = &resolved {
                for f in batch.schema().fields() {
                    if f.name() == &t.ts_column {
                        return Err(Error::KeySpec(format!(
                            "--latest-by column {:?} is also a key column of the lookup; give a \
                             separate timestamp column or select key columns with --key",
                            t.ts_column
                        )));
                    }
                }
            }
            let columns: Vec<String> = batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            let mut recorder = self.windowed(recorder, req, pinned, replayer, &columns)?;
            let query = resolved
                .as_ref()
                .map_or(RunQuery::Locate, |t| t.query.clone());
            recorder.record_query("keys", KeyInput::Batch(&batch), query, None)?;
            return Ok((recorder.finish()?, resolved));
        }
        let mut recorder = self.windowed(recorder, req, pinned, replayer, &[])?;
        if let Some(dir) = &req.bench_workload {
            let file = if dir.is_dir() {
                dir.join("workload.json")
            } else {
                dir.clone()
            };
            record_bench_workload(&mut recorder, &file).map_err(Error::from)?;
        }
        Ok((recorder.finish()?, resolved))
    }

    /// `recorder`, recording window runs when the request gives a window:
    /// its time column (`--ts`, else the one a window lookup on `columns`
    /// would take) and both bounds in that column's unit.
    fn windowed<'r>(
        &self,
        recorder: WorkloadRecorder<'r>,
        req: &WhyRequest,
        pinned: Option<&Pinned>,
        replayer: &Replayer<'_>,
        columns: &[String],
    ) -> Result<WorkloadRecorder<'r>> {
        let Some(window) = &req.window else {
            return Ok(recorder);
        };
        let schema = replayer.schema();
        let ts_column = if let Some(c) = &req.ts_column {
            c.clone()
        } else {
            let (specs, bounded) = match pinned {
                Some(p) => (p.specs().0, bounds_time_columns(p, columns)),
                None => (Vec::new(), Vec::new()),
            };
            let first = replayer.catalog().files().first().map(|f| f.path.as_str());
            let partition = partition_columns(&self.partitioning(pinned)?, self.root(), first)?;
            default_window_column(schema, &specs, bounded, partition, columns)?
        };
        let ty = schema
            .field_with_name(&ts_column)
            .map_err(|_| {
                Error::KeySpec(format!(
                    "the window's time column {ts_column:?} is not in the dataset; name one with \
                     --ts"
                ))
            })?
            .data_type()
            .clone();
        let TimeWindow { from, to } = window;
        let lo = instant_in_unit("window", from, &ts_column, &ty)?;
        let hi = instant_in_unit("window", to, &ts_column, &ty)?;
        if lo > hi {
            return Err(Error::Other(format!(
                "the window starts at {from} ({ts_column} = {lo}) after it ends at {to} ({hi}); \
                 give the start first"
            )));
        }
        recorder.in_window(RunWindow { ts_column, lo, hi })
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
        temporal: Option<&ResolvedTemporal>,
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
        let mut windows: Vec<RunWindow> = Vec::new();
        for w in workload.runs().iter().filter_map(|r| r.window.as_ref()) {
            if !windows.contains(w) {
                windows.push(w.clone());
            }
        }
        if let Some(t) = &temporal {
            for cols in &run_columns {
                if cols.contains(&t.ts_column) {
                    return Err(Error::KeySpec(format!(
                        "--latest-by column {:?} is also a key column of the workload; give a \
                         separate timestamp column",
                        t.ts_column
                    )));
                }
            }
        }
        let query_kind = temporal
            .as_ref()
            .map_or(QUERY_KIND_LOCATE, |t| t.query.kind_str())
            .to_string();
        let lookup_kind = temporal
            .as_ref()
            .map_or(LookupKind::Locate, |t| t.lookup.clone());
        let lookback = temporal.as_ref().and_then(|t| t.lookback.clone());
        report.workload = Some(WorkloadInfo {
            hash: workload.hash(),
            runs: workload.runs().len() as u64,
            query_kind,
            lookup: temporal.as_ref().map(|t| t.lookup.clone()),
            lookback: lookback.clone(),
            columns: run_columns.clone(),
            keys_p50_p95_max: workload.keys_p50_p95_max(),
            null_tuples: workload.runs().iter().map(|r| r.null_tuples).sum(),
            windows,
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
        if let Some(t) = &temporal {
            for c in &run_columns {
                let spec = KeySpec::Temporal {
                    columns: c.clone(),
                    ts_column: t.ts_column.clone(),
                };
                if !current_names.contains(&spec.name()) && names.insert(spec.name()) {
                    chosen.push((spec, CandidateOrigin::Workload));
                }
            }
        }
        for c in &run_columns {
            let spec = KeySpec::Exact { columns: c.clone() };
            if !current_names.contains(&spec.name()) && names.insert(spec.name()) {
                chosen.push((spec, CandidateOrigin::Workload));
            }
        }
        // A window run: the temporal key on its key columns and the
        // window's time column, whose version chains prune by time per key.
        let windowed: BTreeSet<(Vec<String>, String)> = workload
            .runs()
            .iter()
            .filter_map(|r| {
                r.window
                    .as_ref()
                    .map(|w| (r.columns.clone(), w.ts_column.clone()))
            })
            .collect();
        for (columns, ts_column) in windowed {
            let orderable = replayer
                .schema()
                .field_with_name(&ts_column)
                .is_ok_and(|f| bellwether_core::encoding::check_ts_type(f.data_type()).is_ok());
            if !orderable || columns.contains(&ts_column) {
                continue;
            }
            let spec = KeySpec::Temporal { columns, ts_column };
            if !current_names.contains(&spec.name()) && names.insert(spec.name()) {
                chosen.push((spec, CandidateOrigin::Window));
            }
        }
        let mut from_profile = 0usize;
        for entry in &report.profile.fanout {
            let cols = &entry.fanout.columns;
            let exact_spec = KeySpec::Exact {
                columns: cols.clone(),
            };
            if !serves(&exact_spec) {
                if !current_names.contains(&exact_spec.name())
                    && !names.contains(&exact_spec.name())
                {
                    report.not_serving.push(exact_spec);
                }
                continue;
            }
            if !point(&exact_spec) {
                continue;
            }
            if let Some(t) = &temporal {
                if !cols.contains(&t.ts_column) {
                    let tspec = KeySpec::Temporal {
                        columns: cols.clone(),
                        ts_column: t.ts_column.clone(),
                    };
                    if !current_names.contains(&tspec.name())
                        && !names.contains(&tspec.name())
                        && from_profile < req.max_candidates
                    {
                        names.insert(tspec.name());
                        chosen.push((tspec, CandidateOrigin::Profile));
                        from_profile += 1;
                    }
                }
            }
            if !current_names.contains(&exact_spec.name())
                && !names.contains(&exact_spec.name())
                && from_profile < req.max_candidates
            {
                names.insert(exact_spec.name());
                chosen.push((exact_spec, CandidateOrigin::Profile));
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
            let (replay_hash, scored) = if temporal.is_some()
                && workload
                    .runs()
                    .iter()
                    .all(|r| matches!(r.query(), Ok(RunQuery::Locate)))
            {
                // A recorded locate workload replayed with --latest-by: score
                // against a synthetic temporal view of the same runs, or
                // score directly when the workload already records the query.
                let tw = with_query(
                    workload,
                    &temporal.map(|t| t.query.clone()).unwrap_or_default(),
                )?;
                replayer
                    .record(&ledger, &proposal.hash, &tw, cache, &self.now())
                    .map_err(Error::from)?
            } else {
                replayer
                    .record(&ledger, &proposal.hash, workload, cache, &self.now())
                    .map_err(Error::from)?
            };
            let locate = if temporal.is_some() {
                let specs = proposed_specs(replayer.current_specs(), &spec);
                Some(total(&replayer.replay_specs_as(
                    &specs,
                    workload,
                    cache,
                    &RunQuery::Locate,
                )?))
            } else {
                None
            };
            report.candidates.push(Candidate {
                spec,
                origin,
                proposed: total(&scored.replay.proposed),
                locate,
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
            _ => match &temporal {
                Some(t) => total(&replayer.replay_specs_as(
                    replayer.current_specs(),
                    workload,
                    cache,
                    &t.query,
                )?),
                None => total(&replayer.replay_current(workload, cache)?),
            },
        };
        report.current = Some(current);
        if temporal.is_some() {
            report.current_locate = Some(total(&replayer.replay_specs_as(
                replayer.current_specs(),
                workload,
                cache,
                &RunQuery::Locate,
            )?));
        }
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
                lookup: lookup_kind,
                lookback,
                current,
                proposed: c.proposed,
            });
        Ok(())
    }
}

/// A run's lookup in words, for refusals: `locate`, `latest`, `asof at 30`,
/// either with `, lookback 3600` (in the timestamp column's unit), or
/// `window 10..=20 of ts`.
fn describe(q: &RunQuery) -> String {
    let base = match (q.asof_ts(), q.window()) {
        (Some(ts), _) => format!("asof at {ts}"),
        (None, Some(w)) => format!("window {}..={} of {}", w.lo, w.hi, w.ts_column),
        (None, None) => q.kind_str().to_string(),
    };
    match q.lookback() {
        Some(l) => format!("{base}, lookback {l}"),
        None => base,
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

/// The layout culprit documented on [`Engine::why`]: `None` unless the
/// floor was priced (a candidate exact key on a run's own columns, or a
/// current spec serving the first run) and every priced configuration reads
/// at least [`LAYOUT_BOUND_FRACTION`] of the `runs × indexed_row_groups` row
/// groups the runs span. A dataset of one row group never names the layout.
fn layout_culprit(r: &WhyReport, w: &Workload, indexed_row_groups: u64) -> Option<Culprit> {
    let current = r.current?;
    let columns = w.runs().first()?.columns.clone();
    let floor_priced = r
        .candidates
        .iter()
        .any(|c| c.origin == CandidateOrigin::Workload)
        || r.current_specs
            .iter()
            .any(|s| servable(s, &columns).is_some());
    let row_groups = indexed_row_groups * w.runs().len() as u64;
    // One row group has nothing to prune: no layout could do better.
    if !floor_priced || indexed_row_groups < 2 {
        return None;
    }
    let least_row_groups = r
        .candidates
        .iter()
        .map(|c| c.proposed.row_groups)
        .chain([current.row_groups])
        .min()?;
    #[allow(clippy::cast_precision_loss)]
    if (least_row_groups as f64) < LAYOUT_BOUND_FRACTION * row_groups as f64 {
        return None;
    }
    let lowest_fanout = r
        .profile
        .fanout
        .iter()
        .map(|e| &e.fanout)
        .filter(|f| f.columns.len() == 1)
        .min_by(|a, b| {
            a.row_groups_per_value[0]
                .total_cmp(&b.row_groups_per_value[0])
                .then(a.row_groups_per_value[1].total_cmp(&b.row_groups_per_value[1]))
        })
        .cloned();
    Some(Culprit {
        fanout: fanout_of(&r.profile, &columns),
        columns,
        reason: CulpritReason::Layout {
            least_row_groups,
            row_groups,
            lowest_fanout,
        },
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
    let query = workload
        .and_then(|w| w.runs().first())
        .and_then(|r| r.query().ok())
        .unwrap_or(RunQuery::Locate);
    let serving = current_specs
        .iter()
        .find(|s| serves_query(s, &columns, &query));
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

#[derive(Clone, Debug)]
struct ResolvedTemporal {
    ts_column: String,
    query: RunQuery,
    lookup: LookupKind,
    lookback: Option<LookbackWindow>,
}

/// Validates `req.latest_by` (with `req.at` and `req.lookback`) against the
/// dataset schema before profiling or recording: the column must exist in
/// the dataset and have a type that can order versions (`check_ts_type`:
/// timestamp, date, time, or integer), and `at` and `lookback` must convert
/// exactly to that column's unit. `None` for a `locate` request.
fn resolve_temporal(req: &WhyRequest, schema: &Schema) -> Result<Option<ResolvedTemporal>> {
    if req.lookup_mode().map_err(Error::KeySpec)? == LookupMode::Locate {
        return Ok(None);
    }
    let Some(ts_column) = &req.latest_by else {
        return Ok(None);
    };
    let ts_column = ts_column.trim();
    if ts_column.is_empty() {
        return Err(Error::KeySpec(
            "--latest-by needs a timestamp column name; pass --latest-by <column>".into(),
        ));
    }
    let field = schema.field_with_name(ts_column).map_err(|_| {
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        Error::KeySpec(format!(
            "--latest-by column {ts_column:?} is not in the dataset schema (columns: {}); pass an \
             existing timestamp, date, time, or integer column with --latest-by",
            names.join(", ")
        ))
    })?;
    let ts_type = field.data_type();
    check_ts_type(ts_type).map_err(|_| {
        Error::KeySpec(format!(
            "--latest-by column {ts_column:?} has type {ts_type}, which cannot order versions; \
             pass a timestamp, date, time, or integer column with --latest-by"
        ))
    })?;
    let (query, lookup) = match &req.at {
        None => (RunQuery::Latest, LookupKind::Latest),
        Some(at) => {
            let ts = instant_in_unit("at", at, ts_column, ts_type)?;
            (
                RunQuery::Asof { ts },
                LookupKind::Asof {
                    at: at.clone(),
                    ts,
                    ts_column: ts_column.to_string(),
                },
            )
        }
    };
    let lookback = match &req.lookback {
        None => None,
        Some(given) => {
            let lb = Lookback::parse(given)?;
            Some(LookbackWindow {
                given: lb.as_given().to_string(),
                length: lb.in_unit(ts_column, ts_type)?,
            })
        }
    };
    let query = match &lookback {
        None => query,
        Some(l) => RunQuery::Lookback {
            asof: query.asof_ts(),
            lookback: l.length,
        },
    };
    Ok(Some(ResolvedTemporal {
        ts_column: ts_column.to_string(),
        query,
        lookup,
        lookback,
    }))
}

/// When `--workload <hash>` loads a workload whose runs already record
/// `latest` or `asof`, derive the temporal lookup metadata from the workload
/// and any current temporal spec.
fn infer_workload_temporal(
    workload: &Workload,
    current_specs: &[KeySpec],
) -> Result<Option<ResolvedTemporal>> {
    let Some(first) = workload.runs().first() else {
        return Ok(None);
    };
    let query = first.query()?;
    // A window run reads every row of its keys in a fixed window: no
    // version is chosen, so there is no temporal lookup to infer.
    if matches!(query, RunQuery::Locate | RunQuery::Window(_)) {
        return Ok(None);
    }
    let ts_column = current_specs
        .iter()
        .find_map(|s| match s {
            KeySpec::Temporal { ts_column, .. } => Some(ts_column.clone()),
            _ => None,
        })
        .ok_or_else(|| {
            Error::KeySpec(format!(
                "workload {} records {} runs; pass --latest-by <timestamp column> so `why` knows \
                 which timestamp column to build temporal candidates on",
                workload.hash(),
                first.query_kind
            ))
        })?;
    let lookup = match query.asof_ts() {
        None => LookupKind::Latest,
        Some(ts) => LookupKind::Asof {
            at: ts.to_string(),
            ts,
            ts_column: ts_column.clone(),
        },
    };
    let lookback = query.lookback().map(|length| LookbackWindow {
        given: length.to_string(),
        length,
    });
    Ok(Some(ResolvedTemporal {
        ts_column,
        query,
        lookup,
        lookback,
    }))
}

fn with_query(workload: &Workload, query: &RunQuery) -> Result<Workload> {
    let mut m = workload.manifest().clone();
    for r in &mut m.runs {
        r.query_kind = query.kind_str().to_string();
        r.asof = query.asof_ts();
        r.lookback = query.lookback();
    }
    Workload::new(m)
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
