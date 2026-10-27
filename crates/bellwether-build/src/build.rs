//! [`Builder::build`]: discover, fingerprint, assign ids, write segments in
//! parallel, and publish everything in one manifest commit.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::{Schema, SchemaRef};
use bellwether_core::encoding;
use bellwether_core::segment::{validate_spec, ColumnType};
use bellwether_core::sidecar::SidecarDef;
use bellwether_source::{FingerprintPolicy, Partitioning, DEFAULT_CHUNK_SIZE};
use bellwether_store::{
    commit_with, now_rfc3339, read_manifest, spec_json, ChunkRange, ColumnDef, CommitOptions,
    Extra, FileEntry, Manifest, RowGroupSpace, RowGroupTombstone, SchemaInfo, SegmentRef,
    SourceInfo, StagedSegments, StoreExt,
};
use bellwether_types::format::STORAGE_VERSION;
use bellwether_types::{Error, FileFingerprint, KeySpec, Result, RowGroupId, SketchSpec};
use roaring::RoaringBitmap;

use crate::config::{
    columns_to_schema, parse_policy, persistable, policy_name, schema_to_columns, BuildObserver,
    BuildOptions, SourceKind,
};
use crate::coverage::{completeness, completeness_map, covered, live_ids, segment_of, Locator};
use crate::segments::{self, Ctx, FileRow, SpecPlan};
use crate::sources::{infer_schema, DataSource};
use crate::target::{
    bloom_bounds_def, decl_of, declared_key_specs, declared_sidecars, declared_sketches,
    declared_targets, fold_legacy, known_decl, sketch_columns, sketch_decl, tombstone_decl,
    tombstone_name, tombstone_of, unknown_kind, Target, SKETCH_KIND,
};
use bellwether_core::tombstone::TOMBSTONE_KIND;

/// Most key specs one index may declare.
pub const MAX_KEY_SPECS: usize = 64;

/// Builds and maintains one index: the manifest and segments under
/// `index_root` in `store`, over one source folder (ADR 003, 012, 013).
pub struct Builder<S: StoreExt + ?Sized> {
    store: Arc<S>,
    index_root: String,
}

impl<S: StoreExt + ?Sized> std::fmt::Debug for Builder<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Builder")
            .field("index_root", &self.index_root)
            .finish_non_exhaustive()
    }
}

/// What a build did.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BuildReport {
    /// The manifest sequence the build committed (ADR 020), or `None` when
    /// there was nothing to commit.
    pub sequence: Option<u64>,
    /// Files seen for the first time.
    pub files_new: u64,
    /// Tracked files whose fingerprint changed (ADR 013).
    pub files_changed: u64,
    /// Tracked files no longer present; their ids are retired.
    pub files_removed: u64,
    /// Tracked files with an unchanged fingerprint; none of them is read
    /// unless a key spec needs its row groups.
    pub files_unchanged: u64,
    /// Fresh row-group ids assigned.
    pub row_groups_assigned: u64,
    /// Row-group ids retired by drift or removal; never reused.
    pub row_groups_retired: u64,
    /// `Source::read_columns` calls: row groups read, once each.
    pub row_groups_read: u64,
    /// `Source::row_groups` calls made by the workers.
    pub files_listed: u64,
    /// Segments written and committed.
    pub segments_written: u64,
    /// Their manifest entries.
    pub segments: Vec<SegmentRef>,
    /// Key specs whose segments were rebuilt over every live row group: new
    /// specs, specs whose definition changed under the same name, and specs
    /// whose key or timestamp column changed type, and the specs in
    /// `specs_repaired`.
    pub specs_rebuilt: Vec<String>,
    /// Counted key specs (by identity) rebuilt because two of their
    /// segments covered one live row group, so their rows would have been
    /// counted twice: a manifest no build writes (another tool's, or an
    /// edited one), which every planner refuses as a corrupt index.
    pub specs_repaired: Vec<String>,
    /// Text keys rebuilt because their segments were built under other
    /// Unicode tables than this binary's (after a deliberate upgrade of a
    /// table crate), with the tables they recorded; each is also in
    /// `specs_rebuilt`.
    pub text_tables_replaced: Vec<TextTablesReplaced>,
    /// Key specs removed because [`BuildOptions::remove_unlisted`] was set
    /// and they were not listed; their segments were dropped.
    pub specs_removed: Vec<String>,
    /// Unchanged files whose per-row-group statistics (storage version 2:
    /// rows, bytes, column bytes) this build recorded, each from one
    /// `Source::row_groups` call: every file of a version 1 manifest on the
    /// first build by this binary that commits, and every file when the
    /// schema's columns changed. Zero when nothing was committed.
    pub files_filled: u64,
    /// The storage version of the manifest this build committed over, when
    /// older than this binary's: the commit upgraded the index to
    /// [`bellwether_types::format::STORAGE_VERSION`] (ADR 028).
    pub upgraded_from: Option<u32>,
    /// The sidecars of an index written before M5 (per-key time bounds,
    /// bitemporal versions) this build folded into key specs, each with the
    /// identity of the spec it folded into (`("bounds:id@ts",
    /// "exact:id@ts")`); those specs were rebuilt (`specs_rebuilt`). Only
    /// what folded: a sidecar no key spec could take is in
    /// [`BuildReport::sidecars_kept`].
    pub sidecars_folded: Vec<(String, String)>,
    /// The sidecars of an index written before M5 no key spec can take
    /// (another spec on their key columns holds its slot with another time
    /// column): kept as sidecars, built like any target, still planned.
    pub sidecars_kept: Vec<String>,
    /// On a build with nothing to do over an index written before M5: the
    /// folds its next build with work will make (as `sidecars_folded`).
    pub sidecars_fold_pending: Vec<(String, String)>,
    /// Restarts after a conflicting concurrent commit.
    pub restarts: u32,
    /// Most row groups held by workers at once (at most the thread count).
    pub peak_row_groups_held: u64,
    /// Largest key count one open segment reached (see [`BuildOptions`]).
    pub peak_segment_keys: u64,
    /// Wall time.
    pub elapsed: Duration,
}

/// A text key `index` rebuilt because its segments recorded other Unicode
/// tables ([`BuildReport::text_tables_replaced`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TextTablesReplaced {
    /// The key's name.
    pub spec: String,
    /// The tables its segments recorded.
    pub recorded: bellwether_core::text::UnicodeTables,
    /// This binary's tables, which the rebuilt segments record.
    pub current: bellwether_core::text::UnicodeTables,
}

enum Attempt {
    Done(Box<BuildReport>),
    Restart { claimed: bool },
}

impl<S: StoreExt + ?Sized> Builder<S> {
    /// A builder for the index under `index_root` in `store`.
    pub fn new(store: Arc<S>, index_root: impl Into<String>) -> Self {
        Self {
            store,
            index_root: index_root.into(),
        }
    }

    /// The store.
    #[must_use]
    pub fn store(&self) -> &Arc<S> {
        &self.store
    }

    /// The index root within the store.
    #[must_use]
    pub fn index_root(&self) -> &str {
        &self.index_root
    }

    /// The current manifest, if the index exists.
    pub fn manifest(&self) -> Result<Option<Manifest>> {
        Ok(read_manifest(&*self.store, &self.index_root)?.map(|(m, _)| m))
    }

    /// The sequence at which `spec` became complete over every tracked row
    /// group (ADR 012), or `None` if it is not declared exactly so, not
    /// recorded, or not covering every live row group in the current
    /// manifest. See [`crate::completeness`].
    pub fn completeness(&self, spec: &KeySpec) -> Result<Option<u64>> {
        Ok(self.manifest()?.and_then(|m| completeness(&m, spec)))
    }

    /// Removes `spec` from the index in one manifest commit that reads no
    /// source file: its declaration, its completeness entry, and its
    /// segments, which the commit tombstones for the store's garbage
    /// collector. Returns the committed sequence, or `None` if the index
    /// does not declare `spec`. A different spec declared under the same
    /// name ([`KeySpec::name`] omits `ts_column` and `fpr`) is an error,
    /// never removed in its place. A temporal or bitemporal spec given
    /// with the tombstone it is declared with (B8) removes that tombstone
    /// only (its sidecar declaration, segments and completeness), keeping
    /// the spec; given without it, the spec goes with its tombstone.
    pub fn remove_spec(&self, spec: &KeySpec) -> Result<Option<u64>> {
        let name = spec.name();
        let absent = Cell::new(false);
        let committed = commit_with(
            &*self.store,
            &self.index_root,
            &StagedSegments::new(),
            CommitOptions::default(),
            |m| {
                let declared = declared_key_specs(m);
                let d = match declared.iter().find(|s| s.name() == name) {
                    None => {
                        absent.set(true);
                        return Err(Error::KeySpec(format!("key spec {name} is not declared")));
                    }
                    Some(d) if d != spec && d.without_tombstone() != *spec => {
                        return Err(Error::KeySpec(format!(
                            "the index declares {} under the name {name}, not {}",
                            d.identity(),
                            spec.identity()
                        )))
                    }
                    Some(d) => d.clone(),
                };
                let mut next = m.clone();
                if d.tombstone().is_some() {
                    let tomb = tombstone_name(&d);
                    next.schema.sidecars.retain(|s| {
                        !(s.kind == TOMBSTONE_KIND
                            && tombstone_of(s, &m.schema.key_specs).as_ref() == Some(&d))
                    });
                    next.segments.retain(|s| {
                        !(s.kind.as_deref() == Some(TOMBSTONE_KIND) && s.key_spec == tomb)
                    });
                    next.key_spec_completeness.remove(&tomb);
                }
                if spec.tombstone().is_none() {
                    // Segments and completeness are keyed by the declared
                    // spec's identity (storage version 2).
                    let identity = spec.identity();
                    next.schema.key_specs.retain(|s| s.name() != name);
                    next.segments.retain(|s| s.key_spec != identity);
                    next.key_spec_completeness.remove(&identity);
                }
                Ok(next)
            },
        );
        match committed {
            Ok(c) => Ok(Some(c.sequence)),
            Err(_) if absent.get() => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Drops the segments and the completeness entry of the declared
    /// sketch `spec`, keeping its declaration, in one manifest commit that
    /// reads no source file: the next build then sketches every live row
    /// group again. For a sketch whose segments are missing from the store
    /// or damaged (a sketch is never planned with, so nothing else depends
    /// on them). Returns the committed sequence, or `None` if the index does
    /// not declare `spec`.
    pub fn reset_sketch(&self, spec: &SketchSpec) -> Result<Option<u64>> {
        let name = spec.name();
        let absent = Cell::new(false);
        let committed = commit_with(
            &*self.store,
            &self.index_root,
            &StagedSegments::new(),
            CommitOptions::default(),
            |m| {
                if !declared_sketches(m).contains(spec) {
                    absent.set(true);
                    return Err(Error::KeySpec(format!("sketch {name} is not declared")));
                }
                let mut next = m.clone();
                next.segments
                    .retain(|s| !(s.kind.as_deref() == Some(SKETCH_KIND) && s.key_spec == name));
                next.key_spec_completeness.remove(&name);
                Ok(next)
            },
        );
        match committed {
            Ok(c) => Ok(Some(c.sequence)),
            Err(_) if absent.get() => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Builds or updates the index over the files under `source_root`, in
    /// one manifest commit. The index declares `key_specs` and keeps every
    /// spec it already declares under another name (a spec given under an
    /// existing name replaces that definition); with
    /// [`BuildOptions::remove_unlisted`], unlisted specs are removed
    /// instead.
    ///
    /// - Files are discovered and fingerprinted through the source, built
    ///   with the dataset schema (`with_schema`), the index's fingerprint
    ///   policy, and, for text, the chunk size each file records.
    /// - A file seen for the first time gets dense fresh row-group ids, one
    ///   per row group in `index_in_file` order. A tracked file whose
    ///   fingerprint changed has its ids retired (tombstoned) and gets fresh
    ///   ones; a tracked file that is gone has its ids retired. Ids are
    ///   never reused (ADR 013).
    /// - Each declared spec indexes every live row group it does not cover
    ///   yet: the new files' row groups for an existing spec, every row
    ///   group for a new spec, a spec whose definition changed, or a spec
    ///   whose key or timestamp column changed dataset type (its old
    ///   segments are dropped, never merged with the new ones). Kept specs
    ///   index the new files' row groups like any other.
    /// - Only key columns are read, one row group per worker at a time.
    ///   Segments are uploaded as they finish; the manifest commit then
    ///   publishes all of them and the new row-group space together.
    /// - `key_spec_completeness` records a spec only when its segments
    ///   cover every live row group (ADR 012).
    ///
    /// If a concurrent commit changed the row-group space, the schema, or
    /// the source, the build restarts from discovery with fresh ids rather
    /// than rebasing (its segments' bitmaps hold the ids it assigned). Any
    /// other concurrent change (compaction, garbage collection) is rebased
    /// onto. Nothing is committed when nothing changed.
    pub fn build(
        &self,
        source_root: &str,
        key_specs: &[KeySpec],
        options: &BuildOptions,
    ) -> Result<BuildReport> {
        let started = Instant::now();
        let root = canonical_root(source_root)?;
        let mut restarts = 0u32;
        loop {
            match self.attempt(&root, key_specs, options)? {
                Attempt::Done(mut report) => {
                    report.restarts = restarts;
                    report.elapsed = started.elapsed();
                    return Ok(*report);
                }
                Attempt::Restart { claimed } => {
                    restarts += 1;
                    if restarts > options.max_restarts {
                        return Err(Error::VersionConflict {
                            expected: "a manifest stable for the length of one build".into(),
                            found: format!("{restarts} conflicting commits"),
                        });
                    }
                    // A GC claim clears only after its run deletes and
                    // releases; a lost race needs only a short pause.
                    let base_ms: u64 = if claimed { 100 } else { 5 };
                    std::thread::sleep(Duration::from_millis(base_ms << restarts.min(5)));
                }
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    fn attempt(&self, root: &str, key_specs: &[KeySpec], opts: &BuildOptions) -> Result<Attempt> {
        let base = read_manifest(&*self.store, &self.index_root)?.map(|(m, _)| m);
        if let Some(m) = &base {
            // Fresh ids come from `next_id`: a damaged space (an id assigned
            // twice, live and retired at once, or at or above `next_id`)
            // could hand out an id that is already live or retired.
            Locator::new(&m.rowgroup_space)?;
        }
        let cfg = Config::resolve(base.as_ref(), root, opts)?;
        let mut declared = declared_specs(base.as_ref(), key_specs, opts.remove_unlisted)?;
        if let Some((a, b)) = KeySpec::shared_identity(&declared) {
            return Err(shared_identity(root, base.as_ref(), a, b));
        }
        let mut sidecars = declared_sidecar_defs(base.as_ref(), opts.remove_unlisted);
        let sketches = declared_sketch_specs(base.as_ref(), &opts.sketches, opts.remove_unlisted)?;
        // A join key (B3) is not built row group by row group: it is
        // checked here and built whole by `Builder::refresh_join`.
        check_joins(root, &declared, &cfg.schema)?;
        let row_specs = |declared: &[KeySpec]| -> Vec<KeySpec> {
            declared
                .iter()
                .filter(|s| !matches!(s, KeySpec::Join { .. }))
                .cloned()
                .collect()
        };
        let mut specs = plan_specs(&row_specs(&declared), &sidecars, &sketches, &cfg.schema)?;
        let chunk_sizes = base_chunk_sizes(base.as_ref())
            .into_iter()
            .chain(cfg.new_chunk_size.size());
        let mut sources = DataSource::new(
            cfg.kind,
            &cfg.schema,
            cfg.policy,
            &opts.exclude,
            chunk_sizes,
            root,
            &cfg.partitioning,
        )?;
        let discovered = sources.discover(root)?;
        let since = now_rfc3339();
        let observer = opts.observer.as_deref();
        let files = plan_files(
            base.as_ref(),
            &discovered,
            &mut sources,
            &cfg,
            &since,
            observer,
        )?;
        let live = live_ids(&files.space)?;
        let stale = stale_text_specs(&*self.store, &self.index_root, base.as_ref())?;
        let mut work = plan_work(base.as_ref(), &specs, &cfg, &live, &stale);
        // A join key declared, redefined, or removed changes the schema
        // even when no row group needs indexing.
        let joins_unchanged = base.as_ref().is_some_and(|m| {
            crate::target::declared_joins(&m.schema.key_specs)
                == crate::target::declared_joins(&declared)
        });
        if joins_unchanged && is_noop(base.as_ref(), &files, &work, &cfg, &specs)? {
            // Nothing committed: a version 1 index stays at version 1 and
            // keeps planning as before (ADR 028); no statistics were recorded.
            // The fold of a pre-M5 index's sidecars waits for a build with
            // work, and the report says what it will do.
            let mut report = files.report(&work);
            report.files_filled = 0;
            if !sidecars.is_empty() {
                let fold = fold_legacy(&declared, &sidecars);
                report.sidecars_fold_pending = fold.folded;
                report.sidecars_kept = fold.kept.iter().map(SidecarDef::name).collect();
            }
            return Ok(Attempt::Done(Box::new(report)));
        }
        // A build with work folds the sidecars of an index written before
        // M5 into key specs, which it rebuilds (they are new identities);
        // a sidecar no key spec can take stays a sidecar.
        let mut sidecars_folded = Vec::new();
        let mut sidecars_kept = Vec::new();
        if !sidecars.is_empty() {
            let fold = fold_legacy(&declared, &sidecars);
            if let Some((a, b)) = KeySpec::shared_identity(&fold.specs) {
                return Err(shared_identity(root, base.as_ref(), a, b));
            }
            declared = fold.specs;
            sidecars_kept = fold.kept.iter().map(SidecarDef::name).collect();
            sidecars = fold.kept;
            sidecars_folded = fold.folded;
            specs = plan_specs(&row_specs(&declared), &sidecars, &sketches, &cfg.schema)?;
            work = plan_work(base.as_ref(), &specs, &cfg, &live, &stale);
        }
        let mut report = files.report(&work);
        report.sidecars_folded = sidecars_folded;
        report.sidecars_kept = sidecars_kept;
        let items = segments::items(&files.rows, &work.needed, opts.max_segment_row_groups)?;
        let ctx = Ctx {
            store: &*self.store,
            index_root: &self.index_root,
            sources: &sources,
            schema: &cfg.schema,
            files: &files.rows,
            specs: &specs,
            max_keys: opts.max_segment_keys,
            max_bytes: opts.max_segment_bytes,
            observer,
            held: AtomicU64::new(0),
            peak_held: AtomicU64::new(0),
            peak_keys: AtomicU64::new(0),
        };
        let written = segments::write_all(&ctx, &items, opts.thread_count())?;
        report.row_groups_read = written.row_groups_read;
        report.files_listed = written.files_listed;
        report.segments_written = written.segments.len() as u64;
        report.peak_row_groups_held = ctx.peak_held.load(Ordering::SeqCst);
        report.peak_segment_keys = ctx.peak_keys.load(Ordering::SeqCst);
        let delta = Delta {
            planned_space: base
                .as_ref()
                .map(|m| m.rowgroup_space.clone())
                .unwrap_or_default(),
            planned_schema: base.as_ref().map(|m| m.schema.clone()).unwrap_or_default(),
            planned_source: base.as_ref().map(|m| m.source.clone()).unwrap_or_default(),
            source: cfg.source_info.clone(),
            schema: schema_info(base.as_ref(), &cfg, &declared, &sidecars, &sketches),
            space: files.space.clone(),
            keep: &work.keep,
            replaced: &work.replaced,
            added: &written.segments,
        };
        let restart = Cell::new(false);
        let committed = commit_with(
            &*self.store,
            &self.index_root,
            &StagedSegments::new(),
            opts.commit,
            |m| delta.apply(m, &restart),
        );
        match committed {
            Ok(c) => {
                report.sequence = Some(c.sequence);
                report.upgraded_from = base
                    .as_ref()
                    .map(|m| m.storage_version)
                    .filter(|&v| v < STORAGE_VERSION);
                report.segments = written.segments;
                Ok(Attempt::Done(Box::new(report)))
            }
            Err(_) if restart.get() => Ok(Attempt::Restart { claimed: false }),
            Err(Error::SegmentClaimed(_)) => Ok(Attempt::Restart { claimed: true }),
            Err(e) => Err(e),
        }
    }
}

fn canonical_root(root: &str) -> Result<String> {
    let path = std::fs::canonicalize(root).map_err(|e| Error::Source(format!("{root}: {e}")))?;
    path.to_str()
        .map(str::to_string)
        .ok_or_else(|| Error::Source(format!("{root}: path is not UTF-8")))
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// The resolved configuration of one build attempt.
struct Config {
    kind: SourceKind,
    policy: FingerprintPolicy,
    partitioning: Partitioning,
    /// Chunk size for text files seen for the first time; `None` for
    /// Parquet.
    new_chunk_size: NewChunkSize,
    /// The dataset schema, in the form the manifest records.
    schema: SchemaRef,
    /// The schema the base manifest records, if any.
    base_schema: Option<SchemaRef>,
    source_info: SourceInfo,
    columns: Vec<ColumnDef>,
}

impl Config {
    fn resolve(base: Option<&Manifest>, root: &str, opts: &BuildOptions) -> Result<Self> {
        let kind = opts.source;
        let recorded = base.filter(|m| !m.source.kind.is_empty());
        if let Some(m) = recorded {
            let was = SourceKind::of(&m.source)?;
            if was != kind {
                return Err(Error::Other(format!(
                    "the index was built from {} files and this build reads {}; build a new index",
                    was.label(),
                    kind.label()
                )));
            }
            if !m.source.root.is_empty() && m.source.root != root {
                return Err(Error::Other(format!(
                    "the index was built over {} and this build reads {root}; build a new index",
                    m.source.root
                )));
            }
        }
        let recorded_policy = recorded
            .map(|m| parse_policy(m.source.fingerprint_policy.as_deref()))
            .transpose()?;
        let policy = match (opts.fingerprint_policy, recorded_policy) {
            (Some(p), Some(r)) if p != r => {
                return Err(Error::Other(format!(
                    "the index fingerprints files under {}, and this build asks for {}; a policy change \
                     changes every file's identity, so build a new index",
                    policy_name(r),
                    policy_name(p)
                )))
            }
            (Some(p), _) | (None, Some(p)) => p,
            (None, None) => FingerprintPolicy::default(),
        };
        let recorded_partitioning = recorded
            .map(|m| Partitioning::parse(m.source.partitioning.as_deref()))
            .transpose()?;
        let partitioning = match (&opts.partitioning, recorded_partitioning) {
            (Some(p), Some(r)) if *p != r => {
                return Err(Error::Other(format!(
                    "the index was built with partitioning {}, and this build asks for {}; a \
                     partitioning change changes the dataset schema and every row group's \
                     statistics, so build a new index",
                    r.label(),
                    p.label()
                )))
            }
            (Some(p), _) => p.clone(),
            (None, Some(p)) => p,
            (None, None) => Partitioning::None,
        };
        let new_chunk_size = match (kind.text_format(), opts.chunk_size) {
            (None, None) => NewChunkSize::NotText,
            (None, Some(_)) => {
                return Err(Error::Other(
                    "chunk_size applies to CSV and JSON lines sources only".into(),
                ))
            }
            (Some(_), Some(c)) => NewChunkSize::Size(c.max(1)),
            (Some(_), None) => recorded_chunk_size(base),
        };
        let base_schema = match base {
            Some(m) if !m.schema.columns.is_empty() => Some(columns_to_schema(&m.schema.columns)?),
            _ => None,
        };
        let schema = match (&opts.schema, &base_schema) {
            (Some(s), _) => persistable(s)?,
            (None, Some(b)) => SchemaRef::clone(b),
            (None, None) => persistable(
                infer_schema(kind, root, policy, &partitioning, &opts.exclude)?.as_ref(),
            )?,
        };
        let columns = schema_to_columns(&schema, base.map_or(&[][..], |m| &m.schema.columns));
        let mut source_info = base.map(|m| m.source.clone()).unwrap_or_default();
        kind.record(&mut source_info);
        source_info.root = root.to_string();
        source_info.fingerprint_policy = Some(policy_name(policy).to_string());
        source_info.partitioning = partitioning.manifest_name();
        Ok(Self {
            kind,
            policy,
            partitioning,
            new_chunk_size,
            schema,
            base_schema,
            source_info,
            columns,
        })
    }
}

/// The chunk size for text files seen for the first time.
#[derive(Clone, Debug, PartialEq, Eq)]
enum NewChunkSize {
    /// A Parquet source.
    NotText,
    /// This size.
    Size(u64),
    /// Tracked files record several sizes and no option picks one; an error
    /// only if a new text file appears.
    Ambiguous(BTreeSet<u64>),
}

impl NewChunkSize {
    /// The size to record for a new file: `None` for Parquet.
    fn for_new_file(&self, path: &str) -> Result<Option<u64>> {
        match self {
            Self::NotText => Ok(None),
            Self::Size(s) => Ok(Some(*s)),
            Self::Ambiguous(sizes) => Err(Error::Other(format!(
                "{path} is new, and tracked files record several chunk sizes {sizes:?}; pass \
                 BuildOptions::chunk_size to choose one for new files"
            ))),
        }
    }

    fn size(&self) -> Option<u64> {
        match self {
            Self::Size(s) => Some(*s),
            _ => None,
        }
    }
}

/// Every chunk size the base manifest's files record.
fn base_chunk_sizes(base: Option<&Manifest>) -> BTreeSet<u64> {
    base.into_iter()
        .flat_map(|m| m.rowgroup_space.files.iter().filter_map(|f| f.chunk_size))
        .collect()
}

/// The chunk size for new text files when no option is given: the one the
/// base manifest's text files record, or 64 MiB for a new index.
fn recorded_chunk_size(base: Option<&Manifest>) -> NewChunkSize {
    let sizes = base_chunk_sizes(base);
    match sizes.len() {
        0 => NewChunkSize::Size(DEFAULT_CHUNK_SIZE),
        1 => NewChunkSize::Size(sizes.into_iter().next().unwrap_or(DEFAULT_CHUNK_SIZE)),
        _ => NewChunkSize::Ambiguous(sizes),
    }
}

/// The specs a build declares: `key_specs`, plus, unless `remove_unlisted`,
/// every spec the base declares under a name `key_specs` does not use. The
/// base's specs keep their order, a given spec under an existing name
/// replaces that definition in place, and new specs follow. A given spec
/// the base does not already declare is a new declaration and must meet
/// [`KeySpec::check_names`]; one it declares (a storage version 1 spec on a
/// column named `a,b`) is indexed as it is. The base's specs carry their
/// tombstones (B8, [`declared_key_specs`]); a given spec without one that
/// is the base's spec without its tombstone keeps the tombstone (removing
/// it is explicit: `remove_unlisted`, or `Builder::remove_spec` on it).
pub(crate) fn declared_specs(
    base: Option<&Manifest>,
    key_specs: &[KeySpec],
    remove_unlisted: bool,
) -> Result<Vec<KeySpec>> {
    let base_specs = base.map(declared_key_specs).unwrap_or_default();
    let mut given: BTreeMap<String, &KeySpec> = BTreeMap::new();
    for spec in key_specs {
        if given.insert(spec.name(), spec).is_some() {
            return Err(Error::KeySpec(format!(
                "two given key specs share the name {}; KeySpec::name omits ts_column and fpr, \
                 so at most one of them can be declared",
                spec.name()
            )));
        }
        if !base_specs.contains(spec) {
            spec.check_names()?;
        }
    }
    if remove_unlisted {
        return Ok(key_specs.to_vec());
    }
    let keep_tombstone = |base: &KeySpec, g: &KeySpec| -> KeySpec {
        if g.tombstone().is_none() && base.tombstone().is_some() && base.without_tombstone() == *g {
            base.clone()
        } else {
            g.clone()
        }
    };
    let mut out: Vec<KeySpec> = Vec::with_capacity(key_specs.len());
    for spec in &base_specs {
        out.push(
            given
                .get(&spec.name())
                .map_or_else(|| spec.clone(), |g| keep_tombstone(spec, g)),
        );
    }
    for spec in key_specs {
        if !out.iter().any(|o| o.name() == spec.name()) {
            out.push(spec.clone());
        }
    }
    Ok(out)
}

/// The refusal for two declared specs that share an identity, before
/// anything is read or written: storage version 2 files segments by
/// identity, so their segments could not be told apart. Either both come
/// from a storage version 1 index (which allowed it), and the index cannot
/// be upgraded until one is re-declared under its name (a spec declared
/// under an existing name replaces it), or one is a new declaration
/// colliding with a spec the index already declares.
fn shared_identity(root: &str, base: Option<&Manifest>, a: &KeySpec, b: &KeySpec) -> Error {
    let declared = |s: &KeySpec| base.is_some_and(|m| m.schema.key_specs.contains(s));
    let both = format!(
        "the key specs {} and {} share the identity {}, and storage version 2 files segments by \
         identity, so their segments could not be told apart; nothing was read or written",
        spec_json(a),
        spec_json(b),
        a.identity()
    );
    if !(declared(a) && declared(b)) {
        let (new, old) = if declared(a) { (b, a) } else { (a, b) };
        return Error::KeySpec(format!(
            "{both}. {} is a new declaration and {} is already declared, so declare a spec \
             over other columns instead",
            spec_json(new),
            spec_json(old)
        ));
    }
    // A spec re-declared under its name replaces it; the replacement is a
    // new declaration, so its key columns (its name) must be free of the
    // separators. Its other columns (a time column) are the ones to change.
    let clean = |s: &KeySpec| !s.columns().iter().any(|c| c.contains([',', '@']));
    let step = match [a, b].into_iter().find(|s| clean(s)) {
        Some(s) => {
            let placeholder = "<a time column whose name has no ',' or '@'>";
            let fix = |c: &mut String| {
                if c.contains([',', '@']) {
                    *c = placeholder.to_string();
                }
            };
            let mut example = s.clone();
            match &mut example {
                KeySpec::Exact { bounds, .. } | KeySpec::Bloom { bounds, .. } => {
                    bounds.iter_mut().for_each(fix);
                }
                KeySpec::Temporal { ts_column, .. } => fix(ts_column),
                KeySpec::Bitemporal {
                    valid_column,
                    known_column,
                    ..
                } => {
                    fix(valid_column);
                    fix(known_column);
                }
                // An expr key's column is an identifier; it names no time
                // column.
                KeySpec::Range { .. }
                | KeySpec::Expr { .. }
                | KeySpec::Text { .. }
                | KeySpec::List { .. }
                | KeySpec::Join { .. } => {}
            }
            format!(
                "re-declare {} under its name with a time column whose name has no ',' or '@', \
                 for example `bellwether index {root} --key '{}'`: a spec declared under an \
                 existing name replaces it, and that build upgrades the index",
                s.name(),
                spec_json(&example)
            )
        }
        None => format!(
            "both specs' key columns hold ',' or '@', so neither can be re-declared under its \
             name; move the index folder of {root} aside and index again with key specs whose \
             columns have neither"
        ),
    };
    Error::KeySpec(format!(
        "cannot upgrade this storage version 1 index to storage version {STORAGE_VERSION}: {both}. \
         To upgrade it, {step}. Until then the index stays at storage version 1, and every other \
         spec still plans"
    ))
}

/// The schema this build commits: the dataset columns, the declared key
/// specs (each without its tombstone), sidecars, sketches, and tombstones
/// (B8, a `tombstone` sidecar per spec with one), the base's declarations
/// of sidecar kinds this binary does not know (kept unchanged, as their
/// segments are), and the base's unknown fields.
fn schema_info(
    base: Option<&Manifest>,
    cfg: &Config,
    declared: &[KeySpec],
    sidecars: &[SidecarDef],
    sketches: &[SketchSpec],
) -> SchemaInfo {
    SchemaInfo {
        columns: cfg.columns.clone(),
        key_specs: declared.iter().map(KeySpec::without_tombstone).collect(),
        sidecars: sidecars
            .iter()
            .map(decl_of)
            .chain(sketches.iter().map(sketch_decl))
            .chain(
                declared
                    .iter()
                    .filter(|s| s.tombstone().is_some())
                    .map(tombstone_decl),
            )
            .chain(
                base.iter()
                    .flat_map(|m| m.schema.sidecars.iter().filter(|d| !known_decl(d)).cloned()),
            )
            .collect(),
        extra: base.map(|m| m.schema.extra.clone()).unwrap_or_default(),
    }
}

/// The sidecars of an index written before M5 a build keeps declaring
/// until it folds them into key specs: the base's, unless
/// `remove_unlisted` (which no longer lists them).
fn declared_sidecar_defs(base: Option<&Manifest>, remove_unlisted: bool) -> Vec<SidecarDef> {
    if remove_unlisted {
        return Vec::new();
    }
    base.map(declared_sidecars).unwrap_or_default()
}

/// The sketches a build declares, by the rules of [`declared_specs`]:
/// `given`, plus, unless `remove_unlisted`, every sketch the base declares
/// under another name (a given one under an existing name replaces it).
fn declared_sketch_specs(
    base: Option<&Manifest>,
    given: &[SketchSpec],
    remove_unlisted: bool,
) -> Result<Vec<SketchSpec>> {
    let mut names = BTreeSet::new();
    for s in given {
        if !names.insert(s.name()) {
            return Err(Error::KeySpec(format!(
                "the sketch {} is given twice",
                s.name()
            )));
        }
    }
    if remove_unlisted {
        return Ok(given.to_vec());
    }
    let mut out: Vec<SketchSpec> = Vec::new();
    for s in base.map(declared_sketches).unwrap_or_default() {
        out.push(
            given
                .iter()
                .find(|g| g.name() == s.name())
                .cloned()
                .unwrap_or(s),
        );
    }
    for s in given {
        if !out.iter().any(|o| o.name() == s.name()) {
            out.push(s.clone());
        }
    }
    Ok(out)
}

/// Validates the declared key specs, sidecars, and sketches against the
/// dataset schema: key specs first, in declaration order, then sidecars,
/// then sketches.
// One arm per kind of target (key spec, expr key, sidecar, sketch).
#[allow(clippy::too_many_lines)]
fn plan_specs(
    specs: &[KeySpec],
    sidecars: &[SidecarDef],
    sketches: &[SketchSpec],
    schema: &Schema,
) -> Result<Vec<SpecPlan>> {
    if specs.len() + sidecars.len() + sketches.len() > MAX_KEY_SPECS {
        return Err(Error::KeySpec(format!(
            "{} key specs, {} sidecars, and {} sketches declared; at most {MAX_KEY_SPECS} \
             together are supported",
            specs.len(),
            sidecars.len(),
            sketches.len()
        )));
    }
    let column_type = |c: &str| {
        schema
            .field_with_name(c)
            .map(|f| f.data_type().clone())
            .map_err(|_| Error::KeySpec(format!("key spec {}", encoding::not_in_schema(schema, c))))
    };
    let mut names = BTreeSet::new();
    let mut out: Vec<SpecPlan> = specs
        .iter()
        .map(|spec| {
            validate_spec(spec)?;
            let name = spec.name();
            if !names.insert(name.clone()) {
                return Err(Error::KeySpec(format!(
                    "two declared key specs share the name {name}; KeySpec::name omits ts_column and fpr, \
                     so at most one of them can be declared"
                )));
            }
            if let KeySpec::Expr { .. } = spec {
                // The one key column is the virtual column, at the
                // expression's type; the worker reads the expression's
                // inputs and derives it per row group.
                let typed = bellwether_core::expr::typed_spec(spec, schema)?;
                ColumnType::of(typed.data_type())
                    .map_err(|e| Error::KeySpec(format!("{name}: {e}")))?;
                return Ok(vec![SpecPlan {
                    target: Target::Key(spec.clone()),
                    name: spec.identity(),
                    key_types: vec![typed.data_type().clone()],
                    ts_type: None,
                    time_types: Vec::new(),
                    columns: typed.input_columns(),
                }]);
            }
            let mut seen = BTreeSet::new();
            let mut key_types = Vec::new();
            for c in spec.columns() {
                if !seen.insert(c.as_str()) {
                    return Err(Error::KeySpec(format!("{name} lists column {c:?} twice")));
                }
                let t = column_type(c)?;
                // A list key's elements are its keys: the element type is
                // the one its segments encode at and record.
                let t = match spec {
                    KeySpec::List { .. } => bellwether_core::list::check_list_column(c, &t)?,
                    _ => t,
                };
                ColumnType::of(&t).map_err(|e| Error::KeySpec(format!("{name}: key column {c:?}: {e}")))?;
                if matches!(spec, KeySpec::Text { .. }) {
                    bellwether_core::text::check_column_type(c, &t)?;
                }
                key_types.push(t);
            }
            let mut columns = spec.columns().to_vec();
            let ts_type = match spec {
                KeySpec::Temporal { ts_column, .. } => {
                    let t = column_type(ts_column)?;
                    encoding::check_ts_type(&t)?;
                    ColumnType::of(&t).map_err(|e| Error::KeySpec(format!("{name}: {e}")))?;
                    if !columns.contains(ts_column) {
                        columns.push(ts_column.clone());
                    }
                    Some(t)
                }
                _ => None,
            };
            // Per-key bounds (exact) and bitemporal versions: their time
            // columns, each a column a version or window orders by. A
            // refusal names the spec's identity once, and a column the
            // dataset lacks with the dataset's columns.
            let identity = spec.identity();
            let within = |e: Error| match e {
                Error::KeySpec(m) => Error::KeySpec(format!("{identity}: {m}")),
                other => other,
            };
            for c in bellwether_core::segment::record_time_columns(
                spec,
                bellwether_core::segment::Layout::Keyed,
            ) {
                if schema.field_with_name(&c).is_err() {
                    return Err(Error::KeySpec(format!(
                        "{identity}: the time column {c:?} is not in the dataset; its columns are \
                         {}",
                        schema
                            .fields()
                            .iter()
                            .map(|f| f.name().as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )));
                }
            }
            let time_types = bellwether_core::segment::time_types(spec, schema).map_err(within)?;
            for (c, t) in bellwether_core::segment::time_columns(spec)
                .into_iter()
                .zip(&time_types)
            {
                ColumnType::of(t).map_err(within)?;
                if !columns.contains(&c) {
                    columns.push(c);
                }
            }
            // A spec with a tombstone (B8) builds its own segments as the
            // spec without it, and its tombstone sidecar beside them, which
            // reads the tombstone column too.
            let tombstone = match spec.tombstone() {
                Some(t) => {
                    let dt = bellwether_core::tombstone::column_type(spec, schema)?;
                    ColumnType::of(&dt).map_err(within)?;
                    let mut tcols = columns.clone();
                    tcols.push(t.column.clone());
                    Some(SpecPlan {
                        target: Target::Tombstone(spec.clone()),
                        name: tombstone_name(spec),
                        key_types: key_types.clone(),
                        ts_type: ts_type.clone(),
                        time_types: time_types.clone(),
                        columns: tcols,
                    })
                }
                None => None,
            };
            let plain = spec.without_tombstone();
            let mut plans = vec![SpecPlan {
                target: Target::Key(plain.clone()),
                name: plain.identity(),
                key_types: key_types.clone(),
                ts_type,
                time_types,
                columns,
            }];
            plans.extend(tombstone);
            // A bloom with bounds keeps them in `bounds` segments under its
            // identity: a bloom has no per-key record to carry them.
            if let Some(def) = bloom_bounds_def(spec) {
                def.validate().map_err(within)?;
                let time_types = def.time_types(schema).map_err(within)?;
                for t in &time_types {
                    ColumnType::of(t).map_err(|e| Error::KeySpec(format!("{identity}: {e}")))?;
                }
                plans.push(SpecPlan {
                    target: Target::BloomBounds(spec.clone()),
                    name: spec.identity(),
                    key_types,
                    ts_type: None,
                    time_types,
                    columns: def.columns.iter().chain(&def.time_columns).cloned().collect(),
                });
            }
            Ok(plans)
        })
        .collect::<Result<Vec<Vec<SpecPlan>>>>()?
        .into_iter()
        .flatten()
        .collect();
    for def in sidecars {
        def.validate()?;
        let name = def.name();
        if !names.insert(name.clone()) {
            return Err(Error::KeySpec(format!(
                "the sidecar {name} is declared twice"
            )));
        }
        let key_types = def.key_types(schema)?;
        let time_types = def.time_types(schema)?;
        for t in key_types.iter().chain(&time_types) {
            ColumnType::of(t).map_err(|e| Error::KeySpec(format!("{name}: {e}")))?;
        }
        out.push(SpecPlan {
            target: Target::Sidecar(def.clone()),
            name,
            key_types,
            ts_type: None,
            time_types,
            columns: def
                .columns
                .iter()
                .chain(&def.time_columns)
                .cloned()
                .collect(),
        });
    }
    plan_sketches(sketches, schema, &mut names, &mut out)?;
    Ok(out)
}

/// Validates each sketch against the dataset schema (every item column
/// present with a canonical encoding) and appends its plan.
fn plan_sketches(
    sketches: &[SketchSpec],
    schema: &Schema,
    names: &mut BTreeSet<String>,
    out: &mut Vec<SpecPlan>,
) -> Result<()> {
    for spec in sketches {
        bellwether_core::sketch::validate(spec)?;
        let name = spec.name();
        if !names.insert(name.clone()) {
            return Err(Error::KeySpec(format!(
                "the sketch {name} is declared twice"
            )));
        }
        bellwether_core::sketch::item_types(spec, schema)?;
        out.push(SpecPlan {
            target: Target::Sketch(spec.clone()),
            name,
            key_types: Vec::new(),
            ts_type: None,
            time_types: Vec::new(),
            columns: sketch_columns(spec),
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Files and row-group ids
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default)]
struct FileCounts {
    new: u64,
    changed: u64,
    removed: u64,
    unchanged: u64,
    assigned: u64,
    retired: u64,
    /// Unchanged files whose storage version 2 statistics were filled.
    filled: u64,
}

/// The tracked files and row-group space after this build.
struct FilePlan {
    rows: Vec<FileRow>,
    space: RowGroupSpace,
    counts: FileCounts,
}

impl FilePlan {
    fn report(&self, work: &WorkPlan) -> BuildReport {
        BuildReport {
            files_new: self.counts.new,
            files_changed: self.counts.changed,
            files_removed: self.counts.removed,
            files_unchanged: self.counts.unchanged,
            row_groups_assigned: self.counts.assigned,
            row_groups_retired: self.counts.retired,
            files_filled: self.counts.filled,
            specs_rebuilt: work.replaced.iter().cloned().collect(),
            specs_repaired: work.repaired.clone(),
            text_tables_replaced: work.stale.clone(),
            specs_removed: work.removed.clone(),
            ..BuildReport::default()
        }
    }
}

/// Pairs discovered files with tracked ones by path, retires the ids of
/// changed and vanished files, and assigns fresh ids, in path order, to
/// every file seen for the first time or changed.
#[allow(clippy::too_many_lines)]
fn plan_files(
    base: Option<&Manifest>,
    discovered: &[FileFingerprint],
    sources: &mut DataSource,
    cfg: &Config,
    since: &str,
    observer: Option<&dyn BuildObserver>,
) -> Result<FilePlan> {
    let empty = RowGroupSpace::default();
    let base_space = base.map_or(&empty, |m| &m.rowgroup_space);
    let mut by_path: BTreeMap<&str, &FileEntry> = BTreeMap::new();
    for f in &base_space.files {
        if by_path.insert(f.fingerprint.path.as_str(), f).is_some() {
            return Err(Error::Other(format!(
                "the manifest tracks {} twice",
                f.fingerprint.path
            )));
        }
    }
    let present: BTreeSet<&str> = discovered.iter().map(|f| f.path.as_str()).collect();
    let mut counts = FileCounts::default();
    let mut retired: Vec<RowGroupId> = Vec::new();
    for f in &base_space.files {
        if !present.contains(f.fingerprint.path.as_str()) {
            retired.extend(&f.rowgroup_ids);
            counts.removed += 1;
        }
    }
    let columns: Vec<String> = cfg.columns.iter().map(|c| c.name.clone()).collect();
    // Column bytes are aligned with the schema's columns, so a schema whose
    // columns changed refills them for every file.
    let columns_changed =
        base.is_some_and(|m| !m.schema.columns.iter().map(|c| &c.name).eq(columns.iter()));
    let mut next = base_space.next_id;
    let mut entries = Vec::with_capacity(discovered.len());
    for fp in discovered {
        if let Some(old) = by_path.get(fp.path.as_str()) {
            if old.fingerprint == *fp {
                let current = old.has_rowgroup_stats()
                    && (old.column_bytes.is_none() == cfg.kind.text_format().is_some())
                    && !columns_changed;
                if current {
                    entries.push((*old).clone());
                    counts.unchanged += 1;
                    continue;
                }
                // A version 1 entry, or one whose column bytes no longer
                // align: fill the statistics from one `row_groups` call. A
                // footer that disagrees with the recorded row-group count
                // is drift under an unchanged fingerprint (ADR 013): the
                // file's ids are retired and reassigned like a changed
                // file's.
                let stats = sources.rowgroup_stats(fp, old.chunk_size, &columns)?;
                if stats.rows.len() == old.rowgroup_ids.len() {
                    let mut e = (*old).clone();
                    e.rowgroup_rows = Some(stats.rows);
                    e.rowgroup_bytes = Some(stats.bytes);
                    e.column_bytes = stats.column_bytes;
                    entries.push(e);
                    counts.unchanged += 1;
                    counts.filled += 1;
                    continue;
                }
            }
            retired.extend(&old.rowgroup_ids);
            counts.changed += 1;
        } else {
            counts.new += 1;
        }
        let chunk_size = cfg.new_chunk_size.for_new_file(&fp.path)?;
        let layout = sources.layout(fp, chunk_size)?;
        if let Some(o) = observer {
            o.file_laid_out(fp);
        }
        let stats = sources.rowgroup_stats(fp, chunk_size, &columns)?;
        if stats.rows.len() != layout.row_groups {
            return Err(Error::Source(format!(
                "{}: the file lists {} row groups, then {}; it changed while the build read it, \
                 so run the build again",
                fp.path,
                layout.row_groups,
                stats.rows.len()
            )));
        }
        let n = u32::try_from(layout.row_groups)
            .map_err(|_| Error::Other(format!("{}: too many row groups", fp.path)))?;
        let end = next
            .checked_add(n)
            .ok_or_else(|| Error::Other("the row-group id space is exhausted".into()))?;
        counts.assigned += u64::from(n);
        entries.push(FileEntry {
            fingerprint: fp.clone(),
            rowgroup_ids: (next..end).map(RowGroupId).collect(),
            chunk_size,
            chunks: layout.chunks.map(|c| {
                c.into_iter()
                    .map(|r| ChunkRange {
                        start: r.start,
                        end: r.end,
                        extra: Extra::new(),
                    })
                    .collect()
            }),
            extra: Extra::new(),
            rowgroup_rows: Some(stats.rows),
            rowgroup_bytes: Some(stats.bytes),
            column_bytes: stats.column_bytes,
        });
        next = end;
    }
    retired.sort_unstable();
    counts.retired = retired.len() as u64;
    let mut tombstones = base_space.tombstones.clone();
    if !retired.is_empty() {
        tombstones.push(RowGroupTombstone {
            since: since.to_string(),
            rowgroup_ids: retired,
            extra: Extra::new(),
        });
    }
    let space = RowGroupSpace {
        next_id: next,
        files: entries,
        tombstones,
        extra: base_space.extra.clone(),
    };
    let rows = space
        .files
        .iter()
        .map(|f| file_row(f, cfg.kind))
        .collect::<Result<Vec<_>>>()?;
    for r in &rows {
        if let Some(size) = r.chunk_size {
            sources.ensure_chunk_size(size);
        }
    }
    Ok(FilePlan {
        rows,
        space,
        counts,
    })
}

/// A tracked file entry as the workers read it. Text entries must record a
/// chunk size and one chunk per id; Parquet entries must record neither.
fn file_row(f: &FileEntry, kind: SourceKind) -> Result<FileRow> {
    let path = &f.fingerprint.path;
    let chunks = match (kind.text_format().is_some(), f.chunk_size, &f.chunks) {
        (true, Some(_), Some(chunks)) if chunks.len() == f.rowgroup_ids.len() => {
            Some(chunks.iter().map(|c| c.start..c.end).collect())
        }
        (true, _, _) => {
            return Err(Error::Other(format!(
            "{path}: a text file entry must record its chunk size and one chunk per row-group id"
        )))
        }
        (false, None, None) => None,
        (false, _, _) => {
            return Err(Error::Other(format!(
                "{path}: a Parquet file entry records chunks"
            )))
        }
    };
    Ok(FileRow {
        fingerprint: f.fingerprint.clone(),
        ids: f.rowgroup_ids.clone(),
        chunk_size: f.chunk_size,
        chunks,
    })
}

// ---------------------------------------------------------------------------
// Work
// ---------------------------------------------------------------------------

/// Which row groups each declared spec must index in this build.
struct WorkPlan {
    /// Per declared spec, in declaration order.
    needed: Vec<RoaringBitmap>,
    /// Specs rebuilt from scratch: all their base segments are dropped.
    replaced: BTreeSet<String>,
    /// Specs whose base segments are kept.
    keep: BTreeSet<String>,
    /// Base specs no longer declared.
    removed: Vec<String>,
    /// Counted specs rebuilt because two of their base segments cover one
    /// live row group (also in `replaced`).
    repaired: Vec<String>,
    /// Declared text keys replaced because they were built under other
    /// Unicode tables.
    stale: Vec<TextTablesReplaced>,
}

/// Whether two segments listed under `name` both cover one live row group.
/// No build commits that (a build indexes each live row group in one
/// segment per spec, and compaction merges only contiguous ranges), so the
/// manifest was written by another tool or edited. A counted spec's rows
/// would then be counted twice, which every planner refuses
/// ([`crate::overlapping_counts`]); the build rebuilds the spec instead.
fn double_covered(m: &Manifest, name: &str, live: &RoaringBitmap) -> bool {
    let mut seen = RoaringBitmap::new();
    for s in m.segments.iter().filter(|s| segment_of(s, name, None)) {
        let (start, end) = (s.rowgroup_range.start.0, s.rowgroup_range.end.0);
        let mut covers = RoaringBitmap::new();
        if start < end {
            covers.insert_range(start..end);
        }
        covers &= live;
        if !seen.is_disjoint(&covers) {
            return true;
        }
        seen |= covers;
    }
    false
}

/// Whether any column of `target` whose type its segments record (key
/// columns, a temporal timestamp, a sidecar's time columns; a list key's
/// element type) has a different recorded type under `new` than under
/// `old`.
fn types_differ(old: &Schema, new: &Schema, target: &Target) -> bool {
    let list = matches!(target, Target::Key(KeySpec::List { .. }));
    target.typed_columns().iter().any(|c| {
        let recorded = |s: &Schema| {
            let dt = s.field_with_name(c).ok()?.data_type();
            let dt = if list {
                bellwether_core::list::element_type(dt)?
            } else {
                dt
            };
            ColumnType::of(dt).ok()
        };
        match (recorded(old), recorded(new)) {
            (Some(a), Some(b)) => a != b,
            _ => true,
        }
    })
}

/// The text keys of `m` built under Unicode tables other than this
/// binary's, with the tables they recorded (core's `text::recorded_tables`):
/// after a deliberate upgrade of either table crate, `index` rebuilds
/// exactly these from scratch ([`plan_work`]). Every build keeps all of a
/// text key's segments under one table version (a key on other tables is
/// replaced whole, never extended), so one header per key tells: reads one
/// segment's preamble and header per declared text key, whatever its
/// segment count.
fn stale_text_specs<S: StoreExt + ?Sized>(
    store: &S,
    index_root: &str,
    m: Option<&Manifest>,
) -> Result<BTreeMap<String, bellwether_core::text::UnicodeTables>> {
    let now = bellwether_core::text::UnicodeTables::current();
    let mut stale = BTreeMap::new();
    let Some(m) = m else {
        return Ok(stale);
    };
    for spec in m
        .schema
        .key_specs
        .iter()
        .filter(|s| matches!(s, KeySpec::Text { .. }))
    {
        let name = spec.identity();
        let Some(seg) = m.key_spec_segments(&name).next() else {
            continue;
        };
        let src = HeaderBytes {
            store,
            path: bellwether_store::segment_path(index_root, &seg.id),
            size: seg.bytes,
        };
        match bellwether_core::text::recorded_tables(&src)? {
            Some(t) if t == now => {}
            Some(t) => {
                stale.insert(name, t);
            }
            None => {
                return Err(Error::Other(format!(
                    "corrupt segment {}: a text segment records its Unicode tables",
                    seg.id
                )))
            }
        }
    }
    Ok(stale)
}

/// A committed segment read by range (its preamble and header only).
struct HeaderBytes<'a, S: StoreExt + ?Sized> {
    store: &'a S,
    path: String,
    size: u64,
}

impl<S: StoreExt + ?Sized> bellwether_core::segment::ReadAt for HeaderBytes<'_, S> {
    fn size(&self) -> Result<u64> {
        Ok(self.size)
    }

    fn read_at(&self, offset: u64, len: usize) -> Result<std::borrow::Cow<'_, [u8]>> {
        let end = offset
            .checked_add(len as u64)
            .filter(|e| *e <= self.size)
            .ok_or_else(|| Error::Other(format!("corrupt segment {}: range", self.path)))?;
        let bytes = self
            .store
            .get_range(&self.path, offset..end)?
            .ok_or_else(|| Error::Store(format!("segment {} is missing", self.path)))?;
        if bytes.len() != len {
            return Err(Error::Store(format!(
                "segment {}: asked for {len} bytes at {offset}, got {}",
                self.path,
                bytes.len()
            )));
        }
        Ok(std::borrow::Cow::Owned(bytes.to_vec()))
    }
}

fn plan_work(
    base: Option<&Manifest>,
    specs: &[SpecPlan],
    cfg: &Config,
    live: &RoaringBitmap,
    stale: &BTreeMap<String, bellwether_core::text::UnicodeTables>,
) -> WorkPlan {
    let mut work = WorkPlan {
        needed: Vec::with_capacity(specs.len()),
        replaced: BTreeSet::new(),
        keep: BTreeSet::new(),
        removed: Vec::new(),
        repaired: Vec::new(),
        stale: Vec::new(),
    };
    let base_targets = base.map(declared_targets).unwrap_or_default();
    for p in specs {
        let old = base_targets.iter().find(|t| t.slot() == p.target.slot());
        let types_changed = match &cfg.base_schema {
            Some(bs) => types_differ(bs, &cfg.schema, &p.target),
            None => old.is_some(),
        };
        let double = match (base, &p.target) {
            (Some(m), Target::Key(spec)) if spec.counts() => double_covered(m, &p.name, live),
            _ => false,
        };
        let replaced = old != Some(&p.target) || types_changed || double;
        if double && old == Some(&p.target) && !types_changed {
            work.repaired.push(p.name.clone());
        }
        // A text key built under other Unicode tables is rebuilt whole.
        let recorded = stale.get(&p.name);
        let replaced = replaced || recorded.is_some();
        if let Some(t) = recorded {
            work.stale.push(TextTablesReplaced {
                spec: p.name.clone(),
                recorded: t.clone(),
                current: bellwether_core::text::UnicodeTables::current(),
            });
        }
        let needed = match base {
            Some(m) if !replaced => live - &covered(m, &p.name, p.target.kind()),
            _ => live.clone(),
        };
        work.needed.push(needed);
        if replaced {
            work.replaced.insert(p.name.clone());
        } else {
            work.keep.insert(p.name.clone());
        }
    }
    // A spec redefined under its slot is rebuilt, not removed.
    let declared: BTreeSet<String> = specs.iter().map(|p| p.target.slot()).collect();
    work.removed = base_targets
        .iter()
        .filter(|t| !declared.contains(&t.slot()))
        .map(Target::name)
        .collect();
    work
}

/// Whether committing would change nothing. Filling the storage version 2
/// statistics of a version 1 manifest is not a change on its own: a build
/// with nothing else to do commits nothing and leaves the index at version
/// 1; the next commit that changes anything upgrades it.
fn is_noop(
    base: Option<&Manifest>,
    files: &FilePlan,
    work: &WorkPlan,
    cfg: &Config,
    specs: &[SpecPlan],
) -> Result<bool> {
    let Some(m) = base else {
        return Ok(false);
    };
    let declared: Vec<Target> = specs.iter().map(|p| p.target.clone()).collect();
    let unchanged = files.counts.new == 0
        && files.counts.changed == 0
        && files.counts.removed == 0
        && work.needed.iter().all(RoaringBitmap::is_empty)
        && work.replaced.is_empty()
        && work.removed.is_empty()
        && same_source(&m.source, &cfg.source_info)?
        && same_columns(&m.schema.columns, &cfg.columns)?
        && declared_targets(m) == declared
        && m.segments.iter().all(|s| {
            unknown_kind(s)
                || work.keep.contains(&s.key_spec)
                || joins_of(&m.schema.key_specs).contains(&s.key_spec)
        });
    Ok(
        unchanged
            && completeness_map(m, m, &declared, &BTreeSet::new())? == m.key_spec_completeness,
    )
}

/// Whether a manifest's `source` describes the same source as `now`, the
/// one this build would record: equal once both are spelled as storage
/// version 2 spells them (a version 1 manifest carries a CSV dialect in
/// `kind`; a build with nothing else to do does not respell it).
fn same_source(recorded: &SourceInfo, now: &SourceInfo) -> Result<bool> {
    let mut respelled = recorded.clone();
    if !recorded.kind.is_empty() {
        SourceKind::of(recorded)?.record(&mut respelled);
    }
    Ok(respelled == *now)
}

/// Whether a manifest's columns are `now`'s, the ones this build would
/// record, once both are spelled as storage version 2 spells them (a
/// version 1 manifest spells types as Arrow displays them).
fn same_columns(recorded: &[ColumnDef], now: &[ColumnDef]) -> Result<bool> {
    if recorded.is_empty() {
        return Ok(now.is_empty());
    }
    Ok(schema_to_columns(columns_to_schema(recorded)?.as_ref(), recorded) == now)
}

// ---------------------------------------------------------------------------
// The commit
// ---------------------------------------------------------------------------

/// The change one build publishes, applied to whatever manifest is current
/// at commit time.
struct Delta<'a> {
    planned_space: RowGroupSpace,
    planned_schema: SchemaInfo,
    planned_source: SourceInfo,
    source: SourceInfo,
    schema: SchemaInfo,
    space: RowGroupSpace,
    keep: &'a BTreeSet<String>,
    replaced: &'a BTreeSet<String>,
    added: &'a [SegmentRef],
}

impl Delta<'_> {
    /// The manifest to commit on top of `m`, or a conflict that restarts the
    /// build (`restart` set) when `m` changed the row-group space, the
    /// schema, or the source since the build planned: the segments'
    /// bitmaps hold the ids this build assigned, so they cannot be rebased
    /// onto another assignment (ADR 013, ADR 015).
    fn apply(&self, m: &Manifest, restart: &Cell<bool>) -> Result<Manifest> {
        let conflict = |what: &str| {
            restart.set(true);
            Error::VersionConflict {
                expected: format!("the manifest this build planned against ({what} changed since)"),
                found: format!("sequence {}", m.sequence),
            }
        };
        if m.rowgroup_space != self.planned_space {
            return Err(conflict("the row-group space"));
        }
        if m.schema != self.planned_schema {
            return Err(conflict("the schema"));
        }
        if m.source != self.planned_source {
            return Err(conflict("the source"));
        }
        let mut next = m.clone();
        next.source = self.source.clone();
        next.schema = self.schema.clone();
        next.rowgroup_space = self.space.clone();
        // Segments of kinds this binary does not know are kept unchanged,
        // and so is a declared join key's pair segment (its completeness
        // then says whether it still covers the left's row groups).
        let joins = joins_of(&self.schema.key_specs);
        next.segments.retain(|s| {
            unknown_kind(s) || self.keep.contains(&s.key_spec) || joins.contains(&s.key_spec)
        });
        for seg in self.added {
            next.add_segment(seg.clone());
        }
        let targets = declared_targets(&next);
        let map = completeness_map(m, &next, &targets, self.replaced)?;
        if let Some(missing) = targets.iter().find(|t| !map.contains_key(&t.name())) {
            let what = match missing {
                Target::Key(_) => "key spec",
                Target::BloomBounds(_) => "the per-key bounds of key spec",
                Target::Sidecar(_) => "sidecar",
                Target::Sketch(_) => "sketch",
                Target::Tombstone(_) => "tombstone sidecar",
            };
            return Err(Error::Other(format!(
                "the build would leave {what} {} incomplete; nothing was committed",
                missing.name()
            )));
        }
        next.key_spec_completeness = map;
        Ok(next)
    }
}

/// The identities of the join keys (B3) among `specs`.
fn joins_of(specs: &[KeySpec]) -> BTreeSet<String> {
    crate::target::declared_joins(specs)
        .iter()
        .map(KeySpec::identity)
        .collect()
}

/// Checks every declared join key (B3) against the left dataset: its rules
/// (core's `join::check_spec`), its left dataset name equal to the data
/// root's folder name (the left dataset is the root), and its left columns
/// in the schema with types a join pairs. The right side is checked when
/// the pair is built (`Builder::refresh_join`), which reads its index.
pub(crate) fn check_joins(root: &str, declared: &[KeySpec], schema: &Schema) -> Result<()> {
    let folder = std::path::Path::new(root)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default();
    for spec in crate::target::declared_joins(declared) {
        validate_spec(&spec)?;
        let KeySpec::Join { dataset, .. } = &spec else {
            continue;
        };
        if dataset != folder {
            return Err(Error::KeySpec(format!(
                "{} names the left dataset {dataset:?}, but the data root {root} is the folder \
                 {folder:?}: a join key is declared on its left dataset, so pass that folder as \
                 the root, or name this one: join:{folder}.<columns>=<right folder>.<columns>",
                spec.name()
            )));
        }
        for c in spec.columns() {
            let t = schema.field_with_name(c).map_err(|_| {
                Error::KeySpec(format!(
                    "{}: {}",
                    spec.name(),
                    encoding::not_in_schema(schema, c)
                ))
            })?;
            let ct = ColumnType::of(t.data_type())
                .map_err(|e| Error::KeySpec(format!("{}: join column {c:?}: {e}", spec.name())))?;
            // A float is refused here, before anything is built.
            bellwether_core::join::check_column_type(c, &ct)?;
        }
    }
    Ok(())
}
