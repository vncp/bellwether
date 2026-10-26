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
    commit_with, now_rfc3339, read_manifest, ChunkRange, ColumnDef, CommitOptions, Extra,
    FileEntry, Manifest, RowGroupSpace, RowGroupTombstone, SchemaInfo, SegmentRef, SourceInfo,
    StagedSegments, StoreExt,
};
use bellwether_types::{Error, FileFingerprint, KeySpec, Result, RowGroupId};
use roaring::RoaringBitmap;

use crate::config::{
    columns_to_schema, parse_policy, persistable, policy_name, schema_to_columns, BuildObserver,
    BuildOptions, SourceKind,
};
use crate::coverage::{completeness, completeness_map, covered, live_ids, Locator};
use crate::segments::{self, Ctx, FileRow, SpecPlan};
use crate::sources::{infer_schema, DataSource};
use crate::target::{decl_of, declared_sidecars, declared_targets, def_of, unknown_kind, Target};

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
    /// whose key or timestamp column changed type.
    pub specs_rebuilt: Vec<String>,
    /// Key specs removed because [`BuildOptions::remove_unlisted`] was set
    /// and they were not listed; their segments were dropped.
    pub specs_removed: Vec<String>,
    /// Restarts after a conflicting concurrent commit.
    pub restarts: u32,
    /// Most row groups held by workers at once (at most the thread count).
    pub peak_row_groups_held: u64,
    /// Largest key count one open segment reached (see [`BuildOptions`]).
    pub peak_segment_keys: u64,
    /// Wall time.
    pub elapsed: Duration,
}

enum Attempt {
    Done(BuildReport),
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
    /// never removed in its place.
    pub fn remove_spec(&self, spec: &KeySpec) -> Result<Option<u64>> {
        let name = spec.name();
        let absent = Cell::new(false);
        let committed = commit_with(
            &*self.store,
            &self.index_root,
            &StagedSegments::new(),
            CommitOptions::default(),
            |m| {
                match m.schema.key_specs.iter().find(|s| s.name() == name) {
                    None => {
                        absent.set(true);
                        return Err(Error::KeySpec(format!("key spec {name} is not declared")));
                    }
                    Some(d) if d != spec => {
                        return Err(Error::KeySpec(format!(
                            "the index declares {d:?} under the name {name}, not {spec:?}"
                        )))
                    }
                    Some(_) => {}
                }
                let mut next = m.clone();
                next.schema.key_specs.retain(|s| s.name() != name);
                next.segments.retain(|s| s.key_spec != name);
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
                    return Ok(report);
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

    fn attempt(&self, root: &str, key_specs: &[KeySpec], opts: &BuildOptions) -> Result<Attempt> {
        let base = read_manifest(&*self.store, &self.index_root)?.map(|(m, _)| m);
        if let Some(m) = &base {
            // Fresh ids come from `next_id`: a damaged space (an id assigned
            // twice, live and retired at once, or at or above `next_id`)
            // could hand out an id that is already live or retired.
            Locator::new(&m.rowgroup_space)?;
        }
        let cfg = Config::resolve(base.as_ref(), root, opts)?;
        let declared = declared_specs(base.as_ref(), key_specs, opts.remove_unlisted)?;
        let sidecars = declared_sidecar_defs(base.as_ref(), &opts.sidecars, opts.remove_unlisted)?;
        let specs = plan_specs(&declared, &sidecars, &cfg.schema)?;
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
        let work = plan_work(base.as_ref(), &specs, &cfg, &live);
        let mut report = files.report(&work);
        if is_noop(base.as_ref(), &files, &work, &cfg, &specs)? {
            return Ok(Attempt::Done(report));
        }
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
            schema: schema_info(base.as_ref(), &cfg, &declared, &sidecars),
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
                report.segments = written.segments;
                Ok(Attempt::Done(report))
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
            let was = SourceKind::parse(&m.source.kind)?;
            if was != kind {
                return Err(Error::Other(format!(
                    "the index was built from {} files and this build reads {}; build a new index",
                    m.source.kind,
                    kind.manifest_kind()
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
        source_info.kind = kind.manifest_kind();
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
/// replaces that definition in place, and new specs follow.
fn declared_specs(
    base: Option<&Manifest>,
    key_specs: &[KeySpec],
    remove_unlisted: bool,
) -> Result<Vec<KeySpec>> {
    let mut given: BTreeMap<String, &KeySpec> = BTreeMap::new();
    for spec in key_specs {
        if given.insert(spec.name(), spec).is_some() {
            return Err(Error::KeySpec(format!(
                "two given key specs share the name {}; KeySpec::name omits ts_column and fpr, \
                 so at most one of them can be declared",
                spec.name()
            )));
        }
    }
    if remove_unlisted {
        return Ok(key_specs.to_vec());
    }
    let mut out: Vec<KeySpec> = Vec::with_capacity(key_specs.len());
    for spec in base.map_or(&[][..], |m| m.schema.key_specs.as_slice()) {
        out.push(
            given
                .get(&spec.name())
                .map_or_else(|| spec.clone(), |g| (*g).clone()),
        );
    }
    for spec in key_specs {
        if !out.iter().any(|o| o.name() == spec.name()) {
            out.push(spec.clone());
        }
    }
    Ok(out)
}

/// The schema this build commits: the dataset columns, the declared key
/// specs and sidecars, the base's declarations of sidecar kinds this binary
/// does not know (kept unchanged, as their segments are), and the base's
/// unknown fields.
fn schema_info(
    base: Option<&Manifest>,
    cfg: &Config,
    declared: &[KeySpec],
    sidecars: &[SidecarDef],
) -> SchemaInfo {
    SchemaInfo {
        columns: cfg.columns.clone(),
        key_specs: declared.to_vec(),
        sidecars: sidecars
            .iter()
            .map(decl_of)
            .chain(base.iter().flat_map(|m| {
                m.schema
                    .sidecars
                    .iter()
                    .filter(|d| def_of(d).is_none())
                    .cloned()
            }))
            .collect(),
        extra: base.map(|m| m.schema.extra.clone()).unwrap_or_default(),
    }
}

/// The sidecars a build declares, by the rules of [`declared_specs`]:
/// `given`, plus, unless `remove_unlisted`, every sidecar the base declares
/// under another name (a given one under an existing name replaces it).
fn declared_sidecar_defs(
    base: Option<&Manifest>,
    given: &[SidecarDef],
    remove_unlisted: bool,
) -> Result<Vec<SidecarDef>> {
    let mut names = BTreeSet::new();
    for d in given {
        if !names.insert(d.name()) {
            return Err(Error::KeySpec(format!(
                "the sidecar {} is given twice",
                d.name()
            )));
        }
    }
    if remove_unlisted {
        return Ok(given.to_vec());
    }
    let mut out: Vec<SidecarDef> = base.map(declared_sidecars).unwrap_or_default();
    for d in given {
        if !out.iter().any(|o| o.name() == d.name()) {
            out.push(d.clone());
        }
    }
    Ok(out)
}

/// Validates the declared key specs and sidecars against the dataset
/// schema: key specs first, in declaration order, then sidecars.
fn plan_specs(
    specs: &[KeySpec],
    sidecars: &[SidecarDef],
    schema: &Schema,
) -> Result<Vec<SpecPlan>> {
    if specs.len() + sidecars.len() > MAX_KEY_SPECS {
        return Err(Error::KeySpec(format!(
            "{} key specs and {} sidecars declared; at most {MAX_KEY_SPECS} together are \
             supported",
            specs.len(),
            sidecars.len()
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
            let mut seen = BTreeSet::new();
            let mut key_types = Vec::new();
            for c in spec.columns() {
                if !seen.insert(c.as_str()) {
                    return Err(Error::KeySpec(format!("{name} lists column {c:?} twice")));
                }
                let t = column_type(c)?;
                ColumnType::of(&t).map_err(|e| Error::KeySpec(format!("{name}: key column {c:?}: {e}")))?;
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
            Ok(SpecPlan {
                target: Target::Key(spec.clone()),
                name,
                key_types,
                ts_type,
                time_types: Vec::new(),
                columns,
            })
        })
        .collect::<Result<_>>()?;
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
    Ok(out)
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
            specs_rebuilt: work.replaced.iter().cloned().collect(),
            specs_removed: work.removed.clone(),
            ..BuildReport::default()
        }
    }
}

/// Pairs discovered files with tracked ones by path, retires the ids of
/// changed and vanished files, and assigns fresh ids, in path order, to
/// every file seen for the first time or changed.
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
    let mut next = base_space.next_id;
    let mut entries = Vec::with_capacity(discovered.len());
    for fp in discovered {
        if let Some(old) = by_path.get(fp.path.as_str()) {
            if old.fingerprint == *fp {
                entries.push((*old).clone());
                counts.unchanged += 1;
                continue;
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
            rowgroup_rows: None,
            rowgroup_bytes: None,
            column_bytes: None,
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
}

/// Whether any column of `target` whose type its segments record (key
/// columns, a temporal timestamp, a sidecar's time columns) has a different
/// recorded type under `new` than under `old`.
fn types_differ(old: &Schema, new: &Schema, target: &Target) -> bool {
    target.typed_columns().iter().any(|c| {
        let recorded = |s: &Schema| {
            s.field_with_name(c)
                .ok()
                .and_then(|f| ColumnType::of(f.data_type()).ok())
        };
        match (recorded(old), recorded(new)) {
            (Some(a), Some(b)) => a != b,
            _ => true,
        }
    })
}

fn plan_work(
    base: Option<&Manifest>,
    specs: &[SpecPlan],
    cfg: &Config,
    live: &RoaringBitmap,
) -> WorkPlan {
    let mut work = WorkPlan {
        needed: Vec::with_capacity(specs.len()),
        replaced: BTreeSet::new(),
        keep: BTreeSet::new(),
        removed: Vec::new(),
    };
    let base_targets = base.map(declared_targets).unwrap_or_default();
    for p in specs {
        let old = base_targets.iter().find(|t| t.name() == p.name);
        let types_changed = match &cfg.base_schema {
            Some(bs) => types_differ(bs, &cfg.schema, &p.target),
            None => old.is_some(),
        };
        let replaced = old != Some(&p.target) || types_changed;
        let needed = match base {
            Some(m) if !replaced => live - &covered(m, &p.name),
            _ => live.clone(),
        };
        work.needed.push(needed);
        if replaced {
            work.replaced.insert(p.name.clone());
        } else {
            work.keep.insert(p.name.clone());
        }
    }
    let declared: BTreeSet<&str> = specs.iter().map(|p| p.name.as_str()).collect();
    work.removed = base_targets
        .iter()
        .map(Target::name)
        .filter(|n| !declared.contains(n.as_str()))
        .collect();
    work
}

/// Whether committing would change nothing.
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
        && m.source == cfg.source_info
        && m.schema.columns == cfg.columns
        && declared_targets(m) == declared
        && m.segments
            .iter()
            .all(|s| unknown_kind(s) || work.keep.contains(&s.key_spec));
    Ok(
        unchanged
            && completeness_map(m, m, &declared, &BTreeSet::new())? == m.key_spec_completeness,
    )
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
        // Segments of kinds this binary does not know are kept unchanged.
        next.segments
            .retain(|s| unknown_kind(s) || self.keep.contains(&s.key_spec));
        for seg in self.added {
            next.add_segment(seg.clone());
        }
        let targets = declared_targets(&next);
        let map = completeness_map(m, &next, &targets, self.replaced)?;
        if let Some(missing) = targets.iter().find(|t| !map.contains_key(&t.name())) {
            let what = match missing {
                Target::Key(_) => "key spec",
                Target::Sidecar(_) => "sidecar",
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
