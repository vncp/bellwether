//! The multi-machine build (ADR 030): workers write segments, one
//! committer commits once, and they coordinate through the index store
//! only.
//!
//! 1. [`Builder::plan_work`] does everything a build does before it reads a
//!    row group (discover, fingerprint, validate the row-group space,
//!    assign ids, decide each target's work, cut the work items), commits
//!    nothing, and writes an immutable, content-addressed work plan under
//!    `work/<run id>/` in the index store: `items/<n>.json` per work item,
//!    then `plan.json`, whose BLAKE3 is the run id. A work item is a run of
//!    the build's own items (the units a single-machine build hands its
//!    threads), so the segments do not depend on how the work is split.
//! 2. [`Builder::work_item`] runs one work item on any machine that mounts
//!    the data root at the same path and reaches the store: it writes the
//!    item's segments through the build's own worker path (same caps, same
//!    memory bound, the same processed row-group ranges), uploads them
//!    content-addressed, fingerprints every file it read, and writes
//!    `work/<run id>/done/<n>.json`. It never reads or writes a manifest.
//!    Rerunning an item uploads the same bytes and leaves an equal done
//!    file in place.
//! 3. [`Builder::commit_work`] requires a done file for every item, checks
//!    each (its checksum, its item, the files it read against the plan's
//!    fingerprints, its segments tiling exactly the item's row groups per
//!    target), reads every listed segment back (its BLAKE3 against its
//!    id, its header against its target and range), and commits the union
//!    once through the build's own commit closure. A manifest whose
//!    row-group space, schema, or source changed since the plan, that
//!    already references the plan's segments, or that covers, for a kept
//!    target, a row group the plan indexes, refuses the commit as stale.
//!
//! The plan, item, and done files are their own operational artifacts
//! (formats [`PLAN_FORMAT`], [`ITEM_FORMAT`], [`DONE_FORMAT`]); no
//! manifest field or segment byte changes. Every refusal a caller can act
//! on is a [`WorkRefusal`].

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use bellwether_core::segment::Layout;
use bellwether_core::sidecar::{SidecarDef, SidecarReader};
use bellwether_core::sketch::SketchReader;
use bellwether_core::text::UnicodeTables;
use bellwether_core::tombstone::Marks;
use bellwether_core::SegmentReader;
use bellwether_source::{drift, fingerprint_with_policy, FingerprintPolicy, LocalFs, Partitioning};
use bellwether_store::{
    commit_with, join, read_manifest, segment_path, CommitOptions, Extra, Manifest, RowGroupRange,
    RowGroupSpace, SchemaInfo, SegmentRef, SidecarDecl, SourceInfo, StagedSegments, StoreExt,
};
use bellwether_types::format::STORAGE_VERSION;
use bellwether_types::{Error, FileFingerprint, Hash, KeySpec, Result, RowGroupId, SketchSpec};
use bytes::Bytes;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::build::{
    canonical_root, file_row, plan_specs, BaseDigest, Builder, Delta, Planned, Prep, Prepared,
    Stale,
};
use crate::config::{columns_to_schema, parse_policy, BuildOptions, SourceKind};
use crate::coverage::Locator;
use crate::segments::{self, Ctx, FileRow, Item, SpecPlan, Task};
use crate::sources::DataSource;
use crate::target::{decl_of, def_of, Target, TombstoneDropped};
use crate::{BuildReport, TextTablesReplaced, TombstoneRows};

/// The folder under the index root that holds work plans.
pub const WORK_DIR: &str = "work";
/// `format` of a work plan (`work/<run id>/plan.json`).
pub const PLAN_FORMAT: &str = "bellwether-work-plan/1";
/// `format` of a work item (`work/<run id>/items/<n>.json`).
pub const ITEM_FORMAT: &str = "bellwether-work-item/1";
/// `format` of a done file (`work/<run id>/done/<n>.json`).
pub const DONE_FORMAT: &str = "bellwether-work-done/1";
/// Row groups per work item when no split is given.
pub const DEFAULT_ITEM_ROW_GROUPS: u64 = 1024;

/// The engine version a plan records: a worker or committer of another
/// version refuses it, since segment bytes are only guaranteed equal
/// within one version.
const ENGINE: &str = env!("CARGO_PKG_VERSION");

/// The store path of a run's plan, under `index_root`.
#[must_use]
pub fn plan_path(index_root: &str, run: &Hash) -> String {
    join(
        index_root,
        &format!("{WORK_DIR}/{}/plan.json", run.to_hex()),
    )
}

fn item_path(index_root: &str, run: &Hash, n: u32) -> String {
    join(
        index_root,
        &format!("{WORK_DIR}/{}/items/{n}.json", run.to_hex()),
    )
}

fn done_path(index_root: &str, run: &Hash, n: u32) -> String {
    join(
        index_root,
        &format!("{WORK_DIR}/{}/done/{n}.json", run.to_hex()),
    )
}

/// How [`Builder::plan_work`] groups the build's items into work items.
/// Either way an item is a run of consecutive build items, and the
/// segments written do not depend on the split.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ItemSplit {
    /// At most this many work items, balanced by row groups (fewer when
    /// the build has fewer items).
    Items(u32),
    /// Work items of at most this many row groups each, except a build
    /// item larger than that, which is an item of its own.
    RowGroups(u64),
}

impl Default for ItemSplit {
    fn default() -> Self {
        Self::RowGroups(DEFAULT_ITEM_ROW_GROUPS)
    }
}

/// What a plan carries beside the build: see [`Builder::plan_work`].
#[derive(Clone, Debug, Default)]
pub struct WorkOptions {
    /// How to split the work.
    pub split: ItemSplit,
    /// Declared sketches whose segments cannot be read: rebuilt whole, as
    /// a single-machine build resets and rebuilds them.
    pub reset_sketches: Vec<SketchSpec>,
    /// Join keys (B3) given with the build: the committer rebuilds them
    /// after its commit, as a single-machine build does.
    pub joins: Vec<KeySpec>,
    /// The index location as the planner opened it, recorded for workers
    /// and the committer as the default they open.
    pub index: String,
}

/// One work item of a plan, as [`Builder::plan_work`] reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkItemSummary {
    /// Its number, `0..items`.
    pub item: u32,
    /// Row groups it reads.
    pub row_groups: u64,
    /// Build items in it: each writes its own segments.
    pub units: u32,
    /// Files it reads.
    pub files: u32,
}

/// A written work plan.
#[derive(Clone, Debug)]
pub struct WorkPlanned {
    /// The run id: BLAKE3 of the plan's bytes.
    pub run: Hash,
    /// The plan's bytes, as stored at [`WorkPlanned::path`].
    pub plan: Bytes,
    /// Its store path.
    pub path: String,
    /// The manifest sequence it planned against (0 for a new index).
    pub base_sequence: u64,
    /// Its items.
    pub items: Vec<WorkItemSummary>,
    /// What the commit will do to files and ids, and the targets it
    /// rebuilds (the counts of a [`BuildReport`]; nothing read yet).
    pub report: BuildReport,
}

/// What [`Builder::plan_work`] did.
#[derive(Clone, Debug)]
pub enum PlanOutcome {
    /// Nothing to build or commit; no plan written.
    Nothing(Box<BuildReport>),
    /// A plan written.
    Planned(Box<WorkPlanned>),
}

/// Whether [`Builder::work_item`] wrote its done file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DoneWrite {
    /// The first done file for the item.
    New,
    /// An equal done file was there (a rerun): left in place.
    Unchanged,
    /// A done file with other contents (damaged, or another worker's over
    /// other bytes) was replaced.
    Replaced,
}

/// What one worker did.
#[derive(Clone, Debug)]
pub struct ItemDone {
    /// The run.
    pub run: Hash,
    /// The item.
    pub item: u32,
    /// The segments it uploaded.
    pub segments: Vec<SegmentRef>,
    /// Row groups read.
    pub row_groups_read: u64,
    /// The files it read, fingerprinted after reading.
    pub files: Vec<FileFingerprint>,
    /// The done file's store path.
    pub done_path: String,
    /// Whether the done file was written.
    pub write: DoneWrite,
    /// Most row groups held at once.
    pub peak_row_groups_held: u64,
    /// Largest key count one open segment reached.
    pub peak_segment_keys: u64,
    /// Wall time.
    pub elapsed: Duration,
}

/// Per item, what its done file records ([`WorkCommitted::items`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ItemTiming {
    /// The item.
    pub item: u32,
    /// Its worker's wall time.
    pub elapsed: Duration,
    /// Its worker's thread count.
    pub threads: u32,
    /// Row groups it read.
    pub row_groups_read: u64,
    /// Segments it wrote.
    pub segments: u64,
}

/// A committed work plan.
#[derive(Clone, Debug)]
pub struct WorkCommitted {
    /// The run.
    pub run: Hash,
    /// The manifest sequence the plan was made against.
    pub base_sequence: u64,
    /// The build's report: what a single-machine build of the same work
    /// reports, its elapsed time the commit's.
    pub report: BuildReport,
    /// Per item, from its done file.
    pub items: Vec<ItemTiming>,
    /// Join keys given with the plan, for the caller to rebuild.
    pub joins: Vec<KeySpec>,
    /// Sketches the plan rebuilt because their segments could not be read.
    pub reset_sketches: Vec<SketchSpec>,
}

/// A refusal a caller acts on, naming the item to rerun or the plan to
/// make again. Nothing was committed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkRefusal {
    /// The plan is not in this index store (`work/<run>/plan.json`).
    NotPublished {
        /// The run.
        run: Hash,
    },
    /// Items without a done file: not finished, failed, or lost.
    NotDone {
        /// The run.
        run: Hash,
        /// The items, ascending.
        items: Vec<u32>,
    },
    /// A file read for `item` differs from the plan's fingerprint.
    Drift {
        /// The item.
        item: u32,
        /// The file.
        path: String,
        /// The fingerprint the plan recorded.
        planned: Box<FileFingerprint>,
        /// The one the worker saw (`None`: the file is gone).
        read: Option<Box<FileFingerprint>>,
    },
    /// The data root of the plan is not on this machine.
    NoDataRoot {
        /// The root.
        root: String,
    },
    /// `item`'s done file is damaged, forged, or another run's, or lists
    /// outputs that are not exactly its work.
    BadDone {
        /// The item.
        item: u32,
        /// What is wrong.
        reason: String,
    },
    /// A segment `item` lists is not in the store (deleted by garbage
    /// collection, or never fully uploaded) or its bytes do not match.
    Segment {
        /// The item.
        item: u32,
        /// The segment.
        segment: Hash,
        /// Missing, or what is wrong with its bytes.
        reason: String,
    },
    /// A segment `item` lists is claimed by a garbage-collection run.
    Claimed {
        /// The item.
        item: u32,
        /// The segment.
        segment: Hash,
    },
    /// The plan no longer fits the index: make a new one.
    Stale {
        /// The run.
        run: Hash,
        /// What changed.
        what: &'static str,
        /// The plan's base sequence.
        base: u64,
        /// The current sequence.
        current: u64,
        /// Whether the plan's segments are committed already (a second
        /// committer, or a committer whose success was not seen).
        committed: bool,
    },
}

// ---------------------------------------------------------------------------
// Files
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PlanFile {
    format: String,
    engine: String,
    storage_version: u32,
    unicode: UnicodeTables,
    index: String,
    source_root: String,
    base: BaseFile,
    commit: CommitFile,
    read: ReadFile,
    report: ReportFile,
    joins: Vec<KeySpec>,
    items: Vec<ItemEntry>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BaseFile {
    sequence: u64,
    manifest: Option<Hash>,
    storage_version: Option<u32>,
    space: Hash,
    schema: Hash,
    source: Hash,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommitFile {
    source: SourceInfo,
    schema: SchemaInfo,
    space: RowGroupSpace,
    keep: BTreeSet<String>,
    replaced: BTreeSet<String>,
    replaced_base: BTreeSet<Hash>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadFile {
    exclude: Vec<String>,
    key_specs: Vec<KeySpec>,
    sidecars: Vec<SidecarDecl>,
    sketches: Vec<SketchSpec>,
    targets: Vec<TargetFile>,
    max_segment_keys: u64,
    max_segment_bytes: u64,
    max_segment_row_groups: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TargetFile {
    name: String,
    kind: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReportFile {
    files_new: u64,
    files_changed: u64,
    files_removed: u64,
    files_unchanged: u64,
    row_groups_assigned: u64,
    row_groups_retired: u64,
    files_filled: u64,
    specs_rebuilt: Vec<String>,
    specs_repaired: Vec<String>,
    specs_removed: Vec<String>,
    text_tables_replaced: Vec<TextFile>,
    sidecars_folded: Vec<(String, String)>,
    sidecars_kept: Vec<String>,
    tombstones_dropped: Vec<DroppedFile>,
    reset_sketches: Vec<SketchSpec>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TextFile {
    spec: String,
    recorded: UnicodeTables,
    current: UnicodeTables,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DroppedFile {
    sidecar: String,
    tombstone: String,
    now: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ItemEntry {
    item: u32,
    hash: Hash,
    row_groups: u64,
    units: u32,
    files: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ItemFile {
    format: String,
    item: u32,
    units: Vec<UnitFile>,
}

/// One build item: the targets in `mask` (bit `s` = the plan's target
/// `s`) over `tasks`, each `[file, index in file, row-group id]`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct UnitFile {
    mask: u64,
    tasks: Vec<[u32; 3]>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DoneFile {
    body: DoneBody,
    /// BLAKE3 of `body` as serialized.
    checksum: Hash,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DoneBody {
    format: String,
    run: Hash,
    item: u32,
    item_hash: Hash,
    outputs: DoneOutputs,
    threads: u32,
    elapsed_ms: u64,
    peak_row_groups_held: u64,
    peak_segment_keys: u64,
}

/// What a worker produced: equal on every run of one item.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DoneOutputs {
    segments: Vec<DoneSegment>,
    files: Vec<FileFingerprint>,
    row_groups_read: u64,
    files_listed: u64,
    tombstone_rows: Vec<MarksFile>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DoneSegment {
    id: Hash,
    key_spec: String,
    kind: Option<String>,
    start: u32,
    end: u32,
    bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MarksFile {
    sidecar: String,
    rows: u64,
    tombstones: u64,
    nulls: u64,
}

impl DoneSegment {
    fn of(s: &SegmentRef) -> Self {
        Self {
            id: s.id,
            key_spec: s.key_spec.clone(),
            kind: s.kind.clone(),
            start: s.rowgroup_range.start.0,
            end: s.rowgroup_range.end.0,
            bytes: s.bytes,
        }
    }

    fn to_ref(&self) -> SegmentRef {
        SegmentRef {
            id: self.id,
            key_spec: self.key_spec.clone(),
            rowgroup_range: RowGroupRange::new(self.start, self.end),
            bytes: self.bytes,
            kind: self.kind.clone(),
            join: None,
            extra: Extra::new(),
        }
    }
}

/// The one byte spelling of a work file.
fn to_bytes<T: Serialize>(v: &T) -> Result<Bytes> {
    Ok(Bytes::from(serde_json::to_vec_pretty(v)?))
}

/// Parses a work file and requires it to be spelled exactly as this binary
/// writes it, so every byte is either checked or refused.
fn parse_exact<T: Serialize + for<'de> Deserialize<'de>>(
    bytes: &[u8],
) -> std::result::Result<T, String> {
    let v: T = serde_json::from_slice(bytes).map_err(|e| format!("it does not parse ({e})"))?;
    let again = serde_json::to_vec_pretty(&v).map_err(|e| e.to_string())?;
    if again != bytes {
        return Err("it is not spelled as a worker writes it (edited or damaged)".into());
    }
    Ok(v)
}

fn done_bytes(body: &DoneBody) -> Result<Bytes> {
    let checksum = Hash::of(&serde_json::to_vec(body)?);
    to_bytes(&DoneFile {
        body: body.clone(),
        checksum,
    })
}

fn parse_done(bytes: &[u8]) -> std::result::Result<DoneBody, String> {
    let file: DoneFile = parse_exact(bytes)?;
    let sum = serde_json::to_vec(&file.body).map_err(|e| e.to_string())?;
    if Hash::of(&sum) != file.checksum {
        return Err("its checksum does not match its contents".into());
    }
    if file.body.format != DONE_FORMAT {
        return Err(format!(
            "its format is {:?}, not {DONE_FORMAT}",
            file.body.format
        ));
    }
    Ok(file.body)
}

fn corrupt_plan(what: &str) -> Error {
    Error::Other(format!(
        "the work plan is damaged or was edited ({what}); make a new one (plan the work again)"
    ))
}

// ---------------------------------------------------------------------------
// Splitting
// ---------------------------------------------------------------------------

/// Groups `items` (in order) into work items: ranges of consecutive build
/// items, none empty.
fn bundle(items: &[Item], split: ItemSplit) -> Result<Vec<Range<usize>>> {
    let sizes: Vec<u64> = items.iter().map(|i| i.tasks.len() as u64).collect();
    let mut out = Vec::new();
    match split {
        ItemSplit::RowGroups(m) => {
            if m == 0 {
                return Err(Error::InvalidArgument {
                    field: "items",
                    reason: "a work item holds at least one row group".into(),
                });
            }
            let mut start = 0;
            let mut acc = 0u64;
            for (i, &n) in sizes.iter().enumerate() {
                if i > start && acc + n > m {
                    out.push(start..i);
                    start = i;
                    acc = 0;
                }
                acc += n;
            }
            if start < sizes.len() {
                out.push(start..sizes.len());
            }
        }
        ItemSplit::Items(n) => {
            if n == 0 {
                return Err(Error::InvalidArgument {
                    field: "items",
                    reason: "a plan has at least one work item".into(),
                });
            }
            let total: u64 = sizes.iter().sum();
            let n = u64::from(n);
            // Build item `i` goes to bundle floor(rows before it × n /
            // total): at most `n` bundles, balanced by row groups, in order.
            let mut before = 0u64;
            let mut current: Option<(u64, usize)> = None;
            for (i, &s) in sizes.iter().enumerate() {
                let b = if total == 0 {
                    0
                } else {
                    u64::try_from(u128::from(before) * u128::from(n) / u128::from(total))
                        .unwrap_or(n - 1)
                        .min(n - 1)
                };
                match current {
                    Some((cb, start)) if cb != b => {
                        out.push(start..i);
                        current = Some((b, i));
                    }
                    None => current = Some((b, i)),
                    Some(_) => {}
                }
                before += s;
            }
            if let Some((_, start)) = current {
                out.push(start..sizes.len());
            }
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// The plan
// ---------------------------------------------------------------------------

/// A parsed, checked plan, with what a worker needs to read.
struct Loaded {
    run: Hash,
    file: PlanFile,
    kind: SourceKind,
    policy: FingerprintPolicy,
    partitioning: Partitioning,
    schema: SchemaRef,
    specs: Vec<SpecPlan>,
    rows: Vec<FileRow>,
}

/// What [`Builder::read_plan`] tells about a plan before opening anything.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlanSummary {
    /// The run id.
    pub run: Hash,
    /// The index location the planner opened.
    pub index: String,
    /// The data root.
    pub source_root: String,
    /// The base manifest sequence.
    pub base_sequence: u64,
    /// Work items.
    pub items: u32,
    /// The source kind the plan reads.
    pub source: SourceKind,
}

/// Reads a plan's bytes far enough to say which index and data root it is
/// for: its run id is their BLAKE3. Refuses bytes that are not a plan of
/// this binary's version.
pub fn read_plan(bytes: &[u8]) -> Result<PlanSummary> {
    let file = parse_plan(bytes)?;
    Ok(PlanSummary {
        run: Hash::of(bytes),
        index: file.index.clone(),
        source_root: file.source_root.clone(),
        base_sequence: file.base.sequence,
        items: u32::try_from(file.items.len()).unwrap_or(u32::MAX),
        source: SourceKind::of(&file.commit.source)?,
    })
}

fn parse_plan(bytes: &[u8]) -> Result<PlanFile> {
    let file: PlanFile = parse_exact(bytes).map_err(|e| corrupt_plan(&e))?;
    if file.format != PLAN_FORMAT {
        return Err(corrupt_plan(&format!(
            "its format is {:?}, not {PLAN_FORMAT}",
            file.format
        )));
    }
    if file.engine != ENGINE || file.storage_version != STORAGE_VERSION {
        return Err(Error::Other(format!(
            "the work plan was made by engine {} (storage version {}) and this is engine {ENGINE} \
             (storage version {STORAGE_VERSION}): segment bytes are only the same within one \
             version, so run every worker and the commit with the planner's version, or plan \
             the work again with this one",
            file.engine, file.storage_version
        )));
    }
    if file.unicode != UnicodeTables::current() {
        return Err(Error::Other(format!(
            "the work plan was made with Unicode tables {} / {} and this binary has {} / {}: \
             text keys would mix tables, so run every worker with the planner's binary, or plan \
             the work again with this one",
            file.unicode.case,
            file.unicode.words,
            UnicodeTables::current().case,
            UnicodeTables::current().words
        )));
    }
    Ok(file)
}

impl<S: StoreExt + ?Sized> Builder<S> {
    /// Plans a build of `key_specs` over `source_root` for workers on other
    /// machines (ADR 030), as [`Builder::build`] would build it, without
    /// reading a row group or committing: writes the work items and the
    /// plan under `work/<run id>/` and returns the plan. Nothing to do
    /// writes nothing.
    pub fn plan_work(
        &self,
        source_root: &str,
        key_specs: &[KeySpec],
        options: &BuildOptions,
        work: &WorkOptions,
    ) -> Result<PlanOutcome> {
        let started = Instant::now();
        let root = canonical_root(source_root)?;
        let forced: BTreeSet<String> = work.reset_sketches.iter().map(SketchSpec::name).collect();
        let p = match self.prepare(&root, key_specs, options, &forced)? {
            Prep::Noop(mut report) => {
                report.elapsed = started.elapsed();
                return Ok(PlanOutcome::Nothing(report));
            }
            Prep::Work(p) => p,
        };
        let bundles = bundle(&p.items, work.split)?;
        let mut entries = Vec::with_capacity(bundles.len());
        let mut item_files = Vec::with_capacity(bundles.len());
        for (n, range) in bundles.iter().enumerate() {
            let n = u32::try_from(n).map_err(|_| Error::Other("too many work items".into()))?;
            let mut units = Vec::with_capacity(range.len());
            for i in &p.items[range.clone()] {
                let tasks = i
                    .tasks
                    .iter()
                    .map(|t| {
                        u32::try_from(t.file)
                            .map(|f| [f, t.index, t.id])
                            .map_err(|_| Error::Other("too many files for one work plan".into()))
                    })
                    .collect::<Result<Vec<_>>>()?;
                units.push(UnitFile {
                    mask: i.mask,
                    tasks,
                });
            }
            let files: BTreeSet<u32> = units
                .iter()
                .flat_map(|u| u.tasks.iter().map(|t| t[0]))
                .collect();
            let bytes = to_bytes(&ItemFile {
                format: ITEM_FORMAT.into(),
                item: n,
                units,
            })?;
            entries.push(ItemEntry {
                item: n,
                hash: Hash::of(&bytes),
                row_groups: p.items[range.clone()]
                    .iter()
                    .map(|i| i.tasks.len() as u64)
                    .sum(),
                units: u32::try_from(range.len()).unwrap_or(u32::MAX),
                files: u32::try_from(files.len()).unwrap_or(u32::MAX),
            });
            item_files.push(bytes);
        }
        let plan = plan_file(&root, &p, options, work, entries)?;
        let bytes = to_bytes(&plan)?;
        let run = Hash::of(&bytes);
        // Items first, the plan last: a published plan has all its items.
        for (n, b) in item_files.into_iter().enumerate() {
            let n = u32::try_from(n).unwrap_or(u32::MAX);
            self.put_once(&item_path(self.index_root(), &run, n), &b)?;
        }
        let path = plan_path(self.index_root(), &run);
        self.put_once(&path, &bytes)?;
        let mut report = p.report.clone();
        report.elapsed = started.elapsed();
        Ok(PlanOutcome::Planned(Box::new(WorkPlanned {
            run,
            plan: bytes,
            path,
            base_sequence: plan.base.sequence,
            items: plan
                .items
                .iter()
                .map(|e| WorkItemSummary {
                    item: e.item,
                    row_groups: e.row_groups,
                    units: e.units,
                    files: e.files,
                })
                .collect(),
            report,
        })))
    }

    /// Writes `bytes` at `path` unless it holds them already. A work file
    /// is named by its content (or its plan's), so other bytes there are
    /// damage.
    fn put_once(&self, path: &str, bytes: &Bytes) -> Result<()> {
        match self.store().put_if_absent(path, bytes.clone()) {
            Ok(_) => Ok(()),
            Err(Error::VersionConflict { .. }) => match self.store().get_opt(path)? {
                Some((existing, _)) if existing == *bytes => Ok(()),
                _ => Err(Error::Store(format!(
                    "{path} exists with other bytes: the index store's work folder is damaged; \
                     remove {path} and plan the work again"
                ))),
            },
            Err(e) => Err(e),
        }
    }
}

/// The plan file of the prepared build `p`.
fn plan_file(
    root: &str,
    p: &Prepared,
    options: &BuildOptions,
    work: &WorkOptions,
    items: Vec<ItemEntry>,
) -> Result<PlanFile> {
    let (schema, dropped) = p.schema_info();
    let base = p.base.as_ref();
    let digest = BaseDigest::of(base.unwrap_or(&Manifest::new()))?;
    let replaced_base = base
        .map(|m| {
            m.segments
                .iter()
                .filter(|s| p.work.replaced.contains(&s.key_spec))
                .map(|s| s.id)
                .collect()
        })
        .unwrap_or_default();
    let r = &p.report;
    Ok(PlanFile {
        format: PLAN_FORMAT.into(),
        engine: ENGINE.into(),
        storage_version: STORAGE_VERSION,
        unicode: UnicodeTables::current(),
        index: work.index.clone(),
        source_root: root.to_string(),
        base: BaseFile {
            sequence: base.map_or(0, |m| m.sequence),
            manifest: p.base_hash,
            storage_version: base.map(|m| m.storage_version),
            space: digest.space,
            schema: digest.schema,
            source: digest.source,
        },
        commit: CommitFile {
            source: p.cfg.source_info.clone(),
            schema,
            space: p.files.space.clone(),
            keep: p.work.keep.clone(),
            replaced: p.work.replaced.clone(),
            replaced_base,
        },
        read: ReadFile {
            exclude: options.exclude.clone(),
            key_specs: p
                .declared
                .iter()
                .filter(|s| !matches!(s, KeySpec::Join { .. }))
                .cloned()
                .collect(),
            sidecars: p.sidecars.iter().map(decl_of).collect(),
            sketches: p.sketches.clone(),
            targets: p
                .specs
                .iter()
                .map(|s| TargetFile {
                    name: s.name.clone(),
                    kind: s.target.kind().map(str::to_string),
                })
                .collect(),
            max_segment_keys: options.max_segment_keys,
            max_segment_bytes: options.max_segment_bytes,
            max_segment_row_groups: options.max_segment_row_groups as u64,
        },
        report: ReportFile {
            files_new: r.files_new,
            files_changed: r.files_changed,
            files_removed: r.files_removed,
            files_unchanged: r.files_unchanged,
            row_groups_assigned: r.row_groups_assigned,
            row_groups_retired: r.row_groups_retired,
            files_filled: r.files_filled,
            specs_rebuilt: r.specs_rebuilt.clone(),
            specs_repaired: r.specs_repaired.clone(),
            specs_removed: r.specs_removed.clone(),
            text_tables_replaced: r
                .text_tables_replaced
                .iter()
                .map(|t| TextFile {
                    spec: t.spec.clone(),
                    recorded: t.recorded.clone(),
                    current: t.current.clone(),
                })
                .collect(),
            sidecars_folded: r.sidecars_folded.clone(),
            sidecars_kept: r.sidecars_kept.clone(),
            tombstones_dropped: dropped
                .into_iter()
                .map(|d| DroppedFile {
                    sidecar: d.sidecar,
                    tombstone: d.tombstone,
                    now: d.now,
                })
                .collect(),
            reset_sketches: work
                .reset_sketches
                .iter()
                .filter(|s| p.work.replaced.contains(&s.name()))
                .cloned()
                .collect(),
        },
        joins: work.joins.clone(),
        items,
    })
}

impl<S: StoreExt + ?Sized> Builder<S> {
    /// Parses and checks `bytes` as a plan published to this index.
    fn load(&self, bytes: &[u8]) -> Result<std::result::Result<Loaded, WorkRefusal>> {
        let file = parse_plan(bytes)?;
        let run = Hash::of(bytes);
        match self.store().get_opt(&plan_path(self.index_root(), &run))? {
            None => return Ok(Err(WorkRefusal::NotPublished { run })),
            Some((stored, _)) if stored != bytes => {
                return Err(corrupt_plan("its copy in the index store has other bytes"))
            }
            Some(_) => {}
        }
        let kind = SourceKind::of(&file.commit.source)?;
        let policy = parse_policy(file.commit.source.fingerprint_policy.as_deref())?;
        let partitioning = Partitioning::parse(file.commit.source.partitioning.as_deref())?;
        let schema = columns_to_schema(&file.commit.schema.columns)?;
        let sidecars: Vec<SidecarDef> = file
            .read
            .sidecars
            .iter()
            .map(|d| def_of(d).ok_or_else(|| corrupt_plan("a sidecar it reads is unknown")))
            .collect::<Result<_>>()?;
        let specs = plan_specs(
            &file.read.key_specs,
            &sidecars,
            &file.read.sketches,
            &schema,
        )?;
        let targets: Vec<TargetFile> = specs
            .iter()
            .map(|s| TargetFile {
                name: s.name.clone(),
                kind: s.target.kind().map(str::to_string),
            })
            .collect();
        if targets != file.read.targets {
            return Err(corrupt_plan("its targets do not follow from its specs"));
        }
        Locator::new(&file.commit.space)?;
        let rows = file
            .commit
            .space
            .files
            .iter()
            .map(|f| file_row(f, kind))
            .collect::<Result<Vec<_>>>()?;
        if file
            .items
            .iter()
            .enumerate()
            .any(|(i, e)| e.item as usize != i)
        {
            return Err(corrupt_plan("its items are not numbered 0, 1, ..."));
        }
        Ok(Ok(Loaded {
            run,
            file,
            kind,
            policy,
            partitioning,
            schema,
            specs,
            rows,
        }))
    }

    /// Reads and checks item `n` of a loaded plan: the build items it
    /// holds.
    fn load_item(&self, plan: &Loaded, n: u32) -> Result<Vec<Item>> {
        let entry = plan
            .file
            .items
            .get(n as usize)
            .ok_or_else(|| Error::InvalidArgument {
                field: "items",
                reason: format!(
                    "the work plan has items 0 to {}, not {n}",
                    plan.file.items.len().saturating_sub(1)
                ),
            })?;
        let path = item_path(self.index_root(), &plan.run, n);
        let (bytes, _) = self
            .store()
            .get_opt(&path)?
            .ok_or_else(|| corrupt_plan(&format!("its item {n} is missing from {path}")))?;
        if Hash::of(&bytes) != entry.hash {
            return Err(corrupt_plan(&format!(
                "its item {n} at {path} has other bytes"
            )));
        }
        let file: ItemFile = parse_exact(&bytes).map_err(|e| corrupt_plan(&e))?;
        if file.format != ITEM_FORMAT || file.item != n {
            return Err(corrupt_plan(&format!("{path} is not item {n}")));
        }
        let all = if plan.specs.len() >= 64 {
            u64::MAX
        } else {
            (1u64 << plan.specs.len()) - 1
        };
        let mut out = Vec::with_capacity(file.units.len());
        for u in file.units {
            if u.mask == 0 || u.mask & !all != 0 || u.tasks.is_empty() {
                return Err(corrupt_plan(&format!(
                    "item {n} names no or unknown targets"
                )));
            }
            let mut tasks = Vec::with_capacity(u.tasks.len());
            let mut last: Option<u32> = None;
            for [file, index, id] in u.tasks {
                let row = plan.rows.get(file as usize);
                let ok = row
                    .and_then(|r| r.ids.get(index as usize))
                    .is_some_and(|i| i.0 == id)
                    && last.is_none_or(|l| l < id);
                if !ok {
                    return Err(corrupt_plan(&format!(
                        "item {n} names row group {id} out of the plan's files or order"
                    )));
                }
                last = Some(id);
                tasks.push(Task {
                    id,
                    file: file as usize,
                    index,
                });
            }
            out.push(Item {
                mask: u.mask,
                tasks,
            });
        }
        Ok(out)
    }

    // -----------------------------------------------------------------------
    // The worker
    // -----------------------------------------------------------------------

    /// Runs item `item` of the plan `plan` (its bytes): writes and uploads
    /// its segments, fingerprints the files it read, and writes its done
    /// file. Reads no manifest and writes none. `threads` as
    /// [`BuildOptions::threads`].
    pub fn work_item(
        &self,
        plan: &[u8],
        item: u32,
        threads: Option<usize>,
    ) -> Result<std::result::Result<ItemDone, WorkRefusal>> {
        let started = Instant::now();
        let loaded = match self.load(plan)? {
            Ok(l) => l,
            Err(r) => return Ok(Err(r)),
        };
        let items = self.load_item(&loaded, item)?;
        let entry = &loaded.file.items[item as usize];
        if !std::path::Path::new(&loaded.file.source_root).is_dir() {
            return Ok(Err(WorkRefusal::NoDataRoot {
                root: loaded.file.source_root.clone(),
            }));
        }
        let read = &loaded.file.read;
        let sources = DataSource::new(
            loaded.kind,
            &loaded.schema,
            loaded.policy,
            &read.exclude,
            loaded.rows.iter().filter_map(|r| r.chunk_size),
            &loaded.file.source_root,
            &loaded.partitioning,
        )?;
        let ctx = Ctx {
            store: &**self.store(),
            index_root: self.index_root(),
            sources: &sources,
            schema: &loaded.schema,
            files: &loaded.rows,
            specs: &loaded.specs,
            max_keys: read.max_segment_keys,
            max_bytes: read.max_segment_bytes,
            observer: None,
            held: AtomicU64::new(0),
            peak_held: AtomicU64::new(0),
            peak_keys: AtomicU64::new(0),
        };
        let threads = threads
            .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, usize::from))
            .max(1);
        let touched: BTreeSet<usize> = items
            .iter()
            .flat_map(|i| i.tasks.iter().map(|t| t.file))
            .collect();
        // The fingerprints recorded are taken after reading, so a file that
        // changed before or while the worker read it is refused naming the
        // item; a read that failed on a changed file says so too.
        let written = match segments::write_all(&ctx, &items, threads) {
            Ok(w) => w,
            Err(e) => {
                return match check_files(&loaded, item, &touched)? {
                    Err(r) => Ok(Err(r)),
                    Ok(_) => Err(e),
                }
            }
        };
        let files = match check_files(&loaded, item, &touched)? {
            Ok(f) => f,
            Err(r) => return Ok(Err(r)),
        };
        let outputs = DoneOutputs {
            segments: written.segments.iter().map(DoneSegment::of).collect(),
            files: files.clone(),
            row_groups_read: written.row_groups_read,
            files_listed: written.files_listed,
            tombstone_rows: written
                .tombstone_rows
                .iter()
                .map(|t| MarksFile {
                    sidecar: t.sidecar.clone(),
                    rows: t.marks.rows,
                    tombstones: t.marks.tombstones,
                    nulls: t.marks.nulls,
                })
                .collect(),
        };
        let elapsed = started.elapsed();
        let body = DoneBody {
            format: DONE_FORMAT.into(),
            run: loaded.run,
            item,
            item_hash: entry.hash,
            outputs,
            threads: u32::try_from(threads).unwrap_or(u32::MAX),
            elapsed_ms: u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
            peak_row_groups_held: ctx.peak_held.load(Ordering::SeqCst),
            peak_segment_keys: ctx.peak_keys.load(Ordering::SeqCst),
        };
        let path = done_path(self.index_root(), &loaded.run, item);
        let write = self.write_done(&path, &body)?;
        Ok(Ok(ItemDone {
            run: loaded.run,
            item,
            segments: written.segments,
            row_groups_read: written.row_groups_read,
            files,
            done_path: path,
            write,
            peak_row_groups_held: body.peak_row_groups_held,
            peak_segment_keys: body.peak_segment_keys,
            elapsed,
        }))
    }

    /// Writes a done file, conditionally: an equal one (same outputs) is
    /// left in place; another is replaced.
    fn write_done(&self, path: &str, body: &DoneBody) -> Result<DoneWrite> {
        let bytes = done_bytes(body)?;
        for _ in 0..4 {
            match self.store().put_if_absent(path, bytes.clone()) {
                Ok(_) => return Ok(DoneWrite::New),
                Err(Error::VersionConflict { .. }) => {}
                Err(e) => return Err(e),
            }
            let Some((old, version)) = self.store().get_opt(path)? else {
                continue;
            };
            if parse_done(&old).is_ok_and(|o| {
                o.run == body.run
                    && o.item == body.item
                    && o.item_hash == body.item_hash
                    && o.outputs == body.outputs
            }) {
                return Ok(DoneWrite::Unchanged);
            }
            match self.store().put_if_version(path, bytes.clone(), &version) {
                Ok(_) => return Ok(DoneWrite::Replaced),
                Err(Error::VersionConflict { .. }) => {}
                Err(e) => return Err(e),
            }
        }
        Err(Error::VersionConflict {
            expected: format!("one writer of {path}"),
            found: "several workers rewriting it at once".into(),
        })
    }

    // -----------------------------------------------------------------------
    // The commit
    // -----------------------------------------------------------------------

    /// Commits the plan `plan` (its bytes) once every item is done: checks
    /// every done file and every segment it lists, then publishes the union
    /// of the items' segments with the plan's row-group space, schema, and
    /// source in one manifest commit (the build's commit closure). A
    /// garbage-collection claim on a listed segment is retried as a build
    /// retries it, up to `options.max_restarts` times.
    #[allow(clippy::too_many_lines)]
    pub fn commit_work(
        &self,
        plan: &[u8],
        threads: Option<usize>,
        commit: CommitOptions,
        max_restarts: u32,
    ) -> Result<std::result::Result<WorkCommitted, WorkRefusal>> {
        let started = Instant::now();
        let loaded = match self.load(plan)? {
            Ok(l) => l,
            Err(r) => return Ok(Err(r)),
        };
        let run = loaded.run;
        // Every done file first: the refusal names every item missing.
        let mut dones: Vec<(u32, Vec<Item>, Bytes)> = Vec::with_capacity(loaded.file.items.len());
        let mut missing = Vec::new();
        for e in &loaded.file.items {
            let items = self.load_item(&loaded, e.item)?;
            match self
                .store()
                .get_opt(&done_path(self.index_root(), &run, e.item))?
            {
                Some((b, _)) => dones.push((e.item, items, b)),
                None => missing.push(e.item),
            }
        }
        if !missing.is_empty() {
            return Ok(Err(WorkRefusal::NotDone {
                run,
                items: missing,
            }));
        }
        let mut owner: BTreeMap<Hash, u32> = BTreeMap::new();
        let mut added: Vec<SegmentRef> = Vec::new();
        let mut timings = Vec::with_capacity(dones.len());
        let mut report = BuildReport::default();
        let mut marks: BTreeMap<String, Marks> = BTreeMap::new();
        for (n, items, bytes) in &dones {
            let n = *n;
            let body = match parse_done(bytes) {
                Ok(b) => b,
                Err(reason) => return Ok(Err(WorkRefusal::BadDone { item: n, reason })),
            };
            if let Err(r) = check_done(&loaded, n, items, &body) {
                return Ok(Err(r));
            }
            for s in &body.outputs.segments {
                if let Some(other) = owner.insert(s.id, n) {
                    return Ok(Err(WorkRefusal::BadDone {
                        item: n,
                        reason: format!("it lists segment {} that item {other} lists too", s.id),
                    }));
                }
                added.push(s.to_ref());
            }
            report.row_groups_read += body.outputs.row_groups_read;
            report.files_listed += body.outputs.files_listed;
            report.peak_row_groups_held =
                report.peak_row_groups_held.max(body.peak_row_groups_held);
            report.peak_segment_keys = report.peak_segment_keys.max(body.peak_segment_keys);
            for t in &body.outputs.tombstone_rows {
                marks.entry(t.sidecar.clone()).or_default().add(Marks {
                    rows: t.rows,
                    tombstones: t.tombstones,
                    nulls: t.nulls,
                });
            }
            timings.push(ItemTiming {
                item: n,
                elapsed: Duration::from_millis(body.elapsed_ms),
                threads: body.threads,
                row_groups_read: body.outputs.row_groups_read,
                segments: body.outputs.segments.len() as u64,
            });
        }
        if let Some(r) = self.check_segments(&loaded, &added, &owner, threads)? {
            return Ok(Err(r));
        }
        // The order a single-machine build commits in: its workers' outputs
        // in item order, sorted stably by target and range start.
        added.sort_by(|a, b| {
            (&a.key_spec, a.rowgroup_range.start).cmp(&(&b.key_spec, b.rowgroup_range.start))
        });
        let c = &loaded.file.commit;
        let base = &loaded.file.base;
        let delta = Delta {
            planned: Planned::Digest(BaseDigest {
                space: base.space,
                schema: base.schema,
                source: base.source,
            }),
            source: c.source.clone(),
            schema: c.schema.clone(),
            space: c.space.clone(),
            keep: &c.keep,
            replaced: &c.replaced,
            added: &added,
            fresh_outputs: Some(&c.replaced_base),
        };
        let mut restarts = 0u32;
        let committed = loop {
            let stale = Cell::new(None);
            match commit_with(
                &**self.store(),
                self.index_root(),
                &StagedSegments::new(),
                commit,
                |m| delta.apply(m, &stale),
            ) {
                Ok(c) => break c,
                Err(e) => {
                    if let Some(why) = stale.get() {
                        let current = read_manifest(&**self.store(), self.index_root())?
                            .map_or(0, |(m, _)| m.sequence);
                        return Ok(Err(WorkRefusal::Stale {
                            run,
                            what: why.what(),
                            base: base.sequence,
                            current,
                            committed: why == Stale::AlreadyCommitted,
                        }));
                    }
                    if let Error::SegmentClaimed(id) = e {
                        let item = owner.get(&id).copied().unwrap_or_default();
                        restarts += 1;
                        if restarts > max_restarts {
                            return Ok(Err(WorkRefusal::Claimed { item, segment: id }));
                        }
                        // A claim clears only after its run deletes and
                        // releases, as a build waits for it.
                        std::thread::sleep(Duration::from_millis(100 << restarts.min(5)));
                        if !self.store().exists(&segment_path(self.index_root(), &id))? {
                            return Ok(Err(WorkRefusal::Segment {
                                item,
                                segment: id,
                                reason: "deleted by garbage collection".into(),
                            }));
                        }
                        continue;
                    }
                    // A segment gone between the checks and the commit.
                    for s in &added {
                        if !self
                            .store()
                            .exists(&segment_path(self.index_root(), &s.id))?
                        {
                            return Ok(Err(WorkRefusal::Segment {
                                item: owner.get(&s.id).copied().unwrap_or_default(),
                                segment: s.id,
                                reason: "missing from the index store".into(),
                            }));
                        }
                    }
                    return Err(e);
                }
            }
        };
        let r = &loaded.file.report;
        report.sequence = Some(committed.sequence);
        report.files_new = r.files_new;
        report.files_changed = r.files_changed;
        report.files_removed = r.files_removed;
        report.files_unchanged = r.files_unchanged;
        report.row_groups_assigned = r.row_groups_assigned;
        report.row_groups_retired = r.row_groups_retired;
        report.files_filled = r.files_filled;
        report.specs_rebuilt.clone_from(&r.specs_rebuilt);
        report.specs_repaired.clone_from(&r.specs_repaired);
        report.specs_removed.clone_from(&r.specs_removed);
        report.text_tables_replaced = r
            .text_tables_replaced
            .iter()
            .map(|t| TextTablesReplaced {
                spec: t.spec.clone(),
                recorded: t.recorded.clone(),
                current: t.current.clone(),
            })
            .collect();
        report.sidecars_folded.clone_from(&r.sidecars_folded);
        report.sidecars_kept.clone_from(&r.sidecars_kept);
        report.tombstones_dropped = r
            .tombstones_dropped
            .iter()
            .map(|d| TombstoneDropped {
                sidecar: d.sidecar.clone(),
                tombstone: d.tombstone.clone(),
                now: d.now.clone(),
            })
            .collect();
        report.tombstone_rows = marks
            .into_iter()
            .filter_map(|(sidecar, marks)| {
                let tombstone = loaded.specs.iter().find_map(|p| match &p.target {
                    Target::Tombstone(spec) if p.name == sidecar => spec.tombstone().cloned(),
                    _ => None,
                })?;
                Some(TombstoneRows {
                    sidecar,
                    tombstone,
                    marks,
                })
            })
            .collect();
        report.upgraded_from = base.storage_version.filter(|&v| v < STORAGE_VERSION);
        report.segments_written = added.len() as u64;
        report.segments = added;
        report.restarts = restarts;
        report.elapsed = started.elapsed();
        Ok(Ok(WorkCommitted {
            run,
            base_sequence: base.sequence,
            report,
            items: timings,
            joins: loaded.file.joins.clone(),
            reset_sketches: r.reset_sketches.clone(),
        }))
    }

    /// Reads every segment in `added` back, on `threads` threads, one
    /// whole segment per thread at a time (each at most the segment caps):
    /// present, its BLAKE3 its id, its length the listed bytes, and its
    /// header the listed target's with the listed range.
    fn check_segments(
        &self,
        plan: &Loaded,
        added: &[SegmentRef],
        owner: &BTreeMap<Hash, u32>,
        threads: Option<usize>,
    ) -> Result<Option<WorkRefusal>> {
        let threads = threads
            .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, usize::from))
            .max(1);
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .map_err(|e| Error::Other(format!("worker pool: {e}")))?;
        let store: &S = self.store();
        let root = self.index_root();
        let found: Vec<Option<WorkRefusal>> = pool.install(|| {
            added
                .par_iter()
                .map(|s| -> Result<Option<WorkRefusal>> {
                    let item = owner.get(&s.id).copied().unwrap_or_default();
                    let refuse = |reason: String| {
                        Ok(Some(WorkRefusal::Segment {
                            item,
                            segment: s.id,
                            reason,
                        }))
                    };
                    let Some((bytes, _)) = store.get_opt(&segment_path(root, &s.id))? else {
                        return refuse("missing from the index store".into());
                    };
                    if Hash::of(&bytes) != s.id {
                        return refuse("its bytes do not hash to its id".into());
                    }
                    if bytes.len() as u64 != s.bytes {
                        return refuse(format!(
                            "it is {} bytes, and the done file says {}",
                            bytes.len(),
                            s.bytes
                        ));
                    }
                    let target = plan
                        .specs
                        .iter()
                        .find(|p| p.name == s.key_spec && p.target.kind() == s.kind.as_deref())
                        .map(|p| &p.target);
                    match target.map(|t| header_matches(t, &bytes, &s.rowgroup_range)) {
                        Some(Ok(true)) => Ok(None),
                        Some(Ok(false)) => {
                            refuse("its header records another target or range".into())
                        }
                        Some(Err(e)) => refuse(format!("its header does not read ({e})")),
                        None => refuse("it is not one of the plan's targets".into()),
                    }
                })
                .collect::<Result<Vec<_>>>()
        })?;
        Ok(found.into_iter().flatten().next())
    }
}

/// Fingerprints the files `item` reads (indexes into the plan's files) and
/// compares them with the plan's.
fn check_files(
    plan: &Loaded,
    item: u32,
    touched: &BTreeSet<usize>,
) -> Result<std::result::Result<Vec<FileFingerprint>, WorkRefusal>> {
    let mut out = Vec::with_capacity(touched.len());
    for &f in touched {
        let planned = &plan.rows[f].fingerprint;
        let read = match std::fs::metadata(&planned.path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            _ => Some(fingerprint_with_policy(
                &LocalFs,
                &planned.path,
                plan.policy,
            )?),
        };
        match read {
            Some(r) if !drift(planned, &r) => out.push(r),
            read => {
                return Ok(Err(WorkRefusal::Drift {
                    item,
                    path: planned.path.clone(),
                    planned: Box::new(planned.clone()),
                    read: read.map(Box::new),
                }))
            }
        }
    }
    Ok(Ok(out))
}

/// Checks a done file against its item: the run and item, the files it
/// read against the plan, and its segments, per build item and target,
/// tiling exactly the build item's row groups as a writer cuts them (each
/// range starts and ends at row groups of the item, no two overlap, every
/// row group in one).
fn check_done(
    plan: &Loaded,
    n: u32,
    items: &[Item],
    body: &DoneBody,
) -> std::result::Result<(), WorkRefusal> {
    let bad = |reason: String| WorkRefusal::BadDone { item: n, reason };
    if body.run != plan.run {
        return Err(bad(format!("it belongs to run {}", body.run)));
    }
    if body.item != n || body.item_hash != plan.file.items[n as usize].hash {
        return Err(bad(format!("it is item {}'s, not item {n}'s", body.item)));
    }
    let touched: BTreeSet<usize> = items
        .iter()
        .flat_map(|i| i.tasks.iter().map(|t| t.file))
        .collect();
    if body.outputs.files.len() != touched.len() {
        return Err(bad(format!(
            "it lists {} files read, and the item reads {}",
            body.outputs.files.len(),
            touched.len()
        )));
    }
    for (&f, read) in touched.iter().zip(&body.outputs.files) {
        let planned = &plan.rows[f].fingerprint;
        if read.path != planned.path {
            return Err(bad(format!("it lists {} among the files read", read.path)));
        }
        if drift(planned, read) {
            return Err(WorkRefusal::Drift {
                item: n,
                path: planned.path.clone(),
                planned: Box::new(planned.clone()),
                read: Some(Box::new(read.clone())),
            });
        }
    }
    let mut by_target: BTreeMap<(&str, Option<&str>), Vec<&DoneSegment>> = BTreeMap::new();
    for s in &body.outputs.segments {
        if s.start >= s.end {
            return Err(bad(format!("segment {} has an empty range", s.id)));
        }
        by_target
            .entry((s.key_spec.as_str(), s.kind.as_deref()))
            .or_default()
            .push(s);
    }
    let mut used = 0usize;
    for (s, p) in plan.specs.iter().enumerate() {
        let mut segs = by_target
            .remove(&(p.name.as_str(), p.target.kind()))
            .unwrap_or_default();
        segs.sort_by_key(|s| (s.start, s.end));
        let mut next = segs.iter().peekable();
        for item in items.iter().filter(|i| i.mask & (1 << s) != 0) {
            let ids: Vec<u32> = item.tasks.iter().map(|t| t.id).collect();
            let mut i = 0usize;
            while i < ids.len() {
                let Some(seg) = next.next_if(|g| g.start == ids[i]) else {
                    return Err(bad(format!(
                        "no segment of {} starts at row group {} of its work",
                        p.name, ids[i]
                    )));
                };
                used += 1;
                // The range ends one past one of the item's row groups, and
                // takes every row group of the item before that.
                let Some(last) = ids[i..].iter().position(|&id| id + 1 == seg.end) else {
                    return Err(bad(format!(
                        "segment {} of {} ends at {}, not after a row group of its work",
                        seg.id, p.name, seg.end
                    )));
                };
                i += last + 1;
            }
        }
        if let Some(extra) = next.next() {
            return Err(bad(format!(
                "segment {} of {} covers row groups outside its work or twice",
                extra.id, p.name
            )));
        }
    }
    if let Some(((name, _), _)) = by_target.into_iter().next() {
        return Err(bad(format!(
            "it lists segments of {name}, which the item does not build"
        )));
    }
    debug_assert_eq!(used, body.outputs.segments.len());
    Ok(())
}

/// Whether segment `bytes` was written for `target` over `range`, as its
/// header records them.
fn header_matches(target: &Target, bytes: &[u8], range: &RowGroupRange) -> Result<bool> {
    let inclusive = Some((range.start, RowGroupId(range.end.0 - 1)));
    Ok(match target {
        Target::Key(spec) | Target::Tombstone(spec) | Target::BloomBounds(spec) => {
            let layout = match target {
                Target::BloomBounds(_) => Layout::Keyed,
                _ => Layout::for_spec(spec),
            };
            let r = SegmentReader::open(bytes)?;
            r.header().key_spec == *spec
                && r.header().layout == layout
                && r.rowgroup_range() == inclusive
        }
        Target::Sidecar(def) => {
            let r = SidecarReader::open(bytes)?;
            let h = r.header();
            h.kind == def.kind
                && h.columns == def.columns
                && h.time_columns == def.time_columns
                && r.rowgroup_range() == inclusive
        }
        Target::Sketch(spec) => {
            let r = SketchReader::open(bytes)?;
            r.spec() == *spec && r.rowgroup_range() == (range.start.0, range.end.0)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(n: u32) -> Item {
        Item {
            mask: 1,
            tasks: (0..n)
                .map(|i| Task {
                    id: i,
                    file: 0,
                    index: i,
                })
                .collect(),
        }
    }

    #[test]
    fn bundles_are_contiguous_nonempty_and_bounded() {
        let items: Vec<Item> = [3, 1, 4, 1, 5, 9, 2, 6].iter().map(|&n| item(n)).collect();
        for split in [
            ItemSplit::Items(1),
            ItemSplit::Items(3),
            ItemSplit::Items(8),
            ItemSplit::Items(100),
            ItemSplit::RowGroups(1),
            ItemSplit::RowGroups(7),
            ItemSplit::RowGroups(1000),
        ] {
            let b = bundle(&items, split).unwrap();
            assert_eq!(b.first().unwrap().start, 0, "{split:?}");
            assert_eq!(b.last().unwrap().end, items.len(), "{split:?}");
            for w in b.windows(2) {
                assert_eq!(w[0].end, w[1].start, "{split:?}");
            }
            assert!(b.iter().all(|r| !r.is_empty()), "{split:?}");
            match split {
                ItemSplit::Items(n) => assert!(b.len() <= n as usize, "{split:?}"),
                ItemSplit::RowGroups(m) => {
                    for r in &b {
                        let rgs: usize = items[r.clone()].iter().map(|i| i.tasks.len()).sum();
                        assert!(rgs as u64 <= m || r.len() == 1, "{split:?}");
                    }
                }
            }
        }
        assert_eq!(bundle(&items, ItemSplit::Items(3)).unwrap().len(), 3);
        assert!(bundle(&items, ItemSplit::Items(0)).is_err());
        assert!(bundle(&items, ItemSplit::RowGroups(0)).is_err());
        assert_eq!(
            bundle(&[], ItemSplit::Items(4)).unwrap(),
            Vec::<Range<usize>>::new()
        );
    }

    fn body() -> DoneBody {
        DoneBody {
            format: DONE_FORMAT.into(),
            run: Hash::of(b"run"),
            item: 2,
            item_hash: Hash::of(b"item"),
            outputs: DoneOutputs {
                segments: vec![DoneSegment {
                    id: Hash::of(b"seg"),
                    key_spec: "exact:id".into(),
                    kind: None,
                    start: 4,
                    end: 9,
                    bytes: 1234,
                }],
                files: vec![FileFingerprint {
                    path: "/data/a.parquet".into(),
                    size: 99,
                    etag: Some("1700000000".into()),
                    tail_blake3: Hash::of(b"tail"),
                }],
                row_groups_read: 5,
                files_listed: 1,
                tombstone_rows: vec![],
            },
            threads: 4,
            elapsed_ms: 17,
            peak_row_groups_held: 3,
            peak_segment_keys: 10,
        }
    }

    #[test]
    fn a_done_file_round_trips_and_every_byte_flip_is_refused() {
        let b = body();
        let bytes = done_bytes(&b).unwrap();
        assert_eq!(parse_done(&bytes).unwrap(), b);
        for i in 0..bytes.len() {
            for flip in [0x01u8, 0x20, 0x80] {
                let mut bad = bytes.to_vec();
                bad[i] ^= flip;
                assert!(
                    parse_done(&bad).is_err(),
                    "byte {i} ^ {flip:#x} was accepted: {}",
                    String::from_utf8_lossy(&bad)
                );
            }
        }
        for cut in 0..bytes.len() {
            assert!(parse_done(&bytes[..cut]).is_err(), "truncated at {cut}");
        }
    }

    #[test]
    fn plans_refuse_every_byte_flip_before_use() {
        // A plan is checked by its run id (BLAKE3 of its bytes, which is its
        // path) and parsed exactly: any flip is either unparsable, spelled
        // differently, or another run id whose path holds nothing.
        let bytes = to_bytes(&ItemFile {
            format: ITEM_FORMAT.into(),
            item: 0,
            units: vec![UnitFile {
                mask: 3,
                tasks: vec![[0, 0, 0], [0, 1, 1]],
            }],
        })
        .unwrap();
        let ok: ItemFile = parse_exact(&bytes).unwrap();
        assert_eq!(ok.units[0].tasks.len(), 2);
        for i in 0..bytes.len() {
            let mut bad = bytes.to_vec();
            bad[i] ^= 0x01;
            let parsed = parse_exact::<ItemFile>(&bad);
            assert!(
                parsed.is_err() || Hash::of(&bad) != Hash::of(&bytes),
                "byte {i}"
            );
        }
    }
}
