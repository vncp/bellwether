//! The multi-machine build (ADR 030): `index --plan-work` on the machine
//! that commits, `index --worker` on any machine that mounts the data root
//! and reaches the index store, and `index --commit` once every item is
//! done. The build crate's [`bellwether_build::work`] does the work; this
//! module opens the index, writes the local copy of the plan, and turns
//! every refusal into a message that names the command to run next.

use std::path::Path;
use std::sync::Arc;

use bellwether_build::work::{self, DoneWrite, ItemSplit, PlanOutcome, WorkOptions, WorkRefusal};
use bellwether_build::{Builder, DEFAULT_MAX_RESTARTS};
use bellwether_store::{CommitOptions, ObjectStoreBackend};
use bellwether_types::{Error, Hash, Result, SketchSpec};
use serde::{Deserialize, Serialize};

use crate::engine::{
    guard, local_path, root_hint, schema_hint, Engine, EngineOptions, SourceFormat,
};
use crate::results::IndexResult;
use crate::spec::Declaration;

/// The file `index --plan-work <dir>` writes the plan to.
pub const PLAN_FILE: &str = "plan.json";

/// One work item of a plan ([`WorkPlanResult::items`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkItemPlan {
    /// Its number, for `index --worker <plan> --item <n>`.
    pub item: u32,
    /// Row groups it reads.
    pub row_groups: u64,
    /// Build items in it; each writes its own segments.
    pub units: u32,
    /// Files it reads.
    pub files: u32,
}

/// What [`Engine::plan_work`] did (`index --plan-work`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkPlanResult {
    /// The canonical data root.
    pub root: String,
    /// The index location the plan records.
    pub index: String,
    /// The run id (BLAKE3 of the plan), or `None` when there was nothing to
    /// do and no plan was written.
    pub run: Option<Hash>,
    /// The local copy of the plan, to hand to workers and the committer.
    pub plan: Option<String>,
    /// The plan's path in the index store.
    pub store_path: Option<String>,
    /// The manifest sequence the plan was made against (0: a new index).
    pub base_sequence: u64,
    /// Its work items.
    pub items: Vec<WorkItemPlan>,
    /// Row groups the items read in all.
    pub row_groups: u64,
    /// Files seen for the first time.
    pub files_new: u64,
    /// Tracked files whose fingerprint changed: their ids are retired.
    pub files_changed: u64,
    /// Tracked files that are gone.
    pub files_removed: u64,
    /// Tracked files unchanged.
    pub files_unchanged: u64,
    /// Fresh row-group ids the plan assigned.
    pub row_groups_assigned: u64,
    /// Row-group ids the plan retires.
    pub row_groups_retired: u64,
    /// Targets rebuilt over every live row group.
    pub specs_rebuilt: Vec<String>,
    /// Re-declared sketches whose segments could not be read: rebuilt.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sketches_rebuilt: Vec<String>,
}

/// What [`work_item`] did (`index --worker`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkItemResult {
    /// The run.
    pub run: Hash,
    /// The item.
    pub item: u32,
    /// Segments uploaded.
    pub segments: u64,
    /// Their bytes.
    pub bytes: u64,
    /// Row groups read.
    pub row_groups_read: u64,
    /// Files read, each fingerprinted after reading.
    pub files: u64,
    /// The done file's path in the index store.
    pub done: String,
    /// `new`, `unchanged` (a rerun found an equal done file and left it),
    /// or `replaced` (a damaged or other done file was replaced).
    pub written: String,
    /// Most row groups held at once.
    pub peak_row_groups_held: u64,
    /// Largest key count one open segment reached.
    pub peak_segment_keys: u64,
    /// Wall time in milliseconds.
    pub elapsed_ms: u64,
}

/// Per item, what its worker reported ([`WorkCommitResult::items`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkItemTiming {
    /// The item.
    pub item: u32,
    /// Its worker's wall time in milliseconds.
    pub elapsed_ms: u64,
    /// Its worker's threads.
    pub threads: u32,
    /// Row groups it read.
    pub row_groups_read: u64,
    /// Segments it wrote.
    pub segments: u64,
}

/// What [`commit_work`] did (`index --commit`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkCommitResult {
    /// The run committed.
    pub run: Hash,
    /// The manifest sequence the plan was made against.
    pub base_sequence: u64,
    /// Per item, from its done file.
    pub items: Vec<WorkItemTiming>,
    /// The commit's elapsed time in milliseconds (checks included).
    pub elapsed_ms: u64,
    /// What an `index` reports: the sequence committed, segments, and every
    /// declared spec's completeness.
    pub index: IndexResult,
}

impl Engine {
    /// Plans the build `index` would run with `declared` for workers on
    /// other machines (ADR 030): writes the plan and its work items to the
    /// index store under `work/<run id>/`, and the plan to `out/plan.json`.
    /// Commits nothing; nothing to do writes nothing.
    pub fn plan_work(
        &self,
        declared: &[Declaration],
        split: ItemSplit,
        out: &Path,
    ) -> Result<WorkPlanResult> {
        guard(|| self.plan_work_inner(declared, split, out))
            .map_err(|e| root_hint(self.root(), schema_hint(e)))
    }

    fn plan_work_inner(
        &self,
        declared: &[Declaration],
        split: ItemSplit,
        out: &Path,
    ) -> Result<WorkPlanResult> {
        let (specs, joins, options) = self.index_request("index --plan-work", declared)?;
        let file = out.join(PLAN_FILE);
        if file.exists() {
            return Err(Error::InvalidArgument {
                field: "out",
                reason: format!(
                    "{} exists; pass an empty or new directory to --plan-work (a plan is never \
                     overwritten)",
                    file.display()
                ),
            });
        }
        let reset = self.unreadable_sketches(&options.sketches)?;
        // Workers on other machines open this location: a local index is
        // recorded absolute.
        let index = match local_path(self.index_location())? {
            Some(dir) => dir.display().to_string(),
            None => self.index_location().to_string(),
        };
        let builder = Builder::new(Arc::clone(self.store()?), "");
        let outcome = builder.plan_work(
            self.root(),
            &specs,
            &options,
            &WorkOptions {
                split,
                reset_sketches: reset.clone(),
                joins,
                index: index.clone(),
            },
        )?;
        let mut result = WorkPlanResult {
            root: self.root().to_string(),
            index,
            run: None,
            plan: None,
            store_path: None,
            base_sequence: self.manifest()?.map_or(0, |m| m.sequence),
            items: Vec::new(),
            row_groups: 0,
            files_new: 0,
            files_changed: 0,
            files_removed: 0,
            files_unchanged: 0,
            row_groups_assigned: 0,
            row_groups_retired: 0,
            specs_rebuilt: Vec::new(),
            sketches_rebuilt: Vec::new(),
        };
        let p = match outcome {
            PlanOutcome::Nothing(r) => {
                result.files_unchanged = r.files_unchanged;
                return Ok(result);
            }
            PlanOutcome::Planned(p) => p,
        };
        std::fs::create_dir_all(out).map_err(|e| Error::InvalidArgument {
            field: "out",
            reason: format!(
                "cannot create {} ({e}); pass a writable directory",
                out.display()
            ),
        })?;
        std::fs::write(&file, &p.plan).map_err(|e| Error::InvalidArgument {
            field: "out",
            reason: format!(
                "cannot write {} ({e}); pass a writable directory",
                file.display()
            ),
        })?;
        let r = &p.report;
        result.run = Some(p.run);
        result.plan = Some(file.display().to_string());
        result.store_path = Some(p.path.clone());
        result.base_sequence = p.base_sequence;
        result.row_groups = p.items.iter().map(|i| i.row_groups).sum();
        result.items = p
            .items
            .iter()
            .map(|i| WorkItemPlan {
                item: i.item,
                row_groups: i.row_groups,
                units: i.units,
                files: i.files,
            })
            .collect();
        result.files_new = r.files_new;
        result.files_changed = r.files_changed;
        result.files_removed = r.files_removed;
        result.files_unchanged = r.files_unchanged;
        result.row_groups_assigned = r.row_groups_assigned;
        result.row_groups_retired = r.row_groups_retired;
        result.specs_rebuilt.clone_from(&r.specs_rebuilt);
        result.sketches_rebuilt = reset
            .iter()
            .filter(|s| r.specs_rebuilt.contains(&s.name()))
            .map(SketchSpec::label)
            .collect();
        Ok(result)
    }
}

/// The plan at `plan` (a local file `index --plan-work` wrote, or a copy):
/// its bytes, what it says, and the index location to open (`index`, or
/// the one the plan records).
fn open_plan(
    plan: &Path,
    index: Option<&str>,
) -> Result<(Vec<u8>, work::PlanSummary, String, Arc<ObjectStoreBackend>)> {
    let bytes = std::fs::read(plan).map_err(|e| Error::InvalidArgument {
        field: "items",
        reason: format!(
            "cannot read the work plan {} ({e}); pass the plan.json `index --plan-work` wrote",
            plan.display()
        ),
    })?;
    let summary = work::read_plan(&bytes)?;
    let location = index.map_or_else(|| summary.index.clone(), str::to_string);
    if let Some(dir) = local_path(&location)? {
        if !dir.is_dir() {
            return Err(Error::InvalidArgument {
                field: "index",
                reason: format!(
                    "there is no index at {} on this machine; pass --index with the location \
                     of the index store the plan was made in (it recorded {})",
                    dir.display(),
                    summary.index
                ),
            });
        }
    }
    let store = ObjectStoreBackend::from_url(&location).map_err(|e| Error::InvalidArgument {
        field: "index",
        reason: format!("cannot open the index store {location} ({e}); pass --index"),
    })?;
    Ok((bytes, summary, location, Arc::new(store)))
}

/// Runs item `item` of the plan at `plan` (`index --worker`): writes and
/// uploads its segments, and writes its done file. Reads and writes no
/// manifest. `index` overrides the index location the plan records.
pub fn work_item(
    plan: &Path,
    item: u32,
    index: Option<&str>,
    threads: Option<usize>,
) -> Result<WorkItemResult> {
    guard(|| {
        let (bytes, summary, location, store) = open_plan(plan, index)?;
        let builder = Builder::new(store, "");
        let done = builder
            .work_item(&bytes, item, threads)?
            .map_err(|r| refusal(&r, plan, index, &location, &summary))?;
        Ok(WorkItemResult {
            run: done.run,
            item: done.item,
            segments: done.segments.len() as u64,
            bytes: done.segments.iter().map(|s| s.bytes).sum(),
            row_groups_read: done.row_groups_read,
            files: done.files.len() as u64,
            done: done.done_path,
            written: match done.write {
                DoneWrite::New => "new",
                DoneWrite::Unchanged => "unchanged",
                DoneWrite::Replaced => "replaced",
            }
            .to_string(),
            peak_row_groups_held: done.peak_row_groups_held,
            peak_segment_keys: done.peak_segment_keys,
            elapsed_ms: millis(done.elapsed),
        })
    })
}

/// Commits the plan at `plan` once every item is done (`index --commit`):
/// checks every done file and segment, commits the union once, then
/// rebuilds the join keys given with the plan, as `index` does. `index`
/// overrides the index location the plan records.
pub fn commit_work(
    plan: &Path,
    index: Option<&str>,
    threads: Option<usize>,
) -> Result<WorkCommitResult> {
    guard(|| {
        let (bytes, summary, location, _) = open_plan(plan, index)?;
        let engine = Engine::open(
            &summary.source_root,
            EngineOptions {
                index: Some(location.clone()),
                format: SourceFormat::from_kind(summary.source),
                threads,
                ..EngineOptions::default()
            },
        )?;
        let builder = Builder::new(Arc::clone(engine.store()?), "");
        let c = builder
            .commit_work(
                &bytes,
                threads,
                CommitOptions::default(),
                DEFAULT_MAX_RESTARTS,
            )?
            .map_err(|r| refusal(&r, plan, index, &location, &summary))?;
        // A join key that cannot be rebuilt once the commit landed is
        // reported beside it, as `index` reports it.
        let joins = engine.refresh_joins(&c.joins, true)?;
        let sketches = c.reset_sketches.iter().map(SketchSpec::label).collect();
        let elapsed_ms = millis(c.report.elapsed);
        let index = engine.index_result(c.report, joins, sketches)?;
        Ok(WorkCommitResult {
            run: c.run,
            base_sequence: c.base_sequence,
            items: c
                .items
                .iter()
                .map(|t| WorkItemTiming {
                    item: t.item,
                    elapsed_ms: millis(t.elapsed),
                    threads: t.threads,
                    row_groups_read: t.row_groups_read,
                    segments: t.segments,
                })
                .collect(),
            elapsed_ms,
            index,
        })
    })
}

fn millis(d: std::time::Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// A refusal as a message that names the command to run next. `plan` and
/// `index` as the caller gave them.
#[allow(clippy::too_many_lines)] // one arm per refusal
fn refusal(
    r: &WorkRefusal,
    plan: &Path,
    index: Option<&str>,
    location: &str,
    summary: &work::PlanSummary,
) -> Error {
    let plan = plan.display();
    let index_flag = index.map(|i| format!(" --index {i}")).unwrap_or_default();
    let worker =
        |item: u32| format!("`bellwether index --worker {plan} --item {item}{index_flag}`");
    let commit = format!("`bellwether index --commit {plan}{index_flag}`");
    let replan = format!(
        "`bellwether index {} --key ... --plan-work <new dir>` (the keys the plan was made with)",
        summary.source_root
    );
    match r {
        WorkRefusal::NotPublished { run } => Error::InvalidArgument {
            field: "index",
            reason: format!(
                "the work plan {} is not in the index store at {location}: pass --index with the \
                 location it was planned into (the plan records {})",
                short(run),
                summary.index
            ),
        },
        WorkRefusal::NotDone { run, items } => {
            let list: Vec<String> = items.iter().map(u32::to_string).collect();
            Error::Other(format!(
                "the work plan {} is not done: item{} {} {} no done file (its worker has not \
                 finished, failed, or was lost); run {} for {} on a machine that mounts {}, then \
                 {commit} again. Nothing was committed",
                short(run),
                if items.len() == 1 { "" } else { "s" },
                list.join(", "),
                if items.len() == 1 { "has" } else { "have" },
                worker(items[0]),
                if items.len() == 1 {
                    "it".to_string()
                } else {
                    "each of them (--item N)".to_string()
                },
                summary.source_root
            ))
        }
        WorkRefusal::Drift {
            item,
            path,
            planned,
            read,
        } => {
            let now = match read {
                Some(f) => format!(
                    "{} bytes, modified {}, hash {}",
                    f.size,
                    f.etag.as_deref().unwrap_or("?"),
                    short(&f.tail_blake3)
                ),
                None => "gone".to_string(),
            };
            Error::Other(format!(
                "item {item} read {path}, which is not the file the plan fingerprinted (planned \
                 {} bytes, modified {}, hash {}; read {now}): if that machine's copy is stale, \
                 rerun {} on a machine whose copy matches the plan, then {commit}; if the file \
                 changed for good, the plan is stale: run {replan}. Nothing was committed",
                planned.size,
                planned.etag.as_deref().unwrap_or("?"),
                short(&planned.tail_blake3),
                worker(*item)
            ))
        }
        WorkRefusal::NoDataRoot { root } => Error::Source(format!(
            "the work plan reads the data root {root}, which this machine does not have: mount \
             the data at that same path, then run the worker again"
        )),
        WorkRefusal::BadDone { item, reason } => Error::Other(format!(
            "the done file of item {item} is damaged, forged, or not this plan's ({reason}): \
             rerun {} (it rewrites the done file), then {commit} again. Nothing was committed",
            worker(*item)
        )),
        WorkRefusal::Segment {
            item,
            segment,
            reason,
        } => Error::Other(format!(
            "segment {} listed by item {item} is {reason} (garbage collection deletes uploaded \
             segments a commit has not referenced within its retention window): rerun {} (it \
             uploads the same bytes again), then {commit} again. Nothing was committed",
            short(segment),
            worker(*item)
        )),
        WorkRefusal::Claimed { item, segment } => Error::Other(format!(
            "segment {} listed by item {item} is claimed by a garbage-collection run (it waited \
             uncommitted past the retention window): let that run finish, rerun {} (it uploads \
             the segment again), then {commit} again. Nothing was committed",
            short(segment),
            worker(*item)
        )),
        WorkRefusal::Stale {
            run,
            what,
            base,
            current,
            committed: true,
        } => Error::Other(format!(
            "work plan stale: {what} (the plan {} was made at sequence {base}; the index is at \
             {current}): another commit of this plan landed, or an earlier --commit landed and \
             its answer was lost; nothing to do. To index changes made since, run {replan}",
            short(run)
        )),
        WorkRefusal::Stale {
            run,
            what,
            base,
            current,
            committed: false,
        } => Error::Other(format!(
            "work plan stale; run --plan-work again: {what} the plan {} was made (at sequence \
             {base}; the index is at {current}), so the row-group ids it assigned may collide \
             (ADR 013). Run {replan}, then its workers and --commit. The segments this plan's \
             workers uploaded are orphans that garbage collection removes. Nothing was committed",
            short(run)
        )),
    }
}

/// The first 12 hex digits of a hash, as messages print it.
fn short(h: &Hash) -> String {
    h.to_hex()[..12].to_string()
}
