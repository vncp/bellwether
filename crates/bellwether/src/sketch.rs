//! `cardinality` and `overlap` from `sketch` sidecars, and what `why` reads
//! from them (its `sketch` column and the `R-sketch-1` lint).
//!
//! **Approximate, and never planned with.** Every number here comes from
//! `HyperLogLog` registers or `MinHash` signatures and is labelled
//! `approximate`; no planner path reads a sketch (invariant 1). A distinct
//! count is exact only where a complete `exact` key spec on exactly the
//! item's columns holds the keys (`exact_distinct`), with or without
//! per-key counts.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use arrow::datatypes::SchemaRef;
use bellwether_build::{declared_sketches, sketch_completeness};
use bellwether_core::segment::{ColumnType, SegmentReader};
use bellwether_core::sketch::{item_types, ItemSketch, SketchReader, SketchTotals};
use bellwether_replay::{Replayer, TransientSketch};
use bellwether_store::SegmentRef;
use bellwether_types::{
    ApproxCardinality, ApproxOverlap, Error, FileFingerprint, KeySpec, Result, RowGroupMeta,
    SketchSpec, Source, SKETCH_DEFAULT_PRECISION,
};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use crate::engine::{guard, schema_hint, Engine, Pinned};

/// `segments[].kind` of a sketch.
const SKETCH_KIND: &str = "sketch";

/// What [`Engine::cardinality`] answers.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CardinalityRequest {
    /// Items to count, each a column or a `+`-joined tuple (`symbol+ts`).
    /// Empty: every item of every complete sketch.
    #[serde(default)]
    pub items: Vec<String>,
    /// Compute the items no complete sketch holds by reading their columns
    /// from every file (at precision 14), instead of refusing. The result
    /// says how many files and row groups were read.
    #[serde(default)]
    pub transient: bool,
}

/// Where a cardinality came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SketchSource {
    /// Committed sketch segments: no data file was read.
    Index,
    /// Computed now from the data files (`transient`).
    Transient,
}

/// One item's cardinality and where it came from.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ItemCardinality {
    /// Where it came from.
    pub source: SketchSource,
    /// The sketch read, when `source` is `index` (its label, with its
    /// parameters).
    pub sketch: Option<String>,
    /// The approximate count, its bound, and the exact row counts.
    pub cardinality: ApproxCardinality,
}

/// What [`Engine::cardinality`] found. Approximate throughout.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CardinalityResult {
    /// Always `true`: every distinct count is an estimate unless its
    /// `exact_distinct` is set.
    pub approximate: bool,
    /// The canonical data root.
    pub root: String,
    /// The manifest sequence read (0 when not indexed).
    pub sequence: u64,
    /// Data files read to answer: 0 when every item came from the index.
    pub files_read: u64,
    /// Row groups read to answer.
    pub row_groups_read: u64,
    /// One entry per item, in the order asked (or declaration order).
    pub items: Vec<ItemCardinality>,
}

/// What [`Engine::overlap`] found. Approximate throughout.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OverlapResult {
    /// Always `true`.
    pub approximate: bool,
    /// The left item's data root.
    pub left_root: String,
    /// The sketch the left item was read from.
    pub left_sketch: String,
    /// The right item's data root (the left's when one folder).
    pub right_root: String,
    /// The sketch the right item was read from.
    pub right_sketch: String,
    /// The estimate and its bound.
    pub overlap: ApproxOverlap,
}

/// One complete sketch folded over every live file, with the dataset types
/// of its items.
pub(crate) struct Folded {
    pub spec: SketchSpec,
    pub totals: SketchTotals,
    pub types: Vec<Vec<ColumnType>>,
}

impl Engine {
    /// `cardinality(item)` for each requested item: the approximate distinct
    /// count from a complete `sketch` sidecar (no data file is read), with
    /// its error bound, the exact sketched and null row counts, rows per
    /// value (fanout), files per value, and files that repeat a value. When
    /// several complete sketches hold an item, the one with the highest
    /// precision is read (then the first declared): every one describes the
    /// same files, and the finer registers bound the count tighter.
    ///
    /// An item no complete sketch holds is refused (`not_indexed`, naming
    /// the `sketch:` declaration that builds it) unless `transient` is set,
    /// which reads the item's columns from every file and says so. A folder
    /// whose files changed since the index was built is refused
    /// (`stale_index`): the sketch would describe other files.
    pub fn cardinality(&self, request: &CardinalityRequest) -> Result<CardinalityResult> {
        guard(|| self.cardinality_inner(request)).map_err(schema_hint)
    }

    fn cardinality_inner(&self, req: &CardinalityRequest) -> Result<CardinalityResult> {
        let pinned = self.pin()?;
        if let Some(p) = &pinned {
            self.refuse_stale(p, "cardinality")?;
        }
        let folded = match &pinned {
            Some(p) => fold_complete_sketches(p)?,
            None => Vec::new(),
        };
        let wanted = self.requested_items(req, pinned.as_ref(), &folded)?;
        let mut out: Vec<Option<ItemCardinality>> = vec![None; wanted.len()];
        let mut missing: Vec<(usize, Vec<String>)> = Vec::new();
        for (slot, label) in wanted.iter().enumerate() {
            match best_sketch(&folded, label) {
                Some((f, i)) => {
                    let exact = match &pinned {
                        Some(p) => exact_distinct(p, &f.spec.items[i])?,
                        None => None,
                    };
                    out[slot] = Some(ItemCardinality {
                        source: SketchSource::Index,
                        sketch: Some(f.spec.label()),
                        cardinality: f.totals.cardinality(i, exact)?,
                    });
                }
                None if req.transient => {
                    missing.push((slot, label.split('+').map(str::to_string).collect()));
                }
                None => {
                    return Err(Error::NotIndexed {
                        root: self.root().to_string(),
                        operation: "cardinality",
                        needs: Some(format!("sketch:{label}")),
                    })
                }
            }
        }
        let (mut files_read, mut row_groups_read) = (0, 0);
        if !missing.is_empty() {
            let spec = SketchSpec {
                items: missing.iter().map(|(_, i)| i.clone()).collect(),
                precision: SKETCH_DEFAULT_PRECISION,
                minhash: None,
            };
            bellwether_core::sketch::validate(&spec)?;
            let source = self.source(pinned.as_ref())?;
            let replayer = match &pinned {
                Some(p) => Replayer::for_snapshot(&*source, &p.snapshot)?,
                None => Replayer::unindexed(&*source, self.root(), &self.partitioning(None)?)?,
            };
            let t = TransientSketch::build(&*source, replayer.catalog(), &spec)?;
            files_read = t.files_read;
            row_groups_read = t.row_groups_read;
            for (i, (slot, item)) in missing.iter().enumerate() {
                let exact = match &pinned {
                    Some(p) => exact_distinct(p, item)?,
                    None => None,
                };
                out[*slot] = Some(ItemCardinality {
                    source: SketchSource::Transient,
                    sketch: None,
                    cardinality: t.totals.cardinality(i, exact)?,
                });
            }
        }
        Ok(CardinalityResult {
            approximate: true,
            root: self.root().to_string(),
            sequence: pinned.as_ref().map_or(0, Pinned::sequence),
            files_read,
            row_groups_read,
            items: out.into_iter().flatten().collect(),
        })
    }

    /// The items a request names, or with none named, every item of every
    /// complete sketch in declaration order; refused when neither says.
    fn requested_items(
        &self,
        req: &CardinalityRequest,
        pinned: Option<&Pinned>,
        folded: &[Folded],
    ) -> Result<Vec<String>> {
        if !req.items.is_empty() {
            return Ok(req.items.clone());
        }
        let mut all: Vec<String> = Vec::new();
        for f in folded {
            for i in &f.spec.items {
                let label = SketchSpec::item_label(i);
                if !all.contains(&label) {
                    all.push(label);
                }
            }
        }
        if !all.is_empty() {
            return Ok(all);
        }
        Err(match (pinned, req.transient) {
            (_, true) => Error::InvalidArgument {
                field: "items",
                reason: "no complete sketch says which columns to count; name the columns (or \
                         column+column tuples) to compute"
                    .into(),
            },
            (Some(_), false) => Error::NotIndexed {
                root: self.root().to_string(),
                operation: "cardinality",
                needs: Some("sketch:<column>".into()),
            },
            (None, false) => Error::NotIndexed {
                root: self.root().to_string(),
                operation: "cardinality",
                needs: None,
            },
        })
    }

    /// `overlap(left, right)`: the approximate Jaccard similarity of two
    /// sketched items' value sets, each read from a complete sketch of its
    /// folder (`other`, or this one). `MinHash` when both sketches keep
    /// signatures of one length, `HyperLogLog` inclusion-exclusion otherwise;
    /// items of different column types or sketched at different precisions
    /// are refused.
    pub fn overlap(
        &self,
        left: &str,
        right: &str,
        other: Option<&Engine>,
    ) -> Result<OverlapResult> {
        guard(|| self.overlap_inner(left, right, other)).map_err(schema_hint)
    }

    fn overlap_inner(
        &self,
        left: &str,
        right: &str,
        other: Option<&Engine>,
    ) -> Result<OverlapResult> {
        let side = |e: &Engine, label: &str| -> Result<(Folded, usize)> {
            let pinned = e.pin_required("overlap")?;
            e.refuse_stale(&pinned, "overlap")?;
            let mut folded = fold_complete_sketches(&pinned)?;
            let Some(pos) = best_index(&folded, label) else {
                return Err(Error::NotIndexed {
                    root: e.root().to_string(),
                    operation: "overlap",
                    needs: Some(format!("sketch:{label}")),
                });
            };
            let f = folded.swap_remove(pos.0);
            Ok((f, pos.1))
        };
        let (lf, li) = side(self, left)?;
        let right_engine = other.unwrap_or(self);
        let (rf, ri) = side(right_engine, right)?;
        let overlap = bellwether_core::sketch::overlap(
            (
                left,
                lf.totals
                    .item(li)
                    .ok_or_else(|| Error::Other("no left item".into()))?,
                &lf.types[li],
                lf.spec.precision,
            ),
            (
                right,
                rf.totals
                    .item(ri)
                    .ok_or_else(|| Error::Other("no right item".into()))?,
                &rf.types[ri],
                rf.spec.precision,
            ),
        )?;
        Ok(OverlapResult {
            approximate: true,
            left_root: self.root().to_string(),
            left_sketch: lf.spec.label(),
            right_root: right_engine.root().to_string(),
            right_sketch: rf.spec.label(),
            overlap,
        })
    }
}

/// Every complete sketch the pinned manifest declares, folded over every
/// live file from its committed segments. Reads segments, never data.
pub(crate) fn fold_complete_sketches(pinned: &Pinned) -> Result<Vec<Folded>> {
    let m = pinned.manifest();
    let schema = pinned.planner.schema();
    let mut out = Vec::new();
    for spec in declared_sketches(m) {
        if sketch_completeness(m, &spec).is_none() {
            continue;
        }
        let types = item_types(&spec, schema)?
            .iter()
            .map(|t| t.iter().map(ColumnType::of).collect::<Result<Vec<_>>>())
            .collect::<Result<Vec<_>>>()?;
        let totals = fold_sketch(pinned, schema, &spec)?;
        out.push(Folded {
            spec,
            totals,
            types,
        });
    }
    Ok(out)
}

/// The exact distinct count of `columns` over live row groups, when a
/// complete `exact` key spec (with or without per-key counts) on exactly
/// those columns holds it; `None` otherwise.
pub(crate) fn exact_distinct(pinned: &Pinned, columns: &[String]) -> Result<Option<u64>> {
    let (complete, _) = pinned.specs();
    let Some(spec) = complete
        .iter()
        .find(|s| matches!(s, KeySpec::Exact { columns: c, .. } if c.as_slice() == columns))
    else {
        return Ok(None);
    };
    let m = pinned.manifest();
    let schema = pinned.planner.schema();
    let name = spec.name();
    let readers = m
        .key_spec_segments(&name)
        .map(|s| SegmentReader::open_for_dataset(pinned.snapshot.read_segment(&s.id)?, schema))
        .collect::<Result<Vec<_>>>()?;
    let live: RoaringBitmap = m
        .rowgroup_space
        .files
        .iter()
        .flat_map(|f| f.rowgroup_ids.iter().map(|id| id.0))
        .collect();
    Ok(Some(bellwether_core::count_live_keys(&readers, &live)?))
}

/// The complete sketch holding `label` with the highest precision (then the
/// first declared), and the item's index in it.
fn best_index(folded: &[Folded], label: &str) -> Option<(usize, usize)> {
    let mut best: Option<(usize, usize)> = None;
    for (fi, f) in folded.iter().enumerate() {
        if let Some(i) = f.totals.find(label) {
            if best.is_none_or(|(b, _)| f.spec.precision > folded[b].spec.precision) {
                best = Some((fi, i));
            }
        }
    }
    best
}

pub(crate) fn best_sketch<'a>(folded: &'a [Folded], label: &str) -> Option<(&'a Folded, usize)> {
    best_index(folded, label).map(|(f, i)| (&folded[f], i))
}

/// Folds `spec`'s committed segments into one total per file: a file's
/// entries (one per stretch of its row groups, possibly in several
/// segments) merge into the file's sketch, which is folded once every live
/// row group of the file is covered. Entries of retired row groups are
/// skipped. Holds only files whose stretches span segments.
fn fold_sketch(pinned: &Pinned, schema: &SchemaRef, spec: &SketchSpec) -> Result<SketchTotals> {
    let m = pinned.manifest();
    let name = spec.name();
    let mut file_of: HashMap<u32, usize> = HashMap::new();
    let mut need: Vec<usize> = Vec::with_capacity(m.rowgroup_space.files.len());
    for (fi, f) in m.rowgroup_space.files.iter().enumerate() {
        for id in &f.rowgroup_ids {
            file_of.insert(id.0, fi);
        }
        need.push(f.rowgroup_ids.len());
    }
    let mut segs: Vec<&SegmentRef> = m.sidecar_segments(SKETCH_KIND, &name).collect();
    segs.sort_by_key(|s| (s.rowgroup_range.start, s.id));
    let corrupt =
        |what: String| Error::Other(format!("sketch {name}: {what}; re-index the folder"));
    let mut totals = SketchTotals::new(spec)?;
    let mut partial: BTreeMap<usize, (Vec<ItemSketch>, usize)> = BTreeMap::new();
    for seg in segs {
        let bytes = pinned.snapshot.read_segment(&seg.id)?;
        let r = SketchReader::open_for_dataset(&bytes, schema)?;
        if r.spec() != *spec {
            return Err(corrupt(format!(
                "segment {} records {}, not the declared {}",
                seg.id,
                r.spec().label(),
                spec.label()
            )));
        }
        if r.rowgroup_range() != (seg.rowgroup_range.start.0, seg.rowgroup_range.end.0) {
            return Err(corrupt(format!(
                "segment {} records other row groups than its manifest entry",
                seg.id
            )));
        }
        for i in 0..r.len() {
            let e = r.entry(i)?;
            let owners: Vec<Option<usize>> = e
                .rowgroups
                .clone()
                .map(|id| file_of.get(&id).copied())
                .collect();
            if owners.iter().all(Option::is_none) {
                continue;
            }
            let Some(Some(file)) = owners.first().copied() else {
                return Err(corrupt(format!(
                    "an entry covers row groups {:?}, some of them retired",
                    e.rowgroups
                )));
            };
            if owners.iter().any(|o| *o != Some(file)) {
                return Err(corrupt(format!(
                    "an entry covers row groups {:?} of more than one file",
                    e.rowgroups
                )));
            }
            let covered = e.rowgroups.len();
            let slot = partial.entry(file).or_insert_with(|| (Vec::new(), 0));
            if slot.0.is_empty() {
                slot.0 = e.items;
            } else {
                for (a, b) in slot.0.iter_mut().zip(&e.items) {
                    a.merge(b)?;
                }
            }
            slot.1 += covered;
            match slot.1.cmp(&need[file]) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => {
                    if let Some((items, _)) = partial.remove(&file) {
                        totals.add_file(&items)?;
                    }
                }
                std::cmp::Ordering::Greater => {
                    return Err(corrupt(format!(
                        "{} is covered more than once",
                        m.rowgroup_space.files[file].fingerprint.path
                    )))
                }
            }
        }
    }
    if let Some((&file, _)) = partial.iter().next() {
        return Err(corrupt(format!(
            "{} is only partly covered",
            m.rowgroup_space.files[file].fingerprint.path
        )));
    }
    Ok(totals)
}

/// A source that counts the data it reads: row groups through
/// `read_columns`, and the distinct files they belong to. Footer reads
/// (`row_groups`) are metadata and not counted. `why` reports these as the
/// data reads its diagnosis needed.
pub(crate) struct CountingSource<'a> {
    inner: &'a dyn Source,
    row_groups: AtomicU64,
    files: Mutex<std::collections::BTreeSet<String>>,
}

impl<'a> CountingSource<'a> {
    pub(crate) fn new(inner: &'a dyn Source) -> Self {
        Self {
            inner,
            row_groups: AtomicU64::new(0),
            files: Mutex::new(std::collections::BTreeSet::new()),
        }
    }

    /// `(files, row groups)` read so far.
    pub(crate) fn reads(&self) -> (u64, u64) {
        let files = self.files.lock().map_or(0, |f| f.len() as u64);
        (files, self.row_groups.load(Ordering::SeqCst))
    }
}

impl Source for CountingSource<'_> {
    fn discover(&self, root: &str) -> Result<Vec<FileFingerprint>> {
        self.inner.discover(root)
    }

    fn row_groups(&self, file: &FileFingerprint) -> Result<Vec<RowGroupMeta>> {
        self.inner.row_groups(file)
    }

    fn read_columns(
        &self,
        rg: &RowGroupMeta,
        columns: &[String],
    ) -> Result<arrow::array::RecordBatch> {
        let batch = self.inner.read_columns(rg, columns)?;
        self.row_groups.fetch_add(1, Ordering::SeqCst);
        if let Ok(mut f) = self.files.lock() {
            f.insert(rg.file.path.clone());
        }
        Ok(batch)
    }

    fn schema(&self, root: &str) -> Result<SchemaRef> {
        self.inner.schema(root)
    }
}
