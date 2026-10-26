//! Writing segments in parallel (ADR 003, ADR 018).
//!
//! The row groups to index are split into work items: runs of consecutive
//! live row-group ids that need the same set of key specs, cut at file
//! boundaries where possible and at `max_segment_row_groups`, independently
//! of the thread count. A worker takes one item, reads each row group's key
//! columns once (the union over the item's specs), feeds them to one open
//! segment writer per spec, and drops the batch before reading the next.
//! A writer is finished and uploaded as soon as the next row group would
//! take it past the cap. Each finished segment records the half-open range
//! of ids its writer processed: that range, not the ids that happened to
//! produce keys, is the segment's coverage.

use std::collections::HashSet;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Schema};
use bellwether_core::encoding::{self, KeyColumns};
use bellwether_core::sidecar::{self, SidecarWriter};
use bellwether_core::sketch::{self as sketch_seg, SketchWriter};
use bellwether_core::{SegmentWriter, WriterOptions};
use bellwether_store::{segment_path, Extra, RowGroupRange, SegmentRef, StoreExt};
use bellwether_types::{Error, FileFingerprint, KeySpec, Result, RowGroupId, RowGroupMeta};
use bytes::Bytes;
use rayon::prelude::*;
use roaring::RoaringBitmap;

use crate::config::BuildObserver;
use crate::sources::DataSource;
use crate::target::Target;

/// A tracked file as the build sees it after id assignment.
#[derive(Clone, Debug)]
pub(crate) struct FileRow {
    pub(crate) fingerprint: FileFingerprint,
    pub(crate) ids: Vec<RowGroupId>,
    pub(crate) chunk_size: Option<u64>,
    pub(crate) chunks: Option<Vec<Range<u64>>>,
}

/// A declared key spec or sidecar with its dataset types and the columns a
/// worker reads for it.
#[derive(Clone, Debug)]
pub(crate) struct SpecPlan {
    pub(crate) target: Target,
    pub(crate) name: String,
    pub(crate) key_types: Vec<DataType>,
    /// A temporal key spec's timestamp type.
    pub(crate) ts_type: Option<DataType>,
    /// A sidecar's time column types.
    pub(crate) time_types: Vec<DataType>,
    pub(crate) columns: Vec<String>,
}

/// One row group to read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Task {
    pub(crate) id: u32,
    pub(crate) file: usize,
    pub(crate) index: u32,
}

/// One unit of parallel work: consecutive live ids that need the specs in
/// `mask` (bit `s` = spec `s`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Item {
    pub(crate) mask: u64,
    pub(crate) tasks: Vec<Task>,
}

/// Splits the live row groups of `files` into work items. `needed[s]` holds
/// the ids spec `s` must index in this build.
pub(crate) fn items(
    files: &[FileRow],
    needed: &[RoaringBitmap],
    max_rgs: usize,
) -> Result<Vec<Item>> {
    let max_rgs = max_rgs.max(1);
    let mut tasks = Vec::new();
    for (fi, f) in files.iter().enumerate() {
        for (idx, id) in f.ids.iter().enumerate() {
            let index = u32::try_from(idx)
                .map_err(|_| Error::Other("too many row groups in one file".into()))?;
            tasks.push(Task {
                id: id.0,
                file: fi,
                index,
            });
        }
    }
    tasks.sort_unstable_by_key(|t| t.id);
    let mask_of = |id: u32| {
        needed
            .iter()
            .enumerate()
            .filter(|(_, n)| n.contains(id))
            .fold(0u64, |m, (s, _)| m | (1 << s))
    };
    let mut out = Vec::new();
    let mut run: Vec<Task> = Vec::new();
    let mut run_mask = 0u64;
    for t in tasks {
        let mask = mask_of(t.id);
        if mask != run_mask && !run.is_empty() {
            split_run(&std::mem::take(&mut run), run_mask, max_rgs, &mut out);
        }
        run_mask = mask;
        if mask != 0 {
            run.push(t);
        }
    }
    if !run.is_empty() {
        split_run(&run, run_mask, max_rgs, &mut out);
    }
    Ok(out)
}

/// Cuts one run into items of at most `max_rgs` row groups, keeping each
/// file's stretch of row groups in one item when it fits.
fn split_run(run: &[Task], mask: u64, max_rgs: usize, out: &mut Vec<Item>) {
    let mut current: Vec<Task> = Vec::new();
    let mut i = 0;
    while i < run.len() {
        let file = run[i].file;
        let end = run[i..]
            .iter()
            .position(|t| t.file != file)
            .map_or(run.len(), |p| i + p);
        let stretch = &run[i..end];
        if !current.is_empty() && current.len() + stretch.len() > max_rgs {
            out.push(Item {
                mask,
                tasks: std::mem::take(&mut current),
            });
        }
        for chunk in stretch.chunks(max_rgs) {
            if current.len() + chunk.len() > max_rgs {
                out.push(Item {
                    mask,
                    tasks: std::mem::take(&mut current),
                });
            }
            current.extend_from_slice(chunk);
        }
        i = end;
    }
    if !current.is_empty() {
        out.push(Item {
            mask,
            tasks: current,
        });
    }
}

/// Everything a worker needs, shared across the pool.
pub(crate) struct Ctx<'a, S: StoreExt + ?Sized> {
    pub(crate) store: &'a S,
    pub(crate) index_root: &'a str,
    pub(crate) sources: &'a DataSource,
    pub(crate) schema: &'a Schema,
    pub(crate) files: &'a [FileRow],
    pub(crate) specs: &'a [SpecPlan],
    pub(crate) max_keys: u64,
    pub(crate) max_bytes: u64,
    pub(crate) observer: Option<&'a dyn BuildObserver>,
    pub(crate) held: AtomicU64,
    pub(crate) peak_held: AtomicU64,
    pub(crate) peak_keys: AtomicU64,
}

/// What the workers wrote.
#[derive(Debug, Default)]
pub(crate) struct Written {
    pub(crate) segments: Vec<SegmentRef>,
    pub(crate) row_groups_read: u64,
    pub(crate) files_listed: u64,
}

/// Runs every item on a pool of `threads` workers. Each finished segment is
/// uploaded by the worker that wrote it; only its manifest entry is kept.
pub(crate) fn write_all<S: StoreExt + ?Sized>(
    ctx: &Ctx<'_, S>,
    items: &[Item],
    threads: usize,
) -> Result<Written> {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .map_err(|e| Error::Other(format!("worker pool: {e}")))?;
    let outs = pool.install(|| {
        items
            .par_iter()
            .map(|item| process_item(ctx, item))
            .collect::<Result<Vec<_>>>()
    })?;
    let mut written = Written::default();
    for out in outs {
        written.segments.extend(out.segments);
        written.row_groups_read += out.row_groups_read;
        written.files_listed += out.files_listed;
    }
    written.segments.sort_by(|a, b| {
        (&a.key_spec, a.rowgroup_range.start).cmp(&(&b.key_spec, b.rowgroup_range.start))
    });
    Ok(written)
}

/// Counts a row group held by a worker for as long as the guard lives.
struct Held<'a>(&'a AtomicU64);

impl Drop for Held<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl<S: StoreExt + ?Sized> Ctx<'_, S> {
    fn hold(&self) -> Held<'_> {
        let now = self.held.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak_held.fetch_max(now, Ordering::SeqCst);
        Held(&self.held)
    }
}

/// The current file's row groups, as the source reports them, checked
/// against the manifest's record of the file.
struct CurrentFile {
    file: usize,
    metas: Vec<RowGroupMeta>,
}

fn list_file<S: StoreExt + ?Sized>(ctx: &Ctx<'_, S>, fi: usize) -> Result<CurrentFile> {
    let row = &ctx.files[fi];
    let metas = ctx.sources.row_groups(&row.fingerprint, row.chunk_size)?;
    if let Some(o) = ctx.observer {
        o.row_groups_listed(&row.fingerprint);
    }
    if metas.len() != row.ids.len() {
        return Err(Error::Source(format!(
            "{}: the source reports {} row groups, but the manifest assigned {} ids; the file's layout changed",
            row.fingerprint.path,
            metas.len(),
            row.ids.len()
        )));
    }
    if let Some(chunks) = &row.chunks {
        for (meta, chunk) in metas.iter().zip(chunks) {
            if meta.byte_range != Some((chunk.start, chunk.end)) {
                return Err(Error::Source(format!(
                    "{}: chunk {} spans {:?} at chunk size {:?}, but the manifest records {chunk:?}",
                    row.fingerprint.path, meta.index_in_file, meta.byte_range, row.chunk_size
                )));
            }
        }
    }
    Ok(CurrentFile { file: fi, metas })
}

#[derive(Default)]
struct ItemOut {
    segments: Vec<SegmentRef>,
    row_groups_read: u64,
    files_listed: u64,
}

fn process_item<S: StoreExt + ?Sized>(ctx: &Ctx<'_, S>, item: &Item) -> Result<ItemOut> {
    let specs: Vec<&SpecPlan> = ctx
        .specs
        .iter()
        .enumerate()
        .filter(|(s, _)| item.mask & (1 << s) != 0)
        .map(|(_, p)| p)
        .collect();
    let mut columns: Vec<String> = Vec::new();
    for p in &specs {
        for c in &p.columns {
            if !columns.contains(c) {
                columns.push(c.clone());
            }
        }
    }
    let mut writers: Vec<SpecWriter<'_>> = specs.iter().map(|p| SpecWriter::new(p)).collect();
    let mut out = ItemOut::default();
    let mut current: Option<CurrentFile> = None;
    for task in &item.tasks {
        if current.as_ref().is_none_or(|c| c.file != task.file) {
            current = Some(list_file(ctx, task.file)?);
            out.files_listed += 1;
        }
        let Some(cur) = current.as_ref() else {
            return Err(Error::Other("no current file".into()));
        };
        let row = &ctx.files[task.file];
        let index =
            usize::try_from(task.index).map_err(|_| Error::Other("row group index".into()))?;
        let mut meta = cur.metas[index].clone();
        meta.id = Some(RowGroupId(task.id));
        if let Some(chunks) = &row.chunks {
            // The manifest's range, so the source compares both ends of the
            // chunk it recomputes against the recorded one.
            meta.byte_range = Some((chunks[index].start, chunks[index].end));
        }
        let _held = ctx.hold();
        let batch = ctx.sources.read_columns(&meta, &columns, row.chunk_size)?;
        out.row_groups_read += 1;
        if let Some(o) = ctx.observer {
            o.row_group_read(RowGroupId(task.id), &columns, &batch);
        }
        for w in &mut writers {
            w.add(ctx, task.file, task.id, &batch)?;
        }
    }
    for w in writers {
        out.segments.extend(w.finish(ctx)?);
    }
    Ok(out)
}

/// An open segment writer: a key spec's, a sidecar's, or a sketch's.
enum AnyWriter {
    Key(SegmentWriter),
    Sidecar(SidecarWriter),
    Sketch(SketchWriter),
}

/// One spec's open segment within an item.
struct SpecWriter<'a> {
    plan: &'a SpecPlan,
    writer: Option<AnyWriter>,
    start: u32,
    end: u32,
    keys: u64,
    bytes: u64,
    done: Vec<SegmentRef>,
}

impl<'a> SpecWriter<'a> {
    fn new(plan: &'a SpecPlan) -> Self {
        Self {
            plan,
            writer: None,
            start: 0,
            end: 0,
            keys: 0,
            bytes: 0,
            done: Vec::new(),
        }
    }

    fn add<S: StoreExt + ?Sized>(
        &mut self,
        ctx: &Ctx<'_, S>,
        file: usize,
        id: u32,
        batch: &RecordBatch,
    ) -> Result<()> {
        let (keys, bytes) = match (&self.plan.target, &self.writer) {
            // A sketch grows by one entry per file stretch, at most
            // `max_entry_bytes` each: the bound is charged when one starts.
            (Target::Sketch(spec), w) => {
                let starts = match w {
                    Some(AnyWriter::Sketch(w)) => w.starts_entry(file as u64, RowGroupId(id)),
                    _ => true,
                };
                if starts {
                    (1, sketch_seg::max_entry_bytes(spec))
                } else {
                    (0, 0)
                }
            }
            _ => measure(self.plan, batch)?,
        };
        if self.writer.is_some()
            && (self.keys.saturating_add(keys) > ctx.max_keys
                || self.bytes.saturating_add(bytes) > ctx.max_bytes)
        {
            self.flush(ctx)?;
        }
        let writer = match &mut self.writer {
            Some(w) => w,
            slot @ None => {
                self.start = id;
                self.keys = 0;
                self.bytes = 0;
                slot.insert(match &self.plan.target {
                    Target::Key(spec) => AnyWriter::Key(SegmentWriter::for_dataset(
                        spec.clone(),
                        ctx.schema,
                        WriterOptions::default(),
                    )?),
                    Target::Sidecar(def) => {
                        AnyWriter::Sidecar(SidecarWriter::for_dataset(def.clone(), ctx.schema)?)
                    }
                    Target::Sketch(spec) => {
                        AnyWriter::Sketch(SketchWriter::for_dataset(spec, ctx.schema)?)
                    }
                })
            }
        };
        match writer {
            AnyWriter::Key(w) => w.add_batch(RowGroupId(id), batch)?,
            AnyWriter::Sidecar(w) => w.add_batch(RowGroupId(id), batch)?,
            AnyWriter::Sketch(w) => w.add_batch(file as u64, RowGroupId(id), batch)?,
        }
        self.end = id
            .checked_add(1)
            .ok_or_else(|| Error::Other("row-group id space exhausted".into()))?;
        self.keys = self.keys.saturating_add(keys);
        self.bytes = self.bytes.saturating_add(bytes);
        ctx.peak_keys.fetch_max(self.keys, Ordering::SeqCst);
        Ok(())
    }

    fn flush<S: StoreExt + ?Sized>(&mut self, ctx: &Ctx<'_, S>) -> Result<()> {
        let Some(writer) = self.writer.take() else {
            return Ok(());
        };
        // The segment records the row groups its writer processed, so its
        // bytes, and its id, differ from any other segment's even when none
        // of its row groups held a key; the manifest entry claims the same
        // range as coverage.
        let range = RowGroupId(self.start)..RowGroupId(self.end);
        let (id, body) = match writer {
            AnyWriter::Key(mut w) => {
                w.set_rowgroup_range(range)?;
                let seg = w.finish()?;
                (seg.id, seg.bytes)
            }
            AnyWriter::Sidecar(mut w) => {
                w.set_rowgroup_range(range)?;
                let seg = w.finish()?;
                (seg.id, seg.bytes)
            }
            AnyWriter::Sketch(mut w) => {
                w.set_rowgroup_range(range)?;
                let seg = w.finish()?;
                (seg.id, seg.bytes)
            }
        };
        let bytes = body.len() as u64;
        ctx.store
            .put_content_addressed(&segment_path(ctx.index_root, &id), Bytes::from(body))?;
        self.done.push(SegmentRef {
            id,
            key_spec: self.plan.name.clone(),
            rowgroup_range: RowGroupRange::new(self.start, self.end),
            bytes,
            kind: self.plan.target.kind().map(str::to_string),
            extra: Extra::new(),
        });
        Ok(())
    }

    fn finish<S: StoreExt + ?Sized>(mut self, ctx: &Ctx<'_, S>) -> Result<Vec<SegmentRef>> {
        self.flush(ctx)?;
        Ok(self.done)
    }
}

/// What adding `batch` can add to a spec's open segment, as an upper bound:
/// `(keys, bytes)`, counted per layout.
///
/// - exact: the batch's distinct key tuples and their canonical bytes;
/// - temporal: its distinct `(key, ts)` pairs, and the key bytes plus 8 per
///   pair;
/// - bloom: its distinct key tuples, and 8 bytes per tuple (the writer
///   holds one hash per key);
/// - range: one entry if any key is present, and twice the longest key;
/// - a sidecar: core's `sidecar::measure` (distinct keys, and key bytes
///   plus 20 per stored record).
pub(crate) fn measure(plan: &SpecPlan, batch: &RecordBatch) -> Result<(u64, u64)> {
    let spec = match &plan.target {
        Target::Key(spec) => spec,
        Target::Sidecar(def) => {
            return sidecar::measure(def, &plan.key_types, &plan.time_types, batch)
        }
        Target::Sketch(spec) => return Ok((1, sketch_seg::max_entry_bytes(spec))),
    };
    let column = |name: &str| -> Result<ArrayRef> {
        batch
            .column_by_name(name)
            .cloned()
            .ok_or_else(|| Error::KeySpec(format!("column {name} not in batch")))
    };
    let cols = spec
        .columns()
        .iter()
        .map(|c| column(c))
        .collect::<Result<Vec<_>>>()?;
    let keys = KeyColumns::new(&cols, &plan.key_types)?;
    let ts = match (spec, &plan.ts_type) {
        (KeySpec::Temporal { ts_column, .. }, Some(dt)) => Some((column(ts_column)?, dt)),
        _ => None,
    };
    let mut buf = Vec::new();
    if matches!(spec, KeySpec::Range { .. }) {
        let mut longest = None;
        for row in 0..batch.num_rows() {
            if keys.encode_into(row, &mut buf)? {
                longest = longest.max(Some(buf.len() as u64));
            }
        }
        return Ok(longest.map_or((0, 0), |l| (1, 2 * l)));
    }
    let mut seen: HashSet<Vec<u8>> = HashSet::new();
    let mut bytes = 0u64;
    for row in 0..batch.num_rows() {
        if !keys.encode_into(row, &mut buf)? {
            continue;
        }
        let key_len = buf.len() as u64;
        if let Some((arr, dt)) = &ts {
            match encoding::ts_value_as(arr.as_ref(), row, dt)? {
                Some(t) => {
                    buf.push(1);
                    buf.extend_from_slice(&t.to_be_bytes());
                }
                None => buf.push(0),
            }
        }
        if !seen.contains(buf.as_slice()) {
            seen.insert(buf.clone());
            bytes += match spec {
                KeySpec::Bloom { .. } => 8,
                KeySpec::Temporal { .. } => key_len + 8,
                _ => key_len,
            };
        }
    }
    Ok((seen.len() as u64, bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray, TimestampMicrosecondArray};
    use arrow::datatypes::{Field, TimeUnit};
    use bellwether_types::Hash;

    fn row(path: &str, ids: std::ops::Range<u32>) -> FileRow {
        FileRow {
            fingerprint: FileFingerprint {
                path: path.into(),
                size: 1,
                etag: None,
                tail_blake3: Hash::of(path.as_bytes()),
            },
            ids: ids.map(RowGroupId).collect(),
            chunk_size: None,
            chunks: None,
        }
    }

    fn all(n: u32) -> RoaringBitmap {
        (0..n).collect()
    }

    fn ids(item: &Item) -> Vec<u32> {
        item.tasks.iter().map(|t| t.id).collect()
    }

    #[test]
    fn items_follow_files_and_the_size_limit() {
        // files of 3, 2, 5 row groups; limit 4
        let files = vec![row("a", 0..3), row("b", 3..5), row("c", 5..10)];
        let items = items(&files, &[all(10)], 4).unwrap();
        let got: Vec<Vec<u32>> = items.iter().map(ids).collect();
        assert_eq!(
            got,
            vec![vec![0, 1, 2], vec![3, 4], vec![5, 6, 7, 8], vec![9]]
        );
        assert!(items.iter().all(|i| i.mask == 1));
    }

    #[test]
    fn items_split_where_the_needed_specs_change_and_skip_unneeded_ids() {
        // spec 0 needs everything; spec 1 only the new file's ids 6..9
        let files = vec![row("old", 0..6), row("new", 6..9)];
        let mut new_ids = RoaringBitmap::new();
        new_ids.insert_range(6..9);
        let items = items(&files, &[all(9), new_ids.clone()], 100).unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!((items[0].mask, ids(&items[0])), (0b01, (0..6).collect()));
        assert_eq!((items[1].mask, ids(&items[1])), (0b11, (6..9).collect()));
        // nothing needed for the old file at all
        let items = super::items(&files, &[new_ids], 100).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(ids(&items[0]), vec![6, 7, 8]);
    }

    #[test]
    fn items_never_span_a_live_id_they_do_not_index() {
        // a gap at 4 that only spec 1 needs splits spec 0's run
        let files = vec![row("a", 0..8)];
        let mut s0 = all(8);
        s0.remove(4);
        let items = items(&files, &[s0, (4..5).collect()], 100).unwrap();
        let got: Vec<(u64, Vec<u32>)> = items.iter().map(|i| (i.mask, ids(i))).collect();
        assert_eq!(
            got,
            vec![(1, vec![0, 1, 2, 3]), (2, vec![4]), (1, vec![5, 6, 7])]
        );
    }

    #[test]
    fn items_order_by_id_across_files_assigned_out_of_path_order() {
        // b's ids precede a's (b was tracked first, a was rewritten later);
        // b's tail and all of a share an item because a fits whole
        let files = vec![row("a", 4..6), row("b", 0..4)];
        let items = items(&files, &[all(6)], 3).unwrap();
        let got: Vec<Vec<u32>> = items.iter().map(ids).collect();
        assert_eq!(got, vec![vec![0, 1, 2], vec![3, 4, 5]]);
        assert_eq!(items[1].tasks[0].file, 1);
        assert_eq!(items[1].tasks[1].file, 0);
    }

    fn plan_for(spec: KeySpec, schema: &Schema) -> SpecPlan {
        let key_types = spec
            .columns()
            .iter()
            .map(|c| schema.field_with_name(c).unwrap().data_type().clone())
            .collect();
        let (ts_type, mut columns) = (None, spec.columns().to_vec());
        let ts_type = match &spec {
            KeySpec::Temporal { ts_column, .. } => {
                columns.push(ts_column.clone());
                Some(
                    schema
                        .field_with_name(ts_column)
                        .unwrap()
                        .data_type()
                        .clone(),
                )
            }
            _ => ts_type,
        };
        SpecPlan {
            name: spec.name(),
            target: Target::Key(spec),
            key_types,
            ts_type,
            time_types: Vec::new(),
            columns,
        }
    }

    #[test]
    fn measure_counts_distinct_keys_per_layout() {
        let ts_type = DataType::Timestamp(TimeUnit::Microsecond, None);
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("ts", ts_type, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(1),
                    Some(2),
                    None,
                    Some(2),
                ])),
                Arc::new(StringArray::from(vec!["a", "a", "bb", "c", "bb"])),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(1),
                    Some(2),
                    Some(1),
                    Some(1),
                    Some(1),
                ])),
            ],
        )
        .unwrap();
        let exact = plan_for(
            KeySpec::Exact {
                columns: vec!["id".into()],
            },
            &schema,
        );
        assert_eq!(measure(&exact, &batch).unwrap(), (2, 18));
        let temporal = plan_for(
            KeySpec::Temporal {
                columns: vec!["id".into()],
                ts_column: "ts".into(),
            },
            &schema,
        );
        // (1,1) (1,2) (2,1)
        assert_eq!(measure(&temporal, &batch).unwrap(), (3, 3 * 17));
        let bloom = plan_for(
            KeySpec::Bloom {
                columns: vec!["s".into()],
                fpr: 0.01,
            },
            &schema,
        );
        assert_eq!(measure(&bloom, &batch).unwrap(), (3, 24));
        let range = plan_for(KeySpec::Range { column: "s".into() }, &schema);
        // longest key "bb" = 06 62 62 00 00
        assert_eq!(measure(&range, &batch).unwrap(), (1, 10));
        let composite = plan_for(
            KeySpec::Exact {
                columns: vec!["id".into(), "s".into()],
            },
            &schema,
        );
        // (1,a) (2,bb); the null-led row is not indexed
        assert_eq!(measure(&composite, &batch).unwrap(), (2, 9 + 4 + 9 + 5));
    }
}
