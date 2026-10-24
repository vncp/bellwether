//! Layout proposals: costs under a **stated model** of a file layout that
//! does not exist yet. Every such cost has `RunCost::exact == false`, and the
//! model is named in full ([`LayoutModel::describe`]); nothing is silently
//! estimated (ADR 005).
//!
//! # Model, version 1
//!
//! - The dataset's rows are sorted by the `cluster_by` columns, ascending
//!   in canonical byte order (ADR 016), nulls first, ties kept in current
//!   order (files in catalog order, row groups in id order, rows in order).
//! - The sorted rows are re-packed into row groups of `rows_per_row_group`
//!   rows (the last may be short), and the row groups into files of
//!   `row_groups_per_file`. The defaults keep the dataset's current mean
//!   sizes ([`LayoutModel::defaults`]); both are knobs for exact
//!   finite-difference descent (ADR 022).
//! - A row group's bytes are its pro-rata share, by rows, of the dataset's
//!   total compressed bytes: `floor(T * end / R) - floor(T * start / R)` for
//!   rows `[start, end)` of `R`, so the shares sum to `T` exactly. This is the
//!   assumption that makes the model a model: compressed size depends on the
//!   data, and the new layout's has not been measured.
//! - A run reads exactly the row groups that hold one of its keys in the new
//!   layout, which is what an exact index on the run's key columns selects.
//!
//! Under those statements every number is computed exactly: the new row
//! group of each row comes from the sort, and a run's row groups come from
//! an exact transient segment over the modeled row-group ids.
//!
//! # Memory (ADR 018)
//!
//! Two passes over the key and cluster columns, one row group at a time.
//! Held: a map of the distinct cluster keys, each charged its bytes plus
//! [`CLUSTER_KEY_OVERHEAD`] and capped at
//! [`TransientLimits::max_cluster_key_bytes`] (past it, a
//! [`ReplayError::ClusterKeysExceeded`]; a partitioned sort is a later
//! addition); one row group's keys per distinct run key-column set; one open
//! segment writer per set, cut by [`TransientLimits::max_segment_rows`] and
//! [`TransientLimits::max_segment_key_bytes`]; and the finished segments,
//! whose bytes plus the open writers' accounted key bytes stay within
//! [`TransientLimits::max_index_bytes`].

use std::collections::{BTreeMap, HashSet};

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Schema};
use bellwether_core::encoding::{self, KeyColumns};
use bellwether_core::segment::{ColumnType, SegmentReader, SegmentWriter, WriterOptions};
use bellwether_types::{Error, KeySpec, RowGroupId, RunCost, Source};
use bytes::Bytes;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use crate::catalog::Catalog;
use crate::error::{ReplayError, ReplayResult};
use crate::plan::SegmentBytes;
use crate::transient::{TransientLimits, CLUSTER_KEY_OVERHEAD};
use crate::workload::{KeySetSource, Workload};

/// Version of the layout model this module implements.
pub const LAYOUT_MODEL_VERSION: u32 = 1;

/// The layout a proposal is costed under (module docs).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LayoutModel {
    /// Columns rows are sorted by, in priority order.
    pub cluster_by: Vec<String>,
    /// Rows per row group in the new layout.
    pub rows_per_row_group: u64,
    /// Row groups per file in the new layout.
    pub row_groups_per_file: u64,
}

impl LayoutModel {
    /// The model with the dataset's current mean sizes: rows per row group
    /// `ceil(rows / row groups)`, row groups per file `ceil(row groups /
    /// files)`.
    #[must_use]
    pub fn defaults(cluster_by: &[String], catalog: &Catalog) -> Self {
        let row_groups = catalog.len() as u64;
        let files = {
            let mut seen: Vec<usize> = catalog.row_groups().iter().map(|r| r.file).collect();
            seen.sort_unstable();
            seen.dedup();
            seen.len() as u64
        };
        Self {
            cluster_by: cluster_by.to_vec(),
            rows_per_row_group: catalog.rows_total().div_ceil(row_groups.max(1)).max(1),
            row_groups_per_file: row_groups.div_ceil(files.max(1)).max(1),
        }
    }

    /// The model, stated in full for a dataset of `rows` rows and `bytes`
    /// compressed bytes.
    #[must_use]
    pub fn describe(&self, rows: u64, bytes: u64) -> String {
        let row_groups = rows.div_ceil(self.rows_per_row_group);
        format!(
            "layout model v{LAYOUT_MODEL_VERSION}: the dataset's {rows} rows sorted by ({}) \
             ascending, nulls first, ties in current order, re-packed into {row_groups} row \
             groups of {} rows and files of {} row groups ({} files); each row group's bytes are \
             its pro-rata share by rows of the dataset's {bytes} compressed bytes (assumed, not \
             measured); a run reads exactly the row groups holding its keys",
            self.cluster_by.join(", "),
            self.rows_per_row_group,
            self.row_groups_per_file,
            row_groups.div_ceil(self.row_groups_per_file),
        )
    }
}

/// Run costs under a layout model.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LayoutReplay {
    /// The model.
    pub model: LayoutModel,
    /// [`LayoutModel::describe`] for this dataset.
    pub description: String,
    /// One cost per workload run, in workload order, each `exact: false`.
    pub costs: Vec<RunCost>,
}

/// One distinct run key-column set and its modeled segments.
struct KeySet {
    columns: Vec<String>,
    declared: Vec<DataType>,
    writer: SegmentWriter,
    open_rows: u64,
    open_bytes: u64,
    /// First and last modeled row group the open writer's rows went to,
    /// rows with a null key included.
    open_range: Option<(u32, u32)>,
    readers: Vec<SegmentReader<SegmentBytes>>,
}

/// Costs every run of `workload` under `model` (module docs), over the
/// catalog's dataset schema.
pub fn replay_layout(
    source: &dyn Source,
    catalog: &Catalog,
    model: &LayoutModel,
    workload: &Workload,
    keys: &dyn KeySetSource,
    limits: &TransientLimits,
) -> ReplayResult<LayoutReplay> {
    if model.cluster_by.is_empty()
        || model.rows_per_row_group == 0
        || model.row_groups_per_file == 0
    {
        return Err(
            Error::Other("a layout model needs cluster columns and positive sizes".into()).into(),
        );
    }
    if limits.max_segment_rows == 0 || limits.max_segment_key_bytes == 0 {
        return Err(Error::Other("segment caps must be positive".into()).into());
    }
    crate::keys::check_distinct_columns(&model.cluster_by)?;
    let schema = catalog.schema();
    let cluster_types = declared_types(&model.cluster_by, schema)?;
    let (mut next, rows_total) =
        cluster_positions(source, catalog, &model.cluster_by, &cluster_types, limits)?;
    let row_groups = rows_total.div_ceil(model.rows_per_row_group);
    if row_groups > u64::from(u32::MAX) {
        return Err(Error::Other(format!(
            "the modeled layout has {row_groups} row groups, more than an id can name"
        ))
        .into());
    }
    let (mut sets, set_of_run) = key_sets(workload, schema)?;
    index_modeled(
        source,
        catalog,
        model,
        &cluster_types,
        &mut next,
        &mut sets,
        limits,
    )?;
    let bytes_total = catalog.bytes_total();
    let costs = modeled_costs(
        model,
        rows_total,
        bytes_total,
        &sets,
        &set_of_run,
        workload,
        keys,
    )?;
    Ok(LayoutReplay {
        model: model.clone(),
        description: model.describe(rows_total, bytes_total),
        costs,
    })
}

/// Pass 1: the rows per distinct cluster key, turned into each key's first
/// position in sorted order, and the total row count. The map is capped
/// (module docs).
fn cluster_positions(
    source: &dyn Source,
    catalog: &Catalog,
    cluster_by: &[String],
    cluster_types: &[DataType],
    limits: &TransientLimits,
) -> ReplayResult<(BTreeMap<Vec<u8>, u64>, u64)> {
    let mut next: BTreeMap<Vec<u8>, u64> = BTreeMap::new();
    let mut accounted = 0u64;
    let mut rows_total = 0u64;
    for rg in catalog.row_groups() {
        let batch = source.read_columns(&rg.meta, cluster_by)?;
        check_rows(rg.meta.rows, batch.num_rows(), &rg.meta.file.path)?;
        let cols = columns_of(&batch, cluster_by, cluster_types)?;
        for row in 0..batch.num_rows() {
            let k = cluster_key(&cols, cluster_types, row)?;
            if let Some(n) = next.get_mut(&k) {
                *n += 1;
                continue;
            }
            accounted += k.len() as u64 + CLUSTER_KEY_OVERHEAD;
            if accounted > limits.max_cluster_key_bytes {
                return Err(ReplayError::ClusterKeysExceeded {
                    keys: next.len() as u64 + 1,
                    bytes: accounted,
                    cap: limits.max_cluster_key_bytes,
                });
            }
            next.insert(k, 1);
        }
        rows_total += batch.num_rows() as u64;
    }
    let mut at = 0u64;
    for v in next.values_mut() {
        let count = *v;
        *v = at;
        at += count;
    }
    Ok((next, rows_total))
}

/// The distinct key-column sets the workload's runs use, and each run's set.
fn key_sets(workload: &Workload, schema: &Schema) -> ReplayResult<(Vec<KeySet>, Vec<usize>)> {
    let mut sets: Vec<KeySet> = Vec::new();
    let mut set_of_run = Vec::with_capacity(workload.runs().len());
    for run in workload.runs() {
        let declared = declared_types(&run.columns, schema)?;
        check_recorded(&run.columns, &run.key_types, &declared, &run.run_id)?;
        if let Some(i) = sets.iter().position(|s| s.columns == run.columns) {
            set_of_run.push(i);
        } else {
            sets.push(KeySet {
                columns: run.columns.clone(),
                declared,
                writer: exact_writer(&run.columns)?,
                open_rows: 0,
                open_bytes: 0,
                open_range: None,
                readers: Vec::new(),
            });
            set_of_run.push(sets.len() - 1);
        }
    }
    Ok((sets, set_of_run))
}

/// Pass 2: each row's modeled row group (consuming `next`), indexed per
/// key-column set, one row group's keys at a time.
fn index_modeled(
    source: &dyn Source,
    catalog: &Catalog,
    model: &LayoutModel,
    cluster_types: &[DataType],
    next: &mut BTreeMap<Vec<u8>, u64>,
    sets: &mut [KeySet],
    limits: &TransientLimits,
) -> ReplayResult<()> {
    let mut read_cols: Vec<String> = model.cluster_by.clone();
    for s in sets.iter() {
        for c in &s.columns {
            if !read_cols.contains(c) {
                read_cols.push(c.clone());
            }
        }
    }
    let mut finished_bytes = 0u64;
    // Accounted key bytes of every set's open writer.
    let mut open_total = 0u64;
    let mut buf = Vec::new();
    for rg in catalog.row_groups() {
        let batch = source.read_columns(&rg.meta, &read_cols)?;
        check_rows(rg.meta.rows, batch.num_rows(), &rg.meta.file.path)?;
        let cols = columns_of(&batch, &model.cluster_by, cluster_types)?;
        let mut targets = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let k = cluster_key(&cols, cluster_types, row)?;
            let pos = next
                .get_mut(&k)
                .ok_or_else(|| Error::Other("a cluster key changed between passes".into()))?;
            let rg_id = u32::try_from(*pos / model.rows_per_row_group)
                .map_err(|_| Error::Other("modeled row-group id overflows".into()))?;
            *pos += 1;
            targets.push(RowGroupId(rg_id));
        }
        let rows = batch.num_rows() as u64;
        let hull = targets
            .iter()
            .map(|t| t.0)
            .fold(None, |h: Option<(u32, u32)>, t| {
                Some(h.map_or((t, t), |(lo, hi)| (lo.min(t), hi.max(t))))
            });
        for set in sets.iter_mut() {
            let arrays = columns_of(&batch, &set.columns, &set.declared)?;
            let kc = KeyColumns::new(&arrays, &set.declared)?;
            let mut staged = Vec::with_capacity(targets.len());
            let mut distinct: HashSet<Vec<u8>> = HashSet::new();
            let mut bytes = 0u64;
            for (row, target) in targets.iter().enumerate() {
                if kc.encode_into(row, &mut buf)? {
                    if distinct.insert(buf.clone()) {
                        bytes += buf.len() as u64;
                    }
                    staged.push((buf.clone(), *target));
                }
            }
            drop(distinct);
            if set.open_rows > 0
                && (set.open_rows + rows > limits.max_segment_rows
                    || set.open_bytes + bytes > limits.max_segment_key_bytes)
            {
                open_total -= set.open_bytes;
                finish_set(set, &mut finished_bytes, limits)?;
            }
            let held = finished_bytes
                .saturating_add(open_total)
                .saturating_add(bytes);
            if held > limits.max_index_bytes {
                return Err(Error::Other(format!(
                    "the modeled layout's segments would hold {held} bytes (finished {finished_bytes}, \
                     open writers {open_total}, next row group {bytes}), past the {} byte cap",
                    limits.max_index_bytes
                ))
                .into());
            }
            for (key, target) in &staged {
                set.writer.insert(key, *target)?;
            }
            if let Some((lo, hi)) = hull {
                set.open_range = Some(
                    set.open_range
                        .map_or((lo, hi), |(a, b)| (a.min(lo), b.max(hi))),
                );
            }
            set.open_rows += rows;
            set.open_bytes += bytes;
            open_total += bytes;
        }
    }
    for set in sets.iter_mut() {
        if set.open_rows > 0 || set.readers.is_empty() {
            finish_set(set, &mut finished_bytes, limits)?;
        }
    }
    Ok(())
}

/// Every run's cost under the model: exact sums of the modeled statistics.
fn modeled_costs(
    model: &LayoutModel,
    rows_total: u64,
    bytes_total: u64,
    sets: &[KeySet],
    set_of_run: &[usize],
    workload: &Workload,
    keys: &dyn KeySetSource,
) -> ReplayResult<Vec<RunCost>> {
    let rg_bytes = |i: u64| -> u64 {
        let start = i * model.rows_per_row_group;
        let end = (start + model.rows_per_row_group).min(rows_total);
        let share = |r: u64| {
            u64::try_from(u128::from(bytes_total) * u128::from(r) / u128::from(rows_total.max(1)))
                .unwrap_or(u64::MAX)
        };
        share(end) - share(start)
    };
    let mut costs = Vec::with_capacity(workload.runs().len());
    for (run, &si) in workload.runs().iter().zip(set_of_run) {
        let rk = workload.run_keys(keys, run)?;
        let set = &sets[si];
        let project: Vec<usize> = (0..rk.columns().len()).collect();
        let probes = rk.all_probes(&project);
        let mut selected = RoaringBitmap::new();
        if !probes.is_empty() {
            for r in &set.readers {
                selected |= r.lookup(&probes)?;
            }
        }
        let mut files: Vec<u64> = selected
            .iter()
            .map(|i| u64::from(i) / model.row_groups_per_file)
            .collect();
        files.dedup();
        let mut bytes = 0u64;
        for i in &selected {
            bytes = bytes
                .checked_add(rg_bytes(u64::from(i)))
                .ok_or_else(|| Error::Other("byte total overflows u64".into()))?;
        }
        costs.push(RunCost {
            run_id: run.run_id.clone(),
            keys: rk.len() as u64,
            files: files.len() as u64,
            row_groups: selected.len(),
            bytes,
            exact: false,
        });
    }
    Ok(costs)
}

fn exact_writer(columns: &[String]) -> ReplayResult<SegmentWriter> {
    Ok(SegmentWriter::new(
        KeySpec::Exact {
            columns: columns.to_vec(),
        },
        WriterOptions::default(),
    )?)
}

fn finish_set(
    set: &mut KeySet,
    finished_bytes: &mut u64,
    limits: &TransientLimits,
) -> ReplayResult<()> {
    let mut full = std::mem::replace(&mut set.writer, exact_writer(&set.columns)?);
    // Like a transient segment, a modeled one records the modeled row
    // groups its rows went to, keys or not.
    if let Some((lo, hi)) = set.open_range.take() {
        full.set_rowgroup_range(RowGroupId(lo)..RowGroupId(hi + 1))?;
    }
    let seg = full.finish()?;
    *finished_bytes += seg.bytes.len() as u64;
    if *finished_bytes > limits.max_index_bytes {
        return Err(Error::Other(format!(
            "the modeled layout's segments exceed the {} byte cap",
            limits.max_index_bytes
        ))
        .into());
    }
    set.readers
        .push(SegmentReader::open(SegmentBytes::Memory(Bytes::from(
            seg.bytes,
        )))?);
    set.open_rows = 0;
    set.open_bytes = 0;
    Ok(())
}

fn declared_types(columns: &[String], schema: &Schema) -> ReplayResult<Vec<DataType>> {
    Ok(columns
        .iter()
        .map(|c| {
            let dt = schema
                .field_with_name(c)
                .map_err(|_| encoding::missing_column(schema, c))?
                .data_type()
                .clone();
            encoding::check_supported(&dt)
                .map_err(|e| Error::KeySpec(format!("column {c:?}: {e}")))?;
            Ok(dt)
        })
        .collect::<bellwether_types::Result<Vec<_>>>()?)
}

fn check_recorded(
    columns: &[String],
    recorded: &[ColumnType],
    declared: &[DataType],
    run_id: &str,
) -> ReplayResult<()> {
    for ((c, r), d) in columns.iter().zip(recorded).zip(declared) {
        if &ColumnType::of(d)? != r {
            return Err(Error::KeySpec(format!(
                "run {run_id:?} recorded column {c:?} as {r}, but the dataset declares {d}"
            ))
            .into());
        }
    }
    Ok(())
}

fn columns_of(
    batch: &RecordBatch,
    columns: &[String],
    declared: &[DataType],
) -> ReplayResult<Vec<ArrayRef>> {
    Ok(columns
        .iter()
        .zip(declared)
        .map(|(c, d)| {
            let arr = batch
                .column_by_name(c)
                .ok_or_else(|| Error::Source(format!("read did not return column {c:?}")))?;
            encoding::check_convertible(arr.data_type(), d)?;
            Ok(ArrayRef::clone(arr))
        })
        .collect::<bellwether_types::Result<Vec<_>>>()?)
}

fn cluster_key(cols: &[ArrayRef], declared: &[DataType], row: usize) -> ReplayResult<Vec<u8>> {
    let mut k = Vec::new();
    for (arr, d) in cols.iter().zip(declared) {
        encoding::encode_nullable_as(arr.as_ref(), row, d, &mut k)?;
    }
    Ok(k)
}

fn check_rows(expected: u64, got: usize, path: &str) -> ReplayResult<()> {
    if got as u64 == expected {
        Ok(())
    } else {
        Err(Error::Source(format!(
            "{path}: a read returned {got} rows but the statistics say {expected}"
        ))
        .into())
    }
}
