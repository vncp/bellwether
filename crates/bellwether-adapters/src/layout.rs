//! The plan's files as an engine reads them: each planned file's row
//! groups, row-group row counts, footer length, partition values, the
//! read order, and the projection; every reference checked against what
//! the adapter was given, so a plan that does not belong to its inputs is
//! a typed refusal before any engine sees it.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Seek, SeekFrom};

use bellwether_types::ScanPlan;

use crate::residual::column_type;
use crate::{AdapterError, ColumnDef, EmitOptions, Result};

/// A partition value at its dataset type `dt`, by the exact Arrow cast from
/// the path's string that the source applies when it synthesizes the
/// column; `None` for a null.
pub(crate) fn partition_scalar(
    column: &str,
    value: Option<&str>,
    dt: &arrow::datatypes::DataType,
) -> Result<Option<bellwether_core::encoding::Scalar>> {
    let Some(v) = value else { return Ok(None) };
    let bad = |reason: String| AdapterError::Plan(format!(
        "partition value {column}={v} is not a {dt} ({reason})"
    ));
    let strings: arrow::array::ArrayRef = std::sync::Arc::new(arrow::array::StringArray::from(vec![v]));
    let opts = arrow::compute::CastOptions {
        safe: false,
        format_options: arrow::util::display::FormatOptions::default(),
    };
    let typed = arrow::compute::cast_with_options(&strings, dt, &opts).map_err(|e| bad(e.to_string()))?;
    let value = bellwether_core::encoding::value_at(typed.as_ref(), 0)
        .map_err(|e| bad(e.to_string()))?
        .ok_or_else(|| bad("it casts to null".into()))?;
    Ok(Some(bellwether_core::encoding::Scalar::from_value(&value)))
}

/// The Parquet magic that ends every file.
const PAR1: &[u8; 4] = b"PAR1";

/// One planned file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedFile {
    /// The path as the plan spells it.
    pub path: String,
    /// Where the engine reads it ([`crate::Relocate`] applied).
    pub location: String,
    /// The size the plan's fingerprint records.
    pub size: u64,
    /// The planned row groups, ascending.
    pub row_groups: Vec<u32>,
    /// Rows of every row group of the file, when known (from
    /// [`EmitOptions::row_offsets`] or the footer).
    pub rows: Option<Vec<u64>>,
    /// The footer's exact length including the 8-byte trailer, when the
    /// footer was read ([`EmitOptions::footer_hint`]).
    pub footer_len: Option<u64>,
    /// The file's partition values, one per partition column.
    pub partition_values: Vec<Option<String>>,
    /// The planned bytes of this file (the plan entry's `bytes`).
    pub planned_bytes: u64,
}

impl PlannedFile {
    /// The row-group count, when known.
    #[must_use]
    pub fn row_group_count(&self) -> Option<usize> {
        self.rows.as_ref().map(Vec::len)
    }

    /// Whether the plan reads every row group of the file (`None` when the
    /// count is unknown).
    #[must_use]
    pub fn whole(&self) -> Option<bool> {
        self.row_group_count().map(|n| self.row_groups.len() == n)
    }

    /// The `[first, last]` row numbers of each run of consecutive planned
    /// row groups, from the row counts (empty row groups contribute none).
    #[must_use]
    pub fn row_ranges(&self) -> Option<Vec<(u64, u64)>> {
        let rows = self.rows.as_ref()?;
        let mut starts = Vec::with_capacity(rows.len());
        let mut at = 0u64;
        for r in rows {
            starts.push(at);
            at = at.saturating_add(*r);
        }
        let mut out: Vec<(u64, u64)> = Vec::new();
        for &rg in &self.row_groups {
            let i = rg as usize;
            let (first, n) = (starts[i], rows[i]);
            if n == 0 {
                continue;
            }
            let last = first + n - 1;
            match out.last_mut() {
                Some(prev) if prev.1 + 1 == first => prev.1 = last,
                _ => out.push((first, last)),
            }
        }
        Some(out)
    }

    /// The `[first, last]` rows of one row group.
    #[must_use]
    pub fn row_group_range(&self, rg: u32) -> Option<(u64, u64)> {
        let rows = self.rows.as_ref()?;
        let first: u64 = rows.iter().take(rg as usize).sum();
        let n = *rows.get(rg as usize)?;
        (n > 0).then(|| (first, first + n - 1))
    }
}

/// The plan's files and how they are read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Layout {
    /// The planned files, in entry order.
    pub files: Vec<PlannedFile>,
    /// The read order: `(file, row group)` pairs, `file` an index into
    /// `files` (the plan's `order`, else entry order with row groups
    /// ascending).
    pub read_order: Vec<(usize, u32)>,
    /// Whether the plan carried an interleaved `order`.
    pub interleaved: bool,
    /// The columns returned: the plan's projection, else every dataset
    /// column, in order.
    pub output: Vec<String>,
    /// The partition columns, in path order (empty when unpartitioned).
    pub partition_columns: Vec<String>,
}

/// Whether `path` is a local file path (no URL scheme, or `file://`).
fn local_path(path: &str) -> Option<&str> {
    if let Some(rest) = path.strip_prefix("file://") {
        return Some(rest);
    }
    if path.contains("://") {
        return None;
    }
    Some(path)
}

/// Reads the footer of the local file `path`: its exact length including
/// the trailer, and the rows of each row group. Checks the size the plan
/// recorded.
fn read_footer(path: &str, size: u64) -> Result<(u64, Vec<u64>)> {
    let footer = |reason: String| AdapterError::Footer {
        path: path.to_string(),
        reason,
    };
    let local = local_path(path).ok_or_else(|| {
        footer("the footer hint reads local files only; this path is a URL".into())
    })?;
    let mut f = std::fs::File::open(local).map_err(|e| AdapterError::UnknownFile {
        path: path.to_string(),
        why: format!("cannot be opened ({e})"),
    })?;
    let actual = f
        .metadata()
        .map_err(|e| footer(e.to_string()))?
        .len();
    if actual != size {
        return Err(AdapterError::UnknownFile {
            path: path.to_string(),
            why: format!(
                "is {actual} bytes, not the {size} bytes the plan's fingerprint records (it \
                 changed since it was planned)"
            ),
        });
    }
    if size < 12 {
        return Err(footer(format!("{size} bytes is too short for a Parquet file")));
    }
    let mut trailer = [0u8; 8];
    f.seek(SeekFrom::Start(size - 8))
        .and_then(|_| f.read_exact(&mut trailer))
        .map_err(|e| footer(e.to_string()))?;
    if &trailer[4..] != PAR1 {
        return Err(footer("the file does not end in the Parquet magic PAR1".into()));
    }
    let len = u64::from(u32::from_le_bytes([
        trailer[0], trailer[1], trailer[2], trailer[3],
    ]));
    if len + 12 > size {
        return Err(footer(format!(
            "the trailer gives a {len}-byte footer, longer than the file"
        )));
    }
    let mut meta = vec![0u8; usize::try_from(len).map_err(|e| footer(e.to_string()))?];
    f.seek(SeekFrom::Start(size - 8 - len))
        .and_then(|_| f.read_exact(&mut meta))
        .map_err(|e| footer(e.to_string()))?;
    let decoded = parquet::file::metadata::ParquetMetaDataReader::decode_metadata(&meta)
        .map_err(|e| footer(format!("the footer does not decode ({e})")))?;
    let rows = decoded
        .row_groups()
        .iter()
        .map(|rg| u64::try_from(rg.num_rows()).unwrap_or(0))
        .collect();
    Ok((len + 8, rows))
}

/// Lays the plan out, checking every file, row group, order
/// pair, projected column, and partition value against the adapter's
/// inputs.
#[allow(clippy::too_many_lines)]
pub fn layout(
    plan: &ScanPlan,
    schema: &[ColumnDef],
    opts: &EmitOptions,
) -> Result<Layout> {
    let mut seen = BTreeSet::new();
    let partition_columns = opts
        .partitions
        .as_ref()
        .map(|p| p.columns.clone())
        .unwrap_or_default();
    for c in &partition_columns {
        column_type(schema, c, "a partition column")?;
    }
    let mut files = Vec::with_capacity(plan.entries.len());
    for e in &plan.entries {
        let path = e.file.path.clone();
        if !seen.insert(path.clone()) {
            return Err(AdapterError::Plan(format!("file {path} has two entries")));
        }
        let row_groups: Vec<u32> = e.row_groups.iter().collect();
        let mut rows = match &opts.row_offsets {
            Some(o) => Some(
                o.rows(&path)
                    .ok_or_else(|| AdapterError::UnknownFile {
                        path: path.clone(),
                        why: "is not among the files whose row counts were given (the \
                              manifest's files)"
                            .into(),
                    })?
                    .to_vec(),
            ),
            None => None,
        };
        let mut footer_len = None;
        if opts.footer_hint {
            let (len, footer_rows) = read_footer(&path, e.file.size)?;
            if let Some(r) = &rows {
                if *r != footer_rows {
                    return Err(AdapterError::UnknownFile {
                        path: path.clone(),
                        why: format!(
                            "has row groups of {footer_rows:?} rows in its footer, not the \
                             {r:?} the given row counts record"
                        ),
                    });
                }
            }
            rows = Some(footer_rows);
            footer_len = Some(len);
        }
        if let Some(r) = &rows {
            if let Some(&bad) = row_groups.iter().find(|&&rg| rg as usize >= r.len()) {
                return Err(AdapterError::RowGroupOutOfRange {
                    path,
                    row_group: bad,
                    row_groups: r.len(),
                });
            }
        }
        let partition_values = match &opts.partitions {
            Some(p) => {
                let v = p.files.get(&path).ok_or_else(|| AdapterError::UnknownFile {
                    path: path.clone(),
                    why: "has no partition values among those given".into(),
                })?;
                if v.len() != p.columns.len() {
                    return Err(AdapterError::Plan(format!(
                        "{path} has {} partition values for {} partition columns",
                        v.len(),
                        p.columns.len()
                    )));
                }
                v.clone()
            }
            None => Vec::new(),
        };
        files.push(PlannedFile {
            location: opts
                .relocate
                .as_ref()
                .map_or_else(|| path.clone(), |r| r.apply(&path)),
            path,
            size: e.file.size,
            row_groups,
            rows,
            footer_len,
            partition_values,
            planned_bytes: e.bytes,
        });
    }
    let (read_order, interleaved) = match &plan.order {
        Some(order) => {
            let planned: BTreeSet<(usize, u32)> = files
                .iter()
                .enumerate()
                .flat_map(|(i, f)| f.row_groups.iter().map(move |&rg| (i, rg)))
                .collect();
            let mut out = Vec::with_capacity(order.len());
            let mut once = BTreeSet::new();
            for &(entry, rg) in order {
                let pair = (entry as usize, rg);
                if !planned.contains(&pair) {
                    return Err(AdapterError::Plan(format!(
                        "the read order names (entry {entry}, row group {rg}), which the plan \
                         does not read"
                    )));
                }
                if !once.insert(pair) {
                    return Err(AdapterError::Plan(format!(
                        "the read order names (entry {entry}, row group {rg}) twice"
                    )));
                }
                out.push(pair);
            }
            if once.len() != planned.len() {
                return Err(AdapterError::Plan(format!(
                    "the read order names {} of the plan's {} row groups",
                    once.len(),
                    planned.len()
                )));
            }
            (out, true)
        }
        None => (
            files
                .iter()
                .enumerate()
                .flat_map(|(i, f)| f.row_groups.iter().map(move |&rg| (i, rg)))
                .collect(),
            false,
        ),
    };
    let projection: Option<Vec<String>> = {
        let mut proj: Option<Vec<String>> = None;
        for e in &plan.entries {
            if let Some(cols) = &e.columns {
                let names: Vec<String> = cols.iter().map(|c| c.column.clone()).collect();
                match &proj {
                    Some(p) if *p != names => {
                        return Err(AdapterError::Plan(
                            "the entries project different columns".into(),
                        ))
                    }
                    _ => proj = Some(names),
                }
            }
        }
        proj
    };
    let output = match projection {
        Some(cols) => {
            for c in &cols {
                column_type(schema, c, "the plan's projection")?;
            }
            cols
        }
        None => schema.iter().map(|c| c.name.clone()).collect(),
    };
    Ok(Layout {
        files,
        read_order,
        interleaved,
        output,
        partition_columns,
    })
}

/// The row restriction each file needs for `engine`: `None` for a file
/// read whole, else the `[first, last]` row ranges of its planned row
/// groups. A strict subset without row counts is refused.
pub fn row_restrictions(
    layout: &Layout,
    engine: &'static str,
) -> Result<BTreeMap<usize, Option<Vec<(u64, u64)>>>> {
    let mut out = BTreeMap::new();
    for (i, f) in layout.files.iter().enumerate() {
        let r = match f.whole() {
            Some(true) => None,
            Some(false) => f.row_ranges(),
            None => {
                return Err(AdapterError::RowOffsetsNeeded {
                    engine,
                    path: f.path.clone(),
                    why: "to read only the planned row groups' rows, since it cannot pin row \
                          groups",
                })
            }
        };
        out.insert(i, r);
    }
    Ok(out)
}
