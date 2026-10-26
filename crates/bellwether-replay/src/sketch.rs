//! A transient sketch: what a `sketch` sidecar would hold, computed from the
//! data, so `cardinality` can answer (and price the sidecar) before one is
//! built, or over a folder that is not indexed at all.
//!
//! [`TransientSketch::build`] reads only the sketch's item columns, one row
//! group at a time in catalog order, through core's
//! [`FileSketcher`](bellwether_core::sketch::FileSketcher) — the code a build
//! writes segments with — so a transient sketch's registers equal a
//! committed sketch's for the same files (tested: predicted equals built).
//! Its numbers are **approximate**, like every sketch's, and nothing here
//! reaches a planner.
//!
//! Memory (ADR 018): one row group's item columns, one file's open sketch
//! (`2^p` registers and `k` signature slots per item), and the running totals
//! (the same per item).

use std::collections::BTreeSet;

use bellwether_core::sketch::{item_types, FileSketcher, SketchTotals};
use bellwether_types::{Error, Result, SketchSpec, Source};

use crate::catalog::Catalog;

/// What a transient sketch read and computed.
#[derive(Debug)]
pub struct TransientSketch {
    /// The totals, as a committed sketch's segments would fold to.
    pub totals: SketchTotals,
    /// Data files whose row groups were read.
    pub files_read: u64,
    /// Row groups read.
    pub row_groups_read: u64,
    /// Rows read.
    pub rows_read: u64,
}

impl TransientSketch {
    /// Sketches every live row group of `catalog` under `spec`, reading the
    /// item columns through `source`. A row group whose read returns a
    /// different row count than its statistics is an error.
    pub fn build(source: &dyn Source, catalog: &Catalog, spec: &SketchSpec) -> Result<Self> {
        let types = item_types(spec, catalog.schema())?;
        let mut columns: Vec<String> = Vec::new();
        for c in spec.items.iter().flatten() {
            if !columns.contains(c) {
                columns.push(c.clone());
            }
        }
        let mut totals = SketchTotals::new(spec)?;
        let mut sketcher = FileSketcher::new(spec, types)?;
        let mut files = BTreeSet::new();
        let mut current: Option<usize> = None;
        let (mut row_groups_read, mut rows_read) = (0u64, 0u64);
        for rg in catalog.row_groups() {
            if current.is_some_and(|f| f != rg.file) {
                totals.add_file(&sketcher.take()?)?;
            }
            if current != Some(rg.file) && !files.insert(rg.file) {
                return Err(Error::Other(format!(
                    "{}: its row groups are not contiguous in the catalog",
                    rg.meta.file.path
                )));
            }
            current = Some(rg.file);
            let batch = source.read_columns(&rg.meta, &columns)?;
            let rows = batch.num_rows() as u64;
            if rows != rg.meta.rows {
                return Err(Error::Source(format!(
                    "{} row group {} read {rows} rows but its statistics say {}",
                    rg.meta.file.path, rg.meta.index_in_file, rg.meta.rows
                )));
            }
            sketcher.add_batch(&batch)?;
            row_groups_read += 1;
            rows_read += rows;
        }
        if current.is_some() {
            totals.add_file(&sketcher.take()?)?;
        }
        Ok(Self {
            totals,
            files_read: files.len() as u64,
            row_groups_read,
            rows_read,
        })
    }
}
