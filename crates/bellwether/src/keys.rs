//! Key files: the keys a lookup or a recorded run asks for, read from a
//! Parquet file whose columns (all of them, or the ones named) are the key
//! columns, and encoded at the dataset types (ADR 016).

use std::fs::File;
use std::path::PathBuf;

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatchReader;
use bellwether_replay::{RunKeys, RunKeysBuilder};
use bellwether_types::{Error, Result};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use serde::{Deserialize, Serialize};

/// A file of keys: a Parquet file, every column of which is a key column
/// unless `columns` names a subset (in tuple order).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyFile {
    /// The Parquet file.
    pub path: PathBuf,
    /// Key columns to take from it, in tuple order; `None` takes every
    /// column in file order.
    pub columns: Option<Vec<String>>,
}

impl KeyFile {
    /// Every column of `path` is a key column.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            columns: None,
        }
    }

    /// Only `columns`, in this order.
    #[must_use]
    pub fn with_columns(mut self, columns: Vec<String>) -> Self {
        self.columns = Some(columns);
        self
    }

    /// The key columns as one batch holding exactly them, in tuple order.
    /// Memory: the key set, which a run holds anyway (ADR 018).
    pub(crate) fn read(&self) -> Result<RecordBatch> {
        let path = &self.path;
        let fail = |e: &dyn std::fmt::Display| {
            Error::Source(format!(
                "cannot read the key file {} ({e}); pass a Parquet file whose columns are the \
                 key columns",
                path.display()
            ))
        };
        let file = File::open(path).map_err(|e| fail(&e))?;
        let meta = bellwether_source::load_reader_metadata(&file).map_err(|e| fail(&e))?;
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, meta);
        let file_schema = builder.schema().clone();
        let names: Vec<String> = match &self.columns {
            Some(c) => c.clone(),
            None => file_schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect(),
        };
        if names.is_empty() {
            return Err(Error::KeySpec(format!(
                "the key file {} has no columns; it needs one column per key column",
                path.display()
            )));
        }
        let mut leaves = Vec::with_capacity(names.len());
        for n in &names {
            let i = file_schema.index_of(n).map_err(|_| {
                Error::KeySpec(format!(
                    "the key file {} has no column {n:?} (it has {:?}); name its key columns \
                     with --key",
                    path.display(),
                    file_schema
                        .fields()
                        .iter()
                        .map(|f| f.name().as_str())
                        .collect::<Vec<_>>()
                ))
            })?;
            leaves.push(i);
        }
        let mask = ProjectionMask::roots(builder.parquet_schema(), leaves);
        let reader = builder
            .with_projection(mask)
            .build()
            .map_err(|e| fail(&e))?;
        let schema = reader.schema();
        let mut batches = Vec::new();
        for b in reader {
            batches.push(b.map_err(|e| fail(&e))?);
        }
        let all = concat_batches(&schema, &batches).map_err(|e| fail(&e))?;
        // Reorder to tuple order: the projection keeps file order.
        let idx = names
            .iter()
            .map(|n| all.schema().index_of(n).map_err(|e| fail(&e)))
            .collect::<Result<Vec<_>>>()?;
        all.project(&idx).map_err(|e| fail(&e))
    }

    /// The canonical key set at `schema`'s types (nulls dropped and
    /// counted, sorted, deduplicated), with the dropped-null count.
    pub(crate) fn run_keys(&self, schema: &Schema) -> Result<(RunKeys, u64)> {
        let batch = self.read()?;
        let columns: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let mut b = RunKeysBuilder::new(&columns, schema)?;
        b.push_batch(&batch)?;
        let nulls = b.null_tuples();
        Ok((b.finish(), nulls))
    }
}
