//! [`Partitioned`]: partition values from paths (hive directories or a path
//! template, [`crate::partition`]) over any source whose files do not store
//! those columns, the CSV and JSON lines sources in particular. The Parquet
//! source carries the same rules built in (it also checks a stored column
//! against its path); this wrapper gives text sources the same columns.
//!
//! The inner source reads the files under the **physical** schema: the
//! dataset schema without the partition columns ([`physical_schema`]). The
//! wrapper adds each partition column to the dataset schema, to every
//! row group's statistics (`min = max = value`, zero bytes, a null count of
//! zero, or all rows for a null value), and to every read, so the profiler
//! and every planner path see it without a byte of the file being read for
//! it. A text file that also stores a partition column fails the inner
//! source's header or schema check, which names the column; drop it from
//! the files or from the scheme.
//!
//! Typing follows the Parquet source: the dataset schema's type when it
//! declares the column, else a template placeholder's declared type, else
//! [`crate::partition::infer_column_type`] over every value under the root
//! (never the first file's). Under a template every file must match
//! ([`Partitioning::values_of_all`] names up to ten that do not).
//!
//! Ported from the Argon WP21 implementation (text-source partition
//! values), re-implemented on [`Partitioning`].

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bellwether_types::{ColumnStats, Error, FileFingerprint, Result, RowGroupMeta, Source};

use crate::canonical::encode_as;
use crate::partition::{infer_column_type, repeat, typed_value, PartitionValue, Partitioning};

/// `dataset` without the columns named in `partition`: the schema the files
/// themselves hold.
#[must_use]
pub fn physical_schema(dataset: &Schema, partition: &[String]) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        dataset
            .fields()
            .iter()
            .filter(|f| !partition.contains(f.name()))
            .map(|f| f.as_ref().clone())
            .collect::<Vec<Field>>(),
        dataset.metadata().clone(),
    ))
}

/// A source whose files' paths add partition columns (module docs).
#[derive(Debug)]
pub struct Partitioned<S: Source> {
    inner: S,
    root: String,
    scheme: Partitioning,
    dataset: Option<SchemaRef>,
    types: OnceLock<std::result::Result<BTreeMap<String, DataType>, String>>,
}

impl<S: Source> Partitioned<S> {
    /// `inner` (reading the physical schema) with the partition columns
    /// `scheme` gives the files under `root`, typed by `dataset` when
    /// given. With [`Partitioning::None`] it is `inner` unchanged.
    pub fn new(
        inner: S,
        root: impl Into<String>,
        scheme: Partitioning,
        dataset: Option<SchemaRef>,
    ) -> Self {
        Self {
            inner,
            root: root.into(),
            scheme,
            dataset,
            types: OnceLock::new(),
        }
    }

    /// The wrapped source.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    fn values(&self, path: &str) -> Result<Vec<PartitionValue>> {
        self.scheme.values(&self.root, path)
    }

    /// Each partition column's type (module docs), computed once over
    /// every file the inner source discovers.
    fn types(&self) -> Result<&BTreeMap<String, DataType>> {
        self.types
            .get_or_init(|| {
                let files = self
                    .inner
                    .discover(&self.root)
                    .map_err(crate::partition::plain)?;
                let paths: Vec<String> = files.into_iter().map(|f| f.path).collect();
                let mut values: BTreeMap<String, Vec<Option<String>>> = BTreeMap::new();
                for file in self
                    .scheme
                    .values_of_all(&self.root, &paths)
                    .map_err(crate::partition::plain)?
                {
                    for v in file {
                        values.entry(v.name).or_default().push(v.value);
                    }
                }
                Ok(values
                    .into_iter()
                    .map(|(name, vs)| {
                        let declared = self
                            .dataset
                            .as_ref()
                            .and_then(|d| d.field_with_name(&name).ok())
                            .map(|f| f.data_type().clone());
                        let t = declared
                            .or_else(|| self.scheme.declared_type(&name))
                            .unwrap_or_else(|| infer_column_type(vs.iter().map(Option::as_deref)));
                        (name, t)
                    })
                    .collect())
            })
            .as_ref()
            .map_err(|e| Error::Source(e.clone()))
    }

    fn type_of(&self, name: &str) -> Result<DataType> {
        self.types()?.get(name).cloned().ok_or_else(|| {
            Error::Source(format!(
                "partition column {name} is not among those of the files under {}",
                self.root
            ))
        })
    }
}

impl<S: Source> Source for Partitioned<S> {
    fn discover(&self, root: &str) -> Result<Vec<FileFingerprint>> {
        let files = self.inner.discover(root)?;
        if self.scheme.is_none() {
            return Ok(files);
        }
        let paths: Vec<String> = files.iter().map(|f| f.path.clone()).collect();
        let all = self.scheme.values_of_all(&self.root, &paths)?;
        if let Some((first, rest)) = all.split_first() {
            let names = |v: &[PartitionValue]| v.iter().map(|p| p.name.clone()).collect::<Vec<_>>();
            if let Some((i, _)) = rest
                .iter()
                .enumerate()
                .find(|(_, v)| names(v) != names(first))
            {
                return Err(Error::Source(format!(
                    "{} has partition columns {:?}, but {} has {:?}; every file under a \
                     partitioned root needs the same partition columns",
                    paths[i + 1],
                    names(&rest[i]),
                    paths[0],
                    names(first)
                )));
            }
        }
        Ok(files)
    }

    fn row_groups(&self, file: &FileFingerprint) -> Result<Vec<RowGroupMeta>> {
        let mut out = self.inner.row_groups(file)?;
        for p in self.values(&file.path)? {
            let target = self.type_of(&p.name)?;
            let one = typed_value(&p.name, p.value.as_deref(), &target)?;
            let encoded = encode_as(one.as_ref(), 0, &target)?;
            if p.value.is_some() && encoded.is_none() {
                return Err(Error::Source(format!(
                    "{}: partition column {} of type {target} has no canonical encoding",
                    file.path, p.name
                )));
            }
            for rg in &mut out {
                rg.stats.retain(|s| s.column != p.name);
                rg.stats.push(ColumnStats {
                    column: p.name.clone(),
                    min: encoded.clone(),
                    max: encoded.clone(),
                    null_count: Some(if encoded.is_some() { 0 } else { rg.rows }),
                    distinct_count: None,
                    has_bloom: false,
                    // Virtual: reading it costs no bytes.
                    bytes: Some(0),
                });
            }
        }
        Ok(out)
    }

    fn read_columns(&self, rg: &RowGroupMeta, columns: &[String]) -> Result<RecordBatch> {
        let values = self.values(&rg.file.path)?;
        if values.is_empty() {
            return self.inner.read_columns(rg, columns);
        }
        let stored: Vec<String> = columns
            .iter()
            .filter(|c| !values.iter().any(|p| &p.name == *c))
            .cloned()
            .collect();
        let batch = self.inner.read_columns(rg, &stored)?;
        let rows = batch.num_rows();
        let mut fields = Vec::with_capacity(columns.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
        for c in columns {
            if let Some(p) = values.iter().find(|p| &p.name == c) {
                let target = self.type_of(c)?;
                let one = typed_value(c, p.value.as_deref(), &target)?;
                fields.push(Field::new(c.clone(), target, true));
                arrays.push(repeat(&one, rows)?);
            } else {
                let i = batch
                    .schema()
                    .index_of(c)
                    .map_err(|_| Error::KeySpec(format!("column {c:?} not in decoded batch")))?;
                fields.push(batch.schema().field(i).clone());
                arrays.push(Arc::clone(batch.column(i)));
            }
        }
        RecordBatch::try_new_with_options(
            Arc::new(Schema::new(fields)),
            arrays,
            &arrow::array::RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .map_err(|e| Error::Source(format!("{}: {e}", rg.file.path)))
    }

    fn schema(&self, root: &str) -> Result<SchemaRef> {
        if let Some(d) = &self.dataset {
            return Ok(Arc::clone(d));
        }
        let physical = self.inner.schema(root)?;
        if self.scheme.is_none() {
            return Ok(physical);
        }
        let types = self.types()?;
        let mut fields: Vec<Field> = physical
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        let first = self.inner.discover(root)?.into_iter().next();
        let names: Vec<String> = match first {
            Some(f) => self.values(&f.path)?.into_iter().map(|p| p.name).collect(),
            None => Vec::new(),
        };
        for n in names {
            if physical.field_with_name(&n).is_ok() {
                return Err(Error::Source(format!(
                    "the files under {root} store column {n}, which their paths also give; a \
                     text file must not store a partition column, so drop it from the files or \
                     from the scheme"
                )));
            }
            let t = types.get(&n).cloned().unwrap_or(DataType::Utf8);
            fields.push(Field::new(n, t, true));
        }
        Ok(Arc::new(Schema::new_with_metadata(
            fields,
            physical.metadata().clone(),
        )))
    }
}
