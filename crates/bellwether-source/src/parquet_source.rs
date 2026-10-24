//! [`ParquetSource`]: real row groups, footer statistics, projected reads.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use arrow::array::{Array, ArrayRef, RecordBatch, RecordBatchReader, UInt64Array};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, SchemaRef};
use bellwether_types::{ColumnStats, Error, FileFingerprint, Result, RowGroupMeta, Source};
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::ProjectionMask;
use parquet::basic::{ColumnOrder, SortOrder, Type as PhysicalType};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};

use crate::canonical::{normalize_float_bounds, Canonical, DefaultCanonical};
use crate::fingerprint::{fingerprint_with_policy, FingerprintPolicy};
use crate::storage::{LocalFs, Storage};
use crate::{open_verified, reorder, resolve_columns, BoundedCache};

/// File extension [`ParquetSource::discover`] lists.
pub const PARQUET_EXTENSION: &str = "parquet";
/// Byte budget of the in-memory footer cache.
pub const FOOTER_CACHE_BYTES: usize = 256 * 1024 * 1024;

type FooterCache = BoundedCache<FileFingerprint, ArrowReaderMetadata>;

/// A [`Source`] over a folder of Parquet files.
///
/// Each file's footer is parsed once and cached in memory, keyed by
/// fingerprint. A footer is only ever parsed from a handle verified against
/// the full fingerprint, so a cached footer always describes the bytes it is
/// keyed by. `read_columns` re-verifies on its own handle and decodes only
/// the requested column chunks of one row group.
///
/// Statistics are encoded as the **dataset** type of each column: the type in
/// the schema given by [`with_schema`](Self::with_schema), or the file's own
/// type when no dataset schema is supplied.
#[derive(Debug)]
pub struct ParquetSource<S: Storage = LocalFs> {
    storage: S,
    canonical: Arc<dyn Canonical>,
    dataset: Option<SchemaRef>,
    policy: FingerprintPolicy,
    footers: Mutex<FooterCache>,
}

impl Default for ParquetSource<LocalFs> {
    fn default() -> Self {
        Self::new()
    }
}

impl ParquetSource<LocalFs> {
    /// A source over the local filesystem.
    #[must_use]
    pub fn new() -> Self {
        Self::with_storage(LocalFs)
    }
}

impl<S: Storage> ParquetSource<S> {
    /// A source over any [`Storage`].
    pub fn with_storage(storage: S) -> Self {
        Self {
            storage,
            canonical: Arc::new(DefaultCanonical),
            dataset: None,
            policy: FingerprintPolicy::Tail64K,
            footers: Mutex::new(BoundedCache::new(FOOTER_CACHE_BYTES)),
        }
    }

    /// Sets the dataset schema. It is returned by `schema(root)`, and its
    /// column types decide how statistics are encoded: decimals rescale to
    /// its scale, timestamps convert to its unit.
    #[must_use]
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.dataset = Some(schema);
        self
    }

    /// Sets the fingerprint policy used by `discover`, `schema`, and every
    /// verification. It must match the policy the fingerprints were taken
    /// with (`bellwether.toml` `fingerprint_policy`).
    #[must_use]
    pub fn with_fingerprint_policy(mut self, policy: FingerprintPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Replaces the statistics encoder. At integration this will be
    /// `bellwether-core`'s encoder.
    #[must_use]
    pub fn with_canonical(mut self, canonical: Arc<dyn Canonical>) -> Self {
        self.canonical = canonical;
        self
    }

    /// The parsed footer of `file`, from cache or from a verified handle.
    pub fn footer(&self, file: &FileFingerprint) -> Result<ArrowReaderMetadata> {
        if let Some(hit) = self.cache().get(file) {
            return Ok(hit);
        }
        let reader = open_verified(&self.storage, file, self.policy)?;
        self.footer_from(file, &reader)
    }

    /// The footer of `file`, parsing it from `reader` (already verified
    /// against `file`) on a cache miss.
    fn footer_from(
        &self,
        file: &FileFingerprint,
        reader: &S::Reader,
    ) -> Result<ArrowReaderMetadata> {
        if let Some(hit) = self.cache().get(file) {
            return Ok(hit);
        }
        let meta = ArrowReaderMetadata::load(reader, ArrowReaderOptions::new())
            .map_err(|e| Error::Source(format!("{}: footer: {e}", file.path)))?;
        let size = meta.metadata().memory_size();
        self.cache().insert(file.clone(), meta.clone(), size);
        Ok(meta)
    }

    fn cache(&self) -> MutexGuard<'_, FooterCache> {
        // A poisoned cache only means another thread panicked mid-insert; the
        // map itself is still a valid cache.
        self.footers.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The dataset type of column `name`, falling back to the file type.
    fn target_type<'a>(&'a self, name: &str, file_type: &'a DataType) -> &'a DataType {
        self.dataset
            .as_ref()
            .and_then(|s| s.field_with_name(name).ok())
            .map_or(file_type, |f| f.data_type())
    }

    fn encode_at(&self, array: Option<&ArrayRef>, i: usize, target: &DataType) -> Option<Vec<u8>> {
        let array = array?;
        if i >= array.len() || !self.canonical.supports(array.data_type()) {
            return None;
        }
        self.canonical
            .encode(array.as_ref(), i, target)
            .ok()
            .flatten()
    }

    /// Compressed column-chunk bytes per row group, per top-level column, in
    /// schema order: `out[row_group][column]`. A nested column sums its leaf
    /// chunks. Read from the cached footer; no data pages are touched.
    pub fn column_bytes(&self, file: &FileFingerprint) -> Result<Vec<Vec<u64>>> {
        let meta = self.footer(file)?;
        let md = meta.metadata();
        let roots = meta.schema().fields().len();
        Ok((0..md.num_row_groups())
            .map(|i| root_column_bytes(md, i, roots))
            .collect())
    }
}

/// Compressed bytes of each top-level column in row group `rg`.
fn root_column_bytes(md: &ParquetMetaData, rg: usize, roots: usize) -> Vec<u64> {
    let pschema = md.file_metadata().schema_descr();
    let mut out = vec![0u64; roots];
    for (leaf, chunk) in md.row_group(rg).columns().iter().enumerate() {
        let root = pschema.get_column_root_idx(leaf);
        if let Some(slot) = out.get_mut(root) {
            *slot += u64::try_from(chunk.compressed_size()).unwrap_or(0);
        }
    }
    out
}

/// Whether deprecated (`min`/`max`, not `min_value`/`max_value`) statistics
/// may be trusted for a column with sort order `order` and physical type
/// `physical`.
///
/// Old writers ordered them with signed comparison, which is wrong for
/// unsigned types, so they are used only when the column's order is SIGNED.
/// They are never used for `BYTE_ARRAY` or `FIXED_LEN_BYTE_ARRAY` columns,
/// even signed ones such as decimals. Old parquet-mr compared those bytes as
/// signed, and PARQUET-251 wrote corrupt binary statistics.
#[must_use]
pub fn deprecated_stats_usable(order: SortOrder, physical: PhysicalType) -> bool {
    order == SortOrder::SIGNED
        && !matches!(
            physical,
            PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY
        )
}

/// The sort order of leaf column `leaf`, derived from its logical type when
/// the file predates column orders.
fn leaf_sort_order(md: &ParquetMetaData, leaf: usize) -> SortOrder {
    match md.file_metadata().column_order(leaf) {
        ColumnOrder::TYPE_DEFINED_ORDER(o) => o,
        ColumnOrder::UNDEFINED => {
            let col = md.file_metadata().schema_descr().column(leaf);
            ColumnOrder::get_sort_order(
                col.logical_type(),
                col.converted_type(),
                col.physical_type(),
            )
        }
    }
}

/// Whether row group `rg`'s min/max for `leaf` may be used.
fn min_max_trusted(md: &ParquetMetaData, rg: &RowGroupMetaData, leaf: usize) -> bool {
    match rg.column(leaf).statistics() {
        Some(s) if s.is_min_max_deprecated() => {
            let physical = md
                .file_metadata()
                .schema_descr()
                .column(leaf)
                .physical_type();
            deprecated_stats_usable(leaf_sort_order(md, leaf), physical)
        }
        _ => true,
    }
}

/// Footer statistics for one column across all row groups.
struct ColumnColumnar {
    name: String,
    leaf: Option<usize>,
    mins: Option<ArrayRef>,
    maxes: Option<ArrayRef>,
    nulls: Option<UInt64Array>,
}

fn column_stats(meta: &ArrowReaderMetadata, name: &str) -> ColumnColumnar {
    let md = meta.metadata();
    let schema = meta.schema();
    let pschema = md.file_metadata().schema_descr();
    let mut out = ColumnColumnar {
        name: name.to_string(),
        leaf: None,
        mins: None,
        maxes: None,
        nulls: None,
    };
    // Nested and otherwise unconvertible columns report no statistics; that
    // is "unknown", which is always safe for pruning.
    let Ok(conv) = StatisticsConverter::try_new(name, schema, pschema) else {
        return out;
    };
    let conv = conv.with_missing_null_counts_as_zero(false);
    out.leaf = conv.parquet_column_index();
    out.mins = conv.row_group_mins(md.row_groups()).ok();
    out.maxes = conv.row_group_maxes(md.row_groups()).ok();
    out.nulls = conv.row_group_null_counts(md.row_groups()).ok();
    out
}

impl<S: Storage> Source for ParquetSource<S> {
    fn discover(&self, root: &str) -> Result<Vec<FileFingerprint>> {
        self.storage
            .list(root)?
            .into_iter()
            .filter(|p| has_extension(p, PARQUET_EXTENSION))
            .map(|p| fingerprint_with_policy(&self.storage, &p, self.policy))
            .collect()
    }

    fn row_groups(&self, file: &FileFingerprint) -> Result<Vec<RowGroupMeta>> {
        let meta = self.footer(file)?;
        let md = meta.metadata();
        let fields = meta.schema().fields().clone();
        let columns: Vec<ColumnColumnar> = fields
            .iter()
            .map(|f| column_stats(&meta, f.name()))
            .collect();
        let mut out = Vec::with_capacity(md.num_row_groups());
        for (i, rg) in md.row_groups().iter().enumerate() {
            let col_bytes = root_column_bytes(md, i, columns.len());
            let stats = columns
                .iter()
                .zip(fields.iter())
                .enumerate()
                .map(|(ci, (c, field))| {
                    let chunk = c.leaf.map(|leaf| rg.column(leaf));
                    let trusted = c.leaf.is_some_and(|leaf| min_max_trusted(md, rg, leaf));
                    let target = self.target_type(&c.name, field.data_type());
                    let (min, max) = if trusted {
                        normalize_float_bounds(
                            self.encode_at(c.mins.as_ref(), i, target),
                            self.encode_at(c.maxes.as_ref(), i, target),
                        )
                    } else {
                        (None, None)
                    };
                    ColumnStats {
                        column: c.name.clone(),
                        min,
                        max,
                        null_count: c
                            .nulls
                            .as_ref()
                            .filter(|n| i < n.len() && n.is_valid(i))
                            .map(|n| n.value(i)),
                        distinct_count: chunk
                            .and_then(|ch| ch.statistics())
                            .and_then(parquet::file::statistics::Statistics::distinct_count_opt),
                        has_bloom: chunk.is_some_and(|ch| ch.bloom_filter_offset().is_some()),
                        bytes: col_bytes.get(ci).copied(),
                    }
                })
                .collect();
            out.push(RowGroupMeta {
                id: None,
                file: file.clone(),
                index_in_file: u32::try_from(i)
                    .map_err(|_| Error::Source(format!("{}: too many row groups", file.path)))?,
                rows: u64::try_from(rg.num_rows()).unwrap_or(0),
                bytes: u64::try_from(rg.compressed_size()).unwrap_or(0),
                stats,
                byte_range: None,
            });
        }
        Ok(out)
    }

    fn read_columns(&self, rg: &RowGroupMeta, columns: &[String]) -> Result<RecordBatch> {
        // Verify on the handle we decode from: a cached footer must never
        // drive reads into a file that has since been rewritten or replaced.
        let reader = open_verified(&self.storage, &rg.file, self.policy)?;
        let meta = self.footer_from(&rg.file, &reader)?;
        let idx = usize::try_from(rg.index_in_file).unwrap_or(usize::MAX);
        let md = meta.metadata();
        if idx >= md.num_row_groups() {
            return Err(Error::Source(format!(
                "{}: row group {} out of range ({} row groups)",
                rg.file.path,
                rg.index_in_file,
                md.num_row_groups()
            )));
        }
        let roots = resolve_columns(meta.schema(), columns)?;
        let rows = usize::try_from(md.row_group(idx).num_rows()).unwrap_or(0);
        let mask = ProjectionMask::roots(md.file_metadata().schema_descr(), roots);
        let batches = ParquetRecordBatchReaderBuilder::new_with_metadata(reader, meta)
            .with_row_groups(vec![idx])
            .with_projection(mask)
            .with_batch_size(rows.max(1))
            .build()
            .map_err(|e| Error::Source(format!("{}: {e}", rg.file.path)))?;
        let schema = batches.schema();
        let batches = batches
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| Error::Source(format!("{}: {e}", rg.file.path)))?;
        let batch = concat_batches(&schema, &batches)
            .map_err(|e| Error::Source(format!("{}: {e}", rg.file.path)))?;
        reorder(&batch, columns)
    }

    fn schema(&self, root: &str) -> Result<SchemaRef> {
        if let Some(s) = &self.dataset {
            return Ok(Arc::clone(s));
        }
        let first = self
            .storage
            .list(root)?
            .into_iter()
            .find(|p| has_extension(p, PARQUET_EXTENSION))
            .ok_or_else(|| Error::Source(format!("{root}: no .parquet files")))?;
        let fp = fingerprint_with_policy(&self.storage, &first, self.policy)?;
        Ok(self.footer(&fp)?.schema().clone())
    }
}

pub(crate) fn has_extension(path: &str, ext: &str) -> bool {
    std::path::Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case(ext))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deprecated_stats_only_for_signed_order() {
        assert!(deprecated_stats_usable(
            SortOrder::SIGNED,
            PhysicalType::INT64
        ));
        assert!(deprecated_stats_usable(
            SortOrder::SIGNED,
            PhysicalType::DOUBLE
        ));
        assert!(!deprecated_stats_usable(
            SortOrder::UNSIGNED,
            PhysicalType::INT32
        ));
        assert!(!deprecated_stats_usable(
            SortOrder::UNDEFINED,
            PhysicalType::INT96
        ));
        // Binary physical types are never trusted, even with a signed order
        // (a decimal stored as FIXED_LEN_BYTE_ARRAY).
        assert!(!deprecated_stats_usable(
            SortOrder::SIGNED,
            PhysicalType::BYTE_ARRAY
        ));
        assert!(!deprecated_stats_usable(
            SortOrder::SIGNED,
            PhysicalType::FIXED_LEN_BYTE_ARRAY
        ));
    }

    #[test]
    fn legacy_files_derive_order_from_type() {
        use parquet::basic::{ConvertedType, Type};
        // What leaf_sort_order computes for an old file without column orders.
        assert_eq!(
            ColumnOrder::get_sort_order(None, ConvertedType::UTF8, Type::BYTE_ARRAY),
            SortOrder::UNSIGNED
        );
        assert_eq!(
            ColumnOrder::get_sort_order(None, ConvertedType::UINT_32, Type::INT32),
            SortOrder::UNSIGNED
        );
        assert_eq!(
            ColumnOrder::get_sort_order(None, ConvertedType::NONE, Type::INT64),
            SortOrder::SIGNED
        );
    }

    /// The rule wired through a footer: a legacy file (no column orders) with
    /// deprecated statistics on a UTF8 and an INT64 column, plus a UTF8
    /// column with modern statistics.
    #[test]
    fn min_max_trusted_on_a_legacy_footer() {
        use parquet::basic::ConvertedType;
        use parquet::data_type::ByteArray;
        use parquet::file::metadata::{ColumnChunkMetaData, FileMetaData};
        use parquet::file::statistics::Statistics;
        use parquet::schema::types::{SchemaDescriptor, Type};
        use std::sync::Arc;

        let utf8 = |name: &str| {
            Arc::new(
                Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
                    .with_converted_type(ConvertedType::UTF8)
                    .build()
                    .unwrap(),
            )
        };
        let schema = Type::group_type_builder("schema")
            .with_fields(vec![
                utf8("old_s"),
                Arc::new(
                    Type::primitive_type_builder("old_i", PhysicalType::INT64)
                        .build()
                        .unwrap(),
                ),
                utf8("new_s"),
            ])
            .build()
            .unwrap();
        let descr = Arc::new(SchemaDescriptor::new(Arc::new(schema)));
        let bytes = |deprecated| {
            Statistics::byte_array(
                Some(ByteArray::from("a")),
                Some(ByteArray::from("z")),
                None,
                Some(0),
                deprecated,
            )
        };
        let chunk = |i: usize, stats: Statistics| {
            ColumnChunkMetaData::builder(descr.column(i))
                .set_statistics(stats)
                .build()
                .unwrap()
        };
        let rg = RowGroupMetaData::builder(Arc::clone(&descr))
            .set_num_rows(1)
            .set_column_metadata(vec![
                chunk(0, bytes(true)),
                chunk(1, Statistics::int64(Some(1), Some(9), None, Some(0), true)),
                chunk(2, bytes(false)),
            ])
            .build()
            .unwrap();
        let file = FileMetaData::new(1, 1, None, None, Arc::clone(&descr), None);
        let md = ParquetMetaData::new(file, vec![rg]);
        let rg = md.row_group(0);
        assert!(!min_max_trusted(&md, rg, 0), "deprecated UTF8 stats");
        assert!(min_max_trusted(&md, rg, 1), "deprecated INT64 stats");
        assert!(min_max_trusted(&md, rg, 2), "modern UTF8 stats");
    }
}
