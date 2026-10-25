//! [`ParquetSource`]: real row groups, footer statistics, projected reads.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use arrow::array::{Array, ArrayRef, RecordBatch, RecordBatchReader, UInt64Array};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bellwether_types::{ColumnStats, Error, FileFingerprint, Result, RowGroupMeta, Source};
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::ProjectionMask;
use parquet::basic::{ColumnOrder, SortOrder, Type as PhysicalType};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::reader::ChunkReader;

use crate::canonical::{normalize_float_bounds, Canonical, DefaultCanonical};
use crate::fingerprint::{fingerprint_with_policy, FingerprintPolicy};
use crate::partition::{infer_column_type, repeat, typed_value, PartitionValue, Partitioning};
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
    /// The data root and its partitioning scheme (`partition`), or `None`
    /// when paths carry no data.
    partition_root: Option<(String, Partitioning)>,
    /// Partition column types inferred over every file under the root, for
    /// a source without a dataset schema; computed once.
    inferred: OnceLock<std::result::Result<BTreeMap<String, DataType>, String>>,
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
            partition_root: None,
            inferred: OnceLock::new(),
        }
    }

    /// Turns on partitioning under the data root `root` (see
    /// [`crate::partition`]): under [`Partitioning::Hive`] each `name=value`
    /// directory below `root`, under [`Partitioning::Template`] each
    /// placeholder, is a virtual column, reported in every row group's
    /// statistics with `min = max = value` and synthesized by
    /// `read_columns`. [`Partitioning::None`] turns it off. `root` must be
    /// spelled as the discovered paths are (the canonical data root).
    #[must_use]
    pub fn with_partitioning(mut self, root: impl Into<String>, scheme: Partitioning) -> Self {
        self.partition_root = (!scheme.is_none()).then(|| (root.into(), scheme));
        self
    }

    /// The partition columns of `file`, or none without partitioning.
    fn partitions(&self, path: &str) -> Result<Vec<PartitionValue>> {
        match &self.partition_root {
            None => Ok(Vec::new()),
            Some((root, scheme)) => scheme.values(root, path),
        }
    }

    /// The type of partition column `p` in a file whose own schema is
    /// `file_schema`: the dataset schema's when it declares the column,
    /// `None` (not reported) when a dataset schema is given without it,
    /// else the file's own type for a column the file also stores, else the
    /// type a template placeholder declares, else the type inferred over
    /// every file under the root ([`crate::partition::infer_column_type`]).
    fn partition_type(&self, p: &PartitionValue, file_schema: &Schema) -> Result<Option<DataType>> {
        if let Some(ds) = &self.dataset {
            return Ok(ds
                .field_with_name(&p.name)
                .ok()
                .map(|f| f.data_type().clone()));
        }
        if let Ok(f) = file_schema.field_with_name(&p.name) {
            return Ok(Some(f.data_type().clone()));
        }
        self.inferred_types()?
            .get(&p.name)
            .cloned()
            .map(Some)
            .ok_or_else(|| {
                Error::Source(format!(
                    "partition column {} is not among those inferred under {}",
                    p.name,
                    self.partition_root.as_ref().map_or("", |(r, _)| r.as_str())
                ))
            })
    }

    /// Every partition column's type over every Parquet file under the
    /// root (computed once): a template placeholder's declared type, else
    /// the type inferred from every value.
    fn inferred_types(&self) -> Result<&BTreeMap<String, DataType>> {
        let Some((root, scheme)) = &self.partition_root else {
            return Err(Error::Source(
                "partition types asked for without partitioning".into(),
            ));
        };
        self.inferred
            .get_or_init(|| {
                let mut values: BTreeMap<String, Vec<Option<String>>> = BTreeMap::new();
                let paths: Vec<String> = self
                    .storage
                    .list(root)
                    .map_err(crate::partition::plain)?
                    .into_iter()
                    .filter(|p| has_extension(p, PARQUET_EXTENSION))
                    .collect();
                for file in scheme
                    .values_of_all(root, &paths)
                    .map_err(crate::partition::plain)?
                {
                    for v in file {
                        values.entry(v.name).or_default().push(v.value);
                    }
                }
                Ok(values
                    .into_iter()
                    .map(|(name, vs)| {
                        let t = scheme
                            .declared_type(&name)
                            .unwrap_or_else(|| infer_column_type(vs.iter().map(Option::as_deref)));
                        (name, t)
                    })
                    .collect())
            })
            .as_ref()
            .map_err(|e| Error::Source(e.clone()))
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
        let meta = load_reader_metadata(reader)
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

    /// Appends each partition column's statistics to `out` (one entry per
    /// row group of the file), or checks them against the file's own column
    /// of the same name: both footer bounds must equal the path value and
    /// the null count must be zero (all rows, for a null path value).
    /// Unknown or disagreeing statistics are refused.
    fn add_partition_stats(
        &self,
        file: &FileFingerprint,
        file_schema: &Schema,
        out: &mut [RowGroupMeta],
    ) -> Result<()> {
        for p in self.partitions(&file.path)? {
            let Some(target) = self.partition_type(&p, file_schema)? else {
                continue;
            };
            let one = typed_value(&p.name, p.value.as_deref(), &target)?;
            let encoded = match p.value {
                Some(_) => Some(
                    self.canonical
                        .encode(one.as_ref(), 0, &target)?
                        .ok_or_else(|| {
                            Error::Source(format!(
                                "{}: partition column {} of type {target} has no canonical \
                                 encoding",
                                file.path, p.name
                            ))
                        })?,
                ),
                None => None,
            };
            let stored = file_schema.field_with_name(&p.name).is_ok();
            for rg in out.iter_mut() {
                if stored {
                    let s = rg.stats.iter().find(|s| s.column == p.name);
                    let agrees = s.is_some_and(|s| match &encoded {
                        Some(v) => {
                            s.min.as_ref() == Some(v)
                                && s.max.as_ref() == Some(v)
                                && s.null_count == Some(0)
                        }
                        None => s.null_count == Some(rg.rows),
                    });
                    if !agrees {
                        let why = match s {
                            None => "has no statistics for it".to_string(),
                            Some(s) if s.min.is_none() || s.max.is_none() => {
                                "has no trusted min/max for it".to_string()
                            }
                            Some(s) if s.min != s.max => {
                                "holds more than one value of it (its min and max differ)"
                                    .to_string()
                            }
                            Some(s) if s.min != encoded => {
                                "holds another value of it than the path's".to_string()
                            }
                            Some(s) => format!(
                                "has {} null(s) in it",
                                s.null_count
                                    .map_or("an unknown number of".into(), |n| n.to_string())
                            ),
                        };
                        return Err(Error::Source(format!(
                            "{}: row group {} stores column {} and {why}, so it does not show the \
                             value its path gives ({}={}); a file whose path and content \
                             disagree, or whose statistics cannot show they agree, is refused",
                            file.path,
                            rg.index_in_file,
                            p.name,
                            p.name,
                            p.value.as_deref().unwrap_or(crate::partition::HIVE_NULL)
                        )));
                    }
                } else {
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
        }
        Ok(())
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

/// A Parquet file's footer and Arrow schema, with the Arrow types inferred
/// exactly as parquet 53 inferred them: duration-hinted `INT64` columns as
/// `Int64`, `INT96` timestamps at nanoseconds, and narrow decimals as
/// `Decimal128`, also when dictionary-encoded (docs/format.md, "Statistics semantics", inference amendment). Every reader of
/// user Parquet files (data files and key files) goes through this, so a
/// column has one type whichever path reads it.
pub fn load_reader_metadata<R: ChunkReader>(
    reader: &R,
) -> parquet::errors::Result<ArrowReaderMetadata> {
    let meta = ArrowReaderMetadata::load(reader, ArrowReaderOptions::new())?;
    match legacy_inference(&meta) {
        Some(schema) => ArrowReaderMetadata::try_new(
            Arc::clone(meta.metadata()),
            ArrowReaderOptions::new().with_schema(schema),
        ),
        None => Ok(meta),
    }
}

/// The Arrow type parquet 53 inferred for a top-level primitive column that
/// parquet 59 now reads as `inferred`, or `None` when the two agree.
///
/// Inference must not change across a dependency upgrade: a column's type
/// is recorded in the manifest and decides its canonical encoding, so an
/// index built by an older binary must see the same types from the same
/// files. parquet 59 honours three embedded Arrow schema hints that parquet
/// 53 ignored; the source keeps 53's mapping for each:
///
/// - `Duration(_)` on an `INT64` column (written by pyarrow and arrow-rs for
///   duration arrays): 53 read `Int64`.
/// - `Timestamp(unit, tz)` on an `INT96` column with a unit other than
///   nanoseconds: 53 read `Timestamp(ns, None)` (it took a hint only for the
///   time zone, and only at the same unit).
/// - `Decimal32` and `Decimal64` (narrowed from a hint): 53 read
///   `Decimal128` at the same precision and scale.
/// - A `Dictionary` whose value type is one of the above: parquet 53 applied
///   a dictionary hint only when its value hint applied, and otherwise fell
///   back to the plain type, so it read the rewritten value type itself
///   (`Int64`, `Timestamp(ns, None)`, `Decimal128`), not a dictionary. A
///   dictionary whose value type is not rewritten is kept.
///
/// A new type is adopted deliberately, with a format amendment, never by a
/// reader upgrade.
fn legacy_type(inferred: &DataType, physical: PhysicalType) -> Option<DataType> {
    use arrow::datatypes::TimeUnit;
    match (inferred, physical) {
        (DataType::Duration(_), PhysicalType::INT64) => Some(DataType::Int64),
        (DataType::Timestamp(unit, _), PhysicalType::INT96) if *unit != TimeUnit::Nanosecond => {
            Some(DataType::Timestamp(TimeUnit::Nanosecond, None))
        }
        (DataType::Decimal32(p, s) | DataType::Decimal64(p, s), _) => {
            Some(DataType::Decimal128(*p, *s))
        }
        (DataType::Dictionary(_, value), _) => legacy_type(value, physical),
        _ => None,
    }
}

/// The file schema with parquet 53's inference restored (see
/// [`legacy_type`]), or `None` when no column differs.
fn legacy_inference(meta: &ArrowReaderMetadata) -> Option<SchemaRef> {
    use arrow::datatypes::{Field, Schema};
    let schema = meta.schema();
    let roots = meta.metadata().file_metadata().schema_descr().root_schema();
    let parquet_fields = roots.get_fields();
    if parquet_fields.len() != schema.fields().len() {
        return None;
    }
    let mut changed = false;
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .zip(parquet_fields)
        .map(|(f, p)| {
            let legacy = p
                .is_primitive()
                .then(|| legacy_type(f.data_type(), p.get_physical_type()))
                .flatten();
            match legacy {
                Some(t) => {
                    changed = true;
                    f.as_ref().clone().with_data_type(t)
                }
                None => f.as_ref().clone(),
            }
        })
        .collect();
    changed.then(|| Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())))
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
/// the file predates column orders. A column order this reader does not
/// recognize (a later format's union member) has no known order.
fn leaf_sort_order(md: &ParquetMetaData, leaf: usize) -> SortOrder {
    match md.file_metadata().column_order(leaf) {
        ColumnOrder::TYPE_DEFINED_ORDER(o) => o,
        ColumnOrder::UNDEFINED => {
            let col = md.file_metadata().schema_descr().column(leaf);
            ColumnOrder::sort_order_for_type(
                col.logical_type_ref(),
                col.converted_type(),
                col.physical_type(),
            )
        }
        ColumnOrder::UNKNOWN => SortOrder::UNDEFINED,
    }
}

/// Whether row group `rg`'s min/max for `leaf` may be used.
///
/// Never under a column order this reader does not recognize: the Parquet
/// specification says such statistics are ignored, because the order their
/// bounds were computed in is unknown.
fn min_max_trusted(md: &ParquetMetaData, rg: &RowGroupMetaData, leaf: usize) -> bool {
    if md.file_metadata().column_order(leaf) == ColumnOrder::UNKNOWN {
        return false;
    }
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
        let files = self
            .storage
            .list(root)?
            .into_iter()
            .filter(|p| has_extension(p, PARQUET_EXTENSION))
            .collect::<Vec<_>>();
        if let Some((root, scheme)) = &self.partition_root {
            // One set of partition columns, in one order, under one root: a
            // file without `day=` beside files with it would have no value
            // for the column, which is ambiguous. Under a template, every
            // file that does not match is named at once (up to ten).
            let all = scheme.values_of_all(root, &files)?;
            let mut first: Option<(&String, Vec<&str>)> = None;
            for (p, values) in files.iter().zip(&all) {
                let names: Vec<&str> = values.iter().map(|v| v.name.as_str()).collect();
                match &first {
                    None => first = Some((p, names)),
                    Some((fp, f)) if *f != names => {
                        return Err(Error::Source(format!(
                            "{p} has partition columns {names:?}, but {fp} has {f:?}; every \
                             file under a hive-partitioned root needs the same partition \
                             directories"
                        )))
                    }
                    Some(_) => {}
                }
            }
        }
        files
            .into_iter()
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
        self.add_partition_stats(file, meta.schema(), &mut out)?;
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
        let partitions = self.partitions(&rg.file.path)?;
        let file_schema = Arc::clone(meta.schema());
        // Columns the path supplies and the file does not store are
        // synthesized; only the rest is decoded.
        let virtual_cols: Vec<&PartitionValue> = partitions
            .iter()
            .filter(|p| columns.contains(&p.name) && file_schema.field_with_name(&p.name).is_err())
            .collect();
        let stored: Vec<String> = columns
            .iter()
            .filter(|c| !virtual_cols.iter().any(|p| &p.name == *c))
            .cloned()
            .collect();
        let roots = resolve_columns(&file_schema, &stored)?;
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
        if virtual_cols.is_empty() {
            return reorder(&batch, columns);
        }
        if batch.num_rows() != rows {
            return Err(Error::Source(format!(
                "{}: row group {} decoded {} rows, the footer says {rows}",
                rg.file.path,
                rg.index_in_file,
                batch.num_rows()
            )));
        }
        let mut fields = Vec::with_capacity(columns.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
        for c in columns {
            if let Some(p) = virtual_cols.iter().find(|p| &p.name == c) {
                let target = self.partition_type(p, &file_schema)?.ok_or_else(|| {
                    Error::KeySpec(format!(
                        "column {c:?} is a partition column the dataset schema does not declare"
                    ))
                })?;
                let one = typed_value(&p.name, p.value.as_deref(), &target)?;
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
        let file_schema = self.footer(&fp)?.schema().clone();
        if self.partition_root.is_none() {
            return Ok(file_schema);
        }
        // Typing first checks every file under the root, so a template
        // refusal names every file that does not match.
        let inferred = self.inferred_types()?;
        let partitions = self.partitions(&first)?;
        // The file's columns, then each partition column the file does not
        // store, typed as its placeholder declares or by inference over
        // every file under the root.
        let mut fields: Vec<Field> = file_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        for p in &partitions {
            if file_schema.field_with_name(&p.name).is_err() {
                let t = inferred.get(&p.name).cloned().unwrap_or(DataType::Utf8);
                fields.push(Field::new(p.name.clone(), t, true));
            }
        }
        Ok(Arc::new(Schema::new_with_metadata(
            fields,
            file_schema.metadata().clone(),
        )))
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
            ColumnOrder::sort_order_for_type(None, ConvertedType::UTF8, Type::BYTE_ARRAY),
            SortOrder::UNSIGNED
        );
        assert_eq!(
            ColumnOrder::sort_order_for_type(None, ConvertedType::UINT_32, Type::INT32),
            SortOrder::UNSIGNED
        );
        assert_eq!(
            ColumnOrder::sort_order_for_type(None, ConvertedType::NONE, Type::INT64),
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

        // The same row group under column orders: a type-defined order keeps
        // the rules above, and an order the reader does not recognize
        // (parquet 59's `ColumnOrder::UNKNOWN`) trusts no bounds at all,
        // even modern ones on a signed column.
        let signed = ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED);
        let unsigned = ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::UNSIGNED);
        let ordered = |orders: Vec<ColumnOrder>| {
            let file = FileMetaData::new(1, 1, None, None, Arc::clone(&descr), Some(orders));
            ParquetMetaData::new(file, md.row_groups().to_vec())
        };
        let known = ordered(vec![unsigned, signed, unsigned]);
        let rg = known.row_group(0);
        assert!(!min_max_trusted(&known, rg, 0), "deprecated UTF8 stats");
        assert!(min_max_trusted(&known, rg, 1), "deprecated INT64 stats");
        assert!(min_max_trusted(&known, rg, 2), "modern UTF8 stats");
        let unknown = ordered(vec![ColumnOrder::UNKNOWN; 3]);
        let rg = unknown.row_group(0);
        for leaf in 0..3 {
            assert!(!min_max_trusted(&unknown, rg, leaf), "leaf {leaf}");
            assert_eq!(leaf_sort_order(&unknown, leaf), SortOrder::UNDEFINED);
        }
    }
}
