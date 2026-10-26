//! Transient segments for a hypothetical key spec, and their cache.
//!
//! [`TransientIndex::build`] reads only the candidate spec's key columns
//! (and its timestamp column, for a temporal spec), one row group at a time
//! in id order, and indexes them with core's [`SegmentWriter`] exactly as a
//! committed build would, at the dataset types the catalog carries. The
//! finished segments stay in memory and plan through the same
//! [`crate::PlanSource`] as committed ones
//! ([`crate::PlanSource::with_transient`]), so a hypothetical spec is costed
//! by the planner the index itself uses.
//!
//! # Memory (ADR 018)
//!
//! Each row group's keys are measured before they are added, the way the
//! build crate accounts its segments: distinct key tuples and their
//! canonical bytes (distinct `(key, ts)` pairs and 8 bytes more each for a
//! temporal spec, 8 bytes per distinct key for a bloom spec, twice the
//! longest key for a range spec, distinct tokens and their bytes for a
//! text spec). Summed over a segment's row groups these
//! bound what its open writer holds from above. While building, the process
//! holds:
//!
//! - one row group's projected key columns and its measured key set;
//! - one open writer, finished as soon as the next row group would take it
//!   past [`TransientLimits::max_segment_rows`] rows or
//!   [`TransientLimits::max_segment_key_bytes`] accounted key bytes (a single
//!   row group larger than a cap gets a segment of its own);
//! - the finished segments.
//!
//! Every segment records, in its own bytes, the catalog row groups its
//! writer processed (core's `SegmentWriter::set_rowgroup_range`; also
//! [`TransientIndex::segment_ranges`]), so a row group whose key columns are
//! null in every row holds no keys (ADR 014) but is still covered (ADR 012),
//! a segment that processed only such row groups is kept, key-less, and two
//! key-less segments over different row groups never share bytes. A planner
//! that takes the raw bytes ([`TransientIndex::segment_bytes`]) can check
//! coverage from them.
//!
//! Before each row group is added, the finished segments' bytes plus the
//! open writer's accounted key bytes plus the row group's must stay within
//! [`TransientLimits::max_index_bytes`]; otherwise the build stops with an
//! error, never a partial index. [`TransientCache`] holds finished indexes
//! up to its own byte cap and evicts the least recently used. A
//! configuration that needs several hypothetical specs holds all of their
//! indexes at once (each within its cap), since one plan intersects them.

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Schema};
use bellwether_core::encoding::{self, KeyColumns};
use bellwether_core::segment::{
    validate_spec, ColumnType, SegmentReader, SegmentWriter, WriterOptions,
};
use bellwether_types::{Error, Hash, KeySpec, Result, RowGroupId, Source};
use bytes::Bytes;
use roaring::RoaringBitmap;

use crate::catalog::Catalog;
use crate::plan::{SegmentBytes, SpecSegments};

/// Caps on transient builds and the layout model (module docs).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransientLimits {
    /// Rows one open segment writer may take before it is finished.
    pub max_segment_rows: u64,
    /// Accounted key bytes one open segment writer may take before it is
    /// finished (the build crate's `max_segment_bytes` accounting).
    pub max_segment_key_bytes: u64,
    /// Finished segment bytes plus the open writer's accounted key bytes
    /// one transient index (or one layout model) may hold.
    pub max_index_bytes: u64,
    /// Accounted bytes of the layout model's distinct-cluster-key map: each
    /// distinct key counts its bytes plus [`CLUSTER_KEY_OVERHEAD`].
    pub max_cluster_key_bytes: u64,
}

/// Bytes each distinct cluster key is charged beyond its own: the map node,
/// the key's allocation header, and its position counter.
pub const CLUSTER_KEY_OVERHEAD: u64 = 64;

impl Default for TransientLimits {
    fn default() -> Self {
        Self {
            max_segment_rows: 1 << 20,
            max_segment_key_bytes: 64 << 20,
            max_index_bytes: 1 << 30,
            max_cluster_key_bytes: 256 << 20,
        }
    }
}

/// What a transient build read and produced, for stating and testing its
/// bounds.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BuildStats {
    /// Row groups read.
    pub row_groups_read: u64,
    /// Rows read.
    pub rows_read: u64,
    /// The columns each read projected.
    pub columns_read: Vec<String>,
    /// Segments written.
    pub segments: u64,
    /// Most rows indexed by one segment.
    pub largest_segment_rows: u64,
    /// Most accounted key bytes taken by one open writer.
    pub largest_segment_key_bytes: u64,
    /// Total segment bytes.
    pub bytes: u64,
    /// Rows whose key is not indexed because its leading column is null
    /// (ADR 014); for an `expr` key, rows whose expression is null.
    pub null_rows: u64,
}

/// A hypothetical spec's segments over every live row group of one catalog.
#[derive(Debug)]
pub struct TransientIndex {
    manifest_version: String,
    catalog_digest: Hash,
    segments: Arc<SpecSegments>,
    /// First and last row group each segment's writer processed.
    ranges: Vec<(RowGroupId, RowGroupId)>,
    stats: BuildStats,
}

/// The dataset types of a spec's key columns (a list key's element type)
/// and, for a temporal spec, its timestamp column; for an `expr` key, its
/// expression's value type: core's one definition, the types the segment
/// writer records.
pub(crate) fn spec_types(
    spec: &KeySpec,
    schema: &Schema,
) -> Result<(Vec<DataType>, Option<DataType>)> {
    bellwether_core::segment::spec_types(spec, schema)
}

/// Measures one row group's keys for `spec` (module docs): `(keys, bytes)`.
pub(crate) fn measure(
    spec: &KeySpec,
    keys: &[DataType],
    ts: Option<&DataType>,
    batch: &RecordBatch,
) -> Result<(u64, u64)> {
    let column = |name: &str| -> Result<ArrayRef> {
        batch
            .column_by_name(name)
            .cloned()
            .ok_or_else(|| Error::KeySpec(format!("column {name} not in batch")))
    };
    if let KeySpec::List { column: c } = spec {
        // A list key: the row group's distinct element keys and their bytes.
        let elements = bellwether_core::list::ListElements::new(&column(c)?)?;
        let mut seen: HashSet<Vec<u8>> = HashSet::new();
        let mut bytes = 0u64;
        elements.for_each_encoded(&keys[0], |_, k| {
            if !seen.contains(k) {
                bytes += k.len() as u64;
                seen.insert(k.to_vec());
            }
        })?;
        return Ok((seen.len() as u64, bytes));
    }
    if let KeySpec::Text {
        column: c,
        tokenizer,
        bloom,
    } = spec
    {
        return bellwether_core::text::measure(*tokenizer, bloom.is_some(), column(c)?.as_ref());
    }
    let cols = spec
        .columns()
        .iter()
        .map(|c| column(c))
        .collect::<Result<Vec<_>>>()?;
    let kc = KeyColumns::new(&cols, keys)?;
    let ts_col = match (spec, ts) {
        (KeySpec::Temporal { ts_column, .. }, Some(dt)) => Some((column(ts_column)?, dt)),
        _ => None,
    };
    let mut buf = Vec::new();
    if matches!(spec, KeySpec::Range { .. }) {
        let mut longest = None;
        for row in 0..batch.num_rows() {
            if kc.encode_into(row, &mut buf)? {
                longest = longest.max(Some(buf.len() as u64));
            }
        }
        return Ok(longest.map_or((0, 0), |l| (1, 2 * l)));
    }
    let mut seen: HashSet<Vec<u8>> = HashSet::new();
    let mut bytes = 0u64;
    for row in 0..batch.num_rows() {
        if !kc.encode_into(row, &mut buf)? {
            continue;
        }
        let key_len = buf.len() as u64;
        if let Some((arr, dt)) = &ts_col {
            match encoding::ts_value_as(arr.as_ref(), row, dt)? {
                Some(t) => {
                    buf.push(1);
                    buf.extend_from_slice(&t.to_be_bytes());
                }
                None => buf.push(0),
            }
        }
        if seen.insert(buf.clone()) {
            bytes += match spec.physical() {
                KeySpec::Bloom { .. } => 8,
                KeySpec::Temporal { .. } => key_len + 8,
                _ => key_len,
            };
        }
    }
    Ok((seen.len() as u64, bytes))
}

impl TransientIndex {
    /// Builds `spec`'s segments over every row group of `catalog`, reading
    /// key columns through `source` and encoding them at the dataset types
    /// the catalog carries (module docs for the memory bound). A row group
    /// whose read returns a different row count than its statistics is an
    /// error.
    pub fn build(
        source: &dyn Source,
        catalog: &Catalog,
        spec: &KeySpec,
        limits: &TransientLimits,
    ) -> Result<Self> {
        validate_spec(spec)?;
        if limits.max_segment_rows == 0 || limits.max_segment_key_bytes == 0 {
            return Err(Error::Other("segment caps must be positive".into()));
        }
        let schema = catalog.schema();
        let (key_types, ts_type) = spec_types(spec, schema)?;
        // An expr key reads its expression's input columns and derives the
        // virtual column one row group at a time (ADR 018).
        let mut columns: Vec<String> = bellwether_core::expr::input_columns(spec)?;
        if let KeySpec::Temporal { ts_column, .. } = spec {
            if !columns.contains(ts_column) {
                columns.push(ts_column.clone());
            }
        }
        let opts = WriterOptions::default();
        let mut build = Build {
            spec,
            schema,
            limits,
            stats: BuildStats {
                columns_read: columns.clone(),
                ..BuildStats::default()
            },
            readers: Vec::new(),
            ranges: Vec::new(),
        };
        let mut writer = SegmentWriter::for_dataset(spec.clone(), schema, opts.clone())?;
        let (mut open_rows, mut open_bytes) = (0u64, 0u64);
        // First and last row group the open writer processed.
        let mut open: Option<(RowGroupId, RowGroupId)> = None;
        for rg in catalog.row_groups() {
            let batch = source.read_columns(&rg.meta, &columns)?;
            let batch = bellwether_core::expr::with_derived(spec, schema, &batch)?;
            let rows = batch.num_rows() as u64;
            if rows != rg.meta.rows {
                return Err(Error::Source(format!(
                    "{} row group {} read {rows} rows but its statistics say {}",
                    rg.meta.file.path, rg.meta.index_in_file, rg.meta.rows
                )));
            }
            let (_, bytes) = measure(spec, &key_types, ts_type.as_ref(), &batch)?;
            let cut = open_rows > 0
                && (open_rows + rows > limits.max_segment_rows
                    || open_bytes + bytes > limits.max_segment_key_bytes);
            if let (true, Some(range)) = (cut, open) {
                let full = std::mem::replace(
                    &mut writer,
                    SegmentWriter::for_dataset(spec.clone(), schema, opts.clone())?,
                );
                build.finish(full, range, open_rows, open_bytes)?;
                open = None;
                open_rows = 0;
                open_bytes = 0;
            }
            if build.stats.bytes + open_bytes + bytes > limits.max_index_bytes {
                return Err(Error::Other(format!(
                    "transient index for {} would hold {} bytes (finished segments {}, open \
                     writer {}, next row group {}), past its {} byte cap; raise \
                     TransientLimits::max_index_bytes or replay a narrower spec",
                    spec.name(),
                    build.stats.bytes + open_bytes + bytes,
                    build.stats.bytes,
                    open_bytes,
                    bytes,
                    limits.max_index_bytes
                )));
            }
            writer.add_batch(rg.id, &batch)?;
            open = Some((open.map_or(rg.id, |(first, _)| first), rg.id));
            open_rows += rows;
            open_bytes += bytes;
            build.stats.row_groups_read += 1;
            build.stats.rows_read += rows;
            if let Some(lead) = spec.columns().first() {
                build.stats.null_rows += batch
                    .column_by_name(lead)
                    .map_or(0, |c| c.logical_null_count() as u64);
            }
        }
        // The open writer is finished whenever it processed a row group,
        // even one that gave it no keys.
        if let Some(range) = open {
            build.finish(writer, range, open_rows, open_bytes)?;
        }
        let Build {
            stats,
            readers,
            ranges,
            ..
        } = build;
        Ok(Self {
            manifest_version: catalog.manifest_version().to_string(),
            catalog_digest: catalog.digest(),
            segments: Arc::new(SpecSegments::new(spec.clone(), readers)?),
            ranges,
            stats,
        })
    }

    /// The first and last catalog row group each segment's writer
    /// processed, inclusive, in segment order. Every row group of the
    /// catalog lies in exactly one, including a row group whose key columns
    /// are null in every row (module docs).
    #[must_use]
    pub fn segment_ranges(&self) -> &[(RowGroupId, RowGroupId)] {
        &self.ranges
    }

    /// Every row group id inside some segment's processed range.
    #[must_use]
    pub fn covered(&self) -> RoaringBitmap {
        let mut out = RoaringBitmap::new();
        for (first, last) in &self.ranges {
            out.insert_range(first.0..=last.0);
        }
        out
    }

    /// The spec indexed.
    #[must_use]
    pub fn spec(&self) -> &KeySpec {
        self.segments.spec()
    }

    /// The manifest version of the catalog it was built over.
    #[must_use]
    pub fn manifest_version(&self) -> &str {
        &self.manifest_version
    }

    /// The digest of the catalog it was built over.
    #[must_use]
    pub fn catalog_digest(&self) -> Hash {
        self.catalog_digest
    }

    /// Total segment bytes held.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.stats.bytes
    }

    /// What the build read and produced.
    #[must_use]
    pub fn stats(&self) -> &BuildStats {
        &self.stats
    }

    /// For an exact spec (or an `expr` key over exact), the distinct keys
    /// the index holds, as `(low, high)`: exact (`low == high`) when one
    /// segment holds them all; otherwise each segment's key count is exact
    /// but a key may sit in several, so the distinct count lies between the
    /// largest segment's and their sum. `None` for any other spec.
    #[must_use]
    pub fn distinct_keys(&self) -> Option<(u64, u64)> {
        if !matches!(self.spec().physical(), KeySpec::Exact { .. }) {
            return None;
        }
        let readers = self.segments.readers();
        let high = readers.iter().map(SegmentReader::entries).sum();
        let low = match readers {
            [_] | [] => high,
            _ => readers
                .iter()
                .map(SegmentReader::entries)
                .max()
                .unwrap_or(0),
        };
        Some((low, high))
    }

    /// The segments, for planners.
    #[must_use]
    pub fn segments(&self) -> Arc<SpecSegments> {
        Arc::clone(&self.segments)
    }

    /// The raw bytes of every segment, as a planner that opens segments
    /// itself (the build crate's `Planner::plan_with_transient`) takes
    /// them.
    #[must_use]
    pub fn segment_bytes(&self) -> Vec<Bytes> {
        self.segments
            .readers()
            .iter()
            .filter_map(|r| match r.source() {
                SegmentBytes::Memory(b) => Some(b.clone()),
                SegmentBytes::Ranged(_) => None,
            })
            .collect()
    }

    /// For an exact or temporal spec: each probe key's row groups, unioned
    /// across segments. Keys absent from the index are absent from the map.
    pub fn get(
        &self,
        probes: &[Vec<u8>],
    ) -> Result<std::collections::BTreeMap<Vec<u8>, RoaringBitmap>> {
        let mut out: std::collections::BTreeMap<Vec<u8>, RoaringBitmap> =
            std::collections::BTreeMap::new();
        for r in self.segments.readers() {
            for (k, v) in r.get(probes)? {
                *out.entry(k).or_default() |= v.bitmap;
            }
        }
        Ok(out)
    }
}

/// The state of one transient build: finished segments and counters.
struct Build<'a> {
    spec: &'a KeySpec,
    schema: &'a Schema,
    limits: &'a TransientLimits,
    stats: BuildStats,
    readers: Vec<SegmentReader<SegmentBytes>>,
    ranges: Vec<(RowGroupId, RowGroupId)>,
}

impl Build<'_> {
    /// Finishes one writer that processed the row groups `range` (first and
    /// last), took `rows` rows and `key_bytes` accounted key bytes,
    /// enforcing the index byte cap before keeping the segment.
    fn finish(
        &mut self,
        mut writer: SegmentWriter,
        range: (RowGroupId, RowGroupId),
        rows: u64,
        key_bytes: u64,
    ) -> Result<()> {
        // The segment records the row groups it processed, keys or not, so
        // it claims coverage of exactly them and two key-less segments over
        // different row groups never share bytes (docs/format.md).
        writer.set_rowgroup_range(range.0..RowGroupId(range.1 .0 + 1))?;
        let seg = writer.finish()?;
        if seg.rowgroup_range != Some(range) {
            return Err(Error::Other(format!(
                "a transient segment of {} records row groups {:?}, not the {range:?} it processed",
                self.spec.name(),
                seg.rowgroup_range
            )));
        }
        self.stats.bytes += seg.bytes.len() as u64;
        if self.stats.bytes > self.limits.max_index_bytes {
            return Err(Error::Other(format!(
                "transient index for {} exceeds its {} byte cap after {} segments; raise \
                 TransientLimits::max_index_bytes or replay a narrower spec",
                self.spec.name(),
                self.limits.max_index_bytes,
                self.stats.segments + 1
            )));
        }
        self.stats.segments += 1;
        self.stats.largest_segment_rows = self.stats.largest_segment_rows.max(rows);
        self.stats.largest_segment_key_bytes = self.stats.largest_segment_key_bytes.max(key_bytes);
        self.readers.push(SegmentReader::open_for_dataset(
            SegmentBytes::Memory(Bytes::from(seg.bytes)),
            self.schema,
        )?);
        self.ranges.push(range);
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CacheKey {
    spec: String,
    types: String,
    manifest_version: String,
    catalog: Hash,
}

/// Counters of a [`TransientCache`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CacheStats {
    /// Lookups answered from the cache.
    pub hits: u64,
    /// Indexes built because the cache missed.
    pub builds: u64,
    /// Indexes evicted to stay under the byte cap.
    pub evictions: u64,
    /// Indexes held now.
    pub entries: u64,
    /// Bytes held now.
    pub bytes: u64,
}

#[derive(Debug, Default)]
struct CacheInner {
    entries: VecDeque<(CacheKey, Arc<TransientIndex>)>,
    stats: CacheStats,
}

/// Transient indexes by (full spec, the dataset types of its key and
/// timestamp columns, manifest sequence, catalog digest), so a repeated
/// what-if is a lookup. The full serialized spec is the key, not
/// [`KeySpec::name`], which omits a bloom spec's false-positive rate and a
/// temporal spec's timestamp column; the types keep an index encoded at one
/// decimal scale or time unit from serving a caller at another. Bounded by
/// bytes, least recently used first out; an index larger than the whole cap
/// is returned uncached. The cache lives in-process.
#[derive(Debug)]
pub struct TransientCache {
    max_bytes: u64,
    inner: Mutex<CacheInner>,
}

impl TransientCache {
    /// A cache holding at most `max_bytes` of segments.
    #[must_use]
    pub fn new(max_bytes: u64) -> Self {
        Self {
            max_bytes,
            inner: Mutex::new(CacheInner::default()),
        }
    }

    /// The cached index for `spec` over `catalog`, or the result of `build`.
    pub fn get_or_build(
        &self,
        catalog: &Catalog,
        spec: &KeySpec,
        build: impl FnOnce() -> Result<TransientIndex>,
    ) -> Result<Arc<TransientIndex>> {
        let (keys, ts) = spec_types(spec, catalog.schema())?;
        let recorded: Vec<ColumnType> = keys
            .iter()
            .chain(ts.iter())
            .map(ColumnType::of)
            .collect::<Result<_>>()?;
        let key = CacheKey {
            spec: serde_json::to_string(spec)?,
            types: serde_json::to_string(&recorded)?,
            manifest_version: catalog.manifest_version().to_string(),
            catalog: catalog.digest(),
        };
        {
            let mut inner = self.lock()?;
            if let Some(i) = inner.entries.iter().position(|(k, _)| k == &key) {
                if let Some(entry) = inner.entries.remove(i) {
                    let idx = Arc::clone(&entry.1);
                    inner.entries.push_back(entry);
                    inner.stats.hits += 1;
                    return Ok(idx);
                }
            }
        }
        let idx = Arc::new(build()?);
        let mut inner = self.lock()?;
        inner.stats.builds += 1;
        if idx.bytes() <= self.max_bytes {
            while inner.stats.bytes + idx.bytes() > self.max_bytes {
                let Some((_, old)) = inner.entries.pop_front() else {
                    break;
                };
                inner.stats.bytes -= old.bytes();
                inner.stats.entries -= 1;
                inner.stats.evictions += 1;
            }
            inner.stats.bytes += idx.bytes();
            inner.stats.entries += 1;
            inner.entries.push_back((key, Arc::clone(&idx)));
        }
        Ok(idx)
    }

    /// The counters.
    pub fn stats(&self) -> Result<CacheStats> {
        Ok(self.lock()?.stats)
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, CacheInner>> {
        self.inner
            .lock()
            .map_err(|_| Error::Other("transient cache lock poisoned".into()))
    }
}
