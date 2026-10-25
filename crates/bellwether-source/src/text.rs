//! [`ChunkedTextSource`]: CSV and JSON lines split into fixed byte-range
//! chunks that stand in for row groups.
//!
//! Chunk `k + 1` starts right after the first `\n` at or beyond
//! `start_k + chunk_size - 1`, so every chunk ends on a line boundary and
//! every chunk except the last is at least `chunk_size` bytes. The boundaries
//! are a pure function of the file bytes and `chunk_size`, so the same
//! fingerprint and chunk size always yield the same chunks. A chunk longer
//! than 4× `chunk_size` (a very long line, or CR-only line endings) is an
//! error rather than an unbounded read (ADR 018).
//!
//! The chunk size is part of a file's row-group identity. `read_columns`
//! verifies that the chunk it recomputes has exactly the byte length and row
//! count recorded in the `RowGroupMeta`, and errors otherwise. That check does
//! **not** pin the chunk's start offset. With fixed-width lines, chunks of
//! equal length and row count at different offsets are common, so reading
//! row groups through a different chunk size can still return the wrong rows
//! without an error. The caller must build the source with the chunk size
//! recorded in the manifest. Only a recorded byte range, compared at both
//! ends, closes this (see [`ChunkedTextSource::byte_range`]).
//!
//! CSV quoted fields that contain a newline are detected, not supported. Each
//! chunk is scanned with `csv_core`'s quoting rules (see [`csv_ends_in_quotes`]).
//! A chunk that ends inside a quoted field is rejected with `Error::Source`
//! instead of yielding phantom rows. A quote inside an unquoted field (an
//! inch mark such as `5'10"`) is a literal byte, as it is to the parser.

use std::io::{BufReader, Cursor};
use std::ops::Range;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::compute::concat_batches;
use arrow::datatypes::{Schema, SchemaRef};
use bellwether_types::{ColumnStats, Error, FileFingerprint, Result, RowGroupMeta, Source};
use bytes::Bytes;
use parquet::file::reader::ChunkReader;

use crate::canonical::{Canonical, DefaultCanonical, StatsAccumulator};
use crate::fingerprint::{fingerprint_with_policy, FingerprintPolicy};
use crate::parquet_source::has_extension;
use crate::storage::{LocalFs, Storage};
use crate::{open_verified, reorder, resolve_columns, BoundedCache};

/// Default chunk size: 64 MiB.
pub const DEFAULT_CHUNK_SIZE: u64 = 64 * 1024 * 1024;
/// A chunk may grow to this multiple of the chunk size while looking for a
/// newline before it is rejected.
pub const MAX_CHUNK_FACTOR: u64 = 4;
/// Records sampled from the head of a file to infer its schema.
pub const INFER_RECORDS: usize = 1000;
/// Upper bound on the head bytes read for schema inference (extended to
/// the next newline).
pub const INFER_HEAD_BYTES: u64 = 1024 * 1024;
/// Byte budget of each in-memory cache (layouts, scans).
pub const TEXT_CACHE_BYTES: usize = 64 * 1024 * 1024;
/// Rows per decoded batch while scanning one chunk.
const BATCH_ROWS: usize = 8192;
/// Window used when scanning forward for a newline.
const SCAN_WINDOW: u64 = 64 * 1024;

/// Which text format a [`ChunkedTextSource`] parses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TextFormat {
    /// Delimited text. Only the first chunk can hold the header row.
    Csv {
        /// Field delimiter, usually `b','`.
        delimiter: u8,
        /// Whether the first line of each file is a header.
        has_header: bool,
    },
    /// One JSON object per line.
    JsonLines,
}

impl TextFormat {
    /// Comma-separated with a header row.
    #[must_use]
    pub fn csv() -> Self {
        Self::Csv {
            delimiter: b',',
            has_header: true,
        }
    }

    /// File extensions [`ChunkedTextSource::discover`] lists for this format
    /// by name alone. JSON lines also takes `.json` files by content
    /// ([`Self::sniffed_extension`]).
    #[must_use]
    pub fn extensions(&self) -> &'static [&'static str] {
        match self {
            Self::Csv { .. } => &["csv"],
            Self::JsonLines => &["jsonl", "ndjson"],
        }
    }

    /// The extension of files this format reads only after checking their
    /// content ([`sniff_json_lines`]): `.json` for JSON lines, the name GH
    /// Archive and most exporters use for it.
    #[must_use]
    pub fn sniffed_extension(&self) -> Option<&'static str> {
        match self {
            Self::Csv { .. } => None,
            Self::JsonLines => Some("json"),
        }
    }

    /// The files this format reads, as a phrase for messages.
    #[must_use]
    pub fn accepted(&self) -> &'static str {
        match self {
            Self::Csv { .. } => "CSV files (.csv)",
            Self::JsonLines => {
                "JSON lines files (.jsonl, .ndjson, and .json files whose first byte is `{` \
                 and whose every line is one JSON object)"
            }
        }
    }

    /// Whether `path` is a file of this format: by its extension, or, for a
    /// `.json` file under JSON lines, by its content ([`sniff_json_lines`]).
    ///
    /// # Errors
    /// A `.json` file whose first line is one JSON object but a later line
    /// is not (damaged JSON lines, refused rather than left out), or a read
    /// failure.
    pub fn accepts<S: Storage>(&self, storage: &S, path: &str) -> Result<bool> {
        if self.extensions().iter().any(|e| has_extension(path, e)) {
            return Ok(true);
        }
        match self.sniffed_extension() {
            Some(e) if has_extension(path, e) => sniff_json_lines(storage, path),
            _ => Ok(false),
        }
    }

    /// Whether `path` has an extension this format lists or sniffs: the
    /// files [`Self::accepts`] looks at.
    #[must_use]
    pub fn candidate(&self, path: &str) -> bool {
        self.extensions()
            .iter()
            .chain(self.sniffed_extension().as_slice())
            .any(|e| has_extension(path, e))
    }
}

/// A sniffed file's identity: path, size, and version token.
type SniffKey = (String, u64, Option<String>);

/// The longest line [`sniff_json_lines`] reads, in bytes.
const MAX_SNIFF_LINE: usize = 256 * 1024 * 1024;

/// Whether `path` holds JSON lines: its first byte is `{` and every line
/// (blank lines aside) is exactly one JSON object. Reads the whole file in
/// 1 MiB windows, holding at most one line. A file whose first line is not
/// one object (a pretty-printed JSON document, such as an index's own
/// `manifest.json`) is not JSON lines: `Ok(false)`.
///
/// # Errors
/// The first line is one JSON object but a later line is not: the file is
/// JSON lines with a damaged line, so it is refused by name and line rather
/// than silently left out of the dataset. Also a read failure.
pub fn sniff_json_lines<S: Storage>(storage: &S, path: &str) -> Result<bool> {
    const WINDOW: u64 = 1024 * 1024;
    let size = storage.head(path)?.size;
    if size == 0 || storage.read_range(path, 0..1)?.first() != Some(&b'{') {
        return Ok(false);
    }
    // `Err(reason)` when a non-blank line is not exactly one JSON object.
    let check = |line: &[u8]| -> std::result::Result<(), String> {
        let t = line.trim_ascii();
        if t.is_empty() {
            return Ok(());
        }
        if t[0] != b'{' {
            return Err("it does not start with `{`".into());
        }
        serde_json::from_slice::<serde::de::IgnoredAny>(t)
            .map(|_| ())
            .map_err(|e| e.to_string())
    };
    // Line 1 decides whether this is JSON lines at all; a later bad line is
    // damage, and refused.
    let verdict = |line_no: u64, why: String| -> Result<bool> {
        if line_no == 1 {
            return Ok(false);
        }
        Err(Error::Source(format!(
            "{path}: this .json file holds JSON lines (its first line is one JSON object), but \
             line {line_no} is not one ({why}); a .json file is read as JSON lines only when \
             every line is one JSON object, so repair that line, or move the file out of the \
             folder"
        )))
    };
    let mut pending: Vec<u8> = Vec::new();
    let mut line_no = 1u64;
    let mut pos = 0u64;
    while pos < size {
        let window = storage.read_range(path, pos..size.min(pos + WINDOW))?;
        if window.is_empty() {
            break;
        }
        pos += window.len() as u64;
        let mut rest = &window[..];
        while let Some(i) = rest.iter().position(|&b| b == b'\n') {
            let checked = if pending.is_empty() {
                check(&rest[..i])
            } else {
                pending.extend_from_slice(&rest[..i]);
                let c = check(&pending);
                pending.clear();
                c
            };
            if let Err(why) = checked {
                return verdict(line_no, why);
            }
            line_no += 1;
            rest = &rest[i + 1..];
        }
        pending.extend_from_slice(rest);
        if pending.len() > MAX_SNIFF_LINE {
            return verdict(line_no, format!("it is longer than {MAX_SNIFF_LINE} bytes"));
        }
    }
    match check(&pending) {
        Ok(()) => Ok(true),
        Err(why) => verdict(line_no, why),
    }
}

/// Schema and chunk boundaries of one file.
#[derive(Debug)]
struct Layout {
    schema: SchemaRef,
    chunks: Vec<Range<u64>>,
}

impl Layout {
    fn approx_bytes(&self) -> usize {
        self.chunks.len() * std::mem::size_of::<Range<u64>>()
            + self.schema.fields().len() * 128
            + 64
    }
}

fn rgs_approx_bytes(rgs: &[RowGroupMeta]) -> usize {
    rgs.iter()
        .map(|rg| {
            128 + rg.file.path.len()
                + rg.stats
                    .iter()
                    .map(|s| {
                        64 + s.column.len()
                            + s.min.as_ref().map_or(0, Vec::len)
                            + s.max.as_ref().map_or(0, Vec::len)
                    })
                    .sum::<usize>()
        })
        .sum()
}

/// A [`Source`] over CSV or JSON lines files.
///
/// `row_groups` scans each file once. It counts rows and computes statistics
/// for the configured `stats_columns`, and caches the result in memory
/// (bounded by bytes). Columns outside `stats_columns` are reported with
/// unknown (`None`) statistics.
///
/// Column types come from [`with_schema`](Self::with_schema) when given, for
/// both formats. Otherwise they are inferred per file, and inferred types may
/// differ between formats: CSV infers ISO timestamps as `Timestamp`, while
/// JSON lines keeps them as `Utf8`.
#[derive(Debug)]
pub struct ChunkedTextSource<S: Storage = LocalFs> {
    storage: S,
    format: TextFormat,
    chunk_size: u64,
    stats_columns: Vec<String>,
    dataset: Option<SchemaRef>,
    policy: FingerprintPolicy,
    canonical: Arc<dyn Canonical>,
    layouts: Mutex<BoundedCache<FileFingerprint, Arc<Layout>>>,
    scans: Mutex<BoundedCache<FileFingerprint, Vec<RowGroupMeta>>>,
    /// Content checks of sniffed files ([`sniff_json_lines`]) by path, size,
    /// and version token, so a folder listed twice is read once.
    sniffed: Mutex<std::collections::HashMap<SniffKey, bool>>,
    /// Directories whose files are never data ([`Self::with_excluded`]).
    excluded: Vec<String>,
}

impl ChunkedTextSource<LocalFs> {
    /// A source over the local filesystem with the default chunk size and no
    /// statistics columns.
    #[must_use]
    pub fn new(format: TextFormat) -> Self {
        Self::with_storage(LocalFs, format)
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn src_err(path: &str, e: impl std::fmt::Display) -> Error {
    Error::Source(format!("{path}: {e}"))
}

fn get_bytes<R: ChunkReader>(reader: &R, path: &str, range: Range<u64>) -> Result<Bytes> {
    let len = usize::try_from(range.end - range.start).map_err(|e| src_err(path, e))?;
    reader
        .get_bytes(range.start, len)
        .map_err(|e| src_err(path, e))
}

impl<S: Storage> ChunkedTextSource<S> {
    /// A source over any [`Storage`].
    pub fn with_storage(storage: S, format: TextFormat) -> Self {
        Self {
            storage,
            format,
            chunk_size: DEFAULT_CHUNK_SIZE,
            stats_columns: Vec::new(),
            dataset: None,
            policy: FingerprintPolicy::Tail64K,
            canonical: Arc::new(DefaultCanonical),
            layouts: Mutex::new(BoundedCache::new(TEXT_CACHE_BYTES)),
            scans: Mutex::new(BoundedCache::new(TEXT_CACHE_BYTES)),
            sniffed: Mutex::new(std::collections::HashMap::new()),
            excluded: Vec::new(),
        }
    }

    /// Never lists a file under `dir`: discovery and schema inference skip
    /// it. For an index location inside the data root, whose own `.json`
    /// files (a single-line JSON object is valid JSON lines) would otherwise
    /// be read as data. `dir` is compared as listed paths are spelled.
    #[must_use]
    pub fn with_excluded(mut self, dir: impl Into<String>) -> Self {
        let dir = dir.into();
        self.excluded.push(dir.trim_end_matches('/').to_string());
        self
    }

    fn is_excluded(&self, path: &str) -> bool {
        self.excluded.iter().any(|d| {
            path.strip_prefix(d.as_str())
                .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
        })
    }

    /// [`TextFormat::accepts`], remembering each sniffed file's verdict for
    /// its size and version token.
    fn accepts(&self, path: &str) -> Result<bool> {
        if !self.format.candidate(path) || self.is_excluded(path) {
            return Ok(false);
        }
        if self
            .format
            .extensions()
            .iter()
            .any(|e| has_extension(path, e))
        {
            return Ok(true);
        }
        let meta = self.storage.head(path)?;
        let key = (path.to_string(), meta.size, meta.etag);
        if let Some(v) = lock(&self.sniffed).get(&key) {
            return Ok(*v);
        }
        let v = self.format.accepts(&self.storage, path)?;
        lock(&self.sniffed).insert(key, v);
        Ok(v)
    }

    /// Sets the chunk size in bytes. Values below 1 are raised to 1. Row
    /// groups built at one chunk size cannot be read at another.
    #[must_use]
    pub fn with_chunk_size(mut self, bytes: u64) -> Self {
        self.chunk_size = bytes.max(1);
        self
    }

    /// Columns whose min/max/null count are computed on the first scan.
    #[must_use]
    pub fn with_stats_columns<I, C>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = C>,
        C: Into<String>,
    {
        self.stats_columns = columns.into_iter().map(Into::into).collect();
        self
    }

    /// Sets the dataset schema, used instead of inference to parse every
    /// file (CSV and JSON lines alike) and to encode statistics.
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

    /// Replaces the statistics encoder.
    #[must_use]
    pub fn with_canonical(mut self, canonical: Arc<dyn Canonical>) -> Self {
        self.canonical = canonical;
        self
    }

    /// The configured chunk size.
    #[must_use]
    pub fn chunk_size(&self) -> u64 {
        self.chunk_size
    }

    /// Byte ranges of each chunk of `file`, in order. They are contiguous,
    /// cover the file, and each ends just after a `\n` or at end of file.
    pub fn chunk_ranges(&self, file: &FileFingerprint) -> Result<Vec<Range<u64>>> {
        if let Some(hit) = lock(&self.layouts).get(file) {
            return Ok(hit.chunks.clone());
        }
        let reader = open_verified(&self.storage, file, self.policy)?;
        Ok(self.layout_from(file, &reader)?.chunks.clone())
    }

    /// The byte range of one row group: the value the planned
    /// `RowGroupMeta.byte_range` field will carry.
    pub fn byte_range(&self, rg: &RowGroupMeta) -> Result<Range<u64>> {
        let chunks = self.chunk_ranges(&rg.file)?;
        let idx = usize::try_from(rg.index_in_file).unwrap_or(usize::MAX);
        chunks.get(idx).cloned().ok_or_else(|| {
            Error::Source(format!(
                "{}: chunk {} out of range ({} chunks)",
                rg.file.path,
                rg.index_in_file,
                chunks.len()
            ))
        })
    }

    /// The layout of `file`, computed from `reader` (already verified
    /// against `file`) on a cache miss.
    fn layout_from(&self, file: &FileFingerprint, reader: &S::Reader) -> Result<Arc<Layout>> {
        if let Some(hit) = lock(&self.layouts).get(file) {
            return Ok(hit);
        }
        let chunks = self.compute_chunks(reader, &file.path, file.size)?;
        let schema = if let Some(s) = &self.dataset {
            self.check_csv_header(reader, &file.path, file.size, s)?;
            Arc::clone(s)
        } else if file.size == 0 {
            Arc::new(Schema::empty())
        } else {
            // Inference reads a newline-aligned head of the file that does not
            // depend on the chunk size, so every chunk size yields one schema.
            let head_end = if file.size <= INFER_HEAD_BYTES {
                file.size
            } else {
                Self::after_newline(reader, &file.path, INFER_HEAD_BYTES - 1, file.size)?
            };
            self.infer(reader, &file.path, 0..head_end)?
        };
        let layout = Arc::new(Layout { schema, chunks });
        let size = layout.approx_bytes();
        lock(&self.layouts).insert(file.clone(), Arc::clone(&layout), size);
        Ok(layout)
    }

    /// CSV maps columns by position. With a supplied schema and a header row,
    /// the header names must equal the schema's names in order, or columns of
    /// the same type would be silently swapped.
    fn check_csv_header(
        &self,
        reader: &S::Reader,
        path: &str,
        size: u64,
        schema: &Schema,
    ) -> Result<()> {
        let TextFormat::Csv {
            delimiter,
            has_header: true,
        } = self.format
        else {
            return Ok(());
        };
        if size == 0 {
            return Ok(());
        }
        let end = Self::after_newline(reader, path, 0, size.min(INFER_HEAD_BYTES))?;
        let head = get_bytes(reader, path, 0..end)?;
        let header = arrow::csv::reader::Format::default()
            .with_header(true)
            .with_delimiter(delimiter)
            .infer_schema(Cursor::new(&head[..]), Some(0))
            .map_err(|e| src_err(path, e))?
            .0;
        let got: Vec<&str> = header.fields().iter().map(|f| f.name().as_str()).collect();
        let want: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        if got != want {
            return Err(Error::Source(format!(
                "{path}: CSV header {got:?} does not match the dataset schema {want:?}"
            )));
        }
        Ok(())
    }

    fn compute_chunks(&self, reader: &S::Reader, path: &str, size: u64) -> Result<Vec<Range<u64>>> {
        let cap = self.chunk_size.saturating_mul(MAX_CHUNK_FACTOR);
        let mut out = Vec::new();
        let mut start = 0;
        while start < size {
            let target = start.saturating_add(self.chunk_size);
            let end = if target >= size {
                size
            } else {
                // Never scan further than the cap for a newline.
                let limit = size.min(start.saturating_add(cap).saturating_add(1));
                Self::after_newline(reader, path, target - 1, limit)?
            };
            if end - start > cap {
                return Err(Error::Source(format!(
                    "{path}: no newline within {cap} bytes of offset {start} \
                     ({MAX_CHUNK_FACTOR}x chunk size {}); a line is too long or the \
                     file uses CR-only line endings",
                    self.chunk_size
                )));
            }
            out.push(start..end);
            start = end;
        }
        Ok(out)
    }

    /// Offset just past the first `\n` at or after `from`, or `limit`.
    fn after_newline(reader: &S::Reader, path: &str, from: u64, limit: u64) -> Result<u64> {
        let mut pos = from;
        while pos < limit {
            let window = get_bytes(reader, path, pos..limit.min(pos + SCAN_WINDOW))?;
            if window.is_empty() {
                break;
            }
            if let Some(i) = window.iter().position(|&b| b == b'\n') {
                return Ok(pos + u64::try_from(i).unwrap_or(0) + 1);
            }
            pos += u64::try_from(window.len()).unwrap_or(SCAN_WINDOW);
        }
        Ok(limit)
    }

    fn infer(&self, reader: &S::Reader, path: &str, head: Range<u64>) -> Result<SchemaRef> {
        let head = get_bytes(reader, path, head)?;
        let schema = match self.format {
            TextFormat::Csv {
                delimiter,
                has_header,
            } => {
                arrow::csv::reader::Format::default()
                    .with_header(has_header)
                    .with_delimiter(delimiter)
                    .infer_schema(Cursor::new(&head[..]), Some(INFER_RECORDS))
                    .map_err(|e| src_err(path, e))?
                    .0
            }
            TextFormat::JsonLines => {
                arrow::json::reader::infer_json_schema(
                    BufReader::new(Cursor::new(&head[..])),
                    Some(INFER_RECORDS),
                )
                .map_err(|e| src_err(path, e))?
                .0
            }
        };
        Ok(Arc::new(schema))
    }

    /// Decodes chunk `idx`, projected to file-schema column `indices`
    /// (sorted, deduplicated), handing each batch to `f`. Holds one chunk's
    /// raw bytes and one decoded batch at a time.
    fn scan_chunk(
        &self,
        reader: &S::Reader,
        path: &str,
        layout: &Layout,
        idx: usize,
        indices: Vec<usize>,
        mut f: impl FnMut(RecordBatch) -> Result<()>,
    ) -> Result<()> {
        let range = layout.chunks[idx].clone();
        let bytes = get_bytes(reader, path, range.clone())?;
        match self.format {
            TextFormat::Csv {
                delimiter,
                has_header,
            } => {
                // Chunks start at record boundaries by induction: chunk 0 starts
                // at offset 0, and every earlier chunk was checked to end
                // outside a quoted field, i.e. on a record terminator.
                if csv_ends_in_quotes(&bytes, delimiter) {
                    return Err(Error::Source(format!(
                        "{path}: chunk {idx} ({range:?}) ends inside a quoted CSV field; \
                         quoted fields containing newlines are not supported"
                    )));
                }
                let reader = arrow::csv::ReaderBuilder::new(Arc::clone(&layout.schema))
                    .with_header(has_header && idx == 0)
                    .with_delimiter(delimiter)
                    .with_projection(indices)
                    .with_batch_size(BATCH_ROWS)
                    .build(Cursor::new(&bytes[..]))
                    .map_err(|e| src_err(path, e))?;
                for batch in reader {
                    f(batch.map_err(|e| src_err(path, e))?)?;
                }
            }
            TextFormat::JsonLines => {
                let projected = layout
                    .schema
                    .project(&indices)
                    .map_err(|e| src_err(path, e))?;
                let reader = arrow::json::ReaderBuilder::new(Arc::new(projected))
                    .with_batch_size(BATCH_ROWS)
                    .build(Cursor::new(&bytes[..]))
                    .map_err(|e| src_err(path, e))?;
                for batch in reader {
                    f(batch.map_err(|e| src_err(path, e))?)?;
                }
            }
        }
        Ok(())
    }

    /// Column indices to decode: the requested ones, or the first column
    /// alone when none are requested (rows must still be counted).
    fn decode_indices(schema: &Schema, requested: &[usize]) -> Vec<usize> {
        if requested.is_empty() && !schema.fields().is_empty() {
            vec![0]
        } else {
            requested.to_vec()
        }
    }

    fn scan_file(&self, file: &FileFingerprint) -> Result<Vec<RowGroupMeta>> {
        let reader = open_verified(&self.storage, file, self.policy)?;
        let layout = self.layout_from(file, &reader)?;
        let schema = &layout.schema;
        let stat_idx = resolve_columns(schema, &self.stats_columns)?;
        let decode = Self::decode_indices(schema, &stat_idx);
        let mut out = Vec::with_capacity(layout.chunks.len());
        for (k, range) in layout.chunks.iter().enumerate() {
            let mut rows = 0u64;
            let mut accs: Vec<StatsAccumulator> = vec![StatsAccumulator::default(); stat_idx.len()];
            self.scan_chunk(&reader, &file.path, &layout, k, decode.clone(), |batch| {
                rows += u64::try_from(batch.num_rows()).unwrap_or(0);
                for (acc, &col) in accs.iter_mut().zip(&stat_idx) {
                    let field = schema.field(col);
                    if let Some(array) = batch.column_by_name(field.name()) {
                        acc.update(self.canonical.as_ref(), array.as_ref(), field.data_type());
                    }
                }
                Ok(())
            })?;
            let stats = schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, field)| {
                    let acc = stat_idx.iter().position(|&s| s == i).map(|p| &accs[p]);
                    let (min, max) = acc.map_or((None, None), StatsAccumulator::bounds);
                    ColumnStats {
                        column: field.name().clone(),
                        min,
                        max,
                        null_count: acc.map(|a| a.nulls),
                        distinct_count: None,
                        has_bloom: false,
                        bytes: None,
                    }
                })
                .collect();
            out.push(RowGroupMeta {
                id: None,
                file: file.clone(),
                index_in_file: u32::try_from(k)
                    .map_err(|_| Error::Source(format!("{}: too many chunks", file.path)))?,
                rows,
                bytes: range.end - range.start,
                stats,
                byte_range: Some((range.start, range.end)),
            });
        }
        Ok(out)
    }
}

/// Quoting state of [`csv_ends_in_quotes`]. This is `csv_core`'s reader NFA, as
/// arrow-csv configures it (quote `"`, doubled-quote escaping, no escape
/// byte, no comments, CR / LF / CRLF terminators), reduced to the states that
/// decide whether a byte is inside a quoted field. Record-start, field-end,
/// and CRLF states all behave like `FieldStart` for that question.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CsvQuoting {
    /// At the start of a field or record: a quote opens a quoted field.
    FieldStart,
    /// Inside an unquoted field: a quote is a literal byte.
    Unquoted,
    /// Inside a quoted field: delimiters and terminators are data.
    Quoted,
    /// Just read a quote inside a quoted field: a second quote is an escaped
    /// quote, a delimiter or terminator closes the field, and any other byte
    /// continues the field unquoted.
    QuoteInQuoted,
}

/// Whether `bytes`, parsed from a record boundary with `csv_core`'s rules,
/// ends inside a quoted field.
///
/// The rules: a quote opens a quoted field only at the start of a field.
/// Inside a quoted field, `""` is an escaped quote, a quote followed by the
/// delimiter or a terminator (`\r` or `\n`) closes the field, and a quote
/// followed by any other byte drops back to an unquoted field. A quote in the
/// middle of an unquoted field is literal.
#[must_use]
pub fn csv_ends_in_quotes(bytes: &[u8], delimiter: u8) -> bool {
    use CsvQuoting::{FieldStart, QuoteInQuoted, Quoted, Unquoted};
    let ends_field = |c: u8| c == delimiter || c == b'\n' || c == b'\r';
    let mut state = FieldStart;
    for &c in bytes {
        state = match (state, c) {
            (Quoted, b'"') => QuoteInQuoted,
            (FieldStart | QuoteInQuoted, b'"') | (Quoted, _) => Quoted,
            (_, c) if ends_field(c) => FieldStart,
            _ => Unquoted,
        };
    }
    state == Quoted
}

impl<S: Storage> Source for ChunkedTextSource<S> {
    fn discover(&self, root: &str) -> Result<Vec<FileFingerprint>> {
        let mut out = Vec::new();
        // A sniffed `.json` file that is not JSON lines is refused by name,
        // never silently left out of the dataset beside the files read.
        let mut not_lines = Vec::new();
        for p in self.storage.list(root)? {
            if self.accepts(&p)? {
                out.push(fingerprint_with_policy(&self.storage, &p, self.policy)?);
            } else if self.format.candidate(&p) && !self.is_excluded(&p) {
                not_lines.push(p);
            }
        }
        if let Some(first) = not_lines.first() {
            return Err(Error::Source(format!(
                "{first}{}: a .json file here is not JSON lines (its first line is not one JSON \
                 object), so it cannot be read with the other files; this format reads {}. Move \
                 it out of the folder, or convert it to one JSON object per line",
                if not_lines.len() > 1 {
                    format!(" and {} more", not_lines.len() - 1)
                } else {
                    String::new()
                },
                self.format.accepted()
            )));
        }
        Ok(out)
    }

    fn row_groups(&self, file: &FileFingerprint) -> Result<Vec<RowGroupMeta>> {
        if let Some(hit) = lock(&self.scans).get(file) {
            return Ok(hit);
        }
        let scanned = self.scan_file(file)?;
        let size = rgs_approx_bytes(&scanned);
        lock(&self.scans).insert(file.clone(), scanned.clone(), size);
        Ok(scanned)
    }

    fn read_columns(&self, rg: &RowGroupMeta, columns: &[String]) -> Result<RecordBatch> {
        let path = &rg.file.path;
        let reader = open_verified(&self.storage, &rg.file, self.policy)?;
        let layout = self.layout_from(&rg.file, &reader)?;
        let idx = usize::try_from(rg.index_in_file).unwrap_or(usize::MAX);
        let Some(range) = layout.chunks.get(idx) else {
            return Err(Error::Source(format!(
                "{path}: chunk {} out of range ({} chunks); was it built with another chunk size?",
                rg.index_in_file,
                layout.chunks.len()
            )));
        };
        if range.end - range.start != rg.bytes {
            return Err(Error::Source(format!(
                "{path}: chunk {} is {} bytes at chunk size {}, but the row group records {} \
                 bytes; it was built with a different chunk size",
                rg.index_in_file,
                range.end - range.start,
                self.chunk_size,
                rg.bytes
            )));
        }
        // Length and row count alone do not pin where a chunk starts: a chunk
        // built at one size can land at a different offset with the same length
        // at another size (common for fixed-width lines). The recorded range,
        // when present, pins both ends.
        if let Some((start, end)) = rg.byte_range {
            if (range.start, range.end) != (start, end) {
                return Err(Error::Source(format!(
                    "{path}: chunk {} spans bytes {}..{} at chunk size {}, but the row group \
                     records {start}..{end}; it was built with a different chunk size",
                    rg.index_in_file, range.start, range.end, self.chunk_size
                )));
            }
        }
        let wanted = resolve_columns(&layout.schema, columns)?;
        let decode = Self::decode_indices(&layout.schema, &wanted);
        let mut batches = Vec::new();
        self.scan_chunk(&reader, path, &layout, idx, decode.clone(), |b| {
            batches.push(b);
            Ok(())
        })?;
        let decoded_schema = Arc::new(
            layout
                .schema
                .project(&decode)
                .map_err(|e| src_err(path, e))?,
        );
        let batch = if batches.is_empty() {
            RecordBatch::new_empty(decoded_schema)
        } else {
            concat_batches(&batches[0].schema(), &batches).map_err(|e| src_err(path, e))?
        };
        if u64::try_from(batch.num_rows()).ok() != Some(rg.rows) {
            return Err(Error::Source(format!(
                "{path}: chunk {} decoded {} rows, but the row group records {}",
                rg.index_in_file,
                batch.num_rows(),
                rg.rows
            )));
        }
        if columns.is_empty() {
            let opts = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
            return RecordBatch::try_new_with_options(Arc::new(Schema::empty()), vec![], &opts)
                .map_err(|e| src_err(path, e));
        }
        reorder(&batch, columns)
    }

    fn schema(&self, root: &str) -> Result<SchemaRef> {
        if let Some(s) = &self.dataset {
            return Ok(Arc::clone(s));
        }
        let mut first = None;
        for p in self.storage.list(root)? {
            if self.accepts(&p)? {
                first = Some(p);
                break;
            }
        }
        let first = first.ok_or_else(|| {
            Error::Source(format!(
                "{root}: no files of this format; it reads {}",
                self.format.accepted()
            ))
        })?;
        let fp = fingerprint_with_policy(&self.storage, &first, self.policy)?;
        let reader = open_verified(&self.storage, &fp, self.policy)?;
        Ok(Arc::clone(&self.layout_from(&fp, &reader)?.schema))
    }
}

#[cfg(test)]
mod tests {
    use super::csv_ends_in_quotes;

    fn inside(s: &str) -> bool {
        csv_ends_in_quotes(s.as_bytes(), b',')
    }

    #[test]
    fn quote_mid_unquoted_field_is_literal() {
        assert!(!inside("k1,5'10\"\n"));
        assert!(!inside("k5,6'1\"\nk4,c\n"));
        assert!(!inside("a\"b\"c\n"));
    }

    #[test]
    fn quote_opens_only_at_field_start() {
        assert!(inside("k3,\"x\n"));
        assert!(inside("\"x\n"));
        assert!(!inside("k3,\"x\nk9,y\"\n"));
        // After the delimiter, a quote opens a field again.
        assert!(inside("a\",\"b\n"));
    }

    #[test]
    fn doubled_quote_is_escaped_inside_quotes() {
        assert!(!inside("\"a\"\"b\"\n"));
        // `""` keeps the field open, so the newline is data.
        assert!(inside("\"a\"\"\n"));
        assert!(!inside("\"\"\n"));
    }

    #[test]
    fn closing_quote_then_other_byte_drops_to_unquoted() {
        assert!(!inside("\"ab\"cd\n"));
        // ... and a later quote in that field is literal.
        assert!(!inside("\"ab\"c\"\n"));
    }

    #[test]
    fn cr_terminates_and_is_data_inside_quotes() {
        assert!(inside("a,\"b\r\n"));
        assert!(!inside("a,\"b\"\r\n"));
        assert!(!inside("a\r\"b\"\r\n"));
    }

    #[test]
    fn closing_quote_at_end_of_input_is_outside() {
        assert!(!inside("a,\"x\""));
    }

    #[test]
    fn delimiter_is_respected() {
        assert!(csv_ends_in_quotes(b"a;\"b,c\n", b';'));
        assert!(!csv_ends_in_quotes(b"a;\"b,c\n", b','));
    }
}
