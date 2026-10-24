//! Regression tests harvested from the review probes of the source crate.
//!
//! Each test reproduces a defect class that a probe demonstrated during
//! review, and pins the behavior that fixed it:
//!
//! - **Chunk-size sweeps.** A text row group read through any chunk size
//!   returns exactly the rows the whole-file parse places in it when that size
//!   recomputes the recorded `byte_range`, and is refused otherwise. That
//!   includes chunks with the recorded length and row count at another offset,
//!   which only the start of the byte range tells apart.
//! - **Stale footer.** A same-size, same-mtime rewrite after caching is
//!   refused instead of decoding the new bytes with the cached footer.
//! - **Rename-replace race.** A file renamed over the path is caught by the
//!   check on the opened handle, or is never read at all.
//! - **Tail coverage.** A same-size, same-mtime patch outside the hashed tail
//!   is refused under `FingerprintPolicy::Full`, and served under `Tail64K`,
//!   which is the limit the README documents.
//! - **Deprecated statistics.** Footers that carry only the deprecated
//!   `min`/`max` fields keep bounds only for signed, non-byte-array columns.
//! - **Edge fixture.** `edge.parquet` statistics match hand-computed
//!   canonical bytes.

#![allow(clippy::unwrap_used, missing_docs)]

mod common;

use std::fmt::Write as _;
use std::fs::{File, OpenOptions};
use std::io::{Cursor, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, AsArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
use bellwether_source::{
    drift, fingerprint, fingerprint_with_policy, ChunkedTextSource, FingerprintPolicy, LocalFs,
    ObjectMeta, ParquetSource, Storage, TextFormat, TAIL_BYTES,
};
use bellwether_types::{ColumnStats, Error, Result, RowGroupMeta, Source};
use bytes::Bytes;
use common::{fixture, from_hex};
use parquet::arrow::ArrowWriter;
use parquet::basic::{ColumnOrder, Compression, Encoding};
use parquet::file::metadata::{
    FileMetaData, ParquetMetaData, ParquetMetaDataWriter, RowGroupMetaData,
};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;

/// A scratch directory private to this test process.
fn scratch(name: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "bellwether-source-review-probes-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name)
}

fn path_str(p: &Path) -> String {
    p.to_str().unwrap().to_string()
}

/// Overwrites `path` with `bytes` and restores its previous mtime, so only
/// the content changes (what `cp -p`, `rsync -t`, or a coarse-mtime
/// filesystem produce).
fn rewrite_keep_mtime(path: &Path, bytes: &[u8]) {
    let mtime = std::fs::metadata(path).unwrap().modified().unwrap();
    let mut f = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(path)
        .unwrap();
    f.write_all(bytes).unwrap();
    f.set_modified(mtime).unwrap();
}

fn ids(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column(0)
        .as_primitive::<Int64Type>()
        .values()
        .to_vec()
}

// ---------------------------------------------------------------------------
// Chunk-size sweeps
// ---------------------------------------------------------------------------

/// One text file with known records: each record is one line, so the
/// whole-file parse assigns record `i` to the line that starts at
/// `starts[i]`.
struct TextCase {
    path: String,
    format: TextFormat,
    schema: SchemaRef,
    /// Byte offset where each record's line starts, in file order.
    starts: Vec<u64>,
    /// `id` of each record, from arrow's whole-file parse.
    ids: Vec<i64>,
}

impl TextCase {
    fn new(name: &str, format: TextFormat, schema: SchemaRef, text: &str) -> Self {
        let path = scratch(name);
        std::fs::write(&path, text).unwrap();
        let bytes = text.as_bytes();
        let mut line_starts = vec![0_u64];
        for (i, &b) in bytes.iter().enumerate() {
            if b == b'\n' && i + 1 < bytes.len() {
                line_starts.push(u64::try_from(i + 1).unwrap());
            }
        }
        let batches: Vec<RecordBatch> = match format {
            TextFormat::Csv { has_header, .. } => {
                if has_header {
                    line_starts.remove(0);
                }
                arrow::csv::ReaderBuilder::new(Arc::clone(&schema))
                    .with_header(has_header)
                    .build(Cursor::new(bytes))
                    .unwrap()
                    .map(std::result::Result::unwrap)
                    .collect()
            }
            TextFormat::JsonLines => arrow::json::ReaderBuilder::new(Arc::clone(&schema))
                .build(Cursor::new(bytes))
                .unwrap()
                .map(std::result::Result::unwrap)
                .collect(),
        };
        let ids: Vec<i64> = batches.iter().flat_map(ids).collect();
        assert_eq!(ids.len(), line_starts.len(), "{name}: one record per line");
        Self {
            path: path_str(&path),
            format,
            schema,
            starts: line_starts,
            ids,
        }
    }

    fn source(&self, chunk_size: u64) -> ChunkedTextSource {
        ChunkedTextSource::new(self.format)
            .with_schema(Arc::clone(&self.schema))
            .with_chunk_size(chunk_size)
    }

    /// The ids the whole-file parse places in byte range `r`.
    fn expected(&self, r: &Range<u64>) -> Vec<i64> {
        self.starts
            .iter()
            .zip(&self.ids)
            .filter(|(s, _)| r.contains(s))
            .map(|(_, id)| *id)
            .collect()
    }
}

/// A source at one chunk size, with what it built.
struct Built {
    size: u64,
    source: ChunkedTextSource,
    /// `None` when the 4x cap cannot hold a line at this size.
    row_groups: Option<Vec<RowGroupMeta>>,
    /// Chunk ranges at this size; empty when the cap is exceeded.
    ranges: Vec<Range<u64>>,
}

#[derive(Debug, Default)]
struct SweepCounts {
    /// Reads through the chunk size that built the row group. All match.
    same_size: usize,
    /// Reads through another chunk size that recomputes the byte-identical
    /// chunk. All match.
    same_range: usize,
    /// Reads through a chunk size whose chunk differs from the recorded byte
    /// range. All are refused.
    refused: usize,
    /// Refused reads whose chunk had the recorded length and row count at
    /// another offset: only the start of the byte range tells these apart.
    refused_on_start_alone: usize,
}

/// Builds row groups at every size in `sizes` and reads each one through
/// every size, its own included. A read returns exactly the rows the
/// whole-file parse puts in the recorded chunk when the reading size
/// recomputes the recorded `byte_range`, and errors otherwise.
fn sweep(case: &TextCase, sizes: &[u64]) -> SweepCounts {
    let fp = fingerprint(&case.path).unwrap();
    let built: Vec<Built> = sizes
        .iter()
        .map(|&size| {
            let source = case.source(size);
            // Sizes whose 4x cap cannot hold a line build nothing, and every
            // read through them must be refused.
            let row_groups = source.row_groups(&fp).ok();
            let ranges = source.chunk_ranges(&fp).unwrap_or_default();
            Built {
                size,
                source,
                row_groups,
                ranges,
            }
        })
        .collect();
    let mut counts = SweepCounts::default();
    for at_a in &built {
        let Some(rgs) = &at_a.row_groups else {
            continue;
        };
        for rg in rgs {
            let k = usize::try_from(rg.index_in_file).unwrap();
            let ra = &at_a.ranges[k];
            assert_eq!(
                rg.byte_range,
                Some((ra.start, ra.end)),
                "a text row group records its byte range"
            );
            let want = case.expected(ra);
            assert_eq!(u64::try_from(want.len()).unwrap(), rg.rows, "built rows");
            for at_b in &built {
                let (a, b) = (at_a.size, at_b.size);
                let at = format!("{}: built at {a}, read at {b}, row group {k}", case.path);
                let rb = at_b.ranges.get(k);
                let read = at_b.source.read_columns(rg, &["id".into()]);
                if rb == Some(ra) {
                    // The same size, or another size that lands on the
                    // byte-identical chunk: exactly the recorded rows.
                    let batch =
                        read.unwrap_or_else(|e| panic!("{at}: refused its own byte range: {e}"));
                    assert_eq!(ids(&batch), want, "{at}");
                    if a == b {
                        counts.same_size += 1;
                    } else {
                        counts.same_range += 1;
                    }
                } else {
                    // Any other chunk: refused, never other rows.
                    if let Ok(batch) = read {
                        panic!(
                            "{at}: read {:?} from {rb:?} instead of refusing (recorded {ra:?}, \
                             want {want:?})",
                            ids(&batch)
                        );
                    }
                    counts.refused += 1;
                    let start_alone = rb.is_some_and(|rb| {
                        rb.end - rb.start == ra.end - ra.start
                            && case.expected(rb).len() == want.len()
                    });
                    counts.refused_on_start_alone += usize::from(start_alone);
                }
            }
        }
    }
    counts
}

/// Every outcome class must occur, or the sweep cannot tell an enforced byte
/// range from a missing or over-eager one.
fn assert_sweep_covers_every_class(c: &SweepCounts) {
    assert!(c.same_size > 0, "{c:?}");
    assert!(c.same_range > 0, "{c:?}");
    assert!(c.refused > 0, "{c:?}");
    assert!(c.refused_on_start_alone > 0, "{c:?}");
}

#[test]
fn fixed_width_jsonl_sweep_reads_only_the_recorded_byte_range() {
    // 10-byte lines, the shape where equal-length chunks at different
    // offsets are common.
    let mut text = String::new();
    for i in 10..58 {
        writeln!(text, "{{\"id\":{i}}}").unwrap();
    }
    assert!(text.lines().all(|l| l.len() == 9));
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let case = TextCase::new("fixed.jsonl", TextFormat::JsonLines, schema, &text);
    let sizes: Vec<u64> = (20..=90).collect();
    let c = sweep(&case, &sizes);
    eprintln!("fixed-width JSONL sweep: {c:?}");
    assert_sweep_covers_every_class(&c);
}

#[test]
fn variable_width_csv_sweep_reads_only_the_recorded_byte_range() {
    let mut text = String::from("id,payload\n");
    for i in 0..60_i64 {
        let pad = "x".repeat(usize::try_from((i * 7) % 23).unwrap());
        writeln!(text, "{i},p{pad}").unwrap();
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("payload", DataType::Utf8, true),
    ]));
    let case = TextCase::new("variable.csv", TextFormat::csv(), schema, &text);
    let sizes: Vec<u64> = (40..=140).step_by(2).collect();
    let c = sweep(&case, &sizes);
    eprintln!("variable-width CSV sweep: {c:?}");
    assert_sweep_covers_every_class(&c);
}

// ---------------------------------------------------------------------------
// Stale footer and rename-replace race
// ---------------------------------------------------------------------------

/// Patches the little-endian i64 `from` to `to` inside `range` of `bytes`.
fn patch_i64(bytes: &mut [u8], range: Range<u64>, from: i64, to: i64) -> usize {
    let start = usize::try_from(range.start).unwrap();
    let end = usize::try_from(range.end).unwrap();
    let at = start
        + bytes[start..end]
            .windows(8)
            .position(|w| w == from.to_le_bytes())
            .expect("value present in the chunk");
    bytes[at..at + 8].copy_from_slice(&to.to_le_bytes());
    at
}

fn chunk_range(path: &str, rg: usize, col: usize) -> Range<u64> {
    let reader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
    let (start, len) = reader.metadata().row_group(rg).column(col).byte_range();
    start..start + len
}

#[test]
fn same_size_same_mtime_rewrite_after_caching_never_serves_altered_rows() {
    let p = scratch("stale.parquet");
    std::fs::copy(fixture("multi_rg.parquet"), &p).unwrap();
    let path = path_str(&p);
    let source = ParquetSource::new();
    let fp = fingerprint(&path).unwrap();
    let rgs = source.row_groups(&fp).unwrap();
    assert_eq!(
        ids(&source.read_columns(&rgs[1], &["id".into()]).unwrap()),
        vec![5, 6, 7, 8]
    );

    // Change the value 5 to 55 in row group 1, keeping size and mtime.
    let mut bytes = std::fs::read(&p).unwrap();
    patch_i64(&mut bytes, chunk_range(&path, 1, 0), 5, 55);
    rewrite_keep_mtime(&p, &bytes);
    let now = fingerprint(&path).unwrap();
    assert_eq!((now.size, &now.etag), (fp.size, &fp.etag));
    assert!(drift(&fp, &now));

    match source.read_columns(&rgs[1], &["id".into()]) {
        Ok(batch) => panic!("served rows {:?} from a rewritten file", ids(&batch)),
        Err(e) => assert!(
            matches!(e, Error::Source(ref m) if m.contains("drift")),
            "{e}"
        ),
    }
    let err = ParquetSource::new().row_groups(&fp).unwrap_err();
    assert!(
        matches!(err, Error::Source(ref m) if m.contains("drift")),
        "{err}"
    );
}

/// Local storage whose `open` renames `swap` over the requested path,
/// either just before or just after opening it: the window a path-based
/// check-then-open would race in.
#[cfg(unix)]
#[derive(Debug)]
struct RenamingFs {
    swap: Arc<Mutex<Option<PathBuf>>>,
    before_open: bool,
}

#[cfg(unix)]
impl RenamingFs {
    fn rename_now(&self, path: &str) {
        if let Some(from) = self.swap.lock().unwrap().take() {
            std::fs::rename(from, path).unwrap();
        }
    }
}

#[cfg(unix)]
impl Storage for RenamingFs {
    type Reader = File;

    fn list(&self, root: &str) -> Result<Vec<String>> {
        LocalFs.list(root)
    }

    fn head(&self, path: &str) -> Result<ObjectMeta> {
        LocalFs.head(path)
    }

    fn read_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        LocalFs.read_range(path, range)
    }

    fn open(&self, path: &str) -> Result<(File, ObjectMeta)> {
        if self.before_open {
            self.rename_now(path);
        }
        let opened = LocalFs.open(path)?;
        if !self.before_open {
            self.rename_now(path);
        }
        Ok(opened)
    }
}

/// `multi_rg.parquet`'s batch and writer settings, written to `out` by this
/// build's Parquet writer, with the ids replaced when `ids` is given.
///
/// The race probe writes both the original and its lookalike with the same
/// writer, so the sizes match by construction rather than by the committed
/// fixture happening to share a writer version with this build.
#[cfg(unix)]
fn write_multi_rg(out: &Path, ids: Option<&[i64]>) {
    let fx = common::parquet_fixtures()
        .into_iter()
        .find(|f| f.name == "multi_rg.parquet")
        .unwrap();
    let schema = fx.batch.schema();
    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .zip(fx.batch.columns())
        .map(|(f, c)| match (&ids, f.name().as_str()) {
            (Some(ids), "id") => Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
            _ => Arc::clone(c),
        })
        .collect();
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(fx.max_row_group_size))
        .build();
    let mut w = ArrowWriter::try_new(File::create(out).unwrap(), schema, Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// A file written exactly like `multi_rg.parquet` (same batch, same writer
/// settings) but with ids 101..=110, given the same mtime as `like`. Size
/// and mtime match, so only the content hash can tell the two apart.
#[cfg(unix)]
fn lookalike(like: &Path, name: &str) -> PathBuf {
    let out = scratch(name);
    write_multi_rg(&out, Some(&(101..=110).collect::<Vec<i64>>()));
    let mtime = std::fs::metadata(like).unwrap().modified().unwrap();
    File::options()
        .write(true)
        .open(&out)
        .unwrap()
        .set_modified(mtime)
        .unwrap();
    assert_eq!(
        std::fs::metadata(&out).unwrap().len(),
        std::fs::metadata(like).unwrap().len(),
        "the replacement must match the size, so only the content hash differs"
    );
    out
}

#[cfg(unix)]
#[test]
fn rename_replace_is_refused_or_never_read() {
    for before_open in [true, false] {
        let p = scratch(&format!("race-{before_open}.parquet"));
        write_multi_rg(&p, None);
        let path = path_str(&p);
        let fp = fingerprint(&path).unwrap();
        let swap = Arc::new(Mutex::new(None));
        let source = ParquetSource::with_storage(RenamingFs {
            swap: Arc::clone(&swap),
            before_open,
        });
        let rgs = source.row_groups(&fp).unwrap();
        let replacement = lookalike(&p, &format!("race-replacement-{before_open}.parquet"));
        *swap.lock().unwrap() = Some(replacement);
        let result = source.read_columns(&rgs[1], &["id".into()]);
        if before_open {
            // The new file is what gets opened; the content hash refuses it.
            let err = result.unwrap_err();
            assert!(
                matches!(err, Error::Source(ref m) if m.contains("drift")),
                "{err}"
            );
        } else {
            // The handle was opened first, so it still reads the original
            // inode, which matches the fingerprint.
            assert_eq!(ids(&result.unwrap()), vec![5, 6, 7, 8]);
        }
    }
}

// ---------------------------------------------------------------------------
// Tail coverage: Tail64K versus Full
// ---------------------------------------------------------------------------

#[test]
fn patch_outside_the_tail_full_refuses_tail64k_serves() {
    let p = scratch("tail-coverage.parquet");
    let rows = 60_000_i64;
    let batch = RecordBatch::try_from_iter(vec![(
        "id",
        Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>())) as ArrayRef,
    )])
    .unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(20_000))
        .set_dictionary_enabled(false)
        .set_compression(Compression::UNCOMPRESSED)
        .set_encoding(Encoding::PLAIN)
        .build();
    let mut w =
        ArrowWriter::try_new(File::create(&p).unwrap(), batch.schema(), Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let path = path_str(&p);

    let fp_tail = fingerprint_with_policy(&LocalFs, &path, FingerprintPolicy::Tail64K).unwrap();
    let fp_full = fingerprint_with_policy(&LocalFs, &path, FingerprintPolicy::Full).unwrap();
    let tail = ParquetSource::new();
    let full = ParquetSource::new().with_fingerprint_policy(FingerprintPolicy::Full);
    let tail_rgs = tail.row_groups(&fp_tail).unwrap();
    let full_rgs = full.row_groups(&fp_full).unwrap();

    // Patch id 7 in row group 0, far outside the hashed tail.
    let mut bytes = std::fs::read(&p).unwrap();
    let at = patch_i64(&mut bytes, chunk_range(&path, 0, 0), 7, 777);
    assert!(u64::try_from(at).unwrap() + 8 <= fp_tail.size - TAIL_BYTES);
    rewrite_keep_mtime(&p, &bytes);

    // Full hashes every byte: the patch is drift and every read refuses.
    let now_full = fingerprint_with_policy(&LocalFs, &path, FingerprintPolicy::Full).unwrap();
    assert!(drift(&fp_full, &now_full));
    let err = full.read_columns(&full_rgs[0], &["id".into()]).unwrap_err();
    assert!(
        matches!(err, Error::Source(ref m) if m.contains("drift")),
        "{err}"
    );
    let err = ParquetSource::new()
        .with_fingerprint_policy(FingerprintPolicy::Full)
        .row_groups(&fp_full)
        .unwrap_err();
    assert!(
        matches!(err, Error::Source(ref m) if m.contains("drift")),
        "{err}"
    );

    // Tail64K misses it and serves the patched value. This pins the limit
    // the README documents ("Tail coverage"); if it ever starts refusing,
    // update the README together with this assertion.
    let now_tail = fingerprint_with_policy(&LocalFs, &path, FingerprintPolicy::Tail64K).unwrap();
    assert!(!drift(&fp_tail, &now_tail));
    let served = tail.read_columns(&tail_rgs[0], &["id".into()]).unwrap();
    assert_eq!(ids(&served)[7], 777);
}

// ---------------------------------------------------------------------------
// Deprecated statistics
// ---------------------------------------------------------------------------

/// `s` re-expressed with only the deprecated `min`/`max` fields set, as old
/// writers produced them.
fn as_deprecated(s: &Statistics) -> Statistics {
    match s {
        Statistics::Boolean(v) => Statistics::boolean(
            v.min_opt().copied(),
            v.max_opt().copied(),
            v.distinct_count(),
            v.null_count_opt(),
            true,
        ),
        Statistics::Int32(v) => Statistics::int32(
            v.min_opt().copied(),
            v.max_opt().copied(),
            v.distinct_count(),
            v.null_count_opt(),
            true,
        ),
        Statistics::Int64(v) => Statistics::int64(
            v.min_opt().copied(),
            v.max_opt().copied(),
            v.distinct_count(),
            v.null_count_opt(),
            true,
        ),
        Statistics::Int96(v) => Statistics::int96(
            v.min_opt().copied(),
            v.max_opt().copied(),
            v.distinct_count(),
            v.null_count_opt(),
            true,
        ),
        Statistics::Float(v) => Statistics::float(
            v.min_opt().copied(),
            v.max_opt().copied(),
            v.distinct_count(),
            v.null_count_opt(),
            true,
        ),
        Statistics::Double(v) => Statistics::double(
            v.min_opt().copied(),
            v.max_opt().copied(),
            v.distinct_count(),
            v.null_count_opt(),
            true,
        ),
        Statistics::ByteArray(v) => Statistics::byte_array(
            v.min_opt().cloned(),
            v.max_opt().cloned(),
            v.distinct_count(),
            v.null_count_opt(),
            true,
        ),
        Statistics::FixedLenByteArray(v) => Statistics::fixed_len_byte_array(
            v.min_opt().cloned(),
            v.max_opt().cloned(),
            v.distinct_count(),
            v.null_count_opt(),
            true,
        ),
    }
}

/// Removes the trailing `column_orders` field (thrift id 7) from a footer
/// written by `ParquetMetaDataWriter`, which always writes it, so the file
/// looks like one from a writer that predates column orders. Asserts the
/// exact bytes it removes, so a change in the writer fails loudly here.
fn strip_column_orders(footer: &[u8], leaves: usize) -> Vec<u8> {
    let n = footer.len();
    assert_eq!(&footer[n - 4..], b"PAR1");
    let meta = &footer[..n - 8];
    let leaves_nibble = u8::try_from(leaves).unwrap();
    assert!(leaves_nibble < 15, "short list header only");
    // Field 7 (delta 1 after created_by, type list) + list header (size,
    // element type struct) + per leaf: TYPE_ORDER field (id 1, struct),
    // empty TypeDefinedOrder stop, union stop; then the FileMetaData stop.
    let mut tail = vec![0x19, (leaves_nibble << 4) | 0x0C];
    for _ in 0..leaves {
        tail.extend_from_slice(&[0x1C, 0x00, 0x00]);
    }
    tail.push(0x00);
    assert!(meta.ends_with(&tail), "column_orders is not the last field");
    let mut out = meta[..meta.len() - tail.len()].to_vec();
    out.push(0x00);
    let len = u32::try_from(out.len()).unwrap();
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(b"PAR1");
    out
}

/// Copies `src` with its footer rewritten to deprecated-only statistics,
/// with or without column orders.
fn rewrite_deprecated(src: &str, dst: &Path, with_orders: bool) {
    let bytes = std::fs::read(src).unwrap();
    let n = bytes.len();
    let footer_len =
        usize::try_from(u32::from_le_bytes(bytes[n - 8..n - 4].try_into().unwrap())).unwrap();
    let data_end = n - 8 - footer_len;
    let reader = SerializedFileReader::new(File::open(src).unwrap()).unwrap();
    let md = reader.metadata();
    let row_groups: Vec<RowGroupMetaData> = md
        .row_groups()
        .iter()
        .map(|rg| {
            let cols = rg
                .columns()
                .iter()
                .map(|c| match c.statistics() {
                    Some(s) => c
                        .clone()
                        .into_builder()
                        .set_statistics(as_deprecated(s))
                        .build()
                        .unwrap(),
                    None => c.clone(),
                })
                .collect();
            rg.clone()
                .into_builder()
                .set_column_metadata(cols)
                .build()
                .unwrap()
        })
        .collect();
    let fm = md.file_metadata();
    let legacy = FileMetaData::new(
        fm.version(),
        fm.num_rows(),
        Some("parquet-mr version 1.8.1 (legacy statistics)".into()),
        fm.key_value_metadata().cloned(),
        fm.schema_descr_ptr(),
        None,
    );
    let rewritten = ParquetMetaData::new(legacy, row_groups);
    let mut footer = Vec::new();
    ParquetMetaDataWriter::new(&mut footer, &rewritten)
        .finish()
        .unwrap();
    if !with_orders {
        footer = strip_column_orders(&footer, fm.schema_descr().num_columns());
    }
    let mut out = bytes[..data_end].to_vec();
    out.extend_from_slice(&footer);
    std::fs::write(dst, out).unwrap();
}

fn stats_of(path: &str) -> Vec<Vec<ColumnStats>> {
    let fp = fingerprint(path).unwrap();
    ParquetSource::new()
        .row_groups(&fp)
        .unwrap()
        .into_iter()
        .map(|rg| rg.stats)
        .collect()
}

#[test]
fn deprecated_statistics_kept_only_for_signed_non_byte_array_columns() {
    // (fixture, columns whose bounds survive)
    let cases = [
        // f DOUBLE kept; u UINT_32 (unsigned), s UTF8 (byte array),
        // d DECIMAL on FIXED_LEN_BYTE_ARRAY (byte array) dropped.
        ("edge.parquet", vec!["f"]),
        // id INT64 and amount DECIMAL on INT64 kept; name UTF8 dropped.
        ("multi_rg.parquet", vec!["id", "amount"]),
    ];
    for (name, kept) in cases {
        let original = stats_of(&fixture(name));
        for with_orders in [true, false] {
            let dst = scratch(&format!("deprecated-{with_orders}-{name}"));
            rewrite_deprecated(&fixture(name), &dst, with_orders);
            let path = path_str(&dst);

            // The rewrite really is a legacy footer.
            let reader = SerializedFileReader::new(File::open(&path).unwrap()).unwrap();
            let md = reader.metadata();
            let stats = md.row_group(0).column(0).statistics().unwrap();
            assert!(stats.is_min_max_deprecated(), "{name}");
            let order = md.file_metadata().column_order(0);
            assert_eq!(
                matches!(order, ColumnOrder::UNDEFINED),
                !with_orders,
                "{name}: {order:?}"
            );

            let got = stats_of(&path);
            for (rg, (got_rg, orig_rg)) in got.iter().zip(&original).enumerate() {
                for (g, o) in got_rg.iter().zip(orig_rg) {
                    let label = format!("{name} orders={with_orders} rg{rg} {}", g.column);
                    if kept.contains(&g.column.as_str()) {
                        assert_eq!((&g.min, &g.max), (&o.min, &o.max), "{label}");
                    } else {
                        assert_eq!((&g.min, &g.max), (&None, &None), "{label}");
                    }
                    assert_eq!(g.null_count, o.null_count, "{label}");
                }
            }
            // The data pages are untouched and still decode.
            let fp = fingerprint(&path).unwrap();
            let src = ParquetSource::new();
            for rg in src.row_groups(&fp).unwrap() {
                let b = src
                    .read_columns(&rg, &[rg.stats[0].column.clone()])
                    .unwrap();
                assert_eq!(u64::try_from(b.num_rows()).unwrap(), rg.rows);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// edge.parquet hand checks
// ---------------------------------------------------------------------------

#[test]
fn edge_fixture_stats_match_hand_computed_bytes() {
    let rgs = ParquetSource::new()
        .row_groups(&fingerprint(&fixture("edge.parquet")).unwrap())
        .unwrap();
    assert_eq!(rgs.iter().map(|r| r.rows).collect::<Vec<_>>(), vec![3, 2]);
    let hex = |s: Option<&str>| s.map(from_hex);
    // (row group, column, min, max), as lowercase hex of canonical bytes.
    let want: [(usize, &str, Option<&str>, Option<&str>); 8] = [
        // f = [NaN, 1.5, -3.0]: NaN excluded; -3.0 and 1.5 sign-flipped.
        (
            0,
            "f",
            Some("043ff7ffffffffffff"),
            Some("04bff8000000000000"),
        ),
        // f = [NaN, NaN]: no non-NaN value, so unknown.
        (1, "f", None, None),
        // u = [3_000_000_000, 5, 2^31]: unsigned tag 03.
        (
            0,
            "u",
            Some("030000000000000005"),
            Some("0300000000b2d05e00"),
        ),
        // u = [u32::MAX, 0].
        (
            1,
            "u",
            Some("030000000000000000"),
            Some("0300000000ffffffff"),
        ),
        // s = ["é", "zebra", "日本"]: bytewise, "zebra" < "é" < "日本".
        (0, "s", Some("067a656272610000"), Some("06e697a5e69cac0000")),
        // s = ["Ünïcode", "abc"].
        (
            1,
            "s",
            Some("066162630000"),
            Some("06c39c6ec3af636f64650000"),
        ),
        // d Decimal(38,4) = [1234567890123456789012345678.9012, -0.0001, 0]:
        // 05, width 10, 16-byte big-endian with the sign bit flipped.
        (
            0,
            "d",
            Some("05107fffffffffffffffffffffffffffffff"),
            Some("05108000009bd30a3c645943dd1690a03a14"),
        ),
        // d = [10^34 - 0.0001 (unscaled 10^38 - 1), -0.0005].
        (
            1,
            "d",
            Some("05107ffffffffffffffffffffffffffffffb"),
            Some("0510cb3b4ca85a86c47a098a223fffffffff"),
        ),
    ];
    for (rg, col, min, max) in want {
        let s = rgs[rg].stats.iter().find(|s| s.column == col).unwrap();
        assert_eq!(s.min, hex(min), "rg{rg} {col} min");
        assert_eq!(s.max, hex(max), "rg{rg} {col} max");
        assert_eq!(s.null_count, Some(0), "rg{rg} {col} nulls");
    }
}
