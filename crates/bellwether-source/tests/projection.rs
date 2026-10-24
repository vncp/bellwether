//! `read_columns` fetches only the projected column chunk of one row group,
//! through a pluggable `Storage`. The tracking storage records every byte
//! range the source and the Parquet decoder consume.

#![allow(clippy::unwrap_used, missing_docs)]

use std::fs::File;
use std::io::Read;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use bellwether_source::{fingerprint, LocalFs, ObjectMeta, ParquetSource, Storage, TAIL_BYTES};
use bellwether_types::{Result, Source};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{ChunkReader, FileReader, Length, SerializedFileReader};

type Log = Arc<Mutex<Vec<Range<u64>>>>;

#[derive(Debug, Default)]
struct Tracking {
    log: Log,
}

struct TrackedFile {
    inner: File,
    log: Log,
}

/// Logs bytes as the decoder consumes them (not the `BufReader` read-ahead
/// underneath, which is a `LocalFs` detail).
struct TrackedRead {
    inner: std::io::BufReader<File>,
    pos: u64,
    log: Log,
}

impl Read for TrackedRead {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        let end = self.pos + u64::try_from(n).unwrap();
        if n > 0 {
            self.log.lock().unwrap().push(self.pos..end);
        }
        self.pos = end;
        Ok(n)
    }
}

impl Length for TrackedFile {
    fn len(&self) -> u64 {
        self.inner.len()
    }
}

impl ChunkReader for TrackedFile {
    type T = TrackedRead;

    fn get_read(&self, start: u64) -> parquet::errors::Result<TrackedRead> {
        Ok(TrackedRead {
            inner: self.inner.get_read(start)?,
            pos: start,
            log: Arc::clone(&self.log),
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        self.log
            .lock()
            .unwrap()
            .push(start..start + u64::try_from(length).unwrap());
        self.inner.get_bytes(start, length)
    }
}

impl Storage for Tracking {
    type Reader = TrackedFile;

    fn list(&self, root: &str) -> Result<Vec<String>> {
        LocalFs.list(root)
    }

    fn head(&self, path: &str) -> Result<ObjectMeta> {
        LocalFs.head(path)
    }

    fn read_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        self.log.lock().unwrap().push(range.clone());
        LocalFs.read_range(path, range)
    }

    fn open(&self, path: &str) -> Result<(TrackedFile, ObjectMeta)> {
        let (inner, meta) = LocalFs.open(path)?;
        Ok((
            TrackedFile {
                inner,
                log: Arc::clone(&self.log),
            },
            meta,
        ))
    }
}

fn contains(outer: &Range<u64>, inner: &Range<u64>) -> bool {
    outer.start <= inner.start && inner.end <= outer.end
}

/// A file large enough that the fingerprint's 64 KiB tail read does not
/// reach row group 0: 3 row groups of 20 000 rows, plain, uncompressed.
fn write_large(path: &PathBuf) {
    let rows = 60_000_i64;
    let batch = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>())) as ArrayRef,
        ),
        (
            "name",
            Arc::new(StringArray::from(
                (0..rows)
                    .map(|i| format!("name-{i:08}"))
                    .collect::<Vec<_>>(),
            )),
        ),
    ])
    .unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_size(20_000)
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .build();
    let mut w =
        ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

#[test]
fn read_columns_touches_only_the_projected_chunk_of_one_row_group() {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "bellwether-source-projection-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let p = dir.join("large.parquet");
    write_large(&p);
    let path = p.to_str().unwrap().to_string();
    let fp = fingerprint(&path).unwrap();

    // Byte range of rg0's `id` chunk, from an independent footer read.
    let reader = SerializedFileReader::new(File::open(&path).unwrap()).unwrap();
    let md = reader.metadata();
    assert_eq!(md.num_row_groups(), 3);
    let (start, len) = md.row_group(0).column(0).byte_range();
    let wanted = start..start + len;
    let tail = fp.size - TAIL_BYTES..fp.size;
    assert!(
        wanted.end <= tail.start,
        "fixture too small to separate the tail"
    );

    let tracking = Tracking::default();
    let log = Arc::clone(&tracking.log);
    let source = ParquetSource::with_storage(tracking);
    let rgs = source.row_groups(&fp).unwrap();

    // Footer is cached now; record only what read_columns fetches.
    log.lock().unwrap().clear();
    let batch = source.read_columns(&rgs[0], &["id".into()]).unwrap();
    assert_eq!(batch.num_rows(), 20_000);

    let reads = log.lock().unwrap().clone();
    let mut data_bytes = 0;
    for r in &reads {
        if *r == tail {
            continue; // fingerprint verification on the opened handle
        }
        assert!(
            contains(&wanted, r),
            "read {r:?} is outside the projected chunk {wanted:?}"
        );
        data_bytes += r.end - r.start;
    }
    assert_eq!(reads.iter().filter(|r| **r == tail).count(), 1);
    assert!(data_bytes > 0 && data_bytes <= len, "{data_bytes} of {len}");
}
