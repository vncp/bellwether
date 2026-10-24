//! Partition values from Hive-style `name=value` directory components and
//! `--path-template` virtual columns.

#![allow(clippy::unwrap_used, missing_docs)]

use std::fs::File;
use std::io::Read;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Date32Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether_source::canonical::{encode_i64, encode_temporal, encode_utf8, TU_DAY, UNIT_DATE32};
use bellwether_source::partition::parse_date32;
use bellwether_source::{
    ChunkedTextSource, LocalFs, ObjectMeta, ParquetSource, PathTemplate, Storage, TextFormat,
    TAIL_BYTES,
};
use bellwether_types::{Result, Source};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{ChunkReader, Length};

fn scratch(name: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("bellwether-source-partition-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_parquet(path: &Path, batch: &RecordBatch) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .build();
    let mut w =
        ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), Some(props)).unwrap();
    w.write(batch).unwrap();
    w.close().unwrap();
}

type Log = Arc<Mutex<Vec<Range<u64>>>>;

#[derive(Debug, Default)]
struct Tracking {
    log: Log,
}

struct TrackedFile {
    inner: File,
    log: Log,
}

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

#[test]
fn hive_partition_day_symbol_virtual_stats_and_zero_data_reads() {
    let dir = scratch("hive");
    let root = dir.as_path();
    let batch1 = RecordBatch::try_from_iter(vec![
        ("seq", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
        (
            "price",
            Arc::new(Float64Array::from(vec![100.5, 101.0, 102.25])) as ArrayRef,
        ),
    ])
    .unwrap();
    let batch2 = RecordBatch::try_from_iter(vec![
        ("seq", Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef),
        (
            "price",
            Arc::new(Float64Array::from(vec![200.0, 205.5])) as ArrayRef,
        ),
    ])
    .unwrap();

    write_parquet(
        &root.join("day=2026-03-01/symbol=AAPL/part-0.parquet"),
        &batch1,
    );
    write_parquet(
        &root.join("day=2026-03-02/symbol=MSFT/part-0.parquet"),
        &batch2,
    );

    let tracking = Tracking::default();
    let log = Arc::clone(&tracking.log);
    let source = ParquetSource::with_storage(tracking);
    let root_str = root.to_str().unwrap();

    let schema = source.schema(root_str).unwrap();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, vec!["seq", "price", "day", "symbol"]);
    assert_eq!(
        schema.field_with_name("day").unwrap().data_type(),
        &DataType::Date32
    );
    assert_eq!(
        schema.field_with_name("symbol").unwrap().data_type(),
        &DataType::Utf8
    );

    let files = source.discover(root_str).unwrap();
    assert_eq!(files.len(), 2);

    let rgs0 = source.row_groups(&files[0]).unwrap();
    assert_eq!(rgs0.len(), 2);
    let day_days = parse_date32("2026-03-01").unwrap();
    let expected_day = encode_temporal(UNIT_DATE32, TU_DAY, i64::from(day_days));
    let expected_sym = encode_utf8("AAPL");

    for rg in &rgs0 {
        let day_st = rg.stats.iter().find(|s| s.column == "day").unwrap();
        assert_eq!(day_st.min.as_ref(), Some(&expected_day));
        assert_eq!(day_st.max.as_ref(), Some(&expected_day));
        assert_eq!(day_st.null_count, Some(0));
        assert_eq!(day_st.distinct_count, Some(1));
        assert_eq!(day_st.bytes, Some(0));

        let sym_st = rg.stats.iter().find(|s| s.column == "symbol").unwrap();
        assert_eq!(sym_st.min.as_ref(), Some(&expected_sym));
        assert_eq!(sym_st.max.as_ref(), Some(&expected_sym));
        assert_eq!(sym_st.null_count, Some(0));
        assert_eq!(sym_st.distinct_count, Some(1));
        assert_eq!(sym_st.bytes, Some(0));
    }

    // Reading only virtual columns reads zero data pages (only the handle
    // fingerprint verification read).
    log.lock().unwrap().clear();
    let virt_batch = source
        .read_columns(&rgs0[0], &["day".into(), "symbol".into()])
        .unwrap();
    assert_eq!(virt_batch.num_rows(), 2);
    let days_col = virt_batch
        .column_by_name("day")
        .unwrap()
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    assert_eq!(days_col.values(), &[day_days, day_days]);
    let sym_col = virt_batch
        .column_by_name("symbol")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(sym_col.value(0), "AAPL");
    assert_eq!(sym_col.value(1), "AAPL");

    let reads = log.lock().unwrap().clone();
    let tail_start = files[0].size.saturating_sub(TAIL_BYTES);
    for r in &reads {
        assert!(
            r.start >= tail_start,
            "virtual column read touched data bytes at {r:?} (tail starts at {tail_start})"
        );
    }
}

#[test]
fn path_and_physical_content_disagreement_is_refused() {
    let dir = scratch("disagree");
    let root = dir.as_path();

    // Physical file has symbol = "GOOG", but lives under symbol=AAPL/.
    let bad_batch = RecordBatch::try_from_iter(vec![
        ("seq", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
        (
            "symbol",
            Arc::new(StringArray::from(vec!["AAPL", "GOOG"])) as ArrayRef,
        ),
    ])
    .unwrap();
    write_parquet(
        &root.join("day=2026-03-01/symbol=AAPL/bad.parquet"),
        &bad_batch,
    );

    let source = ParquetSource::new();
    let root_str = root.to_str().unwrap();
    let files = source.discover(root_str).unwrap();
    let err = source.row_groups(&files[0]).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("symbol") && msg.contains("AAPL"),
        "expected disagreement error naming column and path value, got: {msg}"
    );

    // Also test CSV physical column disagreement.
    let csv_dir = scratch("csv-disagree");
    let csv_path = csv_dir.join("day=2026-03-01/symbol=AAPL/bad.csv");
    std::fs::create_dir_all(csv_path.parent().unwrap()).unwrap();
    std::fs::write(&csv_path, "seq,symbol\n1,GOOG\n").unwrap();
    let csv_source = ChunkedTextSource::new(TextFormat::Csv {
        delimiter: b',',
        has_header: true,
    });
    let csv_files = csv_source
        .discover(csv_dir.to_str().unwrap())
        .unwrap();
    let csv_err = csv_source.row_groups(&csv_files[0]).unwrap_err();
    assert!(
        csv_err.to_string().contains("symbol") && csv_err.to_string().contains("AAPL"),
        "unexpected CSV error: {csv_err}"
    );
}

#[test]
fn path_template_captures_typed_virtual_columns_and_refuses_unmatched_files() {
    let dir = scratch("template");
    let root = dir.as_path();
    let batch = RecordBatch::try_from_iter(vec![
        (
            "symbol",
            Arc::new(StringArray::from(vec!["AAPL", "MSFT"])) as ArrayRef,
        ),
        (
            "close",
            Arc::new(Float64Array::from(vec![180.0, 410.0])) as ArrayRef,
        ),
    ])
    .unwrap();

    write_parquet(&root.join("2026-03-01.parquet"), &batch);
    write_parquet(&root.join("2026-03-02.parquet"), &batch);

    let tpl = PathTemplate::parse("{day:date}.parquet").unwrap();
    let source = ParquetSource::new().with_path_template(tpl);
    let root_str = root.to_str().unwrap();

    let schema = source.schema(root_str).unwrap();
    assert_eq!(
        schema.field_with_name("day").unwrap().data_type(),
        &DataType::Date32
    );

    let files = source.discover(root_str).unwrap();
    assert_eq!(files.len(), 2);
    let rgs = source.row_groups(&files[0]).unwrap();
    let day_st = rgs[0].stats.iter().find(|s| s.column == "day").unwrap();
    let expected = encode_temporal(
        UNIT_DATE32,
        TU_DAY,
        i64::from(parse_date32("2026-03-01").unwrap()),
    );
    assert_eq!(day_st.min.as_ref(), Some(&expected));
    assert_eq!(day_st.max.as_ref(), Some(&expected));

    // Add 12 files that do not match the template: discover must fail and list
    // up to 10 paths.
    for i in 0..12 {
        write_parquet(&root.join(format!("nested/extra-{i:02}.parquet")), &batch);
    }
    let err = source.discover(root_str).unwrap_err().to_string();
    assert!(
        err.contains("12 discovered file(s) do not match --path-template"),
        "got: {err}"
    );
    assert!(err.contains("(and 2 more)"), "got: {err}");

    // Dataset schema override types a virtual column as Int64 even if it looks like a number.
    let dir2 = scratch("schema-override");
    write_parquet(&dir2.join("epoch=100/part.parquet"), &batch);
    let custom_schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("close", DataType::Float64, false),
        Field::new("epoch", DataType::Int64, false),
    ]));
    let s2 = ParquetSource::new().with_schema(custom_schema);
    let f2 = s2.discover(dir2.to_str().unwrap()).unwrap();
    let rgs2 = s2.row_groups(&f2[0]).unwrap();
    let ep_st = rgs2[0].stats.iter().find(|s| s.column == "epoch").unwrap();
    assert_eq!(ep_st.min.as_ref(), Some(&encode_i64(100)));
    assert_eq!(ep_st.max.as_ref(), Some(&encode_i64(100)));
}
