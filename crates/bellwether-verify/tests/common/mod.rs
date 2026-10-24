//! Shared helpers: a small versioned ledger written as Parquet, and
//! one-call reconciles over two folders.

#![allow(dead_code, clippy::unwrap_used, missing_docs)]

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Decimal128Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_source::ParquetSource;
use bellwether_types::{FileFingerprint, RowGroupMeta, Source};
use bellwether_verify::{Options, ReconcileSpec, Reconciler, Reconciliation, Side};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// One row of the test ledger.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct R {
    pub id: Option<i64>,
    pub ts: Option<i64>,
    pub amount: Option<i128>,
    pub status: String,
    pub note: Option<String>,
}

/// A row with every field set: id, version timestamp (µs), amount in cents.
pub fn r(id: i64, ts: i64, cents: i128, status: &str) -> R {
    R {
        id: Some(id),
        ts: Some(ts),
        amount: Some(cents),
        status: status.into(),
        note: Some(format!("note-{id}")),
    }
}

pub fn ts_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

/// The ledger schema at a given amount scale.
pub fn schema_at(scale: i8) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("ts", ts_type(), true),
        Field::new("amount", DataType::Decimal128(18, scale), true),
        Field::new("status", DataType::Utf8, false),
        Field::new("note", DataType::Utf8, true),
    ]))
}

/// A batch of rows with amounts given in cents and written at `scale`
/// (scale 2 writes cents as is; scale 3 writes cents × 10).
pub fn batch_at(rows: &[R], scale: i8) -> RecordBatch {
    let factor = 10i128.pow(u32::try_from(scale - 2).unwrap_or(0));
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.id).collect::<Vec<_>>(),
        )),
        Arc::new(
            TimestampMicrosecondArray::from(rows.iter().map(|r| r.ts).collect::<Vec<_>>())
                .with_timezone("UTC"),
        ),
        Arc::new(
            Decimal128Array::from(
                rows.iter()
                    .map(|r| r.amount.map(|a| a * factor))
                    .collect::<Vec<_>>(),
            )
            .with_precision_and_scale(18, scale)
            .unwrap(),
        ),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.status.as_str()).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.note.as_deref()).collect::<Vec<_>>(),
        )),
    ];
    RecordBatch::try_new(schema_at(scale), cols).unwrap()
}

/// Writes `batch` to `dir/name` with at most `rows_per_group` rows per row
/// group.
pub fn write_batch(dir: &Path, name: &str, batch: &RecordBatch, rows_per_group: usize) {
    let file = std::fs::File::create(dir.join(name)).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(rows_per_group.max(1)))
        .build();
    let mut w = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
    w.write(batch).unwrap();
    w.close().unwrap();
}

/// Writes rows at scale 2.
pub fn write(dir: &Path, name: &str, rows: &[R], rows_per_group: usize) {
    write_batch(dir, name, &batch_at(rows, 2), rows_per_group);
}

pub fn root(dir: &Path) -> String {
    dir.to_str().unwrap().to_string()
}

/// Reconciles two folders of Parquet with the given spec and options.
pub fn reconcile_with(
    left: &Path,
    right: &Path,
    spec: ReconcileSpec,
    options: Options,
) -> bellwether_verify::Result<Reconciliation> {
    let (ls, rs) = (ParquetSource::new(), ParquetSource::new());
    let (lr, rr) = (root(left), root(right));
    Reconciler::new(spec, options)?.run(&Side::new(&ls, &lr), &Side::new(&rs, &rr))
}

pub fn reconcile(
    left: &Path,
    right: &Path,
    spec: ReconcileSpec,
) -> bellwether_verify::Result<Reconciliation> {
    reconcile_with(left, right, spec, Options::default())
}

/// Two fresh folders.
pub fn dirs() -> (tempfile::TempDir, tempfile::TempDir) {
    (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap())
}

/// A source that counts the calls made to it.
pub struct Spy<S> {
    pub inner: S,
    pub reads: AtomicUsize,
    pub row_group_calls: AtomicUsize,
    pub read_rows: AtomicUsize,
}

impl<S> Spy<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            reads: AtomicUsize::new(0),
            row_group_calls: AtomicUsize::new(0),
            read_rows: AtomicUsize::new(0),
        }
    }

    pub fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }
}

impl<S: Source> Source for Spy<S> {
    fn discover(&self, root: &str) -> bellwether_types::Result<Vec<FileFingerprint>> {
        self.inner.discover(root)
    }

    fn row_groups(&self, file: &FileFingerprint) -> bellwether_types::Result<Vec<RowGroupMeta>> {
        self.row_group_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.row_groups(file)
    }

    fn read_columns(
        &self,
        rg: &RowGroupMeta,
        columns: &[String],
    ) -> bellwether_types::Result<RecordBatch> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let b = self.inner.read_columns(rg, columns)?;
        self.read_rows.fetch_add(b.num_rows(), Ordering::SeqCst);
        Ok(b)
    }

    fn schema(&self, root: &str) -> bellwether_types::Result<SchemaRef> {
        self.inner.schema(root)
    }
}

/// A ledger of `n` keys, `versions` versions each, timestamps 1000 apart,
/// amount = id × 100 + version cents, status by id mod 3.
pub fn ledger(n: i64, versions: i64) -> Vec<R> {
    let mut rows = Vec::new();
    for v in 0..versions {
        for id in 0..n {
            rows.push(r(
                id,
                1_000 * (v + 1),
                i128::from(id * 100 + v),
                ["a", "b", "c"][usize::try_from(id % 3).unwrap()],
            ));
        }
    }
    rows
}
