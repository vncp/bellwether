//! Shared helpers for the build crate's integration tests.
//!
//! Ground truth is computed independently of the code under test: Parquet
//! row groups are decoded with the `parquet` crate's own reader, text chunks
//! are recomputed from the file bytes with the chunking rule written out
//! here, and residual predicates are evaluated by a reference evaluator over
//! canonical bytes.

#![allow(
    dead_code,
    missing_docs,
    clippy::unwrap_used,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::too_many_arguments,
    clippy::similar_names,
    clippy::many_single_char_names,
    clippy::cast_possible_wrap,
    clippy::assert_is_empty,
    clippy::type_complexity,
    clippy::cast_precision_loss,
    clippy::struct_field_names,
    clippy::needless_pass_by_value,
    clippy::format_push_string,
    clippy::redundant_closure_for_method_calls
)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::ops::{ControlFlow, Range};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use bellwether_build::{BuildObserver, BuildOptions, BuildReport, Builder, Planner, SourceKind};
use bellwether_core::encoding::{self, Value};
use bellwether_store::{ObjectMeta, ObjectStoreBackend, StoreExt};
use bellwether_types::{
    FileFingerprint, KeySpec, Predicate, Result, RowGroupId, ScanPlan, Store, StoreVersion,
};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

// ---------------------------------------------------------------------------
// Paths, stores, files
// ---------------------------------------------------------------------------

pub fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/source")
        .join(name)
}

/// A scratch source folder, canonicalized (the builder records canonical
/// paths, so ground truth must use them too).
pub struct Scratch {
    _dir: tempfile::TempDir,
    root: PathBuf,
}

impl Scratch {
    pub fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        Self { _dir: dir, root }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn root_str(&self) -> String {
        self.root.to_str().unwrap().to_string()
    }

    pub fn path(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }

    pub fn path_str(&self, name: &str) -> String {
        self.path(name).to_str().unwrap().to_string()
    }

    pub fn copy_fixture(&self, name: &str) -> PathBuf {
        let to = self.path(name);
        std::fs::copy(fixture(name), &to).unwrap();
        to
    }
}

pub fn memory_store() -> Arc<ObjectStoreBackend> {
    Arc::new(ObjectStoreBackend::memory().unwrap())
}

pub fn builder(store: &Arc<ObjectStoreBackend>) -> Builder<ObjectStoreBackend> {
    Builder::new(Arc::clone(store), "idx")
}

pub fn planner(store: &Arc<ObjectStoreBackend>) -> Planner<ObjectStoreBackend> {
    Planner::open(Arc::clone(store), "idx").unwrap()
}

/// Options for a Parquet build with a supplied dataset schema.
pub fn parquet_opts(schema: &SchemaRef) -> BuildOptions {
    BuildOptions {
        schema: Some(Arc::clone(schema)),
        ..BuildOptions::new(SourceKind::Parquet)
    }
}

/// Writes `batch` to `path` with at most `rows_per_rg` rows per row group.
pub fn write_parquet(path: &Path, batch: &RecordBatch, rows_per_rg: usize) {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).unwrap();
    }
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(rows_per_rg.max(1)))
        .build();
    let file = File::create(path).unwrap();
    let mut w = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
    w.write(batch).unwrap();
    w.close().unwrap();
}

/// `id: Int64, grp: Utf8, ts: Timestamp(µs)`.
pub fn simple_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int64, true),
        arrow::datatypes::Field::new("grp", DataType::Utf8, true),
        arrow::datatypes::Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            true,
        ),
    ]))
}

/// Writes `(id, grp, ts)` rows under `simple_schema`.
pub fn write_simple(path: &Path, rows: &[(i64, &str, i64)], rows_per_rg: usize) {
    let batch = RecordBatch::try_new(
        simple_schema(),
        vec![
            Arc::new(arrow::array::Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(arrow::array::StringArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(arrow::array::TimestampMicrosecondArray::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    write_parquet(path, &batch, rows_per_rg);
}

/// `n` rows `(base + i, "g{i % 4}", i)`.
pub fn simple_rows(base: i64, n: i64) -> Vec<(i64, String, i64)> {
    (0..n)
        .map(|i| (base + i, format!("g{}", i % 4), i))
        .collect()
}

/// Writes `simple_rows(base, n)` with `rows_per_rg` rows per row group.
pub fn write_numbered(path: &Path, base: i64, n: i64, rows_per_rg: usize) {
    let rows = simple_rows(base, n);
    let refs: Vec<(i64, &str, i64)> = rows.iter().map(|(a, b, c)| (*a, b.as_str(), *c)).collect();
    write_simple(path, &refs, rows_per_rg);
}

/// Every live and retired id of a manifest, checked disjoint, duplicate
/// free, and exactly `0..next_id`.
pub fn check_id_space(m: &bellwether_store::Manifest) -> (BTreeSet<u32>, BTreeSet<u32>) {
    let mut live = BTreeSet::new();
    for f in &m.rowgroup_space.files {
        for id in &f.rowgroup_ids {
            assert!(live.insert(id.0), "id {} assigned twice", id.0);
        }
    }
    let mut retired = BTreeSet::new();
    for t in &m.rowgroup_space.tombstones {
        for id in &t.rowgroup_ids {
            assert!(retired.insert(id.0), "id {} retired twice", id.0);
        }
    }
    assert!(live.is_disjoint(&retired), "an id is both live and retired");
    let all: BTreeSet<u32> = live.union(&retired).copied().collect();
    assert_eq!(
        all,
        (0..m.rowgroup_space.next_id).collect::<BTreeSet<u32>>(),
        "ids are dense and never reused"
    );
    (live, retired)
}

pub fn exact(cols: &[&str]) -> KeySpec {
    KeySpec::Exact {
        columns: cols.iter().map(|c| (*c).to_string()).collect(),
        counts: false,
        bounds: None,
    }
}

pub fn bloom(cols: &[&str]) -> KeySpec {
    KeySpec::Bloom {
        columns: cols.iter().map(|c| (*c).to_string()).collect(),
        fpr: 0.01,
        bounds: None,
    }
}

pub fn range(col: &str) -> KeySpec {
    KeySpec::Range { column: col.into() }
}

pub fn temporal(cols: &[&str], ts: &str) -> KeySpec {
    KeySpec::Temporal {
        columns: cols.iter().map(|c| (*c).to_string()).collect(),
        ts_column: ts.into(),
        counts: false,
    }
}

pub fn cols(c: &[&str]) -> Vec<String> {
    c.iter().map(|s| (*s).to_string()).collect()
}

// ---------------------------------------------------------------------------
// Canonical values
// ---------------------------------------------------------------------------

pub fn enc(v: &Value<'_>, dt: &DataType) -> Vec<u8> {
    let mut out = Vec::new();
    encoding::encode_value_as(v, dt, &mut out).unwrap();
    out
}

pub fn enc_i64(v: i64) -> Vec<u8> {
    enc(&Value::Int(v), &DataType::Int64)
}

pub fn enc_str(s: &str) -> Vec<u8> {
    enc(&Value::Utf8(s), &DataType::Utf8)
}

pub fn enc_f64(v: f64) -> Vec<u8> {
    enc(&Value::Float(v), &DataType::Float64)
}

// ---------------------------------------------------------------------------
// Ground truth rows
// ---------------------------------------------------------------------------

/// One source row: where it lives and each column's canonical bytes under
/// the dataset type (`None` for null).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Row {
    pub path: String,
    pub rg: u32,
    pub row: usize,
    pub cols: BTreeMap<String, Option<Vec<u8>>>,
}

impl Row {
    pub fn get(&self, c: &str) -> Option<&Vec<u8>> {
        self.cols.get(c).and_then(Option::as_ref)
    }

    /// The canonical tuple over `columns`, or `None` if any is null.
    pub fn tuple(&self, columns: &[String]) -> Option<Vec<u8>> {
        let mut out = Vec::new();
        for c in columns {
            out.extend_from_slice(self.get(c)?);
        }
        Some(out)
    }
}

fn rows_of(
    path: &str,
    rg: u32,
    first_row: usize,
    batch: &RecordBatch,
    schema: &Schema,
) -> Vec<Row> {
    (0..batch.num_rows())
        .map(|r| {
            let cols = schema
                .fields()
                .iter()
                .filter_map(|f| {
                    let array = batch.column_by_name(f.name())?;
                    let mut out = Vec::new();
                    let present =
                        encoding::encode_array_value_as(array.as_ref(), r, f.data_type(), &mut out)
                            .unwrap();
                    Some((f.name().clone(), present.then_some(out)))
                })
                .collect();
            Row {
                path: path.to_string(),
                rg,
                row: first_row + r,
                cols,
            }
        })
        .collect()
}

/// Every row of a Parquet file, per row group, decoded with the `parquet`
/// crate and encoded under the dataset `schema`.
pub fn parquet_rows(path: &Path, schema: &Schema) -> Vec<Row> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    let n = builder.metadata().num_row_groups();
    let p = path.to_str().unwrap();
    let mut out = Vec::new();
    let mut first = 0;
    for rg in 0..n {
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
            .unwrap()
            .with_row_groups(vec![rg])
            .build()
            .unwrap();
        for batch in reader {
            let batch = batch.unwrap();
            out.extend(rows_of(p, rg as u32, first, &batch, schema));
            first += batch.num_rows();
        }
    }
    out
}

/// Byte ranges of the chunks of `bytes` at `chunk_size`, by the rule in the
/// source crate's docs, written out independently: chunk `k + 1` starts
/// just after the first `\n` at or after `start_k + chunk_size - 1`.
pub fn text_chunks(bytes: &[u8], chunk_size: u64) -> Vec<Range<u64>> {
    let size = bytes.len() as u64;
    let mut out = Vec::new();
    let mut start = 0u64;
    while start < size {
        let target = start + chunk_size;
        let end = if target >= size {
            size
        } else {
            let from = (target - 1) as usize;
            bytes[from..]
                .iter()
                .position(|&b| b == b'\n')
                .map_or(size, |p| (from + p + 1) as u64)
        };
        out.push(start..end);
        start = end;
    }
    out
}

/// Every row of a CSV (with header) or JSON lines file, per chunk.
pub fn text_rows(path: &Path, chunk_size: u64, schema: &SchemaRef, kind: SourceKind) -> Vec<Row> {
    let bytes = std::fs::read(path).unwrap();
    let p = path.to_str().unwrap();
    let mut out = Vec::new();
    let mut first = 0;
    for (k, r) in text_chunks(&bytes, chunk_size).into_iter().enumerate() {
        let chunk = &bytes[r.start as usize..r.end as usize];
        let batches: Vec<RecordBatch> = match kind {
            SourceKind::Csv { has_header, .. } => {
                arrow::csv::ReaderBuilder::new(Arc::clone(schema))
                    .with_header(has_header && k == 0)
                    .build(std::io::Cursor::new(chunk))
                    .unwrap()
                    .map(std::result::Result::unwrap)
                    .collect()
            }
            SourceKind::JsonLines => arrow::json::ReaderBuilder::new(Arc::clone(schema))
                .build(std::io::Cursor::new(chunk))
                .unwrap()
                .map(std::result::Result::unwrap)
                .collect(),
            SourceKind::Parquet => unreachable!(),
        };
        for b in batches {
            out.extend(rows_of(p, k as u32, first, &b, schema));
            first += b.num_rows();
        }
    }
    out
}

/// `(path, row group)` pairs holding a row for which `pred` holds.
pub fn holding(rows: &[Row], pred: impl Fn(&Row) -> bool) -> BTreeSet<(String, u32)> {
    rows.iter()
        .filter(|r| pred(r))
        .map(|r| (r.path.clone(), r.rg))
        .collect()
}

/// `(path, row group)` pairs of a plan.
pub fn plan_rgs(plan: &ScanPlan) -> BTreeSet<(String, u32)> {
    plan.entries
        .iter()
        .flat_map(|e| e.row_groups.iter().map(move |rg| (e.file.path.clone(), rg)))
        .collect()
}

/// The rows a consumer reads for `plan`.
pub fn planned_rows(plan: &ScanPlan, rows: &[Row]) -> Vec<Row> {
    let rgs = plan_rgs(plan);
    rows.iter()
        .filter(|r| rgs.contains(&(r.path.clone(), r.rg)))
        .cloned()
        .collect()
}

/// A reference evaluator for residual predicates over canonical bytes, with
/// SQL semantics for nulls: a null never satisfies a comparison, a null key
/// is in no `In` list, and a null timestamp never wins `LatestPerKey`.
pub fn apply_residual(pred: &Predicate, rows: Vec<Row>) -> Vec<Row> {
    match pred {
        Predicate::In { columns, values } => {
            let set: BTreeSet<&Vec<u8>> = values.iter().collect();
            rows.into_iter()
                .filter(|r| r.tuple(columns).is_some_and(|t| set.contains(&t)))
                .collect()
        }
        Predicate::Between { column, lo, hi } => rows
            .into_iter()
            .filter(|r| r.get(column).is_some_and(|v| lo <= v && v <= hi))
            .collect(),
        Predicate::NotOlderThan {
            columns,
            ts_column,
            bounds,
        } => {
            let bounds: BTreeMap<&Vec<u8>, &Vec<u8>> = bounds.iter().map(|(k, t)| (k, t)).collect();
            rows.into_iter()
                .filter(
                    |r| match r.tuple(columns).and_then(|k| bounds.get(&k).copied()) {
                        None => true,
                        Some(bound) => r.get(ts_column).is_some_and(|t| t >= bound),
                    },
                )
                .collect()
        }
        Predicate::LatestPerKey { columns, ts_column } => {
            let mut best: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
            for r in &rows {
                if let (Some(k), Some(t)) = (r.tuple(columns), r.get(ts_column)) {
                    let e = best.entry(k).or_insert_with(|| t.clone());
                    if t > e {
                        e.clone_from(t);
                    }
                }
            }
            rows.into_iter()
                .filter(|r| match (r.tuple(columns), r.get(ts_column)) {
                    (Some(k), Some(t)) => best.get(&k) == Some(t),
                    _ => false,
                })
                .collect()
        }
        Predicate::And { all } => all.iter().fold(rows, |rows, p| apply_residual(p, rows)),
        Predicate::Text {
            column,
            operator,
            values,
        } => rows
            .into_iter()
            .filter(|r| {
                r.get(column).map(|v| decode_utf8(v)).is_some_and(|v| {
                    values
                        .iter()
                        .any(|a| bellwether_core::text::matches(*operator, a, &v))
                })
            })
            .collect(),
        Predicate::SemiJoin { .. } | Predicate::Join { .. } | Predicate::WindowPartition { .. } => {
            panic!("a lookup's residual is never relational: {pred:?}")
        }
    }
}

/// The string a canonical `Utf8` encoding holds (`0x06`, the bytes with
/// `0x00` escaped as `0x00 0xFF`, then `0x00 0x00`).
pub fn decode_utf8(enc: &[u8]) -> String {
    assert_eq!(enc.first(), Some(&0x06), "not a canonical Utf8 encoding");
    let mut out = Vec::new();
    let mut i = 1;
    while i < enc.len() {
        match (enc[i], enc.get(i + 1)) {
            (0x00, Some(0xFF)) => {
                out.push(0);
                i += 2;
            }
            (0x00, Some(0x00)) => break,
            (b, _) => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).expect("canonical Utf8 holds UTF-8")
}

/// The rows a lookup must return, by definition: rows whose `columns` tuple
/// is in `keys` and whose `ts` is the newest (at or before `asof`) of all
/// rows of the same temporal key, anywhere in the dataset.
pub fn latest_truth(
    rows: &[Row],
    columns: &[String],
    keys: &[Vec<u8>],
    tkey: &[String],
    ts: &str,
    asof: Option<&[u8]>,
) -> Vec<Row> {
    let mut best: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for r in rows {
        if let (Some(k), Some(t)) = (r.tuple(tkey), r.get(ts)) {
            if asof.is_some_and(|a| t.as_slice() > a) {
                continue;
            }
            let e = best.entry(k).or_insert_with(|| t.clone());
            if t > e {
                e.clone_from(t);
            }
        }
    }
    let keys: BTreeSet<&Vec<u8>> = keys.iter().collect();
    rows.iter()
        .filter(|r| r.tuple(columns).is_some_and(|t| keys.contains(&t)))
        .filter(|r| match (r.tuple(tkey), r.get(ts)) {
            (Some(k), Some(t)) => best.get(&k) == Some(t),
            _ => false,
        })
        .cloned()
        .collect()
}

pub fn sorted(mut rows: Vec<Row>) -> Vec<Row> {
    rows.sort();
    rows
}

/// Plans an exact lookup and checks it against the ground truth: every row
/// group holding a key is planned (invariant 1), and the residual applied
/// to the planned rows returns exactly the matching rows (ADR 011).
pub fn check_keys(
    planner: &Planner<ObjectStoreBackend>,
    rows: &[Row],
    specs: &[KeySpec],
    columns: &[String],
    keys: &[Vec<u8>],
) -> ScanPlan {
    let lookup = bellwether_build::Lookup::Keys {
        columns: columns.to_vec(),
        keys: keys.to_vec(),
    };
    let plan = planner.plan(specs, &lookup).unwrap();
    let wanted: BTreeSet<&Vec<u8>> = keys.iter().collect();
    let matches = |r: &Row| r.tuple(columns).is_some_and(|t| wanted.contains(&t));
    let truth = holding(rows, matches);
    let planned = plan_rgs(&plan);
    assert!(
        truth.is_subset(&planned),
        "missing row groups: {:?}",
        truth.difference(&planned).collect::<Vec<_>>()
    );
    let got = sorted(apply_residual(
        plan.residual.as_ref().unwrap(),
        planned_rows(&plan, rows),
    ));
    let want = sorted(rows.iter().filter(|r| matches(r)).cloned().collect());
    assert_eq!(got, want, "residual over the planned rows");
    assert_eq!(plan.row_groups_selected, planned.len() as u64);
    assert_eq!(
        plan.bytes,
        plan.entries.iter().map(|e| e.bytes).sum::<u64>()
    );
    plan
}

/// Plans a latest or asof lookup and checks the residual over the planned
/// rows against the definition (`latest_truth`).
pub fn check_latest(
    planner: &Planner<ObjectStoreBackend>,
    rows: &[Row],
    specs: &[KeySpec],
    columns: &[String],
    keys: &[Vec<u8>],
    tkey: &[String],
    ts_col: &str,
    asof: Option<(i64, Vec<u8>)>,
) -> ScanPlan {
    let lookup = match &asof {
        None => bellwether_build::Lookup::Latest {
            columns: columns.to_vec(),
            keys: keys.to_vec(),
        },
        Some((ts, _)) => bellwether_build::Lookup::AsOf {
            columns: columns.to_vec(),
            keys: keys.to_vec(),
            ts: *ts,
        },
    };
    let plan = planner.plan(specs, &lookup).unwrap();
    let want = sorted(latest_truth(
        rows,
        columns,
        keys,
        tkey,
        ts_col,
        asof.as_ref().map(|(_, b)| b.as_slice()),
    ));
    let truth = want
        .iter()
        .map(|r| (r.path.clone(), r.rg))
        .collect::<BTreeSet<_>>();
    let planned = plan_rgs(&plan);
    assert!(
        truth.is_subset(&planned),
        "missing row groups: {:?}",
        truth.difference(&planned).collect::<Vec<_>>()
    );
    let got = sorted(apply_residual(
        plan.residual.as_ref().unwrap(),
        planned_rows(&plan, rows),
    ));
    assert_eq!(got, want, "residual over the planned rows");
    plan
}

// ---------------------------------------------------------------------------
// Observers
// ---------------------------------------------------------------------------

/// Counts the build's source calls and the row-group batches alive at each
/// read, through weak references to every column it is handed.
#[derive(Default)]
pub struct Probe {
    pub laid_out: AtomicU64,
    pub listed: AtomicU64,
    pub reads: AtomicU64,
    pub peak_alive: AtomicU64,
    alive: Mutex<Vec<Vec<Weak<dyn Array>>>>,
    pub columns: Mutex<BTreeSet<Vec<String>>>,
    pub read_ids: Mutex<Vec<RowGroupId>>,
    pub listed_files: Mutex<Vec<String>>,
}

impl BuildObserver for Probe {
    fn file_laid_out(&self, _file: &FileFingerprint) {
        self.laid_out.fetch_add(1, Ordering::SeqCst);
    }

    fn row_groups_listed(&self, file: &FileFingerprint) {
        self.listed.fetch_add(1, Ordering::SeqCst);
        self.listed_files.lock().unwrap().push(file.path.clone());
    }

    fn row_group_read(&self, id: RowGroupId, columns: &[String], batch: &RecordBatch) {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.read_ids.lock().unwrap().push(id);
        self.columns.lock().unwrap().insert(columns.to_vec());
        let mut alive = self.alive.lock().unwrap();
        alive.retain(|batch| batch.iter().any(|w| w.strong_count() > 0));
        alive.push(batch.columns().iter().map(Arc::downgrade).collect());
        self.peak_alive
            .fetch_max(alive.len() as u64, Ordering::SeqCst);
    }
}

// ---------------------------------------------------------------------------
// A store that runs a hook before its first manifest CAS
// ---------------------------------------------------------------------------

type Hook = Box<dyn FnOnce() + Send>;

/// Delegates to an inner store; the first conditional write of the manifest
/// first runs `hook` (for example a competing build), then proceeds.
pub struct HookStore {
    pub inner: Arc<ObjectStoreBackend>,
    hook: Mutex<Option<Hook>>,
}

impl HookStore {
    pub fn new(inner: Arc<ObjectStoreBackend>, hook: Hook) -> Self {
        Self {
            inner,
            hook: Mutex::new(Some(hook)),
        }
    }

    fn fire(&self, path: &str) {
        if path.ends_with("manifest.json") {
            let hook = self.hook.lock().unwrap().take();
            if let Some(h) = hook {
                h();
            }
        }
    }
}

impl Store for HookStore {
    fn get(&self, path: &str) -> Result<(Bytes, StoreVersion)> {
        self.inner.get(path)
    }
    fn put_if_absent(&self, path: &str, body: Bytes) -> Result<StoreVersion> {
        self.fire(path);
        self.inner.put_if_absent(path, body)
    }
    fn put_if_version(
        &self,
        path: &str,
        body: Bytes,
        expected: &StoreVersion,
    ) -> Result<StoreVersion> {
        self.fire(path);
        self.inner.put_if_version(path, body, expected)
    }
    fn put_content_addressed(&self, path: &str, body: Bytes) -> Result<()> {
        self.inner.put_content_addressed(path, body)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
    fn delete(&self, path: &str) -> Result<()> {
        self.inner.delete(path)
    }
}

impl StoreExt for HookStore {
    fn get_opt(&self, path: &str) -> Result<Option<(Bytes, StoreVersion)>> {
        self.inner.get_opt(path)
    }
    fn exists(&self, path: &str) -> Result<bool> {
        self.inner.exists(path)
    }
    fn get_range(&self, path: &str, range: Range<u64>) -> Result<Option<Bytes>> {
        self.inner.get_range(path, range)
    }
    fn list_pages(
        &self,
        prefix: &str,
        page_size: usize,
        f: &mut dyn FnMut(&[ObjectMeta]) -> Result<ControlFlow<()>>,
    ) -> Result<()> {
        self.inner.list_pages(prefix, page_size, f)
    }
}

/// The raw manifest bytes under `idx`.
pub fn manifest_bytes(store: &ObjectStoreBackend) -> Bytes {
    store.get("idx/manifest.json").unwrap().0
}

/// Builds and asserts a commit happened.
pub fn build_ok(
    b: &Builder<ObjectStoreBackend>,
    root: &str,
    specs: &[KeySpec],
    opts: &BuildOptions,
) -> BuildReport {
    let r = b.build(root, specs, opts).unwrap();
    assert!(r.sequence.is_some(), "expected a commit: {r:?}");
    r
}
