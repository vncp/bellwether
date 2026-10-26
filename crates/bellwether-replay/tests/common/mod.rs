//! Shared test support: a deterministic Parquet corpus written in-test, an
//! oracle that finds the row groups holding a key by scanning the files
//! directly (no index, no canonical encoding), and a minimal index builder
//! that commits segments and a manifest in the build crate's format, with
//! knobs (an incomplete spec, a coverage gap) the real builder never
//! produces.

#![allow(
    dead_code,
    missing_docs,
    clippy::unwrap_used,
    clippy::missing_panics_doc,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::too_many_lines
)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow::array::{
    Array, ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_core::segment::{SegmentWriter, WriterOptions};
use bellwether_replay::{KeyInput, MemoryKeySets, Workload, WorkloadRecorder};
use bellwether_source::ParquetSource;
use bellwether_store::{
    commit, ColumnDef, Extra, FileEntry, ObjectStoreBackend, RowGroupRange, RowGroupSpace,
    SchemaInfo, SegmentRef, SourceInfo, StagedSegments,
};
use bellwether_types::{
    FileFingerprint, KeySpec, Result, RowGroupId, RowGroupMeta, ScanPlan, Source,
};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::ColumnPath;

pub const STATUSES: [&str; 5] = ["new", "open", "held", "paid", "void"];

/// Deterministic 64-bit generator (`SplitMix64`).
pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Shape of an in-test corpus.
#[derive(Clone, Debug)]
pub struct CorpusSpec {
    pub files: usize,
    pub row_groups_per_file: usize,
    pub rows_per_row_group: usize,
    pub entities: u64,
    pub seed: u64,
    /// Every `bloom_every`-th file carries an embedded bloom filter on
    /// `entity_id`; `0` means none.
    pub bloom_every: usize,
    /// `(file, row group)` pairs whose `entity_id` and `version_ts` are
    /// null in every row.
    pub null_row_groups: Vec<(usize, usize)>,
}

impl Default for CorpusSpec {
    fn default() -> Self {
        Self {
            files: 6,
            row_groups_per_file: 3,
            rows_per_row_group: 200,
            entities: 1500,
            seed: 7,
            bloom_every: 2,
            null_row_groups: Vec::new(),
        }
    }
}

/// A corpus on disk.
pub struct Corpus {
    pub dir: tempfile::TempDir,
    pub data: String,
    pub schema: SchemaRef,
    pub spec: CorpusSpec,
}

impl Corpus {
    /// The corpus another process wrote at `data` with `spec`, with a fresh
    /// scratch directory of its own (for stores) in `dir`.
    pub fn open(data: &str, spec: &CorpusSpec) -> Self {
        Self {
            dir: tempfile::tempdir().unwrap(),
            data: data.to_string(),
            schema: corpus_schema(),
            spec: spec.clone(),
        }
    }

    pub fn source(&self) -> ParquetSource {
        ParquetSource::new().with_schema(Arc::clone(&self.schema))
    }

    /// Data files, sorted by path.
    pub fn files(&self) -> Vec<PathBuf> {
        let mut out = Vec::new();
        collect(Path::new(&self.data), &mut out);
        out.sort();
        out
    }
}

fn collect(dir: &Path, out: &mut Vec<PathBuf>) {
    for e in std::fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        if p.is_dir() {
            collect(&p, out);
        } else if p.extension().is_some_and(|x| x == "parquet") {
            out.push(p);
        }
    }
}

pub fn corpus_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::UInt64, true),
        Field::new("batch_id", DataType::Int64, false),
        Field::new(
            "version_ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("status", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

/// Writes the corpus: `files` files under `day=NN/` directories, each of
/// `row_groups_per_file` row groups. Entities are drawn with skew so keys
/// repeat across row groups and files; a few entity ids and timestamps are
/// null, and every one in `null_row_groups`. Byte-identical for equal specs.
pub fn write_corpus(spec: &CorpusSpec) -> Corpus {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let schema = corpus_schema();
    let mut rng = Rng(spec.seed);
    for f in 0..spec.files {
        let day = f / 2;
        let d = data.join(format!("day={day:02}"));
        std::fs::create_dir_all(&d).unwrap();
        let rows = spec.row_groups_per_file * spec.rows_per_row_group;
        let mut entity = Vec::with_capacity(rows);
        let mut batch_id = Vec::with_capacity(rows);
        let mut ts = Vec::with_capacity(rows);
        let mut status = Vec::with_capacity(rows);
        let mut payload = Vec::with_capacity(rows);
        for r in 0..rows {
            // Skew: half the rows come from the first tenth of entities.
            let e = if rng.below(2) == 0 {
                rng.below((spec.entities / 10).max(1))
            } else {
                rng.below(spec.entities)
            };
            entity.push((rng.below(97) != 0).then_some(e));
            batch_id.push(202_601_010_000 + (day as i64) * 10_000 + (r / 37) as i64);
            let t = 1_767_225_600_000_000_i64
                + (f as i64) * 3_600_000_000
                + (r as i64) * 1_000_000
                + rng.below(5_000_000) as i64;
            ts.push((rng.below(89) != 0).then_some(t));
            if spec
                .null_row_groups
                .contains(&(f, r / spec.rows_per_row_group))
            {
                *entity.last_mut().unwrap() = None;
                *ts.last_mut().unwrap() = None;
            }
            status.push(STATUSES[rng.below(5) as usize]);
            payload.push(format!("p{:04}", rng.below(10_000)));
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(UInt64Array::from(entity)) as ArrayRef,
                Arc::new(Int64Array::from(batch_id)),
                Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
                Arc::new(StringArray::from(status)),
                Arc::new(StringArray::from(payload)),
            ],
        )
        .unwrap();
        let mut props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(spec.rows_per_row_group))
            .set_created_by("bellwether-replay tests".into());
        if spec.bloom_every > 0 && f % spec.bloom_every == 0 {
            props = props
                .set_column_bloom_filter_enabled(ColumnPath::from("entity_id"), true)
                .set_column_bloom_filter_max_ndv(
                    ColumnPath::from("entity_id"),
                    spec.rows_per_row_group as u64,
                );
        }
        let file = File::create(d.join(format!("part-{f:03}.parquet"))).unwrap();
        let mut w = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props.build())).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
    Corpus {
        data: data.to_str().unwrap().to_string(),
        dir,
        schema,
        spec: spec.clone(),
    }
}

/// A typed key element, for the oracle.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Val {
    U(u64),
    I(i64),
    S(String),
}

fn val(arr: &ArrayRef, row: usize) -> Option<Val> {
    if arr.is_null(row) {
        return None;
    }
    let any = arr.as_any();
    if let Some(a) = any.downcast_ref::<UInt64Array>() {
        return Some(Val::U(a.value(row)));
    }
    if let Some(a) = any.downcast_ref::<Int64Array>() {
        return Some(Val::I(a.value(row)));
    }
    if let Some(a) = any.downcast_ref::<TimestampMicrosecondArray>() {
        return Some(Val::I(a.value(row)));
    }
    if let Some(a) = any.downcast_ref::<StringArray>() {
        return Some(Val::S(a.value(row).to_string()));
    }
    panic!("unsupported oracle column type {}", arr.data_type());
}

/// `(file path, row group)` of every row group holding one of `keys` over
/// `columns`, found by reading the files directly. A row with a null in a
/// key column matches nothing (ADR 014).
pub fn oracle(
    corpus: &Corpus,
    columns: &[&str],
    keys: &BTreeSet<Vec<Val>>,
) -> BTreeSet<(String, u32)> {
    let mut out = BTreeSet::new();
    for path in corpus.files() {
        for_each_row_group(&path, columns, |rg, cols| {
            let n = cols[0].len();
            for row in 0..n {
                let t: Option<Vec<Val>> = cols.iter().map(|c| val(c, row)).collect();
                if t.is_some_and(|t| keys.contains(&t)) {
                    out.insert((path.to_str().unwrap().to_string(), rg));
                    break;
                }
            }
        });
    }
    out
}

/// Calls `f` with each row group's index and the requested columns.
pub fn for_each_row_group(path: &Path, columns: &[&str], mut f: impl FnMut(u32, &[ArrayRef])) {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    let n = builder.metadata().num_row_groups();
    for rg in 0..n {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
        let schema = builder.schema().clone();
        let idx: Vec<usize> = columns
            .iter()
            .map(|c| schema.index_of(c).unwrap())
            .collect();
        let mask =
            parquet::arrow::ProjectionMask::roots(builder.parquet_schema(), idx.iter().copied());
        let reader = builder
            .with_row_groups(vec![rg])
            .with_projection(mask)
            .with_batch_size(1 << 20)
            .build()
            .unwrap();
        for batch in reader {
            let batch = batch.unwrap();
            let cols: Vec<ArrayRef> = columns
                .iter()
                .map(|c| Arc::clone(batch.column_by_name(c).unwrap()))
                .collect();
            f(rg as u32, &cols);
        }
    }
}

/// Every distinct non-null tuple over `columns` in the corpus.
pub fn distinct_tuples(corpus: &Corpus, columns: &[&str]) -> BTreeSet<Vec<Val>> {
    let mut out = BTreeSet::new();
    for path in corpus.files() {
        for_each_row_group(&path, columns, |_, cols| {
            for row in 0..cols[0].len() {
                if let Some(t) = cols
                    .iter()
                    .map(|c| val(c, row))
                    .collect::<Option<Vec<Val>>>()
                {
                    out.insert(t);
                }
            }
        });
    }
    out
}

/// `(file path, row group)` cells of a plan.
pub fn cells(plan: &ScanPlan) -> BTreeSet<(String, u32)> {
    plan.entries
        .iter()
        .flat_map(|e| e.row_groups.iter().map(move |rg| (e.file.path.clone(), rg)))
        .collect()
}

/// Compressed bytes of every row group, read from the footers with the
/// `parquet` crate (`RowGroupMetaData::compressed_size`), independently of
/// bellwether-source and of the replay catalog.
pub fn footer_bytes(paths: &[PathBuf]) -> BTreeMap<(String, u32), u64> {
    let mut out = BTreeMap::new();
    for path in paths {
        let r = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
        for i in 0..r.num_row_groups() {
            out.insert(
                (path.to_str().unwrap().to_string(), i as u32),
                r.metadata().row_group(i).compressed_size() as u64,
            );
        }
    }
    out
}

/// Files, row groups, and bytes of a set of cells, from the footers.
pub fn cost_of_cells(
    cells: &BTreeSet<(String, u32)>,
    bytes: &BTreeMap<(String, u32), u64>,
) -> (u64, u64, u64) {
    let files: BTreeSet<&String> = cells.iter().map(|(f, _)| f).collect();
    let b: u64 = cells.iter().map(|c| bytes[c]).sum();
    (files.len() as u64, cells.len() as u64, b)
}

/// A key batch over typed tuples.
pub fn key_batch(columns: &[&str], tuples: &BTreeSet<Vec<Val>>) -> RecordBatch {
    let schema = corpus_schema();
    let arrays: Vec<(String, ArrayRef)> = columns
        .iter()
        .enumerate()
        .map(|(j, c)| {
            let dt = schema.field_with_name(c).unwrap().data_type().clone();
            let arr: ArrayRef = match dt {
                DataType::UInt64 => Arc::new(UInt64Array::from(
                    tuples
                        .iter()
                        .map(|t| match &t[j] {
                            Val::U(v) => *v,
                            other => panic!("{other:?}"),
                        })
                        .collect::<Vec<_>>(),
                )),
                DataType::Int64 => Arc::new(Int64Array::from(
                    tuples
                        .iter()
                        .map(|t| match &t[j] {
                            Val::I(v) => *v,
                            other => panic!("{other:?}"),
                        })
                        .collect::<Vec<_>>(),
                )),
                DataType::Utf8 => Arc::new(StringArray::from(
                    tuples
                        .iter()
                        .map(|t| match &t[j] {
                            Val::S(v) => v.clone(),
                            other => panic!("{other:?}"),
                        })
                        .collect::<Vec<_>>(),
                )),
                other => panic!("unsupported key type {other}"),
            };
            ((*c).to_string(), arr)
        })
        .collect();
    RecordBatch::try_from_iter(arrays).unwrap()
}

/// Runs of entity ids: a mix of present and absent keys at a spread of
/// sizes, deterministic for a seed.
pub fn entity_runs(corpus: &Corpus, runs: usize, seed: u64) -> Vec<(String, BTreeSet<Vec<Val>>)> {
    let mut rng = Rng(seed);
    let sizes = [1usize, 2, 3, 5, 8, 13, 40, 120];
    (0..runs)
        .map(|i| {
            let n = sizes[i % sizes.len()];
            let mut keys = BTreeSet::new();
            while keys.len() < n {
                // Some keys beyond the entity range: present nowhere.
                keys.insert(vec![Val::U(rng.below(corpus.spec.entities + 200))]);
            }
            (format!("run-{i:03}"), keys)
        })
        .collect()
}

/// Records typed runs into a fresh in-memory key-set store.
pub fn record(
    columns: &[&str],
    runs: &[(String, BTreeSet<Vec<Val>>)],
) -> (MemoryKeySets, Workload) {
    let keys = MemoryKeySets::new();
    let mut rec = WorkloadRecorder::new(&keys, corpus_schema());
    for (id, tuples) in runs {
        rec.record(id, KeyInput::Batch(&key_batch(columns, tuples)))
            .unwrap();
    }
    let w = rec.finish().unwrap();
    (keys, w)
}

/// How [`build_index`] commits.
#[derive(Clone, Copy, Debug)]
pub struct BuildOptions {
    /// Write each spec as two segments over halves of the id space.
    pub split: bool,
    /// Record the last spec's completeness (every other spec's always is).
    pub complete: bool,
    /// Drop the last spec's second-half segment while still recording its
    /// completeness.
    pub leave_gap: bool,
}

impl Default for BuildOptions {
    fn default() -> Self {
        Self {
            split: true,
            complete: true,
            leave_gap: false,
        }
    }
}

/// Builds segments for `specs` over the corpus and commits one manifest,
/// in the build crate's format. Returns the committed sequence.
pub fn build_index(
    store: &Arc<ObjectStoreBackend>,
    root: &str,
    corpus: &Corpus,
    specs: &[KeySpec],
    opts: BuildOptions,
) -> u64 {
    let source = corpus.source();
    let mut fps: Vec<FileFingerprint> = source.discover(&corpus.data).unwrap();
    fps.sort_by(|a, b| a.path.cmp(&b.path));
    let mut rgs: Vec<(RowGroupId, RowGroupMeta)> = Vec::new();
    let mut files = Vec::new();
    for fp in &fps {
        let metas = source.row_groups(fp).unwrap();
        let mut ids = Vec::new();
        for m in metas {
            let id = RowGroupId(rgs.len() as u32);
            ids.push(id);
            rgs.push((id, m));
        }
        files.push(FileEntry {
            fingerprint: fp.clone(),
            rowgroup_ids: ids,
            chunk_size: None,
            chunks: None,
            extra: Extra::new(),
            rowgroup_rows: None,
            rowgroup_bytes: None,
            column_bytes: None,
        });
    }
    let n = rgs.len() as u32;
    let mid = if opts.split { n / 2 } else { n };
    let mut staged = StagedSegments::new();
    let mut refs = Vec::new();
    for (si, spec) in specs.iter().enumerate() {
        let last = si + 1 == specs.len();
        let mut cols: Vec<String> = spec.columns().to_vec();
        if let KeySpec::Temporal { ts_column, .. } = spec {
            cols.push(ts_column.clone());
        }
        for (lo, hi) in [(0, mid), (mid, n)] {
            if lo >= hi || (opts.leave_gap && last && lo > 0) {
                continue;
            }
            let mut w =
                SegmentWriter::for_dataset(spec.clone(), &corpus.schema, WriterOptions::default())
                    .unwrap();
            // The ref below claims coverage of lo..hi, so the segment
            // records it (docs/format.md).
            w.set_rowgroup_range(RowGroupId(lo)..RowGroupId(hi))
                .unwrap();
            for (id, meta) in &rgs[lo as usize..hi as usize] {
                let batch = source.read_columns(meta, &cols).unwrap();
                w.add_batch(*id, &batch).unwrap();
            }
            let seg = w.finish().unwrap();
            let bytes = seg.bytes.len() as u64;
            let id = staged.stage(Bytes::from(seg.bytes));
            refs.push(SegmentRef {
                id,
                key_spec: spec.name(),
                rowgroup_range: RowGroupRange::new(lo, hi),
                bytes,
                kind: None,
                extra: Extra::new(),
            });
        }
    }
    let columns: Vec<ColumnDef> = corpus
        .schema
        .fields()
        .iter()
        .map(|f| ColumnDef {
            name: f.name().clone(),
            data_type: f.data_type().to_string(),
            nullable: f.is_nullable(),
            extra: Extra::new(),
        })
        .collect();
    let committed = commit(&**store, root, &staged, |m| {
        let mut m = m.clone();
        m.source = SourceInfo {
            kind: "parquet".into(),
            root: corpus.data.clone(),
            fingerprint_policy: None,
            partitioning: None,
            extra: Extra::new(),
        };
        m.schema = SchemaInfo {
            columns: columns.clone(),
            key_specs: specs.to_vec(),
            sidecars: Vec::new(),
            extra: Extra::new(),
        };
        m.rowgroup_space = RowGroupSpace {
            next_id: n,
            files: files.clone(),
            tombstones: Vec::new(),
            extra: Extra::new(),
        };
        m.segments.clone_from(&refs);
        m.key_spec_completeness = BTreeMap::new();
        for (si, s) in specs.iter().enumerate() {
            if opts.complete || si + 1 < specs.len() {
                m.key_spec_completeness.insert(s.name(), m.sequence + 1);
            }
        }
        Ok(m)
    })
    .unwrap();
    committed.sequence
}

/// A [`Source`] that records every call, for proving what replay reads.
pub struct Counting<S> {
    pub inner: S,
    pub reads: Mutex<Vec<(String, u32, Vec<String>)>>,
    pub row_group_calls: Mutex<u64>,
}

impl<S> Counting<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            reads: Mutex::new(Vec::new()),
            row_group_calls: Mutex::new(0),
        }
    }

    pub fn reads(&self) -> Vec<(String, u32, Vec<String>)> {
        self.reads.lock().unwrap().clone()
    }

    pub fn clear(&self) {
        self.reads.lock().unwrap().clear();
    }
}

impl<S: Source> Source for Counting<S> {
    fn discover(&self, root: &str) -> Result<Vec<FileFingerprint>> {
        self.inner.discover(root)
    }

    fn row_groups(&self, file: &FileFingerprint) -> Result<Vec<RowGroupMeta>> {
        *self.row_group_calls.lock().unwrap() += 1;
        self.inner.row_groups(file)
    }

    fn read_columns(&self, rg: &RowGroupMeta, columns: &[String]) -> Result<RecordBatch> {
        self.reads
            .lock()
            .unwrap()
            .push((rg.file.path.clone(), rg.index_in_file, columns.to_vec()));
        self.inner.read_columns(rg, columns)
    }

    fn schema(&self, root: &str) -> Result<SchemaRef> {
        self.inner.schema(root)
    }
}

/// The bench corpus named by `BELLWETHER_BENCH_CORPUS`, or `None` with a
/// message saying the test was skipped and how to run it.
pub fn bench_corpus(test: &str) -> Option<PathBuf> {
    match std::env::var_os("BELLWETHER_BENCH_CORPUS") {
        Some(p) if Path::new(&p).join("workload.json").exists() => Some(PathBuf::from(p)),
        _ => {
            eprintln!(
                "SKIPPED {test}: set BELLWETHER_BENCH_CORPUS to a bellwether-bench corpus \
                 directory (generate, workload, ground-truth, and run with --out \
                 <corpus>/results.json); see crates/bellwether-replay/README.md"
            );
            None
        }
    }
}
