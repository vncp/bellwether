//! Shared test support: a bench-like Parquet layout generator, a brute-force
//! fanout oracle independent of the crate under test, a spy `Source`, and an
//! in-memory `Source` for memory and footer tests.

#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::missing_panics_doc,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow::array::{
    Array, ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_core::encoding::encode_array_value_as;
use bellwether_source::ParquetSource;
use bellwether_store::ObjectStoreBackend;
use bellwether_types::{ColumnStats, Error, FileFingerprint, Hash, Result, RowGroupMeta, Source};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::file::properties::WriterProperties;

pub const DAY_US: i64 = 86_400 * 1_000_000;
pub const WINDOW_US: i64 = 20 * 60 * 1_000_000;
pub const STATUSES: [(&str, u64); 5] = [
    ("posted", 60),
    ("settled", 25),
    ("pending", 10),
    ("reversed", 4),
    ("void", 1),
];

/// `SplitMix64`.
pub fn mix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn mix3(a: u64, b: u64, c: u64) -> u64 {
    mix(mix(mix(a) ^ b) ^ c)
}

pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let m = i64::from(m);
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

/// The start of the 20-minute window as `YYYYMMDDHHMM`.
pub fn batch_stamp(write_us: i64) -> i64 {
    let start = write_us.div_euclid(WINDOW_US) * WINDOW_US;
    let days = start.div_euclid(DAY_US);
    let minute = (start - days * DAY_US) / 60_000_000;
    let (y, m, d) = civil_from_days(days);
    ((y * 100 + i64::from(m)) * 100 + i64::from(d)) * 10_000 + (minute / 60) * 100 + minute % 60
}

/// A scaled-down version of the benchmark's synthetic ledger: rows of a day
/// go round-robin to the day's files as if from parallel writers, so every
/// 20-minute `batch_id` spans every file of its day.
#[derive(Clone, Debug)]
pub struct Layout {
    pub days: u32,
    pub files_per_day: u32,
    pub rgs_per_file: u32,
    pub rows_per_rg: u32,
    pub entities: u64,
    pub late_fraction: f64,
    pub payload_bytes: usize,
    pub seed: u64,
}

impl Layout {
    /// Several files per day, late rows: every pattern planted.
    pub fn planted() -> Self {
        Self {
            days: 3,
            files_per_day: 6,
            rgs_per_file: 2,
            rows_per_rg: 200,
            entities: 3_000,
            late_fraction: 0.05,
            payload_bytes: 48,
            seed: 7,
        }
    }

    /// One file per day and no late rows: no batch stamp, no time scatter.
    pub fn flat() -> Self {
        Self {
            days: 3,
            files_per_day: 1,
            rgs_per_file: 3,
            rows_per_rg: 200,
            entities: 3_000,
            late_fraction: 0.0,
            payload_bytes: 48,
            seed: 11,
        }
    }

    pub fn rows(&self) -> u64 {
        u64::from(self.days)
            * u64::from(self.files_per_day)
            * u64::from(self.rgs_per_file)
            * u64::from(self.rows_per_rg)
    }

    pub fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("entity_id", DataType::UInt64, false),
            Field::new("batch_id", DataType::Int64, false),
            Field::new(
                "version_ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("status", DataType::Utf8, false),
            Field::new("payload", DataType::Utf8, false),
        ]))
    }

    /// Writes `<dir>/day=YYYY-MM-DD/part-NNNNN.parquet` and returns `dir`.
    pub fn write(&self, dir: &Path) -> PathBuf {
        std::fs::create_dir_all(dir).unwrap();
        let start = days_from_civil(2026, 1, 1) * DAY_US;
        let schema = Self::schema();
        let mut global_file = 0u32;
        for day in 0..self.days {
            let (y, m, d) = civil_from_days(start / DAY_US + i64::from(day));
            let day_dir = dir.join(format!("day={y:04}-{m:02}-{d:02}"));
            std::fs::create_dir_all(&day_dir).unwrap();
            let day_start = start + i64::from(day) * DAY_US;
            let files = u64::from(self.files_per_day);
            let rows_day = files * u64::from(self.rgs_per_file) * u64::from(self.rows_per_rg);
            let slot = (DAY_US / rows_day as i64).max(1);
            for f in 0..self.files_per_day {
                let path = day_dir.join(format!("part-{global_file:05}.parquet"));
                global_file += 1;
                let props = WriterProperties::builder()
                    .set_max_row_group_row_count(Some(self.rows_per_rg as usize))
                    .build();
                let mut w =
                    ArrowWriter::try_new(File::create(&path).unwrap(), schema.clone(), Some(props))
                        .unwrap();
                for rg in 0..self.rgs_per_file {
                    let mut e = Vec::new();
                    let mut b = Vec::new();
                    let mut t = Vec::new();
                    let mut s = Vec::new();
                    let mut p = Vec::new();
                    for i in 0..u64::from(self.rows_per_rg) {
                        let pos = u64::from(rg) * u64::from(self.rows_per_rg) + i;
                        let j = u64::from(f) + files * pos;
                        let r = mix3(self.seed, u64::from(day), j);
                        let write_us = day_start
                            + (i128::from(j) * i128::from(DAY_US) / i128::from(rows_day)) as i64
                            + (mix(r ^ 1) % slot as u64) as i64;
                        let late = day > 0
                            && ((mix(r ^ 2) % 1_000_000) as f64) < self.late_fraction * 1_000_000.0;
                        let back = if day > 0 {
                            1 + (mix(r ^ 3) % u64::from(day.min(3))) as i64
                        } else {
                            0
                        };
                        e.push(mix(r ^ 4) % self.entities);
                        b.push(batch_stamp(write_us));
                        t.push(if late {
                            write_us - back * DAY_US
                        } else {
                            write_us
                        });
                        let mut roll = mix(r ^ 5) % 100;
                        let mut status = STATUSES[0].0;
                        for (name, weight) in STATUSES {
                            if roll < weight {
                                status = name;
                                break;
                            }
                            roll -= weight;
                        }
                        s.push(status.to_string());
                        let mut text = String::with_capacity(self.payload_bytes);
                        let mut bits = mix(r ^ 6);
                        for k in 0..self.payload_bytes {
                            if k % 12 == 0 {
                                bits = mix(bits ^ k as u64);
                            }
                            text.push(char::from(b'a' + (bits % 26) as u8));
                            bits >>= 5;
                        }
                        p.push(text);
                    }
                    let batch = RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(UInt64Array::from(e)),
                            Arc::new(Int64Array::from(b)),
                            Arc::new(TimestampMicrosecondArray::from(t).with_timezone("UTC")),
                            Arc::new(StringArray::from(s)),
                            Arc::new(StringArray::from(p)),
                        ],
                    )
                    .unwrap();
                    w.write(&batch).unwrap();
                    w.flush().unwrap();
                }
                w.close().unwrap();
            }
        }
        dir.to_path_buf()
    }
}

/// An exact fanout computed the slow way.
#[derive(Clone, Debug, PartialEq)]
pub struct Oracle {
    pub distinct: u64,
    /// Row groups per value, ascending.
    pub rgs: Vec<u64>,
    /// Files per value, ascending.
    pub files: Vec<u64>,
}

pub fn nearest_rank(sorted: &[u64], q: f64) -> u64 {
    let n = sorted.len();
    let rank = ((q * n as f64).ceil() as usize).clamp(1, n);
    sorted[rank - 1]
}

impl Oracle {
    pub fn files_per_value(&self) -> [f64; 3] {
        [
            nearest_rank(&self.files, 0.5) as f64,
            nearest_rank(&self.files, 0.95) as f64,
            *self.files.last().unwrap() as f64,
        ]
    }

    pub fn row_groups_per_value(&self) -> [f64; 3] {
        [
            nearest_rank(&self.rgs, 0.5) as f64,
            nearest_rank(&self.rgs, 0.95) as f64,
            *self.rgs.last().unwrap() as f64,
        ]
    }

    /// The population quantile at rank `q`, clamped into `[0, 1]`.
    pub fn rg_quantile(&self, q: f64) -> f64 {
        nearest_rank(&self.rgs, q.clamp(0.0, 1.0)) as f64
    }

    pub fn file_quantile(&self, q: f64) -> f64 {
        nearest_rank(&self.files, q.clamp(0.0, 1.0)) as f64
    }
}

/// Renders one cell for the oracle: a typed string, or `None` for null.
fn cell(array: &dyn Array, row: usize) -> Option<String> {
    if array.is_null(row) {
        return None;
    }
    let any = array.as_any();
    if let Some(a) = any.downcast_ref::<UInt64Array>() {
        return Some(format!("u{}", a.value(row)));
    }
    if let Some(a) = any.downcast_ref::<Int64Array>() {
        return Some(format!("i{}", a.value(row)));
    }
    if let Some(a) = any.downcast_ref::<StringArray>() {
        return Some(format!("s{}", a.value(row)));
    }
    if let Some(a) = any.downcast_ref::<TimestampMicrosecondArray>() {
        return Some(format!("t{}", a.value(row)));
    }
    panic!("oracle: unsupported type {}", array.data_type());
}

/// Every `.parquet` file under `dir`, sorted by path.
pub fn parquet_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).unwrap() {
            let p = entry.unwrap().path();
            let name = p.file_name().unwrap().to_string_lossy().to_string();
            if name.starts_with('.') || name.starts_with('_') {
                continue;
            }
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|e| e == "parquet") {
                out.push(p);
            }
        }
    }
    out.sort();
    out
}

/// Exact fanout of every key in `keys` (each a list of column names) over
/// every Parquet file under `dir`, read with the `parquet` crate directly.
/// Nulls follow ADR 014 as amended: a null leading column is not indexed; a
/// null trailing column is a distinct marker.
pub fn oracle(dir: &Path, keys: &[Vec<&str>]) -> Vec<Oracle> {
    type Sets = (BTreeSet<(usize, usize)>, BTreeSet<usize>);
    let mut maps: Vec<BTreeMap<Vec<Option<String>>, Sets>> = vec![BTreeMap::new(); keys.len()];
    for (fi, path) in parquet_files(dir).iter().enumerate() {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
        let n_rgs = builder.metadata().num_row_groups();
        let mut names: Vec<&str> = keys.iter().flatten().copied().collect();
        names.sort_unstable();
        names.dedup();
        for rg in 0..n_rgs {
            let b = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
            let roots: Vec<usize> = names
                .iter()
                .map(|n| b.schema().index_of(n).unwrap())
                .collect();
            let mask = ProjectionMask::roots(b.parquet_schema(), roots);
            let reader = b
                .with_row_groups(vec![rg])
                .with_projection(mask)
                .build()
                .unwrap();
            for batch in reader {
                let batch = batch.unwrap();
                for row in 0..batch.num_rows() {
                    for (k, key) in keys.iter().enumerate() {
                        let values: Vec<Option<String>> = key
                            .iter()
                            .map(|c| cell(batch.column_by_name(c).unwrap().as_ref(), row))
                            .collect();
                        if values[0].is_none() {
                            continue;
                        }
                        let e = maps[k].entry(values).or_default();
                        e.0.insert((fi, rg));
                        e.1.insert(fi);
                    }
                }
            }
        }
    }
    maps.into_iter()
        .map(|m| {
            let mut rgs: Vec<u64> = m.values().map(|s| s.0.len() as u64).collect();
            let mut files: Vec<u64> = m.values().map(|s| s.1.len() as u64).collect();
            rgs.sort_unstable();
            files.sort_unstable();
            Oracle {
                distinct: m.len() as u64,
                rgs,
                files,
            }
        })
        .collect()
}

/// A store rooted at a fresh temporary directory.
pub fn temp_store() -> (tempfile::TempDir, Arc<ObjectStoreBackend>) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ObjectStoreBackend::local(dir.path().join("index")).unwrap());
    (dir, store)
}

/// One `read_columns` call a spy saw.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Read {
    pub path: String,
    pub index_in_file: u32,
    pub columns: Vec<String>,
}

/// A `Source` that records every read and delegates to an inner source.
pub struct SpySource<S: Source> {
    pub inner: S,
    pub reads: Mutex<Vec<Read>>,
    pub footer_reads: Mutex<u64>,
    pub schema_calls: Mutex<u64>,
    pub discover_calls: Mutex<u64>,
}

impl<S: Source> SpySource<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            reads: Mutex::new(Vec::new()),
            footer_reads: Mutex::new(0),
            schema_calls: Mutex::new(0),
            discover_calls: Mutex::new(0),
        }
    }

    pub fn reads(&self) -> Vec<Read> {
        self.reads.lock().unwrap().clone()
    }

    pub fn clear(&self) {
        self.reads.lock().unwrap().clear();
        *self.footer_reads.lock().unwrap() = 0;
        *self.schema_calls.lock().unwrap() = 0;
        *self.discover_calls.lock().unwrap() = 0;
    }
}

impl SpySource<ParquetSource> {
    pub fn parquet() -> Self {
        Self::new(ParquetSource::new())
    }
}

impl<S: Source> Source for SpySource<S> {
    fn discover(&self, root: &str) -> Result<Vec<FileFingerprint>> {
        *self.discover_calls.lock().unwrap() += 1;
        self.inner.discover(root)
    }

    fn row_groups(&self, file: &FileFingerprint) -> Result<Vec<RowGroupMeta>> {
        *self.footer_reads.lock().unwrap() += 1;
        self.inner.row_groups(file)
    }

    fn read_columns(&self, rg: &RowGroupMeta, columns: &[String]) -> Result<RecordBatch> {
        self.reads.lock().unwrap().push(Read {
            path: rg.file.path.clone(),
            index_in_file: rg.index_in_file,
            columns: columns.to_vec(),
        });
        self.inner.read_columns(rg, columns)
    }

    fn schema(&self, root: &str) -> Result<SchemaRef> {
        *self.schema_calls.lock().unwrap() += 1;
        self.inner.schema(root)
    }
}

/// An in-memory dataset: files of row groups, each one `RecordBatch`, with
/// statistics computed from the data (canonical min/max, null counts) and
/// optional per-row-group distinct counts.
pub struct MemSource {
    pub schema: SchemaRef,
    pub files: Vec<(FileFingerprint, Vec<RecordBatch>)>,
    /// When true, statistics report each row group's exact distinct count.
    pub distinct_counts: bool,
}

impl MemSource {
    pub fn new(schema: SchemaRef, files: Vec<Vec<RecordBatch>>) -> Self {
        let files = files
            .into_iter()
            .enumerate()
            .map(|(i, rgs)| {
                let path = format!("mem/file-{i:05}");
                (
                    FileFingerprint {
                        path: path.clone(),
                        size: 1,
                        etag: None,
                        tail_blake3: Hash::of(path.as_bytes()),
                    },
                    rgs,
                )
            })
            .collect();
        Self {
            schema,
            files,
            distinct_counts: false,
        }
    }

    fn stats(&self, batch: &RecordBatch) -> Vec<ColumnStats> {
        self.schema
            .fields()
            .iter()
            .enumerate()
            .map(|(ci, f)| {
                let a = batch.column(ci);
                let mut seen = BTreeSet::new();
                for row in 0..a.len() {
                    let mut out = Vec::new();
                    if encode_array_value_as(a.as_ref(), row, f.data_type(), &mut out).unwrap() {
                        seen.insert(out);
                    }
                }
                ColumnStats {
                    column: f.name().clone(),
                    min: seen.first().cloned(),
                    max: seen.last().cloned(),
                    null_count: Some(a.null_count() as u64),
                    distinct_count: self.distinct_counts.then_some(seen.len() as u64),
                    has_bloom: false,
                    bytes: Some(8 * a.len() as u64),
                }
            })
            .collect()
    }
}

impl Source for MemSource {
    fn discover(&self, _root: &str) -> Result<Vec<FileFingerprint>> {
        Ok(self.files.iter().map(|(f, _)| f.clone()).collect())
    }

    fn row_groups(&self, file: &FileFingerprint) -> Result<Vec<RowGroupMeta>> {
        let (fp, rgs) = self
            .files
            .iter()
            .find(|(f, _)| f == file)
            .ok_or_else(|| Error::Source(format!("{} not found", file.path)))?;
        Ok(rgs
            .iter()
            .enumerate()
            .map(|(i, b)| RowGroupMeta {
                id: None,
                file: fp.clone(),
                index_in_file: i as u32,
                rows: b.num_rows() as u64,
                bytes: 8 * b.num_rows() as u64,
                stats: self.stats(b),
                byte_range: None,
            })
            .collect())
    }

    fn read_columns(&self, rg: &RowGroupMeta, columns: &[String]) -> Result<RecordBatch> {
        let (_, rgs) = self
            .files
            .iter()
            .find(|(f, _)| *f == rg.file)
            .ok_or_else(|| Error::Source(format!("{} not found", rg.file.path)))?;
        let batch = &rgs[rg.index_in_file as usize];
        let idx: Vec<usize> = columns
            .iter()
            .map(|c| {
                batch
                    .schema()
                    .index_of(c)
                    .map_err(|_| Error::KeySpec(format!("no column {c}")))
            })
            .collect::<Result<_>>()?;
        batch
            .project(&idx)
            .map_err(|e| Error::Source(e.to_string()))
    }

    fn schema(&self, _root: &str) -> Result<SchemaRef> {
        Ok(self.schema.clone())
    }
}

/// A one-column `UInt64` batch.
pub fn u64_batch(name: &str, values: Vec<u64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::UInt64, false)]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(UInt64Array::from(values)) as ArrayRef],
    )
    .unwrap()
}

/// Writes a bench-format workload (`workload.json` plus run files) over the
/// `entity_id` key sets given.
pub fn write_bench_workload(dir: &Path, runs: &[Vec<u64>]) -> Hash {
    let runs: Vec<(Option<&str>, Vec<u64>)> = runs.iter().map(|k| (None, k.clone())).collect();
    write_bench_workload_queries(dir, &runs)
}

/// [`write_bench_workload`] with each run's `query` (`None` omits it).
pub fn write_bench_workload_queries(dir: &Path, runs: &[(Option<&str>, Vec<u64>)]) -> Hash {
    std::fs::create_dir_all(dir.join("workload")).unwrap();
    let mut entries = Vec::new();
    let mut workload = blake3::Hasher::new();
    for (i, (query, keys)) in runs.iter().enumerate() {
        let mut keys = keys.clone();
        keys.sort_unstable();
        keys.dedup();
        let file = format!("workload/run-{i:04}.parquet");
        let batch = u64_batch("entity_id", keys.clone());
        let mut w =
            ArrowWriter::try_new(File::create(dir.join(&file)).unwrap(), batch.schema(), None)
                .unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let mut h = blake3::Hasher::new();
        for k in &keys {
            h.update(&k.to_be_bytes());
        }
        let run_hash = Hash(*h.finalize().as_bytes());
        workload.update(&run_hash.0);
        let mut entry = serde_json::json!({
            "run_id": format!("run-{i:04}"),
            "file": file,
            "keys": keys.len(),
            "hash": run_hash,
        });
        if let Some(q) = query {
            entry["query"] = serde_json::json!(q);
        }
        entries.push(entry);
    }
    let hash = Hash(*workload.finalize().as_bytes());
    let manifest = serde_json::json!({
        "format": "bellwether-bench-workload/1",
        "corpus_id": Hash::of(b"test"),
        "runs": entries,
        "hash": hash,
    });
    std::fs::write(
        dir.join("workload.json"),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    hash
}

pub fn path_str(p: &Path) -> String {
    p.to_str().unwrap().to_string()
}
