//! The synthetic versioned ledger: parameters, the deterministic row model,
//! the on-disk manifest, and a metadata-only reader shared by the ground
//! truth and the harness.
//!
//! # Row model
//!
//! Day `d` writes `R_d = files_d * row_groups_per_file * rows_per_row_group`
//! rows. Each day re-versions `A_d = min(R_d, round(churn * entities))`
//! entities: the first `A_d` positions of a per-day pseudo-random permutation
//! of `0..entities`. Row `j` of day `d` (rows of a day are numbered in write
//! time order) is a version of entity `perm_d(j mod A_d)`, so when
//! `R_d <= A_d` every entity has at most one version per day.
//!
//! The day's files are written as if by `files_d` parallel writers: row `j`
//! goes to file `j mod files_d`. Row group `r` of every file of a day
//! therefore covers the same slice of the day's timeline, and every 20-minute
//! `batch_id` window spans every file of its day. That is the planted
//! anti-pattern: `batch_id` has many distinct values and looks like an id,
//! but it is a write-time stamp and each value touches many files. The
//! generator still writes one file and one row group at a time: the row
//! numbers that land in a given row group are computed, not buffered.
//!
//! A `late_fraction` of rows on days after the first carry a `version_ts`
//! back-dated by one to three days (late-arriving corrections), which widens
//! row-group `version_ts` ranges far beyond the rows they hold.

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use bellwether_types::{FileFingerprint, Hash};
use bytes::{Buf, Bytes};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::ProjectionMask;
use parquet::file::reader::{ChunkReader, Length};
use parquet::file::statistics::Statistics;
use serde::{Deserialize, Serialize};

use crate::stats::{mix64, mix_all};
use crate::{io_err, read_json, BenchError, Result};

/// Manifest format tag written into `corpus.json`.
pub const CORPUS_FORMAT: &str = "bellwether-bench-corpus/1";
/// Width of one batch window in microseconds (20 minutes).
pub const BATCH_WINDOW_US: i64 = 20 * 60 * 1_000_000;
/// Microseconds per day.
pub const DAY_US: i64 = 86_400 * 1_000_000;
/// Batch windows per day.
pub const WINDOWS_PER_DAY: i64 = DAY_US / BATCH_WINDOW_US;
/// The synthetic ledger's first day.
pub const START_DATE: (i64, u32, u32) = (2026, 1, 1);
/// Column order of every generated file.
pub const COLUMNS: [&str; 6] = [
    "entity_id",
    "batch_id",
    "version_ts",
    "status",
    "amount",
    "payload",
];
/// Index of `entity_id` in [`COLUMNS`].
pub const COL_ENTITY: usize = 0;
/// Index of `batch_id` in [`COLUMNS`].
pub const COL_BATCH: usize = 1;
/// Index of `version_ts` in [`COLUMNS`].
pub const COL_TS: usize = 2;
/// Index of `status` in [`COLUMNS`].
pub const COL_STATUS: usize = 3;
/// Index of `amount` in [`COLUMNS`].
pub const COL_AMOUNT: usize = 4;
/// Index of `payload` in [`COLUMNS`].
pub const COL_PAYLOAD: usize = 5;

/// Generator parameters. Everything the corpus contains is a pure function
/// of these.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GenParams {
    /// RNG seed.
    pub seed: u64,
    /// Total files across all days. Must be at least `days`.
    pub files: u32,
    /// Row groups per file.
    pub row_groups_per_file: u32,
    /// Rows per row group.
    pub rows_per_row_group: u32,
    /// Distinct entities that can ever appear.
    pub entities: u64,
    /// Fraction of entities re-versioned per day, in `(0, 1]`.
    pub churn: f64,
    /// Days, one partition directory each.
    pub days: u32,
    /// Bytes of the wide `payload` string per row.
    pub payload_bytes: u32,
    /// Fraction of rows (after day 0) whose `version_ts` is back-dated.
    pub late_fraction: f64,
}

impl GenParams {
    /// Checks the parameters are coherent.
    pub fn validate(&self) -> Result<()> {
        let bad = |m: &str| Err(BenchError::Invalid(m.to_string()));
        if self.days == 0 {
            return bad("days must be at least 1");
        }
        if self.files < self.days {
            return bad("files must be at least days (one partition per day)");
        }
        if self.row_groups_per_file == 0 || self.rows_per_row_group == 0 {
            return bad("row groups per file and rows per row group must be positive");
        }
        if self.entities == 0 {
            return bad("entities must be positive");
        }
        if !(self.churn > 0.0 && self.churn <= 1.0) {
            return bad("churn must be in (0, 1]");
        }
        if !(0.0..=1.0).contains(&self.late_fraction) {
            return bad("late fraction must be in [0, 1]");
        }
        if self.days > 3650 {
            return bad("days must be at most 3650");
        }
        Ok(())
    }

    /// Entities re-versioned on a day with `rows` rows.
    #[must_use]
    pub fn active_for(&self, rows: u64) -> u64 {
        let target = ((self.churn * self.entities as f64).round() as u64).clamp(1, self.entities);
        target.min(rows).max(1)
    }

    /// The per-day layout.
    #[must_use]
    pub fn layout(&self) -> Vec<DayLayout> {
        let base = self.files / self.days;
        let extra = self.files % self.days;
        let mut first_file = 0;
        (0..self.days)
            .map(|day| {
                let files = base + u32::from(day < extra);
                let rows = u64::from(files)
                    * u64::from(self.row_groups_per_file)
                    * u64::from(self.rows_per_row_group);
                let out = DayLayout {
                    day,
                    date: date_string(day),
                    files,
                    first_file,
                    rows,
                    active: self.active_for(rows),
                };
                first_file += files;
                out
            })
            .collect()
    }

    /// Total rows the corpus will contain.
    #[must_use]
    pub fn total_rows(&self) -> u64 {
        u64::from(self.files)
            * u64::from(self.row_groups_per_file)
            * u64::from(self.rows_per_row_group)
    }

    /// A rough upper estimate of one row group's in-memory Arrow size, used
    /// to state the generator's memory bound before it runs.
    #[must_use]
    pub fn estimated_row_group_bytes(&self) -> u64 {
        // entity_id, batch_id, version_ts: 8 bytes each; amount: 16;
        // status: <= 8 bytes plus a 4-byte offset; payload plus its offset.
        let per_row = 8 + 8 + 8 + 16 + 12 + 4 + u64::from(self.payload_bytes);
        per_row * u64::from(self.rows_per_row_group)
    }

    /// A rung of the pre-registered corpus ladder (docs/benchmark.md,
    /// "pre-registered sweep values", revised 2026-10-24): days × files per
    /// day, 8 row groups of 100,000 rows per file, churn 0.05 per day,
    /// 256-byte payload, corpus seed 7. `late_fraction` is the WP10 default.
    #[must_use]
    pub fn rung(rung: Rung) -> Self {
        let (days, files_per_day, entities) = match rung {
            Rung::Small => (10, 1, 1_000_000),
            Rung::Medium => (30, 4, 1_000_000),
            Rung::Large => (30, 32, 10_000_000),
        };
        Self {
            seed: 7,
            files: days * files_per_day,
            row_groups_per_file: 8,
            rows_per_row_group: 100_000,
            entities,
            churn: 0.05,
            days,
            payload_bytes: 256,
            late_fraction: 0.02,
        }
    }

    /// What a corpus with these parameters will contain, computed from the
    /// row model without writing anything.
    #[must_use]
    pub fn summary(&self) -> LayoutSummary {
        let layout = self.layout();
        let row_groups = u64::from(self.files) * u64::from(self.row_groups_per_file);
        let versions_per_day: Vec<f64> = layout
            .iter()
            .map(|d| d.rows as f64 / d.active.max(1) as f64)
            .collect();
        LayoutSummary {
            files: u64::from(self.files),
            files_per_day: layout.iter().map(|d| u64::from(d.files)).max().unwrap_or(0),
            row_groups,
            rows: self.total_rows(),
            rows_per_day: layout.iter().map(|d| d.rows).max().unwrap_or(0),
            active_per_day: layout.iter().map(|d| d.active).max().unwrap_or(0),
            versions_per_active_entity_per_day: versions_per_day
                .iter()
                .copied()
                .fold(0.0, f64::max),
            estimated_arrow_bytes: self.estimated_row_group_bytes() * row_groups,
        }
    }
}

/// The three rungs of the pre-registered corpus ladder.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rung {
    /// 10 days × 1 file per day, 1,000,000 entities.
    Small,
    /// 30 days × 4 files per day, 1,000,000 entities.
    Medium,
    /// 30 days × 32 files per day, 10,000,000 entities.
    Large,
}

/// What a corpus will contain (`generate --dry-run`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LayoutSummary {
    /// Files.
    pub files: u64,
    /// Files on the busiest day.
    pub files_per_day: u64,
    /// Row groups.
    pub row_groups: u64,
    /// Rows.
    pub rows: u64,
    /// Rows on the busiest day.
    pub rows_per_day: u64,
    /// Entities re-versioned per day.
    pub active_per_day: u64,
    /// Versions each re-versioned entity gets per day (rows per day over
    /// re-versioned entities); 1.0 means one version per entity per day.
    pub versions_per_active_entity_per_day: f64,
    /// Arrow size in memory (estimate) of all rows, from the row-group
    /// estimate. The
    /// Snappy-compressed files are smaller: 2.1 GB on disk against a 2.5 GB
    /// estimate for the small rung.
    pub estimated_arrow_bytes: u64,
}

/// One day's share of the corpus.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DayLayout {
    /// Zero-based day.
    pub day: u32,
    /// `YYYY-MM-DD`.
    pub date: String,
    /// Files written for this day.
    pub files: u32,
    /// Global index of the day's first file.
    pub first_file: u32,
    /// Rows written this day.
    pub rows: u64,
    /// Entities re-versioned this day.
    pub active: u64,
}

impl DayLayout {
    /// The day's entity permutation.
    #[must_use]
    pub fn permutation(&self, params: &GenParams) -> Permutation {
        Permutation::new(
            params.entities,
            mix_all(&[params.seed, 0xDA7, u64::from(self.day)]),
        )
    }

    /// Microsecond timestamp of this day's midnight, UTC.
    #[must_use]
    pub fn start_us(&self) -> i64 {
        day_start_us(self.day)
    }
}

/// Relative path of a data file under the corpus root.
#[must_use]
pub fn file_rel_path(date: &str, global_file: u32) -> String {
    format!("data/day={date}/part-{global_file:05}.parquet")
}

/// `corpus.json`: everything needed to locate and interpret the data. It is a
/// pure function of [`GenParams`], so its hash identifies the corpus.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CorpusManifest {
    /// [`CORPUS_FORMAT`].
    pub format: String,
    /// Parameters.
    pub params: GenParams,
    /// Per-day layout.
    pub days: Vec<DayLayout>,
    /// Data files relative to the corpus root, in write order.
    pub files: Vec<String>,
    /// Total rows.
    pub rows: u64,
    /// Total row groups.
    pub row_groups: u64,
}

/// A keyed bijection on `0..n`: a four-round Feistel network on the smallest
/// even-bit power of two covering `n`, with cycle walking. O(1) memory, so
/// the generator and the workload agree on each day's active entities
/// without materializing them.
#[derive(Clone, Debug)]
pub struct Permutation {
    n: u64,
    half_bits: u32,
    mask: u64,
    keys: [u64; 4],
}

impl Permutation {
    /// A permutation of `0..n` keyed by `seed`.
    #[must_use]
    pub fn new(n: u64, seed: u64) -> Self {
        let bits = 64 - n.saturating_sub(1).leading_zeros();
        let half_bits = bits.div_ceil(2).max(1);
        let mask = (1u64 << half_bits) - 1;
        let keys = [
            mix64(seed ^ 1),
            mix64(seed ^ 2),
            mix64(seed ^ 3),
            mix64(seed ^ 4),
        ];
        Self {
            n,
            half_bits,
            mask,
            keys,
        }
    }

    fn round_trip(&self, v: u64) -> u64 {
        let mut l = v >> self.half_bits;
        let mut r = v & self.mask;
        for k in self.keys {
            let f = mix64(r ^ k) & self.mask;
            (l, r) = (r, l ^ f);
        }
        (l << self.half_bits) | r
    }

    /// The image of `x`, which must be below `n`.
    #[must_use]
    pub fn apply(&self, x: u64) -> u64 {
        debug_assert!(x < self.n);
        let mut v = x;
        loop {
            v = self.round_trip(v);
            if v < self.n {
                return v;
            }
        }
    }
}

/// Days since 1970-01-01 of a civil date (proleptic Gregorian).
#[must_use]
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

/// Civil date of a day count since 1970-01-01.
#[must_use]
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

fn start_epoch_day() -> i64 {
    days_from_civil(START_DATE.0, START_DATE.1, START_DATE.2)
}

/// Midnight UTC of zero-based day `day`, in microseconds since the epoch.
#[must_use]
pub fn day_start_us(day: u32) -> i64 {
    (start_epoch_day() + i64::from(day)) * DAY_US
}

/// `YYYY-MM-DD` of zero-based day `day`.
#[must_use]
pub fn date_string(day: u32) -> String {
    let (y, m, d) = civil_from_days(start_epoch_day() + i64::from(day));
    format!("{y:04}-{m:02}-{d:02}")
}

/// The batch stamp for a write time: the start of its 20-minute window as the
/// integer `YYYYMMDDHHMM`. It looks like an id; it is a timestamp.
#[must_use]
pub fn batch_id_for(write_us: i64) -> i64 {
    let window_start = write_us.div_euclid(BATCH_WINDOW_US) * BATCH_WINDOW_US;
    let days = window_start.div_euclid(DAY_US);
    let minute_of_day = (window_start - days * DAY_US) / 60_000_000;
    let (y, m, d) = civil_from_days(days);
    ((y * 100 + i64::from(m)) * 100 + i64::from(d)) * 10_000
        + (minute_of_day / 60) * 100
        + minute_of_day % 60
}

/// Metadata of one row group, read from the Parquet footer only.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowGroupInfo {
    /// Rows.
    pub rows: u64,
    /// Compressed bytes on disk, summed over column chunks.
    pub bytes: u64,
    /// `entity_id` min/max from footer statistics.
    pub entity_range: Option<(u64, u64)>,
    /// `version_ts` min/max (microseconds) from footer statistics.
    pub ts_range: Option<(i64, i64)>,
}

/// One data file with its footer metadata.
#[derive(Clone, Debug)]
pub struct FileMeta {
    /// Absolute or root-joined path.
    pub path: PathBuf,
    /// Fingerprint per `bellwether-types`.
    pub fingerprint: FileFingerprint,
    /// Row groups in file order.
    pub row_groups: Vec<RowGroupInfo>,
    /// Footer-cached reader.
    pub reader: RowGroupReader,
}

/// A generated corpus opened for reading: manifest plus footer metadata of
/// every file. Holds no row data.
#[derive(Clone, Debug)]
pub struct Corpus {
    /// Corpus root directory.
    pub root: PathBuf,
    /// Parsed `corpus.json`.
    pub manifest: CorpusManifest,
    /// BLAKE3 of the `corpus.json` bytes.
    pub id: Hash,
    /// Files in manifest order.
    pub files: Vec<FileMeta>,
}

impl Corpus {
    /// Opens a corpus directory written by `generate`.
    pub fn open(root: &Path) -> Result<Self> {
        let manifest_path = root.join("corpus.json");
        let bytes = std::fs::read(&manifest_path).map_err(io_err(&manifest_path))?;
        let id = Hash::of(&bytes);
        let manifest: CorpusManifest = read_json(&manifest_path)?;
        if manifest.format != CORPUS_FORMAT {
            return Err(BenchError::Invalid(format!(
                "{} has format {:?}, expected {CORPUS_FORMAT:?}",
                manifest_path.display(),
                manifest.format
            )));
        }
        let files = manifest
            .files
            .iter()
            .map(|rel| read_file_meta(&root.join(rel), rel))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            root: root.to_path_buf(),
            manifest,
            id,
            files,
        })
    }

    /// Total row groups.
    #[must_use]
    pub fn row_groups_total(&self) -> u64 {
        self.files.iter().map(|f| f.row_groups.len() as u64).sum()
    }

    /// Total compressed bytes.
    #[must_use]
    pub fn bytes_total(&self) -> u64 {
        self.files
            .iter()
            .flat_map(|f| f.row_groups.iter())
            .map(|rg| rg.bytes)
            .sum()
    }

    /// Total file bytes on disk, including footers.
    #[must_use]
    pub fn file_bytes_total(&self) -> u64 {
        self.files.iter().map(|f| f.fingerprint.size).sum()
    }

    /// Index into [`Corpus::files`] of a path as a plan names it: relative to
    /// the corpus root (as in `corpus.json`), or absolute under the root.
    #[must_use]
    pub fn file_index(&self, path: &str) -> Option<usize> {
        let p = Path::new(path);
        let rel = if p.is_absolute() {
            p.strip_prefix(&self.root).ok()?.to_str()?.to_string()
        } else {
            path.trim_start_matches("./").to_string()
        };
        self.files.iter().position(|f| f.fingerprint.path == rel)
    }
}

/// Bytes one read pulled through the Parquet reader, counted inside the
/// [`ChunkReader`] that parquet-rs drives (not summed from metadata).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReadBytes {
    /// Bytes fetched from storage. Storage is read at column-chunk
    /// granularity, as an object-store reader issues range requests: the
    /// first time parquet-rs asks for any byte of a column chunk, the whole
    /// chunk is read once. A chunk parquet-rs never touches is never read.
    pub fetched: u64,
    /// Bytes handed to the page decoder: page headers plus page bodies.
    pub consumed: u64,
}

impl std::ops::AddAssign for ReadBytes {
    fn add_assign(&mut self, rhs: Self) {
        self.fetched += rhs.fetched;
        self.consumed += rhs.consumed;
    }
}

#[derive(Default)]
struct Counters {
    fetched: AtomicU64,
    consumed: AtomicU64,
}

/// Counts bytes handed to the decoder.
struct Consume<R> {
    inner: R,
    counters: Arc<Counters>,
}

impl<R: Read> Read for Consume<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.counters
            .consumed
            .fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }
}

/// A file as parquet-rs sees it. Requests that fall inside a column chunk
/// are served from that chunk, fetched (and counted) on first touch;
/// anything else is read directly and counted.
struct CountingFile {
    file: File,
    len: u64,
    /// `(start, len, row group)` of every column chunk, sorted by start.
    chunks: Arc<Vec<(u64, u64, usize)>>,
    /// Chunks fetched so far, by start offset. Holds at most the row group
    /// being read.
    cache: Mutex<HashMap<u64, Bytes>>,
    counters: Arc<Counters>,
}

impl CountingFile {
    fn chunk_of(&self, offset: u64) -> Option<(u64, u64)> {
        chunk_containing(&self.chunks, offset).map(|(start, len, _)| (start, len))
    }

    fn fetch(&self, start: u64, len: usize) -> std::io::Result<Bytes> {
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(start))?;
        let mut buf = vec![0u8; len];
        file.read_exact(&mut buf)?;
        self.counters
            .fetched
            .fetch_add(len as u64, Ordering::Relaxed);
        Ok(Bytes::from(buf))
    }

    fn chunk(&self, start: u64, len: u64) -> parquet::errors::Result<Bytes> {
        let mut cache = self.cache.lock().map_err(|_| {
            parquet::errors::ParquetError::General("chunk cache lock poisoned".into())
        })?;
        if let Some(bytes) = cache.get(&start) {
            return Ok(bytes.clone());
        }
        let bytes = self.fetch(start, len as usize)?;
        cache.insert(start, bytes.clone());
        Ok(bytes)
    }
}

impl Length for CountingFile {
    fn len(&self) -> u64 {
        self.len
    }
}

impl ChunkReader for CountingFile {
    type T = Consume<bytes::buf::Reader<Bytes>>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let rest = match self.chunk_of(start) {
            Some((chunk_start, len)) => self
                .chunk(chunk_start, len)?
                .slice((start - chunk_start) as usize..),
            // Outside every column chunk (page reads never are): read to EOF.
            None => self.fetch(start, self.len.saturating_sub(start) as usize)?,
        };
        Ok(Consume {
            inner: rest.reader(),
            counters: self.counters.clone(),
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let bytes = match self.chunk_of(start) {
            Some((chunk_start, len)) if start + length as u64 <= chunk_start + len => {
                let from = (start - chunk_start) as usize;
                self.chunk(chunk_start, len)?.slice(from..from + length)
            }
            _ => self.fetch(start, length)?,
        };
        self.counters
            .consumed
            .fetch_add(length as u64, Ordering::Relaxed);
        Ok(bytes)
    }
}

fn chunk_containing(chunks: &[(u64, u64, usize)], offset: u64) -> Option<(u64, u64, usize)> {
    let i = chunks.partition_point(|&(start, _, _)| start <= offset);
    let found = *chunks.get(i.checked_sub(1)?)?;
    (offset < found.0 + found.1).then_some(found)
}

/// A data file opened for row-group reads. The footer is parsed once; every
/// read opens its own file handle, so reads may run in parallel.
#[derive(Clone, Debug)]
pub struct RowGroupReader {
    path: PathBuf,
    meta: ArrowReaderMetadata,
    chunks: Arc<Vec<(u64, u64, usize)>>,
}

impl RowGroupReader {
    /// Parses the footer of `path`.
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).map_err(io_err(path))?;
        let meta = ArrowReaderMetadata::load(&file, ArrowReaderOptions::default())?;
        let mut chunks: Vec<(u64, u64, usize)> = meta
            .metadata()
            .row_groups()
            .iter()
            .enumerate()
            .flat_map(|(i, rg)| {
                rg.columns().iter().map(move |c| {
                    let (start, len) = c.byte_range();
                    (start, len, i)
                })
            })
            .collect();
        chunks.sort_unstable();
        Ok(Self {
            path: path.to_path_buf(),
            meta,
            chunks: Arc::new(chunks),
        })
    }

    /// Row groups in the file.
    #[must_use]
    pub fn num_row_groups(&self) -> usize {
        self.meta.metadata().num_row_groups()
    }

    /// The parsed footer.
    #[must_use]
    pub fn metadata(&self) -> &Arc<parquet::file::metadata::ParquetMetaData> {
        self.meta.metadata()
    }

    /// The path read.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The row group whose column chunk contains byte `offset`, if any.
    #[must_use]
    pub fn row_group_of(&self, offset: u64) -> Option<usize> {
        chunk_containing(&self.chunks, offset).map(|(_, _, rg)| rg)
    }

    /// Reads row group `rg`, projected to the given column indexes (all
    /// columns when `None`), and reports the bytes parquet-rs pulled to do
    /// it. Holds one row group. The footer is not re-read.
    pub fn read(
        &self,
        rg: usize,
        columns: Option<&[usize]>,
    ) -> Result<(Vec<RecordBatch>, ReadBytes)> {
        if rg >= self.num_row_groups() {
            return Err(BenchError::Invalid(format!(
                "{} has {} row groups; row group {rg} does not exist",
                self.path.display(),
                self.num_row_groups()
            )));
        }
        let file = File::open(&self.path).map_err(io_err(&self.path))?;
        let len = file.metadata().map_err(io_err(&self.path))?.len();
        let counters = Arc::new(Counters::default());
        let source = CountingFile {
            file,
            len,
            chunks: self.chunks.clone(),
            cache: Mutex::new(HashMap::new()),
            counters: counters.clone(),
        };
        let rows = self.meta.metadata().row_group(rg).num_rows();
        let mut builder =
            ParquetRecordBatchReaderBuilder::new_with_metadata(source, self.meta.clone())
                .with_row_groups(vec![rg])
                .with_batch_size(usize::try_from(rows).unwrap_or(8192).max(1));
        if let Some(cols) = columns {
            let mask = ProjectionMask::roots(builder.parquet_schema(), cols.iter().copied());
            builder = builder.with_projection(mask);
        }
        let reader = builder.build()?;
        let batches = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(BenchError::from)?;
        let bytes = ReadBytes {
            fetched: counters.fetched.load(Ordering::Relaxed),
            consumed: counters.consumed.load(Ordering::Relaxed),
        };
        Ok((batches, bytes))
    }
}

/// BLAKE3 of the last 64 KiB of a file (or all of it if smaller).
pub fn tail_blake3(path: &Path) -> Result<Hash> {
    let mut f = File::open(path).map_err(io_err(path))?;
    let len = f.metadata().map_err(io_err(path))?.len();
    let start = len.saturating_sub(64 * 1024);
    f.seek(SeekFrom::Start(start)).map_err(io_err(path))?;
    let mut buf = Vec::with_capacity((len - start) as usize);
    f.read_to_end(&mut buf).map_err(io_err(path))?;
    Ok(Hash::of(&buf))
}

fn i64_range(stats: Option<&Statistics>) -> Option<(i64, i64)> {
    match stats? {
        Statistics::Int64(s) => Some((*s.min_opt()?, *s.max_opt()?)),
        _ => None,
    }
}

fn read_file_meta(path: &Path, rel: &str) -> Result<FileMeta> {
    let size = std::fs::metadata(path).map_err(io_err(path))?.len();
    let reader = RowGroupReader::open(path)?;
    let row_groups = reader
        .meta
        .metadata()
        .row_groups()
        .iter()
        .map(|rg| RowGroupInfo {
            rows: rg.num_rows() as u64,
            bytes: rg.compressed_size() as u64,
            // UInt64 is stored as physical INT64; every generated id is below
            // 2^63, so signed and unsigned orderings agree.
            entity_range: i64_range(rg.column(COL_ENTITY).statistics())
                .map(|(lo, hi)| (lo as u64, hi as u64)),
            ts_range: i64_range(rg.column(COL_TS).statistics()),
        })
        .collect();
    Ok(FileMeta {
        path: path.to_path_buf(),
        fingerprint: FileFingerprint {
            path: rel.to_string(),
            size,
            etag: None,
            tail_blake3: tail_blake3(path)?,
        },
        row_groups,
        reader,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permutation_is_a_bijection() {
        for n in [1u64, 2, 3, 7, 100, 1000, 4097] {
            let p = Permutation::new(n, 42);
            let mut seen = vec![false; n as usize];
            for x in 0..n {
                let y = p.apply(x);
                assert!(y < n);
                assert!(!seen[y as usize], "collision at n={n}");
                seen[y as usize] = true;
            }
        }
    }

    #[test]
    fn rungs_match_the_preregistered_ladder() {
        let small = GenParams::rung(Rung::Small);
        let medium = GenParams::rung(Rung::Medium);
        let large = GenParams::rung(Rung::Large);
        for (p, days, files, entities) in [
            (&small, 10, 10, 1_000_000),
            (&medium, 30, 120, 1_000_000),
            (&large, 30, 960, 10_000_000),
        ] {
            assert_eq!(
                (p.days, p.files, p.entities),
                (days, files, entities),
                "{p:?}"
            );
            assert_eq!(
                (
                    p.row_groups_per_file,
                    p.rows_per_row_group,
                    p.payload_bytes,
                    p.seed
                ),
                (8, 100_000, 256, 7)
            );
            assert!((p.churn - 0.05).abs() < 1e-12);
            p.validate().unwrap();
        }
        let s = small.summary();
        assert_eq!(
            (s.files_per_day, s.rows_per_day, s.active_per_day),
            (1, 800_000, 50_000)
        );
        assert!((s.versions_per_active_entity_per_day - 16.0).abs() < 1e-9);
        assert_eq!(large.summary().rows, 768_000_000);
    }

    #[test]
    fn civil_dates_round_trip() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        for z in [-1000i64, 0, 19_000, 20_454, 20_820, 30_000] {
            let (y, m, d) = civil_from_days(z);
            assert_eq!(days_from_civil(y, m, d), z);
        }
        assert_eq!(date_string(0), "2026-01-01");
        assert_eq!(date_string(31), "2026-02-01");
    }

    #[test]
    fn batch_id_is_a_window_stamp() {
        let t = day_start_us(0) + (13 * 60 + 47) * 60 * 1_000_000;
        assert_eq!(batch_id_for(t), 2026_0101_1340);
        assert_eq!(batch_id_for(day_start_us(1)), 2026_0102_0000);
    }
}
