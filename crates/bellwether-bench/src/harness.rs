//! `run`: every recorded key set through every strategy.
//!
//! Strategies that produce a [`ScanPlan`] implement [`IndexUnderTest`]; the
//! Bellwether index plugs in here when WP1/WP4 land. The harness trusts
//! nothing a plan says about itself:
//!
//! - [`verify_plan`] resolves every entry against the corpus (path,
//!   fingerprint, row-group range) and recomputes files, row groups, and
//!   bytes from the footers. A plan naming an unknown file or row group is a
//!   failed run result; a plan whose own counts disagree is kept, scored on
//!   the recomputed cost, and flagged `cost_misreported`.
//! - The exact oracle (row groups that hold a queried key, by scanning
//!   `entity_id`) checks every plan for false negatives (invariant 1).
//! - Executing a plan reads every planned row group through a reader that
//!   counts the bytes parquet-rs pulls ([`crate::corpus::ReadBytes`]), so
//!   `bytes_read` is measured, next to the planned bytes from statistics.
//! - Results are compared row for row: matched count, exact `amount` total,
//!   and an order-independent digest of every matched row.
//!
//! External engines (DuckDB) cannot report which row groups they read, so
//! they report latency, matched rows, and the exact amount total only.
//!
//! Memory (ADR 018): the harness holds footer metadata, the row groups being
//! read by the worker threads, and, because the oracle is computed for every
//! run in one pass over the corpus ([`oracles`]), the union of all runs' key
//! sets. That union is at most the sum of the run sizes: 612,898 keys for
//! the pre-registered workload (200 runs at 50 / 10,000 / 100,000), a few
//! tens of megabytes with the per-key run lists.

use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::Path;
use std::time::Instant;

use arrow::array::{
    Array, Decimal128Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    UInt64Array,
};
use bellwether_types::{RunCost, ScanEntry, ScanPlan};
use parquet::bloom_filter::Sbbf;
use parquet::file::properties::ReaderProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::serialized_reader::ReadOptionsBuilder;
use rayon::prelude::*;
use roaring::RoaringBitmap;

use crate::corpus::{Corpus, ReadBytes, COLUMNS, COL_AMOUNT, COL_ENTITY, COL_PAYLOAD};
use crate::env::{capture, CorpusParameters};
use crate::generate::GeneratorStats;
use crate::results::{
    CorpusResult, Funnel, OracleResult, ReportedCost, Results, RunResult, StrategyInfo,
    StrategyKind, StrategyResult, StrategySummary, WorkloadSummary, DATAFUSION, DUCKDB, INDEX,
    PARQUET_STATS, RAW_SCAN, RESULTS_FORMAT,
};
use crate::stats::p50_p95_max_f64;
use crate::truth::Expected;
use crate::workload::{read_keys, WorkloadManifest};
use crate::{read_json, write_json, BenchError, Key, Result};

/// Anything that turns a key set into a scan plan.
///
/// - `keys` is sorted ascending and deduplicated.
/// - Entry paths are relative to the corpus root, as `corpus.json` lists
///   them, or absolute paths under the root. Fingerprints must match the
///   files (size and tail hash); `etag` is ignored.
/// - The plan must include every row group that holds a key (invariant 1);
///   extra row groups are allowed and show up as cost.
/// - The harness recomputes the plan's cost from the footers and records
///   the plan's own `bytes` and row-group counts beside it.
pub trait IndexUnderTest {
    /// Stable strategy name written to `results.json` (`index` for the
    /// Bellwether index, which the SLO budgets apply to).
    fn name(&self) -> &str;
    /// The plan for one lookup.
    fn plan(&self, keys: &[Key]) -> ScanPlan;
    /// Bytes the most recent [`IndexUnderTest::plan`] read from storage, for
    /// a strategy whose planning reads data (an engine whose pruning scans
    /// key columns, or the index reading segments). `None` when not
    /// measured. Reported beside the execution bytes, never folded into
    /// them (docs/benchmark.md, "latency accounting for engine baselines").
    fn plan_bytes_read(&self) -> Option<u64> {
        None
    }
}

/// Planned row groups by file index into [`Corpus::files`].
pub type Cells = BTreeMap<usize, RoaringBitmap>;

fn plan_from(corpus: &Corpus, mut keep: impl FnMut(usize, usize) -> bool) -> ScanPlan {
    let mut entries = Vec::new();
    let mut bytes = 0u64;
    let mut selected = 0u64;
    for (fi, file) in corpus.files.iter().enumerate() {
        let mut bitmap = RoaringBitmap::new();
        let mut file_bytes = 0u64;
        for (ri, rg) in file.row_groups.iter().enumerate() {
            if keep(fi, ri) {
                bitmap.insert(ri as u32);
                file_bytes += rg.bytes;
            }
        }
        if !bitmap.is_empty() {
            selected += bitmap.len();
            bytes += file_bytes;
            entries.push(ScanEntry {
                file: file.fingerprint.clone(),
                row_groups: bitmap,
                bytes: file_bytes,
                columns: None,
            });
        }
    }
    ScanPlan {
        manifest_version: corpus.id.to_hex(),
        entries,
        residual: None,
        bytes,
        row_groups_total: corpus.row_groups_total(),
        row_groups_selected: selected,
        projected_bytes: None,
        order: None,
        rows: None,
    }
}

/// Baseline: read every row group.
pub struct RawScan<'a> {
    corpus: &'a Corpus,
}

impl<'a> RawScan<'a> {
    /// Over a corpus.
    #[must_use]
    pub fn new(corpus: &'a Corpus) -> Self {
        Self { corpus }
    }
}

impl IndexUnderTest for RawScan<'_> {
    fn name(&self) -> &str {
        RAW_SCAN
    }

    fn plan(&self, _keys: &[Key]) -> ScanPlan {
        plan_from(self.corpus, |_, _| true)
    }

    fn plan_bytes_read(&self) -> Option<u64> {
        Some(0)
    }
}

/// Baseline: Parquet-native pruning, the way a reader without a sidecar
/// index prunes: footer min/max of `entity_id`, then the row group's
/// embedded split-block bloom filter. The filters are read once when the
/// strategy is built and kept, like footers. A row group without statistics
/// or without a filter is not pruned by that test.
pub struct ParquetStats<'a> {
    corpus: &'a Corpus,
    blooms: Vec<Vec<Option<Sbbf>>>,
}

impl<'a> ParquetStats<'a> {
    /// Over a corpus; reads every row group's `entity_id` bloom filter.
    pub fn new(corpus: &'a Corpus) -> Result<Self> {
        let blooms = corpus
            .files
            .iter()
            .map(|f| read_blooms(&f.path, COL_ENTITY))
            .collect::<Result<_>>()?;
        Ok(Self { corpus, blooms })
    }
}

/// The bloom filter of `column` in every row group of a file.
fn read_blooms(path: &Path, column: usize) -> Result<Vec<Option<Sbbf>>> {
    let file = File::open(path).map_err(crate::io_err(path))?;
    let options = ReadOptionsBuilder::new()
        .with_reader_properties(
            ReaderProperties::builder()
                .set_read_bloom_filter(true)
                .build(),
        )
        .build();
    let reader = SerializedFileReader::new_with_options(file, options)?;
    (0..reader.num_row_groups())
        .map(|i| {
            Ok(reader
                .get_row_group(i)?
                .get_column_bloom_filter(column)
                .cloned())
        })
        .collect()
}

impl IndexUnderTest for ParquetStats<'_> {
    fn name(&self) -> &str {
        PARQUET_STATS
    }

    fn plan(&self, keys: &[Key]) -> ScanPlan {
        plan_from(self.corpus, |fi, ri| {
            let Some((lo, hi)) = self.corpus.files[fi].row_groups[ri].entity_range else {
                return true;
            };
            let first = keys.partition_point(|&k| k < lo);
            let in_range = keys[first..].iter().take_while(|&&k| k <= hi);
            match &self.blooms[fi][ri] {
                // `entity_id` is UInt64 stored as INT64; the filter hashes the
                // 8-byte physical value.
                Some(bloom) => in_range.into_iter().any(|&k| bloom.check(&(k as i64))),
                None => in_range.into_iter().next().is_some(),
            }
        })
    }

    /// Planning reads nothing per lookup: footers and bloom filters were read
    /// once when the strategy was built (`setup_ms`).
    fn plan_bytes_read(&self) -> Option<u64> {
        Some(0)
    }
}

/// A plan resolved against the corpus, with its cost recomputed.
#[derive(Clone, Debug, PartialEq)]
pub struct VerifiedPlan {
    /// Planned row groups by file, duplicates merged.
    pub cells: Cells,
    /// Files, row groups, and bytes from the footers.
    pub cost: RunCost,
    /// What the plan said.
    pub reported: ReportedCost,
    /// Whether `reported` disagrees with `cost`.
    pub misreported: bool,
}

/// Resolves a plan against the corpus and recomputes its cost from the
/// footers. Errors (an unknown file, a stale fingerprint, a row group the
/// file does not have) mean the plan cannot be executed.
pub fn verify_plan(
    corpus: &Corpus,
    run_id: &str,
    keys: u64,
    plan: &ScanPlan,
) -> std::result::Result<VerifiedPlan, String> {
    let mut cells = Cells::new();
    let mut entry_bytes_mismatches = 0u64;
    for entry in &plan.entries {
        let fi = corpus
            .file_index(&entry.file.path)
            .ok_or_else(|| format!("plan names unknown file {:?}", entry.file.path))?;
        let file = &corpus.files[fi];
        if entry.file.size != file.fingerprint.size
            || entry.file.tail_blake3 != file.fingerprint.tail_blake3
        {
            return Err(format!(
                "plan's fingerprint for {} does not match the file (stale plan)",
                entry.file.path
            ));
        }
        let available = file.row_groups.len() as u32;
        if let Some(max) = entry.row_groups.max() {
            if max >= available {
                return Err(format!(
                    "plan names row group {max} of {}, which has {available}",
                    entry.file.path
                ));
            }
        }
        let entry_bytes: u64 = entry
            .row_groups
            .iter()
            .map(|rg| file.row_groups[rg as usize].bytes)
            .sum();
        entry_bytes_mismatches += u64::from(entry_bytes != entry.bytes);
        *cells.entry(fi).or_default() |= &entry.row_groups;
    }
    cells.retain(|_, b| !b.is_empty());
    let row_groups = cells.values().map(RoaringBitmap::len).sum();
    let bytes = cells
        .iter()
        .map(|(fi, b)| {
            b.iter()
                .map(|rg| corpus.files[*fi].row_groups[rg as usize].bytes)
                .sum::<u64>()
        })
        .sum();
    let cost = RunCost {
        measured: None,
        run_id: run_id.to_string(),
        keys,
        files: cells.len() as u64,
        row_groups,
        bytes,
        // Every number is a sum of footer statistics.
        exact: true,
        exactness: bellwether_types::Exactness::Exact,
        rows: None,
    };
    let reported = ReportedCost {
        bytes: plan.bytes,
        row_groups_selected: plan.row_groups_selected,
        row_groups_total: plan.row_groups_total,
        entry_bytes_mismatches,
    };
    let misreported = reported.bytes != cost.bytes
        || reported.row_groups_selected != cost.row_groups
        || reported.row_groups_total != corpus.row_groups_total()
        || entry_bytes_mismatches > 0;
    Ok(VerifiedPlan {
        cells,
        cost,
        reported,
        misreported,
    })
}

/// Oracle row groups missing from `cells`.
#[must_use]
pub fn false_negatives(cells: &Cells, oracle: &Cells) -> u64 {
    oracle
        .iter()
        .map(|(fi, want)| match cells.get(fi) {
            Some(have) => want.difference_len(have),
            None => want.len(),
        })
        .sum()
}

fn all_cells(corpus: &Corpus) -> Vec<(usize, usize)> {
    corpus
        .files
        .iter()
        .enumerate()
        .flat_map(|(fi, f)| (0..f.row_groups.len()).map(move |ri| (fi, ri)))
        .collect()
}

fn entity_col(batch: &RecordBatch, idx: usize) -> Result<&UInt64Array> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| BenchError::Invalid("entity_id is not uint64".into()))
}

fn amount_col(batch: &RecordBatch, idx: usize) -> Result<&Decimal128Array> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| BenchError::Invalid("amount is not decimal128".into()))
}

/// An `amount` total in cents as a decimal string with scale 2.
#[must_use]
pub fn format_cents(cents: i128) -> String {
    let sign = if cents < 0 { "-" } else { "" };
    let abs = cents.unsigned_abs();
    format!("{sign}{}.{:02}", abs / 100, abs % 100)
}

/// Parses a decimal string (scale at most 2) to cents.
#[must_use]
pub fn parse_cents(s: &str) -> Option<i128> {
    let s = s.trim().trim_matches('"');
    let (neg, digits) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s),
    };
    let (whole, frac) = digits.split_once('.').unwrap_or((digits, ""));
    if whole.is_empty() || frac.len() > 2 || !frac.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let whole: i128 = whole.parse().ok()?;
    let frac: i128 = format!("{frac:0<2}").parse().ok()?;
    let cents = whole.checked_mul(100)?.checked_add(frac)?;
    Some(if neg { -cents } else { cents })
}

/// Exact oracle for one key set. See [`oracles`].
pub fn oracle(corpus: &Corpus, keys: &HashSet<Key>) -> Result<(Cells, OracleResult)> {
    oracles(corpus, std::slice::from_ref(keys))?
        .pop()
        .ok_or_else(|| BenchError::Invalid("oracle produced no result".into()))
}

/// Exact oracles for every run in one pass: scans `entity_id` and `amount`
/// of every row group once and, per run, keeps the row groups that hold one
/// of its keys. Holds the union of the runs' key sets.
pub fn oracles(corpus: &Corpus, runs: &[HashSet<Key>]) -> Result<Vec<(Cells, OracleResult)>> {
    let mut runs_of: std::collections::HashMap<Key, Vec<u32>> = std::collections::HashMap::new();
    for (r, keys) in runs.iter().enumerate() {
        for &k in keys {
            runs_of.entry(k).or_default().push(r as u32);
        }
    }
    let cells = all_cells(corpus);
    // Per row group: (run, matched rows, amount cents) for runs with a hit.
    let per_cell: Vec<Vec<(u32, u64, i128)>> = cells
        .par_iter()
        .map(|&(fi, ri)| -> Result<Vec<(u32, u64, i128)>> {
            let (batches, _) = corpus.files[fi]
                .reader
                .read(ri, Some(&[COL_ENTITY, COL_AMOUNT]))?;
            let mut hits: BTreeMap<u32, (u64, i128)> = BTreeMap::new();
            for batch in batches {
                let e = entity_col(&batch, 0)?;
                let a = amount_col(&batch, 1)?;
                for i in 0..batch.num_rows() {
                    if let Some(rs) = runs_of.get(&e.value(i)) {
                        for &r in rs {
                            let hit = hits.entry(r).or_insert((0, 0));
                            hit.0 += 1;
                            hit.1 += a.value(i);
                        }
                    }
                }
            }
            Ok(hits.into_iter().map(|(r, (n, c))| (r, n, c)).collect())
        })
        .collect::<Result<_>>()?;
    let mut out: Vec<(Cells, u64, i128)> = vec![(Cells::new(), 0, 0); runs.len()];
    for (&(fi, ri), hits) in cells.iter().zip(&per_cell) {
        for &(r, n, c) in hits {
            let entry = &mut out[r as usize];
            entry.0.entry(fi).or_default().insert(ri as u32);
            entry.1 += n;
            entry.2 += c;
        }
    }
    Ok(out
        .into_iter()
        .map(|(hits, matched, cents)| {
            let row_groups = hits.values().map(RoaringBitmap::len).sum();
            let bytes = hits
                .iter()
                .map(|(fi, b)| {
                    b.iter()
                        .map(|rg| corpus.files[*fi].row_groups[rg as usize].bytes)
                        .sum::<u64>()
                })
                .sum();
            let result = OracleResult {
                files: hits.len() as u64,
                row_groups,
                bytes,
                matched_rows: matched,
                amount_total: format_cents(cents),
            };
            (hits, result)
        })
        .collect())
}

/// Deliberate executor faults, for tests that prove the checks catch them.
/// Never set in a real run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ScanFault {
    /// Execute plans faithfully.
    #[default]
    None,
    /// Skip the first planned row group of every plan.
    DropFirstPlannedRowGroup,
    /// Read every planned row group without its `payload` column.
    DropPayloadColumn,
    /// Also fetch and decode one row group the plan does not name (or, when
    /// the plan names every row group, one of them a second time).
    ReadExtraRowGroup,
}

/// What executing one plan produced.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ScanOutcome {
    /// Rows whose `entity_id` is a key.
    pub matched: u64,
    /// Exact `amount` total of those rows, in cents.
    pub amount_cents: i128,
    /// Wrapping sum of a 128-bit BLAKE3 digest per matched row: equal for
    /// equal multisets of rows, whatever the read order.
    pub digest: u128,
    /// Bytes parquet-rs pulled.
    pub bytes: ReadBytes,
}

fn str_value<'a>(batch: &'a RecordBatch, name: &str, i: usize) -> Option<&'a str> {
    batch
        .column_by_name(name)?
        .as_any()
        .downcast_ref::<StringArray>()
        .map(|a| a.value(i))
}

fn i64_value(batch: &RecordBatch, name: &str, i: usize) -> Option<i64> {
    let col = batch.column_by_name(name)?;
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return Some(a.value(i));
    }
    col.as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .map(|a| a.value(i))
}

fn row_digest(batch: &RecordBatch, entity: u64, amount: i128, i: usize) -> u128 {
    let mut h = blake3::Hasher::new();
    h.update(&entity.to_be_bytes());
    for name in [COLUMNS[1], COLUMNS[2]] {
        h.update(&i64_value(batch, name, i).unwrap_or(0).to_be_bytes());
    }
    for name in [COLUMNS[3], COLUMNS[5]] {
        let v = str_value(batch, name, i).unwrap_or("");
        h.update(&(v.len() as u64).to_be_bytes());
        h.update(v.as_bytes());
    }
    h.update(&amount.to_be_bytes());
    let mut first = [0u8; 16];
    first.copy_from_slice(&h.finalize().as_bytes()[..16]);
    u128::from_be_bytes(first)
}

/// Reads every planned row group (all columns) and filters to the keys: the
/// work a consumer does to make the plan row-exact (ADR 011).
pub fn execute(
    corpus: &Corpus,
    cells: &Cells,
    keys: &HashSet<Key>,
    fault: ScanFault,
) -> Result<ScanOutcome> {
    let mut work: Vec<(usize, usize)> = cells
        .iter()
        .flat_map(|(fi, b)| b.iter().map(move |rg| (*fi, rg as usize)))
        .collect();
    if fault == ScanFault::DropFirstPlannedRowGroup && !work.is_empty() {
        work.remove(0);
    }
    let without_payload: Vec<usize> = (0..COLUMNS.len()).filter(|&c| c != COL_PAYLOAD).collect();
    let projection = (fault == ScanFault::DropPayloadColumn).then_some(without_payload.as_slice());
    let outcomes: Vec<ScanOutcome> = work
        .par_iter()
        .map(|&(fi, ri)| -> Result<ScanOutcome> {
            let (batches, bytes) = corpus.files[fi].reader.read(ri, projection)?;
            let mut out = ScanOutcome {
                bytes,
                ..ScanOutcome::default()
            };
            for batch in batches {
                let e = entity_col(&batch, COL_ENTITY)?;
                let a = amount_col(&batch, COL_AMOUNT)?;
                for i in 0..batch.num_rows() {
                    let entity = e.value(i);
                    if keys.contains(&entity) {
                        let amount = a.value(i);
                        out.matched += 1;
                        out.amount_cents += amount;
                        out.digest = out
                            .digest
                            .wrapping_add(row_digest(&batch, entity, amount, i));
                    }
                }
            }
            Ok(out)
        })
        .collect::<Result<_>>()?;
    let mut total = outcomes
        .into_iter()
        .fold(ScanOutcome::default(), |mut acc, o| {
            acc.matched += o.matched;
            acc.amount_cents += o.amount_cents;
            acc.digest = acc.digest.wrapping_add(o.digest);
            acc.bytes += o.bytes;
            acc
        });
    if fault == ScanFault::ReadExtraRowGroup {
        let unplanned = all_cells(corpus)
            .into_iter()
            .find(|&(fi, ri)| !cells.get(&fi).is_some_and(|b| b.contains(ri as u32)));
        if let Some((fi, ri)) = unplanned.or_else(|| work.first().copied()) {
            // Fetched and decoded; its rows are not counted.
            let (_, bytes) = corpus.files[fi].reader.read(ri, None)?;
            total.bytes += bytes;
        }
    }
    Ok(total)
}

/// Whether to run the DuckDB engine baseline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DuckDbMode {
    /// Do not run it.
    Off,
    /// Run it when this build has the `duckdb` feature and a `duckdb` CLI is
    /// found; otherwise record it as unavailable, with the reason.
    Auto,
    /// Run it, and fail the harness if it is unavailable.
    Required,
}

/// Harness options.
#[derive(Clone, Debug)]
pub struct RunOptions {
    /// Read planned row groups (`scan_ms`, `bytes_read`); otherwise plan only.
    pub scan: bool,
    /// DuckDB baseline mode.
    pub duckdb: DuckDbMode,
    /// Executor fault injection for tests; [`ScanFault::None`] otherwise.
    pub fault: ScanFault,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            scan: true,
            duckdb: DuckDbMode::Auto,
            fault: ScanFault::None,
        }
    }
}

fn ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1e3
}

fn panic_text(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".into())
}

/// Plans, verifies, and (optionally) executes one strategy on one run.
fn run_strategy(
    corpus: &Corpus,
    planner: &dyn IndexUnderTest,
    run_id: &str,
    keys: &[Key],
    key_set: &HashSet<Key>,
    oracle_cells: &Cells,
    options: &RunOptions,
) -> StrategyResult {
    let mut r = StrategyResult::new(planner.name());
    let t = Instant::now();
    let outcome = catch_unwind(AssertUnwindSafe(|| planner.plan(keys)));
    let plan_ms = ms(t);
    r.plan_ms = Some(plan_ms);
    r.plan_bytes_read = planner.plan_bytes_read();
    let plan = match outcome {
        Ok(plan) => plan,
        Err(payload) => {
            r.error = Some(format!("plan() panicked: {}", panic_text(payload.as_ref())));
            return r;
        }
    };
    let verified = match verify_plan(corpus, run_id, keys.len() as u64, &plan) {
        Ok(v) => v,
        Err(e) => {
            r.error = Some(e);
            return r;
        }
    };
    r.false_negative_row_groups = Some(false_negatives(&verified.cells, oracle_cells));
    r.reported = Some(verified.reported.clone());
    r.cost_misreported = Some(verified.misreported);
    let mut scan_ms = None;
    if options.scan {
        let t = Instant::now();
        match execute(corpus, &verified.cells, key_set, options.fault) {
            Ok(out) => {
                scan_ms = Some(ms(t));
                r.bytes_read = Some(out.bytes.fetched);
                r.bytes_consumed = Some(out.bytes.consumed);
                r.bytes_read_minus_planned =
                    Some(out.bytes.fetched as i64 - verified.cost.bytes as i64);
                r.matched_rows = Some(out.matched);
                r.amount_total = Some(format_cents(out.amount_cents));
                r.rows_digest = Some(format!("{:032x}", out.digest));
            }
            Err(e) => r.error = Some(format!("scan failed: {e}")),
        }
    }
    r.scan_ms = scan_ms;
    r.latency_ms = Some(plan_ms + scan_ms.unwrap_or(0.0));
    r.cost = Some(verified.cost);
    r
}

/// Runs the harness over one corpus, with `extra` strategies (the index)
/// after the two baselines.
pub fn run_corpus(
    root: &Path,
    options: &RunOptions,
    extra: &[&dyn IndexUnderTest],
) -> Result<CorpusResult> {
    let corpus = Corpus::open(root)?;
    let workload_path = root.join("workload.json");
    if !workload_path.exists() {
        return Err(BenchError::Invalid(format!(
            "{} is missing; run `bellwether-bench workload --corpus {}` first",
            workload_path.display(),
            root.display()
        )));
    }
    let workload: WorkloadManifest = read_json(&workload_path)?;
    if workload.corpus_id != corpus.id {
        return Err(BenchError::Invalid(
            "workload.json was generated for a different corpus; regenerate it".into(),
        ));
    }
    let generator: Option<GeneratorStats> = {
        let p = root.join("generator-stats.json");
        if p.exists() {
            Some(read_json(&p)?)
        } else {
            None
        }
    };
    let ground_truth: Option<Expected> = {
        let p = root.join("expected.json");
        if p.exists() {
            let e: Expected = read_json(&p)?;
            (e.corpus_id == corpus.id).then_some(e)
        } else {
            None
        }
    };

    let raw = RawScan::new(&corpus);
    let t = Instant::now();
    let stats = ParquetStats::new(&corpus)?;
    let stats_setup = ms(t);
    let mut planners: Vec<&dyn IndexUnderTest> = vec![&raw, &stats];
    #[cfg(feature = "datafusion")]
    let datafusion = datafusion_baseline::DataFusionScan::new(&corpus)?;
    #[cfg(feature = "datafusion")]
    planners.push(&datafusion);
    planners.extend_from_slice(extra);

    let mut strategies: Vec<StrategyInfo> = planners
        .iter()
        .map(|p| StrategyInfo {
            name: p.name().to_string(),
            kind: StrategyKind::Plan,
            available: true,
            setup_ms: (p.name() == PARQUET_STATS).then_some(stats_setup),
            note: match p.name() {
                PARQUET_STATS => Some("footer min/max, then embedded bloom filters".into()),
                DATAFUSION => Some(
                    "DataFusion's Parquet scan pruning (statistics, page index, bloom filters); \
                     the plan is the row groups it read, and plan_ms includes that scan"
                        .into(),
                ),
                _ => None,
            },
        })
        .collect();
    if !cfg!(feature = "datafusion") {
        strategies.push(StrategyInfo {
            name: DATAFUSION.into(),
            kind: StrategyKind::Plan,
            available: false,
            setup_ms: None,
            note: Some("unavailable: built without `--features datafusion`".into()),
        });
    }
    let duck = engine::DuckDb::open(&corpus, options.duckdb);
    let duck_info = duck.info();
    if options.duckdb == DuckDbMode::Required && !duck_info.available {
        return Err(BenchError::Strategy {
            strategy: DUCKDB.into(),
            message: format!(
                "{}; drop --require-duckdb to run without it",
                duck_info.note.unwrap_or_default()
            ),
        });
    }
    strategies.push(duck_info);

    // Every run's keys are read up front so the oracle is one pass over the
    // corpus; this holds the union of the key sets (sum of run sizes).
    let key_lists: Vec<Vec<Key>> = workload
        .runs
        .iter()
        .map(|entry| read_keys(&root.join(&entry.file)))
        .collect::<Result<_>>()?;
    let key_sets: Vec<HashSet<Key>> = key_lists
        .iter()
        .map(|keys| keys.iter().copied().collect())
        .collect();
    let all_oracles = oracles(&corpus, &key_sets)?;

    let mut runs = Vec::with_capacity(workload.runs.len());
    for (((entry, keys), key_set), (oracle_cells, oracle)) in workload
        .runs
        .iter()
        .zip(&key_lists)
        .zip(&key_sets)
        .zip(all_oracles)
    {
        let key_path = root.join(&entry.file);
        let mut results: Vec<StrategyResult> = planners
            .iter()
            .map(|p| {
                run_strategy(
                    &corpus,
                    *p,
                    &entry.run_id,
                    keys,
                    key_set,
                    &oracle_cells,
                    options,
                )
            })
            .collect();
        if let Some(r) = duck.run(&key_path) {
            results.push(r);
        }
        let selected = |name: &str| {
            results
                .iter()
                .find(|r| r.strategy == name)
                .and_then(|r| r.cost.as_ref())
                .map(|c| c.row_groups)
        };
        let funnel = Funnel {
            row_groups_total: corpus.row_groups_total(),
            after_stats: selected(PARQUET_STATS),
            after_index: selected(INDEX),
            oracle: oracle.row_groups,
        };
        eprintln!(
            "  {} keys={:>7} oracle_rgs={:>6} stats_rgs={:>6} of {}",
            entry.run_id,
            keys.len(),
            oracle.row_groups,
            funnel.after_stats.unwrap_or(0),
            funnel.row_groups_total
        );
        runs.push(RunResult {
            run_id: entry.run_id.clone(),
            keys: keys.len() as u64,
            key_hash: entry.hash,
            oracle,
            funnel,
            results,
        });
    }

    let summary = summarize(&strategies, &runs);
    Ok(CorpusResult {
        corpus_id: corpus.id,
        path: root.display().to_string(),
        params: corpus.manifest.params.clone(),
        files_total: corpus.files.len() as u64,
        row_groups_total: corpus.row_groups_total(),
        rows_total: corpus.manifest.rows,
        bytes_total: corpus.bytes_total(),
        file_bytes_total: corpus.file_bytes_total(),
        generator,
        workload: WorkloadSummary {
            hash: workload.hash,
            runs: workload.runs.len() as u64,
            sizes_p50_p95_max: workload.sizes_p50_p95_max,
            clamped: workload.clamped,
        },
        scan_executed: options.scan,
        strategies,
        runs,
        build: Vec::new(),
        summary,
        ground_truth,
    })
}

fn summarize(strategies: &[StrategyInfo], runs: &[RunResult]) -> Vec<StrategySummary> {
    strategies
        .iter()
        .filter(|s| s.available)
        .map(|s| {
            let rs: Vec<&StrategyResult> = runs
                .iter()
                .filter_map(|run| run.results.iter().find(|r| r.strategy == s.name))
                .filter(|r| r.error.is_none())
                .collect();
            let mut lat: Vec<f64> = rs.iter().filter_map(|r| r.latency_ms).collect();
            lat.sort_by(f64::total_cmp);
            let mut plan: Vec<f64> = rs.iter().filter_map(|r| r.plan_ms).collect();
            plan.sort_by(f64::total_cmp);
            let costs: Vec<&RunCost> = rs.iter().filter_map(|r| r.cost.as_ref()).collect();
            let read: Vec<u64> = rs.iter().filter_map(|r| r.bytes_read).collect();
            StrategySummary {
                strategy: s.name.clone(),
                runs: rs.len() as u64,
                latency_ms: p50_p95_max_f64(&lat),
                plan_ms: (!plan.is_empty()).then(|| p50_p95_max_f64(&plan)),
                bytes_planned_total: (!costs.is_empty())
                    .then(|| costs.iter().map(|c| c.bytes).sum()),
                bytes_read_total: (!read.is_empty()).then(|| read.iter().sum()),
                row_groups_total: (!costs.is_empty())
                    .then(|| costs.iter().map(|c| c.row_groups).sum()),
            }
        })
        .collect()
}

/// Runs the harness over several corpora and writes `results.json`.
pub fn run_all(roots: &[&Path], out: &Path, options: &RunOptions) -> Result<Results> {
    let mut corpora = Vec::with_capacity(roots.len());
    for root in roots {
        eprintln!("corpus {}", root.display());
        corpora.push(run_corpus(root, options, &[])?);
    }
    let results = assemble(corpora);
    write_json(out, &results)?;
    Ok(results)
}

/// The DuckDB CLI's version line when this build has the `duckdb` feature
/// and the CLI runs; `None` otherwise.
#[must_use]
pub fn duckdb_cli_version() -> Option<String> {
    #[cfg(feature = "duckdb")]
    {
        let bin = std::env::var(engine::BIN_ENV).unwrap_or_else(|_| "duckdb".into());
        engine::version(&bin)
    }
    #[cfg(not(feature = "duckdb"))]
    {
        None
    }
}

/// Wraps corpus results with the format tag and the environment block.
#[must_use]
pub fn assemble(corpora: Vec<CorpusResult>) -> Results {
    let params = corpora
        .iter()
        .map(|c| CorpusParameters {
            corpus_id: c.corpus_id,
            path: c.path.clone(),
            params: c.params.clone(),
        })
        .collect();
    Results {
        format: RESULTS_FORMAT.into(),
        generated_by: format!("bellwether-bench {}", env!("CARGO_PKG_VERSION")),
        environment: capture(params),
        corpora,
    }
}

#[cfg(feature = "duckdb")]
mod engine {
    //! DuckDB engine baseline through the `duckdb` command-line shell. No
    //! DuckDB is compiled into this crate, so nothing heavy builds under
    //! `--all-features`. The binary is `$BELLWETHER_DUCKDB`, or `duckdb` on
    //! `PATH`.
    //!
    //! The shell runs as `duckdb -no-agent -init /dev/null -csv -noheader`
    //! with `AI_AGENT` removed from its environment: `-init /dev/null` keeps a
    //! user's `~/.duckdbrc` out, and DuckDB 2 otherwise switches to an
    //! "agent" output mode when `AI_AGENT` is set and stdout is not a
    //! terminal. The parser accepts only the requested format (one CSV row
    //! with the expected number of fields, one `.timer` line, nothing on
    //! stderr) and rejects anything else rather than guessing.
    //!
    //! Each run is one shell invocation that semi-joins the corpus against
    //! the run's key file and aggregates every column, so DuckDB must read
    //! them all. Latency is DuckDB's own `.timer` reading (query time,
    //! excluding process start; planning cannot be separated through the
    //! CLI). Files, row groups, and bytes are not observable and are `null`.

    use std::io::Write as _;
    use std::path::Path;
    use std::process::{Command, Stdio};
    use std::time::Instant;

    use super::{format_cents, ms, parse_cents, DuckDbMode, DUCKDB};
    use crate::corpus::Corpus;
    use crate::results::{StrategyInfo, StrategyKind, StrategyResult};

    /// Environment variable naming the DuckDB CLI binary.
    pub(crate) const BIN_ENV: &str = "BELLWETHER_DUCKDB";

    const NULL_INIT: &str = if cfg!(windows) { "NUL" } else { "/dev/null" };

    /// Fields of the lookup query's result row.
    const LOOKUP_FIELDS: usize = 6;

    pub(super) struct DuckDb {
        bin: Option<String>,
        glob: String,
        setup_ms: Option<f64>,
        note: Option<String>,
    }

    fn quote(path: &str) -> String {
        format!("'{}'", path.replace('\'', "''"))
    }

    /// A shell command with the flags and environment every call uses.
    fn shell(bin: &str) -> Command {
        let mut cmd = Command::new(bin);
        cmd.args(["-no-agent", "-init", NULL_INIT, "-csv", "-noheader"])
            .env_remove("AI_AGENT");
        cmd
    }

    /// The CLI's version line (`duckdb -no-agent --version`), if it runs.
    pub(crate) fn version(bin: &str) -> Option<String> {
        let out = Command::new(bin)
            .args(["-no-agent", "--version"])
            .env_remove("AI_AGENT")
            .output()
            .ok()?;
        if !out.status.success() {
            return None;
        }
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .map(str::trim)
            .find(|l| l.starts_with('v'))
            .map(str::to_string)
    }

    /// What one shell invocation printed.
    #[derive(Debug, PartialEq)]
    pub(super) struct CliOutput {
        /// The CSV fields of the single result row.
        pub(super) fields: Vec<String>,
        /// The `.timer` real time in milliseconds.
        pub(super) real_ms: f64,
    }

    /// Splits one CSV line, honoring double quotes.
    fn csv_fields(line: &str) -> Result<Vec<String>, String> {
        let mut fields = Vec::new();
        let mut field = String::new();
        let mut quoted = false;
        let mut chars = line.chars().peekable();
        while let Some(c) = chars.next() {
            match (c, quoted) {
                ('"', true) if chars.peek() == Some(&'"') => {
                    field.push('"');
                    chars.next();
                }
                ('"', _) => quoted = !quoted,
                (',', false) => fields.push(std::mem::take(&mut field)),
                _ => field.push(c),
            }
        }
        if quoted {
            return Err(format!("unterminated quote in {line:?}"));
        }
        fields.push(field);
        Ok(fields)
    }

    /// Parses `-csv -noheader` output with `.timer on`: exactly one data row
    /// of `expected` fields whose first field is a row count, and exactly one
    /// timer line. Anything else is an error.
    ///
    /// ```text
    /// 1234,5678.90,...
    /// Run Time (s): real 0.012 user 0.010000 sys 0.002000
    /// ```
    pub(super) fn parse_output(stdout: &str, expected: usize) -> Result<CliOutput, String> {
        let mut row: Option<Vec<String>> = None;
        let mut real_ms: Option<f64> = None;
        for line in stdout.lines().filter(|l| !l.trim().is_empty()) {
            if let Some(rest) = line.strip_prefix("Run Time (s): real ") {
                let secs = rest
                    .split_whitespace()
                    .next()
                    .and_then(|v| v.parse::<f64>().ok())
                    .ok_or_else(|| format!("unreadable timer line {line:?}"))?;
                if real_ms.replace(secs * 1e3).is_some() {
                    return Err("more than one timer line".into());
                }
                continue;
            }
            let fields = csv_fields(line)?;
            if fields.len() != expected {
                return Err(format!(
                    "expected one CSV row of {expected} fields, got {line:?}"
                ));
            }
            if fields[0].parse::<u64>().is_err() {
                return Err(format!("first field is not a row count: {line:?}"));
            }
            if row.replace(fields).is_some() {
                return Err("more than one result row".into());
            }
        }
        Ok(CliOutput {
            fields: row.ok_or("no result row")?,
            real_ms: real_ms.ok_or("no .timer line")?,
        })
    }

    /// Runs one statement and parses its output strictly.
    fn run_sql(bin: &str, sql: &str, expected: usize) -> Result<CliOutput, String> {
        let mut child = shell(bin)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("cannot start `{bin}`: {e}"))?;
        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(format!(".timer on\n{sql};\n").as_bytes())
                .map_err(|e| format!("writing to `{bin}`: {e}"))?;
        }
        let out = child
            .wait_with_output()
            .map_err(|e| format!("waiting for `{bin}`: {e}"))?;
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
        if !out.status.success() || !stderr.is_empty() {
            return Err(format!(
                "`{bin}` exited with {} and stderr {stderr:?}; stdout {:?}",
                out.status,
                stdout.trim()
            ));
        }
        parse_output(&stdout, expected).map_err(|e| format!("`{bin}` output rejected: {e}"))
    }

    impl DuckDb {
        pub(super) fn open(corpus: &Corpus, mode: DuckDbMode) -> Self {
            let glob = corpus.root.join("data").join("**").join("*.parquet");
            let glob = glob.display().to_string();
            if mode == DuckDbMode::Off {
                return Self {
                    bin: None,
                    glob,
                    setup_ms: None,
                    note: Some("disabled with --no-duckdb".into()),
                };
            }
            let bin = std::env::var(BIN_ENV).unwrap_or_else(|_| "duckdb".into());
            // Warm the OS cache and check the shell works before timing runs.
            let sql = format!(
                "SELECT count(*) FROM read_parquet({}, hive_partitioning = true)",
                quote(&glob)
            );
            let t = Instant::now();
            match run_sql(&bin, &sql, 1) {
                Ok(_) => Self {
                    note: Some(format!(
                        "engine baseline via the duckdb CLI ({}): latency is DuckDB's .timer \
                         query time; files, row groups, and bytes are not observable",
                        version(&bin).unwrap_or_else(|| "version unknown".into())
                    )),
                    bin: Some(bin),
                    glob,
                    setup_ms: Some(ms(t)),
                },
                Err(e) => Self {
                    bin: None,
                    glob,
                    setup_ms: None,
                    note: Some(format!(
                        "unavailable: {e} (install a duckdb CLI that accepts -no-agent, or set \
                         {BIN_ENV})"
                    )),
                },
            }
        }

        pub(super) fn info(&self) -> StrategyInfo {
            StrategyInfo {
                name: DUCKDB.into(),
                kind: StrategyKind::Engine,
                available: self.bin.is_some(),
                setup_ms: self.setup_ms,
                note: self.note.clone(),
            }
        }

        pub(super) fn run(&self, keys: &Path) -> Option<StrategyResult> {
            let bin = self.bin.as_ref()?;
            let sql = format!(
                "SELECT count(*), sum(amount), sum(length(payload)), max(version_ts), \
                 max(batch_id), max(status) \
                 FROM read_parquet({}, hive_partitioning = true) \
                 WHERE entity_id IN (SELECT entity_id FROM read_parquet({}))",
                quote(&self.glob),
                quote(&keys.display().to_string())
            );
            let mut result = StrategyResult::new(DUCKDB);
            match run_sql(bin, &sql, LOOKUP_FIELDS) {
                Ok(out) => {
                    result.latency_ms = Some(out.real_ms);
                    result.matched_rows = out.fields[0].parse().ok();
                    // Canonical form, so "12.3" and "12.30" compare equal; an
                    // unparseable total is kept verbatim and will not match.
                    let amount = out.fields[1].clone();
                    result.amount_total = Some(parse_cents(&amount).map_or(amount, format_cents));
                }
                Err(e) => result.error = Some(e),
            }
            Some(result)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        const ROW: &str = "1234,-567.80,89,2026-01-02 00:00:00+00,202601020000,void";
        const TIMER: &str = "Run Time (s): real 0.012 user 0.010000 sys 0.002000";

        #[test]
        fn accepts_exactly_the_requested_format() {
            let out = parse_output(&format!("{ROW}\n{TIMER}\n"), 6).unwrap();
            assert_eq!(out.fields[0], "1234");
            assert_eq!(out.fields[1], "-567.80");
            assert!((out.real_ms - 12.0).abs() < 1e-9);
            // Order does not matter; quoted fields are unquoted.
            let out = parse_output(&format!("{TIMER}\n\"42\",\"7,5\"\n"), 2).unwrap();
            assert_eq!(out.fields, vec!["42".to_string(), "7,5".to_string()]);
        }

        #[test]
        fn rejects_anything_else() {
            let banner = "duckdb agent mode on: AI_AGENT is set and stdout is not a terminal";
            for (stdout, fields) in [
                (format!("{banner}\n{ROW}\n{TIMER}\n"), 6),
                (format!("{ROW}\n{TIMER}\n"), 5),
                (format!("{ROW}\n"), 6),
                (format!("{TIMER}\n"), 6),
                (format!("{ROW}\n{ROW}\n{TIMER}\n"), 6),
                (format!("{ROW}\n{TIMER}\n{TIMER}\n"), 6),
                ("┌───────┐\n│ 42    │\n└───────┘\n".to_string(), 1),
                (format!("x,1\n{TIMER}\n"), 2),
                (format!("\"1,2\n{TIMER}\n"), 2),
            ] {
                assert!(
                    parse_output(&stdout, fields).is_err(),
                    "accepted {stdout:?}"
                );
            }
        }
    }
}

#[cfg(not(feature = "duckdb"))]
mod engine {
    //! Stub when built without the `duckdb` feature: reports "unavailable".

    use std::path::Path;

    use super::{DuckDbMode, DUCKDB};
    use crate::corpus::Corpus;
    use crate::results::{StrategyInfo, StrategyKind, StrategyResult};

    pub(super) struct DuckDb {
        note: &'static str,
    }

    impl DuckDb {
        pub(super) fn open(_corpus: &Corpus, mode: DuckDbMode) -> Self {
            Self {
                note: if mode == DuckDbMode::Off {
                    "disabled with --no-duckdb"
                } else {
                    "unavailable: built without `--features duckdb`"
                },
            }
        }

        pub(super) fn info(&self) -> StrategyInfo {
            StrategyInfo {
                name: DUCKDB.into(),
                kind: StrategyKind::Engine,
                available: false,
                setup_ms: None,
                note: Some(self.note.into()),
            }
        }

        #[allow(clippy::unused_self)]
        pub(super) fn run(&self, _keys: &Path) -> Option<StrategyResult> {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cents_round_trip() {
        for c in [0i128, 1, -1, 99, -99, 100, 123_456, -123_456, 5_000_000_000] {
            assert_eq!(parse_cents(&format_cents(c)), Some(c));
        }
        assert_eq!(format_cents(-50), "-0.50");
        assert_eq!(parse_cents("12.3"), Some(1230));
        assert_eq!(parse_cents("7"), Some(700));
        assert_eq!(parse_cents("1.234"), None);
        assert_eq!(parse_cents("x"), None);
    }
}

#[cfg(feature = "datafusion")]
pub mod datafusion_baseline {
    //! Apache DataFusion baseline (ADR 019): DataFusion's own Parquet
    //! pruning as a plan strategy.
    //!
    //! The strategy runs DataFusion's Parquet scan (a `DataSourceExec` over a
    //! `ParquetSource`, the successor of the earlier `ParquetExec`) over the
    //! corpus with the
    //! predicate `entity_id IN (keys)`, projected to `entity_id`, with row
    //! group statistics pruning, page-index pruning, and bloom-filter
    //! pruning on (DataFusion's defaults). Every byte range DataFusion asks
    //! for goes through a recording reader; the plan is the set of row groups
    //! whose column chunks it read. Footer, page-index, and bloom-filter
    //! reads fall outside column chunks and do not count as row groups.
    //! DataFusion rewrites `IN` lists into prunable form only up to its own
    //! size limit, so large key sets prune by bloom filters alone, exactly as
    //! DataFusion would.
    //!
    //! Latency is split as docs/benchmark.md requires for an engine whose
    //! pruning scans key columns: `plan_ms` is DataFusion's pruning phase
    //! (including its projected scan), `scan_ms` is executing the resulting
    //! plan through the harness's byte-counting reader. Every byte DataFusion
    //! fetched while planning (page index, bloom filters, `entity_id` chunks)
    //! is reported as `plan_bytes_read`, beside the execution bytes.

    use std::collections::BTreeSet;
    use std::future::Future;
    use std::io::{Read, Seek, SeekFrom};
    use std::ops::Range;
    use std::path::PathBuf;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};

    use bellwether_types::ScanPlan;
    use bytes::Bytes;
    use datafusion::common::DFSchema;
    use datafusion::config::TableParquetOptions;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::{
        FileScanConfigBuilder, ParquetFileReaderFactory, ParquetSource,
    };
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::error::DataFusionError;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion::prelude::{col, lit, SessionContext};
    use parquet::arrow::async_reader::AsyncFileReader;
    use parquet::file::metadata::ParquetMetaData;

    use super::{plan_from, IndexUnderTest};
    use crate::corpus::{Corpus, COLUMNS, COL_ENTITY};
    use crate::generate::schema;
    use crate::results::DATAFUSION;
    use crate::{BenchError, Key, Result};

    use parquet::arrow::arrow_reader::ArrowReaderOptions;

    type Log = Arc<Mutex<Vec<(usize, Range<u64>)>>>;
    type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

    /// The corpus position of a file, attached to its `PartitionedFile`.
    #[derive(Debug, Clone, Copy)]
    struct FileIndex(usize);

    /// Hands DataFusion a recording reader per file.
    #[derive(Debug)]
    struct RecordingFactory {
        files: Vec<(PathBuf, Arc<ParquetMetaData>)>,
        log: Log,
    }

    impl ParquetFileReaderFactory for RecordingFactory {
        fn create_reader(
            &self,
            _partition_index: usize,
            file: PartitionedFile,
            _metadata_size_hint: Option<usize>,
            _metrics: &ExecutionPlanMetricsSet,
        ) -> datafusion::error::Result<Box<dyn AsyncFileReader + Send>> {
            let index = file
                .extension::<FileIndex>()
                .map(|i| i.0)
                .ok_or_else(|| DataFusionError::Internal("file index missing".into()))?;
            let (path, meta) = self
                .files
                .get(index)
                .cloned()
                .ok_or_else(|| DataFusionError::Internal(format!("no file {index}")))?;
            Ok(Box::new(RecordingReader {
                index,
                path,
                meta,
                log: self.log.clone(),
            }))
        }
    }

    /// Serves byte ranges from the local file and records each request.
    struct RecordingReader {
        index: usize,
        path: PathBuf,
        meta: Arc<ParquetMetaData>,
        log: Log,
    }

    impl RecordingReader {
        fn read(&self, range: Range<u64>) -> parquet::errors::Result<Bytes> {
            if let Ok(mut log) = self.log.lock() {
                log.push((self.index, range.clone()));
            }
            let len = usize::try_from(range.end.saturating_sub(range.start)).map_err(|_| {
                parquet::errors::ParquetError::General(format!(
                    "byte range {range:?} does not fit in memory"
                ))
            })?;
            let mut file = std::fs::File::open(&self.path)?;
            file.seek(SeekFrom::Start(range.start))?;
            let mut buf = vec![0u8; len];
            file.read_exact(&mut buf)?;
            Ok(Bytes::from(buf))
        }
    }

    impl AsyncFileReader for RecordingReader {
        fn get_bytes(
            &mut self,
            range: Range<u64>,
        ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
            let result = self.read(range);
            Box::pin(std::future::ready(result))
        }

        fn get_metadata<'a>(
            &'a mut self,
            _options: Option<&'a ArrowReaderOptions>,
        ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
            Box::pin(std::future::ready(Ok(self.meta.clone())))
        }
    }

    /// The DataFusion plan strategy.
    pub struct DataFusionScan<'a> {
        corpus: &'a Corpus,
        runtime: tokio::runtime::Runtime,
        last_plan_bytes: Mutex<Option<u64>>,
    }

    impl<'a> DataFusionScan<'a> {
        /// Over a corpus, with a single-threaded runtime of its own.
        pub fn new(corpus: &'a Corpus) -> Result<Self> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| BenchError::Strategy {
                    strategy: DATAFUSION.into(),
                    message: format!("cannot start a tokio runtime: {e}"),
                })?;
            Ok(Self {
                corpus,
                runtime,
                last_plan_bytes: Mutex::new(None),
            })
        }

        /// Row groups whose column chunks DataFusion read for these keys.
        fn touched(&self, keys: &[Key]) -> datafusion::error::Result<BTreeSet<(usize, usize)>> {
            let log: Log = Arc::new(Mutex::new(Vec::new()));
            let factory = RecordingFactory {
                files: self
                    .corpus
                    .files
                    .iter()
                    .map(|f| (f.path.clone(), f.reader.metadata().clone()))
                    .collect(),
                log: log.clone(),
            };
            let file_schema = schema();
            let ctx = SessionContext::new();
            let df_schema = DFSchema::try_from(file_schema.as_ref().clone())?;
            let expr =
                col(COLUMNS[COL_ENTITY]).in_list(keys.iter().map(|&k| lit(k)).collect(), false);
            let predicate = ctx.create_physical_expr(expr, &df_schema)?;
            let mut options = TableParquetOptions::default();
            options.global.pruning = true;
            options.global.enable_page_index = true;
            options.global.bloom_filter_on_read = true;
            let source = ParquetSource::new(file_schema.clone())
                .with_table_parquet_options(options)
                .with_parquet_file_reader_factory(Arc::new(factory))
                .with_predicate(predicate);
            let mut config =
                FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), Arc::new(source))
                    .with_projection_indices(Some(vec![COL_ENTITY]))?;
            for (i, f) in self.corpus.files.iter().enumerate() {
                config = config.with_file(
                    PartitionedFile::new(f.path.display().to_string(), f.fingerprint.size)
                        .with_extension(FileIndex(i)),
                );
            }
            let exec = DataSourceExec::from_data_source(config.build());
            self.runtime
                .block_on(datafusion::physical_plan::collect(exec, ctx.task_ctx()))?;
            let log = log
                .lock()
                .map_err(|_| DataFusionError::Internal("range log poisoned".into()))?;
            let fetched = log
                .iter()
                .map(|(_, range)| range.end.saturating_sub(range.start))
                .sum();
            if let Ok(mut last) = self.last_plan_bytes.lock() {
                *last = Some(fetched);
            }
            Ok(log
                .iter()
                .filter_map(|(fi, range)| {
                    self.corpus.files[*fi]
                        .reader
                        .row_group_of(range.start)
                        .map(|rg| (*fi, rg))
                })
                .collect())
        }
    }

    impl IndexUnderTest for DataFusionScan<'_> {
        fn name(&self) -> &str {
            DATAFUSION
        }

        fn plan(&self, keys: &[Key]) -> ScanPlan {
            match self.touched(keys) {
                Ok(cells) => plan_from(self.corpus, |fi, ri| cells.contains(&(fi, ri))),
                // The harness records a panicking plan() as a failed run.
                Err(e) => panic!("DataFusion scan failed: {e}"),
            }
        }

        fn plan_bytes_read(&self) -> Option<u64> {
            self.last_plan_bytes.lock().ok().and_then(|last| *last)
        }
    }
}
