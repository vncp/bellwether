//! The key-read pass: fanout per column and per candidate column tuple.
//!
//! One pass over the dataset in global row-group order. For each row group
//! the pass reads only the candidate key columns, in one `read_columns` call,
//! and drops the batch before the next row group (ADR 018: one row group's
//! key columns at a time).
//!
//! Per row, each candidate column is encoded once as its **dataset** type
//! (`bellwether_core::encoding`, ADR 016). A key tuple is the concatenation of
//! its elements' encodings with the semantics of
//! `bellwether_core::encoding::KeyColumns`: a row whose leading column is
//! null is not indexed, and a null trailing column is the `0x00` marker
//! (ADR 014 as amended). Fanout therefore describes exactly the keys a segment
//! built on that tuple would hold, **in the order the tuple is listed**: with
//! nulls, `(a, b)` and `(b, a)` hold different keys, so a tuple is measured in
//! the order it is reported and never relabeled.
//!
//! Every key feeds a fixed-size HyperLogLog sketch and a [`DistinctSampler`].
//! While a sampler's table has never been full, the fanout is exact:
//! `distinct` is the table size and the quantiles run over every distinct
//! value. Once it samples, the quantiles run over a uniform sample of distinct
//! values whose counts are still exact, and `distinct` comes from the sketch
//! (see [`DistinctSource`]).

use std::collections::BTreeSet;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, SchemaRef};
use bellwether_core::encoding::{
    check_supported, encode_array_value_as, nested_kind, not_in_schema, tag,
};
use bellwether_types::{Error, Fanout, Result, RowGroupMeta, Source};
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_128_with_seed;

use crate::footer::{all_null, stats_of, FooterStats};
use crate::quantile::p50_p95_max;
use crate::ranges::{is_time_type, ColumnRanges, TimeColumn};
use crate::sampler::{
    dkw_epsilon, high_water_at_most, high_water_for_budget, peak_bytes_bound, DistinctSampler,
};
use bellwether_core::hll::Hll;

/// Confidence level of the quantile band reported for a sampled fanout:
/// `quantile_rank_error` holds with probability at least `1 - QUANTILE_ALPHA`.
pub const QUANTILE_ALPHA: f64 = 0.001;
/// Auto selection skips a string or binary column wider than this many
/// compressed bytes per row, a document or blob rather than a key, when the
/// source reports column-chunk sizes. No identifier is this wide (a UUID in
/// text is 36 bytes), so the cutoff never drops a string id, even in tiny row
/// groups whose chunk overhead inflates bytes per row. Integers are never
/// skipped by width.
pub const AUTO_MAX_BYTES_PER_ROW: f64 = 256.0;
/// Footer distinct counts that disagree with the sketch by more than this many
/// of its standard errors are refused as unreliable.
pub const FOOTER_SIGMAS: f64 = 4.0;

/// One candidate key column.
#[derive(Clone, Debug, PartialEq)]
pub struct KeyColumn {
    /// Name.
    pub name: String,
    /// Dataset type every value is encoded as.
    pub data_type: DataType,
}

/// A schema column auto selection did not make a candidate key, and why.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkippedColumn {
    /// The column.
    pub column: String,
    /// Why it is not a candidate.
    pub reason: String,
}

/// What the pass measures: candidate columns, the keys over them, and the
/// time columns the range analysis reads from footers.
#[derive(Clone, Debug)]
pub struct KeyPlan {
    /// Candidate key columns in schema order.
    pub columns: Vec<KeyColumn>,
    /// Keys as indices into `columns`: every single column first, then the
    /// tuples, each in the order it is measured and reported.
    pub keys: Vec<Vec<usize>>,
    /// Date and timestamp columns of the range analysis.
    pub time_columns: Vec<TimeColumn>,
    /// Schema columns auto selection skipped, with the reason (empty when
    /// candidates were named explicitly).
    pub skipped: Vec<SkippedColumn>,
}

/// Whether `dt` is a plausible identifier type: integers, strings, binary,
/// and dictionaries of those. Floats, decimals, booleans, and temporal types
/// are measurements, flags, or times, not ids.
#[must_use]
pub fn is_id_like(dt: &DataType) -> bool {
    match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => true,
        DataType::Dictionary(_, v) => is_id_like(v),
        _ => false,
    }
}

/// Whether `dt` is variable-width, so a column of it may be a document or
/// blob rather than a key.
fn could_be_payload(dt: &DataType) -> bool {
    match dt {
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView => true,
        DataType::FixedSizeBinary(n) => *n > 256,
        DataType::Dictionary(_, v) => could_be_payload(v),
        _ => false,
    }
}

fn field<'a>(schema: &'a SchemaRef, name: &str) -> Result<&'a arrow::datatypes::Field> {
    schema
        .field_with_name(name)
        .map_err(|_| Error::KeySpec(not_in_schema(schema, name)))
}

/// Explicit key columns, validated, with their schema positions.
fn explicit_columns(schema: &SchemaRef, names: &[String]) -> Result<Vec<(usize, KeyColumn)>> {
    let mut seen = BTreeSet::new();
    let mut picked = Vec::new();
    for name in names {
        if !seen.insert(name.as_str()) {
            return Err(Error::KeySpec(format!("column {name:?} listed twice")));
        }
        let f = field(schema, name)?;
        check_supported(f.data_type())
            .map_err(|e| Error::KeySpec(format!("column {name:?} cannot be a key: {e}")))?;
        let idx = schema
            .index_of(name)
            .map_err(|e| Error::KeySpec(e.to_string()))?;
        picked.push((
            idx,
            KeyColumn {
                name: name.clone(),
                data_type: f.data_type().clone(),
            },
        ));
    }
    Ok(picked)
}

/// Every id-like column, and every other column with the reason it is not
/// a candidate.
fn auto_columns(
    schema: &SchemaRef,
    footers: &FooterStats,
) -> (Vec<(usize, KeyColumn)>, Vec<SkippedColumn>) {
    let mut picked = Vec::new();
    let mut skipped = Vec::new();
    for (i, f) in schema.fields().iter().enumerate() {
        let dt = f.data_type();
        let reason = if let Some(kind) = nested_kind(dt) {
            Some(format!(
                "a nested {kind} column, not a key candidate; flatten a field into its own \
                 column to profile it"
            ))
        } else if is_time_type(dt) {
            Some(format!("{dt}: a time column, analyzed for range overlap"))
        } else if !is_id_like(dt) {
            Some(format!("{dt}: not an identifier type"))
        } else {
            footers
                .bytes_per_row(f.name())
                .filter(|&b| could_be_payload(dt) && b > AUTO_MAX_BYTES_PER_ROW)
                .map(|b| {
                    format!(
                        "{b:.0} compressed bytes per row, over {AUTO_MAX_BYTES_PER_ROW:.0}: \
                         a document or blob, not a key"
                    )
                })
        };
        match reason {
            Some(reason) => skipped.push(SkippedColumn {
                column: f.name().clone(),
                reason,
            }),
            None => picked.push((
                i,
                KeyColumn {
                    name: f.name().clone(),
                    data_type: dt.clone(),
                },
            )),
        }
    }
    (picked, skipped)
}

/// Every single column, then every combination of 2..=`max_tuple_len`
/// columns (at most `max_tuples`), each ordered with the `supplied` columns
/// first and the rest in schema order.
fn keys_in_measured_order(
    columns: &[KeyColumn],
    max_tuple_len: usize,
    max_tuples: usize,
    supplied: &[String],
) -> Vec<Vec<usize>> {
    let is_supplied = |i: usize| supplied.contains(&columns[i].name);
    let mut keys: Vec<Vec<usize>> = (0..columns.len()).map(|i| vec![i]).collect();
    let mut tuples = 0usize;
    'sizes: for len in 2..=max_tuple_len.min(columns.len()) {
        for combo in combinations(columns.len(), len) {
            if tuples >= max_tuples {
                break 'sizes;
            }
            let (mut first, rest): (Vec<usize>, Vec<usize>) =
                combo.into_iter().partition(|&i| is_supplied(i));
            first.extend(rest);
            keys.push(first);
            tuples += 1;
        }
    }
    keys
}

/// Explicit time columns must be dates or timestamps; `None` selects all.
fn select_time_columns(schema: &SchemaRef, names: Option<&[String]>) -> Result<Vec<TimeColumn>> {
    match names {
        Some(names) => names
            .iter()
            .map(|name| {
                let f = field(schema, name)?;
                if !is_time_type(f.data_type()) {
                    return Err(Error::KeySpec(format!(
                        "time column {name:?} is {}, not a date or timestamp",
                        f.data_type()
                    )));
                }
                Ok(TimeColumn {
                    name: name.clone(),
                    data_type: f.data_type().clone(),
                })
            })
            .collect(),
        None => Ok(schema
            .fields()
            .iter()
            .filter(|f| is_time_type(f.data_type()))
            .map(|f| TimeColumn {
                name: f.name().clone(),
                data_type: f.data_type().clone(),
            })
            .collect()),
    }
}

impl KeyPlan {
    /// Builds the plan.
    ///
    /// Explicit `columns` must exist and have a canonical encoding. `None`
    /// selects every id-like column (see [`is_id_like`]), skipping strings
    /// and binary wider than [`AUTO_MAX_BYTES_PER_ROW`]; every schema column
    /// not selected is listed in `skipped` with its reason. Explicit
    /// `time_columns` must be dates or timestamps; `None` selects all of
    /// them.
    ///
    /// Tuples are every combination of 2..=`max_tuple_len` candidates (at
    /// most `max_tuples`), each ordered with the columns the recorded
    /// workload supplies (`supplied`) first and the rest in schema order:
    /// the order a lookup can seek by prefix.
    pub fn build(
        schema: &SchemaRef,
        footers: &FooterStats,
        columns: Option<&[String]>,
        time_columns: Option<&[String]>,
        max_tuple_len: usize,
        max_tuples: usize,
        supplied: &[String],
    ) -> Result<Self> {
        let (mut picked, skipped) = match columns {
            Some(names) => (explicit_columns(schema, names)?, Vec::new()),
            None => auto_columns(schema, footers),
        };
        picked.sort_by_key(|(i, _)| *i);
        let columns: Vec<KeyColumn> = picked.into_iter().map(|(_, c)| c).collect();
        let keys = keys_in_measured_order(&columns, max_tuple_len, max_tuples, supplied);
        let time_columns = select_time_columns(schema, time_columns)?;
        Ok(Self {
            columns,
            keys,
            time_columns,
            skipped,
        })
    }

    /// Column names of key `k`, in its measured order.
    #[must_use]
    pub fn key_names(&self, k: usize) -> Vec<String> {
        self.keys[k]
            .iter()
            .map(|&i| self.columns[i].name.clone())
            .collect()
    }
}

/// Every `len`-combination of `0..n` in lexicographic order.
fn combinations(n: usize, len: usize) -> Vec<Vec<usize>> {
    let mut out = Vec::new();
    let mut idx: Vec<usize> = (0..len).collect();
    if len == 0 || len > n {
        return out;
    }
    loop {
        out.push(idx.clone());
        let mut i = len;
        loop {
            if i == 0 {
                return out;
            }
            i -= 1;
            if idx[i] != i + n - len {
                break;
            }
        }
        idx[i] += 1;
        for j in i + 1..len {
            idx[j] = idx[j - 1] + 1;
        }
    }
}

/// Where a fanout's `distinct` came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DistinctSource {
    /// Counted exactly: the sampler never sampled.
    Exact,
    /// The HyperLogLog estimate (relative standard error `1.04 / sqrt(2^p)`),
    /// raised to at least the retained sample size.
    Hll,
    /// The HyperLogLog estimate clamped into the bounds footer distinct
    /// counts imply (at least the largest per-row-group count, at most their
    /// sum), after checking it lies within [`FOOTER_SIGMAS`] standard errors
    /// of them.
    HllWithinFooterBounds,
    /// The sum of footer distinct counts over row groups whose ranges are
    /// pairwise disjoint, so each value lives in exactly one row group,
    /// verified against the sketch to within [`FOOTER_SIGMAS`] standard
    /// errors.
    FooterDisjoint,
}

/// How a fanout was measured, for the reader who needs its error.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SampleInfo {
    /// Distinct values whose counts the quantiles were computed from (all of
    /// them when unsampled).
    pub retained: u64,
    /// The sampler's inclusive admission bound on the 128-bit value hash, as
    /// 32 hex digits: the sample is exactly the distinct values hashed at or
    /// below it (`ffff…` when unsampled).
    pub admission_bound: String,
    /// Fraction of the hash space admitted: 1.0 when unsampled. Derived from
    /// `admission_bound`.
    pub rate: f64,
    /// The HyperLogLog estimate, rounded, whether or not it was used.
    pub hll_estimate: u64,
    /// Where `distinct` came from.
    pub distinct_source: DistinctSource,
    /// When sampled: each reported quantile lies, with probability at least
    /// `1 - QUANTILE_ALPHA`, between the population quantiles at ranks `q - e`
    /// and `q + e` (the DKW inequality). The sampled max is a lower bound on
    /// the true max. Derived from `retained`.
    pub quantile_rank_error: Option<f64>,
    /// Table entries held at the peak.
    pub peak_entries: u64,
    /// Hash-table capacity reached at the peak.
    pub peak_capacity: u64,
    /// The table's high-water mark.
    pub high_water: u64,
    /// The estimate exceeded the dataset's exact row count, so `distinct` was
    /// capped at it.
    pub capped_at_rows: bool,
}

/// A fanout and how it was measured.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FanoutEntry {
    /// The measurement, columns in measured order.
    pub fanout: Fanout,
    /// Sampling detail.
    pub sample: SampleInfo,
}

/// The memory the pass was allowed and what it used (ADR 018).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryReport {
    /// Keys measured.
    pub keys: u64,
    /// Per-key high-water mark of the sampler table.
    pub high_water: u64,
    /// Stated bound on every sampler table together, in bytes.
    pub tables_bound_bytes: u64,
    /// Stated bound on every sketch together, in bytes (fixed).
    pub sketches_bytes: u64,
    /// Largest table capacity any key reached.
    pub peak_capacity: u64,
    /// Rows in the largest row group read at once.
    pub max_row_group_rows: u64,
}

/// Pass configuration.
#[derive(Clone, Debug)]
pub struct PassConfig {
    /// Bytes for all sampler tables together.
    pub memory_budget: usize,
    /// Overrides the per-key high-water mark derived from the budget.
    pub max_tracked_per_key: Option<usize>,
    /// HyperLogLog precision.
    pub hll_precision: u8,
    /// Seed of the value hash.
    pub seed: u64,
    /// Disables eviction. Only for the negative control of the memory test.
    pub unbounded: bool,
}

/// What the pass produced.
#[derive(Debug)]
pub struct PassOutput {
    /// One entry per key, in plan order.
    pub entries: Vec<FanoutEntry>,
    /// Memory.
    pub memory: MemoryReport,
}

/// The per-key table high-water mark a configuration implies: the explicit
/// override, or the largest table whose stated peak fits the budget split
/// evenly over `n_keys`.
#[must_use]
pub fn resolve_high_water(config: &PassConfig, n_keys: usize) -> usize {
    match config.max_tracked_per_key {
        Some(n) => high_water_at_most(n),
        None => high_water_for_budget(config.memory_budget / n_keys.max(1)),
    }
}

struct KeyState {
    sampler: DistinctSampler,
    sketch: Hll,
}

/// The working state of one pass.
struct Pass<'a> {
    plan: &'a KeyPlan,
    seed: u64,
    states: Vec<KeyState>,
    read_names: Vec<String>,
    bufs: Vec<Vec<u8>>,
    present: Vec<bool>,
    tuple: Vec<u8>,
    max_rows: u64,
}

impl<'a> Pass<'a> {
    fn new(plan: &'a KeyPlan, config: &PassConfig, high_water: usize) -> Result<Self> {
        let states = (0..plan.keys.len())
            .map(|_| {
                Ok(KeyState {
                    sampler: if config.unbounded {
                        DistinctSampler::unbounded()
                    } else {
                        DistinctSampler::new(high_water)
                    },
                    sketch: Hll::new(config.hll_precision)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            plan,
            seed: config.seed,
            states,
            read_names: plan.columns.iter().map(|c| c.name.clone()).collect(),
            bufs: vec![Vec::new(); plan.columns.len()],
            present: vec![false; plan.columns.len()],
            tuple: Vec::new(),
            max_rows: 0,
        })
    }

    /// Reads one row group's key columns and feeds every key.
    fn row_group(
        &mut self,
        source: &dyn Source,
        rg: &RowGroupMeta,
        file: u32,
        global_rg: u32,
    ) -> Result<()> {
        let plan = self.plan;
        if self.read_names.is_empty() || plan.columns.iter().all(|c| all_null(rg, &c.name)) {
            return Ok(());
        }
        let batch = source.read_columns(rg, &self.read_names)?;
        if batch.num_rows() as u64 != rg.rows {
            return Err(Error::Source(format!(
                "{} row group {}: read {} rows, statistics say {}",
                rg.file.path,
                rg.index_in_file,
                batch.num_rows(),
                rg.rows
            )));
        }
        self.max_rows = self.max_rows.max(rg.rows);
        let arrays: Vec<ArrayRef> = (0..self.read_names.len())
            .map(|i| batch.column(i).clone())
            .collect();
        drop(batch);
        let context = |col: &str, e: Error| {
            Error::Source(format!(
                "{} row group {}: column {col}: {e}",
                rg.file.path, rg.index_in_file
            ))
        };
        let rows = arrays.first().map_or(0, |a| a.len());
        for row in 0..rows {
            for (ci, col) in plan.columns.iter().enumerate() {
                self.bufs[ci].clear();
                self.present[ci] = encode_array_value_as(
                    arrays[ci].as_ref(),
                    row,
                    &col.data_type,
                    &mut self.bufs[ci],
                )
                .map_err(|e| context(&col.name, e))?;
            }
            for (key, state) in plan.keys.iter().zip(self.states.iter_mut()) {
                if !self.present[key[0]] {
                    continue;
                }
                let bytes: &[u8] = if key.len() == 1 {
                    &self.bufs[key[0]]
                } else {
                    build_tuple(key, &self.bufs, &self.present, &mut self.tuple);
                    &self.tuple
                };
                let h = xxh3_128_with_seed(bytes, self.seed);
                #[allow(clippy::cast_possible_truncation)]
                state.sketch.insert(h as u64);
                state.sampler.observe(h, global_rg, file);
            }
        }
        Ok(())
    }
}

/// Builds the canonical tuple of `key` from per-column encodings, with the
/// semantics of `bellwether_core::encoding::KeyColumns::encode_into`: the
/// caller has checked the leading column is present; an absent trailing
/// column is the `0x00` null marker.
fn build_tuple(key: &[usize], bufs: &[Vec<u8>], present: &[bool], out: &mut Vec<u8>) {
    out.clear();
    for &ci in key {
        if present[ci] {
            out.extend_from_slice(&bufs[ci]);
        } else {
            out.push(tag::NULL);
        }
    }
}

/// Runs the key-read pass over every row group.
pub fn key_pass(
    source: &dyn Source,
    footers: &FooterStats,
    plan: &KeyPlan,
    ranges: &[ColumnRanges],
    config: &PassConfig,
) -> Result<PassOutput> {
    let n_keys = plan.keys.len();
    let high_water = resolve_high_water(config, n_keys);
    let mut pass = Pass::new(plan, config, high_water)?;
    let mut global_rg = 0u32;
    for (fi, rg) in footers.row_groups() {
        let file = u32::try_from(fi).map_err(|_| Error::Other("too many files".into()))?;
        pass.row_group(source, rg, file, global_rg)?;
        global_rg = global_rg
            .checked_add(1)
            .ok_or_else(|| Error::Other("too many row groups".into()))?;
    }
    let mut memory = MemoryReport {
        keys: n_keys as u64,
        high_water: if config.unbounded {
            u64::MAX
        } else {
            high_water as u64
        },
        tables_bound_bytes: (peak_bytes_bound(high_water) as u64).saturating_mul(n_keys as u64),
        sketches_bytes: (n_keys as u64) << config.hll_precision,
        peak_capacity: 0,
        max_row_group_rows: pass.max_rows,
    };
    let mut entries = Vec::with_capacity(n_keys);
    for (k, state) in pass.states.into_iter().enumerate() {
        let entry = finish_key(footers, plan, k, state, ranges, config.hll_precision)?;
        memory.peak_capacity = memory.peak_capacity.max(entry.sample.peak_capacity);
        entries.push(entry);
    }
    Ok(PassOutput { entries, memory })
}

/// The fraction of the 128-bit hash space at or below `bound`.
#[must_use]
pub fn rate_of(bound: u128) -> f64 {
    #[allow(clippy::cast_precision_loss)]
    let r = (bound as f64 + 1.0) / 2f64.powi(128);
    r.min(1.0)
}

/// Turns one key's sampler and sketch into its fanout.
fn finish_key(
    footers: &FooterStats,
    plan: &KeyPlan,
    k: usize,
    state: KeyState,
    ranges: &[ColumnRanges],
    precision: u8,
) -> Result<FanoutEntry> {
    let estimate = state.sketch.estimate();
    let high_water = state.sampler.high_water() as u64;
    let outcome = state.sampler.finish();
    let retained = outcome.rgs.len() as u64;
    let mut files_per_value = p50_p95_max(&outcome.files);
    let mut row_groups_per_value = p50_p95_max(&outcome.rgs);
    let (distinct, distinct_source, sampled) = if outcome.sampled {
        let single = (plan.keys[k].len() == 1).then(|| &plan.columns[plan.keys[k][0]]);
        let footer = match single {
            Some(c) => footer_distinct(footers, c, ranges, estimate, retained, precision)?,
            None => None,
        };
        match footer {
            Some((n, DistinctSource::FooterDisjoint)) => {
                // Disjoint ranges: every value is in exactly one row group and
                // one file, so the quantiles are exact too.
                files_per_value = [1.0; 3];
                row_groups_per_value = [1.0; 3];
                (n, DistinctSource::FooterDisjoint, false)
            }
            Some((n, source)) => (n, source, true),
            None => (
                clamp_estimate(estimate, retained, u64::MAX),
                DistinctSource::Hll,
                true,
            ),
        }
    } else {
        (retained, DistinctSource::Exact, false)
    };
    // No key can have more distinct values than the dataset has rows, which
    // the footers give exactly; an estimate above that is capped and noted.
    let rows_total = footers.rows_total();
    let (distinct, capped_at_rows) = bellwether_source::stats::cap_distinct(distinct, rows_total);
    let cached = CachedEntry {
        columns: plan.key_names(k),
        distinct,
        sampled,
        files: as_counts(files_per_value),
        row_groups: as_counts(row_groups_per_value),
        files_total: footers.files_total(),
        row_groups_total: footers.row_groups_total(),
        retained,
        admission_bound: format!("{:032x}", outcome.bound),
        hll_estimate: clamp_estimate(estimate, 0, u64::MAX),
        distinct_source,
        peak_entries: outcome.peak_entries as u64,
        peak_capacity: outcome.peak_capacity as u64,
        high_water,
        capped_at_rows,
    };
    cached.into_entry()
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn as_counts(q: [f64; 3]) -> [u32; 3] {
    q.map(|v| v as u32)
}

/// A fanout as the cache stores it: integers only, so a reused profile is
/// bit-for-bit the fresh one whatever the JSON float parser does. Every float
/// in [`FanoutEntry`] is derived from these on load, by the same functions a
/// fresh computation uses.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachedEntry {
    /// Columns in measured order.
    pub columns: Vec<String>,
    /// Distinct values.
    pub distinct: u64,
    /// Whether sampled.
    pub sampled: bool,
    /// Files per value at p50, p95, max.
    pub files: [u32; 3],
    /// Row groups per value at p50, p95, max.
    pub row_groups: [u32; 3],
    /// Files in the dataset.
    pub files_total: u64,
    /// Row groups in the dataset.
    pub row_groups_total: u64,
    /// Values the quantiles came from.
    pub retained: u64,
    /// Admission bound, 32 hex digits.
    pub admission_bound: String,
    /// Rounded sketch estimate.
    pub hll_estimate: u64,
    /// Where `distinct` came from.
    pub distinct_source: DistinctSource,
    /// Peak table entries.
    pub peak_entries: u64,
    /// Peak table capacity.
    pub peak_capacity: u64,
    /// Table high-water mark.
    pub high_water: u64,
    /// `distinct` was capped at the row count.
    pub capped_at_rows: bool,
}

impl CachedEntry {
    /// The integer form of `e`.
    pub fn from_entry(e: &FanoutEntry) -> Result<Self> {
        let counts = |q: [f64; 3]| -> Result<[u32; 3]> {
            let mut out = [0u32; 3];
            for (slot, v) in out.iter_mut().zip(q) {
                if !(v.is_finite() && v >= 0.0 && v.fract() == 0.0 && v <= f64::from(u32::MAX)) {
                    return Err(Error::Other(format!("fanout quantile {v} is not a count")));
                }
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                {
                    *slot = v as u32;
                }
            }
            Ok(out)
        };
        Ok(Self {
            columns: e.fanout.columns.clone(),
            distinct: e.fanout.distinct,
            sampled: e.fanout.sampled,
            files: counts(e.fanout.files_per_value)?,
            row_groups: counts(e.fanout.row_groups_per_value)?,
            files_total: e.fanout.files_total,
            row_groups_total: e.fanout.row_groups_total,
            retained: e.sample.retained,
            admission_bound: e.sample.admission_bound.clone(),
            hll_estimate: e.sample.hll_estimate,
            distinct_source: e.sample.distinct_source,
            peak_entries: e.sample.peak_entries,
            peak_capacity: e.sample.peak_capacity,
            high_water: e.sample.high_water,
            capped_at_rows: e.sample.capped_at_rows,
        })
    }

    /// The entry with its derived floats.
    pub fn into_entry(self) -> Result<FanoutEntry> {
        let bound = u128::from_str_radix(&self.admission_bound, 16).map_err(|_| {
            Error::Other(format!(
                "admission bound {:?} is not hex",
                self.admission_bound
            ))
        })?;
        Ok(FanoutEntry {
            fanout: Fanout {
                columns: self.columns,
                distinct: self.distinct,
                sampled: self.sampled,
                files_per_value: self.files.map(f64::from),
                row_groups_per_value: self.row_groups.map(f64::from),
                files_total: self.files_total,
                row_groups_total: self.row_groups_total,
            },
            sample: SampleInfo {
                retained: self.retained,
                admission_bound: self.admission_bound,
                rate: rate_of(bound),
                hll_estimate: self.hll_estimate,
                distinct_source: self.distinct_source,
                quantile_rank_error: self
                    .sampled
                    .then(|| dkw_epsilon(self.retained, QUANTILE_ALPHA)),
                peak_entries: self.peak_entries,
                peak_capacity: self.peak_capacity,
                high_water: self.high_water,
                capped_at_rows: self.capped_at_rows,
            },
        })
    }
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
fn clamp_estimate(estimate: f64, lower: u64, upper: u64) -> u64 {
    let rounded = if estimate.is_finite() && estimate > 0.0 {
        estimate.round().min(u64::MAX as f64) as u64
    } else {
        0
    };
    rounded.clamp(lower, upper.max(lower))
}

/// What footer distinct counts say about `col`'s global distinct count, when
/// every row group that may hold a value reports one, checked against the
/// sketch. Per-row-group counts do not add up across row groups in general (a
/// value may repeat), so they bound the answer; only pairwise-disjoint ranges
/// make their sum the answer. Either way the sketch must agree to within
/// [`FOOTER_SIGMAS`] standard errors, or the footers are refused with an
/// error: an approximate or null-counting writer must never be reported as
/// exact. Floats are excluded because their bounds exclude NaN.
fn footer_distinct(
    footers: &FooterStats,
    col: &KeyColumn,
    ranges: &[ColumnRanges],
    estimate: f64,
    retained: u64,
    precision: u8,
) -> Result<Option<(u64, DistinctSource)>> {
    if matches!(
        col.data_type,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    ) {
        return Ok(None);
    }
    let (mut lower, mut upper) = (0u64, 0u64);
    for (_, rg) in footers.row_groups() {
        if all_null(rg, &col.name) {
            continue;
        }
        let Some(dc) = stats_of(rg, &col.name).and_then(|s| s.distinct_count) else {
            return Ok(None);
        };
        lower = lower.max(dc);
        upper = upper.saturating_add(dc);
    }
    let tolerance = FOOTER_SIGMAS * Hll::relative_standard_error(precision);
    let disjoint = ranges.iter().any(|r| r.column == col.name && r.disjoint);
    #[allow(clippy::cast_precision_loss)]
    let (lo, hi) = if disjoint {
        (upper as f64, upper as f64)
    } else {
        (lower.max(retained) as f64, upper as f64)
    };
    if estimate < lo * (1.0 - tolerance) || estimate > hi * (1.0 + tolerance) {
        return Err(Error::Source(format!(
            "column {}: footer distinct counts imply {} distinct values, but the sketch \
             estimates {estimate:.0}, beyond {FOOTER_SIGMAS} standard errors ({:.1}%); the \
             writer's distinct counts are not exact",
            col.name,
            if disjoint {
                format!("exactly {upper} (row groups are disjoint)")
            } else {
                format!("between {} and {upper}", lower.max(retained))
            },
            tolerance * 100.0
        )));
    }
    Ok(Some(if disjoint {
        (upper, DistinctSource::FooterDisjoint)
    } else {
        (
            clamp_estimate(estimate, lower.max(retained), upper),
            DistinctSource::HllWithinFooterBounds,
        )
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn combinations_are_lexicographic() {
        assert_eq!(
            combinations(4, 2),
            vec![
                vec![0, 1],
                vec![0, 2],
                vec![0, 3],
                vec![1, 2],
                vec![1, 3],
                vec![2, 3]
            ]
        );
        assert_eq!(combinations(3, 3), vec![vec![0, 1, 2]]);
        assert_eq!(combinations(2, 3), Vec::<Vec<usize>>::new());
        assert_eq!(combinations(5, 3).len(), 10);
    }

    #[test]
    fn supplied_columns_lead_each_tuple_and_the_rest_keep_schema_order() {
        let columns: Vec<KeyColumn> = ["a", "b", "c"]
            .iter()
            .map(|n| KeyColumn {
                name: (*n).to_string(),
                data_type: DataType::UInt64,
            })
            .collect();
        assert_eq!(
            keys_in_measured_order(&columns, 2, 64, &["c".to_string()]),
            vec![
                vec![0],
                vec![1],
                vec![2],
                vec![0, 1],
                vec![2, 0],
                vec![2, 1]
            ]
        );
        assert_eq!(
            keys_in_measured_order(&columns, 3, 64, &[]),
            vec![
                vec![0],
                vec![1],
                vec![2],
                vec![0, 1],
                vec![0, 2],
                vec![1, 2],
                vec![0, 1, 2]
            ]
        );
        // `max_tuples` caps the composites, never the single columns.
        assert_eq!(keys_in_measured_order(&columns, 3, 1, &[]).len(), 4);
    }

    #[test]
    fn id_like_types() {
        assert!(is_id_like(&DataType::UInt64));
        assert!(is_id_like(&DataType::Utf8));
        assert!(is_id_like(&DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Utf8)
        )));
        assert!(!is_id_like(&DataType::Float64));
        assert!(!is_id_like(&DataType::Decimal128(18, 2)));
        assert!(!is_id_like(&DataType::Boolean));
        assert!(!is_id_like(&DataType::Date32));
        assert!(could_be_payload(&DataType::Utf8));
        assert!(!could_be_payload(&DataType::UInt64));
        assert!(!could_be_payload(&DataType::FixedSizeBinary(16)));
    }

    #[test]
    fn estimates_clamp_into_bounds() {
        assert_eq!(clamp_estimate(10.4, 0, 100), 10);
        assert_eq!(clamp_estimate(10.4, 50, 100), 50);
        assert_eq!(clamp_estimate(1e9, 0, 100), 100);
        assert_eq!(clamp_estimate(f64::NAN, 3, 100), 3);
    }

    #[test]
    fn rates_derive_from_the_bound() {
        assert!((rate_of(u128::MAX) - 1.0).abs() < f64::EPSILON);
        assert!((rate_of(u128::MAX / 4) - 0.25).abs() < 1e-12);
    }

    #[test]
    fn cached_entries_round_trip_bit_for_bit() {
        let fresh = CachedEntry {
            columns: vec!["a".into(), "b".into()],
            distinct: 1_234,
            sampled: true,
            files: [1, 2, 3],
            row_groups: [2, 5, 9],
            files_total: 7,
            row_groups_total: 21,
            retained: 448,
            admission_bound: format!("{:032x}", u128::MAX / 3),
            hll_estimate: 1_234,
            distinct_source: DistinctSource::Hll,
            peak_entries: 896,
            peak_capacity: 896,
            high_water: 896,
            capped_at_rows: false,
        }
        .into_entry()
        .unwrap();
        let json = serde_json::to_vec(&CachedEntry::from_entry(&fresh).unwrap()).unwrap();
        let back: CachedEntry = serde_json::from_slice(&json).unwrap();
        let again = back.into_entry().unwrap();
        assert_eq!(again, fresh);
        let (a, b) = (again.sample.rate, fresh.sample.rate);
        assert_eq!(a.to_bits(), b.to_bits());
        assert_eq!(
            again.sample.quantile_rank_error.unwrap().to_bits(),
            fresh.sample.quantile_rank_error.unwrap().to_bits()
        );
    }

    #[test]
    fn tuples_match_core_key_columns_exactly() {
        use arrow::array::{StringArray, TimestampMicrosecondArray, UInt64Array};
        use bellwether_core::encoding::KeyColumns;
        use std::sync::Arc;

        let n = 64usize;
        let a: ArrayRef = Arc::new(UInt64Array::from(
            (0..n)
                .map(|i| (i % 5 != 0).then_some(i as u64 * 7))
                .collect::<Vec<_>>(),
        ));
        let b: ArrayRef = Arc::new(StringArray::from(
            (0..n)
                .map(|i| (i % 3 != 0).then(|| format!("v\u{0}{i}")))
                .collect::<Vec<_>>(),
        ));
        let c: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(
                (0..n)
                    .map(|i| (i % 4 != 1).then(|| i64::try_from(i).unwrap() - 30))
                    .collect::<Vec<_>>(),
            )
            .with_timezone("UTC"),
        );
        let arrays = [a, b, c];
        let types = [
            DataType::UInt64,
            DataType::Utf8,
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
        ];
        for key in [
            vec![0, 1],
            vec![1, 0],
            vec![0, 1, 2],
            vec![2, 0],
            vec![1, 2, 0],
        ] {
            let cols: Vec<ArrayRef> = key.iter().map(|&i| arrays[i].clone()).collect();
            let decl: Vec<DataType> = key.iter().map(|&i| types[i].clone()).collect();
            let core = KeyColumns::new(&cols, &decl).unwrap();
            for row in 0..n {
                let mut bufs = vec![Vec::new(); 3];
                let mut present = [false; 3];
                for i in 0..3 {
                    present[i] =
                        encode_array_value_as(arrays[i].as_ref(), row, &types[i], &mut bufs[i])
                            .unwrap();
                }
                let mut ours = Vec::new();
                let got = present[key[0]].then(|| {
                    build_tuple(&key, &bufs, &present, &mut ours);
                    ours.clone()
                });
                assert_eq!(got, core.encode(row).unwrap(), "key {key:?} row {row}");
            }
        }
    }
}
