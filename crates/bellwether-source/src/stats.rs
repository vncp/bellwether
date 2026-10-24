//! Row-group statistics decisions shared by the profiler
//! (`bellwether-profile`, its lints) and the benchmark (`bellwether-bench`,
//! its measured `planted` flags). Each is defined once, here, with every
//! constant it uses, so a lint and the ground truth that scores it cannot
//! disagree about what was decided from the same inputs.
//!
//! Both measures read only footer statistics: the canonical-encoded `min` and
//! `max` of a column in each row group (ADR 016). Callers pass row groups in
//! dataset order: files by path, row groups by index within a file.
//!
//! # Overlap depth ([`overlap_depth`])
//!
//! For a date, timestamp, or integer column, the time-weighted mean number of
//! row-group ranges `[min, max]` that contain a point, taken over the union of
//! those ranges: `depth = sum of range lengths / length of their union`, with
//! lengths counted in the column's own unit over closed ranges (a range
//! holding one value has length 1).
//!
//! - A clustered column (disjoint ranges) has depth 1, however sparse or
//!   uneven its values: gaps between ranges are not part of the union.
//! - `k` parallel writers covering the same slice of time give depth `k`.
//! - A *sentinel* range is excluded from the depth and counted in
//!   `excluded`, so one open-record placeholder such as 9999-12-31 cannot
//!   stretch the union and hide everything else. A range is a sentinel when
//!   it is longer than [`SENTINEL_RANGE_FACTOR`] times the median range
//!   **and** reaches outside the span of the ranges that are not that long.
//!   At most [`SENTINEL_MAX_EXCLUDED`] are excluded; when more qualify, none
//!   is. Rows written late stay inside the data's span, so their widened
//!   ranges always count.
//! - Row groups that may hold values but report no bounds are counted in
//!   `unbounded` and left out.
//!
//! `sortedness` is the fraction of consecutive row groups within one file
//! (one writer's write order) whose ranges ascend without overlapping: the
//! later row group's minimum is at or after the earlier one's maximum. Rows
//! written late, with timestamps from earlier periods, pull a row group's
//! minimum back below its predecessor's maximum. Parallel writers do not,
//! since each file still ascends. Every pair of bounded ranges counts,
//! sentinels included. A file with one row group contributes no pair, and with no pairs
//! at all sortedness is 1.0, since nothing shows disorder.
//!
//! `scattered_time_column` is planted, and the lint fires, when depth is at
//! least [`SCATTER_MIN_DEPTH`] and sortedness is below
//! [`SCATTER_MAX_SORTEDNESS`] ([`OverlapDepth::is_scattered`]).
//!
//! # Time correlation ([`time_correlation`])
//!
//! Cross-day concordance between a column and a time column. Each row group
//! is placed on the UTC day of its time-column maximum (late rows lower a
//! row group's minimum, never its maximum). Over every pair of row groups on
//! different days, it is the fraction whose column minima are strictly in day
//! order. It is `None` unless the row groups span at least two distinct days:
//! within one day a sorted id and a write stamp look alike. Values compare as
//! canonical bytes, so the column may be of any type.
//!
//! - A write-batch stamp increases with time across days: 1.0.
//! - An id sorted within each daily file restarts every day, and a
//!   range-partitioned id repeats every day: about 0.5, ties counting as out
//!   of order.
//! - Unlike a rank correlation over all row groups, it is not capped by ties
//!   within a day. A rank correlation tops out near 0.87 on two days whose row
//!   groups share one stamp per day.
//!
//! # Partners ([`partner_order`])
//!
//! Both composite rules judge a column against its composite with a
//! *partner*, and the benchmark and the profiler choose it by the same rule
//! from the same inputs: each candidate's distinct count (and whether it is
//! exact), whether the recorded workload supplies it, and its composite's p95
//! row groups.
//!
//! 1. A *row id*, a column whose exact distinct count reaches the row count,
//!    is refused, unless the workload supplies it: then it is the workload's
//!    key, not an unused row id. A sampled distinct count is never enough to
//!    class a column as a row id, since it is within the sketch's error of
//!    the truth either way.
//! 2. If the workload supplies any remaining candidate, only supplied
//!    candidates are considered: a lookup can seek only by what it supplies.
//! 3. A candidate that repeats ranks before a near-unique one (at least
//!    [`NEAR_UNIQUE_FRACTION`] of the rows distinct), then the smaller
//!    composite p95 row groups, then the caller's order (schema order).
//!
//! The rule then takes the first candidate in that order for which it holds.
//! The benchmark's only candidate is `entity_id`, which its workload
//! supplies, so wherever a recorded workload supplies `entity_id` the
//! profiler considers that candidate alone, and the two decide from
//! identical inputs.
//!
//! [`cap_distinct`] caps an estimated distinct count at the exact row count:
//! no key has more distinct values than there are rows.
//!
//! # Batch stamp ([`batch_stamp`])
//!
//! The whole `batch_stamp_key` decision for a column `B`, judged against a
//! composite of `B` with a key column: the benchmark's `(entity_id,
//! batch_id)`, or the profiler's partners in [`partner_order`]. All three
//! must hold:
//!
//! 1. one value of `B` touches at least [`BATCH_MIN_FILES_P50`] files at the
//!    median;
//! 2. `B`'s median row groups per value is at least [`BATCH_MIN_RATIO`] times
//!    the composite's;
//! 3. `B`'s cross-day concordance with the time column is at least
//!    [`BATCH_MIN_TIME_CORRELATION`], which needs at least two days.

use std::collections::BTreeSet;

use bellwether_types::{Error, FileFingerprint, Result, RowGroupMeta};

use crate::canonical::{
    TAG_INT, TAG_TEMPORAL, TAG_UINT, TU_DAY, TU_MICROSECOND, TU_MILLISECOND, TU_NANOSECOND,
    TU_SECOND, UNIT_DATE32, UNIT_DATE64, UNIT_TIMESTAMP,
};

/// `scattered_time_column`: overlap depth at least this.
pub const SCATTER_MIN_DEPTH: f64 = 1.5;
/// `scattered_time_column`: sortedness below this.
pub const SCATTER_MAX_SORTEDNESS: f64 = 0.9;
/// A sentinel range is longer than this many times the median range (and
/// reaches outside the span of the others).
pub const SENTINEL_RANGE_FACTOR: u128 = 8;
/// At most this many ranges are excluded as sentinels; when more qualify,
/// none is, since that many outliers are the data's own shape.
pub const SENTINEL_MAX_EXCLUDED: usize = 4;
/// A partner with at least this fraction of the rows as distinct values is
/// near-unique, and ranks after one that repeats.
pub const NEAR_UNIQUE_FRACTION: f64 = 0.9;
/// `batch_stamp_key`: one value touches at least this many files at the
/// median.
pub const BATCH_MIN_FILES_P50: f64 = 2.0;
/// `batch_stamp_key`: the column's median row groups per value is at least
/// this many times the composite's.
pub const BATCH_MIN_RATIO: f64 = 2.0;
/// `batch_stamp_key`: cross-day concordance with time at least this.
pub const BATCH_MIN_TIME_CORRELATION: f64 = 0.9;

/// How much a column's row-group ranges overlap, and how ordered they are.
#[derive(Clone, Debug, PartialEq)]
pub struct OverlapDepth {
    /// Time-weighted mean number of ranges containing a point of their union.
    pub depth: f64,
    /// Fraction of consecutive within-file row groups whose ranges ascend
    /// without overlapping.
    pub sortedness: f64,
    /// Ranges excluded from the depth as sentinels (see the module docs).
    pub excluded: u64,
    /// Ranges in the depth, after exclusion.
    pub ranges: u64,
    /// Row groups that may hold values but report no bounds.
    pub unbounded: u64,
    /// Sum of the measured ranges' lengths, in the column's unit.
    pub covered: u128,
    /// Length of the union of the measured ranges.
    pub union: u128,
    /// Consecutive within-file pairs whose ranges ascend without overlap.
    pub sorted_pairs: u64,
    /// Consecutive within-file pairs with both ranges bounded.
    pub pairs: u64,
}

impl OverlapDepth {
    /// Whether the column is a scattered time column: depth at least
    /// [`SCATTER_MIN_DEPTH`] and sortedness below [`SCATTER_MAX_SORTEDNESS`].
    #[must_use]
    pub fn is_scattered(&self) -> bool {
        self.depth >= SCATTER_MIN_DEPTH && self.sortedness < SCATTER_MAX_SORTEDNESS
    }
}

/// The encoding a canonical bound was decoded from; every bound of a column
/// must agree on it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    Int,
    UInt,
    Temporal { kind: u8, unit: u8 },
}

fn decode(bytes: &[u8]) -> Result<(Shape, i128)> {
    let bad = || {
        Error::Source(format!(
            "statistics bound {bytes:02x?} is not an integer, date, or timestamp"
        ))
    };
    let payload = |b: &[u8]| b.try_into().map(u64::from_be_bytes).map_err(|_| bad());
    match bytes.first() {
        Some(&TAG_INT) if bytes.len() == 9 => {
            #[allow(clippy::cast_possible_wrap)]
            let v = (payload(&bytes[1..])? ^ (1 << 63)) as i64;
            Ok((Shape::Int, i128::from(v)))
        }
        Some(&TAG_UINT) if bytes.len() == 9 => Ok((Shape::UInt, i128::from(payload(&bytes[1..])?))),
        Some(&TAG_TEMPORAL) if bytes.len() == 11 => {
            #[allow(clippy::cast_possible_wrap)]
            let v = (payload(&bytes[3..])? ^ (1 << 63)) as i64;
            Ok((
                Shape::Temporal {
                    kind: bytes[1],
                    unit: bytes[2],
                },
                i128::from(v),
            ))
        }
        _ => Err(bad()),
    }
}

/// Units of a temporal shape per UTC day, or `None` for a non-date shape.
fn units_per_day(shape: Shape) -> Option<i128> {
    match shape {
        Shape::Temporal {
            kind: UNIT_DATE32,
            unit: TU_DAY,
        } => Some(1),
        Shape::Temporal {
            kind: UNIT_DATE64,
            unit: TU_MILLISECOND,
        } => Some(86_400_000),
        Shape::Temporal {
            kind: UNIT_TIMESTAMP,
            unit,
        } => match unit {
            TU_SECOND => Some(86_400),
            TU_MILLISECOND => Some(86_400_000),
            TU_MICROSECOND => Some(86_400_000_000),
            TU_NANOSECOND => Some(86_400_000_000_000),
            _ => None,
        },
        _ => None,
    }
}

/// Whether `rg` provably holds no non-null value of `column`.
fn all_null(rg: &RowGroupMeta, column: &str) -> bool {
    rg.rows == 0
        || rg
            .stats
            .iter()
            .find(|s| s.column == column)
            .and_then(|s| s.null_count)
            == Some(rg.rows)
}

/// `(min, max)` canonical bounds of `column` in `rg`, if both are known.
fn bounds<'a>(rg: &'a RowGroupMeta, column: &str) -> Option<(&'a [u8], &'a [u8])> {
    let s = rg.stats.iter().find(|s| s.column == column)?;
    Some((s.min.as_deref()?, s.max.as_deref()?))
}

/// Decodes one column's bounds, refusing bounds that disagree on their
/// encoding: they would compare values of different units.
struct Decoder<'a> {
    column: &'a str,
    shape: Option<Shape>,
}

impl<'a> Decoder<'a> {
    fn new(column: &'a str) -> Self {
        Self {
            column,
            shape: None,
        }
    }

    fn value(&mut self, bytes: &[u8]) -> Result<i128> {
        let (shape, v) =
            decode(bytes).map_err(|e| Error::Source(format!("column {}: {e}", self.column)))?;
        match self.shape {
            None => self.shape = Some(shape),
            Some(s) if s != shape => {
                return Err(Error::Source(format!(
                    "column {}: statistics bounds are encoded as {s:?} and as {shape:?}",
                    self.column
                )))
            }
            Some(_) => {}
        }
        Ok(v)
    }
}

fn length(lo: i128, hi: i128) -> u128 {
    (hi - lo + 1).unsigned_abs()
}

type Range<'a> = Option<(&'a FileFingerprint, i128, i128)>;

/// Indexes into `seen` of the ranges excluded as sentinels (see the module
/// docs): longer than [`SENTINEL_RANGE_FACTOR`] times the median range,
/// reaching outside the span of the ranges that are not that long, and no
/// more than [`SENTINEL_MAX_EXCLUDED`] of them.
fn sentinels(seen: &[Range<'_>]) -> BTreeSet<usize> {
    let mut lengths: Vec<u128> = seen
        .iter()
        .flatten()
        .map(|&(_, lo, hi)| length(lo, hi))
        .collect();
    lengths.sort_unstable();
    let median = lengths
        .get(lengths.len().saturating_sub(1) / 2)
        .copied()
        .unwrap_or(0);
    let limit = median.saturating_mul(SENTINEL_RANGE_FACTOR);
    let span = seen
        .iter()
        .flatten()
        .filter(|&&(_, lo, hi)| length(lo, hi) <= limit)
        .fold(None, |acc: Option<(i128, i128)>, &(_, lo, hi)| {
            Some(acc.map_or((lo, hi), |(a, b)| (a.min(lo), b.max(hi))))
        });
    let Some((span_lo, span_hi)) = span else {
        return BTreeSet::new();
    };
    let candidates: BTreeSet<usize> = seen
        .iter()
        .enumerate()
        .filter_map(|(i, r)| r.map(|(_, lo, hi)| (i, lo, hi)))
        .filter(|&(_, lo, hi)| length(lo, hi) > limit && (lo < span_lo || hi > span_hi))
        .map(|(i, _, _)| i)
        .collect();
    if candidates.len() <= SENTINEL_MAX_EXCLUDED {
        candidates
    } else {
        // Too many to be placeholders: they are the data's own shape.
        BTreeSet::new()
    }
}

/// The overlap depth and sortedness of `column` over `row_groups`, in
/// dataset order. Errors if a bound is not an integer, date, or timestamp,
/// or if the bounds disagree on their encoding.
pub fn overlap_depth(row_groups: &[RowGroupMeta], column: &str) -> Result<OverlapDepth> {
    let mut decoder = Decoder::new(column);
    // (file, lo, hi) per row group with bounds, in dataset order; `None`
    // marks a row group with values but no bounds, which breaks a run.
    let mut seen: Vec<Range<'_>> = Vec::new();
    let mut unbounded = 0u64;
    for rg in row_groups {
        if all_null(rg, column) {
            continue;
        }
        if let Some((lo, hi)) = bounds(rg, column) {
            let (lo, hi) = (decoder.value(lo)?, decoder.value(hi)?);
            seen.push(Some((&rg.file, lo, hi.max(lo))));
        } else {
            unbounded += 1;
            seen.push(None);
        }
    }
    // Sortedness counts every bounded pair, sentinels included: a sentinel's
    // row group is out of order too.
    let (mut sorted_pairs, mut pairs) = (0u64, 0u64);
    for w in seen.windows(2) {
        if let (Some((fa, _, hi_a)), Some((fb, lo_b, _))) = (w[0], w[1]) {
            if fa == fb {
                pairs += 1;
                sorted_pairs += u64::from(lo_b >= hi_a);
            }
        }
    }

    let excluded_at = sentinels(&seen);
    let mut ranges: Vec<(i128, i128)> = seen
        .iter()
        .enumerate()
        .filter(|(i, _)| !excluded_at.contains(i))
        .filter_map(|(_, r)| r.map(|(_, lo, hi)| (lo, hi)))
        .collect();
    let excluded = excluded_at.len() as u64;
    let covered: u128 = ranges.iter().map(|&(lo, hi)| length(lo, hi)).sum();
    ranges.sort_unstable();
    let mut union = 0u128;
    let mut current: Option<(i128, i128)> = None;
    for &(lo, hi) in &ranges {
        current = match current {
            Some((a, b)) if lo <= b + 1 => Some((a, b.max(hi))),
            Some((a, b)) => {
                union += length(a, b);
                Some((lo, hi))
            }
            None => Some((lo, hi)),
        };
    }
    if let Some((a, b)) = current {
        union += length(a, b);
    }
    Ok(OverlapDepth {
        depth: ratio(covered, union, 0.0),
        sortedness: ratio(u128::from(sorted_pairs), u128::from(pairs), 1.0),
        excluded,
        ranges: ranges.len() as u64,
        unbounded,
        covered,
        union,
        sorted_pairs,
        pairs,
    })
}

#[allow(clippy::cast_precision_loss)]
fn ratio(num: u128, den: u128, empty: f64) -> f64 {
    if den == 0 {
        empty
    } else {
        num as f64 / den as f64
    }
}

/// The cross-day concordance of `column` with `time_column` over
/// `row_groups` (see the module docs), or `None` when the row groups with
/// both columns' bounds span fewer than two distinct UTC days. Errors if
/// `time_column` is not a date or timestamp.
pub fn time_correlation(
    row_groups: &[RowGroupMeta],
    column: &str,
    time_column: &str,
) -> Result<Option<f64>> {
    let mut decoder = Decoder::new(time_column);
    let mut points: Vec<(i128, &[u8])> = Vec::new();
    for rg in row_groups {
        let (Some((x, _)), Some((_, tmax))) = (bounds(rg, column), bounds(rg, time_column)) else {
            continue;
        };
        let t = decoder.value(tmax)?;
        let shape = decoder.shape.unwrap_or(Shape::Int);
        let per_day = units_per_day(shape).ok_or_else(|| {
            Error::Source(format!(
                "time column {time_column} is encoded as {shape:?}, not a date or timestamp"
            ))
        })?;
        points.push((t.div_euclid(per_day), x));
    }
    let days: BTreeSet<i128> = points.iter().map(|p| p.0).collect();
    if days.len() < 2 {
        return Ok(None);
    }
    // Rank the column minima, then count, day by day, the earlier-day row
    // groups whose minimum is strictly smaller (a Fenwick tree over ranks).
    let mut distinct: Vec<&[u8]> = points.iter().map(|p| p.1).collect();
    distinct.sort_unstable();
    distinct.dedup();
    points.sort_unstable();
    let mut tree = vec![0u64; distinct.len() + 1];
    let (mut agree, mut total, mut earlier) = (0u64, 0u64, 0u64);
    let mut i = 0;
    while i < points.len() {
        let day = points[i].0;
        let mut j = i;
        while j < points.len() && points[j].0 == day {
            let rank = distinct.partition_point(|v| *v < points[j].1);
            agree += prefix_sum(&tree, rank);
            total += earlier;
            j += 1;
        }
        for p in &points[i..j] {
            add(&mut tree, distinct.partition_point(|v| *v < p.1) + 1);
        }
        earlier += (j - i) as u64;
        i = j;
    }
    Ok(Some(ratio(u128::from(agree), u128::from(total), 0.0)))
}

/// Sum of the first `n` ranks (ranks are 1-based in the tree).
fn prefix_sum(tree: &[u64], mut n: usize) -> u64 {
    let mut s = 0;
    while n > 0 {
        s += tree[n];
        n &= n - 1;
    }
    s
}

fn add(tree: &mut [u64], mut i: usize) {
    while i < tree.len() {
        tree[i] += 1;
        i += i & i.wrapping_neg();
    }
}

/// One candidate partner for a composite rule (see the module docs).
#[derive(Clone, Debug, PartialEq)]
pub struct PartnerCandidate {
    /// The partner column.
    pub column: String,
    /// Its distinct count, capped at the row count ([`cap_distinct`]).
    pub distinct: u64,
    /// Whether `distinct` was counted exactly rather than estimated.
    pub distinct_exact: bool,
    /// Whether the recorded workload supplies the column.
    pub supplied: bool,
    /// p95 row groups per value of its composite with the judged column.
    pub composite_row_groups_p95: f64,
}

/// Caps an estimated distinct count at the exact row count, and says whether
/// it was capped.
#[must_use]
pub fn cap_distinct(distinct: u64, rows_total: u64) -> (u64, bool) {
    (distinct.min(rows_total), distinct > rows_total)
}

/// Whether a column is a row id: its exact distinct count reaches the row
/// count. An estimate never qualifies.
#[must_use]
pub fn is_row_id(distinct: u64, distinct_exact: bool, rows_total: u64) -> bool {
    distinct_exact && rows_total > 0 && distinct >= rows_total
}

/// Indexes into `candidates` in partner order, refused candidates left out
/// (see the module docs). Ties keep the caller's order.
#[must_use]
pub fn partner_order(candidates: &[PartnerCandidate], rows_total: u64) -> Vec<usize> {
    let mut kept: Vec<usize> = (0..candidates.len())
        .filter(|&i| {
            let c = &candidates[i];
            c.supplied || !is_row_id(c.distinct, c.distinct_exact, rows_total)
        })
        .collect();
    if kept.iter().any(|&i| candidates[i].supplied) {
        kept.retain(|&i| candidates[i].supplied);
    }
    #[allow(clippy::cast_precision_loss)]
    let near_unique = |c: &PartnerCandidate| {
        rows_total > 0 && c.distinct as f64 >= NEAR_UNIQUE_FRACTION * rows_total as f64
    };
    kept.sort_by(|&a, &b| {
        let (ca, cb) = (&candidates[a], &candidates[b]);
        near_unique(ca).cmp(&near_unique(cb)).then(
            ca.composite_row_groups_p95
                .total_cmp(&cb.composite_row_groups_p95),
        )
    });
    kept
}

/// The `batch_stamp_key` decision and the numbers behind it.
#[derive(Clone, Debug, PartialEq)]
pub struct BatchStamp {
    /// All three conditions hold (see the module docs).
    pub planted: bool,
    /// Files one value of the column touches at the median.
    pub files_p50: f64,
    /// The column's median row groups per value over the composite's.
    pub ratio: f64,
    /// Cross-day concordance with the time column, `None` within one day.
    pub concordance: Option<f64>,
}

/// Decides whether `column` is a write-batch stamp (see the module docs):
/// `files_p50` and `row_groups_p50` are the column's median files and row
/// groups per value, `composite_row_groups_p50` the median row groups per
/// value of its composite with a key column. Errors as [`time_correlation`].
pub fn batch_stamp(
    files_p50: f64,
    row_groups_p50: f64,
    composite_row_groups_p50: f64,
    row_groups: &[RowGroupMeta],
    column: &str,
    time_column: &str,
) -> Result<BatchStamp> {
    let concordance = time_correlation(row_groups, column, time_column)?;
    let ratio = if composite_row_groups_p50 > 0.0 {
        row_groups_p50 / composite_row_groups_p50
    } else {
        0.0
    };
    Ok(BatchStamp {
        planted: files_p50 >= BATCH_MIN_FILES_P50
            && ratio >= BATCH_MIN_RATIO
            && concordance.is_some_and(|c| c >= BATCH_MIN_TIME_CORRELATION),
        files_p50,
        ratio,
        concordance,
    })
}

#[cfg(test)]
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]
mod tests {
    use super::*;
    use bellwether_types::{ColumnStats, Hash};

    /// `(column, min, max)`.
    type ColSpec = (&'static str, Vec<u8>, Vec<u8>);
    /// `(file, columns)`.
    type RgSpec = (usize, Vec<ColSpec>);

    const DAY_US: i64 = 86_400_000_000;

    fn ts(v: i64) -> Vec<u8> {
        let mut out = vec![TAG_TEMPORAL, UNIT_TIMESTAMP, TU_MICROSECOND];
        out.extend_from_slice(&((v as u64) ^ (1 << 63)).to_be_bytes());
        out
    }

    fn int(v: i64) -> Vec<u8> {
        let mut out = vec![TAG_INT];
        out.extend_from_slice(&((v as u64) ^ (1 << 63)).to_be_bytes());
        out
    }

    fn file(i: usize) -> FileFingerprint {
        FileFingerprint {
            path: format!("f{i:03}"),
            size: 1,
            etag: None,
            tail_blake3: Hash::of(&i.to_be_bytes()),
        }
    }

    /// Row groups `(file, [(column, min, max)])` with 10 rows each.
    fn rgs(spec: &[RgSpec]) -> Vec<RowGroupMeta> {
        let mut index = std::collections::BTreeMap::<usize, u32>::new();
        spec.iter()
            .map(|(f, cols)| {
                let i = index.entry(*f).or_insert(0);
                let rg = RowGroupMeta {
                    id: None,
                    file: file(*f),
                    index_in_file: *i,
                    rows: 10,
                    bytes: 10,
                    stats: cols
                        .iter()
                        .map(|(c, lo, hi)| ColumnStats {
                            column: (*c).to_string(),
                            min: Some(lo.clone()),
                            max: Some(hi.clone()),
                            null_count: Some(0),
                            distinct_count: None,
                            has_bloom: false,
                            bytes: None,
                        })
                        .collect(),
                    byte_range: None,
                };
                *i += 1;
                rg
            })
            .collect()
    }

    fn t(file: usize, lo: i64, hi: i64) -> RgSpec {
        (file, vec![("t", ts(lo), ts(hi))])
    }

    #[test]
    fn sorted_sparse_ranges_have_depth_one() {
        // Uneven gaps between ranges are not part of the union.
        let r = rgs(&[t(0, 0, 9), t(0, 1_000, 1_009), t(0, 50_000, 50_009)]);
        let d = overlap_depth(&r, "t").unwrap();
        assert!((d.depth - 1.0).abs() < 1e-12, "{d:?}");
        assert_eq!((d.covered, d.union), (30, 30));
        assert!((d.sortedness - 1.0).abs() < 1e-12);
        assert!(!d.is_scattered());
    }

    #[test]
    fn parallel_writers_stack_but_stay_sorted() {
        // Four files, each with two ascending slices of the same day.
        let mut spec = Vec::new();
        for f in 0..4 {
            spec.push(t(f, 0, 99));
            spec.push(t(f, 100, 199));
        }
        let d = overlap_depth(&rgs(&spec), "t").unwrap();
        assert!((d.depth - 4.0).abs() < 1e-12, "{d:?}");
        assert_eq!((d.sorted_pairs, d.pairs), (4, 4));
        assert!(!d.is_scattered(), "stacked but every file ascends");
    }

    #[test]
    fn late_rows_pull_minima_back_and_scatter() {
        // Same layout, but each second row group holds a row from long before.
        let mut spec = Vec::new();
        for f in 0..4 {
            spec.push(t(f, 0, 99));
            spec.push(t(f, -500, 199));
        }
        let d = overlap_depth(&rgs(&spec), "t").unwrap();
        assert_eq!((d.sorted_pairs, d.pairs), (0, 4));
        assert!(d.depth >= SCATTER_MIN_DEPTH);
        assert!(d.is_scattered());
    }

    #[test]
    fn a_sentinel_range_is_excluded_not_trusted() {
        let mut spec: Vec<_> = (0..10).map(|i| t(0, i * 100, i * 100 + 99)).collect();
        spec.push(t(0, 1_000, 253_402_214_400_000_000));
        let d = overlap_depth(&rgs(&spec), "t").unwrap();
        assert_eq!(d.excluded, 1);
        assert!((d.depth - 1.0).abs() < 1e-12, "{d:?}");
        // The sentinel's pair counts; it starts after its predecessor ends.
        assert_eq!((d.sorted_pairs, d.pairs), (10, 10));
    }

    #[test]
    fn late_rows_inside_the_span_are_never_sentinels() {
        // Hourly ranges; every third from hour 48 on is widened 48 hours back,
        // 49x the median but inside the data's span: all kept.
        let spec: Vec<_> = (0..200i64)
            .map(|h| {
                let lo = if h >= 48 && h % 3 == 0 { h - 48 } else { h };
                t(0, lo * 100, h * 100 + 99)
            })
            .collect();
        let d = overlap_depth(&rgs(&spec), "t").unwrap();
        assert_eq!(d.excluded, 0, "{d:?}");
        assert!(d.is_scattered(), "{d:?}");
    }

    #[test]
    fn a_few_long_ranges_inside_the_span_are_kept() {
        // Three ranges 20x the median, under the cap of 4, but inside the
        // span of the others: late rows, not placeholders.
        let mut spec: Vec<_> = (0..40).map(|i| t(0, i * 100, i * 100 + 99)).collect();
        for k in 0..3i64 {
            spec.push(t(1, k * 100, k * 100 + 1_999));
        }
        let d = overlap_depth(&rgs(&spec), "t").unwrap();
        assert_eq!(d.excluded, 0, "{d:?}");
        // The same three reaching past the end are excluded.
        let mut spec: Vec<_> = (0..40).map(|i| t(0, i * 100, i * 100 + 99)).collect();
        for k in 0..3i64 {
            spec.push(t(1, 3_000 + k * 100, 3_000 + k * 100 + 1_999));
        }
        assert_eq!(overlap_depth(&rgs(&spec), "t").unwrap().excluded, 3);
    }

    #[test]
    fn more_outliers_than_the_cap_are_the_data() {
        let base: Vec<_> = (0..20).map(|i| t(0, i * 100, i * 100 + 99)).collect();
        let far = |n: i64| {
            let mut spec = base.clone();
            for k in 0..n {
                spec.push(t(1, 0, 1_000_000 + k));
            }
            overlap_depth(&rgs(&spec), "t").unwrap()
        };
        // The documented cap, written out: 4 are excluded, 5 are the data.
        assert_eq!(far(4).excluded, 4);
        assert_eq!(far(5).excluded, 0);
    }

    #[test]
    fn batch_stamp_needs_files_ratio_and_time() {
        let mut spec = Vec::new();
        for d in 0..2i64 {
            for f in 0..2usize {
                spec.push((
                    d as usize * 2 + f,
                    vec![
                        ("t", ts(d * DAY_US), ts(d * DAY_US + 10)),
                        ("b", int(d), int(d)),
                    ],
                ));
            }
        }
        let r = rgs(&spec);
        let yes = batch_stamp(2.0, 4.0, 2.0, &r, "b", "t").unwrap();
        assert!(yes.planted, "{yes:?}");
        assert_eq!((yes.ratio, yes.concordance), (2.0, Some(1.0)));
        assert!(!batch_stamp(1.0, 4.0, 2.0, &r, "b", "t").unwrap().planted);
        assert!(!batch_stamp(2.0, 3.9, 2.0, &r, "b", "t").unwrap().planted);
        let one_day = rgs(&spec[..2]);
        let d = batch_stamp(2.0, 4.0, 2.0, &one_day, "b", "t").unwrap();
        assert!(!d.planted && d.concordance.is_none(), "{d:?}");
    }

    fn cand(
        column: &str,
        distinct: u64,
        exact: bool,
        supplied: bool,
        p95: f64,
    ) -> PartnerCandidate {
        PartnerCandidate {
            column: column.into(),
            distinct,
            distinct_exact: exact,
            supplied,
            composite_row_groups_p95: p95,
        }
    }

    #[test]
    fn a_row_id_is_refused_unless_supplied_and_only_when_exact() {
        let rows = 1_000;
        // An unsupplied exact row id is refused; supplied, it is the key.
        assert_eq!(
            partner_order(&[cand("id", 1_000, true, false, 1.0)], rows),
            Vec::<usize>::new()
        );
        assert_eq!(
            partner_order(&[cand("id", 1_000, true, true, 1.0)], rows),
            vec![0]
        );
        // A sampled count at the row count is not enough to refuse it.
        assert_eq!(
            partner_order(&[cand("id", 1_000, false, false, 1.0)], rows),
            vec![0]
        );
        assert_eq!(cap_distinct(1_009, rows), (1_000, true));
        assert_eq!(cap_distinct(999, rows), (999, false));
    }

    #[test]
    fn supplied_candidates_exclude_the_rest() {
        let c = [
            cand("a", 10, true, false, 1.0),
            cand("b", 10, true, true, 5.0),
        ];
        assert_eq!(partner_order(&c, 1_000), vec![1]);
    }

    #[test]
    fn a_repeating_partner_ranks_before_a_near_unique_one_with_a_smaller_p95() {
        // Both would pass; the near-unique one has the smaller composite p95,
        // yet the repeating one comes first.
        let c = [
            cand("near", 950, true, false, 1.0),
            cand("rep", 100, true, false, 2.0),
        ];
        assert_eq!(partner_order(&c, 1_000), vec![1, 0]);
        // Without the near-unique difference the smaller p95 wins.
        let c = [
            cand("x", 100, true, false, 3.0),
            cand("y", 100, true, false, 2.0),
        ];
        assert_eq!(partner_order(&c, 1_000), vec![1, 0]);
    }

    #[test]
    fn mixed_encodings_and_strings_are_errors() {
        let r = rgs(&[
            (0, vec![("t", ts(0), ts(1))]),
            (0, vec![("t", int(0), int(1))]),
        ]);
        assert!(overlap_depth(&r, "t").is_err());
        let s = rgs(&[(
            0,
            vec![("t", vec![0x06, b'a', 0, 0], vec![0x06, b'b', 0, 0])],
        )]);
        assert!(overlap_depth(&s, "t").is_err());
    }

    #[test]
    fn a_batch_stamp_concords_with_time_across_days() {
        // Two days, three files per day, one row group per file spanning the
        // day; the stamp minimum is the day's first window in every file.
        let mut spec = Vec::new();
        for d in 0..2i64 {
            for f in 0..3usize {
                spec.push((
                    d as usize * 3 + f,
                    vec![
                        ("t", ts(d * DAY_US + 10), ts(d * DAY_US + 1_000 + f as i64)),
                        ("b", int(20_260_101 + d), int(20_260_101 + d)),
                        ("id", int(f as i64), int(1_000 + f as i64)),
                    ],
                ));
            }
        }
        let r = rgs(&spec);
        assert_eq!(time_correlation(&r, "b", "t").unwrap(), Some(1.0));
        // An id range that repeats every day ties across days: discordant.
        assert_eq!(time_correlation(&r, "id", "t").unwrap(), Some(1.0 / 3.0));
        // One day only: undefined.
        assert_eq!(time_correlation(&r[..3], "b", "t").unwrap(), None);
        // The time column must be a date or timestamp.
        assert!(time_correlation(&r, "b", "id").is_err());
    }

    #[test]
    fn concordance_counts_every_cross_day_pair() {
        // Days 0, 1, 2 with minima 5, 3, 9: pairs (0,1) disagree, (0,2) and
        // (1,2) agree.
        let spec: Vec<_> = [(0i64, 5i64), (1, 3), (2, 9)]
            .iter()
            .enumerate()
            .map(|(f, &(d, v))| {
                (
                    f,
                    vec![
                        ("t", ts(d * DAY_US), ts(d * DAY_US + 1)),
                        ("x", int(v), int(v)),
                    ],
                )
            })
            .collect();
        let c = time_correlation(&rgs(&spec), "x", "t").unwrap().unwrap();
        assert!((c - 2.0 / 3.0).abs() < 1e-12, "{c}");
    }
}
