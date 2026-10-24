//! Range analysis from row-group min/max.
//!
//! # [`ColumnRanges`], for every analyzed column
//!
//! Bounds are canonical bytes (ADR 016), so they compare as plain byte
//! strings whatever the column type.
//!
//! - `typical_overlap`: for each row group, the fraction of all row groups
//!   whose `[min, max]` intersects its own; the median over row groups. A
//!   clustered column scores about `1/R`; a column whose every range spans
//!   everything scores 1. A row group that may hold values but reports no
//!   bounds can never be pruned, so it intersects everything. This is the
//!   narrowness gate of `batch_stamp_key`.
//! - `sorted_fraction`: of consecutive row groups in dataset order (files by
//!   path, then row-group index), the fraction whose ranges ascend without
//!   overlapping (`max_i <= min_{i+1}`). `within_file_sorted_fraction`
//!   restricts that to pairs inside one file, which is the write order of a
//!   single writer.
//! - `disjoint`: every range intersects only itself, which proves each value
//!   lives in exactly one row group.
//!
//! # [`TimeScatter`], for date and timestamp columns
//!
//! The overlap depth and sortedness of `bellwether_source::stats`, the one
//! definition the benchmark's measured `planted` flag uses too: depth is the
//! time-weighted mean number of row-group ranges containing a point of their
//! union (sentinel ranges excluded), sortedness the fraction of consecutive
//! within-file row groups whose ranges ascend without overlapping. Computed
//! from footer statistics alone, on every profile; nothing about it is cached.

use arrow::datatypes::DataType;
use bellwether_source::stats::OverlapDepth;
use serde::{Deserialize, Serialize};

use crate::footer::{all_null, stats_of, FooterStats};
use crate::quantile::median_f64;

/// Range statistics of one column, from footers only.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ColumnRanges {
    /// The column.
    pub column: String,
    /// Row groups that may hold a value and report both bounds.
    pub row_groups_with_bounds: u64,
    /// Row groups that may hold a value but report no bounds.
    pub row_groups_unbounded: u64,
    /// Ascending, non-overlapping consecutive pairs in dataset order, if any
    /// pair has bounds on both sides.
    pub sorted_fraction: Option<f64>,
    /// The same, for pairs inside one file.
    pub within_file_sorted_fraction: Option<f64>,
    /// Median fraction of row groups a row group's range intersects.
    pub typical_overlap: f64,
    /// Every range intersects only itself.
    pub disjoint: bool,
}

type Bounds<'a> = Option<(&'a [u8], &'a [u8])>;

/// Analyzes `column`'s row-group bounds.
#[must_use]
pub fn analyze(footers: &FooterStats, column: &str) -> ColumnRanges {
    let total = footers.row_groups_total();
    // Row groups that may hold a non-null value, in dataset order.
    let mut valued: Vec<(usize, Bounds<'_>)> = Vec::new();
    for (fi, rg) in footers.row_groups() {
        if all_null(rg, column) {
            continue;
        }
        let bounds =
            stats_of(rg, column).and_then(|s| Some((s.min.as_deref()?, s.max.as_deref()?)));
        valued.push((fi, bounds));
    }
    let bounded: Vec<(&[u8], &[u8])> = valued.iter().filter_map(|v| v.1).collect();
    let unbounded = valued.len() - bounded.len();
    let mut los: Vec<&[u8]> = bounded.iter().map(|b| b.0).collect();
    let mut his: Vec<&[u8]> = bounded.iter().map(|b| b.1).collect();
    los.sort_unstable();
    his.sort_unstable();

    let mut fractions = Vec::with_capacity(valued.len());
    let mut alone = unbounded == 0;
    for (_, b) in &valued {
        let overlap = match b {
            Some((lo, hi)) => {
                let starts_after = los.len() - los.partition_point(|x| *x <= *hi);
                let ends_before = his.partition_point(|x| *x < *lo);
                bounded.len() - starts_after - ends_before + unbounded
            }
            None => valued.len(),
        };
        alone &= overlap == 1;
        #[allow(clippy::cast_precision_loss)]
        fractions.push(overlap as f64 / total.max(1) as f64);
    }
    let typical_overlap = median_f64(&mut fractions).unwrap_or(0.0);

    let (mut pairs, mut sorted, mut file_pairs, mut file_sorted) = (0u64, 0u64, 0u64, 0u64);
    for w in valued.windows(2) {
        let (Some((_, hi_a)), Some((lo_b, _))) = (w[0].1, w[1].1) else {
            continue;
        };
        let ascending = hi_a <= lo_b;
        pairs += 1;
        sorted += u64::from(ascending);
        if w[0].0 == w[1].0 {
            file_pairs += 1;
            file_sorted += u64::from(ascending);
        }
    }
    #[allow(clippy::cast_precision_loss)]
    let frac = |n: u64, d: u64| (d > 0).then(|| n as f64 / d as f64);
    ColumnRanges {
        column: column.to_string(),
        row_groups_with_bounds: bounded.len() as u64,
        row_groups_unbounded: unbounded as u64,
        sorted_fraction: frac(sorted, pairs),
        within_file_sorted_fraction: frac(file_sorted, file_pairs),
        typical_overlap,
        disjoint: alone,
    }
}

/// Whether the time-scatter analysis applies to `dt`.
#[must_use]
pub fn is_time_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64
    )
}

/// One time column of the analysis.
#[derive(Clone, Debug)]
pub struct TimeColumn {
    /// Name.
    pub name: String,
    /// Dataset type.
    pub data_type: DataType,
}

/// The shared overlap depth of one time column (`bellwether_source::stats`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TimeScatter {
    /// The column.
    pub column: String,
    /// Time-weighted mean number of row-group ranges containing a point of
    /// their union.
    pub depth: f64,
    /// Fraction of consecutive within-file row groups whose ranges ascend
    /// without overlapping.
    pub sortedness: f64,
    /// Whether the column is scattered by the shared definition: depth at
    /// least `SCATTER_MIN_DEPTH` and sortedness below
    /// `SCATTER_MAX_SORTEDNESS`.
    pub scattered: bool,
    /// Ranges measured.
    pub ranges: u64,
    /// Ranges excluded as sentinels (longer than 8 times the median range).
    pub excluded: u64,
    /// Row groups with values but no bounds.
    pub unbounded: u64,
    /// Sum of the measured ranges' lengths, in the column's unit.
    pub covered: u128,
    /// Length of their union.
    pub union: u128,
    /// Consecutive within-file pairs that ascend without overlapping.
    pub sorted_pairs: u64,
    /// Consecutive within-file pairs measured.
    pub pairs: u64,
}

impl TimeScatter {
    /// The report form of the shared measure for `column`.
    #[must_use]
    pub fn new(column: &str, o: &OverlapDepth) -> Self {
        Self {
            column: column.to_string(),
            depth: o.depth,
            sortedness: o.sortedness,
            scattered: o.is_scattered(),
            ranges: o.ranges,
            excluded: o.excluded,
            unbounded: o.unbounded,
            covered: o.covered,
            union: o.union,
            sorted_pairs: o.sorted_pairs,
            pairs: o.pairs,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::footer::FileStats;
    use bellwether_types::{ColumnStats, FileFingerprint, Hash, RowGroupMeta};

    fn int(v: i64) -> Vec<u8> {
        let mut out = vec![0x02];
        #[allow(clippy::cast_sign_loss)]
        out.extend_from_slice(&((v as u64) ^ (1 << 63)).to_be_bytes());
        out
    }

    fn footers(files: &[&[(i64, i64)]]) -> FooterStats {
        FooterStats {
            files: files
                .iter()
                .enumerate()
                .map(|(fi, rgs)| {
                    let fp = FileFingerprint {
                        path: format!("f{fi}"),
                        size: 1,
                        etag: None,
                        tail_blake3: Hash::of(&[u8::try_from(fi).unwrap()]),
                    };
                    FileStats {
                        fingerprint: fp.clone(),
                        row_groups: rgs
                            .iter()
                            .enumerate()
                            .map(|(i, &(lo, hi))| RowGroupMeta {
                                id: None,
                                file: fp.clone(),
                                index_in_file: u32::try_from(i).unwrap(),
                                rows: 10,
                                bytes: 10,
                                stats: vec![ColumnStats {
                                    column: "c".into(),
                                    min: Some(int(lo)),
                                    max: Some(int(hi)),
                                    null_count: Some(0),
                                    distinct_count: None,
                                    has_bloom: false,
                                    bytes: None,
                                }],
                                byte_range: None,
                            })
                            .collect(),
                    }
                })
                .collect(),
        }
    }

    #[test]
    fn sorted_disjoint_column() {
        let f = footers(&[&[(0, 9), (10, 19)], &[(20, 29), (30, 39)]]);
        let r = analyze(&f, "c");
        assert!(r.disjoint);
        assert_eq!(r.sorted_fraction, Some(1.0));
        assert_eq!(r.within_file_sorted_fraction, Some(1.0));
        assert!((r.typical_overlap - 0.25).abs() < 1e-9);
        assert_eq!(r.row_groups_with_bounds, 4);
    }

    #[test]
    fn stacked_parallel_writers_are_narrow_but_not_disjoint() {
        // Two files written in parallel over the same two slices.
        let f = footers(&[&[(0, 9), (10, 19)], &[(0, 9), (10, 19)]]);
        let r = analyze(&f, "c");
        assert!(!r.disjoint);
        assert_eq!(r.within_file_sorted_fraction, Some(1.0));
        // f0rg1 -> f1rg0 descends.
        assert!((r.sorted_fraction.unwrap() - 2.0 / 3.0).abs() < 1e-9);
        assert!((r.typical_overlap - 0.5).abs() < 1e-9);
    }

    #[test]
    fn spanning_ranges_overlap_everything() {
        let f = footers(&[&[(0, 100), (1, 99)], &[(2, 98), (0, 100)]]);
        let r = analyze(&f, "c");
        assert!((r.typical_overlap - 1.0).abs() < 1e-9);
        assert_eq!(r.within_file_sorted_fraction, Some(0.0));
    }

    #[test]
    fn missing_bounds_overlap_everything() {
        let mut f = footers(&[&[(0, 9), (10, 19), (20, 29)]]);
        f.files[0].row_groups[1].stats[0].min = None;
        let r = analyze(&f, "c");
        assert!(!r.disjoint);
        assert_eq!(r.row_groups_unbounded, 1);
        // Each bounded row group overlaps itself and the unbounded one.
        assert!((r.typical_overlap - 2.0 / 3.0).abs() < 1e-9);
        // An all-null row group holds no value and is ignored.
        let mut g = footers(&[&[(0, 9), (10, 19)]]);
        g.files[0].row_groups[1].stats[0].min = None;
        g.files[0].row_groups[1].stats[0].null_count = Some(10);
        assert!(analyze(&g, "c").disjoint);
    }
}
