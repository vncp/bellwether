//! Sampled fanout is within its stated error: the `HyperLogLog` distinct count
//! within four standard errors, each sampled quantile inside the DKW band
//! around the population quantile, the sampled max at most the true max.
//! Footer distinct counts bound the estimate, and make it exact when the
//! row-group ranges are disjoint.

#![allow(
    clippy::unwrap_used,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation
)]

mod common;

use std::collections::{BTreeMap, BTreeSet};

use bellwether_profile::hll::Hll;
use bellwether_profile::{
    profile, DistinctSource, FanoutEntry, ProfileOptions, DEFAULT_HLL_PRECISION,
};
use common::{mix, nearest_rank, temp_store, u64_batch, MemSource};

/// Value `v` occurs in row group `r` when a hash of `(v, r)` falls under a
/// value-specific rate, so fanout varies from one row group to all of them.
fn occurs(v: u64, r: u64) -> bool {
    mix(v.wrapping_mul(0x9E37_79B9) ^ r) % (1 + v % 9) == 0
}

const FILES: u64 = 4;
const RGS_PER_FILE: u64 = 8;
const VALUES: u64 = 30_000;

fn dataset() -> (MemSource, Vec<u64>, Vec<u64>) {
    let mut per_value: BTreeMap<u64, (BTreeSet<u64>, BTreeSet<u64>)> = BTreeMap::new();
    let mut files = Vec::new();
    for f in 0..FILES {
        let mut rgs = Vec::new();
        for i in 0..RGS_PER_FILE {
            let r = f * RGS_PER_FILE + i;
            let values: Vec<u64> = (0..VALUES).filter(|&v| occurs(v, r)).collect();
            for &v in &values {
                let e = per_value.entry(v).or_default();
                e.0.insert(r);
                e.1.insert(f);
            }
            rgs.push(u64_batch("v", values));
        }
        files.push(rgs);
    }
    let mut rgs: Vec<u64> = per_value.values().map(|e| e.0.len() as u64).collect();
    let mut fs: Vec<u64> = per_value.values().map(|e| e.1.len() as u64).collect();
    rgs.sort_unstable();
    fs.sort_unstable();
    let schema = files[0][0].schema();
    (MemSource::new(schema, files), rgs, fs)
}

fn single(report: &bellwether_profile::ProfileReport) -> &FanoutEntry {
    &report.fanout[0]
}

#[test]
fn sampled_fanout_is_within_the_stated_error() {
    let (source, rgs, files) = dataset();
    let (_s, store) = temp_store();
    let exact = profile(
        &source,
        "mem",
        &store,
        &ProfileOptions::for_source("parquet"),
    )
    .unwrap();
    let e = single(&exact);
    assert!(!e.fanout.sampled);
    assert_eq!(e.fanout.distinct, rgs.len() as u64);
    let q = |v: &[u64]| {
        [
            nearest_rank(v, 0.5) as f64,
            nearest_rank(v, 0.95) as f64,
            *v.last().unwrap() as f64,
        ]
    };
    assert_eq!(e.fanout.row_groups_per_value, q(&rgs));
    assert_eq!(e.fanout.files_per_value, q(&files));

    let rse = Hll::relative_standard_error(DEFAULT_HLL_PRECISION);
    for (seed, tracked) in [(1u64, 448usize), (2, 896), (3, 3_584)] {
        let report = profile(
            &source,
            "mem",
            &store,
            &ProfileOptions {
                max_tracked_per_key: Some(tracked),
                seed,
                ..ProfileOptions::for_source("parquet")
            },
        )
        .unwrap();
        let s = single(&report);
        let f = &s.fanout;
        assert!(f.sampled);
        assert_eq!(s.sample.distinct_source, DistinctSource::Hll);
        assert!(s.sample.retained <= tracked as u64);
        let truth = rgs.len() as f64;
        let err = (f.distinct as f64 - truth).abs() / truth;
        assert!(
            err <= 4.0 * rse,
            "distinct {} vs {truth}: {err}",
            f.distinct
        );
        // The sample rate matches the retained fraction.
        assert!(
            (s.sample.rate - s.sample.retained as f64 / truth).abs() < 0.02,
            "{:?}",
            s.sample
        );
        let eps = s.sample.quantile_rank_error.unwrap();
        for (i, qq) in [0.5, 0.95].into_iter().enumerate() {
            for (got, pop) in [
                (f.row_groups_per_value[i], &rgs),
                (f.files_per_value[i], &files),
            ] {
                let lo = nearest_rank(pop, (qq - eps).max(0.0)) as f64;
                let hi = nearest_rank(pop, (qq + eps).min(1.0)) as f64;
                assert!(
                    lo <= got && got <= hi,
                    "q{qq}: {got} outside [{lo}, {hi}] (eps {eps})"
                );
            }
        }
        assert!(f.row_groups_per_value[2] <= *rgs.last().unwrap() as f64);
        assert!(f.files_per_value[2] <= *files.last().unwrap() as f64);
    }
}

fn ids(rows: u64, start: impl Fn(u64) -> u64) -> MemSource {
    let files = (0..FILES)
        .map(|f| {
            (0..RGS_PER_FILE)
                .map(|i| {
                    let r = f * RGS_PER_FILE + i;
                    u64_batch("v", (0..rows).map(|j| start(r) + j).collect())
                })
                .collect()
        })
        .collect::<Vec<Vec<_>>>();
    let schema = files[0][0].schema();
    let mut s = MemSource::new(schema, files);
    s.distinct_counts = true;
    s
}

fn sampled(source: &MemSource) -> FanoutEntry {
    let (_s, store) = temp_store();
    let report = profile(
        source,
        "mem",
        &store,
        &ProfileOptions {
            max_tracked_per_key: Some(14),
            ..ProfileOptions::for_source("parquet")
        },
    )
    .unwrap();
    single(&report).clone()
}

#[test]
fn disjoint_ranges_with_footer_distinct_counts_are_exact_even_when_sampled() {
    // Row group r holds [1000 r, 1000 r + 1000): sorted, disjoint.
    let e = sampled(&ids(1_000, |r| r * 1_000));
    assert_eq!(e.sample.distinct_source, DistinctSource::FooterDisjoint);
    assert!(!e.fanout.sampled);
    assert_eq!(e.fanout.distinct, FILES * RGS_PER_FILE * 1_000);
    assert_eq!(e.fanout.row_groups_per_value, [1.0; 3]);
    assert_eq!(e.fanout.files_per_value, [1.0; 3]);
}

#[test]
fn overlapping_ranges_only_bound_the_estimate() {
    // Row group r holds [500 r, 500 r + 1000): each value in two row groups.
    let source = ids(1_000, |r| r * 500);
    let e = sampled(&source);
    assert_eq!(
        e.sample.distinct_source,
        DistinctSource::HllWithinFooterBounds
    );
    assert!(e.fanout.sampled);
    let truth = (FILES * RGS_PER_FILE - 1) * 500 + 1_000;
    assert!((1_000..=FILES * RGS_PER_FILE * 1_000).contains(&e.fanout.distinct));
    let rse = Hll::relative_standard_error(DEFAULT_HLL_PRECISION);
    let err = (e.fanout.distinct as f64 - truth as f64).abs() / truth as f64;
    assert!(err <= 4.0 * rse, "{} vs {truth}", e.fanout.distinct);
    // Without footer counts the sketch stands alone.
    let mut plain = ids(1_000, |r| r * 500);
    plain.distinct_counts = false;
    assert_eq!(sampled(&plain).sample.distinct_source, DistinctSource::Hll);
}

/// The stated sampling error is calibrated: over 400 independent seeds of a
/// forced sample, each sampled p50 and p95 lies inside its stated DKW band
/// (99.9 %) about as often as stated, the sketch's distinct count lies
/// within two standard errors in at least 90 % of seeds (95 % expected) and
/// within four in all of them, and no sampled max exceeds the true max.
#[test]
fn stated_sampling_error_is_calibrated_over_400_seeds() {
    let mut per: BTreeMap<u64, (BTreeSet<u64>, BTreeSet<u64>)> = BTreeMap::new();
    let mut files = Vec::new();
    for f in 0..4u64 {
        let mut rgs = Vec::new();
        for i in 0..4u64 {
            let r = f * 4 + i;
            let values: Vec<u64> = (0..8_000u64).filter(|&v| occurs(v, r)).collect();
            for &v in &values {
                let e = per.entry(v).or_default();
                e.0.insert(r);
                e.1.insert(f);
            }
            rgs.push(u64_batch("v", values));
        }
        files.push(rgs);
    }
    let mut rgs: Vec<u64> = per.values().map(|e| e.0.len() as u64).collect();
    let mut fs: Vec<u64> = per.values().map(|e| e.1.len() as u64).collect();
    rgs.sort_unstable();
    fs.sort_unstable();
    let truth = rgs.len() as f64;
    let source = MemSource::new(files[0][0].schema(), files);
    let rse = Hll::relative_standard_error(DEFAULT_HLL_PRECISION);
    let (_s, store) = temp_store();
    let seeds = 400u64;
    let (mut checks, mut outside, mut within2) = (0u64, 0u64, 0u64);
    for seed in 1..=seeds {
        let report = profile(
            &source,
            "mem",
            &store,
            &ProfileOptions {
                max_tracked_per_key: Some(448),
                max_tuple_len: 1,
                seed,
                ..ProfileOptions::for_source("mem")
            },
        )
        .unwrap();
        let e = single(&report);
        assert!(e.fanout.sampled);
        let eps = e.sample.quantile_rank_error.unwrap();
        for (i, q) in [0.5, 0.95].into_iter().enumerate() {
            for (got, pop) in [
                (e.fanout.row_groups_per_value[i], &rgs),
                (e.fanout.files_per_value[i], &fs),
            ] {
                let lo = nearest_rank(pop, (q - eps).max(0.0)) as f64;
                let hi = nearest_rank(pop, (q + eps).min(1.0)) as f64;
                checks += 1;
                outside += u64::from(got < lo || got > hi);
            }
        }
        assert!(e.fanout.row_groups_per_value[2] <= *rgs.last().unwrap() as f64);
        assert!(e.fanout.files_per_value[2] <= *fs.last().unwrap() as f64);
        let z = (e.fanout.distinct as f64 - truth) / truth / rse;
        assert!(
            z.abs() <= 4.0,
            "seed {seed}: HLL off by {z:.2} standard errors"
        );
        within2 += u64::from(z.abs() <= 2.0);
    }
    eprintln!(
        "calibration: {checks} quantile checks, {outside} outside the stated band; HLL within 2 sigma in {within2} of {seeds} seeds"
    );
    assert!(
        outside as f64 <= 0.001 * checks as f64 + 1.0,
        "{outside} of {checks}"
    );
    assert!(within2 as f64 >= 0.9 * seeds as f64, "{within2} of {seeds}");
}

/// A unique column's sketch can overshoot the row count (on a 10M-row slice
/// of a public web-analytics benchmark it reported 10,090,761 for 10,000,000
/// rows). The reported distinct is capped at the exact row count and the cap
/// noted; an estimate under the row count is reported as is.
#[test]
fn a_distinct_estimate_above_the_row_count_is_capped_and_noted() {
    let rows = 20_000u64;
    let files: Vec<Vec<arrow::array::RecordBatch>> = (0..4u64)
        .map(|f| {
            (0..4u64)
                .map(|i| {
                    u64_batch(
                        "v",
                        ((f * 4 + i) * 1_250..(f * 4 + i + 1) * 1_250).collect(),
                    )
                })
                .collect()
        })
        .collect();
    let mut source = MemSource::new(files[0][0].schema(), files);
    source.distinct_counts = false;
    let (mut over, mut under) = (None, None);
    for seed in 1..=64u64 {
        let (_s, store) = temp_store();
        let report = profile(
            &source,
            "mem",
            &store,
            &ProfileOptions {
                max_tracked_per_key: Some(14),
                seed,
                ..ProfileOptions::for_source("mem")
            },
        )
        .unwrap();
        let e = single(&report).clone();
        if e.sample.hll_estimate > rows && over.is_none() {
            over = Some((e, bellwether_profile::render(&report, None)));
        } else if e.sample.hll_estimate < rows && under.is_none() {
            under = Some(e);
        }
    }
    let (e, text) = over.expect("some seed overshoots");
    assert_eq!(e.fanout.distinct, rows);
    assert!(e.sample.capped_at_rows);
    assert!(text.contains("capped at the row count"), "{text}");
    let e = under.expect("some seed undershoots");
    assert_eq!(e.fanout.distinct, e.sample.hll_estimate);
    assert!(!e.sample.capped_at_rows);
}
