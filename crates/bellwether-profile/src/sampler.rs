//! Exact per-value incidence counts over a bounded table, sampled by distinct
//! value when the table is full.
//!
//! # What is counted
//!
//! For every distinct key value: the number of distinct row groups and the
//! number of distinct files it occurs in. The profiler visits row groups in
//! one fixed order (files by path, row groups by index), each exactly once,
//! with all row groups of a file contiguous. A value's row groups and files
//! therefore arrive in increasing order, and a counter that increments when
//! the row group (or file) differs from the last one seen counts them
//! exactly, without a set per value.
//!
//! # Sampling: a reservoir keyed by hash priority
//!
//! Each value is identified by a seeded 128-bit hash of its canonical bytes.
//! The table holds at most `high_water` values. When it is full and a new
//! value arrives, the table keeps the `evict_to` values with the smallest
//! hashes and lowers the admission bound to the largest hash it kept. From
//! then on a value is admitted only if its hash is at or below the bound.
//!
//! The bound only ever falls. A value whose hash is at or below the final
//! bound was admissible at its first occurrence and was never evicted, so
//! every occurrence of it was counted: **the counts of every value in the
//! final table are exact**. The final table is exactly the set of distinct
//! values whose hash is at or below the bound, a uniform random sample of
//! distinct values (sampling by value, never by row) at rate
//! `(bound + 1) / 2^128`. Evicting a value and later re-admitting it with
//! partial counts, as a random-replacement reservoir would, cannot happen.
//!
//! Identity is the 128-bit hash, not the bytes. Two distinct values collide
//! with probability about `n^2 / 2^129`, below 2e-21 at a billion distinct
//! values.
//!
//! # Memory (ADR 018)
//!
//! The table never holds more than `high_water` entries, and `high_water`
//! is chosen as the exact capacity of a power-of-two bucket count `B`, so
//! the hash table never grows past `B` buckets of [`SLOT_BYTES`] each. The
//! transient peak is `1.5 * B * SLOT_BYTES`: a resize from `B/2` to `B`
//! buckets holds both tables, and so does the shrink after an eviction.
//! [`peak_bytes_bound`] states it; tests assert the observed table capacity
//! never exceeds `high_water`.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

/// Incidence counter for one distinct value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Track {
    last_rg: u32,
    last_file: u32,
    /// Distinct row groups the value occurs in.
    pub rgs: u32,
    /// Distinct files the value occurs in.
    pub files: u32,
}

impl Track {
    fn first(rg: u32, file: u32) -> Self {
        Self {
            last_rg: rg,
            last_file: file,
            rgs: 1,
            files: 1,
        }
    }

    fn touch(&mut self, rg: u32, file: u32) {
        if self.last_rg != rg {
            self.last_rg = rg;
            self.rgs += 1;
        }
        if self.last_file != file {
            self.last_file = file;
            self.files += 1;
        }
    }
}

/// Bytes per hash-table bucket: one `(u128, Track)` slot plus its control
/// byte.
pub const SLOT_BYTES: usize = std::mem::size_of::<(u128, Track)>() + 1;

/// Smallest table the sampler uses: 16 buckets, 14 entries.
pub const MIN_HIGH_WATER: usize = 14;

/// Keys are already uniform 128-bit hashes; fold them instead of rehashing.
#[derive(Default)]
struct FoldHasher(u64);

impl Hasher for FoldHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = self.0.rotate_left(8) ^ u64::from(b);
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    fn write_u128(&mut self, i: u128) {
        self.0 = (i as u64) ^ ((i >> 64) as u64);
    }
}

type Table = HashMap<u128, Track, BuildHasherDefault<FoldHasher>>;

/// The largest valid high-water mark (`7/8` of a power-of-two bucket count
/// of at least 16) whose [`peak_bytes_bound`] fits in `bytes`, or
/// [`MIN_HIGH_WATER`] if none does.
#[must_use]
pub fn high_water_for_budget(bytes: usize) -> usize {
    let mut buckets = 16usize;
    while buckets
        .checked_mul(2)
        .is_some_and(|b| b <= (1 << 40) && bucket_peak_bytes(b) <= bytes)
    {
        buckets *= 2;
    }
    buckets / 8 * 7
}

/// The largest valid high-water mark not above `entries` (at least
/// [`MIN_HIGH_WATER`]).
#[must_use]
pub fn high_water_at_most(entries: usize) -> usize {
    let mut buckets = 16usize;
    while buckets
        .checked_mul(2)
        .is_some_and(|b| b <= (1 << 40) && b / 8 * 7 <= entries)
    {
        buckets *= 2;
    }
    buckets / 8 * 7
}

fn bucket_peak_bytes(buckets: usize) -> usize {
    (buckets + buckets / 2).saturating_mul(SLOT_BYTES)
}

/// Upper bound on the bytes one sampler's table holds at any instant, for a
/// valid `high_water` (see the module docs).
#[must_use]
pub fn peak_bytes_bound(high_water: usize) -> usize {
    bucket_peak_bytes(high_water / 7 * 8)
}

/// Exact incidence counts for every value in a hash-priority sample.
#[derive(Debug)]
pub struct DistinctSampler {
    table: Table,
    /// Inclusive admission bound on the 128-bit hash.
    max_admit: u128,
    high_water: usize,
    evict_to: usize,
    evictions: u32,
    peak_len: usize,
    peak_capacity: usize,
}

/// What a sampler measured, ready for quantiles.
#[derive(Clone, Debug, PartialEq)]
pub struct SampleOutcome {
    /// Row groups per retained value, ascending.
    pub rgs: Vec<u32>,
    /// Files per retained value, ascending.
    pub files: Vec<u32>,
    /// Whether any value was ever evicted (the table is a sample).
    pub sampled: bool,
    /// Fraction of the hash space admitted: 1.0 when unsampled.
    pub rate: f64,
    /// Inclusive admission bound on the 128-bit hash: the sample is exactly
    /// the distinct values hashed at or below it. `u128::MAX` when unsampled.
    pub bound: u128,
    /// Largest number of entries the table held.
    pub peak_entries: usize,
    /// Largest capacity the table reached.
    pub peak_capacity: usize,
}

impl DistinctSampler {
    /// A sampler holding at most `high_water` values, rounded down to a
    /// valid mark (see [`high_water_at_most`]).
    #[must_use]
    pub fn new(high_water: usize) -> Self {
        let high_water = high_water_at_most(high_water);
        Self {
            table: Table::default(),
            max_admit: u128::MAX,
            high_water,
            evict_to: high_water / 2,
            evictions: 0,
            peak_len: 0,
            peak_capacity: 0,
        }
    }

    /// A sampler that never evicts. Only for tests that need a control which
    /// must break the memory bound.
    #[must_use]
    pub fn unbounded() -> Self {
        Self {
            table: Table::default(),
            max_admit: u128::MAX,
            high_water: usize::MAX,
            evict_to: usize::MAX,
            evictions: 0,
            peak_len: 0,
            peak_capacity: 0,
        }
    }

    /// The high-water mark in effect.
    #[must_use]
    pub fn high_water(&self) -> usize {
        self.high_water
    }

    /// Records one occurrence of the value hashed to `hash` in global row
    /// group `rg` of file `file`. Row groups must arrive in nondecreasing
    /// order with each file's row groups contiguous (module docs).
    pub fn observe(&mut self, hash: u128, rg: u32, file: u32) {
        if hash > self.max_admit {
            return;
        }
        if let Some(track) = self.table.get_mut(&hash) {
            track.touch(rg, file);
            return;
        }
        if self.table.len() >= self.high_water {
            self.evict();
            if hash > self.max_admit {
                return;
            }
        }
        self.table.insert(hash, Track::first(rg, file));
        self.peak_len = self.peak_len.max(self.table.len());
        self.peak_capacity = self.peak_capacity.max(self.table.capacity());
    }

    /// Keeps the `evict_to` smallest hashes and lowers the bound to the
    /// largest of them.
    fn evict(&mut self) {
        let keep = self.evict_to.clamp(1, self.table.len());
        let mut hashes: Vec<u128> = self.table.keys().copied().collect();
        let (_, kth, _) = hashes.select_nth_unstable(keep - 1);
        let bound = *kth;
        drop(hashes);
        self.max_admit = bound;
        self.table.retain(|h, _| *h <= bound);
        self.table.shrink_to_fit();
        self.evictions += 1;
    }

    /// Entries currently held.
    #[must_use]
    pub fn len(&self) -> usize {
        self.table.len()
    }

    /// Whether no value has been retained.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// Largest capacity the table has reached so far.
    #[must_use]
    pub fn peak_capacity(&self) -> usize {
        self.peak_capacity
    }

    /// Finishes the measurement.
    #[must_use]
    pub fn finish(self) -> SampleOutcome {
        let sampled = self.evictions > 0;
        #[allow(clippy::cast_precision_loss)]
        let rate = if sampled {
            (self.max_admit as f64 + 1.0) / 2f64.powi(128)
        } else {
            1.0
        };
        let mut rgs = Vec::with_capacity(self.table.len());
        let mut files = Vec::with_capacity(self.table.len());
        for t in self.table.values() {
            rgs.push(t.rgs);
            files.push(t.files);
        }
        rgs.sort_unstable();
        files.sort_unstable();
        SampleOutcome {
            rgs,
            files,
            sampled,
            rate,
            bound: self.max_admit,
            peak_entries: self.peak_len,
            peak_capacity: self.peak_capacity,
        }
    }
}

/// Half-width, in rank, of a DKW confidence band for quantiles of a uniform
/// random sample of `n` values: with probability at least `1 - alpha`, every
/// sample quantile at rank `q` lies between the population quantiles at
/// ranks `q - eps` and `q + eps`.
#[must_use]
pub fn dkw_epsilon(n: u64, alpha: f64) -> f64 {
    if n == 0 {
        return 1.0;
    }
    #[allow(clippy::cast_precision_loss)]
    let n = n as f64;
    ((2.0 / alpha).ln() / (2.0 * n)).sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};
    use xxhash_rust::xxh3::xxh3_128_with_seed;

    fn h(v: u64) -> u128 {
        xxh3_128_with_seed(&v.to_be_bytes(), 7)
    }

    #[test]
    fn slot_is_33_bytes_so_the_stated_bound_is_honest() {
        assert_eq!(SLOT_BYTES, 33);
        assert_eq!(high_water_at_most(0), 14);
        assert_eq!(high_water_at_most(1000), 896);
        assert_eq!(high_water_at_most(1792), 1792);
        assert_eq!(peak_bytes_bound(14), 24 * 33);
        let hw = high_water_for_budget(10 << 20);
        assert!(peak_bytes_bound(hw) <= 10 << 20);
        assert!(peak_bytes_bound(hw / 7 * 8 * 2 / 8 * 7) > 10 << 20);
    }

    /// Value `v` occurs (three rows each time) in the row groups `rg` with
    /// `rg % 7` in `v % 5 ..= v % 5 + v % 3`; file = `rg / 2`.
    fn occurs(v: u64, rg: u32) -> bool {
        let lo = u32::try_from(v % 5).unwrap();
        let hi = lo + u32::try_from(v % 3).unwrap();
        (lo..=hi).contains(&(rg % 7))
    }

    fn brute(values: u64, rgs: u32) -> BTreeMap<u64, (BTreeSet<u32>, BTreeSet<u32>)> {
        let mut out: BTreeMap<u64, (BTreeSet<u32>, BTreeSet<u32>)> = BTreeMap::new();
        for rg in 0..rgs {
            for v in 0..values {
                if occurs(v, rg) {
                    let e = out.entry(v).or_default();
                    e.0.insert(rg);
                    e.1.insert(rg / 2);
                }
            }
        }
        out
    }

    fn sorted_counts<'a>(sets: impl Iterator<Item = &'a BTreeSet<u32>>) -> Vec<u32> {
        let mut v: Vec<u32> = sets.map(|s| u32::try_from(s.len()).unwrap()).collect();
        v.sort_unstable();
        v
    }

    #[test]
    fn unsampled_counts_are_exact() {
        let mut s = DistinctSampler::new(10_000);
        for rg in 0u32..20 {
            for v in 0u64..500 {
                if occurs(v, rg) {
                    for _ in 0..3 {
                        s.observe(h(v), rg, rg / 2);
                    }
                }
            }
        }
        let want = brute(500, 20);
        let out = s.finish();
        assert!(!out.sampled);
        assert!((out.rate - 1.0).abs() < f64::EPSILON);
        assert_eq!(out.bound, u128::MAX);
        assert_eq!(out.rgs, sorted_counts(want.values().map(|e| &e.0)));
        assert_eq!(out.files, sorted_counts(want.values().map(|e| &e.1)));
    }

    #[test]
    fn sampled_counts_are_exact_for_every_retained_value() {
        // 20 000 values through a table of 896: heavy sampling. The retained
        // set must be exactly {v : h(v) <= bound}, each with exact counts.
        let mut s = DistinctSampler::new(1000);
        assert_eq!(s.high_water(), 896);
        for rg in 0u32..20 {
            for v in 0u64..20_000 {
                if occurs(v, rg) {
                    for _ in 0..3 {
                        s.observe(h(v), rg, rg / 2);
                    }
                }
            }
            assert!(s.peak_capacity() <= s.high_water());
        }
        let out = s.finish();
        assert!(out.sampled);
        assert!(out.peak_entries <= 896);
        assert!(out.peak_capacity <= 896, "{}", out.peak_capacity);
        let want = brute(20_000, 20);
        let kept: Vec<&(BTreeSet<u32>, BTreeSet<u32>)> = want
            .iter()
            .filter(|(v, _)| h(**v) <= out.bound)
            .map(|(_, e)| e)
            .collect();
        assert!(!kept.is_empty() && kept.len() <= 896);
        assert_eq!(out.rgs, sorted_counts(kept.iter().map(|e| &e.0)));
        assert_eq!(out.files, sorted_counts(kept.iter().map(|e| &e.1)));
        #[allow(clippy::cast_precision_loss)]
        let expected_rate = kept.len() as f64 / want.len() as f64;
        assert!((out.rate - expected_rate).abs() < 0.01, "{}", out.rate);
    }

    #[test]
    fn unbounded_sampler_grows_past_any_bound() {
        let mut s = DistinctSampler::unbounded();
        for v in 0u64..5_000 {
            s.observe(h(v), 0, 0);
        }
        let out = s.finish();
        assert!(!out.sampled);
        assert!(out.peak_capacity > 896);
    }

    #[test]
    fn dkw_epsilon_shrinks_with_n() {
        assert!(dkw_epsilon(100, 0.001) > dkw_epsilon(10_000, 0.001));
        assert!((dkw_epsilon(0, 0.01) - 1.0).abs() < f64::EPSILON);
    }
}
