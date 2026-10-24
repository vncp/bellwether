//! Exact quantiles and histograms.
//!
//! Every quantile in this crate is **nearest-rank**: for `n` sorted values the
//! q-quantile is the value at zero-based index `ceil(q * n) - 1` (index 0 when
//! `q * n` rounds to 0). This is the definition the workload generator targets
//! and the ground truth reports, so a number in `workload.json`,
//! `expected.json`, and `results.json` means the same thing everywhere.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Zero-based index of the nearest-rank q-quantile among `n` sorted values.
#[must_use]
pub fn nearest_rank_index(q: f64, n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    let rank = (q * n as f64).ceil() as usize;
    rank.clamp(1, n) - 1
}

/// Nearest-rank quantile of an already sorted slice. `None` when empty.
#[must_use]
pub fn quantile_sorted<T: Copy>(sorted: &[T], q: f64) -> Option<T> {
    if sorted.is_empty() {
        None
    } else {
        Some(sorted[nearest_rank_index(q, sorted.len())])
    }
}

/// `[p50, p95, max]` of a sorted slice, or zeros when empty.
#[must_use]
pub fn p50_p95_max_f64(sorted: &[f64]) -> [f64; 3] {
    [
        quantile_sorted(sorted, 0.5).unwrap_or(0.0),
        quantile_sorted(sorted, 0.95).unwrap_or(0.0),
        sorted.last().copied().unwrap_or(0.0),
    ]
}

/// An exact distribution of small non-negative integers, stored as
/// `value -> how many observations had it`. Memory is proportional to the
/// number of distinct values observed, not the number of observations.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CountHistogram {
    counts: BTreeMap<u64, u64>,
    total: u64,
}

impl CountHistogram {
    /// Records one observation.
    pub fn add(&mut self, value: u64) {
        *self.counts.entry(value).or_insert(0) += 1;
        self.total += 1;
    }

    /// Number of observations.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.total
    }

    /// Nearest-rank quantile.
    #[must_use]
    pub fn quantile(&self, q: f64) -> u64 {
        if self.total == 0 {
            return 0;
        }
        let target = nearest_rank_index(q, self.total as usize) as u64;
        let mut seen = 0u64;
        for (&value, &count) in &self.counts {
            seen += count;
            if seen > target {
                return value;
            }
        }
        self.max()
    }

    /// Largest observation, or 0.
    #[must_use]
    pub fn max(&self) -> u64 {
        self.counts.keys().next_back().copied().unwrap_or(0)
    }

    /// `[p50, p95, max]` as floats, the shape of [`bellwether_types::Fanout`].
    #[must_use]
    pub fn p50_p95_max(&self) -> [f64; 3] {
        [
            self.quantile(0.5) as f64,
            self.quantile(0.95) as f64,
            self.max() as f64,
        ]
    }

    /// Observations equal to `value`.
    #[must_use]
    pub fn count_of(&self, value: u64) -> u64 {
        self.counts.get(&value).copied().unwrap_or(0)
    }

    /// Power-of-two buckets: `[1,1], [2,3], [4,7], ...` (and `[0,0]` when
    /// zeros were observed), each with its observation count. Empty buckets
    /// between the smallest and largest are included so a chart is to scale.
    #[must_use]
    pub fn log2_buckets(&self) -> Vec<Bucket> {
        let mut out = Vec::new();
        if self.total == 0 {
            return out;
        }
        let zeros = self.count_of(0);
        if zeros > 0 {
            out.push(Bucket {
                lo: 0,
                hi: 0,
                values: zeros,
            });
        }
        let max = self.max();
        if max == 0 {
            return out;
        }
        let mut lo = 1u64;
        while lo <= max {
            let hi = lo.saturating_mul(2) - 1;
            let values = self.counts.range(lo..=hi).map(|(_, c)| *c).sum();
            out.push(Bucket { lo, hi, values });
            lo = lo.saturating_mul(2);
        }
        out
    }
}

/// One histogram bucket, inclusive bounds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bucket {
    /// Inclusive lower bound.
    pub lo: u64,
    /// Inclusive upper bound.
    pub hi: u64,
    /// Number of values whose measurement falls in `[lo, hi]`.
    pub values: u64,
}

/// SplitMix64 finalizer: a fast, well-mixed, stateless 64-bit hash.
#[must_use]
pub fn mix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Mixes several words into one seed.
#[must_use]
pub fn mix_all(words: &[u64]) -> u64 {
    words
        .iter()
        .fold(0x6265_6c6c_7765_7468_u64, |acc, &w| mix64(acc ^ mix64(w)))
}

/// A ChaCha12 generator keyed by `words`. The 32-byte key is spelled out
/// here (four SplitMix64 outputs, little-endian) rather than left to
/// `SeedableRng::seed_from_u64`, so the stream depends only on ChaCha12
/// itself and this function.
#[must_use]
pub fn chacha(words: &[u64]) -> rand_chacha::ChaCha12Rng {
    use rand::SeedableRng as _;
    let base = mix_all(words);
    let mut key = [0u8; 32];
    for (i, chunk) in key.as_chunks_mut::<8>().0.iter_mut().enumerate() {
        chunk.copy_from_slice(&mix64(base ^ (i as u64 + 1)).to_le_bytes());
    }
    rand_chacha::ChaCha12Rng::from_seed(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_rank_matches_definition() {
        let v: Vec<u64> = (1..=100).collect();
        assert_eq!(quantile_sorted(&v, 0.5), Some(50));
        assert_eq!(quantile_sorted(&v, 0.95), Some(95));
        assert_eq!(quantile_sorted(&v, 1.0), Some(100));
        assert_eq!(quantile_sorted(&v, 0.0), Some(1));
    }

    #[test]
    fn histogram_quantiles_match_sorted_quantiles() {
        let values = [5u64, 1, 1, 3, 9, 9, 9, 2, 7, 4, 4];
        let mut h = CountHistogram::default();
        for v in values {
            h.add(v);
        }
        let mut sorted = values.to_vec();
        sorted.sort_unstable();
        for q in [0.1, 0.5, 0.9, 0.95, 1.0] {
            assert_eq!(Some(h.quantile(q)), quantile_sorted(&sorted, q));
        }
        let buckets = h.log2_buckets();
        assert_eq!(buckets.iter().map(|b| b.values).sum::<u64>(), 11);
        assert_eq!(buckets.last().unwrap().hi, 15);
    }
}
