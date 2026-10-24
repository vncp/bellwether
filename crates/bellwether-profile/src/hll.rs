//! A fixed-size HyperLogLog sketch for distinct counts.
//!
//! `2^p` one-byte registers, so the sketch never grows with the data
//! (ADR 018). The estimator is Ertl's improved raw estimator ("New
//! cardinality estimation algorithms for HyperLogLog sketches", 2017,
//! algorithm 6). It is unbiased across the whole range, from a handful of
//! values to billions, without the empirical bias tables of HyperLogLog++,
//! and its relative standard error is about `1.04 / sqrt(2^p)`: 0.81 % at the
//! default `p = 14`.
//!
//! The profiler reports the sketch only when the exact table has been
//! sampled; an unsampled fanout reports the exact distinct count instead.

use bellwether_types::{Error, Result};
use serde::{Deserialize, Serialize};

/// Smallest supported precision.
pub const MIN_PRECISION: u8 = 4;
/// Largest supported precision (256 KiB of registers).
pub const MAX_PRECISION: u8 = 18;

/// A HyperLogLog sketch over 64-bit hashes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hll {
    precision: u8,
    registers: Vec<u8>,
}

impl Hll {
    /// An empty sketch with `2^precision` registers.
    pub fn new(precision: u8) -> Result<Self> {
        if !(MIN_PRECISION..=MAX_PRECISION).contains(&precision) {
            return Err(Error::Other(format!(
                "HyperLogLog precision {precision} outside {MIN_PRECISION}..={MAX_PRECISION}"
            )));
        }
        Ok(Self {
            precision,
            registers: vec![0; 1 << precision],
        })
    }

    /// The precision `p`.
    #[must_use]
    pub fn precision(&self) -> u8 {
        self.precision
    }

    /// Bytes of register state; fixed for the life of the sketch.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.registers.len()
    }

    /// Relative standard error of the estimate at `precision`.
    #[must_use]
    pub fn relative_standard_error(precision: u8) -> f64 {
        1.04 / f64::from(1u32 << precision).sqrt()
    }

    /// Adds one hashed value. The hash must be uniform over 64 bits.
    pub fn insert(&mut self, hash: u64) {
        let p = u32::from(self.precision);
        #[allow(clippy::cast_possible_truncation)]
        let index = (hash >> (64 - p)) as usize;
        let rest = hash << p;
        let q = 64 - p;
        // Rank = position of the first 1 bit in the remaining 64 - p bits,
        // or q + 1 when they are all zero.
        let rank = if rest == 0 {
            q + 1
        } else {
            rest.leading_zeros() + 1
        };
        #[allow(clippy::cast_possible_truncation)]
        let rank = rank.min(q + 1) as u8;
        if self.registers[index] < rank {
            self.registers[index] = rank;
        }
    }

    /// The cardinality estimate.
    #[must_use]
    pub fn estimate(&self) -> f64 {
        let p = u32::from(self.precision);
        let q = 64 - p;
        let m = f64::from(1u32 << p);
        // Histogram of register values, 0..=q+1.
        let mut counts = vec![0u32; (q + 2) as usize];
        for &r in &self.registers {
            counts[usize::from(r)] += 1;
        }
        let top = f64::from(counts[(q + 1) as usize]);
        let mut z = m * tau(1.0 - top / m);
        for k in (1..=q as usize).rev() {
            z = f64::midpoint(z, f64::from(counts[k]));
        }
        z += m * sigma(f64::from(counts[0]) / m);
        let alpha_inf = 1.0 / (2.0 * std::f64::consts::LN_2);
        alpha_inf * m * m / z
    }
}

fn sigma(mut x: f64) -> f64 {
    if (x - 1.0).abs() < f64::EPSILON {
        return f64::INFINITY;
    }
    let mut y = 1.0;
    let mut z = x;
    loop {
        x *= x;
        let previous = z;
        z += x * y;
        y += y;
        if (z - previous).abs() <= 0.0 {
            return z;
        }
    }
}

fn tau(mut x: f64) -> f64 {
    if x == 0.0 || (x - 1.0).abs() < f64::EPSILON {
        return 0.0;
    }
    let mut y = 1.0;
    let mut z = 1.0 - x;
    loop {
        x = x.sqrt();
        let previous = z;
        y *= 0.5;
        z -= (1.0 - x).powi(2) * y;
        if (z - previous).abs() <= 0.0 {
            return z / 3.0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xxhash_rust::xxh3::xxh3_64_with_seed;

    #[allow(clippy::cast_precision_loss)]
    fn estimate_for(n: u64, p: u8, seed: u64) -> f64 {
        let mut h = Hll::new(p).unwrap();
        for i in 0..n {
            h.insert(xxh3_64_with_seed(&i.to_be_bytes(), seed));
        }
        h.estimate()
    }

    #[test]
    #[allow(clippy::cast_precision_loss)]
    fn estimates_are_within_four_standard_errors_across_the_range() {
        let p = 12;
        let rse = Hll::relative_standard_error(p);
        for n in [1u64, 10, 100, 1_000, 3_000, 10_000, 30_000, 200_000] {
            for seed in 0..3 {
                let est = estimate_for(n, p, seed);
                let err = (est - n as f64).abs() / n as f64;
                assert!(
                    err <= 4.0 * rse + 1.0 / n as f64,
                    "n={n} seed={seed} est={est} err={err} rse={rse}"
                );
            }
        }
    }

    #[test]
    fn empty_sketch_estimates_zero_and_duplicates_do_not_count() {
        let mut h = Hll::new(14).unwrap();
        assert!(h.estimate().abs() < 1e-9);
        for _ in 0..1000 {
            h.insert(xxh3_64_with_seed(b"same", 0));
        }
        assert!((h.estimate() - 1.0).abs() < 0.01, "{}", h.estimate());
        assert_eq!(h.bytes(), 1 << 14);
    }

    #[test]
    fn precision_is_validated() {
        assert!(Hll::new(3).is_err());
        assert!(Hll::new(19).is_err());
        assert!(Hll::new(4).is_ok());
    }
}
