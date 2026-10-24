//! Nearest-rank quantiles, the definition every number in this crate uses.
//!
//! For `n` sorted values the q-quantile is the value at zero-based index
//! `ceil(q * n) - 1`, clamped to `[0, n - 1]`. This is the definition the
//! benchmark's ground truth reports (`expected.json`, `quantiles`), so a p50
//! here and a p50 there mean the same value.

/// Zero-based index of the nearest-rank q-quantile among `n` sorted values.
#[must_use]
pub fn nearest_rank_index(q: f64, n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    let rank = (q * n as f64).ceil() as usize;
    rank.clamp(1, n) - 1
}

/// `[p50, p95, max]` of an ascending slice as floats (the shape of
/// `Fanout::files_per_value`), or zeros when empty.
#[must_use]
pub fn p50_p95_max(sorted: &[u32]) -> [f64; 3] {
    if sorted.is_empty() {
        return [0.0; 3];
    }
    let at = |q: f64| f64::from(sorted[nearest_rank_index(q, sorted.len())]);
    [at(0.5), at(0.95), f64::from(sorted[sorted.len() - 1])]
}

/// `[p50, p95, max]` of an ascending slice of `u64`, or zeros when empty.
#[must_use]
pub fn p50_p95_max_u64(sorted: &[u64]) -> [u64; 3] {
    if sorted.is_empty() {
        return [0; 3];
    }
    let at = |q: f64| sorted[nearest_rank_index(q, sorted.len())];
    [at(0.5), at(0.95), sorted[sorted.len() - 1]]
}

/// Median of an unsorted slice of finite floats by nearest rank, or `None`
/// when empty.
#[must_use]
pub fn median_f64(values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(f64::total_cmp);
    Some(values[nearest_rank_index(0.5, values.len())])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_rank_matches_the_ground_truth_definition() {
        let v: Vec<u32> = (1..=100).collect();
        assert_eq!(p50_p95_max(&v), [50.0, 95.0, 100.0]);
        assert_eq!(nearest_rank_index(0.0, 10), 0);
        assert_eq!(nearest_rank_index(1.0, 10), 9);
        // ceil(0.5 * 3) - 1 = 1; ceil(0.95 * 3) - 1 = 2.
        assert_eq!(p50_p95_max(&[1, 2, 3]), [2.0, 3.0, 3.0]);
        // ceil(0.5 * 4) - 1 = 1: the lower median, never an interpolation.
        assert_eq!(p50_p95_max(&[1, 2, 3, 4]), [2.0, 4.0, 4.0]);
        assert_eq!(p50_p95_max(&[]), [0.0; 3]);
        assert_eq!(p50_p95_max_u64(&[5]), [5, 5, 5]);
        assert_eq!(median_f64(&mut [3.0, 1.0, 2.0]), Some(2.0));
        assert_eq!(median_f64(&mut []), None);
    }
}
