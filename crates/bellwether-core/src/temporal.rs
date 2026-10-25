//! Per-key version chains for `latest` and `asof` (invariant 5, ADR 014).
//!
//! A [`VersionChain`] lists every distinct `(ts, RowGroupId)` at which a key
//! has a version, sorted ascending, together with `max_ts`, the greatest
//! timestamp of the key in that chain. Rows whose timestamp is null are
//! indexed for membership (the key is not null) but never enter the chain:
//! null timestamps rank last and never win `latest` or `asof`. A key whose
//! every version has a null timestamp therefore has no `latest`.
//!
//! Index-wide results. One segment covers only some row groups, so a
//! segment's chain gives that segment's newest version. [`latest`] and
//! [`asof`] fold every segment of a temporal key spec with
//! [`VersionHit::combine`] and return the index-wide answer; consumers call
//! these and never fold per-segment results themselves.
//!
//! Stale-version rejection. A caller that has pruned row groups for some
//! other predicate must still compare each candidate row against the
//! index-wide `max_ts` of its key ([`VersionHit::ts`] from [`latest`]) before
//! ranking; a row older than that is superseded even if the superseding row
//! group was pruned away. [`is_stale`] is that check.
//!
//! Timestamps are `i64` in the dataset column's unit; see
//! [`crate::encoding::ts_value_as`].

use std::collections::BTreeMap;

use bellwether_types::{Error, Result, RowGroupId};
use roaring::RoaringBitmap;

use crate::segment::{ReadAt, SegmentReader};

/// Every version of one key: distinct `(ts, row group)` pairs, ascending.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VersionChain {
    versions: Vec<(i64, RowGroupId)>,
}

/// The winning version of a key: its timestamp and every row group that holds
/// a version at exactly that timestamp (ties are kept, never broken, so no
/// row group holding the winner is dropped).
#[derive(Clone, Debug, PartialEq)]
pub struct VersionHit {
    /// Winning timestamp. From [`latest`], this is the key's index-wide
    /// `max_ts`.
    pub ts: i64,
    /// Row groups holding a version at `ts`.
    pub row_groups: RoaringBitmap,
}

impl VersionHit {
    /// Folds another segment's hit for the same key into this one: the later
    /// timestamp wins, and equal timestamps union their row groups.
    pub fn combine(&mut self, other: &Self) {
        match other.ts.cmp(&self.ts) {
            std::cmp::Ordering::Greater => self.clone_from(other),
            std::cmp::Ordering::Equal => self.row_groups |= &other.row_groups,
            std::cmp::Ordering::Less => {}
        }
    }

    /// The smallest row group holding the winner, for callers that want one.
    #[must_use]
    pub fn first(&self) -> Option<RowGroupId> {
        self.row_groups.min().map(RowGroupId)
    }
}

/// Whether a row with timestamp `row_ts` is superseded by `hit`. A null
/// timestamp is always stale (ADR 014).
#[must_use]
pub fn is_stale(hit: &VersionHit, row_ts: Option<i64>) -> bool {
    row_ts.is_none_or(|t| t < hit.ts)
}

impl VersionChain {
    /// An empty chain.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Builds a chain from arbitrary pairs: sorts and deduplicates once,
    /// O(n log n).
    #[must_use]
    pub fn from_versions(mut versions: Vec<(i64, RowGroupId)>) -> Self {
        versions.sort_unstable();
        versions.dedup();
        Self { versions }
    }

    /// Merges another chain of the same key in linear time.
    pub fn merge(&mut self, other: &Self) {
        let mut out = Vec::with_capacity(self.versions.len() + other.versions.len());
        let (mut i, mut j) = (0, 0);
        let (a, b) = (&self.versions, &other.versions);
        while i < a.len() && j < b.len() {
            match a[i].cmp(&b[j]) {
                std::cmp::Ordering::Less => {
                    out.push(a[i]);
                    i += 1;
                }
                std::cmp::Ordering::Greater => {
                    out.push(b[j]);
                    j += 1;
                }
                std::cmp::Ordering::Equal => {
                    out.push(a[i]);
                    i += 1;
                    j += 1;
                }
            }
        }
        out.extend_from_slice(&a[i..]);
        out.extend_from_slice(&b[j..]);
        self.versions = out;
    }

    /// The versions, ascending by `(ts, row group)`.
    #[must_use]
    pub fn versions(&self) -> &[(i64, RowGroupId)] {
        &self.versions
    }

    /// Whether the chain has no non-null version.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.versions.is_empty()
    }

    /// The greatest non-null timestamp, if any.
    #[must_use]
    pub fn max_ts(&self) -> Option<i64> {
        self.versions.last().map(|v| v.0)
    }

    fn hit_at(&self, end: usize) -> Option<VersionHit> {
        let ts = self.versions[..end].last()?.0;
        let start = self.versions[..end].partition_point(|v| v.0 < ts);
        let row_groups = self.versions[start..end].iter().map(|v| v.1 .0).collect();
        Some(VersionHit { ts, row_groups })
    }

    /// The newest version in this chain.
    #[must_use]
    pub fn latest(&self) -> Option<VersionHit> {
        self.hit_at(self.versions.len())
    }

    /// The newest version in this chain with timestamp `<= ts`.
    #[must_use]
    pub fn asof(&self, ts: i64) -> Option<VersionHit> {
        self.hit_at(self.versions.partition_point(|v| v.0 <= ts))
    }

    /// Serialized form: `count u32`, `has_max u8`, `max_ts i64`, then
    /// `count` x (`ts i64`, `rg u32`), little-endian.
    pub fn write_to(&self, out: &mut Vec<u8>) -> Result<()> {
        let n = u32::try_from(self.versions.len())
            .map_err(|_| Error::Other("version chain longer than u32::MAX".into()))?;
        out.extend_from_slice(&n.to_le_bytes());
        out.push(u8::from(self.max_ts().is_some()));
        out.extend_from_slice(&self.max_ts().unwrap_or(0).to_le_bytes());
        for (ts, rg) in &self.versions {
            out.extend_from_slice(&ts.to_le_bytes());
            out.extend_from_slice(&rg.0.to_le_bytes());
        }
        Ok(())
    }

    /// Parses the form written by [`VersionChain::write_to`].
    pub fn read_from(bytes: &[u8]) -> Result<Self> {
        let bad = || Error::Other("corrupt version chain".into());
        if bytes.len() < 13 {
            return Err(bad());
        }
        let n = u32::from_le_bytes(bytes[0..4].try_into().map_err(|_| bad())?) as usize;
        let has_max = bytes[4] == 1;
        let max_ts = i64::from_le_bytes(bytes[5..13].try_into().map_err(|_| bad())?);
        let body = &bytes[13..];
        if body.len() != n * 12 {
            return Err(bad());
        }
        let mut versions = Vec::with_capacity(n);
        for c in body.as_chunks::<12>().0 {
            let ts = i64::from_le_bytes(c[0..8].try_into().map_err(|_| bad())?);
            let rg = u32::from_le_bytes(c[8..12].try_into().map_err(|_| bad())?);
            versions.push((ts, RowGroupId(rg)));
        }
        let chain = Self { versions };
        if !chain.versions.windows(2).all(|w| w[0] < w[1])
            || chain.max_ts() != has_max.then_some(max_ts)
        {
            return Err(bad());
        }
        Ok(chain)
    }
}

/// Accumulates versions of one key during a build without quadratic
/// insertion: appends, skips an immediate repeat, and sorts and deduplicates
/// whenever the buffer doubles, so memory stays within about twice the
/// distinct versions.
#[derive(Clone, Debug, Default)]
pub(crate) struct VersionBuffer {
    versions: Vec<(i64, RowGroupId)>,
    compacted: usize,
}

impl VersionBuffer {
    pub(crate) fn push(&mut self, ts: Option<i64>, rg: RowGroupId) {
        let Some(ts) = ts else {
            return;
        };
        if self.versions.last() == Some(&(ts, rg)) {
            return;
        }
        self.versions.push((ts, rg));
        if self.versions.len() > 2 * self.compacted.max(32) {
            self.versions.sort_unstable();
            self.versions.dedup();
            self.compacted = self.versions.len();
        }
    }

    pub(crate) fn finish(self) -> VersionChain {
        VersionChain::from_versions(self.versions)
    }
}

/// Every segment must index the same key spec under the same recorded key
/// and timestamp types; otherwise the same key could be encoded two ways and
/// the fold would split it silently.
fn same_spec<R: ReadAt>(segments: &[SegmentReader<R>]) -> Result<()> {
    let Some(first) = segments.first() else {
        return Ok(());
    };
    let h0 = first.header();
    for s in &segments[1..] {
        let h = s.header();
        if h.key_spec != h0.key_spec {
            return Err(Error::KeySpec(format!(
                "segments of different key specs: {} and {}",
                h0.key_spec.name(),
                h.key_spec.name()
            )));
        }
        if h.key_types != h0.key_types || h.ts_type != h0.ts_type {
            return Err(Error::KeySpec(format!(
                "segments of {} recorded under different types",
                h0.key_spec.name()
            )));
        }
    }
    Ok(())
}

fn fold<R: ReadAt>(
    segments: &[SegmentReader<R>],
    mut per_segment: impl FnMut(&SegmentReader<R>) -> Result<BTreeMap<Vec<u8>, VersionHit>>,
) -> Result<BTreeMap<Vec<u8>, VersionHit>> {
    same_spec(segments)?;
    let mut out: BTreeMap<Vec<u8>, VersionHit> = BTreeMap::new();
    for s in segments {
        for (k, hit) in per_segment(s)? {
            match out.get_mut(&k) {
                Some(acc) => acc.combine(&hit),
                None => {
                    out.insert(k, hit);
                }
            }
        }
    }
    Ok(out)
}

/// The index-wide newest version of each key across every segment of one
/// temporal key spec: the key's `max_ts` and every row group holding a
/// version at it. Keys with no non-null version anywhere are absent.
///
/// Pass every segment of the key spec, regardless of any other predicate the
/// caller will apply, so stale rows can be rejected (invariant 5). Holds the
/// hits for the keys asked plus one segment's lookup state at a time.
pub fn latest<R: ReadAt, K: AsRef<[u8]>>(
    segments: &[SegmentReader<R>],
    keys: &[K],
) -> Result<BTreeMap<Vec<u8>, VersionHit>> {
    fold(segments, |s| s.latest(keys))
}

/// The index-wide newest version of each key with timestamp `<= ts`, across
/// every segment of one temporal key spec. `ts` is in the dataset column's
/// unit.
pub fn asof<R: ReadAt, K: AsRef<[u8]>>(
    segments: &[SegmentReader<R>],
    keys: &[K],
    ts: i64,
) -> Result<BTreeMap<Vec<u8>, VersionHit>> {
    fold(segments, |s| s.asof(keys, ts))
}

/// Every version of each key across every segment of one temporal key spec:
/// the segments' chains merged. Keys absent from every segment are absent.
/// Holds the chains of the keys asked plus one segment's lookup state at a
/// time; for lookback windows ([`window`]), which need every version in a
/// range, not one winner.
pub fn chains<R: ReadAt, K: AsRef<[u8]>>(
    segments: &[SegmentReader<R>],
    keys: &[K],
) -> Result<BTreeMap<Vec<u8>, VersionChain>> {
    same_spec(segments)?;
    let mut out: BTreeMap<Vec<u8>, VersionChain> = BTreeMap::new();
    for s in segments {
        s.require_temporal()?;
        for (k, v) in s.get(keys)? {
            out.entry(k).or_default().merge(&v.chain);
        }
    }
    Ok(out)
}

/// One key's versions in a lookback window: the window's inclusive bounds
/// and every row group holding a version inside it.
#[derive(Clone, Debug, PartialEq)]
pub struct WindowHit {
    /// Inclusive lower bound, in the timestamp column's unit.
    pub lo: i64,
    /// Inclusive upper bound: the asof instant, or the key's newest version.
    pub hi: i64,
    /// Row groups holding a version with `lo <= ts <= hi`.
    pub row_groups: RoaringBitmap,
}

/// The lookback window of one key over `chains` (every chain of the key,
/// for example each zero variant's), counting only versions in `live` row
/// groups. With `asof`, the window is `[asof − lookback, asof]`; without,
/// `[newest − lookback, newest]` where `newest` is the key's newest live
/// version. The lower bound never goes below `floor`, the smallest value
/// the timestamp column's type holds. `None` when no live version falls in
/// the window (or, without `asof`, when the key has no live version).
#[must_use]
pub fn window<'a>(
    chains: impl Iterator<Item = &'a VersionChain> + Clone,
    live: &RoaringBitmap,
    asof: Option<i64>,
    lookback: i64,
    floor: i64,
) -> Option<WindowHit> {
    let in_live = |(_, rg): &&(i64, RowGroupId)| live.contains(rg.0);
    let hi = match asof {
        Some(at) => at,
        None => chains
            .clone()
            .flat_map(|c| c.versions().iter())
            .filter(in_live)
            .map(|(ts, _)| *ts)
            .max()?,
    };
    let lo = hi.saturating_sub(lookback).max(floor);
    let row_groups: RoaringBitmap = chains
        .flat_map(|c| c.versions().iter())
        .filter(in_live)
        .filter(|(ts, _)| (lo..=hi).contains(ts))
        .map(|(_, rg)| rg.0)
        .collect();
    (!row_groups.is_empty()).then_some(WindowHit { lo, hi, row_groups })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn chain(v: &[(Option<i64>, u32)]) -> VersionChain {
        let mut b = VersionBuffer::default();
        for (ts, rg) in v {
            b.push(*ts, RowGroupId(*rg));
        }
        b.finish()
    }

    #[test]
    fn latest_and_asof_keep_ties_and_ignore_nulls() {
        let c = chain(&[(Some(5), 2), (Some(1), 1), (Some(5), 7), (None, 9)]);
        let l = c.latest().unwrap();
        assert_eq!(l.ts, 5);
        assert_eq!(l.row_groups.iter().collect::<Vec<_>>(), vec![2, 7]);
        assert_eq!(c.asof(4).unwrap().ts, 1);
        assert!(c.asof(0).is_none());
        assert!(is_stale(&l, Some(1)));
        assert!(is_stale(&l, None));
        assert!(!is_stale(&l, Some(5)));
        let mut buf = Vec::new();
        c.write_to(&mut buf).unwrap();
        assert_eq!(VersionChain::read_from(&buf).unwrap(), c);
    }

    #[test]
    fn only_null_timestamps_never_win() {
        let c = chain(&[(None, 1)]);
        assert!(c.latest().is_none());
        assert!(c.max_ts().is_none());
    }

    #[test]
    fn buffer_handles_reverse_order_and_duplicates() {
        let mut b = VersionBuffer::default();
        for i in (0..10_000i64).rev() {
            b.push(Some(i % 100), RowGroupId(u32::try_from(i % 7).unwrap()));
            b.push(Some(i % 100), RowGroupId(u32::try_from(i % 7).unwrap()));
        }
        assert!(b.versions.len() <= 2 * 700 + 64);
        let c = b.finish();
        assert_eq!(c.versions().len(), 700);
        assert!(c.versions().windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn combine_prefers_later_and_unions_ties() {
        let mut a = VersionHit {
            ts: 3,
            row_groups: [1u32].into_iter().collect(),
        };
        a.combine(&VersionHit {
            ts: 3,
            row_groups: [4u32].into_iter().collect(),
        });
        assert_eq!(a.row_groups.len(), 2);
        a.combine(&VersionHit {
            ts: 9,
            row_groups: [8u32].into_iter().collect(),
        });
        assert_eq!(a.ts, 9);
        assert_eq!(a.first(), Some(RowGroupId(8)));
        a.combine(&VersionHit {
            ts: 1,
            row_groups: [0u32].into_iter().collect(),
        });
        assert_eq!(a.ts, 9);
    }

    #[test]
    fn a_window_holds_every_live_version_in_range() {
        let a = VersionChain::from_versions(vec![
            (1, RowGroupId(0)),
            (5, RowGroupId(1)),
            (7, RowGroupId(2)),
            (9, RowGroupId(3)),
        ]);
        let b = VersionChain::from_versions(vec![(6, RowGroupId(4)), (10, RowGroupId(5))]);
        let live: RoaringBitmap = [0u32, 1, 2, 3, 4].into_iter().collect();
        let rgs = |w: WindowHit| (w.lo, w.hi, w.row_groups.iter().collect::<Vec<_>>());
        // latest: newest live version is 9 (10 lives in a retired row group)
        let w = window([&a, &b].into_iter(), &live, None, 3, i64::MIN).unwrap();
        assert_eq!(rgs(w), (6, 9, vec![2, 3, 4]));
        // asof 6, lookback 1: versions 5 and 6
        let w = window([&a, &b].into_iter(), &live, Some(6), 1, i64::MIN).unwrap();
        assert_eq!(rgs(w), (5, 6, vec![1, 4]));
        // the floor clamps the lower bound
        let w = window([&a].into_iter(), &live, Some(2), 10, 0).unwrap();
        assert_eq!(rgs(w), (0, 2, vec![0]));
        // nothing in range, or nothing live
        assert!(window([&a].into_iter(), &live, Some(4), 2, i64::MIN).is_none());
        assert!(window([&b].into_iter(), &RoaringBitmap::new(), None, 9, i64::MIN).is_none());
    }
}
