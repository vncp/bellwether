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

/// Per-key, per-row-group `[min_ts, max_ts]` bounds stored in a `bounds`
/// sidecar segment (`KeySpec::Bounds`), sorted ascending by `RowGroupId`.
///
/// Rows with a null timestamp do not enter the bounds table (though their row
/// group is still in the key's bitmap).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KeyBounds {
    ranges: Vec<(RowGroupId, i64, i64)>,
}

impl KeyBounds {
    /// Empty bounds.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Builds bounds from `(rg, min, max)` entries, merging duplicate row
    /// groups by hull `[min1.min(min2), max1.max(max2)]`.
    #[must_use]
    pub fn from_ranges(mut ranges: Vec<(RowGroupId, i64, i64)>) -> Self {
        ranges.sort_unstable_by_key(|r| r.0);
        let mut out: Vec<(RowGroupId, i64, i64)> = Vec::with_capacity(ranges.len());
        for (rg, lo, hi) in ranges {
            let (lo, hi) = (lo.min(hi), lo.max(hi));
            if let Some(last) = out.last_mut() {
                if last.0 == rg {
                    last.1 = last.1.min(lo);
                    last.2 = last.2.max(hi);
                    continue;
                }
            }
            out.push((rg, lo, hi));
        }
        Self { ranges: out }
    }

    /// Records a timestamp `ts` in row group `rg`.
    pub fn record(&mut self, rg: RowGroupId, ts: Option<i64>) {
        let Some(ts) = ts else {
            return;
        };
        match self.ranges.binary_search_by_key(&rg, |r| r.0) {
            Ok(i) => {
                self.ranges[i].1 = self.ranges[i].1.min(ts);
                self.ranges[i].2 = self.ranges[i].2.max(ts);
            }
            Err(i) => {
                self.ranges.insert(i, (rg, ts, ts));
            }
        }
    }

    /// Merges another `KeyBounds` of the same key.
    pub fn merge(&mut self, other: &Self) {
        let mut combined = Vec::with_capacity(self.ranges.len() + other.ranges.len());
        combined.extend_from_slice(&self.ranges);
        combined.extend_from_slice(&other.ranges);
        *self = Self::from_ranges(combined);
    }

    /// Stored `(RowGroupId, min_ts, max_ts)` entries, ascending by row group.
    #[must_use]
    pub fn ranges(&self) -> &[(RowGroupId, i64, i64)] {
        &self.ranges
    }

    /// Whether no non-null timestamp was recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// Row groups whose `[min_ts, max_ts]` for this key overlaps `[lo, hi]`.
    #[must_use]
    pub fn overlapping(&self, lo: i64, hi: i64) -> RoaringBitmap {
        if lo > hi {
            return RoaringBitmap::new();
        }
        self.ranges
            .iter()
            .filter(|&&(_, min, max)| max >= lo && min <= hi)
            .map(|&(rg, _, _)| rg.0)
            .collect()
    }

    /// Serialized form: `count u32`, then `count` x (`rg u32`, `min_ts i64`,
    /// `max_ts i64`), little-endian.
    pub fn write_to(&self, out: &mut Vec<u8>) -> Result<()> {
        let n = u32::try_from(self.ranges.len())
            .map_err(|_| Error::Other("key bounds longer than u32::MAX".into()))?;
        out.extend_from_slice(&n.to_le_bytes());
        for &(rg, min, max) in &self.ranges {
            out.extend_from_slice(&rg.0.to_le_bytes());
            out.extend_from_slice(&min.to_le_bytes());
            out.extend_from_slice(&max.to_le_bytes());
        }
        Ok(())
    }

    /// Parses the form written by [`KeyBounds::write_to`].
    pub fn read_from(bytes: &[u8]) -> Result<Self> {
        let bad = || Error::Other("corrupt key bounds".into());
        if bytes.len() < 4 {
            return Err(bad());
        }
        let n = u32::from_le_bytes(bytes[0..4].try_into().map_err(|_| bad())?) as usize;
        let body = &bytes[4..];
        if body.len() != n * 20 {
            return Err(bad());
        }
        let mut ranges = Vec::with_capacity(n);
        for c in body.as_chunks::<20>().0 {
            let rg = u32::from_le_bytes(c[0..4].try_into().map_err(|_| bad())?);
            let min = i64::from_le_bytes(c[4..12].try_into().map_err(|_| bad())?);
            let max = i64::from_le_bytes(c[12..20].try_into().map_err(|_| bad())?);
            if min > max {
                return Err(bad());
            }
            ranges.push((RowGroupId(rg), min, max));
        }
        if !ranges.windows(2).all(|w| w[0].0 < w[1].0) {
            return Err(bad());
        }
        Ok(Self { ranges })
    }
}

/// Bitemporal version chain per key (`temporal2:key@valid,known`), storing
/// distinct `(valid_ts, known_ts, RowGroupId)` triples sorted ascending.
///
/// Nulls in either clock never enter the chain (ADR 014).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BitemporalChain {
    versions: Vec<(i64, i64, RowGroupId)>,
}

impl BitemporalChain {
    /// An empty bitemporal chain.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Builds a chain from arbitrary `(valid, known, rg)` triples: sorts and
    /// deduplicates once.
    #[must_use]
    pub fn from_versions(mut versions: Vec<(i64, i64, RowGroupId)>) -> Self {
        versions.sort_unstable();
        versions.dedup();
        Self { versions }
    }

    /// Merges another bitemporal chain of the same key in linear time.
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

    /// Stored `(valid, known, RowGroupId)` triples, ascending.
    #[must_use]
    pub fn versions(&self) -> &[(i64, i64, RowGroupId)] {
        &self.versions
    }

    /// Whether the chain has no version with non-null valid and known times.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.versions.is_empty()
    }

    /// Newest version by valid time among entries with `valid <= max_valid`
    /// (when `Some`) and `known <= max_known` (when `Some`). Ties on the
    /// winning valid time (any `known <= max_known`) union their row groups.
    #[must_use]
    pub fn query(&self, max_valid: Option<i64>, max_known: Option<i64>) -> Option<VersionHit> {
        let end = match max_valid {
            Some(v) => self.versions.partition_point(|t| t.0 <= v),
            None => self.versions.len(),
        };
        let slice = &self.versions[..end];
        let mut best: Option<VersionHit> = None;
        for &(v, k, rg) in slice.iter().rev() {
            if max_known.is_some_and(|mk| k > mk) {
                continue;
            }
            match &mut best {
                None => {
                    let mut row_groups = RoaringBitmap::new();
                    row_groups.insert(rg.0);
                    best = Some(VersionHit { ts: v, row_groups });
                }
                Some(hit) if v == hit.ts => {
                    hit.row_groups.insert(rg.0);
                }
                Some(_) => break,
            }
        }
        best
    }

    /// Serialized form: `count u32`, then `count` x (`valid i64`, `known i64`,
    /// `rg u32`), little-endian.
    pub fn write_to(&self, out: &mut Vec<u8>) -> Result<()> {
        let n = u32::try_from(self.versions.len())
            .map_err(|_| Error::Other("bitemporal chain longer than u32::MAX".into()))?;
        out.extend_from_slice(&n.to_le_bytes());
        for &(v, k, rg) in &self.versions {
            out.extend_from_slice(&v.to_le_bytes());
            out.extend_from_slice(&k.to_le_bytes());
            out.extend_from_slice(&rg.0.to_le_bytes());
        }
        Ok(())
    }

    /// Parses the form written by [`BitemporalChain::write_to`].
    pub fn read_from(bytes: &[u8]) -> Result<Self> {
        let bad = || Error::Other("corrupt bitemporal chain".into());
        if bytes.len() < 4 {
            return Err(bad());
        }
        let n = u32::from_le_bytes(bytes[0..4].try_into().map_err(|_| bad())?) as usize;
        let body = &bytes[4..];
        if body.len() != n * 20 {
            return Err(bad());
        }
        let mut versions = Vec::with_capacity(n);
        for c in body.as_chunks::<20>().0 {
            let v = i64::from_le_bytes(c[0..8].try_into().map_err(|_| bad())?);
            let k = i64::from_le_bytes(c[8..16].try_into().map_err(|_| bad())?);
            let rg = u32::from_le_bytes(c[16..20].try_into().map_err(|_| bad())?);
            versions.push((v, k, RowGroupId(rg)));
        }
        if !versions.windows(2).all(|w| w[0] < w[1]) {
            return Err(bad());
        }
        Ok(Self { versions })
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct BitemporalBuffer {
    versions: Vec<(i64, i64, RowGroupId)>,
    compacted: usize,
}

impl BitemporalBuffer {
    pub(crate) fn push(&mut self, valid: Option<i64>, known: Option<i64>, rg: RowGroupId) {
        let (Some(v), Some(k)) = (valid, known) else {
            return;
        };
        if self.versions.last() == Some(&(v, k, rg)) {
            return;
        }
        self.versions.push((v, k, rg));
        if self.versions.len() > 2 * self.compacted.max(32) {
            self.versions.sort_unstable();
            self.versions.dedup();
            self.compacted = self.versions.len();
        }
    }

    pub(crate) fn finish(self) -> BitemporalChain {
        BitemporalChain::from_versions(self.versions)
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
        if h.key_types != h0.key_types
            || h.ts_type != h0.ts_type
            || h.known_ts_type != h0.known_ts_type
        {
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


/// Index-wide bitemporal query (`latest --known` when `valid` is `None`, or
/// `asof --valid V --known K`) across every segment of a `temporal2` key spec.
pub fn query_bitemporal<R: ReadAt, K: AsRef<[u8]>>(
    segments: &[SegmentReader<R>],
    keys: &[K],
    valid: Option<i64>,
    known: Option<i64>,
) -> Result<BTreeMap<Vec<u8>, VersionHit>> {
    fold(segments, |s| s.query_bitemporal(keys, valid, known))
}

/// Index-wide per-key row groups whose `[min_ts, max_ts]` in a `bounds`
/// sidecar segment overlaps `[lo, hi]`, unioned across all segments of the
/// `KeySpec::Bounds`.
pub fn lookup_bounds<R: ReadAt, K: AsRef<[u8]>>(
    segments: &[SegmentReader<R>],
    keys: &[K],
    lo: i64,
    hi: i64,
) -> Result<BTreeMap<Vec<u8>, RoaringBitmap>> {
    same_spec(segments)?;
    let mut out: BTreeMap<Vec<u8>, RoaringBitmap> = BTreeMap::new();
    for s in segments {
        for (k, bm) in s.lookup_bounds(keys, lo, hi)? {
            *out.entry(k).or_default() |= bm;
        }
    }
    Ok(out)
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
}
