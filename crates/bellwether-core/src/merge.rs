//! Merge N segments of the same key spec into one.
//!
//! Keyed segments merge by a k-way merge over their sorted keys: bitmaps of
//! equal keys are OR-ed and version chains are merged. Each input holds one
//! block at a time. Bloom segments merge their per-row-group filters in row
//! group order; filters of one row group are OR-ed when they have the same
//! size and kept as separate entries otherwise (a lookup unions over them),
//! so compaction after a re-index never fails. Range segments merge their
//! `(min, max, rg)` entries in order; a row group present in two inputs
//! keeps both entries, which can only widen a result, never narrow it.
//!
//! The merged header carries the inputs' unknown header fields when every
//! input agrees on them, and merge refuses otherwise (ADR 017). Inputs must
//! also agree on the key spec, on `bloom_hash`, and on the recorded key and
//! timestamp types (`key_types`, `ts_type`): keys encoded under different
//! dataset types would split silently, so such segments never merge. The
//! output is written by this build, so it carries the current storage and
//! encoding versions whatever versions its inputs carried.

use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

use bellwether_types::{Error, Result};

use crate::bloom::BLOCK_BYTES;
use crate::segment::{
    stored_bounds, BlockView, BloomEmitter, EncodedSegment, Index, KeyValue, KeyedEmitter,
    RangeEmitter, ReadAt, SegmentHeader, SegmentReader, WriterOptions,
};

struct KeyedCursor<'a, R> {
    seg: &'a SegmentReader<R>,
    block: usize,
    pos: usize,
    body: Option<Cow<'a, [u8]>>,
}

impl<R: ReadAt> KeyedCursor<'_, R> {
    fn next(&mut self) -> Result<Option<(Vec<u8>, KeyValue)>> {
        let Index::Keyed(blocks) = &self.seg.index else {
            return Ok(None);
        };
        loop {
            let Some(b) = blocks.get(self.block) else {
                return Ok(None);
            };
            if self.body.is_none() {
                self.body = Some(self.seg.block_body(b)?);
                self.pos = 0;
            }
            let Some(body) = self.body.as_deref() else {
                return Ok(None);
            };
            let view = BlockView::parse(body, b.n, self.seg.checked())?;
            if self.pos < view.len() {
                let key = view.key(self.pos)?.to_vec();
                let value = self.seg.value_at(&view, self.pos)?;
                self.pos += 1;
                return Ok(Some((key, value)));
            }
            self.block += 1;
            self.body = None;
        }
    }
}

/// The header of the merged segment: the inputs' key spec, `bloom_hash`,
/// recorded types, and unknown fields, all of which every input must agree
/// on.
fn merged_header<R: ReadAt>(inputs: &[SegmentReader<R>]) -> Result<SegmentHeader> {
    let Some(first) = inputs.first() else {
        return Err(Error::Other("merge needs at least one segment".into()));
    };
    let h0 = first.header();
    for s in &inputs[1..] {
        let h = s.header();
        if h.key_spec != h0.key_spec {
            return Err(Error::KeySpec(format!(
                "cannot merge segments of different key specs: {} and {}",
                h0.key_spec.identity(),
                h.key_spec.identity()
            )));
        }
        if h.layout != h0.layout {
            return Err(Error::KeySpec(format!(
                "cannot merge a segment of {}'s filters with one of its per-key bounds",
                h0.key_spec.identity()
            )));
        }
        if h.bloom_hash != h0.bloom_hash {
            return Err(Error::KeySpec(
                "cannot merge segments whose filters hash different inputs".into(),
            ));
        }
        if h.key_types != h0.key_types || h.ts_type != h0.ts_type || h.time_types != h0.time_types {
            return Err(Error::KeySpec(format!(
                "cannot merge segments whose recorded types differ: {} and {}",
                crate::segment::types(
                    h0.key_types.as_deref().unwrap_or_default(),
                    h0.ts_type.as_ref()
                ),
                crate::segment::types(
                    h.key_types.as_deref().unwrap_or_default(),
                    h.ts_type.as_ref()
                ),
            )));
        }
        if h.extra != h0.extra {
            return Err(Error::Other(
                "cannot merge segments whose unknown header fields differ (ADR 017)".into(),
            ));
        }
    }
    let mut header = SegmentHeader::new(h0.key_spec.clone());
    header.layout = h0.layout;
    header.bloom_hash = h0.bloom_hash;
    header.key_types.clone_from(&h0.key_types);
    header.ts_type.clone_from(&h0.ts_type);
    header.time_types.clone_from(&h0.time_types);
    header.extra.clone_from(&h0.extra);
    Ok(header)
}

/// Merges `inputs`, which must all index the same key spec, into one segment.
///
/// The output records the hull of its inputs' row-group ranges, so a merge
/// of segments that processed row groups holding no keys still records
/// them, and two such merges over different row groups differ. For inputs
/// that record only the row groups their keys reference, the hull is the
/// range the merged keys reference, as before.
pub fn merge<R: ReadAt>(
    inputs: &[SegmentReader<R>],
    opts: &WriterOptions,
) -> Result<EncodedSegment> {
    if let Some(s) = inputs
        .iter()
        .find(|s| s.header().layout == crate::segment::Layout::KeyedJoin)
    {
        return Err(Error::KeySpec(format!(
            "{} is one segment rebuilt whole from both datasets' exact keys, never merged",
            s.key_spec().name()
        )));
    }
    let header = merged_header(inputs)?;
    let hull = inputs
        .iter()
        .filter_map(SegmentReader::rowgroup_range)
        .map(|(a, b)| (a.0, b.0))
        .reduce(|(a, b), (c, d)| (a.min(c), b.max(d)));
    match &inputs[0].index {
        Index::Keyed(_) => merge_keyed(inputs, &header, opts, hull),
        Index::Bloom(_) => merge_bloom(inputs, &header, hull),
        Index::Range(_) => merge_range(inputs, &header, opts, hull),
    }
}

fn merge_keyed<R: ReadAt>(
    inputs: &[SegmentReader<R>],
    header: &SegmentHeader,
    opts: &WriterOptions,
    hull: Option<(u32, u32)>,
) -> Result<EncodedSegment> {
    let mut cursors: Vec<KeyedCursor<'_, R>> = inputs
        .iter()
        .map(|seg| KeyedCursor {
            seg,
            block: 0,
            pos: 0,
            body: None,
        })
        .collect();
    let mut heads: Vec<Option<KeyValue>> = Vec::with_capacity(cursors.len());
    let mut heap = BinaryHeap::new();
    for (i, c) in cursors.iter_mut().enumerate() {
        match c.next()? {
            Some((k, v)) => {
                heap.push(Reverse((k, i)));
                heads.push(Some(v));
            }
            None => heads.push(None),
        }
    }
    let mut out = KeyedEmitter::new(header, opts)?;
    while let Some(Reverse((key, i))) = heap.pop() {
        let mut value = heads[i].take().unwrap_or_default();
        advance(&mut cursors, &mut heads, &mut heap, i)?;
        while heap.peek().is_some_and(|Reverse((k, _))| *k == key) {
            let Some(Reverse((_, j))) = heap.pop() else {
                break;
            };
            if let Some(v) = heads[j].take() {
                fold(&mut value, v, &key)?;
            }
            advance(&mut cursors, &mut heads, &mut heap, j)?;
        }
        out.push(&key, &value)?;
    }
    if let Some((lo, hi)) = hull {
        out.cover(lo, hi);
    }
    out.finish()
}

/// Distinct keys across keyed segments of one key spec whose row groups
/// include at least one id in `live`: the exact distinct count of the
/// spec's columns over the live row groups. A k-way merge over the sorted
/// keys, each input holding one block at a time (ADR 018). Bloom and range
/// segments hold no per-key values and are refused.
pub fn count_live_keys<R: ReadAt>(
    inputs: &[SegmentReader<R>],
    live: &roaring::RoaringBitmap,
) -> Result<u64> {
    if inputs.iter().any(|s| !matches!(s.index, Index::Keyed(_))) {
        return Err(Error::KeySpec(
            "only exact and temporal segments hold their distinct keys".into(),
        ));
    }
    let mut cursors: Vec<KeyedCursor<'_, R>> = inputs
        .iter()
        .map(|seg| KeyedCursor {
            seg,
            block: 0,
            pos: 0,
            body: None,
        })
        .collect();
    let mut heads: Vec<Option<KeyValue>> = Vec::with_capacity(cursors.len());
    let mut heap = BinaryHeap::new();
    for (i, c) in cursors.iter_mut().enumerate() {
        match c.next()? {
            Some((k, v)) => {
                heap.push(Reverse((k, i)));
                heads.push(Some(v));
            }
            None => heads.push(None),
        }
    }
    let mut count = 0u64;
    while let Some(Reverse((key, i))) = heap.pop() {
        let mut held = heads[i].take().is_some_and(|v| !v.bitmap.is_disjoint(live));
        advance(&mut cursors, &mut heads, &mut heap, i)?;
        while heap.peek().is_some_and(|Reverse((k, _))| *k == key) {
            let Some(Reverse((_, j))) = heap.pop() else {
                break;
            };
            if let Some(v) = heads[j].take() {
                held |= !v.bitmap.is_disjoint(live);
            }
            advance(&mut cursors, &mut heads, &mut heap, j)?;
        }
        if held {
            count += 1;
        }
    }
    Ok(count)
}

/// Folds `other`'s value of `key` into `value`: bitmaps OR-ed, chains
/// merged, bitemporal versions united, per-key bounds of one row group
/// widened (the all-null sentinel, `[i64::MAX, i64::MIN]`, absorbs into any
/// real range), and counts combined. Counts of one (key, row group) are
/// never summed across inputs: segments of one spec are built over
/// disjoint row groups, so two inputs that both count a key in one row
/// group are a double count, and the merge is refused rather than storing
/// it.
fn fold(value: &mut KeyValue, other: KeyValue, key: &[u8]) -> Result<()> {
    let bounds = match (value.bounds.take(), &other.bounds) {
        (None, None) => None,
        (Some(a), Some(b)) => {
            let mut per_rg: std::collections::BTreeMap<u32, (i64, i64)> =
                value.bitmap.iter().zip(a).collect();
            for (rg, (lo, hi)) in other.bitmap.iter().zip(b.iter().copied()) {
                per_rg
                    .entry(rg)
                    .and_modify(|w| *w = (w.0.min(lo), w.1.max(hi)))
                    .or_insert((lo, hi));
            }
            Some(per_rg.into_values().collect())
        }
        _ => {
            return Err(Error::Other(
                "cannot merge a value with per-key bounds with one without".into(),
            ))
        }
    };
    value.bounds = bounds;
    if !other.triples.is_empty() {
        value.triples.extend(other.triples.iter().copied());
        value.triples.sort_unstable();
        value.triples.dedup();
    }
    let counts = match (value.counts.take(), other.counts) {
        (None, None) => None,
        (Some(a), Some(b)) => {
            if !(&value.bitmap & &other.bitmap).is_empty() {
                return Err(Error::Other(format!(
                    "cannot merge counted segments that both count key {} in row group {}: \
                     segments of one spec cover disjoint row groups",
                    hex(key),
                    (&value.bitmap & &other.bitmap).min().unwrap_or_default()
                )));
            }
            let mut left = value.bitmap.iter().zip(a).peekable();
            let mut right = other.bitmap.iter().zip(b).peekable();
            let mut merged = Vec::new();
            loop {
                let take_left = match (left.peek(), right.peek()) {
                    (Some(l), Some(r)) => l.0 < r.0,
                    (Some(_), None) => true,
                    (None, Some(_)) => false,
                    (None, None) => break,
                };
                let next = if take_left { left.next() } else { right.next() };
                if let Some((_, n)) = next {
                    merged.push(n);
                }
            }
            Some(merged)
        }
        _ => {
            return Err(Error::Other(
                "cannot merge a counted value with an uncounted one".into(),
            ))
        }
    };
    value.bitmap |= other.bitmap;
    value.chain.merge(&other.chain);
    value.counts = counts;
    Ok(())
}

fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    b.iter().fold(String::new(), |mut s, x| {
        let _ = write!(s, "{x:02x}");
        s
    })
}

fn advance<R: ReadAt>(
    cursors: &mut [KeyedCursor<'_, R>],
    heads: &mut [Option<KeyValue>],
    heap: &mut BinaryHeap<Reverse<(Vec<u8>, usize)>>,
    i: usize,
) -> Result<()> {
    if let Some((k, v)) = cursors[i].next()? {
        heap.push(Reverse((k, i)));
        heads[i] = Some(v);
    }
    Ok(())
}

/// The merge's key stream over the keyed segments of one spec, as an
/// iterator: every distinct key once, ascending, with its values folded
/// across the segments holding it (bitmaps OR-ed, counts combined as
/// [`merge`] combines them). Holds one block per input at a time (ADR 018).
pub(crate) struct MergedKeys<'a, R> {
    cursors: Vec<KeyedCursor<'a, R>>,
    heads: Vec<Option<KeyValue>>,
    heap: BinaryHeap<Reverse<(Vec<u8>, usize)>>,
}

impl<'a, R: ReadAt> MergedKeys<'a, R> {
    /// Opens the stream over `inputs`, which must all be keyed (exact or
    /// temporal) segments.
    pub(crate) fn new(inputs: &'a [SegmentReader<R>]) -> Result<Self> {
        if let Some(s) = inputs.iter().find(|s| {
            !matches!(s.index, Index::Keyed(_))
                || s.header().layout == crate::segment::Layout::KeyedJoin
        }) {
            return Err(Error::KeySpec(format!(
                "{} segments have no per-key values to merge",
                s.header().key_spec.name()
            )));
        }
        let mut cursors: Vec<KeyedCursor<'a, R>> = inputs
            .iter()
            .map(|seg| KeyedCursor {
                seg,
                block: 0,
                pos: 0,
                body: None,
            })
            .collect();
        let mut heads: Vec<Option<KeyValue>> = Vec::with_capacity(cursors.len());
        let mut heap = BinaryHeap::new();
        for (i, c) in cursors.iter_mut().enumerate() {
            match c.next()? {
                Some((k, v)) => {
                    heap.push(Reverse((k, i)));
                    heads.push(Some(v));
                }
                None => heads.push(None),
            }
        }
        Ok(Self {
            cursors,
            heads,
            heap,
        })
    }

    /// The next distinct key and its folded value, or `None` at the end.
    pub(crate) fn next_key(&mut self) -> Result<Option<(Vec<u8>, KeyValue)>> {
        let Some(Reverse((key, i))) = self.heap.pop() else {
            return Ok(None);
        };
        let mut value = self.heads[i].take().unwrap_or_default();
        advance(&mut self.cursors, &mut self.heads, &mut self.heap, i)?;
        while self.heap.peek().is_some_and(|Reverse((k, _))| *k == key) {
            let Some(Reverse((_, j))) = self.heap.pop() else {
                break;
            };
            if let Some(v) = self.heads[j].take() {
                fold(&mut value, v, &key)?;
            }
            advance(&mut self.cursors, &mut self.heads, &mut self.heap, j)?;
        }
        Ok(Some((key, value)))
    }
}

/// Visits every distinct key of the keyed segments `inputs` in ascending
/// order with the union of its bitmaps across them: the merge's key stream,
/// without writing a segment. Holds one block per input at a time
/// (ADR 018). Bloom and range segments have no keys to visit.
pub fn visit_merged_keys<R: ReadAt>(
    inputs: &[SegmentReader<R>],
    mut f: impl FnMut(&[u8], &roaring::RoaringBitmap) -> Result<()>,
) -> Result<()> {
    if let Some(s) = inputs.iter().find(|s| !matches!(s.index, Index::Keyed(_))) {
        return Err(Error::KeySpec(format!(
            "{} segments have no keys to visit",
            s.header().key_spec.name()
        )));
    }
    let mut cursors: Vec<KeyedCursor<'_, R>> = inputs
        .iter()
        .map(|seg| KeyedCursor {
            seg,
            block: 0,
            pos: 0,
            body: None,
        })
        .collect();
    let mut heads: Vec<Option<KeyValue>> = Vec::with_capacity(cursors.len());
    let mut heap = BinaryHeap::new();
    for (i, c) in cursors.iter_mut().enumerate() {
        match c.next()? {
            Some((k, v)) => {
                heap.push(Reverse((k, i)));
                heads.push(Some(v));
            }
            None => heads.push(None),
        }
    }
    while let Some(Reverse((key, i))) = heap.pop() {
        let mut bitmap = heads[i].take().unwrap_or_default().bitmap;
        advance(&mut cursors, &mut heads, &mut heap, i)?;
        while heap.peek().is_some_and(|Reverse((k, _))| *k == key) {
            let Some(Reverse((_, j))) = heap.pop() else {
                break;
            };
            if let Some(v) = heads[j].take() {
                bitmap |= v.bitmap;
            }
            advance(&mut cursors, &mut heads, &mut heap, j)?;
        }
        f(&key, &bitmap)?;
    }
    Ok(())
}

fn check_bitset_len(bits: &[u8]) -> Result<()> {
    if bits.is_empty() || !bits.len().is_multiple_of(BLOCK_BYTES) {
        return Err(Error::Other(format!(
            "corrupt segment: bloom bitset length {}",
            bits.len()
        )));
    }
    Ok(())
}

fn merge_bloom<R: ReadAt>(
    inputs: &[SegmentReader<R>],
    header: &SegmentHeader,
    hull: Option<(u32, u32)>,
) -> Result<EncodedSegment> {
    // (rg, input, entry) in row group order; the indexes are already in memory.
    let mut all = Vec::new();
    for (i, seg) in inputs.iter().enumerate() {
        if let Index::Bloom(entries) = &seg.index {
            for (j, e) in entries.iter().enumerate() {
                all.push((e.rg, i, j));
            }
        }
    }
    all.sort_unstable();
    let mut out = BloomEmitter::new(header)?;
    let mut pos = 0;
    while pos < all.len() {
        let rg = all[pos].0;
        // This row group's filters, OR-ed per distinct bitset size.
        let mut by_size: Vec<(u64, Vec<u8>)> = Vec::new();
        while pos < all.len() && all[pos].0 == rg {
            let (_, input, entry) = all[pos];
            if let Index::Bloom(entries) = &inputs[input].index {
                let e = &entries[entry];
                let bits = inputs[input].bloom_bitset(e)?;
                check_bitset_len(&bits)?;
                match by_size.iter_mut().find(|(_, b)| b.len() == bits.len()) {
                    Some((ndv, acc)) => {
                        *ndv = ndv.saturating_add(e.ndv);
                        for (x, y) in acc.iter_mut().zip(bits.iter()) {
                            *x |= y;
                        }
                    }
                    None => by_size.push((e.ndv, bits.into_owned())),
                }
            }
            pos += 1;
        }
        for (ndv, bits) in &by_size {
            out.push(rg, *ndv, bits)?;
        }
    }
    if let Some((lo, hi)) = hull {
        out.cover(lo, hi);
    }
    Ok(out.finish())
}

fn merge_range<R: ReadAt>(
    inputs: &[SegmentReader<R>],
    header: &SegmentHeader,
    opts: &WriterOptions,
    hull: Option<(u32, u32)>,
) -> Result<EncodedSegment> {
    // Range segments hold one small entry per row group, so collecting the
    // entries is bounded by the row-group count, not by keys.
    let mut entries = Vec::new();
    for seg in inputs {
        seg.for_each_range_entry(|min, max, rg| {
            let (min, max) = stored_bounds(min.to_vec(), max.to_vec());
            entries.push((min, max, rg));
            Ok(())
        })?;
    }
    entries.sort();
    entries.dedup();
    let mut out = RangeEmitter::new(header, opts)?;
    for (min, max, rg) in &entries {
        out.push(min, max, *rg)?;
    }
    if let Some((lo, hi)) = hull {
        out.cover(lo, hi);
    }
    out.finish()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use bellwether_types::KeySpec;

    use super::*;

    fn segment(extra: Option<(&str, i64)>, rg: u32) -> SegmentReader<Vec<u8>> {
        let mut header = SegmentHeader::new(KeySpec::Exact {
            columns: vec!["id".into()],
            counts: false,
            bounds: None,
        });
        if let Some((k, v)) = extra {
            header.extra.insert(k.into(), serde_json::json!(v));
        }
        let mut e = KeyedEmitter::new(&header, &WriterOptions::default()).unwrap();
        let mut value = KeyValue::default();
        value.bitmap.insert(rg);
        e.push(b"k", &value).unwrap();
        SegmentReader::open(e.finish().unwrap().bytes).unwrap()
    }

    #[test]
    fn unknown_header_fields_survive_merge_when_inputs_agree() {
        let m = merge(
            &[
                segment(Some(("future", 7)), 1),
                segment(Some(("future", 7)), 2),
            ],
            &WriterOptions::default(),
        )
        .unwrap();
        let r = SegmentReader::open(m.bytes).unwrap();
        assert_eq!(r.header().extra.get("future"), Some(&serde_json::json!(7)));
        assert_eq!(r.lookup(&[b"k"]).unwrap().len(), 2);
    }

    #[test]
    fn merge_refuses_inputs_whose_unknown_header_fields_differ() {
        assert!(merge(
            &[
                segment(Some(("future", 7)), 1),
                segment(Some(("future", 8)), 2)
            ],
            &WriterOptions::default(),
        )
        .is_err());
        assert!(merge(
            &[segment(Some(("future", 7)), 1), segment(None, 2)],
            &WriterOptions::default(),
        )
        .is_err());
    }

    fn keyless(spec: KeySpec, range: std::ops::Range<u32>) -> SegmentReader<Vec<u8>> {
        let mut w = crate::SegmentWriter::new(spec, WriterOptions::default()).unwrap();
        w.set_rowgroup_range(
            bellwether_types::RowGroupId(range.start)..bellwether_types::RowGroupId(range.end),
        )
        .unwrap();
        SegmentReader::open(w.finish().unwrap().bytes).unwrap()
    }

    #[test]
    fn merge_records_the_hull_of_its_inputs_ranges() {
        use bellwether_types::RowGroupId;
        let opts = WriterOptions::default();
        for spec in [
            KeySpec::Exact {
                columns: vec!["k".into()],
                counts: false,
                bounds: None,
            },
            KeySpec::Bloom {
                columns: vec!["k".into()],
                fpr: 0.01,
                bounds: None,
            },
            KeySpec::Range { column: "k".into() },
        ] {
            let a = merge(
                &[keyless(spec.clone(), 0..2), keyless(spec.clone(), 2..4)],
                &opts,
            )
            .unwrap();
            let b = merge(
                &[keyless(spec.clone(), 4..6), keyless(spec.clone(), 6..8)],
                &opts,
            )
            .unwrap();
            assert_eq!(a.rowgroup_range, Some((RowGroupId(0), RowGroupId(3))));
            assert_eq!(b.rowgroup_range, Some((RowGroupId(4), RowGroupId(7))));
            assert_ne!(a.id, b.id, "{}", spec.name());
        }
        // inputs that record only their keys' row groups: the hull is the
        // merged keys' range, as before
        let m = merge(&[segment(None, 1), segment(None, 9)], &opts).unwrap();
        assert_eq!(m.rowgroup_range, Some((RowGroupId(1), RowGroupId(9))));
    }
}
