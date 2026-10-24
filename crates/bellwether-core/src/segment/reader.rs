//! Segment reader.
//!
//! Opening reads the preamble, the JSON header, the footer, and the block
//! index; nothing else. A lookup then reads, per block it lands in, the
//! block's bloom bitset and, only if the bloom passes, the block's key
//! directory and the value records of the keys found. At most one block is
//! held at a time; keys are sorted first so consecutive keys reuse it.

use std::borrow::Cow;
use std::collections::BTreeMap;

use arrow::datatypes::Schema;
use bellwether_types::{Error, KeySpec, Result, RowGroupId};
use roaring::RoaringBitmap;

use super::format::{
    check_versions, corrupt, spec_types, ColumnType, Cursor, Layout, ReadAt, SegmentHeader,
    FOOTER_LEN, MAGIC, PREAMBLE_LEN,
};
use super::writer::KeyValue;
use crate::bloom::{check_bitset, BloomHash, CanonicalHash, FilterHash};
use crate::encoding::{prefix_bound, tuple_has_null, NAN_KEY};
use crate::temporal::VersionHit;

/// Smallest serialized index entry per layout, used to cap allocations made
/// from a block count read off disk.
const MIN_KEYED_ENTRY: u64 = 4 + 4 + 8 + 4 + 8 + 4 + 4;
const MIN_BLOOM_ENTRY: u64 = 4 + 8 + 4 + 8;
const MIN_RANGE_ENTRY: u64 = 4 + 4 + 8 + 4 + 4;

/// The probe keys a lookup answers: every key except a tuple that provably
/// contains a null element, which matches nothing under ADR 014. A key that
/// is not a well-formed tuple is still looked up as bytes, so a probe is
/// never dropped on a guess.
fn probes<K: AsRef<[u8]>>(keys: &[K]) -> impl Iterator<Item = &[u8]> {
    keys.iter()
        .map(AsRef::as_ref)
        .filter(|k| tuple_has_null(k) != Some(true))
}

/// How many index entries to reserve room for: the block count the index
/// claims, capped by how many entries of the smallest size its bytes can
/// hold, so a corrupt count never drives a large allocation (ADR 018). A
/// zero entry size reserves nothing.
pub(crate) fn reserve_hint(block_count: u64, index_len: u64, min_entry: u64) -> usize {
    let fit = index_len.checked_div(min_entry).unwrap_or(0);
    usize::try_from(block_count.min(fit)).unwrap_or(0)
}

#[derive(Clone, Debug)]
pub(crate) struct KeyedBlock {
    pub(crate) first: Vec<u8>,
    pub(crate) last: Vec<u8>,
    bloom_off: u64,
    bloom_len: u32,
    body_off: u64,
    body_len: u32,
    pub(crate) n: u32,
}

#[derive(Clone, Debug)]
pub(crate) struct BloomEntry {
    pub(crate) rg: u32,
    off: u64,
    len: u32,
    pub(crate) ndv: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct RangeBlock {
    first_min: Vec<u8>,
    max_max: Vec<u8>,
    off: u64,
    len: u32,
    n: u32,
}

#[derive(Clone, Debug)]
pub(crate) enum Index {
    Keyed(Vec<KeyedBlock>),
    Bloom(Vec<BloomEntry>),
    Range(Vec<RangeBlock>),
}

/// A half-open or closed interval over encoded keys.
#[derive(Clone, Debug)]
struct KeyRange {
    lo: Vec<u8>,
    hi: Option<(Vec<u8>, bool)>,
}

impl KeyRange {
    fn below_hi(&self, k: &[u8]) -> bool {
        match &self.hi {
            None => true,
            Some((h, true)) => k <= h.as_slice(),
            Some((h, false)) => k < h.as_slice(),
        }
    }
    fn contains(&self, k: &[u8]) -> bool {
        k >= self.lo.as_slice() && self.below_hi(k)
    }
    /// Whether `[min, max]` intersects this range.
    fn overlaps(&self, min: &[u8], max: &[u8]) -> bool {
        max >= self.lo.as_slice() && self.below_hi(min)
    }
}

/// A parsed view of one keyed block's directory.
pub(crate) struct BlockView<'a> {
    n: usize,
    offsets: &'a [u8],
    keys: &'a [u8],
    refs: &'a [u8],
}

impl<'a> BlockView<'a> {
    pub(crate) fn parse(body: &'a [u8], expect_n: u32) -> Result<Self> {
        let mut c = Cursor::new(body);
        let n = c.u32()?;
        if n != expect_n {
            return Err(corrupt("block key count"));
        }
        let n = n as usize;
        let offsets = c.take((n + 1) * 4)?;
        let total = u32_at(offsets, n) as usize;
        let keys = c.take(total)?;
        let refs = c.take(n * 12)?;
        if !c.is_done() {
            return Err(corrupt("block trailing bytes"));
        }
        Ok(Self {
            n,
            offsets,
            keys,
            refs,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.n
    }

    pub(crate) fn key(&self, i: usize) -> Result<&'a [u8]> {
        let (a, b) = (
            u32_at(self.offsets, i) as usize,
            u32_at(self.offsets, i + 1) as usize,
        );
        self.keys
            .get(a..b)
            .ok_or_else(|| corrupt("block key offsets"))
    }

    fn value_ref(&self, i: usize) -> (u64, usize) {
        let b = &self.refs[i * 12..i * 12 + 12];
        let mut o = [0u8; 8];
        o.copy_from_slice(&b[..8]);
        (u64::from_le_bytes(o), u32_at(&b[8..], 0) as usize)
    }

    fn find(&self, key: &[u8]) -> Result<Option<usize>> {
        let i = self.lower_bound(key)?;
        Ok((i < self.n && self.key(i)? == key).then_some(i))
    }

    fn lower_bound(&self, key: &[u8]) -> Result<usize> {
        let (mut lo, mut hi) = (0, self.n);
        while lo < hi {
            let mid = usize::midpoint(lo, hi);
            if self.key(mid)? < key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }
}

/// The block a lookup is positioned on: its index, bloom bitset, and the key
/// directory once the bloom has passed for some key.
type LoadedBlock<'a> = (usize, Cow<'a, [u8]>, Option<Cow<'a, [u8]>>);

fn u32_at(b: &[u8], i: usize) -> u32 {
    let s = &b[i * 4..i * 4 + 4];
    u32::from_le_bytes([s[0], s[1], s[2], s[3]])
}

/// Reads one segment. Generic over where its bytes live ([`ReadAt`]).
///
/// Memory bound (ADR 018): an open reader holds the header and the block
/// index (two keys and a few integers per block, or one small entry per row
/// group for bloom segments). A lookup additionally holds one block (its
/// bloom bitset and key directory) and the bitmaps or version hits for the
/// keys asked; nothing proportional to the segment's key count.
#[derive(Debug)]
pub struct SegmentReader<R> {
    src: R,
    header: SegmentHeader,
    entries: u64,
    rg: Option<(RowGroupId, RowGroupId)>,
    pub(crate) index: Index,
}

impl<R: ReadAt> SegmentReader<R> {
    /// Opens a segment: checks the preamble versions before parsing anything
    /// else, validates the header against them (versions agree, codec,
    /// layout, `bloom_hash`), and loads the index.
    pub fn open(src: R) -> Result<Self> {
        let size = src.size()?;
        if size < (PREAMBLE_LEN + FOOTER_LEN) as u64 {
            return Err(corrupt("file too short"));
        }
        let pre = src.read_at(0, PREAMBLE_LEN)?;
        let mut c = Cursor::new(&pre);
        if c.take(8)? != MAGIC {
            return Err(corrupt("bad magic"));
        }
        let storage = c.u32()?;
        let encoding = c.u32()?;
        check_versions(storage, encoding)?;
        let header_len = c.u32()? as usize;
        let header_bytes = src.read_at(PREAMBLE_LEN as u64, header_len)?;
        let header: SegmentHeader = serde_json::from_slice(&header_bytes)?;
        header.validate(storage, encoding)?;
        let foot = src.read_at(size - FOOTER_LEN as u64, FOOTER_LEN)?;
        let mut c = Cursor::new(&foot);
        let index_off = c.u64()?;
        let index_len = usize::try_from(c.u64()?).map_err(|_| corrupt("index length"))?;
        if c.take(8)? != MAGIC {
            return Err(corrupt("bad footer magic"));
        }
        let index_bytes = src.read_at(index_off, index_len)?;
        let mut c = Cursor::new(&index_bytes);
        let entries = c.u64()?;
        let has_rg = c.u8()? == 1;
        let (lo, hi) = (c.u32()?, c.u32()?);
        let claimed = c.u32()?;
        let blocks = claimed as usize;
        let reserve = |min_entry| reserve_hint(u64::from(claimed), index_len as u64, min_entry);
        let index = match header.layout {
            Layout::Keyed | Layout::KeyedTemporal => {
                let mut v = Vec::with_capacity(reserve(MIN_KEYED_ENTRY));
                for _ in 0..blocks {
                    v.push(KeyedBlock {
                        first: c.bytes()?.to_vec(),
                        last: c.bytes()?.to_vec(),
                        bloom_off: c.u64()?,
                        bloom_len: c.u32()?,
                        body_off: c.u64()?,
                        body_len: c.u32()?,
                        n: c.u32()?,
                    });
                }
                Index::Keyed(v)
            }
            Layout::BloomPerRowGroup => {
                let mut v = Vec::with_capacity(reserve(MIN_BLOOM_ENTRY));
                for _ in 0..blocks {
                    v.push(BloomEntry {
                        rg: c.u32()?,
                        off: c.u64()?,
                        len: c.u32()?,
                        ndv: c.u64()?,
                    });
                }
                Index::Bloom(v)
            }
            Layout::MinMax => {
                let mut v = Vec::with_capacity(reserve(MIN_RANGE_ENTRY));
                for _ in 0..blocks {
                    v.push(RangeBlock {
                        first_min: c.bytes()?.to_vec(),
                        max_max: c.bytes()?.to_vec(),
                        off: c.u64()?,
                        len: c.u32()?,
                        n: c.u32()?,
                    });
                }
                Index::Range(v)
            }
        };
        if !c.is_done() {
            return Err(corrupt("index trailing bytes"));
        }
        Ok(Self {
            src,
            header,
            entries,
            rg: has_rg.then_some((RowGroupId(lo), RowGroupId(hi))),
            index,
        })
    }

    /// Opens a segment for lookups against a dataset whose schema is
    /// `schema`, refusing it unless its header records exactly the key and
    /// timestamp types that schema declares ([`SegmentReader::check_dataset`]).
    /// Keys encoded under other types (another decimal scale, another time
    /// unit) would silently miss; this refuses instead.
    pub fn open_for_dataset(src: R, schema: &Schema) -> Result<Self> {
        let r = Self::open(src)?;
        r.check_dataset(schema)?;
        Ok(r)
    }

    /// Checks that this segment's recorded key types (and temporal ts type)
    /// equal the types `schema` declares for the key spec's columns. A
    /// segment written from already-encoded keys records no types and is
    /// refused here, because its encoding cannot be verified.
    pub fn check_dataset(&self, schema: &Schema) -> Result<()> {
        let (keys, ts) = spec_types(&self.header.key_spec, schema)?;
        let want_keys = keys
            .iter()
            .map(ColumnType::of)
            .collect::<Result<Vec<_>>>()?;
        let want_ts = ts.as_ref().map(ColumnType::of).transpose()?;
        let Some(have_keys) = &self.header.key_types else {
            return Err(Error::KeySpec(format!(
                "segment for {} records no key types, so it cannot be checked against the dataset",
                self.header.key_spec.name()
            )));
        };
        if have_keys != &want_keys || self.header.ts_type != want_ts {
            return Err(Error::KeySpec(format!(
                "segment for {} was encoded under key types {:?} (ts {:?}); the dataset declares {:?} (ts {:?})",
                self.header.key_spec.name(),
                have_keys,
                self.header.ts_type,
                want_keys,
                want_ts
            )));
        }
        Ok(())
    }

    /// The bytes the segment is read from.
    #[must_use]
    pub fn source(&self) -> &R {
        &self.src
    }

    /// The header.
    #[must_use]
    pub fn header(&self) -> &SegmentHeader {
        &self.header
    }

    /// The key spec this segment indexes.
    #[must_use]
    pub fn key_spec(&self) -> &KeySpec {
        &self.header.key_spec
    }

    /// What this segment's bloom filters hash (`canonical` for range
    /// segments, which have no filters).
    #[must_use]
    pub fn bloom_hash(&self) -> BloomHash {
        self.header.bloom_hash
    }

    /// Distinct keys (keyed), summed per-row-group distinct keys (bloom), or
    /// row groups (range).
    #[must_use]
    pub fn entries(&self) -> u64 {
        self.entries
    }

    /// First and last row group the segment records, inclusive: the row
    /// groups its writer processed when the writer was told
    /// ([`crate::SegmentWriter::set_rowgroup_range`]; a merge records the
    /// hull of its inputs'), otherwise the smallest range holding every row
    /// group a key references.
    #[must_use]
    pub fn rowgroup_range(&self) -> Option<(RowGroupId, RowGroupId)> {
        self.rg
    }

    /// Number of blocks (keyed, range) or row-group filters (bloom).
    #[must_use]
    pub fn block_count(&self) -> usize {
        match &self.index {
            Index::Keyed(v) => v.len(),
            Index::Bloom(v) => v.len(),
            Index::Range(v) => v.len(),
        }
    }

    fn read(&self, off: u64, len: u32) -> Result<Cow<'_, [u8]>> {
        self.src.read_at(off, len as usize)
    }

    pub(crate) fn block_body(&self, b: &KeyedBlock) -> Result<Cow<'_, [u8]>> {
        self.read(b.body_off, b.body_len)
    }

    pub(crate) fn value_at(&self, view: &BlockView<'_>, i: usize) -> Result<KeyValue> {
        let (off, len) = view.value_ref(i);
        let bytes = self.src.read_at(off, len)?;
        KeyValue::decode(&bytes, matches!(self.header.layout, Layout::KeyedTemporal))
    }

    pub(crate) fn bloom_bitset(&self, e: &BloomEntry) -> Result<Cow<'_, [u8]>> {
        self.read(e.off, e.len)
    }

    /// Visits each distinct key of `keys` present in a keyed segment, in
    /// ascending order. Per key: block-index binary search, bloom check,
    /// key binary search. O(k log n) for k keys over n. A probe tuple that
    /// contains a null matches nothing (ADR 014).
    fn visit_keys<K: AsRef<[u8]>>(
        &self,
        keys: &[K],
        mut f: impl FnMut(&[u8], KeyValue),
    ) -> Result<()> {
        let Index::Keyed(blocks) = &self.index else {
            return Err(Error::KeySpec(format!(
                "{} segments have no per-key values",
                self.header.key_spec.name()
            )));
        };
        let mut sorted: Vec<&[u8]> = probes(keys).collect();
        sorted.sort_unstable();
        sorted.dedup();
        let mut cur: Option<LoadedBlock<'_>> = None;
        for key in sorted {
            let i = blocks.partition_point(|b| b.first.as_slice() <= key);
            if i == 0 {
                continue;
            }
            let bi = i - 1;
            let b = &blocks[bi];
            if key > b.last.as_slice() {
                continue;
            }
            if cur.as_ref().is_none_or(|c| c.0 != bi) {
                cur = Some((bi, self.read(b.bloom_off, b.bloom_len)?, None));
            }
            let Some((_, bloom_bits, body)) = cur.as_mut() else {
                continue;
            };
            if !check_bitset(bloom_bits, CanonicalHash::of(key)) {
                continue;
            }
            if body.is_none() {
                *body = Some(self.block_body(b)?);
            }
            let Some(body) = body.as_deref() else {
                continue;
            };
            let view = BlockView::parse(body, b.n)?;
            if let Some(idx) = view.find(key)? {
                f(key, self.value_at(&view, idx)?);
            }
        }
        Ok(())
    }

    /// Row groups that may hold any of `keys` (canonical encodings): the
    /// union bitmap. Exact and temporal segments answer exactly; bloom
    /// segments may include false positives; range segments return every row
    /// group whose `[min, max]` contains a key. Never omits a row group
    /// holding a key (invariant 1). Combining several key specs is the
    /// caller's bitmap intersection. A probe tuple that contains a null
    /// matches nothing (ADR 014, SQL three-valued logic).
    ///
    /// A bloom segment whose filters hash Parquet's PLAIN encoding
    /// (`bloom_hash = parquet_plain`) cannot be probed with canonical keys;
    /// use [`SegmentReader::lookup_hashes`] with
    /// [`crate::bloom::PlainHash`] instead.
    pub fn lookup<K: AsRef<[u8]>>(&self, keys: &[K]) -> Result<RoaringBitmap> {
        let mut out = RoaringBitmap::new();
        match &self.index {
            Index::Keyed(_) => self.visit_keys(keys, |_, v| out |= v.bitmap)?,
            Index::Bloom(_) => {
                let hashes: Vec<CanonicalHash> = probes(keys).map(CanonicalHash::of).collect();
                return self.lookup_hashes(&hashes);
            }
            Index::Range(_) => {
                for k in probes(keys) {
                    self.scan_range(
                        &KeyRange {
                            lo: k.to_vec(),
                            hi: Some((k.to_vec(), true)),
                        },
                        &mut out,
                    )?;
                }
            }
        }
        Ok(out)
    }

    /// Row groups whose bloom filter may hold any of `hashes` (bloom
    /// segments only). The hash kind must match the segment's `bloom_hash`;
    /// a mismatch is an error, never a silent miss.
    pub fn lookup_hashes<H: FilterHash>(&self, hashes: &[H]) -> Result<RoaringBitmap> {
        let Index::Bloom(entries) = &self.index else {
            return Err(Error::KeySpec(format!(
                "{} is not a bloom segment",
                self.header.key_spec.name()
            )));
        };
        if self.header.bloom_hash != H::KIND {
            return Err(Error::KeySpec(format!(
                "this bloom segment's filters hash {}; probe with matching hashes, not {}",
                self.header.bloom_hash.as_str(),
                H::KIND.as_str()
            )));
        }
        let mut out = RoaringBitmap::new();
        if hashes.is_empty() {
            return Ok(out);
        }
        for e in entries {
            if out.contains(e.rg) {
                continue;
            }
            let bits = self.bloom_bitset(e)?;
            if hashes.iter().any(|h| check_bitset(&bits, *h)) {
                out.insert(e.rg);
            }
        }
        Ok(out)
    }

    /// Row groups holding any key `k` with `lo <= k <= hi`, comparing full
    /// encoded keys. For a composite key, passing only leading columns as
    /// `hi` excludes keys that extend `hi`; use
    /// [`SegmentReader::lookup_prefix_range`] for a range over leading
    /// columns. Supported by keyed and range segments; bloom segments cannot
    /// answer ranges.
    pub fn lookup_range(&self, lo: &[u8], hi: &[u8]) -> Result<RoaringBitmap> {
        let mut out = RoaringBitmap::new();
        if lo <= hi {
            self.scan_range(
                &KeyRange {
                    lo: lo.to_vec(),
                    hi: Some((hi.to_vec(), true)),
                },
                &mut out,
            )?;
        }
        Ok(out)
    }

    /// Row groups holding any key whose leading columns lie between the
    /// prefixes `lo` and `hi`, both inclusive: every key `k >= lo` that is
    /// below every extension of `hi` ([`prefix_bound`]). With single-column
    /// prefixes this is `Between` on the leading column.
    pub fn lookup_prefix_range(&self, lo: &[u8], hi: &[u8]) -> Result<RoaringBitmap> {
        let mut out = RoaringBitmap::new();
        if lo <= hi {
            let (_, upper) = prefix_bound(hi);
            self.scan_range(
                &KeyRange {
                    lo: lo.to_vec(),
                    hi: upper.map(|h| (h, false)),
                },
                &mut out,
            )?;
        }
        Ok(out)
    }

    /// Row groups holding any key that starts with `prefix`, typically the
    /// encoding of the leading columns of a composite key (ADR 002).
    pub fn lookup_prefix(&self, prefix: &[u8]) -> Result<RoaringBitmap> {
        let (lo, hi) = prefix_bound(prefix);
        let mut out = RoaringBitmap::new();
        self.scan_range(
            &KeyRange {
                lo,
                hi: hi.map(|h| (h, false)),
            },
            &mut out,
        )?;
        Ok(out)
    }

    fn scan_range(&self, r: &KeyRange, out: &mut RoaringBitmap) -> Result<()> {
        match &self.index {
            Index::Keyed(blocks) => {
                let start = blocks
                    .partition_point(|b| b.first.as_slice() <= r.lo.as_slice())
                    .saturating_sub(1);
                for b in &blocks[start..] {
                    if !r.below_hi(&b.first) {
                        break;
                    }
                    if b.last.as_slice() < r.lo.as_slice() {
                        continue;
                    }
                    let body = self.block_body(b)?;
                    let view = BlockView::parse(&body, b.n)?;
                    for i in view.lower_bound(&r.lo)?..view.len() {
                        if !r.contains(view.key(i)?) {
                            break;
                        }
                        *out |= self.value_at(&view, i)?.bitmap;
                    }
                }
                Ok(())
            }
            Index::Range(blocks) => {
                // Range bounds may come from statistics, which exclude NaN by
                // Parquet convention, so a probe that includes NaN is never
                // pruned (docs/format.md, statistics semantics).
                let nan_probe = r.contains(&NAN_KEY);
                for b in blocks {
                    if !nan_probe {
                        if !r.below_hi(&b.first_min) {
                            break;
                        }
                        if b.max_max.as_slice() < r.lo.as_slice() {
                            continue;
                        }
                    }
                    let body = self.read(b.off, b.len)?;
                    let mut c = Cursor::new(&body);
                    if c.u32()? != b.n {
                        return Err(corrupt("range block count"));
                    }
                    for _ in 0..b.n {
                        let rg = c.u32()?;
                        let min = c.bytes()?;
                        let max = c.bytes()?;
                        if nan_probe || r.overlaps(min, max) {
                            out.insert(rg);
                        }
                    }
                }
                Ok(())
            }
            Index::Bloom(_) => Err(Error::KeySpec(
                "bloom segments cannot answer range or prefix lookups".into(),
            )),
        }
    }

    /// Visits every `(min, max, rg)` entry of a range segment in order, one
    /// block at a time.
    pub(crate) fn for_each_range_entry(
        &self,
        mut f: impl FnMut(&[u8], &[u8], u32) -> Result<()>,
    ) -> Result<()> {
        let Index::Range(blocks) = &self.index else {
            return Err(Error::KeySpec("not a range segment".into()));
        };
        for b in blocks {
            let body = self.read(b.off, b.len)?;
            let mut c = Cursor::new(&body);
            if c.u32()? != b.n {
                return Err(corrupt("range block count"));
            }
            for _ in 0..b.n {
                let rg = c.u32()?;
                let min = c.bytes()?;
                let max = c.bytes()?;
                f(min, max, rg)?;
            }
        }
        Ok(())
    }

    fn require_temporal(&self) -> Result<()> {
        if self.header.layout == Layout::KeyedTemporal {
            Ok(())
        } else {
            Err(Error::KeySpec(format!(
                "{} is not a temporal key spec",
                self.header.key_spec.name()
            )))
        }
    }

    /// The newest version of each key **within this segment**: its newest
    /// timestamp here and the row groups holding a version at it. Keys
    /// absent from the segment, or whose every version here has a null
    /// timestamp, are absent from the map.
    ///
    /// One segment covers only some row groups, so this is not the
    /// index-wide answer that stale-version rejection needs (invariant 5).
    /// Use [`crate::temporal::latest`] over every segment of the key spec.
    pub fn latest<K: AsRef<[u8]>>(&self, keys: &[K]) -> Result<BTreeMap<Vec<u8>, VersionHit>> {
        self.require_temporal()?;
        let mut out = BTreeMap::new();
        self.visit_keys(keys, |k, v| {
            if let Some(hit) = v.chain.latest() {
                out.insert(k.to_vec(), hit);
            }
        })?;
        Ok(out)
    }

    /// The newest version of each key with timestamp `<= ts` **within this
    /// segment**; `ts` is in the dataset column's unit. Use
    /// [`crate::temporal::asof`] for the index-wide answer.
    pub fn asof<K: AsRef<[u8]>>(
        &self,
        keys: &[K],
        ts: i64,
    ) -> Result<BTreeMap<Vec<u8>, VersionHit>> {
        self.require_temporal()?;
        let mut out = BTreeMap::new();
        self.visit_keys(keys, |k, v| {
            if let Some(hit) = v.chain.asof(ts) {
                out.insert(k.to_vec(), hit);
            }
        })?;
        Ok(out)
    }

    /// The full stored value (membership bitmap and version chain) of each
    /// key present.
    pub fn get<K: AsRef<[u8]>>(&self, keys: &[K]) -> Result<BTreeMap<Vec<u8>, KeyValue>> {
        let mut out = BTreeMap::new();
        self.visit_keys(keys, |k, v| {
            out.insert(k.to_vec(), v);
        })?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_hint_caps_a_corrupt_block_count_by_the_index_bytes() {
        // a block count far above what the index bytes can hold
        assert_eq!(reserve_hint(u64::from(u32::MAX), 360, MIN_KEYED_ENTRY), 10);
        assert_eq!(reserve_hint(u64::from(u32::MAX), 240, MIN_BLOOM_ENTRY), 10);
        assert_eq!(reserve_hint(u64::from(u32::MAX), 240, MIN_RANGE_ENTRY), 10);
        // an honest count below the cap is kept
        assert_eq!(reserve_hint(5, 360, MIN_KEYED_ENTRY), 5);
        // an empty index reserves nothing, whatever it claims
        assert_eq!(reserve_hint(1_000, 0, MIN_KEYED_ENTRY), 0);
        // a zero block count reserves nothing
        assert_eq!(reserve_hint(0, 360, MIN_KEYED_ENTRY), 0);
        // a degenerate entry size never divides by zero
        assert_eq!(reserve_hint(7, 360, 0), 0);
    }
}
