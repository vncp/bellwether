//! Segment reader.
//!
//! Opening reads the preamble, the JSON header, the footer, and the block
//! index; nothing else. A lookup then reads, per block it lands in, the
//! block's bloom bitset and, only if the bloom passes, the block's key
//! directory and the value records of the keys found. At most one block is
//! held at a time; keys are sorted first so consecutive keys reuse it.
//!
//! A storage version 2 segment carries an XXH3-64 checksum beside every
//! region a reader reads (the preamble, header, and index in the footer;
//! each block's filter and directory in its index entry; each value record
//! in its directory entry). The reader checks each region as it reads it, so
//! a corrupt byte is refused with an error, never decoded into a wrong
//! answer. A storage version 1 segment has no checksums and is read as
//! before.

use std::borrow::Cow;
use std::collections::BTreeMap;

use arrow::datatypes::Schema;
use bellwether_types::{Error, KeySpec, Result, RowGroupId};
use roaring::RoaringBitmap;

use super::format::{
    check_versions, checksum, corrupt, footer_len, parse_header, spec_types, ColumnType, Cursor,
    Layout, ReadAt, SegmentHeader, ValueShape, MAGIC, PREAMBLE_LEN,
};
use super::types;
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

/// Checks `bytes` against a stored checksum, when the segment stores one.
fn verified<'b>(bytes: Cow<'b, [u8]>, sum: Option<u64>, what: &str) -> Result<Cow<'b, [u8]>> {
    match sum {
        Some(s) if checksum(&[&bytes]) != s => Err(corrupt(&format!("{what} checksum mismatch"))),
        _ => Ok(bytes),
    }
}

#[derive(Clone, Debug)]
pub(crate) struct KeyedBlock {
    pub(crate) first: Vec<u8>,
    pub(crate) last: Vec<u8>,
    bloom_off: u64,
    bloom_len: u32,
    bloom_sum: Option<u64>,
    body_off: u64,
    body_len: u32,
    body_sum: Option<u64>,
    pub(crate) n: u32,
}

#[derive(Clone, Debug)]
pub(crate) struct BloomEntry {
    pub(crate) rg: u32,
    off: u64,
    len: u32,
    pub(crate) ndv: u64,
    sum: Option<u64>,
}

#[derive(Clone, Debug)]
pub(crate) struct RangeBlock {
    first_min: Vec<u8>,
    max_max: Vec<u8>,
    off: u64,
    len: u32,
    n: u32,
    sum: Option<u64>,
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
    /// Bytes per value reference: 12 (`offset u64 | len u32`) in storage
    /// version 1, 20 (`| checksum u64`) in version 2.
    ref_len: usize,
}

impl<'a> BlockView<'a> {
    /// Parses a block directory. `sums`: the segment stores a checksum per
    /// value record (storage version 2).
    pub(crate) fn parse(body: &'a [u8], expect_n: u32, sums: bool) -> Result<Self> {
        let ref_len = if sums { 20 } else { 12 };
        let mut c = Cursor::new(body);
        let n = c.u32()?;
        if n != expect_n {
            return Err(corrupt("block key count"));
        }
        let n = n as usize;
        let offsets = c.take((n + 1) * 4)?;
        let total = u32_at(offsets, n) as usize;
        let keys = c.take(total)?;
        let refs = c.take(n * ref_len)?;
        if !c.is_done() {
            return Err(corrupt("block trailing bytes"));
        }
        Ok(Self {
            n,
            offsets,
            keys,
            refs,
            ref_len,
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

    /// The value record's offset, length, and checksum (version 2).
    fn value_ref(&self, i: usize) -> (u64, usize, Option<u64>) {
        let b = &self.refs[i * self.ref_len..(i + 1) * self.ref_len];
        let u64_at = |s: &[u8]| {
            let mut o = [0u8; 8];
            o.copy_from_slice(&s[..8]);
            u64::from_le_bytes(o)
        };
        let sum = (self.ref_len == 20).then(|| u64_at(&b[12..]));
        (u64_at(b), u32_at(&b[8..], 0) as usize, sum)
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
    /// A join segment's one-sided key counts (`left_only`, `right_only`),
    /// from the end of its index; `None` for every other layout.
    pub(crate) join_totals: Option<(u64, u64)>,
}

impl<R: ReadAt> SegmentReader<R> {
    /// Opens a segment: checks the preamble versions before parsing anything
    /// else, validates the header against them (versions agree, codec,
    /// layout, `bloom_hash`), loads the index, and, for storage version 2,
    /// checks the footer's checksum over the preamble, header, and index.
    #[allow(clippy::too_many_lines)]
    pub fn open(src: R) -> Result<Self> {
        let size = src.size()?;
        if size < (PREAMBLE_LEN + footer_len(1)) as u64 {
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
        let v2 = storage >= 2;
        let foot_len = footer_len(storage);
        if size < (PREAMBLE_LEN + foot_len) as u64 {
            return Err(corrupt("file too short"));
        }
        let header_len = c.u32()? as usize;
        let header_bytes = src.read_at(PREAMBLE_LEN as u64, header_len)?;
        let header = parse_header(&header_bytes)?;
        header.validate(storage, encoding)?;
        let foot = src.read_at(size - foot_len as u64, foot_len)?;
        let mut c = Cursor::new(&foot);
        let index_off = c.u64()?;
        let index_len = usize::try_from(c.u64()?).map_err(|_| corrupt("index length"))?;
        let index_sum = if v2 { Some(c.u64()?) } else { None };
        if c.take(8)? != MAGIC {
            return Err(corrupt("bad footer magic"));
        }
        let index_bytes = src.read_at(index_off, index_len)?;
        if let Some(sum) = index_sum {
            if checksum(&[&pre, &header_bytes, &index_bytes]) != sum {
                return Err(corrupt("header or index checksum mismatch"));
            }
        }
        let sum = |c: &mut Cursor<'_>| -> Result<Option<u64>> {
            if v2 {
                Ok(Some(c.u64()?))
            } else {
                Ok(None)
            }
        };
        let mut c = Cursor::new(&index_bytes);
        let entries = c.u64()?;
        let has_rg = c.u8()? == 1;
        let (lo, hi) = (c.u32()?, c.u32()?);
        let claimed = c.u32()?;
        let blocks = claimed as usize;
        let reserve = |min_entry| reserve_hint(u64::from(claimed), index_len as u64, min_entry);
        let index = match header.layout {
            Layout::Keyed
            | Layout::KeyedTemporal
            | Layout::KeyedJoin
            | Layout::Bitemporal
            | Layout::KeyedAgg
            | Layout::KeyedTombstone => {
                let mut v = Vec::with_capacity(reserve(MIN_KEYED_ENTRY));
                for _ in 0..blocks {
                    v.push(KeyedBlock {
                        first: c.bytes()?.to_vec(),
                        last: c.bytes()?.to_vec(),
                        bloom_off: c.u64()?,
                        bloom_len: c.u32()?,
                        bloom_sum: sum(&mut c)?,
                        body_off: c.u64()?,
                        body_len: c.u32()?,
                        body_sum: sum(&mut c)?,
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
                        sum: sum(&mut c)?,
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
                        sum: sum(&mut c)?,
                    });
                }
                Index::Range(v)
            }
        };
        let join_totals = if header.layout == Layout::KeyedJoin {
            Some((c.u64()?, c.u64()?))
        } else {
            None
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
            join_totals,
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

    /// Checks that this segment's recorded key types (and temporal ts type,
    /// and the time types of per-key bounds or bitemporal versions) equal
    /// the types `schema` declares for the key spec's columns. A
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
        let want_times =
            super::format::record_time_types(&self.header.key_spec, self.header.layout, schema)?
                .iter()
                .map(ColumnType::of)
                .collect::<Result<Vec<_>>>()?;
        let have_times = self.header.time_types.clone().unwrap_or_default();
        if have_times != want_times {
            return Err(Error::KeySpec(format!(
                "segment for {} recorded its time columns as ({}); the dataset declares ({}); \
                 re-index the folder",
                self.header.key_spec.identity(),
                have_times
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", "),
                want_times
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", "),
            )));
        }
        if have_keys != &want_keys || self.header.ts_type != want_ts {
            return Err(Error::KeySpec(format!(
                "segment for {} was encoded under key types {}; the dataset declares {}; \
                 re-index the folder",
                self.header.key_spec.name(),
                types(have_keys, self.header.ts_type.as_ref()),
                types(&want_keys, want_ts.as_ref()),
            )));
        }
        if self.header.layout == Layout::KeyedTombstone {
            let want = ColumnType::of(&crate::tombstone::column_type(
                &self.header.key_spec,
                schema,
            )?)?;
            if self.header.tombstone_type.as_ref() != Some(&want) {
                return Err(Error::KeySpec(format!(
                    "the tombstone segment of {} recorded its tombstone column as {}; the dataset \
                     declares {want}; re-index the folder",
                    self.header.key_spec.identity(),
                    self.header
                        .tombstone_type
                        .as_ref()
                        .map_or_else(|| "no type".to_string(), ToString::to_string),
                )));
            }
        }
        let want_value = crate::agg::value_type(&self.header.key_spec, schema)?
            .as_ref()
            .map(ColumnType::of)
            .transpose()?;
        if self.header.value_type != want_value {
            let show =
                |t: Option<&ColumnType>| t.map_or_else(|| "none".into(), ToString::to_string);
            return Err(Error::KeySpec(format!(
                "segment for {} aggregated its values as {}; the dataset declares {}; re-index \
                 the folder",
                self.header.key_spec.name(),
                show(self.header.value_type.as_ref()),
                show(want_value.as_ref()),
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

    /// Whether the segment stores checksums (storage version 2).
    pub(crate) fn checked(&self) -> bool {
        self.header.storage_version >= 2
    }

    pub(crate) fn block_body(&self, b: &KeyedBlock) -> Result<Cow<'_, [u8]>> {
        verified(
            self.read(b.body_off, b.body_len)?,
            b.body_sum,
            "block directory",
        )
    }

    fn block_bloom(&self, b: &KeyedBlock) -> Result<Cow<'_, [u8]>> {
        verified(
            self.read(b.bloom_off, b.bloom_len)?,
            b.bloom_sum,
            "block filter",
        )
    }

    pub(crate) fn value_at(&self, view: &BlockView<'_>, i: usize) -> Result<KeyValue> {
        if self.header.layout == Layout::KeyedJoin {
            return Err(Error::KeySpec(format!(
                "{} holds a pair of values per key; read it through bellwether_core::join",
                self.header.key_spec.name()
            )));
        }
        let bytes = self.value_bytes(view, i)?;
        KeyValue::decode(
            &bytes,
            ValueShape::of(&self.header.key_spec, self.header.layout),
            self.header.storage_version,
        )
    }

    /// The checked bytes of value record `i` of a block.
    pub(crate) fn value_bytes(&self, view: &BlockView<'_>, i: usize) -> Result<Cow<'_, [u8]>> {
        let (off, len, sum) = view.value_ref(i);
        verified(self.src.read_at(off, len)?, sum, "value record")
    }

    pub(crate) fn bloom_bitset(&self, e: &BloomEntry) -> Result<Cow<'_, [u8]>> {
        verified(self.read(e.off, e.len)?, e.sum, "row-group filter")
    }

    fn range_block(&self, b: &RangeBlock) -> Result<Cow<'_, [u8]>> {
        verified(self.read(b.off, b.len)?, b.sum, "range block")
    }

    /// Whether the segment stores per-key per-row-group counts (a spec with
    /// `counts`, storage version 2).
    #[must_use]
    pub fn has_counts(&self) -> bool {
        self.header.key_spec.counts()
    }

    /// For each key of `keys` present, `(row group, rows)` for every row
    /// group holding it, ascending. `None` when the segment stores no counts
    /// (never zero counts: a reader cannot tell an uncounted key from an
    /// absent one). Holds the block index, one block, and the values of the
    /// keys asked (ADR 018).
    #[allow(clippy::type_complexity)]
    pub fn counts<K: AsRef<[u8]>>(
        &self,
        keys: &[K],
    ) -> Result<Option<BTreeMap<Vec<u8>, Vec<(RowGroupId, u64)>>>> {
        if !self.has_counts() {
            return Ok(None);
        }
        let mut out = BTreeMap::new();
        let mut missing = false;
        self.visit_keys(keys, |k, v| match v.counted() {
            Some(c) => {
                out.insert(k.to_vec(), c);
            }
            None => missing = true,
        })?;
        if missing {
            return Err(corrupt("a counted segment holds a value without counts"));
        }
        Ok(Some(out))
    }

    /// Whether the segment stores per-key time bounds (an exact spec with
    /// `bounds`, storage version 2).
    #[must_use]
    pub fn has_bounds(&self) -> bool {
        ValueShape::of(&self.header.key_spec, self.header.layout).bounds
    }

    /// For each key of `keys` present, its `[min, max]` time per row group
    /// where it has a non-null time, ascending by row group; a row group
    /// where every row of the key has a null time (the stored all-null
    /// sentinel) is left out, as it overlaps no window. `None` when the
    /// segment stores no bounds (never an empty answer: a reader cannot
    /// tell a segment without bounds from keys without times). Holds the
    /// block index, one block, and the values of the keys asked (ADR 018).
    #[allow(clippy::type_complexity)]
    pub fn bounds<K: AsRef<[u8]>>(
        &self,
        keys: &[K],
    ) -> Result<Option<BTreeMap<Vec<u8>, Vec<crate::sidecar::RgBounds>>>> {
        if !self.has_bounds() {
            return Ok(None);
        }
        let mut out = BTreeMap::new();
        let mut missing = false;
        self.visit_keys(keys, |k, v| match v.bounded() {
            Some(b) => {
                out.insert(
                    k.to_vec(),
                    b.into_iter()
                        .map(|(rg, min, max)| crate::sidecar::RgBounds { rg, min, max })
                        .collect(),
                );
            }
            None => missing = true,
        })?;
        if missing {
            return Err(corrupt(
                "a segment with bounds holds a value without bounds",
            ));
        }
        Ok(Some(out))
    }

    /// For each key of `keys` present, its distinct `(valid, known, row
    /// group)` versions, ascending (a bitemporal segment only; refused
    /// otherwise). Holds the block index, one block, and the values of the
    /// keys asked (ADR 018).
    pub fn triples<K: AsRef<[u8]>>(
        &self,
        keys: &[K],
    ) -> Result<BTreeMap<Vec<u8>, Vec<crate::sidecar::Triple>>> {
        if self.header.layout != Layout::Bitemporal {
            return Err(Error::KeySpec(format!(
                "{} is not a bitemporal key spec",
                self.header.key_spec.identity()
            )));
        }
        let mut out = BTreeMap::new();
        self.visit_keys(keys, |k, v| {
            out.insert(
                k.to_vec(),
                v.triples
                    .iter()
                    .map(|&(valid, known, rg)| crate::sidecar::Triple {
                        valid,
                        known,
                        rg: RowGroupId(rg),
                    })
                    .collect(),
            );
        })?;
        Ok(out)
    }

    /// For each key of `keys` present in an agg key's segment, `(row group,
    /// cell)` for every row group holding it, ascending. An error for a
    /// segment of any other spec. Holds the block index, one block, and the
    /// values of the keys asked (ADR 018).
    #[allow(clippy::type_complexity)]
    pub fn agg_cells<K: AsRef<[u8]>>(
        &self,
        keys: &[K],
    ) -> Result<BTreeMap<Vec<u8>, Vec<(RowGroupId, crate::agg::AggCell)>>> {
        if self.header.layout != Layout::KeyedAgg {
            return Err(Error::KeySpec(format!(
                "{} is not an agg key; its segments hold no aggregates",
                self.header.key_spec.name()
            )));
        }
        let mut out = BTreeMap::new();
        let mut missing = false;
        self.visit_keys(keys, |k, v| match v.agg {
            Some(cells) => {
                out.insert(
                    k.to_vec(),
                    v.bitmap.iter().map(RowGroupId).zip(cells).collect(),
                );
            }
            None => missing = true,
        })?;
        if missing {
            return Err(corrupt("an agg segment holds a value without aggregates"));
        }
        Ok(out)
    }

    /// The keys of block `i` of a keyed segment, ascending; `None` past the
    /// last block. One block (at most a block's key cap) at a time, so a
    /// caller walking every key holds one block per segment (ADR 018).
    pub(crate) fn block_keys(&self, i: usize) -> Result<Option<Vec<Vec<u8>>>> {
        let Index::Keyed(blocks) = &self.index else {
            return Err(Error::KeySpec(format!(
                "{} segments have no per-key values",
                self.header.key_spec.name()
            )));
        };
        let Some(b) = blocks.get(i) else {
            return Ok(None);
        };
        let body = self.block_body(b)?;
        let view = BlockView::parse(&body, b.n, self.checked())?;
        (0..view.len())
            .map(|k| view.key(k).map(<[u8]>::to_vec))
            .collect::<Result<Vec<_>>>()
            .map(Some)
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
        self.visit_found(keys, |key, view, idx| {
            f(key, self.value_at(view, idx)?);
            Ok(())
        })
    }

    /// Visits each distinct key of `keys` present in a keyed segment (any
    /// keyed layout, the join layout included), in ascending order, with
    /// its block's directory and its position there. Per key: block-index
    /// binary search, bloom check, key binary search. A probe tuple that
    /// contains a null matches nothing (ADR 014).
    pub(crate) fn visit_found<K: AsRef<[u8]>>(
        &self,
        keys: &[K],
        mut f: impl FnMut(&[u8], &BlockView<'_>, usize) -> Result<()>,
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
                cur = Some((bi, self.block_bloom(b)?, None));
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
            let view = BlockView::parse(body, b.n, self.checked())?;
            if let Some(idx) = view.find(key)? {
                f(key, &view, idx)?;
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
                    let view = BlockView::parse(&body, b.n, self.checked())?;
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
                    let body = self.range_block(b)?;
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
            let body = self.range_block(b)?;
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

    /// Every `(row group, min, max)` of a range segment, ascending by
    /// `(min, max, rg)` as stored; an error for any other layout.
    #[allow(clippy::type_complexity)]
    pub fn range_entries(&self) -> Result<Vec<(RowGroupId, Vec<u8>, Vec<u8>)>> {
        let mut out = Vec::new();
        self.for_each_range_entry(|min, max, rg| {
            out.push((RowGroupId(rg), min.to_vec(), max.to_vec()));
            Ok(())
        })?;
        Ok(out)
    }

    /// Every stored key of a keyed (exact or temporal) segment with its
    /// value, in key order; an error for any other layout. A whole-segment
    /// read like [`Self::range_entries`], for verification and oracles: it
    /// holds every key, where a lookup holds only the keys asked (ADR 018).
    /// Unlike [`Self::counts`] it returns stored tuples that hold a trailing
    /// null marker, which no probe matches (ADR 014).
    pub fn keyed_entries(&self) -> Result<Vec<(Vec<u8>, KeyValue)>> {
        let Index::Keyed(blocks) = &self.index else {
            return Err(Error::KeySpec(format!(
                "{} is not a keyed segment",
                self.header.key_spec.identity()
            )));
        };
        let mut out = Vec::new();
        for b in blocks {
            let body = self.block_body(b)?;
            let view = BlockView::parse(&body, b.n, self.checked())?;
            for i in 0..view.len() {
                out.push((view.key(i)?.to_vec(), self.value_at(&view, i)?));
            }
        }
        Ok(out)
    }

    pub(crate) fn require_temporal(&self) -> Result<()> {
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
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::segment::{SegmentWriter, WriterOptions};

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

    #[test]
    fn range_entries_returns_sorted_bounds_and_refuses_other_layouts() {
        let mut w = SegmentWriter::new(
            KeySpec::Range {
                column: "ts".into(),
            },
            WriterOptions::default(),
        )
        .unwrap();
        w.insert_min_max(RowGroupId(3), b"b", b"d").unwrap();
        w.insert_min_max(RowGroupId(1), b"a", b"c").unwrap();
        w.insert_min_max(RowGroupId(2), b"b", b"c").unwrap();
        let r = SegmentReader::open(w.finish().unwrap().bytes).unwrap();
        assert_eq!(
            r.range_entries().unwrap(),
            vec![
                (RowGroupId(1), b"a".to_vec(), b"c".to_vec()),
                (RowGroupId(2), b"b".to_vec(), b"c".to_vec()),
                (RowGroupId(3), b"b".to_vec(), b"d".to_vec()),
            ]
        );

        let exact = SegmentWriter::new(
            KeySpec::Exact {
                columns: vec!["k".into()],
                counts: false,
                bounds: None,
            },
            WriterOptions::default(),
        )
        .unwrap()
        .finish()
        .unwrap();
        let er = SegmentReader::open(exact.bytes).unwrap();
        assert!(er.range_entries().is_err());
    }
}
