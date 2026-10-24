//! Segment writer. Keys may arrive in any order; [`SegmentWriter`] sorts them
//! and hands them to a streaming emitter that writes one block at a time.
//! [`crate::merge`] drives the same emitters from already-sorted inputs.

use std::collections::{BTreeMap, HashSet};

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Schema};
use bellwether_types::{Error, Hash, KeySpec, Result, RowGroupId};
use roaring::RoaringBitmap;

use super::format::{
    len_u32, spec_types, ColumnType, Layout, Put, SegmentHeader, FOOTER_LEN, MAGIC,
};
use crate::bloom::{BloomHash, CanonicalHash, FilterHash, Sbbf};
use crate::encoding::{self, KeyColumns, NEG_ZERO_KEY, POS_ZERO_KEY};
use crate::temporal::{
    BitemporalBuffer, BitemporalChain, KeyBounds, VersionBuffer, VersionChain,
};

/// The bounds a range segment stores for a row group, with the Parquet
/// statistics reading rule for signed zero applied: a minimum of +0.0 is
/// widened to -0.0 and a maximum of -0.0 to +0.0, so a probe for either zero
/// never prunes a row group holding the other (`docs/format.md`). Widening
/// only ever enlarges a range, so it cannot cause a false negative.
pub(crate) fn stored_bounds(mut min: Vec<u8>, mut max: Vec<u8>) -> (Vec<u8>, Vec<u8>) {
    if min == POS_ZERO_KEY {
        min = NEG_ZERO_KEY.to_vec();
    }
    if max == NEG_ZERO_KEY {
        max = POS_ZERO_KEY.to_vec();
    }
    (min, max)
}

/// Tuning knobs. None of them change what a lookup returns, only block
/// sizes; the reader learns every size from the block index.
#[derive(Clone, Debug)]
pub struct WriterOptions {
    /// Maximum keys per keyed block.
    pub block_keys: usize,
    /// A keyed block is also closed once its keys total this many bytes.
    pub block_key_bytes: usize,
    /// Target false positive rate of each keyed block's bloom filter.
    pub block_bloom_fpp: f64,
    /// Maximum entries per min/max block.
    pub range_block_entries: usize,
}

impl Default for WriterOptions {
    fn default() -> Self {
        Self {
            block_keys: 1024,
            block_key_bytes: 64 * 1024,
            block_bloom_fpp: 0.01,
            range_block_entries: 256,
        }
    }
}

/// A finished segment, ready to store at [`EncodedSegment::path`].
#[derive(Clone, Debug)]
pub struct EncodedSegment {
    /// BLAKE3 of `bytes`; the segment's name.
    pub id: Hash,
    /// The file contents.
    pub bytes: Vec<u8>,
    /// The key spec indexed.
    pub key_spec: KeySpec,
    /// Distinct keys (keyed layouts), sum of per-row-group distinct keys
    /// (bloom), or row groups (range).
    pub entries: u64,
    /// First and last row group id the segment records, inclusive: the row
    /// groups its writer processed when the writer was told
    /// ([`SegmentWriter::set_rowgroup_range`]; merge records the hull of
    /// its inputs' ranges), otherwise the smallest range holding every row
    /// group a key references. `None` for a segment that records neither.
    pub rowgroup_range: Option<(RowGroupId, RowGroupId)>,
}

impl EncodedSegment {
    /// `segments/<blake3>.seg`.
    #[must_use]
    pub fn path(&self) -> String {
        segment_path(&self.id)
    }
}

/// `segments/<blake3>.seg` for a segment id.
#[must_use]
pub fn segment_path(id: &Hash) -> String {
    format!("segments/{}.seg", id.to_hex())
}

// ---------------------------------------------------------------------------
// Shared file framing
// ---------------------------------------------------------------------------

pub(crate) struct Framing {
    pub(crate) out: Vec<u8>,
    spec: KeySpec,
    entries: u64,
    rg: Option<(u32, u32)>,
    blocks: u32,
    index: Vec<u8>,
}

impl Framing {
    fn start(header: &SegmentHeader) -> Result<Self> {
        header.validate(header.storage_version, header.encoding_version)?;
        let json = serde_json::to_vec(header)?;
        let mut out = Vec::with_capacity(4096);
        out.extend_from_slice(&MAGIC);
        out.put_u32(header.storage_version);
        out.put_u32(header.encoding_version);
        out.put_u32(len_u32(json.len())?);
        out.extend_from_slice(&json);
        Ok(Self {
            out,
            spec: header.key_spec.clone(),
            entries: 0,
            rg: None,
            blocks: 0,
            index: Vec::new(),
        })
    }

    fn note_rg(&mut self, lo: u32, hi: u32) {
        self.rg = Some(match self.rg {
            None => (lo, hi),
            Some((a, b)) => (a.min(lo), b.max(hi)),
        });
    }

    /// Records `lo..=hi` as the row groups the writer processed, replacing
    /// the range noted from keys. Every row group a key references must lie
    /// inside it.
    fn set_processed(&mut self, lo: u32, hi: u32) -> Result<()> {
        if lo > hi {
            return Err(Error::Other(format!(
                "processed row-group range {lo}..={hi} is empty"
            )));
        }
        if let Some((a, b)) = self.rg {
            if a < lo || b > hi {
                return Err(Error::Other(format!(
                    "keys reference row groups {a}..={b}, outside the processed range {lo}..={hi}"
                )));
            }
        }
        self.rg = Some((lo, hi));
        Ok(())
    }

    fn pos(&self) -> u64 {
        self.out.len() as u64
    }

    fn finish(mut self) -> EncodedSegment {
        let index_offset = self.pos();
        let mut prelude = Vec::with_capacity(21 + self.index.len());
        prelude.put_u64(self.entries);
        let (lo, hi) = self.rg.unwrap_or((0, 0));
        prelude.put_u8(u8::from(self.rg.is_some()));
        prelude.put_u32(lo);
        prelude.put_u32(hi);
        prelude.put_u32(self.blocks);
        self.out.extend_from_slice(&prelude);
        self.out.extend_from_slice(&self.index);
        let index_len = self.pos() - index_offset;
        self.out.reserve(FOOTER_LEN);
        self.out.put_u64(index_offset);
        self.out.put_u64(index_len);
        self.out.extend_from_slice(&MAGIC);
        EncodedSegment {
            id: Hash::of(&self.out),
            bytes: self.out,
            key_spec: self.spec,
            entries: self.entries,
            rowgroup_range: self.rg.map(|(a, b)| (RowGroupId(a), RowGroupId(b))),
        }
    }
}

// ---------------------------------------------------------------------------
// Keyed (exact, temporal)
// ---------------------------------------------------------------------------

/// Roaring's cookie for the serialized form without run containers, the only
/// form `RoaringBitmap::serialize_into` writes.
const NO_RUN_COOKIE: [u8; 4] = [0x3a, 0x30, 0x00, 0x00];

/// The value stored for one key in a keyed layout.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct KeyValue {
    /// Every row group holding the key.
    pub bitmap: RoaringBitmap,
    /// Version chain (`KeyedTemporal`).
    pub chain: VersionChain,
    /// Per-row-group `[min_ts, max_ts]` bounds (`KeyedBounds`).
    pub bounds: KeyBounds,
    /// Bitemporal `(valid, known, rg)` chain (`KeyedTemporal2`).
    pub chain2: BitemporalChain,
}

impl KeyValue {
    pub(crate) fn encode(&self, layout: Layout, out: &mut Vec<u8>) -> Result<()> {
        let mut bm = Vec::with_capacity(self.bitmap.serialized_size());
        self.bitmap
            .serialize_into(&mut bm)
            .map_err(|e| Error::Other(e.to_string()))?;
        match layout {
            Layout::KeyedTemporal => {
                out.put_bytes(&bm)?;
                self.chain.write_to(out)?;
            }
            Layout::KeyedBounds => {
                out.put_bytes(&bm)?;
                self.bounds.write_to(out)?;
            }
            Layout::KeyedTemporal2 => {
                out.put_bytes(&bm)?;
                self.chain2.write_to(out)?;
            }
            _ => {
                out.extend_from_slice(&bm);
            }
        }
        Ok(())
    }

    /// Parses a value record. The bitmap must be in roaring's no-run
    /// serialized form (cookie `3a 30 00 00`, the only form the writer
    /// emits), fill its bytes exactly, and be non-empty: every stored key is
    /// held by at least one row group (`docs/format.md`). Anything else is
    /// corruption and is refused, never decoded into a wrong answer.
    pub(crate) fn decode(bytes: &[u8], layout: Layout) -> Result<Self> {
        let read_bm = |b: &[u8]| -> Result<RoaringBitmap> {
            if b.get(..4) != Some(&NO_RUN_COOKIE[..]) {
                return Err(Error::Other(
                    "corrupt segment: bitmap is not in the no-run serialized form".into(),
                ));
            }
            let bm = RoaringBitmap::deserialize_from(b)
                .map_err(|e| Error::Other(format!("corrupt segment bitmap: {e}")))?;
            if bm.is_empty() || bm.serialized_size() != b.len() {
                return Err(Error::Other(
                    "corrupt segment: keyed bitmap is empty or does not fill its record".into(),
                ));
            }
            Ok(bm)
        };
        match layout {
            Layout::KeyedTemporal => {
                let mut c = super::format::Cursor::new(bytes);
                let bitmap = read_bm(c.bytes()?)?;
                Ok(Self {
                    bitmap,
                    chain: VersionChain::read_from(c.rest())?,
                    bounds: KeyBounds::new(),
                    chain2: BitemporalChain::new(),
                })
            }
            Layout::KeyedBounds => {
                let mut c = super::format::Cursor::new(bytes);
                let bitmap = read_bm(c.bytes()?)?;
                let bounds = KeyBounds::read_from(c.rest())?;
                if bounds
                    .ranges()
                    .iter()
                    .any(|&(rg, _, _)| !bitmap.contains(rg.0))
                {
                    return Err(Error::Other(
                        "corrupt segment: key bounds reference a row group not in bitmap".into(),
                    ));
                }
                Ok(Self {
                    bitmap,
                    chain: VersionChain::new(),
                    bounds,
                    chain2: BitemporalChain::new(),
                })
            }
            Layout::KeyedTemporal2 => {
                let mut c = super::format::Cursor::new(bytes);
                let bitmap = read_bm(c.bytes()?)?;
                let chain2 = BitemporalChain::read_from(c.rest())?;
                if chain2
                    .versions()
                    .iter()
                    .any(|&(_, _, rg)| !bitmap.contains(rg.0))
                {
                    return Err(Error::Other(
                        "corrupt segment: bitemporal chain references a row group not in bitmap"
                            .into(),
                    ));
                }
                Ok(Self {
                    bitmap,
                    chain: VersionChain::new(),
                    bounds: KeyBounds::new(),
                    chain2,
                })
            }
            _ => Ok(Self {
                bitmap: read_bm(bytes)?,
                chain: VersionChain::new(),
                bounds: KeyBounds::new(),
                chain2: BitemporalChain::new(),
            }),
        }
    }
}

pub(crate) struct KeyedEmitter {
    f: Framing,
    layout: Layout,
    opts: WriterOptions,
    keys: Vec<Vec<u8>>,
    key_bytes: usize,
    values: Vec<u8>,
    value_lens: Vec<u32>,
}

impl KeyedEmitter {
    pub(crate) fn new(header: &SegmentHeader, opts: &WriterOptions) -> Result<Self> {
        Ok(Self {
            f: Framing::start(header)?,
            layout: header.layout,
            opts: opts.clone(),
            keys: Vec::new(),
            key_bytes: 0,
            values: Vec::new(),
            value_lens: Vec::new(),
        })
    }

    /// Widens the recorded row-group range to include `lo..=hi` (merge
    /// records the hull of its inputs' ranges).
    pub(crate) fn cover(&mut self, lo: u32, hi: u32) {
        self.f.note_rg(lo, hi);
    }

    /// Records `lo..=hi` as the row groups the writer processed.
    pub(crate) fn processed(&mut self, lo: u32, hi: u32) -> Result<()> {
        self.f.set_processed(lo, hi)
    }

    /// Keys must arrive strictly ascending, each held by at least one row
    /// group.
    pub(crate) fn push(&mut self, key: &[u8], value: &KeyValue) -> Result<()> {
        if let Some(last) = self.keys.last() {
            if key <= last.as_slice() {
                return Err(Error::Other("keyed emitter: keys out of order".into()));
            }
        }
        let (Some(lo), Some(hi)) = (value.bitmap.min(), value.bitmap.max()) else {
            return Err(Error::Other(
                "keyed emitter: a key must be held by at least one row group".into(),
            ));
        };
        self.f.note_rg(lo, hi);
        let before = self.values.len();
        value.encode(self.layout, &mut self.values)?;
        self.value_lens.push(len_u32(self.values.len() - before)?);
        self.key_bytes += key.len();
        self.keys.push(key.to_vec());
        self.f.entries += 1;
        if self.keys.len() >= self.opts.block_keys.max(1)
            || self.key_bytes >= self.opts.block_key_bytes
        {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        let (Some(first), Some(last)) = (self.keys.first(), self.keys.last()) else {
            return Ok(());
        };
        let f = &mut self.f;
        // values
        let values_off = f.pos();
        f.out.extend_from_slice(&self.values);
        // bloom over canonical keys
        let mut sbbf =
            Sbbf::<CanonicalHash>::with_ndv_fpp(self.keys.len() as u64, self.opts.block_bloom_fpp)?;
        for k in &self.keys {
            sbbf.insert(k);
        }
        let bloom_off = f.pos();
        sbbf.write_to(&mut f.out);
        let bloom_len = len_u32(sbbf.num_bytes())?;
        // body
        let body_off = f.pos();
        let n = len_u32(self.keys.len())?;
        f.out.put_u32(n);
        let mut acc = 0u32;
        f.out.put_u32(0);
        for k in &self.keys {
            acc = acc
                .checked_add(len_u32(k.len())?)
                .ok_or_else(|| Error::Other("block too large".into()))?;
            f.out.put_u32(acc);
        }
        for k in &self.keys {
            f.out.extend_from_slice(k);
        }
        let mut off = values_off;
        for &len in &self.value_lens {
            f.out.put_u64(off);
            f.out.put_u32(len);
            off += u64::from(len);
        }
        let body_len = len_u32(usize::try_from(f.pos() - body_off).unwrap_or(usize::MAX))?;
        // index entry
        f.index.put_bytes(first)?;
        f.index.put_bytes(last)?;
        f.index.put_u64(bloom_off);
        f.index.put_u32(bloom_len);
        f.index.put_u64(body_off);
        f.index.put_u32(body_len);
        f.index.put_u32(n);
        f.blocks += 1;
        self.keys.clear();
        self.values.clear();
        self.value_lens.clear();
        self.key_bytes = 0;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<EncodedSegment> {
        self.flush()?;
        Ok(self.f.finish())
    }
}

// ---------------------------------------------------------------------------
// Bloom per row group
// ---------------------------------------------------------------------------

pub(crate) struct BloomEmitter {
    f: Framing,
    last: Option<u32>,
}

impl BloomEmitter {
    pub(crate) fn new(header: &SegmentHeader) -> Result<Self> {
        Ok(Self {
            f: Framing::start(header)?,
            last: None,
        })
    }

    /// Widens the recorded row-group range to include `lo..=hi` (merge
    /// records the hull of its inputs' ranges).
    pub(crate) fn cover(&mut self, lo: u32, hi: u32) {
        self.f.note_rg(lo, hi);
    }

    /// Records `lo..=hi` as the row groups the writer processed.
    pub(crate) fn processed(&mut self, lo: u32, hi: u32) -> Result<()> {
        self.f.set_processed(lo, hi)
    }

    /// Row groups must arrive in ascending order. A row group may repeat
    /// (merge keeps filters of different sizes as separate entries); a
    /// lookup unions over every entry of a row group.
    pub(crate) fn push(&mut self, rg: u32, ndv: u64, bitset: &[u8]) -> Result<()> {
        if self.last.is_some_and(|l| rg < l) {
            return Err(Error::Other(
                "bloom emitter: row groups out of order".into(),
            ));
        }
        self.last = Some(rg);
        let off = self.f.pos();
        self.f.out.extend_from_slice(bitset);
        self.f.index.put_u32(rg);
        self.f.index.put_u64(off);
        self.f.index.put_u32(len_u32(bitset.len())?);
        self.f.index.put_u64(ndv);
        self.f.entries = self.f.entries.saturating_add(ndv);
        self.f.blocks += 1;
        self.f.note_rg(rg, rg);
        Ok(())
    }

    pub(crate) fn finish(self) -> EncodedSegment {
        self.f.finish()
    }
}

// ---------------------------------------------------------------------------
// Min/max per row group
// ---------------------------------------------------------------------------

pub(crate) struct RangeEmitter {
    f: Framing,
    block_entries: usize,
    pending: Vec<(Vec<u8>, Vec<u8>, u32)>,
    last: Option<(Vec<u8>, Vec<u8>, u32)>,
}

impl RangeEmitter {
    pub(crate) fn new(header: &SegmentHeader, opts: &WriterOptions) -> Result<Self> {
        Ok(Self {
            f: Framing::start(header)?,
            block_entries: opts.range_block_entries.max(1),
            pending: Vec::new(),
            last: None,
        })
    }

    /// Widens the recorded row-group range to include `lo..=hi` (merge
    /// records the hull of its inputs' ranges).
    pub(crate) fn cover(&mut self, lo: u32, hi: u32) {
        self.f.note_rg(lo, hi);
    }

    /// Records `lo..=hi` as the row groups the writer processed.
    pub(crate) fn processed(&mut self, lo: u32, hi: u32) -> Result<()> {
        self.f.set_processed(lo, hi)
    }

    /// Entries must arrive in ascending `(min, max, rg)` order.
    pub(crate) fn push(&mut self, min: &[u8], max: &[u8], rg: u32) -> Result<()> {
        if min > max {
            return Err(Error::Other("range entry with min > max".into()));
        }
        let e = (min.to_vec(), max.to_vec(), rg);
        if self.last.as_ref().is_some_and(|l| &e <= l) {
            return Err(Error::Other("range emitter: entries out of order".into()));
        }
        self.last = Some(e.clone());
        self.pending.push(e);
        self.f.entries += 1;
        self.f.note_rg(rg, rg);
        if self.pending.len() >= self.block_entries {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        let Some(first) = self.pending.first() else {
            return Ok(());
        };
        let max_max = self
            .pending
            .iter()
            .map(|e| &e.1)
            .max()
            .cloned()
            .unwrap_or_default();
        let off = self.f.pos();
        let n = len_u32(self.pending.len())?;
        self.f.out.put_u32(n);
        for (min, max, rg) in &self.pending {
            self.f.out.put_u32(*rg);
            self.f.out.put_bytes(min)?;
            self.f.out.put_bytes(max)?;
        }
        let len = len_u32(usize::try_from(self.f.pos() - off).unwrap_or(usize::MAX))?;
        let first_min = first.0.clone();
        self.f.index.put_bytes(&first_min)?;
        self.f.index.put_bytes(&max_max)?;
        self.f.index.put_u64(off);
        self.f.index.put_u32(len);
        self.f.index.put_u32(n);
        self.f.blocks += 1;
        self.pending.clear();
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<EncodedSegment> {
        self.flush()?;
        Ok(self.f.finish())
    }
}

// ---------------------------------------------------------------------------
// Public writer
// ---------------------------------------------------------------------------

#[derive(Default)]
struct KeyAcc {
    bitmap: RoaringBitmap,
    versions: VersionBuffer,
    bounds: KeyBounds,
    versions2: BitemporalBuffer,
}

enum BloomAcc {
    /// Canonical hashes of the row group's distinct keys.
    Hashes(HashSet<CanonicalHash>),
    /// A ready filter (for example one reused from a Parquet file).
    Filter { ndv: u64, bitset: Vec<u8> },
}

enum Acc {
    Keyed(BTreeMap<Vec<u8>, KeyAcc>),
    Bloom(BTreeMap<u32, BloomAcc>),
    Range(BTreeMap<u32, (Vec<u8>, Vec<u8>)>),
}

/// Dataset types of the spec's columns, from the dataset schema: the Arrow
/// types the encoder converts to, and their recorded form for the header.
#[derive(Clone, Debug)]
struct DatasetTypes {
    keys: Vec<DataType>,
    ts: Option<DataType>,
    known_ts: Option<DataType>,
    recorded_keys: Vec<ColumnType>,
    recorded_ts: Option<ColumnType>,
    recorded_known_ts: Option<ColumnType>,
}

/// Builds one segment for one key spec.
///
/// Keys are canonical-encoded tuples ([`crate::encoding`]). Callers that hold
/// Arrow data construct the writer with [`SegmentWriter::for_dataset`] and
/// call [`SegmentWriter::add_batch`]: every key column is converted to its
/// dataset type before encoding (a file at another decimal scale or time
/// unit encodes identically to the dataset's), a column that cannot convert
/// is an error rather than a silent miss. Nulls follow ADR 014 as amended: a
/// row whose leading key column is null is skipped, and a null trailing key
/// column is encoded as the null marker.
///
/// The writer holds the distinct keys of the segment in memory until
/// [`SegmentWriter::finish`]; the build crate bounds that by capping segment
/// size (`docs/format.md`, build-side memory).
pub struct SegmentWriter {
    spec: KeySpec,
    opts: WriterOptions,
    acc: Acc,
    dataset: Option<DatasetTypes>,
    bloom_hash: Option<BloomHash>,
    /// Row groups the writer processed, inclusive, if the caller said.
    processed: Option<(u32, u32)>,
}

/// Validates a key spec for indexing.
pub fn validate_spec(spec: &KeySpec) -> Result<()> {
    if spec.columns().is_empty() {
        return Err(Error::KeySpec("key spec needs at least one column".into()));
    }
    if let KeySpec::Bloom { fpr, .. } = spec {
        if !(*fpr > 0.0 && *fpr < 1.0) {
            return Err(Error::KeySpec(format!(
                "bloom fpr must be strictly between 0 and 1, got {fpr}"
            )));
        }
    }
    Ok(())
}

impl SegmentWriter {
    /// A writer for already-encoded keys ([`SegmentWriter::insert`] and
    /// friends). [`SegmentWriter::add_batch`] needs
    /// [`SegmentWriter::for_dataset`] instead.
    pub fn new(spec: KeySpec, opts: WriterOptions) -> Result<Self> {
        validate_spec(&spec)?;
        let acc = match Layout::for_spec(&spec) {
            Layout::Keyed
            | Layout::KeyedTemporal
            | Layout::KeyedBounds
            | Layout::KeyedTemporal2 => Acc::Keyed(BTreeMap::new()),
            Layout::BloomPerRowGroup => Acc::Bloom(BTreeMap::new()),
            Layout::MinMax => Acc::Range(BTreeMap::new()),
        };
        Ok(Self {
            spec,
            opts,
            acc,
            dataset: None,
            bloom_hash: None,
            processed: None,
        })
    }

    /// A writer for Arrow batches of a dataset whose schema (as reported by
    /// `Source::schema`) is `schema`. The spec's key columns and timestamp
    /// column take their declared types from it, and the segment header
    /// records those types (`key_types`, `ts_type`).
    pub fn for_dataset(spec: KeySpec, schema: &Schema, opts: WriterOptions) -> Result<Self> {
        let mut w = Self::new(spec, opts)?;
        let (keys, ts, known_ts) = spec_types(&w.spec, schema)?;
        let recorded_keys = keys.iter().map(ColumnType::of).collect::<Result<_>>()?;
        let recorded_ts = ts.as_ref().map(ColumnType::of).transpose()?;
        let recorded_known_ts = known_ts.as_ref().map(ColumnType::of).transpose()?;
        w.dataset = Some(DatasetTypes {
            keys,
            ts,
            known_ts,
            recorded_keys,
            recorded_ts,
            recorded_known_ts,
        });
        Ok(w)
    }

    /// The key spec.
    #[must_use]
    pub fn key_spec(&self) -> &KeySpec {
        &self.spec
    }

    /// Records the row groups this writer processed, `range` half-open and
    /// non-empty. The segment stores them as its row-group range in place
    /// of the default (the smallest range holding every row group a key
    /// references), so a segment whose row groups held no keys still
    /// records them, and two segments that processed different row groups
    /// never have the same bytes, and so never the same id. A builder that
    /// claims coverage by this range must set it (`docs/format.md`). Every
    /// row group a key references must lie inside it; `finish` refuses
    /// otherwise.
    pub fn set_rowgroup_range(&mut self, range: std::ops::Range<RowGroupId>) -> Result<()> {
        if range.start >= range.end {
            return Err(Error::Other(format!(
                "processed row-group range {}..{} is empty",
                range.start.0, range.end.0
            )));
        }
        self.processed = Some((range.start.0, range.end.0 - 1));
        Ok(())
    }

    /// Checks that keys can be recorded for row group `rg`: a bloom segment
    /// holding reused `parquet_plain` filters takes no canonical keys, and a
    /// row group that already has a ready filter takes no keys. Mutates
    /// nothing.
    fn check_insertable(&self, rg: RowGroupId) -> Result<()> {
        if let Acc::Bloom(m) = &self.acc {
            if self.bloom_hash == Some(BloomHash::ParquetPlain) {
                return Err(Error::KeySpec(
                    "this bloom segment holds parquet_plain filters; canonical keys cannot be mixed in"
                        .into(),
                ));
            }
            if matches!(m.get(&rg.0), Some(BloomAcc::Filter { .. })) {
                return Err(Error::KeySpec(format!(
                    "row group {} already has a ready filter",
                    rg.0
                )));
            }
        }
        Ok(())
    }

    /// Records that row group `rg` holds `key`. For temporal specs use
    /// [`SegmentWriter::insert_version`]; calling this on one is an error.
    /// On error nothing is recorded.
    pub fn insert(&mut self, key: &[u8], rg: RowGroupId) -> Result<()> {
        if matches!(
            self.spec,
            KeySpec::Temporal { .. } | KeySpec::Bounds { .. } | KeySpec::Temporal2 { .. }
        ) {
            return Err(Error::KeySpec(
                "temporal/bounds segments need a timestamp; use insert_version, insert_bound, or insert_bitemporal"
                    .into(),
            ));
        }
        self.check_insertable(rg)?;
        match &mut self.acc {
            Acc::Keyed(m) => {
                if let Some(v) = m.get_mut(key) {
                    v.bitmap.insert(rg.0);
                } else {
                    let mut v = KeyAcc::default();
                    v.bitmap.insert(rg.0);
                    m.insert(key.to_vec(), v);
                }
            }
            Acc::Bloom(m) => {
                self.bloom_hash = Some(BloomHash::Canonical);
                if let BloomAcc::Hashes(h) = m
                    .entry(rg.0)
                    .or_insert_with(|| BloomAcc::Hashes(HashSet::new()))
                {
                    h.insert(CanonicalHash::of(key));
                }
            }
            Acc::Range(m) => {
                let e = m
                    .entry(rg.0)
                    .or_insert_with(|| (key.to_vec(), key.to_vec()));
                if key < e.0.as_slice() {
                    e.0 = key.to_vec();
                }
                if key > e.1.as_slice() {
                    e.1 = key.to_vec();
                }
            }
        }
        Ok(())
    }

    /// Records a version of `key` at `ts` in row group `rg` (temporal only).
    /// `ts` is in the dataset column's unit. A `None` timestamp records
    /// membership but never wins (ADR 014).
    pub fn insert_version(&mut self, key: &[u8], ts: Option<i64>, rg: RowGroupId) -> Result<()> {
        let Acc::Keyed(m) = &mut self.acc else {
            return Err(Error::KeySpec(
                "insert_version on a non-temporal spec".into(),
            ));
        };
        if !matches!(self.spec, KeySpec::Temporal { .. }) {
            return Err(Error::KeySpec(
                "insert_version on a non-temporal spec".into(),
            ));
        }
        if let Some(v) = m.get_mut(key) {
            v.bitmap.insert(rg.0);
            v.versions.push(ts, rg);
        } else {
            let mut v = KeyAcc::default();
            v.bitmap.insert(rg.0);
            v.versions.push(ts, rg);
            m.insert(key.to_vec(), v);
        }
        Ok(())
    }

    /// Records a timestamp bound sample of `key` at `ts` in row group `rg`
    /// (`KeySpec::Bounds` only). `None` records key membership in `rg` without
    /// widening the timestamp range.
    pub fn insert_bound(&mut self, key: &[u8], ts: Option<i64>, rg: RowGroupId) -> Result<()> {
        let Acc::Keyed(m) = &mut self.acc else {
            return Err(Error::KeySpec("insert_bound on a non-bounds spec".into()));
        };
        if !matches!(self.spec, KeySpec::Bounds { .. }) {
            return Err(Error::KeySpec("insert_bound on a non-bounds spec".into()));
        }
        let v = m.entry(key.to_vec()).or_default();
        v.bitmap.insert(rg.0);
        v.bounds.record(rg, ts);
        Ok(())
    }

    /// Records a bitemporal version of `key` at `(valid, known)` in row group
    /// `rg` (`KeySpec::Temporal2` only). Nulls in either clock record key
    /// membership in `rg` but never enter the version chain (ADR 014).
    pub fn insert_bitemporal(
        &mut self,
        key: &[u8],
        valid: Option<i64>,
        known: Option<i64>,
        rg: RowGroupId,
    ) -> Result<()> {
        let Acc::Keyed(m) = &mut self.acc else {
            return Err(Error::KeySpec(
                "insert_bitemporal on a non-temporal2 spec".into(),
            ));
        };
        if !matches!(self.spec, KeySpec::Temporal2 { .. }) {
            return Err(Error::KeySpec(
                "insert_bitemporal on a non-temporal2 spec".into(),
            ));
        }
        let v = m.entry(key.to_vec()).or_default();
        v.bitmap.insert(rg.0);
        v.versions2.push(valid, known, rg);
        Ok(())
    }

    /// Records a row group's `[min, max]` directly, for example from footer
    /// statistics (range only). Bounds need not be attained values. Widens
    /// any range already recorded for `rg`. Like every stored range bound, a
    /// +0.0 minimum is stored as -0.0 and a -0.0 maximum as +0.0.
    pub fn insert_min_max(&mut self, rg: RowGroupId, min: &[u8], max: &[u8]) -> Result<()> {
        let Acc::Range(m) = &mut self.acc else {
            return Err(Error::KeySpec("insert_min_max on a non-range spec".into()));
        };
        if min > max {
            return Err(Error::Other("min > max".into()));
        }
        let e = m
            .entry(rg.0)
            .or_insert_with(|| (min.to_vec(), max.to_vec()));
        if min < e.0.as_slice() {
            e.0 = min.to_vec();
        }
        if max > e.1.as_slice() {
            e.1 = max.to_vec();
        }
        Ok(())
    }

    /// Stores a ready filter for row group `rg` (bloom only), for example an
    /// embedded Parquet filter (`PlainHash`, single-column specs only). A
    /// segment holds filters of one hash kind; the kind is recorded in the
    /// header as `bloom_hash`.
    pub fn insert_filter<H: FilterHash>(
        &mut self,
        rg: RowGroupId,
        ndv: u64,
        filter: &Sbbf<H>,
    ) -> Result<()> {
        let Acc::Bloom(m) = &mut self.acc else {
            return Err(Error::KeySpec("insert_filter on a non-bloom spec".into()));
        };
        if H::KIND == BloomHash::ParquetPlain && self.spec.columns().len() != 1 {
            return Err(Error::KeySpec(
                "parquet_plain filters cover exactly one column".into(),
            ));
        }
        if self.bloom_hash.is_some_and(|k| k != H::KIND) {
            return Err(Error::KeySpec(format!(
                "a segment holds filters of one hash kind; this one holds {}",
                self.bloom_hash.map_or("none", BloomHash::as_str)
            )));
        }
        if m.contains_key(&rg.0) {
            return Err(Error::KeySpec(format!(
                "row group {} already has keys or a filter",
                rg.0
            )));
        }
        m.insert(
            rg.0,
            BloomAcc::Filter {
                ndv,
                bitset: filter.to_bytes(),
            },
        );
        self.bloom_hash = Some(H::KIND);
        Ok(())
    }

    /// Indexes every row of `batch` as belonging to row group `rg`. Key
    /// columns are found by name and converted to their dataset types; a row
    /// whose leading key column is null is skipped, and a null trailing key
    /// column is encoded as the null marker (ADR 014, amended).
    ///
    /// All or nothing: the whole batch is encoded into a local buffer first
    /// (one batch, within ADR 018's bound), and keys are recorded only if
    /// every row converts. On error (a missing column, a type that cannot
    /// convert, a value not representable in the dataset type) nothing from
    /// the batch is recorded and the writer stays usable, so a caller that
    /// skips the row group never publishes a segment holding part of it.
    pub fn add_batch(&mut self, rg: RowGroupId, batch: &RecordBatch) -> Result<()> {
        let Some(dataset) = self.dataset.clone() else {
            return Err(Error::KeySpec(
                "add_batch needs the dataset schema; construct the writer with SegmentWriter::for_dataset"
                    .into(),
            ));
        };
        let column = |name: &str| -> Result<ArrayRef> {
            batch
                .column_by_name(name)
                .cloned()
                .ok_or_else(|| Error::KeySpec(format!("column {name} not in batch")))
        };
        let cols = self
            .spec
            .columns()
            .iter()
            .map(|c| column(c))
            .collect::<Result<Vec<_>>>()?;
        let keys = KeyColumns::new(&cols, &dataset.keys)?;
        let ts = match (&self.spec, &dataset.ts) {
            (
                KeySpec::Temporal { ts_column, .. } | KeySpec::Bounds { ts_column, .. },
                Some(declared),
            ) => {
                let arr = column(ts_column)?;
                encoding::check_convertible(arr.data_type(), declared)?;
                Some((arr, declared.clone()))
            }
            (KeySpec::Temporal2 { valid_column, .. }, Some(declared)) => {
                let arr = column(valid_column)?;
                encoding::check_convertible(arr.data_type(), declared)?;
                Some((arr, declared.clone()))
            }
            _ => None,
        };
        let known = match (&self.spec, &dataset.known_ts) {
            (KeySpec::Temporal2 { known_column, .. }, Some(declared)) => {
                let arr = column(known_column)?;
                encoding::check_convertible(arr.data_type(), declared)?;
                Some((arr, declared.clone()))
            }
            _ => None,
        };
        self.check_insertable(rg)?;
        // Stage the batch; nothing is recorded until every row has converted.
        let mut staged: Vec<(Vec<u8>, Option<i64>, Option<i64>)> =
            Vec::with_capacity(batch.num_rows());
        let mut buf = Vec::new();
        for row in 0..batch.num_rows() {
            if !keys.encode_into(row, &mut buf)? {
                continue;
            }
            let t = match &ts {
                Some((arr, declared)) => encoding::ts_value_as(arr.as_ref(), row, declared)?,
                None => None,
            };
            let k = match &known {
                Some((arr, declared)) => encoding::ts_value_as(arr.as_ref(), row, declared)?,
                None => None,
            };
            staged.push((buf.clone(), t, k));
        }
        // Cannot fail: the spec kind picks the method, and check_insertable
        // has passed for this row group.
        for (key, t, k) in &staged {
            match &self.spec {
                KeySpec::Temporal { .. } => self.insert_version(key, *t, rg)?,
                KeySpec::Bounds { .. } => self.insert_bound(key, *t, rg)?,
                KeySpec::Temporal2 { .. } => self.insert_bitemporal(key, *t, *k, rg)?,
                _ => self.insert(key, rg)?,
            }
        }
        Ok(())
    }

    /// Writes the segment.
    pub fn finish(self) -> Result<EncodedSegment> {
        let processed = self.processed;
        let mut header = SegmentHeader::new(self.spec.clone());
        if let Some(d) = &self.dataset {
            header.key_types = Some(d.recorded_keys.clone());
            header.ts_type.clone_from(&d.recorded_ts);
            header.known_ts_type.clone_from(&d.recorded_known_ts);
        }
        match self.acc {
            Acc::Keyed(m) => {
                let mut e = KeyedEmitter::new(&header, &self.opts)?;
                for (k, acc) in m {
                    let v = KeyValue {
                        bitmap: acc.bitmap,
                        chain: acc.versions.finish(),
                        bounds: acc.bounds,
                        chain2: acc.versions2.finish(),
                    };
                    e.push(&k, &v)?;
                }
                if let Some((lo, hi)) = processed {
                    e.processed(lo, hi)?;
                }
                e.finish()
            }
            Acc::Bloom(m) => {
                let KeySpec::Bloom { fpr, .. } = &self.spec else {
                    return Err(Error::KeySpec(
                        "bloom accumulator without bloom spec".into(),
                    ));
                };
                header.bloom_hash = self.bloom_hash.unwrap_or(BloomHash::Canonical);
                let mut e = BloomEmitter::new(&header)?;
                for (rg, acc) in &m {
                    match acc {
                        BloomAcc::Hashes(hashes) => {
                            let mut f =
                                Sbbf::<CanonicalHash>::with_ndv_fpp(hashes.len() as u64, *fpr)?;
                            for h in hashes {
                                f.insert_hash(*h);
                            }
                            e.push(*rg, hashes.len() as u64, &f.to_bytes())?;
                        }
                        BloomAcc::Filter { ndv, bitset } => e.push(*rg, *ndv, bitset)?,
                    }
                }
                if let Some((lo, hi)) = processed {
                    e.processed(lo, hi)?;
                }
                Ok(e.finish())
            }
            Acc::Range(m) => {
                // Widen signed zero before sorting, so entries stay in order.
                let mut entries: Vec<_> = m
                    .into_iter()
                    .map(|(rg, (lo, hi))| {
                        let (lo, hi) = stored_bounds(lo, hi);
                        (lo, hi, rg)
                    })
                    .collect();
                entries.sort();
                let mut e = RangeEmitter::new(&header, &self.opts)?;
                for (lo, hi, rg) in &entries {
                    e.push(lo, hi, *rg)?;
                }
                if let Some((lo, hi)) = processed {
                    e.processed(lo, hi)?;
                }
                e.finish()
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn serialized(bm: &RoaringBitmap) -> Vec<u8> {
        let mut v = Vec::new();
        bm.serialize_into(&mut v).unwrap();
        v
    }

    #[test]
    fn decode_accepts_only_non_empty_no_run_bitmaps_that_fill_their_record() {
        let bm: RoaringBitmap = [1u32, 7].into_iter().collect();
        let good = serialized(&bm);
        assert_eq!(good[..4], NO_RUN_COOKIE);
        assert_eq!(KeyValue::decode(&good, Layout::Keyed).unwrap().bitmap, bm);
        let mut run = good.clone();
        run[0] = 0x3b;
        assert!(KeyValue::decode(&run, Layout::Keyed).is_err(), "run cookie");
        let empty = serialized(&RoaringBitmap::new());
        assert!(
            KeyValue::decode(&empty, Layout::Keyed).is_err(),
            "empty bitmap"
        );
        let mut trailing = good.clone();
        trailing.push(0);
        assert!(
            KeyValue::decode(&trailing, Layout::Keyed).is_err(),
            "trailing bytes"
        );
        // temporal records: a length-prefixed bitmap, then the chain
        let v = KeyValue {
            bitmap: bm,
            chain: VersionChain::from_versions(vec![(3, RowGroupId(1))]),
            ..KeyValue::default()
        };
        let mut rec = Vec::new();
        v.encode(Layout::KeyedTemporal, &mut rec).unwrap();
        assert_eq!(KeyValue::decode(&rec, Layout::KeyedTemporal).unwrap(), v);
        rec[4] = 0x3b; // the bitmap's cookie, after its u32 length
        assert!(KeyValue::decode(&rec, Layout::KeyedTemporal).is_err());
    }

    /// A genuine run-format serialization (cookie `3b 30`): nine containers,
    /// eight one-value arrays and one run container holding {0, 1}. Roaring
    /// decodes it into a valid, non-empty bitmap whose no-run size equals
    /// the record's length, so only the cookie check stands between it and
    /// acceptance.
    fn run_format_record() -> Vec<u8> {
        let mut v = vec![0x3b, 0x30, 0x08, 0x00]; // run cookie, 9 containers
        v.extend_from_slice(&[0x00, 0x01]); // container 8 is a run container
        for key in 0u16..9 {
            v.extend_from_slice(&key.to_le_bytes());
            let cardinality_minus_one: u16 = u16::from(key == 8);
            v.extend_from_slice(&cardinality_minus_one.to_le_bytes());
        }
        v.extend_from_slice(&[0u8; 9 * 4]); // offsets, which readers ignore
        for _ in 0..8 {
            v.extend_from_slice(&5u16.to_le_bytes()); // one value per array
        }
        v.extend_from_slice(&1u16.to_le_bytes()); // one run
        v.extend_from_slice(&0u16.to_le_bytes()); // starting at 0
        v.extend_from_slice(&1u16.to_le_bytes()); // length minus one: {0, 1}
        v
    }

    #[test]
    fn decode_refuses_a_valid_run_format_bitmap() {
        let rec = run_format_record();
        let bm = RoaringBitmap::deserialize_from(&rec[..]).unwrap();
        assert_eq!(bm.len(), 10);
        assert_eq!(bm.serialized_size(), rec.len());
        assert!(KeyValue::decode(&rec, Layout::Keyed).is_err());
    }

    #[test]
    fn the_emitter_refuses_a_key_held_by_no_row_group() {
        let header = SegmentHeader::new(KeySpec::Exact {
            columns: vec!["id".into()],
        });
        let mut e = KeyedEmitter::new(&header, &WriterOptions::default()).unwrap();
        assert!(e.push(b"k", &KeyValue::default()).is_err());
    }

    #[test]
    fn stored_bounds_widen_signed_zero_and_nothing_else() {
        let (pos, neg) = (POS_ZERO_KEY.to_vec(), NEG_ZERO_KEY.to_vec());
        assert_eq!(
            stored_bounds(pos.clone(), pos.clone()),
            (neg.clone(), pos.clone())
        );
        assert_eq!(
            stored_bounds(neg.clone(), neg.clone()),
            (neg.clone(), pos.clone())
        );
        assert_eq!(
            stored_bounds(neg.clone(), pos.clone()),
            (neg.clone(), pos.clone())
        );
        let (one, two) = (vec![4u8, 0xC0], vec![4u8, 0xC1]);
        assert_eq!(stored_bounds(one.clone(), two.clone()), (one, two));
        // a negative minimum and a positive maximum stay as they are
        assert_eq!(
            stored_bounds(vec![4u8, 0x3F], pos.clone()),
            (vec![4u8, 0x3F], pos)
        );
    }

    fn all_kinds() -> Vec<KeySpec> {
        vec![
            KeySpec::Exact {
                columns: vec!["k".into()],
            },
            KeySpec::Bloom {
                columns: vec!["k".into()],
                fpr: 0.01,
            },
            KeySpec::Range { column: "k".into() },
            KeySpec::Temporal {
                columns: vec!["k".into()],
                ts_column: "t".into(),
            },
        ]
    }

    fn keyless(spec: KeySpec, range: Option<std::ops::Range<u32>>) -> EncodedSegment {
        let mut w = SegmentWriter::new(spec, WriterOptions::default()).unwrap();
        if let Some(r) = range {
            w.set_rowgroup_range(RowGroupId(r.start)..RowGroupId(r.end))
                .unwrap();
        }
        w.finish().unwrap()
    }

    /// Two segments whose row groups held no keys must not share bytes, and
    /// so an id, when they processed different row groups: a manifest keyed
    /// by segment id would otherwise keep one and lose the other's coverage.
    #[test]
    fn keyless_segments_over_different_processed_ranges_differ() {
        for spec in all_kinds() {
            let a = keyless(spec.clone(), Some(0..1));
            let b = keyless(spec.clone(), Some(1..3));
            assert_ne!(a.id, b.id, "{}", spec.name());
            assert_eq!(a.rowgroup_range, Some((RowGroupId(0), RowGroupId(0))));
            assert_eq!(b.rowgroup_range, Some((RowGroupId(1), RowGroupId(2))));
            let r = crate::segment::SegmentReader::open(b.bytes.clone()).unwrap();
            assert_eq!(r.rowgroup_range(), Some((RowGroupId(1), RowGroupId(2))));
            assert_eq!(r.entries(), 0);
            // without a processed range, key-less segments of one spec are
            // byte-identical, which is why a builder must set it
            assert_eq!(keyless(spec.clone(), None).id, keyless(spec, None).id);
        }
    }

    #[test]
    fn a_processed_range_must_hold_every_referenced_row_group() {
        let spec = KeySpec::Exact {
            columns: vec!["k".into()],
        };
        let mut w = SegmentWriter::new(spec.clone(), WriterOptions::default()).unwrap();
        w.insert(b"k", RowGroupId(5)).unwrap();
        w.set_rowgroup_range(RowGroupId(0)..RowGroupId(5)).unwrap();
        assert!(w.finish().is_err(), "row group 5 lies outside 0..5");
        let mut w = SegmentWriter::new(spec, WriterOptions::default()).unwrap();
        assert!(w.set_rowgroup_range(RowGroupId(3)..RowGroupId(3)).is_err());
    }

    /// For a keyed segment whose processed range is exactly the range its
    /// keys reference, setting it changes nothing: existing callers' bytes
    /// are unchanged.
    #[test]
    fn a_processed_range_equal_to_the_keyed_range_leaves_the_bytes_unchanged() {
        let make = |range: Option<std::ops::Range<u32>>| {
            let mut w = SegmentWriter::new(
                KeySpec::Exact {
                    columns: vec!["k".into()],
                },
                WriterOptions::default(),
            )
            .unwrap();
            w.insert(b"a", RowGroupId(2)).unwrap();
            w.insert(b"b", RowGroupId(4)).unwrap();
            if let Some(r) = range {
                w.set_rowgroup_range(RowGroupId(r.start)..RowGroupId(r.end))
                    .unwrap();
            }
            w.finish().unwrap()
        };
        assert_eq!(make(None).bytes, make(Some(2..5)).bytes);
        assert_ne!(make(None).bytes, make(Some(0..9)).bytes);
    }
}
