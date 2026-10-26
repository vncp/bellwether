//! `sketch` sidecar segments: per item, per file, HyperLogLog registers and an
//! optional MinHash signature (`docs/format.md`, "Sketch segments").
//!
//! **Approximate, and never planned with.** A sketch answers
//! `count(distinct item)` ([`SketchTotals::cardinality`]) and how much two
//! items' value sets overlap ([`overlap`]), each with an error bound. No
//! planner path reads a sketch: an approximate number in a plan would break
//! invariant 1. The build crate's planner never opens these segments; the
//! facade reads them only for `cardinality`, `overlap`, and the `why`
//! report's `sketch` column.
//!
//! An *item* is one column, or a tuple of columns encoded like a composite
//! key ([`crate::encoding::KeyColumns`], ADR 014): a row whose leading
//! column is null is not sketched and counts as a null; a null trailing
//! column is the null marker. Each value is hashed once with
//! [`value_hash`]; the low 64 bits feed the HyperLogLog
//! ([`crate::hll::Hll`]), and both halves seed the MinHash slots.
//!
//! # Layout
//!
//! All integers are little-endian.
//!
//! ```text
//! preamble  magic "BWSKETCH" | storage_version u32 | encoding_version u32 | header_len u32
//! header    JSON {storage_version, encoding_version, kind:"sketch", items, item_types?, precision,
//!                 minhash, hash:"xxh3_128", seed, codec:"none", ..unknown preserved}
//! body      entry*; entry = rg_start u32 | rg_end u32 | item*      (one entry per file stretch)
//!           item    = rows u64 | nulls u64 | hll | minhash?
//!           hll     = 0x00 | n u32 | n x (index u32 | rank u8)     sparse, iff 4 + 5n < 2^p
//!                   | 0x01 | 2^p x rank u8                         dense, otherwise
//!           minhash = k x u64                                      iff header minhash = k
//! footer    entries u32 | range_start u32 | range_end u32 | blake3 of every byte before the footer (32) | magic "BWSKETCH"
//! ```
//!
//! Entries are strictly ascending and disjoint, each `rg_start < rg_end`,
//! inside `[range_start, range_end)`, the row groups the writer processed.
//! Sparse indices are strictly ascending, every rank is in `1..=65-p`, and a
//! dense register is at most `65-p`. An item with `rows = 0` has an empty
//! sparse HyperLogLog and every MinHash slot `u64::MAX`. The checksum makes
//! every damaged byte a typed refusal, never a silently different estimate.
//!
//! # Memory bound (ADR 018)
//!
//! The writer holds one open entry (per item `2^p` registers and `k` slots)
//! plus the finished entries in their encoded form; the build caps a
//! segment's entries and bytes. A reader holds the segment bytes and decodes
//! one entry at a time.

use std::collections::BTreeSet;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Schema};
use bellwether_types::format::{ENCODING_VERSION, STORAGE_VERSION};
use bellwether_types::{
    ApproxCardinality, ApproxOverlap, Error, ExactDistinctSource, Hash, OverlapMethod, Result,
    RowGroupId, SketchSpec, SKETCH_CONFIDENCE,
};
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_128_with_seed;

use crate::encoding::{self, KeyColumns};
use crate::hll::{Hll, MAX_PRECISION, MIN_PRECISION};
use crate::segment::ColumnType;

/// First and last eight bytes of every sketch segment.
pub const MAGIC: [u8; 8] = *b"BWSKETCH";

/// Bytes before the JSON header: magic, two versions, header length.
pub const PREAMBLE_LEN: usize = 8 + 4 + 4 + 4;

/// Bytes of the footer: entry count, the processed range, the checksum, and
/// the magic.
pub const FOOTER_LEN: usize = 4 + 4 + 4 + 32 + 8;

/// Seed of [`value_hash`]. The same seed the profiler's fanout pass uses, so
/// a column's merged sketch registers equal the profiler's sketch of it.
pub const HASH_SEED: u64 = 0x6265_6c6c_7765_7468;

/// The header's `hash` value: XXH3-128 over the canonical bytes.
pub const HASH_NAME: &str = "xxh3_128";

/// The header's `kind`.
pub const KIND: &str = "sketch";

/// Longest MinHash signature a sketch may keep.
pub const MAX_MINHASH: u16 = 1024;

/// Standard errors each approximate interval spans on either side.
const SIGMAS: f64 = 3.0;

const CODEC_NONE: &str = "none";

fn corrupt(what: &str) -> Error {
    Error::Other(format!("corrupt sketch segment: {what}"))
}

fn len_u32(n: usize) -> Result<u32> {
    u32::try_from(n).map_err(|_| Error::Other(format!("length {n} exceeds u32")))
}

/// The hash every sketched value is reduced to: XXH3-128 of its canonical
/// bytes under [`HASH_SEED`].
#[must_use]
pub fn value_hash(bytes: &[u8]) -> u128 {
    xxh3_128_with_seed(bytes, HASH_SEED)
}

/// Checks a spec on its own: at least one item, every item one or more
/// non-empty column names without `,`, `+`, or `=` (the short form's
/// separators), no column twice in an item, no item twice, a precision in
/// `4..=18`, and a MinHash length in `1..=1024`.
pub fn validate(spec: &SketchSpec) -> Result<()> {
    let bad = |why: String| {
        Err(Error::KeySpec(format!(
            "{} is not a valid sketch: {why}",
            spec.label()
        )))
    };
    if spec.items.is_empty() {
        return bad("it sketches no column".into());
    }
    let mut seen = BTreeSet::new();
    for item in &spec.items {
        if item.is_empty() {
            return bad("an item names no column".into());
        }
        let mut cols = BTreeSet::new();
        for c in item {
            if c.is_empty() {
                return bad("a column name is empty".into());
            }
            if c.contains([',', '+', '=']) {
                return bad(format!(
                    "column {c:?} contains ',', '+', or '=', which the short form uses as separators"
                ));
            }
            if !cols.insert(c.as_str()) {
                return bad(format!(
                    "item {} lists column {c:?} twice",
                    SketchSpec::item_label(item)
                ));
            }
        }
        if !seen.insert(item.clone()) {
            return bad(format!(
                "item {} is listed twice",
                SketchSpec::item_label(item)
            ));
        }
    }
    if !(MIN_PRECISION..=MAX_PRECISION).contains(&spec.precision) {
        return bad(format!(
            "precision {} is outside {MIN_PRECISION}..={MAX_PRECISION}",
            spec.precision
        ));
    }
    if let Some(k) = spec.minhash {
        if k == 0 || k > MAX_MINHASH {
            return bad(format!("minhash {k} is outside 1..={MAX_MINHASH}"));
        }
    }
    Ok(())
}

/// Each item's column types in `schema`, in item order; an error naming the
/// column when one is missing or has no canonical encoding.
pub fn item_types(spec: &SketchSpec, schema: &Schema) -> Result<Vec<Vec<DataType>>> {
    spec.items
        .iter()
        .map(|item| {
            item.iter()
                .map(|c| {
                    let dt = schema
                        .field_with_name(c)
                        .map(|f| f.data_type().clone())
                        .map_err(|_| {
                            Error::KeySpec(format!(
                                "sketch {}: {}",
                                spec.name(),
                                encoding::not_in_schema(schema, c)
                            ))
                        })?;
                    ColumnType::of(&dt).map_err(|e| {
                        Error::KeySpec(format!("sketch {}: column {c:?}: {e}", spec.name()))
                    })?;
                    Ok(dt)
                })
                .collect()
        })
        .collect()
}

/// The recorded form of [`item_types`].
fn recorded_types(types: &[Vec<DataType>]) -> Result<Vec<Vec<ColumnType>>> {
    types
        .iter()
        .map(|t| t.iter().map(ColumnType::of).collect())
        .collect()
}

// ---------------------------------------------------------------------------
// MinHash
// ---------------------------------------------------------------------------

/// A MinHash signature of `k` slots. Slot `i` of a value with hash halves
/// `(h1, h2)` is `mix64(h1 + i * (h2 | 1))` (wrapping), so one hash per value
/// feeds every slot; the signature keeps each slot's minimum.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MinHash {
    slots: Vec<u64>,
}

/// SplitMix64's finalizer: decorrelates the slots derived from one hash.
fn mix64(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

impl MinHash {
    /// An empty signature: every slot `u64::MAX`.
    #[must_use]
    pub fn new(k: u16) -> Self {
        Self {
            slots: vec![u64::MAX; usize::from(k)],
        }
    }

    /// A signature from stored slots.
    #[must_use]
    pub fn from_slots(slots: Vec<u64>) -> Self {
        Self { slots }
    }

    /// The slots.
    #[must_use]
    pub fn slots(&self) -> &[u64] {
        &self.slots
    }

    /// Whether no value was inserted.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.slots.iter().all(|&s| s == u64::MAX)
    }

    /// Adds one value by its [`value_hash`].
    pub fn insert(&mut self, hash: u128) {
        #[allow(clippy::cast_possible_truncation)]
        let h1 = hash as u64;
        #[allow(clippy::cast_possible_truncation)]
        let h2 = ((hash >> 64) as u64) | 1;
        let mut x = h1;
        for slot in &mut self.slots {
            let v = mix64(x);
            if v < *slot {
                *slot = v;
            }
            x = x.wrapping_add(h2);
        }
    }

    /// Unions `other` into `self` (slot-wise minimum). Both must have `k`
    /// slots.
    pub fn merge(&mut self, other: &Self) -> Result<()> {
        if other.slots.len() != self.slots.len() {
            return Err(Error::Other(format!(
                "cannot merge MinHash signatures of {} and {} slots",
                self.slots.len(),
                other.slots.len()
            )));
        }
        for (a, &b) in self.slots.iter_mut().zip(&other.slots) {
            if b < *a {
                *a = b;
            }
        }
        Ok(())
    }

    /// Slots whose minimums agree and are not empty, over `k`.
    fn agreement(&self, other: &Self) -> f64 {
        let k = self.slots.len();
        if k == 0 {
            return 0.0;
        }
        let equal = self
            .slots
            .iter()
            .zip(&other.slots)
            .filter(|(a, b)| a == b && **a != u64::MAX)
            .count();
        #[allow(clippy::cast_precision_loss)]
        let j = equal as f64 / k as f64;
        j
    }
}

// ---------------------------------------------------------------------------
// One file's sketch of one item
// ---------------------------------------------------------------------------

/// One item's sketch over one file (or one stretch of a file's row groups).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ItemSketch {
    /// Rows whose value was sketched (leading column non-null).
    pub rows: u64,
    /// Rows not sketched because the leading column is null.
    pub nulls: u64,
    /// The registers.
    pub hll: Hll,
    /// The signature, when the spec keeps one.
    pub minhash: Option<MinHash>,
}

impl ItemSketch {
    /// An empty sketch at `spec`'s parameters.
    pub fn empty(spec: &SketchSpec) -> Result<Self> {
        Ok(Self {
            rows: 0,
            nulls: 0,
            hll: Hll::new(spec.precision)?,
            minhash: spec.minhash.map(MinHash::new),
        })
    }

    /// Unions `other` (the same item over other rows) into `self`.
    pub fn merge(&mut self, other: &Self) -> Result<()> {
        self.rows = self
            .rows
            .checked_add(other.rows)
            .ok_or_else(|| Error::Other("sketched row count overflows u64".into()))?;
        self.nulls = self
            .nulls
            .checked_add(other.nulls)
            .ok_or_else(|| Error::Other("null count overflows u64".into()))?;
        self.hll.merge(&other.hll)?;
        match (&mut self.minhash, &other.minhash) {
            (Some(a), Some(b)) => a.merge(b),
            (None, None) => Ok(()),
            _ => Err(Error::Other(
                "cannot merge a sketch with a MinHash signature and one without".into(),
            )),
        }
    }

    fn insert(&mut self, bytes: &[u8]) {
        let h = value_hash(bytes);
        #[allow(clippy::cast_possible_truncation)]
        self.hll.insert(h as u64);
        if let Some(m) = &mut self.minhash {
            m.insert(h);
        }
        self.rows += 1;
    }
}

/// Feeds batches of one file into one [`ItemSketch`] per item.
#[derive(Debug)]
pub struct FileSketcher {
    spec: SketchSpec,
    types: Vec<Vec<DataType>>,
    items: Vec<ItemSketch>,
    buf: Vec<u8>,
}

impl FileSketcher {
    /// A sketcher for `spec` whose items have `types` (see [`item_types`]).
    pub fn new(spec: &SketchSpec, types: Vec<Vec<DataType>>) -> Result<Self> {
        validate(spec)?;
        if types.len() != spec.items.len()
            || types
                .iter()
                .zip(&spec.items)
                .any(|(t, i)| t.len() != i.len())
        {
            return Err(Error::Other(
                "sketch item types do not match the spec's items".into(),
            ));
        }
        let items = spec
            .items
            .iter()
            .map(|_| ItemSketch::empty(spec))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            spec: spec.clone(),
            types,
            items,
            buf: Vec::new(),
        })
    }

    /// Adds every row of `batch`, which holds each item's columns.
    pub fn add_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        for (i, item) in self.spec.items.iter().enumerate() {
            let cols = item
                .iter()
                .map(|c| {
                    batch.column_by_name(c).cloned().ok_or_else(|| {
                        Error::KeySpec(format!(
                            "column {c} not in the batch for {}",
                            self.spec.name()
                        ))
                    })
                })
                .collect::<Result<Vec<ArrayRef>>>()?;
            let keys = KeyColumns::new(&cols, &self.types[i])?;
            for row in 0..batch.num_rows() {
                if keys.encode_into(row, &mut self.buf)? {
                    self.items[i].insert(&self.buf);
                } else {
                    self.items[i].nulls += 1;
                }
            }
        }
        Ok(())
    }

    /// The finished sketches, one per item, and a fresh empty sketcher state.
    pub fn take(&mut self) -> Result<Vec<ItemSketch>> {
        let fresh = self
            .spec
            .items
            .iter()
            .map(|_| ItemSketch::empty(&self.spec))
            .collect::<Result<Vec<_>>>()?;
        Ok(std::mem::replace(&mut self.items, fresh))
    }

    /// Whether nothing was added since the last [`Self::take`].
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.iter().all(|i| i.rows == 0 && i.nulls == 0)
    }
}

// ---------------------------------------------------------------------------
// Header
// ---------------------------------------------------------------------------

/// The JSON header after the preamble. Unknown fields are preserved in
/// `extra` (ADR 017).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SketchHeader {
    /// `STORAGE_VERSION` at write time.
    pub storage_version: u32,
    /// `ENCODING_VERSION` at write time.
    pub encoding_version: u32,
    /// Always `"sketch"`.
    pub kind: String,
    /// Sketched items.
    pub items: Vec<Vec<String>>,
    /// Each item's column types, when written from a dataset schema.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_types: Option<Vec<Vec<ColumnType>>>,
    /// HyperLogLog precision.
    pub precision: u8,
    /// MinHash signature length, if kept.
    pub minhash: Option<u16>,
    /// The value hash: [`HASH_NAME`].
    pub hash: String,
    /// Its seed: [`HASH_SEED`].
    pub seed: u64,
    /// Body codec; only `"none"` exists.
    pub codec: String,
    /// Fields written by a newer writer, preserved verbatim.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

impl SketchHeader {
    fn new(spec: &SketchSpec, item_types: Option<Vec<Vec<ColumnType>>>) -> Self {
        Self {
            storage_version: STORAGE_VERSION,
            encoding_version: ENCODING_VERSION,
            kind: KIND.to_string(),
            items: spec.items.clone(),
            item_types,
            precision: spec.precision,
            minhash: spec.minhash,
            hash: HASH_NAME.to_string(),
            seed: HASH_SEED,
            codec: CODEC_NONE.to_string(),
            extra: serde_json::Map::new(),
        }
    }

    /// The spec this header records.
    #[must_use]
    pub fn spec(&self) -> SketchSpec {
        SketchSpec {
            items: self.items.clone(),
            precision: self.precision,
            minhash: self.minhash,
        }
    }

    fn validate(&self, storage: u32, encoding: u32) -> Result<()> {
        if self.storage_version > STORAGE_VERSION {
            return Err(Error::UnsupportedVersion {
                what: "storage",
                found: self.storage_version,
                supported: STORAGE_VERSION,
            });
        }
        if self.encoding_version > ENCODING_VERSION {
            return Err(Error::UnsupportedVersion {
                what: "encoding",
                found: self.encoding_version,
                supported: ENCODING_VERSION,
            });
        }
        if self.storage_version != storage || self.encoding_version != encoding {
            return Err(corrupt("header versions disagree with the preamble"));
        }
        if self.kind != KIND {
            return Err(corrupt(&format!("kind {:?} is not \"sketch\"", self.kind)));
        }
        if self.codec != CODEC_NONE {
            return Err(Error::Other(format!(
                "sketch codec {:?} is not supported; only \"none\" is",
                self.codec
            )));
        }
        if self.hash != HASH_NAME || self.seed != HASH_SEED {
            return Err(Error::Other(format!(
                "sketch hashes values with {} seed {:#x}; this build reads only {HASH_NAME} seed \
                 {HASH_SEED:#x}",
                self.hash, self.seed
            )));
        }
        validate(&self.spec())?;
        if let Some(t) = &self.item_types {
            if t.len() != self.items.len()
                || t.iter().zip(&self.items).any(|(a, b)| a.len() != b.len())
            {
                return Err(corrupt("item_types do not match the items"));
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

/// A finished sketch segment, ready to store at `segments/<blake3>.seg`.
#[derive(Clone, Debug)]
pub struct EncodedSketch {
    /// BLAKE3 of `bytes`: the segment id.
    pub id: Hash,
    /// The segment bytes.
    pub bytes: Vec<u8>,
    /// Entries (file stretches) it holds.
    pub entries: u32,
}

/// The largest number of bytes one entry of `spec` can encode to: per item
/// the two counts, a dense HyperLogLog with its tag, and the signature; plus
/// the row-group range. The build caps a segment's bytes with it.
#[must_use]
pub fn max_entry_bytes(spec: &SketchSpec) -> u64 {
    let per_item = 8 + 8 + 1 + (1u64 << spec.precision) + 8 * u64::from(spec.minhash.unwrap_or(0));
    8 + per_item * spec.items.len() as u64
}

/// Builds a sketch segment: [`Self::add_batch`] per row group, told which
/// file each row group belongs to, then [`Self::finish`].
#[derive(Debug)]
pub struct SketchWriter {
    header: SketchHeader,
    sketcher: FileSketcher,
    /// The open entry: `(file, first rg, one past the last rg)`.
    open: Option<(u64, u32, u32)>,
    body: Vec<u8>,
    entries: u32,
    last_end: u32,
    first: Option<u32>,
    range: Option<(u32, u32)>,
}

impl SketchWriter {
    /// A writer for `spec` whose header records the dataset types of every
    /// item column in `schema`.
    pub fn for_dataset(spec: &SketchSpec, schema: &Schema) -> Result<Self> {
        validate(spec)?;
        let types = item_types(spec, schema)?;
        let header = SketchHeader::new(spec, Some(recorded_types(&types)?));
        Ok(Self {
            sketcher: FileSketcher::new(spec, types)?,
            header,
            open: None,
            body: Vec::new(),
            entries: 0,
            last_end: 0,
            first: None,
            range: None,
        })
    }

    /// Adds row group `rg` of file `file` (any id stable for the file within
    /// this writer). A new entry starts when the file changes or `rg` does
    /// not follow the previous row group; row groups must ascend.
    pub fn add_batch(&mut self, file: u64, rg: RowGroupId, batch: &RecordBatch) -> Result<()> {
        let rg = rg.0;
        if self.first.is_some() && rg < self.last_end {
            return Err(Error::Other(format!(
                "sketch row group {rg} does not ascend past {}",
                self.last_end
            )));
        }
        if self.first.is_none() {
            self.first = Some(rg);
        }
        let continues = matches!(self.open, Some((f, _, end)) if f == file && end == rg);
        if !continues {
            self.close_entry()?;
            self.open = Some((file, rg, rg));
        }
        self.sketcher.add_batch(batch)?;
        let end = rg
            .checked_add(1)
            .ok_or_else(|| Error::Other("row-group id space exhausted".into()))?;
        if let Some(open) = &mut self.open {
            open.2 = end;
        }
        self.last_end = end;
        Ok(())
    }

    /// Whether `add_batch(file, rg, ..)` would start a new entry.
    #[must_use]
    pub fn starts_entry(&self, file: u64, rg: RowGroupId) -> bool {
        !matches!(self.open, Some((f, _, end)) if f == file && end == rg.0)
    }

    /// Records the row groups this writer processed, `[start, end)`, which
    /// must contain every entry. The footer records it, so two segments over
    /// different row groups never share bytes (ADR 013).
    pub fn set_rowgroup_range(&mut self, range: std::ops::Range<RowGroupId>) -> Result<()> {
        if range.start.0 > range.end.0 {
            return Err(Error::Other("sketch row-group range is reversed".into()));
        }
        self.range = Some((range.start.0, range.end.0));
        Ok(())
    }

    fn close_entry(&mut self) -> Result<()> {
        let Some((_, start, end)) = self.open.take() else {
            return Ok(());
        };
        let items = self.sketcher.take()?;
        encode_entry(&mut self.body, start, end, &items, self.header.minhash)?;
        self.entries = self
            .entries
            .checked_add(1)
            .ok_or_else(|| Error::Other("too many sketch entries".into()))?;
        Ok(())
    }

    /// The segment bytes and id.
    pub fn finish(mut self) -> Result<EncodedSketch> {
        self.close_entry()?;
        let first = self.first;
        let (start, end) = match self.range {
            Some(r) => r,
            None => (first.unwrap_or(0), self.last_end),
        };
        if self.entries > 0 && (first.unwrap_or(start) < start || self.last_end > end) {
            return Err(Error::Other(format!(
                "sketch entries span row groups outside the recorded range {start}..{end}"
            )));
        }
        let header = serde_json::to_vec(&self.header)?;
        let mut out =
            Vec::with_capacity(PREAMBLE_LEN + header.len() + self.body.len() + FOOTER_LEN);
        out.extend_from_slice(&MAGIC);
        out.extend_from_slice(&STORAGE_VERSION.to_le_bytes());
        out.extend_from_slice(&ENCODING_VERSION.to_le_bytes());
        out.extend_from_slice(&len_u32(header.len())?.to_le_bytes());
        out.extend_from_slice(&header);
        out.extend_from_slice(&self.body);
        out.extend_from_slice(&self.entries.to_le_bytes());
        out.extend_from_slice(&start.to_le_bytes());
        out.extend_from_slice(&end.to_le_bytes());
        let sum = blake3::hash(&out);
        out.extend_from_slice(sum.as_bytes());
        out.extend_from_slice(&MAGIC);
        Ok(EncodedSketch {
            id: Hash::of(&out),
            bytes: out,
            entries: self.entries,
        })
    }
}

fn encode_entry(
    out: &mut Vec<u8>,
    start: u32,
    end: u32,
    items: &[ItemSketch],
    minhash: Option<u16>,
) -> Result<()> {
    out.extend_from_slice(&start.to_le_bytes());
    out.extend_from_slice(&end.to_le_bytes());
    for item in items {
        out.extend_from_slice(&item.rows.to_le_bytes());
        out.extend_from_slice(&item.nulls.to_le_bytes());
        let regs = item.hll.registers();
        let n = item.hll.nonzero();
        if 4 + 5 * n < regs.len() {
            out.push(0);
            out.extend_from_slice(&len_u32(n)?.to_le_bytes());
            for (i, &r) in regs.iter().enumerate() {
                if r != 0 {
                    out.extend_from_slice(&len_u32(i)?.to_le_bytes());
                    out.push(r);
                }
            }
        } else {
            out.push(1);
            out.extend_from_slice(regs);
        }
        match (minhash, &item.minhash) {
            (Some(k), Some(m)) if m.slots().len() == usize::from(k) => {
                for s in m.slots() {
                    out.extend_from_slice(&s.to_le_bytes());
                }
            }
            (None, None) => {}
            _ => {
                return Err(Error::Other(
                    "sketch item signature does not match the spec".into(),
                ))
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

/// One decoded entry: a file stretch's row groups and its item sketches.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SketchEntry {
    /// The row groups it covers, `[start, end)`, all in one file.
    pub rowgroups: std::ops::Range<u32>,
    /// One sketch per item, in spec order.
    pub items: Vec<ItemSketch>,
}

/// Reads one sketch segment held in memory. Opening checks the checksum and
/// walks every entry, so a reader that opens is a reader whose every entry
/// decodes.
#[derive(Debug)]
pub struct SketchReader<'a> {
    header: SketchHeader,
    bytes: &'a [u8],
    /// Byte offset of each entry.
    offsets: Vec<usize>,
    range: (u32, u32),
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or_else(|| corrupt("length"))?;
        let s = self
            .buf
            .get(self.pos..end)
            .ok_or_else(|| corrupt("truncated"))?;
        self.pos = end;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn u64(&mut self) -> Result<u64> {
        let b = self.take(8)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(b);
        Ok(u64::from_le_bytes(a))
    }
}

impl<'a> SketchReader<'a> {
    /// Opens `bytes`, refusing a newer version, a damaged byte anywhere (the
    /// checksum), and any entry that breaks the layout rules.
    pub fn open(bytes: &'a [u8]) -> Result<Self> {
        if bytes.len() < PREAMBLE_LEN + FOOTER_LEN || bytes[..8] != MAGIC {
            return Err(corrupt("bad magic or too short"));
        }
        if bytes[bytes.len() - 8..] != MAGIC {
            return Err(corrupt("bad trailing magic"));
        }
        let mut c = Cursor { buf: bytes, pos: 8 };
        let storage = c.u32()?;
        let encoding = c.u32()?;
        if storage > STORAGE_VERSION {
            return Err(Error::UnsupportedVersion {
                what: "storage",
                found: storage,
                supported: STORAGE_VERSION,
            });
        }
        if encoding > ENCODING_VERSION {
            return Err(Error::UnsupportedVersion {
                what: "encoding",
                found: encoding,
                supported: ENCODING_VERSION,
            });
        }
        let footer_at = bytes.len() - FOOTER_LEN;
        let sum = blake3::hash(&bytes[..footer_at + 12]);
        if sum.as_bytes()[..] != bytes[footer_at + 12..footer_at + 44] {
            return Err(corrupt("checksum mismatch"));
        }
        let header_len = c.u32()? as usize;
        let header_bytes = c.take(header_len)?;
        if c.pos > footer_at {
            return Err(corrupt("header runs into the footer"));
        }
        let header: SketchHeader =
            serde_json::from_slice(header_bytes).map_err(|e| corrupt(&format!("header: {e}")))?;
        header.validate(storage, encoding)?;
        let mut f = Cursor {
            buf: bytes,
            pos: footer_at,
        };
        let entries = f.u32()?;
        let range = (f.u32()?, f.u32()?);
        if range.0 > range.1 {
            return Err(corrupt("reversed row-group range"));
        }
        let mut reader = Self {
            header,
            bytes: &bytes[..footer_at],
            offsets: Vec::new(),
            range,
        };
        let mut pos = c.pos;
        let mut prev_end = range.0;
        for _ in 0..entries {
            reader.offsets.push(pos);
            let (entry, next) = reader.decode_at(pos)?;
            if entry.rowgroups.start < prev_end || entry.rowgroups.end > range.1 {
                return Err(corrupt(
                    "entries out of order or outside the recorded range",
                ));
            }
            prev_end = entry.rowgroups.end;
            pos = next;
        }
        if pos != footer_at {
            return Err(corrupt("bytes between the last entry and the footer"));
        }
        Ok(reader)
    }

    /// Opens `bytes` and checks the recorded item types against `schema`: a
    /// sketch written under other types hashed other bytes, so its estimates
    /// would not describe this dataset.
    pub fn open_for_dataset(bytes: &'a [u8], schema: &Schema) -> Result<Self> {
        let r = Self::open(bytes)?;
        let spec = r.header.spec();
        let want = recorded_types(&item_types(&spec, schema)?)?;
        match &r.header.item_types {
            Some(have) if *have == want => Ok(r),
            Some(_) => Err(Error::KeySpec(format!(
                "a segment of {} was written under other column types than the dataset declares; \
                 re-index the folder",
                spec.name()
            ))),
            None => Err(Error::KeySpec(format!(
                "a segment of {} records no column types, so it cannot be checked against the \
                 dataset",
                spec.name()
            ))),
        }
    }

    /// The header.
    #[must_use]
    pub fn header(&self) -> &SketchHeader {
        &self.header
    }

    /// The spec the segment was written for.
    #[must_use]
    pub fn spec(&self) -> SketchSpec {
        self.header.spec()
    }

    /// The row groups the writer processed, `[start, end)`.
    #[must_use]
    pub fn rowgroup_range(&self) -> (u32, u32) {
        self.range
    }

    /// Entries in the segment.
    #[must_use]
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    /// Whether the segment holds no entry.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Decodes entry `i`.
    pub fn entry(&self, i: usize) -> Result<SketchEntry> {
        let pos = *self
            .offsets
            .get(i)
            .ok_or_else(|| Error::Other(format!("no sketch entry {i}")))?;
        Ok(self.decode_at(pos)?.0)
    }

    #[allow(clippy::many_single_char_names)]
    fn decode_at(&self, pos: usize) -> Result<(SketchEntry, usize)> {
        let p = self.header.precision;
        let m = 1usize << p;
        let max = Hll::max_rank(p);
        let mut c = Cursor {
            buf: self.bytes,
            pos,
        };
        let start = c.u32()?;
        let end = c.u32()?;
        if start >= end {
            return Err(corrupt("an entry covers no row group"));
        }
        let mut items = Vec::with_capacity(self.header.items.len());
        for _ in 0..self.header.items.len() {
            let rows = c.u64()?;
            let nulls = c.u64()?;
            let regs = match c.u8()? {
                0 => {
                    let n = c.u32()? as usize;
                    if 4 + 5 * n >= m {
                        return Err(corrupt("a sparse register list that dense encodes smaller"));
                    }
                    let mut regs = vec![0u8; m];
                    let mut prev: Option<usize> = None;
                    for _ in 0..n {
                        let i = c.u32()? as usize;
                        let r = c.u8()?;
                        if i >= m || prev.is_some_and(|q| i <= q) || r == 0 || r > max {
                            return Err(corrupt("a sparse register out of order or range"));
                        }
                        regs[i] = r;
                        prev = Some(i);
                    }
                    regs
                }
                1 => {
                    let regs = c.take(m)?.to_vec();
                    let n = regs.iter().filter(|&&r| r != 0).count();
                    if 4 + 5 * n < m {
                        return Err(corrupt("dense registers that sparse encodes smaller"));
                    }
                    regs
                }
                _ => return Err(corrupt("unknown register encoding")),
            };
            let hll = Hll::from_registers(p, regs).map_err(|e| corrupt(&e.to_string()))?;
            let minhash = match self.header.minhash {
                Some(k) => {
                    let mut slots = Vec::with_capacity(usize::from(k));
                    for _ in 0..k {
                        slots.push(c.u64()?);
                    }
                    Some(MinHash::from_slots(slots))
                }
                None => None,
            };
            let empty = hll.nonzero() == 0 && minhash.as_ref().is_none_or(MinHash::is_empty);
            let any = hll.nonzero() > 0 || minhash.as_ref().is_some_and(|s| !s.is_empty());
            if (rows == 0 && !empty) || (rows > 0 && !any) {
                return Err(corrupt("an item's row count disagrees with its registers"));
            }
            items.push(ItemSketch {
                rows,
                nulls,
                hll,
                minhash,
            });
        }
        Ok((
            SketchEntry {
                rowgroups: start..end,
                items,
            },
            c.pos,
        ))
    }
}

// ---------------------------------------------------------------------------
// Totals and lookups
// ---------------------------------------------------------------------------

/// One item's totals over every file folded in.
#[derive(Clone, Debug)]
pub struct ItemTotals {
    /// The union of every file's sketch.
    pub union: ItemSketch,
    /// Files folded in.
    pub files: u64,
    /// Sum of the per-file distinct estimates.
    pub sum_file_estimates: f64,
    /// Files whose estimate's upper bound is below their row count.
    pub files_with_duplicates: u64,
}

/// Every item's totals over a set of files: fold each file once with
/// [`Self::add_file`] (after merging a file's stretches), then read
/// [`Self::cardinality`].
#[derive(Clone, Debug)]
pub struct SketchTotals {
    spec: SketchSpec,
    items: Vec<ItemTotals>,
    /// `copresent[i * n + j]`: files where items `i` and `j` both have a
    /// sketched row (the diagonal: files where item `i` has one).
    copresent: Vec<u64>,
}

impl SketchTotals {
    /// Empty totals for `spec`.
    pub fn new(spec: &SketchSpec) -> Result<Self> {
        validate(spec)?;
        Ok(Self {
            spec: spec.clone(),
            items: spec
                .items
                .iter()
                .map(|_| {
                    Ok(ItemTotals {
                        union: ItemSketch::empty(spec)?,
                        files: 0,
                        sum_file_estimates: 0.0,
                        files_with_duplicates: 0,
                    })
                })
                .collect::<Result<_>>()?,
            copresent: vec![0; spec.items.len() * spec.items.len()],
        })
    }

    /// Files where items `i` and `j` both have a sketched row; with `i ==
    /// j`, files where item `i` has one. Exact.
    #[must_use]
    pub fn copresent(&self, i: usize, j: usize) -> u64 {
        let n = self.items.len();
        if i >= n || j >= n {
            return 0;
        }
        self.copresent[i * n + j]
    }

    /// The spec.
    #[must_use]
    pub fn spec(&self) -> &SketchSpec {
        &self.spec
    }

    /// Item `i`'s totals.
    #[must_use]
    pub fn item(&self, i: usize) -> Option<&ItemTotals> {
        self.items.get(i)
    }

    /// The index of the item `label` (`a` or `a+b`), if sketched.
    #[must_use]
    pub fn find(&self, label: &str) -> Option<usize> {
        self.spec
            .items
            .iter()
            .position(|i| SketchSpec::item_label(i) == label)
    }

    /// Folds one whole file: one sketch per item, in spec order.
    pub fn add_file(&mut self, items: &[ItemSketch]) -> Result<()> {
        if items.len() != self.items.len() {
            return Err(Error::Other(
                "a file's sketch has the wrong number of items".into(),
            ));
        }
        let n = self.items.len();
        for i in 0..n {
            for j in 0..n {
                if items[i].rows > 0 && items[j].rows > 0 {
                    self.copresent[i * n + j] += 1;
                }
            }
        }
        let rse = Hll::relative_standard_error(self.spec.precision);
        for (t, s) in self.items.iter_mut().zip(items) {
            t.union.merge(s)?;
            t.files += 1;
            let est = s.hll.estimate();
            t.sum_file_estimates += est;
            if s.rows > 0 && (est * (1.0 + SIGMAS * rse)).ceil() < u64_f64(s.rows) {
                t.files_with_duplicates += 1;
            }
        }
        Ok(())
    }

    /// `cardinality(item i)`: the approximate distinct count with its bound.
    /// `exact` is the item's exact distinct count when a complete exact key
    /// spec holds it.
    pub fn cardinality(&self, i: usize, exact: Option<u64>) -> Result<ApproxCardinality> {
        let t = self
            .items
            .get(i)
            .ok_or_else(|| Error::Other(format!("no sketch item {i}")))?;
        let item = &self.spec.items[i];
        let rse = Hll::relative_standard_error(self.spec.precision);
        let rows = t.union.rows;
        let (distinct, low, high) = bounded(t.union.hll.estimate(), rse, rows);
        let denom = exact.unwrap_or(distinct);
        let rows_per_value = if denom == 0 {
            0.0
        } else {
            u64_f64(rows) / u64_f64(denom)
        };
        let files_per_value = if distinct == 0 {
            0.0
        } else {
            t.sum_file_estimates / u64_f64(distinct)
        };
        Ok(ApproxCardinality {
            approximate: true,
            item: SketchSpec::item_label(item),
            columns: item.clone(),
            distinct,
            distinct_low: low,
            distinct_high: high,
            relative_standard_error: rse,
            confidence: SKETCH_CONFIDENCE,
            exact_distinct: exact,
            exact_source: exact.map(|_| ExactDistinctSource::ExactSegment),
            rows,
            nulls: t.union.nulls,
            rows_per_value,
            rows_per_value_exact: exact.is_some(),
            files: t.files,
            files_per_value,
            files_with_duplicates: t.files_with_duplicates,
        })
    }
}

#[allow(clippy::cast_precision_loss)]
fn u64_f64(v: u64) -> f64 {
    v as f64
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn f64_u64(v: f64) -> u64 {
    if v.is_finite() && v > 0.0 {
        v as u64
    } else {
        0
    }
}

/// `(estimate, low, high)`, rounded, each clamped to what `rows` sketched
/// rows allow: at most `rows`, and at least one when any row was sketched.
fn bounded(est: f64, rse: f64, rows: u64) -> (u64, u64, u64) {
    if rows == 0 {
        return (0, 0, 0);
    }
    let clamp = |v: u64| v.clamp(1, rows);
    let distinct = clamp(f64_u64(est.round()));
    let low = clamp(f64_u64((est * (1.0 - SIGMAS * rse)).floor()));
    let high = clamp(f64_u64((est * (1.0 + SIGMAS * rse)).ceil()));
    (distinct, low.min(distinct), high.max(distinct))
}

/// `overlap(left, right)`: the approximate Jaccard similarity of two items'
/// value sets. `left_types` and `right_types` are the items' column types:
/// values of different types never compare equal, so such a pair is refused
/// rather than reported as disjoint. MinHash is used when both sides keep
/// signatures of one length, HyperLogLog inclusion-exclusion otherwise.
#[allow(clippy::many_single_char_names, clippy::similar_names)]
pub fn overlap(
    left: (&str, &ItemTotals, &[ColumnType], u8),
    right: (&str, &ItemTotals, &[ColumnType], u8),
) -> Result<ApproxOverlap> {
    let (l_name, l, l_types, l_p) = left;
    let (r_name, r, r_types, r_p) = right;
    if l_types != r_types {
        let show = |t: &[ColumnType]| {
            t.iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        };
        return Err(Error::KeySpec(format!(
            "{l_name} is ({}) and {r_name} is ({}); values of different types never compare \
             equal, so their overlap is not defined; compare items of one type",
            show(l_types),
            show(r_types)
        )));
    }
    if l_p != r_p {
        return Err(Error::KeySpec(format!(
            "{l_name} is sketched at precision {l_p} and {r_name} at {r_p}; their registers do \
             not combine; declare both sketches with one precision (p=)"
        )));
    }
    let rse = Hll::relative_standard_error(l_p);
    let mut union = l.union.hll.clone();
    union.merge(&r.union.hll)?;
    let a = l.union.hll.estimate();
    let b = r.union.hll.estimate();
    let u = union.estimate();
    let rows = l.union.rows.saturating_add(r.union.rows);
    let (left_distinct, _, _) = bounded(a, rse, l.union.rows);
    let (right_distinct, _, _) = bounded(b, rse, r.union.rows);
    let (union_distinct, _, _) = bounded(u, rse, rows);
    let (method, signatures, jaccard, low, high) = match (&l.union.minhash, &r.union.minhash) {
        (Some(x), Some(y)) if x.slots().len() == y.slots().len() && !x.slots().is_empty() => {
            let k = u64_f64(x.slots().len() as u64);
            let j = x.agreement(y);
            // Each slot agrees with probability J, independently: a binomial
            // share, bounded by the Wilson score interval, which stays honest
            // near 0 and 1 where the normal interval collapses.
            let z2 = SIGMAS * SIGMAS;
            let centre = (j + z2 / (2.0 * k)) / (1.0 + z2 / k);
            let half = SIGMAS / (1.0 + z2 / k) * (j * (1.0 - j) / k + z2 / (4.0 * k * k)).sqrt();
            #[allow(clippy::cast_possible_truncation)]
            let sig = x.slots().len() as u16;
            (
                OverlapMethod::Minhash,
                Some(sig),
                j,
                (centre - half).clamp(0.0, j),
                (centre + half).clamp(j, 1.0),
            )
        }
        _ => {
            if u <= 0.0 {
                (OverlapMethod::Hll, None, 0.0, 0.0, 0.0)
            } else {
                let lo = 1.0 - SIGMAS * rse;
                let hi = 1.0 + SIGMAS * rse;
                let i = (a + b - u).max(0.0).min(a.min(b));
                let i_low = (a * lo + b * lo - u * hi).max(0.0);
                let i_high = (a * hi + b * hi - u * lo).min(a.min(b) * hi).max(0.0);
                let j = (i / u).clamp(0.0, 1.0);
                (
                    OverlapMethod::Hll,
                    None,
                    j,
                    (i_low / (u * hi)).clamp(0.0, j),
                    (i_high / (u * lo)).clamp(j, 1.0),
                )
            }
        }
    };
    let intersection =
        f64_u64((jaccard * u64_f64(union_distinct)).round()).min(left_distinct.min(right_distinct));
    Ok(ApproxOverlap {
        approximate: true,
        left: l_name.to_string(),
        right: r_name.to_string(),
        method,
        signatures,
        jaccard,
        jaccard_low: low,
        jaccard_high: high,
        confidence: SKETCH_CONFIDENCE,
        left_distinct,
        right_distinct,
        union_distinct,
        intersection,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mix_and_slots_are_deterministic() {
        let mut a = MinHash::new(4);
        a.insert(value_hash(b"x"));
        let mut b = MinHash::new(4);
        b.insert(value_hash(b"x"));
        assert_eq!(a, b);
        assert!(!a.is_empty());
        assert!((a.agreement(&b) - 1.0).abs() < 1e-12);
    }

    #[test]
    fn bounded_clamps_to_rows() {
        assert_eq!(bounded(0.0, 0.01, 0), (0, 0, 0));
        assert_eq!(bounded(5.2, 0.01, 3), (3, 3, 3));
        let (d, lo, hi) = bounded(1000.0, 0.01, 10_000);
        assert_eq!(d, 1000);
        assert!(lo < d && hi > d);
    }
}
