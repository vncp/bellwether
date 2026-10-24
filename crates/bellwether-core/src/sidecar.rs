//! Sidecar segment files: `segments/<blake3>.seg` (`docs/format.md`, storage
//! version 1).
//!
//! Two kinds exist in storage version 1:
//!
//! - [`SidecarKind::Bounds`] (`bounds`): per key, per row group holding the key
//!   with at least one non-null timestamp, the inclusive `[min, max]` of the
//!   timestamp column over that key's rows in that row group.
//! - [`SidecarKind::Bitemporal`] (`temporal2`): per key, the sorted distinct
//!   `(valid, known, rg)` triples over rows where neither clock is null.
//!
//! # Layout
//!
//! All integers are little-endian; `i64` values are two's complement.
//!
//! ```text
//! preamble  magic "BWSIDECR" | storage_version u32 | encoding_version u32 | header_len u32
//! header    JSON {storage_version, encoding_version, kind, columns, time_columns, codec:"none", key_types?, time_types?, ..unknown preserved in a flatten map}
//!           (key_types/time_types: segment::ColumnType spelling, present together when written for_dataset)
//! body      blocks; block = n u32 | n x entry; entry = key_len u32 | key bytes | count u32 | count x record
//!           bounds record    = rg u32 | min i64 | max i64        (strictly ascending rg, min <= max)
//!           temporal2 record = valid i64 | known i64 | rg u32    (strictly ascending (valid, known, rg))
//!           keys strictly ascending across the whole file; count >= 1
//! index     entries u64 | has_rg u8 | rg_min u32 | rg_max u32 | block_count u32 | block_count x (first_key_len u32 | first_key | last_key_len u32 | last_key | off u64 | len u32 | n u32)
//! footer    index_offset u64 | index_len u64 | magic "BWSIDECR"
//! ```
//!
//! Blocks hold at most [`BLOCK_KEYS`] (1024) keys or [`BLOCK_KEY_BYTES`]
//! (64 KiB) of key bytes.
//!
//! # Memory bound (ADR 018)
//!
//! A lookup holds the block index, one block at a time, and the results for
//! the keys asked. The writer holds every distinct `(key, rg)` (for `bounds`)
//! or `(key, valid, known, rg)` (for `temporal2`) until [`SidecarWriter::finish`];
//! the build crate caps segment size with [`measure`].

use std::collections::{BTreeMap, HashSet};
use std::fmt;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Schema};
use bellwether_types::format::{ENCODING_VERSION, STORAGE_VERSION};
use bellwether_types::{Error, Hash, Result, RowGroupId};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use crate::encoding::{self, tuple_has_null, KeyColumns};
use crate::segment::{segment_path, ColumnType, ReadAt, CODEC_NONE, FOOTER_LEN, PREAMBLE_LEN};

/// First and last eight bytes of every sidecar segment file.
pub const MAGIC: [u8; 8] = *b"BWSIDECR";

/// Maximum keys per sidecar block.
pub const BLOCK_KEYS: usize = 1024;

/// A sidecar block is also closed once its keys total this many bytes.
pub const BLOCK_KEY_BYTES: usize = 64 * 1024;

/// Smallest serialized index entry: two empty keys (`4 + 4`), `off u64` (`8`),
/// `len u32` (`4`), and `n u32` (`4`).
const MIN_INDEX_ENTRY: u64 = 4 + 4 + 8 + 4 + 4;

/// Byte size of one encoded `bounds` or `temporal2` record (`4 + 8 + 8`).
const RECORD_BYTES: usize = 20;

fn corrupt(what: &str) -> Error {
    Error::Other(format!("corrupt sidecar segment: {what}"))
}

fn len_u32(n: usize) -> Result<u32> {
    u32::try_from(n).map_err(|_| Error::Other(format!("length {n} exceeds u32")))
}

fn check_versions(storage: u32, encoding: u32) -> Result<()> {
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
    Ok(())
}

trait Put {
    fn put_u8(&mut self, v: u8);
    fn put_u32(&mut self, v: u32);
    fn put_u64(&mut self, v: u64);
    fn put_i64(&mut self, v: i64);
    fn put_bytes(&mut self, v: &[u8]) -> Result<()>;
}

impl Put for Vec<u8> {
    fn put_u8(&mut self, v: u8) {
        self.push(v);
    }
    fn put_u32(&mut self, v: u32) {
        self.extend_from_slice(&v.to_le_bytes());
    }
    fn put_u64(&mut self, v: u64) {
        self.extend_from_slice(&v.to_le_bytes());
    }
    fn put_i64(&mut self, v: i64) {
        self.extend_from_slice(&v.to_le_bytes());
    }
    fn put_bytes(&mut self, v: &[u8]) -> Result<()> {
        self.put_u32(len_u32(v.len())?);
        self.extend_from_slice(v);
        Ok(())
    }
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

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

    fn i64(&mut self) -> Result<i64> {
        let b = self.take(8)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(b);
        Ok(i64::from_le_bytes(a))
    }

    fn bytes(&mut self) -> Result<&'a [u8]> {
        let n = self.u32()? as usize;
        self.take(n)
    }

    fn is_done(&self) -> bool {
        self.pos == self.buf.len()
    }
}

/// Which kind of sidecar segment a [`SidecarDef`] describes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SidecarKind {
    /// Per-key, per-row-group timestamp `[min, max]` bounds (`"bounds"`).
    Bounds,
    /// Per-key bitemporal `(valid, known, rg)` triples (`"temporal2"`).
    #[serde(rename = "temporal2")]
    Bitemporal,
}

impl SidecarKind {
    /// The canonical string name (`"bounds"` or `"temporal2"`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bounds => "bounds",
            Self::Bitemporal => "temporal2",
        }
    }

    /// Parses `"bounds"` or `"temporal2"`.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "bounds" => Some(Self::Bounds),
            "temporal2" => Some(Self::Bitemporal),
            _ => None,
        }
    }
}

impl fmt::Display for SidecarKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Definition of a sidecar segment: its kind, key columns, and time columns.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SidecarDef {
    /// Sidecar kind (`bounds` or `temporal2`).
    pub kind: SidecarKind,
    /// Key columns in prefix order.
    pub columns: Vec<String>,
    /// Timestamp columns: `[ts]` for `bounds`, `[valid, known]` for `temporal2`.
    pub time_columns: Vec<String>,
}

impl SidecarDef {
    /// Stable identifier for this sidecar definition, such as
    /// `"bounds:symbol@ts"` or `"temporal2:a,b@valid,known"`.
    #[must_use]
    pub fn name(&self) -> String {
        format!(
            "{}:{}@{}",
            self.kind.as_str(),
            self.columns.join(","),
            self.time_columns.join(",")
        )
    }

    /// Validates the column lists for this sidecar kind: at least one key
    /// column, no empty column names, no duplicate column across key and time
    /// columns, and the exact number of time columns for the kind (`1` for
    /// `bounds`, `2` for `temporal2`).
    pub fn validate(&self) -> Result<()> {
        if self.columns.is_empty() {
            return Err(Error::KeySpec(
                "sidecar definition needs at least one key column".into(),
            ));
        }
        let want_time = match self.kind {
            SidecarKind::Bounds => 1,
            SidecarKind::Bitemporal => 2,
        };
        if self.time_columns.len() != want_time {
            return Err(Error::KeySpec(format!(
                "{} sidecar requires exactly {want_time} time column(s), got {}",
                self.kind.as_str(),
                self.time_columns.len()
            )));
        }
        let mut seen = HashSet::with_capacity(self.columns.len() + self.time_columns.len());
        for c in self.columns.iter().chain(&self.time_columns) {
            if c.is_empty() {
                return Err(Error::KeySpec(
                    "sidecar column name must not be empty".into(),
                ));
            }
            if !seen.insert(c.as_str()) {
                return Err(Error::KeySpec(format!(
                    "sidecar column {c} appears more than once; key and time columns must all be distinct"
                )));
            }
        }
        Ok(())
    }

    /// The dataset types of `self.columns` in `schema`, checked with
    /// [`encoding::check_supported`].
    pub fn key_types(&self, schema: &Schema) -> Result<Vec<DataType>> {
        self.validate()?;
        self.columns
            .iter()
            .map(|c| {
                let dt = schema
                    .field_with_name(c)
                    .map_err(|_| {
                        Error::KeySpec(format!("column {c} is not in the dataset schema"))
                    })?
                    .data_type()
                    .clone();
                encoding::check_supported(&dt)?;
                Ok(dt)
            })
            .collect()
    }

    /// The dataset types of `self.time_columns` in `schema`, checked with
    /// [`encoding::check_supported`] and [`encoding::check_ts_type`].
    pub fn time_types(&self, schema: &Schema) -> Result<Vec<DataType>> {
        self.validate()?;
        self.time_columns
            .iter()
            .map(|c| {
                let dt = schema
                    .field_with_name(c)
                    .map_err(|_| {
                        Error::KeySpec(format!("column {c} is not in the dataset schema"))
                    })?
                    .data_type()
                    .clone();
                encoding::check_supported(&dt)?;
                encoding::check_ts_type(&dt)?;
                Ok(dt)
            })
            .collect()
    }
}

/// Inclusive timestamp `[min, max]` of one key in one row group (`bounds` sidecar).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RgBounds {
    /// Row group holding the key.
    pub rg: RowGroupId,
    /// Minimum timestamp of the key in `rg`.
    pub min: i64,
    /// Maximum timestamp of the key in `rg`.
    pub max: i64,
}

impl RgBounds {
    /// Whether `[self.min, self.max]` overlaps `[lo, hi]` (inclusive on both ends).
    #[must_use]
    pub fn overlaps(&self, lo: i64, hi: i64) -> bool {
        lo <= hi && self.min <= hi && lo <= self.max
    }
}

/// One bitemporal version record of a key (`temporal2` sidecar).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Triple {
    /// Valid-time value in the dataset column's unit.
    pub valid: i64,
    /// Known-time value in the dataset column's unit.
    pub known: i64,
    /// Row group holding this `(valid, known)` pair for the key.
    pub rg: RowGroupId,
}

/// Winning bitemporal version of a key and every row group holding it.
#[derive(Clone, Debug, PartialEq)]
pub struct BitemporalHit {
    /// Winning valid-time value.
    pub valid: i64,
    /// Winning known-time value (greatest `known` at `valid`).
    pub known: i64,
    /// Every row group holding `(valid, known)` for this key (ties are kept).
    pub row_groups: RoaringBitmap,
}

/// Index-wide winner over triples from any number of segments of one key:
/// among triples with `known <= known_at` (if `Some`) and `valid <= valid_at`
/// (if `Some`), the greatest `valid`; among those the greatest `known`;
/// `row_groups` = every `rg` holding exactly that `(valid, known)`. `None` if
/// no triple qualifies.
#[must_use]
pub fn bitemporal_winner<'a>(
    triples: impl IntoIterator<Item = &'a Triple>,
    valid_at: Option<i64>,
    known_at: Option<i64>,
) -> Option<BitemporalHit> {
    let mut best: Option<(i64, i64)> = None;
    let mut row_groups = RoaringBitmap::new();
    for t in triples {
        if valid_at.is_some_and(|v| t.valid > v) || known_at.is_some_and(|k| t.known > k) {
            continue;
        }
        let cand = (t.valid, t.known);
        match best {
            None => {
                best = Some(cand);
                row_groups.insert(t.rg.0);
            }
            Some(cur) if cand > cur => {
                best = Some(cand);
                row_groups.clear();
                row_groups.insert(t.rg.0);
            }
            Some(cur) if cand == cur => {
                row_groups.insert(t.rg.0);
            }
            Some(_) => {}
        }
    }
    best.map(|(valid, known)| BitemporalHit {
        valid,
        known,
        row_groups,
    })
}

fn describe_col_type(ct: &ColumnType) -> String {
    match ct {
        ColumnType::Boolean => "Boolean".into(),
        ColumnType::Int8 => "Int8".into(),
        ColumnType::Int16 => "Int16".into(),
        ColumnType::Int32 => "Int32".into(),
        ColumnType::Int64 => "Int64".into(),
        ColumnType::UInt8 => "UInt8".into(),
        ColumnType::UInt16 => "UInt16".into(),
        ColumnType::UInt32 => "UInt32".into(),
        ColumnType::UInt64 => "UInt64".into(),
        ColumnType::Float16 => "Float16".into(),
        ColumnType::Float32 => "Float32".into(),
        ColumnType::Float64 => "Float64".into(),
        ColumnType::Decimal128 { precision, scale } => {
            format!("Decimal128({precision}, {scale})")
        }
        ColumnType::Decimal256 { precision, scale } => {
            format!("Decimal256({precision}, {scale})")
        }
        ColumnType::Utf8 => "Utf8".into(),
        ColumnType::LargeUtf8 => "LargeUtf8".into(),
        ColumnType::Utf8View => "Utf8View".into(),
        ColumnType::Binary => "Binary".into(),
        ColumnType::LargeBinary => "LargeBinary".into(),
        ColumnType::BinaryView => "BinaryView".into(),
        ColumnType::FixedSizeBinary { size } => format!("FixedSizeBinary({size})"),
        ColumnType::Date32 => "Date32".into(),
        ColumnType::Date64 => "Date64".into(),
        ColumnType::Time32 { unit } => format!("Time32({})", describe_unit(*unit)),
        ColumnType::Time64 { unit } => format!("Time64({})", describe_unit(*unit)),
        ColumnType::Timestamp { unit, tz: Some(tz) } => {
            format!("Timestamp({}, {tz})", describe_unit(*unit))
        }
        ColumnType::Timestamp { unit, tz: None } => {
            format!("Timestamp({})", describe_unit(*unit))
        }
    }
}

fn describe_unit(u: crate::segment::RecordedUnit) -> &'static str {
    match u {
        crate::segment::RecordedUnit::Second => "s",
        crate::segment::RecordedUnit::Millisecond => "ms",
        crate::segment::RecordedUnit::Microsecond => "us",
        crate::segment::RecordedUnit::Nanosecond => "ns",
    }
}

/// The JSON header after the preamble of a sidecar segment. Unknown fields are
/// preserved in `extra` (ADR 017).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SidecarHeader {
    /// `bellwether_types::format::STORAGE_VERSION` at write time.
    pub storage_version: u32,
    /// `bellwether_types::format::ENCODING_VERSION` at write time.
    pub encoding_version: u32,
    /// Sidecar kind (`bounds` or `temporal2`).
    pub kind: SidecarKind,
    /// Key columns in prefix order.
    pub columns: Vec<String>,
    /// Timestamp columns (`[ts]` for `bounds`, `[valid, known]` for `temporal2`).
    pub time_columns: Vec<String>,
    /// Compression codec of the body. Only `"none"` exists in version 1.
    pub codec: String,
    /// Each key column's dataset type, in prefix order, when written with
    /// [`SidecarWriter::for_dataset`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_types: Option<Vec<ColumnType>>,
    /// Each time column's dataset type, in order, when written with
    /// [`SidecarWriter::for_dataset`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_types: Option<Vec<ColumnType>>,
    /// Fields written by a newer minor writer, preserved verbatim.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

impl SidecarHeader {
    /// A header for `def` at the current storage and encoding versions.
    #[must_use]
    pub fn new(def: &SidecarDef) -> Self {
        Self {
            storage_version: STORAGE_VERSION,
            encoding_version: ENCODING_VERSION,
            kind: def.kind,
            columns: def.columns.clone(),
            time_columns: def.time_columns.clone(),
            codec: CODEC_NONE.to_string(),
            key_types: None,
            time_types: None,
            extra: serde_json::Map::new(),
        }
    }

    /// Reconstructs the [`SidecarDef`] from this header.
    #[must_use]
    pub fn def(&self) -> SidecarDef {
        SidecarDef {
            kind: self.kind,
            columns: self.columns.clone(),
            time_columns: self.time_columns.clone(),
        }
    }

    /// Validates this header against the preamble versions and storage version 1 rules.
    pub fn validate(&self, preamble_storage: u32, preamble_encoding: u32) -> Result<()> {
        check_versions(self.storage_version, self.encoding_version)?;
        if self.storage_version != preamble_storage || self.encoding_version != preamble_encoding {
            return Err(corrupt(&format!(
                "header versions ({}, {}) disagree with preamble ({preamble_storage}, {preamble_encoding})",
                self.storage_version, self.encoding_version
            )));
        }
        if self.codec != CODEC_NONE {
            return Err(Error::Other(format!(
                "sidecar codec {} is not supported; only \"none\" is supported in storage version 1",
                self.codec
            )));
        }
        let def = self.def();
        def.validate()?;
        match (&self.key_types, &self.time_types) {
            (Some(kt), Some(tt)) => {
                if kt.len() != self.columns.len() {
                    return Err(corrupt(
                        "key_types length does not match the sidecar's key columns",
                    ));
                }
                if tt.len() != self.time_columns.len() {
                    return Err(corrupt(
                        "time_types length does not match the sidecar's time columns",
                    ));
                }
            }
            (None, None) => {}
            _ => {
                return Err(corrupt(
                    "key_types and time_types must be present together or both absent",
                ));
            }
        }
        Ok(())
    }
}

/// A finished sidecar segment, ready to store at `segments/<blake3>.seg`.
#[derive(Clone, Debug)]
pub struct EncodedSidecar {
    /// BLAKE3 of `bytes`; the segment's content-addressed id.
    pub id: Hash,
    /// Complete serialized segment bytes.
    pub bytes: Vec<u8>,
    /// Sidecar definition this segment indexes.
    pub def: SidecarDef,
    /// Number of distinct keys in the segment.
    pub entries: u64,
    /// Inclusive `(min_rg, max_rg)` range recorded in the index, if any.
    pub rowgroup_range: Option<(RowGroupId, RowGroupId)>,
}

impl EncodedSidecar {
    /// Relative store path: `segments/<blake3>.seg`.
    #[must_use]
    pub fn path(&self) -> String {
        segment_path(&self.id)
    }
}

/// Upper bound of what adding `batch` can add to an open [`SidecarWriter`]:
/// `(distinct keys in batch, bytes = key bytes + 20 per (key, rg) for bounds / + 20 per distinct (key, valid, known) for temporal2)`.
///
/// Used by the build crate to cap segment size before calling
/// [`SidecarWriter::add_batch`]. Rows that [`SidecarWriter::add_batch`] skips
/// (a null leading key column, a null `ts` for `bounds`, or a null `valid` or
/// `known` for `temporal2`) contribute nothing.
pub fn measure(
    def: &SidecarDef,
    key_types: &[DataType],
    time_types: &[DataType],
    batch: &RecordBatch,
) -> Result<(u64, u64)> {
    def.validate()?;
    if key_types.len() != def.columns.len() {
        return Err(Error::KeySpec(format!(
            "{} key columns for {} declared key types",
            def.columns.len(),
            key_types.len()
        )));
    }
    if time_types.len() != def.time_columns.len() {
        return Err(Error::KeySpec(format!(
            "{} time columns for {} declared time types",
            def.time_columns.len(),
            time_types.len()
        )));
    }
    for dt in key_types {
        encoding::check_supported(dt)?;
    }
    for dt in time_types {
        encoding::check_supported(dt)?;
        encoding::check_ts_type(dt)?;
    }
    let column = |name: &str| -> Result<ArrayRef> {
        batch
            .column_by_name(name)
            .cloned()
            .ok_or_else(|| Error::KeySpec(format!("column {name} not in batch")))
    };
    let key_cols = def
        .columns
        .iter()
        .map(|c| column(c))
        .collect::<Result<Vec<_>>>()?;
    let keys = KeyColumns::new(&key_cols, key_types)?;
    let time_cols = def
        .time_columns
        .iter()
        .zip(time_types)
        .map(|(c, declared)| {
            let arr = column(c)?;
            encoding::check_convertible(arr.data_type(), declared)?;
            Ok((arr, declared))
        })
        .collect::<Result<Vec<_>>>()?;

    let mut buf = Vec::new();
    match def.kind {
        SidecarKind::Bounds => {
            let (ts_arr, ts_decl) = &time_cols[0];
            let mut seen: HashSet<Vec<u8>> = HashSet::new();
            let mut bytes = 0u64;
            for row in 0..batch.num_rows() {
                if !keys.encode_into(row, &mut buf)? {
                    continue;
                }
                if encoding::ts_value_as(ts_arr.as_ref(), row, ts_decl)?.is_none() {
                    continue;
                }
                if !seen.contains(buf.as_slice()) {
                    bytes += (buf.len() + RECORD_BYTES) as u64;
                    seen.insert(buf.clone());
                }
            }
            Ok((seen.len() as u64, bytes))
        }
        SidecarKind::Bitemporal => {
            let (v_arr, v_decl) = &time_cols[0];
            let (k_arr, k_decl) = &time_cols[1];
            let mut per_key: BTreeMap<Vec<u8>, HashSet<(i64, i64)>> = BTreeMap::new();
            for row in 0..batch.num_rows() {
                if !keys.encode_into(row, &mut buf)? {
                    continue;
                }
                let Some(v) = encoding::ts_value_as(v_arr.as_ref(), row, v_decl)? else {
                    continue;
                };
                let Some(k) = encoding::ts_value_as(k_arr.as_ref(), row, k_decl)? else {
                    continue;
                };
                if let Some(pairs) = per_key.get_mut(buf.as_slice()) {
                    pairs.insert((v, k));
                } else {
                    let mut pairs = HashSet::new();
                    pairs.insert((v, k));
                    per_key.insert(buf.clone(), pairs);
                }
            }
            let distinct_keys = per_key.len() as u64;
            let bytes: u64 = per_key
                .iter()
                .map(|(k, pairs)| k.len() as u64 + (pairs.len() * RECORD_BYTES) as u64)
                .sum();
            Ok((distinct_keys, bytes))
        }
    }
}

#[derive(Clone, Debug, Default)]
struct TripleBuffer {
    triples: Vec<(i64, i64, u32)>,
    compacted: usize,
}

impl TripleBuffer {
    fn push(&mut self, valid: i64, known: i64, rg: u32) {
        let item = (valid, known, rg);
        if self.triples.last() == Some(&item) {
            return;
        }
        self.triples.push(item);
        if self.triples.len() > 2 * self.compacted.max(32) {
            self.triples.sort_unstable();
            self.triples.dedup();
            self.compacted = self.triples.len();
        }
    }

    fn finish(mut self) -> Vec<(i64, i64, u32)> {
        self.triples.sort_unstable();
        self.triples.dedup();
        self.triples
    }
}

enum Acc {
    Bounds(BTreeMap<Vec<u8>, BTreeMap<u32, (i64, i64)>>),
    Bitemporal(BTreeMap<Vec<u8>, TripleBuffer>),
}

#[derive(Clone, Debug)]
struct DatasetTypes {
    keys: Vec<DataType>,
    times: Vec<DataType>,
    recorded_keys: Vec<ColumnType>,
    recorded_times: Vec<ColumnType>,
}

/// Builds a [`SidecarKind::Bounds`] or [`SidecarKind::Bitemporal`] sidecar segment.
///
/// Memory bound (ADR 018): the writer holds every distinct `(key, rg)` (for
/// `bounds`) or `(key, valid, known, rg)` (for `temporal2`) until
/// [`SidecarWriter::finish`]; the build crate caps segment size with [`measure`].
pub struct SidecarWriter {
    def: SidecarDef,
    acc: Acc,
    dataset: Option<DatasetTypes>,
    processed: Option<(u32, u32)>,
}

impl SidecarWriter {
    /// Creates a writer for already-encoded keys ([`SidecarWriter::insert_bounds`]
    /// or [`SidecarWriter::insert_triple`]). Records no column types in the header.
    pub fn new(def: SidecarDef) -> Result<Self> {
        def.validate()?;
        let acc = match def.kind {
            SidecarKind::Bounds => Acc::Bounds(BTreeMap::new()),
            SidecarKind::Bitemporal => Acc::Bitemporal(BTreeMap::new()),
        };
        Ok(Self {
            def,
            acc,
            dataset: None,
            processed: None,
        })
    }

    /// Creates a writer for Arrow batches of a dataset with `schema`. Records
    /// `key_types` and `time_types` in the segment header.
    pub fn for_dataset(def: SidecarDef, schema: &Schema) -> Result<Self> {
        let keys = def.key_types(schema)?;
        let times = def.time_types(schema)?;
        let recorded_keys = keys.iter().map(ColumnType::of).collect::<Result<_>>()?;
        let recorded_times = times.iter().map(ColumnType::of).collect::<Result<_>>()?;
        let mut w = Self::new(def)?;
        w.dataset = Some(DatasetTypes {
            keys,
            times,
            recorded_keys,
            recorded_times,
        });
        Ok(w)
    }

    /// Records the row groups this writer processed (`range` half-open and
    /// non-empty). Every row group a key references must lie inside it;
    /// [`SidecarWriter::finish`] refuses otherwise.
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

    /// Records `[min, max]` for `(key, rg)` in a `bounds` sidecar, widening any
    /// existing bounds for `(key, rg)`. Refuses `min > max` or a `temporal2`
    /// writer.
    pub fn insert_bounds(&mut self, key: &[u8], rg: RowGroupId, min: i64, max: i64) -> Result<()> {
        let Acc::Bounds(m) = &mut self.acc else {
            return Err(Error::KeySpec(
                "insert_bounds cannot be called on a temporal2 sidecar writer; use insert_triple"
                    .into(),
            ));
        };
        if min > max {
            return Err(Error::Other(format!(
                "bounds min ({min}) exceeds max ({max})"
            )));
        }
        let rgs = if let Some(rgs) = m.get_mut(key) {
            rgs
        } else {
            m.entry(key.to_vec()).or_default()
        };
        rgs.entry(rg.0)
            .and_modify(|b| {
                b.0 = b.0.min(min);
                b.1 = b.1.max(max);
            })
            .or_insert((min, max));
        Ok(())
    }

    /// Records `(valid, known, rg)` for `key` in a `temporal2` sidecar. Refuses
    /// a `bounds` writer.
    pub fn insert_triple(
        &mut self,
        key: &[u8],
        valid: i64,
        known: i64,
        rg: RowGroupId,
    ) -> Result<()> {
        let Acc::Bitemporal(m) = &mut self.acc else {
            return Err(Error::KeySpec(
                "insert_triple cannot be called on a bounds sidecar writer; use insert_bounds"
                    .into(),
            ));
        };
        if let Some(buf) = m.get_mut(key) {
            buf.push(valid, known, rg.0);
        } else {
            let mut buf = TripleBuffer::default();
            buf.push(valid, known, rg.0);
            m.insert(key.to_vec(), buf);
        }
        Ok(())
    }

    /// Indexes every row of `batch` for row group `rg`.
    ///
    /// All-or-nothing: the batch is converted and staged into a local buffer
    /// first, and nothing is recorded if any row fails to convert. A row whose
    /// leading key column is null is skipped; a null trailing key column is
    /// encoded as the null marker (`0x00`). For `bounds`, a row with a null
    /// timestamp contributes nothing. For `temporal2`, a row with a null
    /// `valid` or null `known` contributes nothing (nulls never win, ADR 014).
    pub fn add_batch(&mut self, rg: RowGroupId, batch: &RecordBatch) -> Result<()> {
        let Some(dataset) = self.dataset.clone() else {
            return Err(Error::KeySpec(
                "add_batch needs the dataset schema; construct the writer with SidecarWriter::for_dataset"
                    .into(),
            ));
        };
        let column = |name: &str| -> Result<ArrayRef> {
            batch
                .column_by_name(name)
                .cloned()
                .ok_or_else(|| Error::KeySpec(format!("column {name} not in batch")))
        };
        let key_cols = self
            .def
            .columns
            .iter()
            .map(|c| column(c))
            .collect::<Result<Vec<_>>>()?;
        let keys = KeyColumns::new(&key_cols, &dataset.keys)?;
        let time_cols = self
            .def
            .time_columns
            .iter()
            .zip(&dataset.times)
            .map(|(c, declared)| {
                let arr = column(c)?;
                encoding::check_convertible(arr.data_type(), declared)?;
                Ok((arr, declared.clone()))
            })
            .collect::<Result<Vec<_>>>()?;

        let mut buf = Vec::new();
        match self.def.kind {
            SidecarKind::Bounds => {
                let (ts_arr, ts_decl) = &time_cols[0];
                let mut staged: Vec<(Vec<u8>, i64)> = Vec::with_capacity(batch.num_rows());
                for row in 0..batch.num_rows() {
                    if !keys.encode_into(row, &mut buf)? {
                        continue;
                    }
                    let Some(t) = encoding::ts_value_as(ts_arr.as_ref(), row, ts_decl)? else {
                        continue;
                    };
                    staged.push((buf.clone(), t));
                }
                for (k, t) in staged {
                    self.insert_bounds(&k, rg, t, t)?;
                }
            }
            SidecarKind::Bitemporal => {
                let (v_arr, v_decl) = &time_cols[0];
                let (k_arr, k_decl) = &time_cols[1];
                let mut staged: Vec<(Vec<u8>, i64, i64)> = Vec::with_capacity(batch.num_rows());
                for row in 0..batch.num_rows() {
                    if !keys.encode_into(row, &mut buf)? {
                        continue;
                    }
                    let tv = encoding::ts_value_as(v_arr.as_ref(), row, v_decl)?;
                    let tk = encoding::ts_value_as(k_arr.as_ref(), row, k_decl)?;
                    if let (Some(v), Some(k)) = (tv, tk) {
                        staged.push((buf.clone(), v, k));
                    }
                }
                for (key, v, k) in staged {
                    self.insert_triple(&key, v, k, rg)?;
                }
            }
        }
        Ok(())
    }

    /// Finalizes and serializes the sidecar segment.
    pub fn finish(self) -> Result<EncodedSidecar> {
        let mut header = SidecarHeader::new(&self.def);
        if let Some(d) = &self.dataset {
            header.key_types = Some(d.recorded_keys.clone());
            header.time_types = Some(d.recorded_times.clone());
        }
        header.validate(header.storage_version, header.encoding_version)?;
        let json = serde_json::to_vec(&header)?;
        let mut out = Vec::with_capacity(4096);
        out.extend_from_slice(&MAGIC);
        out.put_u32(header.storage_version);
        out.put_u32(header.encoding_version);
        out.put_u32(len_u32(json.len())?);
        out.extend_from_slice(&json);

        let mut emitter = BlockEmitter::new(out);
        match self.acc {
            Acc::Bounds(m) => {
                for (key, rgs) in m {
                    if rgs.is_empty() {
                        continue;
                    }
                    let count = len_u32(rgs.len())?;
                    let mut entry_records = Vec::with_capacity(rgs.len() * RECORD_BYTES);
                    for (rg, (min, max)) in rgs {
                        emitter.note_rg(rg);
                        entry_records.put_u32(rg);
                        entry_records.put_i64(min);
                        entry_records.put_i64(max);
                    }
                    emitter.push_key(&key, count, &entry_records)?;
                }
            }
            Acc::Bitemporal(m) => {
                for (key, buf) in m {
                    let triples = buf.finish();
                    if triples.is_empty() {
                        continue;
                    }
                    let count = len_u32(triples.len())?;
                    let mut entry_records = Vec::with_capacity(triples.len() * RECORD_BYTES);
                    for (valid, known, rg) in triples {
                        emitter.note_rg(rg);
                        entry_records.put_i64(valid);
                        entry_records.put_i64(known);
                        entry_records.put_u32(rg);
                    }
                    emitter.push_key(&key, count, &entry_records)?;
                }
            }
        }
        if let Some((lo, hi)) = self.processed {
            emitter.set_processed(lo, hi)?;
        }
        emitter.finish(self.def)
    }
}

struct BlockEmitter {
    out: Vec<u8>,
    index: Vec<u8>,
    entries: u64,
    rg: Option<(u32, u32)>,
    blocks: u32,
    block_keys: usize,
    block_key_bytes: usize,
    block_first: Option<Vec<u8>>,
    block_last: Option<Vec<u8>>,
    block_buf: Vec<u8>,
}

impl BlockEmitter {
    fn new(out: Vec<u8>) -> Self {
        Self {
            out,
            index: Vec::new(),
            entries: 0,
            rg: None,
            blocks: 0,
            block_keys: 0,
            block_key_bytes: 0,
            block_first: None,
            block_last: None,
            block_buf: Vec::new(),
        }
    }

    fn note_rg(&mut self, rg: u32) {
        self.rg = Some(match self.rg {
            None => (rg, rg),
            Some((a, b)) => (a.min(rg), b.max(rg)),
        });
    }

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

    fn push_key(&mut self, key: &[u8], count: u32, records: &[u8]) -> Result<()> {
        if self.block_first.is_none() {
            self.block_first = Some(key.to_vec());
        }
        self.block_last = Some(key.to_vec());
        self.block_buf.put_bytes(key)?;
        self.block_buf.put_u32(count);
        self.block_buf.extend_from_slice(records);
        self.block_keys += 1;
        self.block_key_bytes += key.len();
        self.entries += 1;
        if self.block_keys >= BLOCK_KEYS || self.block_key_bytes >= BLOCK_KEY_BYTES {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        let (Some(first), Some(last)) = (self.block_first.take(), self.block_last.take()) else {
            return Ok(());
        };
        let off = self.out.len() as u64;
        let n = len_u32(self.block_keys)?;
        self.out.put_u32(n);
        self.out.extend_from_slice(&self.block_buf);
        let len = len_u32(usize::try_from(self.out.len() as u64 - off).unwrap_or(usize::MAX))?;

        self.index.put_bytes(&first)?;
        self.index.put_bytes(&last)?;
        self.index.put_u64(off);
        self.index.put_u32(len);
        self.index.put_u32(n);
        self.blocks += 1;

        self.block_keys = 0;
        self.block_key_bytes = 0;
        self.block_buf.clear();
        Ok(())
    }

    fn finish(mut self, def: SidecarDef) -> Result<EncodedSidecar> {
        self.flush()?;
        let index_offset = self.out.len() as u64;
        self.out.put_u64(self.entries);
        let (lo, hi) = self.rg.unwrap_or((0, 0));
        self.out.put_u8(u8::from(self.rg.is_some()));
        self.out.put_u32(lo);
        self.out.put_u32(hi);
        self.out.put_u32(self.blocks);
        self.out.extend_from_slice(&self.index);
        let index_len = self.out.len() as u64 - index_offset;
        self.out.reserve(FOOTER_LEN);
        self.out.put_u64(index_offset);
        self.out.put_u64(index_len);
        self.out.extend_from_slice(&MAGIC);
        Ok(EncodedSidecar {
            id: Hash::of(&self.out),
            bytes: self.out,
            def,
            entries: self.entries,
            rowgroup_range: self.rg.map(|(a, b)| (RowGroupId(a), RowGroupId(b))),
        })
    }
}

#[derive(Clone, Debug)]
struct SidecarBlock {
    first: Vec<u8>,
    last: Vec<u8>,
    off: u64,
    len: u32,
    n: u32,
}

/// Reads one sidecar segment. Generic over [`ReadAt`].
///
/// Memory bound (ADR 018): an open reader holds the header and the block index.
/// A lookup holds the block index, one block at a time, and the results for
/// the keys asked.
#[derive(Debug)]
pub struct SidecarReader<R> {
    src: R,
    header: SidecarHeader,
    def: SidecarDef,
    entries: u64,
    rg: Option<(RowGroupId, RowGroupId)>,
    blocks: Vec<SidecarBlock>,
}

impl<R: ReadAt> SidecarReader<R> {
    /// Opens a sidecar segment: checks the preamble magic and versions before
    /// parsing the JSON header, validates the header and sidecar definition,
    /// and loads and validates the block index.
    pub fn open(src: R) -> Result<Self> {
        let size = src.size()?;
        if size < (PREAMBLE_LEN + FOOTER_LEN) as u64 {
            return Err(corrupt("file is shorter than preamble plus footer"));
        }
        let pre = src.read_at(0, PREAMBLE_LEN)?;
        let mut c = Cursor::new(&pre);
        if c.take(8)? != MAGIC {
            return Err(corrupt("bad preamble magic; expected BWSIDECR"));
        }
        let storage = c.u32()?;
        let encoding = c.u32()?;
        check_versions(storage, encoding)?;
        let header_len = c.u32()? as usize;
        let body_start = (PREAMBLE_LEN as u64)
            .checked_add(header_len as u64)
            .ok_or_else(|| corrupt("header length overflows file offset"))?;
        let footer_off = size - FOOTER_LEN as u64;
        if body_start > footer_off {
            return Err(corrupt("header extends past footer"));
        }
        let header_bytes = src.read_at(PREAMBLE_LEN as u64, header_len)?;
        let header: SidecarHeader = serde_json::from_slice(&header_bytes)?;
        header.validate(storage, encoding)?;
        let def = header.def();

        let foot = src.read_at(footer_off, FOOTER_LEN)?;
        let mut c = Cursor::new(&foot);
        let index_off = c.u64()?;
        let index_len_u64 = c.u64()?;
        if c.take(8)? != MAGIC {
            return Err(corrupt("bad footer magic; expected BWSIDECR"));
        }
        let index_end = index_off
            .checked_add(index_len_u64)
            .ok_or_else(|| corrupt("index offset plus length overflows"))?;
        if index_off < body_start || index_end != footer_off {
            return Err(corrupt(
                "index range does not sit between header and footer",
            ));
        }
        let index_len = usize::try_from(index_len_u64).map_err(|_| corrupt("index length"))?;
        let index_bytes = src.read_at(index_off, index_len)?;
        let (entries, rg, blocks) =
            Self::parse_index(&index_bytes, index_len_u64, body_start, index_off)?;
        Ok(Self {
            src,
            header,
            def,
            entries,
            rg,
            blocks,
        })
    }

    #[allow(clippy::type_complexity, clippy::too_many_lines)]
    fn parse_index(
        index_bytes: &[u8],
        index_len_u64: u64,
        body_start: u64,
        index_off: u64,
    ) -> Result<(u64, Option<(RowGroupId, RowGroupId)>, Vec<SidecarBlock>)> {
        let mut cur = Cursor::new(index_bytes);
        let entries = cur.u64()?;
        let has_rg = match cur.u8()? {
            0 => false,
            1 => true,
            other => {
                return Err(corrupt(&format!(
                    "index has_rg flag must be 0 or 1, found {other}"
                )))
            }
        };
        let (rg_min, rg_max) = (cur.u32()?, cur.u32()?);
        if has_rg && rg_min > rg_max {
            return Err(corrupt("index rg_min exceeds rg_max"));
        }
        if !has_rg && (rg_min != 0 || rg_max != 0) {
            return Err(corrupt(
                "index has_rg is 0 but rg_min or rg_max is non-zero",
            ));
        }
        let block_count = cur.u32()?;
        let fit = index_len_u64.checked_div(MIN_INDEX_ENTRY).unwrap_or(0);
        let cap = usize::try_from(u64::from(block_count).min(fit)).unwrap_or(0);
        let mut blocks = Vec::with_capacity(cap);
        let mut sum_n = 0u64;
        let mut prev_end = body_start;
        for bi in 0..block_count {
            let first = cur.bytes()?.to_vec();
            let last = cur.bytes()?.to_vec();
            let off = cur.u64()?;
            let len = cur.u32()?;
            let num_keys = cur.u32()?;
            if num_keys == 0 {
                return Err(corrupt("block in index has zero keys"));
            }
            if first.is_empty() || last.is_empty() {
                return Err(corrupt("block in index has an empty key bound"));
            }
            if first > last {
                return Err(corrupt("block in index has first_key > last_key"));
            }
            if num_keys == 1 && first != last {
                return Err(corrupt(
                    "single-key block in index has first_key != last_key",
                ));
            }
            if num_keys > 1 && first == last {
                return Err(corrupt(
                    "multi-key block in index has first_key == last_key",
                ));
            }
            if let Some(prev) = blocks.last() {
                let prev: &SidecarBlock = prev;
                if first <= prev.last {
                    return Err(corrupt(
                        "block index keys are not strictly ascending across blocks",
                    ));
                }
            }
            if off != prev_end {
                return Err(corrupt(&format!(
                    "block {bi} offset {off} does not follow previous end {prev_end}"
                )));
            }
            if len < 4 {
                return Err(corrupt("block length is smaller than key count header"));
            }
            let end = off
                .checked_add(u64::from(len))
                .ok_or_else(|| corrupt("block offset plus length overflows"))?;
            if end > index_off {
                return Err(corrupt("block extends past the start of the index"));
            }
            prev_end = end;
            sum_n = sum_n
                .checked_add(u64::from(num_keys))
                .ok_or_else(|| corrupt("sum of block key counts overflows"))?;
            blocks.push(SidecarBlock {
                first,
                last,
                off,
                len,
                n: num_keys,
            });
        }
        if !cur.is_done() {
            return Err(corrupt("index has trailing bytes"));
        }
        if prev_end != index_off {
            return Err(corrupt(
                "body blocks do not fill the body up to index_offset",
            ));
        }
        if sum_n != entries {
            return Err(corrupt(
                "index entries count does not equal the sum of block key counts",
            ));
        }
        if entries > 0 && !has_rg {
            return Err(corrupt(
                "non-empty sidecar segment must record a row-group range",
            ));
        }
        Ok((
            entries,
            has_rg.then_some((RowGroupId(rg_min), RowGroupId(rg_max))),
            blocks,
        ))
    }
    /// Opens a sidecar segment and verifies that its recorded `key_types` and
    /// `time_types` match `schema`. Refuses a segment written without recorded
    /// types or whose recorded types differ from `schema`.
    pub fn open_for_dataset(src: R, schema: &Schema) -> Result<Self> {
        let r = Self::open(src)?;
        r.check_dataset(schema)?;
        Ok(r)
    }

    fn check_dataset(&self, schema: &Schema) -> Result<()> {
        let want_keys = self
            .def
            .key_types(schema)?
            .iter()
            .map(ColumnType::of)
            .collect::<Result<Vec<_>>>()?;
        let want_times = self
            .def
            .time_types(schema)?
            .iter()
            .map(ColumnType::of)
            .collect::<Result<Vec<_>>>()?;
        let (Some(have_keys), Some(have_times)) = (&self.header.key_types, &self.header.time_types)
        else {
            return Err(Error::KeySpec(format!(
                "sidecar segment for {} records no column types; rebuild the index with the dataset schema",
                self.def.name()
            )));
        };
        for (col, (have, want)) in self
            .def
            .columns
            .iter()
            .zip(have_keys.iter().zip(&want_keys))
        {
            if have != want {
                return Err(Error::KeySpec(format!(
                    "sidecar segment for {} records key column {col} as {}, but the dataset schema declares {}; rebuild the sidecar for the current schema",
                    self.def.name(),
                    describe_col_type(have),
                    describe_col_type(want)
                )));
            }
        }
        for (col, (have, want)) in self
            .def
            .time_columns
            .iter()
            .zip(have_times.iter().zip(&want_times))
        {
            if have != want {
                return Err(Error::KeySpec(format!(
                    "sidecar segment for {} records time column {col} as {}, but the dataset schema declares {}; rebuild the sidecar for the current schema",
                    self.def.name(),
                    describe_col_type(have),
                    describe_col_type(want)
                )));
            }
        }
        Ok(())
    }

    /// The sidecar definition this segment indexes.
    #[must_use]
    pub fn def(&self) -> &SidecarDef {
        &self.def
    }

    /// The parsed segment header.
    #[must_use]
    pub fn header(&self) -> &SidecarHeader {
        &self.header
    }

    /// Number of distinct keys in the segment.
    #[must_use]
    pub fn entries(&self) -> u64 {
        self.entries
    }

    /// Inclusive `(min_rg, max_rg)` row-group range recorded in the segment.
    #[must_use]
    pub fn rowgroup_range(&self) -> Option<(RowGroupId, RowGroupId)> {
        self.rg
    }

    /// Looks up per-row-group `[min, max]` timestamp bounds for each key of
    /// `keys` present in a `bounds` sidecar. Returns an error on a `temporal2`
    /// sidecar.
    ///
    /// Memory bound (ADR 018): holds the block index, one block at a time, and
    /// the returned map for the keys asked.
    pub fn bounds<K: AsRef<[u8]>>(&self, keys: &[K]) -> Result<BTreeMap<Vec<u8>, Vec<RgBounds>>> {
        if self.def.kind != SidecarKind::Bounds {
            return Err(Error::KeySpec(format!(
                "sidecar {} is of kind {}; bounds() requires a bounds sidecar",
                self.def.name(),
                self.def.kind.as_str()
            )));
        }
        let mut out = BTreeMap::new();
        self.visit_keys(
            keys,
            |count, c, (rg_min, rg_max)| {
                let mut v = Vec::with_capacity(count);
                let mut prev_rg: Option<u32> = None;
                for _ in 0..count {
                    let rg = c.u32()?;
                    let min = c.i64()?;
                    let max = c.i64()?;
                    if min > max {
                        return Err(corrupt("bounds record has min > max"));
                    }
                    if rg < rg_min || rg > rg_max {
                        return Err(corrupt(
                            "bounds record row group lies outside the segment's rowgroup_range",
                        ));
                    }
                    if prev_rg.is_some_and(|p| rg <= p) {
                        return Err(corrupt(
                            "bounds records are not in strictly ascending row-group order",
                        ));
                    }
                    prev_rg = Some(rg);
                    v.push(RgBounds {
                        rg: RowGroupId(rg),
                        min,
                        max,
                    });
                }
                Ok(v)
            },
            |k, v| {
                out.insert(k.to_vec(), v);
            },
        )?;
        Ok(out)
    }

    /// Looks up the sorted `(valid, known, rg)` triples for each key of `keys`
    /// present in a `temporal2` sidecar. Returns an error on a `bounds` sidecar.
    ///
    /// Memory bound (ADR 018): holds the block index, one block at a time, and
    /// the returned map for the keys asked.
    pub fn triples<K: AsRef<[u8]>>(&self, keys: &[K]) -> Result<BTreeMap<Vec<u8>, Vec<Triple>>> {
        if self.def.kind != SidecarKind::Bitemporal {
            return Err(Error::KeySpec(format!(
                "sidecar {} is of kind {}; triples() requires a temporal2 sidecar",
                self.def.name(),
                self.def.kind.as_str()
            )));
        }
        let mut out = BTreeMap::new();
        self.visit_keys(
            keys,
            |count, c, (rg_min, rg_max)| {
                let mut v = Vec::with_capacity(count);
                let mut prev: Option<(i64, i64, u32)> = None;
                for _ in 0..count {
                    let valid = c.i64()?;
                    let known = c.i64()?;
                    let rg = c.u32()?;
                    if rg < rg_min || rg > rg_max {
                        return Err(corrupt(
                            "temporal2 record row group lies outside the segment's rowgroup_range",
                        ));
                    }
                    let cur = (valid, known, rg);
                    if prev.is_some_and(|p| cur <= p) {
                        return Err(corrupt(
                            "temporal2 records are not in strictly ascending (valid, known, rg) order",
                        ));
                    }
                    prev = Some(cur);
                    v.push(Triple {
                        valid,
                        known,
                        rg: RowGroupId(rg),
                    });
                }
                Ok(v)
            },
            |k, v| {
                out.insert(k.to_vec(), v);
            },
        )?;
        Ok(out)
    }

    fn visit_keys<K, T>(
        &self,
        keys: &[K],
        mut decode_records: impl FnMut(usize, &mut Cursor<'_>, (u32, u32)) -> Result<T>,
        mut on_hit: impl FnMut(&[u8], T),
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let mut sorted: Vec<&[u8]> = keys
            .iter()
            .map(AsRef::as_ref)
            .filter(|k| tuple_has_null(k) != Some(true))
            .collect();
        sorted.sort_unstable();
        sorted.dedup();
        if sorted.is_empty() || self.blocks.is_empty() {
            return Ok(());
        }
        let (rg_min, rg_max) = self
            .rg
            .map(|(a, b)| (a.0, b.0))
            .ok_or_else(|| corrupt("blocks present without a recorded rowgroup_range"))?;

        let mut pos = 0usize;
        while pos < sorted.len() {
            let key = sorted[pos];
            let part = self
                .blocks
                .partition_point(|blk| blk.first.as_slice() <= key);
            if part == 0 {
                pos += 1;
                continue;
            }
            let blk = &self.blocks[part - 1];
            if key > blk.last.as_slice() {
                pos += 1;
                continue;
            }
            let p_start = pos;
            while pos < sorted.len() && sorted[pos] <= blk.last.as_slice() {
                pos += 1;
            }
            let block_probes = &sorted[p_start..pos];

            let bytes = self.src.read_at(blk.off, blk.len as usize)?;
            let mut cur = Cursor::new(&bytes);
            let num_keys = cur.u32()?;
            if num_keys != blk.n {
                return Err(corrupt("block key count disagrees with the block index"));
            }
            let n_usize = num_keys as usize;
            let mut prev_key: Option<&[u8]> = None;
            let mut probe_idx = 0usize;
            for idx in 0..n_usize {
                let entry_key = cur.bytes()?;
                if entry_key.is_empty() {
                    return Err(corrupt("block contains an empty key"));
                }
                if idx == 0 && entry_key != blk.first.as_slice() {
                    return Err(corrupt(
                        "block's first key disagrees with the block index's first_key",
                    ));
                }
                if idx + 1 == n_usize && entry_key != blk.last.as_slice() {
                    return Err(corrupt(
                        "block's last key disagrees with the block index's last_key",
                    ));
                }
                if entry_key < blk.first.as_slice() || entry_key > blk.last.as_slice() {
                    return Err(corrupt(
                        "block key lies outside the block's first..=last range",
                    ));
                }
                if prev_key.is_some_and(|pk| entry_key <= pk) {
                    return Err(corrupt("block keys are not in strictly ascending order"));
                }
                prev_key = Some(entry_key);

                let count = cur.u32()? as usize;
                if count == 0 {
                    return Err(corrupt("block entry has zero records"));
                }
                let rec_bytes_len = count
                    .checked_mul(RECORD_BYTES)
                    .ok_or_else(|| corrupt("record count overflows byte length"))?;
                let rec_slice = cur.take(rec_bytes_len)?;
                let mut rec_cur = Cursor::new(rec_slice);
                let decoded = decode_records(count, &mut rec_cur, (rg_min, rg_max))?;
                if !rec_cur.is_done() {
                    return Err(corrupt("record slice has unconsumed bytes"));
                }

                while probe_idx < block_probes.len() && block_probes[probe_idx] < entry_key {
                    probe_idx += 1;
                }
                if probe_idx < block_probes.len() && block_probes[probe_idx] == entry_key {
                    on_hit(entry_key, decoded);
                    probe_idx += 1;
                }
            }
            if !cur.is_done() {
                return Err(corrupt("block has trailing bytes after its entries"));
            }
        }
        Ok(())
    }
}
