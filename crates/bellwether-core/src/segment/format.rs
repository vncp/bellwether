//! Byte-level layout shared by the segment writer and reader.

use std::borrow::Cow;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Mutex;

use arrow::datatypes::{DataType, Schema, TimeUnit};
use bellwether_types::format::{ENCODING_VERSION, STORAGE_VERSION};
use bellwether_types::{Error, KeySpec, Result};
use serde::{Deserialize, Serialize};

use crate::bloom::BloomHash;
use crate::encoding;

/// First and last eight bytes of every segment file.
pub const MAGIC: [u8; 8] = *b"BWSEGMNT";
/// Fixed preamble: magic, `storage_version u32`, `encoding_version u32`,
/// `header_len u32`.
pub const PREAMBLE_LEN: usize = 8 + 4 + 4 + 4;
/// Fixed footer: `index_offset u64`, `index_len u64`, magic.
pub const FOOTER_LEN: usize = 8 + 8 + 8;
/// The only codec in storage version 1.
pub const CODEC_NONE: &str = "none";

/// How the body of a segment is organized; follows from the key spec kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Layout {
    /// Sorted key blocks, one SBBF per block, key to roaring bitmap. Exact.
    Keyed,
    /// As `Keyed`, and each key's value also carries a version chain and
    /// `max_ts`. Temporal.
    KeyedTemporal,
    /// One SBBF per row group over that row group's keys. Bloom.
    BloomPerRowGroup,
    /// Per-row-group `(min, max)`, sorted by `min`, in blocks. Range.
    MinMax,
}

impl Layout {
    /// The layout a key spec is stored in.
    #[must_use]
    pub fn for_spec(spec: &KeySpec) -> Self {
        match spec {
            KeySpec::Exact { .. } => Self::Keyed,
            KeySpec::Temporal { .. } => Self::KeyedTemporal,
            KeySpec::Bloom { .. } => Self::BloomPerRowGroup,
            KeySpec::Range { .. } => Self::MinMax,
        }
    }
}

/// A time unit as recorded in a segment header.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordedUnit {
    /// Seconds.
    Second,
    /// Milliseconds.
    Millisecond,
    /// Microseconds.
    Microsecond,
    /// Nanoseconds.
    Nanosecond,
}

impl From<TimeUnit> for RecordedUnit {
    fn from(u: TimeUnit) -> Self {
        match u {
            TimeUnit::Second => Self::Second,
            TimeUnit::Millisecond => Self::Millisecond,
            TimeUnit::Microsecond => Self::Microsecond,
            TimeUnit::Nanosecond => Self::Nanosecond,
        }
    }
}
impl RecordedUnit {
    /// Short name (`s`, `ms`, `µs`, `ns`).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Second => "s",
            Self::Millisecond => "ms",
            Self::Microsecond => "µs",
            Self::Nanosecond => "ns",
        }
    }
}

/// A key or timestamp column's dataset type as recorded in a segment header.
/// Bellwether owns this spelling, so the frozen format does not depend on
/// Arrow's serialization of `DataType`. A dictionary column is recorded as
/// its value type, because the encoding sees only decoded values.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ColumnType {
    /// Boolean.
    Boolean,
    /// Int8.
    Int8,
    /// Int16.
    Int16,
    /// Int32.
    Int32,
    /// Int64.
    Int64,
    /// `UInt8`.
    #[serde(rename = "uint8")]
    UInt8,
    /// `UInt16`.
    #[serde(rename = "uint16")]
    UInt16,
    /// `UInt32`.
    #[serde(rename = "uint32")]
    UInt32,
    /// `UInt64`.
    #[serde(rename = "uint64")]
    UInt64,
    /// Float16.
    Float16,
    /// Float32.
    Float32,
    /// Float64.
    Float64,
    /// Decimal128 with precision and scale.
    Decimal128 {
        /// Precision.
        precision: u8,
        /// Scale.
        scale: i8,
    },
    /// Decimal256 with precision and scale.
    Decimal256 {
        /// Precision.
        precision: u8,
        /// Scale.
        scale: i8,
    },
    /// Utf8.
    Utf8,
    /// `LargeUtf8`.
    LargeUtf8,
    /// `Utf8View`.
    Utf8View,
    /// Binary.
    Binary,
    /// `LargeBinary`.
    LargeBinary,
    /// `BinaryView`.
    BinaryView,
    /// `FixedSizeBinary` of a byte width.
    FixedSizeBinary {
        /// Width in bytes.
        size: i32,
    },
    /// Date32.
    Date32,
    /// Date64.
    Date64,
    /// Time32 in a unit.
    Time32 {
        /// Unit.
        unit: RecordedUnit,
    },
    /// Time64 in a unit.
    Time64 {
        /// Unit.
        unit: RecordedUnit,
    },
    /// Timestamp in a unit, with the zone the dataset declares.
    Timestamp {
        /// Unit.
        unit: RecordedUnit,
        /// Declared zone, if any.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tz: Option<String>,
    },
}

impl ColumnType {
    /// The recorded form of a dataset type; an error for a type with no
    /// canonical encoding.
    pub fn of(dt: &DataType) -> Result<Self> {
        encoding::check_supported(dt)?;
        Ok(match dt {
            DataType::Dictionary(_, v) => return Self::of(v),
            DataType::Boolean => Self::Boolean,
            DataType::Int8 => Self::Int8,
            DataType::Int16 => Self::Int16,
            DataType::Int32 => Self::Int32,
            DataType::Int64 => Self::Int64,
            DataType::UInt8 => Self::UInt8,
            DataType::UInt16 => Self::UInt16,
            DataType::UInt32 => Self::UInt32,
            DataType::UInt64 => Self::UInt64,
            DataType::Float16 => Self::Float16,
            DataType::Float32 => Self::Float32,
            DataType::Float64 => Self::Float64,
            DataType::Decimal128(p, s) => Self::Decimal128 {
                precision: *p,
                scale: *s,
            },
            DataType::Decimal256(p, s) => Self::Decimal256 {
                precision: *p,
                scale: *s,
            },
            DataType::Utf8 => Self::Utf8,
            DataType::LargeUtf8 => Self::LargeUtf8,
            DataType::Utf8View => Self::Utf8View,
            DataType::Binary => Self::Binary,
            DataType::LargeBinary => Self::LargeBinary,
            DataType::BinaryView => Self::BinaryView,
            DataType::FixedSizeBinary(n) => Self::FixedSizeBinary { size: *n },
            DataType::Date32 => Self::Date32,
            DataType::Date64 => Self::Date64,
            DataType::Time32(u) => Self::Time32 { unit: (*u).into() },
            DataType::Time64(u) => Self::Time64 { unit: (*u).into() },
            DataType::Timestamp(u, tz) => Self::Timestamp {
                unit: (*u).into(),
                tz: tz.as_ref().map(ToString::to_string),
            },
            other => {
                encoding::check_supported(other)?;
                return Err(Error::Other(format!(
                    "type {} has no canonical encoding in encoding version 1",
                    encoding::type_summary(other)
                )));
            }
        })
    }
}

impl std::fmt::Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Boolean => f.write_str("Boolean"),
            Self::Int8 => f.write_str("Int8"),
            Self::Int16 => f.write_str("Int16"),
            Self::Int32 => f.write_str("Int32"),
            Self::Int64 => f.write_str("Int64"),
            Self::UInt8 => f.write_str("UInt8"),
            Self::UInt16 => f.write_str("UInt16"),
            Self::UInt32 => f.write_str("UInt32"),
            Self::UInt64 => f.write_str("UInt64"),
            Self::Float16 => f.write_str("Float16"),
            Self::Float32 => f.write_str("Float32"),
            Self::Float64 => f.write_str("Float64"),
            Self::Decimal128 { precision, scale } => write!(f, "Decimal128({precision}, {scale})"),
            Self::Decimal256 { precision, scale } => write!(f, "Decimal256({precision}, {scale})"),
            Self::Utf8 => f.write_str("Utf8"),
            Self::LargeUtf8 => f.write_str("LargeUtf8"),
            Self::Utf8View => f.write_str("Utf8View"),
            Self::Binary => f.write_str("Binary"),
            Self::LargeBinary => f.write_str("LargeBinary"),
            Self::BinaryView => f.write_str("BinaryView"),
            Self::FixedSizeBinary { size } => write!(f, "FixedSizeBinary({size})"),
            Self::Date32 => f.write_str("Date32"),
            Self::Date64 => f.write_str("Date64"),
            Self::Time32 { unit } => write!(f, "Time32({})", unit.as_str()),
            Self::Time64 { unit } => write!(f, "Time64({})", unit.as_str()),
            Self::Timestamp { unit, tz: None } => write!(f, "Timestamp({})", unit.as_str()),
            Self::Timestamp { unit, tz: Some(tz) } => {
                write!(f, "Timestamp({}, {tz:?})", unit.as_str())
            }
        }
    }
}

/// The dataset types a key spec's columns take from a dataset schema: the
/// key columns in prefix order, and the timestamp column for temporal specs.
pub(crate) fn spec_types(
    spec: &KeySpec,
    schema: &Schema,
) -> Result<(Vec<DataType>, Option<DataType>)> {
    let lookup = |column: &str| -> Result<DataType> {
        let dt = schema
            .field_with_name(column)
            .map_err(|_| Error::KeySpec(format!("column {column} is not in the dataset schema")))?
            .data_type()
            .clone();
        encoding::check_supported(&dt)?;
        Ok(dt)
    };
    let keys = spec
        .columns()
        .iter()
        .map(|c| lookup(c))
        .collect::<Result<Vec<_>>>()?;
    let ts = match spec {
        KeySpec::Temporal { ts_column, .. } => {
            let dt = lookup(ts_column)?;
            encoding::check_ts_type(&dt)?;
            Some(dt)
        }
        _ => None,
    };
    Ok((keys, ts))
}

/// The JSON header after the preamble. Unknown fields are preserved in
/// `extra` so a rewrite does not drop them, and merge carries them through
/// when every input agrees (ADR 017).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SegmentHeader {
    /// `bellwether_types::format::STORAGE_VERSION` at write time.
    pub storage_version: u32,
    /// `bellwether_types::format::ENCODING_VERSION` at write time.
    pub encoding_version: u32,
    /// The key spec this segment indexes.
    pub key_spec: KeySpec,
    /// Compression codec of the body. Only `"none"` exists in version 1.
    pub codec: String,
    /// Body layout.
    pub layout: Layout,
    /// What the segment's bloom filters hash (ADR 008 amendment). Present in
    /// every header (`docs/format.md`): `canonical` for keyed and
    /// keyed-temporal block filters, `canonical` or `parquet_plain` for bloom
    /// segments, and `canonical` for range segments, which have no filters.
    /// A missing or unknown value is refused on open.
    pub bloom_hash: BloomHash,
    /// Each key column's dataset type, in prefix order, as the segment's keys
    /// were encoded under. Recorded by writers built from a dataset schema
    /// ([`super::SegmentWriter::for_dataset`]); absent for segments written
    /// from already-encoded keys. Segments whose recorded types differ never
    /// merge, and [`super::SegmentReader::open_for_dataset`] refuses a
    /// segment whose recorded types differ from the dataset's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_types: Option<Vec<ColumnType>>,
    /// The temporal timestamp column's dataset type and unit; recorded
    /// together with `key_types` for temporal specs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ts_type: Option<ColumnType>,
    /// Fields written by a newer minor writer, preserved verbatim.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

impl SegmentHeader {
    /// A header for `spec` at the current versions, with canonical filters.
    #[must_use]
    pub fn new(spec: KeySpec) -> Self {
        let layout = Layout::for_spec(&spec);
        Self {
            storage_version: STORAGE_VERSION,
            encoding_version: ENCODING_VERSION,
            bloom_hash: BloomHash::Canonical,
            layout,
            key_spec: spec,
            codec: CODEC_NONE.to_string(),
            key_types: None,
            ts_type: None,
            extra: serde_json::Map::new(),
        }
    }

    /// Checks a parsed header against its preamble and the rules of storage
    /// version 1. Versions are checked again here (they were checked in the
    /// preamble before parsing) and must agree with the preamble's.
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
                "segment codec {:?} is not supported",
                self.codec
            )));
        }
        if self.layout != Layout::for_spec(&self.key_spec) {
            return Err(corrupt("layout does not match key spec"));
        }
        // Only bloom segments may hold reused Parquet filters.
        if self.bloom_hash == BloomHash::ParquetPlain && self.layout != Layout::BloomPerRowGroup {
            return Err(corrupt(&format!(
                "bloom_hash {} is not valid for layout {:?}",
                self.bloom_hash.as_str(),
                self.layout
            )));
        }
        if let (KeySpec::Bloom { columns, .. }, BloomHash::ParquetPlain) =
            (&self.key_spec, self.bloom_hash)
        {
            if columns.len() != 1 {
                return Err(corrupt("parquet_plain filters cover exactly one column"));
            }
        }
        // Recorded types: one per key column, and the ts type exactly when a
        // temporal spec records key types.
        let temporal = matches!(self.key_spec, KeySpec::Temporal { .. });
        match &self.key_types {
            Some(types) if types.len() != self.key_spec.columns().len() => {
                return Err(corrupt("key_types does not match the key spec's columns"));
            }
            Some(_) if temporal != self.ts_type.is_some() => {
                return Err(corrupt("ts_type is recorded exactly for temporal specs"));
            }
            None if self.ts_type.is_some() => {
                return Err(corrupt("ts_type without key_types"));
            }
            _ => {}
        }
        Ok(())
    }
}

/// Checks the preamble's versions before anything else is parsed.
pub fn check_versions(storage: u32, encoding: u32) -> Result<()> {
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

/// Random-access bytes a segment is read from. Implemented for anything that
/// is `AsRef<[u8]>` (a `Vec<u8>`, `bytes::Bytes`, a caller-provided memory
/// map), which borrows, and for [`FileRange`], which reads each range from
/// disk. The reader only ever asks for the ranges a lookup needs.
pub trait ReadAt {
    /// Total length in bytes.
    fn size(&self) -> Result<u64>;
    /// Reads exactly `len` bytes at `offset`.
    fn read_at(&self, offset: u64, len: usize) -> Result<Cow<'_, [u8]>>;
}

impl<T: AsRef<[u8]> + ?Sized> ReadAt for T {
    fn size(&self) -> Result<u64> {
        Ok(self.as_ref().len() as u64)
    }

    fn read_at(&self, offset: u64, len: usize) -> Result<Cow<'_, [u8]>> {
        let bytes = self.as_ref();
        let start = usize::try_from(offset).map_err(|_| corrupt("offset"))?;
        let end = start.checked_add(len).ok_or_else(|| corrupt("range"))?;
        bytes
            .get(start..end)
            .map(Cow::Borrowed)
            .ok_or_else(|| corrupt("range past end"))
    }
}

/// A segment file read by range with positioned reads.
#[derive(Debug)]
pub struct FileRange {
    file: Mutex<File>,
    size: u64,
}

impl FileRange {
    /// Opens a file for range reads.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let file = File::open(path).map_err(|e| Error::Store(e.to_string()))?;
        let size = file
            .metadata()
            .map_err(|e| Error::Store(e.to_string()))?
            .len();
        Ok(Self {
            file: Mutex::new(file),
            size,
        })
    }
}

impl ReadAt for FileRange {
    fn size(&self) -> Result<u64> {
        Ok(self.size)
    }

    fn read_at(&self, offset: u64, len: usize) -> Result<Cow<'_, [u8]>> {
        if offset.saturating_add(len as u64) > self.size {
            return Err(corrupt("range past end"));
        }
        let mut f = self
            .file
            .lock()
            .map_err(|_| Error::Store("segment file lock poisoned".into()))?;
        let mut buf = vec![0u8; len];
        f.seek(SeekFrom::Start(offset))
            .and_then(|_| f.read_exact(&mut buf))
            .map_err(|e| Error::Store(e.to_string()))?;
        Ok(Cow::Owned(buf))
    }
}

pub(crate) fn corrupt(what: &str) -> Error {
    Error::Other(format!("corrupt segment: {what}"))
}

/// Little-endian appenders.
pub(crate) trait Put {
    fn put_u8(&mut self, v: u8);
    fn put_u32(&mut self, v: u32);
    fn put_u64(&mut self, v: u64);
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
    /// Length-prefixed (`u32`) bytes.
    fn put_bytes(&mut self, v: &[u8]) -> Result<()> {
        self.put_u32(len_u32(v.len())?);
        self.extend_from_slice(v);
        Ok(())
    }
}

pub(crate) fn len_u32(n: usize) -> Result<u32> {
    u32::try_from(n).map_err(|_| Error::Other(format!("length {n} exceeds u32")))
}

/// Bounds-checked little-endian reader over a byte slice.
pub(crate) struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or_else(|| corrupt("length"))?;
        let s = self
            .buf
            .get(self.pos..end)
            .ok_or_else(|| corrupt("truncated"))?;
        self.pos = end;
        Ok(s)
    }

    pub(crate) fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u32(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    pub(crate) fn u64(&mut self) -> Result<u64> {
        let b = self.take(8)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(b);
        Ok(u64::from_le_bytes(a))
    }

    pub(crate) fn bytes(&mut self) -> Result<&'a [u8]> {
        let n = self.u32()? as usize;
        self.take(n)
    }

    pub(crate) fn rest(&mut self) -> &'a [u8] {
        let s = &self.buf[self.pos..];
        self.pos = self.buf.len();
        s
    }

    pub(crate) fn is_done(&self) -> bool {
        self.pos == self.buf.len()
    }
}
