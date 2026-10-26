//! Byte-level layout shared by the segment writer and reader.

use std::borrow::Cow;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Mutex;

use arrow::datatypes::{DataType, Schema, TimeUnit};
use bellwether_types::format::{ENCODING_VERSION, STORAGE_VERSION};
use bellwether_types::{Error, Hash, KeySpec, Result};
use serde::{Deserialize, Serialize};

use crate::bloom::BloomHash;
use crate::encoding;

/// First and last eight bytes of every segment file.
pub const MAGIC: [u8; 8] = *b"BWSEGMNT";
/// Fixed preamble: magic, `storage_version u32`, `encoding_version u32`,
/// `header_len u32`.
pub const PREAMBLE_LEN: usize = 8 + 4 + 4 + 4;
/// Fixed footer of a storage version 1 segment: `index_offset u64`,
/// `index_len u64`, magic.
pub const FOOTER_LEN: usize = 8 + 8 + 8;
/// Fixed footer of a storage version 2 segment: `index_offset u64`,
/// `index_len u64`, `checksum u64` (XXH3-64 of the preamble, the header, and
/// the index), magic.
pub const FOOTER_LEN_V2: usize = 8 + 8 + 8 + 8;
/// The only codec in storage versions 1 and 2.
pub const CODEC_NONE: &str = "none";

/// The footer length of a segment at `storage` version.
#[must_use]
pub fn footer_len(storage: u32) -> usize {
    if storage >= 2 {
        FOOTER_LEN_V2
    } else {
        FOOTER_LEN
    }
}

/// XXH3-64 (seed 0) over the concatenation of `parts`: the integrity check
/// a storage version 2 segment stores beside every region a reader reads
/// (the index, each block's filter and directory, each value record), so a
/// corrupt byte is refused, never decoded into a wrong answer.
#[must_use]
pub fn checksum(parts: &[&[u8]]) -> u64 {
    let mut h = xxhash_rust::xxh3::Xxh3::new();
    for p in parts {
        h.update(p);
    }
    h.digest()
}

/// Appends `v` as unsigned LEB128 (seven bits per byte, least significant
/// group first, high bit set on every byte but the last).
pub(crate) fn put_leb128(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

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
    /// As `Keyed`, with a value record per side: each key's left and right
    /// row-group bitmaps (each in its own dataset's id space), and an index
    /// that ends with the one-sided key counts. Join (B3).
    KeyedJoin,
    /// As `Keyed`, and each key's value also carries one aggregate cell per
    /// row group holding it (`crate::agg`). Agg (B7).
    KeyedAgg,
}

impl Layout {
    /// The layout a key spec is stored in; `None` for a bitemporal spec,
    /// whose versions this release still stores in a `temporal2` sidecar
    /// segment ([`crate::sidecar`]), and for a spec with per-key time
    /// bounds, which ride in a `bounds` sidecar beside the key's segments.
    #[must_use]
    pub fn for_spec(spec: &KeySpec) -> Option<Self> {
        match spec {
            KeySpec::Exact { bounds: None, .. }
            | KeySpec::Text { bloom: None, .. }
            | KeySpec::List { .. } => Some(Self::Keyed),
            KeySpec::Temporal { .. } => Some(Self::KeyedTemporal),
            KeySpec::Bloom { bounds: None, .. } | KeySpec::Text { bloom: Some(_), .. } => {
                Some(Self::BloomPerRowGroup)
            }
            KeySpec::Range { .. } => Some(Self::MinMax),
            KeySpec::Join { .. } => Some(Self::KeyedJoin),
            KeySpec::Agg { .. } => Some(Self::KeyedAgg),
            KeySpec::Exact { .. } | KeySpec::Bloom { .. } | KeySpec::Bitemporal { .. } => None,
            KeySpec::Expr { over, .. } => Self::for_spec(over),
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
            other => return Err(Error::Other(encoding::no_encoding(other))),
        })
    }
}

/// Key types and an optional timestamp type as one phrase for an error
/// message: `(Int64, Utf8)` or `(Int64) with ts Timestamp(µs, "UTC")`.
#[must_use]
pub fn types(keys: &[ColumnType], ts: Option<&ColumnType>) -> String {
    let k: Vec<String> = keys.iter().map(ToString::to_string).collect();
    match ts {
        Some(t) => format!("({}) with ts {t}", k.join(", ")),
        None => format!("({})", k.join(", ")),
    }
}

/// The type as a reader of an error message expects it: Arrow's names
/// (`Int64`, `Decimal128(10, 2)`, `Timestamp(µs, "UTC")`), never the Rust
/// debug form.
impl std::fmt::Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let unit = |u: &RecordedUnit| match u {
            RecordedUnit::Second => "s",
            RecordedUnit::Millisecond => "ms",
            RecordedUnit::Microsecond => "µs",
            RecordedUnit::Nanosecond => "ns",
        };
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
            Self::Time32 { unit: u } => write!(f, "Time32({})", unit(u)),
            Self::Time64 { unit: u } => write!(f, "Time64({})", unit(u)),
            Self::Timestamp { unit: u, tz: None } => write!(f, "Timestamp({})", unit(u)),
            Self::Timestamp {
                unit: u,
                tz: Some(tz),
            } => write!(f, "Timestamp({}, {tz:?})", unit(u)),
        }
    }
}

/// The dataset types a key spec's columns take from a dataset schema: the
/// key columns in prefix order, and the timestamp column for temporal specs.
/// An `expr` key's one key column is its virtual column, at its
/// expression's type. A list key's one "key type" is its column's element
/// type (the list item type, or a map's key type), the type its elements
/// are encoded at. Public so every crate that builds or prices segments
/// takes key types from this one definition.
pub fn spec_types(spec: &KeySpec, schema: &Schema) -> Result<(Vec<DataType>, Option<DataType>)> {
    if let KeySpec::Expr { .. } = spec {
        let t = crate::expr::typed_spec(spec, schema)?;
        return Ok((vec![t.data_type().clone()], None));
    }
    let field = |column: &str| -> Result<DataType> {
        Ok(schema
            .field_with_name(column)
            .map_err(|_| Error::KeySpec(format!("column {column} is not in the dataset schema")))?
            .data_type()
            .clone())
    };
    let lookup = |column: &str| -> Result<DataType> {
        let dt = field(column)?;
        encoding::check_supported(&dt)?;
        Ok(dt)
    };
    if let KeySpec::List { column } = spec {
        return Ok((
            vec![crate::list::check_list_column(column, &field(column)?)?],
            None,
        ));
    }
    let keys = spec
        .columns()
        .iter()
        .map(|c| lookup(c))
        .collect::<Result<Vec<_>>>()?;
    if let KeySpec::Text { column, .. } = spec {
        crate::text::check_column_type(column, &keys[0])?;
    }
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
    /// For a text key (and only a text key): the Unicode tables its tokens
    /// were folded and split under (`docs/format.md`, "Text keys"). A
    /// reader built on other tables refuses the segment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text_unicode: Option<crate::text::UnicodeTables>,
    /// For a join key (B3) only: the left dataset's manifest sequence whose
    /// exact segments the pair index was built from (provenance).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub left_sequence: Option<u64>,
    /// For a join key (B3) only: the identity of the left's live row-group
    /// set the pair index was built over ([`crate::join::live_identity`]).
    /// The pair stays valid while the left's live set is that set: a left
    /// commit that adds, changes, or removes a file makes it stale.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub left_live: Option<Hash>,
    /// For a join key (B3) only: the right dataset's manifest sequence the
    /// pair index was built at (ADR 020).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub right_sequence: Option<u64>,
    /// For a join key (B3) only: BLAKE3 of the right dataset's manifest
    /// bytes the pair index was built at, its content identity. The pair
    /// index describes the right dataset only at that manifest; a reader
    /// whose right dataset's manifest is another (a later sequence, or the
    /// same sequence of an index deleted and rebuilt) refuses to plan with
    /// it (`Error::StaleJoin`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub right_manifest: Option<Hash>,
    /// For a join key (B3) only: each right key column's dataset type, as
    /// `key_types` records the left's; present together with `key_types`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub right_key_types: Option<Vec<ColumnType>>,
    /// For an agg key (and only an agg key) written from a dataset schema:
    /// the value column's dataset type, which its cells were encoded and
    /// summed at (a decimal's scale is not visible in the bytes). Recorded
    /// together with `key_types`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_type: Option<ColumnType>,
    /// Fields written by a newer minor writer, preserved verbatim.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

impl SegmentHeader {
    /// A header for `spec` at the current versions, with canonical filters.
    /// A spec with no segment layout of its own ([`Layout::for_spec`]) gets
    /// the keyed layout here and is refused by [`SegmentHeader::validate`],
    /// so no such segment is ever written.
    #[must_use]
    pub fn new(spec: KeySpec) -> Self {
        let layout = Layout::for_spec(&spec).unwrap_or(Layout::Keyed);
        let spec_is_text = matches!(spec, KeySpec::Text { .. });
        Self {
            storage_version: STORAGE_VERSION,
            encoding_version: ENCODING_VERSION,
            bloom_hash: BloomHash::Canonical,
            layout,
            key_spec: spec,
            codec: CODEC_NONE.to_string(),
            key_types: None,
            ts_type: None,
            text_unicode: spec_is_text.then(crate::text::UnicodeTables::current),
            value_type: None,
            left_sequence: None,
            left_live: None,
            right_sequence: None,
            right_manifest: None,
            right_key_types: None,
            extra: serde_json::Map::new(),
        }
    }

    /// Checks a parsed header against its preamble and the rules of its
    /// storage version. Versions are checked again here (they were checked
    /// in the preamble before parsing) and must agree with the preamble's.
    /// A spec that stores counts needs storage version 2.
    #[allow(clippy::too_many_lines)] // one check per header rule, in order
    pub fn validate(&self, preamble_storage: u32, preamble_encoding: u32) -> Result<()> {
        check_versions(self.storage_version, self.encoding_version)?;
        if self.storage_version != preamble_storage || self.encoding_version != preamble_encoding {
            return Err(corrupt(&format!(
                "header versions ({}, {}) disagree with preamble ({preamble_storage}, {preamble_encoding})",
                self.storage_version, self.encoding_version
            )));
        }
        if self.storage_version == 0 {
            return Err(corrupt("storage version 0"));
        }
        if self.codec != CODEC_NONE {
            return Err(Error::Other(format!(
                "segment codec {:?} is not supported",
                self.codec
            )));
        }
        let Some(layout) = Layout::for_spec(&self.key_spec) else {
            return Err(Error::KeySpec(format!(
                "{} has no key segment of its own in this release: per-key time bounds and \
                 bitemporal versions are stored as sidecar segments",
                self.key_spec.identity()
            )));
        };
        if self.layout != layout {
            return Err(corrupt("layout does not match key spec"));
        }
        if self.key_spec.counts() && self.storage_version < 2 {
            return Err(corrupt(
                "a storage version 1 segment cannot store counts (they arrived in version 2)",
            ));
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
            (self.key_spec.physical(), self.bloom_hash)
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
        let agg = matches!(self.key_spec, KeySpec::Agg { .. });
        match (&self.value_type, &self.key_types) {
            (Some(_), _) if !agg => {
                return Err(corrupt("value_type on a segment that is not an agg key"));
            }
            (Some(_), None) => return Err(corrupt("value_type without key_types")),
            (None, Some(_)) if agg => {
                return Err(corrupt(
                    "an agg segment records key_types without value_type",
                ));
            }
            _ => {}
        }
        if agg && self.storage_version < 2 {
            return Err(corrupt(
                "a storage version 1 segment cannot hold an agg key (agg keys arrived in version 2)",
            ));
        }
        crate::agg::check_spec(&self.key_spec)?;
        match (&self.key_spec, &self.text_unicode) {
            (KeySpec::Text { .. }, None) => {
                return Err(corrupt("a text segment records its Unicode tables"));
            }
            (KeySpec::Text { .. }, Some(t)) => {
                let now = crate::text::UnicodeTables::current();
                if *t != now {
                    return Err(Error::KeySpec(format!(
                        "this {} segment was built with the Unicode tables of {} (case folding) \
                         and {} (word boundaries), and this binary folds and splits text with {} \
                         and {}, so the same value could give other tokens; {}",
                        self.key_spec.name(),
                        t.case,
                        t.words,
                        now.case,
                        now.words,
                        crate::text::REBUILD_STEP
                    )));
                }
            }
            (_, Some(_)) => {
                return Err(corrupt("text_unicode on a segment that is not a text key"))
            }
            (_, None) => {}
        }
        // A join key's header pins what its pair was built from: the left
        // manifest sequence and live set, the right manifest by sequence and
        // content; it records the right key types exactly when it records
        // the left's. No other segment carries any of them.
        let pins = [
            self.left_sequence.is_some(),
            self.left_live.is_some(),
            self.right_sequence.is_some(),
            self.right_manifest.is_some(),
        ];
        if let KeySpec::Join { right_columns, .. } = &self.key_spec {
            if self.storage_version < 2 {
                return Err(corrupt(
                    "a join segment is a storage version 2 kind; version 1 has none",
                ));
            }
            if pins.contains(&false) {
                return Err(corrupt(
                    "a join segment records the left manifest sequence and live row groups and \
                     the right manifest sequence and content hash it was built from",
                ));
            }
            if self.right_key_types.is_some() != self.key_types.is_some() {
                return Err(corrupt(
                    "right_key_types is recorded exactly when key_types is",
                ));
            }
            if self
                .right_key_types
                .as_ref()
                .is_some_and(|t| t.len() != right_columns.len())
            {
                return Err(corrupt(
                    "right_key_types does not match the join key's right columns",
                ));
            }
        } else if pins.contains(&true) || self.right_key_types.is_some() {
            return Err(corrupt(
                "left_sequence, left_live, right_sequence, right_manifest or right_key_types on a \
                 segment that is not a join key",
            ));
        }
        // An expr key's header carries its expression and conformance
        // vectors; a reader recomputes them before trusting the keys.
        crate::join::check_spec(&self.key_spec)?;
        crate::expr::check_spec(&self.key_spec)
    }
}

/// Parses a segment's JSON header. A storage version 2 header in a shape
/// the next storage version 2 release adds (the `bitemporal` layout, a key
/// spec with per-key `bounds` or of the bitemporal kind, a header `bounds`
/// field, each of which changes the value records) is refused with
/// [`Error::UnsupportedShape`], before the typed parse could misread it or
/// fail with a serde error (ADR 028).
pub(crate) fn parse_header(bytes: &[u8], storage: u32) -> Result<SegmentHeader> {
    if storage >= 2 {
        if let Ok(v) = serde_json::from_slice::<serde_json::Value>(bytes) {
            let spec = v.get("key_spec");
            let found = || {
                spec.and_then(|s| serde_json::from_value::<KeySpec>(s.clone()).ok())
                    .map_or_else(|| "in a segment".to_string(), |s| s.identity())
            };
            let shape = if v.get("layout").and_then(|l| l.as_str()) == Some("bitemporal") {
                Some(("segment layout", "bitemporal".to_string()))
            } else if spec.and_then(|s| s.get("kind")).and_then(|k| k.as_str())
                == Some("bitemporal")
            {
                Some(("bitemporal key spec", found()))
            } else if spec.is_some_and(|s| s.get("bounds").is_some()) {
                Some(("key spec with per-key bounds", found()))
            } else if v.get("bounds").is_some() {
                Some(("segment header field", "bounds".to_string()))
            } else {
                None
            };
            if let Some((what, found)) = shape {
                return Err(Error::UnsupportedShape { what, found });
            }
        }
    }
    Ok(serde_json::from_slice(bytes)?)
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

    /// An unsigned LEB128 value in its shortest form: an over-long encoding
    /// (a last byte of zero after the first) or one past 64 bits is refused,
    /// so every count has exactly one spelling.
    pub(crate) fn leb128(&mut self) -> Result<u64> {
        let mut v: u64 = 0;
        for i in 0..10u32 {
            let b = self.u8()?;
            let bits = u64::from(b & 0x7f);
            if i == 9 && b > 1 {
                return Err(corrupt("count exceeds 64 bits"));
            }
            v |= bits << (7 * i);
            if b & 0x80 == 0 {
                if b == 0 && i > 0 {
                    return Err(corrupt("count is not in its shortest LEB128 form"));
                }
                return Ok(v);
            }
        }
        Err(corrupt("count exceeds 64 bits"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leb(bytes: &[u8]) -> Result<u64> {
        let mut c = Cursor::new(bytes);
        let v = c.leb128()?;
        assert!(c.is_done(), "{bytes:02x?} read past one value");
        Ok(v)
    }

    /// Every count has exactly one spelling: the shortest LEB128 form reads
    /// back, an over-long one (a zero last byte after the first) and one
    /// past 64 bits are refused.
    #[test]
    fn leb128_reads_only_the_shortest_form() {
        for v in [
            0u64,
            1,
            127,
            128,
            300,
            16_383,
            16_384,
            u64::from(u32::MAX),
            u64::MAX,
        ] {
            let mut out = Vec::new();
            put_leb128(&mut out, v);
            assert_eq!(leb(&out).expect("the shortest form reads back"), v);
        }
        for over_long in [
            &[0x80, 0x00][..],
            &[0x81, 0x00],
            &[0xac, 0x82, 0x00],
            &[0xff, 0x80, 0x00],
        ] {
            let err = leb(over_long)
                .expect_err("an over-long form is refused")
                .to_string();
            assert!(
                err.contains("shortest LEB128 form"),
                "{over_long:02x?}: {err}"
            );
        }
        let too_wide = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02];
        assert!(leb(&too_wide).is_err());
        assert!(leb(&[0xff; 11]).is_err());
    }
}
