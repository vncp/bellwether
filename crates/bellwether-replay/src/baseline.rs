//! Baselines without a Bellwether index: what a run reads when nothing
//! prunes ([`Baseline::RawScan`]), and when a reader prunes with Parquet's
//! own metadata ([`Baseline::ParquetStats`]). Both are exact sums of
//! row-group statistics under a stated plan, never estimates (ADR 005).
//!
//! # `ParquetStats`
//!
//! The pruning `docs/benchmark.md` defines for the Parquet-native baseline:
//! a row group is read when some key tuple passes, for every key column,
//! both
//!
//! - the footer's min/max for that column (`ColumnStats`, canonical bytes
//!   at the dataset type): the element lies within `[min, max]`, or a bound
//!   is unknown, or the element is the canonical NaN (statistics exclude NaN;
//!   `docs/format.md`); and
//! - the row group's embedded split-block bloom filter for that column, when
//!   it has one: the element, converted to the file column's physical value,
//!   may be in the filter. An embedded filter hashes the PLAIN encoding (ADR
//!   008 amendment), so the probe is that encoding of the converted value:
//!   - `INT32`/`INT64`: integers (unsigned as their bit pattern), dates
//!     (days, or milliseconds for `INT64`), decimals (rescaled exactly to
//!     the file's scale), times and timestamps (converted exactly to the
//!     file's unit), as 4 or 8 little-endian bytes;
//!   - `FLOAT`/`DOUBLE`: the IEEE bits (a value a `FLOAT` cannot hold
//!     exactly is not in the file; both signed zeros are probed);
//!   - `BYTE_ARRAY`: the string or binary bytes;
//!   - `FIXED_LEN_BYTE_ARRAY`: decimals as big-endian two's complement of the
//!     column's width, `Float16` as its two little-endian bytes, fixed
//!     binary as its bytes.
//!
//!   A value the file column cannot hold (out of range, inexact at its
//!   scale or unit) cannot be in the file and fails the probe. A NaN probe
//!   is never pruned by a filter: writers hash whatever NaN payload they
//!   wrote. A run column whose embedded filter has none of these shapes
//!   (`BOOLEAN`, `INT96`, a decimal stored as `BYTE_ARRAY`, a `Decimal256`
//!   dataset column) is refused with [`ReplayError::UnprobeableFilter`]: the
//!   baseline is never silently weaker than its definition.
//!
//! A window run ([`crate::RunWindow`]) is read by both baselines only where
//! its time column can hold a row of the window: `ParquetStats` also skips a
//! row group whose footer min/max for that column (known, canonical bytes)
//! lie wholly outside the window, as a Parquet reader's range predicate
//! would; `RawScan` still reads every live row group. The residual is then
//! `And[In, Between]` on the time column.
//!
//! Filters are read when the baseline is built, from local files whose
//! fingerprint is verified on the opened handle first, and are held for the
//! baseline's lifetime: every embedded filter of every run column of every
//! file. A [`crate::Replayer`] pricing unserved runs under `ParquetStats`
//! reads each column's filters once and keeps them for its own lifetime.
//! Nothing else of a file is read. On `bellwether-bench` corpora this is
//! the bench's `parquet_stats` strategy, and the bench tests check
//! equality run by run.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::{i256, ArrowPrimitiveType, DataType, Float16Type, Schema};
use bellwether_core::encoding::NAN_KEY;
use bellwether_source::{FingerprintPolicy, LocalFs, Storage};
use bellwether_types::{ColumnStats, Error, FileFingerprint, Hash, ScanPlan};
use parquet::basic::{ConvertedType, LogicalType, TimeUnit as ParquetUnit, Type as PhysicalType};
use parquet::bloom_filter::Sbbf;
use parquet::data_type::ByteArray;
use parquet::file::properties::ReaderProperties;
use parquet::file::reader::{ChunkReader, FileReader, SerializedFileReader};
use parquet::file::serialized_reader::ReadOptionsBuilder;
use parquet::schema::types::ColumnDescriptor;
use roaring::RoaringBitmap;

use crate::catalog::Catalog;
use crate::error::{ReplayError, ReplayResult};
use crate::keys::RunKeys;
use crate::plan::{with_residual, EncodedWindow};
use crate::workload::RunWindow;

/// A configuration without a Bellwether index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Baseline {
    /// Every live row group, whatever the keys.
    RawScan,
    /// Footer min/max, then embedded bloom filters (module docs).
    ParquetStats,
}

impl Baseline {
    /// The name a [`bellwether_types::Replay`] records in `unserved`:
    /// `raw_scan` or `parquet_stats`.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::RawScan => "raw_scan",
            Self::ParquetStats => "parquet_stats",
        }
    }
}

/// Time units by their number of steps per second.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Unit {
    Second,
    Milli,
    Micro,
    Nano,
}

impl Unit {
    fn per_second(self) -> i64 {
        match self {
            Self::Second => 1,
            Self::Milli => 1_000,
            Self::Micro => 1_000_000,
            Self::Nano => 1_000_000_000,
        }
    }

    fn of_parquet(u: ParquetUnit) -> Self {
        match u {
            ParquetUnit::MILLIS => Self::Milli,
            ParquetUnit::MICROS => Self::Micro,
            ParquetUnit::NANOS => Self::Nano,
        }
    }

    fn of_tag(tag: u8) -> Option<Self> {
        match tag {
            0x01 => Some(Self::Second),
            0x02 => Some(Self::Milli),
            0x03 => Some(Self::Micro),
            0x04 => Some(Self::Nano),
            _ => None,
        }
    }
}

/// Converts `v` from unit `from` to `to`: finer by checked multiplication,
/// coarser only when exact.
fn convert_unit(v: i64, from: Unit, to: Unit) -> Option<i64> {
    let (f, t) = (from.per_second(), to.per_second());
    if t >= f {
        v.checked_mul(t / f)
    } else {
        let d = f / t;
        (v % d == 0).then_some(v / d)
    }
}

/// How a run column's canonical element becomes one file column's PLAIN
/// value, for probing that column's embedded filter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Conversion {
    Int32 {
        signed: bool,
    },
    Int64 {
        signed: bool,
    },
    Float,
    Double,
    Float16,
    DecimalInt32 {
        scale: i8,
        precision: u8,
    },
    DecimalInt64 {
        scale: i8,
        precision: u8,
    },
    DecimalFixed {
        scale: i8,
        precision: u8,
        len: usize,
    },
    Bytes,
    FixedBytes {
        len: usize,
    },
    DateDays,
    DateMillis,
    Timestamp {
        unit: Unit,
    },
    TimeInt32 {
        unit: Unit,
    },
    TimeInt64 {
        unit: Unit,
    },
}

/// The conversion for a run column of dataset type `dataset` into the file
/// column `descr`, or why there is none.
#[allow(clippy::too_many_lines)]
fn conversion(dataset: &DataType, descr: &ColumnDescriptor) -> Result<Conversion, String> {
    let logical = descr.logical_type_ref();
    let converted = descr.converted_type();
    let physical = descr.physical_type();
    let shape = || format!("{physical} {logical:?} {converted:?}");
    let dataset = match dataset {
        DataType::Dictionary(_, v) => v.as_ref(),
        d => d,
    };
    let integer = || match &logical {
        Some(LogicalType::Integer(t)) => Some(t.is_signed),
        Some(_) => None,
        None => match converted {
            ConvertedType::NONE
            | ConvertedType::INT_8
            | ConvertedType::INT_16
            | ConvertedType::INT_32
            | ConvertedType::INT_64 => Some(true),
            ConvertedType::UINT_8
            | ConvertedType::UINT_16
            | ConvertedType::UINT_32
            | ConvertedType::UINT_64 => Some(false),
            _ => None,
        },
    };
    let decimal = || -> Option<(i8, u8)> {
        match &logical {
            Some(LogicalType::Decimal(d)) => {
                Some((i8::try_from(d.scale).ok()?, u8::try_from(d.precision).ok()?))
            }
            None if converted == ConvertedType::DECIMAL => Some((
                i8::try_from(descr.type_scale()).ok()?,
                u8::try_from(descr.type_precision()).ok()?,
            )),
            _ => None,
        }
    };
    let width = || usize::try_from(descr.type_length()).map_err(|_| shape());
    let none = || Err(format!("a {dataset} run column stored as {}", shape()));
    match dataset {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => match (physical, integer()) {
            (PhysicalType::INT32, Some(signed)) => Ok(Conversion::Int32 { signed }),
            (PhysicalType::INT64, Some(signed)) => Ok(Conversion::Int64 { signed }),
            _ => none(),
        },
        DataType::Float16 | DataType::Float32 | DataType::Float64 => match physical {
            PhysicalType::FLOAT => Ok(Conversion::Float),
            PhysicalType::DOUBLE => Ok(Conversion::Double),
            PhysicalType::FIXED_LEN_BYTE_ARRAY
                if logical == Some(&LogicalType::Float16) && descr.type_length() == 2 =>
            {
                Ok(Conversion::Float16)
            }
            _ => none(),
        },
        DataType::Decimal128(_, _) => match (physical, decimal()) {
            (PhysicalType::INT32, Some((scale, precision))) => {
                Ok(Conversion::DecimalInt32 { scale, precision })
            }
            (PhysicalType::INT64, Some((scale, precision))) => {
                Ok(Conversion::DecimalInt64 { scale, precision })
            }
            (PhysicalType::FIXED_LEN_BYTE_ARRAY, Some((scale, precision))) => {
                Ok(Conversion::DecimalFixed {
                    scale,
                    precision,
                    len: width()?,
                })
            }
            (PhysicalType::BYTE_ARRAY, Some(_)) => Err(format!(
                "a decimal stored as BYTE_ARRAY ({}): its byte length is the writer's choice, so \
                 its PLAIN value cannot be reproduced",
                shape()
            )),
            _ => none(),
        },
        DataType::Decimal256(_, _) => Err(format!(
            "a Decimal256 run column ({}) is not probed",
            shape()
        )),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => match physical {
            PhysicalType::BYTE_ARRAY
                if matches!(
                    logical,
                    None | Some(LogicalType::String | LogicalType::Enum | LogicalType::Json)
                ) && matches!(
                    converted,
                    ConvertedType::NONE
                        | ConvertedType::UTF8
                        | ConvertedType::ENUM
                        | ConvertedType::JSON
                ) =>
            {
                Ok(Conversion::Bytes)
            }
            _ => none(),
        },
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => match physical {
            PhysicalType::BYTE_ARRAY
                if matches!(logical, None | Some(LogicalType::Bson))
                    && matches!(converted, ConvertedType::NONE | ConvertedType::BSON) =>
            {
                Ok(Conversion::Bytes)
            }
            _ => none(),
        },
        DataType::FixedSizeBinary(_) => match physical {
            PhysicalType::FIXED_LEN_BYTE_ARRAY
                if matches!(logical, None | Some(LogicalType::Uuid))
                    && converted == ConvertedType::NONE =>
            {
                Ok(Conversion::FixedBytes { len: width()? })
            }
            _ => none(),
        },
        DataType::Date32 | DataType::Date64 => match physical {
            PhysicalType::INT32
                if logical == Some(&LogicalType::Date) || converted == ConvertedType::DATE =>
            {
                Ok(Conversion::DateDays)
            }
            PhysicalType::INT64 if logical.is_none() && converted == ConvertedType::NONE => {
                Ok(Conversion::DateMillis)
            }
            _ => none(),
        },
        DataType::Timestamp(_, _) => match (physical, &logical, converted) {
            (PhysicalType::INT64, Some(LogicalType::Timestamp(t)), _) => {
                Ok(Conversion::Timestamp {
                    unit: Unit::of_parquet(t.unit),
                })
            }
            (PhysicalType::INT64, None, ConvertedType::TIMESTAMP_MILLIS) => {
                Ok(Conversion::Timestamp { unit: Unit::Milli })
            }
            (PhysicalType::INT64, None, ConvertedType::TIMESTAMP_MICROS) => {
                Ok(Conversion::Timestamp { unit: Unit::Micro })
            }
            _ => none(),
        },
        DataType::Time32(_) | DataType::Time64(_) => match (physical, &logical, converted) {
            (PhysicalType::INT32, Some(LogicalType::Time(t)), _) => Ok(Conversion::TimeInt32 {
                unit: Unit::of_parquet(t.unit),
            }),
            (PhysicalType::INT32, None, ConvertedType::TIME_MILLIS) => {
                Ok(Conversion::TimeInt32 { unit: Unit::Milli })
            }
            (PhysicalType::INT64, Some(LogicalType::Time(t)), _) => Ok(Conversion::TimeInt64 {
                unit: Unit::of_parquet(t.unit),
            }),
            (PhysicalType::INT64, None, ConvertedType::TIME_MICROS) => {
                Ok(Conversion::TimeInt64 { unit: Unit::Micro })
            }
            _ => none(),
        },
        _ => Err(format!(
            "a {dataset} run column ({}) has no PLAIN conversion",
            shape()
        )),
    }
}

/// What one probe of an embedded filter is.
#[derive(Debug, PartialEq, Eq)]
enum Probe {
    /// The PLAIN bytes to hash.
    Plain(Vec<u8>),
    /// The value cannot be in the file column.
    Absent,
    /// The filter cannot exclude the value (a NaN).
    Any,
}

/// A canonical element decoded at its dataset type.
enum Decoded {
    Int(i128),
    Float(f64),
    Decimal(i256),
    Bytes(Vec<u8>),
    /// Kind tag, unit tag, value.
    Temporal(u8, u8, i64),
}

fn decode(elem: &[u8]) -> Option<Decoded> {
    let fixed8 = |b: &[u8]| -> Option<[u8; 8]> { b.get(1..9)?.try_into().ok() };
    match *elem.first()? {
        0x02 if elem.len() == 9 => {
            let mut b = fixed8(elem)?;
            b[0] ^= 0x80;
            Some(Decoded::Int(i128::from(i64::from_be_bytes(b))))
        }
        0x03 if elem.len() == 9 => {
            Some(Decoded::Int(i128::from(u64::from_be_bytes(fixed8(elem)?))))
        }
        0x04 if elem.len() == 9 => {
            // Sign-aware flip (format.md): positive values had the sign bit
            // set, negative values had every bit inverted.
            let bits = u64::from_be_bytes(fixed8(elem)?);
            let raw = if bits & (1 << 63) == 0 {
                !bits
            } else {
                bits & !(1 << 63)
            };
            Some(Decoded::Float(f64::from_bits(raw)))
        }
        0x05 => {
            let (width, body) = (elem.get(1)?, elem.get(2..)?);
            let n = match width {
                0x10 => 16,
                0x20 => 32,
                _ => return None,
            };
            if body.len() != n {
                return None;
            }
            let mut b = body.to_vec();
            b[0] ^= 0x80;
            // Sign-extend to 32 bytes.
            let sign = if b[0] & 0x80 == 0 { 0x00 } else { 0xFF };
            let mut wide = [sign; 32];
            wide[32 - n..].copy_from_slice(&b);
            Some(Decoded::Decimal(i256::from_be_bytes(wide)))
        }
        0x06 | 0x07 => unescape(elem).map(Decoded::Bytes),
        0x08 if elem.len() == 11 => {
            let mut b: [u8; 8] = elem.get(3..11)?.try_into().ok()?;
            b[0] ^= 0x80;
            Some(Decoded::Temporal(elem[1], elem[2], i64::from_be_bytes(b)))
        }
        _ => None,
    }
}

/// The raw bytes of a canonical string or binary element.
fn unescape(elem: &[u8]) -> Option<Vec<u8>> {
    if !matches!(elem.first(), Some(0x06 | 0x07)) {
        return None;
    }
    let mut out = Vec::with_capacity(elem.len());
    let mut j = 1;
    while j < elem.len() {
        match (elem[j], elem.get(j + 1)) {
            (0x00, Some(0x00)) => return (j + 2 == elem.len()).then_some(out),
            (0x00, Some(0xFF)) => {
                out.push(0);
                j += 2;
            }
            (0x00, _) => return None,
            (b, _) => {
                out.push(b);
                j += 1;
            }
        }
    }
    None
}

/// `v` at `from` scale rescaled exactly to `to` scale and within
/// `precision` digits, or `None` when the file column cannot hold it.
fn rescale(v: i256, from: i8, to: i8, precision: u8) -> Option<i256> {
    let ten = i256::from_i128(10);
    let diff = i32::from(to) - i32::from(from);
    let scaled = if diff >= 0 {
        v.checked_mul(ten.checked_pow(u32::try_from(diff).ok()?)?)?
    } else {
        let d = ten.checked_pow(u32::try_from(-diff).ok()?)?;
        if v.checked_rem(d)? != i256::ZERO {
            return None;
        }
        v.checked_div(d)?
    };
    let limit = ten.checked_pow(u32::from(precision))?;
    let magnitude = if scaled.is_negative() {
        scaled.checked_neg()?
    } else {
        scaled
    };
    (magnitude < limit).then_some(scaled)
}

/// The probe of `elem` (a canonical element at `dataset`'s type) into a
/// file column under `conv`; `None` if the element does not have the shape
/// the conversion was built for (an internal mismatch, refused upstream).
#[allow(clippy::too_many_lines)]
fn probe(elem: &[u8], dataset: &DataType, conv: Conversion) -> Option<Probe> {
    let decoded = decode(elem)?;
    let dataset_scale = match dataset {
        DataType::Decimal128(_, s) | DataType::Decimal256(_, s) => *s,
        DataType::Dictionary(_, v) => match v.as_ref() {
            DataType::Decimal128(_, s) | DataType::Decimal256(_, s) => *s,
            _ => 0,
        },
        _ => 0,
    };
    let plain = |b: &[u8]| Probe::Plain(b.to_vec());
    let p = match (conv, decoded) {
        (Conversion::Int32 { signed: true }, Decoded::Int(v)) => {
            i32::try_from(v).map_or(Probe::Absent, |x| plain(&x.to_le_bytes()))
        }
        (Conversion::Int32 { signed: false }, Decoded::Int(v)) => {
            u32::try_from(v).map_or(Probe::Absent, |x| plain(&x.to_le_bytes()))
        }
        (Conversion::Int64 { signed: true }, Decoded::Int(v)) => {
            i64::try_from(v).map_or(Probe::Absent, |x| plain(&x.to_le_bytes()))
        }
        (Conversion::Int64 { signed: false }, Decoded::Int(v)) => {
            u64::try_from(v).map_or(Probe::Absent, |x| plain(&x.to_le_bytes()))
        }
        (Conversion::Float | Conversion::Double | Conversion::Float16, Decoded::Float(v))
            if v.is_nan() =>
        {
            Probe::Any
        }
        (Conversion::Double, Decoded::Float(v)) => plain(&v.to_bits().to_le_bytes()),
        (Conversion::Float, Decoded::Float(v)) => {
            // Narrowing is exact only when it round-trips bit for bit.
            #[allow(clippy::cast_possible_truncation)]
            let f = v as f32;
            if f64::from(f).to_bits() == v.to_bits() {
                plain(&f.to_bits().to_le_bytes())
            } else {
                Probe::Absent
            }
        }
        (Conversion::Float16, Decoded::Float(v)) => {
            type F16 = <Float16Type as ArrowPrimitiveType>::Native;
            let h = F16::from_f64(v);
            if h.to_f64().to_bits() == v.to_bits() {
                plain(&h.to_le_bytes())
            } else {
                Probe::Absent
            }
        }
        (Conversion::DecimalInt32 { scale, precision }, Decoded::Decimal(v)) => {
            match rescale(v, dataset_scale, scale, precision)
                .and_then(i256::to_i128)
                .and_then(|x| i32::try_from(x).ok())
            {
                Some(x) => plain(&x.to_le_bytes()),
                None => Probe::Absent,
            }
        }
        (Conversion::DecimalInt64 { scale, precision }, Decoded::Decimal(v)) => {
            match rescale(v, dataset_scale, scale, precision)
                .and_then(i256::to_i128)
                .and_then(|x| i64::try_from(x).ok())
            {
                Some(x) => plain(&x.to_le_bytes()),
                None => Probe::Absent,
            }
        }
        (
            Conversion::DecimalFixed {
                scale,
                precision,
                len,
            },
            Decoded::Decimal(v),
        ) => match rescale(v, dataset_scale, scale, precision) {
            Some(x) if (1..=32).contains(&len) => {
                let wide = x.to_be_bytes();
                let (head, tail) = wide.split_at(32 - len);
                let sign = if x.is_negative() { 0xFF } else { 0x00 };
                // The dropped bytes must be pure sign extension, and the
                // kept bytes must carry the same sign.
                if head.iter().all(|&b| b == sign) && (tail[0] & 0x80 == sign & 0x80) {
                    plain(tail)
                } else {
                    Probe::Absent
                }
            }
            _ => Probe::Absent,
        },
        (Conversion::Bytes, Decoded::Bytes(b)) => Probe::Plain(b),
        (Conversion::FixedBytes { len }, Decoded::Bytes(b)) => {
            if b.len() == len {
                Probe::Plain(b)
            } else {
                Probe::Absent
            }
        }
        (Conversion::DateDays, Decoded::Temporal(0x01, _, days)) => {
            i32::try_from(days).map_or(Probe::Absent, |x| plain(&x.to_le_bytes()))
        }
        (Conversion::DateDays, Decoded::Temporal(0x02, _, ms)) => {
            if ms % 86_400_000 == 0 {
                i32::try_from(ms / 86_400_000).map_or(Probe::Absent, |x| plain(&x.to_le_bytes()))
            } else {
                Probe::Absent
            }
        }
        (Conversion::DateMillis, Decoded::Temporal(0x01, _, days)) => days
            .checked_mul(86_400_000)
            .map_or(Probe::Absent, |x| plain(&x.to_le_bytes())),
        (Conversion::DateMillis, Decoded::Temporal(0x02, _, ms)) => plain(&ms.to_le_bytes()),
        // A timestamp, or a time stored as INT64: the value in the file's
        // unit.
        (Conversion::Timestamp { unit }, Decoded::Temporal(0x04, tag, v))
        | (Conversion::TimeInt64 { unit }, Decoded::Temporal(0x03, tag, v)) => {
            match convert_unit(v, Unit::of_tag(tag)?, unit) {
                Some(x) => plain(&x.to_le_bytes()),
                None => Probe::Absent,
            }
        }
        (Conversion::TimeInt32 { unit }, Decoded::Temporal(0x03, tag, v)) => {
            match convert_unit(v, Unit::of_tag(tag)?, unit).and_then(|x| i32::try_from(x).ok()) {
                Some(x) => plain(&x.to_le_bytes()),
                None => Probe::Absent,
            }
        }
        _ => return None,
    };
    Some(p)
}

#[derive(Clone)]
pub(crate) struct Embedded {
    filter: Sbbf,
    conversion: Conversion,
    dataset: DataType,
}

/// One column's embedded filters, by catalog row-group position.
pub(crate) type ColumnFilters = BTreeMap<usize, Embedded>;

/// Plans a run under a [`Baseline`].
pub struct BaselinePlanner<'a> {
    catalog: &'a Catalog,
    kind: Baseline,
    /// Embedded filters by column name.
    filters: BTreeMap<String, Arc<ColumnFilters>>,
}

impl std::fmt::Debug for BaselinePlanner<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BaselinePlanner")
            .field("kind", &self.kind)
            .field(
                "filters",
                &self.filters.values().map(|m| m.len()).sum::<usize>(),
            )
            .finish_non_exhaustive()
    }
}

impl<'a> BaselinePlanner<'a> {
    /// The raw-scan baseline.
    #[must_use]
    pub fn raw_scan(catalog: &'a Catalog) -> Self {
        Self {
            catalog,
            kind: Baseline::RawScan,
            filters: BTreeMap::new(),
        }
    }

    /// The Parquet-statistics baseline for keys on `columns`: reads every
    /// embedded bloom filter of those columns from every catalog file that
    /// declares one (`ColumnStats::has_bloom`), verifying each file's
    /// fingerprint under `policy` on the opened handle first. A declared
    /// filter that cannot be probed exactly, or cannot be read, is a
    /// [`ReplayError::UnprobeableFilter`].
    pub fn parquet_stats(
        catalog: &'a Catalog,
        columns: &[String],
        policy: FingerprintPolicy,
    ) -> ReplayResult<Self> {
        let mut filters = BTreeMap::new();
        for column in columns {
            if !filters.contains_key(column) {
                let loaded = column_filters(catalog, column, policy)?;
                filters.insert(column.clone(), Arc::new(loaded));
            }
        }
        Ok(Self::parquet_stats_with(catalog, filters))
    }

    /// The Parquet-statistics baseline over filters already read by
    /// [`column_filters`], one entry per key column a run may name.
    pub(crate) fn parquet_stats_with(
        catalog: &'a Catalog,
        filters: BTreeMap<String, Arc<ColumnFilters>>,
    ) -> Self {
        Self {
            catalog,
            kind: Baseline::ParquetStats,
            filters,
        }
    }

    /// The baseline.
    #[must_use]
    pub fn kind(&self) -> Baseline {
        self.kind
    }

    /// The plan for one run's keys (in `window`, for a window run), with
    /// the `In` residual over them (and `Between` on the window's column).
    /// An empty key set reads nothing.
    pub fn plan(&self, keys: &RunKeys, window: Option<&RunWindow>) -> ReplayResult<ScanPlan> {
        let window = window
            .map(|w| EncodedWindow::new(self.catalog.schema(), w))
            .transpose()?;
        let selected = self.selection(keys, window.as_ref())?;
        let plan = self.catalog.plan_for(&selected)?;
        Ok(with_residual(plan, keys, window.as_ref()))
    }

    /// The ids of the row groups [`BaselinePlanner::plan`] reads.
    pub(crate) fn select(
        &self,
        keys: &RunKeys,
        window: Option<&RunWindow>,
    ) -> ReplayResult<RoaringBitmap> {
        let window = window
            .map(|w| EncodedWindow::new(self.catalog.schema(), w))
            .transpose()?;
        self.selection(keys, window.as_ref())
    }

    fn selection(
        &self,
        keys: &RunKeys,
        window: Option<&EncodedWindow>,
    ) -> ReplayResult<RoaringBitmap> {
        if keys.is_empty() {
            return Ok(RoaringBitmap::new());
        }
        match self.kind {
            Baseline::RawScan => Ok(self.catalog.all_ids()),
            Baseline::ParquetStats => {
                let all: Vec<usize> = (0..keys.columns().len()).collect();
                self.stats_selection(keys, &all, window)
            }
        }
    }
    fn stats_selection(
        &self,
        keys: &RunKeys,
        all: &[usize],
        window: Option<&EncodedWindow>,
    ) -> ReplayResult<RoaringBitmap> {
        let mut selected = RoaringBitmap::new();
        for (pos, rg) in self.catalog.row_groups().iter().enumerate() {
            if let Some(w) = window {
                let outside = stats_of(&rg.meta.stats, &w.window.ts_column).is_some_and(|s| {
                    matches!((&s.min, &s.max), (Some(lo), Some(hi))
                        if hi.as_slice() < w.lo.as_slice() || lo.as_slice() > w.hi.as_slice())
                });
                if outside {
                    continue;
                }
            }
            let stats: Vec<Option<&ColumnStats>> = keys
                .columns()
                .iter()
                .map(|c| stats_of(&rg.meta.stats, c))
                .collect();
            let filters: Vec<Option<&Embedded>> = keys
                .columns()
                .iter()
                .map(|c| self.filters.get(c).and_then(|m| m.get(&pos)))
                .collect();
            'tuples: for i in 0..keys.len() {
                for probe_bytes in keys.probes(i, all) {
                    let mut pass = true;
                    for (j, elem) in split(&probe_bytes, keys, i).into_iter().enumerate() {
                        if !in_range(elem, stats[j]) || !Self::in_filter(elem, filters[j])? {
                            pass = false;
                            break;
                        }
                    }
                    if pass {
                        selected.insert(rg.id.0);
                        break 'tuples;
                    }
                }
            }
        }
        Ok(selected)
    }

    fn in_filter(elem: &[u8], filter: Option<&Embedded>) -> ReplayResult<bool> {
        let Some(f) = filter else {
            return Ok(true);
        };
        match probe(elem, &f.dataset, f.conversion) {
            Some(Probe::Plain(b)) => Ok(f.filter.check(&ByteArray::from(b))),
            Some(Probe::Absent) => Ok(false),
            Some(Probe::Any) => Ok(true),
            None => Err(Error::Other(format!(
                "a key element does not have the shape of its {} dataset type",
                f.dataset
            ))
            .into()),
        }
    }
}

/// Every embedded bloom filter of `column` in the catalog, by row-group
/// position: each file that declares one (`ColumnStats::has_bloom`) is
/// opened once, its fingerprint verified under `policy` on the opened
/// handle first. A declared filter that cannot be probed exactly, or
/// cannot be read, is a [`ReplayError::UnprobeableFilter`].
pub(crate) fn column_filters(
    catalog: &Catalog,
    column: &str,
    policy: FingerprintPolicy,
) -> ReplayResult<ColumnFilters> {
    let schema = catalog.schema();
    let mut filters = ColumnFilters::new();
    for (fi, fp) in catalog.files().iter().enumerate() {
        let wanted: Vec<usize> = catalog
            .row_groups()
            .iter()
            .enumerate()
            .filter(|(_, rg)| {
                rg.file == fi && stats_of(&rg.meta.stats, column).is_some_and(|s| s.has_bloom)
            })
            .map(|(pos, _)| pos)
            .collect();
        if wanted.is_empty() {
            continue;
        }
        let refuse = |why: String| ReplayError::UnprobeableFilter {
            file: fp.path.clone(),
            column: column.to_string(),
            why,
        };
        let reader = open_verified(fp, policy)?;
        let options = ReadOptionsBuilder::new()
            .with_reader_properties(
                ReaderProperties::builder()
                    .set_read_bloom_filter(true)
                    .build(),
            )
            .build();
        let file = SerializedFileReader::new_with_options(reader, options)
            .map_err(|e| Error::Source(format!("{}: {e}", fp.path)))?;
        let descr = file.metadata().file_metadata().schema_descr_ptr();
        let ci = (0..descr.num_columns())
            .find(|&i| descr.column(i).path().parts() == [column])
            .ok_or_else(|| refuse("the filter is not on a top-level column".into()))?;
        let dataset = dataset_type(schema, column)?;
        let conv = conversion(&dataset, &descr.column(ci)).map_err(refuse)?;
        for pos in wanted {
            let index = catalog.row_groups()[pos].meta.index_in_file as usize;
            let rg = file
                .get_row_group(index)
                .map_err(|e| Error::Source(format!("{}: {e}", fp.path)))?;
            let filter = rg.get_column_bloom_filter(ci).ok_or_else(|| {
                refuse("the footer declares a filter that could not be read".into())
            })?;
            filters.insert(
                pos,
                Embedded {
                    filter: filter.clone(),
                    conversion: conv,
                    dataset: dataset.clone(),
                },
            );
        }
    }
    Ok(filters)
}

fn dataset_type(schema: &Schema, column: &str) -> ReplayResult<DataType> {
    Ok(schema
        .field_with_name(column)
        .map_err(|_| Error::KeySpec(format!("column {column:?} is not in the dataset schema")))?
        .data_type()
        .clone())
}

/// The elements of a probe built from tuple `i` over every column: probes
/// differ from the tuple only in a float zero's sign, which keeps every
/// element's length, so the tuple's element boundaries apply.
fn split<'p>(probe: &'p [u8], keys: &RunKeys, i: usize) -> Vec<&'p [u8]> {
    let mut out = Vec::with_capacity(keys.columns().len());
    let mut at = 0;
    for j in 0..keys.columns().len() {
        let len = keys.element(i, j).len();
        out.push(&probe[at..at + len]);
        at += len;
    }
    out
}

fn stats_of<'s>(stats: &'s [ColumnStats], column: &str) -> Option<&'s ColumnStats> {
    stats.iter().find(|s| s.column == column)
}

fn in_range(elem: &[u8], stats: Option<&ColumnStats>) -> bool {
    match stats.map(|s| (&s.min, &s.max)) {
        Some((Some(lo), Some(hi))) => {
            elem == NAN_KEY.as_slice() || (lo.as_slice() <= elem && elem <= hi.as_slice())
        }
        _ => true,
    }
}

/// Opens a local file and verifies, on the opened handle, that it still
/// matches `fp` under `policy`; every read then goes through that handle.
fn open_verified(fp: &FileFingerprint, policy: FingerprintPolicy) -> ReplayResult<std::fs::File> {
    let (reader, meta) = LocalFs.open(&fp.path)?;
    let drift = |what: &str| {
        Error::Source(format!(
            "{}: fingerprint drift ({what}); re-discover the file (ADR 013)",
            fp.path
        ))
    };
    if meta.size != fp.size {
        return Err(drift("size").into());
    }
    if meta.etag != fp.etag {
        return Err(drift("etag or mtime").into());
    }
    let read = |start: u64, len: u64| -> ReplayResult<bytes::Bytes> {
        let len = usize::try_from(len)
            .map_err(|_| Error::Source(format!("{}: read too large", fp.path)))?;
        Ok(reader
            .get_bytes(start, len)
            .map_err(|e| Error::Source(format!("{}: {e}", fp.path)))?)
    };
    let hash = match policy {
        FingerprintPolicy::Tail64K => {
            let start = meta.size.saturating_sub(bellwether_source::TAIL_BYTES);
            Hash::of(&read(start, meta.size - start)?)
        }
        FingerprintPolicy::Full => {
            // The whole file through one hasher, one bounded window at a
            // time, as the source fingerprints it.
            const WINDOW: u64 = 1 << 20;
            let mut hasher = blake3::Hasher::new();
            let mut pos = 0;
            while pos < meta.size {
                let len = WINDOW.min(meta.size - pos);
                hasher.update(&read(pos, len)?);
                pos += len;
            }
            Hash(*hasher.finalize().as_bytes())
        }
    };
    if hash != fp.tail_blake3 {
        return Err(drift("content hash").into());
    }
    Ok(reader)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::TimeUnit;
    use bellwether_core::encoding::{encode_value_as, Value};

    use super::*;

    fn enc(v: Value<'_>, dt: &DataType) -> Vec<u8> {
        let mut out = Vec::new();
        encode_value_as(&v, dt, &mut out).unwrap();
        out
    }

    #[test]
    fn integers_probe_their_plain_bit_pattern_or_are_absent() {
        let p = |v: Value<'_>, dt: &DataType, c| probe(&enc(v, dt), dt, c).unwrap();
        let i64_t = DataType::Int64;
        let u64_t = DataType::UInt64;
        assert_eq!(
            p(Value::Int(-5), &i64_t, Conversion::Int32 { signed: true }),
            Probe::Plain((-5i32).to_le_bytes().to_vec())
        );
        assert_eq!(
            p(Value::Int(-5), &i64_t, Conversion::Int64 { signed: false }),
            Probe::Absent
        );
        assert_eq!(
            p(
                Value::Int(1 << 40),
                &i64_t,
                Conversion::Int32 { signed: true }
            ),
            Probe::Absent
        );
        // u64 at and above 2^63, u32 at and above 2^31: the bit pattern.
        assert_eq!(
            p(
                Value::UInt(u64::MAX),
                &u64_t,
                Conversion::Int64 { signed: false }
            ),
            Probe::Plain(u64::MAX.to_le_bytes().to_vec())
        );
        assert_eq!(
            p(
                Value::UInt((1 << 31) + 3),
                &u64_t,
                Conversion::Int32 { signed: false }
            ),
            Probe::Plain(((1u32 << 31) + 3).to_le_bytes().to_vec())
        );
        assert_eq!(
            p(
                Value::UInt(u64::MAX),
                &u64_t,
                Conversion::Int64 { signed: true }
            ),
            Probe::Absent
        );
    }

    #[test]
    fn floats_probe_exact_bits_nan_is_never_excluded() {
        let f = DataType::Float64;
        let p = |v: f64, c| probe(&enc(Value::Float(v), &f), &f, c).unwrap();
        assert_eq!(
            p(1.5, Conversion::Double),
            Probe::Plain(1.5f64.to_bits().to_le_bytes().to_vec())
        );
        assert_eq!(
            p(1.5, Conversion::Float),
            Probe::Plain(1.5f32.to_bits().to_le_bytes().to_vec())
        );
        assert_eq!(p(0.1, Conversion::Float), Probe::Absent, "not exact in f32");
        assert_eq!(
            p(-0.0, Conversion::Double),
            Probe::Plain((-0.0f64).to_bits().to_le_bytes().to_vec())
        );
        assert_eq!(p(f64::NAN, Conversion::Double), Probe::Any);
        assert_eq!(p(1.5, Conversion::Float16), Probe::Plain(vec![0x00, 0x3E]));
    }

    #[test]
    fn decimals_rescale_exactly_to_the_file_scale() {
        let d = DataType::Decimal128(20, 3);
        let p =
            |v: i128, c| probe(&enc(Value::Decimal128 { value: v, scale: 3 }, &d), &d, c).unwrap();
        // 1.500 at scale 2 is 150.
        assert_eq!(
            p(
                1500,
                Conversion::DecimalInt32 {
                    scale: 2,
                    precision: 9
                }
            ),
            Probe::Plain(150i32.to_le_bytes().to_vec())
        );
        // 1.501 cannot be held at scale 2.
        assert_eq!(
            p(
                1501,
                Conversion::DecimalInt64 {
                    scale: 2,
                    precision: 18
                }
            ),
            Probe::Absent
        );
        // Past the file's precision.
        assert_eq!(
            p(
                123_456,
                Conversion::DecimalInt32 {
                    scale: 3,
                    precision: 4
                }
            ),
            Probe::Absent
        );
        // FLBA(5): big-endian two's complement, sign-extended.
        assert_eq!(
            p(
                -1000,
                Conversion::DecimalFixed {
                    scale: 3,
                    precision: 10,
                    len: 5
                }
            ),
            Probe::Plain(vec![0xFF, 0xFF, 0xFF, 0xFC, 0x18])
        );
        assert_eq!(
            p(
                1500,
                Conversion::DecimalFixed {
                    scale: 4,
                    precision: 10,
                    len: 5
                }
            ),
            Probe::Plain(vec![0x00, 0x00, 0x00, 0x3A, 0x98])
        );
    }

    #[test]
    fn temporals_convert_units_exactly() {
        let ts = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let p = |v: i64, c| {
            probe(
                &enc(
                    Value::Timestamp {
                        value: v,
                        unit: TimeUnit::Microsecond,
                    },
                    &ts,
                ),
                &ts,
                c,
            )
            .unwrap()
        };
        assert_eq!(
            p(5_000, Conversion::Timestamp { unit: Unit::Milli }),
            Probe::Plain(5i64.to_le_bytes().to_vec())
        );
        assert_eq!(
            p(5_001, Conversion::Timestamp { unit: Unit::Milli }),
            Probe::Absent
        );
        assert_eq!(
            p(5, Conversion::Timestamp { unit: Unit::Nano }),
            Probe::Plain(5_000i64.to_le_bytes().to_vec())
        );
        let date = DataType::Date32;
        assert_eq!(
            probe(
                &enc(Value::Date32(19_000), &date),
                &date,
                Conversion::DateDays
            )
            .unwrap(),
            Probe::Plain(19_000i32.to_le_bytes().to_vec())
        );
    }

    #[test]
    fn strings_and_fixed_bytes_probe_their_bytes() {
        let s = DataType::Utf8;
        assert_eq!(
            probe(&enc(Value::Utf8("a\0b"), &s), &s, Conversion::Bytes).unwrap(),
            Probe::Plain(vec![b'a', 0, b'b'])
        );
        let fb = DataType::FixedSizeBinary(3);
        assert_eq!(
            probe(
                &enc(Value::Binary(b"xyz"), &fb),
                &fb,
                Conversion::FixedBytes { len: 3 }
            )
            .unwrap(),
            Probe::Plain(b"xyz".to_vec())
        );
        assert_eq!(
            probe(
                &enc(Value::Binary(b"xyz"), &fb),
                &fb,
                Conversion::FixedBytes { len: 4 }
            )
            .unwrap(),
            Probe::Absent
        );
        assert_eq!(unescape(&[0x06, b'a', 0, 0, 1]), None);
        assert_eq!(unescape(&[0x06, b'a', 0, 7]), None);
    }

    #[test]
    fn nan_probes_are_never_pruned_by_range() {
        let stats = ColumnStats {
            column: "x".into(),
            min: Some(vec![0x04, 0x80, 0, 0, 0, 0, 0, 0, 0]),
            max: Some(vec![0x04, 0xC0, 0, 0, 0, 0, 0, 0, 0]),
            null_count: None,
            distinct_count: None,
            has_bloom: false,
            bytes: None,
        };
        assert!(in_range(&NAN_KEY, Some(&stats)));
        assert!(!in_range(&[0x04, 0xD0, 0, 0, 0, 0, 0, 0, 0], Some(&stats)));
        assert!(in_range(&[0x04, 0xD0, 0, 0, 0, 0, 0, 0, 0], None));
    }
}
