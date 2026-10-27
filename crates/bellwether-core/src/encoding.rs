//! Canonical memcomparable byte encoding, encoding version 1 (ADR 002, ADR 016,
//! `docs/format.md`).
//!
//! One byte encoding per Arrow type serves key ordering, bloom hashing, and
//! reconcile row hashing. For any two values of one column, the byte order of
//! their encodings equals their typed order, and every encoding is
//! self-delimiting, so a composite key is the concatenation of its element
//! encodings and a prefix of complete elements seeks every extension.
//!
//! # Encoding is relative to the dataset type
//!
//! The dataset schema (`Source::schema`) is the single source of column types.
//! A value read from a file whose type differs (another decimal scale,
//! another timestamp unit, a narrower integer) is first converted to the
//! dataset type, then encoded. Conversion is exact or an error, never a
//! rounding: [`encode_value_as`], [`encode_array_value_as`], and
//! [`KeyColumns`] take the declared type. Rules:
//!
//! - the tag always comes from the dataset type;
//! - integers convert across widths and signedness, range-checked against the
//!   dataset type;
//! - floats convert across widths; a value not exactly representable in a
//!   Float16/Float32 dataset type is an error (NaN converts to NaN);
//! - the three string types are interchangeable, as are the binary types; a
//!   `FixedSizeBinary(n)` dataset type requires length `n`; strings and
//!   binary do not convert into each other;
//! - decimals rescale to the dataset scale and must fit the dataset precision;
//! - Date32 and Date64 convert into each other exactly;
//! - times and timestamps convert to the dataset unit: to a finer unit by
//!   checked multiplication, to a coarser unit only when the division is
//!   exact. Timestamps are UTC instants; the zone is ignored and a zone-less
//!   value is taken as UTC;
//! - anything across families (an integer into a decimal, a date into a
//!   timestamp) is an error.
//!
//! # Bytes
//!
//! | Dataset type | Encoding |
//! |---|---|
//! | Null | `0x00` marker. A key tuple whose leading column is null is not indexed; a null trailing column is encoded as the marker (ADR 014, amended), and a probe tuple containing the marker matches nothing ([`tuple_has_null`]). Row hashing uses the same marker ([`encode_nullable_as`]) |
//! | Boolean | `0x01` + `0x00`/`0x01` |
//! | `Int8`..`Int64` | `0x02` + i64, 8 bytes big-endian, sign bit flipped |
//! | `UInt8`..`UInt64` | `0x03` + u64, 8 bytes big-endian |
//! | `Float16`..`Float64` | `0x04` + f64 bits with sign-aware flip; one canonical NaN, sorting last; `-0.0 < +0.0` |
//! | `Decimal128(p, s)` | `0x05 0x10` + i128 at scale `s`, 16 bytes big-endian, sign bit flipped |
//! | `Decimal256(p, s)` | `0x05 0x20` + i256 at scale `s`, 32 bytes big-endian, sign bit flipped |
//! | `Utf8`, `LargeUtf8`, `Utf8View` | `0x06` + bytes with `0x00` escaped as `0x00 0xFF`, terminated by `0x00 0x00` |
//! | `Binary`, `LargeBinary`, `FixedSizeBinary`, `BinaryView` | `0x07` + same escaping |
//! | `Date32` | `0x08 0x01 0x05` + days as Int64 encoding |
//! | `Date64` | `0x08 0x02 0x02` + milliseconds as Int64 encoding |
//! | `Time32(u)`, `Time64(u)` | `0x08 0x03 <u>` + time of day in unit `u` as Int64 encoding |
//! | `Timestamp(u, _)` | `0x08 0x04 <u>` + UTC instant in unit `u` as Int64 encoding |
//! | `Dictionary(_, v)` | the encoding of the decoded value under `v` |
//! | Composite tuple | concatenation of element encodings |
//!
//! Time-unit tags `<u>`: `0x01` second, `0x02` millisecond, `0x03`
//! microsecond, `0x04` nanosecond, `0x05` day. "Int64 encoding" is the 8-byte
//! big-endian two's complement with the sign bit flipped. Because times and
//! timestamps keep the dataset unit, a year-9999 sentinel in a millisecond or
//! microsecond column encodes without overflow. Duration, Interval, and
//! nested types have no encoding in version 1 and are rejected.
//!
//! These byte values are agreed with WP2 (`bellwether-source`), whose
//! statistics encoder must match byte for byte.

use arrow::array::{downcast_dictionary_array, Array, ArrayRef, AsArray};
use arrow::datatypes::{
    i256, ArrowPrimitiveType, DataType, Date32Type, Date64Type, Decimal128Type, Decimal256Type,
    Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use bellwether_types::{Error, Result};

/// Type tags, the first byte of every element encoding.
pub mod tag {
    /// Null marker, used only in row hashing. Nulls are never indexed.
    pub const NULL: u8 = 0x00;
    /// Boolean.
    pub const BOOLEAN: u8 = 0x01;
    /// Signed integers of every width.
    pub const INT: u8 = 0x02;
    /// Unsigned integers of every width.
    pub const UINT: u8 = 0x03;
    /// Floating point of every width.
    pub const FLOAT: u8 = 0x04;
    /// Decimals; followed by a width tag from [`super::width`].
    pub const DECIMAL: u8 = 0x05;
    /// UTF-8 strings.
    pub const UTF8: u8 = 0x06;
    /// Binary.
    pub const BINARY: u8 = 0x07;
    /// Dates, times, and timestamps; followed by a kind tag from
    /// [`super::temporal_kind`] and a unit tag from [`super::time_unit`].
    pub const TEMPORAL: u8 = 0x08;
}

/// Decimal width tags that follow [`tag::DECIMAL`]: the payload byte count.
pub mod width {
    /// Decimal128: 16 payload bytes.
    pub const DECIMAL128: u8 = 0x10;
    /// Decimal256: 32 payload bytes.
    pub const DECIMAL256: u8 = 0x20;
}

/// Kind tags that follow [`tag::TEMPORAL`] (the "unit tag" of
/// `docs/format.md`).
pub mod temporal_kind {
    /// Date32.
    pub const DATE32: u8 = 0x01;
    /// Date64.
    pub const DATE64: u8 = 0x02;
    /// Time32 and Time64.
    pub const TIME: u8 = 0x03;
    /// Timestamp.
    pub const TIMESTAMP: u8 = 0x04;
}

/// Time-unit tags that follow the temporal kind tag.
pub mod time_unit {
    /// Seconds.
    pub const SECOND: u8 = 0x01;
    /// Milliseconds.
    pub const MILLISECOND: u8 = 0x02;
    /// Microseconds.
    pub const MICROSECOND: u8 = 0x03;
    /// Nanoseconds.
    pub const NANOSECOND: u8 = 0x04;
    /// Days (Date32 only).
    pub const DAY: u8 = 0x05;
}

/// The canonical encoding of NaN, the greatest float encoding. Range
/// statistics exclude NaN by Parquet convention, so a range probe that
/// includes this key is never pruned (`docs/format.md`, statistics
/// semantics).
pub const NAN_KEY: [u8; 9] = [tag::FLOAT, 0xFF, 0xF8, 0, 0, 0, 0, 0, 0];

/// The canonical encoding of +0.0.
pub(crate) const POS_ZERO_KEY: [u8; 9] = [tag::FLOAT, 0x80, 0, 0, 0, 0, 0, 0, 0];

/// The canonical encoding of -0.0.
pub(crate) const NEG_ZERO_KEY: [u8; 9] =
    [tag::FLOAT, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];

const MS_PER_DAY: i64 = 86_400_000;

/// A typed scalar. Arrow arrays are read into this with [`value_at`]; callers
/// holding a single value (a query key typed on the command line) build one
/// directly. It is encoded under a declared dataset type with
/// [`encode_value_as`], or under its natural type ([`Value::natural_type`])
/// with [`encode_value`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Value<'a> {
    /// Boolean.
    Boolean(bool),
    /// A signed integer of any width.
    Int(i64),
    /// An unsigned integer of any width.
    UInt(u64),
    /// A float of any width.
    Float(f64),
    /// A Decimal128 unscaled `value` at `scale`.
    Decimal128 {
        /// Unscaled value.
        value: i128,
        /// Scale `value` is expressed at.
        scale: i8,
    },
    /// A Decimal256 unscaled `value` at `scale`.
    Decimal256 {
        /// Unscaled value.
        value: i256,
        /// Scale `value` is expressed at.
        scale: i8,
    },
    /// A UTF-8 string.
    Utf8(&'a str),
    /// Raw bytes.
    Binary(&'a [u8]),
    /// Days since the epoch.
    Date32(i32),
    /// Milliseconds since the epoch.
    Date64(i64),
    /// Time of day in `unit`.
    Time {
        /// Value in `unit`.
        value: i64,
        /// Unit of `value`.
        unit: TimeUnit,
    },
    /// An instant in `unit` since the epoch, UTC.
    Timestamp {
        /// Value in `unit`.
        value: i64,
        /// Unit of `value`.
        unit: TimeUnit,
    },
}

impl Value<'_> {
    /// The type a value encodes under when no dataset type is given: the
    /// widest type of its family at its own scale or unit.
    #[must_use]
    pub fn natural_type(&self) -> DataType {
        match *self {
            Value::Boolean(_) => DataType::Boolean,
            Value::Int(_) => DataType::Int64,
            Value::UInt(_) => DataType::UInt64,
            Value::Float(_) => DataType::Float64,
            Value::Decimal128 { scale, .. } => DataType::Decimal128(38, scale),
            Value::Decimal256 { scale, .. } => DataType::Decimal256(76, scale),
            Value::Utf8(_) => DataType::Utf8,
            Value::Binary(_) => DataType::Binary,
            Value::Date32(_) => DataType::Date32,
            Value::Date64(_) => DataType::Date64,
            Value::Time { unit, .. } => match unit {
                TimeUnit::Second | TimeUnit::Millisecond => DataType::Time32(unit),
                TimeUnit::Microsecond | TimeUnit::Nanosecond => DataType::Time64(unit),
            },
            Value::Timestamp { unit, .. } => DataType::Timestamp(unit, None),
        }
    }
}

/// A value converted to its dataset type, ready to write.
enum Normalized<'a> {
    Boolean(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    Decimal128(i128),
    Decimal256(i256),
    Utf8(&'a str),
    Binary(&'a [u8]),
    Temporal { kind: u8, unit: u8, value: i64 },
}

fn mismatch(v: &Value<'_>, declared: &DataType) -> Error {
    Error::Other(format!(
        "a {} value does not convert to dataset type {declared}",
        v.natural_type()
    ))
}

fn unrepresentable(what: impl std::fmt::Display, declared: &DataType) -> Error {
    Error::Other(format!(
        "{what} is not representable in dataset type {declared}"
    ))
}

fn unit_rank(u: TimeUnit) -> u32 {
    match u {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    }
}

fn unit_tag(u: TimeUnit) -> u8 {
    match u {
        TimeUnit::Second => time_unit::SECOND,
        TimeUnit::Millisecond => time_unit::MILLISECOND,
        TimeUnit::Microsecond => time_unit::MICROSECOND,
        TimeUnit::Nanosecond => time_unit::NANOSECOND,
    }
}

/// Converts `value` from unit `from` to unit `to`: to a finer unit by checked
/// multiplication, to a coarser unit only if the division is exact.
pub fn convert_time_unit(value: i64, from: TimeUnit, to: TimeUnit) -> Result<i64> {
    let (rf, rt) = (unit_rank(from), unit_rank(to));
    if rt >= rf {
        let factor = 1000i64.pow(rt - rf);
        value
            .checked_mul(factor)
            .ok_or_else(|| Error::Other(format!("{value} {from:?} overflows i64 in {to:?}")))
    } else {
        let factor = 1000i64.pow(rf - rt);
        if value % factor == 0 {
            Ok(value / factor)
        } else {
            Err(Error::Other(format!(
                "{value} {from:?} is not a whole number of {to:?}; refusing to round"
            )))
        }
    }
}

fn int_range(declared: &DataType) -> Option<(i128, i128)> {
    Some(match declared {
        DataType::Int8 => (i128::from(i8::MIN), i128::from(i8::MAX)),
        DataType::Int16 => (i128::from(i16::MIN), i128::from(i16::MAX)),
        DataType::Int32 => (i128::from(i32::MIN), i128::from(i32::MAX)),
        DataType::Int64 => (i128::from(i64::MIN), i128::from(i64::MAX)),
        DataType::UInt8 => (0, i128::from(u8::MAX)),
        DataType::UInt16 => (0, i128::from(u16::MAX)),
        DataType::UInt32 => (0, i128::from(u32::MAX)),
        DataType::UInt64 => (0, i128::from(u64::MAX)),
        _ => return None,
    })
}

fn rescale(value: i256, scale: i8, declared: &DataType, p: u8, s: i8) -> Result<i256> {
    let diff = i32::from(s) - i32::from(scale);
    let ten = i256::from_i128(10);
    let factor = ten
        .checked_pow(diff.unsigned_abs())
        .ok_or_else(|| unrepresentable(format!("decimal at scale {scale}"), declared))?;
    let v = if diff >= 0 {
        value.checked_mul(factor)
    } else if value.checked_rem(factor) == Some(i256::ZERO) {
        value.checked_div(factor)
    } else {
        None
    }
    .ok_or_else(|| unrepresentable(format!("decimal {value} at scale {scale}"), declared))?;
    let bound = ten
        .checked_pow(u32::from(p))
        .ok_or_else(|| unrepresentable("decimal precision", declared))?;
    let neg_bound = bound
        .checked_neg()
        .ok_or_else(|| unrepresentable("decimal precision", declared))?;
    if v >= bound || v <= neg_bound {
        return Err(unrepresentable(
            format!("decimal {value} at scale {scale} (exceeds precision {p})"),
            declared,
        ));
    }
    Ok(v)
}

#[allow(clippy::too_many_lines)]
fn normalize<'a>(v: &Value<'a>, declared: &DataType) -> Result<Normalized<'a>> {
    match declared {
        DataType::Dictionary(_, inner) => normalize(v, inner),
        DataType::Boolean => match *v {
            Value::Boolean(b) => Ok(Normalized::Boolean(b)),
            _ => Err(mismatch(v, declared)),
        },
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => {
            let x: i128 = match *v {
                Value::Int(i) => i128::from(i),
                Value::UInt(u) => i128::from(u),
                _ => return Err(mismatch(v, declared)),
            };
            let (lo, hi) = int_range(declared).ok_or_else(|| mismatch(v, declared))?;
            if x < lo || x > hi {
                return Err(unrepresentable(x, declared));
            }
            if lo < 0 {
                Ok(Normalized::Int(
                    i64::try_from(x).map_err(|_| unrepresentable(x, declared))?,
                ))
            } else {
                Ok(Normalized::UInt(
                    u64::try_from(x).map_err(|_| unrepresentable(x, declared))?,
                ))
            }
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            let Value::Float(f) = *v else {
                return Err(mismatch(v, declared));
            };
            // Exact representability: the narrow round trip returns the same
            // bits (so -0.0 stays -0.0).
            let exact = f.is_nan()
                || match declared {
                    DataType::Float16 => {
                        <Float16Type as ArrowPrimitiveType>::Native::from_f64(f)
                            .to_f64()
                            .to_bits()
                            == f.to_bits()
                    }
                    #[allow(clippy::cast_possible_truncation)] // checked by the round trip
                    DataType::Float32 => f64::from(f as f32).to_bits() == f.to_bits(),
                    _ => true,
                };
            if exact {
                Ok(Normalized::Float(f))
            } else {
                Err(unrepresentable(f, declared))
            }
        }
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => {
            let (value, scale) = match *v {
                Value::Decimal128 { value, scale } => (i256::from_i128(value), scale),
                Value::Decimal256 { value, scale } => (value, scale),
                _ => return Err(mismatch(v, declared)),
            };
            let x = rescale(value, scale, declared, *p, *s)?;
            if matches!(declared, DataType::Decimal128(_, _)) {
                Ok(Normalized::Decimal128(
                    x.to_i128().ok_or_else(|| unrepresentable(x, declared))?,
                ))
            } else {
                Ok(Normalized::Decimal256(x))
            }
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => match *v {
            Value::Utf8(s) => Ok(Normalized::Utf8(s)),
            _ => Err(mismatch(v, declared)),
        },
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => match *v {
            Value::Binary(b) => Ok(Normalized::Binary(b)),
            _ => Err(mismatch(v, declared)),
        },
        DataType::FixedSizeBinary(n) => match *v {
            Value::Binary(b) if usize::try_from(*n).is_ok_and(|n| n == b.len()) => {
                Ok(Normalized::Binary(b))
            }
            Value::Binary(b) => Err(unrepresentable(
                format!("a {}-byte value", b.len()),
                declared,
            )),
            _ => Err(mismatch(v, declared)),
        },
        DataType::Date32 => {
            let days = match *v {
                Value::Date32(d) => d,
                Value::Date64(ms) if ms % MS_PER_DAY == 0 => i32::try_from(ms / MS_PER_DAY)
                    .map_err(|_| unrepresentable(format!("date {ms} ms"), declared))?,
                Value::Date64(ms) => {
                    return Err(unrepresentable(
                        format!("date {ms} ms (not a whole day)"),
                        declared,
                    ))
                }
                _ => return Err(mismatch(v, declared)),
            };
            Ok(Normalized::Temporal {
                kind: temporal_kind::DATE32,
                unit: time_unit::DAY,
                value: i64::from(days),
            })
        }
        DataType::Date64 => {
            let ms = match *v {
                Value::Date32(d) => i64::from(d) * MS_PER_DAY,
                Value::Date64(ms) => ms,
                _ => return Err(mismatch(v, declared)),
            };
            Ok(Normalized::Temporal {
                kind: temporal_kind::DATE64,
                unit: time_unit::MILLISECOND,
                value: ms,
            })
        }
        DataType::Time32(u) | DataType::Time64(u) => {
            let Value::Time { value, unit } = *v else {
                return Err(mismatch(v, declared));
            };
            let x = convert_time_unit(value, unit, *u)?;
            if matches!(declared, DataType::Time32(_)) && i32::try_from(x).is_err() {
                return Err(unrepresentable(format!("time {x}"), declared));
            }
            Ok(Normalized::Temporal {
                kind: temporal_kind::TIME,
                unit: unit_tag(*u),
                value: x,
            })
        }
        DataType::Timestamp(u, _) => {
            let Value::Timestamp { value, unit } = *v else {
                return Err(mismatch(v, declared));
            };
            Ok(Normalized::Temporal {
                kind: temporal_kind::TIMESTAMP,
                unit: unit_tag(*u),
                value: convert_time_unit(value, unit, *u)?,
            })
        }
        other => Err(Error::Other(no_encoding(other))),
    }
}

#[inline]
fn put_i64(out: &mut Vec<u8>, v: i64) {
    let mut b = v.to_be_bytes();
    b[0] ^= 0x80;
    out.extend_from_slice(&b);
}

#[inline]
fn put_escaped(out: &mut Vec<u8>, t: u8, bytes: &[u8]) {
    out.reserve(bytes.len() + 3);
    out.push(t);
    for &b in bytes {
        out.push(b);
        if b == 0 {
            out.push(0xFF);
        }
    }
    out.extend_from_slice(&[0x00, 0x00]);
}

#[inline]
fn put_f64(out: &mut Vec<u8>, f: f64) {
    const CANONICAL_NAN: u64 = 0x7FF8_0000_0000_0000;
    let bits = if f.is_nan() {
        CANONICAL_NAN
    } else {
        f.to_bits()
    };
    let flipped = if bits >> 63 == 1 {
        !bits
    } else {
        bits | (1 << 63)
    };
    out.push(tag::FLOAT);
    out.extend_from_slice(&flipped.to_be_bytes());
}

fn write(n: &Normalized<'_>, out: &mut Vec<u8>) {
    match *n {
        Normalized::Boolean(b) => out.extend_from_slice(&[tag::BOOLEAN, u8::from(b)]),
        Normalized::Int(i) => {
            out.push(tag::INT);
            put_i64(out, i);
        }
        Normalized::UInt(u) => {
            out.push(tag::UINT);
            out.extend_from_slice(&u.to_be_bytes());
        }
        Normalized::Float(f) => put_f64(out, f),
        Normalized::Decimal128(x) => {
            let mut b = x.to_be_bytes();
            b[0] ^= 0x80;
            out.extend_from_slice(&[tag::DECIMAL, width::DECIMAL128]);
            out.extend_from_slice(&b);
        }
        Normalized::Decimal256(x) => {
            let mut b = x.to_be_bytes();
            b[0] ^= 0x80;
            out.extend_from_slice(&[tag::DECIMAL, width::DECIMAL256]);
            out.extend_from_slice(&b);
        }
        Normalized::Utf8(s) => put_escaped(out, tag::UTF8, s.as_bytes()),
        Normalized::Binary(b) => put_escaped(out, tag::BINARY, b),
        Normalized::Temporal { kind, unit, value } => {
            out.extend_from_slice(&[tag::TEMPORAL, kind, unit]);
            put_i64(out, value);
        }
    }
}

/// Appends the canonical encoding of `value` converted to the dataset type
/// `declared`. Errors if the value is not exactly representable there.
pub fn encode_value_as(value: &Value<'_>, declared: &DataType, out: &mut Vec<u8>) -> Result<()> {
    write(&normalize(value, declared)?, out);
    Ok(())
}

/// Appends the canonical encoding of `value` under its natural type
/// ([`Value::natural_type`]). Use [`encode_value_as`] when the dataset type
/// is known; decimals and timestamps encode differently under other scales,
/// widths, and units.
pub fn encode_value(value: &Value<'_>, out: &mut Vec<u8>) -> Result<()> {
    encode_value_as(value, &value.natural_type(), out)
}

/// Encodes a composite tuple under the dataset types `declared`, one per
/// element: the concatenation of the element encodings.
pub fn encode_tuple_as(values: &[Value<'_>], declared: &[DataType]) -> Result<Vec<u8>> {
    if values.len() != declared.len() {
        return Err(Error::KeySpec(format!(
            "{} values for {} declared types",
            values.len(),
            declared.len()
        )));
    }
    let mut out = Vec::with_capacity(values.len() * 11);
    for (v, d) in values.iter().zip(declared) {
        encode_value_as(v, d, &mut out)?;
    }
    Ok(out)
}

/// Encodes a composite tuple, each element under its natural type.
pub fn encode_tuple(values: &[Value<'_>]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(values.len() * 11);
    for v in values {
        encode_value(v, &mut out)?;
    }
    Ok(out)
}

/// The kind of nested type `dt` is (`"struct"`, `"list"`, `"map"`,
/// `"union"`, `"run-end encoded"`), looking through dictionaries; `None` for
/// a flat type. A nested column can sit in a dataset, but only a flat column
/// can be a key: canonical encoding version 1 has no nested types.
#[must_use]
pub fn nested_kind(dt: &DataType) -> Option<&'static str> {
    match dt {
        DataType::Struct(_) => Some("struct"),
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::ListView(_)
        | DataType::LargeListView(_) => Some("list"),
        DataType::Map(_, _) => Some("map"),
        DataType::Union(_, _) => Some("union"),
        DataType::RunEndEncoded(_, _) => Some("run-end encoded"),
        DataType::Dictionary(_, v) => nested_kind(v),
        _ => None,
    }
}

/// Why a value of type `dt` cannot be canonically encoded, as a sentence.
/// A nested type gets a sentence saying to flatten it (its Arrow spelling
/// lists every child field, so it is not repeated); any other type is
/// named.
#[must_use]
pub fn no_encoding(dt: &DataType) -> String {
    match nested_kind(dt) {
        Some(kind) => format!(
            "a nested {kind} column cannot be a key, and a nested field cannot be addressed \
             (canonical encoding version 1 has flat types only); flatten the field you look \
             up into its own column first (in SQL, `SELECT s.f AS s_f` for field `f` of column \
             `s`) and key on that column"
        ),
        None => format!("type {dt} has no canonical encoding in encoding version 1"),
    }
}

/// Why `name` is not a column of `schema`, as a sentence. A dotted name
/// whose head is a nested column (`repo.id` with `repo` a struct) gets the
/// reason and what to do: a nested field cannot be addressed, so flatten
/// it into its own column first (contract v3 backlog #36).
#[must_use]
pub fn not_in_schema(schema: &arrow::datatypes::Schema, name: &str) -> String {
    let nested = name.split_once('.').and_then(|(head, _)| {
        let f = schema.field_with_name(head).ok()?;
        Some((head, nested_kind(f.data_type())?))
    });
    match nested {
        Some((head, kind)) => format!(
            "column {name:?} is not in the dataset schema: {head:?} is a nested {kind} column, \
             and a nested field cannot be addressed; flatten it into its own column first \
             (for example `SELECT {name} AS {}`) and name that column",
            name.replace('.', "_")
        ),
        None => format!("column {name:?} is not in the dataset schema"),
    }
}

/// Returns `Ok(())` if `dt` has a canonical encoding in version 1.
pub fn check_supported(dt: &DataType) -> Result<()> {
    match dt {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _)
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond)
        | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond)
        | DataType::Timestamp(_, _) => Ok(()),
        DataType::Dictionary(_, v) => check_supported(v),
        other => Err(Error::Other(no_encoding(other))),
    }
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
enum Family {
    Boolean,
    Integer,
    Float,
    Decimal,
    Utf8,
    Binary,
    Date,
    Time,
    Timestamp,
}

fn family(dt: &DataType) -> Result<Family> {
    check_supported(dt)?;
    Ok(match dt {
        DataType::Dictionary(_, v) => return family(v),
        DataType::Boolean => Family::Boolean,
        DataType::Float16 | DataType::Float32 | DataType::Float64 => Family::Float,
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => Family::Decimal,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Family::Utf8,
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => Family::Binary,
        DataType::Date32 | DataType::Date64 => Family::Date,
        DataType::Time32(_) | DataType::Time64(_) => Family::Time,
        DataType::Timestamp(_, _) => Family::Timestamp,
        _ => Family::Integer,
    })
}

/// Checks that a file column of type `from` can be converted to the dataset
/// type `to` at all. Individual values can still fail to convert (out of
/// range, inexact); those errors surface when the value is encoded.
pub fn check_convertible(from: &DataType, to: &DataType) -> Result<()> {
    if family(from)? == family(to)? {
        Ok(())
    } else {
        Err(Error::KeySpec(format!(
            "a file column of type {from} cannot be encoded as dataset type {to}"
        )))
    }
}

fn prim<T: ArrowPrimitiveType>(array: &dyn Array, row: usize) -> T::Native {
    array.as_primitive::<T>().value(row)
}

/// Reads `array[row]` as a [`Value`] in the array's own type, decoding
/// dictionaries. `None` if the value is null.
#[allow(clippy::too_many_lines)]
pub fn value_at(array: &dyn Array, row: usize) -> Result<Option<Value<'_>>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let v = match array.data_type() {
        DataType::Boolean => Value::Boolean(array.as_boolean().value(row)),
        DataType::Int8 => Value::Int(prim::<Int8Type>(array, row).into()),
        DataType::Int16 => Value::Int(prim::<Int16Type>(array, row).into()),
        DataType::Int32 => Value::Int(prim::<Int32Type>(array, row).into()),
        DataType::Int64 => Value::Int(prim::<Int64Type>(array, row)),
        DataType::UInt8 => Value::UInt(prim::<UInt8Type>(array, row).into()),
        DataType::UInt16 => Value::UInt(prim::<UInt16Type>(array, row).into()),
        DataType::UInt32 => Value::UInt(prim::<UInt32Type>(array, row).into()),
        DataType::UInt64 => Value::UInt(prim::<UInt64Type>(array, row)),
        DataType::Float16 => Value::Float(prim::<Float16Type>(array, row).to_f64()),
        DataType::Float32 => Value::Float(prim::<Float32Type>(array, row).into()),
        DataType::Float64 => Value::Float(prim::<Float64Type>(array, row)),
        DataType::Decimal128(_, s) => Value::Decimal128 {
            value: prim::<Decimal128Type>(array, row),
            scale: *s,
        },
        DataType::Decimal256(_, s) => Value::Decimal256 {
            value: prim::<Decimal256Type>(array, row),
            scale: *s,
        },
        DataType::Utf8 => Value::Utf8(array.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => Value::Utf8(array.as_string::<i64>().value(row)),
        DataType::Utf8View => Value::Utf8(array.as_string_view().value(row)),
        DataType::Binary => Value::Binary(array.as_binary::<i32>().value(row)),
        DataType::LargeBinary => Value::Binary(array.as_binary::<i64>().value(row)),
        DataType::BinaryView => Value::Binary(array.as_binary_view().value(row)),
        DataType::FixedSizeBinary(_) => Value::Binary(array.as_fixed_size_binary().value(row)),
        DataType::Date32 => Value::Date32(prim::<Date32Type>(array, row)),
        DataType::Date64 => Value::Date64(prim::<Date64Type>(array, row)),
        DataType::Time32(TimeUnit::Second) => Value::Time {
            value: prim::<Time32SecondType>(array, row).into(),
            unit: TimeUnit::Second,
        },
        DataType::Time32(TimeUnit::Millisecond) => Value::Time {
            value: prim::<Time32MillisecondType>(array, row).into(),
            unit: TimeUnit::Millisecond,
        },
        DataType::Time64(TimeUnit::Microsecond) => Value::Time {
            value: prim::<Time64MicrosecondType>(array, row),
            unit: TimeUnit::Microsecond,
        },
        DataType::Time64(TimeUnit::Nanosecond) => Value::Time {
            value: prim::<Time64NanosecondType>(array, row),
            unit: TimeUnit::Nanosecond,
        },
        DataType::Timestamp(u, _) => Value::Timestamp {
            value: match u {
                TimeUnit::Second => prim::<TimestampSecondType>(array, row),
                TimeUnit::Millisecond => prim::<TimestampMillisecondType>(array, row),
                TimeUnit::Microsecond => prim::<TimestampMicrosecondType>(array, row),
                TimeUnit::Nanosecond => prim::<TimestampNanosecondType>(array, row),
            },
            unit: *u,
        },
        DataType::Dictionary(_, _) => {
            return downcast_dictionary_array!(
                array => match array.key(row) {
                    Some(k) => value_at(array.values().as_ref(), k),
                    None => Ok(None),
                },
                t => Err(Error::Other(format!("unsupported dictionary type {t}")))
            )
        }
        other => return Err(Error::Other(no_encoding(other))),
    };
    Ok(Some(v))
}

/// Appends the canonical encoding of `array[row]` converted to the dataset
/// type `declared`. Returns `false` and writes nothing if the value is null;
/// nulls are never indexed (ADR 014).
pub fn encode_array_value_as(
    array: &dyn Array,
    row: usize,
    declared: &DataType,
    out: &mut Vec<u8>,
) -> Result<bool> {
    match value_at(array, row)? {
        Some(v) => {
            encode_value_as(&v, declared, out)?;
            Ok(true)
        }
        None => Ok(false),
    }
}

/// [`encode_array_value_as`] with the array's own type as the dataset type.
pub fn encode_array_value(array: &dyn Array, row: usize, out: &mut Vec<u8>) -> Result<bool> {
    encode_array_value_as(array, row, array.data_type(), out)
}

/// Appends `array[row]` for row hashing under the dataset type: the
/// canonical encoding, or the single `0x00` null marker. Never use this for
/// index keys.
pub fn encode_nullable_as(
    array: &dyn Array,
    row: usize,
    declared: &DataType,
    out: &mut Vec<u8>,
) -> Result<()> {
    if !encode_array_value_as(array, row, declared, out)? {
        out.push(tag::NULL);
    }
    Ok(())
}

/// [`encode_nullable_as`] with the array's own type as the dataset type.
pub fn encode_nullable(array: &dyn Array, row: usize, out: &mut Vec<u8>) -> Result<()> {
    encode_nullable_as(array, row, array.data_type(), out)
}

/// Checks that `dt` can order versions as a temporal key spec's `ts_column`.
pub fn check_ts_type(dt: &DataType) -> Result<()> {
    match family(dt) {
        Ok(Family::Timestamp | Family::Date | Family::Time | Family::Integer) => Ok(()),
        _ => Err(Error::KeySpec(format!(
            "ts_column of type {dt} cannot order versions"
        ))),
    }
}

/// The smallest ordering value (in the unit [`ts_value_as`] returns) a
/// `ts_column` of dataset type `dt` can hold: the lower clamp of a lookback
/// window, so the window's bound always encodes at the column's type.
pub fn ts_min(dt: &DataType) -> Result<i64> {
    check_ts_type(dt)?;
    Ok(match dt {
        DataType::Dictionary(_, v) => return ts_min(v),
        DataType::Int8 => i8::MIN.into(),
        DataType::Int16 => i16::MIN.into(),
        DataType::Int32 | DataType::Date32 => i32::MIN.into(),
        DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Time32(_)
        | DataType::Time64(_) => 0,
        _ => i64::MIN,
    })
}

/// Reads the ordering value of a temporal `ts_column` at `row`, converted to
/// the dataset type `declared`. `None` for null; nulls rank last and never
/// win (ADR 014).
///
/// The value is in the dataset column's unit: UTC in the column's time unit
/// for timestamps, the column's unit for times, days for Date32,
/// milliseconds for Date64, and the integer itself for integer columns (for
/// example a version number). `asof` takes timestamps in the same unit.
pub fn ts_value_as(array: &dyn Array, row: usize, declared: &DataType) -> Result<Option<i64>> {
    check_ts_type(declared)?;
    let Some(v) = value_at(array, row)? else {
        return Ok(None);
    };
    match normalize(&v, declared)? {
        Normalized::Int(i) => Ok(Some(i)),
        Normalized::UInt(u) => i64::try_from(u)
            .map(Some)
            .map_err(|_| Error::Other(format!("ts value {u} does not fit in i64"))),
        Normalized::Temporal { value, .. } => Ok(Some(value)),
        _ => Err(Error::KeySpec(format!(
            "ts_column of type {declared} cannot order versions"
        ))),
    }
}

/// [`ts_value_as`] with the array's own type as the dataset type.
pub fn ts_value(array: &dyn Array, row: usize) -> Result<Option<i64>> {
    ts_value_as(array, row, array.data_type())
}

/// The key columns of one batch, each paired with its dataset type, for
/// row-at-a-time encoding.
#[derive(Debug, Clone)]
pub struct KeyColumns {
    columns: Vec<(ArrayRef, DataType)>,
}

impl KeyColumns {
    /// Pairs each column with its dataset type and checks that every column
    /// converts to its declared type.
    pub fn new(columns: &[ArrayRef], declared: &[DataType]) -> Result<Self> {
        if columns.is_empty() {
            return Err(Error::KeySpec("a key needs at least one column".into()));
        }
        if columns.len() != declared.len() {
            return Err(Error::KeySpec(format!(
                "{} key columns for {} declared types",
                columns.len(),
                declared.len()
            )));
        }
        let mut out = Vec::with_capacity(columns.len());
        for (c, d) in columns.iter().zip(declared) {
            check_convertible(c.data_type(), d)?;
            out.push((ArrayRef::clone(c), d.clone()));
        }
        Ok(Self { columns: out })
    }

    /// Number of rows.
    #[must_use]
    pub fn len(&self) -> usize {
        self.columns.first().map_or(0, |c| c.0.len())
    }

    /// Whether there are no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Encodes the key tuple at `row` into `out` (cleared first), following
    /// ADR 014 as amended: nulls are not indexed per tuple, not per column.
    /// Returns `false` if the leading column is null at `row`: such a row is
    /// not indexed. A null in a trailing column is encoded as the `0x00` null
    /// marker, so the tuple stays prefix-seekable by its leading columns.
    pub fn encode_into(&self, row: usize, out: &mut Vec<u8>) -> Result<bool> {
        out.clear();
        for (i, (c, d)) in self.columns.iter().enumerate() {
            if !encode_array_value_as(c.as_ref(), row, d, out)? {
                if i == 0 {
                    out.clear();
                    return Ok(false);
                }
                out.push(tag::NULL);
            }
        }
        Ok(true)
    }

    /// Encodes the key tuple at `row`, or `None` if its leading column is
    /// null.
    pub fn encode(&self, row: usize) -> Result<Option<Vec<u8>>> {
        let mut out = Vec::new();
        Ok(self.encode_into(row, &mut out)?.then_some(out))
    }
}

/// Whether a canonical key tuple contains a null element (the `0x00`
/// marker), found by walking its self-delimiting elements. `None` if the
/// bytes are not a well-formed tuple of encoding version 1. Under ADR 014 a
/// probe tuple that contains a null matches nothing (SQL three-valued
/// logic), even though stored tuples may carry trailing null markers.
#[must_use]
pub fn tuple_has_null(key: &[u8]) -> Option<bool> {
    let mut i = 0usize;
    let mut has_null = false;
    while i < key.len() {
        has_null |= key[i] == tag::NULL;
        i += element_len(key, i)?;
    }
    Some(has_null)
}

/// The byte range of each element of a canonical tuple, in order, found by
/// walking its self-delimiting elements; `None` if the bytes are not a
/// well-formed tuple of encoding version 1. A null element is the one-byte
/// `0x00` marker.
#[must_use]
pub fn tuple_elements(key: &[u8]) -> Option<Vec<std::ops::Range<usize>>> {
    let mut i = 0usize;
    let mut out = Vec::new();
    while i < key.len() {
        let len = element_len(key, i)?;
        out.push(i..i + len);
        i += len;
    }
    Some(out)
}

/// The length of the element starting at `key[i]`, `None` if it is not a
/// well-formed element that fits in `key`.
fn element_len(key: &[u8], i: usize) -> Option<usize> {
    let len = match *key.get(i)? {
        tag::NULL => 1,
        tag::BOOLEAN => 2,
        tag::INT | tag::UINT | tag::FLOAT => 9,
        tag::DECIMAL => match *key.get(i + 1)? {
            width::DECIMAL128 => 2 + 16,
            width::DECIMAL256 => 2 + 32,
            _ => return None,
        },
        tag::UTF8 | tag::BINARY => {
            let mut j = i + 1;
            loop {
                if *key.get(j)? == 0x00 {
                    match *key.get(j + 1)? {
                        0x00 => break j + 2 - i,
                        0xFF => j += 2,
                        _ => return None,
                    }
                } else {
                    j += 1;
                }
            }
        }
        tag::TEMPORAL => 11,
        _ => return None,
    };
    (i + len <= key.len()).then_some(len)
}

/// Bounds for a prefix seek: every encoded key `k` that starts with `prefix`
/// satisfies `lower <= k` and, if `upper` is `Some`, `k < upper`; and every
/// key in that range starts with `prefix`. `upper` is `None` when no finite
/// upper bound exists (the prefix is empty or all `0xFF`).
///
/// Because each element encoding is self-delimiting, passing the encoding of
/// the leading columns of a composite key selects exactly the keys whose
/// leading columns equal those values.
#[must_use]
pub fn prefix_bound(prefix: &[u8]) -> (Vec<u8>, Option<Vec<u8>>) {
    let lower = prefix.to_vec();
    let mut upper = prefix.to_vec();
    while let Some(last) = upper.last_mut() {
        if *last == 0xFF {
            upper.pop();
        } else {
            *last += 1;
            return (lower, Some(upper));
        }
    }
    (lower, None)
}

/// An owned typed value decoded from a canonical encoding by [`decode`]:
/// the value at the dataset type it was encoded under, with the dataset's
/// decimal scale and time unit, so a consumer never re-interprets a key by
/// its own cast. [`Scalar::as_value`] borrows it back as a [`Value`] for
/// [`encode_value_as`], which reproduces the bytes exactly.
#[derive(Clone, Debug, PartialEq)]
pub enum Scalar {
    /// Boolean.
    Boolean(bool),
    /// A signed integer, in range for the dataset's width.
    Int(i64),
    /// An unsigned integer, in range for the dataset's width.
    UInt(u64),
    /// A float, exactly representable at the dataset's width; NaN is the
    /// canonical NaN, and the sign of a zero is kept.
    Float(f64),
    /// A Decimal128 unscaled `value` at the dataset's `scale`.
    Decimal128 {
        /// Unscaled value.
        value: i128,
        /// The dataset scale.
        scale: i8,
    },
    /// A Decimal256 unscaled `value` at the dataset's `scale`.
    Decimal256 {
        /// Unscaled value.
        value: i256,
        /// The dataset scale.
        scale: i8,
    },
    /// A UTF-8 string.
    Utf8(String),
    /// Raw bytes.
    Binary(Vec<u8>),
    /// Days since the epoch.
    Date32(i32),
    /// Milliseconds since the epoch.
    Date64(i64),
    /// Time of day in the dataset's `unit`.
    Time {
        /// Value in `unit`.
        value: i64,
        /// The dataset unit.
        unit: TimeUnit,
    },
    /// A UTC instant in the dataset's `unit`.
    Timestamp {
        /// Value in `unit`.
        value: i64,
        /// The dataset unit.
        unit: TimeUnit,
    },
}

impl Scalar {
    /// The value as a borrowed [`Value`], for [`encode_value_as`].
    #[must_use]
    pub fn as_value(&self) -> Value<'_> {
        match self {
            Self::Boolean(b) => Value::Boolean(*b),
            Self::Int(i) => Value::Int(*i),
            Self::UInt(u) => Value::UInt(*u),
            Self::Float(f) => Value::Float(*f),
            Self::Decimal128 { value, scale } => Value::Decimal128 {
                value: *value,
                scale: *scale,
            },
            Self::Decimal256 { value, scale } => Value::Decimal256 {
                value: *value,
                scale: *scale,
            },
            Self::Utf8(s) => Value::Utf8(s),
            Self::Binary(b) => Value::Binary(b),
            Self::Date32(d) => Value::Date32(*d),
            Self::Date64(d) => Value::Date64(*d),
            Self::Time { value, unit } => Value::Time {
                value: *value,
                unit: *unit,
            },
            Self::Timestamp { value, unit } => Value::Timestamp {
                value: *value,
                unit: *unit,
            },
        }
    }

    /// The owned form of `v`.
    #[must_use]
    pub fn from_value(v: &Value<'_>) -> Self {
        match *v {
            Value::Boolean(b) => Self::Boolean(b),
            Value::Int(i) => Self::Int(i),
            Value::UInt(u) => Self::UInt(u),
            Value::Float(f) => Self::Float(f),
            Value::Decimal128 { value, scale } => Self::Decimal128 { value, scale },
            Value::Decimal256 { value, scale } => Self::Decimal256 { value, scale },
            Value::Utf8(s) => Self::Utf8(s.to_string()),
            Value::Binary(b) => Self::Binary(b.to_vec()),
            Value::Date32(d) => Self::Date32(d),
            Value::Date64(d) => Self::Date64(d),
            Value::Time { value, unit } => Self::Time { value, unit },
            Value::Timestamp { value, unit } => Self::Timestamp { value, unit },
        }
    }
}

fn bad_encoding(dt: &crate::segment::ColumnType, why: impl std::fmt::Display) -> Error {
    Error::Other(format!(
        "canonical bytes do not decode as dataset type {}: {why}",
        dt.data_type()
    ))
}

fn read_u64(bytes: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_be_bytes(bytes.get(at..at + 8)?.try_into().ok()?))
}

fn read_flipped_i64(bytes: &[u8], at: usize) -> Option<i64> {
    #[allow(clippy::cast_possible_wrap)] // the encoding's sign flip
    read_u64(bytes, at).map(|u| (u ^ (1 << 63)) as i64)
}

/// Reads the escaped payload of a string or binary element starting at
/// `at` (just after its tag): the bytes and the element's payload length
/// including the `0x00 0x00` terminator.
fn read_escaped(bytes: &[u8], at: usize) -> Option<(Vec<u8>, usize)> {
    let mut out = Vec::new();
    let mut j = at;
    loop {
        match *bytes.get(j)? {
            0x00 => match *bytes.get(j + 1)? {
                0x00 => return Some((out, j + 2 - at)),
                0xFF => {
                    out.push(0x00);
                    j += 2;
                }
                _ => return None,
            },
            b => {
                out.push(b);
                j += 1;
            }
        }
    }
}

fn recorded_unit(u: crate::segment::RecordedUnit) -> TimeUnit {
    use crate::segment::RecordedUnit as R;
    match u {
        R::Second => TimeUnit::Second,
        R::Millisecond => TimeUnit::Millisecond,
        R::Microsecond => TimeUnit::Microsecond,
        R::Nanosecond => TimeUnit::Nanosecond,
    }
}

/// Decodes one element of a canonical encoding starting at `bytes[at]`
/// under the dataset type `dt`: the value and the element's length. The
/// tag, the decimal width, and the temporal kind and unit must be exactly
/// the ones `dt` encodes under, and the value must be in range for `dt`
/// (an integer for its width, a float exactly representable at its width,
/// a decimal within its precision, a `FixedSizeBinary` of its size), so
/// every accepted element re-encodes to the same bytes. A null marker is
/// refused here; [`decode_tuple`] reads it as a null.
#[allow(clippy::too_many_lines)]
fn decode_at(bytes: &[u8], at: usize, dt: &crate::segment::ColumnType) -> Result<(Scalar, usize)> {
    use crate::segment::ColumnType as T;
    let head = *bytes
        .get(at)
        .ok_or_else(|| bad_encoding(dt, "no bytes left for the element"))?;
    let short = || bad_encoding(dt, "the element is truncated");
    let want_tag = |t: u8| {
        if head == t {
            Ok(())
        } else if head == tag::NULL {
            Err(bad_encoding(dt, "the element is the null marker"))
        } else {
            Err(bad_encoding(
                dt,
                format!("tag 0x{head:02x} where 0x{t:02x} was expected"),
            ))
        }
    };
    let temporal = |kind: u8, unit: u8| -> Result<i64> {
        want_tag(tag::TEMPORAL)?;
        let (k, u) = (
            *bytes.get(at + 1).ok_or_else(short)?,
            *bytes.get(at + 2).ok_or_else(short)?,
        );
        if (k, u) != (kind, unit) {
            return Err(bad_encoding(
                dt,
                format!(
                    "temporal kind 0x{k:02x} unit 0x{u:02x} where 0x{kind:02x} 0x{unit:02x} was \
                     expected (another unit is another dataset type)"
                ),
            ));
        }
        read_flipped_i64(bytes, at + 3).ok_or_else(short)
    };
    let out_of_range =
        |what: String| bad_encoding(dt, format!("{what} is out of range for the dataset type"));
    Ok(match dt {
        T::Boolean => {
            want_tag(tag::BOOLEAN)?;
            match *bytes.get(at + 1).ok_or_else(short)? {
                0 => (Scalar::Boolean(false), 2),
                1 => (Scalar::Boolean(true), 2),
                b => return Err(bad_encoding(dt, format!("boolean byte 0x{b:02x}"))),
            }
        }
        T::Int8 | T::Int16 | T::Int32 | T::Int64 => {
            want_tag(tag::INT)?;
            let v = read_flipped_i64(bytes, at + 1).ok_or_else(short)?;
            let (lo, hi) = int_range(&dt.data_type()).unwrap_or((i128::MIN, i128::MAX));
            if i128::from(v) < lo || i128::from(v) > hi {
                return Err(out_of_range(v.to_string()));
            }
            (Scalar::Int(v), 9)
        }
        T::UInt8 | T::UInt16 | T::UInt32 | T::UInt64 => {
            want_tag(tag::UINT)?;
            let v = read_u64(bytes, at + 1).ok_or_else(short)?;
            let (_, hi) = int_range(&dt.data_type()).unwrap_or((0, i128::MAX));
            if i128::from(v) > hi {
                return Err(out_of_range(v.to_string()));
            }
            (Scalar::UInt(v), 9)
        }
        T::Float16 | T::Float32 | T::Float64 => {
            want_tag(tag::FLOAT)?;
            let bits = read_u64(bytes, at + 1).ok_or_else(short)?;
            let raw = if bits >> 63 == 1 {
                bits ^ (1 << 63)
            } else {
                !bits
            };
            let f = f64::from_bits(raw);
            if f.is_nan() && raw != 0x7FF8_0000_0000_0000 {
                return Err(bad_encoding(dt, "a NaN other than the canonical NaN"));
            }
            // The encoder admits only values exact at the dataset width.
            let mut probe = Vec::with_capacity(9);
            encode_value_as(&Value::Float(f), &dt.data_type(), &mut probe)
                .map_err(|_| out_of_range(format!("{f:?}")))?;
            (Scalar::Float(f), 9)
        }
        T::Decimal128 { precision, scale } | T::Decimal256 { precision, scale } => {
            want_tag(tag::DECIMAL)?;
            let width = *bytes.get(at + 1).ok_or_else(short)?;
            let wide = matches!(dt, T::Decimal256 { .. });
            let expected = if wide {
                width::DECIMAL256
            } else {
                width::DECIMAL128
            };
            if width != expected {
                return Err(bad_encoding(
                    dt,
                    format!("decimal width 0x{width:02x} where 0x{expected:02x} was expected"),
                ));
            }
            let bound = i256::from_i128(10)
                .checked_pow(u32::from(*precision))
                .ok_or_else(|| bad_encoding(dt, "the precision has no bound"))?;
            if wide {
                let mut raw: [u8; 32] = bytes
                    .get(at + 2..at + 34)
                    .ok_or_else(short)?
                    .try_into()
                    .map_err(|_| short())?;
                raw[0] ^= 0x80;
                let v = i256::from_be_bytes(raw);
                if v >= bound || v <= bound.wrapping_neg() {
                    return Err(out_of_range(format!("decimal {v}")));
                }
                (
                    Scalar::Decimal256 {
                        value: v,
                        scale: *scale,
                    },
                    34,
                )
            } else {
                let mut raw: [u8; 16] = bytes
                    .get(at + 2..at + 18)
                    .ok_or_else(short)?
                    .try_into()
                    .map_err(|_| short())?;
                raw[0] ^= 0x80;
                let v = i128::from_be_bytes(raw);
                let w = i256::from_i128(v);
                if w >= bound || w <= bound.wrapping_neg() {
                    return Err(out_of_range(format!("decimal {v}")));
                }
                (
                    Scalar::Decimal128 {
                        value: v,
                        scale: *scale,
                    },
                    18,
                )
            }
        }
        T::Utf8 | T::LargeUtf8 | T::Utf8View => {
            want_tag(tag::UTF8)?;
            let (raw, len) = read_escaped(bytes, at + 1)
                .ok_or_else(|| bad_encoding(dt, "the string is not terminated"))?;
            let s = String::from_utf8(raw)
                .map_err(|_| bad_encoding(dt, "the string is not valid UTF-8"))?;
            (Scalar::Utf8(s), len + 1)
        }
        T::Binary | T::LargeBinary | T::BinaryView | T::FixedSizeBinary { .. } => {
            want_tag(tag::BINARY)?;
            let (raw, len) = read_escaped(bytes, at + 1)
                .ok_or_else(|| bad_encoding(dt, "the binary value is not terminated"))?;
            if let T::FixedSizeBinary { size } = dt {
                if usize::try_from(*size).ok() != Some(raw.len()) {
                    return Err(bad_encoding(
                        dt,
                        format!("a {}-byte value for a {size}-byte type", raw.len()),
                    ));
                }
            }
            (Scalar::Binary(raw), len + 1)
        }
        T::Date32 => {
            let v = temporal(temporal_kind::DATE32, time_unit::DAY)?;
            let d = i32::try_from(v).map_err(|_| out_of_range(format!("date {v}")))?;
            (Scalar::Date32(d), 11)
        }
        T::Date64 => (
            Scalar::Date64(temporal(temporal_kind::DATE64, time_unit::MILLISECOND)?),
            11,
        ),
        T::Time32 { unit } | T::Time64 { unit } => {
            let u = recorded_unit(*unit);
            let v = temporal(temporal_kind::TIME, unit_tag(u))?;
            if matches!(dt, T::Time32 { .. }) && i32::try_from(v).is_err() {
                return Err(out_of_range(format!("time {v}")));
            }
            (Scalar::Time { value: v, unit: u }, 11)
        }
        T::Timestamp { unit, .. } => {
            let u = recorded_unit(*unit);
            let v = temporal(temporal_kind::TIMESTAMP, unit_tag(u))?;
            (Scalar::Timestamp { value: v, unit: u }, 11)
        }
    })
}

/// Decodes the canonical encoding (ADR 016) of one value of the dataset
/// type `dt` into a typed [`Scalar`] at that type: decimals at the dataset
/// scale, times and timestamps in the dataset unit. The bytes must be
/// exactly one element that [`encode_value_as`] could have written under
/// `dt`, so `encode_value_as(&decode(b, dt)?.as_value(), &dt.data_type())`
/// gives `b` back; anything else (another tag, width, kind or unit, a
/// value out of the type's range, trailing bytes, the null marker) is an
/// error naming why, never a guess.
pub fn decode(bytes: &[u8], dt: &crate::segment::ColumnType) -> Result<Scalar> {
    let (v, len) = decode_at(bytes, 0, dt)?;
    if len != bytes.len() {
        return Err(bad_encoding(
            dt,
            format!("{} bytes follow the element", bytes.len() - len),
        ));
    }
    Ok(v)
}

/// Decodes a canonical composite tuple of `types` (one element per type,
/// in order), each element as [`decode`] does; a `0x00` null marker is
/// `None` (ADR 014). The bytes must hold exactly that many elements.
pub fn decode_tuple(
    bytes: &[u8],
    types: &[crate::segment::ColumnType],
) -> Result<Vec<Option<Scalar>>> {
    let mut out = Vec::with_capacity(types.len());
    let mut at = 0usize;
    for dt in types {
        if bytes.get(at) == Some(&tag::NULL) {
            out.push(None);
            at += 1;
            continue;
        }
        let (v, len) = decode_at(bytes, at, dt)?;
        out.push(Some(v));
        at += len;
    }
    if at != bytes.len() {
        return Err(Error::Other(format!(
            "a canonical tuple of {} elements has {} bytes left over",
            types.len(),
            bytes.len() - at
        )));
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn enc(v: Value<'_>) -> Vec<u8> {
        encode_tuple(&[v]).unwrap()
    }

    fn enc_as(v: Value<'_>, d: &DataType) -> Result<Vec<u8>> {
        encode_tuple_as(&[v], std::slice::from_ref(d))
    }

    #[test]
    fn exact_bytes_for_each_tag() {
        assert_eq!(enc(Value::Boolean(true)), vec![0x01, 0x01]);
        assert_eq!(
            enc(Value::Int(-1)),
            vec![0x02, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
        );
        assert_eq!(enc(Value::Int(0)), vec![0x02, 0x80, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(enc(Value::UInt(1)), vec![0x03, 0, 0, 0, 0, 0, 0, 0, 1]);
        assert_eq!(
            enc(Value::Utf8("a\0b")),
            vec![0x06, b'a', 0, 0xFF, b'b', 0, 0]
        );
        assert_eq!(enc(Value::Binary(b"")), vec![0x07, 0, 0]);
        assert_eq!(enc(Value::Float(f64::NAN)), NAN_KEY.to_vec());
        assert_eq!(enc(Value::Float(f64::NAN)), enc(Value::Float(-f64::NAN)));
        assert!(enc(Value::Float(f64::INFINITY)) < enc(Value::Float(f64::NAN)));
        assert!(enc(Value::Float(-0.0)) < enc(Value::Float(0.0)));
    }

    #[test]
    fn temporal_and_decimal_vectors_agreed_with_wp2() {
        // Timestamp(us) 1 = 08 04 03 80 00 00 00 00 00 00 01
        assert_eq!(
            enc(Value::Timestamp {
                value: 1,
                unit: TimeUnit::Microsecond
            }),
            vec![0x08, 0x04, 0x03, 0x80, 0, 0, 0, 0, 0, 0, 0x01]
        );
        assert_eq!(enc(Value::Date32(0))[..3], [0x08, 0x01, 0x05]);
        assert_eq!(enc(Value::Date64(0))[..3], [0x08, 0x02, 0x02]);
        assert_eq!(
            enc(Value::Time {
                value: 1,
                unit: TimeUnit::Millisecond
            })[..3],
            [0x08, 0x03, 0x02]
        );
        let d = enc(Value::Decimal128 { value: 1, scale: 0 });
        assert_eq!(
            (d.len(), d[0], d[1], d[2], d[17]),
            (18, 0x05, 0x10, 0x80, 1)
        );
        let d = enc(Value::Decimal256 {
            value: i256::from_i128(1),
            scale: 0,
        });
        assert_eq!((d.len(), d[1]), (34, 0x20));
    }

    #[test]
    fn decimals_rescale_to_the_dataset_scale() {
        let ds = DataType::Decimal128(10, 3);
        let a = enc_as(
            Value::Decimal128 {
                value: 150,
                scale: 2,
            },
            &ds,
        )
        .unwrap();
        let b = enc_as(
            Value::Decimal128 {
                value: 1500,
                scale: 3,
            },
            &ds,
        )
        .unwrap();
        assert_eq!(a, b, "1.50 at scale 2 and 1.500 at scale 3");
        // not exact at scale 3
        assert!(enc_as(
            Value::Decimal128 {
                value: 15001,
                scale: 4
            },
            &ds
        )
        .is_err());
        // exceeds precision 10
        assert!(enc_as(
            Value::Decimal128 {
                value: 10_000_000_000,
                scale: 3
            },
            &ds
        )
        .is_err());
        // width follows the dataset type
        let wide = DataType::Decimal256(40, 3);
        assert_eq!(
            enc_as(
                Value::Decimal128 {
                    value: 150,
                    scale: 2
                },
                &wide
            )
            .unwrap()
            .len(),
            34
        );
    }

    #[test]
    fn timestamps_keep_the_dataset_unit() {
        let us = DataType::Timestamp(TimeUnit::Microsecond, None);
        // year 9999 sentinel in microseconds: no overflow
        let sentinel = 253_402_300_799_999_999i64;
        let e = enc_as(
            Value::Timestamp {
                value: sentinel,
                unit: TimeUnit::Microsecond,
            },
            &us,
        )
        .unwrap();
        assert_eq!(e[..3], [0x08, 0x04, 0x03]);
        // ms file under a us dataset: multiply
        assert_eq!(
            enc_as(
                Value::Timestamp {
                    value: 5,
                    unit: TimeUnit::Millisecond
                },
                &us
            )
            .unwrap(),
            enc_as(
                Value::Timestamp {
                    value: 5000,
                    unit: TimeUnit::Microsecond
                },
                &us
            )
            .unwrap()
        );
        // ns file under a us dataset: exact or error
        assert!(enc_as(
            Value::Timestamp {
                value: 1_001,
                unit: TimeUnit::Nanosecond
            },
            &us
        )
        .is_err());
        // seconds overflowing nanoseconds is an error
        let ns = DataType::Timestamp(TimeUnit::Nanosecond, None);
        assert!(enc_as(
            Value::Timestamp {
                value: i64::MAX / 10,
                unit: TimeUnit::Second
            },
            &ns
        )
        .is_err());
    }

    #[test]
    fn conversions_follow_the_dataset_type() {
        // the tag comes from the dataset type
        assert_eq!(
            enc_as(Value::UInt(5), &DataType::Int64).unwrap()[0],
            tag::INT
        );
        assert!(enc_as(Value::Int(-1), &DataType::UInt64).is_err());
        assert!(enc_as(Value::Int(300), &DataType::Int8).is_err());
        assert!(enc_as(Value::Float(0.1), &DataType::Float32).is_err());
        assert!(enc_as(Value::Float(0.5), &DataType::Float32).is_ok());
        assert!(enc_as(Value::Float(f64::NAN), &DataType::Float16).is_ok());
        assert!(enc_as(Value::Binary(b"abc"), &DataType::FixedSizeBinary(4)).is_err());
        assert!(enc_as(Value::Utf8("a"), &DataType::Binary).is_err());
        assert!(enc_as(Value::Int(1), &DataType::Decimal128(10, 0)).is_err());
        assert_eq!(
            enc_as(Value::Date64(2 * MS_PER_DAY), &DataType::Date32).unwrap(),
            enc(Value::Date32(2))
        );
        assert!(enc_as(Value::Date64(1), &DataType::Date32).is_err());
        assert!(check_convertible(&DataType::Utf8, &DataType::LargeUtf8).is_ok());
        assert!(check_convertible(&DataType::Int32, &DataType::Float64).is_err());
        // a time that converts exactly but does not fit Time32's i32 is refused
        let ns = |value| Value::Time {
            value,
            unit: TimeUnit::Nanosecond,
        };
        assert!(enc_as(
            ns(3_000_000_000_000_000_000),
            &DataType::Time32(TimeUnit::Second)
        )
        .is_err());
        assert!(enc_as(
            ns(2_000_000_000_000_000_000),
            &DataType::Time32(TimeUnit::Second)
        )
        .is_ok());
    }

    #[test]
    fn tuple_has_null_walks_every_element_kind() {
        let mut t = encode_tuple(&[
            Value::Boolean(true),
            Value::Int(-1),
            Value::UInt(7),
            Value::Float(f64::NAN),
            Value::Decimal128 { value: 5, scale: 1 },
            Value::Decimal256 {
                value: i256::from_i128(5),
                scale: 1,
            },
            Value::Utf8("a\0\0b"),
            Value::Binary(&[0, 0xFF, 0]),
            Value::Date32(3),
            Value::Timestamp {
                value: 1,
                unit: TimeUnit::Microsecond,
            },
        ])
        .unwrap();
        assert_eq!(tuple_has_null(&t), Some(false));
        assert_eq!(tuple_has_null(&[]), Some(false));
        // a trailing and a middle null marker
        t.push(tag::NULL);
        assert_eq!(tuple_has_null(&t), Some(true));
        let mut mid = enc(Value::Int(1));
        mid.push(tag::NULL);
        mid.extend_from_slice(&enc(Value::Utf8("x")));
        assert_eq!(tuple_has_null(&mid), Some(true));
        // malformed tuples are not guessed at
        assert_eq!(tuple_has_null(&[tag::INT, 0x80]), None);
        assert_eq!(tuple_has_null(&[tag::UTF8, b'a', 0x00]), None);
        assert_eq!(tuple_has_null(&[tag::UTF8, b'a', 0x00, 0x01]), None);
        assert_eq!(tuple_has_null(&[tag::DECIMAL, 0x11]), None);
        assert_eq!(tuple_has_null(&[0x09]), None);
    }

    #[test]
    fn signed_zero_keys_match_the_encoder() {
        assert_eq!(enc(Value::Float(0.0)), POS_ZERO_KEY.to_vec());
        assert_eq!(enc(Value::Float(-0.0)), NEG_ZERO_KEY.to_vec());
    }

    #[test]
    fn prefix_bounds() {
        assert_eq!(prefix_bound(&[1, 2]), (vec![1, 2], Some(vec![1, 3])));
        assert_eq!(prefix_bound(&[1, 0xFF]), (vec![1, 0xFF], Some(vec![2])));
        assert_eq!(prefix_bound(&[0xFF]), (vec![0xFF], None));
        assert_eq!(prefix_bound(&[]), (vec![], None));
    }
}
