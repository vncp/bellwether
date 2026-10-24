//! Canonical byte encoding of Arrow scalars for row-group statistics
//! (ADR 016, `docs/format.md`, encoding version 1).
//!
//! This is a **local copy** kept so this crate does not depend on
//! `bellwether-core`. It must be unified with `bellwether-core::encoding` at
//! integration. Byte values the format leaves open (decimal width tag,
//! time-unit tag) are named constants below and were proposed to WP1.
//!
//! Every value is encoded **as the dataset type**: the column type in the
//! schema the source reports for its root. A file whose decimal scale or
//! timestamp unit differs from the dataset's is converted exactly, and a
//! value that cannot be represented is an error, never rounded.
//!
//! The encoding is memcomparable: for two non-null values encoded as the same
//! dataset type, `a < b` iff `encode(a) < encode(b)` bytewise. Statistics
//! accumulation relies on this.

use std::fmt;

use arrow::array::{downcast_dictionary_array, Array, AsArray};
use arrow::datatypes::{
    i256, ArrowPrimitiveType, DataType, Date32Type, Date64Type, Decimal128Type, Decimal256Type,
    Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use bellwether_types::{Error, Result};

/// Type tag: Boolean.
pub const TAG_BOOL: u8 = 0x01;
/// Type tag: signed integers.
pub const TAG_INT: u8 = 0x02;
/// Type tag: unsigned integers.
pub const TAG_UINT: u8 = 0x03;
/// Type tag: floating point.
pub const TAG_FLOAT: u8 = 0x04;
/// Type tag: decimals.
pub const TAG_DECIMAL: u8 = 0x05;
/// Type tag: UTF-8 strings.
pub const TAG_UTF8: u8 = 0x06;
/// Type tag: binary.
pub const TAG_BINARY: u8 = 0x07;
/// Type tag: dates, times, timestamps.
pub const TAG_TEMPORAL: u8 = 0x08;

/// Decimal width tag: Decimal128 (16 value bytes follow).
pub const WIDTH_DECIMAL128: u8 = 0x10;
/// Decimal width tag: Decimal256 (32 value bytes follow).
pub const WIDTH_DECIMAL256: u8 = 0x20;

/// Temporal kind tag following [`TAG_TEMPORAL`]: Date32.
pub const UNIT_DATE32: u8 = 0x01;
/// Temporal kind tag: Date64.
pub const UNIT_DATE64: u8 = 0x02;
/// Temporal kind tag: Time32/Time64.
pub const UNIT_TIME: u8 = 0x03;
/// Temporal kind tag: Timestamp (UTC).
pub const UNIT_TIMESTAMP: u8 = 0x04;

/// Time-unit tag: seconds.
pub const TU_SECOND: u8 = 0x01;
/// Time-unit tag: milliseconds.
pub const TU_MILLISECOND: u8 = 0x02;
/// Time-unit tag: microseconds.
pub const TU_MICROSECOND: u8 = 0x03;
/// Time-unit tag: nanoseconds.
pub const TU_NANOSECOND: u8 = 0x04;
/// Time-unit tag: days (Date32).
pub const TU_DAY: u8 = 0x05;

/// The single NaN bit pattern every NaN is normalized to before encoding.
const CANONICAL_NAN: u64 = 0x7ff8_0000_0000_0000;

/// Encodes one Arrow scalar into canonical bytes.
///
/// The default methods implement `docs/format.md` encoding version 1. The
/// trait exists so the integration step can swap in `bellwether-core`'s
/// encoder without touching the sources.
pub trait Canonical: Send + Sync + fmt::Debug {
    /// Canonical bytes of `array[row]` encoded as the dataset type `target`,
    /// or `None` when the value is null. Errors for unsupported types, type
    /// mismatches, and values not exactly representable as `target`.
    fn encode(&self, array: &dyn Array, row: usize, target: &DataType) -> Result<Option<Vec<u8>>> {
        encode_as(array, row, target)
    }

    /// Whether `data_type` has a canonical encoding.
    fn supports(&self, data_type: &DataType) -> bool {
        is_supported(data_type)
    }
}

/// The encoding from `docs/format.md`, encoding version 1.
#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultCanonical;

impl Canonical for DefaultCanonical {}

/// Whether `data_type` has a canonical encoding in version 1.
#[must_use]
pub fn is_supported(data_type: &DataType) -> bool {
    match data_type {
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
        | DataType::Timestamp(_, _) => true,
        DataType::Dictionary(_, value) => is_supported(value),
        _ => false,
    }
}

/// Whether `bytes` is the canonical encoding of NaN.
#[must_use]
pub fn is_nan_encoding(bytes: &[u8]) -> bool {
    bytes == encode_f64(f64::NAN).as_slice()
}

/// Applies the Parquet float-statistics reading rules to encoded bounds:
/// a NaN bound is ignored (unknown); a min of +0 may hide -0 values, so it
/// widens to -0; a max of -0 may hide +0 values, so it widens to +0. The
/// canonical encoding keeps ±0 distinct, so without this an attained -0.0
/// would fall outside a +0.0 lower bound. Non-float bounds pass through.
#[must_use]
pub fn normalize_float_bounds(
    min: Option<Vec<u8>>,
    max: Option<Vec<u8>>,
) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    let pos_zero = encode_f64(0.0);
    let neg_zero = encode_f64(-0.0);
    let min =
        min.filter(|b| !is_nan_encoding(b)).map(
            |b| {
                if b == pos_zero {
                    neg_zero.clone()
                } else {
                    b
                }
            },
        );
    let max =
        max.filter(|b| !is_nan_encoding(b)).map(
            |b| {
                if b == neg_zero {
                    pos_zero.clone()
                } else {
                    b
                }
            },
        );
    (min, max)
}

// ---------------------------------------------------------------------------
// Primitive encoders
// ---------------------------------------------------------------------------

/// Signed integer: tag + big-endian i64 with the sign bit flipped.
#[must_use]
pub fn encode_i64(v: i64) -> Vec<u8> {
    let mut out = Vec::with_capacity(9);
    out.push(TAG_INT);
    push_i64(&mut out, v);
    out
}

/// Unsigned integer: tag + big-endian u64.
#[must_use]
pub fn encode_u64(v: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(9);
    out.push(TAG_UINT);
    out.extend_from_slice(&v.to_be_bytes());
    out
}

/// Float: tag + IEEE-754 f64 bits with the sign-aware flip; NaN sorts last.
#[must_use]
pub fn encode_f64(v: f64) -> Vec<u8> {
    let bits = if v.is_nan() {
        CANONICAL_NAN
    } else {
        v.to_bits()
    };
    let flipped = if bits >> 63 == 1 {
        !bits
    } else {
        bits | (1 << 63)
    };
    let mut out = Vec::with_capacity(9);
    out.push(TAG_FLOAT);
    out.extend_from_slice(&flipped.to_be_bytes());
    out
}

/// Boolean: tag + `0x00`/`0x01`.
#[must_use]
pub fn encode_bool(v: bool) -> Vec<u8> {
    vec![TAG_BOOL, u8::from(v)]
}

/// Decimal128 already at the dataset scale: tag + width tag + 16-byte
/// big-endian, sign flipped.
#[must_use]
pub fn encode_decimal128(v: i128) -> Vec<u8> {
    let mut b = v.to_be_bytes();
    b[0] ^= 0x80;
    let mut out = Vec::with_capacity(18);
    out.push(TAG_DECIMAL);
    out.push(WIDTH_DECIMAL128);
    out.extend_from_slice(&b);
    out
}

/// Decimal256 already at the dataset scale: tag + width tag + 32-byte
/// big-endian, sign flipped.
#[must_use]
pub fn encode_decimal256(v: i256) -> Vec<u8> {
    let mut b = v.to_be_bytes();
    b[0] ^= 0x80;
    let mut out = Vec::with_capacity(34);
    out.push(TAG_DECIMAL);
    out.push(WIDTH_DECIMAL256);
    out.extend_from_slice(&b);
    out
}

/// UTF-8 string: tag + escaped bytes + `0x00 0x00` terminator.
#[must_use]
pub fn encode_utf8(v: &str) -> Vec<u8> {
    escaped(TAG_UTF8, v.as_bytes())
}

/// Binary: tag + escaped bytes + `0x00 0x00` terminator.
#[must_use]
pub fn encode_binary(v: &[u8]) -> Vec<u8> {
    escaped(TAG_BINARY, v)
}

/// Temporal value: tag + kind tag + time-unit tag + Int64 encoding of `v`.
#[must_use]
pub fn encode_temporal(kind: u8, time_unit: u8, v: i64) -> Vec<u8> {
    let mut out = Vec::with_capacity(11);
    out.push(TAG_TEMPORAL);
    out.push(kind);
    out.push(time_unit);
    push_i64(&mut out, v);
    out
}

/// Time-unit tag for an Arrow [`TimeUnit`].
#[must_use]
pub fn time_unit_tag(unit: TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => TU_SECOND,
        TimeUnit::Millisecond => TU_MILLISECOND,
        TimeUnit::Microsecond => TU_MICROSECOND,
        TimeUnit::Nanosecond => TU_NANOSECOND,
    }
}

fn push_i64(out: &mut Vec<u8>, v: i64) {
    let mut b = v.to_be_bytes();
    b[0] ^= 0x80;
    out.extend_from_slice(&b);
}

fn escaped(tag: u8, bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len() + 3);
    out.push(tag);
    for &b in bytes {
        out.push(b);
        if b == 0 {
            out.push(0xFF);
        }
    }
    out.extend_from_slice(&[0x00, 0x00]);
    out
}

// ---------------------------------------------------------------------------
// Typed scalars and conversion to the dataset type
// ---------------------------------------------------------------------------

/// One non-null value as read from a file, before conversion.
#[derive(Debug, Clone, Copy)]
enum Scalar<'a> {
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    Decimal(i256, i8),
    Str(&'a str),
    Bin(&'a [u8]),
    Date32(i32),
    Date64(i64),
    Time(i64, TimeUnit),
    Timestamp(i64, TimeUnit),
}

fn prim<T: ArrowPrimitiveType>(array: &dyn Array, row: usize) -> T::Native {
    array.as_primitive::<T>().value(row)
}

fn mismatch(from: &str, target: &DataType) -> Error {
    Error::Other(format!(
        "cannot encode a {from} value as dataset type {target}"
    ))
}

fn scalar(array: &dyn Array, row: usize) -> Result<Option<Scalar<'_>>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let s = match array.data_type() {
        DataType::Boolean => Scalar::Bool(array.as_boolean().value(row)),
        DataType::Int8 => Scalar::Int(i64::from(prim::<Int8Type>(array, row))),
        DataType::Int16 => Scalar::Int(i64::from(prim::<Int16Type>(array, row))),
        DataType::Int32 => Scalar::Int(i64::from(prim::<Int32Type>(array, row))),
        DataType::Int64 => Scalar::Int(prim::<Int64Type>(array, row)),
        DataType::UInt8 => Scalar::UInt(u64::from(prim::<UInt8Type>(array, row))),
        DataType::UInt16 => Scalar::UInt(u64::from(prim::<UInt16Type>(array, row))),
        DataType::UInt32 => Scalar::UInt(u64::from(prim::<UInt32Type>(array, row))),
        DataType::UInt64 => Scalar::UInt(prim::<UInt64Type>(array, row)),
        DataType::Float16 => Scalar::Float(prim::<Float16Type>(array, row).to_f64()),
        DataType::Float32 => Scalar::Float(f64::from(prim::<Float32Type>(array, row))),
        DataType::Float64 => Scalar::Float(prim::<Float64Type>(array, row)),
        DataType::Decimal128(_, s) => {
            Scalar::Decimal(i256::from_i128(prim::<Decimal128Type>(array, row)), *s)
        }
        DataType::Decimal256(_, s) => Scalar::Decimal(prim::<Decimal256Type>(array, row), *s),
        DataType::Utf8 => Scalar::Str(array.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => Scalar::Str(array.as_string::<i64>().value(row)),
        DataType::Utf8View => Scalar::Str(array.as_string_view().value(row)),
        DataType::Binary => Scalar::Bin(array.as_binary::<i32>().value(row)),
        DataType::LargeBinary => Scalar::Bin(array.as_binary::<i64>().value(row)),
        DataType::BinaryView => Scalar::Bin(array.as_binary_view().value(row)),
        DataType::FixedSizeBinary(_) => Scalar::Bin(array.as_fixed_size_binary().value(row)),
        DataType::Date32 => Scalar::Date32(prim::<Date32Type>(array, row)),
        DataType::Date64 => Scalar::Date64(prim::<Date64Type>(array, row)),
        DataType::Time32(TimeUnit::Second) => Scalar::Time(
            i64::from(prim::<Time32SecondType>(array, row)),
            TimeUnit::Second,
        ),
        DataType::Time32(TimeUnit::Millisecond) => Scalar::Time(
            i64::from(prim::<Time32MillisecondType>(array, row)),
            TimeUnit::Millisecond,
        ),
        DataType::Time64(TimeUnit::Microsecond) => Scalar::Time(
            prim::<Time64MicrosecondType>(array, row),
            TimeUnit::Microsecond,
        ),
        DataType::Time64(TimeUnit::Nanosecond) => Scalar::Time(
            prim::<Time64NanosecondType>(array, row),
            TimeUnit::Nanosecond,
        ),
        DataType::Timestamp(u, _) => {
            let v = match u {
                TimeUnit::Second => prim::<TimestampSecondType>(array, row),
                TimeUnit::Millisecond => prim::<TimestampMillisecondType>(array, row),
                TimeUnit::Microsecond => prim::<TimestampMicrosecondType>(array, row),
                TimeUnit::Nanosecond => prim::<TimestampNanosecondType>(array, row),
            };
            Scalar::Timestamp(v, *u)
        }
        DataType::Dictionary(_, _) => {
            return downcast_dictionary_array!(
                array => match array.key(row) {
                    Some(k) => scalar_owned_dict(array.values().as_ref(), k),
                    None => Ok(None),
                },
                t => Err(Error::Other(format!("unsupported dictionary type {t}")))
            );
        }
        other => {
            return Err(Error::Other(format!(
                "no canonical encoding for Arrow type {other}"
            )))
        }
    };
    Ok(Some(s))
}

// Dictionary values live as long as the dictionary array, which outlives the
// returned scalar via the `array` borrow.
fn scalar_owned_dict(values: &dyn Array, k: usize) -> Result<Option<Scalar<'_>>> {
    scalar(values, k)
}

fn units_per_second(u: TimeUnit) -> i64 {
    match u {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

/// Converts `v` from unit `from` to unit `to` exactly.
fn convert_unit(v: i64, from: TimeUnit, to: TimeUnit) -> Result<i64> {
    let (f, t) = (units_per_second(from), units_per_second(to));
    if t >= f {
        v.checked_mul(t / f)
            .ok_or_else(|| Error::Other(format!("{v} {from:?} overflows i64 {to:?}")))
    } else {
        let d = f / t;
        if v % d == 0 {
            Ok(v / d)
        } else {
            Err(Error::Other(format!(
                "{v} {from:?} is not exactly representable in {to:?}"
            )))
        }
    }
}

fn pow10(exp: u32) -> Result<i256> {
    i256::from_i128(10)
        .checked_pow(exp)
        .ok_or_else(|| Error::Other(format!("10^{exp} overflows i256")))
}

/// Rescales `v` at `scale` to `(precision, target_scale)` exactly.
fn rescale(v: i256, scale: i8, precision: u8, target_scale: i8) -> Result<i256> {
    let diff = i32::from(target_scale) - i32::from(scale);
    let out = if diff >= 0 {
        v.checked_mul(pow10(diff.unsigned_abs())?)
            .ok_or_else(|| Error::Other(format!("decimal {v} overflows at scale {target_scale}")))?
    } else {
        let d = pow10(diff.unsigned_abs())?;
        if v.checked_rem(d) != Some(i256::ZERO) {
            return Err(Error::Other(format!(
                "decimal {v} at scale {scale} is not representable at scale {target_scale}"
            )));
        }
        v.checked_div(d)
            .ok_or_else(|| Error::Other("decimal division overflow".into()))?
    };
    let bound = pow10(u32::from(precision))?;
    let neg_bound = bound
        .checked_neg()
        .ok_or_else(|| Error::Other("decimal bound overflow".into()))?;
    if out >= bound || out <= neg_bound {
        return Err(Error::Other(format!(
            "decimal {out} exceeds precision {precision}"
        )));
    }
    Ok(out)
}

const MS_PER_DAY: i64 = 86_400_000;

/// Inclusive value range of an integer dataset type, `None` for non-integers.
fn int_range(dt: &DataType) -> Option<(i128, i128)> {
    Some(match dt {
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

fn encode_scalar(s: Scalar<'_>, target: &DataType) -> Result<Vec<u8>> {
    use DataType as D;
    let target = match target {
        D::Dictionary(_, v) => v.as_ref(),
        t => t,
    };
    Ok(match (s, target) {
        (Scalar::Bool(b), D::Boolean) => encode_bool(b),
        (Scalar::Int(_) | Scalar::UInt(_), _) if int_range(target).is_some() => {
            let v = match s {
                Scalar::Int(v) => i128::from(v),
                Scalar::UInt(v) => i128::from(v),
                _ => unreachable!("guarded above"),
            };
            let (lo, hi) = int_range(target).unwrap_or((0, 0));
            if v < lo || v > hi {
                return Err(Error::Other(format!(
                    "integer {v} out of range for dataset type {target}"
                )));
            }
            if lo < 0 {
                encode_i64(i64::try_from(v).map_err(|_| mismatch("integer", target))?)
            } else {
                encode_u64(u64::try_from(v).map_err(|_| mismatch("integer", target))?)
            }
        }
        (Scalar::Float(v), D::Float64) => encode_f64(v),
        (Scalar::Float(v), D::Float32) => {
            // Exactly representable in f32, or NaN.
            #[allow(clippy::cast_possible_truncation)]
            let narrowed = f64::from(v as f32);
            if !(v.is_nan() || narrowed.to_bits() == v.to_bits()) {
                return Err(mismatch("non-f32-representable float", target));
            }
            encode_f64(v)
        }
        (Scalar::Float(v), D::Float16) => {
            if !(v.is_nan()
                || <Float16Type as ArrowPrimitiveType>::Native::from_f64(v)
                    .to_f64()
                    .to_bits()
                    == v.to_bits())
            {
                return Err(mismatch("non-f16-representable float", target));
            }
            encode_f64(v)
        }
        (Scalar::Decimal(v, s), D::Decimal128(p, ts)) => {
            let r = rescale(v, s, *p, *ts)?;
            encode_decimal128(
                r.to_i128()
                    .ok_or_else(|| mismatch("wide decimal", target))?,
            )
        }
        (Scalar::Decimal(v, s), D::Decimal256(p, ts)) => encode_decimal256(rescale(v, s, *p, *ts)?),
        (Scalar::Str(v), D::Utf8 | D::LargeUtf8 | D::Utf8View) => encode_utf8(v),
        (Scalar::Bin(v), D::Binary | D::LargeBinary | D::BinaryView) => encode_binary(v),
        (Scalar::Bin(v), D::FixedSizeBinary(n)) => {
            if usize::try_from(*n).ok() != Some(v.len()) {
                return Err(mismatch("binary of the wrong length", target));
            }
            encode_binary(v)
        }
        (Scalar::Date32(d), D::Date32) => encode_temporal(UNIT_DATE32, TU_DAY, i64::from(d)),
        (Scalar::Date64(ms), D::Date32) => {
            if ms % MS_PER_DAY != 0 {
                return Err(mismatch("Date64 with a time part", target));
            }
            encode_temporal(UNIT_DATE32, TU_DAY, ms / MS_PER_DAY)
        }
        (Scalar::Date64(ms), D::Date64) => encode_temporal(UNIT_DATE64, TU_MILLISECOND, ms),
        (Scalar::Date32(d), D::Date64) => encode_temporal(
            UNIT_DATE64,
            TU_MILLISECOND,
            i64::from(d)
                .checked_mul(MS_PER_DAY)
                .ok_or_else(|| mismatch("Date32", target))?,
        ),
        (Scalar::Time(v, u), D::Time32(tu) | D::Time64(tu)) => {
            encode_temporal(UNIT_TIME, time_unit_tag(*tu), convert_unit(v, u, *tu)?)
        }
        (Scalar::Timestamp(v, u), D::Timestamp(tu, _)) => {
            encode_temporal(UNIT_TIMESTAMP, time_unit_tag(*tu), convert_unit(v, u, *tu)?)
        }
        (s, t) => return Err(mismatch(&format!("{s:?}"), t)),
    })
}

/// Encodes `array[row]` as the dataset type `target`. `Ok(None)` for null.
pub fn encode_as(array: &dyn Array, row: usize, target: &DataType) -> Result<Option<Vec<u8>>> {
    match scalar(array, row)? {
        None => Ok(None),
        Some(s) => encode_scalar(s, target).map(Some),
    }
}

/// Encodes `array[row]` as its own type (the file type is the dataset type).
pub fn encode_value(array: &dyn Array, row: usize) -> Result<Option<Vec<u8>>> {
    encode_as(array, row, array.data_type())
}

/// Running min/max/null-count over canonical encodings of one column.
///
/// NaN is excluded from the bounds, matching the Parquet writer convention:
/// float min/max are bounds over non-NaN values.
#[derive(Debug, Default, Clone)]
pub(crate) struct StatsAccumulator {
    pub(crate) min: Option<Vec<u8>>,
    pub(crate) max: Option<Vec<u8>>,
    pub(crate) nulls: u64,
    /// Set when a value could not be encoded; min/max are then unknown.
    pub(crate) poisoned: bool,
}

impl StatsAccumulator {
    /// Folds every row of `array` in, encoding as `target`.
    pub(crate) fn update(
        &mut self,
        canonical: &dyn Canonical,
        array: &dyn Array,
        target: &DataType,
    ) {
        for row in 0..array.len() {
            match canonical.encode(array, row, target) {
                Ok(None) => self.nulls += 1,
                Ok(Some(enc)) => {
                    if is_nan_encoding(&enc) {
                        continue;
                    }
                    if self.min.as_ref().map_or(true, |m| enc < *m) {
                        self.min = Some(enc.clone());
                    }
                    if self.max.as_ref().map_or(true, |m| enc > *m) {
                        self.max = Some(enc);
                    }
                }
                Err(_) => self.poisoned = true,
            }
        }
    }

    /// `(min, max)` unless poisoned.
    pub(crate) fn bounds(&self) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
        if self.poisoned {
            (None, None)
        } else {
            (self.min.clone(), self.max.clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, Decimal128Array, DictionaryArray, Float64Array, Int32Array, Int64Array,
        StringArray, TimestampMicrosecondArray, TimestampSecondArray,
    };
    use arrow::datatypes::Int8Type;

    use super::*;

    fn enc(a: &dyn Array, i: usize) -> Vec<u8> {
        encode_value(a, i).unwrap().unwrap()
    }

    #[test]
    fn table_bytes_are_exact() {
        assert_eq!(encode_bool(true), vec![0x01, 0x01]);
        assert_eq!(encode_i64(1), vec![0x02, 0x80, 0, 0, 0, 0, 0, 0, 1]);
        assert_eq!(
            encode_i64(-1),
            vec![0x02, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]
        );
        assert_eq!(encode_u64(1), vec![0x03, 0, 0, 0, 0, 0, 0, 0, 1]);
        assert_eq!(
            encode_utf8("a\0b"),
            vec![0x06, b'a', 0x00, 0xFF, b'b', 0x00, 0x00]
        );
        assert_eq!(encode_binary(b""), vec![0x07, 0x00, 0x00]);
        assert_eq!(
            encode_temporal(UNIT_DATE32, TU_DAY, 0),
            vec![0x08, 0x01, 0x05, 0x80, 0, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(encode_f64(0.0), vec![0x04, 0x80, 0, 0, 0, 0, 0, 0, 0]);
        // Vector pinned identically in bellwether-core: Timestamp(us) 1.
        let ts = TimestampMicrosecondArray::from(vec![1]);
        assert_eq!(
            enc(&ts, 0),
            vec![0x08, 0x04, 0x03, 0x80, 0, 0, 0, 0, 0, 0, 0x01]
        );
        let d = encode_decimal128(1);
        assert_eq!(d.len(), 18);
        assert_eq!(&d[..3], &[0x05, 0x10, 0x80]);
        assert_eq!(d[17], 1);
        let d = encode_decimal256(i256::from_i128(-1));
        assert_eq!(d.len(), 34);
        assert_eq!(&d[..3], &[0x05, 0x20, 0x7f]);
    }

    fn hex(s: &str) -> Vec<u8> {
        s.split(' ')
            .map(|b| u8::from_str_radix(b, 16).unwrap())
            .collect()
    }

    /// The pinned test vectors in `docs/format.md` (encoding version 1),
    /// through the array path the sources use.
    #[test]
    fn format_md_pinned_vectors() {
        let ts = TimestampMicrosecondArray::from(vec![1]);
        assert_eq!(enc(&ts, 0), hex("08 04 03 80 00 00 00 00 00 00 01"));
        let ints = Int64Array::from(vec![0, -1]);
        assert_eq!(enc(&ints, 0), hex("02 80 00 00 00 00 00 00 00"));
        assert_eq!(enc(&ints, 1), hex("02 7f ff ff ff ff ff ff ff"));
        let strs = StringArray::from(vec!["a", "a\0b"]);
        assert_eq!(enc(&strs, 0), hex("06 61 00 00"));
        assert_eq!(enc(&strs, 1), hex("06 61 00 ff 62 00 00"));
        let bools = arrow::array::BooleanArray::from(vec![true]);
        assert_eq!(enc(&bools, 0), hex("01 01"));
    }

    #[test]
    fn narrow_ints_widen_to_i64() {
        let a = Int32Array::from(vec![-5]);
        assert_eq!(enc(&a, 0), encode_i64(-5));
    }

    #[test]
    fn integer_order_is_preserved() {
        let vals = [i64::MIN, -1_000, -1, 0, 1, 7, i64::MAX];
        for w in vals.windows(2) {
            assert!(encode_i64(w[0]) < encode_i64(w[1]), "{w:?}");
        }
    }

    #[test]
    fn float_order_is_total_with_nan_last() {
        let vals = [
            f64::NEG_INFINITY,
            -1.5,
            -0.0,
            0.0,
            f64::MIN_POSITIVE,
            2.0,
            f64::INFINITY,
            f64::NAN,
        ];
        for w in vals.windows(2) {
            assert!(encode_f64(w[0]) < encode_f64(w[1]), "{w:?}");
        }
        assert_eq!(encode_f64(f64::NAN), encode_f64(-f64::NAN));
        let a = Float64Array::from(vec![f64::NAN]);
        assert_eq!(enc(&a, 0), encode_f64(f64::NAN));
    }

    #[test]
    fn string_order_handles_embedded_nul_and_prefixes() {
        let vals = ["", "\0", "\0\0", "\0a", "a", "a\0", "ab", "b", "é", "日本"];
        for w in vals.windows(2) {
            assert!(encode_utf8(w[0]) < encode_utf8(w[1]), "{w:?}");
        }
    }

    #[test]
    fn timestamps_keep_the_dataset_unit() {
        let us = TimestampMicrosecondArray::from(vec![1_000_000]).with_timezone("UTC");
        // Own unit: microseconds, tagged as such.
        assert_eq!(
            enc(&us, 0),
            encode_temporal(UNIT_TIMESTAMP, TU_MICROSECOND, 1_000_000)
        );
        // A seconds file read into a microsecond dataset converts exactly.
        let s = TimestampSecondArray::from(vec![1]);
        let target = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        assert_eq!(encode_as(&s, 0, &target).unwrap().unwrap(), enc(&us, 0));
        // Year-9999 sentinel in microseconds does not overflow.
        let sentinel = TimestampMicrosecondArray::from(vec![253_402_300_799_999_999]);
        assert!(encode_value(&sentinel, 0).unwrap().is_some());
        // ... but cannot be widened to nanoseconds.
        let ns = DataType::Timestamp(TimeUnit::Nanosecond, None);
        assert!(encode_as(&sentinel, 0, &ns).is_err());
        // Coarsening is exact or an error.
        let ms = DataType::Timestamp(TimeUnit::Millisecond, None);
        let fine = TimestampMicrosecondArray::from(vec![1_500, 2_000]);
        assert!(encode_as(&fine, 0, &ms).is_err());
        assert_eq!(
            encode_as(&fine, 1, &ms).unwrap().unwrap(),
            encode_temporal(UNIT_TIMESTAMP, TU_MILLISECOND, 2)
        );
    }

    #[test]
    fn decimals_rescale_to_the_dataset_scale() {
        let a = Decimal128Array::from(vec![1050, 99])
            .with_precision_and_scale(10, 2)
            .unwrap();
        // Own type.
        assert_eq!(enc(&a, 0), encode_decimal128(1050));
        // Wider dataset scale: exact multiply.
        let t = DataType::Decimal128(12, 4);
        assert_eq!(
            encode_as(&a, 0, &t).unwrap().unwrap(),
            encode_decimal128(105_000)
        );
        // Narrower dataset scale: 10.50 -> 10.5 is exact, 0.99 is not.
        let t = DataType::Decimal128(10, 1);
        assert_eq!(
            encode_as(&a, 0, &t).unwrap().unwrap(),
            encode_decimal128(105)
        );
        assert!(encode_as(&a, 1, &t).is_err());
        // Precision overflow is an error.
        let t = DataType::Decimal128(3, 2);
        assert!(encode_as(&a, 0, &t).is_err());
        // Decimal256 dataset widens to 32 bytes.
        let t = DataType::Decimal256(40, 2);
        assert_eq!(
            encode_as(&a, 0, &t).unwrap().unwrap(),
            encode_decimal256(i256::from_i128(1050))
        );
    }

    #[test]
    fn decimal_order_is_preserved() {
        let vals = [i128::MIN + 1, -10, -1, 0, 1, 10, i128::MAX];
        for w in vals.windows(2) {
            assert!(encode_decimal128(w[0]) < encode_decimal128(w[1]), "{w:?}");
        }
    }

    #[test]
    fn nulls_and_dictionaries() {
        let a = Int64Array::from(vec![None, Some(3)]);
        assert_eq!(encode_value(&a, 0).unwrap(), None);
        let d: DictionaryArray<Int8Type> = vec![Some("x"), None, Some("y")].into_iter().collect();
        assert_eq!(enc(&d, 0), encode_utf8("x"));
        assert_eq!(encode_value(&d, 1).unwrap(), None);
        assert_eq!(enc(&d, 2), encode_utf8("y"));
    }

    #[test]
    fn float_bounds_follow_parquet_reading_rules() {
        let nan = encode_f64(f64::NAN);
        assert_eq!(
            normalize_float_bounds(Some(nan.clone()), Some(nan)),
            (None, None)
        );
        assert_eq!(
            normalize_float_bounds(Some(encode_f64(0.0)), Some(encode_f64(-0.0))),
            (Some(encode_f64(-0.0)), Some(encode_f64(0.0)))
        );
        assert_eq!(
            normalize_float_bounds(Some(encode_f64(-1.0)), Some(encode_f64(2.0))),
            (Some(encode_f64(-1.0)), Some(encode_f64(2.0)))
        );
        // Non-float bounds pass through.
        assert_eq!(
            normalize_float_bounds(Some(encode_i64(0)), Some(encode_i64(0))),
            (Some(encode_i64(0)), Some(encode_i64(0)))
        );
    }

    #[test]
    fn type_mismatch_is_an_error() {
        let a = Int64Array::from(vec![1]);
        assert!(encode_as(&a, 0, &DataType::Utf8).is_err());
    }

    #[test]
    fn dataset_type_picks_the_tag_and_range() {
        use arrow::array::{Float64Array, UInt32Array};
        let u = UInt32Array::from(vec![7]);
        assert_eq!(
            encode_as(&u, 0, &DataType::Int64).unwrap().unwrap(),
            encode_i64(7)
        );
        let big = Int64Array::from(vec![300, -1]);
        assert!(encode_as(&big, 0, &DataType::Int8).is_err());
        assert!(encode_as(&big, 1, &DataType::UInt64).is_err());
        let f = Float64Array::from(vec![0.5, 0.1, f64::NAN]);
        assert!(encode_as(&f, 0, &DataType::Float32).is_ok());
        assert!(encode_as(&f, 1, &DataType::Float32).is_err());
        assert!(encode_as(&f, 2, &DataType::Float16).is_ok());
        let s = StringArray::from(vec!["ab"]);
        assert!(encode_as(&s, 0, &DataType::Binary).is_err());
        assert_eq!(
            encode_as(&s, 0, &DataType::LargeUtf8).unwrap().unwrap(),
            encode_utf8("ab")
        );
    }

    #[test]
    fn accumulator_takes_bytewise_extremes_and_skips_nan() {
        let a: ArrayRef = Arc::new(StringArray::from(vec![
            Some("m"),
            None,
            Some("b"),
            Some("z"),
        ]));
        let mut acc = StatsAccumulator::default();
        acc.update(&DefaultCanonical, a.as_ref(), a.data_type());
        assert_eq!(acc.nulls, 1);
        assert_eq!(
            acc.bounds(),
            (Some(encode_utf8("b")), Some(encode_utf8("z")))
        );
        let f: ArrayRef = Arc::new(Float64Array::from(vec![1.0, f64::NAN, -2.0]));
        let mut acc = StatsAccumulator::default();
        acc.update(&DefaultCanonical, f.as_ref(), f.data_type());
        assert_eq!(
            acc.bounds(),
            (Some(encode_f64(-2.0)), Some(encode_f64(1.0)))
        );
        assert_eq!(acc.nulls, 0);
    }
}
