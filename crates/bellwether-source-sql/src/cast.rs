//! Exact conversion of what a driver returns to the type the source asked
//! for: the dataset type (`docs/format.md`: values are encoded as the
//! dataset type). A value that cannot be represented exactly is an error
//! naming the column and the value, never rounded, truncated, or nulled.
//!
//! Accepted conversions: integers between widths (overflow is an error);
//! integers and decimals to a decimal of any scale and back to an integer
//! (only when nothing after the point is lost); decimal text (`"12.30"`,
//! `"-1e3"`) to a decimal or an integer; floats between widths (only when
//! the value round-trips); a timestamp between units (a sub-unit remainder
//! is an error) and between zones (the instant is kept: a timestamp
//! without a zone is read as UTC, as Parquet's local timestamps are); ISO
//! 8601 text to a timestamp or a date; 0 and 1 to a boolean; string and
//! binary representations between each other; an all-null column of type
//! `Null` to any type. Anything else is refused.

use std::sync::Arc;

use arrow::array::{
    new_null_array, Array, ArrayRef, AsArray, BooleanArray, Date32Array, Decimal128Array,
    Int64Array, RecordBatch, RecordBatchOptions, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::{
    DataType, Decimal128Type, Field, Float32Type, Float64Type, Int64Type, Schema, SchemaRef,
    TimeUnit,
};
use bellwether_types::{Error, Result};

/// Converts every column of `batch` to `expect`'s type, by position.
/// `what` names the query in errors.
pub(crate) fn conform(batch: &RecordBatch, expect: &SchemaRef, what: &str) -> Result<RecordBatch> {
    if batch.num_columns() != expect.fields().len() {
        return Err(Error::Source(format!(
            "{what}: the engine returned {} columns, {} were asked for",
            batch.num_columns(),
            expect.fields().len()
        )));
    }
    let mut arrays = Vec::with_capacity(batch.num_columns());
    let mut fields = Vec::with_capacity(batch.num_columns());
    for (array, field) in batch.columns().iter().zip(expect.fields()) {
        let converted = to_type(array, field.data_type())
            .map_err(|e| Error::Source(format!("{what}: column {:?}: {e}", field.name())))?;
        if !field.is_nullable() && converted.null_count() > 0 {
            return Err(Error::Source(format!(
                "{what}: column {:?} is declared NOT NULL and the engine returned a null",
                field.name()
            )));
        }
        fields.push(Field::new(field.name(), field.data_type().clone(), true));
        arrays.push(converted);
    }
    RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(|e| Error::Source(format!("{what}: {e}")))
}

/// `array` as `target`, exactly.
#[allow(clippy::too_many_lines)]
pub(crate) fn to_type(
    array: &ArrayRef,
    target: &DataType,
) -> std::result::Result<ArrayRef, String> {
    use DataType as D;
    let source = array.data_type();
    if source == target {
        return Ok(Arc::clone(array));
    }
    if let DataType::Dictionary(_, value) = source {
        let flat = arrow_cast(array, value)?;
        return to_type(&flat, target);
    }
    if *source == DataType::Null {
        return Ok(new_null_array(target, array.len()));
    }
    match (source, target) {
        (s, t) if is_int(s) && is_int(t) => arrow_cast(array, t),
        (s, D::Decimal128(p, sc)) if is_int(s) => {
            let wide = arrow_cast(array, &D::Int64)?;
            ints_to_decimal(wide.as_primitive::<Int64Type>(), *p, *sc)
        }
        (D::Decimal128(_, from), D::Decimal128(p, to)) => {
            rescale(array.as_primitive::<Decimal128Type>(), *from, *p, *to)
        }
        (D::Decimal128(_, from), t) if is_int(t) => {
            let as_int = rescale(array.as_primitive::<Decimal128Type>(), *from, 38, 0)?;
            let ints: Int64Array = as_int
                .as_primitive::<Decimal128Type>()
                .iter()
                .map(|v| v.map(i64::try_from).transpose())
                .collect::<std::result::Result<_, _>>()
                .map_err(|_| "a value does not fit in a 64-bit integer".to_string())?;
            arrow_cast(&(Arc::new(ints) as ArrayRef), t)
        }
        (s, t) if is_text(s) && (is_int(t) || matches!(t, D::Decimal128(..))) => {
            let text = arrow_cast(array, &D::Utf8)?;
            let (p, sc) = match t {
                D::Decimal128(p, sc) => (*p, *sc),
                _ => (38, 0),
            };
            let values: Decimal128Array = text
                .as_string::<i32>()
                .iter()
                .map(|v| v.map(|s| parse_decimal(s, p, sc)).transpose())
                .collect::<std::result::Result<Decimal128Array, String>>()?;
            let values = values
                .with_precision_and_scale(p, sc)
                .map_err(|e| e.to_string())?;
            to_type(&(Arc::new(values) as ArrayRef), t)
        }
        (D::Float32, D::Float64) => arrow_cast(array, target),
        (D::Float64, D::Float32) => {
            let narrowed = arrow_cast(array, target)?;
            let back = arrow_cast(&narrowed, &D::Float64)?;
            let (a, b) = (
                array.as_primitive::<Float64Type>(),
                back.as_primitive::<Float64Type>(),
            );
            if a.iter().zip(b.iter()).any(|(x, y)| match (x, y) {
                (Some(x), Some(y)) => x.to_bits() != y.to_bits() && !(x.is_nan() && y.is_nan()),
                _ => false,
            }) {
                return Err("a double does not fit a 32-bit float exactly".into());
            }
            let _ = narrowed.as_primitive::<Float32Type>();
            Ok(narrowed)
        }
        (D::Timestamp(from, _), D::Timestamp(to, tz)) => {
            let values = timestamp_values(array, *from);
            let converted: Vec<Option<i64>> = values
                .into_iter()
                .map(|v| v.map(|v| convert_unit(v, *from, *to)).transpose())
                .collect::<std::result::Result<_, _>>()?;
            Ok(timestamp_array(converted, *to, tz.clone()))
        }
        (s, D::Timestamp(unit, tz)) if is_text(s) => {
            let text = arrow_cast(array, &D::Utf8)?;
            let values: Vec<Option<i64>> = text
                .as_string::<i32>()
                .iter()
                .map(|v| v.map(|s| parse_timestamp(s, *unit)).transpose())
                .collect::<std::result::Result<_, _>>()?;
            Ok(timestamp_array(values, *unit, tz.clone()))
        }
        (s, D::Date32) if is_text(s) => {
            let text = arrow_cast(array, &D::Utf8)?;
            let values: Date32Array = text
                .as_string::<i32>()
                .iter()
                .map(|v| v.map(parse_date).transpose())
                .collect::<std::result::Result<_, _>>()?;
            Ok(Arc::new(values))
        }
        (D::Date64, D::Date32) => {
            let values: Date32Array = array
                .as_primitive::<arrow::datatypes::Date64Type>()
                .iter()
                .map(|v| {
                    v.map(|ms| {
                        if ms % 86_400_000 == 0 {
                            i32::try_from(ms / 86_400_000)
                                .map_err(|_| format!("date {ms} ms is out of range"))
                        } else {
                            Err(format!("date {ms} ms is not midnight"))
                        }
                    })
                    .transpose()
                })
                .collect::<std::result::Result<_, _>>()?;
            Ok(Arc::new(values))
        }
        (s, D::Boolean) if is_int(s) => {
            let wide = arrow_cast(array, &D::Int64)?;
            let values: BooleanArray = wide
                .as_primitive::<Int64Type>()
                .iter()
                .map(|v| match v {
                    None => Ok(None),
                    Some(0) => Ok(Some(false)),
                    Some(1) => Ok(Some(true)),
                    Some(other) => Err(format!("{other} is not a boolean (0 or 1)")),
                })
                .collect::<std::result::Result<_, _>>()?;
            Ok(Arc::new(values))
        }
        (D::Time64(_) | D::Time32(_), D::Time64(_) | D::Time32(_)) => {
            // Exactness: convert there and back and compare.
            let there = arrow_cast(array, target)?;
            let back = arrow_cast(&there, source)?;
            if back.to_data() != array.to_data() {
                return Err("a time of day does not fit the dataset's unit exactly".into());
            }
            Ok(there)
        }
        (s, t) if is_text(s) && is_text(t) => arrow_cast(array, t),
        (s, t) if is_binary(s) && is_binary(t) => arrow_cast(array, t),
        _ => Err(format!(
            "the engine returned {source}, which is not read as the dataset type {target} \
             (the type the dataset declares); cast the column in a view, or give the dataset \
             schema"
        )),
    }
}

fn arrow_cast(array: &ArrayRef, to: &DataType) -> std::result::Result<ArrayRef, String> {
    cast_with_options(
        array,
        to,
        &CastOptions {
            safe: false,
            ..CastOptions::default()
        },
    )
    .map_err(|e| format!("{e}"))
}

fn is_int(dt: &DataType) -> bool {
    dt.is_integer()
}

fn is_text(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_binary(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
    )
}

/// `10^exp` as an `i128`, or `None` past `10^38`.
pub(crate) fn pow10(exp: u32) -> Option<i128> {
    10i128.checked_pow(exp)
}

/// Whether `v` has at most `precision` digits.
fn fits(v: i128, precision: u8) -> bool {
    pow10(u32::from(precision)).is_none_or(|limit| v.unsigned_abs() < limit.unsigned_abs())
}

fn ints_to_decimal(
    ints: &arrow::array::PrimitiveArray<Int64Type>,
    precision: u8,
    scale: i8,
) -> std::result::Result<ArrayRef, String> {
    let values: Decimal128Array = ints
        .iter()
        .map(|v| {
            v.map(|v| shift(i128::from(v), 0, precision, scale))
                .transpose()
        })
        .collect::<std::result::Result<_, _>>()?;
    Ok(Arc::new(
        values
            .with_precision_and_scale(precision, scale)
            .map_err(|e| e.to_string())?,
    ))
}

fn rescale(
    array: &arrow::array::PrimitiveArray<Decimal128Type>,
    from: i8,
    precision: u8,
    to: i8,
) -> std::result::Result<ArrayRef, String> {
    let values: Decimal128Array = array
        .iter()
        .map(|v| v.map(|v| shift(v, from, precision, to)).transpose())
        .collect::<std::result::Result<_, _>>()?;
    Ok(Arc::new(
        values
            .with_precision_and_scale(precision, to)
            .map_err(|e| e.to_string())?,
    ))
}

/// `v` at scale `from` as a value at scale `to` with at most `precision`
/// digits, exactly.
pub(crate) fn shift(v: i128, from: i8, precision: u8, to: i8) -> std::result::Result<i128, String> {
    let show = || render_decimal(v, from);
    let out = match i32::from(to) - i32::from(from) {
        0 => v,
        d if d > 0 => pow10(d.unsigned_abs())
            .and_then(|m| v.checked_mul(m))
            .ok_or_else(|| format!("{} overflows at scale {to}", show()))?,
        d => {
            let m = pow10(d.unsigned_abs())
                .ok_or_else(|| format!("{} cannot be rescaled to scale {to}", show()))?;
            if v % m != 0 {
                return Err(format!(
                    "{} has more fractional digits than the dataset's scale {to}; it is not \
                     rounded",
                    show()
                ));
            }
            v / m
        }
    };
    if !fits(out, precision) {
        return Err(format!(
            "{} needs more than the dataset's precision {precision} at scale {to}",
            show()
        ));
    }
    Ok(out)
}

/// A decimal's text form: `-12.30` for 1230 at scale 2.
pub(crate) fn render_decimal(v: i128, scale: i8) -> String {
    if scale <= 0 {
        let zeros = "0".repeat(usize::from(scale.unsigned_abs()));
        return if v == 0 {
            "0".into()
        } else {
            format!("{v}{zeros}")
        };
    }
    let digits = v.unsigned_abs().to_string();
    let s = usize::from(scale.unsigned_abs());
    let padded = format!("{digits:0>width$}", width = s + 1);
    let (int, frac) = padded.split_at(padded.len() - s);
    format!("{}{int}.{frac}", if v < 0 { "-" } else { "" })
}

/// Parses decimal text exactly at `scale` with at most `precision`
/// digits: an optional sign, digits with an optional point, and an
/// optional exponent (`1.5e3`). Digits past the scale must be zeros.
pub(crate) fn parse_decimal(
    text: &str,
    precision: u8,
    scale: i8,
) -> std::result::Result<i128, String> {
    let s = text.trim();
    let bad = || format!("{text:?} is not a decimal number");
    let (mantissa, exp) = match s.find(['e', 'E']) {
        Some(i) => (&s[..i], s[i + 1..].parse::<i32>().map_err(|_| bad())?),
        None => (s, 0),
    };
    let (neg, digits) = match mantissa.as_bytes().first() {
        Some(b'-') => (true, &mantissa[1..]),
        Some(b'+') => (false, &mantissa[1..]),
        _ => (false, mantissa),
    };
    let (int, frac) = digits.split_once('.').unwrap_or((digits, ""));
    if (int.is_empty() && frac.is_empty())
        || !int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit())
    {
        return Err(bad());
    }
    let all = format!("{int}{frac}");
    let all = all.trim_start_matches('0');
    // The value is all * 10^(exp - frac.len()), wanted at `scale`.
    let frac_len = i32::try_from(frac.len()).map_err(|_| bad())?;
    let shift_by = exp - frac_len + i32::from(scale);
    let mut keep = all.to_string();
    if shift_by < 0 {
        let drop = usize::try_from(shift_by.unsigned_abs()).map_err(|_| bad())?;
        let cut = keep.len().saturating_sub(drop);
        if keep[cut..].bytes().any(|b| b != b'0') {
            return Err(format!(
                "{text:?} has more fractional digits than the dataset's scale {scale}; it is not \
                 rounded"
            ));
        }
        keep.truncate(cut);
    } else {
        let zeros = usize::try_from(shift_by).map_err(|_| bad())?;
        if keep.len() + zeros > 39 && !keep.is_empty() {
            return Err(format!("{text:?} overflows a 38-digit decimal"));
        }
        if !keep.is_empty() {
            keep.push_str(&"0".repeat(zeros));
        }
    }
    let magnitude: i128 = if keep.is_empty() {
        0
    } else {
        keep.parse()
            .map_err(|_| format!("{text:?} overflows a 38-digit decimal"))?
    };
    let v = if neg { -magnitude } else { magnitude };
    if !fits(v, precision) {
        return Err(format!(
            "{text:?} needs more than the dataset's precision {precision} at scale {scale}"
        ));
    }
    Ok(v)
}

fn units_per_second(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

/// `v` in `from` as a count of `to`, exactly.
pub(crate) fn convert_unit(
    v: i64,
    from: TimeUnit,
    to: TimeUnit,
) -> std::result::Result<i64, String> {
    let (f, t) = (units_per_second(from), units_per_second(to));
    if t >= f {
        v.checked_mul(t / f)
            .ok_or_else(|| format!("timestamp {v} ({from:?}) overflows at {to:?}"))
    } else if v % (f / t) == 0 {
        Ok(v / (f / t))
    } else {
        Err(format!(
            "timestamp {v} ({from:?}) has a part finer than the dataset's unit {to:?}; it is not \
             truncated"
        ))
    }
}

fn timestamp_values(array: &ArrayRef, unit: TimeUnit) -> Vec<Option<i64>> {
    use arrow::datatypes::{
        TimestampMicrosecondType as Us, TimestampMillisecondType as Ms,
        TimestampNanosecondType as Ns, TimestampSecondType as S,
    };
    match unit {
        TimeUnit::Second => array.as_primitive::<S>().iter().collect(),
        TimeUnit::Millisecond => array.as_primitive::<Ms>().iter().collect(),
        TimeUnit::Microsecond => array.as_primitive::<Us>().iter().collect(),
        TimeUnit::Nanosecond => array.as_primitive::<Ns>().iter().collect(),
    }
}

/// The count at `row` of a timestamp column of `unit`.
pub(crate) fn timestamp_column(array: &ArrayRef, unit: TimeUnit, row: usize) -> i64 {
    use arrow::datatypes::{
        TimestampMicrosecondType as Us, TimestampMillisecondType as Ms,
        TimestampNanosecondType as Ns, TimestampSecondType as S,
    };
    match unit {
        TimeUnit::Second => array.as_primitive::<S>().value(row),
        TimeUnit::Millisecond => array.as_primitive::<Ms>().value(row),
        TimeUnit::Microsecond => array.as_primitive::<Us>().value(row),
        TimeUnit::Nanosecond => array.as_primitive::<Ns>().value(row),
    }
}

/// A timestamp array of `unit` and zone `tz` from counts.
pub(crate) fn timestamp_array(
    values: Vec<Option<i64>>,
    unit: TimeUnit,
    tz: Option<Arc<str>>,
) -> ArrayRef {
    match unit {
        TimeUnit::Second => Arc::new(TimestampSecondArray::from(values).with_timezone_opt(tz)),
        TimeUnit::Millisecond => {
            Arc::new(TimestampMillisecondArray::from(values).with_timezone_opt(tz))
        }
        TimeUnit::Microsecond => {
            Arc::new(TimestampMicrosecondArray::from(values).with_timezone_opt(tz))
        }
        TimeUnit::Nanosecond => {
            Arc::new(TimestampNanosecondArray::from(values).with_timezone_opt(tz))
        }
    }
}

fn days_since_epoch(y: i32, m: u32, d: u32) -> Option<i64> {
    let date = chrono::NaiveDate::from_ymd_opt(y, m, d)?;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
    Some(date.signed_duration_since(epoch).num_days())
}

fn parse_ymd(s: &str) -> Option<i64> {
    let mut parts = s.splitn(3, '-');
    let first = parts.next()?;
    let (neg, year) = match first.strip_prefix('-') {
        Some(y) => (true, y),
        None => (false, first),
    };
    if year.len() < 4 {
        return None;
    }
    let y: i32 = year.parse().ok()?;
    let m = parts.next()?;
    let d = parts.next()?;
    if m.len() != 2 || d.len() != 2 {
        return None;
    }
    days_since_epoch(if neg { -y } else { y }, m.parse().ok()?, d.parse().ok()?)
}

/// `YYYY-MM-DD` as days since 1970-01-01.
pub(crate) fn parse_date(text: &str) -> std::result::Result<i32, String> {
    parse_ymd(text.trim())
        .and_then(|d| i32::try_from(d).ok())
        .ok_or_else(|| format!("{text:?} is not a date (YYYY-MM-DD)"))
}

/// ISO 8601 text (`YYYY-MM-DD`, then optionally `T` or a space and
/// `HH:MM[:SS[.fraction]]`, then optionally `Z` or `±HH[:MM]`) as a count
/// of `unit` since the epoch. Text with no zone is read as UTC. A fraction
/// finer than `unit` must be zeros.
pub(crate) fn parse_timestamp(text: &str, unit: TimeUnit) -> std::result::Result<i64, String> {
    let s = text.trim();
    let bad = || format!("{text:?} is not an ISO 8601 timestamp (YYYY-MM-DD HH:MM:SS[.f][Z])");
    let (date, rest) = match s.find(['T', ' ']) {
        Some(i) if i >= 10 => (&s[..i], s[i + 1..].trim_start()),
        _ => (s, ""),
    };
    let days = parse_ymd(date).ok_or_else(bad)?;
    // Zone suffix.
    let (time, offset_secs) = split_zone(rest).ok_or_else(bad)?;
    let mut nanos_of_day: i128 = 0;
    if !time.is_empty() {
        let mut hms = time.splitn(3, ':');
        let h: i128 = two_digits(hms.next()).ok_or_else(bad)?;
        let m: i128 = two_digits(hms.next()).ok_or_else(bad)?;
        let (sec, frac) = match hms.next() {
            None => (0, ""),
            Some(sf) => match sf.split_once('.') {
                Some((sec, frac)) => (two_digits(Some(sec)).ok_or_else(bad)?, frac),
                None => (two_digits(Some(sf)).ok_or_else(bad)?, ""),
            },
        };
        if h > 23
            || m > 59
            || sec > 60
            || frac.len() > 9
            || !frac.bytes().all(|b| b.is_ascii_digit())
        {
            return Err(bad());
        }
        let frac_ns: i128 = if frac.is_empty() {
            0
        } else {
            format!("{frac:0<9}").parse().map_err(|_| bad())?
        };
        nanos_of_day = ((h * 60 + m) * 60 + sec) * 1_000_000_000 + frac_ns;
    }
    let total = i128::from(days) * 86_400_000_000_000 + nanos_of_day
        - i128::from(offset_secs) * 1_000_000_000;
    let per = i128::from(1_000_000_000 / units_per_second(unit));
    if total % per != 0 {
        return Err(format!(
            "{text:?} has a part finer than the dataset's unit {unit:?}; it is not truncated"
        ));
    }
    i64::try_from(total / per).map_err(|_| format!("{text:?} is out of range at {unit:?}"))
}

fn two_digits(s: Option<&str>) -> Option<i128> {
    let s = s?;
    (s.len() == 2 && s.bytes().all(|b| b.is_ascii_digit()))
        .then(|| s.parse().ok())
        .flatten()
}

/// Splits a zone suffix off a time of day: `("12:00:00", seconds east)`.
fn split_zone(rest: &str) -> Option<(&str, i64)> {
    if let Some(t) = rest.strip_suffix('Z').or_else(|| rest.strip_suffix('z')) {
        return Some((t.trim_end(), 0));
    }
    // A sign after the first character of the time.
    match rest
        .char_indices()
        .skip(1)
        .find(|(_, c)| *c == '+' || *c == '-')
    {
        None => Some((rest, 0)),
        Some((i, c)) => {
            let zone = &rest[i + 1..];
            let (h, m) = match zone.split_once(':') {
                Some((h, m)) => (h, m),
                None if zone.len() == 4 => zone.split_at(2),
                None => (zone, "00"),
            };
            let h: i64 = (h.len() == 2).then(|| h.parse().ok()).flatten()?;
            let m: i64 = (m.len() == 2).then(|| m.parse().ok()).flatten()?;
            if h > 23 || m > 59 {
                return None;
            }
            let secs = (h * 60 + m) * 60;
            Some((rest[..i].trim_end(), if c == '-' { -secs } else { secs }))
        }
    }
}

/// A shortest round-trip decimal rendering of a double, as decimal text:
/// how a SQLite `REAL` in a column declared `DECIMAL(p,s)` is read.
pub(crate) fn float_text(v: f64) -> std::result::Result<String, String> {
    if !v.is_finite() {
        return Err(format!("{v} is not a decimal number"));
    }
    Ok(format!("{v:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Float64Array;

    #[test]
    fn decimals_parse_exactly_or_not_at_all() {
        assert_eq!(parse_decimal("12.30", 10, 2), Ok(1230));
        assert_eq!(parse_decimal("12.3", 10, 4), Ok(123_000));
        assert_eq!(parse_decimal("-0.5", 10, 1), Ok(-5));
        assert_eq!(parse_decimal("1.5e3", 10, 0), Ok(1500));
        assert_eq!(parse_decimal("1500e-3", 10, 1), Ok(15));
        assert_eq!(parse_decimal("12.300", 10, 2), Ok(1230));
        assert!(parse_decimal("12.305", 10, 2).is_err());
        assert!(parse_decimal("123456", 5, 0).is_err());
        assert!(parse_decimal("abc", 10, 2).is_err());
        assert!(parse_decimal(".", 10, 2).is_err());
        assert!(parse_decimal("NaN", 10, 2).is_err());
        assert_eq!(parse_decimal("0", 10, 2), Ok(0));
        assert_eq!(parse_decimal(&float_text(0.1).unwrap(), 10, 2), Ok(10));
        assert_eq!(parse_decimal(&float_text(19.99).unwrap(), 10, 2), Ok(1999));
        assert!(parse_decimal(&float_text(0.125).unwrap(), 10, 2).is_err());
    }

    #[test]
    fn rescaling_is_exact() {
        assert_eq!(shift(1230, 2, 12, 4), Ok(123_000));
        assert_eq!(shift(123_000, 4, 12, 2), Ok(1230));
        assert!(shift(123_456, 4, 12, 2).is_err());
        assert!(shift(99_999, 0, 5, 1).is_err());
        assert_eq!(render_decimal(-5, 2), "-0.05");
        assert_eq!(render_decimal(1230, 2), "12.30");
        assert_eq!(render_decimal(12, -2), "1200");
    }

    #[test]
    fn timestamps_parse_in_their_unit() {
        let ms = TimeUnit::Millisecond;
        assert_eq!(parse_timestamp("1970-01-01 00:00:01.5", ms), Ok(1500));
        assert_eq!(parse_timestamp("1970-01-01T00:00:01.500Z", ms), Ok(1500));
        assert_eq!(parse_timestamp("1970-01-01T01:00:00+01:00", ms), Ok(0));
        assert_eq!(
            parse_timestamp("1969-12-31 23:59:59", TimeUnit::Second),
            Ok(-1)
        );
        assert_eq!(
            parse_timestamp("2024-02-29", TimeUnit::Second),
            Ok(1_709_164_800)
        );
        assert!(parse_timestamp("1970-01-01 00:00:00.0005", ms).is_err());
        assert!(parse_timestamp("1970-13-01", ms).is_err());
        assert!(parse_timestamp("yesterday", ms).is_err());
        assert_eq!(
            convert_unit(3, TimeUnit::Millisecond, TimeUnit::Microsecond),
            Ok(3000)
        );
        assert!(convert_unit(3001, TimeUnit::Microsecond, TimeUnit::Millisecond).is_err());
        assert_eq!(parse_date("2024-01-02"), Ok(19_724));
    }

    #[test]
    #[allow(clippy::many_single_char_names)]
    fn arrays_convert_or_refuse() {
        let a: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(1230), None])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        );
        let b = to_type(&a, &DataType::Decimal128(12, 4)).unwrap();
        assert_eq!(b.as_primitive::<Decimal128Type>().value(0), 123_000);
        assert!(b.is_null(1));
        let c: ArrayRef = Arc::new(
            Decimal128Array::from(vec![12_345])
                .with_precision_and_scale(10, 4)
                .unwrap(),
        );
        assert!(to_type(&c, &DataType::Decimal128(10, 2)).is_err());
        let big: ArrayRef = Arc::new(Int64Array::from(vec![i64::MAX]));
        assert!(to_type(&big, &DataType::Int32).is_err());
        let t: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![7]));
        let u = to_type(&t, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();
        assert_eq!(
            u.as_primitive::<arrow::datatypes::TimestampMicrosecondType>()
                .value(0),
            7000
        );
        let s: ArrayRef = Arc::new(arrow::array::StringArray::from(vec!["x"]));
        assert!(to_type(&s, &DataType::Int64).is_err());
        let f: ArrayRef = Arc::new(Float64Array::from(vec![0.1]));
        assert!(to_type(&f, &DataType::Decimal128(10, 2)).is_err());
    }
}
