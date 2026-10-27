//! Safe SQL text: quoted identifiers, escaped string literals, and typed
//! literals written from integers, so no value or name from a plan or a
//! schema is ever interpolated raw.

use arrow::datatypes::{DataType, TimeUnit};
use bellwether_core::encoding::Scalar;

use crate::residual::value_type;
use crate::{AdapterError, Result};

/// A double-quoted SQL identifier, `"` doubled. A NUL cannot be quoted and
/// is refused.
pub(crate) fn ident(name: &str, engine: &'static str) -> Result<String> {
    if name.contains('\0') {
        return Err(AdapterError::Unsupported {
            engine,
            what: format!("column {name:?}"),
            reason: "has a NUL character, which no SQL identifier can hold".into(),
            next: "rename the column, or emit for DataFusion".into(),
        });
    }
    Ok(format!("\"{}\"", name.replace('"', "\"\"")))
}

/// A single-quoted SQL string literal, `'` doubled, NUL characters
/// spliced in with `chr(0)` (a literal cannot hold one).
pub(crate) fn string(s: &str) -> String {
    let parts: Vec<String> = s
        .split('\0')
        .map(|p| format!("'{}'", p.replace('\'', "''")))
        .collect();
    if parts.len() == 1 {
        parts.into_iter().next().unwrap_or_default()
    } else {
        format!("({})", parts.join(" || chr(0) || "))
    }
}

/// `days` since 1970-01-01 as a proleptic Gregorian (year, month, day).
pub(crate) fn civil(days: i64) -> (i64, u32, u32) {
    // Howard Hinnant's days_from_civil inverse.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)] // 1..=12, 1..=31
    (if m <= 2 { y + 1 } else { y }, m as u32, d as u32)
}

fn per_second(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

fn digits(unit: TimeUnit) -> usize {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}

/// `YYYY-MM-DD` of `days`; years before 1 are refused (their spelling
/// differs between engines).
pub(crate) fn date_text(days: i64, engine: &'static str) -> Result<String> {
    let (y, m, d) = civil(days);
    if y < 1 {
        return Err(AdapterError::Unsupported {
            engine,
            what: format!("a date in year {y}"),
            reason: "is before year 1, which SQL engines spell differently".into(),
            next: "emit for DataFusion, whose literals are typed values".into(),
        });
    }
    Ok(format!("{y:04}-{m:02}-{d:02}"))
}

/// `HH:MM:SS[.fraction]` of a time of day in `unit`.
pub(crate) fn time_text(value: i64, unit: TimeUnit) -> String {
    let per = per_second(unit);
    let secs = value.div_euclid(per);
    let frac = value.rem_euclid(per);
    let (h, mi, s) = (secs / 3600, (secs / 60) % 60, secs % 60);
    let mut out = format!("{h:02}:{mi:02}:{s:02}");
    if digits(unit) > 0 {
        out.push_str(&format!(".{frac:0width$}", width = digits(unit)));
    }
    out
}

/// `YYYY-MM-DD HH:MM:SS[.fraction]` of a UTC instant in `unit`.
pub(crate) fn timestamp_text(value: i64, unit: TimeUnit, engine: &'static str) -> Result<String> {
    let per = per_second(unit);
    let secs = value.div_euclid(per);
    let frac = value.rem_euclid(per);
    let date = date_text(secs.div_euclid(86_400), engine)?;
    let tod = secs.rem_euclid(86_400) * per + frac;
    Ok(format!("{date} {}", time_text(tod, unit)))
}

/// The DuckDB type of a dataset type, as `read_parquet` types the Parquet
/// column Arrow writes for it.
pub(crate) fn duckdb_type(dt: &DataType) -> Result<String> {
    let unsupported = |reason: &str| AdapterError::Unsupported {
        engine: "duckdb",
        what: format!("a {dt} value"),
        reason: reason.to_string(),
        next: "emit for DataFusion, which reads it at its dataset type".into(),
    };
    Ok(match value_type(dt) {
        DataType::Boolean => "BOOLEAN".into(),
        DataType::Int8 => "TINYINT".into(),
        DataType::Int16 => "SMALLINT".into(),
        DataType::Int32 => "INTEGER".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::UInt8 => "UTINYINT".into(),
        DataType::UInt16 => "USMALLINT".into(),
        DataType::UInt32 => "UINTEGER".into(),
        DataType::UInt64 => "UBIGINT".into(),
        DataType::Float16 | DataType::Float32 => "FLOAT".into(),
        DataType::Float64 => "DOUBLE".into(),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => {
            if *p > 38 {
                return Err(unsupported(
                    "has more than 38 digits, and DuckDB reads such a Parquet decimal as a \
                     DOUBLE, so the comparison would not be exact",
                ));
            }
            if *s < 0 {
                return Err(unsupported("has a negative scale, which DuckDB has no type for"));
            }
            format!("DECIMAL({p},{s})")
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "VARCHAR".into(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "BLOB".into(),
        DataType::Date32 | DataType::Date64 => "DATE".into(),
        DataType::Time32(_) | DataType::Time64(TimeUnit::Microsecond) => "TIME".into(),
        DataType::Time64(_) => "TIME_NS".into(),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => "TIMESTAMP_NS".into(),
        DataType::Timestamp(_, None) => "TIMESTAMP".into(),
        DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => "TIMESTAMPTZ_NS".into(),
        DataType::Timestamp(_, Some(_)) => "TIMESTAMPTZ".into(),
        _ => return Err(unsupported("has no DuckDB literal")),
    })
}

/// A typed DuckDB literal of `s`, a value of dataset type `dt`, written
/// from its integer form: `CAST('<text>' AS <type>)`, exact for every
/// supported type (a time zone column's instant carries `+00`).
pub(crate) fn duckdb_literal(s: &Scalar, dt: &DataType) -> Result<String> {
    const E: &str = "duckdb";
    let ty = duckdb_type(dt)?;
    let cast = |text: String| format!("CAST({} AS {ty})", string(&text));
    Ok(match s {
        Scalar::Boolean(b) => if *b { "TRUE" } else { "FALSE" }.into(),
        Scalar::Int(v) => format!("CAST({v} AS {ty})"),
        Scalar::UInt(v) => format!("CAST({v} AS {ty})"),
        Scalar::Float(f) => cast(if f.is_nan() {
            "NaN".into()
        } else if f.is_infinite() {
            if *f > 0.0 { "Infinity" } else { "-Infinity" }.into()
        } else {
            format!("{f:?}")
        }),
        Scalar::Decimal128 { value, scale } => {
            cast(bellwether_core::agg::render_scaled(*value, *scale))
        }
        Scalar::Decimal256 { value, scale } => {
            let v = value.to_i128().ok_or_else(|| AdapterError::Unsupported {
                engine: E,
                what: format!("decimal {value}"),
                reason: "does not fit 38 digits".into(),
                next: "emit for DataFusion".into(),
            })?;
            cast(bellwether_core::agg::render_scaled(v, *scale))
        }
        Scalar::Utf8(v) => string(v),
        Scalar::Binary(b) => {
            let mut text = String::with_capacity(b.len() * 4);
            for byte in b {
                text.push_str(&format!("\\x{byte:02X}"));
            }
            format!("CAST('{text}' AS BLOB)")
        }
        Scalar::Date32(d) => cast(date_text(i64::from(*d), E)?),
        Scalar::Date64(ms) => {
            if ms.rem_euclid(86_400_000) != 0 {
                return Err(AdapterError::Unsupported {
                    engine: E,
                    what: format!("Date64 value {ms}"),
                    reason: "is not a whole day, and a Parquet DATE holds days".into(),
                    next: "emit for DataFusion".into(),
                });
            }
            cast(date_text(ms.div_euclid(86_400_000), E)?)
        }
        Scalar::Time { value, unit } => cast(time_text(*value, *unit)),
        Scalar::Timestamp { value, unit } => {
            let text = timestamp_text(*value, *unit, E)?;
            match value_type(dt) {
                DataType::Timestamp(_, Some(_)) => cast(format!("{text}+00")),
                _ => cast(text),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_dates_match_known_days() {
        assert_eq!(civil(0), (1970, 1, 1));
        assert_eq!(civil(19_723), (2024, 1, 1));
        assert_eq!(civil(-1), (1969, 12, 31));
        assert_eq!(civil(11_016), (2000, 2, 29));
        assert_eq!(civil(-719_162), (1, 1, 1));
    }

    #[test]
    fn literals_quote_everything() {
        assert_eq!(string("o'k"), "'o''k'");
        assert_eq!(string("a\0b"), "('a' || chr(0) || 'b')");
        assert_eq!(ident("we\"ird", "duckdb").unwrap(), "\"we\"\"ird\"");
        assert!(ident("a\0", "duckdb").is_err());
        assert_eq!(
            timestamp_text(-1, TimeUnit::Microsecond, "duckdb").unwrap(),
            "1969-12-31 23:59:59.999999"
        );
        assert_eq!(
            timestamp_text(1_704_067_200_123_456_789, TimeUnit::Nanosecond, "duckdb").unwrap(),
            "2024-01-01 00:00:00.123456789"
        );
        assert_eq!(time_text(3_723_000, TimeUnit::Millisecond), "01:02:03.000");
    }
}
