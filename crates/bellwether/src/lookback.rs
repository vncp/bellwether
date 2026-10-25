//! Time windows given as text: a lookback (`--lookback 252d`) and a range
//! (`--window 2026-01-01..2026-03-01`).
//!
//! A lookback is a whole number and a unit. The units are calendar units of
//! fixed length: `s` is a second, `m` 60 seconds, `h` 3,600 seconds, `d`
//! 86,400 seconds (one UTC calendar day; timestamps count no leap seconds),
//! and `w` 7 days. Months and years have no fixed length, and trading days
//! depend on an exchange's calendar, so none of them is a unit: a trading
//! calendar is out of scope, and `252d` means 252 calendar days. A bare
//! number is a lookback in an integer time column's own unit (a version
//! number, say), and only there.
//!
//! A lookback of `N` ending at `end` is the window `[end - N, end]`, both
//! ends inclusive, in the time column's dataset unit. For a column coarser
//! than a second the start is exact: a `Date32` row on day `d` is in the
//! window when its midnight is, so `N` converts to whole days rounding down
//! (a 36-hour lookback ending on day `d` starts on day `d - 1`). A start
//! below the column's range is clamped to its smallest value, which keeps
//! every row a longer window would.

use arrow::datatypes::{DataType, TimeUnit};
use bellwether_types::{Error, Result};

/// A lookback (module docs).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lookback {
    text: String,
    amount: i64,
    unit: Option<Unit>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Unit {
    Second,
    Minute,
    Hour,
    Day,
    Week,
}

impl Unit {
    fn seconds(self) -> i64 {
        match self {
            Self::Second => 1,
            Self::Minute => 60,
            Self::Hour => 3_600,
            Self::Day => 86_400,
            Self::Week => 7 * 86_400,
        }
    }
}

const UNITS: &str = "s (seconds), m (minutes), h (hours), d (calendar days of 24 hours, UTC), or \
                     w (7 days); months, years, and trading days are not units, because their \
                     length varies (a trading-day calendar is out of scope)";

impl Lookback {
    /// Parses `252d`, `90m`, `2w`, or a bare `5` (module docs). The number
    /// is a non-negative whole number.
    pub fn parse(text: &str) -> Result<Self> {
        let t = text.trim();
        let split = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
        let (digits, unit) = t.split_at(split);
        if digits.is_empty() {
            return Err(Error::Other(format!(
                "--lookback {text:?} does not start with a whole number; write a number and a \
                 unit, for example 252d, with the unit one of {UNITS}"
            )));
        }
        let amount: i64 = digits.parse().map_err(|_| {
            Error::Other(format!("--lookback {text:?} is too large for a lookback"))
        })?;
        let unit = match unit {
            "" => None,
            "s" => Some(Unit::Second),
            "m" => Some(Unit::Minute),
            "h" => Some(Unit::Hour),
            "d" => Some(Unit::Day),
            "w" => Some(Unit::Week),
            other => {
                return Err(Error::Other(format!(
                    "--lookback {text:?} has the unit {other:?}; the unit is one of {UNITS}"
                )))
            }
        };
        if let Some(u) = unit {
            amount.checked_mul(u.seconds()).ok_or_else(|| {
                Error::Other(format!("--lookback {text:?} is too large for a lookback"))
            })?;
        }
        Ok(Self {
            text: t.to_string(),
            amount,
            unit,
        })
    }

    /// The lookback as given (trimmed).
    #[must_use]
    pub fn as_given(&self) -> &str {
        &self.text
    }

    /// The lookback in seconds, or `None` for a bare number.
    #[must_use]
    pub fn seconds(&self) -> Option<i64> {
        self.unit.map(|u| self.amount * u.seconds())
    }

    /// The lookback in `ty`'s unit, rounded down for a unit coarser than
    /// the lookback's (module docs), for the time column `column`.
    pub(crate) fn in_unit(&self, column: &str, ty: &DataType) -> Result<i64> {
        let nanos_per_unit: Option<i128> = match ty {
            DataType::Timestamp(u, _) | DataType::Time32(u) | DataType::Time64(u) => {
                Some(match u {
                    TimeUnit::Second => 1_000_000_000,
                    TimeUnit::Millisecond => 1_000_000,
                    TimeUnit::Microsecond => 1_000,
                    TimeUnit::Nanosecond => 1,
                })
            }
            DataType::Date64 => Some(1_000_000),
            DataType::Date32 => Some(86_400 * 1_000_000_000),
            _ => None,
        };
        let integer = ty.is_integer();
        match (self.seconds(), nanos_per_unit) {
            (Some(secs), Some(per)) => {
                let n = i128::from(secs) * 1_000_000_000 / per;
                Ok(i64::try_from(n).unwrap_or(i64::MAX))
            }
            (None, None) if integer => Ok(self.amount),
            (Some(_), None) if integer => Err(Error::KeySpec(format!(
                "{column} is an integer column ({ty}) with no time unit, so --lookback {:?} \
                 cannot be converted to it; give the lookback as a bare whole number in its unit",
                self.text
            ))),
            (None, Some(_)) => Err(Error::KeySpec(format!(
                "--lookback {:?} has no unit, and {column} is a {ty} column; add a unit: {UNITS}",
                self.text
            ))),
            _ => Err(Error::KeySpec(format!(
                "{column} has type {ty}, which a time window cannot bound; use a timestamp, \
                 date, or integer column"
            ))),
        }
    }
}

/// The start of a window of `n` (in `ty`'s unit) ending at `end`: `end - n`,
/// clamped to the smallest value `ty` holds (module docs).
pub(crate) fn window_start(end: i64, n: i64, ty: &DataType) -> i64 {
    let min: i64 = match ty {
        DataType::Int8 => i64::from(i8::MIN),
        DataType::Int16 => i64::from(i16::MIN),
        DataType::Int32 | DataType::Date32 | DataType::Time32(_) => i64::from(i32::MIN),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => 0,
        _ => i64::MIN,
    };
    end.saturating_sub(n).max(min)
}

/// Splits a window `A..B` into its two ends, each non-empty; `..` appears
/// in no instant, date, or integer, so the split is unambiguous.
pub fn split_window(text: &str) -> Result<(&str, &str)> {
    match text.split_once("..") {
        Some((a, b)) if !a.trim().is_empty() && !b.trim().is_empty() && !b.contains("..") => {
            Ok((a.trim(), b.trim()))
        }
        _ => Err(Error::Other(format!(
            "--window {text:?} is not START..END; give both ends, for example \
             2026-01-01..2026-03-01 (each an RFC 3339 instant, a date, or an integer in the time \
             column's unit, both inclusive)"
        ))),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn lookbacks_parse_in_calendar_units() {
        assert_eq!(
            Lookback::parse("252d").unwrap().seconds(),
            Some(252 * 86_400)
        );
        assert_eq!(Lookback::parse(" 90m ").unwrap().seconds(), Some(5_400));
        assert_eq!(Lookback::parse("2w").unwrap().seconds(), Some(14 * 86_400));
        assert_eq!(Lookback::parse("3h").unwrap().seconds(), Some(10_800));
        assert_eq!(Lookback::parse("0s").unwrap().seconds(), Some(0));
        assert_eq!(Lookback::parse("7").unwrap().seconds(), None);
        for bad in [
            "",
            "d",
            "-1d",
            "1.5d",
            "3mo",
            "1y",
            "252td",
            "5 d",
            "99999999999999999999d",
        ] {
            assert!(Lookback::parse(bad).is_err(), "{bad:?}");
        }
        let e = Lookback::parse("252td").unwrap_err().to_string();
        assert!(e.contains("trading"), "{e}");
    }

    #[test]
    fn a_lookback_converts_to_the_column_unit_rounding_down() {
        let us = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let d = Lookback::parse("1d").unwrap();
        assert_eq!(d.in_unit("ts", &us).unwrap(), 86_400_000_000);
        assert_eq!(d.in_unit("day", &DataType::Date32).unwrap(), 1);
        assert_eq!(
            Lookback::parse("36h")
                .unwrap()
                .in_unit("day", &DataType::Date32)
                .unwrap(),
            1
        );
        assert_eq!(
            Lookback::parse("23h")
                .unwrap()
                .in_unit("day", &DataType::Date32)
                .unwrap(),
            0
        );
        assert_eq!(d.in_unit("d64", &DataType::Date64).unwrap(), 86_400_000);
        assert_eq!(
            Lookback::parse("5")
                .unwrap()
                .in_unit("v", &DataType::Int64)
                .unwrap(),
            5
        );
        assert!(d.in_unit("v", &DataType::Int64).is_err());
        assert!(Lookback::parse("5").unwrap().in_unit("ts", &us).is_err());
        assert!(d.in_unit("s", &DataType::Utf8).is_err());
    }

    #[test]
    fn a_window_start_is_clamped_to_the_column_range() {
        assert_eq!(window_start(10, 3, &DataType::Int64), 7);
        assert_eq!(window_start(2, 5, &DataType::UInt32), 0);
        assert_eq!(
            window_start(0, i64::MAX, &DataType::Date32),
            i64::from(i32::MIN)
        );
        assert_eq!(window_start(i64::MIN + 1, 5, &DataType::Int64), i64::MIN);
    }

    #[test]
    fn windows_split_at_two_dots() {
        assert_eq!(
            split_window("2026-01-01..2026-03-01").unwrap(),
            ("2026-01-01", "2026-03-01")
        );
        assert_eq!(split_window("1 .. 9").unwrap(), ("1", "9"));
        for bad in ["2026-01-01", "..5", "5..", "1..2..3", ""] {
            assert!(split_window(bad).is_err(), "{bad:?}");
        }
    }
}
