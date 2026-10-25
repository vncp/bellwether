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
//! ends inclusive, in the time column's dataset unit. `N` must be a whole
//! number of that unit: a 36-hour lookback on a `Date32` column (whose unit
//! is a day) is refused, never rounded; `1d` or `2d` is what it can
//! measure. A start below the column's range is clamped to its smallest
//! value, which keeps every row a longer window would.

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
    /// Largest first, so the first that divides a length is the largest it
    /// is whole in.
    const ALL: [Self; 5] = [
        Self::Week,
        Self::Day,
        Self::Hour,
        Self::Minute,
        Self::Second,
    ];

    fn seconds(self) -> i64 {
        match self {
            Self::Second => 1,
            Self::Minute => 60,
            Self::Hour => 3_600,
            Self::Day => 86_400,
            Self::Week => 7 * 86_400,
        }
    }

    fn suffix(self) -> &'static str {
        match self {
            Self::Second => "s",
            Self::Minute => "m",
            Self::Hour => "h",
            Self::Day => "d",
            Self::Week => "w",
        }
    }

    fn plural(self) -> &'static str {
        match self {
            Self::Second => "seconds",
            Self::Minute => "minutes",
            Self::Hour => "hours",
            Self::Day => "days",
            Self::Week => "weeks",
        }
    }

    fn parse(suffix: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|u| u.suffix() == suffix)
    }
}

const UNITS: &str = "s (seconds), m (minutes), h (hours), d (calendar days of 24 hours, UTC), or \
                     w (7 days); months, years, and trading days are not units, because their \
                     length varies (a trading-day calendar is out of scope)";

/// A lookback that cannot be used: the typed invalid argument `lookback`.
fn refuse(reason: String) -> Error {
    Error::InvalidArgument {
        field: "lookback",
        reason,
    }
}

/// `seconds` as a lookback in the largest unit it is whole in (`36h`).
fn whole_lookback(seconds: i128) -> String {
    let unit = Unit::ALL
        .into_iter()
        .find(|u| seconds % i128::from(u.seconds()) == 0)
        .unwrap_or(Unit::Second);
    format!("{}{}", seconds / i128::from(unit.seconds()), unit.suffix())
}

/// `num / den` (both non-negative) as a decimal when it has at most four
/// decimal places (`1.5`), else `None`.
fn short_decimal(num: i128, den: i128) -> Option<String> {
    let whole = num / den;
    let scaled = (num % den) * 10_000;
    if scaled % den != 0 {
        return None;
    }
    let frac = format!("{:04}", scaled / den);
    Some(format!("{whole}.{}", frac.trim_end_matches('0')))
}

impl Lookback {
    /// Parses `252d`, `90m`, `2w`, or a bare `5` (module docs). The number
    /// is a non-negative whole number; anything else is the typed invalid
    /// argument `lookback`, with a lookback to pass instead.
    pub fn parse(text: &str) -> Result<Self> {
        let t = text.trim();
        let split = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
        let (digits, unit) = t.split_at(split);
        if let Some(frac) = unit
            .strip_prefix('.')
            .filter(|f| f.starts_with(|c: char| c.is_ascii_digit()))
        {
            return Err(refuse(fractional(t, digits, frac)));
        }
        if digits.is_empty() {
            return Err(refuse(format!(
                "--lookback {text:?} does not start with a whole number; write a whole number and \
                 a unit, for example --lookback 252d, the unit one of {UNITS}; or a bare whole \
                 number for an integer time column"
            )));
        }
        let unit = match unit {
            "" => None,
            other => Some(Unit::parse(other).ok_or_else(|| {
                refuse(format!(
                    "--lookback {text:?} has the unit {other:?}; write a whole number and a \
                     unit, for example --lookback 252d, the unit one of {UNITS}"
                ))
            })?),
        };
        let longest = i64::MAX / unit.map_or(1, Unit::seconds);
        let amount = digits
            .parse::<i64>()
            .ok()
            .filter(|a| *a <= longest)
            .ok_or_else(|| {
                refuse(format!(
                    "--lookback {text:?} is too long; the longest lookback is {longest}{}",
                    unit.map_or("", Unit::suffix)
                ))
            })?;
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

    /// The lookback in `ty`'s unit, for the time column `column` (module
    /// docs): exact, or the typed invalid argument `lookback` when it is not
    /// a whole number of the unit or longer than the column can measure.
    pub(crate) fn in_unit(&self, column: &str, ty: &DataType) -> Result<i64> {
        if let DataType::Dictionary(_, v) = ty {
            return self.in_unit(column, v);
        }
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
        let text = &self.text;
        match (self.seconds(), nanos_per_unit) {
            (Some(secs), Some(per)) => {
                let nanos = i128::from(secs) * 1_000_000_000;
                if nanos % per != 0 {
                    return Err(refuse(self.not_whole(column, ty, nanos, per)));
                }
                i64::try_from(nanos / per).map_err(|_| {
                    // The unit is set: `seconds` is `Some`.
                    let unit = self.unit.unwrap_or(Unit::Second);
                    let longest =
                        i128::from(i64::MAX) * per / (i128::from(unit.seconds()) * 1_000_000_000);
                    refuse(format!(
                        "--lookback {text:?} is longer than {column} ({ty}) can measure; the \
                         longest lookback on it is {longest}{s}: pass --lookback {longest}{s} or \
                         less",
                        s = unit.suffix()
                    ))
                })
            }
            (None, None) if integer => Ok(self.amount),
            (Some(secs), None) if integer => Err(refuse(format!(
                "{column} is an integer column ({ty}) with no time unit, so --lookback {text:?} \
                 cannot be converted to it; give the lookback as a bare whole number in its own \
                 unit, for example --lookback {secs} if {column} counts seconds"
            ))),
            (None, Some(_)) => Err(refuse(format!(
                "--lookback {text:?} has no unit, and {column} is a {ty} column; add a unit, for \
                 example --lookback {text}d, the unit one of {UNITS}"
            ))),
            _ => Err(Error::KeySpec(format!(
                "{column} has type {ty}, which a time window cannot bound; use a timestamp, \
                 date, or integer column"
            ))),
        }
    }

    /// Why a lookback of `nanos` is not a whole number of `per`, the
    /// column's unit in nanoseconds, with the two lookbacks around it.
    fn not_whole(&self, column: &str, ty: &DataType, nanos: i128, per: i128) -> String {
        let text = &self.text;
        let lower = nanos / per;
        // Every lookback is whole seconds, so a unit it is not a whole
        // number of is longer than a second: a day, for a date column.
        let Some(unit) = Unit::ALL
            .into_iter()
            .find(|u| i128::from(u.seconds()) * 1_000_000_000 == per)
        else {
            return format!(
                "--lookback {text:?} is not a whole number of {column}'s unit ({ty}); a \
                 lookback on it is never rounded"
            );
        };
        let (s, plural) = (unit.suffix(), unit.plural());
        let size = short_decimal(nanos, per).map_or_else(
            || format!("between {lower} and {} {plural}", lower + 1),
            |d| format!("{d} {plural}"),
        );
        format!(
            "--lookback {text:?} is {size}, and {column} ({ty}) counts whole {plural}, so a \
             lookback on it is a whole number of {plural}, never rounded: pass --lookback \
             {lower}{s} or --lookback {}{s}",
            lower + 1
        )
    }
}

/// Why `text`, the decimal `digits.frac` (with an optional unit after
/// `frac`'s digits), is not a lookback, with the whole lookback to pass.
fn fractional(text: &str, digits: &str, frac: &str) -> String {
    let split = frac
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(frac.len());
    let (frac, suffix) = frac.split_at(split);
    let unit = Unit::parse(suffix);
    let head = format!("--lookback {text:?} is not a whole number; a lookback is a whole number");
    // The length in seconds (bare units without a unit) as `num / den`;
    // `None` when it overflows.
    let exact = || -> Option<(i128, i128)> {
        let den = 10_i128.checked_pow(u32::try_from(frac.len()).ok()?)?;
        let whole: i128 = if digits.is_empty() {
            0
        } else {
            digits.parse().ok()?
        };
        let num = whole
            .checked_mul(den)?
            .checked_add(frac.parse::<i128>().ok()?)?
            .checked_mul(i128::from(unit.map_or(1, Unit::seconds)))?;
        Some((num, den))
    };
    match (suffix, unit, exact()) {
        ("", _, Some((num, den))) => format!(
            "{head}, bare for an integer time column: pass --lookback {} or --lookback {}",
            num / den,
            num / den + 1
        ),
        (_, Some(_), Some((num, den))) if num % den == 0 => format!(
            "{head} and a unit: pass --lookback {}, the same length",
            whole_lookback(num / den)
        ),
        (_, Some(_), Some((num, den))) => format!(
            "{head} and a unit, and the smallest unit is a second: pass --lookback {} or \
             --lookback {}",
            whole_lookback(num / den),
            whole_lookback(num / den + 1)
        ),
        _ => format!("{head} and a unit, for example --lookback 252d, the unit one of {UNITS}"),
    }
}

/// Whether a window's start lies after its end, judged where both ends are
/// the same kind of value (two integers, or two instants or dates), so a
/// reversed window is a usage error before any lookup runs. Ends of mixed
/// kinds are judged later, in the time column's unit.
#[must_use]
pub fn window_reversed(from: &str, to: &str) -> bool {
    let instant = |s: &str| {
        let s = s.trim();
        let full = if s.len() == 10 && s.as_bytes().get(4) == Some(&b'-') {
            format!("{s}T00:00:00Z")
        } else {
            s.to_string()
        };
        bellwether_store::parse_rfc3339(&full)
            .ok()
            .and_then(|t| t.timestamp_nanos_opt())
    };
    match (from.trim().parse::<i64>(), to.trim().parse::<i64>()) {
        (Ok(a), Ok(b)) => a > b,
        _ => matches!((instant(from), instant(to)), (Some(a), Some(b)) if a > b),
    }
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
        // Every refusal is the typed invalid argument `lookback`.
        for bad in ["", "1.5d", "3mo", "99999999999999999999d"] {
            assert!(
                matches!(
                    Lookback::parse(bad),
                    Err(Error::InvalidArgument {
                        field: "lookback",
                        ..
                    })
                ),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn a_fractional_lookback_is_not_a_whole_number_and_names_the_same_length() {
        let e = |t: &str| Lookback::parse(t).unwrap_err().to_string();
        for (given, pass) in [
            ("1.5d", "pass --lookback 36h, the same length"),
            ("0.5h", "pass --lookback 30m, the same length"),
            ("2.0w", "pass --lookback 2w, the same length"),
            ("1.25s", "pass --lookback 1s or --lookback 2s"),
            ("2.5", "pass --lookback 2 or --lookback 3"),
        ] {
            let msg = e(given);
            assert!(msg.contains("is not a whole number"), "{msg}");
            assert!(msg.contains(pass), "{given}: {msg}");
            assert!(!msg.contains("has the unit"), "{msg}");
        }
        assert!(e("1.5x").contains("is not a whole number"));
        let msg = e("99999999999999999999d");
        assert!(
            msg.contains("the longest lookback is 106751991167300d"),
            "{msg}"
        );
    }

    #[test]
    fn a_lookback_converts_to_the_column_unit_exactly() {
        let us = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let d = Lookback::parse("1d").unwrap();
        assert_eq!(d.in_unit("ts", &us).unwrap(), 86_400_000_000);
        assert_eq!(d.in_unit("day", &DataType::Date32).unwrap(), 1);
        // Not a whole number of days: refused, never rounded, with the two
        // lookbacks around it.
        for (partial, size, pass) in [
            ("36h", "is 1.5 days", "pass --lookback 1d or --lookback 2d"),
            ("6h", "is 0.25 days", "pass --lookback 0d or --lookback 1d"),
            (
                "23h",
                "is between 0 and 1 days",
                "pass --lookback 0d or --lookback 1d",
            ),
            (
                "1s",
                "is between 0 and 1 days",
                "pass --lookback 0d or --lookback 1d",
            ),
        ] {
            let err = Lookback::parse(partial)
                .unwrap()
                .in_unit("day", &DataType::Date32)
                .unwrap_err();
            assert!(
                matches!(
                    err,
                    Error::InvalidArgument {
                        field: "lookback",
                        ..
                    }
                ),
                "{err:?}"
            );
            let e = err.to_string();
            assert!(e.contains(size), "{e}");
            assert!(e.contains("counts whole days"), "{e}");
            assert!(e.contains(pass), "{e}");
        }
        assert_eq!(d.in_unit("d64", &DataType::Date64).unwrap(), 86_400_000);
        let dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(us.clone()));
        assert_eq!(d.in_unit("ts", &dict).unwrap(), 86_400_000_000);
        assert_eq!(
            Lookback::parse("5")
                .unwrap()
                .in_unit("v", &DataType::Int64)
                .unwrap(),
            5
        );
        let e = d.in_unit("v", &DataType::Int64).unwrap_err().to_string();
        assert!(
            e.contains("for example --lookback 86400 if v counts seconds"),
            "{e}"
        );
        let e = Lookback::parse("5")
            .unwrap()
            .in_unit("ts", &us)
            .unwrap_err()
            .to_string();
        assert!(e.contains("for example --lookback 5d"), "{e}");
        assert!(d.in_unit("s", &DataType::Utf8).is_err());
        // Longer than the column can measure: the longest is named.
        let ns = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let e = Lookback::parse("9999999w")
            .unwrap()
            .in_unit("ts", &ns)
            .unwrap_err()
            .to_string();
        assert!(e.contains("the longest lookback on it is 15250w"), "{e}");
        let e = Lookback::parse("99999999w")
            .unwrap()
            .in_unit("ts", &us)
            .unwrap_err()
            .to_string();
        assert!(e.contains("pass --lookback 15250284w or less"), "{e}");
    }

    #[test]
    fn lookbacks_convert_exactly_or_are_refused() {
        let conv =
            |given: &str, column: &str, ty: &DataType| Lookback::parse(given)?.in_unit(column, ty);
        let us = DataType::Timestamp(TimeUnit::Microsecond, None);
        let ms = DataType::Timestamp(TimeUnit::Millisecond, None);
        assert_eq!(conv("252d", "ts", &us).unwrap(), 252 * 86_400_000_000);
        assert_eq!(conv("2w", "d", &DataType::Date32).unwrap(), 14);
        assert_eq!(conv("48h", "d", &DataType::Date32).unwrap(), 2);
        assert_eq!(conv("90m", "ts", &ms).unwrap(), 5_400_000);
        assert_eq!(conv("0s", "ts", &ms).unwrap(), 0);
        assert_eq!(conv("7", "v", &DataType::Int32).unwrap(), 7);
        // not a whole number of days, no unit, a unit on an integer column,
        // an unknown unit, a sign, and an overflow are refused
        assert!(conv("1h", "d", &DataType::Date32).is_err());
        assert!(conv("30", "ts", &us).is_err());
        assert!(conv("7d", "v", &DataType::Int64).is_err());
        assert!(conv("3y", "ts", &us).is_err());
        assert!(conv("-1d", "ts", &us).is_err());
        assert!(conv("99999999999999999999w", "ts", &us).is_err());
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
