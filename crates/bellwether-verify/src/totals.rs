//! Exact decimal totals.
//!
//! A total is the exact sum of a decimal column's non-null values at the
//! dataset scale. It is reported as a scaled integer plus the scale: an
//! `i128` for a Decimal128 column and an `i256` for a Decimal256 column. A
//! total whose exact value does not fit that integer is an error. Floats are
//! never involved.
//!
//! Sums are held in a [`Wide`] accumulator, a two's-complement `i256` plus a
//! signed count of 2^256 wraps, which represents every reachable sum exactly
//! and canonically. Addition is therefore associative and commutative, so a
//! total and its overflow verdict do not depend on row order or on how many
//! passes the comparison took.

use std::fmt;

use arrow::datatypes::i256;
use serde::{Deserialize, Serialize};

/// An exact running sum: `carry × 2^256 + low`, with `low` a signed `i256`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Wide {
    carry: i64,
    low: i256,
}

impl Wide {
    /// Adds a value at the dataset scale.
    pub(crate) fn add(&mut self, v: i256) {
        if let Some(sum) = self.low.checked_add(v) {
            self.low = sum;
        } else {
            // Two values of one sign overflowed: the true sum is the wrapped
            // value plus (or minus) 2^256.
            self.low = self.low.wrapping_add(v);
            self.carry += if v.is_negative() { -1 } else { 1 };
        }
    }

    /// Adds another accumulator.
    pub(crate) fn merge(&mut self, other: &Wide) {
        self.add(other.low);
        self.carry += other.carry;
    }

    /// The sum as an `i128`, if it fits.
    pub(crate) fn to_i128(self) -> Option<i128> {
        (self.carry == 0).then(|| self.low.to_i128()).flatten()
    }

    /// The sum as an `i256`, if it fits.
    pub(crate) fn to_i256(self) -> Option<i256> {
        (self.carry == 0).then_some(self.low)
    }
}

/// An exact total: the integer at the dataset scale.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TotalValue {
    /// A Decimal128 column's total.
    I128(i128),
    /// A Decimal256 column's total.
    I256(i256),
}

impl fmt::Display for TotalValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::I128(v) => write!(f, "{v}"),
            Self::I256(v) => write!(f, "{v}"),
        }
    }
}

/// The serialized form: the width named explicitly, so a Decimal256 total
/// that happens to fit in an `i128` still reads back as an `i256`, and the
/// value as a decimal string, since JSON numbers do not carry 128- or 256-bit
/// integers exactly through common readers.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TotalRepr {
    width: Width,
    value: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Width {
    I128,
    I256,
}

impl Serialize for TotalValue {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let width = match self {
            Self::I128(_) => Width::I128,
            Self::I256(_) => Width::I256,
        };
        TotalRepr {
            width,
            value: self.to_string(),
        }
        .serialize(s)
    }
}

impl<'de> Deserialize<'de> for TotalValue {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let r = TotalRepr::deserialize(d)?;
        let bad =
            || serde::de::Error::custom(format!("{:?} is not an integer of its width", r.value));
        match r.width {
            Width::I128 => r.value.parse::<i128>().map(Self::I128).map_err(|_| bad()),
            Width::I256 => i256::from_string(&r.value).map(Self::I256).ok_or_else(bad),
        }
    }
}

/// A total reported as a scaled integer plus the scale.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExactTotal {
    /// The total at `scale`: the decimal value is `unscaled × 10^-scale`.
    pub unscaled: TotalValue,
    /// The dataset scale of the column.
    pub scale: i8,
}

impl ExactTotal {
    /// The decimal rendered at the declared scale, for example `-12.30` for
    /// `unscaled = -1230, scale = 2`. Exact for every value and scale.
    #[must_use]
    pub fn render(&self) -> String {
        render_decimal(&self.unscaled.to_string(), self.scale)
    }
}

impl fmt::Display for ExactTotal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.render())
    }
}

/// Renders the integer `digits` (optionally with a leading `-`) at `scale`.
pub(crate) fn render_decimal(digits: &str, scale: i8) -> String {
    let (neg, abs) = match digits.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, digits),
    };
    let sign = if neg { "-" } else { "" };
    if scale <= 0 {
        if abs == "0" {
            return "0".into();
        }
        let zeros = "0".repeat(usize::from(scale.unsigned_abs()));
        return format!("{sign}{abs}{zeros}");
    }
    let s = usize::from(scale.unsigned_abs());
    let padded = format!("{abs:0>width$}", width = s + 1);
    let (int, frac) = padded.split_at(padded.len() - s);
    format!("{sign}{int}.{frac}")
}

/// Finalizes a sum for a column of the given width.
pub(crate) fn finalize(sum: Wide, wide: bool, scale: i8) -> Option<ExactTotal> {
    let unscaled = if wide {
        TotalValue::I256(sum.to_i256()?)
    } else {
        TotalValue::I128(sum.to_i128()?)
    };
    Some(ExactTotal { unscaled, scale })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn w(values: &[i256]) -> Wide {
        let mut acc = Wide::default();
        for v in values {
            acc.add(*v);
        }
        acc
    }

    #[test]
    fn renders_at_every_scale() {
        assert_eq!(render_decimal("-1230", 2), "-12.30");
        assert_eq!(render_decimal("5", 3), "0.005");
        assert_eq!(render_decimal("-5", 3), "-0.005");
        assert_eq!(render_decimal("0", 2), "0.00");
        assert_eq!(render_decimal("12", 0), "12");
        assert_eq!(render_decimal("12", -2), "1200");
        assert_eq!(render_decimal("0", -2), "0");
    }

    #[test]
    fn totals_round_trip_with_their_width() {
        for t in [
            TotalValue::I128(-5),
            TotalValue::I256(i256::from_i128(-5)),
            TotalValue::I256(i256::MAX),
            TotalValue::I128(i128::MIN),
        ] {
            let json = serde_json::to_string(&t).unwrap();
            assert_eq!(
                serde_json::from_str::<TotalValue>(&json).unwrap(),
                t,
                "{json}"
            );
        }
        assert_eq!(
            serde_json::to_string(&TotalValue::I256(i256::from_i128(7))).unwrap(),
            r#"{"width":"i256","value":"7"}"#
        );
        assert!(serde_json::from_str::<TotalValue>(r#"{"width":"i128","value":"1e3"}"#).is_err());
        assert!(serde_json::from_str::<TotalValue>(r#""7""#).is_err());
    }

    #[test]
    fn wide_sums_are_order_independent_across_wraps() {
        let big = i256::MAX;
        let values = [
            big,
            big,
            i256::from_i128(-7),
            big.wrapping_neg(),
            big.wrapping_neg(),
        ];
        let forward = w(&values);
        let mut rev = values;
        rev.reverse();
        assert_eq!(forward, w(&rev));
        assert_eq!(forward.to_i256(), Some(i256::from_i128(-7)));
        // An intermediate overflow of i128 does not decide the verdict.
        let m = i256::from_i128(i128::MAX);
        let s = w(&[m, m, m.wrapping_neg()]);
        assert_eq!(s.to_i128(), Some(i128::MAX));
        assert_eq!(w(&[m, i256::ONE]).to_i128(), None);
        assert_eq!(w(&[big, i256::ONE]).to_i256(), None);
        // Merging partial sums equals adding every value once.
        let (a, b) = values.split_at(2);
        let mut merged = w(a);
        merged.merge(&w(b));
        assert_eq!(merged, forward);
    }
}
