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

/// Why a float could not be quantized ([`quantize`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuantizeError {
    /// NaN or an infinity: no decimal value.
    NotFinite,
    /// The rounded value times `10^scale` does not fit an `i128`.
    TooLarge,
    /// The scale is above [`crate::MAX_QUANTIZE_SCALE`].
    ScaleTooLarge,
}

impl fmt::Display for QuantizeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NotFinite => "NaN and infinity have no decimal value",
            Self::TooLarge => "the value at this scale does not fit a 128-bit decimal",
            Self::ScaleTooLarge => "the scale is above 38, the most a 128-bit decimal holds",
        })
    }
}

/// `round_half_even(value × 10^scale)` from `value`'s exact binary value,
/// as an `i128`: the integer at `scale` a decimal column would store. No
/// float arithmetic is involved; `value = m × 2^e` is expanded exactly and
/// the rounding is decided on integers, so a tie (`0.125` at scale 2) goes
/// to the even neighbor (`0.12`) and a value just below a tie never rounds
/// up. `-0.0` quantizes to `0`.
pub fn quantize(value: f64, scale: u8) -> Result<i128, QuantizeError> {
    if !value.is_finite() {
        return Err(QuantizeError::NotFinite);
    }
    if scale > crate::spec::MAX_QUANTIZE_SCALE {
        return Err(QuantizeError::ScaleTooLarge);
    }
    let bits = value.to_bits();
    let negative = bits >> 63 == 1;
    let exp_bits = i32::try_from((bits >> 52) & 0x7ff).unwrap_or(0);
    let fraction = bits & ((1u64 << 52) - 1);
    // value = mantissa × 2^exponent exactly.
    let (mantissa, exponent) = if exp_bits == 0 {
        (fraction, -1074)
    } else {
        (fraction | (1u64 << 52), exp_bits - 1075)
    };
    if mantissa == 0 {
        return Ok(0);
    }
    let pow = i256::from_i128(10).wrapping_pow(u32::from(scale));
    // mantissa < 2^53 and 10^scale < 2^127, so scaled < 2^180: exact in an
    // i256.
    let scaled = i256::from_i128(i128::from(mantissa)).wrapping_mul(pow);
    let magnitude = if exponent >= 0 {
        // mantissa ≥ 2^52, so an exponent ≥ 75 puts the value at or above
        // 2^127.
        if exponent >= 75 {
            return Err(QuantizeError::TooLarge);
        }
        scaled << u8::try_from(exponent).map_err(|_| QuantizeError::TooLarge)?
    } else {
        let shift = exponent.unsigned_abs();
        if shift > 200 {
            // scaled < 2^180 < 2^(shift − 1): below half of one unit, so 0.
            i256::ZERO
        } else {
            let shift = u8::try_from(shift).map_err(|_| QuantizeError::TooLarge)?;
            let quotient = scaled >> shift;
            let remainder = scaled.wrapping_sub(quotient << shift);
            let half = i256::ONE << (shift - 1);
            let odd = quotient.as_i128() & 1 == 1;
            if remainder > half || (remainder == half && odd) {
                quotient.wrapping_add(i256::ONE)
            } else {
                quotient
            }
        }
    };
    let magnitude = magnitude.to_i128().ok_or(QuantizeError::TooLarge)?;
    Ok(if negative { -magnitude } else { magnitude })
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

    /// The oracle: Rust's exact decimal expansion of `v` (formatting with
    /// an explicit precision prints the exact binary value; an f64 has at
    /// most 1,074 fractional digits), rounded half to even by digits.
    fn by_digits(v: f64, scale: u8) -> Result<i128, QuantizeError> {
        if !v.is_finite() {
            return Err(QuantizeError::NotFinite);
        }
        let s = format!("{:.1100}", v.abs());
        let (int, frac) = s.split_once('.').unwrap();
        let (kept, rest) = frac.split_at(usize::from(scale));
        let digits = format!("{int}{kept}");
        let mut q: i128 = digits
            .trim_start_matches('0')
            .parse::<i128>()
            .or_else(|e| {
                if digits.trim_start_matches('0').is_empty() {
                    Ok(0)
                } else {
                    Err(e)
                }
            })
            .map_err(|_| QuantizeError::TooLarge)?;
        let first = rest.as_bytes()[0];
        let tail_nonzero = rest[1..].bytes().any(|b| b != b'0');
        let up = first > b'5' || (first == b'5' && (tail_nonzero || q % 2 == 1));
        if up {
            q = q.checked_add(1).ok_or(QuantizeError::TooLarge)?;
        }
        Ok(if v.is_sign_negative() { -q } else { q })
    }

    #[test]
    fn quantization_rounds_the_exact_value_half_to_even() {
        // exact ties go to the even neighbor
        assert_eq!(quantize(0.125, 2), Ok(12));
        assert_eq!(quantize(0.375, 2), Ok(38));
        assert_eq!(quantize(2.5, 0), Ok(2));
        assert_eq!(quantize(3.5, 0), Ok(4));
        assert_eq!(quantize(-2.5, 0), Ok(-2));
        // 0.35 is 0.34999999999999997779…: not a tie, rounds down; 2.675 is
        // 2.67499999999999982236431605997495353221893310546875
        assert_eq!(quantize(0.35, 1), Ok(3));
        assert_eq!(quantize(2.675, 2), Ok(267));
        // 1.0000000000000002 at scale 15 is 1000000000000000.2…
        assert_eq!(
            quantize(1.000_000_000_000_000_2, 15),
            Ok(1_000_000_000_000_000)
        );
        assert_eq!(quantize(-0.0, 4), Ok(0));
        assert_eq!(quantize(f64::MIN_POSITIVE / 4.0, 38), Ok(0));
        assert_eq!(quantize(123_456.789, 4), Ok(1_234_567_890));
        assert_eq!(quantize(f64::NAN, 4), Err(QuantizeError::NotFinite));
        assert_eq!(
            quantize(f64::NEG_INFINITY, 4),
            Err(QuantizeError::NotFinite)
        );
        assert_eq!(quantize(1e38, 4), Err(QuantizeError::TooLarge));
        assert_eq!(quantize(f64::MAX, 0), Err(QuantizeError::TooLarge));
        assert_eq!(quantize(1.0, 39), Err(QuantizeError::ScaleTooLarge));
        // 2^126 fits, 2^127 does not
        assert_eq!(quantize(2f64.powi(126), 0), Ok(1i128 << 126));
        assert_eq!(quantize(2f64.powi(127), 0), Err(QuantizeError::TooLarge));
        assert_eq!(quantize(-(2f64.powi(126)), 0), Ok(-(1i128 << 126)));
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(4096))]

        /// Every f64 (normal, subnormal, huge, tiny) at every scale agrees
        /// with the digit-string oracle.
        #[test]
        fn quantization_matches_the_exact_decimal_expansion(
            bits in proptest::prelude::any::<u64>(),
            scale in 0u8..=38,
        ) {
            let v = f64::from_bits(bits);
            proptest::prop_assert_eq!(quantize(v, scale), by_digits(v, scale));
        }

        /// Values with few digits, where ties and near-ties live.
        #[test]
        fn quantization_matches_on_short_decimals(
            units in -10_000_000i64..10_000_000,
            exp in 0i32..9,
            scale in 0u8..=8,
        ) {
            #[allow(clippy::cast_precision_loss)]
            let v = units as f64 / 10f64.powi(exp);
            proptest::prop_assert_eq!(quantize(v, scale), by_digits(v, scale));
        }
    }
}
