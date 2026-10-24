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

/// Parses a `--totals` column specification: either a bare decimal column
/// name, or `<column>:dec(<scale>)` for exact float quantization.
pub(crate) fn parse_total_spec(spec: &str) -> Result<(&str, Option<i8>), String> {
    let Some((col, rest)) = spec.rsplit_once(':') else {
        return Ok((spec, None));
    };
    if col.is_empty() {
        return Err(format!(
            "total spec {spec:?} has no column name; write <column> or <column>:dec(<scale>)"
        ));
    }
    let Some(inner) = rest.strip_prefix("dec(").and_then(|s| s.strip_suffix(')')) else {
        return Err(format!(
            "total spec {spec:?} has unknown quantization {rest:?}; use {col}:dec(<scale>), for example {col}:dec(4)"
        ));
    };
    let scale: i8 = inner.parse().map_err(|_| {
        format!(
            "total spec {spec:?} has invalid scale {inner:?}; pass an integer in -38..=38, for example {col}:dec(4)"
        )
    })?;
    if !(-38..=38).contains(&scale) {
        return Err(format!(
            "total spec {spec:?} has scale {scale} outside -38..=38"
        ));
    }
    Ok((col, Some(scale)))
}

/// Exact unsigned multi-word big integer (little-endian `u64` limbs, no
/// trailing zero limbs) for exact binary-float to decimal conversion.
#[derive(Clone, Default, PartialEq, Eq)]
struct BigUint(Vec<u64>);

impl BigUint {
    fn from_u64(v: u64) -> Self {
        if v == 0 {
            Self(Vec::new())
        } else {
            Self(vec![v])
        }
    }

    fn is_zero(&self) -> bool {
        self.0.is_empty()
    }

    fn is_odd(&self) -> bool {
        self.0.first().is_some_and(|w| w & 1 == 1)
    }

    fn trim(&mut self) {
        while self.0.last() == Some(&0) {
            self.0.pop();
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    fn mul_small(&mut self, factor: u64) {
        if self.is_zero() || factor == 1 {
            return;
        }
        if factor == 0 {
            self.0.clear();
            return;
        }
        let mut carry = 0u128;
        for w in &mut self.0 {
            let prod = u128::from(*w) * u128::from(factor) + carry;
            *w = prod as u64;
            carry = prod >> 64;
        }
        if carry != 0 {
            self.0.push(carry as u64);
        }
    }

    fn mul_pow5(&mut self, mut exp: u32) {
        const POW5_27: u64 = 7_450_580_596_923_828_125; // 5^27
        while exp >= 27 {
            self.mul_small(POW5_27);
            exp -= 27;
        }
        if exp > 0 {
            self.mul_small(5u64.pow(exp));
        }
    }

    fn shl_bits(&mut self, bits: u32) {
        if self.is_zero() || bits == 0 {
            return;
        }
        let word_shift = (bits / 64) as usize;
        let bit_shift = bits % 64;
        if bit_shift == 0 {
            let mut out = vec![0u64; word_shift];
            out.extend_from_slice(&self.0);
            self.0 = out;
            return;
        }
        let mut out = vec![0u64; word_shift + self.0.len() + 1];
        let mut carry = 0u64;
        for (i, &w) in self.0.iter().enumerate() {
            out[word_shift + i] = (w << bit_shift) | carry;
            carry = w >> (64 - bit_shift);
        }
        out[word_shift + self.0.len()] = carry;
        self.0 = out;
        self.trim();
    }

    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.0.len().cmp(&other.0.len()) {
            std::cmp::Ordering::Equal => {
                for (&a, &b) in self.0.iter().zip(&other.0).rev() {
                    if a != b {
                        return a.cmp(&b);
                    }
                }
                std::cmp::Ordering::Equal
            }
            ord => ord,
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    fn bit_len(&self) -> u32 {
        match self.0.last() {
            None => 0,
            Some(&top) => (self.0.len() as u32 - 1) * 64 + (64 - top.leading_zeros()),
        }
    }

    fn bit(&self, idx: u32) -> bool {
        let w = (idx / 64) as usize;
        let b = idx % 64;
        self.0.get(w).is_some_and(|v| (v >> b) & 1 == 1)
    }

    fn set_bit(&mut self, idx: u32) {
        let w = (idx / 64) as usize;
        let b = idx % 64;
        if self.0.len() <= w {
            self.0.resize(w + 1, 0);
        }
        self.0[w] |= 1u64 << b;
    }

    fn shl1_with_bit(&mut self, b: bool) {
        let mut carry = u64::from(b);
        for w in &mut self.0 {
            let next = *w >> 63;
            *w = (*w << 1) | carry;
            carry = next;
        }
        if carry != 0 {
            self.0.push(carry);
        }
    }

    fn sub_assign(&mut self, other: &Self) {
        let mut borrow = 0u64;
        for (i, w) in self.0.iter_mut().enumerate() {
            let rhs = other.0.get(i).copied().unwrap_or(0);
            let (d1, b1) = w.overflowing_sub(rhs);
            let (d2, b2) = d1.overflowing_sub(borrow);
            *w = d2;
            borrow = u64::from(b1 || b2);
        }
        debug_assert_eq!(borrow, 0);
        self.trim();
    }

    fn add_one(&mut self) {
        let mut carry = 1u64;
        for w in &mut self.0 {
            let (s, c) = w.overflowing_add(carry);
            *w = s;
            carry = u64::from(c);
            if carry == 0 {
                break;
            }
        }
        if carry != 0 {
            self.0.push(carry);
        }
    }

    /// Exact division `self / rhs`, returning `(quotient, remainder)`.
    fn div_rem(&self, rhs: &Self) -> (Self, Self) {
        debug_assert!(!rhs.is_zero());
        if self.cmp(rhs) == std::cmp::Ordering::Less {
            return (Self::default(), self.clone());
        }
        let mut q = Self::default();
        let mut r = Self::default();
        for i in (0..self.bit_len()).rev() {
            r.shl1_with_bit(self.bit(i));
            if r.cmp(rhs) != std::cmp::Ordering::Less {
                r.sub_assign(rhs);
                q.set_bit(i);
            }
        }
        q.trim();
        r.trim();
        (q, r)
    }

    fn to_u128(&self) -> Option<u128> {
        match self.0.as_slice() {
            [] => Some(0),
            [lo] => Some(u128::from(*lo)),
            [lo, hi] => Some(u128::from(*lo) | (u128::from(*hi) << 64)),
            _ => None,
        }
    }
}

/// Converts an `f64` to its exact decimal expansion and rounds half-to-even at
/// `scale`, returning the scaled integer as an `i128` (or `None` on overflow;
/// caller checks `is_finite` first).
pub(crate) fn quantize_f64_half_to_even(v: f64, scale: i8) -> Option<i128> {
    debug_assert!(v.is_finite());
    if v == 0.0 {
        return Some(0);
    }
    let bits = v.to_bits();
    let neg = (bits >> 63) != 0;
    let exp_bits = ((bits >> 52) & 0x7ff) as i32;
    let frac = bits & ((1u64 << 52) - 1);
    let (mant, exp2) = if exp_bits == 0 {
        (frac, -1074i32)
    } else {
        (frac | (1u64 << 52), exp_bits - 1023 - 52)
    };
    let s = i32::from(scale);
    // Exact value * 10^s = mant * 2^(exp2 + s) * 5^s = num / den.
    let mut num = BigUint::from_u64(mant);
    let mut den = BigUint::from_u64(1);
    if s >= 0 {
        num.mul_pow5(s.unsigned_abs());
    } else {
        den.mul_pow5(s.unsigned_abs());
    }
    let e2 = exp2 + s;
    if e2 >= 0 {
        num.shl_bits(e2.unsigned_abs());
    } else {
        den.shl_bits(e2.unsigned_abs());
    }
    let (mut q, r) = num.div_rem(&den);
    if !r.is_zero() {
        let mut twice_r = r;
        twice_r.shl_bits(1);
        match twice_r.cmp(&den) {
            std::cmp::Ordering::Greater => q.add_one(),
            std::cmp::Ordering::Equal if q.is_odd() => q.add_one(),
            _ => {}
        }
    }
    let mag = q.to_u128()?;
    if neg {
        if mag == (1u128 << 127) {
            Some(i128::MIN)
        } else {
            i128::try_from(mag).ok().map(|x| -x)
        }
    } else {
        i128::try_from(mag).ok()
    }
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
