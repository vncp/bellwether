//! Property tests for the canonical encoding (ADR 002, 016): encoded byte
//! order equals typed order for every type; composite tuples order
//! lexicographically with prefixes first; values from files at another
//! decimal scale, decimal width, or timestamp unit encode exactly as the
//! dataset type says, or fail loudly.
#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::many_single_char_names,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::needless_for_each,
    clippy::case_sensitive_file_extension_comparisons
)]

use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    Decimal256Array, DictionaryArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, LargeStringArray, StringArray, StringViewArray,
    Time32MillisecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{i256, DataType, Int32Type, TimeUnit};
use bellwether_core::encoding::{
    encode_array_value, encode_array_value_as, encode_nullable_as, encode_tuple, encode_tuple_as,
    encode_value, prefix_bound, KeyColumns, Value,
};
use proptest::prelude::*;

fn enc(v: Value<'_>) -> Vec<u8> {
    encode_tuple(&[v]).unwrap()
}

fn enc_arr(a: &dyn Array, row: usize) -> Vec<u8> {
    let mut out = Vec::new();
    assert!(encode_array_value(a, row, &mut out).unwrap());
    out
}

fn enc_arr_as(a: &dyn Array, row: usize, declared: &DataType) -> bellwether_types::Result<Vec<u8>> {
    let mut out = Vec::new();
    encode_array_value_as(a, row, declared, &mut out).map(|_| out)
}

fn canon_f64(f: f64) -> f64 {
    if f.is_nan() {
        f64::NAN
    } else {
        f
    }
}

fn check_pair<T, F>(a: T, b: T, typed: Ordering, f: F)
where
    F: Fn(T) -> Vec<u8>,
{
    assert_eq!(f(a).cmp(&f(b)), typed);
}

fn any_f64() -> impl Strategy<Value = f64> {
    prop_oneof![
        any::<f64>(),
        Just(f64::NAN),
        Just(-f64::NAN),
        Just(f64::INFINITY),
        Just(f64::NEG_INFINITY),
        Just(0.0),
        Just(-0.0),
        Just(f64::MIN_POSITIVE),
    ]
}

/// Strings biased toward NUL bytes, where escaping and terminators matter.
fn nul_string() -> impl Strategy<Value = String> {
    prop_oneof![
        any::<String>(),
        "[\\x00\\x01a]{0,6}",
        Just(String::new()),
        Just("\0".to_string()),
        Just("a\0".to_string()),
        Just("a".to_string()),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn bool_order(a: bool, b: bool) {
        check_pair(a, b, a.cmp(&b), |v| enc(Value::Boolean(v)));
        let arr = BooleanArray::from(vec![a, b]);
        prop_assert_eq!(enc_arr(&arr, 0).cmp(&enc_arr(&arr, 1)), a.cmp(&b));
    }

    #[test]
    fn signed_order_all_widths(a: i64, b: i64, c: i8, d: i8, e: i16, f: i16, g: i32, h: i32) {
        check_pair(a, b, a.cmp(&b), |v| enc(Value::Int(v)));
        let arr = Int64Array::from(vec![a, b]);
        prop_assert_eq!(enc_arr(&arr, 0).cmp(&enc_arr(&arr, 1)), a.cmp(&b));
        let arr = Int8Array::from(vec![c, d]);
        prop_assert_eq!(enc_arr(&arr, 0).cmp(&enc_arr(&arr, 1)), c.cmp(&d));
        prop_assert_eq!(enc_arr(&arr, 0), enc(Value::Int(c.into())));
        let arr = Int16Array::from(vec![e, f]);
        prop_assert_eq!(enc_arr(&arr, 0).cmp(&enc_arr(&arr, 1)), e.cmp(&f));
        let arr = Int32Array::from(vec![g, h]);
        prop_assert_eq!(enc_arr(&arr, 0).cmp(&enc_arr(&arr, 1)), g.cmp(&h));
        prop_assert_eq!(enc_arr(&arr, 0), enc(Value::Int(g.into())));
        // a narrow file column under a wide dataset type: same bytes
        prop_assert_eq!(enc_arr_as(&arr, 0, &DataType::Int64).unwrap(), enc(Value::Int(g.into())));
    }

    #[test]
    fn unsigned_order_all_widths(a: u64, b: u64, c: u8, d: u8, e: u16, f: u16, g: u32, h: u32) {
        check_pair(a, b, a.cmp(&b), |v| enc(Value::UInt(v)));
        let arr = UInt64Array::from(vec![a, b]);
        prop_assert_eq!(enc_arr(&arr, 0).cmp(&enc_arr(&arr, 1)), a.cmp(&b));
        let arr = UInt8Array::from(vec![c, d]);
        prop_assert_eq!(enc_arr(&arr, 0).cmp(&enc_arr(&arr, 1)), c.cmp(&d));
        let arr = UInt16Array::from(vec![e, f]);
        prop_assert_eq!(enc_arr(&arr, 0).cmp(&enc_arr(&arr, 1)), e.cmp(&f));
        let arr = UInt32Array::from(vec![g, h]);
        prop_assert_eq!(enc_arr(&arr, 0).cmp(&enc_arr(&arr, 1)), g.cmp(&h));
        prop_assert_eq!(enc_arr(&arr, 0), enc(Value::UInt(g.into())));
        // the dataset type picks the tag: a UInt32 file under Int64 is signed
        prop_assert_eq!(enc_arr_as(&arr, 0, &DataType::Int64).unwrap(), enc(Value::Int(g.into())));
    }

    /// Integers convert across widths and signedness only within the dataset
    /// type's range; outside it they are errors.
    #[test]
    fn integers_are_range_checked_against_the_dataset_type(v: i64, u: u64) {
        let arr = Int64Array::from(vec![v]);
        for (dt, lo, hi) in [
            (DataType::Int8, i64::from(i8::MIN), i64::from(i8::MAX)),
            (DataType::Int16, i64::from(i16::MIN), i64::from(i16::MAX)),
            (DataType::Int32, i64::from(i32::MIN), i64::from(i32::MAX)),
            (DataType::UInt8, 0, i64::from(u8::MAX)),
            (DataType::UInt32, 0, i64::from(u32::MAX)),
            (DataType::UInt64, 0, i64::MAX),
        ] {
            prop_assert_eq!(enc_arr_as(&arr, 0, &dt).is_ok(), v >= lo && v <= hi, "{} into {}", v, dt);
        }
        let arr = UInt64Array::from(vec![u]);
        prop_assert_eq!(enc_arr_as(&arr, 0, &DataType::Int64).is_ok(), i64::try_from(u).is_ok());
    }

    #[test]
    fn float_total_order_nan_last(a in any_f64(), b in any_f64(), c: f32, d: f32) {
        let typed = canon_f64(a).total_cmp(&canon_f64(b));
        check_pair(a, b, typed, |v| enc(Value::Float(v)));
        let arr = Float64Array::from(vec![a, b]);
        prop_assert_eq!(enc_arr(&arr, 0).cmp(&enc_arr(&arr, 1)), typed);
        let arr = Float32Array::from(vec![c, d]);
        let typed32 = canon_f64(c.into()).total_cmp(&canon_f64(d.into()));
        prop_assert_eq!(enc_arr(&arr, 0).cmp(&enc_arr(&arr, 1)), typed32);
        if !a.is_nan() {
            prop_assert!(enc(Value::Float(a)) < enc(Value::Float(f64::NAN)));
        }
    }

    // Values span the full precision a Decimal128 can declare (38 digits).
    #[test]
    fn decimal128_order(a in -(10i128.pow(38) - 1)..10i128.pow(38), b in -(10i128.pow(38) - 1)..10i128.pow(38)) {
        let mk = |v| Value::Decimal128 { value: v, scale: 2 };
        check_pair(a, b, a.cmp(&b), |v| enc(mk(v)));
        let arr = Decimal128Array::from(vec![a, b]).with_precision_and_scale(38, 2).unwrap();
        prop_assert_eq!(enc_arr(&arr, 0), enc(mk(a)));
        prop_assert_eq!(enc_arr(&arr, 0).cmp(&enc_arr(&arr, 1)), a.cmp(&b));
    }

    // High halves bounded so values stay within 76 digits of precision.
    #[test]
    fn decimal256_order(ah in -10i128.pow(37)..10i128.pow(37), al: u128, bh in -10i128.pow(37)..10i128.pow(37), bl: u128) {
        let a = i256::from_parts(al, ah);
        let b = i256::from_parts(bl, bh);
        let mk = |v| Value::Decimal256 { value: v, scale: 0 };
        check_pair(a, b, a.cmp(&b), |v| enc(mk(v)));
        let arr = Decimal256Array::from(vec![a, b]);
        prop_assert_eq!(enc_arr(&arr, 0).cmp(&enc_arr(&arr, 1)), a.cmp(&b));
    }

    /// Two files at different scales and widths under one dataset type:
    /// equal values encode identically, order follows the true rational
    /// value, and the width comes from the dataset type.
    #[test]
    fn decimals_follow_the_dataset_scale_and_width(
        a in -1_000_000_000i128..1_000_000_000,
        b in -1_000_000_000i128..1_000_000_000,
        s1 in 0i8..4, s2 in 0i8..4, wide in any::<bool>(),
    ) {
        // dataset scale 4 holds every file scale 0..=3 exactly
        let dataset = if wide { DataType::Decimal256(40, 4) } else { DataType::Decimal128(20, 4) };
        let f1 = Decimal128Array::from(vec![a]).with_precision_and_scale(18, s1).unwrap();
        let f2: ArrayRef = if wide {
            Arc::new(Decimal256Array::from(vec![i256::from_i128(b)]).with_precision_and_scale(40, s2).unwrap())
        } else {
            Arc::new(Decimal128Array::from(vec![b]).with_precision_and_scale(18, s2).unwrap())
        };
        let e1 = enc_arr_as(&f1, 0, &dataset).unwrap();
        let e2 = enc_arr_as(f2.as_ref(), 0, &dataset).unwrap();
        // typed order: a / 10^s1 vs b / 10^s2
        let typed = (a * 10i128.pow(u32::try_from(4 - s1).unwrap()))
            .cmp(&(b * 10i128.pow(u32::try_from(4 - s2).unwrap())));
        prop_assert_eq!(e1.cmp(&e2), typed);
        prop_assert_eq!(e1.len(), if wide { 34 } else { 18 });
        prop_assert_eq!(e1.len(), e2.len());
        // the same value written at another scale is the same key
        let rescaled = Decimal128Array::from(vec![a * 10]).with_precision_and_scale(19, s1 + 1).unwrap();
        prop_assert_eq!(enc_arr_as(&rescaled, 0, &dataset).unwrap(), e1.clone());
        // and the same reconcile row-hash bytes
        let (mut h1, mut h2) = (Vec::new(), Vec::new());
        encode_nullable_as(&f1, 0, &dataset, &mut h1).unwrap();
        encode_nullable_as(&rescaled, 0, &dataset, &mut h2).unwrap();
        prop_assert_eq!(h1, h2);
    }

    #[test]
    fn decimals_that_do_not_fit_the_dataset_type_are_errors(
        a in 1i128..1_000_000,
        digits in 1u32..5,
    ) {
        let dataset = DataType::Decimal128(10, 2);
        // a value with a non-zero digit below the dataset scale
        let fine = Decimal128Array::from(vec![a * 10 + 1]).with_precision_and_scale(18, 3).unwrap();
        prop_assert!(enc_arr_as(&fine, 0, &dataset).is_err());
        // a value beyond the dataset precision
        let big = Decimal128Array::from(vec![10i128.pow(9 + digits) + a]).with_precision_and_scale(38, 0).unwrap();
        prop_assert!(enc_arr_as(&big, 0, &dataset).is_err());
    }

    #[test]
    fn utf8_order(a in nul_string(), b in nul_string()) {
        check_pair(a.as_str(), b.as_str(), a.cmp(&b), |v| enc(Value::Utf8(v)));
        let arr = StringArray::from(vec![a.as_str(), b.as_str()]);
        prop_assert_eq!(enc_arr(&arr, 0).cmp(&enc_arr(&arr, 1)), a.cmp(&b));
        let large = LargeStringArray::from(vec![a.as_str()]);
        prop_assert_eq!(enc_arr(&large, 0), enc_arr(&arr, 0));
        let view = StringViewArray::from(vec![a.as_str()]);
        prop_assert_eq!(enc_arr(&view, 0), enc_arr(&arr, 0));
    }

    #[test]
    fn binary_order(a in prop::collection::vec(prop_oneof![Just(0u8), Just(0xFF), Just(1), any::<u8>()], 0..8),
                    b in prop::collection::vec(prop_oneof![Just(0u8), Just(0xFF), Just(1), any::<u8>()], 0..8)) {
        check_pair(a.as_slice(), b.as_slice(), a.cmp(&b), |v| enc(Value::Binary(v)));
        let arr = BinaryArray::from(vec![a.as_slice(), b.as_slice()]);
        prop_assert_eq!(enc_arr(&arr, 0).cmp(&enc_arr(&arr, 1)), a.cmp(&b));
    }

    #[test]
    fn fixed_size_binary_order(a: [u8; 4], b: [u8; 4]) {
        let arr = FixedSizeBinaryArray::try_from_iter(vec![a, b].into_iter()).unwrap();
        prop_assert_eq!(enc_arr(&arr, 0).cmp(&enc_arr(&arr, 1)), a.cmp(&b));
        prop_assert_eq!(enc_arr(&arr, 0), enc(Value::Binary(&a)));
    }

    #[test]
    fn date_and_time_order(a: i32, b: i32, c: i64, d: i64, e in 0i32..86_400_000, f in 0i32..86_400_000,
                           g in 0i64..86_400_000_000_000, h in 0i64..86_400_000_000_000) {
        let arr = Date32Array::from(vec![a, b]);
        prop_assert_eq!(enc_arr(&arr, 0).cmp(&enc_arr(&arr, 1)), a.cmp(&b));
        prop_assert_eq!(enc_arr(&arr, 0), enc(Value::Date32(a)));
        let arr = Date64Array::from(vec![c, d]);
        prop_assert_eq!(enc_arr(&arr, 0).cmp(&enc_arr(&arr, 1)), c.cmp(&d));
        let arr = Time32MillisecondArray::from(vec![e, f]);
        prop_assert_eq!(enc_arr(&arr, 0).cmp(&enc_arr(&arr, 1)), e.cmp(&f));
        prop_assert_eq!(enc_arr(&arr, 0), enc(Value::Time { value: e.into(), unit: TimeUnit::Millisecond }));
        let arr = Time64NanosecondArray::from(vec![g, h]);
        prop_assert_eq!(enc_arr(&arr, 0).cmp(&enc_arr(&arr, 1)), g.cmp(&h));
        // a ms time under a ns dataset type converts exactly
        let ms = Time32MillisecondArray::from(vec![e]);
        let ns = DataType::Time64(TimeUnit::Nanosecond);
        prop_assert_eq!(enc_arr_as(&ms, 0, &ns).unwrap(),
                        enc(Value::Time { value: i64::from(e) * 1_000_000, unit: TimeUnit::Nanosecond }));
    }

    /// Within one column every i64 encodes in order with no overflow, the
    /// year-9999 sentinel included, because the unit is the dataset's.
    #[test]
    fn timestamps_order_within_a_column_without_overflow(
        a in prop_oneof![any::<i64>(), Just(253_402_300_799_999_999i64), Just(i64::MAX), Just(i64::MIN)],
        b: i64,
    ) {
        for (arr, unit) in [
            (Arc::new(TimestampMicrosecondArray::from(vec![a, b])) as ArrayRef, 0x03u8),
            (Arc::new(TimestampMillisecondArray::from(vec![a, b])) as ArrayRef, 0x02),
            (Arc::new(TimestampSecondArray::from(vec![a, b]).with_timezone("+05:00")) as ArrayRef, 0x01),
        ] {
            let (ea, eb) = (enc_arr(arr.as_ref(), 0), enc_arr(arr.as_ref(), 1));
            prop_assert_eq!(ea.cmp(&eb), a.cmp(&b));
            prop_assert_eq!(&ea[..3], &[0x08, 0x04, unit][..]);
        }
    }

    /// Files in other units convert to the dataset unit: finer by checked
    /// multiplication, coarser only when exact; order follows the instant.
    #[test]
    fn timestamps_convert_to_the_dataset_unit(
        ms in -9_000_000_000_000_000i64..9_000_000_000_000_000,
        us in any::<i64>(),
        ns in any::<i64>(),
    ) {
        let dataset = DataType::Timestamp(TimeUnit::Microsecond, None);
        let ms_arr = TimestampMillisecondArray::from(vec![ms]);
        let us_arr = TimestampMicrosecondArray::from(vec![us]);
        let ns_arr = TimestampNanosecondArray::from(vec![ns]);
        let e_us = enc_arr_as(&us_arr, 0, &dataset).unwrap();
        match enc_arr_as(&ms_arr, 0, &dataset) {
            Ok(e_ms) => {
                prop_assert_eq!(e_ms.cmp(&e_us), (i128::from(ms) * 1000).cmp(&i128::from(us)));
            }
            Err(_) => prop_assert!(ms.checked_mul(1000).is_none(), "only overflow may fail"),
        }
        if let Ok(e_ns) = enc_arr_as(&ns_arr, 0, &dataset) {
            prop_assert_eq!(ns % 1000, 0);
            prop_assert_eq!(e_ns.cmp(&e_us), i128::from(ns).cmp(&(i128::from(us) * 1000)));
        } else {
            prop_assert_ne!(ns % 1000, 0, "only an inexact value may fail");
        }
    }

    #[test]
    fn tuple_order_is_lexicographic(ai: i64, a_s in nul_string(), bi: i64, b_s in nul_string()) {
        let ea = encode_tuple(&[Value::Int(ai), Value::Utf8(&a_s)]).unwrap();
        let eb = encode_tuple(&[Value::Int(bi), Value::Utf8(&b_s)]).unwrap();
        prop_assert_eq!(ea.cmp(&eb), (ai, &a_s).cmp(&(bi, &b_s)));
        // string-leading tuples: the string terminator must delimit correctly
        let ea = encode_tuple(&[Value::Utf8(&a_s), Value::Int(ai)]).unwrap();
        let eb = encode_tuple(&[Value::Utf8(&b_s), Value::Int(bi)]).unwrap();
        prop_assert_eq!(ea.cmp(&eb), (&a_s, ai).cmp(&(&b_s, bi)));
    }

    #[test]
    fn prefix_sorts_before_every_extension(p in nul_string(), q: i64, r in nul_string(), other in nul_string()) {
        let prefix = encode_tuple(&[Value::Utf8(&p)]).unwrap();
        let mut ext = prefix.clone();
        encode_value(&Value::Int(q), &mut ext).unwrap();
        encode_value(&Value::Utf8(&r), &mut ext).unwrap();
        prop_assert!(prefix < ext);
        let (lo, hi) = prefix_bound(&prefix);
        prop_assert!(lo <= ext);
        if let Some(hi) = &hi {
            prop_assert!(&ext < hi);
        }
        // A tuple whose first element differs falls outside the bound.
        let mut not_ext = encode_tuple(&[Value::Utf8(&other)]).unwrap();
        encode_value(&Value::Int(q), &mut not_ext).unwrap();
        let inside = not_ext >= lo && hi.as_ref().map_or(true, |h| &not_ext < h);
        prop_assert_eq!(inside, other == p);
    }
}

#[test]
fn time64_overflow_is_an_error_not_a_panic() {
    let a = Time64MicrosecondArray::from(vec![i64::MAX / 10]);
    // under its own type nothing is multiplied
    assert!(enc_arr_as(&a, 0, a.data_type()).is_ok());
    // converting to nanoseconds overflows: an error, never a wrap or a panic
    assert!(enc_arr_as(&a, 0, &DataType::Time64(TimeUnit::Nanosecond)).is_err());
}

#[test]
fn key_columns_convert_skip_null_rows_and_decode_dictionaries() {
    let ids: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
    let dict: DictionaryArray<Int32Type> = vec![Some("a"), Some("b"), None].into_iter().collect();
    let names: ArrayRef = Arc::new(dict);
    let kc = KeyColumns::new(
        &[ids.clone(), names.clone()],
        &[DataType::Int64, DataType::Utf8],
    )
    .unwrap();
    assert_eq!(
        kc.encode(0).unwrap().unwrap(),
        encode_tuple_as(
            &[Value::Int(1), Value::Utf8("a")],
            &[DataType::Int64, DataType::Utf8]
        )
        .unwrap()
    );
    // ADR 014 as amended: a null leading column means the tuple is not
    // indexed; a null trailing column is encoded as the 0x00 marker
    assert!(kc.encode(1).unwrap().is_none());
    let mut trailing_null = encode_tuple_as(&[Value::Int(3)], &[DataType::Int64]).unwrap();
    trailing_null.push(0x00);
    assert_eq!(kc.encode(2).unwrap().unwrap(), trailing_null);
    // a column that cannot convert to its declared type is refused up front
    assert!(KeyColumns::new(&[ids, names], &[DataType::Utf8, DataType::Utf8]).is_err());
}
