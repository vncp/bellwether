//! `encoding::decode` (WP16): the canonical bytes of a value decode to the
//! typed value at the dataset type, and re-encode to the same bytes, for
//! every type with an encoding; bytes the encoder could not have written
//! under that type are refused, never guessed.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use arrow::datatypes::{i256, DataType, TimeUnit};
use bellwether_core::encoding::{decode, decode_tuple, encode_value_as, Scalar, Value};
use bellwether_core::segment::ColumnType;
use proptest::prelude::*;

fn enc(v: &Value<'_>, dt: &DataType) -> Vec<u8> {
    let mut out = Vec::new();
    encode_value_as(v, dt, &mut out).unwrap();
    out
}

/// Every dataset type with an encoding, each with a strategy for values
/// the encoder accepts under it (given in another unit, scale, or width
/// where the type converts them).
fn typed_value() -> impl Strategy<Value = (DataType, Value<'static>)> {
    let units = prop_oneof![
        Just(TimeUnit::Second),
        Just(TimeUnit::Millisecond),
        Just(TimeUnit::Microsecond),
        Just(TimeUnit::Nanosecond)
    ];
    let strings: &'static [&'static str] = &["", "a", "a\0b", "ünï", "\0\0", "x\u{ff}"];
    let blobs: &'static [&'static [u8]] = &[b"", b"\0", b"ab\0\xff", b"\xff\xff"];
    prop_oneof![
        any::<bool>().prop_map(|b| (DataType::Boolean, Value::Boolean(b))),
        any::<i8>().prop_map(|v| (DataType::Int8, Value::Int(v.into()))),
        any::<i16>().prop_map(|v| (DataType::Int16, Value::Int(v.into()))),
        any::<i32>().prop_map(|v| (DataType::Int32, Value::Int(v.into()))),
        any::<i64>().prop_map(|v| (DataType::Int64, Value::Int(v))),
        any::<u8>().prop_map(|v| (DataType::UInt8, Value::UInt(v.into()))),
        any::<u32>().prop_map(|v| (DataType::UInt32, Value::UInt(v.into()))),
        any::<u64>().prop_map(|v| (DataType::UInt64, Value::UInt(v))),
        any::<f32>().prop_map(|v| (DataType::Float32, Value::Float(v.into()))),
        any::<f64>().prop_map(|v| (DataType::Float64, Value::Float(v))),
        prop_oneof![
            Just(0.0f64),
            Just(-0.0),
            Just(f64::NAN),
            Just(f64::INFINITY)
        ]
        .prop_map(|v| (DataType::Float64, Value::Float(v))),
        // A decimal given at another scale is rescaled to the dataset's.
        (-10i128.pow(15)..10i128.pow(15), 0i8..4).prop_map(|(v, s)| (
            DataType::Decimal128(38, 6),
            Value::Decimal128 { value: v, scale: s }
        )),
        (-10i128.pow(15)..10i128.pow(15)).prop_map(|v| (
            DataType::Decimal128(18, 2),
            Value::Decimal128 { value: v, scale: 2 }
        )),
        any::<i64>().prop_map(|v| (
            DataType::Decimal256(76, 10),
            Value::Decimal256 {
                value: i256::from_i128(v.into()),
                scale: 10
            }
        )),
        (0..strings.len()).prop_map(move |i| (DataType::Utf8, Value::Utf8(strings[i]))),
        (0..strings.len()).prop_map(move |i| (DataType::Utf8View, Value::Utf8(strings[i]))),
        (0..blobs.len()).prop_map(move |i| (DataType::Binary, Value::Binary(blobs[i]))),
        Just((DataType::FixedSizeBinary(2), Value::Binary(b"\0z"))),
        any::<i32>().prop_map(|v| (DataType::Date32, Value::Date32(v))),
        any::<i32>().prop_map(|v| (DataType::Date64, Value::Date32(v))),
        (0i64..86_400).prop_map(|v| (
            DataType::Time32(TimeUnit::Second),
            Value::Time {
                value: v,
                unit: TimeUnit::Second
            }
        )),
        (0i64..86_400_000_000).prop_map(|v| (
            DataType::Time64(TimeUnit::Microsecond),
            Value::Time {
                value: v,
                unit: TimeUnit::Microsecond
            }
        )),
        // A timestamp given in a coarser unit converts to the dataset's.
        (units.clone(), -10i64.pow(9)..10i64.pow(9), units).prop_filter_map(
            "the conversion to the dataset unit must be exact",
            |(dataset, v, given)| {
                let dt = DataType::Timestamp(dataset, Some("UTC".into()));
                let val = Value::Timestamp {
                    value: v,
                    unit: given,
                };
                let mut out = Vec::new();
                encode_value_as(&val, &dt, &mut out).ok()?;
                Some((dt, val))
            }
        ),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

    /// decode(encode(v)) re-encodes to the same bytes, at the dataset
    /// scale and unit, and never at the value's own.
    #[test]
    fn decode_round_trips_the_encoder((dt, v) in typed_value()) {
        let bytes = enc(&v, &dt);
        let ct = ColumnType::of(&dt).unwrap();
        let s = decode(&bytes, &ct).unwrap();
        prop_assert_eq!(enc(&s.as_value(), &dt), bytes.clone());
        match (&dt, &s) {
            (DataType::Decimal128(_, scale), Scalar::Decimal128 { scale: got, .. })
            | (DataType::Decimal256(_, scale), Scalar::Decimal256 { scale: got, .. }) => {
                prop_assert_eq!(scale, got);
            }
            (DataType::Timestamp(unit, _), Scalar::Timestamp { unit: got, .. }) => {
                prop_assert_eq!(unit, got);
            }
            _ => {}
        }
        // Every strict prefix and every extension is refused, never a panic.
        for cut in 0..bytes.len() {
            prop_assert!(decode(&bytes[..cut], &ct).is_err());
        }
        let mut longer = bytes.clone();
        longer.push(0);
        prop_assert!(decode(&longer, &ct).is_err());
    }

    /// Arbitrary bytes under any type: a typed refusal or a value that
    /// re-encodes to exactly those bytes; never a panic.
    #[test]
    fn arbitrary_bytes_decode_exactly_or_not_at_all(
        (dt, _) in typed_value(),
        bytes in proptest::collection::vec(any::<u8>(), 0..40),
    ) {
        let ct = ColumnType::of(&dt).unwrap();
        if let Ok(s) = decode(&bytes, &ct) {
            prop_assert_eq!(enc(&s.as_value(), &dt), bytes);
        }
    }
}

#[test]
fn a_decimal_decodes_at_the_dataset_scale_not_the_value_scale() {
    let dt = DataType::Decimal128(10, 4);
    let bytes = enc(
        &Value::Decimal128 {
            value: 125,
            scale: 2,
        },
        &dt,
    );
    let s = decode(&bytes, &ColumnType::of(&dt).unwrap()).unwrap();
    assert_eq!(
        s,
        Scalar::Decimal128 {
            value: 12_500,
            scale: 4
        }
    );
}

#[test]
fn bytes_of_another_type_width_or_unit_are_refused() {
    let ms = DataType::Timestamp(TimeUnit::Millisecond, None);
    let us = DataType::Timestamp(TimeUnit::Microsecond, None);
    let b = enc(
        &Value::Timestamp {
            value: 7,
            unit: TimeUnit::Millisecond,
        },
        &ms,
    );
    let e = decode(&b, &ColumnType::of(&us).unwrap()).unwrap_err();
    assert!(e.to_string().contains("another unit"), "{e}");
    let d128 = enc(
        &Value::Decimal128 { value: 1, scale: 0 },
        &DataType::Decimal128(5, 0),
    );
    assert!(decode(&d128, &ColumnType::of(&DataType::Decimal256(5, 0)).unwrap()).is_err());
    // An Int64 encoding of a value out of an Int8 dataset's range.
    let big = enc(&Value::Int(300), &DataType::Int64);
    assert!(decode(&big, &ColumnType::of(&DataType::Int8).unwrap()).is_err());
    // A Float64 value not exact at Float32.
    let f = enc(&Value::Float(0.1), &DataType::Float64);
    assert!(decode(&f, &ColumnType::of(&DataType::Float32).unwrap()).is_err());
    // A decimal beyond its precision.
    let wide = enc(
        &Value::Decimal128 {
            value: 10_000,
            scale: 0,
        },
        &DataType::Decimal128(10, 0),
    );
    assert!(decode(&wide, &ColumnType::of(&DataType::Decimal128(4, 0)).unwrap()).is_err());
    // The null marker is not a value; a tuple reads it as a null.
    assert!(decode(&[0x00], &ColumnType::of(&DataType::Int64).unwrap()).is_err());
}

#[test]
fn a_tuple_decodes_each_element_and_reads_null_markers() {
    let types = [
        ColumnType::of(&DataType::Utf8).unwrap(),
        ColumnType::of(&DataType::Int32).unwrap(),
        ColumnType::of(&DataType::Float64).unwrap(),
    ];
    let mut bytes = enc(&Value::Utf8("k\0"), &DataType::Utf8);
    bytes.push(0x00);
    bytes.extend(enc(&Value::Float(-0.0), &DataType::Float64));
    let got = decode_tuple(&bytes, &types).unwrap();
    assert_eq!(got[0], Some(Scalar::Utf8("k\0".into())));
    assert_eq!(got[1], None);
    assert!(matches!(got[2], Some(Scalar::Float(f)) if f == 0.0 && f.is_sign_negative()));
    assert!(decode_tuple(&bytes[..bytes.len() - 1], &types).is_err());
    let mut extra = bytes.clone();
    extra.push(0x01);
    assert!(decode_tuple(&extra, &types).is_err());
}
