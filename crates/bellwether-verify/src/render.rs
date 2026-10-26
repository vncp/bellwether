//! Human-readable renderings of canonical key and group tuples.
//!
//! Identity is always the canonical bytes; this module only decodes them for
//! display, building a one-value Arrow array of the dataset type and
//! formatting it with Arrow's own display rules, so a key reads the way the
//! same value reads in any Arrow-based tool.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    Decimal256Array, Float64Array, Int64Array, StringArray, Time32MillisecondArray,
    Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt64Array,
};
use arrow::datatypes::{i256, DataType, TimeUnit};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use bellwether_core::encoding::tag;

/// Decodes a canonical tuple of `types` into one display string per element
/// (`None` for a null marker). Returns `None` if the bytes do not decode as
/// that tuple, which never happens for tuples this crate encoded.
pub fn render_tuple(bytes: &[u8], types: &[DataType]) -> Option<Vec<Option<String>>> {
    let mut out = Vec::with_capacity(types.len());
    let mut i = 0usize;
    for dt in types {
        let dt = match dt {
            DataType::Dictionary(_, v) => v.as_ref(),
            other => other,
        };
        if *bytes.get(i)? == tag::NULL {
            out.push(None);
            i += 1;
            continue;
        }
        let (array, len) = decode(bytes, i, dt)?;
        let formatter = ArrayFormatter::try_new(array.as_ref(), &FormatOptions::default()).ok()?;
        out.push(Some(formatter.value(0).to_string()));
        i += len;
    }
    (i == bytes.len()).then_some(out)
}

fn be_u64(bytes: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_be_bytes(bytes.get(at..at + 8)?.try_into().ok()?))
}

fn flipped_i64(bytes: &[u8], at: usize) -> Option<i64> {
    // Big-endian two's complement with the sign bit flipped.
    #[allow(clippy::cast_possible_wrap)]
    Some((be_u64(bytes, at)? ^ (1 << 63)) as i64)
}

fn unescape(bytes: &[u8], at: usize) -> Option<(Vec<u8>, usize)> {
    let mut out = Vec::new();
    let mut j = at;
    loop {
        let b = *bytes.get(j)?;
        if b == 0x00 {
            match *bytes.get(j + 1)? {
                0x00 => return Some((out, j + 2 - at)),
                0xFF => {
                    out.push(0x00);
                    j += 2;
                }
                _ => return None,
            }
        } else {
            out.push(b);
            j += 1;
        }
    }
}

#[allow(clippy::too_many_lines)]
fn decode(bytes: &[u8], at: usize, dt: &DataType) -> Option<(ArrayRef, usize)> {
    let head = *bytes.get(at)?;
    Some(match dt {
        DataType::Boolean if head == tag::BOOLEAN => (
            Arc::new(BooleanArray::from(vec![*bytes.get(at + 1)? == 1])) as ArrayRef,
            2,
        ),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            if head == tag::INT =>
        {
            (
                Arc::new(Int64Array::from(vec![flipped_i64(bytes, at + 1)?])),
                9,
            )
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
            if head == tag::UINT =>
        {
            (Arc::new(UInt64Array::from(vec![be_u64(bytes, at + 1)?])), 9)
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 if head == tag::FLOAT => {
            let bits = be_u64(bytes, at + 1)?;
            let raw = if bits >> 63 == 1 {
                bits ^ (1 << 63)
            } else {
                !bits
            };
            (Arc::new(Float64Array::from(vec![f64::from_bits(raw)])), 9)
        }
        DataType::Decimal128(precision, scale) if head == tag::DECIMAL => {
            let mut raw: [u8; 16] = bytes.get(at + 2..at + 18)?.try_into().ok()?;
            raw[0] ^= 0x80;
            let array = Decimal128Array::from(vec![i128::from_be_bytes(raw)])
                .with_precision_and_scale(*precision, *scale)
                .ok()?;
            (Arc::new(array), 18)
        }
        DataType::Decimal256(precision, scale) if head == tag::DECIMAL => {
            let mut raw: [u8; 32] = bytes.get(at + 2..at + 34)?.try_into().ok()?;
            raw[0] ^= 0x80;
            let array = Decimal256Array::from(vec![i256::from_be_bytes(raw)])
                .with_precision_and_scale(*precision, *scale)
                .ok()?;
            (Arc::new(array), 34)
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View if head == tag::UTF8 => {
            let (raw, len) = unescape(bytes, at + 1)?;
            let text = String::from_utf8(raw).ok()?;
            (Arc::new(StringArray::from(vec![text])), len + 1)
        }
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_)
            if head == tag::BINARY =>
        {
            let (raw, len) = unescape(bytes, at + 1)?;
            (Arc::new(BinaryArray::from_vec(vec![&raw[..]])), len + 1)
        }
        DataType::Date32 if head == tag::TEMPORAL => {
            let days = i32::try_from(flipped_i64(bytes, at + 3)?).ok()?;
            (Arc::new(Date32Array::from(vec![days])), 11)
        }
        DataType::Date64 if head == tag::TEMPORAL => (
            Arc::new(Date64Array::from(vec![flipped_i64(bytes, at + 3)?])),
            11,
        ),
        DataType::Time32(unit) | DataType::Time64(unit) if head == tag::TEMPORAL => {
            let value = flipped_i64(bytes, at + 3)?;
            let array: ArrayRef = match unit {
                TimeUnit::Second => {
                    Arc::new(Time32SecondArray::from(vec![i32::try_from(value).ok()?]))
                }
                TimeUnit::Millisecond => Arc::new(Time32MillisecondArray::from(vec![
                    i32::try_from(value).ok()?,
                ])),
                TimeUnit::Microsecond => Arc::new(Time64MicrosecondArray::from(vec![value])),
                TimeUnit::Nanosecond => Arc::new(Time64NanosecondArray::from(vec![value])),
            };
            (array, 11)
        }
        DataType::Timestamp(unit, _) if head == tag::TEMPORAL => {
            // The canonical value is a UTC instant and the zone is not part
            // of the encoding, so it renders at offset +00:00 (named zones
            // would also need Arrow's chrono-tz feature to format).
            let value = flipped_i64(bytes, at + 3)?;
            let zone: Option<Arc<str>> = Some("+00:00".into());
            let array: ArrayRef = match unit {
                TimeUnit::Second => {
                    Arc::new(TimestampSecondArray::from(vec![value]).with_timezone_opt(zone))
                }
                TimeUnit::Millisecond => {
                    Arc::new(TimestampMillisecondArray::from(vec![value]).with_timezone_opt(zone))
                }
                TimeUnit::Microsecond => {
                    Arc::new(TimestampMicrosecondArray::from(vec![value]).with_timezone_opt(zone))
                }
                TimeUnit::Nanosecond => {
                    Arc::new(TimestampNanosecondArray::from(vec![value]).with_timezone_opt(zone))
                }
            };
            (array, 11)
        }
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rowhash::push_key;
    use arrow::array::Array;

    fn roundtrip(array: &dyn Array, dt: &DataType) -> Option<String> {
        let mut key = Vec::new();
        push_key(&mut key, &[(array, dt)], 0).unwrap();
        render_tuple(&key, std::slice::from_ref(dt))
            .unwrap()
            .pop()
            .unwrap()
    }

    #[test]
    fn renders_decoded_values_like_arrow_displays_them() {
        let ts = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let stamp =
            TimestampMicrosecondArray::from(vec![1_704_067_200_000_001]).with_timezone("UTC");
        assert_eq!(
            roundtrip(&stamp, &ts).unwrap(),
            "2024-01-01T00:00:00.000001Z"
        );
        let money = Decimal128Array::from(vec![-1230])
            .with_precision_and_scale(10, 2)
            .unwrap();
        assert_eq!(
            roundtrip(&money, &DataType::Decimal128(10, 2)).unwrap(),
            "-12.30"
        );
        let text = StringArray::from(vec!["a\0b"]);
        assert_eq!(roundtrip(&text, &DataType::Utf8).unwrap(), "a\0b");
        let null = Int64Array::from(vec![None]);
        assert_eq!(roundtrip(&null, &DataType::Int64), None);
        let zero = Float64Array::from(vec![-0.0]);
        assert_eq!(roundtrip(&zero, &DataType::Float64).unwrap(), "-0.0");
        let day = Date32Array::from(vec![19_723]);
        assert_eq!(roundtrip(&day, &DataType::Date32).unwrap(), "2024-01-01");
    }

    #[test]
    fn malformed_tuples_do_not_render() {
        assert!(render_tuple(&[0x02, 0x80], &[DataType::Int64]).is_none());
        assert!(render_tuple(&[0x06, b'a', 0x00, 0x00, 0x01], &[DataType::Utf8]).is_none());
    }
}
