//! Probe keys: validation against the dataset types, projection onto a key
//! spec's columns, signed-zero expansion, and timestamp encoding.
//!
//! A probe is a canonical tuple (ADR 002, ADR 016): the concatenation of its
//! elements' encodings, each self-delimiting. A probe encoded under the
//! wrong type (another time unit, another decimal width, a string for an
//! integer) would silently miss every key, so each element is checked
//! against its column's dataset type before any lookup, and a mismatch is an
//! error.

use std::ops::Range;

use arrow::datatypes::{DataType, TimeUnit};
use bellwether_core::encoding::{self, tag, temporal_kind, time_unit, width, Value};
use bellwether_types::{Error, Result};

/// The leading bytes every element of `dt` starts with, and the element's
/// total length when it is fixed (`None` for escaped variable-length
/// strings and binary).
fn element_shape(dt: &DataType) -> Result<(Vec<u8>, Option<usize>)> {
    let unit = |u: TimeUnit| match u {
        TimeUnit::Second => time_unit::SECOND,
        TimeUnit::Millisecond => time_unit::MILLISECOND,
        TimeUnit::Microsecond => time_unit::MICROSECOND,
        TimeUnit::Nanosecond => time_unit::NANOSECOND,
    };
    Ok(match dt {
        DataType::Boolean => (vec![tag::BOOLEAN], Some(2)),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            (vec![tag::INT], Some(9))
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            (vec![tag::UINT], Some(9))
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => (vec![tag::FLOAT], Some(9)),
        DataType::Decimal128(_, _) => (vec![tag::DECIMAL, width::DECIMAL128], Some(18)),
        DataType::Decimal256(_, _) => (vec![tag::DECIMAL, width::DECIMAL256], Some(34)),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => (vec![tag::UTF8], None),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => (vec![tag::BINARY], None),
        DataType::Date32 => (
            vec![tag::TEMPORAL, temporal_kind::DATE32, time_unit::DAY],
            Some(11),
        ),
        DataType::Date64 => (
            vec![tag::TEMPORAL, temporal_kind::DATE64, time_unit::MILLISECOND],
            Some(11),
        ),
        DataType::Time32(u) | DataType::Time64(u) => {
            (vec![tag::TEMPORAL, temporal_kind::TIME, unit(*u)], Some(11))
        }
        DataType::Timestamp(u, _) => (
            vec![tag::TEMPORAL, temporal_kind::TIMESTAMP, unit(*u)],
            Some(11),
        ),
        DataType::Dictionary(_, v) => return element_shape(v),
        other => return Err(Error::KeySpec(encoding::no_encoding(other))),
    })
}

/// Length of the escaped string or binary element starting at `start`
/// (the tag byte), through its `00 00` terminator.
fn escaped_len(key: &[u8], start: usize) -> Option<usize> {
    let mut j = start + 1;
    loop {
        if *key.get(j)? == 0x00 {
            match *key.get(j + 1)? {
                0x00 => return Some(j + 2 - start),
                0xFF => j += 2,
                _ => return None,
            }
        } else {
            j += 1;
        }
    }
}

/// Splits `key` into exactly `types.len()` elements, checking each against
/// its column's dataset type. A single `0x00` null marker is accepted in any
/// position (a probe containing a null matches nothing, ADR 014). Returns
/// the byte span of each element.
pub(crate) fn split_checked(key: &[u8], types: &[(String, DataType)]) -> Result<Vec<Range<usize>>> {
    let mut spans = Vec::with_capacity(types.len());
    let mut i = 0usize;
    for (column, dt) in types {
        let bad = |why: &str| {
            Error::KeySpec(format!(
                "probe element {} for column {column:?} ({dt}) is not a valid canonical encoding: {why}",
                spans.len()
            ))
        };
        let Some(&first) = key.get(i) else {
            return Err(bad("the probe ends early"));
        };
        let len = if first == tag::NULL {
            1
        } else {
            let (head, fixed) = element_shape(dt)?;
            if !key[i..].starts_with(&head) {
                return Err(bad("its type tag does not match the column's dataset type"));
            }
            match fixed {
                Some(n) => n,
                None => escaped_len(key, i).ok_or_else(|| bad("unterminated string or binary"))?,
            }
        };
        if i + len > key.len() {
            return Err(bad("the probe ends early"));
        }
        spans.push(i..i + len);
        i += len;
    }
    if i != key.len() {
        return Err(Error::KeySpec(format!(
            "probe has {} trailing bytes after its {} elements",
            key.len() - i,
            types.len()
        )));
    }
    Ok(spans)
}

/// The elements of `key` at `positions`, concatenated in that order.
pub(crate) fn project(key: &[u8], spans: &[Range<usize>], positions: &[usize]) -> Vec<u8> {
    let mut out = Vec::with_capacity(key.len());
    for &p in positions {
        out.extend_from_slice(&key[spans[p].clone()]);
    }
    out
}

/// The canonical encodings of +0.0 and -0.0.
pub(crate) fn zero_encodings() -> (Vec<u8>, Vec<u8>) {
    let enc = |v: f64| {
        let mut out = Vec::with_capacity(9);
        // Float64 encodes every f64; this cannot fail.
        let _ = encoding::encode_value_as(&Value::Float(v), &DataType::Float64, &mut out);
        out
    };
    (enc(0.0), enc(-0.0))
}

/// `key` and every variant obtained by replacing float zero elements with
/// the other zero. Canonical encoding keeps +0.0 and -0.0 apart, while an
/// equality probe for zero must match both (`docs/format.md`, statistics
/// semantics), so a probe with `z` float-zero elements becomes `2^z` probes.
pub(crate) fn zero_variants(key: &[u8]) -> Vec<Vec<u8>> {
    let (pos, neg) = zero_encodings();
    let mut out = vec![Vec::with_capacity(key.len())];
    let mut i = 0usize;
    while i < key.len() {
        let len = element_len(key, i).unwrap_or(key.len() - i);
        let elem = &key[i..i + len];
        let other = if elem == pos.as_slice() {
            Some(neg.as_slice())
        } else if elem == neg.as_slice() {
            Some(pos.as_slice())
        } else {
            None
        };
        match other {
            None => out.iter_mut().for_each(|v| v.extend_from_slice(elem)),
            Some(other) => {
                let mut doubled = Vec::with_capacity(out.len() * 2);
                for v in out {
                    let mut a = v.clone();
                    a.extend_from_slice(elem);
                    let mut b = v;
                    b.extend_from_slice(other);
                    doubled.push(a);
                    doubled.push(b);
                }
                out = doubled;
            }
        }
        i += len;
    }
    out
}

/// Length of the element at `i` from its tag alone, for already validated
/// keys.
fn element_len(key: &[u8], i: usize) -> Option<usize> {
    Some(match *key.get(i)? {
        tag::NULL => 1,
        tag::BOOLEAN => 2,
        tag::INT | tag::UINT | tag::FLOAT => 9,
        tag::DECIMAL => match *key.get(i + 1)? {
            width::DECIMAL128 => 18,
            width::DECIMAL256 => 34,
            _ => return None,
        },
        tag::UTF8 | tag::BINARY => escaped_len(key, i)?,
        tag::TEMPORAL => 11,
        _ => return None,
    })
}

/// Widens inclusive bounds for signed zero: a +0.0 lower bound becomes -0.0
/// and a -0.0 upper bound becomes +0.0, so a range that includes zero
/// includes both encodings of it.
pub(crate) fn widen_zero_bounds(lo: &[u8], hi: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let (pos, neg) = zero_encodings();
    let lo = if lo == pos.as_slice() {
        neg.clone()
    } else {
        lo.to_vec()
    };
    let hi = if hi == neg.as_slice() {
        pos
    } else {
        hi.to_vec()
    };
    (lo, hi)
}

/// The canonical encoding of a temporal ordering value `ts`, as core's
/// `ts_value_as` reports it, under the timestamp column's dataset type: a
/// timestamp or time in the column's unit, days for `Date32`, milliseconds
/// for `Date64`, or the integer itself for an integer column.
pub(crate) fn encode_ts(ts: i64, declared: &DataType) -> Result<Vec<u8>> {
    let value = match declared {
        DataType::Timestamp(unit, _) => Value::Timestamp {
            value: ts,
            unit: *unit,
        },
        DataType::Time32(unit) | DataType::Time64(unit) => Value::Time {
            value: ts,
            unit: *unit,
        },
        DataType::Date32 => Value::Date32(
            i32::try_from(ts)
                .map_err(|_| Error::KeySpec(format!("{ts} days is out of range for Date32")))?,
        ),
        DataType::Date64 => Value::Date64(ts),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Value::Int(ts),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => Value::UInt(
            u64::try_from(ts)
                .map_err(|_| Error::KeySpec(format!("{ts} is out of range for {declared}")))?,
        ),
        DataType::Dictionary(_, v) => return encode_ts(ts, v),
        other => {
            return Err(Error::KeySpec(format!(
                "ts_column of type {other} cannot order versions"
            )))
        }
    };
    let mut out = Vec::with_capacity(11);
    encoding::encode_value_as(&value, declared, &mut out)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, Date32Array, Date64Array, Int16Array, Int32Array, Int64Array,
        Time32MillisecondArray, Time64NanosecondArray, TimestampMillisecondArray,
        TimestampSecondArray, UInt32Array, UInt8Array,
    };

    fn col(name: &str, dt: DataType) -> (String, DataType) {
        (name.into(), dt)
    }

    fn enc(v: &Value<'_>, dt: &DataType) -> Vec<u8> {
        let mut out = Vec::new();
        encoding::encode_value_as(v, dt, &mut out).unwrap();
        out
    }

    #[test]
    fn split_accepts_well_typed_tuples_and_null_markers() {
        let types = [
            col("id", DataType::Int64),
            col("name", DataType::Utf8),
            col(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
            col("amount", DataType::Decimal128(10, 2)),
        ];
        let mut key = enc(&Value::Int(7), &DataType::Int64);
        key.extend(enc(&Value::Utf8("a\0b"), &DataType::Utf8));
        key.extend(enc(
            &Value::Timestamp {
                value: 5,
                unit: TimeUnit::Microsecond,
            },
            &types[2].1,
        ));
        key.extend(enc(
            &Value::Decimal128 {
                value: 150,
                scale: 2,
            },
            &types[3].1,
        ));
        let spans = split_checked(&key, &types).unwrap();
        assert_eq!(spans.len(), 4);
        assert_eq!(
            project(&key, &spans, &[1]),
            enc(&Value::Utf8("a\0b"), &DataType::Utf8)
        );
        assert_eq!(
            project(&key, &spans, &[3, 0]),
            [&key[spans[3].clone()], &key[spans[0].clone()]].concat()
        );
        // a trailing null marker
        let mut with_null = enc(&Value::Int(7), &DataType::Int64);
        with_null.push(tag::NULL);
        assert_eq!(split_checked(&with_null, &types[..2]).unwrap().len(), 2);
    }

    #[test]
    fn split_refuses_mistyped_short_and_long_probes() {
        let micros = DataType::Timestamp(TimeUnit::Microsecond, None);
        let millis = DataType::Timestamp(TimeUnit::Millisecond, None);
        let ms = enc(
            &Value::Timestamp {
                value: 1,
                unit: TimeUnit::Millisecond,
            },
            &millis,
        );
        // a millisecond timestamp probing a microsecond column
        assert!(split_checked(&ms, &[col("ts", micros)]).is_err());
        // a string probing an integer column
        let s = enc(&Value::Utf8("7"), &DataType::Utf8);
        assert!(split_checked(&s, &[col("id", DataType::Int64)]).is_err());
        // Decimal256 bytes for a Decimal128 column
        let d = enc(
            &Value::Decimal128 { value: 1, scale: 0 },
            &DataType::Decimal256(40, 0),
        );
        assert!(split_checked(&d, &[col("d", DataType::Decimal128(10, 0))]).is_err());
        // too few and too many elements
        let one = enc(&Value::Int(1), &DataType::Int64);
        assert!(split_checked(
            &one,
            &[col("a", DataType::Int64), col("b", DataType::Int64)]
        )
        .is_err());
        let two = [one.clone(), one.clone()].concat();
        assert!(split_checked(&two, &[col("a", DataType::Int64)]).is_err());
        // unterminated string
        assert!(split_checked(&[tag::UTF8, b'a'], &[col("s", DataType::Utf8)]).is_err());
    }

    #[test]
    fn zero_variants_cover_both_zeros_in_every_position() {
        let (pos, neg) = zero_encodings();
        assert_ne!(pos, neg);
        let one = enc(&Value::Float(1.0), &DataType::Float64);
        assert_eq!(zero_variants(&one), vec![one.clone()]);
        assert_eq!(zero_variants(&pos), vec![pos.clone(), neg.clone()]);
        let key = [pos.clone(), one.clone(), neg.clone()].concat();
        let mut v = zero_variants(&key);
        v.sort();
        let mut want = vec![
            [pos.clone(), one.clone(), neg.clone()].concat(),
            [pos.clone(), one.clone(), pos.clone()].concat(),
            [neg.clone(), one.clone(), neg.clone()].concat(),
            [neg.clone(), one.clone(), pos.clone()].concat(),
        ];
        want.sort();
        assert_eq!(v, want);
        // an Int64 0 is not a float zero
        let int0 = enc(&Value::Int(0), &DataType::Int64);
        assert_eq!(zero_variants(&int0), vec![int0]);
    }

    #[test]
    fn zero_bounds_widen_outward_only() {
        let (pos, neg) = zero_encodings();
        let one = enc(&Value::Float(1.0), &DataType::Float64);
        let m1 = enc(&Value::Float(-1.0), &DataType::Float64);
        assert_eq!(widen_zero_bounds(&pos, &one), (neg.clone(), one.clone()));
        assert_eq!(widen_zero_bounds(&m1, &neg), (m1.clone(), pos.clone()));
        assert_eq!(widen_zero_bounds(&neg, &pos), (neg.clone(), pos.clone()));
        assert_eq!(widen_zero_bounds(&m1, &one), (m1, one));
    }

    /// `encode_ts` must produce exactly the bytes the stored column value
    /// encodes to, for every type a temporal spec may order by.
    #[test]
    fn ts_encoding_matches_the_column_encoding_for_every_ts_type() {
        let cases: Vec<(ArrayRef, i64)> = vec![
            (
                Arc::new(TimestampMillisecondArray::from(vec![-5]).with_timezone("UTC")),
                -5,
            ),
            (
                Arc::new(TimestampSecondArray::from(vec![1_700_000_000])),
                1_700_000_000,
            ),
            (Arc::new(Date32Array::from(vec![19_723])), 19_723),
            (
                Arc::new(Date64Array::from(vec![1_704_067_200_000])),
                1_704_067_200_000,
            ),
            (
                Arc::new(Time32MillisecondArray::from(vec![3_600_000])),
                3_600_000,
            ),
            (Arc::new(Time64NanosecondArray::from(vec![7])), 7),
            (Arc::new(Int16Array::from(vec![-3])), -3),
            (Arc::new(Int32Array::from(vec![12])), 12),
            (Arc::new(Int64Array::from(vec![i64::MIN])), i64::MIN),
            (Arc::new(UInt8Array::from(vec![200])), 200),
            (
                Arc::new(UInt32Array::from(vec![4_000_000_000])),
                4_000_000_000,
            ),
        ];
        for (array, want_ts) in cases {
            let dt = array.data_type().clone();
            let ts = encoding::ts_value_as(array.as_ref(), 0, &dt)
                .unwrap()
                .unwrap();
            assert_eq!(ts, want_ts, "{dt}");
            let mut stored = Vec::new();
            assert!(encoding::encode_array_value_as(array.as_ref(), 0, &dt, &mut stored).unwrap());
            assert_eq!(encode_ts(ts, &dt).unwrap(), stored, "{dt}");
        }
        assert!(encode_ts(-1, &DataType::UInt64).is_err());
        assert!(encode_ts(i64::MAX, &DataType::Date32).is_err());
        assert!(encode_ts(1, &DataType::Utf8).is_err());
    }
}
