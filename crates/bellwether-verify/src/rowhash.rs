//! The reconcile row hash (`docs/format.md`, ADR 016), and key tuples.
//!
//! A row's hash is xxh3-128 (default secret, seed 0) over, for each hashed
//! column in the shared order (the left dataset schema's order minus
//! ignored columns):
//!
//! 1. the column name's length in bytes, as a 4-byte big-endian `u32`;
//! 2. the column name's UTF-8 bytes;
//! 3. the value's canonical encoding at the side's dataset type, or the
//!    single `0x00` null marker. Decimals are rescaled to the dataset scale
//!    and timestamps converted to the dataset unit, exactly or not at all.
//!
//! `docs/format.md` does not fix the width or byte order of the length
//! prefix; this crate fixes it as above, and `tests/row_hash.rs` pins it
//! with a golden vector.

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::DataType;
use bellwether_core::encoding;
use xxhash_rust::xxh3::xxh3_128;

use crate::error::{Result, VerifyError};

/// Appends one column's contribution to a row-hash input: the name's length
/// as a big-endian `u32`, the name, then the canonical encoding of
/// `array[row]` at `dataset_type` (or `0x00` for null).
pub fn push_hashed_column(
    out: &mut Vec<u8>,
    name: &str,
    array: &dyn Array,
    row: usize,
    dataset_type: &DataType,
) -> bellwether_types::Result<()> {
    let len = u32::try_from(name.len()).map_err(|_| {
        bellwether_types::Error::Other(format!("column name of {} bytes", name.len()))
    })?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(name.as_bytes());
    encoding::encode_nullable_as(array, row, dataset_type, out)
}

/// The row hash of `batch[row]` over `columns` (name and dataset type, in
/// hash order). The batch must hold every named column.
pub fn row_hash(
    batch: &RecordBatch,
    row: usize,
    columns: &[(String, DataType)],
) -> bellwether_types::Result<u128> {
    let mut buf = Vec::new();
    for (name, dt) in columns {
        let array = batch.column_by_name(name).ok_or_else(|| {
            bellwether_types::Error::KeySpec(format!("column {name:?} not in the batch"))
        })?;
        push_hashed_column(&mut buf, name, array.as_ref(), row, dt)?;
    }
    Ok(xxh3_128(&buf))
}

/// Appends the key tuple of `row`: the concatenation of each key column's
/// canonical encoding at its dataset type, with `0x00` for a null element.
///
/// Unlike an index key (ADR 014), a reconcile key keeps rows whose key has
/// nulls: two rows whose keys are both null in the same positions compare
/// as the same key, so no row is silently left out of the comparison.
/// (The reconciler encodes per column to locate errors; this is the same
/// encoding, for tests.)
#[cfg(test)]
pub(crate) fn push_key(
    out: &mut Vec<u8>,
    columns: &[(&dyn Array, &DataType)],
    row: usize,
) -> bellwether_types::Result<()> {
    for (array, dt) in columns {
        encoding::encode_nullable_as(*array, row, dt, out)?;
    }
    Ok(())
}

/// Checks that `key` is a well-formed canonical tuple of `types`, with one
/// element per key column, each of the right family, and no null element.
/// Decimal scale is not visible in the bytes, so callers must encode at the
/// dataset types.
pub(crate) fn check_probe(key: &[u8], types: &[DataType]) -> Result<()> {
    use encoding::{tag, temporal_kind, time_unit, width};
    let bad = |why: String| {
        VerifyError::InvalidSpec(format!(
            "workload key {} is not a canonical key tuple at the dataset types: {why}",
            hex(key)
        ))
    };
    match encoding::tuple_has_null(key) {
        None => return Err(bad("malformed".into())),
        Some(true) => {
            return Err(bad(
                "it contains a null, and a probe containing a null matches nothing (ADR 014)"
                    .into(),
            ))
        }
        Some(false) => {}
    }
    let mut i = 0usize;
    for dt in types {
        let dt = match dt {
            DataType::Dictionary(_, v) => v.as_ref(),
            other => other,
        };
        if i >= key.len() {
            return Err(bad(format!(
                "{} key columns but fewer elements",
                types.len()
            )));
        }
        let (want, len): (&[u8], usize) = match dt {
            DataType::Boolean => (&[tag::BOOLEAN], 2),
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                (&[tag::INT], 9)
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                (&[tag::UINT], 9)
            }
            DataType::Float16 | DataType::Float32 | DataType::Float64 => (&[tag::FLOAT], 9),
            DataType::Decimal128(..) => (&[tag::DECIMAL, width::DECIMAL128], 18),
            DataType::Decimal256(..) => (&[tag::DECIMAL, width::DECIMAL256], 34),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => (
                &[tag::UTF8],
                escaped_len(key, i).ok_or_else(|| bad("bad string".into()))?,
            ),
            DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_) => (
                &[tag::BINARY],
                escaped_len(key, i).ok_or_else(|| bad("bad binary".into()))?,
            ),
            DataType::Date32 => (&[tag::TEMPORAL, temporal_kind::DATE32, time_unit::DAY], 11),
            DataType::Date64 => (
                &[tag::TEMPORAL, temporal_kind::DATE64, time_unit::MILLISECOND],
                11,
            ),
            DataType::Time32(u) | DataType::Time64(u) => {
                (&[tag::TEMPORAL, temporal_kind::TIME, unit_tag(*u)], 11)
            }
            DataType::Timestamp(u, _) => {
                (&[tag::TEMPORAL, temporal_kind::TIMESTAMP, unit_tag(*u)], 11)
            }
            other => return Err(bad(format!("type {other} cannot be a key"))),
        };
        let head = key.get(i..i + want.len()).unwrap_or_default();
        if head != want || i + len > key.len() {
            return Err(bad(format!(
                "element {} does not encode a {dt} value",
                hex(&key[i..])
            )));
        }
        i += len;
    }
    if i != key.len() {
        return Err(bad(format!(
            "{} trailing bytes after {} elements",
            key.len() - i,
            types.len()
        )));
    }
    Ok(())
}

fn unit_tag(u: arrow::datatypes::TimeUnit) -> u8 {
    use arrow::datatypes::TimeUnit;
    use encoding::time_unit;
    match u {
        TimeUnit::Second => time_unit::SECOND,
        TimeUnit::Millisecond => time_unit::MILLISECOND,
        TimeUnit::Microsecond => time_unit::MICROSECOND,
        TimeUnit::Nanosecond => time_unit::NANOSECOND,
    }
}

/// Length of the escaped string or binary element starting at `i` (its tag
/// included), or `None` if it is not terminated.
fn escaped_len(key: &[u8], i: usize) -> Option<usize> {
    let mut j = i + 1;
    loop {
        if *key.get(j)? == 0x00 {
            match *key.get(j + 1)? {
                0x00 => return Some(j + 2 - i),
                0xFF => j += 2,
                _ => return None,
            }
        } else {
            j += 1;
        }
    }
}

/// Lowercase hex.
pub(crate) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use std::sync::Arc;

    #[test]
    fn probe_check_accepts_well_formed_and_refuses_the_rest() {
        let types = [DataType::Int64, DataType::Utf8];
        let mut k = Vec::new();
        let ids = Int64Array::from(vec![7]);
        let names = StringArray::from(vec!["a\0b"]);
        push_key(
            &mut k,
            &[(&ids, &DataType::Int64), (&names, &DataType::Utf8)],
            0,
        )
        .unwrap();
        check_probe(&k, &types).unwrap();
        // Truncated, trailing bytes, wrong family, null element.
        assert!(check_probe(&k[..k.len() - 1], &types).is_err());
        let mut long = k.clone();
        long.push(0x01);
        assert!(check_probe(&long, &types).is_err());
        assert!(check_probe(&k, &[DataType::UInt64, DataType::Utf8]).is_err());
        let nulls = StringArray::from(vec![None::<&str>]);
        let mut n = Vec::new();
        push_key(
            &mut n,
            &[(&ids, &DataType::Int64), (&nulls, &DataType::Utf8)],
            0,
        )
        .unwrap();
        assert!(check_probe(&n, &types).is_err());
    }

    #[test]
    fn row_hash_reads_columns_by_name() {
        let batch = RecordBatch::try_from_iter([
            ("a", Arc::new(Int64Array::from(vec![1])) as _),
            ("b", Arc::new(StringArray::from(vec!["x"])) as _),
        ])
        .unwrap();
        let ab = row_hash(
            &batch,
            0,
            &[("a".into(), DataType::Int64), ("b".into(), DataType::Utf8)],
        )
        .unwrap();
        let ba = row_hash(
            &batch,
            0,
            &[("b".into(), DataType::Utf8), ("a".into(), DataType::Int64)],
        )
        .unwrap();
        assert_ne!(ab, ba, "column order is part of the hash");
        assert!(row_hash(&batch, 0, &[("c".into(), DataType::Int64)]).is_err());
    }
}
