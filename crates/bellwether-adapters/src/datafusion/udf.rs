//! Scalar functions that evaluate the residual tests SQL has no exact
//! spelling for with Bellwether's own definitions (`bellwether_core`):
//! text matches (full case folding, UAX #29 words), expression values
//! (the expression language, bit for bit), and list membership over
//! canonical element encodings. Each returns `false` for a null, so a
//! filter drops the row (ADR 014). One more maps a float to its canonical
//! order key (the 8 bytes of its canonical encoding as a `UInt64`), so
//! equality, grouping, joins, and ranges compare floats exactly as
//! canonical bytes do: `-0.0` apart from `+0.0` (DataFusion's hash join
//! and `=` compare them equal), every NaN one value, ordered last.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Array, AsArray, BooleanArray, BooleanBuilder, RecordBatch, UInt64Array};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, Float16Type, Float32Type, Float64Type, Schema,
};
use bellwether_core::encoding::KeyColumns;
use bellwether_core::expr::Typed;
use bellwether_core::list::ListElements;
use bellwether_types::TextOp;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{create_udf, ColumnarValue, ScalarUDF, Volatility};

/// Arrow's half-precision float.
pub(crate) type F16 = <Float16Type as ArrowPrimitiveType>::Native;

fn df(e: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(e.to_string())
}

fn bools(v: Vec<bool>) -> ColumnarValue {
    ColumnarValue::Array(Arc::new(BooleanArray::from(v)))
}

/// The canonical order key of `f` (widened to `f64`): the big-endian
/// integer of the 8 bytes after the tag of its canonical encoding.
#[must_use]
pub(crate) fn float_order(f: f64) -> u64 {
    let bits = if f.is_nan() {
        0x7FF8_0000_0000_0000
    } else {
        f.to_bits()
    };
    if bits >> 63 == 1 {
        !bits
    } else {
        bits | (1 << 63)
    }
}

/// Maps a float column of type `dt` to its canonical order key
/// ([`float_order`]); a null stays null.
pub(crate) fn canonical_order(dt: &DataType) -> ScalarUDF {
    let t = dt.clone();
    create_udf(
        &format!("bellwether_float_order_{}", dt.to_string().to_lowercase()),
        vec![dt.clone()],
        DataType::UInt64,
        Volatility::Immutable,
        Arc::new(move |args: &[ColumnarValue]| {
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let a = &arrays[0];
            let out: UInt64Array = match t {
                DataType::Float64 => a
                    .as_primitive::<Float64Type>()
                    .iter()
                    .map(|v| v.map(float_order))
                    .collect(),
                DataType::Float32 => a
                    .as_primitive::<Float32Type>()
                    .iter()
                    .map(|v| v.map(|v| float_order(f64::from(v))))
                    .collect(),
                DataType::Float16 => a
                    .as_primitive::<Float16Type>()
                    .iter()
                    .map(|v| v.map(|v| float_order(v.to_f64())))
                    .collect(),
                _ => return Err(df(format!("the float order key of a {t} column"))),
            };
            Ok(ColumnarValue::Array(Arc::new(out)))
        }),
    )
}

/// `column` (a string column of type `dt`) matches one of `args` under
/// `op`, both sides case-folded.
pub(crate) fn text(name: &str, dt: &DataType, op: TextOp, args: &[String]) -> ScalarUDF {
    let folded: Vec<String> = args.iter().map(|a| bellwether_core::text::fold(a)).collect();
    create_udf(
        name,
        vec![dt.clone()],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(move |a: &[ColumnarValue]| {
            let arrays = ColumnarValue::values_to_arrays(a)?;
            let arr = &arrays[0];
            let test = |v: Option<&str>| {
                v.is_some_and(|v| bellwether_core::text::matches_any(op, &folded, v))
            };
            let out: Vec<bool> = match arr.data_type() {
                DataType::Utf8 => arr.as_string::<i32>().iter().map(test).collect(),
                DataType::LargeUtf8 => arr.as_string::<i64>().iter().map(test).collect(),
                DataType::Utf8View => arr.as_string_view().iter().map(test).collect(),
                other => return Err(df(format!("a text match over a {other} column"))),
            };
            Ok(bools(out))
        }),
    )
}

/// The expression's value, recomputed from `inputs`, is one of `values`
/// (canonical encodings).
pub(crate) fn expr_in(
    name: &str,
    typed: Typed,
    inputs: Vec<(String, DataType)>,
    values: HashSet<Vec<u8>>,
) -> ScalarUDF {
    let schema = Arc::new(Schema::new(
        inputs
            .iter()
            .map(|(n, t)| Field::new(n, t.clone(), true))
            .collect::<Vec<_>>(),
    ));
    create_udf(
        name,
        inputs.iter().map(|(_, t)| t.clone()).collect(),
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(move |a: &[ColumnarValue]| {
            let arrays = ColumnarValue::values_to_arrays(a)?;
            let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).map_err(df)?;
            let encoded = typed.encode_rows(&batch).map_err(df)?;
            Ok(bools(
                encoded
                    .iter()
                    .map(|v| v.as_ref().is_some_and(|v| values.contains(v)))
                    .collect(),
            ))
        }),
    )
}

/// The list (or map, by key) holds, for every group, an element whose
/// canonical encoding at `element` is in the group. `ContainsAny` is one
/// group of every value. A null list matches nothing.
pub(crate) fn contains(
    name: &str,
    column: &DataType,
    element: DataType,
    groups: Vec<HashSet<Vec<u8>>>,
) -> ScalarUDF {
    create_udf(
        name,
        vec![column.clone()],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(move |a: &[ColumnarValue]| {
            let arrays = ColumnarValue::values_to_arrays(a)?;
            let lists = ListElements::new(&arrays[0]).map_err(df)?;
            let enc: KeyColumns = lists.encoder(&element).map_err(df)?;
            let mut out = BooleanBuilder::with_capacity(lists.len());
            let mut buf = Vec::new();
            for row in 0..lists.len() {
                let Some(range) = lists.row(row) else {
                    out.append_value(false);
                    continue;
                };
                let mut hit = vec![false; groups.len()];
                for i in range {
                    if enc.encode_into(i, &mut buf).map_err(df)? {
                        for (g, h) in groups.iter().zip(hit.iter_mut()) {
                            *h |= g.contains(&buf);
                        }
                    }
                }
                out.append_value(hit.iter().all(|h| *h));
            }
            Ok(ColumnarValue::Array(Arc::new(out.finish())))
        }),
    )
}
