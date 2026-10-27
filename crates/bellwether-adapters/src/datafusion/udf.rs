//! Scalar functions that evaluate the residual tests SQL has no exact
//! spelling for with Bellwether's own definitions (`bellwether_core`):
//! text matches (full case folding, UAX #29 words), expression values
//! (the expression language, bit for bit), and list membership over
//! canonical element encodings. Each returns `false` for a null, so a
//! filter drops the row (ADR 014). One more maps every float NaN to the
//! canonical NaN, so an equality under Arrow's total order compares floats
//! as canonical bytes do.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, BooleanBuilder, RecordBatch};
use arrow::datatypes::{DataType, Field, Float16Type, Float32Type, Float64Type, Schema};
use bellwether_core::encoding::KeyColumns;
use bellwether_core::expr::Typed;
use bellwether_core::list::ListElements;
use bellwether_types::TextOp;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{create_udf, ColumnarValue, ScalarUDF, Volatility};

fn df(e: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(e.to_string())
}

fn bools(v: Vec<bool>) -> ColumnarValue {
    ColumnarValue::Array(Arc::new(BooleanArray::from(v)))
}

/// Maps every NaN of a float column of type `dt` to the canonical NaN.
pub(crate) fn canonical_nan(dt: &DataType) -> ScalarUDF {
    let t = dt.clone();
    create_udf(
        &format!("bellwether_canonical_nan_{}", dt.to_string().to_lowercase()),
        vec![dt.clone()],
        dt.clone(),
        Volatility::Immutable,
        Arc::new(move |args: &[ColumnarValue]| {
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let a = &arrays[0];
            let out: ArrayRef = match t {
                DataType::Float64 => Arc::new(
                    a.as_primitive::<Float64Type>()
                        .unary::<_, Float64Type>(|v| if v.is_nan() { f64::NAN } else { v }),
                ),
                DataType::Float32 => Arc::new(
                    a.as_primitive::<Float32Type>()
                        .unary::<_, Float32Type>(|v| if v.is_nan() { f32::NAN } else { v }),
                ),
                DataType::Float16 => Arc::new(a.as_primitive::<Float16Type>().unary::<_, Float16Type>(
                    |v| if v.is_nan() { arrow::datatypes::f16::NAN } else { v },
                )),
                _ => return Err(df(format!("canonical NaN of a {t} column"))),
            };
            Ok(ColumnarValue::Array(out))
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

/// Whether a list-typed array (for the `contains` input) is supported.
#[must_use]
pub(crate) fn is_list(dt: &DataType) -> bool {
    matches!(dt, DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _))
}

#[allow(dead_code)]
fn _assert_array(_: &dyn Array) {}
