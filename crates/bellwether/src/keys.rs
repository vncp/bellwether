//! Key files and inline key values: the keys a lookup or a recorded run
//! asks for, read from a Parquet file whose columns (all of them, or the
//! ones named) are the key columns or given inline as JSON rows over named
//! key columns, and encoded at the dataset types (ADR 016).

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Date64Builder, Decimal128Builder,
    Decimal256Builder, FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int16Builder,
    Int32Builder, Int64Builder, Int8Builder, RecordBatch, StringBuilder, Time32MillisecondBuilder,
    Time32SecondBuilder, Time64MicrosecondBuilder, Time64NanosecondBuilder,
    TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
    TimestampSecondBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow::compute::concat_batches;
use arrow::datatypes::{i256, DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatchReader;
use bellwether_core::encoding::{self, Value as EncValue};
use bellwether_replay::{RunKeys, RunKeysBuilder};
use bellwether_types::{Error, Result};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::index::instant_in_unit;

/// A set of keys: a Parquet file, every column of which is a key column
/// unless `columns` names a subset (in tuple order), or inline `key_values`
/// rows over `columns`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyFile {
    /// The Parquet file (empty when `key_values` are given inline).
    #[serde(default)]
    pub path: PathBuf,
    /// Key columns to take from it, in tuple order; `None` takes every
    /// column in file order. Required with `key_values`.
    pub columns: Option<Vec<String>>,
    /// Inline key tuples (one inner `Vec<Value>` per tuple, one scalar per
    /// column in `columns`), instead of reading `path`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_values: Option<Vec<Vec<Value>>>,
}

impl KeyFile {
    /// Every column of `path` is a key column.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            columns: None,
            key_values: None,
        }
    }

    /// Inline key tuples over `columns`.
    #[must_use]
    pub fn from_values(columns: Vec<String>, key_values: Vec<Vec<Value>>) -> Self {
        Self {
            path: PathBuf::new(),
            columns: Some(columns),
            key_values: Some(key_values),
        }
    }

    /// Only `columns`, in this order.
    #[must_use]
    pub fn with_columns(mut self, columns: Vec<String>) -> Self {
        self.columns = Some(columns);
        self
    }

    /// Inline `key_values` rows.
    #[must_use]
    pub fn with_key_values(mut self, key_values: Vec<Vec<Value>>) -> Self {
        self.key_values = Some(key_values);
        self
    }
    /// The key column names in tuple order, read from the footer alone when
    /// the caller did not name them.
    pub(crate) fn column_names(&self) -> Result<Vec<String>> {
        if self.key_values.is_some() {
            return self.inline_columns().map(<[String]>::to_vec);
        }
        if let Some(c) = &self.columns {
            return Ok(c.clone());
        }
        let path = &self.path;
        let fail = |e: &dyn std::fmt::Display| Error::InvalidArgument {
            field: "keys",
            reason: format!(
                "cannot read the key file {} ({e}); pass a Parquet file whose columns are the \
                 key columns",
                path.display()
            ),
        };
        let file = File::open(path).map_err(|e| fail(&e))?;
        let meta = bellwether_source::load_reader_metadata(&file).map_err(|e| fail(&e))?;
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, meta);
        Ok(builder
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect())
    }

    /// The key columns as one batch holding exactly them, in tuple order.
    /// Memory: the key set, which a run holds anyway (ADR 018).
    pub(crate) fn read(&self) -> Result<RecordBatch> {
        if let Some(rows) = &self.key_values {
            let cols = self.inline_columns()?;
            return Err(Error::InvalidArgument {
                field: "key_values",
                reason: format!(
                    "inline key_values over ({}) need the dataset schema to parse ({} row(s))",
                    cols.join(", "),
                    rows.len()
                ),
            });
        }
        let path = &self.path;
        let fail = |e: &dyn std::fmt::Display| Error::InvalidArgument {
            field: "keys",
            reason: format!(
                "cannot read the key file {} ({e}); pass a Parquet file whose columns are the \
                 key columns",
                path.display()
            ),
        };
        let file = File::open(path).map_err(|e| fail(&e))?;
        let meta = bellwether_source::load_reader_metadata(&file).map_err(|e| fail(&e))?;
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, meta);
        let file_schema = builder.schema().clone();
        let names: Vec<String> = match &self.columns {
            Some(c) => c.clone(),
            None => file_schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect(),
        };
        if names.is_empty() {
            return Err(Error::InvalidArgument {
                field: "keys",
                reason: format!(
                    "the key file {} has no columns; it needs one column per key column",
                    path.display()
                ),
            });
        }
        let mut leaves = Vec::with_capacity(names.len());
        for n in &names {
            let i = file_schema
                .index_of(n)
                .map_err(|_| Error::InvalidArgument {
                    field: "key_columns",
                    reason: format!(
                        "the key file {} has no column {n:?} (it has {:?}); name key columns the \
                         file has",
                        path.display(),
                        file_schema
                            .fields()
                            .iter()
                            .map(|f| f.name().as_str())
                            .collect::<Vec<_>>()
                    ),
                })?;
            leaves.push(i);
        }
        let mask = ProjectionMask::roots(builder.parquet_schema(), leaves);
        let reader = builder
            .with_projection(mask)
            .build()
            .map_err(|e| fail(&e))?;
        let schema = reader.schema();
        let mut batches = Vec::new();
        for b in reader {
            batches.push(b.map_err(|e| fail(&e))?);
        }
        let all = concat_batches(&schema, &batches).map_err(|e| fail(&e))?;
        // Reorder to tuple order: the projection keeps file order.
        let idx = names
            .iter()
            .map(|n| all.schema().index_of(n).map_err(|e| fail(&e)))
            .collect::<Result<Vec<_>>>()?;
        all.project(&idx).map_err(|e| fail(&e))
    }

    /// The canonical key set at `schema`'s types (nulls dropped and
    /// counted, sorted, deduplicated), with the dropped-null count.
    /// The key columns as one batch holding exactly them, in tuple order,
    /// parsing inline `key_values` against `schema` when given.
    pub(crate) fn read_with_schema(&self, schema: &Schema) -> Result<RecordBatch> {
        if let Some(rows) = &self.key_values {
            let cols = self.inline_columns()?;
            return inline_batch(cols, rows, schema);
        }
        self.read()
    }

    fn inline_columns(&self) -> Result<&[String]> {
        if !self.path.as_os_str().is_empty() {
            return Err(Error::InvalidArgument {
                field: "key_values",
                reason: "give `keys` (a Parquet key file) or `key_values` (inline key tuples), \
                         not both"
                    .into(),
            });
        }
        let rows = self.key_values.as_deref().unwrap_or_default();
        if rows.is_empty() {
            return Err(Error::InvalidArgument {
                field: "key_values",
                reason: "`key_values` is empty; pass at least one key tuple".into(),
            });
        }
        let cols = self
            .columns
            .as_deref()
            .filter(|c| !c.is_empty())
            .ok_or_else(|| Error::InvalidArgument {
                field: "key_columns",
                reason: "`key_values` requires `key_columns` naming the key column(s) in tuple \
                         order"
                    .into(),
            })?;
        for (r, row) in rows.iter().enumerate() {
            if row.len() != cols.len() {
                return Err(Error::InvalidArgument {
                    field: "key_values",
                    reason: format!(
                        "`key_values` row {r} has {} value(s), but `key_columns` names {} \
                         column(s) ({})",
                        row.len(),
                        cols.len(),
                        cols.join(", ")
                    ),
                });
            }
        }
        Ok(cols)
    }

    /// The canonical key set at `schema`'s types (nulls dropped and
    /// counted, sorted, deduplicated), with the dropped-null count.
    pub(crate) fn run_keys(&self, schema: &Schema) -> Result<(RunKeys, u64)> {
        let batch = self.read_with_schema(schema)?;
        let columns: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let mut b = RunKeysBuilder::new(&columns, schema)?;
        b.push_batch(&batch)?;
        let nulls = b.null_tuples();
        Ok((b.finish(), nulls))
    }
}

/// Builds a [`RecordBatch`] from inline JSON `rows` over `columns` at the
/// dataset `schema`'s column types (or element types for list/map columns,
/// or expression types for expression columns).
fn inline_batch(columns: &[String], rows: &[Vec<Value>], schema: &Schema) -> Result<RecordBatch> {
    // Validate distinct columns and resolve each column's target type via
    // RunKeysBuilder first, so unknown or duplicate columns produce the same
    // errors as a key file.
    let _ = RunKeysBuilder::new(columns, schema)?;
    let mut fields = Vec::with_capacity(columns.len());
    let mut arrays = Vec::with_capacity(columns.len());
    for (col_idx, col) in columns.iter().enumerate() {
        let raw_dt = match schema.field_with_name(col) {
            Ok(f) => f.data_type().clone(),
            Err(_) => bellwether_core::expr::Typed::parse(col, schema)?
                .data_type()
                .clone(),
        };
        let elem_dt = match bellwether_core::list::element_type(&raw_dt) {
            Some(_) => bellwether_core::list::check_list_column(col, &raw_dt)?,
            None => raw_dt,
        };
        let target_dt = match &elem_dt {
            DataType::Dictionary(_, inner) => inner.as_ref().clone(),
            other => other.clone(),
        };
        let arr = build_column(col, &target_dt, rows, col_idx)?;
        fields.push(Field::new(col, arr.data_type().clone(), true));
        arrays.push(arr);
    }
    let batch_schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(batch_schema, arrays).map_err(|e| Error::InvalidArgument {
        field: "key_values",
        reason: format!("cannot build key batch from `key_values` ({e})"),
    })
}

fn bad_value(col: &str, dt: &DataType, row: usize, why: impl std::fmt::Display) -> Error {
    Error::InvalidArgument {
        field: "key_values",
        reason: format!("`key_values` row {row} for column {col:?} ({dt}): {why}"),
    }
}

fn parse_signed(
    col: &str,
    dt: &DataType,
    row: usize,
    v: &Value,
    lo: i128,
    hi: i128,
) -> Result<i64> {
    let x: i128 = match v {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i128::from(i)
            } else if let Some(u) = n.as_u64() {
                i128::from(u)
            } else {
                return Err(bad_value(col, dt, row, format!("{v} is not an integer")));
            }
        }
        Value::String(s) => s
            .trim()
            .parse::<i128>()
            .map_err(|_| bad_value(col, dt, row, format!("{v} is not an integer")))?,
        _ => {
            return Err(bad_value(
                col,
                dt,
                row,
                format!("expected an integer, got {v}"),
            ))
        }
    };
    if x < lo || x > hi {
        return Err(bad_value(
            col,
            dt,
            row,
            format!("{x} is out of range for {dt}"),
        ));
    }
    i64::try_from(x).map_err(|_| bad_value(col, dt, row, format!("{x} is out of range for {dt}")))
}

fn parse_unsigned(col: &str, dt: &DataType, row: usize, v: &Value, hi: u64) -> Result<u64> {
    let x: u64 = match v {
        Value::Number(n) => n
            .as_u64()
            .ok_or_else(|| bad_value(col, dt, row, format!("{v} is not an unsigned integer")))?,
        Value::String(s) => s
            .trim()
            .parse::<u64>()
            .map_err(|_| bad_value(col, dt, row, format!("{v} is not an unsigned integer")))?,
        _ => {
            return Err(bad_value(
                col,
                dt,
                row,
                format!("expected an unsigned integer, got {v}"),
            ))
        }
    };
    if x > hi {
        return Err(bad_value(
            col,
            dt,
            row,
            format!("{x} is out of range for {dt}"),
        ));
    }
    Ok(x)
}

fn parse_float(col: &str, dt: &DataType, row: usize, v: &Value) -> Result<f64> {
    let f = match v {
        Value::Number(n) => n
            .as_f64()
            .ok_or_else(|| bad_value(col, dt, row, format!("{v} is not a float")))?,
        Value::String(s) => s
            .trim()
            .parse::<f64>()
            .map_err(|_| bad_value(col, dt, row, format!("{v} is not a float")))?,
        _ => {
            return Err(bad_value(
                col,
                dt,
                row,
                format!("expected a number, got {v}"),
            ))
        }
    };
    let mut probe = Vec::with_capacity(9);
    encoding::encode_value_as(&EncValue::Float(f), dt, &mut probe)
        .map_err(|e| bad_value(col, dt, row, e))?;
    Ok(f)
}

fn parse_decimal_str(col: &str, dt: &DataType, row: usize, s: &str) -> Result<(i256, i8)> {
    let s = s.trim();
    if s.is_empty() {
        return Err(bad_value(col, dt, row, "empty decimal string"));
    }
    let (digits, scale) = match s.split_once('.') {
        Some((int_part, frac_part)) => {
            if frac_part.is_empty()
                || !frac_part.bytes().all(|b| b.is_ascii_digit())
                || (int_part.is_empty() || int_part == "-" || int_part == "+")
            {
                return Err(bad_value(col, dt, row, format!("{s:?} is not a decimal")));
            }
            let scale = i8::try_from(frac_part.len()).map_err(|_| {
                bad_value(col, dt, row, format!("{s:?} has too many decimal places"))
            })?;
            (format!("{int_part}{frac_part}"), scale)
        }
        None => (s.to_string(), 0i8),
    };
    let val = i256::from_string(&digits)
        .ok_or_else(|| bad_value(col, dt, row, format!("{s:?} is not a decimal")))?;
    Ok((val, scale))
}

fn parse_decimal(col: &str, dt: &DataType, row: usize, v: &Value) -> Result<Vec<u8>> {
    let (val, scale) = match v {
        Value::Number(n) => parse_decimal_str(col, dt, row, &n.to_string())?,
        Value::String(s) => parse_decimal_str(col, dt, row, s)?,
        _ => {
            return Err(bad_value(
                col,
                dt,
                row,
                format!("expected a decimal string or number, got {v}"),
            ))
        }
    };
    let mut out = Vec::with_capacity(34);
    encoding::encode_value_as(&EncValue::Decimal256 { value: val, scale }, dt, &mut out)
        .map_err(|e| bad_value(col, dt, row, e))?;
    Ok(out)
}

fn parse_temporal(col: &str, dt: &DataType, row: usize, v: &Value) -> Result<i64> {
    let text = match v {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                return Ok(i);
            }
            return Err(bad_value(
                col,
                dt,
                row,
                format!("{v} is not an integer instant"),
            ));
        }
        Value::String(s) => s.as_str(),
        _ => {
            return Err(bad_value(
                col,
                dt,
                row,
                format!("expected an RFC 3339 instant, date, or integer, got {v}"),
            ))
        }
    };
    instant_in_unit("key_values", text, col, dt).map_err(|e| bad_value(col, dt, row, e))
}

fn parse_time(col: &str, dt: &DataType, row: usize, unit: TimeUnit, v: &Value) -> Result<i64> {
    let x = match v {
        Value::Number(n) => n
            .as_i64()
            .ok_or_else(|| bad_value(col, dt, row, format!("{v} is not an integer time")))?,
        Value::String(s) => s
            .trim()
            .parse::<i64>()
            .map_err(|_| bad_value(col, dt, row, format!("{v} is not an integer time")))?,
        _ => {
            return Err(bad_value(
                col,
                dt,
                row,
                format!("expected an integer time, got {v}"),
            ))
        }
    };
    let mut probe = Vec::with_capacity(11);
    encoding::encode_value_as(&EncValue::Time { value: x, unit }, dt, &mut probe)
        .map_err(|e| bad_value(col, dt, row, e))?;
    Ok(x)
}

fn parse_hex_bytes(col: &str, dt: &DataType, row: usize, s: &str) -> Result<Vec<u8>> {
    let hex = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    if !hex.len().is_multiple_of(2) {
        return Err(bad_value(
            col,
            dt,
            row,
            "hex binary string must have an even number of digits",
        ));
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&hex[i..i + 2], 16)
                .map_err(|_| bad_value(col, dt, row, format!("{s:?} is not valid hex")))
        })
        .collect()
}

#[allow(
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::many_single_char_names
)]
fn build_column(col: &str, dt: &DataType, rows: &[Vec<Value>], col_idx: usize) -> Result<ArrayRef> {
    let n = rows.len();
    Ok(match dt {
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(n);
            for (r, row) in rows.iter().enumerate() {
                match &row[col_idx] {
                    Value::Null => b.append_null(),
                    Value::Bool(v) => b.append_value(*v),
                    other => {
                        return Err(bad_value(
                            col,
                            dt,
                            r,
                            format!("expected a boolean, got {other}"),
                        ))
                    }
                }
            }
            Arc::new(b.finish())
        }
        DataType::Int8 => {
            let mut b = Int8Builder::with_capacity(n);
            for (r, row) in rows.iter().enumerate() {
                match &row[col_idx] {
                    Value::Null => b.append_null(),
                    v => b.append_value(parse_signed(
                        col,
                        dt,
                        r,
                        v,
                        i128::from(i8::MIN),
                        i128::from(i8::MAX),
                    )? as i8),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Int16 => {
            let mut b = Int16Builder::with_capacity(n);
            for (r, row) in rows.iter().enumerate() {
                match &row[col_idx] {
                    Value::Null => b.append_null(),
                    v => b.append_value(parse_signed(
                        col,
                        dt,
                        r,
                        v,
                        i128::from(i16::MIN),
                        i128::from(i16::MAX),
                    )? as i16),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Int32 => {
            let mut b = Int32Builder::with_capacity(n);
            for (r, row) in rows.iter().enumerate() {
                match &row[col_idx] {
                    Value::Null => b.append_null(),
                    v => b.append_value(parse_signed(
                        col,
                        dt,
                        r,
                        v,
                        i128::from(i32::MIN),
                        i128::from(i32::MAX),
                    )? as i32),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(n);
            for (r, row) in rows.iter().enumerate() {
                match &row[col_idx] {
                    Value::Null => b.append_null(),
                    v => b.append_value(parse_signed(
                        col,
                        dt,
                        r,
                        v,
                        i128::from(i64::MIN),
                        i128::from(i64::MAX),
                    )?),
                }
            }
            Arc::new(b.finish())
        }
        DataType::UInt8 => {
            let mut b = UInt8Builder::with_capacity(n);
            for (r, row) in rows.iter().enumerate() {
                match &row[col_idx] {
                    Value::Null => b.append_null(),
                    v => b.append_value(parse_unsigned(col, dt, r, v, u64::from(u8::MAX))? as u8),
                }
            }
            Arc::new(b.finish())
        }
        DataType::UInt16 => {
            let mut b = UInt16Builder::with_capacity(n);
            for (r, row) in rows.iter().enumerate() {
                match &row[col_idx] {
                    Value::Null => b.append_null(),
                    v => b.append_value(parse_unsigned(col, dt, r, v, u64::from(u16::MAX))? as u16),
                }
            }
            Arc::new(b.finish())
        }
        DataType::UInt32 => {
            let mut b = UInt32Builder::with_capacity(n);
            for (r, row) in rows.iter().enumerate() {
                match &row[col_idx] {
                    Value::Null => b.append_null(),
                    v => b.append_value(parse_unsigned(col, dt, r, v, u64::from(u32::MAX))? as u32),
                }
            }
            Arc::new(b.finish())
        }
        DataType::UInt64 => {
            let mut b = UInt64Builder::with_capacity(n);
            for (r, row) in rows.iter().enumerate() {
                match &row[col_idx] {
                    Value::Null => b.append_null(),
                    v => b.append_value(parse_unsigned(col, dt, r, v, u64::MAX)?),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Float32 => {
            let mut b = Float32Builder::with_capacity(n);
            for (r, row) in rows.iter().enumerate() {
                match &row[col_idx] {
                    Value::Null => b.append_null(),
                    v => b.append_value(parse_float(col, dt, r, v)? as f32),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Float16 | DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(n);
            for (r, row) in rows.iter().enumerate() {
                match &row[col_idx] {
                    Value::Null => b.append_null(),
                    v => b.append_value(parse_float(col, dt, r, v)?),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Decimal128(p, s) => {
            let mut b =
                Decimal128Builder::with_capacity(n).with_data_type(DataType::Decimal128(*p, *s));
            let col_type = bellwether_core::segment::ColumnType::Decimal128 {
                precision: *p,
                scale: *s,
            };
            for (r, row) in rows.iter().enumerate() {
                match &row[col_idx] {
                    Value::Null => b.append_null(),
                    v => {
                        let bytes = parse_decimal(col, dt, r, v)?;
                        let bellwether_core::encoding::Scalar::Decimal128 { value, .. } =
                            encoding::decode(&bytes, &col_type)
                                .map_err(|e| bad_value(col, dt, r, e))?
                        else {
                            return Err(bad_value(col, dt, r, "unexpected decimal decode"));
                        };
                        b.append_value(value);
                    }
                }
            }
            Arc::new(b.finish())
        }
        DataType::Decimal256(p, s) => {
            let mut b =
                Decimal256Builder::with_capacity(n).with_data_type(DataType::Decimal256(*p, *s));
            let col_type = bellwether_core::segment::ColumnType::Decimal256 {
                precision: *p,
                scale: *s,
            };
            for (r, row) in rows.iter().enumerate() {
                match &row[col_idx] {
                    Value::Null => b.append_null(),
                    v => {
                        let bytes = parse_decimal(col, dt, r, v)?;
                        let bellwether_core::encoding::Scalar::Decimal256 { value, .. } =
                            encoding::decode(&bytes, &col_type)
                                .map_err(|e| bad_value(col, dt, r, e))?
                        else {
                            return Err(bad_value(col, dt, r, "unexpected decimal decode"));
                        };
                        b.append_value(value);
                    }
                }
            }
            Arc::new(b.finish())
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let mut b = StringBuilder::with_capacity(n, n * 8);
            for (r, row) in rows.iter().enumerate() {
                match &row[col_idx] {
                    Value::Null => b.append_null(),
                    Value::String(s) => b.append_value(s),
                    other => {
                        return Err(bad_value(
                            col,
                            dt,
                            r,
                            format!("expected a string, got {other}"),
                        ))
                    }
                }
            }
            Arc::new(b.finish())
        }
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            let mut b = BinaryBuilder::with_capacity(n, n * 8);
            for (r, row) in rows.iter().enumerate() {
                match &row[col_idx] {
                    Value::Null => b.append_null(),
                    Value::String(s) => b.append_value(parse_hex_bytes(col, dt, r, s)?),
                    other => {
                        return Err(bad_value(
                            col,
                            dt,
                            r,
                            format!("expected a hex string, got {other}"),
                        ))
                    }
                }
            }
            Arc::new(b.finish())
        }
        DataType::FixedSizeBinary(len) => {
            let mut b = FixedSizeBinaryBuilder::with_capacity(n, *len);
            for (r, row) in rows.iter().enumerate() {
                match &row[col_idx] {
                    Value::Null => b.append_null(),
                    Value::String(s) => {
                        let bytes = parse_hex_bytes(col, dt, r, s)?;
                        b.append_value(&bytes)
                            .map_err(|e| bad_value(col, dt, r, e))?;
                    }
                    other => {
                        return Err(bad_value(
                            col,
                            dt,
                            r,
                            format!("expected a hex string, got {other}"),
                        ))
                    }
                }
            }
            Arc::new(b.finish())
        }
        DataType::Date32 => {
            let mut b = Date32Builder::with_capacity(n);
            for (r, row) in rows.iter().enumerate() {
                match &row[col_idx] {
                    Value::Null => b.append_null(),
                    v => {
                        let days = i32::try_from(parse_temporal(col, dt, r, v)?)
                            .map_err(|_| bad_value(col, dt, r, "date out of Date32 range"))?;
                        b.append_value(days);
                    }
                }
            }
            Arc::new(b.finish())
        }
        DataType::Date64 => {
            let mut b = Date64Builder::with_capacity(n);
            for (r, row) in rows.iter().enumerate() {
                match &row[col_idx] {
                    Value::Null => b.append_null(),
                    v => b.append_value(parse_temporal(col, dt, r, v)?),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Time32(TimeUnit::Second) => {
            let mut b = Time32SecondBuilder::with_capacity(n);
            for (r, row) in rows.iter().enumerate() {
                match &row[col_idx] {
                    Value::Null => b.append_null(),
                    v => b.append_value(parse_time(col, dt, r, TimeUnit::Second, v)? as i32),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            let mut b = Time32MillisecondBuilder::with_capacity(n);
            for (r, row) in rows.iter().enumerate() {
                match &row[col_idx] {
                    Value::Null => b.append_null(),
                    v => b.append_value(parse_time(col, dt, r, TimeUnit::Millisecond, v)? as i32),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let mut b = Time64MicrosecondBuilder::with_capacity(n);
            for (r, row) in rows.iter().enumerate() {
                match &row[col_idx] {
                    Value::Null => b.append_null(),
                    v => b.append_value(parse_time(col, dt, r, TimeUnit::Microsecond, v)?),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let mut b = Time64NanosecondBuilder::with_capacity(n);
            for (r, row) in rows.iter().enumerate() {
                match &row[col_idx] {
                    Value::Null => b.append_null(),
                    v => b.append_value(parse_time(col, dt, r, TimeUnit::Nanosecond, v)?),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Timestamp(TimeUnit::Second, tz) => {
            let mut b = TimestampSecondBuilder::with_capacity(n).with_timezone_opt(tz.clone());
            for (r, row) in rows.iter().enumerate() {
                match &row[col_idx] {
                    Value::Null => b.append_null(),
                    v => b.append_value(parse_temporal(col, dt, r, v)?),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            let mut b = TimestampMillisecondBuilder::with_capacity(n).with_timezone_opt(tz.clone());
            for (r, row) in rows.iter().enumerate() {
                match &row[col_idx] {
                    Value::Null => b.append_null(),
                    v => b.append_value(parse_temporal(col, dt, r, v)?),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(n).with_timezone_opt(tz.clone());
            for (r, row) in rows.iter().enumerate() {
                match &row[col_idx] {
                    Value::Null => b.append_null(),
                    v => b.append_value(parse_temporal(col, dt, r, v)?),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            let mut b = TimestampNanosecondBuilder::with_capacity(n).with_timezone_opt(tz.clone());
            for (r, row) in rows.iter().enumerate() {
                match &row[col_idx] {
                    Value::Null => b.append_null(),
                    v => b.append_value(parse_temporal(col, dt, r, v)?),
                }
            }
            Arc::new(b.finish())
        }
        other => {
            return Err(Error::KeySpec(encoding::no_encoding(other)));
        }
    })
}
