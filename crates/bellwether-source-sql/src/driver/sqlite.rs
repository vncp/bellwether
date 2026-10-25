//! SQLite through rusqlite (bundled SQLite), opened read-only.
//!
//! SQLite values carry a storage class, not a column type, so this driver
//! builds the asked (declared) Arrow type from each value, by these rules,
//! and refuses anything else naming the column and the value:
//!
//! - integers: `INTEGER` values;
//! - floats: `REAL` values, and `INTEGER` values up to 2^53;
//! - `DECIMAL(p,s)`: `INTEGER` values, decimal `TEXT`, and a `REAL` read as
//!   the shortest decimal text that reads back as the same double, each
//!   exactly at scale `s`;
//! - text and blobs: `TEXT` and `BLOB` values;
//! - booleans: the integers 0 and 1;
//! - dates: `TEXT` `YYYY-MM-DD`;
//! - timestamps: ISO 8601 `TEXT` (no zone means UTC), and `INTEGER` counts
//!   of the declared unit only for a column declared with a precision
//!   (`TIMESTAMP(3)` counts milliseconds); a bare `TIMESTAMP` holding an
//!   integer is refused, since nothing says what it counts.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
    RecordBatch, RecordBatchOptions, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_types::{Error, Result};
use rusqlite::types::ValueRef;
use rusqlite::{Connection, OpenFlags};

use super::Driver;
use crate::cast::{
    float_text, parse_date, parse_decimal, parse_timestamp, timestamp_array, to_type,
};
use crate::dialect::INTEGER_UNIT;

/// Rows per batch handed back.
const BATCH_ROWS: usize = 65_536;

/// A read-only SQLite connection.
pub(crate) struct SqliteDriver {
    conn: Connection,
}

fn err(e: impl std::fmt::Display) -> Error {
    Error::Source(format!("sqlite: {e}"))
}

impl SqliteDriver {
    /// Opens the database file at `path` read-only. A missing file is an
    /// error; nothing is created.
    pub(crate) fn open(path: &str) -> Result<Self> {
        if !path.starts_with("file:") && !std::path::Path::new(path).is_file() {
            return Err(Error::Source(format!(
                "sqlite: no database file at {path:?}; pass db=<path to the .sqlite file>"
            )));
        }
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|e| Error::Source(format!("sqlite: cannot open {path:?}: {e}")))?;
        conn.busy_timeout(Duration::from_secs(30)).map_err(err)?;
        Ok(Self { conn })
    }
}

/// One column's values as they are read, before the array is built.
enum Col {
    Int(Vec<Option<i64>>),
    Float(Vec<Option<f64>>),
    Dec(Vec<Option<i128>>, u8, i8),
    Text(Vec<Option<String>>),
    Bin(Vec<Option<Vec<u8>>>),
    Bool(Vec<Option<bool>>),
    Date(Vec<Option<i32>>),
    Ts(Vec<Option<i64>>, TimeUnit, Option<Arc<str>>, bool),
}

impl Col {
    fn new(field: &Field) -> Result<Self> {
        Ok(match field.data_type() {
            t if t.is_integer() => Self::Int(Vec::new()),
            DataType::Float32 | DataType::Float64 => Self::Float(Vec::new()),
            DataType::Decimal128(p, s) => Self::Dec(Vec::new(), *p, *s),
            DataType::Utf8 => Self::Text(Vec::new()),
            DataType::Binary => Self::Bin(Vec::new()),
            DataType::Boolean => Self::Bool(Vec::new()),
            DataType::Date32 => Self::Date(Vec::new()),
            DataType::Timestamp(u, tz) => Self::Ts(
                Vec::new(),
                *u,
                tz.clone(),
                field.metadata().contains_key(INTEGER_UNIT),
            ),
            other => {
                return Err(Error::Source(format!(
                    "sqlite: column {:?} is asked for as {other}, which this driver does not \
                     build",
                    field.name()
                )))
            }
        })
    }

    fn push(&mut self, v: ValueRef<'_>) -> std::result::Result<(), String> {
        let shown = || show(v);
        let wrong = |what: &str| format!("holds {}, which is not {what}", shown());
        match self {
            Self::Int(out) => out.push(match v {
                ValueRef::Null => None,
                ValueRef::Integer(i) => Some(i),
                _ => return Err(wrong("an integer")),
            }),
            Self::Float(out) => out.push(match v {
                ValueRef::Null => None,
                ValueRef::Real(f) => Some(f),
                ValueRef::Integer(i) if i.unsigned_abs() <= 1 << 53 =>
                {
                    #[allow(clippy::cast_precision_loss)]
                    Some(i as f64)
                }
                _ => return Err(wrong("a float")),
            }),
            Self::Dec(out, p, s) => out.push(match v {
                ValueRef::Null => None,
                ValueRef::Integer(i) => Some(parse_decimal(&i.to_string(), *p, *s)?),
                ValueRef::Real(f) => Some(parse_decimal(&float_text(f)?, *p, *s)?),
                ValueRef::Text(t) => Some(parse_decimal(utf8(t)?, *p, *s)?),
                ValueRef::Blob(_) => return Err(wrong("a decimal")),
            }),
            Self::Text(out) => out.push(match v {
                ValueRef::Null => None,
                ValueRef::Text(t) => Some(utf8(t)?.to_string()),
                _ => return Err(wrong("text")),
            }),
            Self::Bin(out) => out.push(match v {
                ValueRef::Null => None,
                ValueRef::Blob(b) => Some(b.to_vec()),
                _ => return Err(wrong("a blob")),
            }),
            Self::Bool(out) => out.push(match v {
                ValueRef::Null => None,
                ValueRef::Integer(0) => Some(false),
                ValueRef::Integer(1) => Some(true),
                _ => return Err(wrong("a boolean (0 or 1)")),
            }),
            Self::Date(out) => out.push(match v {
                ValueRef::Null => None,
                ValueRef::Text(t) => Some(parse_date(utf8(t)?)?),
                _ => return Err(wrong("a date (YYYY-MM-DD text)")),
            }),
            Self::Ts(out, unit, _, integer_ok) => out.push(match v {
                ValueRef::Null => None,
                ValueRef::Text(t) => Some(parse_timestamp(utf8(t)?, *unit)?),
                ValueRef::Integer(i) if *integer_ok => Some(i),
                ValueRef::Integer(_) => {
                    return Err(format!(
                        "holds the integer {}, and a column declared TIMESTAMP without a \
                         precision says nothing about what an integer counts; declare \
                         TIMESTAMP(0) for seconds, (3) for milliseconds, (6) for microseconds, \
                         or (9) for nanoseconds",
                        shown()
                    ))
                }
                _ => return Err(wrong("a timestamp")),
            }),
        }
        Ok(())
    }

    fn finish(self, field: &Field) -> std::result::Result<ArrayRef, String> {
        Ok(match self {
            Self::Int(v) => to_type(
                &(Arc::new(Int64Array::from(v)) as ArrayRef),
                field.data_type(),
            )?,
            Self::Float(v) => to_type(
                &(Arc::new(Float64Array::from(v)) as ArrayRef),
                field.data_type(),
            )?,
            Self::Dec(v, p, s) => Arc::new(
                Decimal128Array::from(v)
                    .with_precision_and_scale(p, s)
                    .map_err(|e| e.to_string())?,
            ),
            Self::Text(v) => Arc::new(StringArray::from(v)),
            Self::Bin(v) => Arc::new(BinaryArray::from_iter(v)),
            Self::Bool(v) => Arc::new(BooleanArray::from(v)),
            Self::Date(v) => Arc::new(Date32Array::from(v)),
            Self::Ts(v, unit, tz, _) => timestamp_array(v, unit, tz),
        })
    }
}

fn utf8(b: &[u8]) -> std::result::Result<&str, String> {
    std::str::from_utf8(b).map_err(|_| "holds text that is not UTF-8".to_string())
}

fn show(v: ValueRef<'_>) -> String {
    match v {
        ValueRef::Null => "NULL".into(),
        ValueRef::Integer(i) => format!("the integer {i}"),
        ValueRef::Real(f) => format!("the real {f:?}"),
        ValueRef::Text(t) => {
            let s = String::from_utf8_lossy(t);
            let cut: String = s.chars().take(40).collect();
            format!("the text {cut:?}")
        }
        ValueRef::Blob(b) => format!("a {}-byte blob", b.len()),
    }
}

fn build(fields: &[Arc<Field>], cols: Vec<Col>, rows: usize) -> Result<RecordBatch> {
    let mut arrays = Vec::with_capacity(cols.len());
    for (c, f) in cols.into_iter().zip(fields) {
        arrays.push(
            c.finish(f)
                .map_err(|e| Error::Source(format!("sqlite: column {:?}: {e}", f.name())))?,
        );
    }
    let schema = Arc::new(Schema::new(
        fields
            .iter()
            .map(|f| Field::new(f.name(), f.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    RecordBatch::try_new_with_options(
        schema,
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .map_err(err)
}

impl Driver for SqliteDriver {
    fn query(
        &mut self,
        sql: &str,
        native: &SchemaRef,
        each: &mut dyn FnMut(RecordBatch) -> Result<()>,
    ) -> Result<()> {
        let mut stmt = self.conn.prepare(sql).map_err(err)?;
        let fields = native.fields();
        if fields.is_empty() {
            // Nothing asked for (a timed run): every row is stepped
            // through and dropped.
            let mut rows = stmt.query([]).map_err(err)?;
            while rows.next().map_err(err)?.is_some() {}
            return Ok(());
        }
        if stmt.column_count() != fields.len() {
            return Err(Error::Source(format!(
                "sqlite: the query returns {} columns, {} were asked for",
                stmt.column_count(),
                fields.len()
            )));
        }
        let fresh = || {
            fields
                .iter()
                .map(|f| Col::new(f))
                .collect::<Result<Vec<_>>>()
        };
        let mut cols = fresh()?;
        let mut rows = stmt.query([]).map_err(err)?;
        let mut n = 0usize;
        while let Some(row) = rows.next().map_err(err)? {
            for (i, (c, f)) in cols.iter_mut().zip(fields.iter()).enumerate() {
                let v = row.get_ref(i).map_err(err)?;
                c.push(v)
                    .map_err(|e| Error::Source(format!("sqlite: column {:?} {e}", f.name())))?;
            }
            n += 1;
            if n == BATCH_ROWS {
                each(build(fields, std::mem::replace(&mut cols, fresh()?), n)?)?;
                n = 0;
            }
        }
        if n > 0 {
            each(build(fields, cols, n)?)?;
        }
        Ok(())
    }

    fn execute(&mut self, sql: &str) -> Result<()> {
        self.conn.execute_batch(sql).map_err(err)
    }
}
