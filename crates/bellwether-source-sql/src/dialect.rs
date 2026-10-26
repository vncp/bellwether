//! What differs between engines: quoting, the information schema, the
//! declared types and their Arrow types, literals, aggregates that must
//! compare bytes rather than collated text, and how to hold a snapshot.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{
    DataType, Date32Type, Decimal128Type, Field, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Int8Type, Schema, SchemaRef, TimeUnit, UInt16Type, UInt32Type, UInt64Type,
    UInt8Type,
};
use bellwether_types::{Error, Result};

use crate::cast::render_decimal;
use crate::url::{EngineKind, TableName};

/// Field metadata on a SQLite column declared with an explicit time
/// precision (`TIMESTAMP(3)`): an integer stored in it is a count of that
/// unit since the epoch. Without it an integer is refused, since nothing
/// says which unit it counts.
pub(crate) const INTEGER_UNIT: &str = "bellwether.sqlite.integer_unit";

/// Field metadata on a Postgres `character(n)` column: compared ignoring
/// trailing blanks but read padded, so `MIN`/`MAX` are not bounds of the
/// values read, and none are reported.
pub(crate) const PG_BPCHAR: &str = "bellwether.postgres.bpchar";

/// `[0-9]` repeated as a SQLite GLOB.
const D4: &str = "[0-9][0-9][0-9][0-9]";
const D2: &str = "[0-9][0-9]";

/// The engine-specific SQL.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Dialect(pub EngineKind);

/// A column as the catalog declares it.
#[derive(Clone, Debug)]
pub(crate) struct Declared {
    pub name: String,
    /// The declared type as the catalog spells it.
    pub spelled: String,
    pub nullable: bool,
    pub precision: Option<i64>,
    pub scale: Option<i64>,
    pub datetime_precision: Option<i64>,
}

/// The snapshot a run holds.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SnapshotSql {
    pub begin: &'static [&'static str],
    pub end: &'static str,
    /// How the record and the human output name it.
    pub label: &'static str,
}

impl Dialect {
    /// `"name"` (`[name]` in SQL Server), with the quote doubled inside.
    pub fn ident(self, name: &str) -> String {
        match self.0 {
            EngineKind::Mssql => format!("[{}]", name.replace(']', "]]")),
            _ => format!("\"{}\"", name.replace('"', "\"\"")),
        }
    }

    /// A string literal.
    pub fn string(self, s: &str) -> String {
        match self.0 {
            EngineKind::Mssql => format!("N'{}'", s.replace('\'', "''")),
            _ => format!("'{}'", s.replace('\'', "''")),
        }
    }

    pub fn table(self, t: &TableName) -> String {
        format!("{}.{}", self.ident(&t.schema), self.ident(&t.table))
    }

    /// The query listing `t`'s columns in order, and the schema of its
    /// answer: name, declared type, nullable, precision, scale, time
    /// precision.
    pub fn columns_query(self, t: &TableName) -> (String, SchemaRef) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("spelled", DataType::Utf8, true),
            Field::new("nullable", DataType::Utf8, true),
            Field::new("precision", DataType::Int64, true),
            Field::new("scale", DataType::Int64, true),
            Field::new("datetime_precision", DataType::Int64, true),
        ]));
        let (s, n) = (self.string(&t.schema), self.string(&t.table));
        let sql = match self.0 {
            EngineKind::Sqlite => format!(
                "SELECT name, type, CASE WHEN \"notnull\" = 0 THEN 'YES' ELSE 'NO' END, NULL, \
                 NULL, NULL FROM pragma_table_info({n}, {s}) ORDER BY cid"
            ),
            EngineKind::Duckdb => format!(
                "SELECT column_name, data_type, is_nullable, CAST(numeric_precision AS BIGINT), \
                 CAST(numeric_scale AS BIGINT), CAST(datetime_precision AS BIGINT) FROM \
                 information_schema.columns WHERE table_schema = {s} AND table_name = {n} \
                 ORDER BY ordinal_position"
            ),
            EngineKind::Postgres => format!(
                "SELECT column_name::text, data_type::text, is_nullable::text, \
                 numeric_precision::bigint, numeric_scale::bigint, datetime_precision::bigint \
                 FROM information_schema.columns WHERE table_schema = {s} AND table_name = {n} \
                 ORDER BY ordinal_position"
            ),
            EngineKind::Mssql => format!(
                "SELECT CAST(COLUMN_NAME AS NVARCHAR(256)), CAST(DATA_TYPE AS NVARCHAR(256)), \
                 CAST(IS_NULLABLE AS NVARCHAR(8)), CAST(NUMERIC_PRECISION AS BIGINT), \
                 CAST(NUMERIC_SCALE AS BIGINT), CAST(DATETIME_PRECISION AS BIGINT) FROM \
                 INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = {s} AND TABLE_NAME = {n} \
                 ORDER BY ORDINAL_POSITION"
            ),
            // Answered by the driver's table schema instead.
            EngineKind::FlightSql => String::new(),
        };
        (sql, schema)
    }

    /// The Arrow type a declared column is read as, or why it is refused.
    pub fn native_type(self, c: &Declared) -> std::result::Result<Field, String> {
        let spelled = c.spelled.trim().to_ascii_uppercase();
        let dt = match self.0 {
            EngineKind::Sqlite => return sqlite_type(c, &spelled),
            EngineKind::Duckdb => duckdb_type(&spelled)?,
            EngineKind::Postgres => postgres_type(c, &spelled)?,
            EngineKind::Mssql => mssql_type(c, &spelled)?,
            EngineKind::FlightSql => {
                return Err("Flight SQL columns are typed by the driver".into())
            }
        };
        let field = Field::new(&c.name, dt, c.nullable);
        Ok(
            if self.0 == EngineKind::Postgres && spelled == "CHARACTER" {
                field.with_metadata([(PG_BPCHAR.to_string(), "true".to_string())].into())
            } else {
                field
            },
        )
    }

    /// The snapshot a run holds, or `None` where the engine has none this
    /// crate can rely on.
    pub fn snapshot(self) -> Option<SnapshotSql> {
        match self.0 {
            EngineKind::Sqlite => Some(SnapshotSql {
                begin: &["BEGIN DEFERRED"],
                end: "ROLLBACK",
                label: "one SQLite read transaction (a snapshot)",
            }),
            EngineKind::Duckdb => Some(SnapshotSql {
                begin: &["BEGIN TRANSACTION"],
                end: "ROLLBACK",
                label: "one DuckDB transaction (snapshot isolation)",
            }),
            EngineKind::Postgres => Some(SnapshotSql {
                begin: &["BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"],
                end: "ROLLBACK",
                label: "one Postgres transaction at REPEATABLE READ",
            }),
            EngineKind::Mssql => Some(SnapshotSql {
                begin: &[
                    "SET TRANSACTION ISOLATION LEVEL SNAPSHOT",
                    "BEGIN TRANSACTION",
                ],
                end: "ROLLBACK TRANSACTION",
                label: "one SQL Server transaction at SNAPSHOT isolation",
            }),
            EngineKind::FlightSql => None,
        }
    }

    /// A query whose one row says whether the snapshot can be held
    /// (`1`) or not (`0`), for an engine where that is a database setting.
    pub fn snapshot_probe(self) -> Option<&'static str> {
        match self.0 {
            EngineKind::Mssql => Some(
                "SELECT CAST(snapshot_isolation_state AS BIGINT) FROM sys.databases WHERE name = \
                 DB_NAME()",
            ),
            _ => None,
        }
    }

    /// `MIN` and `MAX` of a column as bounds in the canonical order, or
    /// `None` where the engine cannot give that order: strings compare by
    /// UTF-8 bytes (binary collation), booleans as 0 and 1. Postgres
    /// `bytea` has no `MIN`, and `character(n)` compares ignoring trailing
    /// blanks while it is read padded: no bounds for either.
    pub fn min_max(self, field: &Field) -> Option<(String, String)> {
        let (column, dt) = (field.name(), field.data_type());
        if self.0 == EngineKind::Postgres
            && (matches!(
                dt,
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView
            ) || field.metadata().contains_key(PG_BPCHAR))
        {
            return None;
        }
        let c = self.ident(column);
        let text = matches!(
            dt,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        );
        match (self.0, dt) {
            // SQL Server orders nvarchar by UTF-16 code units under BIN2,
            // which differs from UTF-8 byte order above U+FFFF; Flight SQL
            // servers declare no collation. No string bounds there.
            (EngineKind::Mssql | EngineKind::FlightSql, _) if text => None,
            (EngineKind::Postgres, _) if text => Some((
                format!("MIN({c} COLLATE \"C\")"),
                format!("MAX({c} COLLATE \"C\")"),
            )),
            (EngineKind::Sqlite, _) if text => Some((
                format!("MIN({c} COLLATE BINARY)"),
                format!("MAX({c} COLLATE BINARY)"),
            )),
            (EngineKind::Duckdb, _) if text => Some((
                format!("MIN({c} COLLATE \"binary\")"),
                format!("MAX({c} COLLATE \"binary\")"),
            )),
            (EngineKind::Postgres, DataType::Boolean) => {
                Some((format!("bool_and({c})"), format!("bool_or({c})")))
            }
            (EngineKind::Mssql, DataType::Boolean) => Some((
                format!("MIN(CAST({c} AS TINYINT))"),
                format!("MAX(CAST({c} AS TINYINT))"),
            )),
            _ => Some((format!("MIN({c})"), format!("MAX({c})"))),
        }
    }

    /// SQLite only: an expression counting the non-null values of `field`
    /// whose storage class or text shape `MIN`/`MAX` would not order as the
    /// values are read (a text timestamp with a zone or another separator,
    /// a date with spaces, a number stored as text, integers beside text).
    /// When it is not zero the unit reports no bounds for the column.
    pub fn irregular(self, field: &Field) -> Option<String> {
        if self.0 != EngineKind::Sqlite {
            return None;
        }
        let c = self.ident(field.name());
        let regular = match field.data_type() {
            DataType::Timestamp(..) => format!(
                "CASE WHEN SUM(typeof({c}) = 'integer') = COUNT({c}) THEN COUNT({c}) ELSE \
                 SUM(typeof({c}) = 'text' AND ({c} GLOB '{D4}-{D2}-{D2} {D2}:{D2}:{D2}' OR \
                 ({c} GLOB '{D4}-{D2}-{D2} {D2}:{D2}:{D2}.[0-9]*' AND substr({c}, 21) NOT GLOB \
                 '*[^0-9]*'))) END"
            ),
            DataType::Date32 => {
                format!("SUM(typeof({c}) = 'text' AND {c} GLOB '{D4}-{D2}-{D2}')")
            }
            DataType::Decimal128(..) | DataType::Float32 | DataType::Float64 => {
                format!("SUM(typeof({c}) IN ('integer', 'real'))")
            }
            _ => return None,
        };
        Some(format!("COUNT({c}) - COALESCE({regular}, 0)"))
    }

    /// The bytes of one value of a variable-width column.
    pub fn octets(self, field: &Field) -> String {
        let c = self.ident(field.name());
        let text = matches!(
            field.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        );
        match self.0 {
            EngineKind::Sqlite => format!("length(CAST({c} AS BLOB))"),
            // `strlen` is the UTF-8 byte length; a cast of text to BLOB
            // fails on any non-ASCII character.
            EngineKind::Duckdb if text => format!("strlen({c})"),
            EngineKind::Duckdb | EngineKind::Postgres | EngineKind::FlightSql => {
                format!("octet_length({c})")
            }
            EngineKind::Mssql => format!("DATALENGTH({c})"),
        }
    }

    /// `CAST(<expr> AS BIGINT)` in the engine's spelling.
    pub fn bigint(expr: &str) -> String {
        format!("CAST({expr} AS BIGINT)")
    }

    /// The declared partitions of a Postgres table: one row per leaf
    /// partition, `schema`, `table`. Empty for a table that is not
    /// partitioned.
    pub fn partitions_query(self, t: &TableName) -> Option<String> {
        match self.0 {
            EngineKind::Postgres => Some(format!(
                "SELECT n.nspname::text, c.relname::text FROM pg_partition_tree(\
                 {}::regclass) p JOIN pg_class c ON c.oid = p.relid JOIN pg_namespace n ON \
                 n.oid = c.relnamespace WHERE p.isleaf AND p.relid <> {}::regclass ORDER BY 1, 2",
                self.string(&self.table(t)),
                self.string(&self.table(t))
            )),
            _ => None,
        }
    }

    /// The statement a human runs to give `table` an index on `columns`:
    /// what an attest-only apply names.
    pub fn create_index(self, table: &TableName, columns: &[String]) -> String {
        let name: String = format!("bw_{}_{}", table.table, columns.join("_"))
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        let cols: Vec<String> = columns.iter().map(|c| self.ident(c)).collect();
        format!(
            "CREATE INDEX {} ON {} ({})",
            self.ident(&name),
            self.table(table),
            cols.join(", ")
        )
    }

    /// The row of `array` as a literal of the column's own type, for a
    /// range unit's boundary. The value came from the engine's own answer,
    /// so the literal compares in the engine's own order.
    /// `integer_time`: the column is a SQLite time column holding integer
    /// counts of its unit, compared as integers.
    pub fn literal(self, array: &ArrayRef, row: usize, integer_time: bool) -> Result<String> {
        if array.is_null(row) {
            return Err(Error::Source("a range boundary is null".into()));
        }
        let unsupported = || {
            Error::Source(format!(
                "a range column of type {} cannot be cut into units; give range= an integer, \
                 decimal, float, date, timestamp, or string column",
                array.data_type()
            ))
        };
        Ok(match array.data_type() {
            DataType::Int8 => array.as_primitive::<Int8Type>().value(row).to_string(),
            DataType::Int16 => array.as_primitive::<Int16Type>().value(row).to_string(),
            DataType::Int32 => array.as_primitive::<Int32Type>().value(row).to_string(),
            DataType::Int64 => array.as_primitive::<Int64Type>().value(row).to_string(),
            DataType::UInt8 => array.as_primitive::<UInt8Type>().value(row).to_string(),
            DataType::UInt16 => array.as_primitive::<UInt16Type>().value(row).to_string(),
            DataType::UInt32 => array.as_primitive::<UInt32Type>().value(row).to_string(),
            DataType::UInt64 => array.as_primitive::<UInt64Type>().value(row).to_string(),
            DataType::Float32 => {
                float_literal(f64::from(array.as_primitive::<Float32Type>().value(row)))?
            }
            DataType::Float64 => float_literal(array.as_primitive::<Float64Type>().value(row))?,
            DataType::Decimal128(_, s) => {
                render_decimal(array.as_primitive::<Decimal128Type>().value(row), *s)
            }
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                let s = arrow::compute::cast(array, &DataType::Utf8)
                    .map_err(|e| Error::Source(e.to_string()))?;
                self.string(s.as_string::<i32>().value(row))
            }
            DataType::Date32 => {
                let days = array.as_primitive::<Date32Type>().value(row);
                let d = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                    .and_then(|e| e.checked_add_signed(chrono::TimeDelta::days(i64::from(days))))
                    .ok_or_else(unsupported)?;
                match self.0 {
                    EngineKind::Sqlite => self.string(&d.format("%Y-%m-%d").to_string()),
                    EngineKind::Mssql => format!("CAST('{}' AS DATE)", d.format("%Y-%m-%d")),
                    _ => format!("DATE '{}'", d.format("%Y-%m-%d")),
                }
            }
            DataType::Timestamp(unit, tz) => {
                let v = crate::cast::timestamp_column(array, *unit, row);
                if integer_time && self.0 == EngineKind::Sqlite {
                    v.to_string()
                } else {
                    self.timestamp_literal(v, *unit, tz.is_some())?
                }
            }
            _ => return Err(unsupported()),
        })
    }

    fn timestamp_literal(self, v: i64, unit: TimeUnit, zoned: bool) -> Result<String> {
        let per = match unit {
            TimeUnit::Second => 1,
            TimeUnit::Millisecond => 1_000,
            TimeUnit::Microsecond => 1_000_000,
            TimeUnit::Nanosecond => 1_000_000_000,
        };
        let secs = v.div_euclid(per);
        let sub = v.rem_euclid(per);
        let nanos = u32::try_from(sub * (1_000_000_000 / per))
            .map_err(|_| Error::Source(format!("timestamp {v} is out of range")))?;
        let dt = chrono::DateTime::from_timestamp(secs, nanos)
            .ok_or_else(|| Error::Source(format!("timestamp {v} is out of range")))?;
        let text = dt.format("%Y-%m-%d %H:%M:%S%.9f").to_string();
        Ok(match (self.0, zoned) {
            (EngineKind::Sqlite, _) => self.string(&text),
            (EngineKind::Mssql, false) => format!("CAST('{text}' AS DATETIME2(7))"),
            (EngineKind::Mssql, true) => format!("CAST('{text} +00:00' AS DATETIMEOFFSET(7))"),
            (EngineKind::Postgres, false) => format!("TIMESTAMP '{text}'"),
            (EngineKind::Postgres, true) => format!("TIMESTAMPTZ '{text}+00'"),
            (_, false) => match unit {
                TimeUnit::Nanosecond => format!("CAST('{text}' AS TIMESTAMP_NS)"),
                _ => format!("TIMESTAMP '{text}'"),
            },
            (_, true) => format!("CAST('{text}+00' AS TIMESTAMPTZ)"),
        })
    }
}

fn float_literal(v: f64) -> Result<String> {
    if v.is_finite() {
        Ok(format!("{v:e}"))
    } else {
        Err(Error::Source(format!(
            "a range boundary is {v}; cut the table by a column without NaN or infinity"
        )))
    }
}

fn refuse(spelled: &str) -> String {
    format!(
        "its declared type {spelled} has no canonical encoding here (ADR 016); read the table \
         through a view that casts or leaves out the column"
    )
}

fn decimal(c: &Declared, spelled: &str) -> std::result::Result<DataType, String> {
    match (c.precision, c.scale) {
        (Some(p), Some(s)) if (1..=38).contains(&p) && (0..=p).contains(&s) => Ok(
            DataType::Decimal128(u8::try_from(p).unwrap_or(38), i8::try_from(s).unwrap_or(0)),
        ),
        (Some(p), Some(_)) if p > 38 => Err(format!(
            "its declared type {spelled} has more than 38 digits; read it through a view that \
             casts it to at most DECIMAL(38, s)"
        )),
        _ => Err(format!(
            "its declared type {spelled} declares no precision and scale, so values cannot be \
             compared at one scale; declare NUMERIC(p, s) or read it through a view that casts it"
        )),
    }
}

/// `DECIMAL(p,s)`, `NUMERIC(p, s)` → `(p, s)`.
fn paren_numbers(spelled: &str) -> Option<Vec<i64>> {
    let open = spelled.find('(')?;
    let close = spelled.rfind(')')?;
    spelled[open + 1..close]
        .split(',')
        .map(|n| n.trim().parse().ok())
        .collect()
}

fn unit_of_precision(p: i64) -> Option<TimeUnit> {
    match p {
        0 => Some(TimeUnit::Second),
        1..=3 => Some(TimeUnit::Millisecond),
        4..=6 => Some(TimeUnit::Microsecond),
        7..=9 => Some(TimeUnit::Nanosecond),
        _ => None,
    }
}

fn duckdb_type(s: &str) -> std::result::Result<DataType, String> {
    use DataType as D;
    Ok(match s {
        "BOOLEAN" => D::Boolean,
        "TINYINT" => D::Int8,
        "SMALLINT" => D::Int16,
        "INTEGER" => D::Int32,
        "BIGINT" => D::Int64,
        "UTINYINT" => D::UInt8,
        "USMALLINT" => D::UInt16,
        "UINTEGER" => D::UInt32,
        "UBIGINT" => D::UInt64,
        "FLOAT" | "REAL" => D::Float32,
        "DOUBLE" => D::Float64,
        "VARCHAR" => D::Utf8,
        "BLOB" => D::Binary,
        "DATE" => D::Date32,
        "TIME" => D::Time64(TimeUnit::Microsecond),
        "TIMESTAMP" => D::Timestamp(TimeUnit::Microsecond, None),
        "TIMESTAMP_S" => D::Timestamp(TimeUnit::Second, None),
        "TIMESTAMP_MS" => D::Timestamp(TimeUnit::Millisecond, None),
        "TIMESTAMP_NS" => D::Timestamp(TimeUnit::Nanosecond, None),
        "TIMESTAMP WITH TIME ZONE" => D::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        d if d.starts_with("DECIMAL(") => match paren_numbers(d).as_deref() {
            Some([p, s]) if (1..=38).contains(p) && (0..=*p).contains(s) => D::Decimal128(
                u8::try_from(*p).unwrap_or(38),
                i8::try_from(*s).unwrap_or(0),
            ),
            _ => return Err(refuse(d)),
        },
        other => return Err(refuse(other)),
    })
}

fn postgres_type(c: &Declared, s: &str) -> std::result::Result<DataType, String> {
    use DataType as D;
    Ok(match s {
        "BOOLEAN" => D::Boolean,
        "SMALLINT" => D::Int16,
        "INTEGER" => D::Int32,
        "BIGINT" => D::Int64,
        "REAL" => D::Float32,
        "DOUBLE PRECISION" => D::Float64,
        "NUMERIC" => decimal(c, s)?,
        "TEXT" | "CHARACTER VARYING" | "CHARACTER" | "NAME" => D::Utf8,
        "BYTEA" => D::Binary,
        "DATE" => D::Date32,
        "TIME WITHOUT TIME ZONE" => D::Time64(TimeUnit::Microsecond),
        "TIMESTAMP WITHOUT TIME ZONE" => D::Timestamp(TimeUnit::Microsecond, None),
        "TIMESTAMP WITH TIME ZONE" => D::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        other => return Err(refuse(other)),
    })
}

fn mssql_type(c: &Declared, s: &str) -> std::result::Result<DataType, String> {
    use DataType as D;
    Ok(match s {
        "BIT" => D::Boolean,
        "TINYINT" => D::UInt8,
        "SMALLINT" => D::Int16,
        "INT" => D::Int32,
        "BIGINT" => D::Int64,
        "REAL" => D::Float32,
        "FLOAT" => D::Float64,
        "DECIMAL" | "NUMERIC" => decimal(c, s)?,
        "MONEY" => D::Decimal128(19, 4),
        "SMALLMONEY" => D::Decimal128(10, 4),
        "CHAR" | "VARCHAR" | "NCHAR" | "NVARCHAR" | "TEXT" | "NTEXT" => D::Utf8,
        "BINARY" | "VARBINARY" | "IMAGE" => D::Binary,
        "DATE" => D::Date32,
        // datetime counts 1/300 s: exact only in nanoseconds.
        "DATETIME" => D::Timestamp(TimeUnit::Nanosecond, None),
        "SMALLDATETIME" => D::Timestamp(TimeUnit::Second, None),
        "DATETIME2" => D::Timestamp(
            unit_of_precision(c.datetime_precision.unwrap_or(7)).ok_or_else(|| refuse(s))?,
            None,
        ),
        other => return Err(refuse(other)),
    })
}

/// SQLite: the declared type decides the Arrow type, by name first and then
/// by SQLite's affinity rules. A column with no declared type, or a
/// `NUMERIC`/`DECIMAL` without a scale, is refused: its values could be of
/// any storage class.
fn sqlite_type(c: &Declared, s: &str) -> std::result::Result<Field, String> {
    use DataType as D;
    let base = s.split('(').next().unwrap_or("").trim();
    let mut integer_unit = false;
    let dt = match base {
        "BOOLEAN" | "BOOL" => D::Boolean,
        "DATE" => D::Date32,
        "DATETIME" | "TIMESTAMP" => match paren_numbers(s).as_deref() {
            None => D::Timestamp(TimeUnit::Microsecond, None),
            Some([p]) => {
                integer_unit = true;
                D::Timestamp(unit_of_precision(*p).ok_or_else(|| refuse(s))?, None)
            }
            Some(_) => return Err(refuse(s)),
        },
        "DECIMAL" | "NUMERIC" => match paren_numbers(s).as_deref() {
            Some([p, sc]) => decimal(
                &Declared {
                    precision: Some(*p),
                    scale: Some(*sc),
                    ..c.clone()
                },
                s,
            )?,
            _ => {
                return Err(format!(
                    "its declared type {s} has no precision and scale; declare DECIMAL(p, s)"
                ))
            }
        },
        "" => {
            return Err(
                "it declares no type, so its values may be of any storage class; declare one"
                    .into(),
            )
        }
        b if b.contains("INT") => D::Int64,
        b if b.contains("CHAR") || b.contains("CLOB") || b.contains("TEXT") => D::Utf8,
        b if b.contains("BLOB") => D::Binary,
        b if b.contains("REAL") || b.contains("FLOA") || b.contains("DOUB") => D::Float64,
        other => return Err(refuse(other)),
    };
    let field = Field::new(&c.name, dt, c.nullable);
    Ok(if integer_unit {
        field.with_metadata([(INTEGER_UNIT.to_string(), "true".to_string())].into())
    } else {
        field
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn declared(spelled: &str) -> Declared {
        Declared {
            name: "c".into(),
            spelled: spelled.into(),
            nullable: true,
            precision: None,
            scale: None,
            datetime_precision: None,
        }
    }

    #[test]
    fn sqlite_declared_types() {
        let d = Dialect(EngineKind::Sqlite);
        let t = |s: &str| d.native_type(&declared(s)).map(|f| f.data_type().clone());
        assert_eq!(t("BIGINT"), Ok(DataType::Int64));
        assert_eq!(t("varchar(20)"), Ok(DataType::Utf8));
        assert_eq!(t("DECIMAL(10,2)"), Ok(DataType::Decimal128(10, 2)));
        assert_eq!(
            t("TIMESTAMP(3)"),
            Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
        );
        assert_eq!(
            t("TIMESTAMP"),
            Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
        );
        assert!(t("").is_err());
        assert!(t("NUMERIC").is_err());
        assert!(d
            .native_type(&declared("TIMESTAMP(3)"))
            .unwrap()
            .metadata()
            .contains_key(INTEGER_UNIT));
    }

    #[test]
    fn duckdb_declared_types() {
        let d = Dialect(EngineKind::Duckdb);
        let t = |s: &str| d.native_type(&declared(s)).map(|f| f.data_type().clone());
        assert_eq!(t("DECIMAL(18,3)"), Ok(DataType::Decimal128(18, 3)));
        assert_eq!(
            t("TIMESTAMP_MS"),
            Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
        );
        assert!(t("INTEGER[]").is_err());
        assert!(t("HUGEINT").unwrap_err().contains("view"));
    }

    #[test]
    fn identifiers_and_strings_are_quoted() {
        let d = Dialect(EngineKind::Postgres);
        assert_eq!(d.ident("a\"b"), "\"a\"\"b\"");
        assert_eq!(d.string("it's"), "'it''s'");
        let m = Dialect(EngineKind::Mssql);
        assert_eq!(m.ident("a]b"), "[a]]b]");
    }
}
