//! The driver boundary: everything that crosses from a database into this
//! crate passes through [`Driver`], so this is where a corrupt or
//! truncated response must become a typed error and never a panic or a
//! silent answer.
//!
//! A driver runs one SQL text and hands back Arrow batches. It may return
//! its own column types (ADBC and ODBC drivers do); the source then
//! converts every column to the type it asked for, exactly or with an
//! error (README, "Snapshot discipline"). The SQLite driver, whose values carry no
//! column type, builds the asked types directly.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use bellwether_types::{Error, Result};

use crate::url::{EngineKind, SqlUrl, TableName};

#[cfg(feature = "adbc")]
pub(crate) mod adbc;
#[cfg(feature = "odbc")]
pub(crate) mod odbc;
#[cfg(feature = "sqlite")]
pub(crate) mod sqlite;

/// One connection to a database: runs SQL, returns Arrow batches.
///
/// Implemented here over ADBC, SQLite, and ODBC; a caller may supply its
/// own (`SqlSource::with_driver`), for example to reach an engine through
/// a pool. Every method is called with the source's connection lock held,
/// one call at a time.
pub trait Driver: Send {
    /// Runs the query `sql` and calls `each` with every batch of its
    /// result, in order. `native` names the columns the query returns and
    /// the type each is declared as; a driver whose values carry their own
    /// types may return those instead (they are converted afterwards), one
    /// whose values carry none builds `native`'s types. An error from
    /// `each` stops the query and is returned.
    fn query(
        &mut self,
        sql: &str,
        native: &SchemaRef,
        each: &mut dyn FnMut(RecordBatch) -> Result<()>,
    ) -> Result<()>;

    /// Runs a statement that returns no rows (`BEGIN`, `ROLLBACK`).
    fn execute(&mut self, sql: &str) -> Result<()>;

    /// The Arrow schema the driver reports for `table`, for an engine with
    /// no information schema (Flight SQL). Others answer from the
    /// information schema and never call this.
    fn table_schema(&mut self, table: &TableName) -> Result<arrow::datatypes::Schema> {
        Err(Error::Source(format!(
            "this driver reports no table schema for {table}; read it from the information \
             schema"
        )))
    }
}

/// `message` with every occurrence of the connection replaced: a driver
/// may echo it, and it may hold a credential.
pub(crate) fn scrub(message: &str, secret: &str) -> String {
    if secret.is_empty() {
        message.to_string()
    } else {
        message.replace(secret, "<connection>")
    }
}

/// Opens the driver for `url`'s engine. With `create`, a DuckDB database
/// file that does not exist is created; a source passes `false`.
pub(crate) fn open(url: &SqlUrl, create: bool) -> Result<Box<dyn Driver>> {
    match url.engine {
        #[cfg(feature = "sqlite")]
        EngineKind::Sqlite => Ok(Box::new(sqlite::SqliteDriver::open(&url.db)?)),
        #[cfg(feature = "adbc")]
        EngineKind::Duckdb | EngineKind::Postgres | EngineKind::FlightSql => {
            Ok(Box::new(adbc::AdbcDriver::open(url, create)?))
        }
        #[cfg(feature = "odbc")]
        EngineKind::Mssql => Ok(Box::new(odbc::OdbcDriver::open(&url.db)?)),
        #[allow(unreachable_patterns)]
        other => {
            let _ = create;
            Err(Error::Source(format!(
                "this build of bellwether reads no {other} tables: it was built without the \
                 `{}` feature of bellwether-source-sql; rebuild with it (`cargo build --features \
                 {}`)",
                feature_of(other),
                feature_of(other)
            )))
        }
    }
}

fn feature_of(engine: EngineKind) -> &'static str {
    match engine {
        EngineKind::Sqlite => "sqlite",
        EngineKind::Duckdb | EngineKind::Postgres | EngineKind::FlightSql => "adbc",
        EngineKind::Mssql => "odbc",
    }
}

/// Collects every batch of `sql`. Only for results bounded by the caller:
/// one unit's projected columns, one row of aggregates, a catalog listing.
pub(crate) fn collect(
    driver: &mut dyn Driver,
    sql: &str,
    native: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    let mut out = Vec::new();
    driver.query(sql, native, &mut |b| {
        out.push(b);
        Ok(())
    })?;
    Ok(out)
}
