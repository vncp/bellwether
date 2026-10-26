//! SQL Server (or any ODBC data source) through `odbc-api` and
//! `arrow-odbc`, which fetch result sets in bulk into Arrow batches.
//! `db` is an ODBC connection string (`Driver={ODBC Driver 18 for SQL
//! Server};Server=...;...`). Links the system ODBC driver manager.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow_odbc::odbc_api::{self, Connection, ConnectionOptions};
use arrow_odbc::OdbcReaderBuilder;
use bellwether_types::{Error, Result};

use super::Driver;

/// A connection through the ODBC driver manager.
pub(crate) struct OdbcDriver {
    conn: Connection<'static>,
}

fn err(e: impl std::fmt::Display) -> Error {
    Error::Source(format!("mssql (ODBC): {e}"))
}

impl OdbcDriver {
    pub(crate) fn open(connection_string: &str) -> Result<Self> {
        let env = odbc_api::environment().map_err(err)?;
        let conn = env
            .connect_with_connection_string(connection_string, ConnectionOptions::default())
            .map_err(|e| {
                Error::Source(format!(
                    "mssql (ODBC): cannot connect ({}); check the ODBC driver is installed and \
                     the connection string in db= (or the variable db=env:NAME names)",
                    super::scrub(&e.to_string(), connection_string)
                ))
            })?;
        Ok(Self { conn })
    }
}

impl Driver for OdbcDriver {
    fn query(
        &mut self,
        sql: &str,
        _native: &SchemaRef,
        each: &mut dyn FnMut(RecordBatch) -> Result<()>,
    ) -> Result<()> {
        let Some(cursor) = self.conn.execute(sql, (), None).map_err(err)? else {
            return Err(err(format!("the query returned no result set: {sql}")));
        };
        let reader = OdbcReaderBuilder::new().build(cursor).map_err(err)?;
        for batch in reader {
            each(batch.map_err(err)?)?;
        }
        Ok(())
    }

    fn execute(&mut self, sql: &str) -> Result<()> {
        self.conn.execute(sql, (), None).map_err(err)?;
        Ok(())
    }
}
