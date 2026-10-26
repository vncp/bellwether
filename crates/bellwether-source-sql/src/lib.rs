//! SQL tables as Bellwether sources (WP20, ADR 027).
//!
//! A table is a [`Source`](bellwether_types::Source): each declared
//! partition (Postgres), each key range of a configured column, or else the
//! whole table is a *unit* that plays a file of one row group. A unit's
//! fingerprint path is `sql://<engine>/<schema>.<table>/<unit>`, its size
//! its row count, and its etag its identity (a version column's maximum,
//! or a checksum of the unit's statistics); `tail_blake3` hashes the
//! identity string (`docs/format.md`, "SQL sources").
//!
//! - [`SqlSource::connect`] opens a `sql://` root ([`url`]) through ADBC
//!   (DuckDB, Postgres, Flight SQL), SQLite (rusqlite), or ODBC (SQL
//!   Server, feature `odbc`), and begins the run's snapshot.
//! - Statistics come from one aggregate query per unit (`MIN`, `MAX` in
//!   byte order, `COUNT(*) - COUNT(col)`, octet sums), so every bound is a
//!   bound of the data itself; the README says which numbers are counts,
//!   bounds, or measured.
//! - [`SqlSource::finish`] ends a run: with a snapshot nothing can have
//!   changed; without one every unit's identity is re-read and a change is
//!   refused ([`bellwether_types::Error::SourceChanged`]).
//! - [`measured`]: the measured cost models and the [`MeasuredScorer`].
//!
//! Memory (ADR 018): `read_columns` holds one unit's projected columns;
//! a unit over [`SqlOptions::max_unit_rows`] rows is refused at discovery,
//! naming key-range units. Discovery holds one aggregate row per unit and
//! one boundary per range unit.

#![forbid(unsafe_code)]
// Engine names (DuckDB, SQLite, PostgreSQL) read as prose here, as in the
// bench and profile crates.
#![allow(clippy::doc_markdown)]
#![cfg_attr(test, allow(clippy::unwrap_used))]

mod cast;
mod dialect;
pub mod driver;
pub mod measured;
mod source;
pub mod url;

pub use driver::Driver;
pub use measured::{
    ExplainScorer, MeasuredScore, MeasuredScorer, Measurement, TimingScorer, MEASURED_AGGREGATES,
    MEASURED_PLAN, MEASURED_WALL,
};
pub use source::{
    open_driver, SqlOptions, SqlSource, TableRun, DEFAULT_MAX_UNIT_BYTES, DEFAULT_MAX_UNIT_ROWS,
};
pub use url::{is_sql_root, EngineKind, SnapshotPolicy, SqlUrl, TableName};
