//! The `sql://` root: which engine, which tables, how to connect, and how
//! to split each table into units (`docs/format.md`, "SQL sources").
//!
//! ```text
//! sql://<engine>/<schema>.<table>[,<schema>.<table>...]?db=<connection>
//!       [&driver=<library>][&range=<column>&units=<n>][&version=<column>]
//!       [&snapshot=transaction|reread]
//! ```
//!
//! `db` is a file path (SQLite, DuckDB), a connection URI (Postgres, Flight
//! SQL), or an ODBC connection string (SQL Server); `db=env:NAME` reads it
//! from the environment variable `NAME`, so a password never appears in a
//! command line or a record. Nothing after `?` is part of the root's
//! identity: the root a record names is `sql://<engine>/<tables>`.

use std::fmt;
use std::fmt::Write as _;

use bellwether_types::{Error, Result};

/// The database engine behind a `sql://` root.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EngineKind {
    /// SQLite, in process (rusqlite with the bundled SQLite).
    Sqlite,
    /// DuckDB, in process, through its ADBC driver (libduckdb).
    Duckdb,
    /// Postgres, through the ADBC Postgres driver.
    Postgres,
    /// Any Flight SQL server, through the ADBC Flight SQL driver.
    FlightSql,
    /// SQL Server, through ODBC and arrow-odbc.
    Mssql,
}

impl EngineKind {
    /// Every engine, in documentation order.
    pub const ALL: [Self; 5] = [
        Self::Sqlite,
        Self::Duckdb,
        Self::Postgres,
        Self::FlightSql,
        Self::Mssql,
    ];

    /// The engine's name in a `sql://` root.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::Duckdb => "duckdb",
            Self::Postgres => "postgres",
            Self::FlightSql => "flightsql",
            Self::Mssql => "mssql",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "sqlite" => Some(Self::Sqlite),
            "duckdb" => Some(Self::Duckdb),
            "postgres" | "postgresql" => Some(Self::Postgres),
            "flightsql" => Some(Self::FlightSql),
            "mssql" | "sqlserver" => Some(Self::Mssql),
            _ => None,
        }
    }

    /// The schema a table name without one is read from.
    #[must_use]
    pub fn default_schema(self) -> &'static str {
        match self {
            Self::Sqlite | Self::Duckdb => "main",
            Self::Postgres | Self::FlightSql => "public",
            Self::Mssql => "dbo",
        }
    }
}

impl fmt::Display for EngineKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A table, by schema and name, exactly as the catalog spells them.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableName {
    /// Schema (`main` in SQLite and DuckDB, `public` in Postgres, `dbo` in
    /// SQL Server by default).
    pub schema: String,
    /// Table (or view) name.
    pub table: String,
}

impl fmt::Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.schema, self.table)
    }
}

/// How a run over a table holds the table still.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotPolicy {
    /// One read transaction at `REPEATABLE READ` or the engine's snapshot
    /// equivalent, where the engine has one; otherwise the identities are
    /// re-read at the end.
    #[default]
    Transaction,
    /// No transaction: every unit's identity is re-read when the run ends
    /// and the run is refused if any changed. For a SQLite database in
    /// rollback-journal mode, where a read transaction makes writers wait.
    Reread,
}

/// How units are cut from each table when the engine declares no
/// partitions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeUnits {
    /// The column whose values cut the table.
    pub column: String,
    /// How many units (at most; equal boundary values merge).
    pub units: u32,
}

/// A parsed `sql://` root.
#[derive(Clone, PartialEq, Eq)]
pub struct SqlUrl {
    /// The engine.
    pub engine: EngineKind,
    /// The tables, in the order given.
    pub tables: Vec<TableName>,
    /// The connection: a path, a URI, or an ODBC connection string, with
    /// `env:` resolved.
    pub db: String,
    /// The ADBC driver library (a path or a name), when not the default.
    pub driver: Option<String>,
    /// Key-range units, when asked for.
    pub range: Option<RangeUnits>,
    /// A column whose maximum per unit is the unit's identity.
    pub version: Option<String>,
    /// The snapshot discipline.
    pub snapshot: SnapshotPolicy,
}

impl fmt::Debug for SqlUrl {
    // The connection may hold a password: never printed.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlUrl")
            .field("root", &self.root())
            .field("driver", &self.driver)
            .field("range", &self.range)
            .field("version", &self.version)
            .field("snapshot", &self.snapshot)
            .finish_non_exhaustive()
    }
}

/// The largest number of range units a root may ask for.
pub const MAX_UNITS: u32 = 4096;

/// Whether `s` names a `sql://` root.
#[must_use]
pub fn is_sql_root(s: &str) -> bool {
    s.get(..6).is_some_and(|p| p.eq_ignore_ascii_case("sql://"))
}

fn usage(given: &str, what: &str) -> Error {
    Error::Source(format!(
        "{}: {what}; a table root reads sql://<engine>/<schema>.<table>?db=<connection> \
         (engines: sqlite, duckdb, postgres, flightsql, mssql)",
        redact(given)
    ))
}

/// `given` with everything after `?` removed: the part of a root that may
/// be printed.
#[must_use]
pub fn redact(given: &str) -> &str {
    given.split_once('?').map_or(given, |(root, _)| root)
}

impl SqlUrl {
    /// Parses a `sql://` root. Unknown parameters, a missing `db`, an
    /// unknown engine, or a malformed table are errors naming the fix.
    #[allow(clippy::too_many_lines)]
    pub fn parse(given: &str) -> Result<Self> {
        if !is_sql_root(given) {
            return Err(usage(given, "not a sql:// root"));
        }
        let rest = &given[6..];
        let (path, query) = rest.split_once('?').unwrap_or((rest, ""));
        let (engine, tables) = path
            .split_once('/')
            .ok_or_else(|| usage(given, "no table after the engine"))?;
        let engine = EngineKind::parse(engine)
            .ok_or_else(|| usage(given, &format!("unknown engine {engine:?}")))?;
        let mut parsed = Vec::new();
        for t in tables.trim_end_matches('/').split(',') {
            let t = t.trim();
            if t.is_empty() {
                return Err(usage(given, "an empty table name"));
            }
            let (schema, table) = match t.split_once('.') {
                Some((s, tb)) => (s, tb),
                None => (engine.default_schema(), t),
            };
            if schema.is_empty() || table.is_empty() || table.contains('.') {
                return Err(usage(
                    given,
                    &format!("table {t:?} is not <schema>.<table> (one dot; %2E in a name)"),
                ));
            }
            let schema = decode(schema).map_err(|e| usage(given, &e))?;
            let table = decode(table).map_err(|e| usage(given, &e))?;
            let name = TableName { schema, table };
            if parsed.contains(&name) {
                return Err(usage(given, &format!("table {name} is listed twice")));
            }
            parsed.push(name);
        }
        let mut db = None;
        let mut driver = None;
        let mut range_col = None;
        let mut units = None;
        let mut version = None;
        let mut snapshot = SnapshotPolicy::default();
        for (k, v) in url::form_urlencoded::parse(query.as_bytes()) {
            let v = v.into_owned();
            let seen = match k.as_ref() {
                "db" => db.replace(v).is_some(),
                "driver" => driver.replace(v).is_some(),
                "range" => range_col.replace(v).is_some(),
                "version" => version.replace(v).is_some(),
                "units" => {
                    let n: u32 = v
                        .parse()
                        .ok()
                        .filter(|n| (1..=MAX_UNITS).contains(n))
                        .ok_or_else(|| {
                            usage(
                                given,
                                &format!("units={v:?} is not a whole number in 1..={MAX_UNITS}"),
                            )
                        })?;
                    units.replace(n).is_some()
                }
                "snapshot" => {
                    snapshot = match v.as_str() {
                        "transaction" => SnapshotPolicy::Transaction,
                        "reread" => SnapshotPolicy::Reread,
                        other => {
                            return Err(usage(
                                given,
                                &format!("snapshot={other:?} is neither transaction nor reread"),
                            ))
                        }
                    };
                    false
                }
                other => {
                    return Err(usage(
                        given,
                        &format!(
                            "unknown parameter {other:?} (known: db, driver, range, units, \
                             version, snapshot)"
                        ),
                    ))
                }
            };
            if seen {
                return Err(usage(given, &format!("parameter {k:?} is given twice")));
            }
        }
        let db = db.ok_or_else(|| usage(given, "no db= parameter naming the database"))?;
        let db = match db.strip_prefix("env:") {
            Some(var) => std::env::var(var).map_err(|_| {
                usage(
                    given,
                    &format!("db=env:{var} names an environment variable that is not set"),
                )
            })?,
            None => db,
        };
        let range = match (range_col, units) {
            (Some(column), Some(units)) => Some(RangeUnits { column, units }),
            (None, None) => None,
            (Some(_), None) => {
                return Err(usage(given, "range= needs units=<n> (how many key ranges)"))
            }
            (None, Some(_)) => {
                return Err(usage(
                    given,
                    "units= needs range=<column> to cut the table by",
                ))
            }
        };
        if driver.is_some() && matches!(engine, EngineKind::Sqlite | EngineKind::Mssql) {
            return Err(usage(
                given,
                &format!(
                    "driver= names an ADBC driver library, and {engine} is not read through ADBC"
                ),
            ));
        }
        Ok(Self {
            engine,
            tables: parsed,
            db,
            driver,
            range,
            version,
            snapshot,
        })
    }

    /// The root a record names: `sql://<engine>/<schema>.<table>[,...]`,
    /// with no connection parameter.
    #[must_use]
    pub fn root(&self) -> String {
        let tables: Vec<String> = self.tables.iter().map(ToString::to_string).collect();
        format!("sql://{}/{}", self.engine, tables.join(","))
    }

    /// The statements a human runs to give each table an index on
    /// `columns`: what an attest-only apply of a key names (ADR 027).
    #[must_use]
    pub fn create_index_statements(&self, columns: &[String]) -> Vec<String> {
        let d = crate::dialect::Dialect(self.engine);
        self.tables
            .iter()
            .map(|t| d.create_index(t, columns))
            .collect()
    }

    /// BLAKE3 of the connection with its credentials removed: a URI's
    /// `user:password@`, query parameters and ODBC keys naming a password,
    /// token, secret, or key. Two databases holding tables of one name are
    /// then two identities, and no credential is hashed into a record.
    #[must_use]
    pub fn connection_identity(&self) -> bellwether_types::Hash {
        bellwether_types::Hash::of(strip_credentials(&self.db).as_bytes())
    }

    /// What, besides the tables, decides the units and their identities:
    /// part of the source configuration a cache is keyed by.
    #[must_use]
    pub fn unit_config(&self) -> String {
        let mut s = String::new();
        if let Some(r) = &self.range {
            let _ = write!(s, ":range={}:units={}", r.column, r.units);
        }
        if let Some(v) = &self.version {
            let _ = write!(s, ":version={v}");
        }
        s
    }
}

fn secret_key(k: &str) -> bool {
    let k = k.trim().to_ascii_lowercase();
    [
        "pass",
        "pwd",
        "token",
        "secret",
        "key",
        "auth",
        "credential",
    ]
    .iter()
    .any(|w| k.contains(w))
}

/// `db` without credentials (see [`SqlUrl::connection_identity`]).
#[must_use]
pub fn strip_credentials(db: &str) -> String {
    let (base, query) = db.split_once('?').unwrap_or((db, ""));
    let base = match base.split_once("://") {
        Some((scheme, rest)) => match rest.split_once('@') {
            Some((_, host))
                if !rest[..rest.find('/').unwrap_or(rest.len())].is_empty()
                    && rest.find('@') < rest.find('/').or(Some(usize::MAX)) =>
            {
                format!("{scheme}://{host}")
            }
            _ => base.to_string(),
        },
        None => base.to_string(),
    };
    // ODBC: key=value pairs separated by `;`.
    let base = if base.contains(';') && base.contains('=') {
        base.split(';')
            .filter(|kv| !kv.split_once('=').is_some_and(|(k, _)| secret_key(k)))
            .collect::<Vec<_>>()
            .join(";")
    } else {
        base
    };
    let query: Vec<&str> = query
        .split('&')
        .filter(|kv| !kv.is_empty() && !secret_key(kv.split_once('=').map_or(*kv, |(k, _)| k)))
        .collect();
    if query.is_empty() {
        base
    } else {
        format!("{base}?{}", query.join("&"))
    }
}

/// A root as given, its `db=` value replaced by `<connection>` unless it
/// names an environment variable (`db=env:NAME`): what may be printed.
#[must_use]
pub fn redact_db(given: &str) -> String {
    let Some((root, query)) = given.split_once('?') else {
        return given.to_string();
    };
    let kept: Vec<String> = query
        .split('&')
        .map(|kv| match kv.split_once('=') {
            Some(("db", v)) if !v.starts_with("env:") => "db=<connection>".to_string(),
            _ => kv.to_string(),
        })
        .collect();
    format!("{root}?{}", kept.join("&"))
}

/// Percent-decodes one path segment (`%2E` is a literal dot inside a
/// name; a bare `.` separates schema and table).
fn decode(s: &str) -> std::result::Result<String, String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = s
                .get(i + 1..i + 3)
                .and_then(|h| u8::from_str_radix(h, 16).ok())
                .ok_or_else(|| format!("{s:?} holds a malformed percent escape"))?;
            out.push(hex);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    let d = String::from_utf8(out).map_err(|_| format!("{s:?} does not decode to UTF-8"))?;
    if d.contains('\0') {
        return Err("a table name holds a NUL byte".to_string());
    }
    Ok(d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_full_root() {
        let u = SqlUrl::parse(
            "sql://duckdb/main.trips,trips_2?db=/tmp/x.duckdb&range=pickup&units=12&version=v",
        )
        .unwrap();
        assert_eq!(u.engine, EngineKind::Duckdb);
        assert_eq!(u.tables[1].to_string(), "main.trips_2");
        assert_eq!(u.db, "/tmp/x.duckdb");
        assert_eq!(u.range.as_ref().unwrap().units, 12);
        assert_eq!(u.root(), "sql://duckdb/main.trips,main.trips_2");
        assert_eq!(u.snapshot, SnapshotPolicy::Transaction);
    }

    #[test]
    fn refuses_what_it_cannot_read() {
        for bad in [
            "sql://oracle/a.b?db=x",
            "sql://duckdb/a.b",
            "sql://duckdb/a.b?db=x&colour=red",
            "sql://duckdb/a.b?db=x&range=c",
            "sql://duckdb/a.b?db=x&units=3",
            "sql://duckdb/a.b?db=x&units=0",
            "sql://duckdb/a.b.c?db=x",
            "sql://duckdb/?db=x",
            "sql://duckdb/a.b,a.b?db=x",
            "sql://sqlite/a.b?db=x&driver=y",
            "sql://duckdb/a.b?db=x&db=y",
            "sql://duckdb/a.b?db=env:BELLWETHER_TEST_UNSET_VARIABLE",
            "file:///tmp",
        ] {
            let e = SqlUrl::parse(bad).unwrap_err().to_string();
            assert!(!e.contains("db=x&"), "{e}");
            assert!(e.contains("sql://<engine>"), "{e}");
        }
    }

    #[test]
    fn the_connection_is_never_printed() {
        let u = SqlUrl::parse("sql://postgres/public.t?db=postgresql://u:secret@h/d").unwrap();
        assert!(!format!("{u:?}").contains("secret"));
        assert!(!u.root().contains("secret"));
        let e = SqlUrl::parse("sql://postgres/public.t?db=postgresql://u:secret@h/d&x=1")
            .unwrap_err()
            .to_string();
        assert!(!e.contains("secret"), "{e}");
    }
}
