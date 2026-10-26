//! DuckDB, Postgres, and Flight SQL through the ADBC driver manager
//! (`adbc_driver_manager`, Apache Arrow ADBC). The manager loads the
//! engine's ADBC driver library at run time and hands back Arrow batches
//! in the workspace's own Arrow version, so nothing is converted between
//! Arrow copies and no native code is linked at build time.
//!
//! Which library: `driver=` in the root, else an environment variable
//! (`BELLWETHER_DUCKDB_DRIVER`, `BELLWETHER_ADBC_POSTGRESQL_DRIVER`,
//! `BELLWETHER_ADBC_FLIGHTSQL_DRIVER`), else, for DuckDB, `libduckdb` in the
//! usual install directories, else the driver manager's search by name
//! (driver manifests, then the system library path).
//!
//! DuckDB allows one open instance of a database file per process, so the
//! instance is shared: every source over the same file in this process
//! connects through one `ManagedDatabase`, closed when the last one drops.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, Weak};

use adbc_core::options::{AdbcVersion, OptionDatabase, OptionValue};
use adbc_core::{Connection as _, Database as _, Driver as _, Statement as _, LOAD_FLAG_DEFAULT};
use adbc_driver_manager::{ManagedConnection, ManagedDatabase, ManagedDriver};
use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use bellwether_types::{Error, Result};

use super::Driver;
use crate::url::{EngineKind, SqlUrl, TableName};

/// One database instance, shared by every source over the same target.
struct Shared {
    database: ManagedDatabase,
    /// The canonical database path (DuckDB), which a driver may print.
    path: String,
    // Keeps the library loaded as long as the database lives.
    _driver: ManagedDriver,
}

type Instances = Mutex<HashMap<(EngineKind, String, bool), Weak<Shared>>>;

static INSTANCES: LazyLock<Instances> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// A connection through an ADBC driver.
pub(crate) struct AdbcDriver {
    engine: EngineKind,
    /// The connection, scrubbed from every error message.
    secrets: Vec<String>,
    conn: ManagedConnection,
    _shared: Arc<Shared>,
}

/// An ADBC error as a source error, with every spelling of the connection
/// (`secrets`: as given, and the canonical path a driver prints) replaced:
/// a driver may echo it. A file another process holds read-write gets the
/// next step.
fn adbc_err(
    engine: EngineKind,
    secrets: &[String],
) -> impl Fn(adbc_core::error::Error) -> Error + '_ {
    move |e| {
        let mut m = scrub_all(e.message.trim(), secrets);
        if m.contains("Could not set lock") || m.contains("Conflicting lock") {
            m.push_str(
                "; another process holds the database file read-write: stop it, or read a \
                 copy of the file",
            );
        }
        Error::Source(format!("{engine} (ADBC): {m}"))
    }
}

/// `message` with every secret replaced, longest first, so a path that
/// contains another (`/private/var/x` and `/var/x`) is replaced whole.
fn scrub_all(message: &str, secrets: &[String]) -> String {
    let mut sorted: Vec<&String> = secrets.iter().filter(|s| !s.is_empty()).collect();
    sorted.sort_by_key(|s| std::cmp::Reverse(s.len()));
    sorted
        .iter()
        .fold(message.to_string(), |m, s| super::scrub(&m, s))
}

/// The driver library's environment variable, default name, and init
/// symbol for `engine`.
fn library(engine: EngineKind) -> (&'static str, &'static str, Option<&'static [u8]>) {
    match engine {
        EngineKind::Duckdb => (
            "BELLWETHER_DUCKDB_DRIVER",
            "duckdb",
            Some(b"duckdb_adbc_init"),
        ),
        EngineKind::Postgres => (
            "BELLWETHER_ADBC_POSTGRESQL_DRIVER",
            "adbc_driver_postgresql",
            None,
        ),
        _ => (
            "BELLWETHER_ADBC_FLIGHTSQL_DRIVER",
            "adbc_driver_flightsql",
            None,
        ),
    }
}

/// Directories DuckDB's own installers put `libduckdb` in.
fn duckdb_candidates() -> Vec<PathBuf> {
    let names = ["libduckdb.dylib", "libduckdb.so"];
    [
        "/opt/homebrew/lib",
        "/usr/local/lib",
        "/usr/lib",
        "/usr/lib/x86_64-linux-gnu",
    ]
    .iter()
    .flat_map(|d| names.iter().map(move |n| Path::new(d).join(n)))
    .filter(|p| p.is_file())
    .collect()
}

fn load(url: &SqlUrl) -> Result<ManagedDriver> {
    let engine = url.engine;
    let (var, name, entry) = library(engine);
    let given = url
        .driver
        .clone()
        .or_else(|| std::env::var(var).ok().filter(|v| !v.is_empty()));
    let try_version = |load: &dyn Fn(AdbcVersion) -> adbc_core::error::Result<ManagedDriver>| {
        load(AdbcVersion::V110).or_else(|_| load(AdbcVersion::V100))
    };
    let missing = |detail: String| {
        Error::Source(format!(
            "{engine}: cannot load the ADBC driver ({detail}); install it and pass \
             driver=<path to the library> in the root, or set {var}{}",
            if engine == EngineKind::Duckdb {
                " (the driver is libduckdb itself: libduckdb.so or libduckdb.dylib from a DuckDB \
                 release)"
            } else {
                ""
            }
        ))
    };
    if let Some(path) = given {
        return try_version(&|v| ManagedDriver::load_dynamic_from_filename(&path, entry, v))
            .map_err(|e| missing(format!("{path}: {}", e.message.trim())));
    }
    if engine == EngineKind::Duckdb {
        for p in duckdb_candidates() {
            if let Ok(d) = try_version(&|v| ManagedDriver::load_dynamic_from_filename(&p, entry, v))
            {
                return Ok(d);
            }
        }
    }
    try_version(&|v| ManagedDriver::load_from_name(name, entry, v, LOAD_FLAG_DEFAULT, None))
        .map_err(|e| missing(format!("{name} not found: {}", e.message.trim())))
}

fn shared(url: &SqlUrl, create: bool) -> Result<Arc<Shared>> {
    let engine = url.engine;
    // A DuckDB file is shared per canonical path; a server is never shared
    // (each source opens its own session).
    let key = match engine {
        EngineKind::Duckdb if url.db != ":memory:" => {
            let path = Path::new(&url.db);
            let missing = |e: std::io::Error| {
                Error::Source(format!(
                    "duckdb: no database file at the db= path ({}); pass db=<path to the .duckdb \
                     file>",
                    e.kind()
                ))
            };
            let canonical = if create && !path.exists() {
                let parent = match path.parent() {
                    Some(p) if !p.as_os_str().is_empty() => p,
                    _ => Path::new("."),
                };
                std::fs::canonicalize(parent)
                    .map_err(missing)?
                    .join(path.file_name().unwrap_or_default())
            } else {
                std::fs::canonicalize(path).map_err(missing)?
            };
            canonical.to_string_lossy().into_owned()
        }
        _ => String::new(),
    };
    let mut map = INSTANCES
        .lock()
        .map_err(|_| Error::Other("the ADBC instance map is poisoned".into()))?;
    if !key.is_empty() {
        // A source reuses a read-write instance this process already has
        // open (two instances of one file would not see each other's
        // writes), else opens its own read-only one.
        let modes: &[bool] = if create { &[true] } else { &[true, false] };
        if let Some(s) = modes
            .iter()
            .find_map(|m| map.get(&(engine, key.clone(), *m)).and_then(Weak::upgrade))
        {
            return Ok(s);
        }
    }
    let mut driver = load(url)?;
    let option = match engine {
        EngineKind::Duckdb => OptionDatabase::Other("path".into()),
        _ => OptionDatabase::Uri,
    };
    let target = if key.is_empty() {
        url.db.clone()
    } else {
        key.clone()
    };
    let mut opts = vec![(option, OptionValue::String(target))];
    if engine == EngineKind::Duckdb && !create && !key.is_empty() {
        // A source only reads: other processes may hold the file too.
        opts.push((
            OptionDatabase::Other("access_mode".into()),
            OptionValue::String("READ_ONLY".into()),
        ));
    }
    let database = driver
        .new_database_with_opts(opts)
        .map_err(adbc_err(engine, &[url.db.clone(), key.clone()]))?;
    let s = Arc::new(Shared {
        database,
        path: key.clone(),
        _driver: driver,
    });
    if !key.is_empty() {
        map.retain(|_, w| w.strong_count() > 0);
        map.insert((engine, key, create), Arc::downgrade(&s));
    }
    Ok(s)
}

impl AdbcDriver {
    /// Connects; with `create`, a DuckDB file that does not exist is
    /// created (a source never creates one).
    pub(crate) fn open(url: &SqlUrl, create: bool) -> Result<Self> {
        let shared = shared(url, create)?;
        let conn = shared
            .database
            .new_connection()
            .map_err(adbc_err(url.engine, &[url.db.clone(), shared.path.clone()]))?;
        Ok(Self {
            engine: url.engine,
            secrets: vec![url.db.clone(), shared.path.clone()],
            conn,
            _shared: shared,
        })
    }
}

impl Driver for AdbcDriver {
    fn query(
        &mut self,
        sql: &str,
        _native: &SchemaRef,
        each: &mut dyn FnMut(RecordBatch) -> Result<()>,
    ) -> Result<()> {
        let e = adbc_err(self.engine, &self.secrets);
        let mut st = self.conn.new_statement().map_err(&e)?;
        st.set_sql_query(sql).map_err(&e)?;
        let reader = st.execute().map_err(&e)?;
        for batch in reader {
            let batch = batch.map_err(|err| {
                Error::Source(format!(
                    "{} (ADBC): reading a result: {}",
                    self.engine,
                    scrub_all(&err.to_string(), &self.secrets)
                ))
            })?;
            each(batch)?;
        }
        Ok(())
    }

    fn execute(&mut self, sql: &str) -> Result<()> {
        let e = adbc_err(self.engine, &self.secrets);
        let mut st = self.conn.new_statement().map_err(&e)?;
        st.set_sql_query(sql).map_err(&e)?;
        st.execute_update().map_err(&e)?;
        Ok(())
    }

    fn table_schema(&mut self, table: &TableName) -> Result<Schema> {
        self.conn
            .get_table_schema(None, Some(&table.schema), &table.table)
            .map_err(adbc_err(self.engine, &self.secrets))
    }
}
