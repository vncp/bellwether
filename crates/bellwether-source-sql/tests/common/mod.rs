//! Shared test helpers: the engines' in-process databases, and drivers
//! that misbehave on purpose.
#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::missing_panics_doc,
    clippy::doc_markdown
)]

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use bellwether_source_sql::{open_driver, Driver, EngineKind, SnapshotPolicy, SqlUrl, TableName};
use bellwether_types::Result;

/// Where libduckdb is, for the DuckDB tests. DuckDB is read through its
/// ADBC driver, which is libduckdb itself; CI downloads it and sets
/// `BELLWETHER_DUCKDB_DRIVER`. Absent locally, the DuckDB tests say so and
/// return; absent in CI (`CI` set), they fail.
pub fn duckdb_driver() -> Option<String> {
    if let Ok(p) = std::env::var("BELLWETHER_DUCKDB_DRIVER") {
        if Path::new(&p).is_file() {
            return Some(p);
        }
    }
    for p in [
        "/opt/homebrew/lib/libduckdb.dylib",
        "/usr/local/lib/libduckdb.dylib",
        "/usr/local/lib/libduckdb.so",
        "/usr/lib/libduckdb.so",
    ] {
        if Path::new(p).is_file() {
            return Some(p.to_string());
        }
    }
    assert!(
        std::env::var_os("CI").is_none(),
        "CI must run the DuckDB tests: set BELLWETHER_DUCKDB_DRIVER to libduckdb"
    );
    eprintln!(
        "SKIPPED: no libduckdb found (set BELLWETHER_DUCKDB_DRIVER to libduckdb.so or \
         libduckdb.dylib from a DuckDB release to run the DuckDB tests)"
    );
    None
}

/// A raw URL value for `db=`.
pub fn q(s: &str) -> String {
    s.replace('%', "%25")
        .replace('&', "%26")
        .replace('#', "%23")
}

/// The `sql://` root of `table` in the DuckDB file `db`, with `extra`
/// parameters.
pub fn duck_root(db: &Path, table: &str, extra: &str) -> String {
    format!(
        "sql://duckdb/main.{table}?db={}{extra}",
        q(&db.to_string_lossy())
    )
}

pub fn sqlite_root(db: &Path, table: &str, extra: &str) -> String {
    format!(
        "sql://sqlite/main.{table}?db={}{extra}",
        q(&db.to_string_lossy())
    )
}

/// Runs statements on a DuckDB file through its ADBC driver (the same
/// in-process instance the sources use).
pub fn duck_exec(db: &Path, statements: &[&str]) {
    let url = duck_url(db);
    let mut d = open_driver(&url).unwrap();
    for s in statements {
        d.execute(s).unwrap_or_else(|e| panic!("{s}: {e}"));
    }
}

pub fn duck_url(db: &Path) -> SqlUrl {
    SqlUrl {
        engine: EngineKind::Duckdb,
        tables: vec![TableName {
            schema: "main".into(),
            table: "t".into(),
        }],
        db: db.to_string_lossy().into_owned(),
        driver: duckdb_driver(),
        range: None,
        version: None,
        snapshot: SnapshotPolicy::Transaction,
    }
}

/// Runs statements on a SQLite file (created when absent).
pub fn sqlite_exec(db: &Path, statements: &[&str]) {
    let c = rusqlite::Connection::open(db).unwrap();
    for s in statements {
        c.execute_batch(s).unwrap_or_else(|e| panic!("{s}: {e}"));
    }
}

pub fn tmp() -> (tempfile::TempDir, PathBuf) {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().to_path_buf();
    (d, p)
}

/// What a test driver does to a response.
pub type Hook = Box<dyn FnMut(&str, RecordBatch) -> Result<Option<RecordBatch>> + Send>;
/// Called before each query with its SQL.
pub type Before = Box<dyn FnMut(&str) + Send>;

/// A driver around a real one that rewrites responses or acts before a
/// query: the deliberate-break and corrupt-response tests.
pub struct Wrapped {
    pub inner: Box<dyn Driver>,
    pub hook: Hook,
    pub before: Before,
    pub log: Arc<Mutex<Vec<String>>>,
}

impl Wrapped {
    pub fn new(inner: Box<dyn Driver>) -> Self {
        Self {
            inner,
            hook: Box::new(|_, b| Ok(Some(b))),
            before: Box::new(|_| {}),
            log: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl Driver for Wrapped {
    fn query(
        &mut self,
        sql: &str,
        native: &SchemaRef,
        each: &mut dyn FnMut(RecordBatch) -> Result<()>,
    ) -> Result<()> {
        (self.before)(sql);
        self.log.lock().unwrap().push(sql.to_string());
        let hook = &mut self.hook;
        self.inner.query(sql, native, &mut |b| match hook(sql, b)? {
            Some(b) => each(b),
            None => Ok(()),
        })
    }

    fn execute(&mut self, sql: &str) -> Result<()> {
        self.log.lock().unwrap().push(sql.to_string());
        self.inner.execute(sql)
    }
}
