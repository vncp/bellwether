//! The reference adapter: ClickHouse over its HTTP interface.
//!
//! - **submit**: the query text as the request body, `query_id` = the tag,
//!   `readonly=2` (no write, no DDL), `default_format=Null` (the engine
//!   produces and discards the rows), and the caches that change what a
//!   run reads turned off: `use_query_cache=0`, and
//!   `use_query_condition_cache=0` where the server has it.
//! - **export**: the same request with `default_format=Parquet`, streamed
//!   into `<dir>/part-0.parquet` and checked to be whole Parquet: one run,
//!   logged under its own tag.
//! - **stats**: `system.query_log` by `query_id` (`read_rows`,
//!   `read_bytes`, `memory_usage`, `query_duration_ms`, `result_rows`, CPU
//!   from `ProfileEvents`), after `SYSTEM FLUSH LOGS` where the user may,
//!   else by waiting for the log's flush (bounded). The finished entry
//!   must be the only one under the tag, logged for this user with the
//!   exact text submitted, and not served from a cache: a `SETTINGS`
//!   clause in the agent's text overrides the request's settings, so a
//!   run that hit the query cache or the query condition cache, or that
//!   turned the query cache on, is refused (`cache_served`).
//! - **units**: one per partition of each MergeTree table, from
//!   `system.parts` (active parts). Identity: rows, the lowest and highest
//!   block numbers, and the highest mutation version, which an insert, a
//!   delete, or a mutation changes and a background merge does not.
//!
//! Every metadata statement is fixed text with the database and tables
//! passed as query parameters (`{db:String}`), never spliced.

use std::collections::BTreeMap;
use std::io::Read as _;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_types::{
    ColumnStats, Error, FileFingerprint, Hash, QueryStats, Result, RowGroupMeta, Source,
};
use serde_json::{Map, Value};

use crate::http::{self, METADATA};
use crate::uri::EngineUri;
use crate::{
    check_query, denied_scratch, export_dir, listed, refused, Exported, MeasuredEngine,
    ResultHandle, Tag,
};

/// The model of every ClickHouse cost.
pub const MODEL: &str = "clickhouse.query_log";

/// How long [`MeasuredEngine::stats`] waits for the query log to record a
/// tag: the server's default flush interval (7.5 s) four times over.
const LOG_WAIT: Duration = Duration::from_secs(30);

/// ClickHouse's error code for a refused privilege.
const ACCESS_DENIED: i64 = 497;

/// One partition of one table: a unit.
#[derive(Clone, Debug)]
struct Unit {
    table: String,
    partition: String,
    meta: RowGroupMeta,
}

/// A ClickHouse server as a measured source.
pub struct ClickHouse {
    root: String,
    /// `clickhouse://host/db`: the prefix of every unit's path.
    db_root: String,
    base: String,
    database: String,
    tables: Vec<String>,
    user: String,
    password: Option<String>,
    query_timeout: Option<Duration>,
    max_unit_rows: u64,
    /// The cache settings every run of the agent's text turns off, beyond
    /// the query cache (off on every request): those this server has.
    caches_off: Vec<(&'static str, &'static str)>,
    client: reqwest::blocking::Client,
    units: Mutex<Option<Arc<Vec<Unit>>>>,
}

impl std::fmt::Debug for ClickHouse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouse")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

/// A column's type, as ClickHouse declares it and as the source reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ColumnType {
    arrow: DataType,
    nullable: bool,
    /// How the column is selected so ClickHouse writes exactly `arrow`'s
    /// values: `{c}` stands for the quoted column name.
    expr: &'static str,
}

/// Maps a ClickHouse type to the Arrow type the source reads it as.
fn column_type(declared: &str) -> ColumnType {
    let mut t = declared.trim();
    let mut nullable = false;
    loop {
        if let Some(inner) = t
            .strip_prefix("Nullable(")
            .and_then(|s| s.strip_suffix(')'))
        {
            nullable = true;
            t = inner;
        } else if let Some(inner) = t
            .strip_prefix("LowCardinality(")
            .and_then(|s| s.strip_suffix(')'))
        {
            t = inner;
        } else {
            break;
        }
    }
    let plain = |arrow: DataType| ColumnType {
        arrow,
        nullable,
        expr: "{c}",
    };
    let text = ColumnType {
        arrow: DataType::Utf8,
        nullable,
        expr: "toString({c})",
    };
    let args = |prefix: &str| -> Option<Vec<String>> {
        t.strip_prefix(prefix)
            .and_then(|s| s.strip_suffix(')'))
            .map(|s| s.split(',').map(|a| a.trim().to_string()).collect())
    };
    match t {
        "UInt8" => plain(DataType::UInt8),
        "UInt16" => plain(DataType::UInt16),
        "UInt32" => plain(DataType::UInt32),
        "UInt64" => plain(DataType::UInt64),
        "Int8" => plain(DataType::Int8),
        "Int16" => plain(DataType::Int16),
        "Int32" => plain(DataType::Int32),
        "Int64" => plain(DataType::Int64),
        "Float32" => plain(DataType::Float32),
        "Float64" => plain(DataType::Float64),
        "Bool" => plain(DataType::Boolean),
        "String" => plain(DataType::Utf8),
        "Date" | "Date32" => plain(DataType::Date32),
        "DateTime" => ColumnType {
            arrow: DataType::Timestamp(TimeUnit::Second, None),
            nullable,
            expr: "toInt64(toUnixTimestamp({c}))",
        },
        _ if t.starts_with("DateTime(") => ColumnType {
            arrow: DataType::Timestamp(TimeUnit::Second, None),
            nullable,
            expr: "toInt64(toUnixTimestamp({c}))",
        },
        _ if t.starts_with("DateTime64(") => {
            let p = args("DateTime64(").and_then(|a| a.first().and_then(|p| p.parse::<u8>().ok()));
            let (unit, expr) = match p {
                Some(0) => (TimeUnit::Second, "toInt64(toUnixTimestamp({c}))"),
                Some(3) => (TimeUnit::Millisecond, "toUnixTimestamp64Milli({c})"),
                Some(6) => (TimeUnit::Microsecond, "toUnixTimestamp64Micro({c})"),
                Some(9) => (TimeUnit::Nanosecond, "toUnixTimestamp64Nano({c})"),
                // Another precision has no Arrow unit: read as its text.
                _ => return text,
            };
            ColumnType {
                arrow: DataType::Timestamp(unit, None),
                nullable,
                expr,
            }
        }
        _ if t.starts_with("Decimal") => {
            let parsed = if let Some(a) = args("Decimal(") {
                match (a.first(), a.get(1)) {
                    (Some(p), Some(s)) => p.parse::<u8>().ok().zip(s.parse::<i8>().ok()),
                    _ => None,
                }
            } else {
                [("Decimal32(", 9u8), ("Decimal64(", 18), ("Decimal128(", 38)]
                    .into_iter()
                    .find_map(|(pre, p)| {
                        args(pre).and_then(|a| {
                            a.first().and_then(|s| s.parse::<i8>().ok()).map(|s| (p, s))
                        })
                    })
            };
            match parsed {
                Some((p, s)) if p <= 38 => plain(DataType::Decimal128(p, s)),
                // Wider than a 128-bit decimal: read as its exact text.
                _ => text,
            }
        }
        // UUID, FixedString, Enum, IPv4/6, and nested types: their text,
        // exactly as ClickHouse prints it.
        _ => text,
    }
}

fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
}

/// The code and message of a ClickHouse error body (`Code: 497. DB::Exception: ...`).
fn error_code(body: &str) -> Option<i64> {
    let rest = body.trim_start().strip_prefix("Code: ")?;
    let end = rest.find(|c: char| !c.is_ascii_digit())?;
    rest[..end].parse().ok()
}

fn first_line(body: &str) -> String {
    let line = body.trim().lines().next().unwrap_or("").trim();
    let mut s: String = line.chars().take(400).collect();
    if line.chars().count() > 400 {
        s.push_str("...");
    }
    s
}

fn u64_of(row: &Map<String, Value>, key: &str) -> Result<u64> {
    match row.get(key) {
        Some(Value::Number(n)) => n
            .as_u64()
            .or_else(|| n.as_i64().and_then(|i| u64::try_from(i).ok()))
            .ok_or_else(|| bad_row(key)),
        Some(Value::String(s)) => s.parse().map_err(|_| bad_row(key)),
        _ => Err(bad_row(key)),
    }
}

fn str_of<'a>(row: &'a Map<String, Value>, key: &str) -> Result<&'a str> {
    row.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| bad_row(key))
}

fn bad_row(key: &str) -> Error {
    Error::Source(format!(
        "ClickHouse returned a metadata row without a usable {key:?}; the server's system \
         tables are not the shape this adapter reads"
    ))
}

impl ClickHouse {
    /// Connects to the server `uri` names and checks it answers.
    pub fn connect(uri: &EngineUri) -> Result<Self> {
        let secure = matches!(uri.param("secure")?.as_deref(), Some("true" | "1"));
        let host = if uri.host.contains(':') {
            uri.host.clone()
        } else {
            format!("{}:{}", uri.host, if secure { 8443 } else { 8123 })
        };
        let mut ch = Self {
            root: uri.root(),
            db_root: format!("clickhouse://{}/{}", uri.host, uri.database),
            base: format!("{}://{host}/", if secure { "https" } else { "http" }),
            database: uri.database.clone(),
            tables: uri.tables.clone(),
            user: uri.user.clone().unwrap_or_else(|| "default".into()),
            password: uri.param("password")?,
            query_timeout: uri.timeout_secs()?.map(Duration::from_secs),
            max_unit_rows: crate::DEFAULT_MAX_UNIT_ROWS,
            caches_off: Vec::new(),
            client: http::client()?,
            units: Mutex::new(None),
        };
        // The query condition cache, where this server has it (it arrived
        // in 25.3); the query cache is off on every request, metadata
        // included (a user's profile may turn it on, and then the server
        // refuses to cache a read of its system tables).
        let known = ch.meta(
            "SELECT name FROM system.settings WHERE name = 'use_query_condition_cache'",
            &[],
        )?;
        if !known.is_empty() {
            ch.caches_off.push(("use_query_condition_cache", "0"));
        }
        Ok(ch)
    }

    /// Runs the agent's query once under `tag` in `format`, read-only, with
    /// the caches off; the response streams the rows.
    fn run(&self, q: &str, tag: &Tag, format: &str) -> Result<reqwest::blocking::Response> {
        let mut params: Vec<(&str, &str)> = vec![
            ("query_id", tag.as_str()),
            ("readonly", "2"),
            ("default_format", format),
        ];
        params.extend_from_slice(&self.caches_off);
        self.post(q, &params, self.query_timeout)
    }

    /// With another cap on the rows of a unit `read_columns` reads.
    #[must_use]
    pub fn with_max_unit_rows(mut self, rows: u64) -> Self {
        self.max_unit_rows = rows;
        self
    }

    fn post(
        &self,
        sql: &str,
        params: &[(&str, &str)],
        timeout: Option<Duration>,
    ) -> Result<reqwest::blocking::Response> {
        let mut req = self
            .client
            .post(&self.base)
            .query(&[
                ("database", self.database.as_str()),
                // The query cache serves what an earlier run returned, so
                // no request of bellwether's ever reads or fills it.
                ("use_query_cache", "0"),
            ])
            .query(params)
            .header("X-ClickHouse-User", &self.user)
            .body(sql.to_string());
        if let Some(p) = &self.password {
            req = req.header("X-ClickHouse-Key", p);
        }
        if let Some(t) = timeout {
            req = req.timeout(t);
        }
        let resp = req.send().map_err(|e| http::transport(&self.root, e))?;
        if resp.status().is_success() {
            return Ok(resp);
        }
        let status = resp.status();
        let mut body = String::new();
        let _ = resp.take(64 * 1024).read_to_string(&mut body);
        Err(self.engine_error(status.as_u16(), &body))
    }

    fn engine_error(&self, status: u16, body: &str) -> Error {
        let code = error_code(body);
        let line = first_line(body);
        match code {
            Some(516 | 192 | 193 | 194) => Error::Source(format!(
                "{}: ClickHouse refused the credentials ({line}); give the user in the root and \
                 its password as password=env:NAME",
                self.root
            )),
            _ => Error::Source(format!(
                "{}: ClickHouse refused the request (HTTP {status}): {line}",
                self.root
            )),
        }
    }

    /// Runs a fixed metadata statement and returns its rows.
    fn meta(&self, sql: &str, params: &[(&str, &str)]) -> Result<Vec<Map<String, Value>>> {
        let mut all: Vec<(&str, &str)> = vec![
            ("default_format", "JSONEachRow"),
            ("output_format_json_quote_64bit_integers", "0"),
            ("readonly", "2"),
        ];
        all.extend_from_slice(params);
        let mut resp = self.post(sql, &all, Some(METADATA))?;
        let mut body = String::new();
        resp.read_to_string(&mut body)
            .map_err(|e| Error::Source(format!("{}: cannot read a response ({e})", self.root)))?;
        body.lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| {
                serde_json::from_str::<Map<String, Value>>(l).map_err(|e| {
                    Error::Source(format!(
                        "{}: ClickHouse returned a row that is not JSON ({e})",
                        self.root
                    ))
                })
            })
            .collect()
    }

    /// The tables of the root: those named, or every table of the database,
    /// each checked to exist and to be of the MergeTree family.
    fn tables(&self) -> Result<Vec<String>> {
        let rows = self.meta(
            "SELECT name, engine FROM system.tables WHERE database = {db:String} ORDER BY name",
            &[("param_db", self.database.as_str())],
        )?;
        let found: BTreeMap<String, String> = rows
            .iter()
            .map(|r| {
                Ok((
                    str_of(r, "name")?.to_string(),
                    str_of(r, "engine")?.to_string(),
                ))
            })
            .collect::<Result<_>>()?;
        let wanted: Vec<String> = if self.tables.is_empty() {
            found
                .iter()
                .filter(|(_, e)| e.ends_with("MergeTree"))
                .map(|(n, _)| n.clone())
                .collect()
        } else {
            self.tables.clone()
        };
        for t in &wanted {
            match found.get(t) {
                None => {
                    return Err(Error::Source(format!(
                        "{}: there is no table {t:?} in database {:?} (or the user may not see \
                         it); its tables are {}",
                        self.root,
                        self.database,
                        found.keys().cloned().collect::<Vec<_>>().join(", ")
                    )))
                }
                Some(e) if !e.ends_with("MergeTree") => {
                    return Err(Error::Source(format!(
                        "{}: table {t:?} is a {e} table; a table's identity is read from the \
                         parts of a MergeTree-family table, so name MergeTree tables only",
                        self.root
                    )))
                }
                Some(_) => {}
            }
        }
        Ok(wanted)
    }

    fn list_units(&self) -> Result<Arc<Vec<Unit>>> {
        if let Some(u) = self.units.lock().map_err(|_| poisoned())?.as_ref() {
            return Ok(Arc::clone(u));
        }
        let tables = self.tables()?;
        let names = serde_json::to_string(&tables)?.replace('"', "'");
        let rows = self.meta(
            "SELECT table, partition_id, sum(rows) AS rows, sum(bytes_on_disk) AS bytes, \
             min(min_block_number) AS lo, max(max_block_number) AS hi, \
             max(if(data_version > min_block_number, data_version, 0)) AS mutation \
             FROM system.parts WHERE active AND database = {db:String} \
             AND has({tables:Array(String)}, table) \
             GROUP BY table, partition_id ORDER BY table, partition_id",
            &[
                ("param_db", self.database.as_str()),
                ("param_tables", names.as_str()),
            ],
        )?;
        let prefix = &self.db_root;
        let mut units = Vec::with_capacity(rows.len());
        for r in &rows {
            let table = str_of(r, "table")?.to_string();
            let partition = str_of(r, "partition_id")?.to_string();
            let rows_n = u64_of(r, "rows")?;
            let etag = format!(
                "v1;rows={rows_n};blocks={}-{};mutation={}",
                u64_of(r, "lo")?,
                u64_of(r, "hi")?,
                u64_of(r, "mutation")?
            );
            let file = FileFingerprint {
                path: format!("{prefix}/{table}/{partition}"),
                size: rows_n,
                tail_blake3: Hash::of(etag.as_bytes()),
                etag: Some(etag),
            };
            units.push(Unit {
                table,
                partition,
                meta: RowGroupMeta {
                    id: None,
                    file,
                    index_in_file: 0,
                    rows: rows_n,
                    bytes: u64_of(r, "bytes")?,
                    stats: Vec::new(),
                    byte_range: None,
                },
            });
        }
        let units = Arc::new(units);
        *self.units.lock().map_err(|_| poisoned())? = Some(Arc::clone(&units));
        Ok(units)
    }

    fn table_schema(&self, table: &str) -> Result<Vec<(String, ColumnType)>> {
        let rows = self.meta(
            "SELECT name, type FROM system.columns WHERE database = {db:String} AND table = \
             {t:String} ORDER BY position",
            &[("param_db", self.database.as_str()), ("param_t", table)],
        )?;
        rows.iter()
            .map(|r| {
                Ok((
                    str_of(r, "name")?.to_string(),
                    column_type(str_of(r, "type")?),
                ))
            })
            .collect()
    }

    fn unit(&self, file: &FileFingerprint) -> Result<Unit> {
        let units = self.list_units()?;
        let u = units
            .iter()
            .find(|u| u.meta.file.path == file.path)
            .ok_or_else(|| Error::Source(format!("{}: no unit {}", self.root, file.path)))?;
        if u.meta.file != *file {
            return Err(Error::Source(format!(
                "{}: the unit's identity given is not the one listed; the table changed since \
                 it was listed, so list it again",
                file.path
            )));
        }
        Ok(u.clone())
    }

    /// Forgets the unit listing, so the next call lists the tables again.
    pub fn relist(&self) -> Result<()> {
        *self.units.lock().map_err(|_| poisoned())? = None;
        Ok(())
    }
}

fn poisoned() -> Error {
    Error::Other("the unit listing lock is poisoned".into())
}

impl MeasuredEngine for ClickHouse {
    fn root(&self) -> &str {
        &self.root
    }

    fn model(&self) -> &'static str {
        MODEL
    }

    fn submit(&self, query: &str, tag: &Tag) -> Result<ResultHandle> {
        let q = check_query(query)?;
        let mut resp = self.run(q, tag, "Null")?;
        // A query that names its own output format still returns rows:
        // they are read and dropped, never kept.
        std::io::copy(&mut resp, &mut std::io::sink()).map_err(|e| {
            Error::Source(format!(
                "{}: query {tag} failed while its rows were discarded ({e})",
                self.root
            ))
        })?;
        Ok(ResultHandle {
            tag: tag.clone(),
            query: q.to_string(),
            logged: q.to_string(),
            exported: None,
        })
    }

    fn export(&self, query: &str, tag: &Tag, dir: &Path) -> Result<ResultHandle> {
        let q = check_query(query)?;
        export_dir(&self.root, dir)?;
        let file = dir.join("part-0.parquet");
        let written = (|| -> Result<Exported> {
            let mut resp = match self.run(q, tag, "Parquet") {
                Ok(r) => r,
                Err(Error::Source(m)) if m.contains(&format!("Code: {ACCESS_DENIED}.")) => {
                    let permission = "SELECT on every table the query reads, for the user \
                                      bellwether connects as"
                        .to_string();
                    return Err(Error::ExportDenied {
                        message: format!(
                            "{} refused to export the result of query {tag} ({m}); grant \
                             {permission}",
                            self.root
                        ),
                        engine: self.root.clone(),
                        permission,
                    });
                }
                Err(e) => return Err(e),
            };
            let mut out = std::fs::File::create(&file)
                .map_err(|e| denied_scratch(&self.root, dir, &e.to_string()))?;
            std::io::copy(&mut resp, &mut out).map_err(|e| {
                Error::Source(format!(
                    "{}: the export of query {tag} was cut off ({e})",
                    self.root
                ))
            })?;
            drop(out);
            // A server error after the first bytes arrives inside the body:
            // a file that is not whole Parquet is refused, never read.
            let f = std::fs::File::open(&file)
                .map_err(|e| Error::Source(format!("cannot reopen {} ({e})", file.display())))?;
            parquet::file::reader::SerializedFileReader::new(f).map_err(|e| {
                Error::Source(format!(
                    "{}: the export of query {tag} is not whole Parquet ({e}); the query failed \
                     while it streamed",
                    self.root
                ))
            })?;
            listed(dir)
        })();
        match written {
            Ok(exported) => Ok(ResultHandle {
                tag: tag.clone(),
                query: q.to_string(),
                logged: q.to_string(),
                exported: Some(exported),
            }),
            Err(e) => {
                let _ = std::fs::remove_dir_all(dir);
                Err(e)
            }
        }
    }

    fn stats(&self, run: &ResultHandle) -> Result<QueryStats> {
        let tag = &run.tag;
        let deadline = std::time::Instant::now() + LOG_WAIT;
        loop {
            // Flushing the log needs the SYSTEM FLUSH LOGS privilege; without
            // it, the log's own flush interval is waited for.
            let _ = self.post("SYSTEM FLUSH LOGS", &[], Some(METADATA));
            let rows = self.meta(
                "SELECT type, user, query, read_rows, read_bytes, memory_usage, \
                 query_duration_ms, result_rows, ProfileEvents['UserTimeMicroseconds'] + \
                 ProfileEvents['SystemTimeMicroseconds'] AS cpu_us, \
                 ProfileEvents['SelectedParts'] AS parts, \
                 ProfileEvents['SelectedPartsTotal'] AS parts_total, \
                 ProfileEvents['QueryCacheHits'] AS cache_hits, \
                 ProfileEvents['QueryConditionCacheHits'] AS condition_cache_hits, \
                 Settings['use_query_cache'] AS use_query_cache, exception \
                 FROM system.query_log WHERE query_id = {tag:String} AND type IN \
                 ('QueryFinish', 'ExceptionBeforeStart', 'ExceptionWhileProcessing') \
                 ORDER BY event_time_microseconds LIMIT 16",
                &[("param_tag", tag.as_str())],
            )?;
            if rows.len() > 1 {
                return Err(refused(
                    &self.root,
                    tag,
                    "ambiguous_log_entry",
                    &format!(
                        "system.query_log holds {} finished entries under the tag, so another \
                         client ran a query under it and the entry of this run cannot be told \
                         apart; run again",
                        rows.len()
                    ),
                ));
            }
            if let Some(r) = rows.first() {
                return self.stats_of(run, r);
            }
            if std::time::Instant::now() >= deadline {
                return Err(Error::Source(format!(
                    "{}: system.query_log holds no finished entry for query {tag} after {}s; \
                     the server may not log queries (query_log in its configuration, \
                     log_queries=1 for the user), so its cost cannot be measured",
                    self.root,
                    LOG_WAIT.as_secs()
                )));
            }
            std::thread::sleep(Duration::from_millis(500));
        }
    }
}

impl ClickHouse {
    /// One finished `system.query_log` entry as `run`'s statistics, or the
    /// refusal of it (module docs).
    fn stats_of(&self, run: &ResultHandle, r: &Map<String, Value>) -> Result<QueryStats> {
        let tag = &run.tag;
        if str_of(r, "user")? != self.user || str_of(r, "query")? != run.logged {
            return Err(refused(
                &self.root,
                tag,
                "not_the_query_submitted",
                &format!(
                    "the entry under the tag was logged for another user or with another text \
                     than the query submitted as user {}, so another client ran under the tag; \
                     run again",
                    self.user
                ),
            ));
        }
        if str_of(r, "type")? != "QueryFinish" {
            return Err(Error::Source(format!(
                "{}: query {tag} failed in the engine, so it has no cost to read: {}",
                self.root,
                first_line(str_of(r, "exception").unwrap_or(""))
            )));
        }
        let cache_on = str_of(r, "use_query_cache")?;
        let (hits, condition_hits) = (u64_of(r, "cache_hits")?, u64_of(r, "condition_cache_hits")?);
        if hits > 0 || condition_hits > 0 || matches!(cache_on, "1" | "true" | "True") {
            return Err(refused(
                &self.root,
                tag,
                "cache_served",
                &format!(
                    "the query turned a cache on (use_query_cache={}, {hits} query cache hits, \
                     {condition_hits} query condition cache hits), so the log records what the \
                     cache served, not what the query reads; propose the rewrite again without \
                     the SETTINGS that turn a cache on",
                    if cache_on.is_empty() { "0" } else { cache_on }
                ),
            ));
        }
        let opt = |k: &str| -> Result<Option<u64>> { Ok(Some(u64_of(r, k)?).filter(|v| *v > 0)) };
        Ok(QueryStats {
            tag: tag.to_string(),
            read_rows: u64_of(r, "read_rows")?,
            read_bytes: u64_of(r, "read_bytes")?,
            peak_memory_bytes: u64_of(r, "memory_usage")?,
            cpu_ns: Some(u64_of(r, "cpu_us")?.saturating_mul(1_000)),
            duration_ns: u64_of(r, "query_duration_ms")?.saturating_mul(1_000_000),
            result_rows: u64_of(r, "result_rows")?,
            units_read: Some(u64_of(r, "parts")?),
            units_total: opt("parts_total")?,
        })
    }
}

impl Source for ClickHouse {
    fn discover(&self, root: &str) -> Result<Vec<FileFingerprint>> {
        if root != self.root {
            return Err(Error::Source(format!(
                "this source is {}, not {root}",
                self.root
            )));
        }
        Ok(self
            .list_units()?
            .iter()
            .map(|u| u.meta.file.clone())
            .collect())
    }

    fn row_groups(&self, file: &FileFingerprint) -> Result<Vec<RowGroupMeta>> {
        let u = self.unit(file)?;
        let schema = self.schema(&self.root)?;
        let mut meta = u.meta;
        meta.stats = schema
            .fields()
            .iter()
            .map(|f| ColumnStats {
                column: f.name().clone(),
                min: None,
                max: None,
                null_count: None,
                distinct_count: None,
                has_bloom: false,
                bytes: None,
            })
            .collect();
        Ok(vec![meta])
    }

    fn read_columns(&self, rg: &RowGroupMeta, columns: &[String]) -> Result<RecordBatch> {
        let u = self.unit(&rg.file)?;
        if u.meta.rows > self.max_unit_rows {
            return Err(Error::Source(format!(
                "{}: partition {} of {} holds {} rows, more than the {} one read may hold \
                 (ADR 018); partition the table more finely",
                self.root, u.partition, u.table, u.meta.rows, self.max_unit_rows
            )));
        }
        let declared = self.table_schema(&u.table)?;
        let mut fields = Vec::with_capacity(columns.len());
        let mut exprs = Vec::with_capacity(columns.len());
        for c in columns {
            let (_, t) = declared.iter().find(|(n, _)| n == c).ok_or_else(|| {
                Error::Source(format!(
                    "{}: table {} has no column {c:?}",
                    self.root, u.table
                ))
            })?;
            exprs.push(format!(
                "{} AS {}",
                t.expr.replace("{c}", &quote_ident(c)),
                quote_ident(c)
            ));
            fields.push(Field::new(c, t.arrow.clone(), t.nullable));
        }
        let schema: SchemaRef = Arc::new(Schema::new(fields));
        if columns.is_empty() {
            return RecordBatch::try_new_with_options(
                schema,
                Vec::new(),
                &arrow::array::RecordBatchOptions::new()
                    .with_row_count(Some(usize::try_from(u.meta.rows).unwrap_or(usize::MAX))),
            )
            .map_err(|e| Error::Source(e.to_string()));
        }
        let sql = format!(
            "SELECT {} FROM {}.{} WHERE _partition_id = {{p:String}}",
            exprs.join(", "),
            quote_ident(&self.database),
            quote_ident(&u.table)
        );
        let resp = self.post(
            &sql,
            &[
                ("param_p", u.partition.as_str()),
                ("readonly", "2"),
                ("default_format", "ArrowStream"),
                ("output_format_arrow_string_as_string", "1"),
                ("output_format_arrow_low_cardinality_as_dictionary", "0"),
                // The server compresses Arrow IPC with lz4 by default; the
                // workspace's arrow reads uncompressed streams only.
                ("output_format_arrow_compression_method", "none"),
            ],
            self.query_timeout,
        )?;
        let reader = arrow::ipc::reader::StreamReader::try_new(resp, None).map_err(|e| {
            Error::Source(format!(
                "{}: the read of {} is not Arrow ({e})",
                self.root, rg.file.path
            ))
        })?;
        let mut columns_read: Vec<Vec<ArrayRef>> = vec![Vec::new(); columns.len()];
        let mut rows = 0u64;
        for batch in reader {
            let batch = batch.map_err(|e| Error::Source(format!("{}: {e}", self.root)))?;
            rows += batch.num_rows() as u64;
            for (i, f) in schema.fields().iter().enumerate() {
                let a = batch.column(i);
                let a = if a.data_type() == f.data_type() {
                    Arc::clone(a)
                } else {
                    arrow::compute::cast(a, f.data_type()).map_err(|e| {
                        Error::Source(format!(
                            "{}: column {} read as {} cannot be {} ({e})",
                            self.root,
                            f.name(),
                            a.data_type(),
                            f.data_type()
                        ))
                    })?
                };
                columns_read[i].push(a);
            }
        }
        if rows != u.meta.rows {
            return Err(Error::Source(format!(
                "{}: partition {} of {} returned {rows} rows where its listing says {}; the \
                 table changed while it was read, so list it again",
                self.root, u.partition, u.table, u.meta.rows
            )));
        }
        let arrays = columns_read
            .into_iter()
            .map(|parts| {
                let refs: Vec<&dyn Array> = parts.iter().map(AsRef::as_ref).collect();
                arrow::compute::concat(&refs).map_err(|e| Error::Source(e.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;
        RecordBatch::try_new(schema, arrays).map_err(|e| Error::Source(e.to_string()))
    }

    fn schema(&self, _root: &str) -> Result<SchemaRef> {
        let tables = self.tables()?;
        let mut shape: Option<(String, Vec<(String, ColumnType)>)> = None;
        for t in tables {
            let s = self.table_schema(&t)?;
            match &shape {
                None => shape = Some((t, s)),
                Some((first, s0)) if *s0 != s => {
                    return Err(Error::Source(format!(
                        "{}: tables {first} and {t} have different columns; one root reads \
                         tables of one shape",
                        self.root
                    )))
                }
                Some(_) => {}
            }
        }
        let (_, columns) = shape.ok_or_else(|| {
            Error::Source(format!(
                "{}: the database has no MergeTree table to read",
                self.root
            ))
        })?;
        Ok(Arc::new(Schema::new(
            columns
                .into_iter()
                .map(|(n, t)| Field::new(n, t.arrow, t.nullable))
                .collect::<Vec<_>>(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn types_map_to_the_arrow_type_read() {
        let t = column_type("Nullable(DateTime64(3, 'UTC'))");
        assert_eq!(t.arrow, DataType::Timestamp(TimeUnit::Millisecond, None));
        assert!(t.nullable);
        assert_eq!(column_type("LowCardinality(String)").arrow, DataType::Utf8);
        assert_eq!(
            column_type("Decimal(18, 4)").arrow,
            DataType::Decimal128(18, 4)
        );
        assert_eq!(
            column_type("Decimal64(2)").arrow,
            DataType::Decimal128(18, 2)
        );
        assert_eq!(column_type("Decimal(76, 2)").expr, "toString({c})");
        assert_eq!(column_type("UUID").expr, "toString({c})");
        assert_eq!(column_type("Date").arrow, DataType::Date32);
        assert_eq!(
            column_type("DateTime('Europe/Paris')").arrow,
            DataType::Timestamp(TimeUnit::Second, None)
        );
        assert_eq!(column_type("DateTime64(4)").expr, "toString({c})");
    }

    #[test]
    fn error_bodies_yield_their_code() {
        assert_eq!(
            error_code("Code: 497. DB::Exception: default: Not enough privileges."),
            Some(497)
        );
        assert_eq!(error_code("garbage"), None);
        assert_eq!(quote_ident("a`b"), "`a\\`b`");
    }
}
