//! The second adapter: Kusto (Azure Data Explorer) over its REST API.
//!
//! - **submit**: `POST /v2/rest/query` with `x-ms-client-request-id:
//!   bellwether;<tag>` and the request properties `request_readonly`
//!   (no write), `notruncation` (the whole result is produced), and
//!   `results_progressive_enabled: false`. The response is read and
//!   dropped; only its last 1 MiB is kept, to read the completion frame
//!   that says whether the query failed.
//! - **stats**: `.show queries`, filtered to `ClientActivityId ==
//!   'bellwether;<tag>'`: `ScannedExtentsStatistics` (rows and extents read),
//!   `CacheStatistics` (shard bytes read), `MemoryPeak`, `TotalCpu`,
//!   `Duration`, `ResultSetStatistics`.
//! - **export**: `.export to parquet (h@"<scratch>/<dir>;impersonate") <|
//!   <query>`, written by the cluster into the storage folder the root's
//!   `scratch=` names, which the local scratch folder must mount. Without
//!   the permission the refusal is [`Error::ExportDenied`].
//! - **units**: one per extent, from `.show table extents` (or
//!   `.show database extents`). Identity: the extent id and its rows, so
//!   ingestion, deletion, and a background merge each change it (a run
//!   across a merge refuses rather than certifies).
//!
//! Authentication: a bearer token for `<cluster>/.default` from the Azure
//! Identity credential chain, tried in this order: a service principal's
//! secret from the environment (`AZURE_TENANT_ID`, `AZURE_CLIENT_ID`,
//! `AZURE_CLIENT_SECRET`), workload identity (`AZURE_FEDERATED_TOKEN_FILE`),
//! a managed identity, then the developer tools' sign-in (Azure CLI, Azure
//! Developer CLI). `auth=none` sends no token, for the Kusto emulator on
//! a loopback `endpoint=`.

use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{
    ArrayRef, BooleanBuilder, DurationNanosecondBuilder, Float64Builder, Int32Builder,
    Int64Builder, RecordBatch, StringBuilder, TimestampNanosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_types::{
    ColumnStats, Error, FileFingerprint, Hash, QueryStats, Result, RowGroupMeta, Source,
};
use serde_json::{json, Map, Value};

use crate::http::{self, METADATA};
use crate::uri::EngineUri;
use crate::{check_query, denied_scratch, listed, Exported, MeasuredEngine, ResultHandle, Tag};

/// The model of every Kusto cost.
pub const MODEL: &str = "kusto.query_log";

/// How long [`MeasuredEngine::stats`] waits for `.show queries` to list a
/// finished query.
const LOG_WAIT: Duration = Duration::from_secs(60);

/// Bytes of a discarded result kept to read its completion frame.
const TAIL: usize = 1 << 20;

/// The request id prefix: Kusto logs `ClientActivityId` as given.
const PREFIX: &str = "bellwether;";

/// How a response body is read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Keep {
    /// Every byte (metadata, bounded by the engine's own answers).
    All,
    /// Only the last MiB (a discarded query result).
    Tail,
}

/// One request to the cluster: `POST <path>` with a JSON body and a
/// request id; returns the HTTP status and the body as kept. The seam the
/// tests replace.
pub trait Transport: Send + Sync {
    /// Sends the request.
    fn post(
        &self,
        path: &str,
        body: &Value,
        request_id: &str,
        keep: Keep,
    ) -> Result<(u16, Vec<u8>)>;
}

/// The Kusto REST API over HTTPS with a bearer token.
pub struct HttpTransport {
    endpoint: String,
    client: reqwest::blocking::Client,
    token: Option<TokenChain>,
    query_timeout: Option<Duration>,
}

impl Transport for HttpTransport {
    fn post(
        &self,
        path: &str,
        body: &Value,
        request_id: &str,
        keep: Keep,
    ) -> Result<(u16, Vec<u8>)> {
        let mut req = self
            .client
            .post(format!("{}{path}", self.endpoint))
            .header("x-ms-client-request-id", request_id)
            .header("x-ms-app", "bellwether")
            .json(body);
        if let Some(t) = &self.token {
            req = req.bearer_auth(t.token()?);
        }
        req = req.timeout(match keep {
            Keep::All
                if path.contains("mgmt")
                    && !body["csl"].as_str().unwrap_or("").starts_with(".export") =>
            {
                METADATA
            }
            _ => self.query_timeout.unwrap_or(Duration::from_secs(6 * 3600)),
        });
        let mut resp = req.send().map_err(|e| http::transport(&self.endpoint, e))?;
        let status = resp.status().as_u16();
        let mut out = Vec::new();
        match keep {
            Keep::All => {
                resp.read_to_end(&mut out).map_err(|e| {
                    Error::Source(format!("{}: cannot read a response ({e})", self.endpoint))
                })?;
            }
            Keep::Tail => {
                let mut buf = vec![0u8; 64 * 1024];
                loop {
                    let n = resp.read(&mut buf).map_err(|e| {
                        Error::Source(format!("{}: the result was cut off ({e})", self.endpoint))
                    })?;
                    if n == 0 {
                        break;
                    }
                    out.extend_from_slice(&buf[..n]);
                    if out.len() > 2 * TAIL {
                        out.drain(..out.len() - TAIL);
                    }
                }
            }
        }
        Ok((status, out))
    }
}

/// The Azure Identity credential chain (module docs), with the token cached
/// until five minutes before it expires.
struct TokenChain {
    scope: String,
    sources: Vec<(
        &'static str,
        Arc<dyn azure_core::credentials::TokenCredential>,
    )>,
    cached: Mutex<Option<(String, std::time::SystemTime)>>,
    runtime: tokio::runtime::Runtime,
}

impl TokenChain {
    fn new(endpoint: &str) -> Result<Self> {
        use azure_identity::{
            ClientSecretCredential, DeveloperToolsCredential, ManagedIdentityCredential,
            WorkloadIdentityCredential,
        };
        let mut sources: Vec<(
            &'static str,
            Arc<dyn azure_core::credentials::TokenCredential>,
        )> = Vec::new();
        if let (Ok(tenant), Ok(client), Ok(secret)) = (
            std::env::var("AZURE_TENANT_ID"),
            std::env::var("AZURE_CLIENT_ID"),
            std::env::var("AZURE_CLIENT_SECRET"),
        ) {
            if let Ok(c) = ClientSecretCredential::new(
                &tenant,
                client,
                azure_core::credentials::Secret::new(secret),
                None,
            ) {
                sources.push(("a service principal's secret (AZURE_CLIENT_SECRET)", c));
            }
        }
        if std::env::var_os("AZURE_FEDERATED_TOKEN_FILE").is_some() {
            if let Ok(c) = WorkloadIdentityCredential::new(None) {
                sources.push(("workload identity (AZURE_FEDERATED_TOKEN_FILE)", c));
            }
        }
        if let Ok(c) = ManagedIdentityCredential::new(None) {
            sources.push(("a managed identity", c));
        }
        if let Ok(c) = DeveloperToolsCredential::new(None) {
            sources.push(("the Azure CLI or Azure Developer CLI sign-in", c));
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| Error::Other(format!("cannot start the token runtime ({e})")))?;
        Ok(Self {
            scope: format!("{}/.default", endpoint.trim_end_matches('/')),
            sources,
            cached: Mutex::new(None),
            runtime,
        })
    }

    fn token(&self) -> Result<String> {
        let mut cached = self
            .cached
            .lock()
            .map_err(|_| Error::Other("the token cache lock is poisoned".into()))?;
        if let Some((t, until)) = cached.as_ref() {
            if std::time::SystemTime::now() + Duration::from_secs(300) < *until {
                return Ok(t.clone());
            }
        }
        let mut tried = Vec::new();
        for (name, c) in &self.sources {
            match self
                .runtime
                .block_on(c.get_token(&[self.scope.as_str()], None))
            {
                Ok(t) => {
                    let until = std::time::UNIX_EPOCH
                        + Duration::from_secs(
                            u64::try_from(t.expires_on.unix_timestamp()).unwrap_or(0),
                        );
                    let s = t.token.secret().to_string();
                    *cached = Some((s.clone(), until));
                    return Ok(s);
                }
                Err(e) => tried.push(format!("{name}: {e}")),
            }
        }
        Err(Error::Source(format!(
            "no Azure credential gave a token for {}; tried, in order: {}; sign in with `az \
             login`, or set AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET",
            self.scope,
            if tried.is_empty() {
                "nothing (no credential source is configured)".to_string()
            } else {
                tried.join("; ")
            }
        )))
    }
}

/// One extent: a unit.
#[derive(Clone, Debug)]
struct Unit {
    table: String,
    extent: String,
    meta: RowGroupMeta,
}

/// A Kusto database as a measured source.
pub struct Kusto {
    root: String,
    db_root: String,
    database: String,
    tables: Vec<String>,
    transport: Box<dyn Transport>,
    /// The storage folder the cluster exports into, and the local folder
    /// that mounts it.
    scratch: Option<(String, PathBuf)>,
    max_unit_rows: u64,
    units: Mutex<Option<Arc<Vec<Unit>>>>,
}

impl std::fmt::Debug for Kusto {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Kusto")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

/// `['name']`: a quoted Kusto identifier (names are checked to hold no
/// quote, uri.rs).
fn ident(name: &str) -> String {
    format!("['{name}']")
}

fn bad(root: &str, what: &str) -> Error {
    Error::Source(format!(
        "{root}: Kusto returned {what}; the response is not the shape this adapter reads"
    ))
}

/// The message of a Kusto error body (`{"error": {"@message" | "message"}}`).
fn error_message(body: &[u8]) -> String {
    let v: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let e = &v["error"];
    let m = e["@message"]
        .as_str()
        .or_else(|| e["message"].as_str())
        .map_or_else(
            || String::from_utf8_lossy(&body[..body.len().min(400)]).into_owned(),
            str::to_string,
        );
    match e["code"].as_str() {
        Some(c) => format!("{c}: {m}"),
        None => m,
    }
}

/// The rows of a management (v1) response's first table, by column name.
pub fn v1_rows(root: &str, body: &[u8]) -> Result<Vec<Map<String, Value>>> {
    let v: Value =
        serde_json::from_slice(body).map_err(|_| bad(root, "a body that is not JSON"))?;
    let table = v["Tables"]
        .as_array()
        .and_then(|t| t.first())
        .ok_or_else(|| bad(root, "no table"))?;
    table_rows(root, table)
}

fn table_rows(root: &str, table: &Value) -> Result<Vec<Map<String, Value>>> {
    let names: Vec<&str> = table["Columns"]
        .as_array()
        .ok_or_else(|| bad(root, "a table without columns"))?
        .iter()
        .map(|c| {
            c["ColumnName"]
                .as_str()
                .ok_or_else(|| bad(root, "a column without a name"))
        })
        .collect::<Result<_>>()?;
    table["Rows"]
        .as_array()
        .ok_or_else(|| bad(root, "a table without rows"))?
        .iter()
        .map(|r| {
            let r = r
                .as_array()
                .ok_or_else(|| bad(root, "a row that is not a list"))?;
            if r.len() != names.len() {
                return Err(bad(root, "a row of another width than its columns"));
            }
            Ok(names
                .iter()
                .zip(r)
                .map(|(n, v)| ((*n).to_string(), v.clone()))
                .collect())
        })
        .collect()
}

/// A primary result: its columns (name, Kusto type) and its rows.
pub type Primary = (Vec<(String, String)>, Vec<Vec<Value>>);

/// The primary result of a query (v2) response: its columns (name, type)
/// and rows. A response whose completion frame reports errors, or whose
/// rows hold an error object, is refused with the engine's message.
pub fn v2_primary(root: &str, body: &[u8]) -> Result<Primary> {
    let frames: Vec<Value> =
        serde_json::from_slice(body).map_err(|_| bad(root, "a body that is not a frame list"))?;
    let mut primary = None;
    for f in &frames {
        match f["FrameType"].as_str() {
            Some("DataTable") if f["TableKind"] == "PrimaryResult" && primary.is_none() => {
                primary = Some(f);
            }
            Some("DataSetCompletion") if f["HasErrors"] == true || f["Cancelled"] == true => {
                return Err(Error::Source(format!(
                    "{root}: the query failed in the engine: {}",
                    one_api_error(f)
                )));
            }
            _ => {}
        }
    }
    let t = primary.ok_or_else(|| bad(root, "no primary result"))?;
    let columns: Vec<(String, String)> = t["Columns"]
        .as_array()
        .ok_or_else(|| bad(root, "a result without columns"))?
        .iter()
        .map(|c| {
            Ok((
                c["ColumnName"]
                    .as_str()
                    .ok_or_else(|| bad(root, "a column without a name"))?
                    .to_string(),
                c["ColumnType"]
                    .as_str()
                    .ok_or_else(|| bad(root, "a column without a type"))?
                    .to_string(),
            ))
        })
        .collect::<Result<_>>()?;
    let mut rows = Vec::new();
    for r in t["Rows"]
        .as_array()
        .ok_or_else(|| bad(root, "a result without rows"))?
    {
        match r {
            Value::Array(a) if a.len() == columns.len() => rows.push(a.clone()),
            Value::Object(o) if o.contains_key("OneApiErrors") => {
                return Err(Error::Source(format!(
                    "{root}: the query failed while it streamed: {}",
                    one_api_error(r)
                )));
            }
            _ => return Err(bad(root, "a row of another width than its columns")),
        }
    }
    Ok((columns, rows))
}

fn one_api_error(v: &Value) -> String {
    v["OneApiErrors"]
        .as_array()
        .and_then(|e| e.first())
        .and_then(|e| {
            e["error"]["@message"]
                .as_str()
                .or_else(|| e["error"]["message"].as_str())
        })
        .unwrap_or("no message")
        .chars()
        .take(400)
        .collect()
}

/// Whether the last bytes of a discarded v2 result end in a completion
/// frame without errors. `Err` carries the engine's message.
pub fn tail_completed(root: &str, tail: &[u8]) -> Result<()> {
    let text = String::from_utf8_lossy(tail);
    let start = text
        .rfind("{\"FrameType\":\"DataSetCompletion\"")
        .ok_or_else(|| bad(root, "a result without its completion frame (cut off?)"))?;
    let end = text
        .rfind('}')
        .ok_or_else(|| bad(root, "an unterminated completion frame"))?;
    let frame: Value = serde_json::from_str(&text[start..=end])
        .map_err(|_| bad(root, "a completion frame that is not JSON"))?;
    if frame["HasErrors"] == true || frame["Cancelled"] == true {
        return Err(Error::Source(format!(
            "{root}: the query failed in the engine: {}",
            one_api_error(&frame)
        )));
    }
    Ok(())
}

/// A Kusto timespan (`[d.]hh:mm:ss[.fffffff]`) in nanoseconds.
#[must_use]
pub fn timespan_ns(s: &str) -> Option<u64> {
    let s = s.trim();
    let (days, rest) = match s.split_once('.') {
        Some((d, r)) if !d.contains(':') => (d.parse::<u64>().ok()?, r),
        _ => (0, s),
    };
    let mut parts = rest.splitn(3, ':');
    let h: u64 = parts.next()?.parse().ok()?;
    let m: u64 = parts.next()?.parse().ok()?;
    let sec = parts.next()?;
    let (whole, frac) = sec.split_once('.').unwrap_or((sec, ""));
    let whole: u64 = whole.parse().ok()?;
    if frac.len() > 9 || !frac.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let frac_ns: u64 = if frac.is_empty() {
        0
    } else {
        frac.parse::<u64>().ok()? * 10u64.pow(9 - u32::try_from(frac.len()).ok()?)
    };
    Some((((days * 24 + h) * 60 + m) * 60 + whole) * 1_000_000_000 + frac_ns)
}

/// A dynamic value: JSON as given, or a string holding JSON (management
/// responses encode dynamic columns as text).
fn dynamic(v: &Value) -> Value {
    match v {
        Value::String(s) => serde_json::from_str(s).unwrap_or(Value::Null),
        other => other.clone(),
    }
}

fn num(v: &Value) -> Option<u64> {
    v.as_u64()
        .or_else(|| v.as_i64().and_then(|i| u64::try_from(i).ok()))
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

/// The statistics of one `.show queries` row.
pub fn stats_of(root: &str, tag: &Tag, row: &Map<String, Value>) -> Result<QueryStats> {
    let scanned = dynamic(&row["ScannedExtentsStatistics"]);
    let cache = dynamic(&row["CacheStatistics"]);
    let result = dynamic(&row["ResultSetStatistics"]);
    let shard = |tier: &str, k: &str| num(&cache["Shards"][tier][k]).unwrap_or(0);
    let read_bytes = shard("Hot", "HitBytes")
        + shard("Hot", "MissBytes")
        + shard("Cold", "HitBytes")
        + shard("Cold", "MissBytes")
        + num(&cache["Shards"]["BypassBytes"]).unwrap_or(0);
    let result_rows = result["TablesStatistics"]
        .as_array()
        .map_or(0, |t| t.iter().filter_map(|x| num(&x["RowCount"])).sum());
    let span = |k: &str| row.get(k).and_then(Value::as_str).and_then(timespan_ns);
    Ok(QueryStats {
        tag: tag.to_string(),
        read_rows: num(&scanned["ScannedRowsCount"]).unwrap_or(0),
        read_bytes,
        peak_memory_bytes: row
            .get("MemoryPeak")
            .and_then(num)
            .ok_or_else(|| bad(root, "a query without MemoryPeak"))?,
        cpu_ns: span("TotalCpu"),
        duration_ns: span("Duration").ok_or_else(|| bad(root, "a query without a Duration"))?,
        result_rows,
        units_read: num(&scanned["ScannedExtentsCount"]),
        units_total: num(&scanned["TotalExtentsCount"]),
    })
}

/// Kusto's column type to the Arrow type the source reads it as.
fn arrow_type(kusto: &str) -> DataType {
    match kusto {
        "bool" | "boolean" => DataType::Boolean,
        "int" => DataType::Int32,
        "long" => DataType::Int64,
        "real" | "double" => DataType::Float64,
        "datetime" | "date" => DataType::Timestamp(TimeUnit::Nanosecond, None),
        "timespan" | "time" => DataType::Duration(TimeUnit::Nanosecond),
        // string, guid, dynamic, and decimal (which declares no scale):
        // their exact text.
        _ => DataType::Utf8,
    }
}

/// An ISO 8601 UTC datetime as Kusto writes it, in nanoseconds.
fn datetime_ns(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .and_then(|d| d.timestamp_nanos_opt())
}

/// One column of JSON values as an Arrow array of `dt`.
pub fn column(root: &str, name: &str, dt: &DataType, values: &[&Value]) -> Result<ArrayRef> {
    let wrong = |v: &Value| {
        Error::Source(format!(
            "{root}: column {name:?} holds {v} where a {dt} value was declared"
        ))
    };
    Ok(match dt {
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(values.len());
            for v in values {
                match v {
                    Value::Null => b.append_null(),
                    Value::Bool(x) => b.append_value(*x),
                    other => return Err(wrong(other)),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Int32 => {
            let mut b = Int32Builder::with_capacity(values.len());
            for v in values {
                match v {
                    Value::Null => b.append_null(),
                    other => b.append_value(
                        other
                            .as_i64()
                            .and_then(|i| i32::try_from(i).ok())
                            .ok_or_else(|| wrong(other))?,
                    ),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(values.len());
            for v in values {
                match v {
                    Value::Null => b.append_null(),
                    other => b.append_value(other.as_i64().ok_or_else(|| wrong(other))?),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(values.len());
            for v in values {
                match v {
                    Value::Null => b.append_null(),
                    Value::String(s) => b.append_value(match s.as_str() {
                        "NaN" => f64::NAN,
                        "Infinity" => f64::INFINITY,
                        "-Infinity" => f64::NEG_INFINITY,
                        _ => return Err(wrong(v)),
                    }),
                    other => b.append_value(other.as_f64().ok_or_else(|| wrong(other))?),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            let mut b = TimestampNanosecondBuilder::with_capacity(values.len());
            for v in values {
                match v {
                    Value::Null => b.append_null(),
                    Value::String(s) => b.append_value(datetime_ns(s).ok_or_else(|| wrong(v))?),
                    other => return Err(wrong(other)),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            let mut b = DurationNanosecondBuilder::with_capacity(values.len());
            for v in values {
                match v {
                    Value::Null => b.append_null(),
                    Value::String(s) => {
                        let (neg, body) = s
                            .strip_prefix('-')
                            .map_or((false, s.as_str()), |r| (true, r));
                        let ns = i64::try_from(timespan_ns(body).ok_or_else(|| wrong(v))?)
                            .map_err(|_| wrong(v))?;
                        b.append_value(if neg { -ns } else { ns });
                    }
                    other => return Err(wrong(other)),
                }
            }
            Arc::new(b.finish())
        }
        _ => {
            let mut b = StringBuilder::with_capacity(values.len(), values.len() * 8);
            for v in values {
                match v {
                    Value::Null => b.append_null(),
                    Value::String(s) => b.append_value(s),
                    other => b.append_value(other.to_string()),
                }
            }
            Arc::new(b.finish())
        }
    })
}

impl Kusto {
    /// Connects to the cluster `uri` names. `scratch` is the local folder
    /// that mounts the root's `scratch=` storage folder; exports need both.
    pub fn connect(uri: &EngineUri, scratch: Option<&Path>) -> Result<Self> {
        let endpoint = match uri.param("endpoint")? {
            Some(e) => e.trim_end_matches('/').to_string(),
            None => format!("https://{}", uri.host),
        };
        let auth = uri.param("auth")?.unwrap_or_else(|| "default".into());
        let token = match auth.as_str() {
            "default" => Some(TokenChain::new(&endpoint)?),
            "none" => {
                let loopback = ["http://127.0.0.1", "http://localhost", "http://[::1]"]
                    .iter()
                    .any(|p| endpoint.starts_with(p));
                if !loopback {
                    return Err(Error::InvalidArgument {
                        field: "twin_of",
                        reason: format!(
                            "auth=none sends no token and is for the Kusto emulator on a \
                             loopback endpoint; {} is not one",
                            uri.root()
                        ),
                    });
                }
                None
            }
            other => {
                return Err(Error::InvalidArgument {
                    field: "twin_of",
                    reason: format!("auth={other} is not default or none"),
                })
            }
        };
        let transport = HttpTransport {
            endpoint,
            client: http::client()?,
            token,
            query_timeout: uri.timeout_secs()?.map(Duration::from_secs),
        };
        Self::with_transport(uri, scratch, Box::new(transport))
    }

    /// Over a transport the caller built (the tests' canned cluster).
    pub fn with_transport(
        uri: &EngineUri,
        scratch: Option<&Path>,
        transport: Box<dyn Transport>,
    ) -> Result<Self> {
        let remote = uri.param("scratch")?;
        if let Some(r) = &remote {
            if r.contains('"') || r.contains(';') {
                return Err(Error::InvalidArgument {
                    field: "twin_of",
                    reason: "scratch= is a storage folder URI without quotes or ';' (the \
                             export adds ;impersonate)"
                        .into(),
                });
            }
        }
        Ok(Self {
            root: uri.root(),
            db_root: format!("kusto://{}/{}", uri.host, uri.database),
            database: uri.database.clone(),
            tables: uri.tables.clone(),
            transport,
            scratch: remote.zip(scratch.map(Path::to_path_buf)),
            max_unit_rows: crate::DEFAULT_MAX_UNIT_ROWS,
            units: Mutex::new(None),
        })
    }

    fn request_id(tag: &str) -> String {
        format!("{PREFIX}{tag}")
    }

    fn mgmt(&self, csl: &str, tag: &str) -> Result<Vec<Map<String, Value>>> {
        let (status, body) = self.transport.post(
            "/v1/rest/mgmt",
            &json!({"db": self.database, "csl": csl}),
            &Self::request_id(tag),
            Keep::All,
        )?;
        if status >= 300 {
            return Err(Error::Source(format!(
                "{}: Kusto refused a command (HTTP {status}): {}",
                self.root,
                error_message(&body)
            )));
        }
        v1_rows(&self.root, &body)
    }

    fn query(&self, csl: &str, tag: &str) -> Result<Primary> {
        let (status, body) = self.transport.post(
            "/v2/rest/query",
            &json!({"db": self.database, "csl": csl,
                    "properties": {"Options": {"request_readonly": true,
                                               "results_progressive_enabled": false}}}),
            &Self::request_id(tag),
            Keep::All,
        )?;
        if status >= 300 {
            return Err(Error::Source(format!(
                "{}: Kusto refused a query (HTTP {status}): {}",
                self.root,
                error_message(&body)
            )));
        }
        v2_primary(&self.root, &body)
    }

    fn list_units(&self) -> Result<Arc<Vec<Unit>>> {
        if let Some(u) = self.units.lock().map_err(|_| poisoned())?.as_ref() {
            return Ok(Arc::clone(u));
        }
        let scope = match self.tables.as_slice() {
            [] => format!(".show database {} extents", ident(&self.database)),
            [t] => format!(".show table {} extents", ident(t)),
            ts => format!(
                ".show tables ({}) extents",
                ts.iter().map(|t| ident(t)).collect::<Vec<_>>().join(", ")
            ),
        };
        let rows = self.mgmt(
            &format!("{scope} | project ExtentId, TableName, RowCount, ExtentSize | order by TableName asc, ExtentId asc"),
            &format!("bw-{}-extents", Tag::run()),
        )?;
        let mut units = Vec::with_capacity(rows.len());
        for r in &rows {
            let extent = r["ExtentId"]
                .as_str()
                .ok_or_else(|| bad(&self.root, "an extent without an id"))?
                .to_string();
            if !extent.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-') {
                return Err(bad(&self.root, "an extent id that is not a guid"));
            }
            let table = r["TableName"]
                .as_str()
                .ok_or_else(|| bad(&self.root, "an extent without a table"))?
                .to_string();
            let rows_n =
                num(&r["RowCount"]).ok_or_else(|| bad(&self.root, "an extent without RowCount"))?;
            let etag = format!("v1;rows={rows_n};extent={extent}");
            let file = FileFingerprint {
                path: format!("{}/{table}/{extent}", self.db_root),
                size: rows_n,
                tail_blake3: Hash::of(etag.as_bytes()),
                etag: Some(etag),
            };
            units.push(Unit {
                table,
                extent,
                meta: RowGroupMeta {
                    id: None,
                    file,
                    index_in_file: 0,
                    rows: rows_n,
                    bytes: num(&r["ExtentSize"]).unwrap_or(0),
                    stats: Vec::new(),
                    byte_range: None,
                },
            });
        }
        let units = Arc::new(units);
        *self.units.lock().map_err(|_| poisoned())? = Some(Arc::clone(&units));
        Ok(units)
    }

    fn table_schema(&self, table: &str) -> Result<Vec<(String, String)>> {
        let (_, rows) = self.query(
            &format!(
                "{} | getschema | project ColumnName, ColumnType",
                ident(table)
            ),
            &format!("bw-{}-schema", Tag::run()),
        )?;
        rows.iter()
            .map(|r| {
                match (
                    r.first().and_then(Value::as_str),
                    r.get(1).and_then(Value::as_str),
                ) {
                    (Some(n), Some(t)) => Ok((n.to_string(), t.to_string())),
                    _ => Err(bad(&self.root, "a schema row without a name or type")),
                }
            })
            .collect()
    }

    fn table_names(&self) -> Result<Vec<String>> {
        if !self.tables.is_empty() {
            return Ok(self.tables.clone());
        }
        let rows = self.mgmt(
            ".show tables | project TableName",
            &format!("bw-{}-tables", Tag::run()),
        )?;
        rows.iter()
            .map(|r| {
                r["TableName"]
                    .as_str()
                    .map(str::to_string)
                    .ok_or_else(|| bad(&self.root, "a table without a name"))
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
                "{}: the extent given is not the one listed; the table changed since it was \
                 listed, so list it again",
                file.path
            )));
        }
        Ok(u.clone())
    }

    /// Forgets the unit listing, so the next call lists the extents again.
    pub fn relist(&self) -> Result<()> {
        *self.units.lock().map_err(|_| poisoned())? = None;
        Ok(())
    }

    fn denied(&self, message: &str) -> Error {
        let storage = self
            .scratch
            .as_ref()
            .map_or("the scratch storage folder", |(r, _)| r.as_str());
        let permission = format!(
            "the right to run .export on database {} (the Viewer role or higher), and write \
             access to {storage} for the caller (the export impersonates it)",
            self.database
        );
        Error::ExportDenied {
            message: format!(
                "{} refused the export ({message}); grant {permission}",
                self.root
            ),
            engine: self.root.clone(),
            permission,
        }
    }
}

fn poisoned() -> Error {
    Error::Other("the extent listing lock is poisoned".into())
}

impl MeasuredEngine for Kusto {
    fn root(&self) -> &str {
        &self.root
    }

    fn model(&self) -> &'static str {
        MODEL
    }

    fn submit(&self, query: &str, tag: &Tag) -> Result<ResultHandle> {
        let q = check_query(query)?;
        if q.starts_with('.') {
            return Err(Error::InvalidArgument {
                field: "rewrite",
                reason: "a query that starts with '.' is a management command; each side of a \
                         rewrite pair is a query"
                    .into(),
            });
        }
        let (status, tail) = self.transport.post(
            "/v2/rest/query",
            &json!({"db": self.database, "csl": q,
                    "properties": {"Options": {"request_readonly": true,
                                               "notruncation": true,
                                               "results_progressive_enabled": false}}}),
            &Self::request_id(tag.as_str()),
            Keep::Tail,
        )?;
        if status >= 300 {
            return Err(Error::Source(format!(
                "{}: Kusto refused query {tag} (HTTP {status}): {}",
                self.root,
                error_message(&tail)
            )));
        }
        tail_completed(&self.root, &tail)?;
        Ok(ResultHandle {
            tag: tag.clone(),
            query: q.to_string(),
        })
    }

    fn stats(&self, tag: &Tag) -> Result<QueryStats> {
        let deadline = std::time::Instant::now() + LOG_WAIT;
        loop {
            let rows = self.mgmt(
                &format!(
                    ".show queries | where ClientActivityId == '{}' | project State, \
                     Duration, TotalCpu, MemoryPeak, ScannedExtentsStatistics, \
                     CacheStatistics, ResultSetStatistics, FailureReason",
                    Self::request_id(tag.as_str())
                ),
                &format!("{}-log", tag.as_str()),
            )?;
            if let Some(r) = rows.first() {
                match r["State"].as_str() {
                    Some("Completed") => return stats_of(&self.root, tag, r),
                    Some("InProgress") | None => {}
                    Some(state) => {
                        return Err(Error::Source(format!(
                            "{}: query {tag} ended {state} in the engine, so it has no cost to \
                             read: {}",
                            self.root,
                            r["FailureReason"].as_str().unwrap_or("no reason given")
                        )))
                    }
                }
            }
            if std::time::Instant::now() >= deadline {
                return Err(Error::Source(format!(
                    "{}: .show queries lists no completed query {tag} after {}s; the principal \
                     must be allowed to see its own queries",
                    self.root,
                    LOG_WAIT.as_secs()
                )));
            }
            std::thread::sleep(Duration::from_secs(1));
        }
    }

    fn export(&self, handle: &ResultHandle, dir: &Path) -> Result<Exported> {
        let Some((remote, local)) = &self.scratch else {
            return Err(Error::InvalidArgument {
                field: "scratch",
                reason: format!(
                    "{} exports into a storage folder: name it in the root (scratch=<storage \
                     uri>) and pass the local folder that mounts it as the scratch folder",
                    self.root
                ),
            });
        };
        let rel = dir
            .strip_prefix(local)
            .map_err(|_| Error::InvalidArgument {
                field: "scratch",
                reason: format!(
                    "the export folder {} is not under the scratch folder {}",
                    dir.display(),
                    local.display()
                ),
            })?;
        let rel = rel
            .to_str()
            .filter(|r| !r.contains('"') && !r.contains(';'))
            .ok_or_else(|| Error::InvalidArgument {
                field: "scratch",
                reason: "an export folder name is UTF-8 without quotes or ';'".into(),
            })?
            .replace('\\', "/");
        if dir.exists() && std::fs::read_dir(dir).map_or(true, |mut d| d.next().is_some()) {
            return Err(Error::Other(format!(
                "the export folder {} exists and is not empty; exports go to a fresh folder",
                dir.display()
            )));
        }
        let target = format!("{}/{rel}", remote.trim_end_matches('/'));
        let csl = format!(
            ".export to parquet (h@\"{target};impersonate\") with (namePrefix=\"part\", \
             distributed=false) <| {}",
            handle.query
        );
        let (status, body) = self.transport.post(
            "/v1/rest/mgmt",
            &json!({"db": self.database, "csl": csl,
                    "properties": {"Options": {"request_readonly": false}}}),
            &Self::request_id(&format!("{}-x", handle.tag)),
            Keep::All,
        )?;
        if status >= 300 {
            let m = error_message(&body);
            let lower = m.to_ascii_lowercase();
            if status == 401
                || status == 403
                || [
                    "forbidden",
                    "not authorized",
                    "unauthorized",
                    "authorizationpermissionmismatch",
                    "permission",
                ]
                .iter()
                .any(|k| lower.contains(k))
            {
                return Err(self.denied(&m));
            }
            return Err(Error::Source(format!(
                "{}: the export of query {} failed (HTTP {status}): {m}",
                self.root, handle.tag
            )));
        }
        let rows = v1_rows(&self.root, &body)?;
        let records: u64 = rows.iter().filter_map(|r| num(&r["NumRecords"])).sum();
        if !dir.is_dir() {
            if records == 0 {
                std::fs::create_dir_all(dir)
                    .map_err(|e| denied_scratch(&self.root, dir, &e.to_string()))?;
            } else {
                return Err(Error::Source(format!(
                    "{}: the cluster exported {records} rows to {target}, and none is under {}; \
                     the scratch folder must be the local mount of scratch=",
                    self.root,
                    dir.display()
                )));
            }
        }
        listed(dir)
    }
}

impl Source for Kusto {
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
        let schema = self.table_schema(&u.table)?;
        let mut meta = u.meta;
        meta.stats = schema
            .into_iter()
            .map(|(column, _)| ColumnStats {
                column,
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
                "{}: extent {} of {} holds {} rows, more than the {} one read may hold (ADR 018)",
                self.root, u.extent, u.table, u.meta.rows, self.max_unit_rows
            )));
        }
        let declared = self.table_schema(&u.table)?;
        let mut fields = Vec::with_capacity(columns.len());
        for c in columns {
            let (_, t) = declared.iter().find(|(n, _)| n == c).ok_or_else(|| {
                Error::Source(format!(
                    "{}: table {} has no column {c:?}",
                    self.root, u.table
                ))
            })?;
            fields.push(Field::new(c, arrow_type(t), true));
        }
        let schema: SchemaRef = Arc::new(Schema::new(fields));
        let projection = if columns.is_empty() {
            "| count".to_string()
        } else {
            format!(
                "| project {}",
                columns
                    .iter()
                    .map(|c| format!("['{}']", c.replace('\'', "\\'")))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let (_, rows) = self.query(
            &format!(
                "{} | where extent_id() == guid({}) {projection}",
                ident(&u.table),
                u.extent
            ),
            &format!("bw-{}-read", Tag::run()),
        )?;
        let n = if columns.is_empty() {
            rows.first()
                .and_then(|r| r.first())
                .and_then(num)
                .unwrap_or(0)
        } else {
            rows.len() as u64
        };
        if n != u.meta.rows {
            return Err(Error::Source(format!(
                "{}: extent {} returned {n} rows where its listing says {}; the table changed \
                 while it was read, so list it again",
                self.root, u.extent, u.meta.rows
            )));
        }
        if columns.is_empty() {
            return RecordBatch::try_new_with_options(
                schema,
                Vec::new(),
                &arrow::array::RecordBatchOptions::new()
                    .with_row_count(Some(usize::try_from(n).unwrap_or(usize::MAX))),
            )
            .map_err(|e| Error::Source(e.to_string()));
        }
        let arrays = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let values: Vec<&Value> = rows.iter().map(|r| &r[i]).collect();
                column(&self.root, f.name(), f.data_type(), &values)
            })
            .collect::<Result<Vec<_>>>()?;
        RecordBatch::try_new(schema, arrays).map_err(|e| Error::Source(e.to_string()))
    }

    fn schema(&self, _root: &str) -> Result<SchemaRef> {
        let mut shape: Option<(String, Vec<(String, String)>)> = None;
        for t in self.table_names()? {
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
        let (_, cols) = shape
            .ok_or_else(|| Error::Source(format!("{}: the database has no table", self.root)))?;
        Ok(Arc::new(Schema::new(
            cols.into_iter()
                .map(|(n, t)| Field::new(n, arrow_type(&t), true))
                .collect::<Vec<_>>(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Array as _;

    use super::*;

    #[test]
    fn timespans_parse_to_nanoseconds() {
        assert_eq!(timespan_ns("00:00:01.5"), Some(1_500_000_000));
        assert_eq!(
            timespan_ns("1.02:03:04.0000001"),
            Some(((26 * 60 + 3) * 60 + 4) * 1_000_000_000 + 100)
        );
        assert_eq!(timespan_ns("00:00:00"), Some(0));
        assert_eq!(timespan_ns("x"), None);
        assert_eq!(timespan_ns("00:00:01.1234567891"), None);
    }

    #[test]
    fn a_show_queries_row_yields_the_logged_numbers() {
        let row: Map<String, Value> = serde_json::from_value(json!({
            "State": "Completed",
            "Duration": "00:00:02.5000000",
            "TotalCpu": "00:00:07.2500000",
            "MemoryPeak": 123_456_789,
            "ScannedExtentsStatistics": "{\"ScannedExtentsCount\":4,\"TotalExtentsCount\":40,\"ScannedRowsCount\":1000,\"TotalRowsCount\":10000}",
            "CacheStatistics": {"Shards": {"Hot": {"HitBytes": 10, "MissBytes": 5}, "Cold": {"HitBytes": 1, "MissBytes": 0}, "BypassBytes": 2}},
            "ResultSetStatistics": "{\"TableCount\":1,\"TablesStatistics\":[{\"RowCount\":7,\"TableSize\":99}]}",
            "FailureReason": ""
        }))
        .unwrap();
        let tag = Tag::new("r", "p0-before").unwrap();
        let s = stats_of("kusto://c/db", &tag, &row).unwrap();
        assert_eq!(s.read_rows, 1000);
        assert_eq!(s.read_bytes, 18);
        assert_eq!(s.peak_memory_bytes, 123_456_789);
        assert_eq!(s.cpu_ns, Some(7_250_000_000));
        assert_eq!(s.duration_ns, 2_500_000_000);
        assert_eq!(s.result_rows, 7);
        assert_eq!((s.units_read, s.units_total), (Some(4), Some(40)));
    }

    #[test]
    fn a_failed_completion_frame_is_refused_with_the_engine_message() {
        let ok = br#"...,"Rows":[[1]]},{"FrameType":"DataSetCompletion","HasErrors":false,"Cancelled":false}]"#;
        tail_completed("k", ok).unwrap();
        let bad = br#"x},{"FrameType":"DataSetCompletion","HasErrors":true,"Cancelled":false,"OneApiErrors":[{"error":{"code":"LimitsExceeded","@message":"Query exceeded the memory limit"}}]}]"#;
        let e = tail_completed("k", bad).unwrap_err().to_string();
        assert!(e.contains("memory limit"), "{e}");
        assert!(tail_completed("k", b"[{\"FrameType\":\"DataTable\"").is_err());
    }

    #[test]
    fn primary_results_convert_to_typed_columns() {
        let body = serde_json::to_vec(&json!([
            {"FrameType": "DataSetHeader"},
            {"FrameType": "DataTable", "TableKind": "QueryProperties", "Columns": [], "Rows": []},
            {"FrameType": "DataTable", "TableKind": "PrimaryResult",
             "Columns": [{"ColumnName": "id", "ColumnType": "long"},
                         {"ColumnName": "ts", "ColumnType": "datetime"},
                         {"ColumnName": "amt", "ColumnType": "decimal"},
                         {"ColumnName": "x", "ColumnType": "real"}],
             "Rows": [[1, "2024-01-02T03:04:05.1234567Z", "12.30", "NaN"], [null, null, null, 1.5]]},
            {"FrameType": "DataSetCompletion", "HasErrors": false, "Cancelled": false}
        ]))
        .unwrap();
        let (cols, rows) = v2_primary("k", &body).unwrap();
        assert_eq!(cols[1], ("ts".to_string(), "datetime".to_string()));
        let ts: Vec<&Value> = rows.iter().map(|r| &r[1]).collect();
        let a = column("k", "ts", &arrow_type("datetime"), &ts).unwrap();
        let a = a
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(a.value(0), 1_704_164_645_123_456_700);
        assert!(a.is_null(1));
        let amt: Vec<&Value> = rows.iter().map(|r| &r[2]).collect();
        let a = column("k", "amt", &arrow_type("decimal"), &amt).unwrap();
        let a = a
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(a.value(0), "12.30");
        let bad = serde_json::to_vec(&json!([
            {"FrameType": "DataTable", "TableKind": "PrimaryResult", "Columns": [{"ColumnName": "a", "ColumnType": "long"}], "Rows": [[1, 2]]}
        ]))
        .unwrap();
        assert!(v2_primary("k", &bad).is_err());
        let x = json!("x");
        let wrong: Vec<&Value> = vec![&x];
        assert!(column("k", "id", &DataType::Int64, &wrong).is_err());
    }
}
