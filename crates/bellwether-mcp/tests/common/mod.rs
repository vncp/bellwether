//! Test corpora written directly with the `parquet` crate, and an oracle
//! that reads them back without any engine code.

#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::case_sensitive_file_extension_comparisons,
    clippy::needless_pass_by_value
)]

use std::collections::BTreeSet;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// `id: Int64, batch: Int64, ts: Timestamp(µs, UTC), payload: Utf8`.
pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("batch", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

/// One row: `(id, batch, ts micros, payload)`.
pub type Row = (i64, i64, Option<i64>, String);

/// Writes `row_groups` (each a list of rows) to `path`, one Parquet row
/// group per entry.
pub fn write_file(path: &Path, row_groups: &[Vec<Row>]) {
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(usize::MAX))
        .build();
    let file = File::create(path).unwrap();
    let mut w = ArrowWriter::try_new(file, schema(), Some(props)).unwrap();
    for rows in row_groups {
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef,
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1))),
                Arc::new(
                    TimestampMicrosecondArray::from(rows.iter().map(|r| r.2).collect::<Vec<_>>())
                        .with_timezone("UTC"),
                ),
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|r| r.3.clone()),
                )),
            ],
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.flush().unwrap();
    }
    w.close().unwrap();
}

/// A versioned ledger: `files` files of `rgs` row groups of `rows` rows.
/// Row `k` of the whole corpus has id `k % entities` (so each id has
/// several versions spread over row groups), batch `k / 50` (a write
/// stamp), ts `k` seconds, and a payload.
pub fn ledger_corpus(dir: &Path, files: usize, rgs: usize, rows: usize, entities: i64) {
    ledger_corpus_with(dir, files, rgs, rows, |k| k % entities);
}

/// [`ledger_corpus`] with row `k`'s id `(k * 37) % entities`: consecutive
/// rows get ids far apart, so every row group's id range spans nearly the
/// whole key space and the files' own min/max statistics skip nothing for
/// a point lookup. The loop tests use it: on ids sorted into row groups,
/// min/max pruning already reads what `exact:id` would, so `why`
/// recommends nothing and `fix` refuses `nothing_to_gain`. `entities` must
/// not be a multiple of 37.
pub fn scattered_corpus(dir: &Path, files: usize, rgs: usize, rows: usize, entities: i64) {
    assert_ne!(entities % 37, 0, "37 must not divide entities");
    ledger_corpus_with(dir, files, rgs, rows, |k| (k * 37) % entities);
}

fn ledger_corpus_with(dir: &Path, files: usize, rgs: usize, rows: usize, id: impl Fn(i64) -> i64) {
    std::fs::create_dir_all(dir).unwrap();
    let mut k: i64 = 0;
    for f in 0..files {
        let mut groups = Vec::new();
        for _ in 0..rgs {
            let mut g = Vec::new();
            for _ in 0..rows {
                g.push((id(k), k / 50, Some(k * 1_000_000), format!("p{k:06}")));
                k += 1;
            }
            groups.push(g);
        }
        write_file(&dir.join(format!("part-{f:03}.parquet")), &groups);
    }
}

/// Writes a key file with one Int64 column `name`.
pub fn write_keys(path: &Path, name: &str, keys: &[i64]) {
    let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(keys.to_vec())) as ArrayRef],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// Oracle: `(file name, row group index)` of every row group under `dir`
/// holding an `id` in `keys`, read with the `parquet` crate.
pub fn oracle(dir: &Path, keys: &[i64]) -> BTreeSet<(String, usize)> {
    let want: BTreeSet<i64> = keys.iter().copied().collect();
    let mut out = BTreeSet::new();
    let mut names: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
        .collect();
    names.sort();
    for p in names {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&p).unwrap()).unwrap();
        let n = builder.metadata().num_row_groups();
        for rg in 0..n {
            let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&p).unwrap())
                .unwrap()
                .with_row_groups(vec![rg])
                .build()
                .unwrap();
            for b in reader {
                let b = b.unwrap();
                let ids = b
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                if (0..ids.len()).any(|i| want.contains(&ids.value(i))) {
                    out.insert((p.file_name().unwrap().to_string_lossy().to_string(), rg));
                }
            }
        }
    }
    out
}

/// The ledger file of `hash` under an index directory.
pub fn ledger_bytes(index: &Path, hash: &str) -> Vec<u8> {
    std::fs::read(index.join("ledger").join(format!("{hash}.rec"))).unwrap()
}

// ---------------------------------------------------------------------------
// MCP helpers: the server over a folder, and the store's object list read
// straight from the local index directory (no engine crate involved).
// ---------------------------------------------------------------------------

/// The fixed clock tests run under, so records are byte-identical.
pub const CLOCK: &str = "2026-10-24T12:00:00.000Z";

/// A server over `data` with a fixed clock and `budget`.
pub fn server_with(data: &Path, budget: bellwether_mcp::Budget) -> bellwether_mcp::Server {
    server_at(data, budget, Some(CLOCK))
}

/// A server over `data` with `budget`, under `clock` (`None`: the system
/// clock, so two records written at different moments differ).
pub fn server_at(
    data: &Path,
    budget: bellwether_mcp::Budget,
    clock: Option<&str>,
) -> bellwether_mcp::Server {
    bellwether_mcp::Server::new(&bellwether_mcp::Config {
        root: data.to_str().unwrap().to_string(),
        engine: bellwether::EngineOptions {
            clock: clock.map(str::to_string),
            ..bellwether::EngineOptions::default()
        },
        budget,
        join: None,
        workspace: false,
    })
    .unwrap()
}

/// Write-nothing tests run under both: a fixed clock makes a rewritten
/// record byte-identical to the one already stored and could hide a write.
pub const CLOCKS: [Option<&str>; 2] = [Some(CLOCK), None];

/// A server over `data` with a fixed clock and the default budget.
pub fn server(data: &Path) -> bellwether_mcp::Server {
    server_with(data, bellwether_mcp::Budget::default())
}

/// Every file under `dir`, recursively, with its bytes, sorted by path.
pub fn objects(dir: &Path) -> std::collections::BTreeMap<String, Vec<u8>> {
    let mut out = std::collections::BTreeMap::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in rd {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else {
                let rel = p.strip_prefix(dir).unwrap().to_string_lossy().to_string();
                out.insert(rel, std::fs::read(&p).unwrap());
            }
        }
    }
    out
}

/// What changed between two object lists: the kinds of new ledger records
/// (by their `"record"` tag), and every other new or changed path.
pub fn delta(
    before: &std::collections::BTreeMap<String, Vec<u8>>,
    after: &std::collections::BTreeMap<String, Vec<u8>>,
) -> (Vec<String>, Vec<String>) {
    let mut kinds = Vec::new();
    let mut other = Vec::new();
    for (p, bytes) in after {
        if before.get(p) == Some(bytes) {
            continue;
        }
        if p.starts_with("ledger/") && p.ends_with(".rec") {
            let v: serde_json::Value = serde_json::from_slice(bytes).unwrap();
            kinds.push(v["record"].as_str().unwrap_or("?").to_string());
        } else {
            other.push(p.clone());
        }
    }
    for p in before.keys() {
        if !after.contains_key(p) {
            other.push(format!("removed {p}"));
        }
    }
    kinds.sort();
    kinds.dedup();
    (kinds, other)
}

/// Calls `tool` and asserts it was not refused.
pub fn ok(
    s: &bellwether_mcp::Server,
    tool: &str,
    args: serde_json::Value,
) -> bellwether_mcp::Reply {
    let r = s.call(tool, args);
    assert!(
        !r.is_error(),
        "{tool} refused: {}",
        serde_json::to_string(&r.envelope.refusal).unwrap()
    );
    r
}

/// The refusal code of a refused call.
pub fn code(r: &bellwether_mcp::Reply) -> String {
    assert!(r.is_error(), "not refused: {}", r.text);
    r.envelope
        .refusal
        .as_ref()
        .unwrap()
        .code
        .as_str()
        .to_string()
}

/// The result text parsed.
pub fn json(r: &bellwether_mcp::Reply) -> serde_json::Value {
    serde_json::from_str(&r.text).unwrap()
}

/// Waits for a job to finish and returns its result.
pub fn finish(s: &bellwether_mcp::Server, id: &str) -> bellwether_mcp::Reply {
    for _ in 0..6000 {
        let st = s.call("jobs.status", serde_json::json!({ "job": id }));
        let state = st.envelope.job.as_ref().unwrap().state;
        if state == bellwether_mcp::jobs::JobState::Done {
            return s.call("jobs.result", serde_json::json!({ "job": id }));
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    panic!("job {id} did not finish");
}

// ---------------------------------------------------------------------------
// A minimal MCP client over a child process's stdin and stdout: one JSON-RPC
// message per line, as the stdio transport frames them.
// ---------------------------------------------------------------------------

/// The protocol version the client asks for in `initialize`.
pub const PROTOCOL: &str = "2025-11-25";

/// A server child process and the client end of its stdio.
pub struct Client {
    child: std::process::Child,
    stdin: Option<std::process::ChildStdin>,
    stdout: std::io::BufReader<std::process::ChildStdout>,
    next: u64,
    /// Every line the server wrote to stdout, in order.
    pub lines: Vec<String>,
}

impl Client {
    /// Spawns `exe` with `args`, stdin and stdout piped, stderr captured.
    pub fn spawn(exe: &str, args: &[&str]) -> Self {
        let mut child = std::process::Command::new(exe)
            .args(args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take();
        let stdout = std::io::BufReader::new(child.stdout.take().unwrap());
        Self {
            child,
            stdin,
            stdout,
            next: 1,
            lines: Vec::new(),
        }
    }

    fn send(&mut self, msg: &serde_json::Value) {
        use std::io::Write as _;
        let stdin = self.stdin.as_mut().unwrap();
        let mut line = serde_json::to_string(msg).unwrap();
        line.push('\n');
        stdin.write_all(line.as_bytes()).unwrap();
        stdin.flush().unwrap();
    }

    /// Sends a request and returns its response (`result` or `error`),
    /// skipping notifications. Every stdout line must be a JSON-RPC 2.0
    /// message: stdout is the protocol channel and nothing else.
    pub fn request(&mut self, method: &str, params: serde_json::Value) -> serde_json::Value {
        use std::io::BufRead as _;
        let id = self.next;
        self.next += 1;
        self.send(&serde_json::json!({
            "jsonrpc": "2.0", "id": id, "method": method, "params": params
        }));
        loop {
            let mut line = String::new();
            let n = self.stdout.read_line(&mut line).unwrap();
            assert!(n > 0, "the server closed stdout during {method}");
            let v: serde_json::Value = serde_json::from_str(line.trim_end())
                .unwrap_or_else(|e| panic!("a stdout line is not JSON ({e}): {line:?}"));
            assert_eq!(v["jsonrpc"], "2.0", "{line}");
            self.lines.push(line);
            if v["id"] == serde_json::json!(id) {
                return v;
            }
        }
    }

    /// Sends a notification.
    pub fn notify(&mut self, method: &str) {
        self.send(&serde_json::json!({ "jsonrpc": "2.0", "method": method }));
    }

    /// The handshake: `initialize`, then `notifications/initialized`.
    pub fn initialize(&mut self) -> serde_json::Value {
        let r = self.request(
            "initialize",
            serde_json::json!({
                "protocolVersion": PROTOCOL,
                "capabilities": {},
                "clientInfo": { "name": "bellwether-test", "version": "0" }
            }),
        );
        assert!(r.get("error").is_none(), "{r}");
        self.notify("notifications/initialized");
        r
    }

    /// `tools/call`, returning the result object.
    pub fn call(&mut self, tool: &str, args: serde_json::Value) -> serde_json::Value {
        let r = self.request(
            "tools/call",
            serde_json::json!({ "name": tool, "arguments": args }),
        );
        assert!(r.get("error").is_none(), "{r}");
        r["result"].clone()
    }

    /// Closes stdin and waits: the server's exit status, stdout left, and
    /// stderr.
    pub fn close(mut self) -> (std::process::ExitStatus, String, String) {
        use std::io::Read as _;
        drop(self.stdin.take());
        let mut rest = String::new();
        self.stdout.read_to_string(&mut rest).unwrap();
        let mut err = String::new();
        if let Some(mut e) = self.child.stderr.take() {
            e.read_to_string(&mut err).unwrap();
        }
        (self.child.wait().unwrap(), rest, err)
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The text content of a `tools/call` result.
pub fn text(result: &serde_json::Value) -> &str {
    result["content"][0]["text"].as_str().unwrap()
}
