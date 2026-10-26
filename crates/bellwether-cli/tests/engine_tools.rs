//! ADR 010 for the engine loop of ADR 033, against a real ClickHouse
//! server: for the same inputs on the same ledger, the text each MCP engine
//! tool returns is the CLI's `--json` line. The twin diagnosis (`why
//! --twin-of --gate` = `score.why`) and the rewrite proposal (`propose
//! --rewrite` = `propose.rewrite`) are byte for byte. The measured steps
//! (`workload record --from-engine` = `score.record`, `fix --verify-only` =
//! `score.verify`, `fix --certificate` = `apply.receipt`) draw a fresh run
//! id for their tags, and the engine's log reports each run's own CPU time,
//! wall time, and peak memory: those three numbers are zeroed, run ids and
//! record hashes renamed consistently, and then the two lines must be the
//! same bytes. Every other number (rows and bytes read, units, result rows,
//! the prediction, the certificate's counts) is compared as is.
//!
//! Runs when `BELLWETHER_CLICKHOUSE_URL` names a server
//! (`clickhouse://127.0.0.1:8123/default`, a user with no password that may
//! create a database); prints `SKIPPED:` and passes otherwise, except in CI
//! (`CI` set), where the workflow starts one and the test must run.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::doc_markdown
)]

mod common;

use std::collections::HashMap;
use std::path::Path;
use std::process::{Command, Stdio};

use common::{path, run};
use serde_json::{json, Value};

const CLOCK: &str = "2026-10-26T12:00:00.000Z";

/// A fresh database on the configured server, dropped on drop.
struct Server {
    http: String,
    host: String,
    db: String,
}

impl Server {
    fn new() -> Option<Self> {
        let Ok(url) = std::env::var("BELLWETHER_CLICKHOUSE_URL") else {
            assert!(
                std::env::var_os("CI").is_none(),
                "CI must run the engine tool tests: set BELLWETHER_CLICKHOUSE_URL"
            );
            eprintln!(
                "SKIPPED: BELLWETHER_CLICKHOUSE_URL is not set (for example \
                 clickhouse://127.0.0.1:8123/default); the engine tool tests need a server"
            );
            return None;
        };
        let u = bellwether::source_engine::EngineUri::parse(&url).unwrap();
        let db = format!("bw_cli_{}", bellwether::source_engine::Tag::run());
        let s = Self {
            http: format!("http://{}/", u.host),
            host: u.host,
            db,
        };
        s.sql(&format!("CREATE DATABASE {}", s.db));
        Some(s)
    }

    /// Runs `q` over HTTP; returns the response body.
    fn post(&self, q: &str) -> Vec<u8> {
        let mut child = Command::new("curl")
            .args(["-sS", "--fail-with-body", "--data-binary", "@-", &self.http])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("curl");
        {
            use std::io::Write;
            child.stdin.take().unwrap().write_all(q.as_bytes()).unwrap();
        }
        let out = child.wait_with_output().unwrap();
        assert!(
            out.status.success(),
            "{q}: {}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        out.stdout
    }

    fn sql(&self, q: &str) -> String {
        String::from_utf8(self.post(q)).unwrap()
    }

    fn uri(&self) -> String {
        format!("clickhouse://{}/{}/orders", self.host, self.db)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = Command::new("curl")
            .args([
                "-sS",
                "--data-binary",
                &format!("DROP DATABASE IF EXISTS {}", self.db),
                &self.http,
            ])
            .output();
    }
}

/// `text` with every run id (16 lowercase hex digits after a `-`) and
/// every record hash (64 lowercase hex digits) renamed in order of first
/// appearance.
fn renamed(text: &str) -> String {
    let b = text.as_bytes();
    let hex = |c: u8| c.is_ascii_digit() || (b'a'..=b'f').contains(&c);
    let mut names: HashMap<&str, String> = HashMap::new();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < b.len() {
        if hex(b[i]) && (i == 0 || !hex(b[i - 1]) && !b[i - 1].is_ascii_alphabetic()) {
            let mut j = i;
            while j < b.len() && hex(b[j]) {
                j += 1;
            }
            let run = j - i;
            let bounded = j == b.len() || !b[j].is_ascii_alphanumeric();
            let after_dash = i > 0 && b[i - 1] == b'-';
            if bounded && (run == 64 || (run == 16 && after_dash)) {
                let n = names.len();
                let name = names
                    .entry(&text[i..j])
                    .or_insert_with(|| format!("<{}{n}>", if run == 64 { "h" } else { "run" }));
                out.push_str(name);
            } else {
                out.push_str(&text[i..j]);
            }
            i = j;
            continue;
        }
        out.push(char::from(b[i]));
        i += 1;
    }
    out
}

/// The numbers an engine's log reports differently for every run of the
/// same query.
const PER_RUN: [&str; 3] = ["cpu_ns", "duration_ns", "peak_memory_bytes"];

/// `line` parsed, every [`PER_RUN`] number set to 0, serialized again, and
/// [`renamed`].
fn measured(line: &str) -> String {
    fn zero(v: &mut Value) {
        match v {
            Value::Object(m) => {
                for (k, x) in m.iter_mut() {
                    if PER_RUN.contains(&k.as_str()) && x.is_number() {
                        *x = json!(0);
                    } else {
                        zero(x);
                    }
                }
            }
            Value::Array(a) => a.iter_mut().for_each(zero),
            _ => {}
        }
    }
    let mut v: Value = serde_json::from_str(line.trim_end()).unwrap();
    zero(&mut v);
    renamed(&serde_json::to_string(&v).unwrap())
}

/// Asserts `cli == mcp`, showing the text around the first differing byte.
fn same(cli: &str, mcp: &str, what: &str) {
    if cli == mcp {
        return;
    }
    let at = cli
        .bytes()
        .zip(mcp.bytes())
        .position(|(a, b)| a != b)
        .unwrap_or(cli.len().min(mcp.len()));
    let from = at.saturating_sub(120);
    let around = |s: &str| {
        String::from_utf8_lossy(&s.as_bytes()[from..(at + 120).min(s.len())]).into_owned()
    };
    panic!(
        "{what}: the CLI line and the MCP text differ at byte {at}\n cli: …{}\n mcp: …{}",
        around(cli),
        around(mcp)
    );
}

fn h(v: &Value, ptr: &str) -> String {
    v.pointer(ptr)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("no {ptr} in {v}"))
        .to_string()
}

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let p = e.unwrap().path();
        let dest = to.join(p.file_name().unwrap());
        if p.is_dir() {
            copy_tree(&p, &dest);
        } else {
            std::fs::copy(&p, &dest).unwrap();
        }
    }
}

#[test]
fn the_engine_loop_returns_the_cli_json_line_from_every_engine_tool() {
    let Some(s) = Server::new() else { return };
    // The engine's table: 4,000 rows in 4 partitions; `id` repeats every
    // 1,000 rows, `batch` is the partition.
    s.sql(&format!(
        "CREATE TABLE {}.orders (id UInt64, batch UInt64, amount Int64) ENGINE = MergeTree \
         PARTITION BY batch ORDER BY id",
        s.db
    ));
    s.sql(&format!(
        "INSERT INTO {}.orders SELECT number % 1000, intDiv(number, 1000), toInt64(number) \
         FROM numbers(4000)",
        s.db
    ));
    // The lake twin: the same rows as Parquet, one file per partition, row
    // groups of 250 rows.
    let dir = tempfile::tempdir().unwrap();
    let lake = dir.path().join("lake");
    std::fs::create_dir_all(&lake).unwrap();
    for b in 0..4 {
        let bytes = s.post(&format!(
            "SELECT id, batch, amount FROM {}.orders WHERE batch = {b} ORDER BY id \
             FORMAT Parquet SETTINGS output_format_parquet_row_group_size = 250",
            s.db
        ));
        std::fs::write(lake.join(format!("part-{b}.parquet")), bytes).unwrap();
    }
    let scratch = dir.path().join("scratch");
    std::fs::create_dir_all(&scratch).unwrap();
    let keys_sql = dir.path().join("keys.sql");
    std::fs::write(
        &keys_sql,
        format!(
            "SELECT id, batch FROM {}.orders WHERE id IN (3, 7) AND batch = 0",
            s.db
        ),
    )
    .unwrap();
    let before = dir.path().join("before.sql");
    let after = dir.path().join("after.sql");
    std::fs::write(
        &before,
        format!(
            "SELECT id, batch, amount FROM {}.orders WHERE (id, batch) IN (SELECT id, batch \
             FROM {}.orders WHERE id IN (3, 7) AND batch = 0)",
            s.db, s.db
        ),
    )
    .unwrap();
    std::fs::write(
        &after,
        format!(
            "SELECT id, batch, amount FROM {}.orders WHERE batch = 0 AND id IN (3, 7)",
            s.db
        ),
    )
    .unwrap();
    let uri = s.uri();
    let (d, sc) = (path(&lake), path(&scratch));
    let srv = bellwether_mcp::Server::new(&bellwether_mcp::Config {
        root: d.to_string(),
        engine: bellwether::EngineOptions {
            clock: Some(CLOCK.into()),
            twin_of: Some(uri.clone()),
            scratch: Some(sc.to_string()),
            ..bellwether::EngineOptions::default()
        },
        budget: bellwether_mcp::Budget::default(),
    })
    .unwrap();
    let mcp = |tool: &str, args: Value| -> (String, Value) {
        let r = srv.call(tool, args);
        assert!(
            !r.is_error(),
            "{tool} refused: {}",
            serde_json::to_string(&r.envelope.refusal).unwrap()
        );
        let v: Value = serde_json::from_str(&r.text).unwrap();
        (r.text + "\n", v)
    };
    let cli = |args: &[&str]| run(&[&["--clock", CLOCK, "--json"], args].concat(), 0);

    // score.record = workload record --from-engine.
    let (m, v) = mcp(
        "score.record",
        json!({"keys_query": std::fs::read_to_string(&keys_sql).unwrap(), "wait": true}),
    );
    let c = cli(&[
        "workload",
        "record",
        d,
        "--from-engine",
        &uri,
        "--keys-query",
        path(&keys_sql),
        "--scratch",
        sc,
    ]);
    same(&measured(&c), &measured(&m), "score.record");
    assert_ne!(c, m, "two runs, two tags");
    let wl = h(&v, "/workload");
    assert_eq!(v["keys"], 2, "{v}");

    // score.why = why --workload --twin-of --gate: byte for byte.
    let (m, v) = mcp(
        "score.why",
        json!({"workload": wl, "gates": [{"name": "in", "limit": 1}], "wait": true}),
    );
    let c = cli(&[
        "why",
        d,
        "--workload",
        &wl,
        "--twin-of",
        &uri,
        "--gate",
        "in:1",
    ]);
    same(&c, &m, "score.why");
    assert_eq!(v["twin"]["of"]["engine"], uri, "{v}");
    assert_eq!(v["twin"]["of"]["engine_rows"], 4000, "{v}");
    let ev = h(&v, "/evidence/hash");

    // propose.rewrite = propose --rewrite: byte for byte.
    let shape = bellwether::engine_rewrite(
        vec![bellwether::types::QueryPair {
            before: std::fs::read_to_string(&before).unwrap(),
            after: std::fs::read_to_string(&after).unwrap(),
        }],
        Vec::new(),
        None,
    );
    let (m, v) = mcp(
        "propose.rewrite",
        json!({"shape": serde_json::to_value(&shape).unwrap(), "evidence": [ev], "by": "agent"}),
    );
    let pair = format!("{}={}", path(&before), path(&after));
    let c = cli(&[
        "propose",
        d,
        "--rewrite",
        &pair,
        "--evidence",
        &ev,
        "--by",
        "agent",
    ]);
    same(&c, &m, "propose.rewrite");
    let p = h(&v, "/proposal/hash");

    // score.verify = fix --verify-only: measured in the engine each time.
    let (m, v) = mcp("score.verify", json!({"proposal": p, "wait": true}));
    let c = cli(&[
        "fix",
        d,
        "--proposal",
        &p,
        "--verify-only",
        "--twin-of",
        &uri,
        "--scratch",
        sc,
    ]);
    same(&measured(&c), &measured(&m), "score.verify");
    assert_eq!(v["score"]["model"], "clickhouse.query_log", "{v}");
    let cert = h(&v, "/certificate/hash");
    let prediction = &v["score"]["replay"]["record"]["prediction"];
    assert_eq!(prediction["predicted_rows"], 2, "{v}");

    // apply.receipt = fix --certificate, once from each side over the same
    // ledger state.
    let index = lake.join(".bellwether");
    let saved = dir.path().join("saved-index");
    copy_tree(&index, &saved);
    let (m, v) = mcp("apply.receipt", json!({"certificate": cert, "wait": true}));
    std::fs::remove_dir_all(&index).unwrap();
    copy_tree(&saved, &index);
    let c = cli(&[
        "fix",
        d,
        "--certificate",
        &cert,
        "--twin-of",
        &uri,
        "--scratch",
        sc,
    ]);
    same(&measured(&c), &measured(&m), "apply.receipt");
    assert_eq!(v["outcome"]["kind"], "attested", "{v}");
    assert_eq!(
        v["outcome"]["receipt"]["record"]["attest_only"], true,
        "{v}"
    );
    // The scratch folder is empty again: every export was deleted.
    assert_eq!(std::fs::read_dir(&scratch).unwrap().count(), 0);
}
