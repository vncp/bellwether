//! The engine tools of ADR 033 over MCP, with an in-process query engine
//! (`EngineOptions::engine_adapter`):
//!
//! - ADR 010: each tool's text is the facade's result serialized exactly as
//!   the CLI's `--json` line prints it (`serde_json::to_string`), byte for
//!   byte. A measurement draws a fresh run id for its tags (an engine's log
//!   finds a query by tag, so two runs never share one), and the records
//!   covering a tag get other hashes; for `score.record`, `score.verify`,
//!   and `apply.receipt` the two texts are compared byte for byte after a
//!   consistent renaming of run ids and record hashes, and nothing else may
//!   differ. The CLI binary against a real engine is
//!   `crates/bellwether-cli/tests/engine_tools.rs`.
//! - `caller_supplied_cost`: a score, cost, measurement, replay, prediction,
//!   verdict, or certificate field in any tool's arguments, at the top level
//!   or nested, called directly or started as a job, is refused by name and
//!   writes nothing.
//! - `export_denied` names the permission, and no certificate is written.
//!
//! Deliberate break (made once in a scratch copy; the named test failed):
//! map a schema error to `invalid_input` in `jobs.start` only
//! (`a_caller_supplied_cost_is_refused_by_name_and_writes_nothing`).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::assert_is_empty
)]

mod common;

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bellwether::source_engine::{EngineAdapter, Exported, MeasuredEngine, ResultHandle, Tag};
use bellwether::types::{
    Error, FileFingerprint, Gate, Hash, QueryStats, Result, RowGroupMeta, Source,
};
use bellwether::{Engine, EngineConnector, EngineOptions, FixRequest, WhyRequest};
use common::{code, delta, json, objects, ok};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde_json::{json, Value};

const ROOT: &str = "clickhouse://fake:8123/db/orders";
const CLOCK: &str = "2026-10-26T00:00:00.000Z";

#[derive(Default)]
struct State {
    queries: HashMap<String, (RecordBatch, QueryStats)>,
    /// Runs: tag and query.
    runs: Vec<(String, String)>,
    deny: bool,
}

struct Fake(Arc<Mutex<State>>);

impl Fake {
    fn run(&self, query: &str, tag: &Tag) -> Result<ResultHandle> {
        let mut s = self.0.lock().unwrap();
        if !s.queries.contains_key(query.trim()) {
            return Err(Error::Source(format!("fake: unknown query {query}")));
        }
        s.runs.push((tag.to_string(), query.trim().to_string()));
        Ok(ResultHandle {
            tag: tag.clone(),
            query: query.trim().to_string(),
            logged: query.trim().to_string(),
            exported: None,
        })
    }
}

impl MeasuredEngine for Fake {
    fn root(&self) -> &str {
        ROOT
    }
    fn model(&self) -> &'static str {
        "clickhouse.query_log"
    }
    fn submit(&self, query: &str, tag: &Tag) -> Result<ResultHandle> {
        self.run(query, tag)
    }
    fn export(&self, query: &str, tag: &Tag, dir: &Path) -> Result<ResultHandle> {
        if self.0.lock().unwrap().deny {
            return Err(Error::ExportDenied {
                engine: ROOT.into(),
                permission: "the fake export grant".into(),
                message: format!("{ROOT} refused the export; grant the fake export grant"),
            });
        }
        let mut handle = self.run(query, tag)?;
        let batch = self.0.lock().unwrap().queries[query.trim()].0.clone();
        std::fs::create_dir_all(dir).unwrap();
        let path = dir.join("part-0.parquet");
        let mut w =
            ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let bytes = std::fs::metadata(&path).unwrap().len();
        handle.exported = Some(Exported {
            files: vec![path],
            bytes,
        });
        Ok(handle)
    }
    fn stats(&self, run: &ResultHandle) -> Result<QueryStats> {
        let s = self.0.lock().unwrap();
        let q = s
            .runs
            .iter()
            .find(|(t, _)| t == run.tag.as_str())
            .map(|(_, q)| q.clone())
            .ok_or_else(|| Error::Source(format!("fake: no log entry for {}", run.tag)))?;
        let (batch, template) = &s.queries[&q];
        let mut st = template.clone();
        st.tag = run.tag.to_string();
        st.result_rows = batch.num_rows() as u64;
        Ok(st)
    }
}

impl Source for Fake {
    fn discover(&self, _root: &str) -> Result<Vec<FileFingerprint>> {
        Ok(["p0", "p1"]
            .iter()
            .map(|p| FileFingerprint {
                path: format!("{ROOT}/{p}"),
                size: 800,
                etag: Some("v1;rows=800".into()),
                tail_blake3: Hash::of(p.as_bytes()),
            })
            .collect())
    }
    fn row_groups(&self, _f: &FileFingerprint) -> Result<Vec<RowGroupMeta>> {
        Ok(Vec::new())
    }
    fn read_columns(&self, _rg: &RowGroupMeta, _c: &[String]) -> Result<RecordBatch> {
        Err(Error::Source("fake: no reads".into()))
    }
    fn schema(&self, _root: &str) -> Result<SchemaRef> {
        Ok(Arc::new(Schema::empty()))
    }
}

fn batch(rows: &[(u64, u64)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("batch", DataType::UInt64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn stats(read_rows: u64, read_bytes: u64) -> QueryStats {
    QueryStats {
        tag: String::new(),
        read_rows,
        read_bytes,
        peak_memory_bytes: 1_000_000,
        cpu_ns: Some(1_000_000),
        duration_ns: 2_000_000,
        result_rows: 2,
        units_read: Some(2),
        units_total: Some(2),
    }
}

/// The lake twin: 4 files of 400 rows, row groups of 100; `id` cycles over
/// 200 values, `batch` is `row / 400`.
fn lake(dir: &Path) -> PathBuf {
    let out = dir.join("lake");
    std::fs::create_dir_all(&out).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("batch", DataType::UInt64, false),
    ]));
    let mut row = 0u64;
    for f in 0..4 {
        let mut id = Vec::new();
        let mut b = Vec::new();
        for _ in 0..400 {
            id.push(row % 200);
            b.push(row / 400);
            row += 1;
        }
        let rb = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(UInt64Array::from(id)) as ArrayRef,
                Arc::new(UInt64Array::from(b)),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(100))
            .build();
        let mut w = ArrowWriter::try_new(
            File::create(out.join(format!("part-{f:03}.parquet"))).unwrap(),
            Arc::clone(&schema),
            Some(props),
        )
        .unwrap();
        w.write(&rb).unwrap();
        w.close().unwrap();
    }
    out
}

struct World {
    dir: tempfile::TempDir,
    root: PathBuf,
    state: Arc<Mutex<State>>,
}

impl World {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = lake(dir.path());
        let state = Arc::new(Mutex::new(State::default()));
        let rows = [(3, 0), (7, 0)];
        {
            let mut s = state.lock().unwrap();
            s.queries
                .insert("SELECT keys".into(), (batch(&rows), stats(1600, 12_800)));
            s.queries
                .insert("SELECT before".into(), (batch(&rows), stats(1600, 64_000)));
            s.queries
                .insert("SELECT after".into(), (batch(&rows), stats(400, 16_000)));
        }
        Self { dir, root, state }
    }

    fn options(&self) -> EngineOptions {
        let st = Arc::clone(&self.state);
        EngineOptions {
            twin_of: Some(ROOT.to_string()),
            scratch: Some(self.root.join("_scratch").display().to_string()),
            clock: Some(CLOCK.into()),
            engine_adapter: Some(EngineConnector(Arc::new(
                move |_uri: &str, _s: Option<&Path>| -> Result<Box<dyn EngineAdapter>> {
                    Ok(Box::new(Fake(Arc::clone(&st))))
                },
            ))),
            ..EngineOptions::default()
        }
    }

    fn server(&self) -> bellwether_mcp::Server {
        bellwether_mcp::Server::new(&bellwether_mcp::Config {
            root: self.root.to_str().unwrap().to_string(),
            engine: self.options(),
            budget: bellwether_mcp::Budget::default(),
        })
        .unwrap()
    }

    /// The facade over the same folder and ledger, opened as the CLI opens
    /// it.
    fn facade(&self) -> Engine {
        Engine::open(self.root.to_str().unwrap(), self.options()).unwrap()
    }

    fn index(&self) -> PathBuf {
        self.root.join(".bellwether")
    }
}

/// `text` with every run id (16 lowercase hex digits after a `-`) and
/// every record hash (64 lowercase hex digits) renamed in order of first
/// appearance: two texts equal after this are the same bytes up to a
/// consistent renaming of run ids and hashes.
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
                i = j;
                continue;
            }
            out.push_str(&text[i..j]);
            i = j;
            continue;
        }
        out.push(char::from(b[i]));
        i += 1;
    }
    out
}

/// Asserts `a == b`, showing the text around the first differing byte.
fn same(a: &str, b: &str, what: &str) {
    if a == b {
        return;
    }
    let at = a
        .bytes()
        .zip(b.bytes())
        .position(|(x, y)| x != y)
        .unwrap_or(a.len().min(b.len()));
    let from = at.saturating_sub(120);
    let around = |s: &str| {
        String::from_utf8_lossy(&s.as_bytes()[from..(at + 120).min(s.len())]).into_owned()
    };
    panic!(
        "{what}: the MCP text and the facade's JSON line differ at byte {at}\n mcp:    …{}\n facade: …{}",
        around(a),
        around(b)
    );
}

fn h(v: &Value, ptr: &str) -> String {
    v.pointer(ptr)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("no {ptr} in {v}"))
        .to_string()
}

fn hash(s: &str) -> Hash {
    serde_json::from_value(json!(s)).unwrap()
}

/// Copies every file under `from` into `to`.
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
fn every_engine_tool_returns_the_facade_json_line() {
    let w = World::new();
    let srv = w.server();
    let e = w.facade();

    // score.record = workload record --from-engine: a fresh tag per run.
    let m = ok(
        &srv,
        "score.record",
        json!({"keys_query": "SELECT keys", "key_columns": ["id", "batch"], "wait": true}),
    );
    let f = e
        .record_from_engine("SELECT keys", Some(&["id".into(), "batch".into()]))
        .unwrap();
    same(
        &renamed(&m.text),
        &renamed(&serde_json::to_string(&f).unwrap()),
        "score.record",
    );
    assert_ne!(
        m.text,
        serde_json::to_string(&f).unwrap(),
        "two runs, two tags"
    );
    let wl = h(&json(&m), "/workload");
    assert_eq!(wl, f.workload.to_hex());

    // score.why with a twin and gates = why --workload --twin-of --gate:
    // byte for byte (no measurement; the identity is the engine's listing).
    let gates = vec![Gate {
        name: "in".into(),
        limit: 1,
    }];
    let m = ok(
        &srv,
        "score.why",
        json!({"workload": wl, "gates": [{"name": "in", "limit": 1}], "wait": true}),
    );
    let f = e
        .why(&WhyRequest {
            workload: Some(hash(&wl)),
            gates,
            max_candidates: bellwether::DEFAULT_MAX_CANDIDATES,
            ..WhyRequest::default()
        })
        .unwrap();
    same(&m.text, &serde_json::to_string(&f).unwrap(), "score.why");
    let v = json(&m);
    assert!(v["twin"]["of"]["engine"] == ROOT, "{v}");
    let ev = h(&v, "/evidence/hash");

    // propose.rewrite = propose --rewrite: byte for byte.
    let shape = bellwether::engine_rewrite(
        vec![bellwether::types::QueryPair {
            before: "SELECT before".into(),
            after: "SELECT after".into(),
        }],
        Vec::new(),
        None,
    );
    let m = ok(
        &srv,
        "propose.rewrite",
        json!({"shape": serde_json::to_value(&shape).unwrap(), "evidence": [ev], "by": "agent"}),
    );
    let f = e
        .propose(
            bellwether::types::ProposalBody::Rewrite { shape },
            &[hash(&ev)],
            "agent",
        )
        .unwrap();
    same(
        &m.text,
        &serde_json::to_string(&f).unwrap(),
        "propose.rewrite",
    );
    let p = h(&json(&m), "/proposal/hash");

    // score.verify = fix --verify-only: measured, so renamed.
    let m = ok(&srv, "score.verify", json!({"proposal": p, "wait": true}));
    let f = e
        .fix(&FixRequest {
            proposal: Some(hash(&p)),
            verify_only: true,
            ..FixRequest::default()
        })
        .unwrap();
    same(
        &renamed(&m.text),
        &renamed(&serde_json::to_string(&f).unwrap()),
        "score.verify",
    );
    let v = json(&m);
    assert_eq!(v["score"]["model"], "clickhouse.query_log", "{v}");
    let cert = h(&v, "/certificate/hash");

    // apply.receipt = fix --certificate, once from each side over the same
    // ledger state: an attest-only receipt.
    let saved = w.dir.path().join("saved-index");
    copy_tree(&w.index(), &saved);
    let m = ok(
        &srv,
        "apply.receipt",
        json!({"certificate": cert, "wait": true}),
    );
    std::fs::remove_dir_all(w.index()).unwrap();
    copy_tree(&saved, &w.index());
    let f = w
        .facade()
        .fix(&FixRequest {
            certificate: Some(hash(&cert)),
            ..FixRequest::default()
        })
        .unwrap();
    same(
        &renamed(&m.text),
        &renamed(&serde_json::to_string(&f).unwrap()),
        "apply.receipt",
    );
    let v = json(&m);
    assert_eq!(v["outcome"]["kind"], "attested", "{v}");
    assert_eq!(
        v["outcome"]["receipt"]["record"]["attest_only"], true,
        "{v}"
    );
}

#[test]
fn renaming_keeps_everything_but_run_ids_and_hashes() {
    let a = format!(
        "{{\"tag\":\"bw-0123456789abcdef-p0-before\",\"h\":\"{}\",\"n\":1234567890123456}}",
        "a".repeat(64)
    );
    let b = format!(
        "{{\"tag\":\"bw-fedcba9876543210-p0-before\",\"h\":\"{}\",\"n\":1234567890123456}}",
        "b".repeat(64)
    );
    assert_eq!(renamed(&a), renamed(&b));
    // Another count is a difference.
    assert_ne!(
        renamed(&a),
        renamed(&b.replace("1234567890123456", "1234567890123457"))
    );
    // Two hashes in one text that were equal stay equal, and two that were
    // distinct stay distinct.
    let two = |x: &str, y: &str| format!("[\"{x}\",\"{y}\"]");
    let (h1, h2) = ("1".repeat(64), "2".repeat(64));
    assert_ne!(renamed(&two(&h1, &h1)), renamed(&two(&h1, &h2)));
}

#[test]
fn a_caller_supplied_cost_is_refused_by_name_and_writes_nothing() {
    let w = World::new();
    let srv = w.server();
    let ev = h(&json(&ok(&srv, "observe.why", json!({}))), "/evidence/hash");
    let p = "0".repeat(64);
    let base: Vec<(&str, Value)> = vec![
        ("observe.why", json!({})),
        ("score.why", json!({"workload": p, "wait": true})),
        ("score.verify", json!({"proposal": p, "wait": true})),
        (
            "score.record",
            json!({"keys_query": "SELECT keys", "wait": true}),
        ),
        (
            "propose.rewrite",
            json!({"shape": {"before": "b", "after": "a", "invariant": "i",
                             "pairs": [{"before": "SELECT before", "after": "SELECT after"}]},
                   "evidence": [ev], "by": "agent"}),
        ),
        (
            "propose.key",
            json!({"spec": "exact:id", "evidence": [ev], "by": "agent"}),
        ),
        ("apply.receipt", json!({"certificate": p, "wait": true})),
    ];
    let fields = [
        "score",
        "cost",
        "costs",
        "measured",
        "replay",
        "stats",
        "read_rows",
        "read_bytes",
        "prediction",
        "verdict",
        "pass",
    ];
    for (tool, args) in &base {
        for field in fields {
            let mut a = args.clone();
            a[field] = json!(1);
            for via in ["direct", "jobs.start"] {
                let before = objects(&w.index());
                let reply = if via == "direct" {
                    srv.call(tool, a.clone())
                } else {
                    srv.call("jobs.start", json!({"tool": tool, "arguments": a}))
                };
                assert_eq!(code(&reply), "caller_supplied_cost", "{tool} {field} {via}");
                let r = reply.envelope.refusal.as_ref().unwrap();
                assert_eq!(r.fields["field"], field, "{tool} {via}");
                assert!(r.message.contains(field), "{}", r.message);
                assert!(!r.next.is_empty());
                let (kinds, other) = delta(&before, &objects(&w.index()));
                assert!(
                    kinds.is_empty() && other.is_empty(),
                    "{tool} wrote {kinds:?} {other:?}"
                );
            }
        }
    }
    // Nested: a replay inside a rewrite's shape, a cost inside a pair.
    let mut nested = base[4].1.clone();
    nested["shape"]["replay"] = json!({"current": []});
    assert_eq!(
        code(&srv.call("propose.rewrite", nested)),
        "caller_supplied_cost"
    );
    let mut nested = base[4].1.clone();
    nested["shape"]["pairs"][0]["cost"] = json!(5);
    let r = srv.call("propose.rewrite", nested);
    assert_eq!(code(&r), "caller_supplied_cost");
    assert_eq!(r.envelope.refusal.as_ref().unwrap().fields["field"], "cost");
    // Any other unknown field stays `invalid_input`.
    let mut other = base[0].1.clone();
    other["colour"] = json!("red");
    assert_eq!(code(&srv.call("observe.why", other)), "invalid_input");
    // Nothing reached the engine.
    assert!(w.state.lock().unwrap().runs.is_empty());
}

#[test]
fn an_export_the_engine_refuses_is_export_denied_with_no_certificate() {
    let w = World::new();
    let srv = w.server();
    let wl = h(
        &json(&ok(
            &srv,
            "score.record",
            json!({"keys_query": "SELECT keys", "wait": true}),
        )),
        "/workload",
    );
    let ev = h(
        &json(&ok(
            &srv,
            "score.why",
            json!({"workload": wl, "wait": true}),
        )),
        "/evidence/hash",
    );
    let p = h(
        &json(&ok(
            &srv,
            "propose.rewrite",
            json!({"shape": {"before": "b", "after": "a", "invariant": "i",
                             "pairs": [{"before": "SELECT before", "after": "SELECT after"}]},
                   "evidence": [ev], "by": "agent"}),
        )),
        "/proposal/hash",
    );
    w.state.lock().unwrap().deny = true;
    let r = srv.call("score.verify", json!({"proposal": p, "wait": true}));
    assert_eq!(code(&r), "export_denied");
    let refusal = r.envelope.refusal.as_ref().unwrap();
    assert_eq!(refusal.fields["permission"], "the fake export grant");
    assert_eq!(refusal.fields["engine"], ROOT);
    assert!(refusal.next.contains("permission"), "{}", refusal.next);
    let v = json(&r);
    assert!(v["certificate"].is_null(), "{v}");
    // The export is the measured run: nothing was measured or recorded.
    assert!(v["score"].is_null(), "{v}");
    assert!(
        refusal.next.contains("nothing is measured"),
        "{}",
        refusal.next
    );
}

/// The proposal, verified over MCP (`score.verify`): its certificate.
fn verified_over_mcp(srv: &bellwether_mcp::Server) -> String {
    let wl = h(
        &json(&ok(
            srv,
            "score.record",
            json!({"keys_query": "SELECT keys", "wait": true}),
        )),
        "/workload",
    );
    let ev = h(
        &json(&ok(srv, "score.why", json!({"workload": wl, "wait": true}))),
        "/evidence/hash",
    );
    let p = h(
        &json(&ok(
            srv,
            "propose.rewrite",
            json!({"shape": {"before": "b", "after": "a", "invariant": "i",
                             "pairs": [{"before": "SELECT before", "after": "SELECT after"}]},
                   "evidence": [ev], "by": "agent"}),
        )),
        "/proposal/hash",
    );
    let v = json(&ok(
        srv,
        "score.verify",
        json!({"proposal": p, "wait": true}),
    ));
    assert_eq!(v["outcome"]["kind"], "verified", "{v}");
    h(&v, "/certificate/hash")
}

/// `score.verify` applies nothing and is not gated; `apply.receipt` on its
/// certificate gates its own measurement, so a rewrite that reads no fewer
/// bytes never gets a receipt over MCP.
#[test]
fn score_verify_then_apply_receipt_with_no_gain_is_refused() {
    let w = World::new();
    w.state
        .lock()
        .unwrap()
        .queries
        .get_mut("SELECT after")
        .unwrap()
        .1 = stats(1600, 64_000);
    let srv = w.server();
    let cert = verified_over_mcp(&srv);
    let before = objects(&w.index());
    let r = srv.call("apply.receipt", json!({"certificate": cert, "wait": true}));
    assert_eq!(code(&r), "nothing_to_gain", "{}", r.text);
    let (kinds, _) = delta(&before, &objects(&w.index()));
    assert!(!kinds.iter().any(|k| k.contains("receipt")), "{kinds:?}");
}

#[test]
fn score_record_without_a_query_engine_is_refused_and_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let root = lake(dir.path());
    let srv = common::server(&root);
    let before = objects(&root.join(".bellwether"));
    let r = srv.call(
        "score.record",
        json!({"keys_query": "SELECT keys", "wait": true}),
    );
    assert!(r.is_error(), "{}", r.text);
    let refusal = r.envelope.refusal.as_ref().unwrap();
    assert!(refusal.message.contains("--twin-of"), "{}", refusal.message);
    let (kinds, other) = delta(&before, &objects(&root.join(".bellwether")));
    assert!(kinds.is_empty() && other.is_empty());
}
