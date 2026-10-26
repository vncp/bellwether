//! Query engines as measured sources through the facade (ADR 033), with an
//! in-process adapter (`EngineOptions::engine_adapter`): the twin's
//! diagnosis against a brute-force oracle over the decoded files, the four
//! twin lints firing and not firing, the rewrite loop ending in an
//! attest-only receipt, export denial writing no certificate and no
//! receipt, a failing certificate keeping the prediction error, a
//! fabricated Replay never reaching a receipt, a changed engine refused, and
//! the apply tier re-measuring.
//!
//! Deliberate breaks (each made once in a scratch copy; the named test
//! failed): drop the `attest_only` check on a measured replay's receipt in
//! the ledger (`a_measured_replay_never_backs_an_apply_receipt`); write the
//! Replay after the verify stage (`a_failing_certificate_keeps_the_prediction_error`);
//! certify on export denial (`export_denial_writes_no_certificate_and_no_receipt`);
//! skip the identity re-read after the exports
//! (`an_engine_that_changes_during_the_run_is_refused`).
#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::many_single_char_names
)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bellwether::source_engine::{EngineAdapter, Exported, MeasuredEngine, ResultHandle, Tag};
use bellwether::types::{
    Error, EvidenceBody, FileFingerprint, Gate, Hash, ProposalBody, QueryPair, QueryStats, Record,
    Result, RowGroupMeta, Source,
};
use bellwether::{
    engine_rewrite, Engine, EngineConnector, EngineOptions, FixOutcome, FixRequest, KeyFile,
    ProposeResult, Refusal, Score, WhyRequest,
};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

const ROOT: &str = "clickhouse://fake:8123/db/orders";
const MODEL: &str = "clickhouse.query_log";

/// What the fake engine answers.
#[derive(Default)]
struct State {
    /// Query text to (result, stats template).
    queries: HashMap<String, (RecordBatch, QueryStats)>,
    /// Tags submitted, with the query each ran.
    submitted: Vec<(String, String)>,
    /// Exports refused.
    deny: bool,
    /// Unit identities the listing reports.
    units: Vec<(String, u64)>,
    /// Bump the units after this many exports (0: never).
    change_after_exports: usize,
    exports: usize,
}

struct Fake(Arc<Mutex<State>>);

impl MeasuredEngine for Fake {
    fn root(&self) -> &str {
        ROOT
    }
    fn model(&self) -> &'static str {
        MODEL
    }
    fn submit(&self, query: &str, tag: &Tag) -> Result<ResultHandle> {
        let mut s = self.0.lock().unwrap();
        if !s.queries.contains_key(query.trim()) {
            return Err(Error::Source(format!("fake: unknown query {query}")));
        }
        s.submitted
            .push((tag.to_string(), query.trim().to_string()));
        Ok(ResultHandle {
            tag: tag.clone(),
            query: query.trim().to_string(),
        })
    }
    fn stats(&self, tag: &Tag) -> Result<QueryStats> {
        let s = self.0.lock().unwrap();
        let q = s
            .submitted
            .iter()
            .find(|(t, _)| t == tag.as_str())
            .map(|(_, q)| q.clone())
            .ok_or_else(|| Error::Source(format!("fake: no log entry for {tag}")))?;
        let mut st = s.queries[&q].1.clone();
        st.tag = tag.to_string();
        Ok(st)
    }
    fn export(&self, handle: &ResultHandle, dir: &Path) -> Result<Exported> {
        let mut s = self.0.lock().unwrap();
        if s.deny {
            return Err(Error::ExportDenied {
                engine: ROOT.into(),
                permission: "the fake export grant".into(),
                message: format!("{ROOT} refused the export; grant the fake export grant"),
            });
        }
        s.exports += 1;
        if s.change_after_exports > 0 && s.exports == s.change_after_exports {
            s.units.push(("p9".into(), 1));
        }
        let batch = s.queries[&handle.query].0.clone();
        std::fs::create_dir_all(dir).unwrap();
        let path = dir.join("part-0.parquet");
        let mut w =
            ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let bytes = std::fs::metadata(&path).unwrap().len();
        Ok(Exported {
            files: vec![path],
            bytes,
        })
    }
}

impl Source for Fake {
    fn discover(&self, _root: &str) -> Result<Vec<FileFingerprint>> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .units
            .iter()
            .map(|(p, rows)| FileFingerprint {
                path: format!("{ROOT}/{p}"),
                size: *rows,
                etag: Some(format!("v1;rows={rows}")),
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

/// The lake: `files` Parquet files of `per_file` rows each, row groups of
/// `rg` rows; `id` cycles over `ids`, `batch` is `row / batch_rows`, and a
/// wide `note` column.
fn lake(
    dir: &Path,
    files: usize,
    per_file: usize,
    rg: usize,
    ids: u64,
    batch_rows: u64,
) -> PathBuf {
    let out = dir.join("lake");
    std::fs::create_dir_all(&out).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("batch", DataType::UInt64, false),
        Field::new("amount", DataType::Int64, true),
        Field::new("note", DataType::Utf8, false),
    ]));
    let mut row = 0u64;
    for f in 0..files {
        let mut id = Vec::new();
        let mut batch = Vec::new();
        let mut amount = Vec::new();
        let mut note = Vec::new();
        for _ in 0..per_file {
            id.push(row % ids);
            batch.push(row / batch_rows);
            amount.push((!row.is_multiple_of(7)).then_some(i64::try_from(row).unwrap()));
            // Distinct per row, so the wide column stays wide on disk
            // (one repeated value would dictionary-encode to nothing).
            note.push(format!("{row:0>200}"));
            row += 1;
        }
        let b = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(UInt64Array::from(id)) as ArrayRef,
                Arc::new(UInt64Array::from(batch)),
                Arc::new(Int64Array::from(amount)),
                Arc::new(StringArray::from(note)),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(rg))
            .build();
        let mut w = ArrowWriter::try_new(
            File::create(out.join(format!("part-{f:03}.parquet"))).unwrap(),
            Arc::clone(&schema),
            Some(props),
        )
        .unwrap();
        w.write(&b).unwrap();
        w.close().unwrap();
    }
    out
}

fn keys_file(dir: &Path, ids: &[u64], batches: Option<&[u64]>) -> PathBuf {
    let p = dir.join("keys.parquet");
    let mut fields = vec![Field::new("id", DataType::UInt64, false)];
    let mut cols: Vec<ArrayRef> = vec![Arc::new(UInt64Array::from(ids.to_vec()))];
    if let Some(b) = batches {
        fields.push(Field::new("batch", DataType::UInt64, false));
        cols.push(Arc::new(UInt64Array::from(b.to_vec())));
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(Arc::clone(&schema), cols).unwrap();
    let mut w = ArrowWriter::try_new(File::create(&p).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    p
}

/// A result batch for the fake engine.
fn result(rows: &[(u64, u64, i64)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("batch", DataType::UInt64, false),
        Field::new("amount", DataType::Int64, true),
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
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn stats(read_rows: u64, read_bytes: u64, peak: u64) -> QueryStats {
    QueryStats {
        tag: String::new(),
        read_rows,
        read_bytes,
        peak_memory_bytes: peak,
        cpu_ns: Some(1_000_000),
        duration_ns: 2_000_000,
        result_rows: 2,
        units_read: Some(3),
        units_total: Some(4),
    }
}

struct World {
    dir: tempfile::TempDir,
    root: PathBuf,
    scratch: PathBuf,
    state: Arc<Mutex<State>>,
}

impl World {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = lake(dir.path(), 4, 400, 100, 200, 400);
        let scratch = dir.path().join("scratch");
        let state = Arc::new(Mutex::new(State {
            units: vec![("p0".into(), 800), ("p1".into(), 800)],
            ..State::default()
        }));
        Self {
            dir,
            root,
            scratch,
            state,
        }
    }

    fn options(&self, keep: bool) -> EngineOptions {
        let st = Arc::clone(&self.state);
        EngineOptions {
            twin_of: Some(ROOT.to_string()),
            scratch: Some(self.scratch.display().to_string()),
            keep_exports: keep,
            clock: Some("2026-10-26T00:00:00Z".into()),
            engine_adapter: Some(EngineConnector(Arc::new(
                move |_uri: &str, _s: Option<&Path>| -> Result<Box<dyn EngineAdapter>> {
                    Ok(Box::new(Fake(Arc::clone(&st))))
                },
            ))),
            ..EngineOptions::default()
        }
    }

    fn engine(&self) -> Engine {
        Engine::open(self.root.to_str().unwrap(), self.options(false)).unwrap()
    }

    fn query(&self, text: &str, rows: &[(u64, u64, i64)], st: QueryStats) {
        self.state
            .lock()
            .unwrap()
            .queries
            .insert(text.to_string(), (result(rows), st));
    }

    /// `why` over a key sample of ids 3 and 7 (batch 0), recorded.
    fn why(&self, e: &Engine) -> (Hash, bellwether::WhyReport) {
        let keys = keys_file(self.dir.path(), &[3, 7], Some(&[0, 0]));
        let r = e
            .why(&WhyRequest {
                keys: Some(KeyFile::new(keys)),
                gates: vec![Gate {
                    name: "in".into(),
                    limit: 1,
                }],
                ..WhyRequest::default()
            })
            .unwrap();
        (r.evidence.hash.unwrap(), r)
    }
}

/// A one-pair rewrite proposal citing `evidence`.
fn propose(e: &Engine, evidence: Hash) -> Hash {
    let shape = engine_rewrite(
        vec![QueryPair {
            before: "SELECT before".into(),
            after: "SELECT after".into(),
        }],
        Vec::new(),
        None,
    );
    match e
        .propose(ProposalBody::Rewrite { shape }, &[evidence], "agent")
        .unwrap()
    {
        ProposeResult::Written { proposal } => proposal.hash,
        other => panic!("{other:?}"),
    }
}

fn fix(e: &Engine, p: Hash) -> bellwether::FixResult {
    e.fix(&FixRequest {
        proposal: Some(p),
        ..FixRequest::default()
    })
    .unwrap()
}

/// The ledger under the lake, as a store writer sees it.
fn ledger(root: &Path) -> bellwether_ledger::Ledger<Arc<bellwether_store::ObjectStoreBackend>> {
    bellwether_ledger::Ledger::new(Arc::new(
        bellwether_store::ObjectStoreBackend::local(root.join(".bellwether")).unwrap(),
    ))
}

/// Every record of `kind` in the ledger folder under the lake.
fn ledger_records(root: &Path) -> Vec<Record> {
    let dir = root.join(".bellwether").join("ledger");
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let p = entry.unwrap().path();
        if p.extension().is_some_and(|x| x == "rec") {
            out.push(serde_json::from_slice(&std::fs::read(&p).unwrap()).unwrap());
        }
    }
    out
}

fn count<F: Fn(&Record) -> bool>(root: &Path, f: F) -> usize {
    ledger_records(root).iter().filter(|r| f(r)).count()
}

#[test]
fn the_twin_diagnosis_matches_a_brute_force_oracle_over_the_decoded_files() {
    let w = World::new();
    let e = w.engine();
    let (ev, r) = w.why(&e);
    // The oracle: decode every file, count rows, row groups, and files
    // holding each sampled (id, batch) value and each projection.
    let sample: BTreeSet<(u64, u64)> = [(3, 0), (7, 0)].into_iter().collect();
    let mut rows = 0u64;
    let mut rgs = BTreeSet::new();
    let mut files = BTreeSet::new();
    let mut by_id: BTreeMap<u64, (u64, BTreeSet<String>)> = BTreeMap::new();
    let mut paths: Vec<PathBuf> = std::fs::read_dir(&w.root)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
        .collect();
    paths.sort();
    for p in &paths {
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            File::open(p).unwrap(),
        )
        .unwrap()
        .with_batch_size(100)
        .build()
        .unwrap();
        for (g, b) in reader.enumerate() {
            let b = b.unwrap();
            let id = b.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
            let batch = b.column(1).as_any().downcast_ref::<UInt64Array>().unwrap();
            for i in 0..b.num_rows() {
                if sample.contains(&(id.value(i), batch.value(i))) {
                    rows += 1;
                    rgs.insert((p.clone(), g));
                    files.insert(p.clone());
                }
                if [3, 7].contains(&id.value(i)) {
                    let e = by_id.entry(id.value(i)).or_default();
                    e.0 += 1;
                    e.1.insert(p.display().to_string());
                }
            }
        }
    }
    let twin = r.twin.as_ref().unwrap();
    let key = twin.key.as_ref().unwrap();
    assert_eq!(key.columns, vec!["id", "batch"]);
    assert_eq!(
        (key.rows, key.row_groups, key.files),
        (rows, rgs.len() as u64, files.len() as u64)
    );
    assert_eq!(key.keyset, 2);
    let id_only = twin
        .candidates
        .iter()
        .find(|c| c.spec.columns() == ["id".to_string()])
        .and_then(|c| c.twin.as_ref())
        .unwrap();
    let max_rows = by_id.values().map(|v| v.0).max().unwrap();
    let max_files = by_id.values().map(|v| v.1.len() as u64).max().unwrap();
    assert_eq!(id_only.rows_per_key[2], max_rows);
    assert_eq!(id_only.files_per_key[2], max_files);
    assert_eq!(twin.rows_read, 1600);
    // The Evidence records the twin, freshness and prediction included.
    let Record::Evidence(evidence) = ledger(&w.root).get(&ev).unwrap() else {
        panic!()
    };
    let EvidenceBody::Profile { twin: Some(t), .. } = evidence.body else {
        panic!("no twin in the evidence")
    };
    assert_eq!(t.engine, ROOT);
    assert_eq!((t.engine_rows, t.lake_rows, t.units), (1600, 1600, 2));
    assert_eq!(t.predicted_rows, rows);
    assert_eq!(
        t.gates,
        vec![Gate {
            name: "in".into(),
            limit: 1
        }]
    );
    // The gate: two keys over a limit of one.
    assert!(!key.gates[0].fits);
    assert!(twin.lints.iter().any(|l| l.rule == "unbounded_membership"));
}

#[test]
fn each_twin_lint_fires_on_its_shape_and_not_on_a_negative() {
    let dir = tempfile::tempdir().unwrap();
    // 120 files of 2,000 rows; batch covers 200,000 rows per value; ids
    // repeat every 1,000 rows. Wide note column: the key columns are a
    // small share of the bytes.
    let root = lake(dir.path(), 120, 2_000, 1_000, 1_000, 200_000);
    let st = Arc::new(Mutex::new(State {
        units: vec![("p0".into(), 240_000)],
        ..State::default()
    }));
    let opts = |st: &Arc<Mutex<State>>| {
        let st = Arc::clone(st);
        EngineOptions {
            twin_of: Some(ROOT.to_string()),
            engine_adapter: Some(EngineConnector(Arc::new(
                move |_u: &str, _s: Option<&Path>| -> Result<Box<dyn EngineAdapter>> {
                    Ok(Box::new(Fake(Arc::clone(&st))))
                },
            ))),
            ..EngineOptions::default()
        }
    };
    let e = Engine::open(root.to_str().unwrap(), opts(&st)).unwrap();
    let keys = keys_file(dir.path(), &[3], Some(&[0]));
    let rules = |r: &bellwether::WhyReport| -> BTreeSet<String> {
        r.twin
            .as_ref()
            .unwrap()
            .lints
            .iter()
            .map(|l| l.rule.clone())
            .collect()
    };
    // Locate: low-cardinality (batch: 200,000 rows per value, id: 200),
    // wide-before-narrow; under the gate, so no membership lint.
    let r = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&keys)),
            gates: vec![Gate {
                name: "in".into(),
                limit: 10,
            }],
            ..WhyRequest::default()
        })
        .unwrap();
    let fired = rules(&r);
    assert!(fired.contains("low_cardinality_key"), "{fired:?}");
    assert!(fired.contains("wide_before_narrow"), "{fired:?}");
    assert!(!fired.contains("unbounded_membership"), "{fired:?}");
    assert!(!fired.contains("dedup_before_filter"), "{fired:?}");
    let low = &r
        .twin
        .as_ref()
        .unwrap()
        .lints
        .iter()
        .find(|l| l.rule == "low_cardinality_key")
        .unwrap();
    assert_eq!(low.columns, vec!["batch", "id"]);
    // The twin holds the engine's rows (240,000 both): high confidence.
    assert!((low.confidence - 0.9).abs() < 1e-9, "{}", low.confidence);
    // Negative for low-cardinality: a sample on id alone has no partner.
    let ids_only = keys_file(dir.path(), &[3, 4, 5], None);
    let r = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&ids_only)),
            gates: vec![Gate {
                name: "in".into(),
                limit: 2,
            }],
            ..WhyRequest::default()
        })
        .unwrap();
    let fired = rules(&r);
    assert!(!fired.contains("low_cardinality_key"), "{fired:?}");
    assert!(fired.contains("unbounded_membership"), "{fired:?}");
    // Negative for wide-before-narrow (and for membership, under its
    // gate): 150 ids select 36,000 of 240,000 rows, 15%, above the
    // 10% a key projection must stay under.
    let many: Vec<u64> = (0..150).collect();
    let many = keys_file(dir.path(), &many, None);
    let r = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&many)),
            gates: vec![Gate {
                name: "in".into(),
                limit: 1_000,
            }],
            ..WhyRequest::default()
        })
        .unwrap();
    let fired = rules(&r);
    assert!(!fired.contains("wide_before_narrow"), "{fired:?}");
    assert!(!fired.contains("unbounded_membership"), "{fired:?}");
}

#[test]
fn dedup_before_filter_fires_for_a_latest_lookup_over_few_files() {
    let dir = tempfile::tempdir().unwrap();
    // 200 files; the id is unique per file range, so a key is in one file.
    let root = lake(dir.path(), 200, 100, 100, 1_000_000, 1_000_000);
    let st = Arc::new(Mutex::new(State {
        units: vec![("p0".into(), 20_000)],
        ..State::default()
    }));
    let s2 = Arc::clone(&st);
    let e = Engine::open(
        root.to_str().unwrap(),
        EngineOptions {
            twin_of: Some(ROOT.to_string()),
            engine_adapter: Some(EngineConnector(Arc::new(
                move |_u: &str, _s: Option<&Path>| -> Result<Box<dyn EngineAdapter>> {
                    Ok(Box::new(Fake(Arc::clone(&s2))))
                },
            ))),
            ..EngineOptions::default()
        },
    )
    .unwrap();
    let keys = keys_file(dir.path(), &[5], None);
    let latest = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&keys)),
            latest_by: Some("amount".into()),
            ..WhyRequest::default()
        })
        .unwrap();
    let rules: Vec<&str> = latest
        .twin
        .as_ref()
        .unwrap()
        .lints
        .iter()
        .map(|l| l.rule.as_str())
        .collect();
    assert!(rules.contains(&"dedup_before_filter"), "{rules:?}");
    // The same keys as point lookups: no latest per key asked.
    let point = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&keys)),
            ..WhyRequest::default()
        })
        .unwrap();
    assert!(!point
        .twin
        .as_ref()
        .unwrap()
        .lints
        .iter()
        .any(|l| l.rule == "dedup_before_filter"));
    // The twin is behind the engine (20,000 lake rows, 20,000 engine rows:
    // fresh); make the engine bigger and the confidence drops.
    st.lock().unwrap().units.push(("p1".into(), 5));
    let stale = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&keys)),
            latest_by: Some("amount".into()),
            ..WhyRequest::default()
        })
        .unwrap();
    let l = stale
        .twin
        .as_ref()
        .unwrap()
        .lints
        .iter()
        .find(|l| l.rule == "dedup_before_filter")
        .unwrap();
    assert!((l.confidence - 0.6).abs() < 1e-9);
}

#[test]
fn the_rewrite_loop_ends_in_an_attest_only_receipt_binding_the_prediction() {
    let w = World::new();
    let rows = [(3, 0, 3), (7, 0, 7)];
    w.query("SELECT before", &rows, stats(1600, 64_000, 9_000_000));
    w.query("SELECT after", &rows, stats(400, 16_000, 1_000_000));
    let e = w.engine();
    let (ev, _) = w.why(&e);
    let p = propose(&e, ev);
    let r = fix(&e, p);
    let FixOutcome::Attested {
        receipt,
        target,
        deploy,
    } = &r.outcome
    else {
        panic!("{:?}", r.outcome)
    };
    assert_eq!(target, ROOT);
    assert_eq!(deploy, &vec!["SELECT after".to_string()]);
    let Record::Receipt(rc) = &receipt.record else {
        panic!()
    };
    assert!(rc.attest_only);
    assert_eq!(rc.manifest_before, rc.manifest_after);
    assert_eq!(rc.evidence, vec![ev]);
    let Some(Score::Measured { replay, model, .. }) = &r.score else {
        panic!("{:?}", r.score)
    };
    assert_eq!(model, MODEL);
    let Record::Replay(rp) = &replay.record else {
        panic!()
    };
    assert_eq!(rc.replay, replay.hash);
    assert_eq!(rc.prediction, rp.prediction);
    let pred = rp.prediction.as_ref().unwrap();
    assert_eq!((pred.before_read_rows, pred.after_read_rows), (1600, 400));
    assert_eq!(pred.evidence, ev);
    assert!(rp.current.iter().all(|c| !c.exact && c.measured.is_some()));
    // The query texts are hashed into the replay's inputs.
    assert!(rp.inputs.contains(&Hash::of(b"SELECT before")));
    assert!(rp.inputs.contains(&Hash::of(b"SELECT after")));
    // The tags the engine logged are the ones the replay cites.
    let tags: Vec<String> = w
        .state
        .lock()
        .unwrap()
        .submitted
        .iter()
        .map(|t| t.0.clone())
        .collect();
    assert_eq!(rp.current[0].run_id, tags[0]);
    assert_eq!(rp.proposed[0].run_id, tags[1]);
    // Exports deleted after the certificate.
    let left: Vec<_> = std::fs::read_dir(&w.scratch).unwrap().collect();
    assert!(left.is_empty(), "{left:?}");
    let run = r.engine.as_ref().unwrap();
    assert!(!run.kept);
    assert_eq!(run.identity_after.unwrap().hash, run.identity_before.hash);
    // The certificate cites both identities.
    let Record::Certificate(c) = &r.certificate.as_ref().unwrap().record else {
        panic!()
    };
    assert!(c.pass);
    assert_eq!(c.provenance.inputs[0], run.identity_before.hash);
    assert_eq!(c.provenance.inputs[1], run.identity_after.unwrap().hash);
}

#[test]
fn keep_keeps_the_exports() {
    let w = World::new();
    let rows = [(3, 0, 3)];
    w.query("SELECT before", &rows, stats(10, 100, 10));
    w.query("SELECT after", &rows, stats(5, 50, 5));
    let e = Engine::open(w.root.to_str().unwrap(), w.options(true)).unwrap();
    let (ev, _) = w.why(&e);
    let p = propose(&e, ev);
    let r = fix(&e, p);
    assert!(matches!(r.outcome, FixOutcome::Attested { .. }));
    let run = r.engine.unwrap();
    assert!(run.kept);
    let dir = PathBuf::from(run.scratch.unwrap());
    assert!(dir
        .join("p0")
        .join("before")
        .join("part-0.parquet")
        .is_file());
}

#[test]
fn export_denial_writes_no_certificate_and_no_receipt() {
    let w = World::new();
    let rows = [(3, 0, 3)];
    w.query("SELECT before", &rows, stats(10, 100, 10));
    w.query("SELECT after", &rows, stats(5, 50, 5));
    w.state.lock().unwrap().deny = true;
    let e = w.engine();
    let (ev, _) = w.why(&e);
    let p = propose(&e, ev);
    let r = fix(&e, p);
    let FixOutcome::Refused {
        refusal: Refusal::ExportDenied { permission, .. },
    } = &r.outcome
    else {
        panic!("{:?}", r.outcome)
    };
    assert_eq!(permission, "the fake export grant");
    assert!(r.certificate.is_none());
    assert_eq!(count(&w.root, |r| matches!(r, Record::Certificate(_))), 0);
    assert_eq!(count(&w.root, |r| matches!(r, Record::Receipt(_))), 0);
    // The measured replay stands, with its prediction.
    assert_eq!(
        count(
            &w.root,
            |r| matches!(r, Record::Replay(x) if x.prediction.is_some())
        ),
        1
    );
}

#[test]
fn a_failing_certificate_keeps_the_prediction_error() {
    let w = World::new();
    w.query(
        "SELECT before",
        &[(3, 0, 3), (7, 0, 7)],
        stats(1600, 64_000, 9),
    );
    w.query(
        "SELECT after",
        &[(3, 0, 3), (7, 0, 8)],
        stats(400, 16_000, 1),
    );
    let e = w.engine();
    let (ev, _) = w.why(&e);
    let p = propose(&e, ev);
    let r = fix(&e, p);
    assert!(
        matches!(
            r.outcome,
            FixOutcome::Refused {
                refusal: Refusal::CertificateFailed { mismatched: 1, .. }
            }
        ),
        "{:?}",
        r.outcome
    );
    assert_eq!(count(&w.root, |r| matches!(r, Record::Receipt(_))), 0);
    let replays: Vec<Record> = ledger_records(&w.root)
        .into_iter()
        .filter(|r| matches!(r, Record::Replay(x) if x.prediction.is_some()))
        .collect();
    assert_eq!(replays.len(), 1);
    let Record::Replay(rp) = &replays[0] else {
        panic!()
    };
    assert_eq!(rp.prediction.as_ref().unwrap().after_read_rows, 400);
}

#[test]
fn a_fabricated_replay_never_reaches_a_receipt() {
    let w = World::new();
    let rows = [(3, 0, 3)];
    w.query("SELECT before", &rows, stats(1600, 64_000, 9_000));
    w.query("SELECT after", &rows, stats(400, 16_000, 1_000));
    let e = w.engine();
    let (ev, _) = w.why(&e);
    let p = propose(&e, ev);
    // A Replay the caller made up, planted in the ledger for the proposal.
    let mut fake = stats(1, 1, 1);
    fake.tag = "bw-0000000000000000-p0-after".into();
    let planted = bellwether::types::Replay {
        record_version: bellwether::types::format::RECORD_VERSION,
        proposal: p,
        manifest_version: "0".into(),
        workload: ev,
        inputs: vec![ev],
        current: vec![cost(&fake)],
        proposed: vec![cost(&fake)],
        model: Some(MODEL.into()),
        prediction: None,
        created_at: "2026-10-26T00:00:00.000Z".into(),
    };
    let planted_hash = ledger(&w.root)
        .put(&Record::Replay(planted.clone()))
        .unwrap();
    // Cited as evidence: refused as the wrong kind, with a typed error.
    let shape = engine_rewrite(
        vec![QueryPair {
            before: "SELECT before".into(),
            after: "SELECT after".into(),
        }],
        Vec::new(),
        None,
    );
    let err = e
        .propose(
            ProposalBody::Rewrite { shape },
            &[ev, planted_hash],
            "agent",
        )
        .unwrap_err();
    assert!(matches!(err, Error::WrongKind { .. }), "{err}");
    // Present in the ledger: the engine measures for itself and its
    // receipt cites its own replay.
    let r = fix(&e, p);
    let FixOutcome::Attested { receipt, .. } = &r.outcome else {
        panic!("{:?}", r.outcome)
    };
    let Record::Receipt(rc) = &receipt.record else {
        panic!()
    };
    assert_ne!(rc.replay, planted_hash);
    assert_eq!(rc.prediction.as_ref().unwrap().after_read_rows, 400);
}

fn cost(s: &QueryStats) -> bellwether::types::RunCost {
    bellwether::types::RunCost {
        run_id: s.tag.clone(),
        keys: 1,
        files: 1,
        row_groups: 1,
        bytes: s.read_bytes,
        exact: false,
        measured: Some(s.clone()),
    }
}

#[test]
fn an_engine_that_changes_during_the_run_is_refused() {
    let w = World::new();
    let rows = [(3, 0, 3)];
    w.query("SELECT before", &rows, stats(10, 100, 10));
    w.query("SELECT after", &rows, stats(5, 50, 5));
    w.state.lock().unwrap().change_after_exports = 2;
    let e = w.engine();
    let (ev, _) = w.why(&e);
    let p = propose(&e, ev);
    let err = e
        .fix(&FixRequest {
            proposal: Some(p),
            ..FixRequest::default()
        })
        .unwrap_err();
    assert!(matches!(err, Error::SourceChanged { .. }), "{err}");
    assert!(err.to_string().contains("run fix again"), "{err}");
    assert_eq!(count(&w.root, |r| matches!(r, Record::Certificate(_))), 0);
    assert_eq!(count(&w.root, |r| matches!(r, Record::Receipt(_))), 0);
}

#[test]
fn the_apply_tier_re_measures_and_refuses_a_changed_engine() {
    let w = World::new();
    let rows = [(3, 0, 3)];
    w.query("SELECT before", &rows, stats(10, 100, 10));
    w.query("SELECT after", &rows, stats(5, 50, 5));
    let e = w.engine();
    let (ev, _) = w.why(&e);
    let p = propose(&e, ev);
    let v = e
        .fix(&FixRequest {
            proposal: Some(p),
            verify_only: true,
            ..FixRequest::default()
        })
        .unwrap();
    assert!(matches!(v.outcome, FixOutcome::Verified));
    let cert = v.certificate.unwrap().hash;
    let dry = e.apply_dry_run(&cert).unwrap();
    assert!(
        matches!(dry.outcome, FixOutcome::WouldApply { .. }),
        "{:?}",
        dry.outcome
    );
    let submitted = w.state.lock().unwrap().submitted.len();
    let a = e
        .fix(&FixRequest {
            certificate: Some(cert),
            ..FixRequest::default()
        })
        .unwrap();
    assert!(
        matches!(a.outcome, FixOutcome::Attested { .. }),
        "{:?}",
        a.outcome
    );
    // Scored again now: two more submissions.
    assert_eq!(w.state.lock().unwrap().submitted.len(), submitted + 2);
    // The engine moves on: the certificate no longer describes it.
    w.state.lock().unwrap().units.push(("p2".into(), 1));
    let b = e
        .fix(&FixRequest {
            certificate: Some(cert),
            ..FixRequest::default()
        })
        .unwrap();
    assert!(
        matches!(
            b.outcome,
            FixOutcome::Refused {
                refusal: Refusal::DataChanged { .. }
            }
        ),
        "{:?}",
        b.outcome
    );
}

#[test]
fn no_gain_is_refused_after_the_measurement() {
    let w = World::new();
    let rows = [(3, 0, 3)];
    w.query("SELECT before", &rows, stats(10, 100, 10));
    w.query("SELECT after", &rows, stats(10, 100, 10));
    let e = w.engine();
    let (ev, _) = w.why(&e);
    let p = propose(&e, ev);
    let r = fix(&e, p);
    assert!(
        matches!(
            r.outcome,
            FixOutcome::Refused {
                refusal: Refusal::NothingToGain { .. }
            }
        ),
        "{:?}",
        r.outcome
    );
    assert_eq!(count(&w.root, |r| matches!(r, Record::Certificate(_))), 0);
}

#[test]
fn a_rewrite_of_engine_queries_needs_the_twin_and_the_scratch() {
    let w = World::new();
    let e = w.engine();
    // Evidence without a twin: refused, naming the evidence.
    let plain = Engine::open(
        w.root.to_str().unwrap(),
        EngineOptions {
            clock: Some("2026-10-26T00:00:00Z".into()),
            ..EngineOptions::default()
        },
    )
    .unwrap();
    let r = plain.why(&WhyRequest::default()).unwrap();
    let shape = engine_rewrite(
        vec![QueryPair {
            before: "SELECT before".into(),
            after: "SELECT after".into(),
        }],
        vec!["id".into()],
        None,
    );
    let err = e
        .propose(
            ProposalBody::Rewrite { shape },
            &[r.evidence.hash.unwrap()],
            "agent",
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            Error::InvalidArgument {
                field: "evidence",
                ..
            }
        ),
        "{err}"
    );
    // Gates without a twin: refused.
    let err = plain
        .why(&WhyRequest {
            gates: vec![Gate {
                name: "in".into(),
                limit: 1,
            }],
            ..WhyRequest::default()
        })
        .unwrap_err();
    assert!(
        matches!(err, Error::InvalidArgument { field: "gates", .. }),
        "{err}"
    );
    // No scratch folder: refused before anything is submitted.
    w.query("SELECT before", &[(3, 0, 3)], stats(10, 100, 10));
    w.query("SELECT after", &[(3, 0, 3)], stats(5, 50, 5));
    let (ev, _) = w.why(&e);
    let p = propose(&e, ev);
    let mut o = w.options(false);
    o.scratch = None;
    let no_scratch = Engine::open(w.root.to_str().unwrap(), o).unwrap();
    let err = no_scratch
        .fix(&FixRequest {
            proposal: Some(p),
            ..FixRequest::default()
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            Error::NoWorkload {
                field: "scratch",
                ..
            }
        ),
        "{err}"
    );
    assert_eq!(w.state.lock().unwrap().submitted.len(), 0);
    // An engine root as the data root is refused, naming the twin flow.
    let err = Engine::open(ROOT, EngineOptions::default()).unwrap_err();
    assert!(err.to_string().contains("--twin-of"), "{err}");
}

#[test]
fn records_without_the_new_fields_keep_their_bytes() {
    // A receipt and a replay of a key apply serialize without the ADR 033
    // fields, so every record written before keeps its hash.
    let r = bellwether::types::Receipt {
        record_version: 2,
        evidence: vec![],
        proposal: Hash::of(b"p"),
        replay: Hash::of(b"r"),
        certificate: Hash::of(b"c"),
        manifest_before: "1".into(),
        manifest_after: "2".into(),
        created_at: "x".into(),
        attest_only: false,
        prediction: None,
    };
    let s = serde_json::to_string(&r).unwrap();
    assert!(
        !s.contains("attest_only") && !s.contains("prediction"),
        "{s}"
    );
    let back: bellwether::types::Receipt = serde_json::from_str(&s).unwrap();
    assert_eq!(back, r);
}
