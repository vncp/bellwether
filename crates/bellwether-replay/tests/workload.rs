//! The workload recorder: identity, persistence, encoding at dataset types,
//! nulls, and the bench workload format (written here in the bench's exact
//! shape, so the reader is exercised without the bench corpus).

#![allow(clippy::unwrap_used, missing_docs, clippy::too_many_lines)]

mod common;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether_core::segment::ColumnType;
use bellwether_replay::{
    record_bench_workload, KeyInput, KeySetSink, KeySetSource, MemoryKeySets, ReplayError,
    ReplayResult, Replayer, RunKeys, RunKeysBuilder, RunQuery, Workload, WorkloadFile,
    WorkloadRecorder, QUERY_KIND_ASOF, QUERY_KIND_LATEST, QUERY_KIND_LOCATE,
};
use bellwether_store::{Cache, ObjectStoreBackend};
use bellwether_types::{Error, Hash};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use serde_json::json;

fn schema(dt: DataType) -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("entity_id", dt, true)]))
}

fn u64_batch(keys: &[Option<u64>]) -> RecordBatch {
    RecordBatch::try_from_iter(vec![(
        "entity_id",
        Arc::new(UInt64Array::from(keys.to_vec())) as ArrayRef,
    )])
    .unwrap()
}

fn write_parquet(path: &Path, batch: &RecordBatch) {
    let mut w = ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), None).unwrap();
    w.write(batch).unwrap();
    w.close().unwrap();
}

#[test]
fn identity_ignores_input_form_and_order_and_sees_every_key() {
    let dir = tempfile::tempdir().unwrap();
    let s = schema(DataType::UInt64);
    let a = u64_batch(&[Some(3), Some(1), Some(3), None, Some(2)]);
    let b = u64_batch(&[Some(9)]);
    let pa = dir.path().join("a.parquet");
    write_parquet(&pa, &a);

    let k1 = MemoryKeySets::new();
    let mut r1 = WorkloadRecorder::new(&k1, Arc::clone(&s));
    r1.record("run-a", KeyInput::Batch(&a)).unwrap();
    r1.record("run-b", KeyInput::Batch(&b)).unwrap();
    let w1 = r1.finish().unwrap();

    let k2 = MemoryKeySets::new();
    let mut r2 = WorkloadRecorder::new(&k2, Arc::clone(&s));
    r2.record("run-b", KeyInput::Batch(&b)).unwrap();
    r2.record("run-a", KeyInput::Parquet(&pa)).unwrap();
    let w2 = r2.finish().unwrap();
    assert_eq!(w1.hash(), w2.hash(), "same keys, any form, any order");
    assert_eq!(w1, w2);

    let run = w1.run("run-a").unwrap();
    assert_eq!(
        (run.keys, run.null_tuples),
        (3, 1),
        "distinct, nulls dropped and counted"
    );
    assert_eq!(run.template, "columns(entity_id)", "a column-set grouping");
    assert_eq!(run.query_kind, QUERY_KIND_LOCATE);
    assert_eq!(run.key_types, vec![ColumnType::UInt64]);
    assert_eq!(w1.keys_p50_p95_max(), [1, 3, 3]);
    let rk = w1.run_keys(&k1, run).unwrap();
    assert_eq!(rk.len(), 3);
    assert_eq!(rk.hash().unwrap(), run.key_set);

    // One more key, or another run id, is another workload.
    let k3 = MemoryKeySets::new();
    let mut r3 = WorkloadRecorder::new(&k3, Arc::clone(&s));
    r3.record(
        "run-a",
        KeyInput::Batch(&u64_batch(&[Some(1), Some(2), Some(3), Some(4)])),
    )
    .unwrap();
    r3.record("run-b", KeyInput::Batch(&b)).unwrap();
    assert_ne!(r3.finish().unwrap().hash(), w1.hash());
    let k4 = MemoryKeySets::new();
    let mut r4 = WorkloadRecorder::new(&k4, Arc::clone(&s));
    r4.record("run-a2", KeyInput::Batch(&a)).unwrap();
    r4.record("run-b", KeyInput::Batch(&b)).unwrap();
    assert_ne!(r4.finish().unwrap().hash(), w1.hash());

    // Recording a run id twice is an error.
    let k5 = MemoryKeySets::new();
    let mut r5 = WorkloadRecorder::new(&k5, Arc::clone(&s));
    r5.record("x", KeyInput::Batch(&b)).unwrap();
    assert!(r5.record("x", KeyInput::Batch(&b)).is_err());
}

#[test]
fn keys_encode_at_the_dataset_type_and_a_mismatch_is_refused_at_replay() {
    // A UInt32 key column recorded against a dataset that declares Int64.
    let keys = MemoryKeySets::new();
    let mut rec = WorkloadRecorder::new(&keys, schema(DataType::Int64));
    let batch = RecordBatch::try_from_iter(vec![(
        "entity_id",
        Arc::new(UInt32Array::from(vec![1, 2])) as ArrayRef,
    )])
    .unwrap();
    rec.record("r", KeyInput::Batch(&batch)).unwrap();
    let w = rec.finish().unwrap();
    assert_eq!(w.runs()[0].key_types, vec![ColumnType::Int64]);

    // Replayed against a dataset that declares UInt64: refused, not guessed.
    let corpus = common::write_corpus(&common::CorpusSpec::default());
    let source = corpus.source();
    let replayer = Replayer::unindexed(
        &source,
        &corpus.data,
        &bellwether_source::Partitioning::None,
    )
    .unwrap();
    let err = replayer.replay_specs(&[], &w, &keys).unwrap_err();
    assert!(
        matches!(&err, Error::KeySpec(m) if m.contains("re-record")),
        "{err}"
    );

    // A value the dataset type cannot hold is an error when recording.
    let keys = MemoryKeySets::new();
    let mut rec = WorkloadRecorder::new(&keys, schema(DataType::UInt64));
    let negative = RecordBatch::try_from_iter(vec![(
        "entity_id",
        Arc::new(Int64Array::from(vec![-1])) as ArrayRef,
    )])
    .unwrap();
    assert!(rec.record("r", KeyInput::Batch(&negative)).is_err());
}

#[test]
fn workloads_round_trip_through_the_cache_and_workload_json() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ObjectStoreBackend::local(dir.path()).unwrap());
    let cache = Cache::new(Arc::clone(&store), "idx");
    let mut rec = WorkloadRecorder::new(&cache, schema(DataType::UInt64));
    rec.record("r1", KeyInput::Batch(&u64_batch(&[Some(5), Some(6)])))
        .unwrap();
    rec.record("r2", KeyInput::Batch(&u64_batch(&[Some(7)])))
        .unwrap();
    let w = rec.finish().unwrap();
    // Key sets and the manifest live under their BLAKE3 in cache/.
    assert!(dir
        .path()
        .join(format!("idx/cache/{}.bin", w.hash().to_hex()))
        .is_file());
    assert_eq!(Workload::load(&cache, &w.hash()).unwrap(), w);
    for run in w.runs() {
        let bytes = cache.get(&run.key_set).unwrap();
        assert_eq!(Hash::of(&bytes), run.key_set);
    }
    assert!(matches!(
        Workload::load(&cache, &Hash::of(b"nothing")),
        Err(Error::NotFound(_))
    ));

    // A manifest stored in a non-canonical form is refused even though its
    // name is its hash.
    let pretty = serde_json::to_vec_pretty(w.manifest()).unwrap();
    let h = Hash::of(&pretty);
    cache.put(&h, Bytes::from(pretty)).unwrap();
    assert!(Workload::load(&cache, &h).is_err());

    // workload.json: round trip, and refusal of a doctored file.
    let path = dir.path().join("workload.json");
    w.write_json(&path).unwrap();
    assert_eq!(Workload::read_json(&path).unwrap(), w);
    let mut file: WorkloadFile = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    file.runs[0].keys += 1;
    assert!(
        Workload::from_file(file.clone()).is_err(),
        "hash no longer matches"
    );
    let mut file2: WorkloadFile = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    file2.keys_p50_p95_max = [9, 9, 9];
    assert!(Workload::from_file(file2).is_err(), "sizes no longer match");
    let mut newer: WorkloadFile = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    newer.format = "bellwether-replay-workload/2".into();
    assert!(matches!(
        Workload::from_file(newer),
        Err(Error::UnsupportedVersion {
            what: "workload",
            ..
        })
    ));
}

/// Writes a corpus directory in `bellwether-bench`'s workload format.
fn bench_workload(dir: &Path, runs: &[(&str, Vec<u64>)]) -> serde_json::Value {
    std::fs::create_dir_all(dir.join("workload")).unwrap();
    let corpus_json = br#"{"format":"bellwether-bench-corpus/1"}"#;
    std::fs::write(dir.join("corpus.json"), corpus_json).unwrap();
    let mut entries = Vec::new();
    let mut all = Vec::new();
    for (id, keys) in runs {
        let mut sorted = keys.clone();
        sorted.sort_unstable();
        sorted.dedup();
        let mut h = Vec::new();
        for k in &sorted {
            h.extend_from_slice(&k.to_be_bytes());
        }
        let hash = Hash::of(&h);
        all.extend_from_slice(&hash.0);
        let file = format!("workload/{id}.parquet");
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "entity_id",
                DataType::UInt64,
                false,
            )])),
            vec![Arc::new(UInt64Array::from(keys.clone())) as ArrayRef],
        )
        .unwrap();
        write_parquet(&dir.join(&file), &batch);
        entries.push(json!({"run_id": id, "file": file, "keys": sorted.len(), "hash": hash}));
    }
    let manifest = json!({
        "format": "bellwether-bench-workload/1",
        "corpus_id": Hash::of(corpus_json),
        "params": {"seed": 7, "runs": runs.len(), "p50": 1, "p95": 2, "max": 3},
        "available_entities": 100,
        "clamped": false,
        "sizes_p50_p95_max": [1, 2, 3],
        "runs": entries,
        "hash": Hash::of(&all),
    });
    std::fs::write(
        dir.join("workload.json"),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    manifest
}

fn record_bench(dir: &Path) -> ReplayResult<Workload> {
    let keys = MemoryKeySets::new();
    let mut rec = WorkloadRecorder::new(&keys, schema(DataType::UInt64));
    record_bench_workload(&mut rec, &dir.join("workload.json"))?;
    Ok(rec.finish()?)
}

#[test]
fn the_bench_workload_format_is_read_and_verified() {
    let dir = tempfile::tempdir().unwrap();
    let runs = [("run-0000", vec![4, 2, 2, 9]), ("run-0001", vec![7])];
    let manifest = bench_workload(dir.path(), &runs);
    let w = record_bench(dir.path()).unwrap();
    assert_eq!(w.runs().len(), 2);
    assert_eq!(w.runs()[0].run_id, "run-0000");
    assert_eq!(w.runs()[0].keys, 3);
    assert_eq!(w.runs()[1].keys, 1);

    let rewrite = |m: serde_json::Value| {
        std::fs::write(
            dir.path().join("workload.json"),
            serde_json::to_vec(&m).unwrap(),
        )
        .unwrap();
    };
    // Every mismatch is a BenchMismatch, naming the run when one failed.
    let expect = |run: Option<&str>, what: &str| match record_bench(dir.path()) {
        Err(ReplayError::BenchMismatch { run_id, detail }) => {
            assert_eq!(run_id.as_deref(), run, "{detail}");
            assert!(detail.contains(what), "{what}: {detail}");
        }
        other => panic!("expected a BenchMismatch ({what}), got {other:?}"),
    };

    // A run hash that does not match the run file.
    let mut m = manifest.clone();
    m["runs"][0]["hash"] = json!(Hash::of(b"other"));
    rewrite(m);
    expect(None, "does not match its runs");
    // A consistent workload hash over a wrong run hash: caught per run.
    let mut m = manifest.clone();
    m["runs"][0]["hash"] = json!(Hash::of(b"other"));
    let mut all = Vec::new();
    all.extend_from_slice(&Hash::of(b"other").0);
    all.extend_from_slice(
        &Hash::from_hex(m["runs"][1]["hash"].as_str().unwrap())
            .unwrap()
            .0,
    );
    m["hash"] = json!(Hash::of(&all));
    rewrite(m);
    expect(Some("run-0000"), "hashing to");
    // A key count that does not match.
    let mut m = manifest.clone();
    m["runs"][1]["keys"] = json!(2);
    rewrite(m);
    expect(Some("run-0001"), "records 2 keys");
    // Generated for another corpus.
    let mut m = manifest.clone();
    m["corpus_id"] = json!(Hash::of(b"another corpus"));
    rewrite(m);
    expect(None, "generated for corpus");
    // A run file outside the corpus.
    let mut m = manifest.clone();
    m["runs"][0]["file"] = json!("../escape.parquet");
    rewrite(m);
    expect(Some("run-0000"), "not inside the corpus");
    // The same run id twice.
    let mut m = manifest.clone();
    m["runs"][1]["run_id"] = json!("run-0000");
    rewrite(m);
    expect(Some("run-0000"), "appears twice");
    // A newer format.
    let mut m = manifest.clone();
    m["format"] = json!("bellwether-bench-workload/2");
    rewrite(m);
    assert!(matches!(
        record_bench(dir.path()),
        Err(ReplayError::Engine(Error::UnsupportedVersion {
            what: "bench workload",
            ..
        }))
    ));
    // A run file that is not one UInt64 column.
    rewrite(manifest);
    let bad = RecordBatch::try_from_iter(vec![(
        "entity_id",
        Arc::new(Int64Array::from(vec![1])) as ArrayRef,
    )])
    .unwrap();
    write_parquet(&dir.path().join("workload/run-0001.parquet"), &bad);
    expect(Some("run-0001"), "one UInt64 key column");
}

/// The bench reader verifies every run before recording any: a mismatch on
/// the last run leaves the recorder and the key-set sink exactly as they
/// were.
#[test]
fn a_bench_mismatch_on_the_last_run_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let runs = [
        ("run-0000", vec![4, 2, 9]),
        ("run-0001", vec![7, 8]),
        ("run-0002", vec![1]),
    ];
    let mut manifest = bench_workload(dir.path(), &runs);
    // Tamper the last run's hash and keep the workload hash consistent, so
    // only the per-run check (pass 1, last run) can catch it.
    manifest["runs"][2]["hash"] = json!(Hash::of(b"tampered"));
    let mut all = Vec::new();
    for i in 0..3 {
        all.extend_from_slice(
            &Hash::from_hex(manifest["runs"][i]["hash"].as_str().unwrap())
                .unwrap()
                .0,
        );
    }
    manifest["hash"] = json!(Hash::of(&all));
    std::fs::write(
        dir.path().join("workload.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    let keys = MemoryKeySets::new();
    let mut rec = WorkloadRecorder::new(&keys, schema(DataType::UInt64));
    let e = record_bench_workload(&mut rec, &dir.path().join("workload.json")).unwrap_err();
    assert!(
        matches!(&e, ReplayError::BenchMismatch { run_id: Some(r), .. } if r == "run-0002"),
        "{e}"
    );
    assert!(matches!(Error::from(e), Error::Source(_)));
    assert_eq!(rec.len(), 0, "the recorder is untouched");
    assert!(keys.is_empty().unwrap(), "the sink is untouched");
}

#[test]
fn a_workload_accepts_locate_latest_and_asof_and_refuses_unknown_query_kinds() {
    let keys = MemoryKeySets::new();
    let mut rec = WorkloadRecorder::new(&keys, schema(DataType::UInt64));
    rec.record("r0", KeyInput::Batch(&u64_batch(&[Some(1)])))
        .unwrap();
    rec.record_query(
        "r1",
        KeyInput::Batch(&u64_batch(&[Some(2)])),
        RunQuery::Latest,
        None,
    )
    .unwrap();
    rec.record_query(
        "r2",
        KeyInput::Batch(&u64_batch(&[Some(3)])),
        RunQuery::Asof { ts: 100 },
        None,
    )
    .unwrap();
    let w = rec.finish().unwrap();
    assert_eq!(w.runs()[0].query_kind, QUERY_KIND_LOCATE);
    assert_eq!(w.runs()[0].asof, None);
    assert_eq!(w.runs()[1].query_kind, QUERY_KIND_LATEST);
    assert_eq!(w.runs()[1].asof, None);
    assert_eq!(w.runs()[2].query_kind, QUERY_KIND_ASOF);
    assert_eq!(w.runs()[2].asof, Some(100));
    let json = serde_json::to_value(w.manifest()).unwrap();
    assert!(
        json["runs"][0].get("asof").is_none(),
        "locate omits asof so existing hashes stay stable"
    );
    assert!(json["runs"][1].get("asof").is_none());
    assert_eq!(json["runs"][2]["asof"], json!(100));
    let mut manifest = w.manifest().clone();
    manifest.runs[0].query_kind = "between".into();
    assert!(matches!(
        Workload::new(manifest),
        Err(Error::UnknownKind {
            what: "query kind",
            ..
        })
    ));
    // asof without ts, or locate/latest with ts, is refused.
    let mut no_ts = w.manifest().clone();
    no_ts.runs[2].asof = None;
    assert!(Workload::new(no_ts).is_err());
    let mut stray_ts = w.manifest().clone();
    stray_ts.runs[0].asof = Some(1);
    assert!(Workload::new(stray_ts).is_err());
}

/// A source that serves other bytes than it was asked for.
struct Lying(MemoryKeySets, Bytes);

impl KeySetSource for Lying {
    fn get(&self, hash: &Hash) -> bellwether_types::Result<Bytes> {
        self.0.get(hash)?;
        Ok(self.1.clone())
    }
}

#[test]
fn run_keys_checks_the_hash_whatever_the_source_serves() {
    let keys = MemoryKeySets::new();
    let mut rec = WorkloadRecorder::new(&keys, schema(DataType::UInt64));
    rec.record("a", KeyInput::Batch(&u64_batch(&[Some(1), Some(2)])))
        .unwrap();
    rec.record("b", KeyInput::Batch(&u64_batch(&[Some(3)])))
        .unwrap();
    let w = rec.finish().unwrap();
    // Run b's valid payload served under run a's hash: a well-formed key
    // set, so only the hash check stops it.
    let other = keys.get(&w.run("b").unwrap().key_set).unwrap();
    let lying = Lying(keys.clone(), other);
    let err = w.run_keys(&lying, w.run("a").unwrap()).unwrap_err();
    assert!(
        err.to_string().contains("served bytes that hash to"),
        "{err}"
    );
    assert!(Workload::load(&lying, &w.hash()).is_err());
}

#[test]
fn corrupt_payloads_manifests_and_workload_files_never_panic() {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let s = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_from_iter(vec![
        (
            "a",
            Arc::new(Int64Array::from(vec![1, -2, 3, 3])) as ArrayRef,
        ),
        (
            "s",
            Arc::new(arrow::array::StringArray::from(vec!["x", "a\0b", "", "zz"])) as ArrayRef,
        ),
    ])
    .unwrap();
    let mut b = RunKeysBuilder::new(&["a".to_string(), "s".to_string()], &s).unwrap();
    b.push_batch(&batch).unwrap();
    let good = b.finish().to_payload().unwrap();
    let mut panics = 0;
    for i in 0..good.len() {
        for op in [0u8, 1] {
            let mut bad = good.clone();
            bad[i] = if op == 0 {
                bad[i] ^ 0xFF
            } else {
                bad[i].wrapping_add(1)
            };
            if catch_unwind(AssertUnwindSafe(|| RunKeys::from_payload(&bad))).is_err() {
                panics += 1;
            }
        }
    }
    let keys = MemoryKeySets::new();
    let mut rec = WorkloadRecorder::new(&keys, s);
    rec.record("r1", KeyInput::Batch(&batch)).unwrap();
    let w = rec.finish().unwrap();
    let manifest = serde_json::to_vec(w.manifest()).unwrap();
    for i in 0..manifest.len() {
        let mut bad = manifest.clone();
        bad[i] ^= 0x01;
        let h = Hash::of(&bad);
        let store = MemoryKeySets::new();
        store.put(&h, Bytes::from(bad)).unwrap();
        if catch_unwind(AssertUnwindSafe(|| Workload::load(&store, &h))).is_err() {
            panics += 1;
        }
    }
    let file = serde_json::to_vec(&w.to_file()).unwrap();
    for i in 0..file.len() {
        let mut bad = file.clone();
        bad[i] ^= 0x01;
        let r = catch_unwind(AssertUnwindSafe(|| {
            serde_json::from_slice::<WorkloadFile>(&bad)
                .ok()
                .map(Workload::from_file)
        }));
        panics += usize::from(r.is_err());
    }
    assert_eq!(panics, 0);
}
