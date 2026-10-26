//! The PR #22 review's CLI probes, kept as regression tests: ADR 010 against
//! the in-process facade, and why's prediction against locate's plan.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

mod common;

use std::collections::BTreeSet;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether::types::{
    format, CertBody, Certificate, Coverage, Hash, Provenance, Receipt, Record,
};
use bellwether::{spec, Engine, EngineOptions, KeyFile, WhyRequest};
use common::{corpus, keys, path, run};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;

/// The fixed clock both sides use, so records are byte-identical.
const CLOCK: &str = "2026-10-24T12:00:00.000Z";

fn engine(data: &Path) -> Engine {
    Engine::open(
        path(data),
        EngineOptions {
            clock: Some(CLOCK.into()),
            ..EngineOptions::default()
        },
    )
    .unwrap()
}

fn line<T: serde::Serialize>(t: &T) -> String {
    serde_json::to_string(t).unwrap() + "\n"
}

/// Writes `record` into the ledger directory under its hash, as a fixture
/// (the CLI tests cannot depend on the ledger crate; the receipt is read
/// only).
fn plant(index: &Path, record: &Record) -> Hash {
    let bytes = serde_json::to_vec(record).unwrap();
    let h = Hash::of(&bytes);
    std::fs::write(
        index.join("ledger").join(format!("{}.rec", h.to_hex())),
        bytes,
    )
    .unwrap();
    h
}

/// ADR 010: the CLI's `--json` is byte for byte the serialization of the
/// in-process facade's result for the same call (the test WP12 reuses).
/// Both sides run under the same fixed clock, so even the records they
/// write are identical.
#[test]
fn probe_adr010_cli_json_equals_in_process_facade() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 2, 3, 30, 200);
    let c = ["--clock", CLOCK];
    let cli = |args: &[&str]| run(&[&c[..], args].concat(), 0);
    let e = engine(&data);

    // why, no workload.
    let out = cli(&["--json", "why", path(&data)]);
    assert_eq!(out, line(&e.why(&WhyRequest::default()).unwrap()));

    // why with a key file: records included.
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[7, 8, 150]);
    let out = cli(&["--json", "why", path(&data), "--from", path(&kf)]);
    let req = WhyRequest {
        keys: Some(KeyFile::new(&kf)),
        ..WhyRequest::default()
    };
    let why = e.why(&req).unwrap();
    assert_eq!(out, line(&why));

    // index, locate, latest, asof.
    let out = cli(&["--json", "index", path(&data), "--key", "temporal:id@ts"]);
    let idx: bellwether::IndexResult = serde_json::from_str(&out).unwrap();
    assert_eq!(idx.committed, Some(1));
    let f = KeyFile::new(&kf);
    let out = cli(&["--json", "locate", path(&data), "--keys", path(&kf)]);
    assert_eq!(out, line(&e.locate(&f).unwrap()));
    let out = cli(&["--json", "latest", path(&data), "--keys", path(&kf)]);
    assert_eq!(out, line(&e.latest(&f).unwrap()));
    let at = "1970-01-01T00:01:00Z";
    let out = cli(&[
        "--json",
        "asof",
        path(&data),
        "--keys",
        path(&kf),
        "--at",
        at,
    ]);
    assert_eq!(out, line(&e.asof(&f, at).unwrap()));

    // A successful receipt show: a receipt over why's candidate (its
    // certificate planted as a fixture standing in for verify).
    let cand = &why.candidates[0];
    let index = data.join(".bellwether");
    let cert = plant(
        &index,
        &Record::Certificate(Certificate {
            record_version: format::RECORD_VERSION,
            proposal: Some(cand.proposal.hash),
            coverage: Coverage::Workload {
                workload: why.workload.as_ref().unwrap().hash,
                keys: 3,
            },
            pass: true,
            body: CertBody {
                missing_right: 0,
                missing_left: 0,
                mismatched: 0,
                ignored_columns: vec![],
                totals_delta: vec![],
            },
            provenance: Provenance {
                inputs: vec![Hash::of(b"fixture")],
                oldest_input_age: Duration::from_secs(1),
                manifest_version: Some("0".into()),
                created_at: CLOCK.into(),
            },
        }),
    );
    let receipt = plant(
        &index,
        &Record::Receipt(Receipt {
            attest_only: false,
            prediction: None,
            record_version: format::RECORD_VERSION,
            evidence: vec![],
            proposal: cand.proposal.hash,
            replay: cand.replay.hash,
            certificate: cert,
            manifest_before: "0".into(),
            manifest_after: "1".into(),
            created_at: CLOCK.into(),
        }),
    );
    let out = cli(&["--json", "receipt", "show", path(&data), &receipt.to_hex()]);
    let shown = e.receipt(&receipt).unwrap();
    assert!(shown.intact);
    assert_eq!(
        shown.records.len(),
        5,
        "the proposal's profile evidence too"
    );
    assert_eq!(out, line(&shown));
}

fn entity_ids(data: &std::path::Path) -> Vec<u64> {
    let mut all = BTreeSet::new();
    let mut names = Vec::new();
    let mut stack = vec![data.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|x| x == "parquet") {
                names.push(p);
            }
        }
    }
    names.sort();
    for p in names {
        let r = ParquetRecordBatchReaderBuilder::try_new(File::open(&p).unwrap())
            .unwrap()
            .build()
            .unwrap();
        for b in r {
            let b = b.unwrap();
            let c = b
                .column_by_name("entity_id")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .clone();
            for i in 0..c.len() {
                all.insert(c.value(i));
            }
        }
    }
    all.into_iter().collect()
}

fn u64_keys(path: &std::path::Path, ks: &[u64]) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "entity_id",
        DataType::UInt64,
        false,
    )]));
    let b = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt64Array::from(ks.to_vec())) as ArrayRef],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    w.write(&b).unwrap();
    w.close().unwrap();
}

/// Focus 5: why's predicted cost before any index equals what locate plans
/// after `index` for the same keys, on a bench-generated corpus.
#[test]
fn probe_why_prediction_equals_locate_on_a_bench_corpus() {
    use bellwether_bench::corpus::GenParams;
    let dir = tempfile::tempdir().unwrap();
    let params = GenParams {
        seed: 3,
        files: 16,
        row_groups_per_file: 4,
        rows_per_row_group: 4000,
        entities: 40_000,
        churn: 0.2,
        days: 4,
        payload_bytes: 16,
        late_fraction: 0.05,
    };
    let c = dir.path().join("corpus");
    bellwether_bench::generate::generate(&params, &c).unwrap();
    let data = c.join("data");
    let ids = entity_ids(&data);
    eprintln!(
        "data files: {:?}",
        std::fs::read_dir(&data)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect::<Vec<_>>()
    );
    eprintln!(
        "distinct ids {} first {:?} last {:?}",
        ids.len(),
        &ids[..ids.len().min(5)],
        ids.last()
    );
    let mut files = Vec::new();
    for (i, n) in [1usize, 25, 600, 5000].iter().enumerate() {
        let step = (ids.len() / n).max(1);
        let mut ks: Vec<u64> = ids.iter().step_by(step).take(*n).copied().collect();
        if i == 3 {
            ks.push(u64::MAX); // absent
        }
        let f = dir.path().join(format!("k{i}.parquet"));
        u64_keys(&f, &ks);
        files.push(f);
    }
    let e = Engine::open(path(&data), EngineOptions::default()).unwrap();
    let mut predicted = Vec::new();
    for f in &files {
        let w = e
            .why(&WhyRequest {
                keys: Some(KeyFile::new(f)),
                ..WhyRequest::default()
            })
            .unwrap();
        assert_eq!(
            w.candidates[0].origin,
            bellwether::CandidateOrigin::Workload
        );
        predicted.push(w.candidates[0].proposed);
    }
    e.index(&[spec::parse("exact:entity_id").unwrap()]).unwrap();
    for (f, p) in files.iter().zip(&predicted) {
        let l = e.locate(&KeyFile::new(f)).unwrap();
        eprintln!("{}: predicted {:?} planned {:?}", f.display(), p, l.planned);
        assert_eq!(l.planned, *p, "why's prediction equals locate's plan");
        assert!(
            l.planned.row_groups > 0,
            "keys sampled from the data plan something"
        );
        let w = e
            .why(&WhyRequest {
                keys: Some(KeyFile::new(f)),
                ..WhyRequest::default()
            })
            .unwrap();
        assert_eq!(w.current, Some(*p), "why after index: current = prediction");
    }
    // Same through the CLI for one key file.
    let out = run(
        &["--json", "locate", path(&data), "--keys", path(&files[2])],
        0,
    );
    let v: serde_json::Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v["planned"]["bytes"].as_u64().unwrap(), predicted[2].bytes);
}
