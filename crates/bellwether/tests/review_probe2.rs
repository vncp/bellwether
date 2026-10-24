//! The PR #22 review's probes, part 2, kept as regression tests: a folder
//! that changed after its index, observe-only writes, a damaged ledger,
//! invariant 5 through a pruning spec, and an index folder inside the data.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::format_push_string,
    clippy::items_after_statements
)]

mod common;

use std::collections::BTreeSet;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use bellwether::types::{EvidenceBody, Predicate, Record};
use bellwether::{spec, Engine, EngineOptions, KeyFile, PlanResult, SourceFormat, WhyRequest};
use common::{ledger_corpus, oracle, write_file, write_keys};
use parquet::arrow::ArrowWriter;

fn planned(r: &PlanResult) -> BTreeSet<(String, usize)> {
    let Record::Evidence(e) = &r.evidence.record else {
        panic!()
    };
    let EvidenceBody::Plan { plan } = &e.body else {
        panic!()
    };
    plan.entries
        .iter()
        .flat_map(|en| {
            let n = Path::new(&en.file.path)
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string();
            en.row_groups.iter().map(move |g| (n.clone(), g as usize))
        })
        .collect()
}

#[test]
fn probe_new_file_after_index() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data, 2, 2, 10, 50);
    let e = Engine::open(data.to_str().unwrap(), EngineOptions::default()).unwrap();
    e.index(&[spec::parse("exact:id").unwrap()]).unwrap();
    // A new file lands holding key 7 (and a brand-new key 9999).
    write_file(
        &data.join("part-999.parquet"),
        &[vec![
            (7, 0, Some(1), "new".into()),
            (9999, 0, Some(2), "n2".into()),
        ]],
    );
    let kf = dir.path().join("k.parquet");
    write_keys(&kf, "id", &[7, 9999]);
    // Invariant 1 over the folder: the plan would miss part-999, so every
    // lookup refuses and names `bellwether index`.
    for r in [
        e.locate(&KeyFile::new(&kf)),
        e.latest(&KeyFile::new(&kf)),
        e.asof(&KeyFile::new(&kf), "1"),
    ] {
        let err = r.unwrap_err().to_string();
        assert!(err.contains("bellwether index"), "{err}");
        assert!(err.contains("part-999.parquet"), "{err}");
    }
    // `why` reports the untracked file and counts it in the dataset.
    let w = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&kf)),
            ..WhyRequest::default()
        })
        .unwrap();
    assert_eq!(w.untracked.len(), 1);
    assert!(w.untracked[0].ends_with("part-999.parquet"));
    assert_eq!(
        (w.dataset.files, w.dataset.row_groups, w.dataset.rows),
        (3, 5, 42)
    );
    // After indexing it, the plan covers it.
    e.index(&[]).unwrap();
    let r = e.locate(&KeyFile::new(&kf)).unwrap();
    assert_eq!(planned(&r), oracle(&data, &[7, 9999]));
    // A file rewritten in place is refused the same way.
    write_file(
        &data.join("part-000.parquet"),
        &[vec![(7, 0, Some(1), "rewritten".into())]],
    );
    let err = e.locate(&KeyFile::new(&kf)).unwrap_err().to_string();
    assert!(
        err.contains("changed file") && err.contains("bellwether index"),
        "{err}"
    );
    // A tracked file removed is refused too.
    std::fs::remove_file(data.join("part-000.parquet")).unwrap();
    let err = e.locate(&KeyFile::new(&kf)).unwrap_err().to_string();
    assert!(
        err.contains("removed file") && err.contains("bellwether index"),
        "{err}"
    );
}

#[test]
fn probe_observe_records_only_where_it_can_write() {
    // A writable fresh folder: open writes nothing; `why` with no workload
    // records the profile Evidence, and only that.
    let dir = tempfile::tempdir().unwrap();
    let writable = dir.path().join("writable");
    ledger_corpus(&writable, 1, 2, 10, 5);
    let e = Engine::open(writable.to_str().unwrap(), EngineOptions::default()).unwrap();
    assert!(
        !writable.join(".bellwether").exists(),
        "open writes nothing"
    );
    let w = e.why(&WhyRequest::default()).unwrap();
    assert!(w.evidence.recorded && w.evidence.hash.is_some());
    let ledger: Vec<_> = std::fs::read_dir(writable.join(".bellwether").join("ledger"))
        .unwrap()
        .collect();
    assert_eq!(ledger.len(), 1, "the profile Evidence only");

    // A read-only data root and no --index: observe still succeeds, the
    // Evidence is returned unrecorded with one line naming --index, and
    // nothing is written; recording keys fails with a one-line remedy
    // naming --index; with --index elsewhere it works.
    let data = dir.path().join("data");
    ledger_corpus(&data, 1, 2, 10, 5);
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&data, std::fs::Permissions::from_mode(0o555)).unwrap();
    let e = Engine::open(data.to_str().unwrap(), EngineOptions::default()).unwrap();
    let w = e.why(&WhyRequest::default()).unwrap();
    assert_eq!(w.dataset.files, 1);
    assert!(!w.evidence.recorded && w.evidence.hash.is_none());
    let note = w.evidence.note.as_deref().unwrap();
    assert!(note.contains("--index") && !note.contains('\n'), "{note}");
    assert_ne!(w.profile.fanout.len(), 0, "profiled in memory");
    assert!(!data.join(".bellwether").exists(), "nothing written");
    let kf = dir.path().join("k.parquet");
    write_keys(&kf, "id", &[1]);
    let keyed = WhyRequest {
        keys: Some(KeyFile::new(&kf)),
        ..WhyRequest::default()
    };
    let err = e.why(&keyed).unwrap_err().to_string();
    assert!(err.contains("--index") && !err.contains('\n'), "{err}");
    let elsewhere = dir.path().join("idx");
    let e = Engine::open(
        data.to_str().unwrap(),
        EngineOptions {
            index: Some(elsewhere.to_str().unwrap().into()),
            ..EngineOptions::default()
        },
    )
    .unwrap();
    let w = e.why(&keyed).unwrap();
    assert!(w.evidence.recorded);
    assert_eq!(w.candidates[0].spec.name(), "exact:id");
    assert!(elsewhere.join("ledger").exists());
    std::fs::set_permissions(&data, std::fs::Permissions::from_mode(0o755)).unwrap();
}

#[test]
fn probe_ledger_corruption_receipt_and_report() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data, 2, 2, 10, 50);
    let e = Engine::open(data.to_str().unwrap(), EngineOptions::default()).unwrap();
    let kf = dir.path().join("k.parquet");
    write_keys(&kf, "id", &[3]);
    let w = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&kf)),
            ..WhyRequest::default()
        })
        .unwrap();
    let rp = w.candidates[0].replay.hash;
    let p = data
        .join(".bellwether")
        .join("ledger")
        .join(format!("{}.rec", rp.to_hex()));
    let s = std::fs::read_to_string(&p).unwrap();
    // flip one digit inside the JSON (still valid JSON)
    let idx = s.find("\"bytes\":").unwrap() + 8;
    let mut b = s.into_bytes();
    b[idx] = if b[idx] == b'9' { b'1' } else { b[idx] + 1 };
    std::fs::write(&p, &b).unwrap();
    let out = dir.path().join("r.html");
    let r = e.report(&out).unwrap();
    assert_eq!(
        r.unreadable,
        vec![rp],
        "a tampered record is skipped and listed"
    );
    // garbage file with a valid-looking name
    std::fs::write(
        data.join(".bellwether")
            .join("ledger")
            .join(format!("{}.rec", "ab".repeat(32))),
        b"not json",
    )
    .unwrap();
    std::fs::remove_file(&p).unwrap();
    let r = e.report(&out).unwrap();
    assert_eq!(
        r.unreadable.len(),
        1,
        "a garbage record is skipped and listed"
    );
    assert!(std::fs::read_to_string(&out)
        .unwrap()
        .contains("Unreadable records"));
}

#[test]
fn probe_invariant5_newest_pruned_by_another_spec() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    // id 1: old version (batch 10, ts 1) in rg0; newest (batch 20, ts 9) in rg1.
    write_file(
        &data.join("a.parquet"),
        &[
            vec![(1, 10, Some(1), "old".into()), (2, 10, Some(1), "x".into())],
            vec![(1, 20, Some(9), "new".into()), (3, 20, Some(1), "y".into())],
        ],
    );
    let e = Engine::open(data.to_str().unwrap(), EngineOptions::default()).unwrap();
    e.index(&[
        spec::parse("temporal:id@ts").unwrap(),
        spec::parse("exact:id,batch").unwrap(),
    ])
    .unwrap();
    // Key file (id, batch) = (1, 10): the exact spec prunes rg1.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("batch", DataType::Int64, false),
    ]));
    let kf = dir.path().join("k.parquet");
    let b = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            Arc::new(Int64Array::from(vec![10])),
        ],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(File::create(&kf).unwrap(), schema, None).unwrap();
    w.write(&b).unwrap();
    w.close().unwrap();
    let r = e.latest(&KeyFile::new(&kf)).unwrap();
    let Record::Evidence(ev) = &r.evidence.record else {
        panic!()
    };
    let EvidenceBody::Plan { plan } = &ev.body else {
        panic!()
    };
    let Some(Predicate::And { all }) = &plan.residual else {
        panic!()
    };
    let bounds = all
        .iter()
        .find_map(|p| match p {
            Predicate::NotOlderThan { bounds, .. } => Some(bounds.clone()),
            _ => None,
        })
        .unwrap();
    let mut nine = Vec::new();
    bellwether_core::encoding::encode_value_as(
        &bellwether_core::encoding::Value::Timestamp {
            value: 9,
            unit: TimeUnit::Microsecond,
        },
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        &mut nine,
    )
    .unwrap();
    let _ = TimestampMicrosecondArray::from(vec![0i64]);
    assert_eq!(bounds.len(), 1);
    assert_eq!(
        bounds[0].1, nine,
        "NotOlderThan carries the index-wide newest (ts 9) though rg1 is pruned"
    );
}

#[test]
fn probe_non_hidden_index_dir_with_text_formats() {
    for (fmt, ext) in [
        (
            SourceFormat::Csv {
                delimiter: b',',
                has_header: true,
            },
            "csv",
        ),
        (SourceFormat::JsonLines, "jsonl"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("d");
        std::fs::create_dir_all(&data).unwrap();
        for f in 0..2 {
            let mut s = String::new();
            if ext == "csv" {
                s.push_str("id,v\n");
            }
            for r in 0..10 {
                if ext == "csv" {
                    s.push_str(&format!("{},{r}\n", f * 10 + r));
                } else {
                    s.push_str(&format!("{{\"id\":{},\"v\":{r}}}\n", f * 10 + r));
                }
            }
            std::fs::write(data.join(format!("f{f}.{ext}")), s).unwrap();
        }
        let idx = data.join("idx");
        let e = Engine::open(
            data.to_str().unwrap(),
            EngineOptions {
                format: fmt,
                index: Some(idx.to_str().unwrap().into()),
                ..EngineOptions::default()
            },
        )
        .unwrap();
        let a = e.index(&[spec::parse("exact:id").unwrap()]).unwrap();
        assert_eq!((a.files_new, a.sequence), (2, 1), "{ext}");
        let kf = dir.path().join("k.parquet");
        write_keys(&kf, "id", &[3]);
        e.locate(&KeyFile::new(&kf)).unwrap();
        let b = e.index(&[spec::parse("exact:id").unwrap()]).unwrap();
        assert_eq!(
            (b.files_new, b.committed),
            (0, None),
            "{ext}: ledger and cache are not data"
        );
        let w = e.why(&WhyRequest::default()).unwrap();
        assert_eq!(w.dataset.files, 2, "{ext}");
        assert!(w.untracked.is_empty(), "{ext}");
    }
}
