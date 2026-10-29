//! The PR #1 review's probes, kept as regression tests: the recording rule
//! and data-folder hygiene (no write probe left behind, nothing new in the
//! data folder, `--index` at an existing read-only directory), clocked bytes
//! and cache reuse, re-profiling when a file or the text chunk size changes,
//! the Evidence's provenance and the receipt chain through it, and the floor
//! against `locate` on a bench corpus.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::assert_is_empty,
    clippy::match_wildcard_for_single_variants,
    clippy::format_push_string
)]

mod common;

use std::collections::BTreeSet;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether::types::Record;
use bellwether::{
    spec, CandidateOrigin, Engine, EngineOptions, FixOutcome, FixRequest, KeyFile, WhyReport,
    WhyRequest,
};
use common::{corpus, keys, path, run, scattered};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;

/// Every path under `dir` (hidden included), relative.
fn listing(dir: &Path) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            out.insert(p.strip_prefix(dir).unwrap().display().to_string());
            if p.is_dir() {
                stack.push(p);
            }
        }
    }
    out
}

fn probes_left(dir: &Path) -> Vec<String> {
    listing(dir)
        .into_iter()
        .filter(|p| p.contains("write-probe"))
        .collect()
}

#[cfg(unix)]
fn set_ro(p: &Path, ro: bool) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(
        p,
        std::fs::Permissions::from_mode(if ro { 0o555 } else { 0o755 }),
    )
    .unwrap();
}

#[test]
fn probe_recording_rule_and_data_folder_hygiene() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 2, 3, 20, 30);
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[1, 2, 29]);

    // (a) read-only data folder, default index: profile computed, nothing written, exit 0.
    // Read-only directory modes are Unix semantics (Windows ignores the
    // read-only attribute on directories).
    #[cfg(unix)]
    {
        let before = listing(&data);
        set_ro(&data, true);
        let out = run(&["--json", "why", path(&data)], 0);
        let r: WhyReport = serde_json::from_str(&out).unwrap();
        eprintln!(
            "ro, no keys: recorded={} hash={:?} note={:?} fanout_entries={}",
            r.evidence.recorded,
            r.evidence.hash,
            r.evidence.note,
            r.profile.fanout.len()
        );
        assert!(!r.evidence.recorded);
        assert!(r.evidence.note.as_deref().unwrap().contains("--index"));
        assert_eq!(r.evidence.note.as_deref().unwrap().lines().count(), 1);
        let human = common::bw().args(["why", path(&data)]).output().unwrap();
        eprintln!(
            "ro human exit={:?} last lines: {:?}",
            human.status.code(),
            String::from_utf8_lossy(&human.stdout)
                .lines()
                .rev()
                .take(2)
                .collect::<Vec<_>>()
        );
        // (b) read-only, with keys: the --index refusal wins.
        let o = common::bw()
            .args(["why", path(&data), "--from", path(&kf)])
            .output()
            .unwrap();
        eprintln!(
            "ro with keys: exit={:?} stderr={}",
            o.status.code(),
            String::from_utf8_lossy(&o.stderr).trim()
        );
        assert_eq!(o.status.code(), Some(4));
        set_ro(&data, false);
        assert_eq!(
            listing(&data),
            before,
            "nothing written in a read-only data folder"
        );
    }

    // (c) writable data folder, --index elsewhere: data folder untouched, no probe left.
    let before = listing(&data);
    let idx = dir.path().join("idx");
    run(&["--index", path(&idx), "why", path(&data)], 0);
    run(
        &[
            "--index",
            path(&idx),
            "why",
            path(&data),
            "--from",
            path(&kf),
        ],
        0,
    );
    assert_eq!(
        listing(&data),
        before,
        "--index elsewhere: data folder untouched"
    );
    assert!(probes_left(&idx).is_empty(), "{:?}", probes_left(&idx));

    // (d) --index is an existing read-only directory inside a writable data folder.
    #[cfg(unix)]
    {
        let ro_idx = data.join("ro-idx");
        std::fs::create_dir_all(&ro_idx).unwrap();
        set_ro(&ro_idx, true);
        let before_d = listing(&data);
        let out = run(&["--json", "--index", path(&ro_idx), "why", path(&data)], 0);
        let r: WhyReport = serde_json::from_str(&out).unwrap();
        eprintln!(
            "ro --index inside data: recorded={} note={:?}",
            r.evidence.recorded, r.evidence.note
        );
        assert!(!r.evidence.recorded);
        set_ro(&ro_idx, false);
        assert_eq!(
            listing(&data),
            before_d,
            "nothing created in the data folder by the probe"
        );
    }

    // (e) default index on a writable folder: the probe lives in .bellwether only and is removed.
    let before_e = listing(&data);
    run(&["why", path(&data)], 0);
    let after: BTreeSet<String> = listing(&data);
    let new: Vec<_> = after
        .difference(&before_e)
        .filter(|p| !p.starts_with(".bellwether"))
        .collect();
    eprintln!(
        "default index: new non-index paths in data folder: {new:?}; probes left: {:?}",
        probes_left(&data)
    );
    assert!(new.is_empty());
    assert!(probes_left(&data).is_empty());
}

#[test]
fn probe_clock_bytes_and_cache_reuse() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 2, 3, 20, 30);
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[1, 2, 29]);
    let clock = "2026-10-24T00:00:00Z";
    let a = run(
        &[
            "--json",
            "--clock",
            clock,
            "why",
            path(&data),
            "--from",
            path(&kf),
        ],
        0,
    );
    // An unclocked run warms (and reuses) the profile cache.
    let u1 = run(&["--json", "why", path(&data), "--from", path(&kf)], 0);
    let u2 = run(&["--json", "why", path(&data), "--from", path(&kf)], 0);
    let b = run(
        &[
            "--json",
            "--clock",
            clock,
            "why",
            path(&data),
            "--from",
            path(&kf),
        ],
        0,
    );
    assert_eq!(
        a, b,
        "clocked output is byte-identical before and after a warm cache"
    );
    // In-process facade under the same clock: byte-identical to the CLI.
    let e = Engine::open(
        path(&data),
        EngineOptions {
            clock: Some(clock.into()),
            ..EngineOptions::default()
        },
    )
    .unwrap();
    let f = serde_json::to_string(
        &e.why(&WhyRequest {
            keys: Some(KeyFile::new(&kf)),
            ..WhyRequest::default()
        })
        .unwrap(),
    )
    .unwrap()
        + "\n";
    assert_eq!(a, f, "ADR 010 under --clock with the profile");
    // Unclocked cold vs warm: same measurements.
    let r1: WhyReport = serde_json::from_str(&u1).unwrap();
    let r2: WhyReport = serde_json::from_str(&u2).unwrap();
    let ra: WhyReport = serde_json::from_str(&a).unwrap();
    let m = |r: &WhyReport| {
        serde_json::json!({"fanout": r.profile.fanout, "lints": r.profile.lints,
        "cands": r.candidates.iter().map(|c| (c.spec.clone(), c.proposed)).collect::<Vec<_>>(),
        "culprit": r.culprit.as_ref().map(|c| (c.columns.clone(), c.fanout.clone())),
        "fanout_ev": match &r.evidence.evidence.body { bellwether::types::EvidenceBody::Profile { fanout, lints, .. } => serde_json::json!([fanout, lints]), _ => serde_json::Value::Null }}).to_string()
    };
    assert_eq!(m(&r1), m(&r2), "cold vs warm cache: same measurements");
    assert_eq!(m(&r1), m(&ra), "unclocked vs clocked: same measurements");
    eprintln!(
        "provenance ages: cold {:?} warm {:?} clocked {:?}",
        r1.evidence.evidence.provenance.oldest_input_age,
        r2.evidence.evidence.provenance.oldest_input_age,
        ra.evidence.evidence.provenance.oldest_input_age
    );
}

#[test]
fn probe_cache_invalidation_on_data_and_chunk_changes() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 2, 3, 20, 30);
    let r1: WhyReport = serde_json::from_str(&run(&["--json", "why", path(&data)], 0)).unwrap();
    // Replace one file with different content (more distinct ids).
    corpus(&dir.path().join("other"), 1, 3, 20, 1000);
    std::fs::copy(
        dir.path().join("other").join("part-000.parquet"),
        data.join("part-001.parquet"),
    )
    .unwrap();
    let r2: WhyReport = serde_json::from_str(&run(&["--json", "why", path(&data)], 0)).unwrap();
    let id = |r: &WhyReport| {
        r.profile
            .fanout
            .iter()
            .find(|e| e.fanout.columns == ["id"])
            .map(|e| e.fanout.distinct)
    };
    eprintln!(
        "id distinct before {:?} after file replaced {:?}",
        id(&r1),
        id(&r2)
    );
    assert_ne!(
        id(&r1),
        id(&r2),
        "a replaced file is re-profiled, not served from the cache"
    );

    // Text: the chunk size is part of the cache key.
    let t = dir.path().join("text");
    std::fs::create_dir_all(&t).unwrap();
    let mut s = String::from("id,v\n");
    for i in 0..200 {
        s.push_str(&format!("{},{}\n", i % 37, i));
    }
    std::fs::write(t.join("a.csv"), &s).unwrap();
    let c1: WhyReport = serde_json::from_str(&run(
        &[
            "--json",
            "--format",
            "csv",
            "--chunk-size",
            "256",
            "why",
            path(&t),
        ],
        0,
    ))
    .unwrap();
    let c2: WhyReport = serde_json::from_str(&run(
        &[
            "--json",
            "--format",
            "csv",
            "--chunk-size",
            "1024",
            "why",
            path(&t),
        ],
        0,
    ))
    .unwrap();
    eprintln!(
        "csv row groups: dataset {} vs {}; profile {} vs {}",
        c1.dataset.row_groups,
        c2.dataset.row_groups,
        c1.profile.row_groups_total,
        c2.profile.row_groups_total
    );
    assert_eq!(c1.profile.row_groups_total, c1.dataset.row_groups);
    assert_eq!(c2.profile.row_groups_total, c2.dataset.row_groups);
}

#[test]
fn probe_evidence_provenance_and_receipt_chain() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    // Ids scattered over row groups (37k mod 400): the files' min/max
    // statistics skip nothing, so a key is recommended and applied.
    scattered(&data, 3, 4, 25, 400);
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[5, 77, 123]);
    let e = Engine::open(path(&data), EngineOptions::default()).unwrap();
    e.index(&[spec::parse("exact:batch").unwrap()]).unwrap();
    let w = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&kf)),
            ..WhyRequest::default()
        })
        .unwrap();
    let ev = &w.evidence;
    let p = &ev.evidence.provenance;
    eprintln!(
        "evidence recorded={} inputs={} manifest={:?} created_at={} age={:?}",
        ev.recorded,
        p.inputs.len(),
        p.manifest_version,
        p.created_at,
        p.oldest_input_age
    );
    assert!(ev.recorded && !p.inputs.is_empty());
    assert_eq!(p.manifest_version.as_deref(), Some("1"));
    let h = ev.hash.unwrap();
    let stored = std::fs::read(
        data.join(".bellwether/ledger")
            .join(format!("{}.rec", h.to_hex())),
    )
    .unwrap();
    assert_eq!(
        stored,
        serde_json::to_vec(&Record::Evidence(ev.evidence.clone())).unwrap()
    );
    for c in &w.candidates {
        let Record::Proposal(pr) = &c.proposal.record else {
            panic!()
        };
        assert_eq!(
            pr.evidence,
            vec![h],
            "every proposal cites the profile Evidence"
        );
    }
    let rec = w.recommended.as_ref().unwrap();
    let f = e
        .fix(&FixRequest {
            proposal: Some(rec.proposal),
            workload: Some(w.workload.as_ref().unwrap().hash),
            ..FixRequest::default()
        })
        .unwrap();
    let FixOutcome::Applied { receipt, .. } = &f.outcome else {
        panic!("{:?}", f.outcome)
    };
    let chain = e.receipt(&receipt.hash).unwrap();
    eprintln!(
        "receipt chain intact={} links={:?}",
        chain.intact,
        chain
            .chain
            .iter()
            .map(|l| (l.kind, l.status.clone()))
            .collect::<Vec<_>>()
    );
    assert!(chain.intact);
    assert!(chain.chain.iter().any(|l| l.hash == h));
}

fn entity_ids(data: &Path) -> Vec<u64> {
    let mut all = BTreeSet::new();
    let mut stack = vec![data.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
                continue;
            }
            if p.extension().is_some_and(|x| x == "parquet") {
                for b in ParquetRecordBatchReaderBuilder::try_new(File::open(&p).unwrap())
                    .unwrap()
                    .build()
                    .unwrap()
                {
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
        }
    }
    all.into_iter().collect()
}

/// Focus 3: the floor candidate's prediction equals locate's plan after index.
#[test]
fn probe_why_floor_equals_locate_on_a_bench_corpus() {
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
    let schema = Arc::new(Schema::new(vec![Field::new(
        "entity_id",
        DataType::UInt64,
        false,
    )]));
    let mut files = Vec::new();
    for (i, n) in [1usize, 25, 600].iter().enumerate() {
        let step = (ids.len() / n).max(1);
        let ks: Vec<u64> = ids.iter().step_by(step).take(*n).copied().collect();
        let f = dir.path().join(format!("k{i}.parquet"));
        let b = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt64Array::from(ks)) as ArrayRef],
        )
        .unwrap();
        let mut w = ArrowWriter::try_new(File::create(&f).unwrap(), schema.clone(), None).unwrap();
        w.write(&b).unwrap();
        w.close().unwrap();
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
        let floor = w
            .candidates
            .iter()
            .find(|c| c.origin == CandidateOrigin::Workload)
            .unwrap();
        eprintln!(
            "candidates: {:?}",
            w.candidates
                .iter()
                .map(|c| (bellwether::spec::label(&c.spec), c.origin))
                .collect::<Vec<_>>()
        );
        predicted.push(floor.proposed);
    }
    e.index(&[spec::parse("exact:entity_id").unwrap()]).unwrap();
    for (f, p) in files.iter().zip(&predicted) {
        let l = e.locate(&KeyFile::new(f)).unwrap();
        eprintln!("predicted {p:?} planned {:?}", l.planned);
        assert_eq!(l.planned, *p);
        let w = e
            .why(&WhyRequest {
                keys: Some(KeyFile::new(f)),
                ..WhyRequest::default()
            })
            .unwrap();
        assert_eq!(w.current, Some(*p));
    }
}
