//! Follow-ups to PR #22: reserved proposer names, and the plan-time folder
//! check under the `full` fingerprint policy (metadata first, no lake-wide
//! re-hash).

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::io::{Seek, SeekFrom, Write};

use bellwether::types::{KeySpec, ProposalBody, Record};
use bellwether::{
    spec, Engine, EngineOptions, FingerprintPolicy, KeyFile, ProposeRefusal, ProposeResult,
    WhyRequest, ENGINE_PROPOSER,
};
use common::{ledger_corpus, write_keys};

#[test]
fn the_engines_proposer_names_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data, 1, 2, 10, 5);
    let e = Engine::open(data.to_str().unwrap(), EngineOptions::default()).unwrap();
    e.index(&[spec::parse("exact:id").unwrap()]).unwrap();
    write_keys(&dir.path().join("k.parquet"), "id", &[1]);
    let ev = e
        .locate(&KeyFile::new(dir.path().join("k.parquet")))
        .unwrap()
        .evidence
        .hash;
    let body = ProposalBody::Key {
        spec: KeySpec::Exact {
            columns: vec!["batch".into()],
        },
    };
    let ledger = data.join(".bellwether").join("ledger");
    let before = std::fs::read_dir(&ledger).unwrap().count();
    for name in ["engine", " Engine ", "ENGINE"] {
        let r = e.propose(body.clone(), &[ev], name).unwrap();
        assert_eq!(
            r,
            ProposeResult::Refused {
                refusal: ProposeRefusal::ReservedProposer {
                    name: name.trim().to_string()
                }
            },
            "{name:?}"
        );
    }
    assert_eq!(
        std::fs::read_dir(&ledger).unwrap().count(),
        before,
        "nothing written"
    );
    assert!(e
        .propose(body, &[ev], "engineer")
        .unwrap()
        .proposal()
        .is_some());
    // The engine's own candidates still carry the reserved name.
    let w = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(dir.path().join("k.parquet"))),
            ..WhyRequest::default()
        })
        .unwrap();
    assert!(
        w.candidates.iter().all(|c| c.spec.name() != "exact:id"),
        "exact:id is current"
    );
    for c in &w.candidates {
        let Record::Proposal(p) = &c.proposal.record else {
            panic!()
        };
        assert_eq!(p.proposed_by, ENGINE_PROPOSER);
    }
}

/// Under `full`, the folder check compares path, size, and mtime only. An
/// in-place patch that keeps both passes the folder check (no lake-wide
/// re-hash), and is caught where `full` promises: the planner verifies the
/// full-content fingerprint of each file it plans before reading it. A patch
/// that changes the mtime is refused by the folder check itself.
#[test]
fn the_folder_check_under_full_compares_metadata_and_the_planner_rehashes_planned_files() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    // ids k % 1000: files hold disjoint ids (0..9, 10..19, 20..29).
    ledger_corpus(&data, 3, 2, 5, 1_000);
    let e = Engine::open(
        data.to_str().unwrap(),
        EngineOptions {
            fingerprint_policy: Some(FingerprintPolicy::Full),
            ..EngineOptions::default()
        },
    )
    .unwrap();
    e.index(&[spec::parse("exact:id").unwrap()]).unwrap();
    let manifest = std::fs::read_to_string(data.join(".bellwether/manifest.json")).unwrap();
    let m: serde_json::Value = serde_json::from_str(&manifest).unwrap();
    assert_eq!(m["source"]["fingerprint_policy"], "full");

    // Patch one byte of part-000 near its start, then restore its mtime.
    let target = data.join("part-000.parquet");
    let mtime = std::fs::metadata(&target).unwrap().modified().unwrap();
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(&target)
            .unwrap();
        f.seek(SeekFrom::Start(8)).unwrap();
        f.write_all(&[0xA5]).unwrap();
        f.set_modified(mtime).unwrap();
    }
    let in_other_file = dir.path().join("k1.parquet");
    write_keys(&in_other_file, "id", &[15]);
    let r = e.locate(&KeyFile::new(&in_other_file)).unwrap();
    assert_eq!(
        r.planned.files, 1,
        "the folder check did not re-hash part-000"
    );
    let in_patched_file = dir.path().join("k0.parquet");
    write_keys(&in_patched_file, "id", &[3]);
    let err = e
        .locate(&KeyFile::new(&in_patched_file))
        .unwrap_err()
        .to_string();
    assert!(err.contains("part-000.parquet"), "{err}");

    // A patch that moves the mtime is a changed fingerprint: refused before
    // planning, naming `bellwether index`.
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(&target)
            .unwrap();
        f.seek(SeekFrom::Start(8)).unwrap();
        f.write_all(&[0x5A]).unwrap();
        f.set_modified(mtime + std::time::Duration::from_secs(5))
            .unwrap();
    }
    let err = e
        .locate(&KeyFile::new(&in_other_file))
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("changed file") && err.contains("bellwether index"),
        "{err}"
    );
}
