//! The facade's index, lookup, propose, receipt, and report operations
//! against corpora written directly with the `parquet` crate.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::collections::BTreeSet;

use bellwether::types::{
    EvidenceBody, Hash, KeySpec, Predicate, ProposalBody, Record, RewriteShape,
};
use bellwether::{spec, Engine, EngineOptions, KeyFile, LookupKind, Written};
use common::{ledger_bytes, ledger_corpus, oracle, write_keys};

fn open(root: &std::path::Path) -> Engine {
    Engine::open(root.to_str().unwrap(), EngineOptions::default()).unwrap()
}

/// The record bytes the facade returned are exactly the ledger's file.
fn assert_stored(index: &std::path::Path, w: &Written) {
    let bytes = serde_json::to_vec(&w.record).unwrap();
    assert_eq!(Hash::of(&bytes), w.hash);
    assert_eq!(ledger_bytes(index, &w.hash.to_hex()), bytes);
}

fn planned(w: &Written) -> BTreeSet<(String, usize)> {
    let Record::Evidence(e) = &w.record else {
        panic!("not evidence")
    };
    let EvidenceBody::Plan { plan } = &e.body else {
        panic!("not a plan")
    };
    plan.entries
        .iter()
        .flat_map(|en| {
            let name = std::path::Path::new(&en.file.path)
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string();
            en.row_groups
                .iter()
                .map(move |r| (name.clone(), r as usize))
        })
        .collect()
}

#[test]
fn open_canonicalizes_and_refuses_remote_data_roots() {
    let dir = tempfile::tempdir().unwrap();
    ledger_corpus(&dir.path().join("d"), 1, 1, 10, 5);
    let e = open(&dir.path().join("d").join("..").join("d"));
    assert_eq!(
        e.root(),
        std::fs::canonicalize(dir.path().join("d"))
            .unwrap()
            .to_str()
            .unwrap()
    );
    let err = Engine::open("s3://bucket/data", EngineOptions::default()).unwrap_err();
    assert!(err.to_string().contains("local files only"), "{err}");
    let err = Engine::open(
        dir.path().join("missing").to_str().unwrap(),
        EngineOptions::default(),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("pass an existing directory"),
        "{err}"
    );
}

#[test]
fn locate_plans_every_row_group_holding_a_key_and_records_the_plan() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    // 300 rows, ids k % 1000: each id is in exactly one row group, so a key
    // set selects a strict subset and a raw scan would fail the oracle.
    ledger_corpus(&data, 3, 4, 25, 1_000);
    let e = open(&data);
    let err = e
        .locate(&KeyFile::new(dir.path().join("k.parquet")))
        .unwrap_err();
    assert!(err.to_string().contains("bellwether index"), "{err}");

    let idx = e.index(&[spec::parse("exact:id").unwrap()]).unwrap();
    assert_eq!(idx.committed, Some(1));
    assert_eq!(idx.specs[0].complete_at, Some(1));
    let again = e.index(&[spec::parse("exact:id").unwrap()]).unwrap();
    assert_eq!(again.committed, None, "nothing new to index");

    let keys = [3i64, 17, 139, 260, 1_000];
    write_keys(&dir.path().join("k.parquet"), "id", &keys);
    let r = e
        .locate(&KeyFile::new(dir.path().join("k.parquet")))
        .unwrap();
    assert_eq!(r.keys, 5);
    assert_eq!(r.null_tuples, 0);
    assert_eq!(r.sequence, 1);
    let want = oracle(&data, &keys);
    assert_eq!(
        want.len(),
        3,
        "keys 3 and 17 share a row group; 1000 is absent"
    );
    assert_eq!(planned(&r.evidence), want);
    assert_eq!(r.row_groups_total, 12);
    assert_eq!(r.files_total, 3);
    // The typed summary is the plan Evidence's numbers.
    let Record::Evidence(ev) = &r.evidence.record else {
        unreachable!()
    };
    let EvidenceBody::Plan { plan } = &ev.body else {
        unreachable!()
    };
    assert_eq!(r.planned.files, plan.entries.len() as u64);
    assert_eq!(r.planned.row_groups, plan.row_groups_selected);
    assert_eq!(r.planned.bytes, plan.bytes);
    assert_eq!(
        plan.bytes,
        plan.entries.iter().map(|e| e.bytes).sum::<u64>()
    );
    assert_eq!((r.planned.files, r.planned.row_groups), (3, 3));
    assert!(r.planned.bytes > 0);
    let index = data.join(".bellwether");
    assert_stored(&index, &r.evidence);
    let Record::Evidence(ev) = &r.evidence.record else {
        unreachable!()
    };
    assert_eq!(ev.provenance.manifest_version.as_deref(), Some("1"));
    assert_eq!(ev.provenance.inputs, vec![idx.manifest, r.key_set]);
    assert!(
        index
            .join("cache")
            .join(format!("{}.bin", r.key_set.to_hex()))
            .exists(),
        "the cited key set resolves in the cache"
    );
}

#[test]
fn latest_and_asof_carry_the_invariant_5_residual_and_convert_instants_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data, 2, 3, 20, 7);
    let e = open(&data);
    e.index(&[spec::parse("temporal:id@ts").unwrap()]).unwrap();
    write_keys(&dir.path().join("k.parquet"), "id", &[2, 5]);
    let keys = KeyFile::new(dir.path().join("k.parquet"));
    for r in [
        e.latest(&keys).unwrap(),
        e.asof(&keys, "1970-01-01T00:00:30Z").unwrap(),
    ] {
        let Record::Evidence(ev) = &r.evidence.record else {
            unreachable!()
        };
        let EvidenceBody::Plan { plan } = &ev.body else {
            unreachable!()
        };
        let Some(Predicate::And { all }) = &plan.residual else {
            panic!("temporal residual is an And: {:?}", plan.residual)
        };
        let n = all
            .iter()
            .position(|p| matches!(p, Predicate::NotOlderThan { .. }))
            .unwrap();
        let l = all
            .iter()
            .position(|p| matches!(p, Predicate::LatestPerKey { .. }))
            .unwrap();
        assert!(n < l, "NotOlderThan applies before LatestPerKey");
    }
    let r = e.asof(&keys, "1970-01-01T00:00:30Z").unwrap();
    assert_eq!(
        r.lookup,
        LookupKind::Asof {
            at: "1970-01-01T00:00:30Z".into(),
            ts: 30_000_000,
            ts_column: "ts".into()
        }
    );
    let err = e.asof(&keys, "1970-01-01T00:00:30.0000001Z").unwrap_err();
    assert!(err.to_string().contains("finer than"), "{err}");
    let err = e.latest(&keys.clone().with_columns(vec!["batch".into()]));
    assert!(err.is_err());
}

#[test]
fn propose_writes_a_scoreless_proposal_citing_resolving_evidence_only() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data, 1, 2, 10, 5);
    let e = open(&data);
    e.index(&[spec::parse("exact:id").unwrap()]).unwrap();
    write_keys(&dir.path().join("k.parquet"), "id", &[1]);
    let ev = e
        .locate(&KeyFile::new(dir.path().join("k.parquet")))
        .unwrap()
        .evidence
        .hash;
    let key = ProposalBody::Key {
        spec: KeySpec::Exact {
            columns: vec!["batch".into()],
        },
    };
    assert!(e.propose(key.clone(), &[], "agent").is_err(), "no evidence");
    let missing = Hash::of(b"nothing");
    let err = e.propose(key.clone(), &[missing], "agent").unwrap_err();
    assert!(err.to_string().contains("not in the ledger"), "{err}");
    let unknown = ProposalBody::Key {
        spec: KeySpec::Exact {
            columns: vec!["nope".into()],
        },
    };
    assert!(e.propose(unknown, &[ev], "agent").is_err());
    let empty = ProposalBody::Rewrite {
        shape: RewriteShape {
            before: "a".into(),
            after: " ".into(),
            invariant: "c".into(),
        },
    };
    assert!(e.propose(empty, &[ev], "agent").is_err());

    let p = e.propose(key, &[ev], "agent").unwrap();
    let written = p.proposal().unwrap();
    assert_stored(&data.join(".bellwether"), written);
    // Citing a proposal as evidence is refused by the facade itself, before
    // the ledger, with what to do.
    let err = e
        .propose(
            ProposalBody::Layout {
                cluster_by: vec!["ts".into()],
            },
            &[written.hash],
            "agent",
        )
        .unwrap_err()
        .to_string();
    assert!(err.contains("not an evidence record"), "{err}");
    let json = serde_json::to_value(&p).unwrap().to_string();
    for word in ["score", "estimate", "confidence", "cost"] {
        assert!(!json.contains(word), "{word} in {json}");
    }
}

#[test]
fn receipt_refuses_a_hash_that_is_not_a_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data, 1, 1, 10, 5);
    let e = open(&data);
    e.index(&[spec::parse("exact:id").unwrap()]).unwrap();
    write_keys(&dir.path().join("k.parquet"), "id", &[1]);
    let ev = e
        .locate(&KeyFile::new(dir.path().join("k.parquet")))
        .unwrap()
        .evidence
        .hash;
    assert!(e.receipt(&ev).is_err());
    let err = e.receipt(&Hash::of(b"absent")).unwrap_err();
    assert!(err.to_string().contains("not in the ledger"), "{err}");
}

#[test]
fn report_is_one_self_contained_html_file() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data, 1, 1, 10, 5);
    let e = open(&data);
    e.index(&[spec::parse("exact:id").unwrap()]).unwrap();
    let out = dir.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let r = e.report(&out).unwrap();
    let html = std::fs::read_to_string(out.join("report.html")).unwrap();
    assert_eq!(r.bytes, html.len() as u64);
    assert_eq!(r.blake3, Hash::of(html.as_bytes()));
    let lower = html.to_lowercase();
    for banned in ["<script", "src=", "href=", "@import", "url("] {
        assert!(!lower.contains(banned), "{banned}");
    }
}
