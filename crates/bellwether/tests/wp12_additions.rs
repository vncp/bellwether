//! The facade additions WP12 needs, each read-only or a dry run: typed
//! stale-index and wrong-kind errors with `folder_status` as the one source
//! of the file lists, `propose_dry_run` and `apply_dry_run` writing nothing
//! anywhere, and the ledger views (`findings`, `receipts`, `scorecard`).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::assert_is_empty
)]

mod common;

use std::path::{Path, PathBuf};

use bellwether::types::{Error, Hash, HashKind, KeySpec, ProposalBody};
use bellwether::{
    spec, Engine, EngineOptions, FixOutcome, FixRequest, KeyFile, ProposeRefusal, ProposeResult,
    Refusal, WhyRequest, UNATTRIBUTED,
};
use common::{ledger_corpus, write_file, write_keys};

fn open(root: &Path) -> Engine {
    Engine::open(root.to_str().unwrap(), EngineOptions::default()).unwrap()
}

/// Every file under `dir`, recursively, sorted (missing dir: none).
fn objects(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
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
                out.push(p);
            }
        }
    }
    out.sort();
    out
}

/// A folder, a key file, `why` over it (profile Evidence, engine proposals
/// and replays), and a passing certificate for the first candidate.
struct Fixture {
    _dir: tempfile::TempDir,
    data: PathBuf,
    engine: Engine,
    evidence: Hash,
    proposal: Hash,
    certificate: Hash,
}

fn fixture(indexed: bool) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data, 3, 4, 25, 400);
    let kf = dir.path().join("k.parquet");
    write_keys(&kf, "id", &[5, 77, 123]);
    if indexed {
        open(&data)
            .index(&[spec::parse("exact:batch").unwrap()])
            .unwrap();
    }
    let engine = open(&data);
    let why = engine
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&kf)),
            ..WhyRequest::default()
        })
        .unwrap();
    let evidence = why.evidence.hash.unwrap();
    let proposal = why.candidates[0].proposal.hash;
    let v = engine
        .fix(&FixRequest {
            proposal: Some(proposal),
            workload: Some(why.workload.as_ref().unwrap().hash),
            verify_only: true,
            ..FixRequest::default()
        })
        .unwrap();
    assert!(matches!(v.outcome, FixOutcome::Verified), "{:?}", v.outcome);
    let certificate = v.certificate.unwrap().hash;
    Fixture {
        _dir: dir,
        data,
        engine,
        evidence,
        proposal,
        certificate,
    }
}

/// `folder_status` and the stale-index error carry the same lists, from one
/// source; on a folder never indexed it writes nothing and is empty.
#[test]
fn folder_status_is_the_source_of_the_stale_index_lists() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data, 2, 2, 10, 50);
    let e = open(&data);
    let s = e.folder_status().unwrap();
    assert_eq!(s.sequence, None);
    assert!(!s.is_stale());
    assert!(
        !data.join(".bellwether").exists(),
        "status created the index"
    );

    e.index(&[spec::parse("exact:id").unwrap()]).unwrap();
    assert!(!e.folder_status().unwrap().is_stale());
    write_file(
        &data.join("part-900.parquet"),
        &[vec![(1, 1, Some(1), "x".into())]],
    );
    std::fs::remove_file(data.join("part-000.parquet")).unwrap();
    let s = e.folder_status().unwrap();
    assert_eq!(s.sequence, Some(1));
    assert!(s.is_stale());
    assert_eq!(s.untracked.len(), 1);
    assert!(s.untracked[0].ends_with("part-900.parquet"));
    assert_eq!(s.missing.len(), 1);
    assert!(s.missing[0].ends_with("part-000.parquet"));
    let kf = dir.path().join("k.parquet");
    write_keys(&kf, "id", &[1]);
    match e.locate(&KeyFile::new(&kf)).unwrap_err() {
        Error::StaleIndex {
            untracked,
            changed,
            missing,
            message,
        } => {
            assert_eq!(untracked, s.untracked);
            assert_eq!(changed, s.changed);
            assert_eq!(missing, s.missing);
            assert!(message.contains("bellwether index"), "{message}");
        }
        other => panic!("not a typed stale-index error: {other}"),
    }
}

/// A hash that resolves to another kind is a typed `WrongKind` at apply
/// and at propose; an absent one is `RecordNotFound` with the kind it was
/// given as. Nothing is written.
#[test]
fn wrong_kind_and_not_found_are_typed() {
    let f = fixture(false);
    let index = f.data.join(".bellwether");
    let before = objects(&index);
    for dry in [false, true] {
        let err = if dry {
            f.engine.apply_dry_run(&f.proposal).unwrap_err()
        } else {
            f.engine
                .fix(&FixRequest {
                    certificate: Some(f.proposal),
                    ..FixRequest::default()
                })
                .unwrap_err()
        };
        match err {
            Error::WrongKind {
                hash,
                expected,
                found,
            } => {
                assert_eq!(hash, f.proposal);
                assert_eq!(expected, HashKind::Certificate);
                assert_eq!(found, "a proposal");
            }
            other => panic!("[dry={dry}] {other}"),
        }
        let absent = Hash::of(b"absent");
        let err = if dry {
            f.engine.apply_dry_run(&absent).unwrap_err()
        } else {
            f.engine
                .fix(&FixRequest {
                    certificate: Some(absent),
                    ..FixRequest::default()
                })
                .unwrap_err()
        };
        assert!(
            matches!(err, Error::RecordNotFound { kind: HashKind::Certificate, hash, .. } if hash == absent),
            "[dry={dry}] {err}"
        );
    }
    let err = f
        .engine
        .propose(
            ProposalBody::Layout {
                cluster_by: vec!["id".into()],
            },
            &[f.proposal],
            "agent",
        )
        .unwrap_err();
    assert!(matches!(err, Error::WrongKind { .. }), "{err}");
    assert_eq!(objects(&index), before);
}

/// `propose_dry_run` returns the Proposal `propose` would write, with the
/// hash the ledger then names it by (under a fixed clock), and writes
/// nothing; on a folder with no index it creates no directory. Refusals and
/// errors are the same as `propose`'s.
#[test]
fn propose_dry_run_writes_nothing_and_predicts_the_hash() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data, 2, 2, 10, 50);
    let e = open(&data);
    let body = ProposalBody::Key {
        spec: KeySpec::Exact {
            columns: vec!["id".into()],
            counts: false,
            bounds: None,
        },
        sidecars: Vec::new(),
    };
    let err = e
        .propose_dry_run(body.clone(), &[Hash::of(b"x")], "agent")
        .unwrap_err();
    assert!(err.to_string().contains("not in the ledger"), "{err}");
    assert!(
        !data.join(".bellwether").exists(),
        "dry run created the index"
    );

    let clocked = Engine::open(
        data.to_str().unwrap(),
        EngineOptions {
            clock: Some("2026-10-24T12:00:00.000Z".into()),
            ..EngineOptions::default()
        },
    )
    .unwrap();
    let ev = clocked.why(&WhyRequest::default()).unwrap();
    let ev = ev.evidence.hash.unwrap();
    let index = data.join(".bellwether");
    let before = objects(&index);
    let dry = clocked
        .propose_dry_run(body.clone(), &[ev], "agent")
        .unwrap();
    let ProposeResult::WouldWrite { proposal: would } = &dry else {
        panic!("{dry:?}");
    };
    assert!(dry.proposal().is_none());
    assert_eq!(objects(&index), before, "the dry run wrote");
    assert_eq!(
        clocked
            .propose_dry_run(body.clone(), &[ev], "engine")
            .unwrap(),
        ProposeResult::Refused {
            refusal: ProposeRefusal::ReservedProposer {
                name: "engine".into()
            }
        }
    );
    assert!(clocked
        .propose_dry_run(
            ProposalBody::Layout {
                cluster_by: vec!["nope".into()]
            },
            &[ev],
            "agent"
        )
        .is_err());
    assert_eq!(objects(&index), before, "a refused dry run wrote");
    let real = clocked.propose(body, &[ev], "agent").unwrap();
    let written = real.proposal().unwrap();
    assert_eq!(written.hash, would.hash);
    assert_eq!(written.record, would.record);
}

/// `apply_dry_run` runs the gate and the kind refusals and writes nothing:
/// a passing current certificate would apply; after a commit it is stale;
/// after the data changes it is `data_changed`; a failing one is
/// `certificate_failed`. The real apply then succeeds where the dry run
/// said it would.
#[test]
fn apply_dry_run_writes_nothing_and_matches_the_gate() {
    let f = fixture(false);
    let index = f.data.join(".bellwether");
    let before = objects(&index);
    let r = f.engine.apply_dry_run(&f.certificate).unwrap();
    assert_eq!(
        r.outcome,
        FixOutcome::WouldApply {
            sequence: 0,
            expected: 1
        }
    );
    assert!(r.score.is_none());
    assert_eq!(objects(&index), before, "the dry run wrote");

    // Data changes without a commit: refused, nothing written.
    let g = fixture(false);
    let gi = g.data.join(".bellwether");
    write_file(
        &g.data.join("part-999.parquet"),
        &[vec![(5, 999, Some(9_999_000_000), "late".into())]],
    );
    let gb = objects(&gi);
    let r = g.engine.apply_dry_run(&g.certificate).unwrap();
    assert!(
        matches!(
            r.outcome,
            FixOutcome::Refused {
                refusal: Refusal::DataChanged { .. }
            }
        ),
        "{:?}",
        r.outcome
    );
    assert_eq!(objects(&gi), gb);

    // A commit lands: stale.
    let h = fixture(true);
    h.engine
        .index(&[spec::parse("exact:payload").unwrap()])
        .unwrap();
    let hi = h.data.join(".bellwether");
    let hb = objects(&hi);
    let r = h.engine.apply_dry_run(&h.certificate).unwrap();
    assert!(
        matches!(
            r.outcome,
            FixOutcome::Refused {
                refusal: Refusal::StaleManifest { .. }
            }
        ),
        "{:?}",
        r.outcome
    );
    assert_eq!(objects(&hi), hb);

    // The real apply where the dry run said it would.
    let a = f
        .engine
        .fix(&FixRequest {
            certificate: Some(f.certificate),
            ..FixRequest::default()
        })
        .unwrap();
    assert!(
        matches!(a.outcome, FixOutcome::Applied { .. }),
        "{:?}",
        a.outcome
    );
}

/// The ledger views: findings is the newest profile per root with the
/// receipts citing it; receipts lists every receipt with an intact chain;
/// the scorecard counts proposals, certificates, and receipts per rule id.
/// None writes anything, and on a folder with no index they are empty and
/// create nothing.
#[test]
fn ledger_views_read_the_ledger_and_write_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let empty = dir.path().join("empty");
    ledger_corpus(&empty, 1, 1, 5, 5);
    let e = open(&empty);
    assert!(e.findings().unwrap().findings.is_empty());
    assert!(e.receipts().unwrap().receipts.is_empty());
    assert!(e.scorecard().unwrap().rules.is_empty());
    assert!(!empty.join(".bellwether").exists());

    let f = fixture(false);
    let applied = f
        .engine
        .fix(&FixRequest {
            certificate: Some(f.certificate),
            ..FixRequest::default()
        })
        .unwrap();
    let FixOutcome::Applied { receipt, .. } = &applied.outcome else {
        panic!("{:?}", applied.outcome);
    };
    let index = f.data.join(".bellwether");
    let before = objects(&index);

    let findings = f.engine.findings().unwrap();
    assert_eq!(findings.findings.len(), 1);
    let finding = &findings.findings[0];
    assert_eq!(finding.root, f.engine.root());
    assert_eq!(finding.evidence.hash, f.evidence);
    assert_eq!(finding.receipts, vec![receipt.hash]);
    assert!(findings.unreadable.is_empty());

    let receipts = f.engine.receipts().unwrap();
    assert_eq!(receipts.receipts.len(), 1);
    assert_eq!(receipts.receipts[0].receipt.hash, receipt.hash);
    assert!(receipts.receipts[0].intact);
    assert_eq!(receipts.receipts[0].chain.len(), 5);

    let card = f.engine.scorecard().unwrap();
    let total_proposals: u64 = card.rules.iter().map(|r| r.proposals).sum();
    assert!(total_proposals >= 1);
    // The applied proposal's rows carry its passing certificate and receipt.
    let rows_with_receipt: Vec<_> = card.rules.iter().filter(|r| r.receipts > 0).collect();
    assert!(!rows_with_receipt.is_empty(), "{card:?}");
    for r in &rows_with_receipt {
        assert_eq!(r.receipts, 1);
        assert_eq!(r.certificates_passed, 1);
        assert_eq!(r.certificates_failed, 0);
    }
    // Every proposal is counted under at least one row.
    let proposals = std::fs::read_dir(index.join("ledger"))
        .unwrap()
        .filter(|e| {
            std::fs::read(e.as_ref().unwrap().path())
                .unwrap()
                .starts_with(br#"{"record":"proposal""#)
        })
        .count() as u64;
    assert!(total_proposals >= proposals);
    let names: Vec<&str> = card.rules.iter().map(|r| r.rule.as_str()).collect();
    let mut sorted = names.clone();
    sorted.sort_unstable();
    assert_eq!(names, sorted);
    assert!(names.iter().all(|n| *n == UNATTRIBUTED || !n.is_empty()));
    assert_eq!(objects(&index), before, "a ledger view wrote");

    // A record this build cannot read is listed, not fatal.
    let junk = Hash::of(b"junk");
    std::fs::write(
        index.join("ledger").join(format!("{}.rec", junk.to_hex())),
        b"{\"record\":\"evidence\",\"record_version\":999}",
    )
    .unwrap();
    assert_eq!(f.engine.findings().unwrap().unreadable, vec![junk]);
}
