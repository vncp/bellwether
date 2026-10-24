//! `fix` and `reconcile` end to end through the CLI: a planted
//! `batch_stamp_key` corpus yields an intact receipt whose funnel bytes are
//! the replay's; a failing rewrite exits 1 with no receipt; applying a
//! certificate against a stale manifest exits 3 and applies nothing; a key
//! that is already complete is refused, never attested.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

mod common;

use std::path::Path;

use bellwether::types::Record;
use bellwether::{
    FixOutcome, FixResult, ReceiptResult, ReconcileResult, Refusal, ReportResult, Score, WhyReport,
};
use common::{check_records, corpus, index_dir, keys, path, planted, run, typed};

/// A `plan` Evidence hash to cite: index `exact:id` (a no-op when already
/// indexed) and locate one key.
fn evidence_of(data: &Path, dir: &Path) -> String {
    run(&["index", path(data), "--key", "exact:id"], 0);
    let kf = dir.join("evidence-key.parquet");
    keys(&kf, "id", &[1]);
    let plan: bellwether::PlanResult = typed(&run(
        &["--json", "locate", path(data), "--keys", path(&kf)],
        0,
    ));
    plan.evidence.hash.to_hex()
}

/// Receipts in the ledger under `index`.
fn receipts(index: &Path) -> usize {
    let Ok(dir) = std::fs::read_dir(index.join("ledger")) else {
        return 0;
    };
    dir.filter(|e| {
        let bytes = std::fs::read(e.as_ref().unwrap().path()).unwrap();
        bytes.starts_with(br#"{"record":"receipt""#)
    })
    .count()
}

fn sequence(index: &Path) -> u64 {
    let m: serde_json::Value =
        serde_json::from_slice(&std::fs::read(index.join("manifest.json")).unwrap()).unwrap();
    m["sequence"].as_u64().unwrap()
}

#[test]
fn fix_on_a_planted_batch_stamp_corpus_writes_an_intact_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let corpus_dir = dir.path().join("corpus");
    let data = planted(&corpus_dir, 6);
    let index = index_dir(&data);
    run(&["index", path(&data), "--key", "exact:batch_id"], 0);
    let out = run(
        &[
            "--json",
            "why",
            path(&data),
            "--bench-workload",
            path(&corpus_dir),
        ],
        0,
    );
    let why: WhyReport = typed(&out);
    let rec = why.recommended.clone().unwrap();
    let workload = why.workload.as_ref().unwrap().hash;

    let out = run(
        &[
            "--json",
            "fix",
            path(&data),
            "--proposal",
            &rec.proposal.to_hex(),
            "--workload",
            &workload.to_hex(),
        ],
        0,
    );
    let fix: FixResult = typed(&out);
    let v: serde_json::Value = serde_json::from_str(&out).unwrap();
    check_records(&index, &v);
    let FixOutcome::Applied {
        receipt,
        realized,
        warning,
    } = &fix.outcome
    else {
        panic!("{:?}", fix.outcome)
    };
    assert_eq!(warning, &None);
    // fix replayed the same proposal at manifest_after: the realized cost.
    let realized = realized
        .as_ref()
        .expect("a key apply writes its realized replay");
    let Record::Replay(after_apply) = &realized.record else {
        panic!()
    };
    assert_eq!(after_apply.manifest_version, "2");
    assert_eq!(after_apply.proposal, rec.proposal);
    let realized_bytes: u64 = after_apply.current.iter().map(|c| c.bytes).sum();
    assert_eq!(
        realized_bytes, rec.proposed.bytes,
        "realized equals predicted"
    );
    // report pairs the receipt with it, straight after fix.
    let out = run(
        &[
            "--json",
            "report",
            path(&data),
            "--html",
            path(&dir.path().join("after-fix.html")),
        ],
        0,
    );
    let rep: ReportResult = typed(&out);
    assert_eq!(rep.realized, 1, "report shows the receipt as realized");
    let Record::Receipt(r) = &receipt.record else {
        panic!()
    };
    assert_eq!(
        (r.manifest_before.as_str(), r.manifest_after.as_str()),
        ("1", "2")
    );
    // The funnel `why` showed is the replay `fix` computed and attested.
    let Some(Score::Replayed {
        replay, proposed, ..
    }) = &fix.score
    else {
        panic!()
    };
    assert_eq!(r.replay, replay.hash);
    assert_eq!(proposed.bytes, rec.proposed.bytes);
    assert_eq!(proposed.row_groups, rec.proposed.row_groups);
    let Some(cert) = &fix.certificate else {
        panic!()
    };
    let Record::Certificate(c) = &cert.record else {
        panic!()
    };
    assert!(c.pass);
    assert_eq!(c.provenance.manifest_version.as_deref(), Some("1"));

    // The chain is intact and every link is the ledger's bytes.
    let out = run(
        &[
            "--json",
            "receipt",
            "show",
            path(&data),
            &receipt.hash.to_hex(),
        ],
        0,
    );
    let shown: ReceiptResult = typed(&out);
    assert!(shown.intact);
    let v: serde_json::Value = serde_json::from_str(&out).unwrap();
    let recs = check_records(&index, &v);
    assert_eq!(recs.len(), shown.chain.len());

    // Re-running why on the same workload realizes the prediction, and the
    // report pairs them.
    let out = run(
        &[
            "--json",
            "why",
            path(&data),
            "--workload",
            &workload.to_hex(),
        ],
        0,
    );
    let after: WhyReport = typed(&out);
    assert_eq!(
        after.current.unwrap(),
        rec.proposed,
        "realized equals predicted"
    );
    let out = run(
        &[
            "--json",
            "report",
            path(&data),
            "--html",
            path(&dir.path().join("r.html")),
        ],
        0,
    );
    let rep: ReportResult = typed(&out);
    assert_eq!(rep.receipts, vec![receipt.hash]);
    assert_eq!(rep.realized, 1);
}

#[test]
fn fix_on_a_rewrite_whose_outputs_differ_exits_1_and_writes_no_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 1, 2, 20, 40);
    let before = dir.path().join("before");
    let after = dir.path().join("after");
    corpus(&before, 1, 2, 20, 40);
    corpus(&after, 1, 2, 20, 40);
    // One changed row on the after side: rewrite the file with other ids.
    corpus(&after, 1, 2, 20, 39);
    let ev = evidence_of(&data, dir.path());
    let p = run(
        &[
            "propose",
            path(&data),
            "--evidence",
            &ev,
            "--before",
            "join on batch",
            "--after",
            "join on id",
            "--invariant",
            "the rows per id are unchanged",
        ],
        0,
    );
    let proposal = p.trim().trim_start_matches("proposal ").to_string();
    let out = run(
        &[
            "--json",
            "fix",
            path(&data),
            "--proposal",
            &proposal,
            "--before-output",
            path(&before),
            "--after-output",
            path(&after),
            "--key",
            "id",
        ],
        1,
    );
    let fix: FixResult = typed(&out);
    assert!(matches!(
        fix.outcome,
        FixOutcome::Refused {
            refusal: Refusal::CertificateFailed { mismatched, .. }
        } if mismatched > 0
    ));
    assert_eq!(
        fix.invariant.as_deref(),
        Some("the rows per id are unchanged")
    );
    assert!(matches!(fix.score, Some(Score::NotScorable { .. })));
    assert_eq!(receipts(&index_dir(&data)), 0);

    // The apply tier re-checks the certificate: applying the failing one is
    // refused as failing (exit 1), before anything else is considered.
    let failing = fix.certificate.as_ref().unwrap().hash.to_hex();
    let out = run(
        &["--json", "fix", path(&data), "--certificate", &failing],
        1,
    );
    let resumed: FixResult = typed(&out);
    assert!(matches!(
        resumed.outcome,
        FixOutcome::Refused {
            refusal: Refusal::CertificateFailed { .. }
        }
    ));
    assert_eq!(receipts(&index_dir(&data)), 0);

    // Identical outputs verify, and are refused at attest: rewrites have
    // no replay yet.
    let out = run(
        &[
            "--json",
            "fix",
            path(&data),
            "--proposal",
            &proposal,
            "--before-output",
            path(&before),
            "--after-output",
            path(&before),
            "--key",
            "id",
        ],
        3,
    );
    let fix: FixResult = typed(&out);
    assert!(matches!(
        fix.outcome,
        FixOutcome::Refused {
            refusal: Refusal::NoReplay { .. }
        }
    ));
    assert_eq!(receipts(&index_dir(&data)), 0);
}

#[test]
fn applying_a_certificate_against_a_stale_manifest_exits_3_and_applies_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 2, 3, 20, 100);
    let index = index_dir(&data);
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[4, 9, 60]);
    run(&["index", path(&data), "--key", "exact:batch"], 0);
    let why: WhyReport = typed(&run(
        &["--json", "why", path(&data), "--from", path(&kf)],
        0,
    ));
    let rec = why.recommended.unwrap();
    let workload = why.workload.unwrap().hash;
    let fix: FixResult = typed(&run(
        &[
            "--json",
            "fix",
            path(&data),
            "--proposal",
            &rec.proposal.to_hex(),
            "--workload",
            &workload.to_hex(),
            "--verify-only",
        ],
        0,
    ));
    assert!(matches!(fix.outcome, FixOutcome::Verified));
    let cert = fix.certificate.unwrap().hash;
    // A second build lands between the certificate and the apply.
    run(&["index", path(&data), "--key", "range:ts"], 0);
    let stale = sequence(&index);
    let out = run(
        &[
            "--json",
            "fix",
            path(&data),
            "--certificate",
            &cert.to_hex(),
        ],
        3,
    );
    let r: FixResult = typed(&out);
    assert!(matches!(
        r.outcome,
        FixOutcome::Refused {
            refusal: Refusal::StaleManifest { .. }
        }
    ));
    assert_eq!(sequence(&index), stale, "nothing was applied");
    assert_eq!(receipts(&index), 0);
}

#[test]
fn a_key_that_is_already_complete_is_refused_never_attested() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 1, 3, 20, 50);
    let index = index_dir(&data);
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[1, 2]);
    run(&["index", path(&data), "--key", "exact:id"], 0);
    let why: WhyReport = typed(&run(
        &["--json", "why", path(&data), "--from", path(&kf)],
        0,
    ));
    let workload = why.workload.unwrap().hash;
    let ev = evidence_of(&data, dir.path());
    let p = run(
        &[
            "propose",
            path(&data),
            "--evidence",
            &ev,
            "--key",
            "exact:id",
        ],
        0,
    );
    let proposal = p.trim().trim_start_matches("proposal ").to_string();
    let before = sequence(&index);
    // The key is current, so its replay predicts no saving: the gain gate
    // refuses first, citing that replay ...
    let out = run(
        &[
            "--json",
            "fix",
            path(&data),
            "--proposal",
            &proposal,
            "--workload",
            &workload.to_hex(),
        ],
        3,
    );
    let r: FixResult = typed(&out);
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
    // ... and past the gate the apply finds nothing to do.
    let out = run(
        &[
            "--json",
            "fix",
            path(&data),
            "--proposal",
            &proposal,
            "--workload",
            &workload.to_hex(),
            "--allow-no-gain",
        ],
        3,
    );
    let r: FixResult = typed(&out);
    assert!(matches!(
        r.outcome,
        FixOutcome::Refused {
            refusal: Refusal::NothingToApply { .. }
        }
    ));
    assert_eq!(sequence(&index), before);
    assert_eq!(receipts(&index), 0);
}

#[test]
fn a_layout_proposal_is_refused_and_reconcile_exits_1_on_differences() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 1, 2, 20, 40);
    let ev = evidence_of(&data, dir.path());
    let p = run(
        &["propose", path(&data), "--evidence", &ev, "--layout", "ts"],
        0,
    );
    let proposal = p.trim().trim_start_matches("proposal ").to_string();
    let r: FixResult = typed(&run(
        &["--json", "fix", path(&data), "--proposal", &proposal],
        3,
    ));
    assert!(matches!(
        r.outcome,
        FixOutcome::Refused {
            refusal: Refusal::LayoutNotApplicable
        }
    ));

    let same = dir.path().join("same");
    corpus(&same, 1, 2, 20, 40);
    let other = dir.path().join("other");
    corpus(&other, 1, 2, 20, 39);
    let ok: ReconcileResult = typed(&run(
        &[
            "--json",
            "reconcile",
            path(&data),
            path(&same),
            "--key",
            "id,payload",
        ],
        0,
    ));
    assert!(ok.outcome.pass);
    let v: serde_json::Value = serde_json::to_value(&ok).unwrap();
    check_records(&index_dir(&data), &v);
    let bad: ReconcileResult = typed(&run(
        &[
            "--json",
            "reconcile",
            path(&data),
            path(&other),
            "--key",
            "id,payload",
        ],
        1,
    ));
    assert!(!bad.outcome.pass);
}

#[test]
fn applying_a_certificate_after_the_data_changed_exits_3_naming_re_verification() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 2, 3, 20, 100);
    let index = index_dir(&data);
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[4, 9, 60]);
    let why: WhyReport = typed(&run(
        &["--json", "why", path(&data), "--from", path(&kf)],
        0,
    ));
    let rec = why.recommended.unwrap();
    let workload = why.workload.unwrap().hash;
    let fix: FixResult = typed(&run(
        &[
            "--json",
            "fix",
            path(&data),
            "--proposal",
            &rec.proposal.to_hex(),
            "--workload",
            &workload.to_hex(),
            "--verify-only",
        ],
        0,
    ));
    let cert = fix.certificate.unwrap().hash;
    // A file lands with no commit: the manifest sequence is unchanged.
    let first = std::fs::read_dir(&data)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|x| x == "parquet"))
        .unwrap();
    std::fs::copy(&first, data.join("late.parquet")).unwrap();
    let out = run(
        &[
            "--json",
            "fix",
            path(&data),
            "--certificate",
            &cert.to_hex(),
        ],
        3,
    );
    let r: FixResult = typed(&out);
    assert!(matches!(
        r.outcome,
        FixOutcome::Refused {
            refusal: Refusal::DataChanged { .. }
        }
    ));
    assert_eq!(receipts(&index), 0);
    let human = run(&["fix", path(&data), "--certificate", &cert.to_hex()], 3);
    assert!(human.contains("re-verify"), "{human}");
}
