//! Invariant 2: a Receipt implies a passing Certificate.
//!
//! Each violation is a single mutation of an otherwise valid chain, and each
//! rejection is checked for the specific rule, so no test passes because some
//! other link happened to be broken. A valid chain is accepted at the end.

#![allow(clippy::unwrap_used)]

mod common;

use bellwether_ledger::{record_path, Ledger, RecordKind};
use bellwether_types::{Hash, Record};
use common::{assert_rule, certificate, proposal, replay, valid_chain, MemStore, AFTER, BEFORE};

fn no_receipts(ledger: &Ledger<&MemStore>) {
    assert_eq!(ledger.list(RecordKind::Receipt).unwrap().count(), 0);
}

#[test]
fn missing_certificate_is_rejected() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let mut f = valid_chain(&ledger);
    f.receipt.certificate = Hash::of(b"never written");
    assert_rule(
        ledger.put_receipt(&f.receipt, AFTER),
        "receipt's certificate",
    );
    assert_rule(ledger.put_receipt(&f.receipt, AFTER), "does not resolve");
    no_receipts(&ledger);
}

#[test]
fn failing_certificate_is_rejected() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let mut f = valid_chain(&ledger);
    // A failing certificate is itself a legitimate record and is stored.
    let failing = ledger
        .put(&Record::Certificate(certificate(Some(f.proposal), false)))
        .unwrap();
    f.receipt.certificate = failing;
    assert_rule(ledger.put_receipt(&f.receipt, AFTER), "did not pass");
    no_receipts(&ledger);
}

#[test]
fn mismatched_proposal_is_rejected() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let mut f = valid_chain(&ledger);
    // A replay that scored a different (but real) proposal.
    let other = ledger
        .put(&Record::Proposal(proposal(
            vec![f.evidence],
            "someone else",
        )))
        .unwrap();
    let other_replay = ledger.put(&Record::Replay(replay(other))).unwrap();
    f.receipt.replay = other_replay;
    assert_rule(
        ledger.put_receipt(&f.receipt, AFTER),
        "not the receipt's proposal",
    );
    no_receipts(&ledger);
}

#[test]
fn certificate_computed_against_another_manifest_is_rejected() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let mut f = valid_chain(&ledger);
    // Rule (b): the certificate must be computed against manifest_before.
    // A certificate from an older manifest is a stale verification.
    let mut stale = certificate(Some(f.proposal), true);
    stale.provenance.manifest_version = Some("6".into());
    f.receipt.certificate = ledger.put(&Record::Certificate(stale)).unwrap();
    assert_rule(ledger.put_receipt(&f.receipt, AFTER), "manifest_before");
    // A certificate that names no manifest version never reaches the ledger
    // (invariant 6), so it cannot satisfy the rule either.
    let mut unpinned = certificate(Some(f.proposal), true);
    unpinned.provenance.manifest_version = None;
    assert_rule(
        ledger.put(&Record::Certificate(unpinned)),
        "names no manifest version",
    );
    no_receipts(&ledger);
}

#[test]
fn manifest_after_greater_than_current_is_rejected() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let f = valid_chain(&ledger);
    // Rule (c), upper bound: manifest_after <= current. A receipt cannot
    // claim an apply whose commit is not current yet, for example one written
    // before the apply committed (current == manifest_before).
    assert_rule(
        ledger.put_receipt(&f.receipt, BEFORE),
        "receipt's manifest_after 8 is greater than the current manifest sequence 7",
    );
    no_receipts(&ledger);
}

#[test]
fn manifest_after_not_greater_than_manifest_before_for_a_key_proposal_is_rejected() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let f = valid_chain(&ledger);
    // Rule (c), lower bound: manifest_before < manifest_after for a Key
    // proposal. An apply that left the manifest where it was, or moved it
    // backwards, is refused.
    for (after, why) in [
        (
            BEFORE,
            "only a rewrite may leave the manifest unchanged, and this is a key proposal",
        ),
        ("6", "an apply never moves the manifest backwards"),
    ] {
        let mut r = f.receipt.clone();
        r.manifest_after = after.into();
        assert_rule(
            ledger.put_receipt(&r, AFTER),
            &format!(
                "receipt's manifest_after {after} is not greater than manifest_before 7; {why}"
            ),
        );
    }
    no_receipts(&ledger);
}

#[test]
fn maintenance_commits_between_apply_and_receipt_are_allowed() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let f = valid_chain(&ledger);
    // GC claim/release and compaction advance the sequence too (ADR 020). If
    // they land between the apply (sequence 8) and the receipt write, the
    // current sequence is past manifest_after, and the receipt is legitimate.
    let h = ledger.put_receipt(&f.receipt, "11").unwrap();
    // Re-writing it later is idempotent.
    assert_eq!(ledger.put_receipt(&f.receipt, "12").unwrap(), h);
}

#[test]
fn manifest_sequences_compare_numerically() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let f = valid_chain(&ledger);
    // A chain computed against sequence 9, applied to produce 10. As strings,
    // "10" sorts before "9", so string comparison would get both bounds wrong.
    let mut rp = replay(f.proposal);
    rp.manifest_version = "9".into();
    let mut cert = certificate(Some(f.proposal), true);
    cert.provenance.manifest_version = Some("9".into());
    let mut r = f.receipt;
    r.replay = ledger.put(&Record::Replay(rp)).unwrap();
    r.certificate = ledger.put(&Record::Certificate(cert)).unwrap();
    r.manifest_before = "9".into();
    r.manifest_after = "10".into();
    assert_rule(
        ledger.put_receipt(&r, "9"),
        "receipt's manifest_after 10 is greater than the current manifest sequence 9",
    );
    ledger.put_receipt(&r, "10").unwrap();
}

#[test]
fn malformed_manifest_sequences_are_rejected() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let f = valid_chain(&ledger);
    // Sequences are u64s in canonical decimal (ADR 020), so string equality
    // and numeric equality agree.
    for bad in [
        "v8",
        "08",
        "+8",
        " 8",
        "8 ",
        "",
        "8.0",
        "18446744073709551616",
    ] {
        let mut r = f.receipt.clone();
        r.manifest_after = bad.into();
        assert_rule(
            ledger.put_receipt(&r, AFTER),
            &format!("receipt's manifest_after {bad:?} is not a manifest sequence number"),
        );
        assert_rule(
            ledger.put_receipt(&f.receipt, bad),
            &format!("current manifest sequence {bad:?} is not a manifest sequence number"),
        );
    }
    let mut r = f.receipt;
    r.manifest_before = "v7".into();
    assert_rule(
        ledger.put_receipt(&r, AFTER),
        "receipt's manifest_before \"v7\" is not a manifest sequence number",
    );
    no_receipts(&ledger);
}

#[test]
fn certificate_for_another_proposal_is_rejected() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let mut f = valid_chain(&ledger);
    let other = ledger
        .put(&Record::Proposal(proposal(vec![], "someone else")))
        .unwrap();
    f.receipt.certificate = ledger
        .put(&Record::Certificate(certificate(Some(other), true)))
        .unwrap();
    assert_rule(ledger.put_receipt(&f.receipt, AFTER), "certificate");
    assert_rule(
        ledger.put_receipt(&f.receipt, AFTER),
        "not the receipt's proposal",
    );
    no_receipts(&ledger);
}

#[test]
fn certificate_hash_of_the_wrong_kind_is_rejected() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let mut f = valid_chain(&ledger);
    f.receipt.certificate = f.proposal;
    assert_rule(ledger.put_receipt(&f.receipt, AFTER), "not a certificate");
    no_receipts(&ledger);
}

#[test]
fn unresolved_proposal_or_replay_is_rejected() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let f = valid_chain(&ledger);

    let mut r = f.receipt.clone();
    r.proposal = Hash::of(b"no such proposal");
    assert_rule(
        ledger.put_receipt(&r, AFTER),
        &format!("receipt's proposal {} does not resolve", r.proposal),
    );

    let mut r = f.receipt.clone();
    r.replay = Hash::of(b"no such replay");
    assert_rule(
        ledger.put_receipt(&r, AFTER),
        &format!("receipt's replay {} does not resolve", r.replay),
    );

    let mut r = f.receipt;
    r.replay = f.certificate;
    assert_rule(ledger.put_receipt(&r, AFTER), "not a replay");
    no_receipts(&ledger);
}

#[test]
fn generic_put_never_stores_a_receipt() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let f = valid_chain(&ledger);
    assert_rule(ledger.put(&Record::Receipt(f.receipt)), "put_receipt");
    no_receipts(&ledger);
}

#[test]
fn valid_chain_is_accepted() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let f = valid_chain(&ledger);
    let h = ledger.put_receipt(&f.receipt, AFTER).unwrap();
    assert_eq!(h, Record::Receipt(f.receipt.clone()).hash().unwrap());
    assert_eq!(ledger.get(&h).unwrap(), Record::Receipt(f.receipt));
    let receipts: Vec<_> = ledger
        .list(RecordKind::Receipt)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(receipts, vec![h]);
}

#[test]
fn certificate_without_a_proposal_cannot_back_a_receipt() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let mut f = valid_chain(&ledger);
    // A plain reconcile (no proposal) is a storable certificate...
    f.receipt.certificate = ledger
        .put(&Record::Certificate(certificate(None, true)))
        .unwrap();
    // ...but it verified nothing in particular, so it backs no receipt.
    assert_rule(
        ledger.put_receipt(&f.receipt, AFTER),
        "verified no proposal",
    );
    no_receipts(&ledger);
}

#[test]
fn passing_certificate_with_differences_is_rejected() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let f = valid_chain(&ledger);
    for mutate in [
        |c: &mut bellwether_types::Certificate| c.body.mismatched = 1,
        |c: &mut bellwether_types::Certificate| c.body.missing_left = 1,
        |c: &mut bellwether_types::Certificate| c.body.missing_right = 1,
        // rule 9, record version 3: an uncomparable key is not a pass
        |c: &mut bellwether_types::Certificate| c.body.uncomparable = 1,
        |c: &mut bellwether_types::Certificate| {
            c.body.totals_delta = vec![("g".into(), "1.00".into(), "2.00".into())];
        },
    ] {
        let mut c = certificate(Some(f.proposal), true);
        mutate(&mut c);
        assert_rule(
            ledger.put(&Record::Certificate(c)),
            "says pass but reports differences",
        );
    }
    no_receipts(&ledger);
}

/// Plants a record straight into the store, skipping `put` and its rules, as
/// an older build or someone with store access could have written it.
fn plant(store: &MemStore, record: &Record) -> Hash {
    let raw = serde_json::to_vec(record).unwrap();
    let h = Hash::of(&raw);
    store.raw_put(&record_path(&h), raw);
    h
}

#[test]
fn receipt_rechecks_a_passing_certificate_with_differences() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let f = valid_chain(&ledger);
    // Certificates that `put` would refuse (rule 9), planted so that only the
    // receipt-time recheck stands between them and a receipt.
    for mutate in [
        |c: &mut bellwether_types::Certificate| c.body.mismatched = 5,
        |c: &mut bellwether_types::Certificate| c.body.missing_left = 5,
        |c: &mut bellwether_types::Certificate| c.body.missing_right = 5,
        |c: &mut bellwether_types::Certificate| {
            c.body.totals_delta = vec![("g".into(), "1.00".into(), "2.00".into())];
        },
    ] {
        let mut c = certificate(Some(f.proposal), true);
        mutate(&mut c);
        let planted = plant(&store, &Record::Certificate(c));
        let mut r = f.receipt.clone();
        r.certificate = planted;
        assert_rule(
            ledger.put_receipt(&r, AFTER),
            &format!("certificate {planted} says pass but reports differences"),
        );
    }
    no_receipts(&ledger);

    // Control: a clean certificate planted the same way is accepted, so the
    // rejections above come from the recheck, not from the planting.
    let mut clean = certificate(Some(f.proposal), true);
    clean.body.ignored_columns.push("loaded_at".into());
    let mut r = f.receipt;
    r.certificate = plant(&store, &Record::Certificate(clean));
    ledger.put_receipt(&r, AFTER).unwrap();
}

#[test]
fn replay_computed_against_another_manifest_is_rejected() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let mut f = valid_chain(&ledger);
    let mut r = replay(f.proposal);
    r.manifest_version = "1".into();
    f.receipt.replay = ledger.put(&Record::Replay(r)).unwrap();
    assert_rule(ledger.put_receipt(&f.receipt, AFTER), "replay");
    assert_rule(
        ledger.put_receipt(&f.receipt, AFTER),
        "not the receipt's manifest_before",
    );
    no_receipts(&ledger);
}

#[test]
fn only_a_rewrite_may_leave_the_manifest_unchanged() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let mut f = valid_chain(&ledger);
    // A key proposal that "applied" without moving the manifest.
    f.receipt.manifest_after = BEFORE.into();
    assert_rule(
        ledger.put_receipt(&f.receipt, BEFORE),
        "is not greater than manifest_before 7; only a rewrite may leave the manifest unchanged",
    );
    no_receipts(&ledger);

    // A rewrite changes a consumer, not the index, so it may.
    let mut p = proposal(vec![f.evidence], "agent");
    p.body = bellwether_types::ProposalBody::Rewrite {
        shape: bellwether_types::RewriteShape {
            before: "scan by batch_id".into(),
            after: "lookup by order_id".into(),
            invariant: "same rows".into(),
            before_plan: None,
            after_plan: None,
        },
    };
    let ph = ledger.put(&Record::Proposal(p)).unwrap();
    f.receipt.proposal = ph;
    f.receipt.replay = ledger.put(&Record::Replay(replay(ph))).unwrap();
    f.receipt.certificate = ledger
        .put(&Record::Certificate(certificate(Some(ph), true)))
        .unwrap();
    // Even a rewrite never moves the manifest backwards.
    let mut backwards = f.receipt.clone();
    backwards.manifest_after = "6".into();
    assert_rule(
        ledger.put_receipt(&backwards, BEFORE),
        "an apply never moves the manifest backwards",
    );
    // Unchanged is fine, with or without later maintenance commits.
    ledger.put_receipt(&f.receipt, BEFORE).unwrap();
    ledger.put_receipt(&f.receipt, "9").unwrap();
}

#[test]
fn receipt_with_bad_created_at_or_wrong_kind_evidence_is_rejected() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let f = valid_chain(&ledger);

    let mut r = f.receipt.clone();
    r.created_at = "x".into();
    assert_rule(ledger.put_receipt(&r, AFTER), "RFC 3339");

    let mut r = f.receipt.clone();
    r.evidence = vec![f.certificate];
    assert_rule(ledger.put_receipt(&r, AFTER), "not an evidence");

    // Evidence that is simply absent is allowed (explain shows it missing).
    let mut r = f.receipt;
    r.evidence.push(Hash::of(b"not yet written"));
    ledger.put_receipt(&r, AFTER).unwrap();
}
