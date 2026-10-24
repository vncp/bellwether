//! Storage behaviour: content addressing, idempotence, integrity, versions,
//! listing, unknown-field preservation, the non-receipt write rules, and
//! `explain`.

#![allow(clippy::unwrap_used)]

mod common;

use std::time::Duration;

use bellwether_ledger::{parse_record_path, record_path, Ledger, LinkStatus, RecordKind};
use bellwether_types::{format, Error, Hash, KeySpec, ProposalBody, Receipt, Record};
use common::{
    assert_rule, certificate, evidence, proposal, replay, valid_chain, MemStore, AFTER, BEFORE,
};

#[test]
fn put_is_content_addressed_and_idempotent() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let rec = Record::Proposal(proposal(vec![], "agent"));
    let a = ledger.put(&rec).unwrap();
    let b = ledger.put(&rec).unwrap();
    assert_eq!(a, b);
    assert_eq!(a, rec.hash().unwrap());
    assert_eq!(store.len(), 1);
    let path = record_path(&a);
    assert_eq!(path, format!("ledger/{}.rec", a.to_hex()));
    let (bytes, _) = bellwether_types::Store::get(&store, &path).unwrap();
    assert_eq!(&bytes[..], &serde_json::to_vec(&rec).unwrap()[..]);
    assert_eq!(ledger.get(&a).unwrap(), rec);
}

#[test]
fn missing_record_is_not_found() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let h = Hash::of(b"absent");
    assert!(matches!(ledger.get(&h), Err(Error::NotFound(x)) if x == h));
}

#[test]
fn tampered_record_is_detected() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let rec = Record::Proposal(proposal(vec![], "agent"));
    let h = ledger.put(&rec).unwrap();
    let forged = serde_json::to_vec(&Record::Proposal(proposal(vec![], "mallory"))).unwrap();
    store.raw_put(&record_path(&h), forged);
    assert_rule(ledger.get(&h), "does not match its name");
}

#[test]
fn newer_record_version_is_refused_on_put() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let mut p = proposal(vec![], "agent");
    p.record_version = format::RECORD_VERSION + 1;
    assert!(matches!(
        ledger.put(&Record::Proposal(p)),
        Err(Error::UnsupportedVersion { what: "record", .. })
    ));

    let f = valid_chain(&ledger);
    let mut r = f.receipt;
    r.record_version = format::RECORD_VERSION + 1;
    assert!(matches!(
        ledger.put_receipt(&r, AFTER),
        Err(Error::UnsupportedVersion { .. })
    ));
}

#[test]
fn newer_record_version_is_refused_on_get() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    // A v2 record whose shape this build cannot parse at all: the version
    // check must fire before the typed parse.
    let raw = format!(
        r#"{{"record":"proposal","record_version":{},"whatever":"new shape"}}"#,
        format::RECORD_VERSION + 1
    );
    let h = Hash::of(raw.as_bytes());
    store.raw_put(&record_path(&h), raw.into_bytes());
    let expect = |r: Result<_, Error>| {
        assert!(
            matches!(r, Err(Error::UnsupportedVersion { what: "record", found, supported })
                if found == format::RECORD_VERSION + 1 && supported == format::RECORD_VERSION),
        );
    };
    expect(ledger.get(&h).map(|_| ()));
    expect(ledger.get_raw(&h).map(|_| ()));
}

#[test]
fn newer_record_of_an_unknown_kind_is_unsupported_not_a_serde_error() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let f = valid_chain(&ledger);
    // Both field orders: the version must be checked whatever the kind.
    let newer_v = format::RECORD_VERSION + 1;
    let raws: [Vec<u8>; 2] = [
        format!(r#"{{"record":"signature","record_version":{newer_v},"x":1}}"#).into_bytes(),
        format!(r#"{{"record_version":{newer_v},"record":"signature"}}"#).into_bytes(),
    ];
    let mut newer = Vec::new();
    for raw in raws {
        let h = Hash::of(&raw);
        store.raw_put(&record_path(&h), raw.clone());
        for r in [
            ledger.get_raw(&h).map(|_| ()),
            ledger.get(&h).map(|_| ()),
            ledger.kind(&h).map(|_| ()),
        ] {
            assert!(
                matches!(r, Err(Error::UnsupportedVersion { found, .. }) if found == newer_v),
                "{r:?}"
            );
        }
        newer.push(h);
    }
    // A newer record of a known kind, too.
    let raw = format!(r#"{{"record":"evidence","record_version":{newer_v}}}"#).into_bytes();
    let h = Hash::of(&raw);
    store.raw_put(&record_path(&h), raw);
    newer.push(h);

    // list() skips what it cannot read rather than failing on it...
    let evidence: Vec<Hash> = ledger
        .list(RecordKind::Evidence)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(evidence, vec![f.evidence]);
    let proposals: Vec<Hash> = ledger
        .list(RecordKind::Proposal)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(proposals, vec![f.proposal]);
    // ...and list_unreadable() surfaces it.
    let mut unreadable: Vec<Hash> = ledger
        .list_unreadable()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    unreadable.sort();
    newer.sort();
    assert_eq!(unreadable, newer);
}

#[test]
fn record_version_zero_is_rejected() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let mut p = proposal(vec![], "agent");
    p.record_version = 0;
    assert_rule(ledger.put(&Record::Proposal(p)), "record_version 0");
}

#[test]
fn records_that_do_not_round_trip_are_rejected_at_write() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);

    // NaN serializes as null and would make get() fail.
    let mut p = proposal(vec![], "agent");
    p.body = ProposalBody::Key {
        spec: KeySpec::Bloom {
            columns: vec!["id".into()],
            fpr: f64::NAN,
        },
    };
    assert_rule(ledger.put(&Record::Proposal(p)), "round-trip");

    // A duration beyond u64 nanoseconds (about 584 years) cannot be encoded
    // at all and is refused before anything is written.
    let mut e = evidence();
    e.provenance.oldest_input_age = Duration::from_secs(u64::MAX);
    assert!(ledger.put(&Record::Evidence(e)).is_err());

    assert_eq!(store.len(), 0);

    // Durations are integer nanoseconds (contract v2), so a fractional age
    // of any size round-trips exactly and is accepted.
    let mut e = evidence();
    e.provenance.oldest_input_age = Duration::new(123_456_789, 987_654_321);
    let h = ledger.put(&Record::Evidence(e.clone())).unwrap();
    match ledger.get(&h).unwrap() {
        Record::Evidence(back) => assert_eq!(
            back.provenance.oldest_input_age,
            e.provenance.oldest_input_age
        ),
        other => panic!("expected evidence, got {other:?}"),
    }
}

#[test]
fn ages_of_any_precision_round_trip_exactly() {
    const BOUND: u64 = 1 << 23; // the former float-seconds precision cliff
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let put_and_get = |age: Duration| {
        let mut e = evidence();
        e.provenance.oldest_input_age = age;
        let h = ledger.put(&Record::Evidence(e)).unwrap();
        match ledger.get(&h).unwrap() {
            Record::Evidence(back) => assert_eq!(back.provenance.oldest_input_age, age),
            other => panic!("expected evidence, got {other:?}"),
        }
    };
    // Under integer-nanosecond encoding there is no precision cliff: values
    // below, at, and far beyond the old bound, at every precision, are exact.
    for age in [
        Duration::new(BOUND - 1, 999_999_999),
        Duration::new(BOUND - 1, 1),
        Duration::new(30 * 86_400, 123_456_789),
        Duration::from_secs(BOUND),
        Duration::from_secs(BOUND + 12_345),
        Duration::from_secs(1 << 33),
        Duration::new(BOUND + 7, 500_000_000),
        Duration::new(10_569_850, 255_984_487),
        Duration::new(123_456_789, 987_654_321),
        Duration::new(3_000_000_000, 1),
    ] {
        put_and_get(age);
    }
}

#[test]
fn stored_negative_age_is_an_error_not_a_panic() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let mut f = valid_chain(&ledger);
    let mut v =
        serde_json::to_value(Record::Certificate(certificate(Some(f.proposal), true))).unwrap();
    v["provenance"]["oldest_input_age"] = serde_json::json!(-1.0);
    let raw = serde_json::to_vec(&v).unwrap();
    let h = Hash::of(&raw);
    store.raw_put(&record_path(&h), raw);
    assert_rule(ledger.get(&h), "oldest_input_age");
    f.receipt.certificate = h;
    assert_rule(ledger.put_receipt(&f.receipt, AFTER), "oldest_input_age");
}

#[test]
fn record_names_are_lowercase_hex_both_ways() {
    let h = Hash::of(b"x");
    assert_eq!(parse_record_path(&record_path(&h)), Some(h));
    let upper = format!("ledger/{}.rec", h.to_hex().to_uppercase());
    assert_eq!(parse_record_path(&upper), None);

    // An uppercase-named object is not a ledger record: list skips it.
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let rec = Record::Proposal(proposal(vec![], "a"));
    let raw = serde_json::to_vec(&rec).unwrap();
    store.raw_put(
        &format!("ledger/{}.rec", Hash::of(&raw).to_hex().to_uppercase()),
        raw,
    );
    assert_eq!(ledger.list(RecordKind::Proposal).unwrap().count(), 0);
}

#[test]
fn explain_flags_bad_links_instead_of_failing() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let f = valid_chain(&ledger);
    // A receipt that never passed the write rules (planted straight into the
    // store, as an attacker with store access could): its evidence is a
    // certificate, its replay is missing, and its certificate is a newer
    // version this build cannot read.
    let newer_v = format::RECORD_VERSION + 1;
    let newer = format!(r#"{{"record":"certificate","record_version":{newer_v}}}"#).into_bytes();
    let newer_h = Hash::of(&newer);
    store.raw_put(&record_path(&newer_h), newer);
    let ghost = Hash::of(b"ghost replay");
    let r = Receipt {
        evidence: vec![f.certificate],
        replay: ghost,
        certificate: newer_h,
        ..f.receipt
    };
    let raw = serde_json::to_vec(&Record::Receipt(r)).unwrap();
    let h = Hash::of(&raw);
    store.raw_put(&record_path(&h), raw);

    let chain = ledger.explain(&h).unwrap();
    assert!(!chain.is_intact());
    let statuses: Vec<_> = chain.links.iter().map(|l| l.status.clone()).collect();
    assert_eq!(
        statuses[0],
        LinkStatus::WrongKind {
            found: RecordKind::Certificate
        }
    );
    assert_eq!(statuses[1], LinkStatus::Ok); // the proposal's own evidence
    assert_eq!(statuses[2], LinkStatus::Ok); // proposal
    assert_eq!(statuses[3], LinkStatus::Missing); // replay
    assert!(matches!(statuses[4], LinkStatus::Unreadable { .. })); // certificate
    assert_eq!(statuses[5], LinkStatus::Ok); // receipt
    let text = chain.render();
    assert!(text.contains("WRONG KIND (is a certificate)"), "{text}");
    assert!(text.contains("MISSING (not in ledger)"), "{text}");
    assert!(
        text.contains(&format!("UNREADABLE (unsupported record version {newer_v}")),
        "{text}"
    );
}

#[test]
fn unknown_fields_survive_a_raw_read() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let mut v = serde_json::to_value(Record::Proposal(proposal(vec![], "agent"))).unwrap();
    v["added_by_a_later_build"] = serde_json::json!({"x": 1});
    let raw = serde_json::to_vec(&v).unwrap();
    let h = Hash::of(&raw);
    store.raw_put(&record_path(&h), raw.clone());
    // The raw read is byte-identical, so nothing is lost.
    assert_eq!(&ledger.get_raw(&h).unwrap()[..], &raw[..]);
    // The typed read still works; the unknown field is dropped, because
    // records are versioned, not field-preserving (ADR 017 amendment).
    assert!(matches!(ledger.get(&h).unwrap(), Record::Proposal(_)));
}

#[test]
fn replay_and_certificate_need_a_resolving_proposal() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let ghost = Hash::of(b"ghost");
    assert_rule(
        ledger.put(&Record::Replay(replay(ghost))),
        "replay's proposal",
    );
    assert_rule(
        ledger.put(&Record::Certificate(certificate(Some(ghost), true))),
        "certificate's proposal",
    );
    // Resolving to the wrong kind is no better than not resolving.
    let e = ledger.put(&Record::Evidence(evidence())).unwrap();
    assert_rule(ledger.put(&Record::Replay(replay(e))), "not a proposal");
    // A certificate with no proposal (a plain reconcile) needs nothing.
    ledger
        .put(&Record::Certificate(certificate(None, true)))
        .unwrap();
    assert_eq!(store.len(), 2);
}

#[test]
fn list_filters_by_kind_and_ignores_foreign_paths() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let f = valid_chain(&ledger);
    store.raw_put("ledger/README", "not a record");
    store.raw_put("segments/abc.seg", "not a record");
    let all = |k| {
        let mut v: Vec<Hash> = ledger.list(k).unwrap().collect::<Result<_, _>>().unwrap();
        v.sort();
        v
    };
    assert_eq!(all(RecordKind::Evidence), vec![f.evidence]);
    assert_eq!(all(RecordKind::Proposal), vec![f.proposal]);
    assert_eq!(all(RecordKind::Replay), vec![f.replay]);
    assert_eq!(all(RecordKind::Certificate), vec![f.certificate]);
    assert_eq!(all(RecordKind::Receipt).len(), 0);
}

#[test]
fn explain_walks_the_chain() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let f = valid_chain(&ledger);
    let mut receipt = f.receipt.clone();
    let ghost = Hash::of(b"evidence that was never written");
    receipt.evidence.push(ghost);
    let h = ledger.put_receipt(&receipt, AFTER).unwrap();

    let chain = ledger.explain(&h).unwrap();
    let kinds: Vec<_> = chain.links.iter().map(|l| l.kind).collect();
    assert_eq!(
        kinds,
        [
            RecordKind::Evidence,
            RecordKind::Evidence,
            RecordKind::Proposal,
            RecordKind::Replay,
            RecordKind::Certificate,
            RecordKind::Receipt,
        ]
    );
    assert_eq!(chain.links[0].hash, f.evidence);
    assert_eq!(chain.links[0].status, LinkStatus::Ok);
    assert_eq!(chain.links[1].status, LinkStatus::Missing);
    assert!(!chain.is_intact());
    assert_eq!(chain.links[5].hash, h);

    let text = chain.render();
    let lines: Vec<&str> = text.lines().collect();
    let p = |h: &Hash| h.to_hex()[..12].to_owned();
    let expected = [
        format!(
            "{}  evidence     2026-10-24T00:00:00Z  body=profile  root=./orders  fanout=0  lints=0  inputs=1  age=90s  manifest=7",
            p(&f.evidence)
        ),
        format!(
            "{}  evidence     -                     MISSING (not in ledger)",
            p(&ghost)
        ),
        format!(
            "{}  proposal     2026-10-24T00:01:00Z  key=exact:order_id  by=agent",
            p(&f.proposal)
        ),
        format!(
            "{}  replay       2026-10-24T00:02:00Z  runs=2  bytes_current=1000000  bytes_proposed=5000  manifest=7",
            p(&f.replay)
        ),
        format!(
            "{}  certificate  2026-10-24T00:03:00Z  coverage=workload(4748 keys)  pass=true  mismatched=0",
            p(&f.certificate)
        ),
        format!(
            "{}  receipt      2026-10-24T00:04:00Z  manifest=7->8",
            p(&h)
        ),
    ];
    assert_eq!(lines, expected);
}

#[test]
fn explain_refuses_a_non_receipt() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let f = valid_chain(&ledger);
    assert!(matches!(ledger.explain(&f.proposal), Err(Error::Other(_))));
    assert!(matches!(
        ledger.explain(&Hash::of(b"absent")),
        Err(Error::NotFound(_))
    ));
}

#[test]
fn transient_store_errors_are_not_mistaken_for_absence() {
    use std::sync::atomic::Ordering;
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let f = valid_chain(&ledger);
    store.fail_reads.store(true, Ordering::SeqCst);
    assert!(matches!(ledger.get(&f.proposal), Err(Error::Store(_))));
    // A receipt is still refused, but with the real cause, not a rule.
    assert!(matches!(
        ledger.put_receipt(&f.receipt, AFTER),
        Err(Error::Store(_))
    ));
}

#[test]
fn plan_evidence_round_trips_and_explains() {
    use bellwether_types::{EvidenceBody, FileFingerprint, ScanEntry, ScanPlan};
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let mut rgs = roaring::RoaringBitmap::new();
    rgs.insert(0);
    rgs.insert(7);
    let mut e = evidence();
    e.body = EvidenceBody::Plan {
        plan: ScanPlan {
            manifest_version: BEFORE.into(),
            entries: vec![ScanEntry {
                file: FileFingerprint {
                    path: "a.parquet".into(),
                    size: 10,
                    etag: None,
                    tail_blake3: Hash::of(b"t"),
                },
                row_groups: rgs,
                bytes: 4096,
            }],
            residual: None,
            bytes: 4096,
            row_groups_total: 8,
            row_groups_selected: 2,
        },
    };
    let rec = Record::Evidence(e);
    let eh = ledger.put(&rec).unwrap();
    assert_eq!(ledger.get(&eh).unwrap(), rec);

    let mut f = valid_chain(&ledger);
    f.receipt.evidence = vec![eh];
    let h = ledger.put_receipt(&f.receipt, AFTER).unwrap();
    let text = ledger.explain(&h).unwrap().render();
    let first = text.lines().next().unwrap();
    assert!(
        first.ends_with("body=plan  bytes=4096  row_groups=2/8  inputs=1  age=90s  manifest=7"),
        "{first}"
    );
}
