//! Regression tests harvested from the two review rounds of this crate.
//!
//! Each test replays an attack or failure a reviewer built against an earlier
//! version of the ledger and pins the fix. Cases that `forged_receipt.rs`,
//! `ledger.rs`, `provenance_required.rs`, or `write_paths.rs` already cover
//! are not repeated here.
//!
//! Durations are integer nanoseconds (contract v2), so the duration tests
//! assert exact round trips: every age up to `u64::MAX` nanoseconds is stored
//! and read back identical under its own hash, and nothing larger is
//! accepted. Everything runs against the in-memory store from `tests/common`,
//! so no files are written.

#![allow(clippy::unwrap_used)]

mod common;

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::time::Duration;

use bellwether_ledger::{record_path, Ledger, LinkStatus, RecordKind};
use bellwether_types::{
    format, Coverage, Error, EvidenceBody, Fanout, Hash, KeySpec, ProposalBody, Record,
};
use common::{
    assert_rule, certificate, evidence, proposal, replay, valid_chain, MemStore, AFTER, BEFORE,
};
use serde_json::{json, Value};

const ALL_KINDS: [RecordKind; 5] = [
    RecordKind::Evidence,
    RecordKind::Proposal,
    RecordKind::Replay,
    RecordKind::Certificate,
    RecordKind::Receipt,
];

/// Writes bytes under their content address without going through `put`,
/// as an older build or someone with store access could.
fn plant_bytes(store: &MemStore, raw: Vec<u8>) -> Hash {
    let h = Hash::of(&raw);
    store.raw_put(&record_path(&h), raw);
    h
}

fn plant(store: &MemStore, record: &Record) -> Hash {
    plant_bytes(store, serde_json::to_vec(record).unwrap())
}

fn plant_json(store: &MemStore, value: &Value) -> Hash {
    plant_bytes(store, serde_json::to_vec(value).unwrap())
}

/// Runs `f`; a panic fails the test naming `case`.
fn no_panic<T>(case: &str, f: impl FnOnce() -> T) -> T {
    catch_unwind(AssertUnwindSafe(f)).unwrap_or_else(|_| panic!("{case} panicked"))
}

fn with_age(record: Record, age: Duration) -> Record {
    match record {
        Record::Evidence(mut e) => {
            e.provenance.oldest_input_age = age;
            Record::Evidence(e)
        }
        Record::Certificate(mut c) => {
            c.provenance.oldest_input_age = age;
            Record::Certificate(c)
        }
        other => panic!("{} carries no provenance", RecordKind::of(&other)),
    }
}

// ---------------------------------------------------------------------------
// Forged receipts and wrong-kind references
// ---------------------------------------------------------------------------

#[test]
fn wrong_kind_citations_are_refused_in_proposal_and_receipt_lists() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let f = valid_chain(&ledger);
    let stored = store.len();

    // forged_receipt.rs covers a receipt citing a certificate; these are the
    // other wrong kinds, in both citation lists.
    for (kind, h) in [
        ("a certificate", f.certificate),
        ("a replay", f.replay),
        ("a proposal", f.proposal),
    ] {
        assert_rule(
            ledger.put(&Record::Proposal(proposal(vec![f.evidence, h], "agent"))),
            &format!("proposal's evidence {h} resolves to {kind}, not an evidence"),
        );
    }
    for (kind, h) in [("a replay", f.replay), ("a proposal", f.proposal)] {
        let mut r = f.receipt.clone();
        r.evidence = vec![h];
        assert_rule(
            ledger.put_receipt(&r, AFTER),
            &format!("receipt's evidence {h} resolves to {kind}, not an evidence"),
        );
    }
    // The receipt's own proposal reference must name a proposal.
    let mut r = f.receipt.clone();
    r.proposal = f.replay;
    assert_rule(
        ledger.put_receipt(&r, AFTER),
        &format!(
            "receipt's proposal {} resolves to a replay, not a proposal",
            f.replay
        ),
    );
    assert_eq!(store.len(), stored, "refused writes wrote nothing");
}

#[test]
fn a_citation_that_later_resolves_to_another_kind_is_flagged_by_explain() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let f = valid_chain(&ledger);

    // Cited evidence is kind-checked at write time, and an absent hash is
    // allowed. So a proposal can cite the hash of a record that is written
    // later and is not evidence.
    let later = Record::Proposal(proposal(vec![], "later"));
    let later_h = later.hash().unwrap();
    let p = ledger
        .put(&Record::Proposal(proposal(vec![later_h], "agent")))
        .unwrap();
    assert_eq!(ledger.put(&later).unwrap(), later_h);
    // Once that record exists, the same citation is refused at write time.
    assert_rule(
        ledger.put(&Record::Proposal(proposal(vec![later_h], "agent 2"))),
        &format!("proposal's evidence {later_h} resolves to a proposal, not an evidence"),
    );

    // A receipt for the earlier proposal still passes every receipt rule...
    let mut r = f.receipt;
    r.proposal = p;
    r.replay = ledger.put(&Record::Replay(replay(p))).unwrap();
    r.certificate = ledger
        .put(&Record::Certificate(certificate(Some(p), true)))
        .unwrap();
    let h = ledger.put_receipt(&r, AFTER).unwrap();

    // ...and explain flags the citation instead of failing or trusting it.
    let chain = ledger.explain(&h).unwrap();
    let text = chain.render();
    assert!(!chain.is_intact(), "{text}");
    let flagged: Vec<_> = chain
        .links
        .iter()
        .filter(|l| l.status != LinkStatus::Ok)
        .collect();
    assert_eq!(flagged.len(), 1, "{text}");
    assert_eq!(flagged[0].hash, later_h);
    assert_eq!(flagged[0].kind, RecordKind::Evidence);
    assert_eq!(
        flagged[0].status,
        LinkStatus::WrongKind {
            found: RecordKind::Proposal
        }
    );
    assert!(text.contains("WRONG KIND (is a proposal)"), "{text}");
}

#[test]
fn a_planted_receipt_over_a_failing_certificate_shows_the_failure() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let f = valid_chain(&ledger);
    // A failing certificate is a legitimate stored record, and put_receipt
    // refuses a receipt that cites it.
    let failing = ledger
        .put(&Record::Certificate(certificate(Some(f.proposal), false)))
        .unwrap();
    let mut r = f.receipt;
    r.certificate = failing;
    assert_rule(ledger.put_receipt(&r, AFTER), "did not pass");

    // Someone with store access writes it anyway. The threat model scopes
    // that out of prevention, but the read side must not hide the failure.
    let h = plant(&store, &Record::Receipt(r));
    assert!(matches!(ledger.get(&h).unwrap(), Record::Receipt(_)));
    let chain = ledger.explain(&h).unwrap();
    // Every link is present and of the expected kind, so is_intact() holds.
    // It is documented to check presence and kind only, never the receipt
    // rules; if that ever changes, change this assertion and the doc together.
    assert!(chain.is_intact(), "{}", chain.render());
    let cert = chain
        .links
        .iter()
        .find(|l| l.kind == RecordKind::Certificate)
        .unwrap();
    assert_eq!(cert.hash, failing);
    assert!(
        cert.facts
            .contains(&("pass".to_owned(), "false".to_owned())),
        "{}",
        chain.render()
    );
}

#[test]
fn a_layout_proposal_must_advance_the_manifest() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let f = valid_chain(&ledger);
    let mut p = proposal(vec![f.evidence], "agent");
    p.body = ProposalBody::Layout {
        cluster_by: vec!["order_date".into()],
    };
    let ph = ledger.put(&Record::Proposal(p)).unwrap();
    let mut r = f.receipt;
    r.proposal = ph;
    r.replay = ledger.put(&Record::Replay(replay(ph))).unwrap();
    r.certificate = ledger
        .put(&Record::Certificate(certificate(Some(ph), true)))
        .unwrap();

    // forged_receipt.rs covers a key proposal; a layout change rewrites the
    // files, so it must move the manifest too.
    let mut unchanged = r.clone();
    unchanged.manifest_after = BEFORE.into();
    assert_rule(
        ledger.put_receipt(&unchanged, BEFORE),
        "only a rewrite may leave the manifest unchanged, and this is a layout proposal",
    );
    // Control: the same chain with an advanced manifest is accepted.
    ledger.put_receipt(&r, AFTER).unwrap();
}

// ---------------------------------------------------------------------------
// Headers and versions (ADR 017)
// ---------------------------------------------------------------------------

#[test]
fn an_unknown_kind_at_a_supported_version_is_unreadable_on_every_read_path() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let f = valid_chain(&ledger);
    let raw = format!(
        r#"{{"record":"signature","record_version":{},"key":"k1"}}"#,
        format::RECORD_VERSION
    );
    let h = plant_bytes(&store, raw.into_bytes());

    // The typed unknown-kind error naming the kind, not the version error:
    // the version is one this build supports.
    for (path, result) in [
        ("get_raw", ledger.get_raw(&h).map(|_| ())),
        ("get", ledger.get(&h).map(|_| ())),
        ("kind", ledger.kind(&h).map(|_| ())),
    ] {
        let e = result.expect_err(path);
        assert!(
            matches!(
                &e,
                Error::UnknownKind { what: "record kind", found } if found == "signature"
            ),
            "{path}: {e:?}"
        );
    }
    // list() skips it under every kind, and list_unreadable() reports it.
    for kind in ALL_KINDS {
        let listed: Vec<Hash> = ledger
            .list(kind)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(!listed.contains(&h), "{kind}");
    }
    let unreadable: Vec<Hash> = ledger
        .list_unreadable()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(unreadable, vec![h]);

    // explain flags it as an unreadable link; it never fails on a bad link.
    let mut r = f.receipt;
    r.evidence = vec![h];
    let receipt = plant(&store, &Record::Receipt(r));
    let chain = ledger
        .explain(&receipt)
        .expect("explain must flag an unknown-kind link as unreadable, not fail");
    assert_eq!(chain.links[0].hash, h);
    assert!(
        matches!(chain.links[0].status, LinkStatus::Unreadable { .. }),
        "{}",
        chain.render()
    );
}

#[test]
fn malformed_headers_are_reported_never_read_or_dropped() {
    let v = format::RECORD_VERSION;
    let cases = [
        // A version past u32: unreadable, whatever error names it.
        format!(
            r#"{{"record":"evidence","record_version":{}}}"#,
            u64::from(u32::MAX) + 1
        ),
        format!(r#"{{"record":"evidence","record_version":{v},"record_version":{v}}}"#),
        format!(r#"{{"record_version":{v}}}"#),
    ];
    for raw in cases {
        let store = MemStore::default();
        let ledger = Ledger::new(&store);
        let h = plant_bytes(&store, raw.clone().into_bytes());
        assert!(ledger.get_raw(&h).is_err(), "{raw}");
        assert!(ledger.get(&h).is_err(), "{raw}");

        // Never listed as readable under any kind...
        let mut errors = 0;
        for kind in ALL_KINDS {
            for item in ledger.list(kind).unwrap() {
                match item {
                    Ok(listed) => assert_ne!(listed, h, "{raw}"),
                    Err(_) => errors += 1,
                }
            }
        }
        // ...and never silently dropped: it surfaces as an error or as an
        // unreadable record.
        let surfaced = ledger.list_unreadable().unwrap().any(|item| match item {
            Ok(listed) => listed == h,
            Err(_) => true,
        });
        assert!(errors > 0 || surfaced, "{raw}");
    }
}

#[test]
fn citing_a_newer_record_is_refused_with_the_version_error() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let f = valid_chain(&ledger);
    let newer_version = format::RECORD_VERSION + 1;
    let raw = format!(r#"{{"record":"evidence","record_version":{newer_version}}}"#);
    let newer = plant_bytes(&store, raw.into_bytes());
    let stored = store.len();

    // This build cannot confirm a newer record's kind, so it cannot check the
    // citation. The refusal is the typed version error, never a serde error
    // and never a silent accept.
    let by_proposal = ledger.put(&Record::Proposal(proposal(vec![newer], "agent")));
    let mut r = f.receipt;
    r.evidence = vec![newer];
    let by_receipt = ledger.put_receipt(&r, AFTER);
    for result in [by_proposal, by_receipt] {
        assert!(
            matches!(
                result,
                Err(Error::UnsupportedVersion { what: "record", found, .. }) if found == newer_version
            ),
            "{result:?}"
        );
    }
    assert_eq!(store.len(), stored, "refused writes wrote nothing");
}

// ---------------------------------------------------------------------------
// Values that do not survive the canonical JSON
// ---------------------------------------------------------------------------

#[test]
fn non_finite_floats_are_refused_in_every_float_field() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);

    // ledger.rs covers a NaN bloom fpr; these are the infinities and the
    // other float fields. Each would be written as null and never read back.
    for fpr in [f64::INFINITY, f64::NEG_INFINITY] {
        let mut p = proposal(vec![], "agent");
        p.body = ProposalBody::Key {
            spec: KeySpec::Bloom {
                columns: vec!["order_id".into()],
                fpr,
            },
        };
        assert_rule(ledger.put(&Record::Proposal(p)), "round-trip");
    }

    // Fanout quantiles over an empty set of values come out as NaN.
    let mut e = evidence();
    let EvidenceBody::Profile { fanout, .. } = &mut e.body else {
        panic!("the evidence fixture is a profile");
    };
    fanout.push(Fanout {
        columns: vec!["batch_id".into()],
        distinct: 0,
        sampled: false,
        files_per_value: [f64::NAN; 3],
        row_groups_per_value: [f64::NAN; 3],
        files_total: 0,
        row_groups_total: 0,
    });
    assert_rule(ledger.put(&Record::Evidence(e)), "round-trip");

    let mut c = certificate(None, true);
    c.coverage = Coverage::Sampled {
        keys: 10,
        fraction: f64::NAN,
    };
    assert_rule(ledger.put(&Record::Certificate(c)), "round-trip");

    assert_eq!(store.len(), 0, "nothing was written");
}

#[test]
fn the_largest_encodable_age_round_trips_and_one_nanosecond_more_is_refused() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    // The largest age the encoding carries, about 584 years, is stored and
    // read back exactly under its own name, on both records with provenance.
    let largest = Duration::from_nanos(u64::MAX);
    for base in [
        Record::Evidence(evidence()),
        Record::Certificate(certificate(None, true)),
    ] {
        let record = with_age(base, largest);
        let h = ledger
            .put(&record)
            .unwrap_or_else(|e| panic!("{largest:?} was refused: {e}"));
        let back = ledger.get(&h).unwrap();
        assert_eq!(back, record, "{largest:?} read back changed");
        assert_eq!(back.hash().unwrap(), h, "{largest:?} drifted from its name");
    }

    // Anything larger cannot be encoded and is refused without writing.
    // ledger.rs covers u64::MAX seconds on evidence; this adds the exact
    // boundary, Duration::MAX, and the certificate's provenance.
    let stored = store.len();
    let past = largest + Duration::from_nanos(1);
    for (base, age) in [
        (Record::Evidence(evidence()), past),
        (Record::Certificate(certificate(None, true)), past),
        (Record::Evidence(evidence()), Duration::MAX),
        (Record::Certificate(certificate(None, true)), Duration::MAX),
        (
            Record::Certificate(certificate(None, true)),
            Duration::from_secs(u64::MAX),
        ),
    ] {
        let record = with_age(base, age);
        let result = no_panic(&format!("put with age {age:?}"), || ledger.put(&record));
        assert!(result.is_err(), "{age:?} was accepted");
    }
    assert_eq!(store.len(), stored, "refused writes wrote nothing");
}

#[test]
fn every_age_round_trips_exactly() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    // xorshift64 with a fixed seed: the same ages on every run.
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    // From zero to 500 years. 97 and 100 days straddle 2^23 seconds, where
    // the earlier f64-seconds encoding began to lose nanoseconds. ledger.rs
    // pins hand-picked ages; this sweeps 3,600 on both records that carry
    // provenance.
    for days in [0_u64, 1, 30, 97, 100, 365, 730, 36_500, 182_500] {
        // Nanosecond, microsecond, millisecond, and whole-second precision.
        for step in [1_u32, 1_000, 1_000_000, 1_000_000_000] {
            for i in 0..100 {
                let secs = days * 86_400 + next() % 86_400;
                let nanos = u32::try_from(next() % 1_000_000_000).unwrap() / step * step;
                let age = Duration::new(secs, nanos);
                let base = if i % 2 == 0 {
                    Record::Evidence(evidence())
                } else {
                    Record::Certificate(certificate(None, true))
                };
                let record = with_age(base, age);
                let h = ledger
                    .put(&record)
                    .unwrap_or_else(|e| panic!("{age:?} was refused: {e}"));
                let back = ledger.get(&h).unwrap();
                assert_eq!(back, record, "{age:?} read back changed");
                assert_eq!(back.hash().unwrap(), h, "{age:?} drifted from its name");
            }
        }
    }
}

#[test]
fn a_stored_age_reads_back_exactly_or_fails_without_panicking() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let f = valid_chain(&ledger);
    // A stored age must be a u64 count of nanoseconds; anything else is a
    // damaged or foreign record. ledger.rs covers -1.0 through get and
    // put_receipt; this adds explain, both ends of the range, and the other
    // shapes. Each case names the age it must read back as, if any.
    let cases: [(Value, Option<u64>); 11] = [
        (json!(0), Some(0)),
        (json!(90_000_000_000_u64), Some(90_000_000_000)),
        (json!(u64::MAX), Some(u64::MAX)),
        (json!(-1.0), None),
        (json!(-1), None),
        (json!(-0.0), None),
        (json!(1.5), None),
        (json!(1e300), None),
        // 2^64, one past the largest u64.
        (serde_json::from_str("18446744073709551616").unwrap(), None),
        (json!("90"), None),
        (Value::Null, None),
    ];
    for (age, expected) in cases {
        let mut e = serde_json::to_value(Record::Evidence(evidence())).unwrap();
        e["provenance"]["oldest_input_age"] = age.clone();
        let eh = plant_json(&store, &e);
        let mut c =
            serde_json::to_value(Record::Certificate(certificate(Some(f.proposal), true))).unwrap();
        c["provenance"]["oldest_input_age"] = age.clone();
        let ch = plant_json(&store, &c);

        let got = no_panic(&format!("get, age {age}"), || ledger.get(&eh));
        let mut r = f.receipt.clone();
        r.certificate = ch;
        let receipt = no_panic(&format!("put_receipt, age {age}"), || {
            ledger.put_receipt(&r, AFTER)
        });
        let mut planted = f.receipt.clone();
        planted.evidence = vec![eh];
        let ph = plant(&store, &Record::Receipt(planted));
        let chain = no_panic(&format!("explain, age {age}"), || ledger.explain(&ph))
            .unwrap_or_else(|e| panic!("explain failed on a bad link, age {age}: {e}"));
        no_panic(&format!("list, age {age}"), || {
            ledger.list(RecordKind::Evidence).unwrap().count()
        });

        if let Some(nanos) = expected {
            let Ok(Record::Evidence(back)) = &got else {
                panic!("age {age} did not read back: {got:?}");
            };
            assert_eq!(
                back.provenance.oldest_input_age,
                Duration::from_nanos(nanos)
            );
            assert!(receipt.is_ok(), "age {age}: {receipt:?}");
            assert_eq!(chain.links[0].status, LinkStatus::Ok, "{}", chain.render());
        } else {
            assert!(got.is_err(), "get accepted age {age}");
            assert!(receipt.is_err(), "put_receipt accepted age {age}");
            assert!(
                matches!(chain.links[0].status, LinkStatus::Unreadable { .. }),
                "age {age}: {}",
                chain.render()
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Names
// ---------------------------------------------------------------------------

#[test]
fn a_record_under_a_non_canonical_name_is_invisible_to_every_read() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let rec = Record::Proposal(proposal(vec![], "agent"));
    let raw = serde_json::to_vec(&rec).unwrap();
    let h = Hash::of(&raw);
    let hex = h.to_hex();
    // ledger.rs covers an all-uppercase name for parse_record_path and
    // list(). One uppercase letter is enough to make a name non-canonical.
    let first_letter = hex.find(|c: char| c.is_ascii_alphabetic()).unwrap();
    let mut mixed = hex.clone();
    mixed.replace_range(
        first_letter..=first_letter,
        &hex[first_letter..=first_letter].to_ascii_uppercase(),
    );
    for name in [hex.to_ascii_uppercase(), mixed] {
        store.raw_put(&format!("ledger/{name}.rec"), raw.clone());
    }

    for kind in ALL_KINDS {
        assert_eq!(ledger.list(kind).unwrap().count(), 0, "{kind}");
    }
    assert_eq!(ledger.list_unreadable().unwrap().count(), 0);
    assert!(matches!(ledger.get(&h), Err(Error::NotFound(x)) if x == h));
    assert!(matches!(ledger.kind(&h), Err(Error::NotFound(x)) if x == h));
    // Writing the record for real stores it under its canonical name.
    assert_eq!(ledger.put(&rec).unwrap(), h);
    assert_eq!(ledger.list(RecordKind::Proposal).unwrap().count(), 1);
}
