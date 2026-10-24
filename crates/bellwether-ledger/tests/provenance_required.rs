//! Invariant 6: Evidence and Certificates carry provenance. The ledger rejects
//! one without.

#![allow(clippy::unwrap_used)]

mod common;

use bellwether_ledger::{Ledger, RecordKind};
use bellwether_types::Record;
use common::{assert_rule, certificate, evidence, MemStore};

#[test]
fn evidence_without_inputs_is_rejected() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let mut e = evidence();
    e.provenance.inputs.clear();
    assert_rule(ledger.put(&Record::Evidence(e)), "no inputs");
    assert_eq!(store.len(), 0);
}

#[test]
fn evidence_with_non_rfc3339_created_at_is_rejected() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    for bad in [
        "",
        "yesterday",
        "2026-10-24",
        "2026-10-24 00:00:00",
        "1729728000",
    ] {
        let mut e = evidence();
        e.provenance.created_at = bad.into();
        assert_rule(ledger.put(&Record::Evidence(e)), "RFC 3339");
    }
    assert_eq!(store.len(), 0);
}

#[test]
fn evidence_with_provenance_is_accepted() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let e = evidence();
    let h = ledger.put(&Record::Evidence(e.clone())).unwrap();
    let back = ledger.get(&h).unwrap();
    let Record::Evidence(back) = back else {
        panic!("expected evidence")
    };
    assert_eq!(back.provenance, e.provenance);
    assert_eq!(back.provenance.inputs.len(), 1);
    assert_eq!(ledger.kind(&h).unwrap(), RecordKind::Evidence);
}

#[test]
fn evidence_may_omit_the_manifest_version() {
    // A profile of an unindexed folder has no manifest yet.
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let mut e = evidence();
    e.provenance.manifest_version = None;
    ledger.put(&Record::Evidence(e)).unwrap();
}

#[test]
fn certificate_provenance_is_required() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);

    let mut c = certificate(None, true);
    c.provenance.inputs.clear();
    assert_rule(ledger.put(&Record::Certificate(c)), "no inputs");

    let mut c = certificate(None, true);
    c.provenance.created_at = "yesterday".into();
    assert_rule(ledger.put(&Record::Certificate(c)), "RFC 3339");

    // A certificate is always computed against a manifest.
    let mut c = certificate(None, true);
    c.provenance.manifest_version = None;
    assert_rule(
        ledger.put(&Record::Certificate(c)),
        "names no manifest version",
    );

    assert_eq!(store.len(), 0);
    ledger
        .put(&Record::Certificate(certificate(None, true)))
        .unwrap();
}
