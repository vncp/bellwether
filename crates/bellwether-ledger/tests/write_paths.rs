//! The only public write paths are `put`, `put_receipt`, and `sign`.
//!
//! The ledger does not hand out its store (a `compile_fail` doctest in the
//! crate root pins that). This test wraps the store in one that counts every
//! mutating call, exercises every public read API and every refused write,
//! and asserts that only successful `put`/`put_receipt`/`sign` calls wrote,
//! that verification writes nothing, and that nothing ever deleted.

#![allow(clippy::unwrap_used)]

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};

use bellwether_ledger::{Ledger, RecordKind, SigningKey, TrustedKey, TrustedKeys};
use bellwether_types::{Hash, Record, Result, Store, StoreVersion};
use bytes::Bytes;
use common::{proposal, valid_chain, MemStore, AFTER, BEFORE};

const T: &str = "2026-10-25T12:00:00.000Z";

#[derive(Default)]
struct Counting {
    inner: MemStore,
    writes: AtomicUsize,
    deletes: AtomicUsize,
}

impl Store for Counting {
    fn get(&self, path: &str) -> Result<(Bytes, StoreVersion)> {
        self.inner.get(path)
    }
    fn put_if_absent(&self, path: &str, body: Bytes) -> Result<StoreVersion> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.put_if_absent(path, body)
    }
    fn put_if_version(
        &self,
        path: &str,
        body: Bytes,
        expected: &StoreVersion,
    ) -> Result<StoreVersion> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.put_if_version(path, body, expected)
    }
    fn put_content_addressed(&self, path: &str, body: Bytes) -> Result<()> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.put_content_addressed(path, body)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
    fn delete(&self, path: &str) -> Result<()> {
        self.deletes.fetch_add(1, Ordering::SeqCst);
        self.inner.delete(path)
    }
}

#[test]
fn only_put_and_put_receipt_write_and_nothing_deletes() {
    let store = Counting::default();
    let ledger = Ledger::new(&store);
    let writes = || store.writes.load(Ordering::SeqCst);

    let f = valid_chain(&ledger);
    assert_eq!(writes(), 4, "evidence, proposal, replay, certificate");
    let receipt = ledger.put_receipt(&f.receipt, AFTER).unwrap();
    assert_eq!(writes(), 5);

    // Every read API.
    for h in [f.evidence, f.proposal, f.replay, f.certificate, receipt] {
        ledger.get(&h).unwrap();
        ledger.get_raw(&h).unwrap();
        ledger.kind(&h).unwrap();
    }
    let _ = ledger.get(&Hash::of(b"absent"));
    for k in [
        RecordKind::Evidence,
        RecordKind::Proposal,
        RecordKind::Replay,
        RecordKind::Certificate,
        RecordKind::Receipt,
    ] {
        let _: Vec<_> = ledger.list(k).unwrap().collect();
    }
    let _: Vec<_> = ledger.list_unreadable().unwrap().collect();
    ledger.explain(&receipt).unwrap();
    assert_eq!(writes(), 5, "reads never write");

    // Refused writes write nothing.
    let refused = [
        ledger.put(&Record::Receipt(f.receipt.clone())),
        // The current sequence is behind manifest_after.
        ledger.put_receipt(&f.receipt, BEFORE),
        ledger.put_receipt(&f.receipt, "not a sequence"),
        ledger.put(&Record::Proposal({
            let mut p = proposal(vec![], "agent");
            p.created_at = "not a time".into();
            p
        })),
    ];
    assert!(refused.iter().all(Result::is_err), "{refused:?}");
    assert_eq!(writes(), 5, "refused writes never write");

    assert_eq!(store.deletes.load(Ordering::SeqCst), 0);
}

#[test]
fn sign_writes_one_object_and_verification_writes_nothing() {
    let store = Counting::default();
    let ledger = Ledger::new(&store);
    let writes = || store.writes.load(Ordering::SeqCst);
    let f = valid_chain(&ledger);
    let receipt = ledger.put_receipt(&f.receipt, AFTER).unwrap();
    let key = SigningKey::from_seed(&[1; 32]);
    let before = writes();
    ledger.sign(&receipt, &key, T).unwrap();
    ledger.sign(&f.certificate, &key, T).unwrap();
    assert_eq!(writes(), before + 2, "one object per signature");

    let mut trusted = TrustedKeys::new();
    trusted
        .insert(TrustedKey {
            name: "ci".into(),
            public_key: key.public_key(),
            valid_from: None,
            revoked_at: None,
        })
        .unwrap();
    let signed = writes();
    for h in [receipt, f.certificate] {
        ledger.signatures(&h).unwrap();
        ledger.verify_against(&h, &trusted).unwrap();
        ledger.verify_against(&h, &TrustedKeys::new()).unwrap();
    }
    ledger.explain(&receipt).unwrap();
    ledger.explain_verified(&receipt, &trusted).unwrap();
    let _ = ledger.signatures(&Hash::of(b"absent"));
    assert_eq!(writes(), signed, "verification never writes");

    // Refused signs write nothing: a proposal, an absent hash, a bad time.
    let _ = ledger.sign(&f.proposal, &key, T);
    let _ = ledger.sign(&Hash::of(b"absent"), &key, T);
    let _ = ledger.sign(&receipt, &key, "yesterday");
    assert_eq!(writes(), signed);
    assert_eq!(store.deletes.load(Ordering::SeqCst), 0);
}
