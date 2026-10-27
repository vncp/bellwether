//! Detached signatures (ADR 029): signing a valid chain, every verdict, the
//! refusals at `sign`, and an every-byte corruption sweep over a signature
//! object.
//!
//! Deliberate breaks each test here catches:
//! - the domain tag dropped from the message: `message_matches_an_independent_oracle`,
//!   `a_signature_without_the_domain_tag_is_invalid`;
//! - the record hash left out of the message: `a_moved_signature_is_invalid`;
//! - the embedded public key trusted on its own:
//!   `a_valid_signature_by_an_unlisted_key_is_untrusted`;
//! - `signed_at` outside the key's window accepted:
//!   `a_revoked_or_not_yet_valid_key_is_untrusted_with_its_window`.

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

mod common;

use bellwether_ledger::{
    record_path, signature_message, InvalidReason, Ledger, LinkStatus, RecordKind, SignaturePath,
    SignatureStatus, SigningKey, TrustError, TrustedKey, TrustedKeys, Verdict, SIGNATURE_DOMAIN,
};
use bellwether_types::{Error, Hash, Receipt, Record, Store, StoreVersion};
use common::{assert_rule, certificate, valid_chain, Fixture, MemStore, AFTER, BEFORE};
use ed25519_dalek::Signer as _;
use serde::Serialize;

const T: &str = "2026-10-25T12:00:00.000Z";

fn key(n: u8) -> SigningKey {
    SigningKey::from_seed(&[n; 32])
}

fn trust(key: &SigningKey, name: &str) -> TrustedKey {
    TrustedKey {
        name: name.into(),
        public_key: key.public_key(),
        valid_from: None,
        revoked_at: None,
    }
}

fn trusting(keys: &[(&SigningKey, &str)]) -> TrustedKeys {
    let mut t = TrustedKeys::new();
    for (k, n) in keys {
        t.insert(trust(k, n)).unwrap();
    }
    t
}

/// The stored object's fields in written order, to forge variants that are
/// still canonical JSON.
#[derive(Serialize, serde::Deserialize, Clone)]
struct Object {
    signature_version: u32,
    record: String,
    kind: String,
    alg: String,
    key_id: String,
    public_key: String,
    signed_at: String,
    sig: String,
}

fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    b.iter().fold(String::new(), |mut s, x| {
        let _ = write!(s, "{x:02x}");
        s
    })
}

/// A stored valid chain with its receipt written.
fn chain(store: &MemStore) -> (Fixture, Hash) {
    let ledger = Ledger::new(store);
    let f = valid_chain(&ledger);
    let receipt = ledger.put_receipt(&f.receipt, AFTER).unwrap();
    (f, receipt)
}

/// A second, distinct receipt over the same chain.
fn second_receipt(store: &MemStore, f: &Fixture) -> Hash {
    let mut r = f.receipt.clone();
    r.created_at = "2026-10-24T00:05:00Z".into();
    Ledger::new(store).put_receipt(&r, AFTER).unwrap()
}

fn object_at(store: &MemStore, at: &SignaturePath) -> Object {
    serde_json::from_slice(&store.get(&at.path()).unwrap().0).unwrap()
}

fn put_object(store: &MemStore, at: &SignaturePath, o: &Object) {
    store.raw_put(&at.path(), serde_json::to_vec(o).unwrap());
}

fn only_status(ledger: &Ledger<&MemStore>, h: &Hash) -> SignatureStatus {
    let mut s = ledger.signatures(h).unwrap();
    assert_eq!(s.len(), 1, "{s:?}");
    s.remove(0)
}

fn invalid_reason(s: SignatureStatus) -> InvalidReason {
    match s {
        SignatureStatus::Invalid { reason, .. } => reason,
        other @ SignatureStatus::Valid { .. } => panic!("expected Invalid, got {other:?}"),
    }
}

#[test]
fn a_valid_chain_signed_verifies_as_signed() {
    let store = MemStore::default();
    let (f, receipt) = chain(&store);
    let ledger = Ledger::new(&store);
    let k = key(1);
    let at_r = ledger.sign(&receipt, &k, T).unwrap();
    let at_c = ledger.sign(&f.certificate, &k, T).unwrap();
    assert_eq!(
        at_r.path(),
        format!(
            "ledger/{}.sig.{}.json",
            receipt.to_hex(),
            k.key_id().to_hex()
        )
    );
    assert_eq!(at_c.record, f.certificate);

    // The object is exactly the documented canonical JSON.
    let bytes = store.get(&at_r.path()).unwrap().0;
    let o: Object = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(serde_json::to_vec(&o).unwrap(), bytes.to_vec());
    assert_eq!(o.signature_version, 1);
    assert_eq!(o.record, receipt.to_hex());
    assert_eq!(o.kind, "receipt");
    assert_eq!(o.alg, "ed25519");
    assert_eq!(o.key_id, Hash::of(k.public_key().as_bytes()).to_hex());
    assert_eq!(o.public_key, k.public_key().to_hex());
    assert_eq!(o.signed_at, T);
    assert_eq!(o.sig.len(), 128);

    let t = trusting(&[(&k, "ci")]);
    for h in [receipt, f.certificate] {
        assert_eq!(
            ledger.signatures(&h).unwrap(),
            vec![SignatureStatus::Valid {
                key_id: k.key_id(),
                public_key: k.public_key(),
                signed_at: T.into(),
            }]
        );
        assert_eq!(
            ledger.verify_against(&h, &t).unwrap(),
            Verdict::Signed {
                by: "ci".into(),
                key_id: k.key_id(),
                signed_at: T.into(),
            }
        );
    }

    // explain: sig= on the receipt and certificate lines only, and only
    // against a trust set.
    let text = ledger.explain_verified(&receipt, &t).unwrap().render();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 5, "{text}");
    for (i, l) in lines.iter().enumerate() {
        let signed_line = l.contains(" certificate ") || l.contains(" receipt ");
        assert_eq!(l.contains("sig=valid(ci)"), signed_line, "line {i}: {l}");
    }
    let plain = ledger.explain(&receipt).unwrap();
    assert!(!plain.render().contains("sig="), "{}", plain.render());
    // The chain serializes the signatures always, the verdict only against
    // a trust set.
    let json = serde_json::to_value(&plain).unwrap();
    let last = &json["links"][4];
    assert_eq!(last["signatures"][0]["status"], "valid");
    assert!(last.get("verdict").is_none());
    let json = serde_json::to_value(ledger.explain_verified(&receipt, &t).unwrap()).unwrap();
    assert_eq!(json["links"][4]["verdict"]["verdict"], "signed");
    assert_eq!(json["links"][3]["verdict"]["by"], "ci");
    assert!(json["links"][2].get("signatures").is_none());

    // Signature objects are not records: listing and the unreadable list
    // ignore them.
    assert_eq!(ledger.list(RecordKind::Receipt).unwrap().count(), 1);
    assert_eq!(ledger.list_unreadable().unwrap().count(), 0);
}

#[test]
fn message_matches_an_independent_oracle() {
    let record = Hash::of(b"some record");
    let mut expected = b"bellwether-signature-v1\0".to_vec();
    expected.extend_from_slice(b"receipt");
    expected.push(0);
    expected.extend_from_slice(&record.0);
    expected.extend_from_slice(T.as_bytes());
    assert_eq!(signature_message(RecordKind::Receipt, &record, T), expected);
    assert_eq!(SIGNATURE_DOMAIN, b"bellwether-signature-v1\0");

    // The stored sig is ed25519 over exactly that message, computed here
    // with the library directly.
    let store = MemStore::default();
    let (_, receipt) = chain(&store);
    let ledger = Ledger::new(&store);
    let at = ledger.sign(&receipt, &key(3), T).unwrap();
    let mut message = b"bellwether-signature-v1\0".to_vec();
    message.extend_from_slice(b"receipt\0");
    message.extend_from_slice(&receipt.0);
    message.extend_from_slice(T.as_bytes());
    let oracle = ed25519_dalek::SigningKey::from_bytes(&[3; 32]).sign(&message);
    assert_eq!(object_at(&store, &at).sig, hex(&oracle.to_bytes()));
}

#[test]
fn a_signature_without_the_domain_tag_is_invalid() {
    let store = MemStore::default();
    let (_, receipt) = chain(&store);
    let ledger = Ledger::new(&store);
    let at = ledger.sign(&receipt, &key(1), T).unwrap();
    // Another protocol signing the same fields without the tag.
    let mut bare = b"receipt\0".to_vec();
    bare.extend_from_slice(&receipt.0);
    bare.extend_from_slice(T.as_bytes());
    let mut o = object_at(&store, &at);
    o.sig = hex(&ed25519_dalek::SigningKey::from_bytes(&[1; 32])
        .sign(&bare)
        .to_bytes());
    put_object(&store, &at, &o);
    assert_eq!(
        invalid_reason(only_status(&ledger, &receipt)),
        InvalidReason::BadSignature
    );
}

#[test]
fn altered_record_bytes_are_unreadable_whatever_the_signature() {
    let store = MemStore::default();
    let (f, receipt) = chain(&store);
    let ledger = Ledger::new(&store);
    let k = key(1);
    ledger.sign(&receipt, &k, T).unwrap();
    ledger.sign(&f.certificate, &k, T).unwrap();
    let t = trusting(&[(&k, "ci")]);

    // The certificate's bytes altered: its link is UNREADABLE, with no
    // signature status, and the hash mismatch is the error for it.
    let path = record_path(&f.certificate);
    let mut bytes = store.get(&path).unwrap().0.to_vec();
    let at = bytes.iter().position(|b| *b == b'4').unwrap();
    bytes[at] = b'5';
    store.raw_put(&path, bytes);
    assert_rule(ledger.signatures(&f.certificate), "does not match its name");
    assert_rule(
        ledger.verify_against(&f.certificate, &t),
        "does not match its name",
    );
    let chain = ledger.explain_verified(&receipt, &t).unwrap();
    let cert = &chain.links[3];
    assert!(
        matches!(cert.status, LinkStatus::Unreadable { .. }),
        "{chain:?}"
    );
    assert!(cert.verdict.is_none() && cert.signatures.is_none());
    assert!(chain.render().contains("UNREADABLE"));

    // The receipt's own bytes altered: nothing is verified at all.
    let path = record_path(&receipt);
    let mut bytes = store.get(&path).unwrap().0.to_vec();
    let last = bytes.len() - 3;
    bytes[last] ^= 1;
    store.raw_put(&path, bytes);
    assert_rule(
        ledger.verify_against(&receipt, &t),
        "does not match its name",
    );
}

#[test]
fn a_moved_signature_is_invalid() {
    let store = MemStore::default();
    let (f, r1) = chain(&store);
    let r2 = second_receipt(&store, &f);
    let ledger = Ledger::new(&store);
    let k = key(1);
    let at1 = ledger.sign(&r1, &k, T).unwrap();
    let at2 = SignaturePath {
        record: r2,
        key_id: k.key_id(),
    };
    let t = trusting(&[(&k, "ci")]);

    // Copied byte for byte beside another record.
    store.raw_put(&at2.path(), store.get(&at1.path()).unwrap().0);
    assert_eq!(
        invalid_reason(only_status(&ledger, &r2)),
        InvalidReason::RecordMismatch { signed: r1 }
    );
    assert!(matches!(
        ledger.verify_against(&r2, &t).unwrap(),
        Verdict::Invalid { .. }
    ));

    // Copied and rewritten to name the other record: the message binds the
    // hash, so the signature does not verify.
    let mut o = object_at(&store, &at1);
    o.record = r2.to_hex();
    put_object(&store, &at2, &o);
    assert_eq!(
        invalid_reason(only_status(&ledger, &r2)),
        InvalidReason::BadSignature
    );
    // The original is untouched and still signed.
    assert!(ledger.verify_against(&r1, &t).unwrap().is_signed());
}

#[test]
fn an_edited_kind_signed_at_or_sig_is_invalid() {
    let store = MemStore::default();
    let (_, receipt) = chain(&store);
    let ledger = Ledger::new(&store);
    let at = ledger.sign(&receipt, &key(1), T).unwrap();
    let original = object_at(&store, &at);

    let mut o = original.clone();
    o.kind = "certificate".into();
    put_object(&store, &at, &o);
    assert_eq!(
        invalid_reason(only_status(&ledger, &receipt)),
        InvalidReason::KindMismatch {
            signed: "certificate".into(),
            stored: RecordKind::Receipt
        }
    );

    let mut o = original.clone();
    o.signed_at = "2026-10-25T12:00:00.001Z".into();
    put_object(&store, &at, &o);
    assert_eq!(
        invalid_reason(only_status(&ledger, &receipt)),
        InvalidReason::BadSignature
    );

    for bit in [0u8, 3, 7] {
        let mut sig = ed25519_dalek_bytes(&original.sig);
        sig[17] ^= 1 << bit;
        let mut o = original.clone();
        o.sig = hex(&sig);
        put_object(&store, &at, &o);
        assert_eq!(
            invalid_reason(only_status(&ledger, &receipt)),
            InvalidReason::BadSignature,
            "bit {bit}"
        );
    }

    // Another key's public key under this key's name.
    let mut o = original.clone();
    o.public_key = key(2).public_key().to_hex();
    put_object(&store, &at, &o);
    assert_eq!(
        invalid_reason(only_status(&ledger, &receipt)),
        InvalidReason::KeyIdMismatch
    );

    let mut o = original;
    o.alg = "rsa".into();
    put_object(&store, &at, &o);
    assert_eq!(
        invalid_reason(only_status(&ledger, &receipt)),
        InvalidReason::UnknownAlgorithm { alg: "rsa".into() }
    );
}

fn ed25519_dalek_bytes(h: &str) -> Vec<u8> {
    (0..h.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&h[i..i + 2], 16).unwrap())
        .collect()
}

#[test]
fn a_valid_signature_by_an_unlisted_key_is_untrusted() {
    let store = MemStore::default();
    let (_, receipt) = chain(&store);
    let ledger = Ledger::new(&store);
    let (a, b) = (key(1), key(2));
    ledger.sign(&receipt, &b, T).unwrap();

    // The object's own public key proves only self-consistency: with no
    // trust set, or one that lists another key, it is not Signed.
    for t in [TrustedKeys::new(), trusting(&[(&a, "ci")])] {
        assert_eq!(
            ledger.verify_against(&receipt, &t).unwrap(),
            Verdict::Untrusted {
                key_ids: vec![b.key_id()],
                outside_window: vec![],
            }
        );
    }
    let text = ledger
        .explain_verified(&receipt, &trusting(&[(&a, "ci")]))
        .unwrap()
        .render();
    assert!(
        text.contains(&format!("sig=untrusted({})", &b.key_id().to_hex()[..12])),
        "{text}"
    );

    // Signed by both: the trusted one decides.
    ledger.sign(&receipt, &a, T).unwrap();
    assert_eq!(ledger.signatures(&receipt).unwrap().len(), 2);
    assert_eq!(
        ledger
            .verify_against(&receipt, &trusting(&[(&a, "ci")]))
            .unwrap(),
        Verdict::Signed {
            by: "ci".into(),
            key_id: a.key_id(),
            signed_at: T.into(),
        }
    );
}

#[test]
fn a_revoked_or_not_yet_valid_key_is_untrusted_with_its_window() {
    let store = MemStore::default();
    let (_, receipt) = chain(&store);
    let ledger = Ledger::new(&store);
    let k = key(1);
    ledger.sign(&receipt, &k, T).unwrap();
    let windowed = |from: Option<&str>, rev: Option<&str>| {
        let mut t = TrustedKeys::new();
        t.insert(TrustedKey {
            valid_from: from.map(Into::into),
            revoked_at: rev.map(Into::into),
            ..trust(&k, "ci")
        })
        .unwrap();
        t
    };
    for (from, rev) in [
        (None, Some("2026-10-25T11:59:59.999Z")),
        (None, Some(T)), // revoked at the very instant: does not count
        (Some("2026-10-25T12:00:00.001Z"), None),
        (
            Some("2026-01-01T00:00:00.000Z"),
            Some("2026-06-01T00:00:00.000Z"),
        ),
    ] {
        let v = ledger
            .verify_against(&receipt, &windowed(from, rev))
            .unwrap();
        let Verdict::Untrusted {
            key_ids,
            outside_window,
        } = &v
        else {
            panic!("{from:?}..{rev:?}: expected Untrusted, got {v:?}");
        };
        assert_eq!(key_ids, &vec![k.key_id()]);
        assert_eq!(outside_window.len(), 1);
        assert_eq!(outside_window[0].valid_from.as_deref(), from);
        assert_eq!(outside_window[0].revoked_at.as_deref(), rev);
        assert_eq!(outside_window[0].signed_at, T);
        let shown = v.to_string();
        assert!(shown.contains("outside its window"), "{shown}");
    }
    // Inside the window, bounds included at the start.
    for (from, rev) in [
        (Some(T), None),
        (None, Some("2026-10-25T12:00:00.001Z")),
        (
            Some("2026-01-01T00:00:00.000Z"),
            Some("2027-01-01T00:00:00.000Z"),
        ),
    ] {
        assert!(
            ledger
                .verify_against(&receipt, &windowed(from, rev))
                .unwrap()
                .is_signed(),
            "{from:?}..{rev:?}"
        );
    }
}

#[test]
fn an_unsigned_receipt_is_unsigned() {
    let store = MemStore::default();
    let (f, receipt) = chain(&store);
    let ledger = Ledger::new(&store);
    let t = trusting(&[(&key(1), "ci")]);
    assert_eq!(ledger.signatures(&receipt).unwrap(), vec![]);
    assert_eq!(
        ledger.verify_against(&receipt, &t).unwrap(),
        Verdict::Unsigned
    );
    assert_eq!(
        ledger.verify_against(&f.certificate, &t).unwrap(),
        Verdict::Unsigned
    );
    let text = ledger.explain_verified(&receipt, &t).unwrap().render();
    assert_eq!(text.matches("sig=none").count(), 2, "{text}");
}

#[test]
fn a_newer_signature_version_is_invalid_not_parsed() {
    let store = MemStore::default();
    let (_, receipt) = chain(&store);
    let ledger = Ledger::new(&store);
    let k = key(1);
    let at = ledger.sign(&receipt, &k, T).unwrap();
    let mut o = object_at(&store, &at);
    o.signature_version = 2;
    put_object(&store, &at, &o);
    assert_eq!(
        invalid_reason(only_status(&ledger, &receipt)),
        InvalidReason::Newer {
            found: 2,
            supported: 1
        }
    );
    // Even with fields this build has never heard of.
    store.raw_put(
        &at.path(),
        br#"{"signature_version":2,"chain":["x"],"sig":"?"}"#.to_vec(),
    );
    let reason = invalid_reason(only_status(&ledger, &receipt));
    assert!(matches!(reason, InvalidReason::Newer { found: 2, .. }));
    assert!(reason.to_string().contains("upgrade"), "{reason}");
}

#[test]
fn an_invalid_signature_beside_a_trusted_one_is_reported() {
    let store = MemStore::default();
    let (_, receipt) = chain(&store);
    let ledger = Ledger::new(&store);
    let (a, b) = (key(1), key(2));
    ledger.sign(&receipt, &a, T).unwrap();
    let at_b = ledger.sign(&receipt, &b, T).unwrap();
    let mut o = object_at(&store, &at_b);
    o.signed_at = "2026-10-25T12:00:01.000Z".into();
    put_object(&store, &at_b, &o);
    let v = ledger
        .verify_against(&receipt, &trusting(&[(&a, "ci")]))
        .unwrap();
    let Verdict::Invalid { invalid, signed } = &v else {
        panic!("expected Invalid, got {v:?}");
    };
    assert_eq!(invalid.len(), 1);
    assert_eq!(invalid[0].key_id, b.key_id());
    assert_eq!(signed.as_ref().map(|s| s.by.as_str()), Some("ci"));
    assert_eq!(v.token(), "INVALID");
}

#[test]
fn sign_refuses_anything_but_a_stored_receipt_or_certificate() {
    let store = MemStore::default();
    let (f, _) = chain(&store);
    let ledger = Ledger::new(&store);
    let k = key(1);
    for (h, kind) in [
        (f.proposal, "a proposal"),
        (f.evidence, "an evidence"),
        (f.replay, "a replay"),
    ] {
        assert_rule(ledger.sign(&h, &k, T), kind);
    }
    assert_rule(ledger.sign(&Hash::of(b"absent"), &k, T), "no such record");
    // A failing certificate is a legitimate record, and signable; this one
    // is valid under put's rules.
    let failing = ledger
        .put(&Record::Certificate(certificate(Some(f.proposal), false)))
        .unwrap();
    ledger.sign(&failing, &k, T).unwrap();
    // signed_at in any other spelling.
    for t in [
        "2026-10-25T12:00:00Z",
        "2026-10-25T12:00:00.000+00:00",
        "now",
    ] {
        assert_rule(ledger.sign(&f.certificate, &k, t), "milliseconds");
    }
    // Nothing was written for any refusal: only the one signature exists.
    let sigs = store
        .list("ledger/")
        .unwrap()
        .into_iter()
        .filter(|p| p.contains(".sig."))
        .count();
    assert_eq!(sigs, 1);
}

/// Writes `record` straight into the store, as an attacker with store write
/// access could, bypassing every write-time rule.
fn plant(store: &MemStore, record: &Record) -> Hash {
    let bytes = serde_json::to_vec(record).unwrap();
    let h = Hash::of(&bytes);
    store.raw_put(&record_path(&h), bytes);
    h
}

#[test]
fn sign_refuses_a_planted_chain_that_put_receipt_would_refuse() {
    let store = MemStore::default();
    let (f, _) = chain(&store);
    let ledger = Ledger::new(&store);
    let k = key(1);

    // A Key receipt that leaves the manifest unchanged (rule 18).
    let mut r: Receipt = f.receipt.clone();
    r.manifest_after = BEFORE.into();
    let planted = plant(&store, &Record::Receipt(r));
    assert_rule(
        ledger.sign(&planted, &k, T),
        "not greater than manifest_before",
    );

    // A receipt over a failing certificate (rule 12).
    let failing = ledger
        .put(&Record::Certificate(certificate(Some(f.proposal), false)))
        .unwrap();
    let mut r = f.receipt.clone();
    r.certificate = failing;
    let planted = plant(&store, &Record::Receipt(r));
    assert_rule(ledger.sign(&planted, &k, T), "did not pass");

    // A certificate that says pass but reports differences (rule 9).
    let mut c = certificate(Some(f.proposal), true);
    c.body.mismatched = 3;
    let planted_cert = plant(&store, &Record::Certificate(c));
    assert_rule(ledger.sign(&planted_cert, &k, T), "says pass but reports");

    let sigs = store
        .list("ledger/")
        .unwrap()
        .into_iter()
        .filter(|p| p.contains(".sig."))
        .count();
    assert_eq!(sigs, 0, "a refused sign writes nothing");
}

#[test]
fn signing_again_is_idempotent_only_when_identical() {
    let store = MemStore::default();
    let (_, receipt) = chain(&store);
    let ledger = Ledger::new(&store);
    let k = key(1);
    let at = ledger.sign(&receipt, &k, T).unwrap();
    let first = store.get(&at.path()).unwrap().0;
    assert_eq!(ledger.sign(&receipt, &k, T).unwrap(), at);
    assert_rule(
        ledger.sign(&receipt, &k, "2026-10-25T12:00:00.001Z"),
        "never overwritten",
    );
    assert_eq!(store.get(&at.path()).unwrap().0, first);
}

#[test]
fn trusted_keys_refuse_bad_windows_and_duplicates() {
    let k = key(1);
    let mut t = TrustedKeys::new();
    t.insert(trust(&k, "ci")).unwrap();
    let e = t.insert(trust(&k, "again")).unwrap_err();
    assert!(matches!(e, TrustError::Duplicate { .. }), "{e:?}");
    assert!(e.to_string().contains("trusted twice"), "{e}");
    assert!(!e.to_string().contains("ledger"), "{e}");
    let mut t = TrustedKeys::new();
    let e = t
        .insert(TrustedKey {
            valid_from: Some("2026-10-25".into()),
            ..trust(&k, "ci")
        })
        .unwrap_err();
    assert!(
        matches!(
            e,
            TrustError::NotUtcMillis {
                field: "valid_from",
                ..
            }
        ),
        "{e:?}"
    );
    let e = t
        .insert(TrustedKey {
            valid_from: Some(T.into()),
            revoked_at: Some(T.into()),
            ..trust(&k, "ci")
        })
        .unwrap_err();
    assert!(matches!(e, TrustError::EmptyWindow { .. }), "{e:?}");
    assert!(e.to_string().contains("not before revoked_at"), "{e}");
    assert!(t.is_empty());
}

/// A small-order public key (the identity point) with the signature that
/// verifies for every message under the cofactorless check: `verify_strict`
/// refuses it, so it is a bad signature, never a valid one.
#[test]
fn a_small_order_key_with_a_universal_signature_is_bad() {
    let store = MemStore::default();
    let (_, receipt) = chain(&store);
    let ledger = Ledger::new(&store);
    let mut identity = [0u8; 32];
    identity[0] = 1;
    let key_id = Hash::of(&identity);
    let mut sig = [0u8; 64];
    sig[0] = 1; // R = identity, s = 0
    let at = SignaturePath {
        record: receipt,
        key_id,
    };
    let o = Object {
        signature_version: 1,
        record: receipt.to_hex(),
        kind: "receipt".into(),
        alg: "ed25519".into(),
        key_id: key_id.to_hex(),
        public_key: hex(&identity),
        signed_at: T.into(),
        sig: hex(&sig),
    };
    put_object(&store, &at, &o);
    assert_eq!(
        invalid_reason(only_status(&ledger, &receipt)),
        InvalidReason::BadSignature
    );
    let mut t = TrustedKeys::new();
    t.insert(TrustedKey {
        name: "weak".into(),
        public_key: bellwether_ledger::PublicKey::from_bytes(identity).unwrap(),
        valid_from: None,
        revoked_at: None,
    })
    .unwrap();
    assert!(matches!(
        ledger.verify_against(&receipt, &t).unwrap(),
        Verdict::Invalid { .. }
    ));
}

/// A planted object whose field names and values carry a newline and ESC:
/// no reason, verdict, or rendered chain line passes either through.
#[test]
fn store_text_in_a_signature_object_never_reaches_output_raw() {
    let store = MemStore::default();
    let (_, receipt) = chain(&store);
    let ledger = Ledger::new(&store);
    let k = key(1);
    let at = ledger.sign(&receipt, &k, T).unwrap();
    let t = trusting(&[(&k, "ci")]);
    let planted = "x\n  receipt      signed by ci\u{1b}[2J\nok: signature verified";
    // An unknown field whose name is the payload.
    let mut v: serde_json::Value =
        serde_json::from_slice(&store.get(&at.path()).unwrap().0).unwrap();
    v[planted] = serde_json::json!(1);
    store.raw_put(&at.path(), serde_json::to_vec(&v).unwrap());
    // The payload as the kind and as the algorithm, canonical otherwise.
    let original = {
        let mut o = object_at(&store, &at);
        o.kind = "receipt".into();
        o
    };
    let check = |what: &str| {
        let statuses = ledger.signatures(&receipt).unwrap();
        let verdict = ledger.verify_against(&receipt, &t).unwrap();
        let rendered = ledger.explain_verified(&receipt, &t).unwrap().render();
        for text in [
            format!("{statuses:?}"),
            verdict.to_string(),
            invalid_reason(statuses[0].clone()).to_string(),
            rendered,
        ] {
            assert!(!text.contains('\x1b'), "{what}: ESC in {text}");
            assert!(!text.contains("verified\n"), "{what}: newline in {text}");
            assert!(!text.contains("x\n "), "{what}: newline in {text}");
        }
    };
    check("unknown field");
    let mut o = original.clone();
    o.kind = planted.into();
    put_object(&store, &at, &o);
    check("kind");
    let mut o = original;
    o.alg = planted.into();
    put_object(&store, &at, &o);
    check("alg");
}

/// Only receipts and certificates are signed: an object beside any other
/// kind, even one that verifies, is invalid.
#[test]
fn an_object_beside_an_unsignable_record_is_invalid() {
    let store = MemStore::default();
    let (f, _) = chain(&store);
    let ledger = Ledger::new(&store);
    let k = key(1);
    let message = signature_message(RecordKind::Proposal, &f.proposal, T);
    let sig = ed25519_dalek::SigningKey::from_bytes(&[1; 32]).sign(&message);
    let at = SignaturePath {
        record: f.proposal,
        key_id: k.key_id(),
    };
    put_object(
        &store,
        &at,
        &Object {
            signature_version: 1,
            record: f.proposal.to_hex(),
            kind: "proposal".into(),
            alg: "ed25519".into(),
            key_id: k.key_id().to_hex(),
            public_key: k.public_key().to_hex(),
            signed_at: T.into(),
            sig: hex(&sig.to_bytes()),
        },
    );
    assert_eq!(
        invalid_reason(only_status(&ledger, &f.proposal)),
        InvalidReason::Unsignable {
            stored: RecordKind::Proposal
        }
    );
}

#[test]
fn signing_key_debug_shows_the_key_id_only() {
    let k = key(9);
    let shown = format!("{k:?}");
    assert!(shown.contains(&k.key_id().to_hex()), "{shown}");
    assert!(!shown.contains(&hex(&[9; 32])), "{shown}");
}

#[test]
fn every_byte_corruption_of_a_signature_object_is_invalid_never_a_panic() {
    let store = MemStore::default();
    let (_, receipt) = chain(&store);
    let ledger = Ledger::new(&store);
    let k = key(1);
    let at = ledger.sign(&receipt, &k, T).unwrap();
    let good = store.get(&at.path()).unwrap().0.to_vec();
    let t = trusting(&[(&k, "ci")]);
    for i in 0..good.len() {
        for flip in [0x01u8, 0x20, 0x80] {
            let mut bad = good.clone();
            bad[i] ^= flip;
            store.raw_put(&at.path(), bad);
            let status = only_status(&ledger, &receipt);
            assert!(
                matches!(status, SignatureStatus::Invalid { .. }),
                "byte {i} ^ {flip:#x} still verifies: {status:?}"
            );
            assert!(matches!(
                ledger.verify_against(&receipt, &t).unwrap(),
                Verdict::Invalid { .. }
            ));
        }
    }
    for len in 0..good.len() {
        store.raw_put(&at.path(), good[..len].to_vec());
        assert!(matches!(
            only_status(&ledger, &receipt),
            SignatureStatus::Invalid { .. }
        ));
    }
    store.raw_put(&at.path(), good);
    assert!(ledger.verify_against(&receipt, &t).unwrap().is_signed());
}

#[test]
fn names_that_are_not_canonical_are_not_signature_objects() {
    let store = MemStore::default();
    let (_, receipt) = chain(&store);
    let ledger = Ledger::new(&store);
    let k = key(1);
    let at = ledger.sign(&receipt, &k, T).unwrap();
    let bytes = store.get(&at.path()).unwrap().0;
    // Uppercase hex, a wrong extension, a truncated key id.
    let upper = format!(
        "ledger/{}.sig.{}.json",
        receipt.to_hex(),
        k.key_id().to_hex().to_uppercase()
    );
    store.raw_put(&upper, bytes.clone());
    store.raw_put(&format!("{}.bak", at.path()), bytes.clone());
    store.raw_put(&format!("ledger/{}.sig.abc.json", receipt.to_hex()), bytes);
    assert_eq!(ledger.signatures(&receipt).unwrap().len(), 1);
    assert_eq!(ledger.list(RecordKind::Receipt).unwrap().count(), 1);
    assert_eq!(ledger.list_unreadable().unwrap().count(), 0);
    assert!(matches!(
        ledger.signatures(&Hash::of(b"absent")),
        Err(Error::NotFound(_))
    ));
}

/// Records planted under their own BLAKE3 names, with a newline and ESC in
/// the receipt's `created_at` and in a profile Evidence's `root`: every
/// `explain` line comes out escaped, one line per link, with no ESC byte.
#[test]
fn store_text_in_planted_records_is_escaped_in_explain() {
    let store = MemStore::default();
    let (f, _) = chain(&store);
    let ledger = Ledger::new(&store);
    let payload = "x\n  receipt      signed by ci\u{1b}[2J\nok: signature verified";
    let mut ev = common::evidence();
    if let bellwether_types::EvidenceBody::Profile { root, .. } = &mut ev.body {
        *root = payload.into();
    }
    let ev = plant(&store, &Record::Evidence(ev));
    let mut r = f.receipt.clone();
    r.created_at = payload.into();
    r.evidence = vec![ev];
    let receipt = plant(&store, &Record::Receipt(r));
    let k = key(1);
    let rendered = [
        ledger.explain(&receipt).unwrap().render(),
        ledger
            .explain_verified(&receipt, &trusting(&[(&k, "ci")]))
            .unwrap()
            .render(),
    ];
    for text in rendered {
        assert!(!text.contains('\u{1b}'), "{text}");
        assert_eq!(text.lines().count(), 6, "one line per link:\n{text}");
        assert!(!text.lines().any(|l| l.starts_with("ok: ")), "{text}");
        assert!(text.contains("\\n  receipt"), "{text}");
        assert!(text.contains("\\u{1b}[2J"), "{text}");
    }
    let chain = ledger.explain(&receipt).unwrap();
    let json = serde_json::to_string(&chain).unwrap();
    assert!(!json.contains("\\u001b"), "{json}");
}

/// A store that counts listings (WP22b: one listing per command).
#[derive(Default)]
struct Listings {
    inner: MemStore,
    lists: std::sync::atomic::AtomicUsize,
}

impl Store for Listings {
    fn get(&self, path: &str) -> bellwether_types::Result<(bytes::Bytes, StoreVersion)> {
        self.inner.get(path)
    }
    fn put_if_absent(
        &self,
        path: &str,
        body: bytes::Bytes,
    ) -> bellwether_types::Result<StoreVersion> {
        self.inner.put_if_absent(path, body)
    }
    fn put_if_version(
        &self,
        path: &str,
        body: bytes::Bytes,
        expected: &StoreVersion,
    ) -> bellwether_types::Result<StoreVersion> {
        self.inner.put_if_version(path, body, expected)
    }
    fn put_content_addressed(
        &self,
        path: &str,
        body: bytes::Bytes,
    ) -> bellwether_types::Result<()> {
        self.inner.put_content_addressed(path, body)
    }
    fn list(&self, prefix: &str) -> bellwether_types::Result<Vec<String>> {
        self.lists.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.list(prefix)
    }
    fn delete(&self, path: &str) -> bellwether_types::Result<()> {
        self.inner.delete(path)
    }
}

#[test]
fn explaining_lists_the_ledger_once_and_an_index_serves_every_receipt() {
    // WP22b: `explain` listed the whole ledger once per signed link (twice
    // per receipt), and `ledger receipts` once more per receipt; a command
    // now lists once and reads every signature from that listing.
    let store = Listings::default();
    let ledger = Ledger::new(&store.inner);
    let f = valid_chain(&ledger);
    let receipt = ledger.put_receipt(&f.receipt, AFTER).unwrap();
    let other = second_receipt(&store.inner, &f);
    let (k1, k2) = (key(1), key(2));
    for (h, k) in [(&receipt, &k1), (&receipt, &k2), (&f.certificate, &k1)] {
        ledger.sign(h, k, T).unwrap();
    }
    let t = trusting(&[(&k1, "ci")]);
    let counted = Ledger::new(&store);
    let lists = || store.lists.load(std::sync::atomic::Ordering::SeqCst);

    let plain = counted.explain(&receipt).unwrap();
    assert_eq!(lists(), 1, "explain lists once");
    let verified = counted.explain_verified(&receipt, &t).unwrap();
    assert_eq!(lists(), 2, "explain_verified lists once");

    let index = counted.signature_index().unwrap();
    assert_eq!(lists(), 3);
    assert_eq!(index.of(&receipt).len(), 2);
    assert_eq!(index.of(&f.certificate).len(), 1);
    assert!(index.of(&other).is_empty());
    // Every read from the index is the read with its own listing.
    assert_eq!(counted.explain_in(&receipt, None, &index).unwrap(), plain);
    assert_eq!(
        counted.explain_in(&receipt, Some(&t), &index).unwrap(),
        verified
    );
    for h in [receipt, f.certificate, other] {
        assert_eq!(
            counted.signatures_in(&h, &index).unwrap(),
            ledger.signatures(&h).unwrap()
        );
    }
    counted.explain_in(&other, Some(&t), &index).unwrap();
    assert_eq!(lists(), 3, "reads through an index list nothing");

    // The index is a snapshot of one listing: a signature written after it
    // is read only by a new listing.
    ledger.sign(&other, &k1, T).unwrap();
    assert!(counted.signatures_in(&other, &index).unwrap().is_empty());
    assert_eq!(counted.signatures(&other).unwrap().len(), 1);
    // An index keeps only signature objects, deduplicated per record.
    let path = SignaturePath {
        record: other,
        key_id: k1.key_id(),
    }
    .path();
    let again = bellwether_ledger::SignatureIndex::from_paths([
        path.as_str(),
        path.as_str(),
        "ledger/not-a-signature.json",
    ]);
    assert_eq!(again.of(&other).len(), 1);
}
