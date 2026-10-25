//! Signing in the facade (ADR 029): `fix` signs what it wrote right after
//! the receipt, and nothing else signs; keys and trust files are refused
//! with the fix named.
//!
//! Deliberate break: signing before `put_receipt` returns fails
//! `fix_signs_the_receipt_and_certificate_byte_identically_under_a_fixed_clock`
//! (the ledger refuses to sign a receipt it does not hold).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use bellwether::types::{Error, Hash};
use bellwether::{
    Engine, EngineOptions, FixOutcome, FixRequest, FixResult, KeyFile, RecordKind, SigningKey,
    TrustedKey, TrustedKeys, Verdict, WhyRequest,
};
use common::{ledger_corpus, write_keys};

const CLOCK: &str = "2026-10-25T12:00:00.000Z";
const SEED: [u8; 32] = [42; 32];

fn key() -> SigningKey {
    SigningKey::from_seed(&SEED)
}

fn trusting(k: &SigningKey) -> TrustedKeys {
    let mut t = TrustedKeys::new();
    t.insert(TrustedKey {
        name: "ci".into(),
        public_key: k.public_key(),
        valid_from: None,
        revoked_at: None,
    })
    .unwrap();
    t
}

fn open(data: &Path, signing_key: Option<SigningKey>, trusted: Option<TrustedKeys>) -> Engine {
    Engine::open(
        data.to_str().unwrap(),
        EngineOptions {
            clock: Some(CLOCK.into()),
            signing_key,
            trusted_keys: trusted,
            ..EngineOptions::default()
        },
    )
    .unwrap()
}

/// A corpus under `dir/data`, keys, and the first candidate's proposal and
/// workload from `why`.
fn corpus(dir: &Path) -> (PathBuf, Hash, Hash) {
    let data = dir.join("data");
    ledger_corpus(&data, 3, 4, 25, 400);
    let kf = dir.join("k.parquet");
    write_keys(&kf, "id", &[5, 77, 123]);
    let (p, wl) = why_of(&data, &kf);
    (data, p, wl)
}

fn why_of(data: &Path, kf: &Path) -> (Hash, Hash) {
    let w = open(data, None, None)
        .why(&WhyRequest {
            keys: Some(KeyFile::new(kf)),
            ..WhyRequest::default()
        })
        .unwrap();
    (
        w.candidates[0].proposal.hash,
        w.workload.as_ref().unwrap().hash,
    )
}

fn fix(e: &Engine, proposal: Hash, workload: Hash, verify_only: bool) -> FixResult {
    e.fix(&FixRequest {
        proposal: Some(proposal),
        workload: Some(workload),
        verify_only,
        ..FixRequest::default()
    })
    .unwrap()
}

/// Every signature object under the index, by file name.
fn sig_objects(data: &Path) -> BTreeMap<String, Vec<u8>> {
    let ledger = data.join(".bellwether").join("ledger");
    let Ok(dir) = std::fs::read_dir(&ledger) else {
        return BTreeMap::new();
    };
    dir.map(|e| e.unwrap().path())
        // The local store's lock files (`<path>.lock#0`) are its own.
        .filter(|p| {
            let name = p.to_string_lossy();
            name.contains(".sig.") && name.ends_with(".json")
        })
        .map(|p| {
            (
                p.file_name().unwrap().to_string_lossy().into_owned(),
                std::fs::read(&p).unwrap(),
            )
        })
        .collect()
}

fn receipt_hash(r: &FixResult) -> Hash {
    match &r.outcome {
        FixOutcome::Applied { receipt, .. } => receipt.hash,
        other => panic!("expected Applied, got {other:?}"),
    }
}

#[test]
fn fix_signs_the_receipt_and_certificate_byte_identically_under_a_fixed_clock() {
    let dir = tempfile::tempdir().unwrap();
    // One corpus (its files' fingerprints are part of the records); the
    // index is removed between the two runs.
    let (data, _, _) = corpus(dir.path());
    let kf = dir.path().join("k.parquet");
    let run = || {
        let (p, wl) = why_of(&data, &kf);
        let r = fix(&open(&data, Some(key()), None), p, wl, false);
        let receipt = receipt_hash(&r);
        let cert = r.certificate.as_ref().unwrap().hash;
        assert_eq!(
            r.signatures
                .iter()
                .map(|s| (s.record, s.kind))
                .collect::<Vec<_>>(),
            vec![
                (receipt, RecordKind::Receipt),
                (cert, RecordKind::Certificate)
            ]
        );
        for s in &r.signatures {
            assert_eq!(s.key_id, key().key_id());
            assert_eq!(s.signed_at, CLOCK);
            assert_eq!(
                s.path,
                format!("ledger/{}.sig.{}.json", s.record, key().key_id())
            );
        }
        let objects = sig_objects(&data);
        assert_eq!(objects.len(), 2, "{:?}", objects.keys());

        // Read back against a trust set: both signed.
        let shown = open(&data, None, Some(trusting(&key())))
            .receipt(&receipt)
            .unwrap();
        assert_eq!(shown.signed, Some(true));
        for kind in [RecordKind::Receipt, RecordKind::Certificate] {
            let link = shown.chain.iter().find(|l| l.kind == kind).unwrap();
            assert!(matches!(
                link.verdict,
                Some(Verdict::Signed { ref by, .. }) if by == "ci"
            ));
            assert_eq!(link.signatures.as_ref().unwrap().len(), 1);
        }
        // Without trusted keys: the signatures are listed, no verdict.
        let plain = open(&data, None, None).receipt(&receipt).unwrap();
        assert_eq!(plain.signed, None);
        assert!(plain.chain.iter().all(|l| l.verdict.is_none()));
        std::fs::remove_dir_all(data.join(".bellwether")).unwrap();
        objects
    };
    let first = run();
    let second = run();
    assert_eq!(first, second, "same clock, same key: same signature bytes");
}

#[test]
fn fix_without_a_key_writes_no_signature_and_reads_as_unsigned() {
    let dir = tempfile::tempdir().unwrap();
    let (data, p, wl) = corpus(dir.path());
    let r = fix(&open(&data, None, None), p, wl, false);
    let receipt = receipt_hash(&r);
    assert_eq!(r.signatures, vec![]);
    assert!(sig_objects(&data).is_empty());
    let shown = open(&data, None, Some(trusting(&key())))
        .receipt(&receipt)
        .unwrap();
    assert_eq!(shown.signed, Some(false));
    let link = shown
        .chain
        .iter()
        .find(|l| l.kind == RecordKind::Receipt)
        .unwrap();
    assert_eq!(link.verdict, Some(Verdict::Unsigned));
    assert_eq!(link.signatures, Some(vec![]));
}

#[test]
fn a_key_nobody_trusts_reads_as_untrusted() {
    let dir = tempfile::tempdir().unwrap();
    let (data, p, wl) = corpus(dir.path());
    let receipt = receipt_hash(&fix(&open(&data, Some(key()), None), p, wl, false));
    let other = SigningKey::from_seed(&[7; 32]);
    let shown = open(&data, None, Some(trusting(&other)))
        .receipt(&receipt)
        .unwrap();
    assert_eq!(shown.signed, Some(false));
    let link = shown
        .chain
        .iter()
        .find(|l| l.kind == RecordKind::Receipt)
        .unwrap();
    assert!(matches!(link.verdict, Some(Verdict::Untrusted { .. })));
}

#[test]
fn verify_only_signs_the_certificate_and_the_apply_tier_signs_only_its_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let (data, p, wl) = corpus(dir.path());
    // Verify-only without a key: the certificate stays unsigned.
    let v = fix(&open(&data, None, None), p, wl, true);
    assert!(matches!(v.outcome, FixOutcome::Verified));
    assert_eq!(v.signatures, vec![]);
    let cert = v.certificate.as_ref().unwrap().hash;

    // Applying that certificate with a key signs the receipt only: the
    // certificate was written by another call.
    let e = open(&data, Some(key()), None);
    let a = e
        .fix(&FixRequest {
            certificate: Some(cert),
            ..FixRequest::default()
        })
        .unwrap();
    let receipt = receipt_hash(&a);
    assert_eq!(
        a.signatures
            .iter()
            .map(|s| (s.record, s.kind))
            .collect::<Vec<_>>(),
        vec![(receipt, RecordKind::Receipt)]
    );
    // So the chain is not signed: its certificate carries no signature.
    let shown = open(&data, None, Some(trusting(&key())))
        .receipt(&receipt)
        .unwrap();
    assert_eq!(shown.signed, Some(false));

    // A verify-only run with a key signs its certificate alone.
    let dir2 = tempfile::tempdir().unwrap();
    let (data2, p2, wl2) = corpus(dir2.path());
    let v2 = fix(&open(&data2, Some(key()), None), p2, wl2, true);
    let cert2 = v2.certificate.as_ref().unwrap().hash;
    assert_eq!(
        v2.signatures
            .iter()
            .map(|s| (s.record, s.kind))
            .collect::<Vec<_>>(),
        vec![(cert2, RecordKind::Certificate)]
    );
    assert_eq!(sig_objects(&data2).len(), 1);
}

#[test]
fn nothing_but_fix_signs() {
    let dir = tempfile::tempdir().unwrap();
    let (data, p, wl) = corpus(dir.path());
    let e = open(&data, Some(key()), None);
    let kf = dir.path().join("k.parquet");
    let why = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&kf)),
            ..WhyRequest::default()
        })
        .unwrap();
    let evidence = why.evidence.hash.unwrap();
    let w = e
        .propose(
            bellwether::types::ProposalBody::Key {
                spec: bellwether::spec::parse("exact:batch").unwrap(),
            },
            &[evidence],
            "agent",
        )
        .unwrap();
    assert!(w.proposal().is_some());
    // A verify-only run without a key, then the apply dry run with one.
    let v = fix(&open(&data, None, None), p, wl, true);
    let cert = v.certificate.as_ref().unwrap().hash;
    e.apply_dry_run(&cert).unwrap();
    e.receipts().unwrap();
    e.findings().unwrap();
    e.scorecard().unwrap();
    assert!(
        sig_objects(&data).is_empty(),
        "{:?}",
        sig_objects(&data).keys()
    );
}

#[test]
fn the_signing_key_never_enters_the_store() {
    let dir = tempfile::tempdir().unwrap();
    let (data, p, wl) = corpus(dir.path());
    fix(&open(&data, Some(key()), None), p, wl, false);
    let hex = bellwether::types::Hash(SEED).to_hex();
    let mut stack = vec![data.join(".bellwether")];
    let mut files = 0;
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
                continue;
            }
            files += 1;
            let bytes = std::fs::read(&p).unwrap();
            assert!(
                !bytes.windows(SEED.len()).any(|w| w == SEED),
                "seed bytes in {}",
                p.display()
            );
            assert!(
                !String::from_utf8_lossy(&bytes).contains(&hex),
                "seed hex in {}",
                p.display()
            );
        }
    }
    assert!(files > 2);
}

#[test]
fn report_counts_signed_receipts_against_trusted_keys() {
    let dir = tempfile::tempdir().unwrap();
    let (data, p, wl) = corpus(dir.path());
    fix(&open(&data, Some(key()), None), p, wl, false);
    let out = dir.path().join("report.html");
    let r = open(&data, None, Some(trusting(&key())))
        .report(&out)
        .unwrap();
    assert_eq!(r.signed, Some(1));
    let html = std::fs::read_to_string(&out).unwrap();
    assert!(html.contains("Signatures:"), "{html}");
    assert!(html.contains("signed by ci"), "{html}");
    let r = open(&data, None, None).report(&out).unwrap();
    assert_eq!(r.signed, None);
}

// -- key files --------------------------------------------------------------

fn field(e: &Error) -> &'static str {
    match e {
        Error::InvalidArgument { field, .. } => field,
        other => panic!("expected InvalidArgument, got {other:?}"),
    }
}

#[test]
fn generated_keys_round_trip_and_are_never_overwritten() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("signer");
    let g = bellwether::generate_key(&out, Some("ci"), Some(CLOCK)).unwrap();
    assert_eq!(g.public_path, format!("{}.pub", out.display()));
    assert_eq!(g.public.name, "ci");
    assert_eq!(g.public.created_at, CLOCK);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(&out).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }
    assert_eq!(std::fs::read(&out).unwrap().len(), 32);
    let k = bellwether::load_signing_key(&out).unwrap();
    assert_eq!(k.key_id(), g.public.key_id);
    let p = bellwether::read_public_key(&bellwether::public_path(&out)).unwrap();
    assert_eq!(p, g.public);

    // Never overwritten: the seed, or the .pub beside a fresh name.
    let before = std::fs::read(&out).unwrap();
    let e = bellwether::generate_key(&out, None, None).unwrap_err();
    assert_eq!(field(&e), "out");
    assert!(e.to_string().contains("never overwritten"), "{e}");
    assert_eq!(std::fs::read(&out).unwrap(), before);
    let other = dir.path().join("other");
    std::fs::write(bellwether::public_path(&other), b"{}").unwrap();
    assert_eq!(
        field(&bellwether::generate_key(&other, None, None).unwrap_err()),
        "out"
    );
    assert!(!other.exists());
}

#[test]
fn unusable_signing_key_files_are_refused_with_the_fix_named() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("signer");
    bellwether::generate_key(&out, None, None).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&out, std::fs::Permissions::from_mode(0o644)).unwrap();
        let e = bellwether::load_signing_key(&out).unwrap_err();
        assert_eq!(field(&e), "signing_key");
        let msg = e.to_string();
        assert!(msg.contains("0644") && msg.contains("chmod 600"), "{msg}");
        std::fs::set_permissions(&out, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let e = bellwether::load_signing_key(&dir.path().join("absent")).unwrap_err();
    assert_eq!(field(&e), "signing_key");
    assert!(e.to_string().contains("keys generate"), "{e}");
    let e = bellwether::load_signing_key(&bellwether::public_path(&out)).unwrap_err();
    assert!(e.to_string().contains("public key file"), "{e}");
    let short = dir.path().join("short");
    std::fs::write(&short, [1u8; 31]).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&short, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let e = bellwether::load_signing_key(&short).unwrap_err();
    assert!(e.to_string().contains("31 bytes"), "{e}");
    assert!(bellwether::load_signing_key(dir.path())
        .unwrap_err()
        .to_string()
        .contains("not a file"));
}

#[test]
fn trust_files_round_trip_and_refuse_bad_entries() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = (dir.path().join("a"), dir.path().join("b"));
    let ga = bellwether::generate_key(&a, Some("ci"), None).unwrap();
    let gb = bellwether::generate_key(&b, Some("release"), None).unwrap();
    let file = dir.path().join("trusted-keys.toml");
    let t = bellwether::trust_key(&file, &bellwether::public_path(&a), None, None).unwrap();
    assert_eq!(t.keys, 1);
    let t = bellwether::trust_key(
        &file,
        &bellwether::public_path(&b),
        Some("2026-01-01T00:00:00Z"),
        Some("2027-01-01T00:00:00+01:00"),
    )
    .unwrap();
    assert_eq!(t.keys, 2);
    assert_eq!(
        t.key.valid_from.as_deref(),
        Some("2026-01-01T00:00:00.000Z")
    );
    assert_eq!(
        t.key.revoked_at.as_deref(),
        Some("2026-12-31T23:00:00.000Z")
    );
    let keys = bellwether::load_trusted_keys(&file).unwrap();
    assert_eq!(keys.len(), 2);
    assert_eq!(keys.get(&ga.public.key_id).unwrap().name, "ci");
    assert_eq!(
        keys.get(&gb.public.key_id).unwrap().revoked_at.as_deref(),
        Some("2026-12-31T23:00:00.000Z")
    );

    // Trusted twice: refused, the file unchanged.
    let before = std::fs::read(&file).unwrap();
    let e = bellwether::trust_key(&file, &bellwether::public_path(&a), None, None).unwrap_err();
    assert!(e.to_string().contains("already trusts"), "{e}");
    assert_eq!(std::fs::read(&file).unwrap(), before);
    // A bad window: refused, the file unchanged.
    let c = dir.path().join("c");
    bellwether::generate_key(&c, None, None).unwrap();
    let e =
        bellwether::trust_key(&file, &bellwether::public_path(&c), Some("soon"), None).unwrap_err();
    assert!(e.to_string().contains("--valid-from"), "{e}");
    assert_eq!(std::fs::read(&file).unwrap(), before);

    // A key id edited away from its public key; an unknown field; a
    // missing file.
    let text = String::from_utf8(before).unwrap();
    let edited = dir.path().join("edited.toml");
    let wrong = Hash::of(b"other").to_hex();
    std::fs::write(
        &edited,
        text.replacen(&ga.public.key_id.to_hex(), &wrong, 1),
    )
    .unwrap();
    let e = bellwether::load_trusted_keys(&edited).unwrap_err();
    assert!(
        e.to_string().contains("is not the id of its public key"),
        "{e}"
    );
    std::fs::write(&edited, format!("{text}\nextra = 1\n")).unwrap();
    assert_eq!(
        field(&bellwether::load_trusted_keys(&edited).unwrap_err()),
        "trusted_keys"
    );
    let e = bellwether::load_trusted_keys(&dir.path().join("absent.toml")).unwrap_err();
    assert!(e.to_string().contains("keys trust"), "{e}");
}
