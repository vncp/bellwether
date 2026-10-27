//! The CI row of the threat model (WP17): `Engine::receipt_gate` reads the
//! chain's own records back rather than trusting `explain`'s intact flag.
//! Each attack is planted in a real ledger and must fail with its named
//! check: a certificate that did not pass (behind an intact chain), an
//! older manifest, another workload, other data, an unsigned or
//! untrusted-signed receipt under `require_signature`, a hash of another
//! kind, an unknown hash, and an altered record.
//!
//! Deliberate break: taking `explain`'s intact flag as the certificate
//! check fails `a_failed_certificate_behind_an_intact_chain_fails_the_certificate_check`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

mod common;

use std::path::{Path, PathBuf};

use bellwether::types::{Hash, Receipt, Record};
use bellwether::{
    Engine, EngineOptions, FixOutcome, FixRequest, GateCheck, GateFailure, GateRequirements,
    GateVerdict, KeyFile, SigningKey, TrustedKey, TrustedKeys, WhyRequest,
};
use common::{scattered_corpus, write_keys};

const CLOCK: &str = "2026-10-27T12:00:00.000Z";

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

struct Lake {
    _dir: tempfile::TempDir,
    data: PathBuf,
    receipt: Hash,
    certificate: Hash,
    workload: Hash,
}

/// A corpus where `exact:id` prunes, its key fixed with `key` signing:
/// the receipt, its certificate, and the workload.
fn lake(key: Option<SigningKey>) -> Lake {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    // Ids scattered over every row group's range: the files' own min/max
    // statistics skip nothing, and a key on id selects few row groups.
    scattered_corpus(&data, 3, 4, 25, 400);
    let kf = dir.path().join("k.parquet");
    write_keys(&kf, "id", &[5, 77, 123]);
    let w = open(&data, None, None)
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&kf)),
            ..WhyRequest::default()
        })
        .unwrap();
    let proposal = w.recommended.as_ref().expect("a recommendation").proposal;
    assert_eq!(proposal, w.candidates[0].proposal.hash);
    let workload = w.workload.as_ref().unwrap().hash;
    let r = open(&data, key, None)
        .fix(&FixRequest {
            proposal: Some(proposal),
            workload: Some(workload),
            ..FixRequest::default()
        })
        .unwrap();
    let FixOutcome::Applied { receipt, .. } = &r.outcome else {
        panic!("{:?}", r.outcome)
    };
    Lake {
        receipt: receipt.hash,
        certificate: r.certificate.as_ref().unwrap().hash,
        workload,
        data,
        _dir: dir,
    }
}

fn req(l: &Lake, require_signature: bool) -> GateRequirements {
    GateRequirements {
        root: l.data.to_str().unwrap().to_string(),
        workload: l.workload,
        require_signature,
    }
}

fn record_path(data: &Path, h: &Hash) -> PathBuf {
    data.join(".bellwether")
        .join("ledger")
        .join(format!("{}.rec", h.to_hex()))
}

/// Writes `record` straight into the store, past every ledger rule, as an
/// attacker with write access to the store could.
fn plant(data: &Path, record: &Record) -> Hash {
    let bytes = serde_json::to_vec(record).unwrap();
    let h = Hash::of(&bytes);
    std::fs::write(record_path(data, &h), bytes).unwrap();
    h
}

fn get(data: &Path, h: &Hash) -> Record {
    serde_json::from_slice(&std::fs::read(record_path(data, h)).unwrap()).unwrap()
}

fn receipt_of(data: &Path, h: &Hash) -> Receipt {
    match get(data, h) {
        Record::Receipt(r) => r,
        other => panic!("{other:?}"),
    }
}

fn failed(v: &GateVerdict) -> (GateCheck, &GateFailure) {
    assert!(!v.pass, "{v:?}");
    (v.failed_check().unwrap(), v.failure.as_ref().unwrap())
}

#[test]
fn a_real_receipt_passes_every_check_and_reports_its_facts() {
    let key = SigningKey::from_seed(&[7; 32]);
    let l = lake(Some(key.clone()));
    let v = open(&l.data, None, Some(trusting(&key)))
        .receipt_gate(&l.receipt, &req(&l, true))
        .unwrap();
    assert!(v.pass, "{v:?}");
    assert_eq!(
        v.checks
            .iter()
            .map(|c| (c.check, c.pass))
            .collect::<Vec<_>>(),
        [
            GateCheck::Kind,
            GateCheck::Chain,
            GateCheck::Certificate,
            GateCheck::Coverage,
            GateCheck::Data,
            GateCheck::Signature
        ]
        .map(|c| (c, true))
    );
    let f = v.facts.unwrap();
    let seq = |s: &str| s.parse::<u64>().unwrap();
    assert!(seq(&f.manifest_after) > seq(&f.manifest_before), "{f:?}");
    let replay = f.replay.unwrap();
    assert!(replay.proposed.bytes < replay.current.bytes, "{replay:?}");
    // `fix` replays the key at manifest_after: the realized cost.
    assert_eq!(f.realized.unwrap().current, replay.proposed);
    assert!(matches!(
        f.signature,
        Some(bellwether::Verdict::Signed { ref by, .. }) if by == "ci"
    ));
    // Without a required signature the check is not run.
    let v = open(&l.data, None, None)
        .receipt_gate(&l.receipt, &req(&l, false))
        .unwrap();
    assert!(v.pass && v.checks.len() == 5, "{v:?}");
}

#[test]
fn a_failed_certificate_behind_an_intact_chain_fails_the_certificate_check() {
    let l = lake(None);
    let mut cert = match get(&l.data, &l.certificate) {
        Record::Certificate(c) => c,
        other => panic!("{other:?}"),
    };
    cert.pass = false;
    cert.body.mismatched = 2;
    let bad = plant(&l.data, &Record::Certificate(cert));
    let mut r = receipt_of(&l.data, &l.receipt);
    r.certificate = bad;
    let planted = plant(&l.data, &Record::Receipt(r));
    let e = open(&l.data, None, None);
    // Every link is present and of its kind: explain calls it intact.
    assert!(e.receipt(&planted).unwrap().intact);
    let v = e.receipt_gate(&planted, &req(&l, false)).unwrap();
    let (check, f) = failed(&v);
    assert_eq!(check, GateCheck::Certificate);
    assert!(
        matches!(
            f,
            GateFailure::CertificateFailed {
                certificate,
                pass: false,
                mismatched: 2,
                ..
            } if *certificate == bad
        ),
        "{f:?}"
    );
}

#[test]
fn an_older_manifest_another_workload_and_other_data_fail_their_checks() {
    let l = lake(None);
    let e = open(&l.data, None, None);

    // A receipt claiming a later manifest_before than its certificate and
    // replay were computed against: they describe an older manifest.
    let real = receipt_of(&l.data, &l.receipt);
    let mut r = real.clone();
    r.manifest_before.clone_from(&r.manifest_after);
    let planted = plant(&l.data, &Record::Receipt(r));
    let v = e.receipt_gate(&planted, &req(&l, false)).unwrap();
    let (check, f) = failed(&v);
    assert_eq!(check, GateCheck::Certificate);
    assert!(
        matches!(f, GateFailure::StaleManifest { record, computed: Some(c), manifest_before, .. }
            if *record == l.certificate
                && *c == real.manifest_before
                && *manifest_before == real.manifest_after),
        "{f:?}"
    );

    // The step declares another workload than the certificate covered.
    let other = Hash::of(b"another workload");
    let v = e
        .receipt_gate(
            &l.receipt,
            &GateRequirements {
                workload: other,
                ..req(&l, false)
            },
        )
        .unwrap();
    let (check, f) = failed(&v);
    assert_eq!(check, GateCheck::Coverage);
    assert!(
        matches!(f, GateFailure::WrongWorkload { covered, required, .. }
            if *covered == l.workload && *required == other),
        "{f:?}"
    );

    // The receipt is about a folder with other data: a copy of the root
    // with one file left out.
    let copy = l.data.parent().unwrap().join("copy");
    std::fs::create_dir_all(&copy).unwrap();
    for f in ["part-000.parquet", "part-001.parquet"] {
        std::fs::copy(l.data.join(f), copy.join(f)).unwrap();
    }
    let v = e
        .receipt_gate(
            &l.receipt,
            &GateRequirements {
                root: copy.to_str().unwrap().to_string(),
                ..req(&l, false)
            },
        )
        .unwrap();
    let (check, f) = failed(&v);
    assert_eq!(check, GateCheck::Data);
    assert!(
        matches!(f, GateFailure::DataChanged { certified: Some(c), current, .. } if c != current),
        "{f:?}"
    );

    // A file lands in the root after the receipt: the data changed.
    std::fs::copy(
        l.data.join("part-000.parquet"),
        l.data.join("part-009.parquet"),
    )
    .unwrap();
    let v = e.receipt_gate(&l.receipt, &req(&l, false)).unwrap();
    assert_eq!(failed(&v).0, GateCheck::Data);
}

#[test]
fn an_unsigned_or_untrusted_receipt_fails_when_a_signature_is_required() {
    let trusted = SigningKey::from_seed(&[7; 32]);
    let l = lake(None);
    let e = open(&l.data, None, Some(trusting(&trusted)));
    let (check, f) = {
        let v = e.receipt_gate(&l.receipt, &req(&l, true)).unwrap();
        let (c, f) = failed(&v);
        (c, f.clone())
    };
    assert_eq!(check, GateCheck::Signature);
    assert_eq!(
        f,
        GateFailure::Unsigned {
            record: l.receipt,
            kind: bellwether::RecordKind::Receipt
        }
    );

    let stranger = SigningKey::from_seed(&[9; 32]);
    let s = lake(Some(stranger.clone()));
    let v = open(&s.data, None, Some(trusting(&trusted)))
        .receipt_gate(&s.receipt, &req(&s, true))
        .unwrap();
    let (check, f) = failed(&v);
    assert_eq!(check, GateCheck::Signature);
    assert_eq!(
        *f,
        GateFailure::UntrustedKey {
            record: s.receipt,
            kind: bellwether::RecordKind::Receipt,
            key_ids: vec![stranger.key_id()]
        }
    );

    // A trusted receipt over a certificate nobody trusted signed: planted
    // beside the real chain, the receipt re-signed by the trusted key.
    let k = lake(Some(trusted.clone()));
    let sig_dir = k.data.join(".bellwether").join("ledger");
    for e in std::fs::read_dir(&sig_dir).unwrap() {
        let p = e.unwrap().path();
        let name = p.file_name().unwrap().to_string_lossy().into_owned();
        if name.starts_with(&k.certificate.to_hex()) && name.contains(".sig.") {
            std::fs::remove_file(&p).unwrap();
        }
    }
    let v = open(&k.data, None, Some(trusting(&trusted)))
        .receipt_gate(&k.receipt, &req(&k, true))
        .unwrap();
    let (check, f) = failed(&v);
    assert_eq!(check, GateCheck::Signature);
    assert_eq!(
        *f,
        GateFailure::Unsigned {
            record: k.certificate,
            kind: bellwether::RecordKind::Certificate
        }
    );

    // A required signature with no trusted keys is the caller's error.
    let err = open(&l.data, None, None)
        .receipt_gate(&l.receipt, &req(&l, true))
        .unwrap_err();
    assert!(
        matches!(
            err,
            bellwether::types::Error::InvalidArgument {
                field: "trusted_keys",
                ..
            }
        ),
        "{err:?}"
    );
}

#[test]
fn another_kind_an_unknown_hash_and_altered_records_fail_the_kind_or_chain_check() {
    let l = lake(None);
    let e = open(&l.data, None, None);
    let v = e.receipt_gate(&l.certificate, &req(&l, false)).unwrap();
    let (check, f) = failed(&v);
    assert_eq!(check, GateCheck::Kind);
    assert!(
        matches!(
            f,
            GateFailure::WrongKind {
                expected: bellwether::RecordKind::Receipt,
                found: bellwether::RecordKind::Certificate,
                ..
            }
        ),
        "{f:?}"
    );
    let v = e
        .receipt_gate(&Hash::of(b"nothing"), &req(&l, false))
        .unwrap();
    assert!(matches!(
        failed(&v),
        (GateCheck::Kind, GateFailure::NotFound { .. })
    ));

    // An altered link: the replay's bytes no longer hash to its name.
    let replay = receipt_of(&l.data, &l.receipt).replay;
    let p = record_path(&l.data, &replay);
    let mut bytes = std::fs::read(&p).unwrap();
    let at = bytes.iter().position(|b| *b == b'1').unwrap();
    bytes[at] = b'2';
    std::fs::write(&p, &bytes).unwrap();
    let v = e.receipt_gate(&l.receipt, &req(&l, false)).unwrap();
    let (check, f) = failed(&v);
    assert_eq!(check, GateCheck::Chain);
    assert!(
        matches!(f, GateFailure::Unreadable { record, .. } if *record == replay),
        "{f:?}"
    );

    // The receipt itself altered.
    let p = record_path(&l.data, &l.receipt);
    let mut bytes = std::fs::read(&p).unwrap();
    bytes.push(b' ');
    std::fs::write(&p, &bytes).unwrap();
    let v = e.receipt_gate(&l.receipt, &req(&l, false)).unwrap();
    assert!(matches!(
        failed(&v),
        (GateCheck::Kind, GateFailure::Unreadable { .. })
    ));
}

#[test]
fn the_gate_writes_nothing() {
    let l = lake(None);
    let list = |d: &Path| {
        let mut v: Vec<(PathBuf, Vec<u8>)> = walk(d)
            .into_iter()
            .map(|p| {
                let b = std::fs::read(&p).unwrap();
                (p, b)
            })
            .collect();
        v.sort();
        v
    };
    let before = list(l.data.parent().unwrap());
    let v = open(&l.data, None, None)
        .receipt_gate(&l.receipt, &req(&l, false))
        .unwrap();
    assert!(v.pass);
    assert_eq!(before, list(l.data.parent().unwrap()));
}

fn walk(d: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(d).unwrap() {
        let p = e.unwrap().path();
        if p.is_dir() {
            out.extend(walk(&p));
        } else {
            out.push(p);
        }
    }
    out
}
