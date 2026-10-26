//! Shared test helpers: an in-memory `Store` and a valid-chain fixture.

#![allow(dead_code, clippy::unwrap_used, missing_docs)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use bellwether_ledger::Ledger;
use bellwether_types::{
    format, CertBody, Certificate, Coverage, Error, Evidence, EvidenceBody, Hash, KeySpec,
    Proposal, ProposalBody, Provenance, Receipt, Record, Replay, Result, RunCost, Store,
    StoreVersion,
};
use bytes::Bytes;

/// A minimal in-memory store. Missing paths return `Error::NotFound` with the
/// hash of the path, which is deliberately not the record's hash, so the
/// ledger's normalization is exercised. `fail_reads` simulates a transient
/// backend failure.
#[derive(Default)]
pub struct MemStore {
    files: Mutex<BTreeMap<String, Bytes>>,
    pub fail_reads: AtomicBool,
}

impl MemStore {
    pub fn raw_put(&self, path: &str, body: impl Into<Bytes>) {
        self.files
            .lock()
            .unwrap()
            .insert(path.to_owned(), body.into());
    }

    pub fn len(&self) -> usize {
        self.files.lock().unwrap().len()
    }
}

fn version_of(b: &Bytes) -> StoreVersion {
    StoreVersion(Hash::of(b).to_hex())
}

impl Store for MemStore {
    fn get(&self, path: &str) -> Result<(Bytes, StoreVersion)> {
        if self.fail_reads.load(Ordering::SeqCst) {
            return Err(Error::Store(format!("transient failure reading {path}")));
        }
        let files = self.files.lock().unwrap();
        let b = files
            .get(path)
            .cloned()
            .ok_or_else(|| Error::NotFound(Hash::of(path.as_bytes())))?;
        let v = version_of(&b);
        Ok((b, v))
    }

    fn put_if_absent(&self, path: &str, body: Bytes) -> Result<StoreVersion> {
        let mut files = self.files.lock().unwrap();
        if files.contains_key(path) {
            return Err(Error::Store(format!("exists: {path}")));
        }
        let v = version_of(&body);
        files.insert(path.to_owned(), body);
        Ok(v)
    }

    fn put_if_version(
        &self,
        path: &str,
        body: Bytes,
        expected: &StoreVersion,
    ) -> Result<StoreVersion> {
        let mut files = self.files.lock().unwrap();
        let found = files.get(path).map(version_of);
        if found.as_ref() != Some(expected) {
            return Err(Error::VersionConflict {
                expected: expected.0.clone(),
                found: found.map(|v| v.0).unwrap_or_default(),
            });
        }
        let v = version_of(&body);
        files.insert(path.to_owned(), body);
        Ok(v)
    }

    fn put_content_addressed(&self, path: &str, body: Bytes) -> Result<()> {
        self.files.lock().unwrap().insert(path.to_owned(), body);
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self
            .files
            .lock()
            .unwrap()
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }

    fn delete(&self, path: &str) -> Result<()> {
        self.files.lock().unwrap().remove(path);
        Ok(())
    }
}

/// Manifest sequence before apply (ADR 020): what the certificate and replay
/// are computed against.
pub const BEFORE: &str = "7";
/// Manifest sequence the apply produced. The current sequence when the
/// receipt is written is at least this (maintenance commits may follow).
pub const AFTER: &str = "8";
pub const T0: &str = "2026-10-24T00:00:00Z";

pub fn provenance() -> Provenance {
    Provenance {
        inputs: vec![Hash::of(b"cache/footers")],
        oldest_input_age: Duration::from_secs(90),
        manifest_version: Some(BEFORE.into()),
        created_at: T0.into(),
    }
}

pub fn evidence() -> Evidence {
    Evidence {
        record_version: format::RECORD_VERSION,
        provenance: provenance(),
        body: EvidenceBody::Profile {
            root: "./orders".into(),
            fanout: vec![],
            lints: vec![],
            workload_keys: Some([40, 120_000, 415_000]),
        },
    }
}

pub fn proposal(evidence: Vec<Hash>, by: &str) -> Proposal {
    Proposal {
        record_version: format::RECORD_VERSION,
        evidence,
        proposed_by: by.into(),
        created_at: "2026-10-24T00:01:00Z".into(),
        body: ProposalBody::Key {
            spec: KeySpec::Exact {
                columns: vec!["order_id".into()],
                counts: false,
                bounds: None,
            },
            sidecars: Vec::new(),
        },
    }
}

pub fn run(id: &str, bytes: u64) -> RunCost {
    RunCost {
        run_id: id.into(),
        keys: 40,
        files: 3,
        row_groups: 9,
        bytes,
        exact: true,
        exactness: bellwether_types::Exactness::Exact,
        rows: None,
    }
}

pub fn replay(proposal: Hash) -> Replay {
    Replay {
        record_version: format::RECORD_VERSION,
        proposal,
        manifest_version: BEFORE.into(),
        workload: Hash::of(b"workload"),
        inputs: vec![Hash::of(b"catalog digest")],
        current: vec![run("p50", 900_000), run("p95", 100_000)],
        proposed: vec![run("p50", 4_000), run("p95", 1_000)],
        model: None,
        created_at: "2026-10-24T00:02:00Z".into(),
        configuration: Vec::new(),
        operators: Vec::new(),
        unserved: None,
    }
}

pub fn certificate(proposal: Option<Hash>, pass: bool) -> Certificate {
    let mut provenance = provenance();
    provenance.created_at = "2026-10-24T00:03:00Z".into();
    Certificate {
        record_version: format::RECORD_VERSION,
        proposal,
        coverage: Coverage::Workload {
            workload: Hash::of(b"workload"),
            keys: 4748,
        },
        pass,
        body: CertBody {
            missing_right: 0,
            missing_left: 0,
            mismatched: u64::from(!pass),
            ignored_columns: vec!["updated_at".into()],
            totals_delta: vec![],
            examples: Vec::new(),
            uncomparable: 0,
            totals: Vec::new(),
            row_filter: None,
        },
        provenance,
    }
}

/// Hashes of a fully stored, valid chain, plus the receipt that would
/// complete it (not yet written).
pub struct Fixture {
    pub evidence: Hash,
    pub proposal: Hash,
    pub replay: Hash,
    pub certificate: Hash,
    pub receipt: Receipt,
}

pub fn valid_chain<S>(ledger: &Ledger<S>) -> Fixture
where
    S: std::ops::Deref,
    S::Target: Store,
{
    let evidence = ledger.put(&Record::Evidence(self::evidence())).unwrap();
    let proposal = ledger
        .put(&Record::Proposal(self::proposal(vec![evidence], "agent")))
        .unwrap();
    let replay = ledger.put(&Record::Replay(self::replay(proposal))).unwrap();
    let certificate = ledger
        .put(&Record::Certificate(self::certificate(
            Some(proposal),
            true,
        )))
        .unwrap();
    let receipt = Receipt {
        record_version: format::RECORD_VERSION,
        evidence: vec![evidence],
        proposal,
        replay,
        certificate,
        manifest_before: BEFORE.into(),
        manifest_after: AFTER.into(),
        created_at: "2026-10-24T00:04:00Z".into(),
    };
    Fixture {
        evidence,
        proposal,
        replay,
        certificate,
        receipt,
    }
}

/// Asserts `r` is a `LedgerRule` error whose message contains `needle`.
pub fn assert_rule<T: std::fmt::Debug>(r: Result<T>, needle: &str) {
    match r {
        Err(Error::LedgerRule(msg)) => assert!(
            msg.contains(needle),
            "rule message {msg:?} does not mention {needle:?}"
        ),
        other => panic!("expected LedgerRule containing {needle:?}, got {other:?}"),
    }
}
