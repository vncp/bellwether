//! The PR #22 review's receipt probe, kept as a regression test: a real
//! receipt (its certificate written through the ledger crate, standing in
//! for verify until WP8 lands), then `receipt`, `why`, and `report`.
//!
//! The realized Replay is written here the way `fix` writes it after a
//! successful apply: the same proposal replayed at `manifest_after`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use bellwether::types::{
    format, CertBody, Certificate, Coverage, Hash, Provenance, Receipt, Record,
};
use bellwether::{spec, Engine, EngineOptions, KeyFile, LinkStatus, WhyRequest};
use bellwether_ledger::Ledger;
use bellwether_replay::{Replayer, Workload};
use bellwether_source::ParquetSource;
use bellwether_store::{now_rfc3339, Cache, ObjectStoreBackend, Snapshot};
use common::{ledger_bytes, ledger_corpus, write_keys};

fn grouped(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::new();
    for (i, ch) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

#[test]
fn probe_receipt_and_report_over_a_real_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data, 3, 4, 25, 400);
    let e = Engine::open(data.to_str().unwrap(), EngineOptions::default()).unwrap();
    e.index(&[spec::parse("exact:batch").unwrap()]).unwrap();
    let kf = dir.path().join("k.parquet");
    write_keys(&kf, "id", &[5, 77, 123]);
    let req = WhyRequest {
        keys: Some(KeyFile::new(&kf)),
        ..WhyRequest::default()
    };
    let w1 = e.why(&req).unwrap();
    let cand = &w1.candidates[0];
    let workload = w1.workload.as_ref().unwrap().hash;
    let index = data.join(".bellwether");
    let store = Arc::new(ObjectStoreBackend::from_url(index.to_str().unwrap()).unwrap());
    let ledger = Ledger::new(Arc::clone(&store));
    let cert = Certificate {
        record_version: format::RECORD_VERSION,
        proposal: Some(cand.proposal.hash),
        coverage: Coverage::Workload { workload, keys: 3 },
        pass: true,
        body: CertBody {
            missing_right: 0,
            missing_left: 0,
            mismatched: 0,
            ignored_columns: vec![],
            totals_delta: vec![],
            examples: Vec::new(),
            uncomparable: 0,
            totals: Vec::new(),
            row_filter: None,
            agg: Vec::new(),
        },
        provenance: Provenance {
            inputs: vec![Hash::of(b"stand-in for verify")],
            oldest_input_age: Duration::from_secs(1),
            manifest_version: Some("1".into()),
            created_at: now_rfc3339(),
        },
    };
    let c = ledger.put(&Record::Certificate(cert)).unwrap();
    e.index(&[spec::parse("exact:id").unwrap()]).unwrap();
    let receipt = Receipt {
        attest_only: false,
        prediction: None,
        record_version: format::RECORD_VERSION,
        evidence: vec![],
        proposal: cand.proposal.hash,
        replay: cand.replay.hash,
        certificate: c,
        manifest_before: "1".into(),
        manifest_after: "2".into(),
        created_at: now_rfc3339(),
    };
    let rh = ledger.put_receipt(&receipt, "2").unwrap();

    // J4, J5: every link resolves, the chain is intact, and each record is
    // the ledger's stored bytes.
    let r = e.receipt(&rh).unwrap();
    assert!(r.intact);
    assert_eq!(
        r.chain.len(),
        5,
        "the profile evidence, proposal, replay, certificate, receipt"
    );
    assert_eq!(r.records.len(), r.chain.len());
    for (w, l) in r.records.iter().zip(&r.chain) {
        assert_eq!(w.hash, l.hash);
        let bytes = serde_json::to_vec(&w.record).unwrap();
        assert_eq!(Hash::of(&bytes), w.hash);
        assert_eq!(ledger_bytes(&index, &w.hash.to_hex()), bytes);
    }

    // R5: with the floor now current it is no candidate, and the current
    // cost is still computed: the prediction, 2,661 bytes (2,967 under
    // parquet 53, whose writer also wrote page-header statistics).
    let w2 = e.why(&req).unwrap();
    assert_eq!(w2.sequence, 2);
    assert!(w2
        .candidates
        .iter()
        .all(|c| c.origin == bellwether::CandidateOrigin::Profile));
    assert_eq!(w2.current, Some(cand.proposed), "realized equals predicted");
    assert_eq!(w2.current.unwrap().bytes, 2_661);

    // What `fix` writes after a successful apply: the same proposal replayed
    // at manifest_after. `report` pairs it with the receipt.
    let source = ParquetSource::new();
    let snapshot = Snapshot::open(Arc::clone(&store), "").unwrap();
    let replayer = Replayer::for_snapshot(&source, &snapshot).unwrap();
    assert_eq!(replayer.manifest_version(), "2");
    let cache = Cache::new(Arc::clone(&store), "");
    let wl = Workload::load(&cache, &workload).unwrap();
    let (realized, scored) = replayer
        .record(&ledger, &cand.proposal.hash, &wl, &cache, &now_rfc3339())
        .unwrap();
    let realized_bytes: u64 = scored.replay.current.iter().map(|c| c.bytes).sum();
    assert_eq!(realized_bytes, cand.proposed.bytes);

    let out = dir.path().join("r.html");
    let rep = e.report(&out).unwrap();
    assert_eq!(rep.receipts, vec![rh]);
    assert_eq!(
        rep.realized, 1,
        "realized replay {realized} pairs with the receipt"
    );
    let html = std::fs::read_to_string(&out).unwrap();
    let lower = html.to_lowercase();
    for banned in [
        "<script", "src=", "href=", "@import", "url(", "http://", "https://",
    ] {
        assert!(!lower.contains(banned), "{banned}");
    }
    assert!(
        html.contains(&grouped(cand.proposed.bytes)),
        "predicted bytes rendered"
    );
    assert!(
        html.contains(&realized.to_hex()[..12]),
        "realized replay cited"
    );

    // J5 the other way: a missing link is flagged and its record is absent.
    std::fs::remove_file(
        index
            .join("ledger")
            .join(format!("{}.rec", cand.replay.hash.to_hex())),
    )
    .unwrap();
    let r = e.receipt(&rh).unwrap();
    assert!(!r.intact);
    assert_eq!(r.records.len(), r.chain.len() - 1);
    assert!(r
        .chain
        .iter()
        .any(|l| l.hash == cand.replay.hash && l.status == LinkStatus::Missing));
}
