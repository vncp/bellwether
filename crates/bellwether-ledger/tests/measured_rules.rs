//! ADR 033 ledger rules: measured current costs only for a rewrite that
//! names its model; a cost carrying engine statistics is never exact; a
//! prediction belongs to a measured rewrite; a measured replay backs only
//! an attest-only receipt, which is a rewrite's and leaves the manifest
//! where it was; and a receipt's prediction is its replay's. Every refusal
//! writes nothing.

#![allow(clippy::unwrap_used, clippy::many_single_char_names)]

mod common;

use bellwether_ledger::Ledger;
use bellwether_types::{
    format, Hash, Prediction, ProposalBody, QueryPair, QueryStats, Receipt, Record, Replay,
    RewriteShape,
};
use common::{assert_rule, certificate, evidence, proposal, replay, MemStore, BEFORE};

fn stats(tag: &str) -> QueryStats {
    QueryStats {
        tag: tag.into(),
        read_rows: 100,
        read_bytes: 1_000,
        peak_memory_bytes: 10_000,
        cpu_ns: Some(5),
        duration_ns: 7,
        result_rows: 2,
        units_read: Some(1),
        units_total: None,
    }
}

fn engine_shape() -> RewriteShape {
    RewriteShape {
        before: "1 query".into(),
        after: "its rewrite".into(),
        invariant: "same rows".into(),
        pairs: vec![QueryPair {
            before: "SELECT 1".into(),
            after: "SELECT 1".into(),
        }],
        key: vec!["id".into()],
        keys: None,
    }
}

fn prediction(evidence: Hash) -> Prediction {
    Prediction {
        evidence,
        predicted_rows: 10,
        predicted_row_groups: 1,
        before_read_rows: 100,
        after_read_rows: 20,
        before_read_bytes: 1_000,
        after_read_bytes: 200,
        before_ratio: Some("10.000".into()),
        after_ratio: Some("2.000".into()),
    }
}

/// A measured replay of `p`: every cost inexact with engine statistics.
fn measured(p: Hash, evidence: Hash) -> Replay {
    let mut r = replay(p);
    for (i, c) in r
        .current
        .iter_mut()
        .chain(r.proposed.iter_mut())
        .enumerate()
    {
        c.exact = false;
        c.measured = Some(stats(&format!("bw-x-{i}")));
    }
    r.model = Some("clickhouse.query_log".into());
    r.prediction = Some(prediction(evidence));
    r
}

fn setup(ledger: &Ledger<&MemStore>, body: ProposalBody) -> (Hash, Hash) {
    let e = ledger.put(&Record::Evidence(evidence())).unwrap();
    let mut p = proposal(vec![e], "agent");
    p.body = body;
    (e, ledger.put(&Record::Proposal(p)).unwrap())
}

#[test]
fn a_measured_rewrite_replay_is_recorded_and_a_key_one_is_still_refused() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let (e, p) = setup(
        &ledger,
        ProposalBody::Rewrite {
            shape: engine_shape(),
        },
    );
    let h = ledger.put(&Record::Replay(measured(p, e))).unwrap();
    assert!(matches!(ledger.get(&h).unwrap(), Record::Replay(_)));
    // Without the model: refused.
    let mut r = measured(p, e);
    r.model = None;
    r.prediction = None;
    assert_rule(
        ledger.put(&Record::Replay(r)),
        "exact for the current configuration",
    );
    // A key proposal with inexact current costs and a model: still refused.
    let (_, k) = setup(&ledger, common::proposal(vec![], "x").body);
    let before = store.len();
    let mut r = measured(k, e);
    r.prediction = None;
    assert_rule(
        ledger.put(&Record::Replay(r)),
        "exact for the current configuration",
    );
    assert_eq!(store.len(), before);
    // A layout proposal likewise.
    let (_, l) = setup(
        &ledger,
        ProposalBody::Layout {
            cluster_by: vec!["id".into()],
        },
    );
    let mut r = measured(l, e);
    r.prediction = None;
    assert_rule(
        ledger.put(&Record::Replay(r)),
        "exact for the current configuration",
    );
}

#[test]
fn engine_statistics_are_never_exact_and_a_prediction_needs_a_measured_rewrite() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let (e, p) = setup(
        &ledger,
        ProposalBody::Rewrite {
            shape: engine_shape(),
        },
    );
    let mut r = measured(p, e);
    r.proposed[0].exact = true;
    assert_rule(ledger.put(&Record::Replay(r)), "must not be marked exact");
    // A prediction on an exact replay: refused.
    let mut r = replay(p);
    r.prediction = Some(prediction(e));
    assert_rule(ledger.put(&Record::Replay(r)), "prediction");
}

fn receipt(p: Hash, replay: Hash, cert: Hash, e: Hash, attest_only: bool) -> Receipt {
    Receipt {
        record_version: format::RECORD_VERSION,
        evidence: vec![e],
        proposal: p,
        replay,
        certificate: cert,
        manifest_before: BEFORE.into(),
        manifest_after: BEFORE.into(),
        created_at: "2026-10-24T00:04:00Z".into(),
        attest_only,
        prediction: None,
    }
}

#[test]
fn a_measured_replay_never_backs_an_apply_receipt() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let (e, p) = setup(
        &ledger,
        ProposalBody::Rewrite {
            shape: engine_shape(),
        },
    );
    let m = measured(p, e);
    let r = ledger.put(&Record::Replay(m.clone())).unwrap();
    let c = ledger
        .put(&Record::Certificate(certificate(Some(p), true)))
        .unwrap();
    // Not attest-only: refused.
    let mut rc = receipt(p, r, c, e, false);
    rc.prediction = m.prediction.clone();
    let before = store.len();
    assert_rule(ledger.put_receipt(&rc, BEFORE), "can only attest");
    // Attest-only but moving the manifest: refused.
    let mut rc = receipt(p, r, c, e, true);
    rc.prediction = m.prediction.clone();
    rc.manifest_after = "8".into();
    assert_rule(ledger.put_receipt(&rc, "8"), "applied nothing");
    // A prediction other than the replay's: refused.
    let mut rc = receipt(p, r, c, e, true);
    let mut other = prediction(e);
    other.after_read_rows = 1;
    rc.prediction = Some(other);
    assert_rule(ledger.put_receipt(&rc, BEFORE), "prediction");
    // No prediction at all where the replay has one: refused.
    let rc = receipt(p, r, c, e, true);
    assert_rule(ledger.put_receipt(&rc, BEFORE), "prediction");
    assert_eq!(store.len(), before);
    // Attest-only, unchanged manifest, the replay's prediction: accepted.
    let mut rc = receipt(p, r, c, e, true);
    rc.prediction = m.prediction;
    let h = ledger.put_receipt(&rc, BEFORE).unwrap();
    match ledger.get(&h).unwrap() {
        Record::Receipt(back) => assert_eq!(back, rc),
        other => panic!("{other:?}"),
    }
}

#[test]
fn an_attest_only_receipt_is_a_rewrite_s() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let fx = common::valid_chain(&ledger);
    let mut rc = fx.receipt.clone();
    rc.attest_only = true;
    rc.manifest_after = rc.manifest_before.clone();
    assert_rule(
        ledger.put_receipt(&rc, BEFORE),
        "attest-only receipt is for a rewrite",
    );
}

#[test]
fn the_new_fields_are_absent_from_records_that_do_not_use_them() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let fx = common::valid_chain(&ledger);
    for rec in [
        Record::Receipt(fx.receipt.clone()),
        ledger.get(&fx.replay).unwrap(),
        ledger.get(&fx.proposal).unwrap(),
        ledger.get(&fx.evidence).unwrap(),
    ] {
        let text = serde_json::to_string(&rec).unwrap();
        for f in [
            "attest_only",
            "prediction",
            "measured",
            "pairs",
            // `RunCost.keys` is a count; the rewrite shape's `keys` is a hash.
            "\"keys\":\"",
            "twin",
        ] {
            assert!(!text.contains(f), "{f} in {text}");
        }
    }
}
