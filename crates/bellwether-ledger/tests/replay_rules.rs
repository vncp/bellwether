//! Contract v2.1 replay rules: a replay names the data it read, and its
//! exactness matches its proposal's kind. Every refusal writes nothing.

#![allow(clippy::unwrap_used)]

mod common;

use bellwether_ledger::{record_path, Ledger};
use bellwether_types::{Exactness, Hash, ProposalBody, Quantity, Record, RewriteShape, RunCost};
use common::{assert_rule, evidence, proposal, replay, MemStore};

/// Marks a cost measured by a model: `exact` false and its `exactness`
/// saying so, as record version 3 requires of both fields together.
fn inexact(c: &mut RunCost) {
    c.exact = false;
    c.exactness = Exactness::Measured {
        model: "layout.v1".into(),
    };
}

/// Marks a cost exact again.
fn exact(c: &mut RunCost) {
    c.exact = true;
    c.exactness = Exactness::Exact;
}

fn key_proposal_hash(ledger: &Ledger<&MemStore>) -> Hash {
    let e = ledger.put(&Record::Evidence(evidence())).unwrap();
    ledger
        .put(&Record::Proposal(proposal(vec![e], "agent")))
        .unwrap()
}

fn proposal_of(ledger: &Ledger<&MemStore>, body: ProposalBody) -> Hash {
    let e = ledger.put(&Record::Evidence(evidence())).unwrap();
    let mut p = proposal(vec![e], "agent");
    p.body = body;
    ledger.put(&Record::Proposal(p)).unwrap()
}

#[test]
fn a_replay_without_inputs_is_refused_and_nothing_is_written() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let p = key_proposal_hash(&ledger);
    let before = store.len();
    let mut r = replay(p);
    r.inputs.clear();
    assert_rule(ledger.put(&Record::Replay(r)), "names no inputs");
    assert_eq!(store.len(), before, "a refused replay writes nothing");
}

#[test]
fn a_key_proposal_replay_must_be_exact_in_every_run() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let p = key_proposal_hash(&ledger);
    let mut r = replay(p);
    inexact(&mut r.proposed[1]);
    assert_rule(ledger.put(&Record::Replay(r)), "must be exact in every run");
    let mut r = replay(p);
    inexact(&mut r.current[0]);
    assert_rule(
        ledger.put(&Record::Replay(r)),
        "exact for the current configuration",
    );
}

#[test]
fn a_key_proposal_replay_must_not_name_a_model() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let p = key_proposal_hash(&ledger);
    let mut r = replay(p);
    r.model = Some("clustered-by-key".into());
    assert_rule(ledger.put(&Record::Replay(r)), "must not name a cost model");
    // A blank or padded model is refused outright, never stored as "present".
    for bad in ["", "   ", "\t\n", "  padded  "] {
        let mut r = replay(p);
        r.model = Some(bad.into());
        assert_rule(ledger.put(&Record::Replay(r)), "non-empty name");
    }
}

#[test]
fn a_record_written_now_carries_the_current_version_only() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let p = key_proposal_hash(&ledger);
    let mut r = replay(p);
    r.record_version = 1;
    let before = store.len();
    assert_rule(ledger.put(&Record::Replay(r)), "must carry record_version");
    assert_eq!(store.len(), before);
}

#[test]
fn a_receipt_rechecks_its_replay() {
    use common::{certificate, AFTER, BEFORE};
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let e = ledger.put(&Record::Evidence(evidence())).unwrap();
    let p = ledger
        .put(&Record::Proposal(proposal(vec![e], "agent")))
        .unwrap();
    // A key replay with an inexact cost and no model, planted straight into
    // the store as if by an older build or an attacker with store access.
    let mut bad = replay(p);
    inexact(&mut bad.proposed[0]);
    let bytes = serde_json::to_vec(&Record::Replay(bad)).unwrap();
    let bad_h = Hash::of(&bytes);
    store.raw_put(&record_path(&bad_h), bytes);
    let c = ledger
        .put(&Record::Certificate(certificate(Some(p), true)))
        .unwrap();
    let receipt = bellwether_types::Receipt {
        record_version: bellwether_types::format::RECORD_VERSION,
        evidence: vec![e],
        proposal: p,
        replay: bad_h,
        certificate: c,
        manifest_before: BEFORE.into(),
        manifest_after: AFTER.into(),
        created_at: "2026-10-24T00:04:00Z".into(),
    };
    let before = store.len();
    assert_rule(ledger.put_receipt(&receipt, AFTER), "receipt's replay");
    assert_eq!(store.len(), before);
}

#[test]
fn bloom_false_positive_rates_round_trip_through_the_ledger() {
    // 1/11 and friends are not representable in shortest-float JSON without
    // serde_json's float_roundtrip; the ledger's write-back check then
    // refuses the proposal. This test fails if that feature is dropped.
    use bellwether_types::KeySpec;
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let e = ledger.put(&Record::Evidence(evidence())).unwrap();
    for d in 2u32..=400 {
        let fpr = 1.0 / f64::from(d);
        let mut pr = proposal(vec![e], "agent");
        pr.body = ProposalBody::Key {
            spec: KeySpec::Bloom {
                columns: vec!["order_id".into()],
                fpr,
                bounds: None,
            },
            sidecars: Vec::new(),
        };
        let h = ledger
            .put(&Record::Proposal(pr.clone()))
            .unwrap_or_else(|err| panic!("fpr 1/{d} refused: {err}"));
        match ledger.get(&h).unwrap() {
            Record::Proposal(back) => assert_eq!(back, pr, "fpr 1/{d}"),
            other => panic!("expected a proposal, got {other:?}"),
        }
    }
}

#[test]
fn a_layout_proposal_replay_needs_a_model_and_exact_current_costs() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let p = proposal_of(
        &ledger,
        ProposalBody::Layout {
            cluster_by: vec!["order_id".into()],
        },
    );
    let mut r = replay(p);
    inexact(&mut r.proposed[0]);
    assert_rule(
        ledger.put(&Record::Replay(r.clone())),
        "must name the cost model",
    );
    r.model = Some("perfectly clustered by order_id".into());
    inexact(&mut r.current[1]);
    assert_rule(
        ledger.put(&Record::Replay(r.clone())),
        "exact for the current configuration",
    );
    exact(&mut r.current[1]);
    let h = ledger.put(&Record::Replay(r.clone())).unwrap();
    match ledger.get(&h).unwrap() {
        Record::Replay(back) => {
            assert_eq!(back, r, "inputs and model round-trip exactly");
            assert_eq!(back.record_version, 3);
        }
        other => panic!("expected a replay, got {other:?}"),
    }
}

#[test]
fn a_rewrite_proposal_replay_follows_the_same_exactness_rule() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let p = proposal_of(
        &ledger,
        ProposalBody::Rewrite {
            shape: RewriteShape {
                before: "join on batch_id".into(),
                after: "join on (entity_id, batch_id)".into(),
                invariant: "same rows for every trigger key".into(),
                before_plan: None,
                after_plan: None,
            },
        },
    );
    // Exact throughout, no model: accepted.
    let h = ledger.put(&Record::Replay(replay(p))).unwrap();
    assert!(matches!(ledger.get(&h).unwrap(), Record::Replay(_)));
    // A model while every cost is exact: refused.
    let mut r = replay(p);
    r.model = Some("measured".into());
    assert_rule(ledger.put(&Record::Replay(r)), "must not name a cost model");
    // An inexact proposed cost without a model: refused; with one: accepted.
    let mut r = replay(p);
    inexact(&mut r.proposed[0]);
    assert_rule(
        ledger.put(&Record::Replay(r.clone())),
        "must name the cost model",
    );
    r.model = Some("after-shape priced from statistics of the rewritten output".into());
    ledger.put(&Record::Replay(r)).unwrap();
}

#[test]
fn a_version_one_replay_without_inputs_or_model_remains_readable() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let p = key_proposal_hash(&ledger);
    let mut r = replay(p);
    r.record_version = 1;
    r.inputs.clear();
    r.model = None;
    let bytes = serde_json::to_vec(&Record::Replay(r.clone())).unwrap();
    let text = String::from_utf8_lossy(&bytes);
    assert!(
        !text.contains("inputs") && !text.contains("model"),
        "a version-1 record carries neither new field: {text}"
    );
    let h = Hash::of(&bytes);
    store.raw_put(&record_path(&h), bytes);
    match ledger.get(&h).unwrap() {
        Record::Replay(back) => {
            assert_eq!(back, r);
            assert_eq!(back.inputs, Vec::<Hash>::new());
            assert_eq!(back.model, None);
            assert_eq!(back.record_version, 1);
        }
        other => panic!("expected a replay, got {other:?}"),
    }
}

#[test]
fn a_blank_model_never_satisfies_an_inexact_replay() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let p = proposal_of(
        &ledger,
        ProposalBody::Layout {
            cluster_by: vec!["order_id".into()],
        },
    );
    for bad in ["", "   ", "\t\n", "  padded  "] {
        let mut r = replay(p);
        inexact(&mut r.proposed[0]);
        r.model = Some(bad.into());
        let before = store.len();
        assert_rule(ledger.put(&Record::Replay(r)), "non-empty name");
        assert_eq!(store.len(), before);
    }
}

#[test]
fn explain_shows_the_model_only_when_a_replay_has_one() {
    use common::{certificate, valid_chain, AFTER, BEFORE};
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let evidence_hash = ledger.put(&Record::Evidence(evidence())).unwrap();
    let mut pr = proposal(vec![evidence_hash], "agent");
    pr.body = ProposalBody::Layout {
        cluster_by: vec!["order_id".into()],
    };
    let layout_proposal = ledger.put(&Record::Proposal(pr)).unwrap();
    let mut scored = replay(layout_proposal);
    inexact(&mut scored.proposed[0]);
    scored.model = Some("perfectly clustered by order_id".into());
    let replay_hash = ledger.put(&Record::Replay(scored)).unwrap();
    let certificate_hash = ledger
        .put(&Record::Certificate(certificate(
            Some(layout_proposal),
            true,
        )))
        .unwrap();
    let receipt = bellwether_types::Receipt {
        record_version: bellwether_types::format::RECORD_VERSION,
        evidence: vec![evidence_hash],
        proposal: layout_proposal,
        replay: replay_hash,
        certificate: certificate_hash,
        manifest_before: BEFORE.into(),
        manifest_after: AFTER.into(),
        created_at: "2026-10-24T00:04:00Z".into(),
    };
    let layout_receipt = ledger.put_receipt(&receipt, AFTER).unwrap();
    let text = ledger.explain(&layout_receipt).unwrap().render();
    assert!(
        text.contains("model=perfectly clustered by order_id"),
        "a modelled number must be marked: {text}"
    );

    // A key chain's replay line carries no model fact.
    let key_chain = valid_chain(&ledger);
    let key_receipt = ledger.put_receipt(&key_chain.receipt, AFTER).unwrap();
    assert_ne!(key_receipt, layout_receipt);
    let text = ledger.explain(&key_receipt).unwrap().render();
    assert!(!text.contains("model="), "{text}");
}

/// Rule 7b, record version 3: `exact` agrees with `exactness`, a measured
/// cost needs the replay's model, a bounded interval is ordered, and a
/// bounded `rows` records its upper bound.
#[test]
fn exactness_agrees_with_exact_and_a_measured_cost_names_the_model() {
    let store = MemStore::default();
    let ledger = Ledger::new(&store);
    let p = proposal_of(
        &ledger,
        ProposalBody::Layout {
            cluster_by: vec!["order_id".into()],
        },
    );
    let mut r = replay(p);
    r.model = Some("perfectly clustered by order_id".into());
    // exact=false with exactness left Exact: the two fields disagree
    r.proposed[0].exact = false;
    assert_rule(
        ledger.put(&Record::Replay(r.clone())),
        "exact is true exactly when exactness is exact",
    );
    // exactness measured with exact still true: disagree the other way
    exact(&mut r.proposed[0]);
    r.proposed[0].exactness = Exactness::Measured {
        model: "layout.v1".into(),
    };
    assert_rule(
        ledger.put(&Record::Replay(r.clone())),
        "exact is true exactly when exactness is exact",
    );
    // a measured cost with an unnamed model
    inexact(&mut r.proposed[0]);
    r.proposed[0].exactness = Exactness::Measured { model: " ".into() };
    assert_rule(ledger.put(&Record::Replay(r.clone())), "name the model");
    // an empty bounded interval
    r.proposed[0].exactness = Exactness::Bounded { low: 5, high: 4 };
    assert_rule(ledger.put(&Record::Replay(r.clone())), "an empty interval");
    // bounded rows must record their upper bound
    inexact(&mut r.proposed[0]);
    r.proposed[0].rows = Some(Quantity {
        value: 3,
        exactness: Exactness::Bounded { low: 1, high: 9 },
    });
    assert_rule(
        ledger.put(&Record::Replay(r.clone())),
        "records its upper bound",
    );
    r.proposed[0].rows = Some(Quantity::exact(7));
    ledger.put(&Record::Replay(r.clone())).unwrap();
}
