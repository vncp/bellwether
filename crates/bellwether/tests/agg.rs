//! Agg keys (B7) through the facade, against an oracle that reads the
//! files with the `parquet` crate: the why → fix → receipt → aggregate
//! round trip, exact decimal sums with nulls (ADR 014), and reconcile with
//! and without agg keys, including a planted one-unit difference.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::path::Path;

use bellwether::spec::parse;
use bellwether::types::{AggregateFn, KeySpec, ProposalBody, Record};
use bellwether::{
    AggregateQuery, Engine, EngineOptions, FixOutcome, FixRequest, KeyFile, ReconcileRequest,
    WhyRequest,
};
use common::{agg_corpus, agg_oracle, agg_row, dec6, write_agg_file, write_keys, AggRow};

const ACCOUNTS: i64 = 40;

fn open(dir: &Path) -> Engine {
    Engine::open(dir.to_str().unwrap(), EngineOptions::default()).unwrap()
}

fn query() -> AggregateQuery {
    AggregateQuery {
        value: "amount".into(),
        functions: vec![
            AggregateFn::Sum,
            AggregateFn::Count,
            AggregateFn::Min,
            AggregateFn::Max,
        ],
    }
}

/// `why` on `keys` as one aggregate run, then `fix` of the candidate
/// `want` names, to a receipt; returns the fix's certificate. The counted
/// key saves no bytes on its own when every row group holds a key (its
/// counts are what let the agg key answer), so its fix allows no gain.
fn why_then_fix(
    e: &Engine,
    keys: &Path,
    want: &KeySpec,
    allow_no_gain: bool,
) -> bellwether::types::Certificate {
    let w = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(keys)),
            aggregate: Some(query()),
            ..WhyRequest::default()
        })
        .unwrap();
    let wl = w.workload.as_ref().unwrap().hash;
    let c = w
        .candidates
        .iter()
        .find(|c| &c.spec == want)
        .unwrap_or_else(|| {
            panic!(
                "no candidate {}: {:?}",
                want.name(),
                w.candidates
                    .iter()
                    .map(|c| c.spec.name())
                    .collect::<Vec<_>>()
            )
        });
    let r = e
        .fix(&FixRequest {
            proposal: Some(c.proposal.hash),
            workload: Some(wl),
            allow_no_gain,
            ..FixRequest::default()
        })
        .unwrap();
    assert!(
        matches!(r.outcome, FixOutcome::Applied { .. }),
        "{}: {:?}",
        want.name(),
        r.outcome
    );
    let Record::Certificate(cert) = r.certificate.unwrap().record else {
        panic!("not a certificate")
    };
    cert
}

/// why proposes the counted key first, fix applies it; why then proposes
/// the agg key, fix checks its cells against the rows and applies it; the
/// aggregate it answers is the oracle's, every key complete, nothing read.
#[test]
fn why_fix_receipt_aggregate_round_trip_matches_the_decoded_files() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    agg_corpus(&data, 3, 4, 50, ACCOUNTS);
    let kf = dir.path().join("k.parquet");
    // The hot key 0, ordinary keys, and a key with no rows.
    write_keys(&kf, "account", &[0, 5, 17, 39, 1_000]);
    let e = open(&data);
    let counted = parse("exact:account,counts").unwrap();
    let cert = why_then_fix(&e, &kf, &counted, true);
    assert!(cert.body.agg.is_empty(), "{:?}", cert.body.agg);
    let agg = parse("agg:account@amount{sum,count,min,max}").unwrap();
    let cert = why_then_fix(&e, &kf, &agg, false);
    assert!(cert.pass);
    let [served] = cert.body.agg.as_slice() else {
        panic!("{:?}", cert.body.agg)
    };
    assert_eq!(served.side, "proposed");
    assert_eq!(served.specs, vec![agg.identity()]);
    let check = served.checked.as_ref().unwrap();
    assert_eq!((check.keys, check.differ), (5, 0));
    assert_eq!(check.row_groups_read, 12);
    let oracle = agg_oracle(&data);
    let rows: u64 = [0, 5, 17, 39].iter().map(|k| oracle[k].rows).sum();
    assert_eq!(served.rows, rows);

    let a = e
        .aggregate(&KeyFile::new(&kf), "amount", &query().functions)
        .unwrap();
    assert!(a.complete);
    assert_eq!(a.residual_row_groups, 0);
    assert_eq!(a.answered_by, agg.identity());
    for g in &a.groups {
        let k: i64 = g.key[0].as_deref().unwrap().parse().unwrap();
        let o = oracle.get(&k).cloned().unwrap_or_default();
        assert_eq!(g.rows, o.rows, "{k}");
        assert_eq!(g.nulls, o.nulls, "{k}");
        assert_eq!(g.count, o.rows - o.nulls, "{k}");
        let shown = |v: &Option<bellwether::AggValue>| v.as_ref().and_then(|v| v.value.clone());
        if o.rows == 0 {
            continue;
        }
        assert_eq!(shown(&g.sum), Some(dec6(o.sum)), "{k}");
        assert_eq!(shown(&g.min), o.min.map(dec6), "{k}");
        assert_eq!(shown(&g.max), o.max.map(dec6), "{k}");
    }
    // Null amounts are counted, never summed (asserted per key above).
    assert!(a.groups.iter().any(|g| g.nulls > 0));
}

/// Proposing the agg key before its counted key is complete is refused,
/// naming the propose step that comes first.
#[test]
fn an_agg_proposal_without_the_counted_key_names_the_propose_step_first() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    agg_corpus(&data, 1, 2, 30, ACCOUNTS);
    let kf = dir.path().join("k.parquet");
    write_keys(&kf, "account", &[1, 2]);
    let e = open(&data);
    let w = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&kf)),
            aggregate: Some(query()),
            ..WhyRequest::default()
        })
        .unwrap();
    // why proposes the counted key, not the agg key it cannot yet serve.
    assert!(w
        .candidates
        .iter()
        .all(|c| !matches!(c.spec, KeySpec::Agg { .. })));
    let ev = w.evidence.hash.unwrap();
    let err = e
        .propose(
            ProposalBody::Key {
                spec: parse("agg:account@amount{sum,count}").unwrap(),
                sidecars: Vec::new(),
            },
            &[ev],
            "test",
        )
        .unwrap_err()
        .to_string();
    // The propose step that comes first, citing the same evidence.
    assert!(err.contains("bellwether propose "), "{err}");
    assert!(
        err.contains(&format!(
            " --evidence {ev} --key 'exact:account,counts'` and apply it with `bellwether fix`"
        )),
        "{err}"
    );
}

/// Writes the corpus of [`agg_corpus`] with `change` applied to each row,
/// and every row's account set (a side with a null key is read, never
/// answered from its agg key).
fn corpus_with(dir: &Path, files: usize, rgs: usize, rows: usize, change: impl Fn(&mut AggRow)) {
    std::fs::create_dir_all(dir).unwrap();
    let mut k = 0i64;
    for f in 0..files {
        let groups: Vec<Vec<AggRow>> = (0..rgs)
            .map(|_| {
                (0..rows)
                    .map(|_| {
                        let mut r = agg_row(k, ACCOUNTS);
                        r.0 = r.0.or(Some(1));
                        change(&mut r);
                        k += 1;
                        r
                    })
                    .collect()
            })
            .collect();
        write_agg_file(&dir.join(format!("part-{f:03}.parquet")), &groups);
    }
}

/// By account, every other column but the total ignored: the reconcile an
/// agg key on (account, amount) can answer.
fn reconcile_req() -> ReconcileRequest {
    ReconcileRequest {
        key: vec!["account".into()],
        ignore: vec!["amount".into(), "entry".into()],
        totals: vec!["amount".into()],
        ..ReconcileRequest::default()
    }
}

fn index_agg(dir: &Path) {
    open(dir)
        .index(&[
            parse("exact:account,counts").unwrap(),
            parse("agg:account@amount{sum,count}").unwrap(),
        ])
        .unwrap();
}

/// Reconcile's totals are the oracle's exact decimal sums whether a side is
/// read or answered from its agg key; a planted one-unit difference fails
/// it both ways; the certificate names the agg keys only when they served,
/// and `R-agg-1` fires only on a side read where one would have.
#[test]
fn reconcile_with_and_without_agg_keys_catches_one_unit_and_says_which_side() {
    let dir = tempfile::tempdir().unwrap();
    let (left, right, bad) = (
        dir.path().join("left"),
        dir.path().join("right"),
        dir.path().join("bad"),
    );
    corpus_with(&left, 2, 3, 40, |_| {});
    corpus_with(&right, 2, 3, 40, |_| {});
    // One amount of account 5, one millionth more.
    corpus_with(&bad, 2, 3, 40, |r| {
        if r.2 == 125 {
            assert_eq!(r.0, Some(5));
            r.1 = Some(r.1.unwrap() + 1);
        }
    });
    let oracle = agg_oracle(&left);
    let total: i128 = oracle.values().map(|o| o.sum).sum();

    // Without agg keys: both sides read, the lint names the agg key.
    let l = open(&left);
    let r = l
        .reconcile(right.to_str().unwrap(), &reconcile_req())
        .unwrap();
    assert!(r.outcome.pass);
    assert_eq!(r.outcome.agg.len(), 0);
    let Record::Certificate(c) = &r.certificate.record else {
        panic!()
    };
    assert_eq!(c.body.agg.len(), 0);
    assert_eq!(r.outcome.totals.len(), 1);
    assert_eq!(r.outcome.totals[0].left.render(), dec6(total));
    assert!(r.lints.iter().any(|l| l.rule == "R-agg-1"), "{:?}", r.lints);
    let r = l
        .reconcile(bad.to_str().unwrap(), &reconcile_req())
        .unwrap();
    assert!(!r.outcome.pass);
    assert_eq!(r.outcome.totals[0].right.render(), dec6(total + 1));

    // With agg keys on both: neither side read, the certificate names both.
    index_agg(&left);
    index_agg(&right);
    index_agg(&bad);
    let l = open(&left);
    let r = l
        .reconcile(right.to_str().unwrap(), &reconcile_req())
        .unwrap();
    assert!(r.outcome.pass, "{:?}", r.outcome.mismatches);
    let sides: Vec<&str> = r.outcome.agg.iter().map(|a| a.side.as_str()).collect();
    assert_eq!(sides, ["left", "right"]);
    assert!(r.outcome.agg.iter().all(|a| a.checked.is_none()));
    assert_eq!(r.outcome.memory.rows_read, [0, 0]);
    let Record::Certificate(c) = &r.certificate.record else {
        panic!()
    };
    assert_eq!(c.body.agg, r.outcome.agg);
    assert_eq!(r.outcome.totals[0].left.render(), dec6(total));
    assert!(r.lints.iter().all(|l| l.rule != "R-agg-1"), "{:?}", r.lints);
    let r = l
        .reconcile(bad.to_str().unwrap(), &reconcile_req())
        .unwrap();
    assert!(!r.outcome.pass);
    assert_eq!(r.outcome.agg.len(), 2);
    assert_eq!(r.outcome.totals[0].right.render(), dec6(total + 1));
    let Record::Certificate(c) = &r.certificate.record else {
        panic!()
    };
    assert!(!c.pass);
    assert_eq!(c.body.totals_delta.len(), 1);

    // One side indexed, the other not: only that side is named.
    let plain = dir.path().join("plain");
    corpus_with(&plain, 2, 3, 40, |_| {});
    let r = l
        .reconcile(plain.to_str().unwrap(), &reconcile_req())
        .unwrap();
    assert!(r.outcome.pass);
    let sides: Vec<&str> = r.outcome.agg.iter().map(|a| a.side.as_str()).collect();
    assert_eq!(sides, ["left"]);
}
