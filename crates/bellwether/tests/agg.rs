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
    AggregateQuery, CulpritReason, Engine, EngineOptions, FixOutcome, FixRequest, KeyFile,
    ReconcileRequest, WhyRequest,
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

/// `why` on `keys` as one aggregate run `q`, then `fix` of the candidate
/// `want` names, to a receipt; returns the fix's certificate. The counted
/// key saves no bytes on its own when every row group holds a key (its
/// counts are what let the agg key answer), so its fix allows no gain.
fn why_then_fix(
    e: &Engine,
    keys: &Path,
    want: &KeySpec,
    allow_no_gain: bool,
    q: &AggregateQuery,
) -> bellwether::types::Certificate {
    let w = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(keys)),
            aggregate: Some(q.clone()),
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
    let cert = why_then_fix(&e, &kf, &counted, true, &query());
    assert!(cert.body.agg.is_empty(), "{:?}", cert.body.agg);
    let agg = parse("agg:account@amount{sum,count,min,max}").unwrap();
    let cert = why_then_fix(&e, &kf, &agg, false, &query());
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
    // why again: the agg key answers the aggregates and nothing is read,
    // so no layout or lint is the culprit.
    let again = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&kf)),
            aggregate: Some(query()),
            ..WhyRequest::default()
        })
        .unwrap();
    assert_eq!(again.current.unwrap().row_groups, 0);
    let culprit = again.culprit.as_ref().expect("a culprit");
    assert_eq!(
        culprit.reason,
        CulpritReason::AggAnswered { spec: agg.clone() },
        "{culprit:?}"
    );
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

    // The planted file replaced by the right one and the index updated:
    // the retired row groups' cells (one of them a millionth off) still sit
    // in the old segments and must not count. Both sides are answered from
    // their agg keys again, with every live row and the oracle's total.
    std::fs::copy(right.join("part-001.parquet"), bad.join("part-001.parquet")).unwrap();
    index_agg(&bad);
    let r = l
        .reconcile(bad.to_str().unwrap(), &reconcile_req())
        .unwrap();
    assert!(r.outcome.pass, "{:?}", r.outcome.mismatches);
    let sides: Vec<&str> = r.outcome.agg.iter().map(|a| a.side.as_str()).collect();
    assert_eq!(sides, ["left", "right"]);
    assert_eq!(r.outcome.agg[1].rows, 240);
    assert_eq!(r.outcome.memory.rows_read, [0, 0]);
    assert_eq!(r.outcome.totals[0].right.render(), dec6(total));
}

/// A side holding rows whose account is null is read, never answered from
/// its agg key: those rows are in no cell (ADR 014), so the key's rows are
/// not every row of the side, and a total from the cells would miss them.
#[test]
fn a_side_with_null_keys_is_read_not_answered_from_its_agg_key() {
    let dir = tempfile::tempdir().unwrap();
    let (left, right) = (dir.path().join("left"), dir.path().join("right"));
    let null_every_50th = |r: &mut AggRow| {
        if r.2 % 50 == 7 {
            r.0 = None;
        }
    };
    corpus_with(&left, 2, 3, 40, null_every_50th);
    corpus_with(&right, 2, 3, 40, null_every_50th);
    index_agg(&left);
    index_agg(&right);
    let r = open(&left)
        .reconcile(right.to_str().unwrap(), &reconcile_req())
        .unwrap();
    assert!(r.outcome.pass, "{:?}", r.outcome.mismatches);
    assert_eq!(r.outcome.agg.len(), 0, "{:?}", r.outcome.agg);
    assert_eq!(r.outcome.memory.rows_read, [240, 240]);
    let Record::Certificate(c) = &r.certificate.record else {
        panic!()
    };
    assert_eq!(c.body.agg.len(), 0);
    // Each side says why its agg key was not used, with the count.
    let sides: Vec<&str> = r
        .outcome
        .agg_not_used
        .iter()
        .map(|n| n.side.as_str())
        .collect();
    assert_eq!(sides, ["left", "right"], "{:?}", r.outcome.agg_not_used);
    for n in &r.outcome.agg_not_used {
        assert_eq!(n.spec, "agg:account@amount{sum,count}");
        assert_eq!(
            n.causes,
            [
                "5 of its 240 rows have a null account and are in no agg key (ADR 014), so the \
              rows are read to compare them"
            ],
        );
        assert_eq!(n.next, None);
    }
    assert_eq!(r.outcome.sides[0].null_key_rows, 5);
    assert_eq!(r.outcome.sides[1].null_key_rows, 5);
    // R-agg-1 never recommends a key that could not answer this side.
    assert!(r.lints.iter().all(|l| l.rule != "R-agg-1"), "{:?}", r.lints);
}

/// A reconcile that compares the total row by row (`--ignore entry`, not
/// `amount`) cannot be answered from an agg key: each side says so, and
/// names the `--ignore` list that would let it.
#[test]
fn a_compared_total_is_read_and_the_next_step_names_the_ignore_list() {
    let dir = tempfile::tempdir().unwrap();
    let (left, right) = (dir.path().join("left"), dir.path().join("right"));
    corpus_with(&left, 1, 2, 30, |_| {});
    corpus_with(&right, 1, 2, 30, |_| {});
    index_agg(&left);
    index_agg(&right);
    let req = ReconcileRequest {
        ignore: vec!["entry".into()],
        ..reconcile_req()
    };
    let r = open(&left)
        .reconcile(right.to_str().unwrap(), &req)
        .unwrap();
    assert!(r.outcome.pass, "{:?}", r.outcome.mismatches);
    assert!(r.outcome.agg.is_empty(), "{:?}", r.outcome.agg);
    assert_eq!(r.outcome.memory.rows_read, [60, 60]);
    let sides: Vec<&str> = r
        .outcome
        .agg_not_used
        .iter()
        .map(|n| n.side.as_str())
        .collect();
    assert_eq!(sides, ["left", "right"], "{:?}", r.outcome.agg_not_used);
    for n in &r.outcome.agg_not_used {
        assert_eq!(
            n.causes,
            [
                "amount is compared row by row, and the agg key holds only each key's rows and \
              the total of amount"
            ]
        );
        let next = n.next.as_deref().unwrap();
        assert!(next.contains("--ignore amount,entry"), "{next}");
        assert!(
            next.contains("compares amount only through its exact total"),
            "{next}"
        );
    }
    // Following it, both sides are answered from their agg keys.
    let r = open(&left)
        .reconcile(right.to_str().unwrap(), &reconcile_req())
        .unwrap();
    assert_eq!(r.outcome.memory.rows_read, [0, 0]);
    assert!(
        r.outcome.agg_not_used.is_empty(),
        "{:?}",
        r.outcome.agg_not_used
    );
}

/// A side whose files changed since its index was built is read, never
/// answered from its stale agg key nor refused: the reconcile names the
/// changed file and the `index` run that brings the index up to date.
#[test]
fn a_stale_agg_side_is_read_and_names_the_index_run() {
    let dir = tempfile::tempdir().unwrap();
    let (left, right) = (dir.path().join("left"), dir.path().join("right"));
    corpus_with(&left, 2, 2, 30, |_| {});
    corpus_with(&right, 2, 2, 30, |_| {});
    index_agg(&left);
    index_agg(&right);
    // The right side's second file rewritten with one amount a millionth
    // more, after its index was built.
    corpus_with(&dir.path().join("next"), 2, 2, 30, |r| {
        if r.2 == 75 {
            r.1 = Some(r.1.unwrap() + 1);
        }
    });
    std::fs::copy(
        dir.path().join("next").join("part-001.parquet"),
        right.join("part-001.parquet"),
    )
    .unwrap();
    let r = open(&left)
        .reconcile(right.to_str().unwrap(), &reconcile_req())
        .unwrap();
    // The planted difference is found: the right side's rows were read.
    assert!(!r.outcome.pass);
    assert_eq!(r.outcome.counts.totals_differ, 1, "{:?}", r.outcome.counts);
    let sides: Vec<&str> = r.outcome.agg.iter().map(|a| a.side.as_str()).collect();
    assert_eq!(sides, ["left"]);
    assert_eq!(r.outcome.memory.rows_read, [0, 120]);
    let [n] = r.outcome.agg_not_used.as_slice() else {
        panic!("{:?}", r.outcome.agg_not_used)
    };
    assert_eq!(n.side, "right");
    assert_eq!(n.spec, "agg:account@amount{sum,count}");
    assert_eq!(n.causes.len(), 1, "{:?}", n.causes);
    assert!(
        n.causes[0].starts_with("a file was changed since the index was built (")
            && n.causes[0].contains("part-001.parquet"),
        "{:?}",
        n.causes
    );
    assert_eq!(
        n.next.as_deref(),
        Some(
            format!(
                "run `bellwether index {}` to bring the index up to date",
                right.canonicalize().unwrap().display()
            )
            .as_str()
        )
    );
}

/// A float column holding infinities and NaN: sums follow IEEE 754 across
/// row groups (`[inf, 1]` is `inf`, `[-inf, inf]` and anything with NaN
/// are `NaN`, `-0.0 + 0.0` prints `0.0` as a float does), `fix` certifies
/// its agg key
/// (cells compare bit for bit), and every float sum is labelled measured.
#[test]
fn a_float_column_with_inf_and_nan_certifies_and_sums_are_measured() {
    use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use bellwether::types::Exactness;
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("account", DataType::Int64, false),
        Field::new("v", DataType::Float64, true),
    ]));
    // Two row groups; per account: one value in each.
    let groups: [[f64; 5]; 2] = [
        [f64::INFINITY, f64::NAN, 1.5, f64::NEG_INFINITY, -0.0],
        [1.0, 2.0, 2.25, f64::INFINITY, 0.0],
    ];
    let props = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_row_count(Some(usize::MAX))
        .build();
    let mut w = parquet::arrow::ArrowWriter::try_new(
        std::fs::File::create(data.join("part-000.parquet")).unwrap(),
        Arc::clone(&schema),
        Some(props),
    )
    .unwrap();
    for g in groups {
        let b = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
                Arc::new(Float64Array::from(g.to_vec())),
            ],
        )
        .unwrap();
        w.write(&b).unwrap();
        w.flush().unwrap();
    }
    w.close().unwrap();
    let kf = dir.path().join("k.parquet");
    write_keys(&kf, "account", &[1, 2, 3, 4, 5]);
    let q = AggregateQuery {
        value: "v".into(),
        functions: vec![AggregateFn::Sum, AggregateFn::Count],
    };
    let e = open(&data);
    why_then_fix(&e, &kf, &parse("exact:account,counts").unwrap(), true, &q);
    let agg = parse("agg:account@v{sum,count}").unwrap();
    let cert = why_then_fix(&e, &kf, &agg, false, &q);
    assert!(cert.pass);
    let check = cert.body.agg[0].checked.as_ref().unwrap();
    assert_eq!((check.keys, check.differ), (5, 0));

    let a = e.aggregate(&KeyFile::new(&kf), "v", &q.functions).unwrap();
    assert!(a.complete);
    let sums: Vec<(String, Option<String>)> = a
        .groups
        .iter()
        .map(|g| {
            let s = g.sum.as_ref().unwrap();
            assert_eq!(
                s.exactness,
                Exactness::Measured {
                    model: bellwether::types::AGG_FLOAT_SUM_MODEL.to_string()
                },
                "{g:?}"
            );
            (g.key[0].clone().unwrap(), s.value.clone())
        })
        .collect();
    let want = [
        ("1", "inf"),
        ("2", "NaN"),
        ("3", "3.75"),
        ("4", "NaN"),
        ("5", "0.0"),
    ];
    assert_eq!(
        sums,
        want.map(|(k, v)| (k.to_string(), Some(v.to_string())))
            .to_vec()
    );
}

/// Writes one file of `(k: Int64, v: Decimal128(38, 0))`, one row group
/// per entry of `groups`.
fn write_wide(path: &Path, groups: &[Vec<(i64, i128)>]) {
    use arrow::array::{ArrayRef, Decimal128Array, Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Decimal128(38, 0), false),
    ]));
    let props = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_row_count(Some(usize::MAX))
        .build();
    let mut w = parquet::arrow::ArrowWriter::try_new(
        std::fs::File::create(path).unwrap(),
        Arc::clone(&schema),
        Some(props),
    )
    .unwrap();
    for g in groups {
        let b = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(g.iter().map(|r| r.0))) as ArrayRef,
                Arc::new(
                    Decimal128Array::from_iter_values(g.iter().map(|r| r.1))
                        .with_precision_and_scale(38, 0)
                        .unwrap(),
                ),
            ],
        )
        .unwrap();
        w.write(&b).unwrap();
        w.flush().unwrap();
    }
    w.close().unwrap();
}

/// Key 1 holds 10^38 − 1 in each of two row groups (and key 2 its
/// negation), so its sum fits each cell but overflows 128 bits across
/// them: lookups that do not ask for the sum still answer; one that does
/// is refused naming the agg key, the column, the key, and the functions
/// to ask instead; a reconcile reads that side (the grand total, 0, is
/// exact) and says why.
#[test]
fn a_sum_overflowing_across_row_groups_fails_only_the_sum() {
    const BIG: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;
    let dir = tempfile::tempdir().unwrap();
    let (left, right) = (dir.path().join("left"), dir.path().join("right"));
    for d in [&left, &right] {
        std::fs::create_dir_all(d).unwrap();
        write_wide(
            &d.join("part-000.parquet"),
            &[vec![(1, BIG), (2, -BIG)], vec![(1, BIG), (2, -BIG), (3, 7)]],
        );
    }
    open(&left)
        .index(&[
            parse("exact:k,counts").unwrap(),
            parse("agg:k@v{sum,count,max}").unwrap(),
        ])
        .unwrap();
    let kf = dir.path().join("k.parquet");
    write_keys(&kf, "k", &[1, 3]);
    let e = open(&left);
    let a = e
        .aggregate(
            &KeyFile::new(&kf),
            "v",
            &[AggregateFn::Count, AggregateFn::Max],
        )
        .unwrap();
    assert!(a.complete);
    let got: Vec<(u64, Option<String>)> = a
        .groups
        .iter()
        .map(|g| (g.count, g.max.as_ref().unwrap().value.clone()))
        .collect();
    assert_eq!(
        got,
        [(2, Some(BIG.to_string())), (1, Some("7".to_string()))]
    );
    let err = e
        .aggregate(&KeyFile::new(&kf), "v", &[AggregateFn::Sum])
        .unwrap_err()
        .to_string();
    assert!(err.contains("agg:k@v{sum,count,max}"), "{err}");
    assert!(err.contains("the sum of v for key (1)"), "{err}");
    assert!(err.contains("--fn count,max"), "{err}");

    let req = ReconcileRequest {
        key: vec!["k".into()],
        ignore: vec!["v".into()],
        totals: vec!["v".into()],
        ..ReconcileRequest::default()
    };
    let r = e.reconcile(right.to_str().unwrap(), &req).unwrap();
    assert!(r.outcome.pass, "{:?}", r.outcome.mismatches);
    assert!(r.outcome.agg.is_empty(), "{:?}", r.outcome.agg);
    assert_eq!(r.outcome.memory.rows_read, [5, 5]);
    assert_eq!(r.outcome.totals[0].left.render(), "7");
    let [n] = r.outcome.agg_not_used.as_slice() else {
        panic!("{:?}", r.outcome.agg_not_used)
    };
    assert_eq!(n.side, "left");
    assert_eq!(
        n.causes,
        [
            "a key's total of v overflows the agg key's 128-bit sum, so the rows are read to \
          total it exactly"
        ]
    );
}

/// The left side indexed without an agg key, at a later manifest than the
/// right side's agg key: the right is answered from its agg key and the
/// certificate is anchored at the left's manifest, the right's cited as an
/// input.
#[test]
fn an_agg_side_on_the_right_at_another_manifest_than_the_left() {
    let dir = tempfile::tempdir().unwrap();
    let (left, right) = (dir.path().join("left"), dir.path().join("right"));
    corpus_with(&left, 1, 2, 30, |_| {});
    corpus_with(&right, 1, 2, 30, |_| {});
    let l = open(&left);
    l.index(&[parse("exact:account,counts").unwrap()]).unwrap();
    l.index(&[parse("exact:entry").unwrap()]).unwrap();
    index_agg(&right);
    let r = open(&left)
        .reconcile(right.to_str().unwrap(), &reconcile_req())
        .unwrap();
    assert!(r.outcome.pass, "{:?}", r.outcome.mismatches);
    let sides: Vec<&str> = r.outcome.agg.iter().map(|a| a.side.as_str()).collect();
    assert_eq!(sides, ["right"]);
    assert_eq!(r.outcome.memory.rows_read, [60, 0]);
    let Record::Certificate(c) = &r.certificate.record else {
        panic!()
    };
    assert_eq!(c.provenance.manifest_version.as_deref(), Some("2"));
}
