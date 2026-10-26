//! Join keys (B3) through the facade, on two folders beside each other:
//! `index` declaring and refreshing the join key (a missing exact key on
//! either side refused naming the command), `semijoin` and `join_overlap`
//! against an oracle that decodes both datasets' files, freshness on
//! either side (`stale_join` with the refresh command, `join_verify`), and
//! the loop why --mode semijoin -> fix -> receipt -> semijoin, where the
//! plan after the fix is the one why priced.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::cast_possible_wrap,
    missing_docs
)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
use bellwether::types::{Error, KeySpec, Record};
use bellwether::{
    Engine, EngineOptions, FixOutcome, FixRequest, JoinFreshness, KeyFile, LookupKind, LookupMode,
    WhyRequest,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

const CLOCK: &str = "2026-10-26T12:00:00.000Z";

fn schema(key: &str) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(key, DataType::Int64, true),
        Field::new("amount", DataType::Int64, true),
    ]))
}

fn write(path: &Path, key: &str, ids: &[Option<i64>], per_rg: usize) {
    let batch = RecordBatch::try_new(
        schema(key),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from((0..ids.len() as i64).collect::<Vec<_>>())),
        ],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(per_rg))
        .build();
    let mut w =
        ArrowWriter::try_new(File::create(path).unwrap(), schema(key), Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// Orders 0..300 clustered, 10 per row group; payables for 150..450 with
/// 1:N skew (id % 3 + 1 rows each), shuffled over row groups of 30.
fn corpus(root: &Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let (l, r) = (root.join("orders"), root.join("payables"));
    std::fs::create_dir_all(&l).unwrap();
    std::fs::create_dir_all(&r).unwrap();
    for f in 0..3i64 {
        let ids: Vec<Option<i64>> = (f * 100..(f + 1) * 100).map(Some).collect();
        write(&l.join(format!("o-{f}.parquet")), "order_id", &ids, 10);
    }
    let mut pay = Vec::new();
    for id in 150..450i64 {
        for _ in 0..=(id % 3) {
            pay.push(Some(id));
        }
    }
    let n = pay.len();
    let shuffled: Vec<Option<i64>> = (0..n).map(|i| pay[(i * 53) % n]).collect();
    let half = n / 2;
    write(&r.join("p-0.parquet"), "paid_order", &shuffled[..half], 30);
    write(&r.join("p-1.parquet"), "paid_order", &shuffled[half..], 30);
    (
        std::fs::canonicalize(l).unwrap(),
        std::fs::canonicalize(r).unwrap(),
    )
}

/// Every (file, row group) holding each key, decoded with `parquet`.
fn decode(dir: &Path, key: &str) -> BTreeMap<i64, BTreeSet<(String, usize)>> {
    let mut out: BTreeMap<i64, BTreeSet<(String, usize)>> = BTreeMap::new();
    let mut paths: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
        .collect();
    paths.sort();
    for p in paths {
        let b = ParquetRecordBatchReaderBuilder::try_new(File::open(&p).unwrap()).unwrap();
        let n = b.metadata().num_row_groups();
        for rg in 0..n {
            let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&p).unwrap())
                .unwrap()
                .with_row_groups(vec![rg])
                .build()
                .unwrap();
            for batch in reader {
                let batch = batch.unwrap();
                let col = batch
                    .column_by_name(key)
                    .unwrap()
                    .as_primitive::<Int64Type>();
                for v in col.iter().flatten() {
                    out.entry(v)
                        .or_default()
                        .insert((p.to_str().unwrap().to_string(), rg));
                }
            }
        }
    }
    out
}

fn engine(data: &Path) -> Engine {
    Engine::open(
        data.to_str().unwrap(),
        EngineOptions {
            clock: Some(CLOCK.into()),
            ..EngineOptions::default()
        },
    )
    .unwrap()
}

fn exact(c: &str) -> KeySpec {
    bellwether::spec::parse(&format!("exact:{c}")).unwrap()
}

fn join() -> KeySpec {
    bellwether::spec::parse("join:orders.order_id=payables.paid_order").unwrap()
}

fn write_keys(path: &Path, keys: &[Option<i64>]) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "order_id",
        DataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(keys.to_vec())) as ArrayRef],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// The planned (file, row group) cells of a plan Evidence.
fn cells(w: &bellwether::Written) -> BTreeSet<(String, usize)> {
    let Record::Evidence(e) = &w.record else {
        panic!("{:?}", w.record)
    };
    let bellwether::types::EvidenceBody::Plan { plan } = &e.body else {
        panic!()
    };
    plan.entries
        .iter()
        .flat_map(|e| {
            e.row_groups
                .iter()
                .map(move |rg| (e.file.path.clone(), rg as usize))
        })
        .collect()
}

#[test]
fn index_declares_the_join_key_and_refuses_a_missing_exact_key_with_its_command() {
    let dir = tempfile::tempdir().unwrap();
    let (l, r) = corpus(dir.path());
    let left = engine(&l);
    // Never indexed, only the join key: the exact key is named.
    let err = left.index(&[join()]).unwrap_err();
    assert!(err.to_string().contains("--key exact:order_id"), "{err}");
    // The right dataset has no index.
    let err = left.index(&[exact("order_id"), join()]).unwrap_err();
    match &err {
        Error::NotIndexed {
            root, needs: None, ..
        } => assert_eq!(root, r.to_str().unwrap()),
        other => panic!("{other:?}"),
    }
    // Indexed without the right exact key: named, with its root.
    engine(&r).index(&[exact("amount")]).unwrap();
    let err = left.index(&[join()]).unwrap_err();
    match &err {
        Error::NotIndexed {
            root,
            needs: Some(n),
            ..
        } => {
            assert_eq!(root, r.to_str().unwrap());
            assert_eq!(n, "exact:paid_order");
        }
        other => panic!("{other:?}"),
    }
    engine(&r).index(&[exact("paid_order")]).unwrap();
    let before = left.index(&[]).unwrap().sequence;
    let done = left.index(&[join()]).unwrap();
    assert_eq!(
        done.committed,
        Some(before + 1),
        "declared and built in one commit"
    );
    assert_eq!(done.joins.len(), 1);
    let j = &done.joins[0];
    assert!(j.refreshed);
    assert_eq!(j.spec, join());
    assert_eq!(j.right_root, r.to_str().unwrap());
    assert!(matches!(j.state, JoinFreshness::Fresh { .. }), "{j:?}");
    assert!(j.next.is_none());
    // Again: fresh, nothing committed.
    let again = left.index(&[]).unwrap();
    assert_eq!(again.committed, None);
    assert!(!again.joins[0].refreshed);
    // Another left folder name is refused.
    let err = left
        .index(&[bellwether::spec::parse("join:invoices.order_id=payables.paid_order").unwrap()])
        .unwrap_err();
    assert!(err.to_string().contains("the folder \"orders\""), "{err}");
    // A right folder that does not exist.
    let err = left
        .index(&[bellwether::spec::parse("join:orders.order_id=missing.paid_order").unwrap()])
        .unwrap_err();
    assert!(err.to_string().contains("beside the data root"), "{err}");
}

fn ready() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let (l, r) = corpus(dir.path());
    engine(&r).index(&[exact("paid_order")]).unwrap();
    engine(&l).index(&[exact("order_id"), join()]).unwrap();
    (dir, l, r)
}

#[test]
fn semijoin_and_overlap_equal_the_decoded_files() {
    let (dir, l, r) = ready();
    let (lk, rk) = (decode(&l, "order_id"), decode(&r, "paid_order"));
    let left = engine(&l);
    let asked = [
        Some(3),
        Some(149),
        Some(150),
        Some(299),
        Some(300),
        Some(444),
        None,
    ];
    let keys = dir.path().join("keys.parquet");
    write_keys(&keys, &asked);
    // Named by its right side or whole: one answer.
    let a = left
        .semijoin("payables.paid_order", &KeyFile::new(&keys))
        .unwrap();
    let b = left
        .semijoin(
            "join:orders.order_id=payables.paid_order",
            &KeyFile::new(&keys),
        )
        .unwrap();
    assert_eq!(a, b);
    let matched: Vec<i64> = asked
        .iter()
        .flatten()
        .copied()
        .filter(|k| lk.contains_key(k) && rk.contains_key(k))
        .collect();
    assert_eq!(a.keys, 6);
    assert_eq!(a.null_tuples, 1);
    assert_eq!(a.matched, matched.len() as u64);
    let need = |m: &BTreeMap<i64, BTreeSet<(String, usize)>>| {
        matched
            .iter()
            .flat_map(|k| m[k].iter().cloned())
            .collect::<BTreeSet<_>>()
    };
    assert_eq!(cells(&a.left.evidence), need(&lk), "left step");
    assert_eq!(cells(&a.right.evidence), need(&rk), "right step");
    assert_eq!(a.left.root, l.to_str().unwrap());
    assert_eq!(a.right.root, r.to_str().unwrap());
    assert_eq!(a.right.columns, vec!["paid_order".to_string()]);

    let o = left.join_overlap("payables.paid_order").unwrap();
    let both: Vec<&i64> = lk.keys().filter(|k| rk.contains_key(*k)).collect();
    assert_eq!(o.both, both.len() as u64);
    assert_eq!(o.left_only, (lk.len() - both.len()) as u64);
    assert_eq!(o.right_only, (rk.len() - both.len()) as u64);
    assert_eq!(
        o.max_right_row_groups,
        both.iter().map(|k| rk[*k].len() as u64).max().unwrap()
    );
    assert_eq!(
        o.sum_right_row_groups,
        both.iter().map(|k| rk[*k].len() as u64).sum::<u64>()
    );
    assert!(o.rows.is_none(), "an uncounted join key has no rows");

    // A key file on other columns, or a join key not declared.
    let err = left
        .semijoin("payables.amount", &KeyFile::new(&keys))
        .unwrap_err();
    assert!(
        matches!(&err, Error::NotIndexed { needs: Some(n), .. } if n == "join:orders.order_id=payables.amount"),
        "{err:?}"
    );
    let err = left.semijoin("payables", &KeyFile::new(&keys)).unwrap_err();
    assert!(
        matches!(err, Error::InvalidArgument { field: "join", .. }),
        "{err:?}"
    );
}

#[test]
fn a_stale_join_key_is_refused_with_the_refresh_command_on_either_side() {
    let (dir, l, r) = ready();
    let keys = dir.path().join("keys.parquet");
    write_keys(&keys, &[Some(160), Some(5)]);
    let left = engine(&l);
    assert!(left.join_verify(None).unwrap().pass);
    // The right commits again.
    write(&r.join("p-9.parquet"), "paid_order", &[Some(5)], 30);
    let err = left
        .semijoin("payables.paid_order", &KeyFile::new(&keys))
        .unwrap_err();
    // Not indexed yet on the right: its index is stale against its files.
    assert!(
        matches!(&err, Error::StaleIndex { message, .. }
            if message.contains(&format!("bellwether index {}", r.to_str().unwrap()))),
        "{err:?}"
    );
    engine(&r).index(&[]).unwrap();
    let err = left
        .semijoin("payables.paid_order", &KeyFile::new(&keys))
        .unwrap_err();
    match &err {
        Error::StaleJoin {
            spec,
            side,
            pinned,
            current,
            message,
        } => {
            assert_eq!(spec, &join().name());
            assert_eq!(*side, "right");
            assert!(current > pinned);
            assert!(
                message.contains(&format!("`bellwether index {}`", l.to_str().unwrap())),
                "{message}"
            );
        }
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        left.join_overlap("payables.paid_order").unwrap_err(),
        Error::StaleJoin { side: "right", .. }
    ));
    let v = left.join_verify(None).unwrap();
    assert!(!v.pass);
    assert!(matches!(v.joins[0].state, JoinFreshness::StaleRight { .. }));
    assert!(v.joins[0]
        .next
        .as_deref()
        .unwrap()
        .starts_with("bellwether index "));
    // `index` on the left refreshes it; 5 is now on both sides.
    let done = left.index(&[]).unwrap();
    assert!(done.joins[0].refreshed);
    let a = left
        .semijoin("payables.paid_order", &KeyFile::new(&keys))
        .unwrap();
    assert_eq!(a.matched, 2);
    // The left's row groups change: stale on the left once indexed.
    write(&l.join("o-9.parquet"), "order_id", &[Some(1000)], 10);
    assert!(matches!(
        left.semijoin("payables.paid_order", &KeyFile::new(&keys))
            .unwrap_err(),
        Error::StaleIndex { .. }
    ));
    // A build that commits the new file leaves the join key stale until
    // its refresh, which the same `index` runs.
    let done = left.index(&[]).unwrap();
    assert!(done.joins[0].refreshed);
    assert!(left.join_verify(None).unwrap().pass);
}

#[test]
fn why_prices_the_join_key_and_fix_applies_it_with_both_certificates() {
    let (dir, l, r) = ready();
    // A left index without the join key yet.
    let left = engine(&l);
    let fresh = tempfile::tempdir().unwrap();
    let (fl, fr) = corpus(fresh.path());
    engine(&fr).index(&[exact("paid_order")]).unwrap();
    let fleft = engine(&fl);
    fleft.index(&[exact("order_id")]).unwrap();
    let keys = dir.path().join("keys.parquet");
    let asked: Vec<Option<i64>> = (0..40).map(|i| Some(i * 11)).collect();
    write_keys(&keys, &asked);
    let w = fleft
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&keys)),
            mode: Some(LookupMode::SemiJoin),
            join: Some("payables.paid_order".into()),
            ..WhyRequest::default()
        })
        .unwrap();
    let info = w.workload.as_ref().unwrap();
    assert_eq!(info.query_kind, "semijoin");
    assert_eq!(
        info.lookup,
        Some(LookupKind::SemiJoin {
            right: "payables".into(),
            right_columns: vec!["paid_order".into()],
        })
    );
    let floor = &w.candidates[0];
    assert_eq!(floor.spec, join());
    let kc = floor.kind_column.as_ref().unwrap();
    assert_eq!(kc.kind, "join");
    let (lk, rk) = (decode(&fl, "order_id"), decode(&fr, "paid_order"));
    let matched: Vec<i64> = asked
        .iter()
        .flatten()
        .copied()
        .filter(|k| lk.contains_key(k) && rk.contains_key(k))
        .collect();
    assert!(
        kc.value.starts_with(&format!(
            "{} of {} keys on both",
            matched.len(),
            asked.len()
        )),
        "{}",
        kc.value
    );
    // Current: two locates (every asked key on each side); proposed: the
    // shared keys' row groups only.
    let locate_cells = |m: &BTreeMap<i64, BTreeSet<(String, usize)>>| {
        asked
            .iter()
            .flatten()
            .filter_map(|k| m.get(k))
            .flatten()
            .cloned()
            .collect::<BTreeSet<_>>()
            .len() as u64
    };
    let current = w.current.unwrap();
    assert_eq!(current.row_groups, locate_cells(&lk) + locate_cells(&rk));
    let shared = |m: &BTreeMap<i64, BTreeSet<(String, usize)>>| {
        matched
            .iter()
            .flat_map(|k| m[k].iter().cloned())
            .collect::<BTreeSet<_>>()
            .len() as u64
    };
    assert_eq!(floor.proposed.row_groups, shared(&lk) + shared(&rk));
    let rec = w.recommended.as_ref().unwrap();
    assert_eq!(rec.spec, join());
    let Record::Replay(replay) = &floor.replay.record else {
        panic!()
    };
    assert_eq!(replay.inputs.len(), 2, "both catalogs are cited");

    // fix: score, both certificates, apply, attest.
    let fixed = fleft
        .fix(&FixRequest {
            proposal: Some(rec.proposal),
            workload: Some(info.hash),
            ..FixRequest::default()
        })
        .unwrap();
    let FixOutcome::Applied {
        receipt, realized, ..
    } = &fixed.outcome
    else {
        panic!("{:?}", fixed.outcome)
    };
    let check = fixed.join.as_ref().unwrap();
    assert_eq!(check.matched, matched.len() as u64);
    assert_eq!(check.right_root, fr.to_str().unwrap());
    let Record::Certificate(right_cert) = &check.certificate.record else {
        panic!()
    };
    assert!(right_cert.pass);
    let Record::Certificate(left_cert) = &fixed.certificate.as_ref().unwrap().record else {
        panic!()
    };
    assert!(left_cert.pass);
    assert!(
        left_cert
            .provenance
            .inputs
            .contains(&check.certificate.hash),
        "the left certificate cites the right one"
    );
    let Record::Receipt(rc) = &receipt.record else {
        panic!()
    };
    assert_eq!(rc.certificate, fixed.certificate.as_ref().unwrap().hash);
    // The realized replay is the predicted one, and the plan is the price.
    let Some(realized) = realized else { panic!() };
    let Record::Replay(after) = &realized.record else {
        panic!()
    };
    assert_eq!(after.current, replay.proposed);
    let a = fleft
        .semijoin("payables.paid_order", &KeyFile::new(&keys))
        .unwrap();
    assert_eq!(
        a.left.planned.row_groups + a.right.planned.row_groups,
        floor.proposed.row_groups
    );
    assert!(fleft.join_verify(None).unwrap().pass);
    // Applying again: nothing to apply.
    let again = fleft
        .fix(&FixRequest {
            proposal: Some(rec.proposal),
            workload: Some(info.hash),
            ..FixRequest::default()
        })
        .unwrap();
    assert!(
        matches!(
            &again.outcome,
            FixOutcome::Refused {
                refusal: bellwether::Refusal::NothingToApply { .. }
            }
        ),
        "{:?}",
        again.outcome
    );
    let _ = (left, l, r);
}

/// A certificate of a join key declared since, applied after the right
/// dataset advanced: refused `stale_join` with the refresh command (dry
/// run and real apply alike), never `nothing_to_apply` because the left
/// side reads complete.
#[test]
fn applying_a_certificate_of_a_stale_declared_join_key_is_refused_stale_join() {
    let dir = tempfile::tempdir().unwrap();
    let (l, r) = corpus(dir.path());
    engine(&r).index(&[exact("paid_order")]).unwrap();
    let left = engine(&l);
    left.index(&[exact("order_id")]).unwrap();
    let keys = dir.path().join("keys.parquet");
    let asked: Vec<Option<i64>> = (0..40).map(|i| Some(i * 11)).collect();
    write_keys(&keys, &asked);
    let w = left
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&keys)),
            mode: Some(LookupMode::SemiJoin),
            join: Some("payables.paid_order".into()),
            ..WhyRequest::default()
        })
        .unwrap();
    let rec = w.recommended.as_ref().unwrap();
    assert_eq!(rec.spec, join());
    let workload = w.workload.as_ref().unwrap().hash;
    // The join key is declared by `index`, then certified (verify only) at
    // that sequence: the certificate is of the current left manifest.
    left.index(&[join()]).unwrap();
    let verified = left
        .fix(&FixRequest {
            proposal: Some(rec.proposal),
            workload: Some(workload),
            verify_only: true,
            ..FixRequest::default()
        })
        .unwrap();
    assert!(
        matches!(verified.outcome, FixOutcome::Verified),
        "{:?}",
        verified.outcome
    );
    let cert = verified.certificate.as_ref().unwrap().hash;
    // The right dataset commits again; the left is unchanged.
    write(&r.join("p-9.parquet"), "paid_order", &[Some(5)], 30);
    engine(&r).index(&[]).unwrap();
    let stale = |err: Error| match err {
        Error::StaleJoin { side, message, .. } => {
            assert_eq!(side, "right");
            assert!(
                message.contains(&format!("`bellwether index {}`", l.to_str().unwrap())),
                "{message}"
            );
        }
        other => panic!("{other:?}"),
    };
    stale(left.apply_dry_run(&cert).unwrap_err());
    stale(
        left.fix(&FixRequest {
            certificate: Some(cert),
            ..FixRequest::default()
        })
        .unwrap_err(),
    );
    // Following the refusal: `index` refreshes the key in a new left
    // commit, so this certificate is now of an older manifest.
    left.index(&[]).unwrap();
    let after = left.apply_dry_run(&cert).unwrap();
    assert!(
        matches!(
            &after.outcome,
            FixOutcome::Refused {
                refusal: bellwether::Refusal::StaleManifest { .. }
            }
        ),
        "{:?}",
        after.outcome
    );
}

#[test]
fn why_semijoin_refuses_what_it_cannot_price() {
    let dir = tempfile::tempdir().unwrap();
    let (l, r) = corpus(dir.path());
    let keys = dir.path().join("keys.parquet");
    write_keys(&keys, &[Some(1)]);
    let req = |join: Option<&str>, mode: Option<LookupMode>| WhyRequest {
        keys: Some(KeyFile::new(&keys)),
        mode,
        join: join.map(str::to_string),
        ..WhyRequest::default()
    };
    let left = engine(&l);
    // --join without the mode, the mode without --join.
    let e = req(Some("payables.paid_order"), None)
        .lookup_mode()
        .unwrap_err();
    assert!(e.contains("--mode semijoin"), "{e}");
    let e = req(None, Some(LookupMode::SemiJoin))
        .lookup_mode()
        .unwrap_err();
    assert!(e.contains("--join"), "{e}");
    // No left index: the exact key to declare.
    let err = left
        .why(&req(
            Some("payables.paid_order"),
            Some(LookupMode::SemiJoin),
        ))
        .unwrap_err();
    assert!(
        matches!(&err, Error::NotIndexed { needs: Some(n), .. } if n == "exact:order_id"),
        "{err:?}"
    );
    left.index(&[exact("order_id")]).unwrap();
    engine(&r).index(&[exact("amount")]).unwrap();
    // No right exact key: named with the right root.
    let err = left
        .why(&req(
            Some("payables.paid_order"),
            Some(LookupMode::SemiJoin),
        ))
        .unwrap_err();
    assert!(
        matches!(&err, Error::NotIndexed { root, needs: Some(n), .. }
            if n == "exact:paid_order" && root == r.to_str().unwrap()),
        "{err:?}"
    );
}

#[test]
fn repeated_semijoins_without_a_join_key_raise_r_join_1() {
    let dir = tempfile::tempdir().unwrap();
    let (l, r) = corpus(dir.path());
    engine(&r).index(&[exact("paid_order")]).unwrap();
    let left = engine(&l);
    left.index(&[exact("order_id")]).unwrap();
    let (a, b) = (dir.path().join("a.parquet"), dir.path().join("b.parquet"));
    write_keys(&a, &[Some(5), Some(160)]);
    write_keys(&b, &[Some(200), Some(400), Some(410)]);
    let req = |more: bool| WhyRequest {
        keys: Some(KeyFile::new(&a)),
        more_keys: if more {
            vec![KeyFile::new(&b)]
        } else {
            Vec::new()
        },
        mode: Some(LookupMode::SemiJoin),
        join: Some("payables.paid_order".into()),
        ..WhyRequest::default()
    };
    let rules = |w: &bellwether::WhyReport| {
        w.profile
            .lints
            .iter()
            .filter(|l| l.rule == "R-join-1")
            .cloned()
            .collect::<Vec<_>>()
    };
    // Two runs on the pair, no join key: fires, naming the key and the
    // command, and the Evidence cites the workload it read.
    let w = left.why(&req(true)).unwrap();
    assert_eq!(w.workload.as_ref().unwrap().runs, 2);
    let lints = rules(&w);
    assert_eq!(lints.len(), 1, "{:?}", w.profile.lints);
    assert!(
        lints[0].message.contains(&join().name()),
        "{}",
        lints[0].message
    );
    assert!(
        lints[0].message.starts_with("2 recorded runs"),
        "{}",
        lints[0].message
    );
    assert!(w
        .evidence
        .evidence
        .provenance
        .inputs
        .contains(&w.workload.as_ref().unwrap().hash));
    // One run: not repeated.
    assert_eq!(rules(&left.why(&req(false)).unwrap()), vec![]);
    // With the join key: silent.
    left.index(&[join()]).unwrap();
    assert_eq!(rules(&left.why(&req(true)).unwrap()), vec![]);
    // A locate workload on the same column: silent.
    let plain = left
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&a)),
            more_keys: vec![KeyFile::new(&b)],
            ..WhyRequest::default()
        })
        .unwrap();
    assert_eq!(rules(&plain), vec![]);
}
