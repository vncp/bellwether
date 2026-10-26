//! Join keys (B3) built into the left index and planned, against an
//! independent oracle: every file of both datasets decoded with the
//! `parquet` crate.
//!
//! - Invariant 1 per side: for random key sets, every left row and every
//!   right row whose key both datasets hold is in its side's plan, and the
//!   residual keeps exactly those rows; a key one side lacks plans nothing.
//!   With counts, each side's `rows` is the exact row count.
//! - `overlap_stats` equals the decoded key sets' overlap.
//! - State: fresh after a refresh; stale on the left once the left's row
//!   groups change, never after a left commit that keeps them (another key
//!   declared, compaction); stale on the right once the right commits
//!   again; a stale key is never planned with, and a refresh makes it
//!   fresh. A refresh of a fresh key commits nothing.
//! - A missing exact key on either side is refused naming it.

#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::similar_names,
    clippy::cast_precision_loss,
    clippy::cast_possible_wrap,
    clippy::type_complexity
)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bellwether_build::{
    transient_pair, CompactOptions, JoinRefresh, JoinState, PairCaps, Planner, DEFAULT_PAIR_CAPS,
};
use bellwether_store::{ObjectStoreBackend, Snapshot};
use bellwether_types::{Error, KeySpec, Predicate, Quantity};
use common::*;
use proptest::prelude::*;

fn left_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("price", DataType::Float64, true),
    ]))
}

fn right_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("paid_order", DataType::Int64, true),
        Field::new("amount", DataType::Int64, true),
    ]))
}

fn write_left(path: &Path, ids: &[Option<i64>], per_rg: usize) {
    let batch = RecordBatch::try_new(
        left_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|i| Some(format!("r{}", i.unwrap_or(0) % 3)))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                ids.iter()
                    .map(|i| i.map(|v| v as f64 / 4.0))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    write_parquet(path, &batch, per_rg);
}

fn write_right(path: &Path, ids: &[Option<i64>], per_rg: usize) {
    let batch = RecordBatch::try_new(
        right_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from((0..ids.len() as i64).collect::<Vec<_>>())),
        ],
    )
    .unwrap();
    write_parquet(path, &batch, per_rg);
}

/// Orders 0..240 (each id once, 60 per file, 8 rows per row group, with a
/// null), and payables for ids 120..360 with 1:N skew (id % 4 + 1 rows
/// each), shuffled across row groups, with nulls.
fn corpus(left: &Path, right: &Path) {
    for f in 0..4i64 {
        let mut ids: Vec<Option<i64>> = (f * 60..(f + 1) * 60).map(Some).collect();
        ids.push(None);
        write_left(&left.join(format!("orders-{f}.parquet")), &ids, 8);
    }
    let mut pay: Vec<Option<i64>> = Vec::new();
    for id in 120..360i64 {
        for _ in 0..=(id % 4) {
            pay.push(Some(id));
        }
    }
    // A fixed shuffle spreads one id's payables over row groups.
    let n = pay.len();
    let mut shuffled: Vec<Option<i64>> = (0..n).map(|i| pay[(i * 37) % n]).collect();
    shuffled.extend([None, None]);
    let half = shuffled.len() / 2;
    write_right(&right.join("pay-0.parquet"), &shuffled[..half], 16);
    write_right(&right.join("pay-1.parquet"), &shuffled[half..], 16);
}

struct Pair {
    _scratch: Scratch,
    left_root: String,
    right_root: String,
    left: Arc<ObjectStoreBackend>,
    right: Arc<ObjectStoreBackend>,
}

impl Pair {
    fn new(counts: bool) -> Self {
        let scratch = Scratch::new();
        let (l, r) = (scratch.path("orders"), scratch.path("payables"));
        std::fs::create_dir_all(&l).unwrap();
        std::fs::create_dir_all(&r).unwrap();
        corpus(&l, &r);
        let p = Self {
            left_root: l.to_str().unwrap().to_string(),
            right_root: r.to_str().unwrap().to_string(),
            _scratch: scratch,
            left: memory_store(),
            right: memory_store(),
        };
        build_ok(
            &builder(&p.left),
            &p.left_root,
            &[exact_counts(&["order_id"], counts)],
            &parquet_opts(&left_schema()),
        );
        build_ok(
            &builder(&p.right),
            &p.right_root,
            &[exact_counts(&["paid_order"], counts)],
            &parquet_opts(&right_schema()),
        );
        p
    }

    fn refresh(&self, spec: &KeySpec) -> JoinRefresh {
        self.try_refresh(spec).unwrap()
    }

    fn try_refresh(&self, spec: &KeySpec) -> bellwether_types::Result<JoinRefresh> {
        builder(&self.left).refresh_join(spec, &self.right, "idx", None, DEFAULT_PAIR_CAPS)
    }

    fn left_rows(&self) -> Vec<Row> {
        rows_under(&self.left_root, &left_schema())
    }

    fn right_rows(&self) -> Vec<Row> {
        rows_under(&self.right_root, &right_schema())
    }

    fn state(&self, spec: &KeySpec) -> JoinState {
        planner(&self.left)
            .join_state(spec, &planner(&self.right))
            .unwrap()
    }
}

fn rows_under(root: &str, schema: &Schema) -> Vec<Row> {
    let mut paths: Vec<_> = std::fs::read_dir(root)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
        .collect();
    paths.sort();
    paths.iter().flat_map(|p| parquet_rows(p, schema)).collect()
}

fn exact_counts(c: &[&str], counts: bool) -> KeySpec {
    KeySpec::Exact {
        columns: cols(c),
        counts,
        bounds: None,
    }
}

fn join(counts: bool) -> KeySpec {
    KeySpec::Join {
        dataset: "orders".into(),
        columns: cols(&["order_id"]),
        right: "payables".into(),
        right_columns: cols(&["paid_order"]),
        counts,
    }
}

fn keys_of(rows: &[Row], column: &str) -> BTreeSet<Vec<u8>> {
    rows.iter()
        .filter_map(|r| r.tuple(&cols(&[column])))
        .collect()
}

/// Invariant 1 on one side: every row holding a matched key is in the plan
/// and survives the residual, and the residual keeps nothing else.
fn check_side(
    side: &str,
    plan: &bellwether_types::ScanPlan,
    rows: &[Row],
    column: &str,
    matched: &BTreeSet<Vec<u8>>,
    counts: bool,
) {
    let c = cols(&[column]);
    let truth: Vec<Row> = rows
        .iter()
        .filter(|r| r.tuple(&c).is_some_and(|t| matched.contains(&t)))
        .cloned()
        .collect();
    let need = holding(&truth, |_| true);
    let got = plan_rgs(plan);
    assert!(
        need.is_subset(&got),
        "{side}: row groups {:?} hold a matched key and are not planned",
        need.difference(&got).collect::<Vec<_>>()
    );
    // Exactly the row groups holding a matched key: the pair index is
    // exact per side, so nothing else is read.
    assert_eq!(
        need, got,
        "{side}: the plan reads row groups no matched key is in"
    );
    let Some(residual) = &plan.residual else {
        panic!("{side}: a semi-join step without a residual")
    };
    match residual {
        Predicate::In { columns, values } => {
            assert_eq!(columns, &c, "{side}: residual columns");
            assert_eq!(
                values.iter().cloned().collect::<BTreeSet<_>>(),
                *matched,
                "{side}: residual keys"
            );
        }
        other => panic!("{side}: residual {other:?}"),
    }
    let kept = apply_residual(residual, planned_rows(plan, rows));
    assert_eq!(sorted(kept), sorted(truth.clone()), "{side}: rows kept");
    if counts {
        assert_eq!(
            plan.rows,
            Some(Quantity::exact(truth.len() as u64)),
            "{side}: exact rows"
        );
    }
}

fn check_semijoin(p: &Pair, spec: &KeySpec, asked: &[Vec<u8>]) {
    let (lrows, rrows) = (p.left_rows(), p.right_rows());
    let (lk, rk) = (keys_of(&lrows, "order_id"), keys_of(&rrows, "paid_order"));
    let asked_set: BTreeSet<Vec<u8>> = asked.iter().cloned().collect();
    let matched: BTreeSet<Vec<u8>> = asked_set
        .iter()
        .filter(|k| lk.contains(*k) && rk.contains(*k))
        .cloned()
        .collect();
    let (lp, rp) = (planner(&p.left), planner(&p.right));
    let plan = lp.plan_semijoin(spec, asked, &rp).unwrap();
    assert_eq!(plan.keys, asked_set.len() as u64);
    assert_eq!(plan.matched, matched.len() as u64);
    assert_eq!(plan.right_sequence, rp.sequence());
    check_side(
        "left",
        &plan.left,
        &lrows,
        "order_id",
        &matched,
        spec.counts(),
    );
    check_side(
        "right",
        &plan.right,
        &rrows,
        "paid_order",
        &matched,
        spec.counts(),
    );
}

#[test]
fn a_semijoin_plans_every_row_of_a_shared_key_on_each_side() {
    for counts in [false, true] {
        let p = Pair::new(counts);
        let spec = join(counts);
        assert!(matches!(p.refresh(&spec), JoinRefresh::Built { .. }));
        // Keys on both sides, the left only, the right only, neither.
        let asked: Vec<Vec<u8>> = [5i64, 119, 120, 121, 200, 239, 240, 300, 999]
            .iter()
            .map(|v| enc_i64(*v))
            .collect();
        check_semijoin(&p, &spec, &asked);
        check_semijoin(&p, &spec, &[]);
        check_semijoin(&p, &spec, &[enc_i64(5), enc_i64(300)]);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]
    #[test]
    fn random_key_sets_keep_invariant_1_per_side(
        ids in proptest::collection::vec(-5i64..400, 0..60),
        counts in any::<bool>(),
    ) {
        let p = Pair::new(counts);
        let spec = join(counts);
        p.refresh(&spec);
        let asked: Vec<Vec<u8>> = ids.iter().map(|v| enc_i64(*v)).collect();
        check_semijoin(&p, &spec, &asked);
    }
}

#[test]
fn overlap_stats_equal_the_decoded_key_sets() {
    for counts in [false, true] {
        let p = Pair::new(counts);
        let spec = join(counts);
        p.refresh(&spec);
        let (lrows, rrows) = (p.left_rows(), p.right_rows());
        let (lc, rc) = (cols(&["order_id"]), cols(&["paid_order"]));
        let per_key = |rows: &[Row], c: &[String]| {
            let mut m: BTreeMap<Vec<u8>, (BTreeSet<(String, u32)>, u64)> = BTreeMap::new();
            for r in rows {
                if let Some(t) = r.tuple(c) {
                    let e = m.entry(t).or_default();
                    e.0.insert((r.path.clone(), r.rg));
                    e.1 += 1;
                }
            }
            m
        };
        let (l, r) = (per_key(&lrows, &lc), per_key(&rrows, &rc));
        let both: Vec<&Vec<u8>> = l.keys().filter(|k| r.contains_key(*k)).collect();
        let stats = planner(&p.left)
            .join_overlap(&spec, &planner(&p.right))
            .unwrap();
        assert_eq!(stats.both, both.len() as u64);
        assert_eq!(stats.left_only, (l.len() - both.len()) as u64);
        assert_eq!(stats.right_only, (r.len() - both.len()) as u64);
        let rgs = |m: &BTreeMap<Vec<u8>, (BTreeSet<(String, u32)>, u64)>| {
            both.iter()
                .map(|k| m[*k].0.len() as u64)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            stats.max_right_row_groups,
            rgs(&r).into_iter().max().unwrap()
        );
        assert_eq!(stats.sum_right_row_groups, rgs(&r).iter().sum::<u64>());
        assert_eq!(
            stats.max_left_row_groups,
            rgs(&l).into_iter().max().unwrap()
        );
        assert_eq!(stats.sum_left_row_groups, rgs(&l).iter().sum::<u64>());
        match (counts, stats.rows) {
            (false, None) => {}
            (true, Some(rows)) => {
                assert_eq!(rows.left, both.iter().map(|k| l[*k].1).sum::<u64>());
                assert_eq!(rows.right, both.iter().map(|k| r[*k].1).sum::<u64>());
                assert_eq!(
                    rows.join,
                    both.iter().map(|k| l[*k].1 * r[*k].1).sum::<u64>()
                );
            }
            other => panic!("counts {counts}: rows {other:?}"),
        }
    }
}

#[test]
fn a_refresh_declares_and_builds_in_one_commit_and_is_a_no_op_when_fresh() {
    let p = Pair::new(false);
    let spec = join(false);
    let before = planner(&p.left).sequence();
    let JoinRefresh::Built {
        sequence,
        right_sequence,
        left_exact,
        right_exact,
        segments,
    } = p.refresh(&spec)
    else {
        panic!("expected a build")
    };
    assert_eq!(sequence, before + 1, "declared and built in one commit");
    assert_eq!(right_sequence, planner(&p.right).sequence());
    assert_eq!(left_exact, exact_counts(&["order_id"], false));
    assert_eq!(right_exact, exact_counts(&["paid_order"], false));
    assert_eq!(
        segments.len(),
        1,
        "the default caps hold this pair in one segment"
    );
    let rsnap = Snapshot::open(Arc::clone(&p.right), "idx").unwrap();
    let pin = segments[0].join.as_ref().unwrap();
    assert_eq!(pin.right_sequence, right_sequence);
    assert_eq!(pin.right_manifest, rsnap.content_hash());
    let lp = planner(&p.left);
    assert!(lp.declared_specs().contains(&spec));
    assert_eq!(
        p.state(&spec),
        JoinState::Fresh {
            complete_at: sequence,
            right_sequence
        }
    );
    assert_eq!(p.refresh(&spec), JoinRefresh::Fresh { right_sequence });
    assert_eq!(planner(&p.left).sequence(), sequence, "nothing committed");
    // The transient pair `why` prices with is the committed segment.
    let lsnap = Snapshot::open(Arc::clone(&p.left), "idx").unwrap();
    let t = transient_pair(&lsnap, &rsnap, &spec, DEFAULT_PAIR_CAPS, u64::MAX).unwrap();
    assert_eq!(t.segments.len(), 1);
    let stored = bellwether_types::Store::get(
        &*p.left,
        &format!(
            "idx/{}",
            bellwether_core::segment::segment_path(&segments[0].id)
        ),
    )
    .unwrap()
    .0;
    // The left sequence in the header differs (built at `before`, now at
    // `sequence`); everything a plan reads is equal.
    let a =
        bellwether_core::SegmentReader::open(bytes::Bytes::from(t.segments[0].clone())).unwrap();
    let b = bellwether_core::SegmentReader::open(stored).unwrap();
    assert_eq!(
        bellwether_core::join::pair_entries(&a).unwrap(),
        bellwether_core::join::pair_entries(&b).unwrap()
    );
    assert_eq!(
        bellwether_core::join::one_sided(&a).unwrap(),
        bellwether_core::join::one_sided(&b).unwrap()
    );
}

/// ADR 018: a refresh under small caps splits the pair by key range into
/// several segments, each listed with the same pins, and plans exactly as
/// the one-segment pair does; `why`'s transient pair under the same caps
/// is the same segments, and one past its byte cap is refused, never
/// truncated.
#[test]
fn small_caps_split_the_pair_and_a_transient_pair_past_its_cap_is_refused() {
    let p = Pair::new(true);
    let spec = join(true);
    let asked: Vec<Vec<u8>> = (100..380i64).map(enc_i64).collect();
    p.refresh(&spec);
    let whole_semi = planner(&p.left)
        .plan_semijoin(&spec, &asked, &planner(&p.right))
        .unwrap();
    let whole_overlap = planner(&p.left)
        .join_overlap(&spec, &planner(&p.right))
        .unwrap();
    // A new right commit makes it stale, so the capped refresh rebuilds.
    build_ok(
        &builder(&p.right),
        &p.right_root,
        &[exact_counts(&["amount"], false)],
        &parquet_opts(&right_schema()),
    );
    let caps = PairCaps {
        max_keys: 7,
        max_bytes: u64::MAX,
    };
    let JoinRefresh::Built { segments, .. } = builder(&p.left)
        .refresh_join(&spec, &p.right, "idx", None, caps)
        .unwrap()
    else {
        panic!("expected a build")
    };
    // 120 shared keys (120..240), 7 per segment.
    assert_eq!(segments.len(), 120usize.div_ceil(7));
    let m = planner(&p.left);
    let listed: Vec<_> = m
        .manifest()
        .segments
        .iter()
        .filter(|s| s.key_spec == spec.identity())
        .collect();
    assert_eq!(listed.len(), segments.len());
    assert!(listed.iter().all(|s| s.join == segments[0].join));
    assert!(matches!(p.state(&spec), JoinState::Fresh { .. }));
    let semi = planner(&p.left)
        .plan_semijoin(&spec, &asked, &planner(&p.right))
        .unwrap();
    assert_eq!(semi.matched, whole_semi.matched);
    assert_eq!(semi.left.entries, whole_semi.left.entries);
    assert_eq!(semi.right.entries, whole_semi.right.entries);
    assert_eq!(semi.left.rows, whole_semi.left.rows);
    assert_eq!(semi.right.rows, whole_semi.right.rows);
    assert_eq!(
        planner(&p.left)
            .join_overlap(&spec, &planner(&p.right))
            .unwrap(),
        whole_overlap
    );
    check_semijoin(&p, &spec, &asked);
    // The transient pair under the same caps: the same segments.
    let lsnap = Snapshot::open(Arc::clone(&p.left), "idx").unwrap();
    let rsnap = Snapshot::open(Arc::clone(&p.right), "idx").unwrap();
    let t = transient_pair(&lsnap, &rsnap, &spec, caps, u64::MAX).unwrap();
    assert_eq!(t.segments.len(), segments.len());
    // Past its byte cap (less than the pair's bytes): refused naming the
    // command that builds it into the index, and nothing is returned.
    let total: u64 = t.segments.iter().map(|s| s.len() as u64).sum();
    let err = transient_pair(&lsnap, &rsnap, &spec, caps, total - 1).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("bellwether index"), "{msg}");
    assert!(msg.contains(&spec.identity()), "{msg}");
    assert!(
        !msg.contains("TransientLimits") && !msg.contains('{'),
        "no internals: {msg}"
    );
    transient_pair(&lsnap, &rsnap, &spec, caps, total).unwrap();
}

#[test]
fn the_left_changing_its_row_groups_makes_the_key_stale_and_a_refresh_fresh() {
    let p = Pair::new(false);
    let spec = join(false);
    p.refresh(&spec);
    // A left commit that keeps every row group: another key declared.
    build_ok(
        &builder(&p.left),
        &p.left_root,
        &[exact_counts(&["region"], false)],
        &parquet_opts(&left_schema()),
    );
    assert!(matches!(p.state(&spec), JoinState::Fresh { .. }));
    // New orders land: the pair no longer covers every live row group.
    write_left(
        &Path::new(&p.left_root).join("orders-9.parquet"),
        &[Some(300), Some(301)],
        8,
    );
    let built = planner(&p.left).sequence();
    build_ok(
        &builder(&p.left),
        &p.left_root,
        &[],
        &parquet_opts(&left_schema()),
    );
    let now = planner(&p.left).sequence();
    match p.state(&spec) {
        JoinState::LeftChanged {
            built_from,
            current,
        } => {
            assert!(built_from < built, "built from {built_from}");
            assert_eq!(current, now);
        }
        other => panic!("expected stale on the left, got {other:?}"),
    }
    let err = planner(&p.left)
        .plan_semijoin(&spec, &[enc_i64(300)], &planner(&p.right))
        .unwrap_err();
    assert!(err.to_string().contains("stale on the left"), "{err}");
    assert!(matches!(p.refresh(&spec), JoinRefresh::Built { .. }));
    assert!(matches!(p.state(&spec), JoinState::Fresh { .. }));
    // 300 is now on both sides, in the new file.
    check_semijoin(&p, &spec, &[enc_i64(300), enc_i64(301), enc_i64(5)]);
}

#[test]
fn the_right_committing_again_makes_the_key_stale_until_refreshed() {
    let p = Pair::new(false);
    let spec = join(false);
    let JoinRefresh::Built { right_sequence, .. } = p.refresh(&spec) else {
        panic!()
    };
    write_right(
        &Path::new(&p.right_root).join("pay-9.parquet"),
        &[Some(5), Some(6)],
        16,
    );
    build_ok(
        &builder(&p.right),
        &p.right_root,
        &[],
        &parquet_opts(&right_schema()),
    );
    let current = planner(&p.right).sequence();
    assert_eq!(
        p.state(&spec),
        JoinState::RightAdvanced {
            pinned: right_sequence,
            current
        }
    );
    let err = planner(&p.left)
        .plan_semijoin(&spec, &[enc_i64(5)], &planner(&p.right))
        .unwrap_err();
    assert!(err.to_string().contains("stale on the right"), "{err}");
    let err = planner(&p.left)
        .join_overlap(&spec, &planner(&p.right))
        .unwrap_err();
    assert!(err.to_string().contains("stale on the right"), "{err}");
    p.refresh(&spec);
    assert!(matches!(p.state(&spec), JoinState::Fresh { right_sequence: s, .. } if s == current));
    check_semijoin(&p, &spec, &[enc_i64(5), enc_i64(6), enc_i64(7)]);
}

/// The overlap a fresh key reports equals the decoded key sets'.
fn check_overlap(p: &Pair, spec: &KeySpec) {
    let (lk, rk) = (
        keys_of(&p.left_rows(), "order_id"),
        keys_of(&p.right_rows(), "paid_order"),
    );
    let both = lk.intersection(&rk).count() as u64;
    let s = planner(&p.left)
        .join_overlap(spec, &planner(&p.right))
        .unwrap();
    assert_eq!(
        (s.both, s.left_only, s.right_only),
        (both, lk.len() as u64 - both, rk.len() as u64 - both)
    );
}

/// Review probe (PR #32): a left file removed retires row groups and adds
/// none, so the pair's range still covers every live left row group; the
/// key must still go stale on the left (its pair pins the left's live
/// set), and a refresh must answer for the files that remain.
#[test]
fn a_left_file_removed_makes_the_key_stale_on_the_left() {
    let p = Pair::new(true);
    let spec = join(true);
    p.refresh(&spec);
    // orders-3 holds ids 180..240, every one shared with payables.
    std::fs::remove_file(Path::new(&p.left_root).join("orders-3.parquet")).unwrap();
    let r = build_ok(
        &builder(&p.left),
        &p.left_root,
        &[],
        &parquet_opts(&left_schema()),
    );
    assert!(r.row_groups_retired > 0, "{r:?}");
    let now = planner(&p.left).sequence();
    match p.state(&spec) {
        JoinState::LeftChanged { current, .. } => assert_eq!(current, now),
        other => panic!("a left file removed must leave the key stale, got {other:?}"),
    }
    let err = planner(&p.left)
        .plan_semijoin(&spec, &[enc_i64(200)], &planner(&p.right))
        .unwrap_err();
    assert!(err.to_string().contains("stale on the left"), "{err}");
    let err = planner(&p.left)
        .join_overlap(&spec, &planner(&p.right))
        .unwrap_err();
    assert!(err.to_string().contains("stale on the left"), "{err}");
    assert!(matches!(p.refresh(&spec), JoinRefresh::Built { .. }));
    assert!(matches!(p.state(&spec), JoinState::Fresh { .. }));
    let asked: Vec<Vec<u8>> = (-2..400i64).map(enc_i64).collect();
    check_semijoin(&p, &spec, &asked);
    check_overlap(&p, &spec);
}

/// Review probe (PR #32): the right index deleted and rebuilt over other
/// data reaches the pinned sequence again; its row-group ids now name
/// other rows. The key must be stale on the right (its pair pins the right
/// manifest's content, not only its sequence), and a refresh must answer
/// for the new right.
#[test]
fn a_right_index_rebuilt_to_the_pinned_sequence_is_stale_on_the_right() {
    let mut p = Pair::new(false);
    let spec = join(false);
    let JoinRefresh::Built { right_sequence, .. } = p.refresh(&spec) else {
        panic!("expected a build")
    };
    // The right's data changes and its index is deleted: pay-0 gone, a new
    // file sorting first holds ids 0..60 (shared with orders) four times
    // each, in more row groups than pay-0 had.
    std::fs::remove_file(Path::new(&p.right_root).join("pay-0.parquet")).unwrap();
    write_right(
        &Path::new(&p.right_root).join("a-0.parquet"),
        &(0..240).map(|i| Some(i % 60)).collect::<Vec<_>>(),
        4,
    );
    p.right = memory_store();
    build_ok(
        &builder(&p.right),
        &p.right_root,
        &[exact_counts(&["paid_order"], false)],
        &parquet_opts(&right_schema()),
    );
    assert_eq!(
        planner(&p.right).sequence(),
        right_sequence,
        "same sequence"
    );
    assert_eq!(
        p.state(&spec),
        JoinState::RightRebuilt {
            pinned: right_sequence,
            current: right_sequence
        }
    );
    let err = planner(&p.left)
        .plan_semijoin(&spec, &[enc_i64(5)], &planner(&p.right))
        .unwrap_err();
    assert!(err.to_string().contains("deleted and rebuilt"), "{err}");
    assert!(matches!(p.refresh(&spec), JoinRefresh::Built { .. }));
    assert!(matches!(p.state(&spec), JoinState::Fresh { .. }));
    let asked: Vec<Vec<u8>> = (-2..400i64).map(enc_i64).collect();
    check_semijoin(&p, &spec, &asked);
    check_overlap(&p, &spec);
}

/// Item 6 of the review: a refresh told which right manifest the caller
/// verified (`expect`) refuses once the right's current manifest is
/// another, before it builds or commits anything.
#[test]
fn a_refresh_expecting_another_right_manifest_refuses_before_committing() {
    let p = Pair::new(false);
    let spec = join(false);
    let verified = Snapshot::open(Arc::clone(&p.right), "idx")
        .unwrap()
        .content_hash();
    // The right commits after the caller verified it.
    build_ok(
        &builder(&p.right),
        &p.right_root,
        &[exact_counts(&["amount"], false)],
        &parquet_opts(&right_schema()),
    );
    let before = planner(&p.left).manifest().clone();
    let r = builder(&p.left)
        .refresh_join(&spec, &p.right, "idx", Some(verified), DEFAULT_PAIR_CAPS)
        .unwrap();
    let now = Snapshot::open(Arc::clone(&p.right), "idx").unwrap();
    match r {
        JoinRefresh::RightChanged { expected, found } => {
            assert_eq!(expected, verified);
            assert_eq!(found.hash, now.content_hash());
            assert_eq!(found.sequence, now.sequence());
        }
        other => panic!("expected RightChanged, got {other:?}"),
    }
    let after = planner(&p.left).manifest().clone();
    assert_eq!(after, before, "nothing declared, built, or committed");
    // Expecting the current manifest builds.
    assert!(matches!(
        builder(&p.left)
            .refresh_join(
                &spec,
                &p.right,
                "idx",
                Some(now.content_hash()),
                DEFAULT_PAIR_CAPS
            )
            .unwrap(),
        JoinRefresh::Built { .. }
    ));
}

/// A left commit that lands while a refresh builds (between its read of
/// the left and its commit) and changes the left's row groups: the refresh
/// starts over from the new left, so the key it commits is fresh and
/// answers for the new file.
#[test]
fn a_left_commit_during_a_refresh_restarts_it_from_the_new_left() {
    let p = Pair::new(false);
    let spec = join(false);
    let (root, store) = (p.left_root.clone(), Arc::clone(&p.left));
    let hook = Box::new(move || {
        write_left(
            &Path::new(&root).join("orders-9.parquet"),
            &[Some(300), Some(301), Some(5)],
            8,
        );
        let r = builder(&store)
            .build(&root, &[], &parquet_opts(&left_schema()))
            .unwrap();
        assert!(r.row_groups_read > 0, "{r:?}");
    });
    let x =
        bellwether_build::Builder::new(Arc::new(HookStore::new(Arc::clone(&p.left), hook)), "idx");
    let JoinRefresh::Built { sequence, .. } = x
        .refresh_join(&spec, &p.right, "idx", None, DEFAULT_PAIR_CAPS)
        .unwrap()
    else {
        panic!("expected a build")
    };
    assert_eq!(planner(&p.left).sequence(), sequence);
    assert!(
        matches!(p.state(&spec), JoinState::Fresh { .. }),
        "{:?}",
        p.state(&spec)
    );
    let asked: Vec<Vec<u8>> = (-2..400i64).map(enc_i64).collect();
    check_semijoin(&p, &spec, &asked);
    check_overlap(&p, &spec);
}

#[test]
fn left_compaction_keeps_the_key_fresh_and_its_plans_equal() {
    let p = Pair::new(true);
    let spec = join(true);
    // More left segments, so compaction has work: one build per new file.
    for f in 10..13i64 {
        write_left(
            &Path::new(&p.left_root).join(format!("orders-{f}.parquet")),
            &[Some(f * 100), Some(150)],
            8,
        );
        build_ok(
            &builder(&p.left),
            &p.left_root,
            &[],
            &parquet_opts(&left_schema()),
        );
    }
    p.refresh(&spec);
    let asked: Vec<Vec<u8>> = [150i64, 151, 1000, 1100, 5]
        .iter()
        .map(|v| enc_i64(*v))
        .collect();
    let before = planner(&p.left)
        .plan_semijoin(&spec, &asked, &planner(&p.right))
        .unwrap();
    let seq = planner(&p.left).sequence();
    let r = builder(&p.left)
        .compact(&CompactOptions::default())
        .unwrap();
    assert!(
        r.sequence.is_some_and(|s| s > seq),
        "compaction committed: {r:?}"
    );
    assert!(matches!(p.state(&spec), JoinState::Fresh { .. }));
    let after = planner(&p.left)
        .plan_semijoin(&spec, &asked, &planner(&p.right))
        .unwrap();
    assert_eq!(after.left.entries, before.left.entries);
    assert_eq!(after.right.entries, before.right.entries);
    assert_eq!(after.left.rows, before.left.rows);
    check_semijoin(&p, &spec, &asked);
    // A commit on the right, compaction included, advances the sequence the
    // key is pinned to: stale until refreshed (ADR 020 pins a sequence, not
    // a set of row groups).
    build_ok(
        &builder(&p.right),
        &p.right_root,
        &[exact_counts(&["amount"], false)],
        &parquet_opts(&right_schema()),
    );
    assert!(matches!(p.state(&spec), JoinState::RightAdvanced { .. }));
}

#[test]
fn a_missing_exact_key_on_either_side_is_refused_naming_it() {
    let p = Pair::new(false);
    // Counted join, uncounted exact keys on both sides: the left is named.
    let err = p.try_refresh(&join(true)).unwrap_err();
    match &err {
        Error::NotIndexed { needs: Some(n), .. } => {
            assert!(n.starts_with("exact:order_id,counts"), "{n}");
            assert!(n.contains("left side"), "{n}");
        }
        other => panic!("{other:?}"),
    }
    // The left counts, the right does not: the right is named.
    build_ok(
        &builder(&p.left),
        &p.left_root,
        &[exact_counts(&["order_id"], true)],
        &parquet_opts(&left_schema()),
    );
    let err = p.try_refresh(&join(true)).unwrap_err();
    match &err {
        Error::NotIndexed {
            root,
            needs: Some(n),
            ..
        } => {
            assert_eq!(root, "payables");
            assert!(n.starts_with("exact:paid_order,counts"), "{n}");
            assert!(n.contains("right side"), "{n}");
        }
        other => panic!("{other:?}"),
    }
    // Nothing was declared by a refused refresh.
    assert!(!planner(&p.left).declared_specs().contains(&join(true)));
    // An exact key on other columns does not count.
    let fresh = Pair::new(false);
    let other = KeySpec::Join {
        dataset: "orders".into(),
        columns: cols(&["order_id"]),
        right: "payables".into(),
        right_columns: cols(&["amount"]),
        counts: false,
    };
    let err = fresh.try_refresh(&other).unwrap_err();
    assert!(
        matches!(&err, Error::NotIndexed { needs: Some(n), .. } if n.starts_with("exact:amount")),
        "{err:?}"
    );
    // An exact key declared, with its segments, but not complete (its
    // completeness entry dropped, as a partial build leaves it) does not
    // count either, on either side: a pair from it could miss row groups.
    for (side, store, name) in [
        ("right", &fresh.right, "exact:paid_order"),
        ("left", &fresh.left, "exact:order_id"),
    ] {
        let staged = bellwether_store::StagedSegments::new();
        bellwether_store::commit_with(
            &**store,
            "idx",
            &staged,
            bellwether_store::CommitOptions::default(),
            |m| {
                let mut next = m.clone();
                assert!(next.key_spec_completeness.remove(name).is_some());
                Ok(next)
            },
        )
        .unwrap();
        assert!(planner(store)
            .declared_specs()
            .iter()
            .any(|s| s.identity() == name));
        let err = fresh.try_refresh(&join(false)).unwrap_err();
        assert!(
            matches!(&err, Error::NotIndexed { needs: Some(n), .. }
                if n.starts_with(name) && n.contains(&format!("{side} side"))),
            "{side}: {err:?}"
        );
        let lsnap = Snapshot::open(Arc::clone(&fresh.left), "idx").unwrap();
        let rsnap = Snapshot::open(Arc::clone(&fresh.right), "idx").unwrap();
        assert!(
            matches!(
                transient_pair(&lsnap, &rsnap, &join(false), DEFAULT_PAIR_CAPS, u64::MAX),
                Err(Error::NotIndexed { .. })
            ),
            "{side}: the price is refused too"
        );
    }
}

#[test]
fn a_float_column_or_another_left_folder_is_refused_before_anything_is_built() {
    let p = Pair::new(false);
    build_ok(
        &builder(&p.left),
        &p.left_root,
        &[exact_counts(&["price"], false)],
        &parquet_opts(&left_schema()),
    );
    let float = KeySpec::Join {
        dataset: "orders".into(),
        columns: cols(&["price"]),
        right: "payables".into(),
        right_columns: cols(&["amount"]),
        counts: false,
    };
    let seq = planner(&p.left).sequence();
    let err = p.try_refresh(&float).unwrap_err();
    assert!(err.to_string().contains("is a float"), "{err}");
    let elsewhere = KeySpec::Join {
        dataset: "invoices".into(),
        columns: cols(&["order_id"]),
        right: "payables".into(),
        right_columns: cols(&["paid_order"]),
        counts: false,
    };
    let err = p.try_refresh(&elsewhere).unwrap_err();
    assert!(err.to_string().contains("the folder \"orders\""), "{err}");
    assert_eq!(planner(&p.left).sequence(), seq, "nothing committed");
}

#[test]
fn a_build_keeps_the_pair_segment_and_a_redeclared_count_replaces_it() {
    let p = Pair::new(true);
    p.refresh(&join(false));
    // An ordinary build keeps the pair segment and its completeness.
    build_ok(
        &builder(&p.left),
        &p.left_root,
        &[exact_counts(&["region"], false)],
        &parquet_opts(&left_schema()),
    );
    let m = Planner::open(Arc::clone(&p.left), "idx").unwrap();
    assert!(m.completeness(&join(false)).is_some());
    // The counted key under the same name replaces it in one commit.
    assert!(matches!(p.refresh(&join(true)), JoinRefresh::Built { .. }));
    let m = planner(&p.left);
    assert!(m.declared_specs().contains(&join(true)));
    assert!(!m.declared_specs().contains(&join(false)));
    assert!(m
        .manifest()
        .segments
        .iter()
        .all(|s| s.key_spec != join(false).identity()));
    assert!(!m
        .manifest()
        .key_spec_completeness
        .contains_key(&join(false).identity()));
    check_semijoin(&p, &join(true), &[enc_i64(150), enc_i64(5)]);
}
