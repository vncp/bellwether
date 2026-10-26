//! Join keys (B3): predicted equals planned. Two datasets beside each
//! other, each indexed by `bellwether_build::Builder` with an exact key on
//! its join column; semi-join runs recorded on the left's column:
//!
//! - With the join key in the configuration, the replayer's two steps, from
//!   the transient pair index (`bellwether_build::transient_pair`), equal
//!   the build planner's `plan_semijoin` over the committed pair segment,
//!   side by side (files, row groups, bytes, residual, rows), with and
//!   without counts; the run's cost is the sum of its steps.
//! - Without it, a run is priced as two locates: each side as the build
//!   planner locates the keys there by its own exact key.
//! - The no-index baselines read every row group on both sides.
//! - A Replay of a semi-join workload cites both catalogs; a run whose
//!   right dataset is not attached, or another one is, is refused naming
//!   it; a join key with no pair index given is refused.

#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

mod common;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bellwether_build::{transient_pair, BuildOptions, Builder, Lookup, Planner};
use bellwether_replay::{
    pair_cost, Baseline, KeyInput, MemoryKeySets, PairInput, Replayer, RunJoin, RunQuery,
    WorkloadRecorder,
};
use bellwether_source::ParquetSource;
use bellwether_store::{ObjectStoreBackend, Snapshot};
use bellwether_types::{Error, KeySpec};
use bytes::Bytes;
use common::Rng;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

fn left_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, true),
        Field::new("region", DataType::Utf8, true),
    ]))
}

fn right_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("paid_order", DataType::Int64, true),
        Field::new("amount", DataType::Int64, true),
    ]))
}

fn write(path: &Path, schema: &SchemaRef, ids: &[Option<i64>], per_rg: usize) {
    let other: ArrayRef = if schema.field(1).data_type() == &DataType::Utf8 {
        Arc::new(StringArray::from(
            ids.iter()
                .map(|i| format!("r{}", i.unwrap_or(0) % 5))
                .collect::<Vec<_>>(),
        ))
    } else {
        Arc::new(Int64Array::from((0..ids.len() as i64).collect::<Vec<_>>()))
    };
    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef, other],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(per_rg))
        .build();
    let mut w = ArrowWriter::try_new(
        std::fs::File::create(path).unwrap(),
        Arc::clone(schema),
        Some(props),
    )
    .unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// Orders 0..400 in order (clustered), payables for 200..600 with 1:N skew
/// (id % 5 + 1 rows), shuffled.
fn corpus(left: &Path, right: &Path) {
    for f in 0..4i64 {
        let mut ids: Vec<Option<i64>> = (f * 100..(f + 1) * 100).map(Some).collect();
        ids.push(None);
        write(
            &left.join(format!("o-{f}.parquet")),
            &left_schema(),
            &ids,
            20,
        );
    }
    let mut pay: Vec<Option<i64>> = Vec::new();
    for id in 200..600i64 {
        for _ in 0..=(id % 5) {
            pay.push(Some(id));
        }
    }
    let n = pay.len();
    let shuffled: Vec<Option<i64>> = (0..n).map(|i| pay[(i * 61) % n]).collect();
    let third = shuffled.len() / 3;
    for (i, chunk) in shuffled.chunks(third + 1).enumerate() {
        write(
            &right.join(format!("p-{i}.parquet")),
            &right_schema(),
            chunk,
            40,
        );
    }
}

fn exact(c: &str, counts: bool) -> KeySpec {
    KeySpec::Exact {
        columns: vec![c.into()],
        counts,
        bounds: None,
    }
}

fn join(counts: bool) -> KeySpec {
    KeySpec::Join {
        dataset: "orders".into(),
        columns: vec!["order_id".into()],
        right: "payables".into(),
        right_columns: vec!["paid_order".into()],
        counts,
    }
}

fn run_join() -> RunJoin {
    RunJoin {
        right: "payables".into(),
        right_columns: vec!["paid_order".into()],
    }
}

fn index(store: &Arc<ObjectStoreBackend>, root: &str, schema: SchemaRef, spec: KeySpec) {
    Builder::new(Arc::clone(store), "idx")
        .build(
            root,
            &[spec],
            &BuildOptions {
                schema: Some(schema),
                max_segment_row_groups: 4,
                ..BuildOptions::default()
            },
        )
        .unwrap();
}

fn pl_version(r: &Replayer<'_>) -> String {
    r.manifest_version()
}

fn key_batch(ids: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "order_id",
            DataType::Int64,
            true,
        )])),
        vec![Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef],
    )
    .unwrap()
}

#[test]
fn semijoin_runs_price_what_the_build_planner_plans() {
    for counts in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        let (l, r) = (root.join("orders"), root.join("payables"));
        std::fs::create_dir_all(&l).unwrap();
        std::fs::create_dir_all(&r).unwrap();
        corpus(&l, &r);
        let (ls, rs) = (
            Arc::new(ObjectStoreBackend::memory().unwrap()),
            Arc::new(ObjectStoreBackend::memory().unwrap()),
        );
        index(
            &ls,
            l.to_str().unwrap(),
            left_schema(),
            exact("order_id", counts),
        );
        index(
            &rs,
            r.to_str().unwrap(),
            right_schema(),
            exact("paid_order", counts),
        );
        let spec = join(counts);
        let lsnap = Snapshot::open(Arc::clone(&ls), "idx").unwrap();
        let rsnap = Snapshot::open(Arc::clone(&rs), "idx").unwrap();
        let pair = transient_pair(&lsnap, &rsnap, &spec).unwrap();
        let lsource = ParquetSource::new().with_schema(left_schema());
        let rsource = ParquetSource::new().with_schema(right_schema());

        let keys = MemoryKeySets::new();
        let mut rec = WorkloadRecorder::new(&keys, left_schema());
        let mut g = Rng(7);
        for i in 0..12 {
            let n = [1usize, 10, 100][i % 3];
            let ids: Vec<i64> = (0..n).map(|_| g.below(700) as i64 - 50).collect();
            rec.record_query(
                &format!("run-{i}"),
                KeyInput::Batch(&key_batch(&ids)),
                RunQuery::SemiJoin(run_join()),
                None,
            )
            .unwrap();
        }
        let workload = rec.finish().unwrap();

        let right_replayer = Replayer::for_snapshot(&rsource, &rsnap)
            .unwrap()
            .with_unserved(Baseline::ParquetStats);
        let replayer = Replayer::for_snapshot(&lsource, &lsnap)
            .unwrap()
            .with_unserved(Baseline::ParquetStats)
            .with_pair(PairInput {
                right: "payables".into(),
                replayer: right_replayer,
                pairs: BTreeMap::from([(spec.identity(), Bytes::from(pair.bytes.clone()))]),
            });
        let current = replayer.current_specs().to_vec();
        assert_eq!(current, vec![exact("order_id", counts)]);
        let with_join = vec![exact("order_id", counts), spec.clone()];

        // The committed pair: what `semijoin` plans.
        Builder::new(Arc::clone(&ls), "idx")
            .refresh_join(&spec, &rs, "idx")
            .unwrap();
        let (lp, rp) = (
            Planner::open(Arc::clone(&ls), "idx").unwrap(),
            Planner::open(Arc::clone(&rs), "idx").unwrap(),
        );
        let joined = replayer.replay_specs(&with_join, &workload, &keys).unwrap();
        let located = replayer.replay_specs(&current, &workload, &keys).unwrap();
        let baseline = replayer
            .replay_baseline(Baseline::RawScan, &workload, &keys)
            .unwrap();
        // No row group is retired here, so every assigned id is live.
        let total =
            u64::from(lp.manifest().rowgroup_space.next_id + rp.manifest().rowgroup_space.next_id);
        let mut pruned = 0;
        for (i, run) in workload.runs().iter().enumerate() {
            let rk = workload.run_keys(&keys, run).unwrap();
            let probes = rk.all_probes(&[0]);
            let mut planned = lp.plan_semijoin(&spec, &probes, &rp).unwrap();
            // Priced before the key was committed (left sequence 1), planned
            // after (2): everything else must be equal.
            planned.left.manifest_version = pl_version(&replayer);
            let (pl, pr) = replayer
                .semijoin_plans(&with_join, &rk, &run_join())
                .unwrap();
            let what = format!("counts {counts} {}", run.run_id);
            assert_eq!(pl, planned.left, "{what}: left step");
            assert_eq!(pr, planned.right, "{what}: right step");
            assert_eq!(
                joined[i],
                pair_cost(&run.run_id, rk.len() as u64, &planned.left, &planned.right),
                "{what}: cost"
            );
            let lp_old = Planner::from_snapshot(lsnap.clone()).unwrap();
            if counts {
                assert!(joined[i].rows.is_some(), "{what}: counted rows");
            }
            // Without the join key: two locates.
            let (ll, lr) = replayer.semijoin_plans(&current, &rk, &run_join()).unwrap();
            let lookup = |c: &str| Lookup::Keys {
                columns: vec![c.into()],
                keys: probes.clone(),
            };
            assert_eq!(
                ll,
                lp_old
                    .plan(&[exact("order_id", counts)], &lookup("order_id"))
                    .unwrap(),
                "{what}: left locate"
            );
            assert_eq!(
                lr,
                rp.plan(&[exact("paid_order", counts)], &lookup("paid_order"))
                    .unwrap(),
                "{what}: right locate"
            );
            assert_eq!(
                located[i],
                pair_cost(&run.run_id, rk.len() as u64, &ll, &lr),
                "{what}: locate cost"
            );
            assert!(joined[i].row_groups <= located[i].row_groups, "{what}");
            if joined[i].row_groups < located[i].row_groups {
                pruned += 1;
            }
            assert_eq!(baseline[i].row_groups, total, "{what}: raw scan");
        }
        assert!(pruned > 0, "the join key reads less on some run");

        // The Replay cites both datasets.
        let body = bellwether_types::ProposalBody::Key {
            spec: spec.clone(),
            sidecars: Vec::new(),
        };
        let scored = replayer
            .score(
                &bellwether_types::Hash::of(b"p"),
                &body,
                &workload,
                &keys,
                "2026-10-26T00:00:00.000Z",
            )
            .unwrap();
        assert_eq!(
            scored.replay.inputs,
            vec![
                replayer.catalog().digest(),
                Replayer::for_snapshot(&rsource, &rsnap)
                    .unwrap()
                    .catalog()
                    .digest()
            ]
        );
    }
}

#[test]
fn a_semijoin_run_without_its_right_dataset_or_pair_index_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let (l, r) = (root.join("orders"), root.join("payables"));
    std::fs::create_dir_all(&l).unwrap();
    std::fs::create_dir_all(&r).unwrap();
    corpus(&l, &r);
    let (ls, rs) = (
        Arc::new(ObjectStoreBackend::memory().unwrap()),
        Arc::new(ObjectStoreBackend::memory().unwrap()),
    );
    index(
        &ls,
        l.to_str().unwrap(),
        left_schema(),
        exact("order_id", false),
    );
    index(
        &rs,
        r.to_str().unwrap(),
        right_schema(),
        exact("paid_order", false),
    );
    let lsnap = Snapshot::open(Arc::clone(&ls), "idx").unwrap();
    let rsnap = Snapshot::open(Arc::clone(&rs), "idx").unwrap();
    let lsource = ParquetSource::new().with_schema(left_schema());
    let rsource = ParquetSource::new().with_schema(right_schema());
    let keys = MemoryKeySets::new();
    let mut rec = WorkloadRecorder::new(&keys, left_schema());
    rec.record_query(
        "one",
        KeyInput::Batch(&key_batch(&[250, 251])),
        RunQuery::SemiJoin(run_join()),
        None,
    )
    .unwrap();
    // One right column per key column.
    let err = rec
        .record_query(
            "two",
            KeyInput::Batch(&key_batch(&[1])),
            RunQuery::SemiJoin(RunJoin {
                right: "payables".into(),
                right_columns: vec!["paid_order".into(), "amount".into()],
            }),
            None,
        )
        .unwrap_err();
    assert!(err.to_string().contains("pair one right column"), "{err}");
    let workload = rec.finish().unwrap();
    let alone = Replayer::for_snapshot(&lsource, &lsnap).unwrap();
    let err = alone.replay_current(&workload, &keys).unwrap_err();
    assert!(
        matches!(&err, Error::InvalidArgument { field: "join", reason } if reason.contains("the folder payables beside the data root")),
        "{err:?}"
    );
    let other = Replayer::for_snapshot(&lsource, &lsnap)
        .unwrap()
        .with_pair(PairInput {
            right: "invoices".into(),
            replayer: Replayer::for_snapshot(&rsource, &rsnap).unwrap(),
            pairs: BTreeMap::new(),
        });
    let err = other.replay_current(&workload, &keys).unwrap_err();
    assert!(
        err.to_string()
            .contains("name payables as the right dataset"),
        "{err}"
    );
    let no_pair = Replayer::for_snapshot(&lsource, &lsnap)
        .unwrap()
        .with_pair(PairInput {
            right: "payables".into(),
            replayer: Replayer::for_snapshot(&rsource, &rsnap).unwrap(),
            pairs: BTreeMap::new(),
        });
    // Without the join key it prices as two locates; with it, a missing
    // pair index is an error, never a guess.
    no_pair.replay_current(&workload, &keys).unwrap();
    let err = no_pair
        .replay_specs(&[exact("order_id", false), join(false)], &workload, &keys)
        .unwrap_err();
    assert!(err.to_string().contains("no pair index was built"), "{err}");
}
