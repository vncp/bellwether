//! Tombstones (B8): predicted equals planned. Over a corpus of versions
//! with retractions, a committed index built by `bellwether_build::Builder`
//! and this crate's transient segments for the same temporal key with a
//! tombstone plan every `latest`/`asof` run that keeps only live rows to
//! the same `ScanPlan` (files, row groups, bytes, residual) as the build
//! planner's own `Lookup::Latest`/`AsOf` with `live`, four ways:
//!
//! 1. the build `Planner` with the lookup (what `latest --live` runs);
//! 2. the build `Planner` as a `PlanSource` over the committed segments;
//! 3. the build `Planner` with this crate's transient segments in their
//!    place (`PlanSource::with_transient`);
//! 4. this crate's `SegmentPlanner` with the same transient segments.
//!
//! The replayers over the snapshot price every run the same, a live run
//! through the key without its tombstone reads what the plain run reads
//! (the residual alone drops the tombstones), and the transient index
//! counts the keys whose newest row is a tombstone as the rows say.

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
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bellwether_build::{BuildOptions, Builder, Lookup as BuildLookup, Planner};
use bellwether_replay::{
    Catalog, KeyInput, MemoryKeySets, PlanSource, Replayer, RunLive, RunQuery, SegmentPlanner,
    TransientIndex, TransientLimits, WorkloadRecorder,
};
use bellwether_source::ParquetSource;
use bellwether_store::{ObjectStoreBackend, Snapshot};
use bellwether_types::{KeySpec, Predicate, Source, Tombstone};
use common::Rng;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

const IDS: u64 = 30;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("ts", DataType::Int64, true),
        Field::new("void", DataType::Boolean, true),
    ]))
}

/// One row: `(id, ts, void)`.
type Row = (Option<i64>, Option<i64>, Option<bool>);

/// `(id, ts, void)` rows: five files of four row groups of 20 rows, each
/// id's versions rising with the file, a third of the newest versions
/// retracted, some keys retracted then written again, null keys, times and
/// tombstone columns.
fn rows() -> Vec<Vec<Vec<Row>>> {
    let mut g = Rng(23);
    (0..5i64)
        .map(|f| {
            (0..4i64)
                .map(|rg| {
                    (0..20)
                        .map(|_| {
                            let id = (g.below(15) != 0).then(|| g.below(IDS) as i64);
                            let ts =
                                (g.below(12) != 0).then(|| f * 100 + rg * 10 + g.below(10) as i64);
                            let void = match g.below(9) {
                                0 => None,
                                1..=3 => Some(true),
                                _ => Some(false),
                            };
                            (id, ts, void)
                        })
                        .collect()
                })
                .collect()
        })
        .collect()
}

fn write_corpus(dir: &Path) {
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(1 << 20))
        .build();
    for (f, file) in rows().into_iter().enumerate() {
        let path = dir.join(format!("part-{f}.parquet"));
        let mut w =
            ArrowWriter::try_new(File::create(path).unwrap(), schema(), Some(props.clone()))
                .unwrap();
        for rg in file {
            let columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(rg.iter().map(|r| r.0).collect::<Vec<_>>())),
                Arc::new(Int64Array::from(rg.iter().map(|r| r.1).collect::<Vec<_>>())),
                Arc::new(BooleanArray::from(
                    rg.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
            ];
            w.write(&RecordBatch::try_new(schema(), columns).unwrap())
                .unwrap();
            w.flush().unwrap();
        }
        w.close().unwrap();
    }
}

fn tomb() -> Tombstone {
    Tombstone {
        column: "void".into(),
        value: None,
    }
}

fn spec() -> KeySpec {
    KeySpec::Temporal {
        columns: vec!["id".into()],
        ts_column: "ts".into(),
        counts: false,
        tombstone: Some(tomb()),
    }
}

fn key_batch(ids: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef],
    )
    .unwrap()
}

/// Live runs (latest and asof at several instants) and their plain
/// counterparts, over single keys and key sets.
fn runs() -> Vec<(String, Vec<i64>, RunQuery)> {
    let mut g = Rng(5);
    let mut out = Vec::new();
    for i in 0..12 {
        let n = 1 + g.below(6) as usize;
        let ids: Vec<i64> = (0..n).map(|_| g.below(IDS + 2) as i64).collect();
        for asof in [None, Some(150), Some(305), Some(1000)] {
            let tag = asof.map_or("latest".to_string(), |a| format!("asof{a}"));
            out.push((
                format!("{i}-{tag}-live"),
                ids.clone(),
                RunQuery::Live(RunLive {
                    asof,
                    tombstone: tomb(),
                }),
            ));
            out.push((
                format!("{i}-{tag}"),
                ids.clone(),
                asof.map_or(RunQuery::Latest, |ts| RunQuery::Asof { ts }),
            ));
        }
    }
    out
}

#[test]
fn live_runs_plan_identically_committed_transient_and_in_crate() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    write_corpus(&data);
    let data = data.to_str().unwrap().to_string();
    let store = Arc::new(ObjectStoreBackend::memory().unwrap());
    let specs = vec![spec()];
    Builder::new(Arc::clone(&store), "idx")
        .build(
            &data,
            &specs,
            &BuildOptions {
                schema: Some(schema()),
                max_segment_row_groups: 5,
                ..BuildOptions::default()
            },
        )
        .unwrap();
    let snapshot = Snapshot::open(Arc::clone(&store), "idx").unwrap();
    let planner = Planner::from_snapshot(snapshot.clone()).unwrap();
    assert_eq!(PlanSource::specs(&planner), specs, "the spec is complete");
    assert!(
        snapshot
            .manifest()
            .segments
            .iter()
            .filter(|s| s.kind.as_deref() == Some("tombstone"))
            .count()
            > 1,
        "the committed tombstone sidecar is split"
    );
    let source = ParquetSource::new().with_schema(schema());
    let root = snapshot.manifest().source.root.clone();
    let catalog = Arc::new(
        Catalog::from_manifest(snapshot.manifest(), &source, source.schema(&root).unwrap())
            .unwrap(),
    );
    let segment_planner = SegmentPlanner::new(Arc::clone(&catalog));
    let limits = TransientLimits {
        max_segment_rows: 150,
        ..TransientLimits::default()
    };
    let keys = MemoryKeySets::new();
    let mut rec = WorkloadRecorder::new(&keys, schema());
    for (id, ids, query) in runs() {
        rec.record_query(&id, KeyInput::Batch(&key_batch(&ids)), query, None)
            .unwrap();
    }
    let workload = rec.finish().unwrap();
    let transient = Arc::new(TransientIndex::build(&source, &catalog, &spec(), &limits).unwrap());
    assert!(transient.stats().segments > 1);
    assert!(transient.tombstone_bytes().len() > 1);
    // the tombstone column: keys whose newest row is a tombstone, as the
    // rows say
    let mut newest: BTreeMap<i64, (i64, bool)> = BTreeMap::new();
    for r in rows().into_iter().flatten().flatten() {
        if let (Some(id), Some(ts)) = (r.0, r.1) {
            let live = r.2 == Some(false);
            let e = newest.entry(id).or_insert((ts, live));
            if ts > e.0 {
                *e = (ts, live);
            } else if ts == e.0 {
                e.1 |= live;
            }
        }
    }
    let n = transient.newest_tombstones().unwrap().unwrap();
    assert_eq!(n.keys, newest.len() as u64);
    assert_eq!(
        n.newest_is_tombstone,
        newest.values().filter(|(_, live)| !live).count() as u64
    );
    assert!(n.newest_is_tombstone > 0);

    let wp4_transient = PlanSource::with_transient(&planner, &[Arc::clone(&transient)]).unwrap();
    let ours_transient = segment_planner
        .with_transient(&[Arc::clone(&transient)])
        .unwrap();
    let plain = vec![spec().without_tombstone()];
    let mut fewer = 0;
    for run in workload.runs() {
        let rk = workload.run_keys(&keys, run).unwrap();
        let query = run.query().unwrap();
        let tuples: Vec<Vec<u8>> = rk.tuples().map(<[u8]>::to_vec).collect();
        let columns = rk.columns().to_vec();
        let (asof, live) = match &query {
            RunQuery::Live(l) => (l.asof, Some(l.tombstone.clone())),
            RunQuery::Latest => (None, None),
            RunQuery::Asof { ts } => (Some(*ts), None),
            other => panic!("{other:?}"),
        };
        let lookup = match asof {
            None => BuildLookup::Latest {
                columns,
                keys: tuples,
                live,
            },
            Some(ts) => BuildLookup::AsOf {
                columns,
                keys: tuples,
                ts,
                live,
            },
        };
        let build_plan = planner.plan(&specs, &lookup).unwrap();
        let committed = PlanSource::plan_query(&planner, &specs, &rk, &query).unwrap();
        let via_wp4 = wp4_transient.plan_query(&specs, &rk, &query).unwrap();
        let via_ours = ours_transient.plan_query(&specs, &rk, &query).unwrap();
        let what = run.run_id.clone();
        assert_eq!(build_plan, committed, "{what}: lookup vs committed");
        assert_eq!(build_plan, via_wp4, "{what}: lookup vs WP4 transient");
        assert_eq!(build_plan, via_ours, "{what}: lookup vs this crate");
        catalog.verify_plan(&via_ours).unwrap();
        if let RunQuery::Live(_) = query {
            // without the tombstone sidecar: the plain run's row groups,
            // and its residual followed by the tombstone test (a key whose
            // newest row is a tombstone stays in `In`: nothing knew)
            let unpruned = PlanSource::plan_query(&planner, &plain, &rk, &query).unwrap();
            let plain_query = asof.map_or(RunQuery::Latest, |ts| RunQuery::Asof { ts });
            let plain_plan = PlanSource::plan_query(&planner, &plain, &rk, &plain_query).unwrap();
            assert_eq!(unpruned.entries, plain_plan.entries, "{what}");
            let (Some(Predicate::And { all: with }), Some(Predicate::And { all: without })) =
                (&unpruned.residual, &plain_plan.residual)
            else {
                panic!("{what}: temporal residuals are conjunctions")
            };
            assert_eq!(&with[..with.len() - 1], without.as_slice(), "{what}");
            assert!(
                matches!(with.last(), Some(Predicate::Live { .. })),
                "{what}"
            );
            assert!(build_plan.row_groups_selected <= plain_plan.row_groups_selected);
            if build_plan.row_groups_selected < plain_plan.row_groups_selected {
                fewer += 1;
            }
        }
    }
    assert!(fewer > 0, "tombstones pruned some live run");

    // The replayers over the snapshot, WP4's and this crate's, price every
    // run the same, with the tombstone and without.
    let via_wp4 = Replayer::for_snapshot(&source, &snapshot).unwrap();
    let ours = Replayer::new(
        &source,
        Arc::clone(&catalog),
        Box::new(SegmentPlanner::from_snapshot(&snapshot, Arc::clone(&catalog)).unwrap()),
        specs.clone(),
    )
    .unwrap();
    assert_eq!(
        via_wp4.replay_current(&workload, &keys).unwrap(),
        ours.replay_current(&workload, &keys).unwrap()
    );
    for config in [specs.clone(), plain.clone(), vec![]] {
        assert_eq!(
            via_wp4.replay_specs(&config, &workload, &keys).unwrap(),
            ours.replay_specs(&config, &workload, &keys).unwrap()
        );
    }
}
