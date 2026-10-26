//! List keys (B4): predicted equals planned. Over a corpus with list and
//! map columns, a committed index built by `bellwether_build::Builder` and
//! this crate's transient segments for the same list keys plan every
//! membership run (`contains_any`, `contains_all`) to the same `ScanPlan`
//! (files, row groups, bytes, residual) as the build planner's own
//! `Lookup::ContainsAny` / `ContainsAll`, four ways:
//!
//! 1. the build `Planner` with the lookup (what `locate --any/--all` runs);
//! 2. the build `Planner` as a `PlanSource` over the committed segments;
//! 3. the build `Planner` with this crate's transient segments in their
//!    place (`PlanSource::with_transient`);
//! 4. this crate's `SegmentPlanner` with the same transient segments.
//!
//! The replayer over the snapshot and the replayer over an unindexed
//! folder price every run the same; the baselines read every row group;
//! and recording refuses what a membership run cannot be.

#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::single_match_else,
    clippy::assert_is_empty
)]

mod common;

use std::collections::BTreeSet;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float64Array, Float64Builder, Int64Array, Int64Builder, ListBuilder,
    MapBuilder, RecordBatch, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bellwether_build::{BuildOptions, Builder, Lookup as BuildLookup, Planner};
use bellwether_replay::{
    Baseline, Catalog, KeyInput, MemoryKeySets, PlanSource, Replayer, RunQuery, SegmentPlanner,
    TransientIndex, TransientLimits, WorkloadRecorder,
};
use bellwether_source::ParquetSource;
use bellwether_store::{ObjectStoreBackend, Snapshot};
use bellwether_types::{KeySpec, Source};
use common::Rng;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

const TAGS: u64 = 40;

fn schema() -> SchemaRef {
    let tags = ListBuilder::new(StringBuilder::new()).finish();
    let vals = ListBuilder::new(Float64Builder::new()).finish();
    let attrs = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new()).finish();
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("tags", tags.data_type().clone(), true),
        Field::new("vals", vals.data_type().clone(), true),
        Field::new("attrs", attrs.data_type().clone(), true),
    ]))
}

/// Six files of four row groups of 25 rows: tags skewed over 40 values and
/// clustered by row group (so a rare tag prunes), duplicates, null and
/// empty lists, null elements, float zeros of both signs and NaN, and maps.
fn write_corpus(dir: &Path) {
    let mut g = Rng(11);
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(1 << 20))
        .build();
    let mut id = 0i64;
    for f in 0..6 {
        let path = dir.join(format!("part-{f}.parquet"));
        let mut w =
            ArrowWriter::try_new(File::create(path).unwrap(), schema(), Some(props.clone()))
                .unwrap();
        for rg in 0..4u64 {
            let mut ids = Vec::new();
            let mut tags = ListBuilder::new(StringBuilder::new());
            let mut vals = ListBuilder::new(Float64Builder::new());
            let mut attrs = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
            let base = (f as u64 * 4 + rg) % TAGS;
            for _ in 0..25 {
                ids.push(id);
                id += 1;
                match g.below(10) {
                    0 => tags.append(false),
                    1 => tags.append(true),
                    _ => {
                        for _ in 0..=g.below(4) {
                            if g.below(12) == 0 {
                                tags.values().append_null();
                            } else if g.below(3) == 0 {
                                // hot: the first three tags, anywhere
                                tags.values().append_value(format!("t{}", g.below(3)));
                            } else {
                                // clustered: near this row group's base
                                let t = (base + g.below(3)) % TAGS;
                                tags.values().append_value(format!("t{t}"));
                            }
                        }
                        tags.append(true);
                    }
                }
                match g.below(8) {
                    0 => vals.append(false),
                    _ => {
                        for _ in 0..g.below(3) {
                            let v =
                                [0.0, -0.0, f64::NAN, 1.5, 2.0 + rg as f64][g.below(5) as usize];
                            vals.values().append_value(v);
                        }
                        vals.append(true);
                    }
                }
                match g.below(6) {
                    0 => attrs.append(false).unwrap(),
                    _ => {
                        let keys: BTreeSet<u64> = (0..g.below(3)).map(|_| g.below(9)).collect();
                        for k in keys {
                            attrs.keys().append_value(format!("k{k}"));
                            attrs.values().append_value(k as i64);
                        }
                        attrs.append(true).unwrap();
                    }
                }
            }
            let columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(tags.finish()),
                Arc::new(vals.finish()),
                Arc::new(attrs.finish()),
            ];
            w.write(&RecordBatch::try_new(schema(), columns).unwrap())
                .unwrap();
            w.flush().unwrap();
        }
        w.close().unwrap();
    }
}

fn list(c: &str) -> KeySpec {
    KeySpec::List { column: c.into() }
}

/// One key batch: a single column named after the list column, of its
/// element type.
fn key_batch(column: &str, values: ArrayRef) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            column,
            values.data_type().clone(),
            true,
        )])),
        vec![values],
    )
    .unwrap()
}

fn strings(v: &[&str]) -> ArrayRef {
    Arc::new(StringArray::from(v.to_vec()))
}

/// Membership runs on every list column, both kinds.
fn runs() -> Vec<(String, &'static str, ArrayRef, RunQuery)> {
    let mut g = Rng(5);
    let mut out = Vec::new();
    for i in 0..10 {
        let n = 1 + g.below(4) as usize;
        let tags: Vec<String> = (0..n).map(|_| format!("t{}", g.below(TAGS + 3))).collect();
        let tags: Vec<&str> = tags.iter().map(String::as_str).collect();
        for query in [RunQuery::ContainsAny, RunQuery::ContainsAll] {
            out.push((
                format!("tags-{i}-{}", query.kind_str()),
                "tags",
                strings(&tags),
                query,
            ));
        }
    }
    for (i, vals) in [
        vec![0.0],
        vec![-0.0],
        vec![f64::NAN],
        vec![-0.0, 3.0],
        vec![1.5, 4.0],
        vec![99.0],
    ]
    .into_iter()
    .enumerate()
    {
        for query in [RunQuery::ContainsAny, RunQuery::ContainsAll] {
            out.push((
                format!("vals-{i}-{}", query.kind_str()),
                "vals",
                Arc::new(Float64Array::from(vals.clone())) as ArrayRef,
                query,
            ));
        }
    }
    for (i, keys) in [vec!["k1"], vec!["k2", "k7"], vec!["k0", "zz"]]
        .into_iter()
        .enumerate()
    {
        for query in [RunQuery::ContainsAny, RunQuery::ContainsAll] {
            out.push((
                format!("attrs-{i}-{}", query.kind_str()),
                "attrs",
                strings(&keys),
                query,
            ));
        }
    }
    out
}

#[test]
fn membership_runs_plan_identically_committed_transient_and_in_crate() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    write_corpus(&data);
    let data = data.to_str().unwrap().to_string();
    let store = Arc::new(ObjectStoreBackend::memory().unwrap());
    let specs = vec![list("tags"), list("vals"), list("attrs")];
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
    assert_eq!(PlanSource::specs(&planner), specs, "every spec is complete");
    assert!(
        snapshot.manifest().segments.len() > specs.len(),
        "the committed index is split"
    );
    let source = ParquetSource::new().with_schema(schema());
    let root = snapshot.manifest().source.root.clone();
    let catalog = Arc::new(
        Catalog::from_manifest(snapshot.manifest(), &source, source.schema(&root).unwrap())
            .unwrap(),
    );
    let segment_planner = SegmentPlanner::new(Arc::clone(&catalog));
    // the transient builder splits too
    let limits = TransientLimits {
        max_segment_rows: 160,
        ..TransientLimits::default()
    };
    let keys = MemoryKeySets::new();
    let mut rec = WorkloadRecorder::new(&keys, schema());
    for (id, column, values, query) in runs() {
        rec.record_query(
            &id,
            KeyInput::Batch(&key_batch(column, values)),
            query,
            None,
        )
        .unwrap();
    }
    let workload = rec.finish().unwrap();
    let total = catalog.len() as u64;
    let (mut pruned, mut planned_runs) = (0, 0);
    for spec in &specs {
        let config = std::slice::from_ref(spec);
        let KeySpec::List { column } = spec else {
            unreachable!()
        };
        let transient = Arc::new(TransientIndex::build(&source, &catalog, spec, &limits).unwrap());
        assert!(transient.stats().segments > 1, "{}", spec.name());
        let wp4_transient =
            PlanSource::with_transient(&planner, &[Arc::clone(&transient)]).unwrap();
        let ours_transient = segment_planner
            .with_transient(&[Arc::clone(&transient)])
            .unwrap();
        for run in workload.runs().iter().filter(|r| &r.columns[0] == column) {
            let rk = workload.run_keys(&keys, run).unwrap();
            let query = run.query().unwrap();
            let values: Vec<Vec<u8>> = rk.tuples().map(<[u8]>::to_vec).collect();
            let lookup = match query {
                RunQuery::ContainsAny => BuildLookup::ContainsAny {
                    column: column.clone(),
                    values,
                },
                RunQuery::ContainsAll => BuildLookup::ContainsAll {
                    column: column.clone(),
                    values,
                },
                _ => unreachable!(),
            };
            let build_plan = planner.plan(config, &lookup).unwrap();
            let committed = PlanSource::plan_query(&planner, config, &rk, &query).unwrap();
            let via_wp4 = wp4_transient.plan_query(config, &rk, &query).unwrap();
            let via_ours = ours_transient.plan_query(config, &rk, &query).unwrap();
            let what = format!("{} {}", spec.name(), run.run_id);
            assert_eq!(build_plan, committed, "{what}: lookup vs committed");
            assert_eq!(build_plan, via_wp4, "{what}: lookup vs WP4 transient");
            assert_eq!(build_plan, via_ours, "{what}: lookup vs this crate");
            catalog.verify_plan(&via_ours).unwrap();
            // no spec: every live row group, the same residual
            let none = planner.plan(&[], &lookup).unwrap();
            assert_eq!(none.row_groups_selected, total, "{what}");
            assert_eq!(none.residual, build_plan.residual, "{what}");
            let ours_none = segment_planner.plan_query(&[], &rk, &query).unwrap();
            assert_eq!(none, ours_none, "{what}: no spec");
            planned_runs += 1;
            if build_plan.row_groups_selected < total {
                pruned += 1;
            }
        }
    }
    assert_eq!(planned_runs, workload.runs().len());
    assert!(pruned > workload.runs().len() / 2, "{pruned} runs pruned");

    // Replayers over the snapshot (WP4), over the snapshot through this
    // crate, and over the unindexed folder price every run the same, for
    // every list key and none.
    let via_wp4 = Replayer::for_snapshot(&source, &snapshot).unwrap();
    let ours = Replayer::new(
        &source,
        Arc::clone(&catalog),
        Box::new(SegmentPlanner::from_snapshot(&snapshot, Arc::clone(&catalog)).unwrap()),
        specs.clone(),
    )
    .unwrap();
    let unindexed =
        Replayer::unindexed(&source, &root, &bellwether_source::Partitioning::None).unwrap();
    assert_eq!(
        via_wp4.replay_current(&workload, &keys).unwrap(),
        ours.replay_current(&workload, &keys).unwrap()
    );
    for config in [specs.clone(), vec![]] {
        let a = via_wp4.replay_specs(&config, &workload, &keys).unwrap();
        assert_eq!(a, ours.replay_specs(&config, &workload, &keys).unwrap());
        let b = unindexed.replay_specs(&config, &workload, &keys).unwrap();
        for (x, y) in a.iter().zip(&b) {
            assert_eq!(
                (x.files, x.row_groups, x.bytes),
                (y.files, y.row_groups, y.bytes),
                "{}: indexed vs unindexed",
                x.run_id
            );
        }
    }
    // Both baselines read every live row group for a membership run, with
    // the membership residual.
    for baseline in [Baseline::RawScan, Baseline::ParquetStats] {
        let costs = via_wp4.replay_baseline(baseline, &workload, &keys).unwrap();
        for c in &costs {
            assert_eq!(c.row_groups, total, "{baseline:?} {}", c.run_id);
        }
    }
    // A membership run no list key serves, priced under the min/max
    // baseline for unserved runs: every row group (a list's statistics are
    // on its nested leaf), the same plan as the raw scan's.
    let by_stats = Replayer::unindexed(&source, &root, &bellwether_source::Partitioning::None)
        .unwrap()
        .with_unserved(Baseline::ParquetStats);
    let raw = unindexed.replay_specs(&[], &workload, &keys).unwrap();
    let stats = by_stats.replay_specs(&[], &workload, &keys).unwrap();
    for (r, s) in raw.iter().zip(&stats) {
        assert_eq!(
            (r.files, r.row_groups, r.bytes),
            (s.files, s.row_groups, s.bytes),
            "{}",
            r.run_id
        );
    }
    // ...and with the list key, the min/max baseline never applies.
    let served = by_stats.replay_specs(&specs, &workload, &keys).unwrap();
    assert_eq!(
        served,
        via_wp4.replay_specs(&specs, &workload, &keys).unwrap()
    );
}

/// A transient list key accounts each row group's distinct elements and
/// their bytes (ADR 018): the open writer's key bytes are measured, a small
/// per-segment key-byte cap splits the index, and an index cap below what
/// the elements need stops the build with an error, never a partial index.
#[test]
fn transient_list_builds_account_their_elements() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    write_corpus(&data);
    let source = ParquetSource::new().with_schema(schema());
    let catalog = Arc::new(Catalog::discover(&source, data.to_str().unwrap(), schema()).unwrap());
    let spec = list("tags");
    let whole =
        TransientIndex::build(&source, &catalog, &spec, &TransientLimits::default()).unwrap();
    let bytes = whole.stats().largest_segment_key_bytes;
    assert!(bytes > 0, "{:?}", whole.stats());
    assert_eq!(whole.stats().segments, 1);
    // A key-byte cap of a third splits it.
    let split = TransientIndex::build(
        &source,
        &catalog,
        &spec,
        &TransientLimits {
            max_segment_key_bytes: bytes / 3,
            ..TransientLimits::default()
        },
    )
    .unwrap();
    assert!(split.stats().segments >= 3, "{:?}", split.stats());
    // An index cap below the elements' bytes: refused.
    let refused = TransientIndex::build(
        &source,
        &catalog,
        &spec,
        &TransientLimits {
            max_index_bytes: bytes / 2,
            ..TransientLimits::default()
        },
    );
    assert!(refused.is_err());
}

#[test]
fn recording_refuses_what_a_membership_run_cannot_be() {
    let keys = MemoryKeySets::new();
    let mut rec = WorkloadRecorder::new(&keys, schema());
    // a flat column as a membership run
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
    let e = rec
        .record_query(
            "a",
            KeyInput::Batch(&key_batch("id", ids.clone())),
            RunQuery::ContainsAny,
            None,
        )
        .unwrap_err()
        .to_string();
    assert!(e.contains("not a list column"), "{e}");
    // a list column as a point lookup
    let e = rec
        .record_query(
            "b",
            KeyInput::Batch(&key_batch("tags", strings(&["t1"]))),
            RunQuery::Locate,
            None,
        )
        .unwrap_err()
        .to_string();
    assert!(e.contains("--any") && e.contains("--all"), "{e}");
    // two columns
    let two = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("tags", DataType::Utf8, true),
            Field::new("id", DataType::Int64, true),
        ])),
        vec![strings(&["t1"]), Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let e = rec
        .record_query("c", KeyInput::Batch(&two), RunQuery::ContainsAll, None)
        .unwrap_err()
        .to_string();
    assert!(e.contains("exactly one list column"), "{e}");
    // a null value in a contains_all run: no row could match
    let with_null: ArrayRef = Arc::new(StringArray::from(vec![Some("t1"), None]));
    let e = rec
        .record_query(
            "d",
            KeyInput::Batch(&key_batch("tags", Arc::clone(&with_null))),
            RunQuery::ContainsAll,
            None,
        )
        .unwrap_err()
        .to_string();
    assert!(e.contains("null") && e.contains("contains_any"), "{e}");
    // ...which contains_any drops and counts
    let run = rec
        .record_query(
            "e",
            KeyInput::Batch(&key_batch("tags", with_null)),
            RunQuery::ContainsAny,
            None,
        )
        .unwrap();
    assert_eq!((run.keys, run.null_tuples), (1, 1));
    // a value of the wrong type for the element
    let e = rec
        .record_query(
            "f",
            KeyInput::Batch(&key_batch("tags", ids)),
            RunQuery::ContainsAny,
            None,
        )
        .unwrap_err()
        .to_string();
    assert!(!e.is_empty());
}
