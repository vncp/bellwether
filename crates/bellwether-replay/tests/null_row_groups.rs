//! A hypothetical spec over a corpus with all-null row groups: row groups
//! whose key columns are null in every row hold no keys (ADR 014) but are
//! processed and covered (ADR 012).
//!
//! - Every spec kind builds over them, with the default limits and with one
//!   row group per segment (so two adjacent all-null row groups each get a
//!   key-less segment over its own range), and every catalog row group lies
//!   in exactly one segment's processed range, which the segment's own bytes
//!   record (core's `set_rowgroup_range`): the two adjacent key-less
//!   segments have different bytes, so different content ids.
//! - No plan selects an all-null row group for a non-null key; exact paths
//!   equal the oracle, every path contains it, and no plan depends on how
//!   the index is split.
//! - A run whose every tuple is null has no keys and plans nothing.
//! - Two catalogs that differ only by an all-null row group hold the same
//!   keys; the transient cache keys them apart (by catalog digest), each
//!   index covers its own catalog, their segment bytes differ, and neither
//!   plans for the other.

#![allow(clippy::unwrap_used, missing_docs, clippy::too_many_lines)]

mod common;

use std::collections::BTreeSet;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray, UInt64Array,
};
use bellwether_replay::{
    Catalog, KeyInput, MemoryKeySets, PlanSource, Replayer, SegmentPlanner, TransientCache,
    TransientIndex, TransientLimits, WorkloadRecorder,
};
use bellwether_types::{KeySpec, Predicate, RowGroupId};
use common::{Corpus, CorpusSpec};
use parquet::arrow::ArrowWriter;

const NULL_RGS: [(usize, usize); 3] = [(0, 1), (2, 0), (2, 1)];
const RG_ROWS: usize = 100;

fn cols(c: &[&str]) -> Vec<String> {
    c.iter().map(|s| (*s).to_string()).collect()
}

fn corpus() -> Corpus {
    common::write_corpus(&CorpusSpec {
        files: 4,
        row_groups_per_file: 3,
        rows_per_row_group: RG_ROWS,
        entities: 400,
        null_row_groups: NULL_RGS.to_vec(),
        ..CorpusSpec::default()
    })
}

fn null_cells(corpus: &Corpus) -> BTreeSet<(String, u32)> {
    let files = corpus.files();
    NULL_RGS
        .iter()
        .map(|&(f, rg)| {
            (
                files[f].to_str().unwrap().to_string(),
                u32::try_from(rg).unwrap(),
            )
        })
        .collect()
}

#[test]
fn all_null_row_groups_are_covered_never_selected_and_split_invariant() {
    let corpus = corpus();
    let source = corpus.source();
    let runs = common::entity_runs(&corpus, 16, 11);
    let (keys, workload) = common::record(&["entity_id"], &runs);
    let nulls = null_cells(&corpus);
    // (spec, whether its path is exact for runs on entity_id)
    let specs = [
        (
            KeySpec::Exact {
                columns: cols(&["entity_id"]),
                counts: false,
                bounds: None,
            },
            true,
        ),
        (
            KeySpec::Temporal {
                columns: cols(&["entity_id"]),
                ts_column: "version_ts".into(),
                counts: false,
                tombstone: None,
            },
            true,
        ),
        (
            KeySpec::Exact {
                columns: cols(&["entity_id", "batch_id"]),
                counts: false,
                bounds: None,
            },
            true,
        ),
        (
            KeySpec::Range {
                column: "entity_id".into(),
            },
            false,
        ),
        (
            KeySpec::Bloom {
                columns: cols(&["entity_id"]),
                fpr: 0.01,
                bounds: None,
            },
            false,
        ),
    ];
    let one_per_rg = TransientLimits {
        max_segment_rows: RG_ROWS as u64,
        ..TransientLimits::default()
    };
    let mut hits = 0;
    for (spec, exact) in &specs {
        let name = spec.name();
        let mut by_limits = Vec::new();
        for limits in [TransientLimits::default(), one_per_rg] {
            let replayer = Replayer::unindexed(
                &source,
                &corpus.data,
                &bellwether_source::Partitioning::None,
            )
            .unwrap()
            .with_limits(limits);
            let catalog = replayer.catalog();
            let idx = replayer.transient(spec).unwrap();
            assert_eq!(idx.covered(), catalog.all_ids(), "{name}: coverage");
            let ranges = idx.segment_ranges();
            assert_eq!(ranges.len() as u64, idx.stats().segments);
            let segments = idx.segments();
            for (r, range) in segments.readers().iter().zip(ranges) {
                assert_eq!(
                    r.rowgroup_range(),
                    Some(*range),
                    "{name}: the bytes record the processed range"
                );
            }
            assert!(
                ranges.windows(2).all(|w| w[0].1 < w[1].0),
                "{name}: ranges are disjoint and ordered: {ranges:?}"
            );
            if limits == one_per_rg {
                let each: Vec<(RowGroupId, RowGroupId)> = catalog
                    .row_groups()
                    .iter()
                    .map(|rg| (rg.id, rg.id))
                    .collect();
                assert_eq!(
                    ranges,
                    each.as_slice(),
                    "{name}: one segment per row group, the key-less ones kept"
                );
                // File 2's row groups 0 and 1 are all null: catalog ids 6, 7.
                let bytes = idx.segment_bytes();
                assert_eq!(bytes.len(), each.len());
                assert_ne!(
                    bytes[6], bytes[7],
                    "{name}: two key-less segments over different row groups share bytes"
                );
            } else {
                assert_eq!(ranges.len(), 1, "{name}: one segment under the defaults");
            }
            let mut cells = Vec::new();
            for run in workload.runs() {
                let rk = workload.run_keys(&keys, run).unwrap();
                let plan = replayer
                    .plan_of_keys(std::slice::from_ref(spec), &rk)
                    .unwrap();
                let got = common::cells(&plan);
                let tuples = &runs.iter().find(|(id, _)| *id == run.run_id).unwrap().1;
                let want = common::oracle(&corpus, &["entity_id"], tuples);
                assert!(
                    got.is_disjoint(&nulls),
                    "{name} {}: selects an all-null row group",
                    run.run_id
                );
                assert!(
                    want.is_subset(&got),
                    "{name} {}: misses {:?}",
                    run.run_id,
                    want.difference(&got).collect::<Vec<_>>()
                );
                if *exact {
                    assert_eq!(got, want, "{name} {}", run.run_id);
                }
                hits += want.len();
                cells.push(got);
            }
            by_limits.push(cells);
        }
        assert_eq!(
            by_limits[0], by_limits[1],
            "{name}: splitting changes a plan"
        );
    }
    assert!(hits > 0, "the runs must find keys (non-vacuity)");
}

#[test]
fn a_run_of_only_null_tuples_has_no_keys_and_plans_nothing() {
    let corpus = corpus();
    let source = corpus.source();
    let replayer = Replayer::unindexed(
        &source,
        &corpus.data,
        &bellwether_source::Partitioning::None,
    )
    .unwrap();
    let keys = MemoryKeySets::new();
    let mut rec = WorkloadRecorder::new(&keys, Arc::clone(&corpus.schema));
    let batch = RecordBatch::try_from_iter(vec![(
        "entity_id",
        Arc::new(UInt64Array::from(vec![None::<u64>, None])) as ArrayRef,
    )])
    .unwrap();
    rec.record("nulls", KeyInput::Batch(&batch)).unwrap();
    let workload = rec.finish().unwrap();
    let run = workload.run("nulls").unwrap();
    assert_eq!((run.keys, run.null_tuples), (0, 2));
    let rk = workload.run_keys(&keys, run).unwrap();
    for spec in [
        KeySpec::Exact {
            columns: cols(&["entity_id"]),
            counts: false,
            bounds: None,
        },
        KeySpec::Range {
            column: "entity_id".into(),
        },
    ] {
        let plan = replayer
            .plan_of_keys(std::slice::from_ref(&spec), &rk)
            .unwrap();
        assert_eq!(plan.row_groups_selected, 0);
        assert_eq!(plan.entries.len(), 0);
        assert!(
            matches!(&plan.residual, Some(Predicate::In { values, .. }) if values.is_empty()),
            "{:?}",
            plan.residual
        );
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let p = e.unwrap().path();
        let dest = to.join(p.file_name().unwrap());
        if p.is_dir() {
            copy_dir(&p, &dest);
        } else {
            std::fs::copy(&p, &dest).unwrap();
        }
    }
}

#[test]
fn catalogs_differing_only_by_an_all_null_row_group_are_keyed_apart() {
    let corpus = corpus();
    let schema = Arc::clone(&corpus.schema);
    let source = corpus.source();
    // B: A's files plus one file of a single all-null row group, sorting last.
    let b_dir = tempfile::tempdir().unwrap();
    copy_dir(Path::new(&corpus.data), b_dir.path());
    let extra = b_dir.path().join("day=99");
    std::fs::create_dir_all(&extra).unwrap();
    let n = 50;
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from(vec![None::<u64>; n])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1_i64; n])),
            Arc::new(TimestampMicrosecondArray::from(vec![None::<i64>; n]).with_timezone("UTC")),
            Arc::new(StringArray::from(vec!["new"; n])),
            Arc::new(StringArray::from(vec!["p"; n])),
        ],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(
        File::create(extra.join("part-099.parquet")).unwrap(),
        Arc::clone(&schema),
        None,
    )
    .unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();

    let a = Catalog::discover(&source, &corpus.data, Arc::clone(&schema)).unwrap();
    let b =
        Catalog::discover(&source, b_dir.path().to_str().unwrap(), Arc::clone(&schema)).unwrap();
    assert_eq!(b.len(), a.len() + 1);
    assert_eq!(a.manifest_version(), b.manifest_version());
    assert_ne!(a.digest(), b.digest());
    let spec = KeySpec::Exact {
        columns: cols(&["entity_id"]),
        counts: false,
        bounds: None,
    };
    let cache = TransientCache::new(1 << 30);
    let lim = TransientLimits::default();
    let ia = cache
        .get_or_build(&a, &spec, || {
            TransientIndex::build(&source, &a, &spec, &lim)
        })
        .unwrap();
    let ib = cache
        .get_or_build(&b, &spec, || {
            TransientIndex::build(&source, &b, &spec, &lim)
        })
        .unwrap();
    let st = cache.stats().unwrap();
    assert_eq!((st.builds, st.hits, st.entries), (2, 0, 2), "no alias");
    assert_eq!(ia.covered(), a.all_ids());
    assert_eq!(ib.covered(), b.all_ids());
    let extra_id = b.row_groups().last().unwrap().id;
    assert!(ib.covered().contains(extra_id.0) && !ia.covered().contains(extra_id.0));
    assert_ne!(
        ia.segment_bytes(),
        ib.segment_bytes(),
        "the same keys over different row groups are different segments"
    );
    let a = Arc::new(a);
    let b = Arc::new(b);
    assert!(SegmentPlanner::new(Arc::clone(&a))
        .with_transient(&[Arc::clone(&ib)])
        .is_err());
    assert!(SegmentPlanner::new(Arc::clone(&b))
        .with_transient(&[Arc::clone(&ia)])
        .is_err());
}
