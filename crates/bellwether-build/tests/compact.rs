//! Compaction merges each spec's segments with core's merge, commits the
//! merged segments, and tombstones the inputs; plans are unchanged, and a
//! merge never claims coverage its inputs did not have.

#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::cast_possible_wrap,
    clippy::assert_is_empty,
    clippy::too_many_lines,
    clippy::type_complexity,
    clippy::cast_precision_loss,
    clippy::redundant_closure_for_method_calls,
    clippy::format_push_string
)]

mod common;

use bellwether_build::{completeness, BuildOptions, CompactOptions, Lookup};
use bellwether_store::{commit, StagedSegments};
use bellwether_types::ScanPlan;
use common::*;

fn specs() -> Vec<bellwether_types::KeySpec> {
    vec![
        exact(&["id"]),
        bloom(&["grp"]),
        range("ts"),
        temporal(&["id"], "ts"),
        exact(&["id", "grp"]),
    ]
}

fn many_segments(s: &Scratch) -> BuildOptions {
    for f in 0..6 {
        write_numbered(&s.path(&format!("p{f}.parquet")), f * 100, 9, 3);
    }
    BuildOptions {
        max_segment_row_groups: 3,
        ..parquet_opts(&simple_schema())
    }
}

fn lookups() -> Vec<(Vec<bellwether_types::KeySpec>, Lookup)> {
    let ids: Vec<Vec<u8>> = [0, 105, 208, 304, 999]
        .iter()
        .map(|&k| enc_i64(k))
        .collect();
    vec![
        (
            vec![exact(&["id"])],
            Lookup::Keys {
                columns: cols(&["id"]),
                keys: ids.clone(),
            },
        ),
        (
            vec![temporal(&["id"], "ts")],
            Lookup::Latest {
                columns: cols(&["id"]),
                keys: ids.clone(),
            },
        ),
        (
            vec![exact(&["id"]), bloom(&["grp"])],
            Lookup::Keys {
                columns: cols(&["id", "grp"]),
                keys: vec![[enc_i64(105), enc_str("g1")].concat()],
            },
        ),
        (
            vec![exact(&["id", "grp"])],
            Lookup::Keys {
                columns: cols(&["id"]),
                keys: ids,
            },
        ),
        (
            vec![range("ts")],
            Lookup::Between {
                column: "ts".into(),
                lo: enc(
                    &bellwether_core::encoding::Value::Timestamp {
                        value: 2,
                        unit: arrow::datatypes::TimeUnit::Microsecond,
                    },
                    &simple_schema().field(2).data_type().clone(),
                ),
                hi: enc(
                    &bellwether_core::encoding::Value::Timestamp {
                        value: 4,
                        unit: arrow::datatypes::TimeUnit::Microsecond,
                    },
                    &simple_schema().field(2).data_type().clone(),
                ),
            },
        ),
    ]
}

fn plans(store: &std::sync::Arc<bellwether_store::ObjectStoreBackend>) -> Vec<ScanPlan> {
    let p = planner(store);
    lookups()
        .iter()
        .map(|(specs, l)| {
            let mut plan = p.plan(specs, l).unwrap();
            plan.manifest_version.clear();
            plan
        })
        .collect()
}

#[test]
fn compaction_merges_contiguous_segments_and_keeps_every_plan() {
    let s = Scratch::new();
    let opts = many_segments(&s);
    let store = memory_store();
    let b = builder(&store);
    build_ok(&b, &s.root_str(), &specs(), &opts);
    let before = b.manifest().unwrap().unwrap();
    let per_spec = |m: &bellwether_store::Manifest, name: &str| {
        m.segments.iter().filter(|x| x.key_spec == name).count()
    };
    assert_eq!(
        per_spec(&before, "exact:id"),
        6,
        "one segment per file-sized item"
    );
    let plans_before = plans(&store);

    let r = b.compact(&CompactOptions::default()).unwrap();
    assert_eq!(r.sequence, Some(2));
    assert_eq!(r.merges, specs().len() as u64);
    let after = b.manifest().unwrap().unwrap();
    for spec in specs() {
        assert_eq!(per_spec(&after, &spec.name()), 1, "{}", spec.name());
        let seg = after
            .segments
            .iter()
            .find(|x| x.key_spec == spec.name())
            .unwrap();
        assert_eq!(
            (seg.rowgroup_range.start.0, seg.rowgroup_range.end.0),
            (0, 18)
        );
        assert_eq!(
            completeness(&after, &spec),
            Some(1),
            "coverage unchanged: {}",
            spec.name()
        );
    }
    for old in &before.segments {
        assert!(after.is_tombstoned(&old.id), "inputs are tombstoned for GC");
    }
    assert_eq!(plans(&store), plans_before);
    let rows: Vec<Row> = (0..6)
        .flat_map(|f| parquet_rows(&s.path(&format!("p{f}.parquet")), &simple_schema()))
        .collect();
    let p = planner(&store);
    let ids: Vec<Vec<u8>> = (0..600).step_by(7).map(enc_i64).collect();
    check_keys(&p, &rows, &[exact(&["id"])], &cols(&["id"]), &ids);
    check_latest(
        &p,
        &rows,
        &[temporal(&["id"], "ts")],
        &cols(&["id"]),
        &ids,
        &cols(&["id"]),
        "ts",
        None,
    );
    // nothing left to merge
    assert_eq!(
        b.compact(&CompactOptions::default()).unwrap().sequence,
        None
    );
}

#[test]
fn compaction_respects_the_size_limit_and_never_bridges_a_gap() {
    let s = Scratch::new();
    let opts = many_segments(&s);
    let store = memory_store();
    let b = builder(&store);
    build_ok(&b, &s.root_str(), &[exact(&["id"])], &opts);
    let m = b.manifest().unwrap().unwrap();
    let mut segs = m.segments.clone();
    segs.sort_by_key(|x| x.rowgroup_range.start);
    // another writer drops the middle segment: a coverage gap
    let gap = segs[1].clone();
    commit(&*store, "idx", &StagedSegments::new(), |cur| {
        let mut next = cur.clone();
        next.segments.retain(|x| x.id != gap.id);
        Ok(next)
    })
    .unwrap();
    assert_eq!(b.completeness(&exact(&["id"])).unwrap(), None);
    let small = CompactOptions {
        max_output_bytes: segs[2].bytes + segs[3].bytes,
        ..CompactOptions::default()
    };
    b.compact(&small).unwrap();
    let after = b.manifest().unwrap().unwrap();
    let (gs, ge) = (gap.rowgroup_range.start.0, gap.rowgroup_range.end.0);
    for seg in &after.segments {
        let (a, z) = (seg.rowgroup_range.start.0, seg.rowgroup_range.end.0);
        assert!(z <= gs || a >= ge, "{a}..{z} claims the gap {gs}..{ge}");
        assert!(
            seg.bytes
                <= small
                    .max_output_bytes
                    .max(segs.iter().map(|x| x.bytes).max().unwrap())
        );
    }
    assert_eq!(
        b.completeness(&exact(&["id"])).unwrap(),
        None,
        "compaction did not paper over the gap"
    );
    // the next build fills the gap
    build_ok(&b, &s.root_str(), &[exact(&["id"])], &opts);
    assert!(b.completeness(&exact(&["id"])).unwrap().is_some());
}
