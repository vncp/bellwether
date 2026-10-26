//! Concurrent builders (ADR 013, ADR 015): a build whose base changed the
//! row-group space under it aborts and restarts with fresh ids instead of
//! rebasing; a concurrent commit that leaves the row-group space alone is
//! rebased onto.

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

use std::sync::Arc;

use bellwether_build::{completeness, Builder, CompactOptions, Planner};
use common::*;

#[test]
fn a_concurrent_build_that_changes_the_row_group_space_restarts_with_fresh_ids() {
    let s = Scratch::new();
    write_numbered(&s.path("m.parquet"), 0, 8, 4);
    let inner = memory_store();
    let opts = parquet_opts(&simple_schema());
    build_ok(&builder(&inner), &s.root_str(), &[exact(&["id"])], &opts);

    // X will add b.parquet (and a new spec). Between X's planning and its
    // manifest CAS, Y adds a.parquet, which sorts first, and indexes both
    // new files: Y gives a the ids X gave b.
    write_numbered(&s.path("b.parquet"), 200, 8, 4);
    let (root, y_store) = (s.root_str(), Arc::clone(&inner));
    let a_path = s.path("a.parquet");
    let hook = Box::new(move || {
        write_numbered(&a_path, 100, 12, 4);
        let y = Builder::new(y_store, "idx");
        let r = y
            .build(&root, &[exact(&["id"])], &parquet_opts(&simple_schema()))
            .unwrap();
        assert_eq!(r.sequence, Some(2));
    });
    let x_store = Arc::new(HookStore::new(Arc::clone(&inner), hook));
    let x = Builder::new(x_store, "idx");
    let r = x
        .build(&s.root_str(), &[exact(&["id"]), bloom(&["grp"])], &opts)
        .unwrap();
    assert_eq!(
        r.restarts, 1,
        "the first attempt aborted on the row-group space change"
    );
    assert_eq!(r.sequence, Some(3));
    assert_eq!(
        r.files_new, 0,
        "after the restart both new files are tracked by Y"
    );
    assert_eq!(r.specs_rebuilt, vec!["bloom:grp@0.01".to_string()]);

    let m = builder(&inner).manifest().unwrap().unwrap();
    let (live, retired) = check_id_space(&m);
    assert_eq!(live.len(), 2 + 3 + 2);
    assert!(retired.is_empty());
    let ids = |name: &str| {
        m.rowgroup_space
            .files
            .iter()
            .find(|f| f.fingerprint.path == s.path_str(name))
            .unwrap()
            .rowgroup_ids
            .iter()
            .map(|i| i.0)
            .collect::<Vec<_>>()
    };
    assert_eq!(ids("m.parquet"), vec![0, 1]);
    assert_eq!(ids("a.parquet"), vec![2, 3, 4]);
    assert_eq!(ids("b.parquet"), vec![5, 6]);
    assert_eq!(completeness(&m, &exact(&["id"])), Some(1));
    assert_eq!(completeness(&m, &bloom(&["grp"])), Some(3));

    // Every key maps to its own file: had X's first attempt been rebased,
    // its segments would point b's keys at a's ids.
    let rows: Vec<Row> = ["m.parquet", "a.parquet", "b.parquet"]
        .iter()
        .flat_map(|f| parquet_rows(&s.path(f), &simple_schema()))
        .collect();
    let p = planner(&inner);
    let keys: Vec<Vec<u8>> = [0, 7, 100, 111, 200, 207]
        .iter()
        .map(|&k| enc_i64(k))
        .collect();
    check_keys(&p, &rows, &[exact(&["id"])], &cols(&["id"]), &keys);
    let tuples: Vec<Vec<u8>> = [(203, "g3"), (104, "g0")]
        .iter()
        .map(|&(k, g)| [enc_i64(k), enc_str(g)].concat())
        .collect();
    let plan = check_keys(
        &p,
        &rows,
        &[exact(&["id"]), bloom(&["grp"])],
        &cols(&["id", "grp"]),
        &tuples,
    );
    assert_eq!(
        plan_rgs(&plan).into_iter().collect::<Vec<_>>(),
        vec![(s.path_str("a.parquet"), 1), (s.path_str("b.parquet"), 0)]
    );
}

#[test]
fn a_concurrent_compaction_is_rebased_onto_without_restart() {
    let s = Scratch::new();
    write_numbered(&s.path("a.parquet"), 0, 8, 4);
    write_numbered(&s.path("b.parquet"), 100, 8, 4);
    let inner = memory_store();
    let opts = bellwether_build::BuildOptions {
        max_segment_row_groups: 2,
        ..parquet_opts(&simple_schema())
    };
    let specs = [exact(&["id"]), temporal(&["id"], "ts")];
    build_ok(&builder(&inner), &s.root_str(), &specs, &opts);
    assert_eq!(
        builder(&inner).manifest().unwrap().unwrap().segments.len(),
        4
    );

    write_numbered(&s.path("c.parquet"), 200, 8, 4);
    let c_store = Arc::clone(&inner);
    let hook = Box::new(move || {
        let r = Builder::new(c_store, "idx")
            .compact(&CompactOptions::default())
            .unwrap();
        assert_eq!(r.merges, 2);
    });
    let x = Builder::new(Arc::new(HookStore::new(Arc::clone(&inner), hook)), "idx");
    let r = x.build(&s.root_str(), &specs, &opts).unwrap();
    assert_eq!(r.restarts, 0);
    assert_eq!(r.sequence, Some(3));
    let m = builder(&inner).manifest().unwrap().unwrap();
    // per spec: the compacted segment over a and b, and the new one over c
    for spec in &specs {
        let mut ranges: Vec<(u32, u32)> = m
            .segments
            .iter()
            .filter(|x| x.key_spec == spec.identity())
            .map(|x| (x.rowgroup_range.start.0, x.rowgroup_range.end.0))
            .collect();
        ranges.sort_unstable();
        assert_eq!(ranges, vec![(0, 4), (4, 6)], "{}", spec.name());
        assert_eq!(completeness(&m, spec), Some(1));
    }
    let rows: Vec<Row> = ["a.parquet", "b.parquet", "c.parquet"]
        .iter()
        .flat_map(|f| parquet_rows(&s.path(f), &simple_schema()))
        .collect();
    let p = Planner::open(Arc::clone(&inner), "idx").unwrap();
    let keys: Vec<Vec<u8>> = [1, 105, 206].iter().map(|&k| enc_i64(k)).collect();
    check_keys(&p, &rows, &[exact(&["id"])], &cols(&["id"]), &keys);
    check_latest(
        &p,
        &rows,
        &[temporal(&["id"], "ts")],
        &cols(&["id"]),
        &keys,
        &cols(&["id"]),
        "ts",
        None,
    );
}
