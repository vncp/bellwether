//! ADR 013: fingerprint drift retires row-group ids, ids are never reused,
//! and retired row groups never appear in a plan.

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

use std::time::{Duration, SystemTime};

use common::*;

fn ids_of(m: &bellwether_store::Manifest, path: &str) -> Vec<u32> {
    m.rowgroup_space
        .files
        .iter()
        .find(|f| f.fingerprint.path == path)
        .map(|f| f.rowgroup_ids.iter().map(|i| i.0).collect())
        .unwrap_or_default()
}

#[test]
fn ids_are_never_reused_across_rewrites_removals_and_readds() {
    let s = Scratch::new();
    let (a, bb) = (s.path("a.parquet"), s.path("b.parquet"));
    write_numbered(&a, 0, 12, 4);
    write_numbered(&bb, 100, 8, 4);
    let (a_s, b_s) = (s.path_str("a.parquet"), s.path_str("b.parquet"));
    let store = memory_store();
    let b = builder(&store);
    let specs = [exact(&["id"]), temporal(&["id"], "ts")];
    let opts = parquet_opts(&simple_schema());
    build_ok(&b, &s.root_str(), &specs, &opts);
    let m = b.manifest().unwrap().unwrap();
    assert_eq!(ids_of(&m, &a_s), vec![0, 1, 2]);
    assert_eq!(ids_of(&m, &b_s), vec![3, 4]);
    check_id_space(&m);

    // rewrite a: its three ids are retired, it gets two fresh ones
    write_numbered(&a, 50, 8, 4);
    let r = build_ok(&b, &s.root_str(), &specs, &opts);
    assert_eq!((r.files_changed, r.files_unchanged), (1, 1));
    assert_eq!((r.row_groups_retired, r.row_groups_assigned), (3, 2));
    assert_eq!(r.row_groups_read, 2, "only the rewritten file is read");
    let m = b.manifest().unwrap().unwrap();
    assert_eq!(ids_of(&m, &a_s), vec![5, 6]);
    assert_eq!(ids_of(&m, &b_s), vec![3, 4]);
    let (_, retired) = check_id_space(&m);
    assert_eq!(retired.into_iter().collect::<Vec<_>>(), vec![0, 1, 2]);
    for spec in &specs {
        assert_eq!(
            bellwether_build::completeness(&m, spec),
            Some(1),
            "continuously complete"
        );
    }

    // plans see only the current versions: a's old keys are gone, new ones
    // map to a's fresh row groups by index
    let rows: Vec<Row> = [&a, &bb]
        .iter()
        .flat_map(|p| parquet_rows(p, &simple_schema()))
        .collect();
    let p = planner(&store);
    let old_keys: Vec<Vec<u8>> = (0..12).map(enc_i64).collect();
    let plan = check_keys(&p, &rows, &[exact(&["id"])], &cols(&["id"]), &old_keys);
    assert_eq!(
        plan.row_groups_selected, 0,
        "retired row groups never appear"
    );
    let plan = check_keys(
        &p,
        &rows,
        &[exact(&["id"])],
        &cols(&["id"]),
        &[enc_i64(55), enc_i64(103)],
    );
    assert_eq!(
        plan_rgs(&plan).into_iter().collect::<Vec<_>>(),
        vec![(a_s.clone(), 1), (b_s.clone(), 0)]
    );
    assert_eq!(plan.row_groups_total, 4);

    // remove b: its ids are retired
    let b_bytes = std::fs::read(&bb).unwrap();
    std::fs::remove_file(&bb).unwrap();
    let r = build_ok(&b, &s.root_str(), &specs, &opts);
    assert_eq!(
        (r.files_removed, r.row_groups_retired, r.row_groups_read),
        (1, 2, 0)
    );
    let m = b.manifest().unwrap().unwrap();
    assert!(ids_of(&m, &b_s).is_empty());
    check_id_space(&m);

    // put the identical bytes back: a first sighting again, fresh ids
    std::fs::write(&bb, &b_bytes).unwrap();
    build_ok(&b, &s.root_str(), &specs, &opts);
    let m = b.manifest().unwrap().unwrap();
    assert_eq!(ids_of(&m, &b_s), vec![7, 8]);
    let (live, retired) = check_id_space(&m);
    assert_eq!(live.into_iter().collect::<Vec<_>>(), vec![5, 6, 7, 8]);
    assert_eq!(retired.into_iter().collect::<Vec<_>>(), vec![0, 1, 2, 3, 4]);
    let rows: Vec<Row> = [&a, &bb]
        .iter()
        .flat_map(|p| parquet_rows(p, &simple_schema()))
        .collect();
    let p = planner(&store);
    let all: Vec<Vec<u8>> = (0..120).map(enc_i64).collect();
    let plan = check_keys(&p, &rows, &[exact(&["id"])], &cols(&["id"]), &all);
    assert_eq!(plan.row_groups_selected, 4);
    check_latest(
        &p,
        &rows,
        &[temporal(&["id"], "ts")],
        &cols(&["id"]),
        &all,
        &cols(&["id"]),
        "ts",
        None,
    );
}

#[test]
fn touching_a_file_changes_its_identity() {
    let s = Scratch::new();
    let a = s.path("a.parquet");
    write_numbered(&a, 0, 6, 3);
    let store = memory_store();
    let b = builder(&store);
    let opts = parquet_opts(&simple_schema());
    build_ok(&b, &s.root_str(), &[exact(&["id"])], &opts);
    // same bytes, new mtime: the fingerprint carries the mtime, so this is
    // drift and the file is re-indexed under fresh ids
    let f = std::fs::File::options().write(true).open(&a).unwrap();
    f.set_modified(SystemTime::now() + Duration::from_secs(3600))
        .unwrap();
    drop(f);
    let drift = b.verify().unwrap();
    assert_eq!(drift.len(), 1);
    assert_eq!(
        drift[0].old.tail_blake3,
        drift[0].new.as_ref().unwrap().tail_blake3
    );
    let r = build_ok(&b, &s.root_str(), &[exact(&["id"])], &opts);
    assert_eq!(
        (r.files_changed, r.row_groups_retired, r.row_groups_assigned),
        (1, 2, 2)
    );
    let m = b.manifest().unwrap().unwrap();
    assert_eq!(ids_of(&m, &s.path_str("a.parquet")), vec![2, 3]);
    check_id_space(&m);
    assert!(b.verify().unwrap().is_empty());
}
