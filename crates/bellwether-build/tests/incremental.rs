//! Incremental builds: only files whose fingerprint is new are read, the
//! work is linear in the files (counted, not timed), and a build with
//! nothing to do reads nothing and commits nothing.

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

use std::sync::atomic::Ordering;
use std::sync::Arc;

use bellwether_build::BuildOptions;
use common::*;

const RGS_PER_FILE: u64 = 3;

fn write_files(s: &Scratch, from: usize, to: usize) {
    for f in from..to {
        write_numbered(
            &s.path(&format!("day-{f:03}.parquet")),
            (f as i64) * 1000,
            12,
            4,
        );
    }
}

fn counted(probe: &Arc<Probe>) -> BuildOptions {
    BuildOptions {
        observer: Some(probe.clone()),
        max_segment_row_groups: 64,
        ..parquet_opts(&simple_schema())
    }
}

fn specs() -> Vec<bellwether_types::KeySpec> {
    vec![
        exact(&["id"]),
        bloom(&["grp"]),
        temporal(&["id"], "ts"),
        range("ts"),
    ]
}

#[test]
fn source_calls_are_linear_in_the_new_files() {
    for files in [2usize, 4, 8] {
        let s = Scratch::new();
        write_files(&s, 0, files);
        let store = memory_store();
        let probe = Arc::new(Probe::default());
        let r = build_ok(&builder(&store), &s.root_str(), &specs(), &counted(&probe));
        let n = files as u64;
        assert_eq!(
            probe.laid_out.load(Ordering::SeqCst),
            n,
            "one layout per new file"
        );
        assert_eq!(
            probe.listed.load(Ordering::SeqCst),
            n,
            "one row_groups call per new file"
        );
        assert_eq!(
            probe.reads.load(Ordering::SeqCst),
            n * RGS_PER_FILE,
            "one read per row group, for four specs"
        );
        assert_eq!((r.files_listed, r.row_groups_read), (n, n * RGS_PER_FILE));
    }
}

#[test]
fn only_new_and_changed_files_are_read() {
    let s = Scratch::new();
    write_files(&s, 0, 5);
    let store = memory_store();
    let b = builder(&store);
    build_ok(
        &b,
        &s.root_str(),
        &specs(),
        &counted(&Arc::new(Probe::default())),
    );

    write_files(&s, 5, 7);
    let probe = Arc::new(Probe::default());
    let r = build_ok(&b, &s.root_str(), &specs(), &counted(&probe));
    assert_eq!((r.files_new, r.files_unchanged, r.files_changed), (2, 5, 0));
    assert_eq!(probe.laid_out.load(Ordering::SeqCst), 2);
    assert_eq!(probe.reads.load(Ordering::SeqCst), 2 * RGS_PER_FILE);
    let listed = probe.listed_files.lock().unwrap().clone();
    assert_eq!(
        listed,
        vec![s.path_str("day-005.parquet"), s.path_str("day-006.parquet")]
    );
    assert!(r.specs_rebuilt.is_empty());
    let m = b.manifest().unwrap().unwrap();
    for spec in specs() {
        assert_eq!(
            bellwether_build::completeness(&m, &spec),
            Some(1),
            "{}",
            spec.name()
        );
    }

    // nothing changed: nothing read, nothing committed
    let before = manifest_bytes(&store);
    let probe = Arc::new(Probe::default());
    let r = b.build(&s.root_str(), &specs(), &counted(&probe)).unwrap();
    assert_eq!(r.sequence, None);
    assert_eq!(
        (
            probe.laid_out.load(Ordering::SeqCst),
            probe.listed.load(Ordering::SeqCst),
            probe.reads.load(Ordering::SeqCst)
        ),
        (0, 0, 0)
    );
    assert_eq!(manifest_bytes(&store), before);

    // a rewritten file is read again, under fresh ids
    write_numbered(&s.path("day-002.parquet"), 9000, 8, 4);
    let probe = Arc::new(Probe::default());
    let r = build_ok(&b, &s.root_str(), &specs(), &counted(&probe));
    assert_eq!(
        (r.files_changed, r.row_groups_retired, r.row_groups_assigned),
        (1, 3, 2)
    );
    assert_eq!(probe.reads.load(Ordering::SeqCst), 2);
    let rows: Vec<Row> = (0..7)
        .flat_map(|f| parquet_rows(&s.path(&format!("day-{f:03}.parquet")), &simple_schema()))
        .collect();
    let p = planner(&store);
    let keys: Vec<Vec<u8>> = [1, 2001, 9003, 5011, 6000]
        .iter()
        .map(|&k| enc_i64(k))
        .collect();
    let plan = check_keys(&p, &rows, &[exact(&["id"])], &cols(&["id"]), &keys);
    assert_eq!(
        plan.row_groups_selected, 4,
        "2001 retired; 1, 9003, 5011, 6000 live"
    );
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

#[test]
fn source_root_kind_and_policy_are_part_of_the_index() {
    let s = Scratch::new();
    write_files(&s, 0, 2);
    let store = memory_store();
    let b = builder(&store);
    let opts = parquet_opts(&simple_schema());
    build_ok(&b, &s.root_str(), &[exact(&["id"])], &opts);
    let other = Scratch::new();
    write_files(&other, 0, 1);
    assert!(
        b.build(&other.root_str(), &[exact(&["id"])], &opts)
            .is_err(),
        "another root"
    );
    let csv = BuildOptions {
        source: bellwether_build::SourceKind::csv(),
        ..opts.clone()
    };
    assert!(
        b.build(&s.root_str(), &[exact(&["id"])], &csv).is_err(),
        "another kind"
    );
    let full = BuildOptions {
        fingerprint_policy: Some(bellwether_source::FingerprintPolicy::Full),
        ..opts.clone()
    };
    assert!(
        b.build(&s.root_str(), &[exact(&["id"])], &full).is_err(),
        "another policy"
    );
    let chunked = BuildOptions {
        chunk_size: Some(64),
        ..opts
    };
    assert!(
        b.build(&s.root_str(), &[exact(&["id"])], &chunked).is_err(),
        "chunk size on Parquet"
    );
}
