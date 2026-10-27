//! ADR 012: adding a key spec builds segments over every tracked row group
//! and records completeness at the commit that finishes; a query on an
//! incomplete spec is an error, never a partial answer.

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

use bellwether_build::{completeness, BuildOptions, Lookup};
use bellwether_core::SegmentReader;
use bellwether_store::{commit, StagedSegments};
use bellwether_types::{Error, KeySpec, Store};
use common::*;

fn three_files(s: &Scratch) {
    write_numbered(&s.path("a.parquet"), 0, 12, 4);
    write_numbered(&s.path("b.parquet"), 100, 12, 4);
    write_numbered(&s.path("c.parquet"), 200, 12, 4);
}

fn all_rows(s: &Scratch) -> Vec<Row> {
    ["a.parquet", "b.parquet", "c.parquet"]
        .iter()
        .flat_map(|f| parquet_rows(&s.path(f), &simple_schema()))
        .collect()
}

#[test]
fn adding_a_spec_builds_every_row_group_and_records_completeness_at_its_commit() {
    let s = Scratch::new();
    three_files(&s);
    let store = memory_store();
    let b = builder(&store);
    let probe = Arc::new(Probe::default());
    let opts = BuildOptions {
        observer: Some(probe.clone()),
        ..parquet_opts(&simple_schema())
    };
    build_ok(&b, &s.root_str(), &[exact(&["id"])], &opts);
    assert_eq!(b.completeness(&exact(&["id"])).unwrap(), Some(1));
    assert_eq!(b.completeness(&bloom(&["grp"])).unwrap(), None);

    let probe = Arc::new(Probe::default());
    let opts = BuildOptions {
        observer: Some(probe.clone()),
        ..parquet_opts(&simple_schema())
    };
    let r = build_ok(&b, &s.root_str(), &[exact(&["id"]), bloom(&["grp"])], &opts);
    assert_eq!(r.sequence, Some(2));
    assert_eq!(r.specs_rebuilt, vec!["bloom:grp@0.01".to_string()]);
    assert_eq!(r.row_groups_read, 9, "every tracked row group, read once");
    // only the new spec's column is read: the exact spec is already complete
    let read_cols = probe.columns.lock().unwrap().clone();
    assert_eq!(
        read_cols.into_iter().collect::<Vec<_>>(),
        vec![cols(&["grp"])]
    );
    let m = b.manifest().unwrap().unwrap();
    assert_eq!(
        completeness(&m, &bloom(&["grp"])),
        Some(2),
        "recorded at the commit that finished"
    );
    assert_eq!(
        completeness(&m, &exact(&["id"])),
        Some(1),
        "kept while continuously complete"
    );
    let p = planner(&store);
    let rows = all_rows(&s);
    check_keys(
        &p,
        &rows,
        &[bloom(&["grp"])],
        &cols(&["grp"]),
        &[enc_str("g1")],
    );
}

#[test]
fn a_query_on_an_incomplete_spec_is_an_error_never_partial() {
    let s = Scratch::new();
    three_files(&s);
    let store = memory_store();
    let b = builder(&store);
    let opts = BuildOptions {
        max_segment_row_groups: 3,
        ..parquet_opts(&simple_schema())
    };
    let spec = exact(&["id"]);
    build_ok(&b, &s.root_str(), std::slice::from_ref(&spec), &opts);
    let m = b.manifest().unwrap().unwrap();
    assert_eq!(m.segments.len(), 3, "one segment per file-sized item");

    // Another writer drops b's segment but leaves the completeness entry.
    let dropped = m.segments[1].clone();
    commit(&*store, "idx", &StagedSegments::new(), |cur| {
        let mut next = cur.clone();
        next.segments.retain(|x| x.id != dropped.id);
        Ok(next)
    })
    .unwrap();
    let m = b.manifest().unwrap().unwrap();
    assert_eq!(
        m.key_spec_completeness.get("exact:id"),
        Some(&1),
        "the stale entry is still there"
    );
    assert_eq!(
        completeness(&m, &spec),
        None,
        "coverage is rechecked, not trusted"
    );
    let p = planner(&store);
    let lookup = Lookup::Keys {
        columns: cols(&["id"]),
        keys: vec![enc_i64(1), enc_i64(105)],
    };
    match p.plan(std::slice::from_ref(&spec), &lookup) {
        Err(Error::KeySpec(msg)) => assert!(msg.contains("not complete"), "{msg}"),
        other => panic!("a partial plan must be an error, got {other:?}"),
    }
    // undeclared, and declared differently under the same name
    assert!(matches!(
        p.plan(&[bloom(&["grp"])], &lookup),
        Err(Error::KeySpec(_))
    ));
    let t1 = temporal(&["id"], "ts");
    assert!(p.plan(&[t1], &lookup).is_err());

    // the next build fills only the gap and records completeness anew
    let r = build_ok(&b, &s.root_str(), std::slice::from_ref(&spec), &opts);
    assert_eq!(r.row_groups_read, 3, "b's three row groups only");
    let m = b.manifest().unwrap().unwrap();
    assert_eq!(completeness(&m, &spec), Some(3));
    let p = planner(&store);
    check_keys(
        &p,
        &all_rows(&s),
        &[spec],
        &cols(&["id"]),
        &[enc_i64(1), enc_i64(105)],
    );
}

#[test]
fn a_failed_build_commits_nothing() {
    let s = Scratch::new();
    three_files(&s);
    let store = memory_store();
    let b = builder(&store);
    let opts = parquet_opts(&simple_schema());
    build_ok(&b, &s.root_str(), &[exact(&["id"])], &opts);
    let before = manifest_bytes(&store);
    // a new file whose `id` is a string cannot be encoded as the dataset's
    // Int64: the build fails and nothing is published
    let bad = arrow::array::RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(arrow::array::StringArray::from(vec!["x"])) as arrow::array::ArrayRef,
        ),
        ("grp", Arc::new(arrow::array::StringArray::from(vec!["g0"]))),
        (
            "ts",
            Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![1])),
        ),
    ])
    .unwrap();
    write_parquet(&s.path("d.parquet"), &bad, 10);
    assert!(b
        .build(&s.root_str(), &[exact(&["id"]), bloom(&["grp"])], &opts)
        .is_err());
    assert_eq!(manifest_bytes(&store), before);
    assert_eq!(b.completeness(&bloom(&["grp"])).unwrap(), None);
    assert_eq!(b.completeness(&exact(&["id"])).unwrap(), Some(1));
}

fn segment_specs(store: &Arc<bellwether_store::ObjectStoreBackend>, name: &str) -> Vec<KeySpec> {
    let m = builder(store).manifest().unwrap().unwrap();
    m.segments
        .iter()
        .filter(|x| x.key_spec == name)
        .map(|x| {
            let bytes = store
                .get(&bellwether_store::segment_path("idx", &x.id))
                .unwrap()
                .0;
            SegmentReader::open(bytes).unwrap().key_spec().clone()
        })
        .collect()
}

#[test]
fn a_definition_change_under_the_same_name_rebuilds_the_spec() {
    let s = Scratch::new();
    // a second timestamp column, so two temporal specs share `temporal:id`
    let rows: Vec<(i64, &str, i64)> = (0..12).map(|i| (i % 5, "g", i)).collect();
    write_simple(&s.path("a.parquet"), &rows, 4);
    let store = memory_store();
    let b = builder(&store);
    let opts = parquet_opts(&simple_schema());
    let by_ts = temporal(&["id"], "ts");
    let by_grp = KeySpec::Temporal {
        columns: cols(&["id"]),
        ts_column: "id".into(),
        counts: false,
        tombstone: None,
    };
    assert_eq!(by_ts.name(), by_grp.name());
    build_ok(&b, &s.root_str(), std::slice::from_ref(&by_ts), &opts);
    // declaring both is refused: an index holds one spec per name
    let err = b
        .build(&s.root_str(), &[by_ts.clone(), by_grp.clone()], &opts)
        .unwrap_err();
    assert!(matches!(err, Error::KeySpec(_)), "{err}");
    let r = build_ok(&b, &s.root_str(), std::slice::from_ref(&by_grp), &opts);
    assert_eq!(r.specs_rebuilt, vec!["temporal:id@id".to_string()]);
    let m = b.manifest().unwrap().unwrap();
    assert_eq!(completeness(&m, &by_grp), Some(2));
    assert_eq!(completeness(&m, &by_ts), None);
    let specs = segment_specs(&store, "temporal:id@id");
    assert!(!specs.is_empty());
    assert!(
        specs.iter().all(|k| k == &by_grp),
        "no segment of the old definition is live"
    );
}

/// Completeness and segments are keyed by [`KeySpec::identity`], which
/// holds every field of the spec. `bloom:grp@0.01` and `bloom:grp@0.05`
/// share the name `bloom:grp`; keyed by name, the old filter's segments
/// and completeness entry would serve the new declaration.
#[test]
fn completeness_is_keyed_by_identity_not_name() {
    let s = Scratch::new();
    three_files(&s);
    let store = memory_store();
    let b = builder(&store);
    let opts = parquet_opts(&simple_schema());
    let tight = bloom(&["grp"]);
    let loose = KeySpec::Bloom {
        columns: cols(&["grp"]),
        fpr: 0.05,
        bounds: None,
    };
    assert_eq!(tight.name(), loose.name());
    assert_ne!(tight.identity(), loose.identity());
    build_ok(&b, &s.root_str(), std::slice::from_ref(&tight), &opts);
    let m = b.manifest().unwrap().unwrap();
    assert_eq!(
        m.key_spec_completeness.keys().collect::<Vec<_>>(),
        vec!["bloom:grp@0.01"]
    );
    assert_eq!(completeness(&m, &tight), Some(1));
    assert_eq!(completeness(&m, &loose), None, "not declared");

    let r = build_ok(&b, &s.root_str(), std::slice::from_ref(&loose), &opts);
    assert_eq!(r.specs_rebuilt, vec!["bloom:grp@0.05".to_string()]);
    let m = b.manifest().unwrap().unwrap();
    assert_eq!(
        m.key_spec_completeness.keys().collect::<Vec<_>>(),
        vec!["bloom:grp@0.05"],
        "recorded under the new identity only"
    );
    assert_eq!(completeness(&m, &loose), Some(2), "recorded at the rebuild");
    assert_eq!(completeness(&m, &tight), None);
    assert!(m.segments.iter().all(|x| x.key_spec == "bloom:grp@0.05"));
    let specs = segment_specs(&store, "bloom:grp@0.05");
    assert!(!specs.is_empty());
    assert!(
        specs.iter().all(|k| k == &loose),
        "no segment of the 0.01 filter is live"
    );

    // A foreign writer re-declares the 0.01 filter but leaves the 0.05
    // segments and entry: the 0.01 spec is not complete, and planning it
    // is an error.
    commit(&*store, "idx", &StagedSegments::new(), |cur| {
        let mut next = cur.clone();
        next.schema.key_specs = vec![tight.clone()];
        Ok(next)
    })
    .unwrap();
    let m = b.manifest().unwrap().unwrap();
    assert_eq!(completeness(&m, &tight), None);
    assert_eq!(completeness(&m, &loose), None);
    let p = planner(&store);
    let err = p
        .plan(
            std::slice::from_ref(&tight),
            &Lookup::Keys {
                columns: cols(&["grp"]),
                keys: vec![b"g".to_vec()],
            },
        )
        .unwrap_err();
    assert!(matches!(err, Error::KeySpec(_)), "{err}");
}

#[test]
fn removing_a_spec_takes_remove_unlisted_and_drops_its_segments_and_completeness() {
    let s = Scratch::new();
    three_files(&s);
    let store = memory_store();
    let b = builder(&store);
    let opts = parquet_opts(&simple_schema());
    build_ok(&b, &s.root_str(), &[exact(&["id"]), bloom(&["grp"])], &opts);
    let bloom_ids: Vec<_> = b
        .manifest()
        .unwrap()
        .unwrap()
        .segments
        .iter()
        .filter(|x| x.key_spec == "bloom:grp@0.01")
        .map(|x| x.id)
        .collect();
    // listing only exact:id keeps bloom:grp; nothing to do, nothing committed
    let r = b.build(&s.root_str(), &[exact(&["id"])], &opts).unwrap();
    assert_eq!(r.sequence, None);
    assert!(r.specs_removed.is_empty());
    // removal is explicit
    let removing = BuildOptions {
        remove_unlisted: true,
        ..opts.clone()
    };
    let r = build_ok(&b, &s.root_str(), &[exact(&["id"])], &removing);
    assert_eq!(r.specs_removed, vec!["bloom:grp@0.01".to_string()]);
    assert_eq!(r.row_groups_read, 0);
    let m = b.manifest().unwrap().unwrap();
    assert!(m.segments.iter().all(|x| x.key_spec == "exact:id"));
    assert!(!m.key_spec_completeness.contains_key("bloom:grp@0.01"));
    assert_eq!(m.schema.key_specs, vec![exact(&["id"])]);
    for id in bloom_ids {
        assert!(m.is_tombstoned(&id), "dropped segments wait for GC");
    }
}

#[test]
fn remove_spec_removes_exactly_the_declared_spec() {
    let s = Scratch::new();
    three_files(&s);
    let store = memory_store();
    let b = builder(&store);
    let opts = parquet_opts(&simple_schema());
    let t = temporal(&["id"], "ts");
    build_ok(&b, &s.root_str(), &[exact(&["id"]), t.clone()], &opts);
    let before = b.manifest().unwrap().unwrap();
    // a different spec under the same name is refused, never removed in its place
    let other = KeySpec::Temporal {
        columns: cols(&["id"]),
        ts_column: "grp".into(),
        counts: false,
        tombstone: None,
    };
    assert!(b.remove_spec(&other).is_err());
    assert_eq!(
        b.remove_spec(&bloom(&["grp"])).unwrap(),
        None,
        "not declared"
    );
    assert_eq!(manifest_bytes(&store), {
        let m = builder(&store).manifest().unwrap().unwrap();
        assert_eq!(m.sequence, before.sequence);
        manifest_bytes(&store)
    });
    let seq = b.remove_spec(&t).unwrap();
    assert_eq!(seq, Some(before.sequence + 1));
    let m = b.manifest().unwrap().unwrap();
    assert_eq!(m.schema.key_specs, vec![exact(&["id"])]);
    assert!(m.segments.iter().all(|x| x.key_spec == "exact:id"));
    assert!(!m.key_spec_completeness.contains_key("temporal:id@ts"));
    assert_eq!(completeness(&m, &exact(&["id"])), Some(1));
    // later builds no longer index it
    write_numbered(&s.path("d.parquet"), 300, 12, 4);
    let r = build_ok(&b, &s.root_str(), &[exact(&["id"])], &opts);
    assert!(r.specs_rebuilt.is_empty());
    let m = b.manifest().unwrap().unwrap();
    assert!(m.segments.iter().all(|x| x.key_spec == "exact:id"));
}
