//! Counts through every path that changes which row groups are live, and
//! the refusals around them, against rows decoded with the `parquet` crate:
//!
//! - counts and plan rows through many segments, compaction, a file rewrite
//!   that retires row groups, and compaction again;
//! - a composite lookup is counted only by a spec on exactly its columns;
//! - two segments of one counted spec over the same row groups are refused
//!   as a corrupt index, never summed;
//! - compaction and garbage collection over a storage version 1 manifest
//!   keep it at version 1, in version 1's keys, and `--columns` reads that
//!   manifest's column bytes from the footers; the next build with work
//!   upgrades it with the arrays;
//! - a footer that disagrees with the recorded row-group count during the
//!   upgrade fill is drift: the file's ids are retired and reassigned;
//! - a key or time column containing `,` or `@` is refused when declared,
//!   since two specs would share an identity.

#![allow(
    missing_docs,
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation
)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_build::{BuildOptions, CompactOptions, Lookup};
use bellwether_store::{Manifest, ObjectStoreBackend};
use bellwether_types::{Error, KeySpec, Store, StoreVersion};
use bytes::Bytes;
use common::*;
use parquet::file::reader::{FileReader, SerializedFileReader};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Utf8, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]))
}

type R = (Option<i64>, Option<&'static str>, Option<i64>);

fn write(path: &Path, rows: &[R], per_rg: usize) {
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(rows.iter().map(|r| r.0).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(StringArray::from(rows.iter().map(|r| r.1).collect::<Vec<_>>())),
            Arc::new(
                TimestampMicrosecondArray::from(rows.iter().map(|r| r.2).collect::<Vec<_>>())
                    .with_timezone("UTC"),
            ),
        ],
    )
    .unwrap();
    write_parquet(path, &batch, per_rg);
}

/// Keys 0..5 with nulls on `a` (not indexed), on `b` (a trailing null,
/// counted under its marker tuple) and on `ts`.
fn rows(seed: i64, n: i64) -> Vec<R> {
    let tags = [Some("x"), None, Some("y"), Some("x"), None];
    (0..n)
        .map(|i| {
            let a = if (i + seed) % 7 == 3 {
                None
            } else {
                Some((i * 3 + seed) % 5)
            };
            let ts = if i % 6 == 1 { None } else { Some(i * 1_000) };
            (a, tags[usize::try_from((i + seed) % 5).unwrap()], ts)
        })
        .collect()
}

fn ex(c: &[&str], counts: bool) -> KeySpec {
    KeySpec::Exact {
        columns: cols(c),
        counts,
        bounds: None,
    }
}

fn manifest(store: &ObjectStoreBackend) -> Manifest {
    Manifest::from_slice(&manifest_bytes(store)).unwrap()
}

/// Oracle: rows, live row groups, and files holding any of `keys` on
/// `columns` (a tuple with a null matches nothing), from the decoded files.
fn oracle(
    store: &ObjectStoreBackend,
    s: &Scratch,
    names: &[&str],
    columns: &[String],
    keys: &[Vec<u8>],
) -> (u64, u64, u64) {
    let m = manifest(store);
    let want: BTreeSet<&Vec<u8>> = keys.iter().collect();
    let (mut rows, mut rgs, mut files) = (0u64, BTreeSet::new(), BTreeSet::new());
    for n in names {
        let p = s.path(n);
        let f = m
            .rowgroup_space
            .files
            .iter()
            .find(|f| f.fingerprint.path == p.to_str().unwrap())
            .unwrap();
        for r in parquet_rows(&p, &schema()) {
            if let Some(t) = r.tuple(columns) {
                if want.contains(&t) {
                    rows += 1;
                    rgs.insert(f.rowgroup_ids[r.rg as usize].0);
                    files.insert((*n).to_string());
                }
            }
        }
    }
    (rows, rgs.len() as u64, files.len() as u64)
}

fn keys_a() -> Vec<Vec<u8>> {
    (0..6).map(enc_i64).collect()
}

fn keys_ab() -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    for a in 0..5 {
        for b in ["x", "y", "z"] {
            let mut k = enc_i64(a);
            k.extend(enc_str(b));
            out.push(k);
        }
    }
    out
}

/// `count` and the plan's rows, for several key sets on `a` and on
/// `(a, b)`, equal the decoded files; no footer is opened.
fn check_count(
    store: &Arc<ObjectStoreBackend>,
    s: &Scratch,
    names: &[&str],
    specs: &[KeySpec],
    what: &str,
) {
    let p = planner(store);
    for (columns, keysets) in [
        (
            cols(&["a"]),
            vec![keys_a(), vec![enc_i64(1)], vec![enc_i64(4), enc_i64(2)]],
        ),
        (cols(&["a", "b"]), vec![keys_ab(), keys_ab()[..4].to_vec()]),
    ] {
        for keys in keysets {
            let want = oracle(store, s, names, &columns, &keys);
            let got = p
                .count(specs, &columns, &keys)
                .unwrap()
                .unwrap_or_else(|| panic!("{what}: no count on {columns:?}"));
            assert_eq!(
                (got.rows, got.row_groups, got.files),
                want,
                "{what}: count on {columns:?} against the decoded files"
            );
            let plan = p
                .plan(
                    specs,
                    &Lookup::Keys {
                        columns: columns.clone(),
                        keys: keys.clone(),
                    },
                )
                .unwrap();
            assert_eq!(plan.rows.map(|q| q.value), Some(want.0), "{what}: plan rows");
        }
    }
    assert_eq!(p.footers_opened(), 0, "{what}: footers opened");
}

/// Counts equal the decoded files through many segments, compaction, a
/// rewrite that retires a file's row groups, and compaction again: a
/// retired row group's counts are never added.
#[test]
fn counts_follow_compaction_and_rewrites_and_never_count_retired_row_groups() {
    let s = Scratch::new();
    write(&s.path("f1.parquet"), &rows(0, 41), 3);
    write(&s.path("f2.parquet"), &rows(2, 37), 4);
    let names = ["f1.parquet", "f2.parquet"];
    let store = memory_store();
    let b = builder(&store);
    let specs = vec![ex(&["a"], true), ex(&["a", "b"], true)];
    let opts = BuildOptions {
        max_segment_row_groups: 1,
        ..parquet_opts(&schema())
    };
    build_ok(&b, &s.root_str(), &specs, &opts);
    let n_before = manifest(&store).segments.len();
    assert!(n_before > 4, "{n_before}");
    check_count(&store, &s, &names, &specs, "many segments");
    let c = b.compact(&CompactOptions::default()).unwrap();
    assert!(c.merges > 0, "{c:?}");
    assert!(manifest(&store).segments.len() < n_before);
    check_count(&store, &s, &names, &specs, "after compaction");
    write(&s.path("f2.parquet"), &rows(9, 29), 2);
    build_ok(&b, &s.root_str(), &specs, &opts);
    assert!(
        !manifest(&store).rowgroup_space.tombstones.is_empty(),
        "the rewrite retires f2's row groups"
    );
    check_count(&store, &s, &names, &specs, "after a rewrite");
    let c = b.compact(&CompactOptions::default()).unwrap();
    assert!(c.merges > 0, "{c:?}");
    check_count(&store, &s, &names, &specs, "after a rewrite and compaction");
}

/// A composite lookup is counted only by a counting spec on exactly its
/// columns: `exact:a,counts` counts rows of every `b` sharing an `a`, so it
/// never counts an `(a, b)` lookup.
#[test]
fn only_a_spec_on_exactly_the_lookup_columns_counts_it() {
    let s = Scratch::new();
    write(&s.path("f1.parquet"), &rows(0, 41), 3);
    let store = memory_store();
    let specs = vec![ex(&["a"], true), ex(&["a", "b"], false)];
    build_ok(&builder(&store), &s.root_str(), &specs, &parquet_opts(&schema()));
    let p = planner(&store);
    let lookup = Lookup::Keys {
        columns: cols(&["a", "b"]),
        keys: keys_ab(),
    };
    let plan = p.plan(&specs, &lookup).unwrap();
    assert_eq!(plan.rows, None, "exact:a,counts counted an (a, b) lookup");
    assert!(p
        .count(&specs, &cols(&["a", "b"]), &keys_ab())
        .unwrap()
        .is_none());
    // The same spec does count a lookup on `a`, and a permuted composite
    // spec with counts counts `(a, b)`.
    let c = p.count(&specs, &cols(&["a"]), &keys_a()).unwrap().unwrap();
    let want = oracle(&store, &s, &["f1.parquet"], &cols(&["a"]), &keys_a());
    assert_eq!(c.rows, want.0);
    let permuted = vec![ex(&["b", "a"], true)];
    let store2 = memory_store();
    build_ok(&builder(&store2), &s.root_str(), &permuted, &parquet_opts(&schema()));
    let c = planner(&store2)
        .count(&permuted, &cols(&["a", "b"]), &keys_ab())
        .unwrap()
        .unwrap();
    let want = oracle(&store2, &s, &["f1.parquet"], &cols(&["a", "b"]), &keys_ab());
    assert_eq!(c.rows, want.0);
}

/// Two committed segments of one counted spec over the same row groups (a
/// manifest another writer produced, or one edited): `count` and a plan
/// refuse it as a corrupt index naming the spec, never summing the rows
/// twice.
#[test]
fn overlapping_counted_segments_are_refused_not_summed() {
    let s = Scratch::new();
    write(&s.path("f1.parquet"), &rows(0, 41), 3);
    let specs = vec![ex(&["a"], true)];
    let store1 = memory_store();
    build_ok(
        &builder(&store1),
        &s.root_str(),
        &specs,
        &BuildOptions {
            max_segment_row_groups: 2,
            ..parquet_opts(&schema())
        },
    );
    let store2 = memory_store();
    build_ok(&builder(&store2), &s.root_str(), &specs, &parquet_opts(&schema()));
    let m2 = manifest(&store2);
    assert_eq!(m2.segments.len(), 1);
    let seg = m2.segments[0].clone();
    let path = format!("idx/segments/{}.seg", seg.id.to_hex());
    let (bytes, _) = store2.get(&path).unwrap();
    store1.put_if_absent(&path, bytes).unwrap();
    let (mbytes, version): (Bytes, StoreVersion) = store1.get("idx/manifest.json").unwrap();
    let mut m1 = Manifest::from_slice(&mbytes).unwrap();
    m1.segments.push(seg);
    store1
        .put_if_version("idx/manifest.json", Bytes::from(m1.to_vec().unwrap()), &version)
        .unwrap();
    let p = planner(&store1);
    let refused = |r: Result<(), Error>, what: &str| match r {
        Err(Error::Other(m)) => assert!(
            m.starts_with("corrupt index:") && m.contains("exact:a,counts"),
            "{what}: {m}"
        ),
        other => panic!("{what}: expected a corrupt-index refusal, got {other:?}"),
    };
    refused(
        p.count(&specs, &cols(&["a"]), &keys_a()).map(|_| ()),
        "count",
    );
    refused(
        p.plan(
            &specs,
            &Lookup::Keys {
                columns: cols(&["a"]),
                keys: keys_a(),
            },
        )
        .map(|_| ()),
        "plan",
    );
}

/// The identity of each declared spec in a manifest, and its name.
fn identities(m: &Manifest) -> BTreeMap<String, String> {
    m.schema
        .key_specs
        .iter()
        .map(|s| (s.identity(), s.name()))
        .collect()
}

/// Rewrites the index's manifest as a storage version 1 binary wrote it:
/// version 1, no row-group arrays, segments and completeness keyed by each
/// spec's name. `edit` changes the file entries further.
fn make_version_1(store: &ObjectStoreBackend, edit: impl FnOnce(&mut serde_json::Value)) {
    let (bytes, version) = store.get("idx/manifest.json").unwrap();
    let names = identities(&Manifest::from_slice(&bytes).unwrap());
    let mut v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    v["storage_version"] = 1.into();
    for f in v["rowgroup_space"]["files"].as_array_mut().unwrap() {
        let f = f.as_object_mut().unwrap();
        for k in ["rowgroup_rows", "rowgroup_bytes", "column_bytes"] {
            f.remove(k);
        }
    }
    for seg in v["segments"].as_array_mut().unwrap() {
        let id = seg["key_spec"].as_str().unwrap().to_string();
        seg["key_spec"] = names[&id].clone().into();
    }
    let completeness = v["key_spec_completeness"].as_object().unwrap().clone();
    v["key_spec_completeness"] = completeness
        .into_iter()
        .map(|(k, n)| (names[&k].clone(), n))
        .collect::<serde_json::Map<_, _>>()
        .into();
    edit(&mut v);
    store
        .put_if_version(
            "idx/manifest.json",
            Bytes::from(serde_json::to_vec_pretty(&v).unwrap()),
            &version,
        )
        .unwrap();
}

/// What a version 1 binary reads: the stored manifest's version and the
/// raw keys of its segments and completeness.
fn stored_keys(store: &ObjectStoreBackend) -> (u64, BTreeSet<String>) {
    let v: serde_json::Value = serde_json::from_slice(&manifest_bytes(store)).unwrap();
    let mut keys: BTreeSet<String> = v["segments"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["key_spec"].as_str().unwrap().to_string())
        .collect();
    keys.extend(v["key_spec_completeness"].as_object().unwrap().keys().cloned());
    (v["storage_version"].as_u64().unwrap(), keys)
}

/// `(rows, bytes, column bytes)` per row group, from the footer.
fn footer(path: &str, columns: &[String]) -> (Vec<u64>, Vec<u64>, Vec<u64>) {
    let reader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
    let (mut rows, mut bytes, mut cb) = (Vec::new(), Vec::new(), Vec::new());
    for rg in reader.metadata().row_groups() {
        rows.push(u64::try_from(rg.num_rows()).unwrap());
        bytes.push(u64::try_from(rg.compressed_size()).unwrap());
        for c in columns {
            let ch = rg.columns().iter().find(|x| x.column_descr().name() == c);
            cb.push(ch.map_or(0, |x| u64::try_from(x.compressed_size()).unwrap()));
        }
    }
    (rows, bytes, cb)
}

/// Every file's arrays equal its footer, and the manifest is version 2.
fn check_arrays(store: &ObjectStoreBackend) {
    let m = manifest(store);
    assert_eq!(m.storage_version, 2);
    let columns: Vec<String> = m.schema.columns.iter().map(|c| c.name.clone()).collect();
    for f in &m.rowgroup_space.files {
        let (rows, bytes, cb) = footer(&f.fingerprint.path, &columns);
        let p = &f.fingerprint.path;
        assert_eq!(f.rowgroup_ids.len(), rows.len(), "{p}");
        assert_eq!(f.rowgroup_rows.as_ref(), Some(&rows), "{p}");
        assert_eq!(f.rowgroup_bytes.as_ref(), Some(&bytes), "{p}");
        assert_eq!(f.column_bytes.as_ref(), Some(&cb), "{p}");
    }
}

/// Compaction and garbage collection over a storage version 1 manifest
/// cannot fill the version 2 arrays (they read no footer), so they keep it
/// at version 1 and write it in version 1's keys (a version 1 binary still
/// reads it); a projection on it reads the column bytes from the footers
/// and says how many; the next build with work upgrades it with the arrays.
#[test]
fn compaction_and_gc_keep_a_version_1_manifest_at_version_1() {
    let s = Scratch::new();
    write(&s.path("f1.parquet"), &rows(0, 41), 3);
    let store = memory_store();
    let specs = vec![
        ex(&["a"], false),
        KeySpec::Bloom {
            columns: cols(&["b"]),
            fpr: 0.01,
            bounds: None,
        },
    ];
    let b = builder(&store);
    let opts = BuildOptions {
        max_segment_row_groups: 1,
        ..parquet_opts(&schema())
    };
    build_ok(&b, &s.root_str(), &specs, &opts);
    make_version_1(&store, |_| {});
    let v1_keys: BTreeSet<String> = ["exact:a", "bloom:b"].map(String::from).into();
    assert_eq!(stored_keys(&store), (1, v1_keys.clone()));

    let c = b.compact(&CompactOptions::default()).unwrap();
    assert!(c.merges > 0, "{c:?}");
    assert_eq!(stored_keys(&store), (1, v1_keys.clone()), "after compaction");
    let m = manifest(&store);
    assert!(m.rowgroup_space.files.iter().all(|f| f.rowgroup_bytes.is_none()));
    bellwether_store::gc(&*store, "idx", Duration::ZERO).unwrap();
    assert_eq!(stored_keys(&store), (1, v1_keys), "after garbage collection");

    // A projection reads the column bytes the manifest lacks from the
    // footers, and they equal the footer's.
    let p = planner(&store);
    let mut plan = p
        .plan(
            &specs,
            &Lookup::Keys {
                columns: cols(&["a"]),
                keys: vec![enc_i64(1)],
            },
        )
        .unwrap();
    let from_footers = p.project(&mut plan, &cols(&["a", "ts"])).unwrap();
    assert_eq!(from_footers, plan.entries.len() as u64);
    let (_, _, cb) = footer(&s.path("f1.parquet").to_string_lossy(), &cols(&["a", "b", "ts"]));
    let want: u64 = plan
        .entries
        .iter()
        .flat_map(|e| e.row_groups.iter())
        .map(|rg| cb[rg as usize * 3] + cb[rg as usize * 3 + 2])
        .sum();
    assert_eq!(plan.projected_bytes, Some(want));

    // A build with nothing to do commits nothing; one with work upgrades.
    let r = b.build(&s.root_str(), &specs, &opts).unwrap();
    assert!(r.sequence.is_none(), "{r:?}");
    write(&s.path("f2.parquet"), &rows(3, 20), 4);
    build_ok(&b, &s.root_str(), &specs, &opts);
    check_arrays(&store);
    let p = planner(&store);
    let mut plan = p
        .plan(
            &specs,
            &Lookup::Keys {
                columns: cols(&["a"]),
                keys: vec![enc_i64(1)],
            },
        )
        .unwrap();
    assert_eq!(p.project(&mut plan, &cols(&["a"])).unwrap(), 0);
    assert_eq!(p.footers_opened(), 0);
}

/// During the first build over a version 1 manifest, a file whose
/// fingerprint is unchanged but whose footer lists another number of row
/// groups than the manifest records is drift (ADR 013): its ids are retired
/// and fresh ones assigned, so the arrays align with the footer and counts
/// stay right.
#[test]
fn a_row_group_count_that_disagrees_with_the_footer_during_the_fill_is_drift() {
    let s = Scratch::new();
    write(&s.path("f1.parquet"), &rows(0, 41), 10);
    let names = ["f1.parquet", "f2.parquet"];
    let store = memory_store();
    let specs = vec![ex(&["a"], true), ex(&["a", "b"], true)];
    let b = builder(&store);
    build_ok(&b, &s.root_str(), &specs, &parquet_opts(&schema()));
    let before = manifest(&store);
    let f1 = &before.rowgroup_space.files[0];
    assert_eq!(f1.rowgroup_ids.len(), 5);
    let next = before.rowgroup_space.next_id;
    // Counted specs cannot sit in a version 1 manifest, so declare the
    // version 1 index with the plain specs it would have held.
    make_version_1(&store, |v| {
        let ids = v["rowgroup_space"]["files"][0]["rowgroup_ids"]
            .as_array_mut()
            .unwrap();
        ids.push(next.into());
        v["rowgroup_space"]["next_id"] = (next + 1).into();
        for spec in v["schema"]["key_specs"].as_array_mut().unwrap() {
            spec.as_object_mut().unwrap().remove("counts");
        }
        v["segments"] = serde_json::json!([]);
        v["key_spec_completeness"] = serde_json::json!({});
    });
    let old: Vec<u32> = manifest(&store).rowgroup_space.files[0]
        .rowgroup_ids
        .iter()
        .map(|id| id.0)
        .collect();
    assert_eq!(old.len(), 6);
    write(&s.path("f2.parquet"), &rows(3, 20), 4);
    let r = build_ok(&b, &s.root_str(), &specs, &parquet_opts(&schema()));
    assert_eq!(r.files_filled, 0, "{r:?}");
    let m = manifest(&store);
    let retired: BTreeSet<u32> = m
        .rowgroup_space
        .tombstones
        .iter()
        .flat_map(|t| t.rowgroup_ids.iter().map(|id| id.0))
        .collect();
    assert!(old.iter().all(|id| retired.contains(id)), "{retired:?}");
    let f1 = m
        .rowgroup_space
        .files
        .iter()
        .find(|f| f.fingerprint.path.ends_with("f1.parquet"))
        .unwrap();
    assert!(f1.rowgroup_ids.iter().all(|id| id.0 > next), "{f1:?}");
    check_arrays(&store);
    check_count(&store, &s, &names, &specs, "after the drift");
}

/// Two different specs never share an identity: a key or time column
/// containing `,` or `@` is refused when declared, so `exact:a` with counts
/// and `exact` on a column named `a,counts` cannot both be built.
#[test]
fn a_spec_column_with_an_identity_separator_is_refused_when_declared() {
    let s = Scratch::new();
    let sch = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("a,counts", DataType::Int64, true),
        Field::new("t@s", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&sch),
        vec![
            Arc::new(Int64Array::from((0..40).collect::<Vec<i64>>())) as ArrayRef,
            Arc::new(Int64Array::from((0..40).map(|i| 1000 + i).collect::<Vec<i64>>())),
            Arc::new(Int64Array::from((0..40).collect::<Vec<i64>>())),
        ],
    )
    .unwrap();
    write_parquet(&s.path("f.parquet"), &batch, 5);
    assert_eq!(
        ex(&["a"], true).identity(),
        ex(&["a,counts"], false).identity(),
        "the two specs the rule keeps apart"
    );
    for (spec, column) in [
        (ex(&["a,counts"], false), "a,counts"),
        (
            KeySpec::Temporal {
                columns: cols(&["a"]),
                ts_column: "t@s".into(),
                counts: false,
            },
            "t@s",
        ),
    ] {
        let store = memory_store();
        let specs = vec![ex(&["a"], true), spec];
        match builder(&store).build(&s.root_str(), &specs, &parquet_opts(&sch)) {
            Err(Error::KeySpec(m)) => assert!(
                m.contains(&format!("\"{column}\"")) && m.contains("',' or '@'"),
                "{m}"
            ),
            other => panic!("expected a key spec refusal, got {other:?}"),
        }
        assert!(store.get("idx/manifest.json").is_err(), "nothing committed");
    }
}
