//! The round 1 reviewer probes of storage version 2 (WP14/B1), kept as
//! regression tests. Independent oracle: rows decoded with the `parquet`
//! crate. Where a probe found a defect (overlapping counted segments summed,
//! a version 1 manifest stamped version 2 by compaction, two specs sharing
//! an identity), it now asserts the fix.

#![allow(
    missing_docs,
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::many_single_char_names
)]

mod common;

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_build::{CompactOptions, Lookup};
use bellwether_store::{Manifest, ObjectStoreBackend};
use bellwether_types::{Hash, KeySpec, Record, Store, StoreVersion};
use bytes::Bytes;
use common::*;

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
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(
                TimestampMicrosecondArray::from(rows.iter().map(|r| r.2).collect::<Vec<_>>())
                    .with_timezone("UTC"),
            ),
        ],
    )
    .unwrap();
    write_parquet(path, &batch, per_rg);
}

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

/// Oracle: rows, live row groups, files holding any of `keys` on `columns`
/// (tuples with a null match nothing).
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
                    files.insert(n.to_string());
                }
            }
        }
    }
    (rows, rgs.len() as u64, files.len() as u64)
}

fn all_keys_a() -> Vec<Vec<u8>> {
    (0..6).map(enc_i64).collect()
}

fn all_keys_ab() -> Vec<Vec<u8>> {
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
            vec![all_keys_a(), vec![enc_i64(1)], vec![enc_i64(4), enc_i64(2)]],
        ),
        (
            cols(&["a", "b"]),
            vec![all_keys_ab(), all_keys_ab()[..4].to_vec()],
        ),
    ] {
        for keys in keysets {
            let got = p.count(specs, &columns, &keys).unwrap();
            let want = oracle(store, s, names, &columns, &keys);
            let got = got.unwrap_or_else(|| panic!("{what}: no count on {columns:?}"));
            assert_eq!(
                (got.rows, got.row_groups, got.files),
                want,
                "{what}: count on {columns:?} vs decoded files"
            );
            // the plan's rows agree
            let plan = p
                .plan(
                    specs,
                    &Lookup::Keys {
                        columns: columns.clone(),
                        keys: keys.clone(),
                    },
                )
                .unwrap();
            assert_eq!(
                plan.rows.map(|q| q.value),
                Some(want.0),
                "{what}: plan rows"
            );
        }
    }
    assert_eq!(p.footers_opened(), 0, "{what}: footers opened");
}

/// Counts against decoded files through many segments, compaction, and a
/// file rewrite.
#[test]
fn probe_counts_oracle_through_compaction_and_rewrite() {
    let s = Scratch::new();
    write(&s.path("f1.parquet"), &rows(0, 41), 3);
    write(&s.path("f2.parquet"), &rows(2, 37), 4);
    let names = ["f1.parquet", "f2.parquet"];
    let store = memory_store();
    let b = builder(&store);
    let specs = vec![ex(&["a"], true), ex(&["a", "b"], true)];
    let opts = bellwether_build::BuildOptions {
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
    check_count(&store, &s, &names, &specs, "after rewrite");
    let c = b.compact(&CompactOptions::default()).unwrap();
    assert!(c.merges > 0, "{c:?}");
    check_count(&store, &s, &names, &specs, "after rewrite and compaction");
}

/// A composite lookup is counted only by a counting spec on exactly its
/// columns; `exact:a,counts` must not count `(a, b)`.
#[test]
fn probe_rows_only_from_a_spec_on_exactly_the_lookup_columns() {
    let s = Scratch::new();
    write(&s.path("f1.parquet"), &rows(0, 41), 3);
    let store = memory_store();
    let specs = vec![ex(&["a"], true), ex(&["a", "b"], false)];
    build_ok(
        &builder(&store),
        &s.root_str(),
        &specs,
        &parquet_opts(&schema()),
    );
    let p = planner(&store);
    let lookup = Lookup::Keys {
        columns: cols(&["a", "b"]),
        keys: all_keys_ab(),
    };
    let plan = p.plan(&specs, &lookup).unwrap();
    assert_eq!(plan.rows, None, "exact:a,counts counted an (a, b) lookup");
    assert!(p
        .count(&specs, &cols(&["a", "b"]), &all_keys_ab())
        .unwrap()
        .is_none());
}

/// Two committed segments of one counted spec over the same row groups (a
/// manifest another writer produced): the read path refuses the index as
/// corrupt rather than summing them (the decoded files hold half).
#[test]
fn probe_overlapping_counted_segments_in_a_manifest() {
    let s = Scratch::new();
    write(&s.path("f1.parquet"), &rows(0, 41), 3);
    let specs = vec![ex(&["a"], true)];
    let store1 = memory_store();
    build_ok(
        &builder(&store1),
        &s.root_str(),
        &specs,
        &bellwether_build::BuildOptions {
            max_segment_row_groups: 2,
            ..parquet_opts(&schema())
        },
    );
    let store2 = memory_store();
    build_ok(
        &builder(&store2),
        &s.root_str(),
        &specs,
        &parquet_opts(&schema()),
    );
    let m2 = manifest(&store2);
    assert_eq!(m2.segments.len(), 1);
    let seg = m2.segments[0].clone();
    let path = format!("idx/segments/{}.seg", seg.id.to_hex());
    let (bytes, _) = store2.get(&path).unwrap();
    store1.put_if_absent(&path, bytes).unwrap();
    let (mbytes, version): (Bytes, StoreVersion) = store1.get("idx/manifest.json").unwrap();
    let mut m1 = Manifest::from_slice(&mbytes).unwrap();
    m1.segments.push(seg);
    let next = m1.to_vec().unwrap();
    store1
        .put_if_version("idx/manifest.json", Bytes::from(next), &version)
        .unwrap();
    let want = oracle(&store1, &s, &["f1.parquet"], &cols(&["a"]), &all_keys_a());
    let p = planner(&store1);
    match p.count(&specs, &cols(&["a"]), &all_keys_a()) {
        Err(e) => assert!(e.to_string().starts_with("corrupt index: "), "{e}"),
        Ok(c) => panic!(
            "overlapping segments: counted {:?} rows, decoded files hold {}",
            c.map(|c| c.rows),
            want.0
        ),
    }
}

/// A storage version 1 manifest compacted by this binary stays at version 1
/// (compaction fills no statistics), and a no-op index leaves it there.
#[test]
fn probe_compaction_over_a_version_1_manifest() {
    let s = Scratch::new();
    write(&s.path("f1.parquet"), &rows(0, 41), 3);
    let store = memory_store();
    let spec = ex(&["a"], false);
    let b = builder(&store);
    let opts = bellwether_build::BuildOptions {
        max_segment_row_groups: 1,
        ..parquet_opts(&schema())
    };
    build_ok(&b, &s.root_str(), std::slice::from_ref(&spec), &opts);
    // make it a version 1 manifest
    let (bytes, version) = store.get("idx/manifest.json").unwrap();
    let mut v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    v["storage_version"] = 1.into();
    for f in v["rowgroup_space"]["files"].as_array_mut().unwrap() {
        let f = f.as_object_mut().unwrap();
        for k in ["rowgroup_rows", "rowgroup_bytes", "column_bytes"] {
            f.remove(k);
        }
    }
    store
        .put_if_version(
            "idx/manifest.json",
            Bytes::from(serde_json::to_vec_pretty(&v).unwrap()),
            &version,
        )
        .unwrap();
    let c = b.compact(&CompactOptions::default()).unwrap();
    assert!(c.merges > 0, "{c:?}");
    let m = manifest(&store);
    let has = m
        .rowgroup_space
        .files
        .iter()
        .all(|f| f.rowgroup_bytes.is_some());
    assert_eq!((m.storage_version, has), (1, false), "after compaction");
    // A no-op index afterwards
    b.build(&s.root_str(), std::slice::from_ref(&spec), &opts)
        .unwrap();
    let m = manifest(&store);
    let has = m
        .rowgroup_space
        .files
        .iter()
        .all(|f| f.rowgroup_bytes.is_some());
    assert_eq!((m.storage_version, has), (1, false), "after a no-op index");
    let p = planner(&store);
    let mut plan = p
        .plan(
            std::slice::from_ref(&spec),
            &Lookup::Keys {
                columns: cols(&["a"]),
                keys: vec![enc_i64(1)],
            },
        )
        .unwrap();
    assert!(p.footers_opened() > 0, "a version 1 plan reads footers");
    assert!(
        p.project(&mut plan, &cols(&["a"])).unwrap() > 0,
        "projected from footers"
    );
    assert!(
        m.storage_version < 2 || has,
        "a version 2 manifest without the version 2 arrays"
    );
}

/// Two different specs would share one identity only through a column name
/// holding `,` or `@`, and such a name is refused.
#[test]
fn probe_identity_is_injective() {
    let a = ex(&["a"], true);
    let b = ex(&["a,counts"], false);
    let t1 = KeySpec::Temporal {
        columns: cols(&["id"]),
        ts_column: "ts".into(),
        counts: true,
    };
    let t2 = KeySpec::Temporal {
        columns: cols(&["id"]),
        ts_column: "ts,counts".into(),
        counts: false,
    };
    let c1 = ex(&["a", "b"], false);
    let c2 = ex(&["a,b"], false);
    for (ok, bad) in [(a, b), (t1, t2), (c1, c2)] {
        assert_eq!(ok.identity(), bad.identity());
        ok.check_names().unwrap();
        let e = bad.check_names().unwrap_err();
        assert!(e.to_string().contains("contains ',' or '@'"), "{e}");
    }
}

/// Both colliding specs declared over one dataset: the build refuses.
#[test]
fn probe_identity_collision_in_an_index() {
    let s = Scratch::new();
    let sch = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("a,counts", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&sch),
        vec![
            Arc::new(Int64Array::from((0..40).collect::<Vec<i64>>())) as ArrayRef,
            Arc::new(Int64Array::from(
                (0..40).map(|i| 1000 + i).collect::<Vec<i64>>(),
            )),
        ],
    )
    .unwrap();
    write_parquet(&s.path("f.parquet"), &batch, 5);
    let store = memory_store();
    let specs = vec![ex(&["a"], true), ex(&["a,counts"], false)];
    let e = builder(&store)
        .build(&s.root_str(), &specs, &parquet_opts(&sch))
        .map(|r| r.sequence)
        .unwrap_err();
    assert!(
        e.to_string().contains("\"a,counts\" contains ',' or '@'"),
        "{e}"
    );
}

/// Every record of the fixtures' ledgers keeps its bytes and hash when
/// read and written back; so do hand-built record version 1 records.
#[test]
fn probe_old_records_keep_bytes_and_hashes() {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut n = 0;
    for fx in ["format-v1", "format-v0.1-pre", "format-v0.1"] {
        let dir = repo.join("fixtures").join(fx).join("index/ledger");
        for e in std::fs::read_dir(dir).unwrap() {
            let p = e.unwrap().path();
            let bytes = std::fs::read(&p).unwrap();
            let r: Record = serde_json::from_slice(&bytes).unwrap();
            let back = serde_json::to_vec(&r).unwrap();
            assert_eq!(
                String::from_utf8_lossy(&back),
                String::from_utf8_lossy(&bytes),
                "{}",
                p.display()
            );
            assert_eq!(
                Hash::of_record(&r).unwrap().to_hex(),
                p.file_stem().unwrap().to_str().unwrap()
            );
            n += 1;
        }
    }
    assert!(n > 20, "{n}");
    let h = "aa".repeat(32);
    let hand = [
        format!(
            r#"{{"record":"proposal","record_version":1,"evidence":["{h}"],"proposed_by":"agent","created_at":"2026-10-20T00:00:00.000Z","body":{{"kind":"key","spec":{{"kind":"bloom","columns":["id"],"fpr":0.05}}}}}}"#
        ),
        format!(
            r#"{{"record":"proposal","record_version":1,"evidence":["{h}"],"proposed_by":"agent","created_at":"2026-10-20T00:00:00.000Z","body":{{"kind":"key","spec":{{"kind":"temporal","columns":["id"],"ts_column":"ts"}}}}}}"#
        ),
        format!(
            r#"{{"record":"replay","record_version":1,"proposal":"{h}","manifest_version":"3","workload":"{h}","current":[{{"run_id":"r1","keys":10,"files":2,"row_groups":5,"bytes":900,"exact":true}}],"proposed":[{{"run_id":"r1","keys":10,"files":1,"row_groups":1,"bytes":100,"exact":true}}],"created_at":"2026-10-20T00:00:00.000Z"}}"#
        ),
        format!(
            r#"{{"record":"certificate","record_version":1,"proposal":"{h}","coverage":{{"kind":"workload","workload":"{h}","keys":10}},"pass":false,"body":{{"missing_right":0,"missing_left":1,"mismatched":2,"ignored_columns":[],"totals_delta":[]}},"provenance":{{"inputs":["{h}"],"oldest_input_age":0,"manifest_version":"3","created_at":"2026-10-20T00:00:00.000Z"}}}}"#
        ),
        format!(
            r#"{{"record":"receipt","record_version":1,"evidence":["{h}"],"proposal":"{h}","replay":"{h}","certificate":"{h}","manifest_before":"3","manifest_after":"4","created_at":"2026-10-20T00:00:00.000Z"}}"#
        ),
    ];
    for j in &hand {
        let r: Record = match serde_json::from_str(j) {
            Ok(r) => r,
            Err(e) => panic!("hand-built v1 record does not parse: {e}\n{j}"),
        };
        let back = serde_json::to_string(&r).unwrap();
        assert_eq!(&back, j, "hand-built v1 record changed bytes");
        assert_eq!(Hash::of_record(&r).unwrap(), Hash::of(j.as_bytes()));
    }
}

/// Float keys: both zeros are one SQL value; NaN; counts from both stored
/// zeros, never twice.
#[test]
fn probe_float_zero_counts() {
    use arrow::array::Float64Array;
    let s = Scratch::new();
    let sch = Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, true)]));
    let vals: Vec<Option<f64>> = vec![
        Some(-0.0),
        Some(0.0),
        Some(0.0),
        Some(1.5),
        None,
        Some(-0.0),
        Some(0.0),
        Some(f64::NAN),
        Some(1.5),
        Some(0.0),
    ];
    let batch = RecordBatch::try_new(
        Arc::clone(&sch),
        vec![Arc::new(Float64Array::from(vals)) as ArrayRef],
    )
    .unwrap();
    write_parquet(&s.path("f.parquet"), &batch, 3);
    let store = memory_store();
    let specs = vec![ex(&["f"], true)];
    build_ok(&builder(&store), &s.root_str(), &specs, &parquet_opts(&sch));
    let p = planner(&store);
    for (keys, want) in [
        (vec![enc_f64(0.0)], 6),
        (vec![enc_f64(-0.0)], 6),
        (vec![enc_f64(0.0), enc_f64(-0.0)], 6),
        (vec![enc_f64(1.5), enc_f64(0.0)], 8),
        (vec![enc_f64(f64::NAN)], 1),
    ] {
        let c = p.count(&specs, &cols(&["f"]), &keys).unwrap().unwrap();
        assert_eq!(c.rows, want, "{keys:?}");
    }
}
