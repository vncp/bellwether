//! The `sketch` sidecar through the builder: what it stores per file equals
//! a sketch of the decoded files (rows and nulls exact, registers
//! identical), it is declared and completed like a sidecar, it follows new
//! and changed files, and **no planner path reads it**: plans are byte-equal
//! with and without a sketch, and stay so with every sketch segment deleted
//! from the store.

#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

mod common;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_build::{declared_sketches, sketch_completeness, BuildOptions, Lookup};
use bellwether_core::sidecar::SidecarKind;
use bellwether_core::sketch::{item_types, FileSketcher, ItemSketch, SketchReader};
use bellwether_store::{segment_path, Manifest, StoreExt};
use bellwether_types::{KeySpec, SketchSpec, Store};
use common::*;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("batch", DataType::Utf8, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]))
}

fn sketch(items: &[&[&str]], p: u8, k: Option<u16>) -> SketchSpec {
    SketchSpec {
        items: items
            .iter()
            .map(|i| i.iter().map(|c| (*c).to_string()).collect())
            .collect(),
        precision: p,
        minhash: k,
    }
}

/// `rows` rows, `(id, batch, ts)`; every 7th id and every 11th batch null.
fn write_file(path: &Path, base: i64, rows: i64, rows_per_rg: usize) {
    let ids: Vec<Option<i64>> = (0..rows)
        .map(|i| (i % 7 != 3).then_some(base + i % 50))
        .collect();
    let batches: Vec<Option<String>> = (0..rows)
        .map(|i| (i % 11 != 5).then(|| format!("b{}", (base + i) / 40)))
        .collect();
    let ts: Vec<Option<i64>> = (0..rows).map(|i| Some(base * 1_000 + i)).collect();
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(batches)),
            Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
        ],
    )
    .unwrap();
    write_parquet(path, &batch, rows_per_rg);
}

fn opts(sketches: Vec<SketchSpec>) -> BuildOptions {
    BuildOptions {
        sketches,
        max_segment_row_groups: 3,
        ..parquet_opts(&schema())
    }
}

/// The sketch of each item over one whole file, decoded with the `parquet`
/// crate (never the source under test) and fed to core's sketcher.
fn oracle(path: &Path, spec: &SketchSpec) -> Vec<ItemSketch> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path).unwrap())
        .unwrap()
        .build()
        .unwrap();
    let mut s = FileSketcher::new(spec, item_types(spec, &schema()).unwrap()).unwrap();
    for b in reader {
        s.add_batch(&b.unwrap()).unwrap();
    }
    s.take().unwrap()
}

/// Every file's merged sketch, from the committed segments, by path.
fn stored(
    store: &Arc<bellwether_store::ObjectStoreBackend>,
    spec: &SketchSpec,
) -> BTreeMap<String, Vec<ItemSketch>> {
    let m = Manifest::from_slice(&manifest_bytes(store)).unwrap();
    let mut file_of = BTreeMap::new();
    for f in &m.rowgroup_space.files {
        for id in &f.rowgroup_ids {
            file_of.insert(id.0, f.fingerprint.path.clone());
        }
    }
    let mut out: BTreeMap<String, Vec<ItemSketch>> = BTreeMap::new();
    for seg in m.sidecar_segments("sketch", &spec.name()) {
        let bytes = store.get(&segment_path("idx", &seg.id)).unwrap().0;
        let r = SketchReader::open_for_dataset(&bytes, &schema()).unwrap();
        assert_eq!(r.spec(), *spec);
        for i in 0..r.len() {
            let e = r.entry(i).unwrap();
            let Some(path) = file_of.get(&e.rowgroups.start).cloned() else {
                continue; // retired
            };
            for id in e.rowgroups.clone() {
                assert_eq!(file_of.get(&id), Some(&path), "an entry spans files");
            }
            match out.get_mut(&path) {
                Some(acc) => {
                    for (a, b) in acc.iter_mut().zip(&e.items) {
                        a.merge(b).unwrap();
                    }
                }
                None => {
                    out.insert(path, e.items);
                }
            }
        }
    }
    out
}

#[test]
fn stored_sketches_equal_the_decoded_files_and_complete_like_a_sidecar() {
    let s = Scratch::new();
    for (i, rows) in [(0i64, 40i64), (1, 95), (2, 7), (3, 120)] {
        write_file(&s.root().join(format!("f{i}.parquet")), i * 100, rows, 16);
    }
    let store = memory_store();
    let b = builder(&store);
    let spec = sketch(&[&["id"], &["batch"], &["id", "batch"]], 12, Some(16));
    build_ok(
        &b,
        &s.root_str(),
        &[exact(&["id"])],
        &opts(vec![spec.clone()]),
    );
    let m = Manifest::from_slice(&manifest_bytes(&store)).unwrap();
    assert_eq!(declared_sketches(&m), vec![spec.clone()]);
    assert!(sketch_completeness(&m, &spec).is_some());
    let decl = m
        .schema
        .sidecars
        .iter()
        .find(|d| d.kind == "sketch")
        .unwrap();
    assert_eq!(decl.name(), "sketch:id,batch,id+batch");
    assert_eq!(decl.params.get("p").map(String::as_str), Some("12"));
    assert_eq!(decl.params.get("minhash").map(String::as_str), Some("16"));
    let got = stored(&store, &spec);
    assert_eq!(got.len(), 4);
    for (path, items) in &got {
        assert_eq!(items, &oracle(Path::new(path), &spec), "{path}");
    }
}

#[test]
fn new_and_changed_files_are_sketched_and_retired_entries_are_not_used() {
    let s = Scratch::new();
    write_file(&s.root().join("a.parquet"), 0, 60, 16);
    write_file(&s.root().join("b.parquet"), 100, 60, 16);
    let store = memory_store();
    let b = builder(&store);
    let spec = sketch(&[&["id"], &["batch"]], 10, None);
    let o = opts(vec![spec.clone()]);
    build_ok(&b, &s.root_str(), &[exact(&["id"])], &o);
    // A new file and a rewritten one.
    write_file(&s.root().join("c.parquet"), 300, 33, 16);
    write_file(&s.root().join("a.parquet"), 500, 61, 16);
    let r = build_ok(&b, &s.root_str(), &[], &o);
    assert_eq!((r.files_new, r.files_changed), (1, 1));
    let m = Manifest::from_slice(&manifest_bytes(&store)).unwrap();
    assert!(sketch_completeness(&m, &spec).is_some());
    let got = stored(&store, &spec);
    assert_eq!(got.len(), 3);
    for (path, items) in &got {
        assert_eq!(items, &oracle(Path::new(path), &spec), "{path}");
    }
    // Same name, another precision: rebuilt over every live row group.
    let finer = sketch(&[&["id"], &["batch"]], 12, None);
    let r = build_ok(&b, &s.root_str(), &[], &opts(vec![finer.clone()]));
    assert!(
        r.specs_rebuilt.contains(&finer.name()),
        "{:?}",
        r.specs_rebuilt
    );
    let m = Manifest::from_slice(&manifest_bytes(&store)).unwrap();
    assert_eq!(declared_sketches(&m), vec![finer.clone()]);
    for (path, items) in &stored(&store, &finer) {
        assert_eq!(items, &oracle(Path::new(path), &finer), "{path}");
    }
}

#[test]
fn a_sketch_on_a_missing_column_is_refused_and_nothing_is_committed() {
    let s = Scratch::new();
    write_file(&s.root().join("a.parquet"), 0, 10, 16);
    let store = memory_store();
    let b = builder(&store);
    let e = b
        .build(
            &s.root_str(),
            &[exact(&["id"])],
            &opts(vec![sketch(&[&["nope"]], 14, None)]),
        )
        .unwrap_err();
    assert!(e.to_string().contains("nope"), "{e}");
    assert!(store.get_opt("idx/manifest.json").unwrap().is_none());
}

/// Every lookup the planner serves, over one corpus.
fn lookups(
    p: &bellwether_build::Planner<bellwether_store::ObjectStoreBackend>,
) -> Vec<(Vec<KeySpec>, Lookup)> {
    let id = |v: i64| enc_i64(v);
    let keys: Vec<Vec<u8>> = [0, 3, 49, 120, 333, 999].into_iter().map(id).collect();
    let specs = p.declared_specs().to_vec();
    let mut out = vec![
        (
            specs.clone(),
            Lookup::Keys {
                columns: cols(&["id"]),
                keys: keys.clone(),
            },
        ),
        (
            specs.clone(),
            Lookup::Keys {
                columns: cols(&["batch"]),
                keys: vec![enc_str("b0"), enc_str("b7")],
            },
        ),
    ];
    out.push((
        specs.clone(),
        Lookup::Latest {
            columns: cols(&["id"]),
            keys: keys.clone(),
        },
    ));
    out.push((
        specs.clone(),
        Lookup::AsOf {
            columns: cols(&["id"]),
            keys: keys.clone(),
            ts: 250_000,
        },
    ));
    out.push((
        specs.clone(),
        Lookup::Between {
            column: "ts".into(),
            lo: enc(
                &bellwether_core::encoding::Value::Timestamp {
                    value: 100_000,
                    unit: TimeUnit::Microsecond,
                },
                &schema().field(2).data_type().clone(),
            ),
            hi: enc(
                &bellwether_core::encoding::Value::Timestamp {
                    value: 200_000,
                    unit: TimeUnit::Microsecond,
                },
                &schema().field(2).data_type().clone(),
            ),
        },
    ));
    out.push((
        specs,
        Lookup::Window {
            columns: cols(&["id"]),
            keys,
            ts_column: "ts".into(),
            lo: 0,
            hi: 150_000,
        },
    ));
    out
}

/// The planner never reads a sketch: an index with a sketch plans every
/// lookup (keys, bloom, range, latest, a bounds window) byte-equal to the
/// same index without one, and still does after every sketch segment is
/// deleted from the store; the sidecars the planner sees never include it.
#[test]
fn no_planner_path_reads_a_sketch() {
    let s = Scratch::new();
    for i in 0..5i64 {
        write_file(&s.root().join(format!("f{i}.parquet")), i * 100, 80, 16);
    }
    // `exact:id@ts`: per-key bounds for the window lookup.
    let specs = [
        KeySpec::Exact {
            columns: cols(&["id"]),
            counts: false,
            bounds: Some("ts".into()),
        },
        bloom(&["batch"]),
        range("ts"),
        temporal(&["id"], "ts"),
    ];
    let plain = memory_store();
    build_ok(&builder(&plain), &s.root_str(), &specs, &opts(Vec::new()));
    let sketched = memory_store();
    let spec = sketch(&[&["id"], &["batch"], &["id", "ts"]], 12, Some(8));
    build_ok(
        &builder(&sketched),
        &s.root_str(),
        &specs,
        &opts(vec![spec.clone()]),
    );
    let (pp, ps) = (planner(&plain), planner(&sketched));
    let timed = ps.timed_sources(&specs);
    assert_ne!(timed.len(), 0);
    assert!(timed.iter().all(|(d, _)| d.kind == SidecarKind::Bounds));
    let lk = lookups(&pp);
    for (sp, lookup) in &lk {
        assert!(ps
            .sidecars_for(sp, lookup)
            .iter()
            .all(|(d, _)| !d.name().starts_with("sketch")));
        let a = pp.plan(sp, lookup).unwrap();
        let b = ps.plan(sp, lookup).unwrap();
        assert_eq!(a.entries, b.entries, "{lookup:?}");
        assert_eq!(a.residual, b.residual, "{lookup:?}");
        assert_eq!(
            (a.bytes, a.row_groups_selected),
            (b.bytes, b.row_groups_selected)
        );
    }
    // Delete every sketch segment: planning is unchanged.
    let m = Manifest::from_slice(&manifest_bytes(&sketched)).unwrap();
    let mut deleted = 0;
    for seg in m.sidecar_segments("sketch", &spec.name()) {
        sketched.delete(&segment_path("idx", &seg.id)).unwrap();
        deleted += 1;
    }
    assert!(deleted > 0);
    let ps = planner(&sketched);
    for (sp, lookup) in &lk {
        let a = pp.plan(sp, lookup).unwrap();
        let b = ps.plan(sp, lookup).unwrap();
        assert_eq!(a.entries, b.entries, "{lookup:?}");
    }
}

/// A tripwire beside the test above: the planner modules (the build
/// crate's plan builder, the replay crate's plan seam, its planners, and
/// its baselines, and the facade's lookup verbs in `index.rs` and
/// `lookback.rs`) never name the sketch kind, so no future edit routes an
/// approximate number into a plan without failing here first. The facade's
/// `index.rs` may name it only where `index` declares and reports sketches.
#[test]
fn planner_sources_never_name_the_sketch_kind() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    for file in [
        "bellwether-build/src/plan.rs",
        "bellwether-build/src/coverage.rs",
        "bellwether-replay/src/plan.rs",
        "bellwether-replay/src/build_plan.rs",
        "bellwether-replay/src/baseline.rs",
        "bellwether-replay/src/replay.rs",
        "bellwether-verify/src/reconcile.rs",
        "bellwether/src/index.rs",
        "bellwether/src/lookback.rs",
    ] {
        let text = std::fs::read_to_string(root.join(file)).unwrap();
        let lower = text.to_lowercase();
        let hits: Vec<&str> = lower.lines().filter(|l| l.contains("sketch")).collect();
        let allowed = |l: &&str| {
            // coverage.rs only reports a sketch's completeness for readers.
            (file.ends_with("coverage.rs") && !l.contains("plan"))
                // index.rs: the `index` verb declares and reports sketches.
                || (file.ends_with("bellwether/src/index.rs")
                    && [
                        "options.sketches",
                        "declared_sketches",
                        "sketch_completeness",
                        "sketch: none",
                        "sketches_rebuilt",
                    ]
                    .iter()
                    .any(|ok| l.contains(ok)))
        };
        assert!(
            hits.iter().all(allowed),
            "{file} names the sketch kind: {hits:?}"
        );
    }
}
