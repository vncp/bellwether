//! Per-key counts and the storage version 2 manifest arrays against an
//! independent oracle: every file decoded with the `parquet` crate.
//!
//! - Counts: for every counting spec (`exact:a,counts`, `exact:a,b,counts`,
//!   `temporal:a@ts,counts`), the counts the segments store per (key, live
//!   row group) equal the decoded rows grouped the same way: a row whose
//!   leading key column is null is not counted; a null trailing column is
//!   the `0x00` marker and its row counts under that tuple (ADR 014). Again
//!   after a file is rewritten, which retires its row-group ids.
//! - The manifest's `rowgroup_rows`, `rowgroup_bytes`, and `column_bytes`
//!   equal the footers, and stay equal after the rewrite (no stale entry for
//!   a retired id).
//! - A plan on a storage version 2 manifest opens no footer, carries the
//!   exact rows from the counts, and a projection's bytes are the footers'
//!   column chunk sizes; the same plan on a manifest without the arrays
//!   (version 1) opens one footer per planned file.

#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::type_complexity,
    clippy::cast_possible_truncation,
    clippy::assert_is_empty,
    clippy::many_single_char_names,
    clippy::similar_names
)]

mod common;

use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_build::Lookup;
use bellwether_core::segment::segment_path;
use bellwether_core::SegmentReader;
use bellwether_store::{Manifest, ObjectStoreBackend};
use bellwether_types::{Exactness, KeySpec, Quantity, Store};
use bytes::Bytes;
use common::*;
use parquet::file::reader::{FileReader, SerializedFileReader};

type Rows<'a> = [(Option<i64>, Option<&'a str>, Option<i64>)];

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

fn write(path: &Path, rows: &Rows<'_>, per_rg: usize) {
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

/// Rows with repeated keys across row groups, null leading and trailing
/// columns, and null times.
fn rows(seed: i64) -> Vec<(Option<i64>, Option<&'static str>, Option<i64>)> {
    let tags = [Some("x"), None, Some("y"), Some("x"), None];
    (0..23)
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

fn specs() -> Vec<KeySpec> {
    vec![
        KeySpec::Exact {
            columns: cols(&["a"]),
            counts: true,
            bounds: None,
        },
        KeySpec::Exact {
            columns: cols(&["a", "b"]),
            counts: true,
            bounds: None,
        },
        KeySpec::Temporal {
            columns: cols(&["a"]),
            ts_column: "ts".into(),
            counts: true,
        },
    ]
}

fn manifest(store: &ObjectStoreBackend) -> Manifest {
    Manifest::from_slice(&manifest_bytes(store)).unwrap()
}

/// The stored key of a decoded row under `columns`: `None` when the leading
/// column is null (not indexed), a null trailing column as `0x00`.
fn stored_key(row: &Row, columns: &[String]) -> Option<Vec<u8>> {
    row.get(&columns[0])?;
    let mut out = Vec::new();
    for c in columns {
        match row.get(c) {
            Some(b) => out.extend_from_slice(b),
            None => out.push(0x00),
        }
    }
    Some(out)
}

/// The oracle: (key -> (live row-group id -> rows)) from the decoded files.
fn oracle(
    m: &Manifest,
    s: &Scratch,
    names: &[&str],
    columns: &[String],
) -> BTreeMap<Vec<u8>, BTreeMap<u32, u64>> {
    let mut out: BTreeMap<Vec<u8>, BTreeMap<u32, u64>> = BTreeMap::new();
    for name in names {
        let path = s.path(name);
        let p = path.to_str().unwrap();
        let file = m
            .rowgroup_space
            .files
            .iter()
            .find(|f| f.fingerprint.path == p)
            .unwrap();
        for row in parquet_rows(&path, &schema()) {
            let Some(key) = stored_key(&row, columns) else {
                continue;
            };
            let id = file.rowgroup_ids[row.rg as usize].0;
            *out.entry(key).or_default().entry(id).or_default() += 1;
        }
    }
    out
}

/// Everything the segments of `spec` store, over live row groups: every
/// stored key (trailing-null tuples included) with its counts.
fn stored(
    store: &ObjectStoreBackend,
    m: &Manifest,
    spec: &KeySpec,
) -> BTreeMap<Vec<u8>, BTreeMap<u32, u64>> {
    let live: std::collections::BTreeSet<u32> = m
        .rowgroup_space
        .files
        .iter()
        .flat_map(|f| f.rowgroup_ids.iter().map(|id| id.0))
        .collect();
    let mut out: BTreeMap<Vec<u8>, BTreeMap<u32, u64>> = BTreeMap::new();
    let identity = spec.identity();
    let segs: Vec<_> = m
        .segments
        .iter()
        .filter(|seg| seg.key_spec == identity)
        .collect();
    assert!(!segs.is_empty(), "{identity}: no segments");
    for seg in segs {
        let (bytes, _) = store
            .get(&format!("idx/{}", segment_path(&seg.id)))
            .unwrap();
        let r = SegmentReader::open(bytes.to_vec()).unwrap();
        assert!(r.has_counts(), "{identity}");
        for (k, value) in r.keyed_entries().unwrap() {
            for (rg, n) in value.counted().unwrap() {
                if live.contains(&rg.0) {
                    let slot = out.entry(k.clone()).or_default().entry(rg.0).or_default();
                    assert_eq!(*slot, 0, "{identity}: row group {} counted twice", rg.0);
                    *slot = n;
                }
            }
        }
    }
    out.retain(|_, v| !v.is_empty());
    out
}

fn check_counts(store: &ObjectStoreBackend, s: &Scratch, names: &[&str]) {
    let m = manifest(store);
    for spec in specs() {
        let columns = spec.columns().to_vec();
        let want = oracle(&m, s, names, &columns);
        // Trailing-null tuples (an Int64 element, then the marker) are among
        // the keys of the composite spec.
        if columns.len() == 2 {
            let marked = enc_i64(0).len() + 1;
            assert!(want.keys().any(|k| k.len() == marked), "{want:?}");
        }
        assert_eq!(stored(store, &m, &spec), want, "{}", spec.identity());
    }
}

/// `(rows, bytes, column bytes)` per row group from the footer.
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

fn check_arrays(store: &ObjectStoreBackend) {
    let m = manifest(store);
    assert_eq!(m.storage_version, 2);
    let columns: Vec<String> = m.schema.columns.iter().map(|c| c.name.clone()).collect();
    for f in &m.rowgroup_space.files {
        let (rows, bytes, cb) = footer(&f.fingerprint.path, &columns);
        assert_eq!(
            f.rowgroup_rows.as_ref(),
            Some(&rows),
            "{}",
            f.fingerprint.path
        );
        assert_eq!(
            f.rowgroup_bytes.as_ref(),
            Some(&bytes),
            "{}",
            f.fingerprint.path
        );
        assert_eq!(f.column_bytes.as_ref(), Some(&cb), "{}", f.fingerprint.path);
    }
}

#[test]
fn counts_and_manifest_arrays_equal_the_decoded_files() {
    let s = Scratch::new();
    write(&s.path("f1.parquet"), &rows(0), 4);
    write(&s.path("f2.parquet"), &rows(2), 5);
    let store = memory_store();
    let b = builder(&store);
    build_ok(&b, &s.root_str(), &specs(), &parquet_opts(&schema()));
    check_counts(&store, &s, &["f1.parquet", "f2.parquet"]);
    check_arrays(&store);

    // Rewrite f2 (new contents, other row-group sizes): its ids retire, the
    // counts and arrays follow the new file.
    write(&s.path("f2.parquet"), &rows(5), 3);
    let r = build_ok(&b, &s.root_str(), &specs(), &parquet_opts(&schema()));
    assert_eq!(r.files_changed, 1, "{r:?}");
    check_counts(&store, &s, &["f1.parquet", "f2.parquet"]);
    check_arrays(&store);
    let m = manifest(&store);
    assert!(!m.rowgroup_space.tombstones.is_empty());
}

#[test]
fn a_version_2_plan_opens_no_footer_and_carries_exact_rows() {
    let s = Scratch::new();
    write(&s.path("f1.parquet"), &rows(0), 4);
    write(&s.path("f2.parquet"), &rows(2), 5);
    let store = memory_store();
    build_ok(
        &builder(&store),
        &s.root_str(),
        &specs(),
        &parquet_opts(&schema()),
    );
    let lookup = Lookup::Keys {
        columns: cols(&["a"]),
        keys: vec![enc_i64(1), enc_i64(4), enc_i64(99)],
    };
    let p = planner(&store);
    let mut plan = p.plan(&specs(), &lookup).unwrap();
    assert_eq!(p.footers_opened(), 0);
    // Rows: the decoded rows whose a is 1 or 4.
    let decoded: Vec<Row> = ["f1.parquet", "f2.parquet"]
        .iter()
        .flat_map(|n| parquet_rows(&s.path(n), &schema()))
        .collect();
    let want = decoded
        .iter()
        .filter(|r| {
            r.get("a")
                .is_some_and(|a| *a == enc_i64(1) || *a == enc_i64(4))
        })
        .count() as u64;
    assert!(want > 0);
    assert_eq!(
        plan.rows,
        Some(Quantity {
            value: want,
            exactness: Exactness::Exact
        })
    );
    // A projection: each entry's bytes of `a` and `ts` over its row groups,
    // from the footers.
    let projection = cols(&["a", "ts"]);
    p.project(&mut plan, &projection).unwrap();
    assert_eq!(p.footers_opened(), 0);
    let m = manifest(&store);
    let columns: Vec<String> = m.schema.columns.iter().map(|c| c.name.clone()).collect();
    let mut total = 0;
    for e in &plan.entries {
        let (_, _, cb) = footer(&e.file.path, &columns);
        let per = |c: &str| -> u64 {
            let j = columns.iter().position(|x| x == c).unwrap();
            e.row_groups
                .iter()
                .map(|rg| cb[rg as usize * columns.len() + j])
                .sum()
        };
        let got: BTreeMap<String, u64> = e
            .columns
            .as_ref()
            .unwrap()
            .iter()
            .map(|c| (c.column.clone(), c.bytes))
            .collect();
        for c in &projection {
            assert_eq!(got[c], per(c), "{} {c}", e.file.path);
            total += per(c);
        }
    }
    assert_eq!(plan.projected_bytes, Some(total));
    assert!(total < plan.entries.iter().map(|e| e.bytes).sum::<u64>());

    // The same manifest without the version 2 arrays, as a version 1 binary
    // wrote it (no counting spec, which version 1 cannot declare): planning
    // opens a footer per planned file to price it.
    let plain = KeySpec::Exact {
        columns: cols(&["a"]),
        counts: false,
        bounds: None,
    };
    let store1 = memory_store();
    build_ok(
        &builder(&store1),
        &s.root_str(),
        std::slice::from_ref(&plain),
        &parquet_opts(&schema()),
    );
    let (bytes, version) = store1.get("idx/manifest.json").unwrap();
    let mut v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    v["storage_version"] = 1.into();
    for f in v["rowgroup_space"]["files"].as_array_mut().unwrap() {
        let f = f.as_object_mut().unwrap();
        for k in ["rowgroup_rows", "rowgroup_bytes", "column_bytes"] {
            assert!(f.remove(k).is_some(), "{k}");
        }
    }
    store1
        .put_if_version(
            "idx/manifest.json",
            Bytes::from(serde_json::to_vec_pretty(&v).unwrap()),
            &version,
        )
        .unwrap();
    let p1 = planner(&store1);
    let plan1 = p1.plan(std::slice::from_ref(&plain), &lookup).unwrap();
    assert_eq!(p1.footers_opened(), plan1.entries.len() as u64);
    assert!(p1.footers_opened() > 0);
    assert_eq!(plan1.rows, None, "no counting spec, no rows");
    let entries = |pl: &bellwether_types::ScanPlan| {
        pl.entries
            .iter()
            .map(|e| (e.file.path.clone(), e.row_groups.clone(), e.bytes))
            .collect::<Vec<_>>()
    };
    // Same row groups and bytes either way.
    assert_eq!(entries(&plan1), entries(&plan));
}
