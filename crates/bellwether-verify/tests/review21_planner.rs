//! Review probe for PR #21: the `IndexPlanner` path over the build crate's
//! planner, for every key-spec kind, with `latest_by` and totals, at one
//! pass and at several. The planned side must read exactly the planned row
//! groups (once per pass) and give the scanned side's result.

#![allow(
    clippy::unwrap_used,
    clippy::pedantic,
    clippy::cloned_ref_to_slice_refs,
    missing_docs,
    dead_code
)]

mod common;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_build::{BuildOptions, Builder, SourceKind};
use bellwether_source::{ChunkedTextSource, ParquetSource, TextFormat};
use bellwether_store::ObjectStoreBackend;
use bellwether_types::{EvidenceBody, FileFingerprint, Hash, KeySpec, RowGroupMeta, Source};
use bellwether_verify::{
    CertificateContext, CoverageRequest, MemoryReport, Options, ReconcileSpec, Reconciler,
    Reconciliation, Side, SidePath, WorkloadKeys,
};
use common::{dirs, r, write, R};

/// A source that records every row group it is asked to read.
struct Recorder<S> {
    inner: S,
    reads: Mutex<Vec<(String, u32)>>,
}

impl<S> Recorder<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            reads: Mutex::new(Vec::new()),
        }
    }
    fn take(&self) -> Vec<(String, u32)> {
        std::mem::take(&mut *self.reads.lock().unwrap())
    }
}

impl<S: Source> Source for Recorder<S> {
    fn discover(&self, root: &str) -> bellwether_types::Result<Vec<FileFingerprint>> {
        self.inner.discover(root)
    }
    fn row_groups(&self, file: &FileFingerprint) -> bellwether_types::Result<Vec<RowGroupMeta>> {
        self.inner.row_groups(file)
    }
    fn read_columns(
        &self,
        rg: &RowGroupMeta,
        columns: &[String],
    ) -> bellwether_types::Result<RecordBatch> {
        self.reads
            .lock()
            .unwrap()
            .push((rg.file.path.clone(), rg.index_in_file));
        self.inner.read_columns(rg, columns)
    }
    fn schema(&self, root: &str) -> bellwether_types::Result<SchemaRef> {
        self.inner.schema(root)
    }
}

fn int_key(v: i64) -> Vec<u8> {
    let mut b = vec![0x02];
    let mut x = v.to_be_bytes();
    x[0] ^= 0x80;
    b.extend_from_slice(&x);
    b
}

fn workload(ids: &[i64]) -> CoverageRequest {
    CoverageRequest::Workload(WorkloadKeys {
        workload: Hash::of(b"w"),
        age: Duration::from_secs(1),
        keys: ids.iter().map(|&i| int_key(i)).collect(),
    })
}

/// Versions of one key spread over files: version v of id goes to file
/// (id + v) mod 4, so a key's newest version is in another file than its
/// older ones.
fn spread(dir: &std::path::Path, edit: bool) {
    let mut files: Vec<Vec<R>> = vec![Vec::new(); 4];
    for id in 0..200i64 {
        for v in 0..3i64 {
            let mut x = r(
                id,
                1_000 * (v + 1),
                i128::from(id * 10 + v),
                ["a", "b"][usize::try_from(id % 2).unwrap()],
            );
            if edit {
                if id == 7 && v == 2 {
                    x.note = Some("newest changed".into());
                }
                if id == 8 && v == 0 {
                    x.note = Some("oldest changed".into());
                }
                if id == 9 && v == 2 {
                    continue; // newest deleted on the right
                }
                if id == 10 {
                    continue; // key missing on the right
                }
            }
            files[usize::try_from((id + v) % 4).unwrap()].push(x);
        }
        if edit && id == 11 {
            let mut n = r(11, 0, 5, "a");
            n.ts = None; // a null version never wins
            files[0].push(n);
        }
    }
    for (i, mut rows) in files.into_iter().enumerate() {
        rows.sort_by_key(|x| x.id);
        write(dir, &format!("part-{i}.parquet"), &rows, 25);
    }
}

fn canonical(dir: &std::path::Path) -> String {
    std::fs::canonicalize(dir)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string()
}

fn strip(mut o: Reconciliation) -> Reconciliation {
    o.memory = MemoryReport::default();
    for s in &mut o.sides {
        s.path = SidePath::Scan;
        s.row_groups = 0;
        s.rows = 0;
        s.manifest = None;
        // An indexed side cites its manifest's catalog digest, a scanned
        // side the folder's (docs/format.md, "Catalog digest"): different
        // by design (contract-v3 #29, #30), so not compared here.
        s.catalog = bellwether_types::Hash([0; 32]);
    }
    o
}

#[test]
fn guard_every_spec_kind_reads_only_the_plan_and_equals_the_scan() {
    let (data, other) = dirs();
    spread(data.path(), false);
    spread(other.path(), true);
    let specs = [
        KeySpec::Exact {
            columns: vec!["id".into()],
            counts: false,
            bounds: None,
        },
        KeySpec::Temporal {
            columns: vec!["id".into()],
            ts_column: "ts".into(),
            counts: false,
            tombstone: None,
        },
        KeySpec::Exact {
            columns: vec!["id".into(), "status".into()],
            counts: false,
            bounds: None,
        },
        KeySpec::Bloom {
            columns: vec!["id".into()],
            fpr: 0.01,
            bounds: None,
        },
        KeySpec::Range {
            column: "id".into(),
        },
    ];
    let ids = [7, 8, 9, 10, 11, 12, 13, 150, 999];
    let spec = ReconcileSpec::new(["id"])
        .latest_by("ts")
        .totals(["amount"])
        .group_by(["status"])
        .coverage(workload(&ids));
    let right = ParquetSource::new();
    let right_root = common::root(other.path());
    for ks in specs {
        let store = Arc::new(ObjectStoreBackend::memory().unwrap());
        Builder::new(Arc::clone(&store), "idx")
            .build(
                &canonical(data.path()),
                std::slice::from_ref(&ks),
                &BuildOptions::new(SourceKind::Parquet),
            )
            .unwrap();
        let planner = bellwether_verify::IndexPlanner::open(&store, "idx").unwrap();
        let left_root = planner.source_root().to_string();
        let left = Recorder::new(ParquetSource::new().with_schema(Arc::clone(planner.schema())));
        let scanned = Reconciler::new(spec.clone(), Options::default())
            .unwrap()
            .run(
                &Side::new(&ParquetSource::new(), &left_root),
                &Side::new(&right, &right_root),
            )
            .unwrap();
        assert!(!scanned.pass);
        for budget in [1u64 << 30, 6 * 1024] {
            let certified = Reconciler::new(
                spec.clone(),
                Options {
                    memory_budget: budget,
                    ..Options::default()
                },
            )
            .unwrap()
            .certify(
                &Side::new(&left, &left_root).indexed(&planner),
                &Side::new(&right, &right_root),
                &CertificateContext::default(),
            )
            .unwrap();
            let out = &certified.outcome;
            let SidePath::Planned {
                row_groups_selected,
                row_groups_total,
                ..
            } = out.sides[0].path
            else {
                panic!("{ks:?}: {:?}", out.sides[0].path);
            };
            assert_eq!(
                strip(out.clone()),
                strip(scanned.clone()),
                "{ks:?} at {budget}: planned and scanned results differ"
            );
            // What was read is exactly the plan, once per pass.
            let EvidenceBody::Plan { plan } = &certified.evidence[0].body else {
                panic!()
            };
            let mut planned: BTreeMap<(String, u32), u64> = BTreeMap::new();
            for e in &plan.entries {
                for i in &e.row_groups {
                    *planned.entry((e.file.path.clone(), i)).or_default() +=
                        u64::from(out.memory.passes) * u64::from(out.memory.restarts + 1);
                }
            }
            let mut read: BTreeMap<(String, u32), u64> = BTreeMap::new();
            for k in left.take() {
                *read.entry(k).or_default() += 1;
            }
            if out.memory.restarts == 0 {
                assert_eq!(
                    read, planned,
                    "{ks:?} at {budget}: read set is not the plan"
                );
            } else {
                assert!(read.keys().all(|k| planned.contains_key(k)));
            }
            eprintln!(
                "R21PLAN {:?} budget {budget}: selected {row_groups_selected} of {row_groups_total}, \
                 passes {}, restarts {}",
                ks.name(),
                out.memory.passes,
                out.memory.restarts
            );
            assert!(row_groups_selected < row_groups_total || matches!(ks, KeySpec::Range { .. }));
        }
    }
}

// ---------------------------------------------------------------------------
// Text sources: byte-range chunks
// ---------------------------------------------------------------------------

fn csv_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

fn csv_lines(ids: &[(i64, &str)]) -> String {
    let mut s = String::from("id,v,ts\n");
    for (id, v) in ids {
        s.push_str(&format!("{id},{v},1970-01-01T00:00:00.000001\n"));
    }
    s
}

fn csv_source(chunk: u64) -> ChunkedTextSource {
    ChunkedTextSource::new(TextFormat::csv())
        .with_schema(csv_schema())
        .with_chunk_size(chunk)
}

fn build_csv(dir: &std::path::Path, chunk: u64) -> (Arc<ObjectStoreBackend>, String) {
    let store = Arc::new(ObjectStoreBackend::memory().unwrap());
    let opts = BuildOptions {
        chunk_size: Some(chunk),
        schema: Some(csv_schema()),
        ..BuildOptions::new(SourceKind::csv())
    };
    Builder::new(Arc::clone(&store), "idx")
        .build(
            &canonical(dir),
            &[KeySpec::Exact {
                columns: vec!["id".into()],
                counts: false,
                bounds: None,
            }],
            &opts,
        )
        .unwrap();
    (store, canonical(dir))
}

#[test]
fn guard_a_csv_side_at_the_index_chunk_size_reads_the_plan_and_equals_the_scan() {
    let (data, other) = dirs();
    let rows: Vec<(i64, &str)> = (0..80).map(|i| (i, "same")).collect();
    std::fs::write(data.path().join("x.csv"), csv_lines(&rows)).unwrap();
    let mut changed = rows.clone();
    changed[33].1 = "diff";
    std::fs::write(other.path().join("x.csv"), csv_lines(&changed)).unwrap();
    let (store, _) = build_csv(data.path(), 64);
    let planner = bellwether_verify::IndexPlanner::open(&store, "idx").unwrap();
    let left_root = planner.source_root().to_string();
    let left = csv_source(64);
    let right = csv_source(64);
    let right_root = common::root(other.path());
    let spec = ReconcileSpec::new(["id"]).coverage(workload(&[5, 33, 70]));
    let out = Reconciler::new(spec.clone(), Options::default())
        .unwrap()
        .run(
            &Side::new(&left, &left_root).indexed(&planner),
            &Side::new(&right, &right_root),
        )
        .unwrap();
    let scanned = Reconciler::new(spec, Options::default())
        .unwrap()
        .run(
            &Side::new(&left, &left_root),
            &Side::new(&right, &right_root),
        )
        .unwrap();
    assert!(matches!(out.sides[0].path, SidePath::Planned { .. }));
    assert_eq!(out.counts, scanned.counts);
    assert_eq!(out.counts.mismatched, 1);
}

/// An indexed text side whose source uses another chunk size than the one
/// the manifest records: the plan's row-group indices name other byte
/// ranges, the reconcile reads them without error, and a workload key whose
/// rows the wrong chunks miss reads as absent on that side. With the key
/// absent on the other side too, the reconcile passes although the left has
/// the key and the right does not.
#[test]
fn finding_an_indexed_text_side_at_another_chunk_size_reads_the_wrong_rows() {
    let (data, other) = dirs();
    let rows: Vec<(i64, &str)> = (0..80).map(|i| (i, "same")).collect();
    std::fs::write(data.path().join("x.csv"), csv_lines(&rows)).unwrap();
    let (store, _) = build_csv(data.path(), 64);
    let planner = bellwether_verify::IndexPlanner::open(&store, "idx").unwrap();
    let left_root = planner.source_root().to_string();
    // Find a key the 64-byte plan selects but whose row the 128-byte chunks
    // at the same indices do not hold, by reading both chunkings.
    let at64 = csv_source(64);
    let at128 = csv_source(128);
    let fp = at64.discover(&left_root).unwrap().remove(0);
    let ids_in = |src: &ChunkedTextSource, i: usize| -> Vec<i64> {
        let rgs = src.row_groups(&fp).unwrap();
        let Some(rg) = rgs.get(i) else {
            return Vec::new();
        };
        let b = src.read_columns(rg, &["id".to_string()]).unwrap();
        let a = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        a.values().to_vec()
    };
    let n64 = at64.row_groups(&fp).unwrap().len();
    let n128 = at128.row_groups(&fp).unwrap().len();
    let chunk64 = |id: i64| (0..n64).find(|&i| ids_in(&at64, i).contains(&id)).unwrap();
    // A victim v and an anchor a such that, at the plan's 64-byte indices
    // {chunk64(v), chunk64(a)}, the 128-byte chunks exist, hold a, and do
    // not hold v.
    let mut pick = None;
    'outer: for v in 0..80i64 {
        for a in 0..80i64 {
            if a == v {
                continue;
            }
            let idx = [chunk64(v), chunk64(a)];
            if idx.iter().any(|&i| i >= n128) {
                continue;
            }
            let read: Vec<i64> = idx.iter().flat_map(|&i| ids_in(&at128, i)).collect();
            if read.contains(&a) && !read.contains(&v) {
                pick = Some((v, a));
                break 'outer;
            }
        }
    }
    let (victim, anchor_key) = pick.expect("a key that moves between chunkings");
    eprintln!("victim {victim}, anchor {anchor_key}, chunks: {n64} at 64 B, {n128} at 128 B");
    // The right side lacks the victim key (a real difference) and agrees
    // on the anchor key.
    let keep: Vec<(i64, &str)> = rows.iter().copied().filter(|(i, _)| *i != victim).collect();
    std::fs::write(other.path().join("x.csv"), csv_lines(&keep)).unwrap();
    let spec = ReconcileSpec::new(["id"]).coverage(workload(&[victim, anchor_key]));
    let right = csv_source(64);
    let right_root = common::root(other.path());
    // The truth, from a scan of both sides.
    let truth = Reconciler::new(spec.clone(), Options::default())
        .unwrap()
        .run(
            &Side::new(&at64, &left_root),
            &Side::new(&right, &right_root),
        )
        .unwrap();
    assert!(!truth.pass, "the left has key {victim}, the right does not");
    // The indexed side read at the wrong chunk size.
    let res = Reconciler::new(spec, Options::default()).unwrap().run(
        &Side::new(&at128, &left_root).indexed(&planner),
        &Side::new(&right, &right_root),
    );
    match res {
        Err(e) => eprintln!("refused: {e}"),
        Ok(out) => panic!(
            "read at chunk size 128 against an index at 64: pass = {}, counts {:?} \
             (the truth fails: {:?})",
            out.pass, out.counts, truth.counts
        ),
    }
}
