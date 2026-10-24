//! Review probe (986ad0f): an indexed text side whose source uses another
//! chunk size with the SAME row-group count, where the planned chunk has the
//! same length but another start. The per-file count check and the plan
//! bytes check both pass; only a byte-range check against the manifest
//! would see it. Searches variable-width CSV for such a case and, if found,
//! asserts the reconcile does not pass when the truth fails.

#![allow(
    clippy::unwrap_used,
    clippy::pedantic,
    clippy::cloned_ref_to_slice_refs,
    missing_docs
)]

use std::sync::Arc;
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bellwether_build::{BuildOptions, Builder, SourceKind};
use bellwether_source::{ChunkedTextSource, TextFormat};
use bellwether_store::ObjectStoreBackend;
use bellwether_types::{FileFingerprint, Hash, KeySpec, Source};
use bellwether_verify::{
    CoverageRequest, IndexPlanner, Options, ReconcileSpec, Reconciler, Side, WorkloadKeys,
};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Utf8, false),
    ]))
}

fn src(chunk: u64) -> ChunkedTextSource {
    ChunkedTextSource::new(TextFormat::csv())
        .with_schema(schema())
        .with_chunk_size(chunk)
}

fn int_key(v: i64) -> Vec<u8> {
    let mut b = vec![0x02];
    let mut x = v.to_be_bytes();
    x[0] ^= 0x80;
    b.extend_from_slice(&x);
    b
}

fn csv(rows: &[(i64, usize)]) -> String {
    let mut s = String::from("id,v\n");
    for (id, w) in rows {
        s.push_str(&format!("{id},{}\n", "x".repeat(*w)));
    }
    s
}

struct Chunks {
    ranges: Vec<(u64, u64)>,
    ids: Vec<Vec<i64>>,
}

fn chunks(s: &ChunkedTextSource, fp: &FileFingerprint) -> Chunks {
    let rgs = s.row_groups(fp).unwrap();
    let mut out = Chunks {
        ranges: Vec::new(),
        ids: Vec::new(),
    };
    for rg in &rgs {
        out.ranges.push(rg.byte_range.unwrap());
        let b = s.read_columns(rg, &["id".to_string()]).unwrap();
        let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        out.ids.push(a.values().to_vec());
    }
    out
}

#[test]
fn an_equal_count_chunking_with_shifted_starts_is_not_a_pass() {
    let mut seed = 12345u64;
    let mut next = || {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (seed >> 33) as usize
    };
    let mut found = None;
    'search: for trial in 0..40 {
        let rows: Vec<(i64, usize)> = (0..60).map(|i| (i, 1 + next() % 25)).collect();
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("x.csv"), csv(&rows)).unwrap();
        let root = dir.path().to_str().unwrap().to_string();
        let fp = src(64).discover(&root).unwrap().remove(0);
        for a in (48..140).step_by(4) {
            let ca = chunks(&src(a), &fp);
            for b in (48..140).step_by(3) {
                if a == b {
                    continue;
                }
                let cb = chunks(&src(b), &fp);
                if ca.ranges.len() != cb.ranges.len() {
                    continue;
                }
                for i in 0..ca.ranges.len() {
                    let (la, lb) = (ca.ranges[i], cb.ranges[i]);
                    if la.1 - la.0 != lb.1 - lb.0 || la.0 == lb.0 {
                        continue;
                    }
                    let victim = ca.ids[i].iter().find(|v| !cb.ids[i].contains(v));
                    let anchor = ca.ids[i].iter().find(|v| cb.ids[i].contains(v));
                    if let (Some(&v), Some(&k)) = (victim, anchor) {
                        found = Some((rows.clone(), a, b, v, k, trial));
                        break 'search;
                    }
                }
            }
        }
    }
    let Some((rows, a, b, victim, anchor, trial)) = found else {
        eprintln!("R21CHUNK no equal-count shifted-start case found");
        return;
    };
    eprintln!("R21CHUNK found: trial {trial}, index at {a}, read at {b}, victim {victim}, anchor {anchor}");
    let (data, other) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    std::fs::write(data.path().join("x.csv"), csv(&rows)).unwrap();
    let keep: Vec<(i64, usize)> = rows.iter().copied().filter(|(i, _)| *i != victim).collect();
    std::fs::write(other.path().join("x.csv"), csv(&keep)).unwrap();
    let store = Arc::new(ObjectStoreBackend::memory().unwrap());
    let canonical = std::fs::canonicalize(data.path())
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let opts = BuildOptions {
        chunk_size: Some(a),
        schema: Some(schema()),
        ..BuildOptions::new(SourceKind::csv())
    };
    Builder::new(Arc::clone(&store), "idx")
        .build(
            &canonical,
            &[KeySpec::Exact {
                columns: vec!["id".into()],
            }],
            &opts,
        )
        .unwrap();
    let planner = IndexPlanner::open(&store, "idx").unwrap();
    let left_root = planner.source_root().to_string();
    let right_root = other.path().to_str().unwrap().to_string();
    let spec = ReconcileSpec::new(["id"]).coverage(CoverageRequest::Workload(WorkloadKeys {
        workload: Hash::of(b"w"),
        age: Duration::ZERO,
        keys: vec![int_key(victim), int_key(anchor)],
    }));
    let truth = Reconciler::new(spec.clone(), Options::default())
        .unwrap()
        .run(
            &Side::new(&src(a), &left_root),
            &Side::new(&src(a), &right_root),
        )
        .unwrap();
    assert!(!truth.pass);
    let wrong = src(b);
    let res = Reconciler::new(spec, Options::default()).unwrap().run(
        &Side::new(&wrong, &left_root).indexed(&planner),
        &Side::new(&src(a), &right_root),
    );
    match res {
        Err(e) => eprintln!("R21CHUNK refused: {e}"),
        Ok(out) => {
            assert!(
                !out.pass,
                "read at {b} against an index at {a}: pass = true, counts {:?}; truth {:?}",
                out.counts, truth.counts
            );
            eprintln!("R21CHUNK not refused but fails: {:?}", out.counts);
        }
    }
}
