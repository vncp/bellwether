//! `sketch` through the facade: `cardinality` from the index equals the
//! transient sketch of the same files (predicted equals built) and holds a
//! brute-force distinct count in its interval; exact counts replace the
//! estimate where a complete exact key spec holds them; changed files are
//! re-sketched and their old entries never counted; a stale folder is
//! refused; `overlap` across two folders; the `why` report's `sketch`
//! column (a funnel `kind_column` for a candidate on a sketched item);
//! `why` from sketches alone reads no data file; `R-sketch-1` fires on a
//! low-cardinality key beside an id and stays quiet otherwise; and a sketch
//! is refused as a proposal.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

mod common;

use std::collections::BTreeSet;
use std::path::Path;

use bellwether::types::{Error, ExactDistinctSource};
use bellwether::{
    CardinalityRequest, Engine, EngineOptions, KeyFile, ProposeRefusal, ProposeResult,
    SketchSource, WhyRequest,
};
use common::{ledger_corpus, write_file, write_keys, Row};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn engine(dir: &Path) -> Engine {
    Engine::open(dir.to_str().unwrap(), EngineOptions::default()).unwrap()
}

fn declare(e: &Engine, s: &str) {
    e.index_declared(&[bellwether::spec::parse_declaration(s).unwrap()])
        .unwrap();
}

fn req(items: &[&str], transient: bool) -> CardinalityRequest {
    CardinalityRequest {
        items: items.iter().map(|s| (*s).to_string()).collect(),
        transient,
    }
}

/// Distinct `(id)` and `(batch)` values over every file, decoded with the
/// `parquet` crate.
fn truth(dir: &Path) -> (BTreeSet<i64>, BTreeSet<i64>, u64) {
    let (mut ids, mut batches, mut rows) = (BTreeSet::new(), BTreeSet::new(), 0u64);
    for f in std::fs::read_dir(dir).unwrap() {
        let p = f.unwrap().path();
        if p.extension().is_none_or(|x| x != "parquet") {
            continue;
        }
        let r = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&p).unwrap())
            .unwrap()
            .build()
            .unwrap();
        for b in r {
            let b = b.unwrap();
            let id = b
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            let bt = b
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            ids.extend(id.values().iter().copied());
            batches.extend(bt.values().iter().copied());
            rows += b.num_rows() as u64;
        }
    }
    (ids, batches, rows)
}

#[test]
fn cardinality_matches_the_files_and_the_transient_sketch() {
    let dir = tempfile::tempdir().unwrap();
    ledger_corpus(dir.path(), 5, 3, 400, 2_500);
    let e = engine(dir.path());
    declare(&e, "exact:batch");
    // Before the sketch: transient reads everything.
    let t = e
        .cardinality(&req(&["id", "batch", "id+batch"], true))
        .unwrap();
    assert_eq!((t.files_read, t.row_groups_read), (5, 15));
    assert!(t.items.iter().all(|i| i.source == SketchSource::Transient));
    declare(&e, "sketch:id,batch,id+batch");
    let c = e.cardinality(&req(&[], false)).unwrap();
    assert!(c.approximate);
    assert_eq!((c.files_read, c.row_groups_read), (0, 0));
    let (ids, batches, rows) = truth(dir.path());
    for (i, item) in c.items.iter().enumerate() {
        assert_eq!(item.source, SketchSource::Index);
        assert_eq!(item.sketch.as_deref(), Some("sketch:id,batch,id+batch"));
        // Predicted (transient) equals built (index).
        assert_eq!(
            item.cardinality, t.items[i].cardinality,
            "{}",
            item.cardinality.item
        );
        let card = &item.cardinality;
        assert_eq!(card.rows, rows);
        assert_eq!(card.files, 5);
        let exact = match card.item.as_str() {
            "id" => ids.len() as u64,
            "batch" => batches.len() as u64,
            _ => rows, // (id, batch) is unique per row here
        };
        assert!(
            card.distinct_low <= exact && exact <= card.distinct_high,
            "{}: {exact} outside [{}, {}]",
            card.item,
            card.distinct_low,
            card.distinct_high
        );
    }
    // batch has a complete exact spec: its distinct count is exact.
    let b = &c.items[1].cardinality;
    assert_eq!(b.exact_distinct, Some(batches.len() as u64));
    assert_eq!(b.exact_source, Some(ExactDistinctSource::ExactSegment));
    assert!(b.rows_per_value_exact);
    // id repeats within files (2,500 ids over 2,000 rows per file).
    assert!(c.items[0].cardinality.files_with_duplicates > 0);
    // (id, batch) never repeats in a file.
    assert_eq!(c.items[2].cardinality.files_with_duplicates, 0);
}

#[test]
fn changed_files_are_resketched_and_a_stale_folder_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    ledger_corpus(dir.path(), 3, 2, 300, 10_000);
    let e = engine(dir.path());
    declare(&e, "sketch:id,batch");
    // Rewrite one file with new values, add another: refused until indexed.
    let rows: Vec<Row> = (0..500)
        .map(|k| (1_000_000 + k, 9_000, Some(k), format!("q{k}")))
        .collect();
    write_file(&dir.path().join("part-000.parquet"), &[rows.clone()]);
    write_file(&dir.path().join("part-100.parquet"), &[rows]);
    let err = e.cardinality(&req(&["id"], false)).unwrap_err();
    assert!(matches!(err, Error::StaleIndex { .. }), "{err}");
    e.index(&[]).unwrap();
    let c = e.cardinality(&req(&["id", "batch"], false)).unwrap();
    let t = e
        .cardinality(&CardinalityRequest {
            items: vec!["id".into(), "batch".into()],
            transient: true,
        })
        .unwrap();
    assert_eq!(c.items[0].source, SketchSource::Index);
    let (ids, _, rows) = truth(dir.path());
    assert_eq!(c.items[0].cardinality.rows, rows);
    assert_eq!(c.items[0].cardinality.files, 4);
    let card = &c.items[0].cardinality;
    let exact = ids.len() as u64;
    assert!(card.distinct_low <= exact && exact <= card.distinct_high);
    // A second engine computing transiently over the same files agrees.
    let fresh = tempfile::tempdir().unwrap();
    for f in std::fs::read_dir(dir.path()).unwrap() {
        let p = f.unwrap().path();
        if p.extension().is_some_and(|x| x == "parquet") {
            std::fs::copy(&p, fresh.path().join(p.file_name().unwrap())).unwrap();
        }
    }
    let u = engine(fresh.path())
        .cardinality(&req(&["id", "batch"], true))
        .unwrap();
    assert_eq!(u.sequence, 0);
    assert_eq!(u.items[0].cardinality, c.items[0].cardinality);
    assert_eq!(t.items[1].cardinality, c.items[1].cardinality);
}

#[test]
fn unsketched_items_are_refused_with_the_declaration_that_builds_them() {
    let dir = tempfile::tempdir().unwrap();
    ledger_corpus(dir.path(), 2, 1, 50, 20);
    let e = engine(dir.path());
    // Unindexed: refused, and transient needs named items.
    match e.cardinality(&req(&["id"], false)).unwrap_err() {
        Error::NotIndexed {
            needs, operation, ..
        } => {
            assert_eq!(needs.as_deref(), Some("sketch:id"));
            assert_eq!(operation, "cardinality");
        }
        other => panic!("{other:?}"),
    }
    let err = e.cardinality(&req(&[], true)).unwrap_err();
    assert!(
        matches!(err, Error::InvalidArgument { field: "items", .. }),
        "{err:?}"
    );
    declare(&e, "sketch:batch");
    let err = e.cardinality(&req(&["id"], false)).unwrap_err();
    assert!(
        err.to_string().contains("no complete sketch holding id"),
        "{err}"
    );
    // A finer sketch holding the same item is preferred.
    declare(&e, "sketch:batch,id,p=16");
    let c = e.cardinality(&req(&["batch"], false)).unwrap();
    assert_eq!(c.items[0].sketch.as_deref(), Some("sketch:batch,id,p=16"));
}

#[test]
fn overlap_across_two_folders() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    // ids 0..1000 and 500..1500: Jaccard 500 / 1500.
    let rows = |lo: i64| -> Vec<Row> {
        (lo..lo + 1000)
            .map(|k| (k, 0, Some(k), "x".into()))
            .collect()
    };
    write_file(&a.path().join("a.parquet"), &[rows(0)]);
    write_file(&b.path().join("b.parquet"), &[rows(500)]);
    let (ea, eb) = (engine(a.path()), engine(b.path()));
    declare(&ea, "sketch:id,minhash=256");
    declare(&eb, "sketch:id,minhash=256");
    let o = ea.overlap("id", "id", Some(&eb)).unwrap();
    assert!(o.approximate);
    let exact = 500.0 / 1500.0;
    assert!(
        o.overlap.jaccard_low <= exact && exact <= o.overlap.jaccard_high,
        "{:?}",
        o.overlap
    );
    assert_eq!(o.left_root, ea.root());
    assert_eq!(o.right_root, eb.root());
    // Without signatures on one side: inclusion-exclusion, still bounded.
    declare(&eb, "sketch:id");
    let o = ea.overlap("id", "id", Some(&eb)).unwrap();
    assert_eq!(o.overlap.method, bellwether::types::OverlapMethod::Hll);
    assert!(o.overlap.jaccard_low <= exact && exact <= o.overlap.jaccard_high);
    // Different types never compare.
    declare(&ea, "sketch:payload");
    let err = ea.overlap("payload", "id", Some(&eb)).unwrap_err();
    assert!(err.to_string().contains("never compare equal"), "{err}");
}

/// 200,000 rows over four files: `batch` has two values of 100,000 rows
/// each (a write stamp), `id` 50,000 values; a second corpus has `batch`
/// with 200 values (1,000 rows each), which is not a low-cardinality key.
fn stamped(dir: &Path, rows_per_value: i64) {
    for f in 0..4i64 {
        let rows: Vec<Row> = (0..50_000i64)
            .map(|i| {
                let k = f * 50_000 + i;
                (k % 50_000, k / rows_per_value, Some(k), String::new())
            })
            .collect();
        write_file(&dir.join(format!("part-{f}.parquet")), &[rows]);
    }
}

#[test]
fn r_sketch_1_fires_on_a_low_cardinality_key_and_why_reads_no_file_from_sketches() {
    let dir = tempfile::tempdir().unwrap();
    stamped(dir.path(), 100_000);
    let e = engine(dir.path());
    declare(&e, "sketch:batch,id");
    let w = e
        .why(&WhyRequest {
            sketches_only: true,
            ..WhyRequest::default()
        })
        .unwrap();
    assert!(w.from_sketches);
    assert_eq!((w.reads.files, w.reads.row_groups), (0, 0));
    assert!(w.profile.fanout.is_empty());
    assert_eq!(w.sketch.len(), 2);
    let lint = w
        .profile
        .lints
        .iter()
        .find(|l| l.rule == "R-sketch-1")
        .expect("R-sketch-1 fires");
    assert_eq!(lint.columns, vec!["batch".to_string(), "id".to_string()]);
    assert!(lint.message.contains("approximate"), "{}", lint.message);
    assert!(lint.message.contains("(id, batch)"), "{}", lint.message);
    // The Evidence records it.
    let rec = format!("{:?}", w.evidence.evidence.body);
    assert!(rec.contains("R-sketch-1"), "{rec}");
    // The full why reads every file for the same diagnosis.
    let full = e.why(&WhyRequest::default()).unwrap();
    assert_eq!(full.reads.files, 4);
    assert!(full.profile.lints.iter().any(|l| l.rule == "R-sketch-1"));
}

#[test]
fn r_sketch_1_stays_quiet_when_values_cover_few_rows_or_no_id_sits_beside() {
    let dir = tempfile::tempdir().unwrap();
    stamped(dir.path(), 1_000);
    let e = engine(dir.path());
    declare(&e, "sketch:batch,id");
    let w = e
        .why(&WhyRequest {
            sketches_only: true,
            ..WhyRequest::default()
        })
        .unwrap();
    assert!(
        w.profile.lints.iter().all(|l| l.rule != "R-sketch-1"),
        "{:?}",
        w.profile.lints
    );
    // A low-cardinality column sketched alone: no partner, no lint.
    let dir = tempfile::tempdir().unwrap();
    stamped(dir.path(), 100_000);
    let e = engine(dir.path());
    declare(&e, "sketch:batch");
    let w = e
        .why(&WhyRequest {
            sketches_only: true,
            ..WhyRequest::default()
        })
        .unwrap();
    assert!(w.profile.lints.iter().all(|l| l.rule != "R-sketch-1"));
}

#[test]
fn a_candidate_on_a_sketched_item_carries_the_sketch_column() {
    let dir = tempfile::tempdir().unwrap();
    ledger_corpus(dir.path(), 3, 2, 200, 300);
    let e = engine(dir.path());
    declare(&e, "sketch:id");
    let keys = tempfile::tempdir().unwrap();
    let kf = keys.path().join("k.parquet");
    write_keys(&kf, "id", &[1, 2, 3]);
    let w = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(kf.to_str().unwrap())),
            ..WhyRequest::default()
        })
        .unwrap();
    let c = w
        .candidates
        .iter()
        .find(|c| c.spec.columns() == ["id".to_string()])
        .unwrap();
    let k = c.kind_column.as_ref().unwrap();
    assert_eq!(k.kind, "sketch");
    assert!(k.value.starts_with('≈'), "{}", k.value);
    assert!(k.label.contains("approximate"), "{}", k.label);
    assert!(!k.exactness.is_exact());
    // The JSON omits the column for candidates without one.
    assert!(w.reads.files > 0);
}

#[test]
fn a_sketch_is_refused_as_a_proposal() {
    let dir = tempfile::tempdir().unwrap();
    ledger_corpus(dir.path(), 2, 1, 50, 20);
    let e = engine(dir.path());
    declare(&e, "exact:id");
    let ev = e
        .why(&WhyRequest::default())
        .unwrap()
        .evidence
        .hash
        .unwrap();
    let d = bellwether::spec::parse_declaration("sketch:batch,id").unwrap();
    match e.propose_key(&d, &[ev], "agent").unwrap() {
        ProposeResult::Refused {
            refusal:
                ProposeRefusal::SidecarNotProposable {
                    sidecar,
                    key,
                    command,
                    ..
                },
        } => {
            assert_eq!(sidecar, "sketch:batch,id");
            assert_eq!(key, None);
            assert!(command.unwrap().ends_with("--key sketch:batch,id"));
        }
        other => panic!("{other:?}"),
    }
}
