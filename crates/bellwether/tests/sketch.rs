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

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::assert_is_empty
)]

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
    ledger_corpus(dir.path(), 5, 3, 400, 1_000);
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
    // id repeats within every file (1,000 ids over 1,200 rows per file).
    assert_eq!(c.items[0].cardinality.files_with_duplicates, 5);
    // (id, batch) never repeats in a file.
    assert_eq!(c.items[2].cardinality.files_with_duplicates, 0);
}

#[test]
fn changed_files_are_resketched_and_a_stale_folder_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    ledger_corpus(dir.path(), 3, 2, 300, 10_000);
    let e = engine(dir.path());
    declare(&e, "sketch:id,batch");
    // An exact key on batch: its distinct count is exact, over live row
    // groups only (the rewrite below retires part-000's batches 0..11).
    declare(&e, "exact:batch");
    // Rewrite one file with new values, add another: refused until indexed.
    let rows: Vec<Row> = (0..500)
        .map(|k| (1_000_000 + k, 9_000, Some(k), format!("q{k}")))
        .collect();
    write_file(
        &dir.path().join("part-000.parquet"),
        std::slice::from_ref(&rows),
    );
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
    let (ids, batches, rows) = truth(dir.path());
    assert_eq!(c.items[0].cardinality.rows, rows);
    assert_eq!(c.items[0].cardinality.files, 4);
    let card = &c.items[0].cardinality;
    let exact = ids.len() as u64;
    assert!(card.distinct_low <= exact && exact <= card.distinct_high);
    assert_eq!(batches.len(), 25);
    assert_eq!(
        c.items[1].cardinality.exact_distinct,
        Some(batches.len() as u64)
    );
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
    // The error bound rides in the label: three standard errors at p = 14.
    assert!(k.label.contains("(±2.4% distinct)"), "{}", k.label);
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
    match e.propose_key(&[d], &[ev], "agent").unwrap() {
        ProposeResult::Refused {
            refusal:
                ProposeRefusal::SketchNotProposable {
                    sketch,
                    declaration,
                    command,
                    ..
                },
        } => {
            assert_eq!(sketch, "sketch:batch,id");
            assert_eq!(declaration, "sketch:batch,id");
            assert!(command.unwrap().ends_with("--key sketch:batch,id"));
        }
        other => panic!("{other:?}"),
    }
}

/// When several complete sketches hold one item, `cardinality` and the
/// `why` column read the one with the highest precision (the ruling in the
/// facade's docs), whatever the declaration order.
#[test]
fn the_finest_sketch_holding_an_item_is_read() {
    let dir = tempfile::tempdir().unwrap();
    ledger_corpus(dir.path(), 3, 2, 200, 300);
    let e = engine(dir.path());
    declare(&e, "sketch:batch,p=10");
    declare(&e, "sketch:id,batch,p=12");
    let c = e.cardinality(&req(&["batch"], false)).unwrap();
    assert_eq!(c.items[0].sketch.as_deref(), Some("sketch:id,batch,p=12"));
    let rse12 = 1.04 / 64.0;
    assert!((c.items[0].cardinality.relative_standard_error - rse12).abs() < 1e-12);
    let w = e.why(&WhyRequest::default()).unwrap();
    let batch = w
        .sketch
        .iter()
        .find(|i| i.cardinality.item == "batch")
        .unwrap();
    assert_eq!(batch.sketch.as_deref(), Some("sketch:id,batch,p=12"));
    assert_eq!(batch.cardinality, c.items[0].cardinality);
}

/// A missing or damaged sketch segment never takes `why` down: plans never
/// read a sketch, so `why` reports the column unavailable with the reason
/// and the command that rebuilds it; `--sketches-only` refuses with the
/// same command; and that command (re-declaring the sketch) sketches every
/// file again, after which both work.
#[test]
fn a_missing_or_damaged_sketch_segment_leaves_why_working() {
    let dir = tempfile::tempdir().unwrap();
    ledger_corpus(dir.path(), 2, 2, 100, 300);
    let e = engine(dir.path());
    declare(&e, "sketch:id,batch");
    let idx = dir.path().join(".bellwether");
    let m: serde_json::Value =
        serde_json::from_slice(&std::fs::read(idx.join("manifest.json")).unwrap()).unwrap();
    let ids: Vec<String> = m["segments"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["kind"] == "sketch")
        .map(|s| s["id"].as_str().unwrap().to_string())
        .collect();
    assert!(!ids.is_empty());
    // Damage one segment, delete the rest.
    let first = idx.join("segments").join(format!("{}.seg", ids[0]));
    let mut bytes = std::fs::read(&first).unwrap();
    bytes[20] ^= 0x01;
    std::fs::write(&first, bytes).unwrap();
    for id in &ids[1..] {
        std::fs::remove_file(idx.join("segments").join(format!("{id}.seg"))).unwrap();
    }
    let w = e.why(&WhyRequest::default()).unwrap();
    assert!(w.sketch.is_empty());
    let why_not = w.sketch_unavailable.unwrap();
    assert!(why_not.contains("unavailable"), "{why_not}");
    assert!(why_not.contains("--key sketch:id,batch"), "{why_not}");
    let only = WhyRequest {
        sketches_only: true,
        ..WhyRequest::default()
    };
    let refused = e.why(&only).unwrap_err().to_string();
    assert!(refused.contains("--key sketch:id,batch"), "{refused}");
    // The next step works: re-declaring the sketch rebuilds it.
    let r = e
        .index_declared(&[bellwether::spec::parse_declaration("sketch:id,batch").unwrap()])
        .unwrap();
    assert_eq!(r.sketches_rebuilt, vec!["sketch:id,batch".to_string()]);
    assert_eq!(r.row_groups_read, 4, "every row group sketched again");
    let w = e.why(&WhyRequest::default()).unwrap();
    assert!(w.sketch_unavailable.is_none());
    assert_eq!(w.sketch.len(), 2);
    assert!(e.why(&only).unwrap().from_sketches);
    // A readable sketch re-declared is left alone.
    let r = e
        .index_declared(&[bellwether::spec::parse_declaration("sketch:id,batch").unwrap()])
        .unwrap();
    assert!(r.sketches_rebuilt.is_empty());
    assert_eq!(r.row_groups_read, 0);
}

/// `why --sketches-only` opens no data file, footers included: with every
/// Parquet file unreadable it still answers from the manifest and the
/// sketch segments, reports zero reads, and gives the dataset's files, row
/// groups, and rows as a full `why` does; `sketch cardinality` answers too,
/// and a full `why` (the control) cannot.
#[cfg(unix)]
#[test]
fn sketches_only_opens_no_data_file() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    ledger_corpus(dir.path(), 4, 2, 200, 300);
    let e = engine(dir.path());
    declare(&e, "sketch:id,batch");
    let full = e.why(&WhyRequest::default()).unwrap();
    assert_eq!(full.reads.footers, 4, "a full why reads every footer");
    let files: Vec<std::path::PathBuf> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|f| f.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
        .collect();
    assert_eq!(files.len(), 4);
    let set = |mode: u32| {
        for f in &files {
            std::fs::set_permissions(f, std::fs::Permissions::from_mode(mode)).unwrap();
        }
    };
    set(0o000);
    if std::fs::File::open(&files[0]).is_ok() {
        // Permission bits do not bind this user (root): nothing to show.
        set(0o644);
        return;
    }
    // A fresh engine: no footer held in memory.
    let e = engine(dir.path());
    let w = e.why(&WhyRequest {
        sketches_only: true,
        ..WhyRequest::default()
    });
    let c = e.cardinality(&req(&[], false));
    let control = e.why(&WhyRequest::default());
    set(0o644);
    let w = w.unwrap();
    assert!(w.from_sketches);
    assert_eq!(w.reads, bellwether::DiagnosisReads::default());
    assert_eq!(
        (w.dataset.files, w.dataset.row_groups, w.dataset.rows),
        (
            full.dataset.files,
            full.dataset.row_groups,
            full.dataset.rows
        )
    );
    assert_eq!(w.sketch.len(), 2);
    assert!(
        !w.profile.inputs.is_empty(),
        "the Evidence cites the manifest"
    );
    assert!(w.evidence.hash.is_some(), "and is recorded");
    let c = c.unwrap();
    assert_eq!((c.files_read, c.row_groups_read), (0, 0));
    assert!(control.is_err(), "a full why must open the files");
}

/// Through the facade, a sketch never reaches a plan or a reconcile: twin
/// folders with the same files and key specs, one also declaring a sketch,
/// give byte-equal `locate`, `latest`, `asof`, and window plans (JSON, apart
/// from the evidence each records, which cites its own manifest), the same
/// `why --from` costs, candidates, and recommendation, and the same
/// multi-pass reconcile (counts, coverage, and memory report, passes
/// included); and again after the sketch's segments are deleted from the
/// store.
#[test]
fn twin_folders_plan_identically_with_and_without_a_sketch() {
    let plain = tempfile::tempdir().unwrap();
    let sketched = tempfile::tempdir().unwrap();
    ledger_corpus(plain.path(), 4, 3, 200, 1_000);
    ledger_corpus(sketched.path(), 4, 3, 200, 1_000);
    let kd = tempfile::tempdir().unwrap();
    let kf = kd.path().join("k.parquet");
    write_keys(&kf, "id", &[3, 77, 140]);
    let keys = KeyFile::new(kf.to_str().unwrap());
    let open = |dir: &Path, extra: &[&str]| {
        let e = engine(dir);
        let declared: Vec<_> = ["exact:id", "temporal:id@ts", "range:ts"]
            .iter()
            .chain(extra)
            .map(|s| bellwether::spec::parse_declaration(s).unwrap())
            .collect();
        e.index_declared(&declared).unwrap();
        e
    };
    let a = open(plain.path(), &[]);
    let b = open(sketched.path(), &["sketch:id,batch,id+batch"]);
    let plans = |e: &Engine, dir: &Path| -> Vec<String> {
        let canonical = std::fs::canonicalize(dir).unwrap();
        [
            e.locate(&keys).unwrap(),
            e.latest(&keys).unwrap(),
            e.asof(&keys, "1970-01-01T00:05:00Z").unwrap(),
            e.locate_window(&keys, "1970-01-01T00:01:00Z", "1970-01-01T00:08:00Z", None)
                .unwrap(),
        ]
        .iter()
        .map(|p| {
            let mut v = serde_json::to_value(p).unwrap();
            v.as_object_mut().unwrap().remove("evidence");
            v.to_string()
                .replace(canonical.to_str().unwrap(), "<root>")
                .replace(dir.to_str().unwrap(), "<root>")
        })
        .collect()
    };
    let why = |e: &Engine| -> String {
        let w = e
            .why(&WhyRequest {
                keys: Some(keys.clone()),
                ..WhyRequest::default()
            })
            .unwrap();
        let candidates: Vec<_> = w
            .candidates
            .iter()
            .map(|c| (&c.spec, c.origin, c.proposed, c.locate))
            .collect();
        let recommended = w
            .recommended
            .as_ref()
            .map(|r| (&r.spec, &r.lookup, r.current, r.proposed));
        serde_json::to_string(&(
            &w.current_specs,
            w.current,
            w.current_locate,
            w.dataset,
            candidates,
            recommended,
            // Data reads depend on the profile's cache; footers do not.
            w.reads.footers,
        ))
        .unwrap()
    };
    // A reconcile of each folder against itself, under a budget that makes
    // it run in several passes.
    let reconciled = |e: &Engine, dir: &Path| -> String {
        let r = e
            .reconcile(
                dir.to_str().unwrap(),
                &bellwether::ReconcileRequest {
                    key: vec!["id".into()],
                    latest_by: Some("ts".into()),
                    memory_budget: Some(64 * 1024),
                    ..bellwether::ReconcileRequest::default()
                },
            )
            .unwrap();
        let o = &r.outcome;
        assert!(o.pass && o.memory.passes > 1, "{:?}", o.memory);
        serde_json::to_string(&(o.pass, &o.counts, &o.coverage, &o.memory)).unwrap()
    };
    let (pa, wa) = (plans(&a, plain.path()), why(&a));
    let ra = reconciled(&a, plain.path());
    // Non-vacuous: the locate plan prunes, and why priced candidates.
    let locate: serde_json::Value = serde_json::from_str(&pa[0]).unwrap();
    assert!(
        locate["planned"]["row_groups"].as_u64().unwrap()
            < locate["row_groups_total"].as_u64().unwrap(),
        "{locate}"
    );
    assert!(wa.contains("proposed") || wa.contains("row_groups"), "{wa}");
    assert_eq!(pa, plans(&b, sketched.path()), "with a sketch");
    assert_eq!(ra, reconciled(&b, sketched.path()), "with a sketch");
    assert_eq!(wa, why(&b), "with a sketch");
    // Delete every sketch segment from the sketched folder's store.
    let idx = sketched.path().join(".bellwether");
    let m: serde_json::Value =
        serde_json::from_slice(&std::fs::read(idx.join("manifest.json")).unwrap()).unwrap();
    let mut deleted = 0;
    for s in m["segments"].as_array().unwrap() {
        if s["kind"] == "sketch" {
            let id = s["id"].as_str().unwrap();
            std::fs::remove_file(idx.join("segments").join(format!("{id}.seg"))).unwrap();
            deleted += 1;
        }
    }
    assert!(deleted > 0);
    assert_eq!(pa, plans(&b, sketched.path()), "sketch segments deleted");
    assert_eq!(wa, why(&b), "sketch segments deleted");
    assert_eq!(
        ra,
        reconciled(&b, sketched.path()),
        "sketch segments deleted"
    );
}

/// A file repeating fewer rows than the detection floor is not flagged,
/// and the floor says so: ten files of 1,000 rows, five of them with 5
/// repeated ids (under the floor of 24 at precision 14), are reported as
/// 0 files with duplicates; with 100 repeated ids each, all five are.
#[test]
fn files_repeating_fewer_rows_than_the_floor_go_undetected() {
    let corpus = |dups: i64| {
        let dir = tempfile::tempdir().unwrap();
        for f in 0..10_i64 {
            let rows: Vec<Row> = (0..1_000_i64)
                .map(|r| {
                    // Files 0..5 repeat `dups` of their ids once each.
                    let id = if f < 5 && r >= 1_000 - dups {
                        f * 1_000 + (r - (1_000 - dups))
                    } else {
                        f * 1_000 + r
                    };
                    (id, f, Some(r), String::new())
                })
                .collect();
            write_file(&dir.path().join(format!("part-{f:03}.parquet")), &[rows]);
        }
        let e = engine(dir.path());
        declare(&e, "sketch:id");
        let c = e.cardinality(&req(&["id"], false)).unwrap();
        (dir, c.items[0].cardinality.clone())
    };
    let (_d, under) = corpus(5);
    assert_eq!(under.duplicate_floor, 24);
    assert_eq!(
        under.files_with_duplicates, 0,
        "5 repeats per file go unseen"
    );
    let (_d, over) = corpus(100);
    assert_eq!(over.duplicate_floor, 24);
    assert_eq!(over.files_with_duplicates, 5);
}

/// `--sketches-only` with no sketch names a declaration over the dataset's
/// real id-like columns, never a template.
#[test]
fn sketches_only_without_a_sketch_names_real_columns() {
    let dir = tempfile::tempdir().unwrap();
    ledger_corpus(dir.path(), 2, 1, 50, 20);
    let e = engine(dir.path());
    declare(&e, "exact:id");
    match e
        .why(&WhyRequest {
            sketches_only: true,
            ..WhyRequest::default()
        })
        .unwrap_err()
    {
        Error::NotIndexed {
            needs: Some(needs), ..
        } => {
            assert!(needs.starts_with("sketch:"), "{needs}");
            assert!(!needs.contains('<'), "{needs}");
            assert!(needs.contains("id"), "{needs}");
        }
        other => panic!("{other}"),
    }
}
