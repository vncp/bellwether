//! Text keys (B5): predicted equals planned. A text run (a recorded
//! `contains`, `has_token`, or `has_prefix` run) priced before its key
//! exists (transient segments, through this crate's planner over an
//! unindexed folder and through the build planner beside committed specs)
//! plans exactly what the committed key plans, run by run, the replay's
//! costs are the committed plans' sums, and its token counts are the
//! committed dictionary's.
#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::type_complexity
)]

use std::collections::BTreeSet;
use std::fs::File;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bellwether_build::{BuildOptions, Builder, SourceKind};
use bellwether_core::text;
use bellwether_core::SegmentReader;
use bellwether_replay::{
    text_residual, KeyInput, MemoryKeySets, Replayer, RunQuery, WorkloadRecorder,
};
use bellwether_source::{ParquetSource, Partitioning};
use bellwether_store::{ObjectStoreBackend, Snapshot};
use bellwether_types::{KeySpec, Predicate, ScanPlan, TextOp, Tokenizer};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("body", DataType::Utf8, true),
    ]))
}

fn write(path: &std::path::Path, start: i64, docs: &[Option<&str>], per_rg: usize) {
    let ids: ArrayRef = Arc::new((start..start + docs.len() as i64).collect::<Int64Array>());
    let bodies: ArrayRef = Arc::new(docs.iter().copied().collect::<StringArray>());
    let b = RecordBatch::try_new(schema(), vec![ids, bodies]).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(per_rg))
        .build();
    let mut w = ArrowWriter::try_new(File::create(path).unwrap(), schema(), Some(props)).unwrap();
    w.write(&b).unwrap();
    w.close().unwrap();
}

fn text(tokenizer: Tokenizer) -> KeySpec {
    KeySpec::Text {
        column: "body".into(),
        tokenizer,
        bloom: None,
    }
}

/// A plan as files, row groups, and bytes: comparable across manifests
/// (an unindexed folder plans at manifest version 0).
fn shape(plan: &ScanPlan) -> (BTreeSet<(String, Vec<u32>, u64)>, u64, u64) {
    (
        plan.entries
            .iter()
            .map(|e| (e.file.path.clone(), e.row_groups.iter().collect(), e.bytes))
            .collect(),
        plan.bytes,
        plan.row_groups_selected,
    )
}

const DOCS: [Option<&str>; 10] = [
    Some("Quarterly guidance raised"),
    None,
    Some("guidance withdrawn; Straße"),
    Some(""),
    Some("ΣΑΣ restatement"),
    Some("dividend 日本"),
    None,
    Some("STRASSE quarterly"),
    Some("İstanbul"),
    Some("unguidanced nothing"),
];

fn queries() -> Vec<(TextOp, &'static str)> {
    [
        (TextOp::HasToken, "guidance"),
        (TextOp::HasToken, "STRAßE"),
        (TextOp::Contains, "uarter"),
        (TextOp::Contains, "σασ"),
        (TextOp::Contains, "ab"),
        (TextOp::Contains, "zzzz"),
        (TextOp::HasPrefix, "rest"),
        (TextOp::HasPrefix, "日"),
    ]
    .to_vec()
}

/// One text run per query, in order (`q0`, `q1`, ...), and one run asking
/// for any of several values (`many`).
fn workload(keys: &MemoryKeySets) -> bellwether_replay::Workload {
    let mut rec = WorkloadRecorder::new(keys, schema());
    for (i, (op, v)) in queries().into_iter().enumerate() {
        rec.record_query(
            &format!("q{i}"),
            KeyInput::Batch(&body_batch(&[v])),
            RunQuery::Text(op),
            None,
        )
        .unwrap();
    }
    rec.record_query(
        "many",
        KeyInput::Batch(&body_batch(&["guidance", "zzzz", "日本"])),
        RunQuery::Text(TextOp::Contains),
        None,
    )
    .unwrap();
    rec.finish().unwrap()
}

fn body_batch(values: &[&str]) -> RecordBatch {
    let a: ArrayRef = Arc::new(StringArray::from(values.to_vec()));
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, false)])),
        vec![a],
    )
    .unwrap()
}

#[test]
fn a_priced_text_key_plans_what_the_built_one_plans() {
    let dir = tempfile::tempdir().unwrap();
    let data = std::fs::canonicalize(dir.path()).unwrap().join("data");
    std::fs::create_dir_all(&data).unwrap();
    write(&data.join("a.parquet"), 0, &DOCS[..5], 2);
    write(&data.join("b.parquet"), 5, &DOCS[5..], 1);
    let root = data.to_str().unwrap();
    let opts = BuildOptions {
        schema: Some(schema()),
        ..BuildOptions::new(SourceKind::Parquet)
    };
    // One index with both text keys committed, one with only a key on id.
    let built = Arc::new(ObjectStoreBackend::memory().unwrap());
    let both = [text(Tokenizer::Words), text(Tokenizer::Trigram)];
    Builder::new(Arc::clone(&built), "idx")
        .build(root, &both, &opts)
        .unwrap();
    let bare = Arc::new(ObjectStoreBackend::memory().unwrap());
    Builder::new(Arc::clone(&bare), "idx")
        .build(
            root,
            &[KeySpec::Exact {
                columns: vec!["id".into()],
                counts: false,
                bounds: None,
            }],
            &opts,
        )
        .unwrap();
    let source = ParquetSource::new().with_schema(schema());
    let built_snap = Snapshot::open(Arc::clone(&built), "idx").unwrap();
    let committed = Replayer::for_snapshot(&source, &built_snap).unwrap();
    let bare_snap = Snapshot::open(Arc::clone(&bare), "idx").unwrap();
    let beside = Replayer::for_snapshot(&source, &bare_snap).unwrap();
    let unindexed = Replayer::unindexed(&source, root, &Partitioning::None).unwrap();
    let keys = MemoryKeySets::new();
    let w = workload(&keys);
    for run in w.runs() {
        let rk = w.run_keys(&keys, run).unwrap();
        let q = run.query().unwrap();
        let residual = text_residual(&rk, &q).unwrap().unwrap();
        assert!(matches!(residual, Predicate::Text { .. }));
        for specs in [
            vec![text(Tokenizer::Words)],
            vec![text(Tokenizer::Trigram)],
            both.to_vec(),
        ] {
            let planned = committed.plan_of_query(&specs, &rk, &q).unwrap();
            let via_build = beside.plan_of_query(&specs, &rk, &q).unwrap();
            let via_segments = unindexed.plan_of_query(&specs, &rk, &q).unwrap();
            let id = &run.run_id;
            assert_eq!(shape(&via_build), shape(&planned), "{id} {specs:?}");
            assert_eq!(shape(&via_segments), shape(&planned), "{id} {specs:?}");
            assert_eq!(planned.residual.as_ref(), Some(&residual));
            assert_eq!(via_segments.residual.as_ref(), Some(&residual));
            assert_eq!(via_build.residual.as_ref(), Some(&residual));
        }
        // No text key: every live row group, in both planners.
        let all = committed.plan_of_query(&[], &rk, &q).unwrap();
        assert_eq!(all.row_groups_selected, all.row_groups_total);
        assert_eq!(
            shape(&unindexed.plan_of_query(&[], &rk, &q).unwrap()),
            shape(&all)
        );
    }
    // The replay's run costs are the committed plans' sums, run by run.
    for specs in [vec![text(Tokenizer::Words)], vec![text(Tokenizer::Trigram)]] {
        let predicted = unindexed.replay_specs(&specs, &w, &keys).unwrap();
        let realized = committed.replay_specs(&specs, &w, &keys).unwrap();
        let strip = |c: &[bellwether_types::RunCost]| -> Vec<(String, u64, u64, u64)> {
            c.iter()
                .map(|r| (r.run_id.clone(), r.keys, r.row_groups, r.bytes))
                .collect()
        };
        assert_eq!(strip(&predicted), strip(&realized), "{specs:?}");
    }
    // Token counts priced before the key exists are the built dictionary's.
    for spec in both {
        let segs: Vec<SegmentReader<bytes::Bytes>> = built_snap
            .manifest()
            .key_spec_segments(&spec.name())
            .map(|s| SegmentReader::open(built_snap.read_segment(&s.id).unwrap()).unwrap())
            .collect();
        let committed_stats = text::token_stats(&segs).unwrap();
        assert_eq!(unindexed.text_stats(&spec).unwrap(), committed_stats);
        assert_eq!(beside.text_stats(&spec).unwrap(), committed_stats);
        assert!(committed_stats.distinct > 0);
    }
}
