//! Text keys (B5) through the facade: the text culprit names, per match,
//! the text keys that serve it (never a value fanout), and a workload that
//! mixes text runs with key lookups is refused by `why` and by `fix`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether::types::{Error, Hash, KeySpec, TextOp, Tokenizer};
use bellwether::{CulpritReason, Engine, EngineOptions, FixRequest, TextQuery, WhyRequest};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// Three row groups of (id, body); "needle" only in the second.
fn corpus(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("body", DataType::Utf8, true),
    ]));
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(usize::MAX))
        .build();
    let file = File::create(dir.join("a.parquet")).unwrap();
    let mut w = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
    for rg in 0..3i64 {
        let ids: Vec<i64> = (rg * 10..rg * 10 + 10).collect();
        let bodies: Vec<String> = ids
            .iter()
            .map(|i| {
                if rg == 1 && i % 2 == 0 {
                    format!("row {i} has the Needle and abc")
                } else {
                    format!("row {i} plain text")
                }
            })
            .collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(bodies)),
            ],
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.flush().unwrap();
    }
    w.close().unwrap();
}

fn open(root: &Path) -> Engine {
    Engine::open(root.to_str().unwrap(), EngineOptions::default()).unwrap()
}

fn q(op: TextOp, value: &str) -> TextQuery {
    TextQuery {
        column: "body".into(),
        op,
        value: value.into(),
    }
}

/// A workload of one key lookup on `id` and one `contains` run on `body`.
fn mixed_workload(data: &Path) -> Hash {
    use bellwether::types::Source;
    use bellwether_replay::{KeyInput, RunQuery, WorkloadRecorder};
    use bellwether_store::{Cache, ObjectStoreBackend};

    let index = data.join(".bellwether");
    std::fs::create_dir_all(&index).unwrap();
    let store = Arc::new(ObjectStoreBackend::from_url(index.to_str().unwrap()).unwrap());
    let cache = Cache::new(store, "");
    let root = std::fs::canonicalize(data).unwrap();
    let schema = bellwether_source::ParquetSource::new()
        .schema(root.to_str().unwrap())
        .unwrap();
    let mut rec = WorkloadRecorder::new(&cache, schema);
    let ids = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![3])) as ArrayRef],
    )
    .unwrap();
    rec.record("keys", KeyInput::Batch(&ids)).unwrap();
    let words = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(vec!["needle"])) as ArrayRef],
    )
    .unwrap();
    rec.record_query(
        "text",
        KeyInput::Batch(&words),
        RunQuery::Text(TextOp::Contains),
        None,
    )
    .unwrap();
    rec.finish().unwrap().hash()
}

/// With a complete words key, a `has_token` run is served by it and a
/// `contains` run by nothing: the culprit says exactly that, per match, and
/// carries no value fanout.
#[test]
fn the_text_culprit_names_what_serves_each_match() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data);
    let e = open(&data);
    let words = KeySpec::Text {
        column: "body".into(),
        tokenizer: Tokenizer::Words,
        bloom: None,
    };
    e.index(std::slice::from_ref(&words)).unwrap();
    let r = e
        .why(&WhyRequest {
            text: vec![q(TextOp::Contains, "abcd"), q(TextOp::HasToken, "Abc")],
            ..WhyRequest::default()
        })
        .unwrap();
    let c = r.culprit.unwrap();
    assert_eq!(c.columns, ["body".to_string()]);
    assert_eq!(c.fanout, None, "a text lookup has no value fanout");
    let CulpritReason::Text { matches } = c.reason else {
        panic!("not a text culprit: {:?}", c.reason);
    };
    let got: Vec<(TextOp, u64, Vec<KeySpec>)> = matches
        .into_iter()
        .map(|m| (m.operator, m.runs, m.served_by))
        .collect();
    assert_eq!(
        got,
        [
            (TextOp::Contains, 1, vec![]),
            (TextOp::HasToken, 1, vec![words]),
        ]
    );
}

/// `why` refuses a recorded workload mixing text runs and key lookups, and
/// so does `fix` (its verify compares one kind at a time), naming the
/// workload.
#[test]
fn a_workload_mixing_text_and_key_runs_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data);
    let e = open(&data);
    let priced = e
        .why(&WhyRequest {
            text: vec![q(TextOp::Contains, "needle")],
            ..WhyRequest::default()
        })
        .unwrap();
    let proposal = priced.recommended.expect("the trigram key pays").proposal;
    let mixed = mixed_workload(&data);
    let err = e
        .why(&WhyRequest {
            workload: Some(mixed),
            ..WhyRequest::default()
        })
        .unwrap_err();
    assert!(err.to_string().contains("mixes 1 text runs"), "{err}");
    let err = e
        .fix(&FixRequest {
            proposal: Some(proposal),
            workload: Some(mixed),
            ..FixRequest::default()
        })
        .unwrap_err();
    match err {
        Error::InvalidArgument { field, reason } => {
            assert_eq!(field, "workload");
            assert!(reason.contains("mixes text lookups"), "{reason}");
        }
        other => panic!("expected InvalidArgument, got {other}"),
    }
}

/// On an unindexed folder of Parquet files, a run no key serves is priced
/// by the files' min/max statistics; a text run never is: its value is a
/// substring, not a whole value, so statistics exclude no row group for it
/// (here every body sorts after "needle", which min/max would wrongly
/// exclude everywhere). The current configuration reads every row group.
#[test]
fn an_unserved_text_run_is_never_narrowed_by_min_max_statistics() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data);
    let r = open(&data)
        .why(&WhyRequest {
            text: vec![q(TextOp::Contains, "needle")],
            ..WhyRequest::default()
        })
        .unwrap();
    let current = r.current.unwrap();
    assert_eq!(current.row_groups, r.dataset.row_groups, "{current:?}");
    assert_eq!(current.bytes, r.dataset.bytes);
}
