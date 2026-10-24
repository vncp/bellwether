//! Shared fixtures for the integration tests: random row-group layouts with
//! a composite key `(id Int64, tag Utf8)` and a millisecond timestamp, any of
//! which may be null.
#![allow(dead_code, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use bellwether_core::encoding::{encode_tuple, Value};
use bellwether_core::{EncodedSegment, SegmentReader, SegmentWriter, WriterOptions};
use bellwether_types::{KeySpec, RowGroupId};
use proptest::prelude::*;
use roaring::RoaringBitmap;

pub const TAGS: [&str; 5] = ["a", "b", "\0", "", "a\0"];

#[derive(Clone, Debug)]
pub struct Row {
    pub id: Option<i64>,
    pub tag: Option<String>,
    /// Milliseconds, the dataset column's unit.
    pub ts: Option<i64>,
}

/// Row groups, indexed by global `RowGroupId`.
pub type Layout = Vec<Vec<Row>>;

pub fn row() -> impl Strategy<Value = Row> {
    (
        prop::option::weighted(0.9, 0i64..16),
        prop::option::weighted(
            0.9,
            prop::sample::select(TAGS.to_vec()).prop_map(String::from),
        ),
        prop::option::weighted(0.85, 0i64..40),
    )
        .prop_map(|(id, tag, ts)| Row { id, tag, ts })
}

pub fn layout() -> impl Strategy<Value = Layout> {
    prop::collection::vec(prop::collection::vec(row(), 0..24), 1..20)
}

/// The dataset schema every fixture batch is written under.
pub fn schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("tag", DataType::Utf8, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
    ])
}

pub fn key(id: i64, tag: &str) -> Vec<u8> {
    encode_tuple(&[Value::Int(id), Value::Utf8(tag)]).unwrap()
}

pub fn tag_key(tag: &str, id: i64) -> Vec<u8> {
    encode_tuple(&[Value::Utf8(tag), Value::Int(id)]).unwrap()
}

pub fn id_key(id: i64) -> Vec<u8> {
    encode_tuple(&[Value::Int(id)]).unwrap()
}

pub fn tag_prefix(tag: &str) -> Vec<u8> {
    encode_tuple(&[Value::Utf8(tag)]).unwrap()
}

pub fn batch(rows: &[Row]) -> RecordBatch {
    let ids: ArrayRef = Arc::new(rows.iter().map(|r| r.id).collect::<Int64Array>());
    let tags: ArrayRef = Arc::new(
        rows.iter()
            .map(|r| r.tag.as_deref())
            .collect::<StringArray>(),
    );
    let ts: ArrayRef = Arc::new(
        rows.iter()
            .map(|r| r.ts)
            .collect::<TimestampMillisecondArray>()
            .with_timezone("UTC"),
    );
    RecordBatch::try_from_iter(vec![("id", ids), ("tag", tags), ("ts", ts)]).unwrap()
}

pub fn exact() -> KeySpec {
    KeySpec::Exact {
        columns: vec!["id".into(), "tag".into()],
    }
}

/// A string-leading composite key, so string delimiting is load-bearing.
pub fn exact_tag_first() -> KeySpec {
    KeySpec::Exact {
        columns: vec!["tag".into(), "id".into()],
    }
}

pub fn bloom(fpr: f64) -> KeySpec {
    KeySpec::Bloom {
        columns: vec!["id".into(), "tag".into()],
        fpr,
    }
}

pub fn range() -> KeySpec {
    KeySpec::Range {
        column: "id".into(),
    }
}

pub fn temporal() -> KeySpec {
    KeySpec::Temporal {
        columns: vec!["id".into(), "tag".into()],
        ts_column: "ts".into(),
    }
}

/// Builds one segment per contiguous chunk of row groups, splitting at
/// `cuts` (row-group indexes).
pub fn build(
    spec: &KeySpec,
    layout: &Layout,
    cuts: &[usize],
    opts: &WriterOptions,
) -> Vec<EncodedSegment> {
    let mut bounds: BTreeSet<usize> = cuts.iter().map(|c| c % layout.len()).collect();
    bounds.insert(0);
    bounds.insert(layout.len());
    let bounds: Vec<usize> = bounds.into_iter().collect();
    let schema = schema();
    bounds
        .windows(2)
        .map(|w| {
            let mut wr = SegmentWriter::for_dataset(spec.clone(), &schema, opts.clone()).unwrap();
            for (rg, rows) in layout.iter().enumerate().take(w[1]).skip(w[0]) {
                wr.add_batch(RowGroupId(u32::try_from(rg).unwrap()), &batch(rows))
                    .unwrap();
            }
            wr.finish().unwrap()
        })
        .collect()
}

pub fn open(segs: &[EncodedSegment]) -> Vec<SegmentReader<Vec<u8>>> {
    segs.iter()
        .map(|s| SegmentReader::open(s.bytes.clone()).unwrap())
        .collect()
}

/// Row groups holding a row that satisfies `f`.
pub fn truth(layout: &Layout, f: impl Fn(&Row) -> bool) -> RoaringBitmap {
    layout
        .iter()
        .enumerate()
        .filter(|(_, rows)| rows.iter().any(&f))
        .map(|(i, _)| u32::try_from(i).unwrap())
        .collect()
}

pub fn has_key(r: &Row, id: i64, tag: &str) -> bool {
    r.id == Some(id) && r.tag.as_deref() == Some(tag)
}

/// The true newest version of a key with timestamp `<= bound` (both in
/// milliseconds, the column's unit), and the row groups holding it.
pub fn true_version(
    layout: &Layout,
    id: i64,
    tag: &str,
    bound: Option<i64>,
) -> Option<(i64, RoaringBitmap)> {
    let max = layout
        .iter()
        .flatten()
        .filter(|r| has_key(r, id, tag))
        .filter_map(|r| r.ts)
        .filter(|t| bound.is_none_or(|b| *t <= b))
        .max()?;
    Some((
        max,
        truth(layout, |r| has_key(r, id, tag) && r.ts == Some(max)),
    ))
}
