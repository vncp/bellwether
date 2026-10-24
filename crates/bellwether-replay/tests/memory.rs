//! The stated memory bounds of transient builds and of the transient cache
//! (ADR 018), tested rather than assumed.
//!
//! - A transient build reads one row group at a time, projected to the
//!   spec's key columns (and timestamp column), in id order.
//! - An open segment writer takes at most `max_segment_rows` rows and
//!   `max_segment_key_bytes` accounted key bytes (or one row group when a row
//!   group alone is larger); splitting the index into more segments never
//!   changes a cost.
//! - Finished segments plus the open writer's accounted key bytes are capped
//!   by `max_index_bytes`, checked before each row group is added; exceeding
//!   it is an error, never a partial index.
//! - The layout model's map of distinct cluster keys is capped by
//!   `max_cluster_key_bytes`; exceeding it is a typed error.
//! - The cache holds at most its byte cap, evicts the least recently used,
//!   keys on the full spec (two bloom specs that share a name are two
//!   entries), and returns an index larger than the cap without keeping it.

#![allow(clippy::unwrap_used, missing_docs)]

mod common;

use std::fs::File;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bellwether_replay::{
    KeyInput, MemoryKeySets, ReplayError, Replayer, TransientLimits, WorkloadRecorder,
    CLUSTER_KEY_OVERHEAD,
};
use bellwether_source::ParquetSource;
use bellwether_types::{Hash, KeySpec, ProposalBody};
use common::CorpusSpec;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

fn cols(c: &[&str]) -> Vec<String> {
    c.iter().map(|s| (*s).to_string()).collect()
}

const WIDE_RGS: usize = 4;
const WIDE_ROWS: usize = 500;
const WIDE_LEN: usize = 200;
/// Accounted bytes of one wide row group: distinct keys, each `06`, 200
/// bytes, and the `00 00` terminator.
const WIDE_RG_BYTES: u64 = (WIDE_ROWS * (1 + WIDE_LEN + 2)) as u64;

fn wide_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "k_wide",
        DataType::Utf8,
        false,
    )]))
}

/// One file of four row groups of 500 distinct 200-byte strings.
fn wide_corpus() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(WIDE_ROWS))
        .build();
    let mut w = ArrowWriter::try_new(
        File::create(data.join("part-0.parquet")).unwrap(),
        wide_schema(),
        Some(props),
    )
    .unwrap();
    for g in 0..WIDE_RGS {
        let vals: Vec<String> = (0..WIDE_ROWS)
            .map(|r| format!("{:0>width$}", g * WIDE_ROWS + r, width = WIDE_LEN))
            .collect();
        w.write(
            &RecordBatch::try_new(
                wide_schema(),
                vec![Arc::new(StringArray::from(vals)) as ArrayRef],
            )
            .unwrap(),
        )
        .unwrap();
    }
    w.close().unwrap();
    let data = data.to_str().unwrap().to_string();
    (dir, data)
}

#[test]
fn wide_keys_cut_segments_by_key_bytes_and_the_open_writer_counts_toward_the_cap() {
    let (_dir, data) = wide_corpus();
    let source = ParquetSource::new().with_schema(wide_schema());
    let spec = KeySpec::Exact {
        columns: cols(&["k_wide"]),
    };
    // Rows never reach their cap; key bytes cut after every row group.
    let by_bytes = Replayer::unindexed(&source, &data)
        .unwrap()
        .with_limits(TransientLimits {
            max_segment_key_bytes: WIDE_RG_BYTES + WIDE_RG_BYTES / 2,
            ..TransientLimits::default()
        });
    let idx = by_bytes.transient(&spec).unwrap();
    assert_eq!(idx.stats().segments, WIDE_RGS as u64);
    assert_eq!(idx.stats().largest_segment_key_bytes, WIDE_RG_BYTES);
    let one = Replayer::unindexed(&source, &data).unwrap();
    assert_eq!(one.transient(&spec).unwrap().stats().segments, 1);
    assert_eq!(
        one.transient(&spec)
            .unwrap()
            .stats()
            .largest_segment_key_bytes,
        WIDE_RG_BYTES * WIDE_RGS as u64
    );
    // An index cap smaller than two row groups' keys stops the build before
    // the open writer takes the second, not at the first finished segment.
    let capped = Replayer::unindexed(&source, &data)
        .unwrap()
        .with_limits(TransientLimits {
            max_index_bytes: WIDE_RG_BYTES + WIDE_RG_BYTES / 2,
            ..TransientLimits::default()
        });
    let err = capped.transient(&spec).unwrap_err();
    assert!(err.to_string().contains("open writer"), "{err}");
}

#[test]
fn the_layout_models_cluster_key_map_is_capped_with_a_typed_error() {
    let (_dir, data) = wide_corpus();
    let source = ParquetSource::new().with_schema(wide_schema());
    let cap = 100_000;
    let replayer = Replayer::unindexed(&source, &data)
        .unwrap()
        .with_limits(TransientLimits {
            max_cluster_key_bytes: cap,
            ..TransientLimits::default()
        });
    let keys = MemoryKeySets::new();
    let mut rec = WorkloadRecorder::new(&keys, wide_schema());
    rec.record(
        "r",
        KeyInput::Batch(
            &RecordBatch::try_new(
                wide_schema(),
                vec![Arc::new(StringArray::from(vec![format!("{:0>200}", 7)])) as ArrayRef],
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let workload = rec.finish().unwrap();
    let err = replayer
        .score(
            &Hash::of(b"layout"),
            &ProposalBody::Layout {
                cluster_by: cols(&["k_wide"]),
            },
            &workload,
            &keys,
            "2026-10-24T00:00:00Z",
        )
        .unwrap_err();
    let per_key = (1 + WIDE_LEN as u64 + 2) + CLUSTER_KEY_OVERHEAD;
    match err {
        ReplayError::ClusterKeysExceeded {
            keys: n,
            bytes,
            cap: c,
        } => {
            assert_eq!(c, cap);
            assert_eq!(n, cap / per_key + 1, "stops at the first key past the cap");
            assert!(bytes > cap && bytes - per_key <= cap);
        }
        other => panic!("expected ClusterKeysExceeded, got {other}"),
    }
    // Under the default cap the same layout is costed.
    let ok = Replayer::unindexed(&source, &data)
        .unwrap()
        .score(
            &Hash::of(b"layout"),
            &ProposalBody::Layout {
                cluster_by: cols(&["k_wide"]),
            },
            &workload,
            &keys,
            "2026-10-24T00:00:00Z",
        )
        .unwrap();
    assert_eq!(ok.replay.proposed[0].row_groups, 1);
}

#[test]
fn segment_caps_bound_the_open_writer_and_never_change_a_cost() {
    let corpus = common::write_corpus(&CorpusSpec::default());
    let source = common::Counting::new(corpus.source());
    let spec = KeySpec::Exact {
        columns: cols(&["entity_id", "batch_id"]),
    };
    let runs = common::entity_runs(&corpus, 10, 2);
    let (keys, workload) = common::record(&["entity_id"], &runs);
    let one = Replayer::unindexed(&source, &corpus.data).unwrap();
    let rows_per_rg = corpus.spec.rows_per_row_group as u64;
    let many = Replayer::unindexed(&source, &corpus.data)
        .unwrap()
        .with_limits(TransientLimits {
            max_segment_rows: rows_per_rg + 1,
            ..TransientLimits::default()
        });
    source.clear();
    let a = one
        .replay_specs(std::slice::from_ref(&spec), &workload, &keys)
        .unwrap();
    let reads = source.reads();
    assert_eq!(reads.len(), one.catalog().len());
    assert!(reads
        .iter()
        .all(|(_, _, c)| c == &cols(&["entity_id", "batch_id"])));
    let b = many
        .replay_specs(std::slice::from_ref(&spec), &workload, &keys)
        .unwrap();
    assert_eq!(a, b, "more segments, same costs");
    let small = many.transient(&spec).unwrap();
    let stats = small.stats();
    assert_eq!(
        stats.segments,
        one.catalog().len() as u64,
        "one row group per segment"
    );
    assert!(stats.largest_segment_rows <= rows_per_rg + 1);
    let big = one.transient(&spec).unwrap();
    assert_eq!(big.stats().segments, 1);
    assert_eq!(big.stats().rows_read, one.catalog().rows_total());
}

#[test]
fn exceeding_the_index_byte_cap_is_an_error_not_a_partial_index() {
    let corpus = common::write_corpus(&CorpusSpec::default());
    let source = corpus.source();
    let replayer = Replayer::unindexed(&source, &corpus.data)
        .unwrap()
        .with_limits(TransientLimits {
            max_segment_rows: 100,
            max_index_bytes: 4096,
            ..TransientLimits::default()
        });
    let err = replayer
        .transient(&KeySpec::Exact {
            columns: cols(&["entity_id"]),
        })
        .unwrap_err();
    assert!(err.to_string().contains("byte cap"), "{err}");
}

#[test]
fn the_cache_is_bounded_by_bytes_and_keyed_by_the_full_spec() {
    let corpus = common::write_corpus(&CorpusSpec::default());
    let source = common::Counting::new(corpus.source());
    let a = KeySpec::Bloom {
        columns: cols(&["entity_id"]),
        fpr: 0.1,
    };
    let b = KeySpec::Bloom {
        columns: cols(&["entity_id"]),
        fpr: 0.01,
    };
    assert_eq!(a.name(), b.name());
    let sizer = Replayer::unindexed(&source, &corpus.data).unwrap();
    let size_a = sizer.transient(&a).unwrap().bytes();
    let size_b = sizer.transient(&b).unwrap().bytes();
    assert_ne!(size_a, size_b, "two specs of one name, two indexes");
    assert_eq!(sizer.cache_stats().unwrap().entries, 2);

    // A cap that holds the larger index but not both.
    let cap = size_a.max(size_b) + 1;
    let r = Replayer::unindexed(&source, &corpus.data)
        .unwrap()
        .with_cache_bytes(cap);
    r.transient(&a).unwrap();
    r.transient(&b).unwrap();
    let s = r.cache_stats().unwrap();
    assert_eq!((s.builds, s.evictions, s.entries), (2, 1, 1));
    assert!(s.bytes <= cap);
    source.clear();
    r.transient(&b).unwrap();
    assert!(source.reads().is_empty(), "the most recent index is a hit");
    r.transient(&a).unwrap();
    assert_eq!(
        source.reads().len(),
        r.catalog().len(),
        "the evicted index is rebuilt"
    );

    // An index larger than the whole cap is returned and not kept.
    let tiny = Replayer::unindexed(&source, &corpus.data)
        .unwrap()
        .with_cache_bytes(16);
    let idx = tiny.transient(&a).unwrap();
    assert!(idx.bytes() > 16);
    let s = tiny.cache_stats().unwrap();
    assert_eq!((s.entries, s.bytes), (0, 0));
}
