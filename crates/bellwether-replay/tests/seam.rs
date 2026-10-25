//! The transient seam (adopted from the PR 18 review's probes): the cache
//! key and the checks `with_transient` and `plan` make.
//!
//! - A changed `fpr` or `ts_column` is a miss, never a stale hit.
//! - Two folders that are both manifest version "0" are keyed apart by
//!   catalog digest, and `with_transient` refuses an index built over
//!   another catalog.
//! - The dataset's key types are part of the digest and of the cache key: a
//!   folder read under a schema that declares `k_dec` at scale 4 does not
//!   reuse the index built at scale 3, `with_transient` refuses such an
//!   index, and `plan` refuses keys encoded at another type.

#![allow(clippy::unwrap_used, missing_docs, clippy::too_many_lines)]

mod typed;

use std::sync::Arc;

use arrow::array::{ArrayRef, Decimal128Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether_replay::{
    Catalog, PlanSource, Replayer, RunKeysBuilder, SegmentPlanner, TransientCache, TransientIndex,
    TransientLimits,
};
use bellwether_source::ParquetSource;
use bellwether_types::{Error, KeySpec};

fn cols(c: &[&str]) -> Vec<String> {
    c.iter().map(|s| (*s).to_string()).collect()
}

#[test]
fn a_changed_fpr_or_ts_column_is_a_miss() {
    let corpus = typed::write_corpus(6, 3, 40, 3);
    let schema = typed::dataset_schema();
    let source = ParquetSource::new().with_schema(Arc::clone(&schema));
    let r = Replayer::unindexed(
        &source,
        &corpus.data,
        &bellwether_source::Partitioning::None,
    )
    .unwrap();
    let t1 = KeySpec::Temporal {
        columns: cols(&["k_i64"]),
        ts_column: "ts".into(),
    };
    let t2 = KeySpec::Temporal {
        columns: cols(&["k_i64"]),
        ts_column: "k_u32".into(),
    };
    assert_eq!(t1.name(), t2.name());
    let a = r.transient(&t1).unwrap();
    let b = r.transient(&t2).unwrap();
    assert_ne!(a.spec(), b.spec());
    let b1 = KeySpec::Bloom {
        columns: cols(&["k_i64"]),
        fpr: 0.1,
    };
    let b2 = KeySpec::Bloom {
        columns: cols(&["k_i64"]),
        fpr: 0.2,
    };
    r.transient(&b1).unwrap();
    r.transient(&b2).unwrap();
    let s = r.cache_stats().unwrap();
    assert_eq!((s.builds, s.hits, s.entries), (4, 0, 4));
}

#[test]
fn version_zero_folders_are_keyed_apart_and_cross_catalog_indexes_refused() {
    let a = typed::write_corpus(6, 3, 40, 1);
    let b = typed::write_corpus(6, 3, 40, 2);
    let schema = typed::dataset_schema();
    let source = ParquetSource::new().with_schema(Arc::clone(&schema));
    let ca = Catalog::discover(&source, &a.data, Arc::clone(&schema)).unwrap();
    let cb = Catalog::discover(&source, &b.data, Arc::clone(&schema)).unwrap();
    assert_eq!(ca.manifest_version(), cb.manifest_version());
    assert_ne!(ca.digest(), cb.digest());
    let spec = KeySpec::Exact {
        columns: cols(&["k_i64"]),
    };
    let cache = TransientCache::new(1 << 30);
    let lim = TransientLimits::default();
    let ia = cache
        .get_or_build(&ca, &spec, || {
            TransientIndex::build(&source, &ca, &spec, &lim)
        })
        .unwrap();
    cache
        .get_or_build(&cb, &spec, || {
            TransientIndex::build(&source, &cb, &spec, &lim)
        })
        .unwrap();
    assert_eq!(cache.stats().unwrap().builds, 2);
    assert!(SegmentPlanner::new(Arc::new(cb))
        .with_transient(&[ia])
        .is_err());
}

#[test]
fn key_types_are_part_of_the_cache_key_and_the_seam_refuses_a_mismatch() {
    let corpus = typed::write_corpus(6, 3, 40, 4);
    let s3 = typed::dataset_schema();
    let mut fields: Vec<Field> = s3.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields[2] = Field::new("k_dec", DataType::Decimal128(20, 4), true);
    let s4 = Arc::new(Schema::new(fields));
    let src3 = ParquetSource::new().with_schema(Arc::clone(&s3));
    let src4 = ParquetSource::new().with_schema(Arc::clone(&s4));
    let c3 = Catalog::discover(&src3, &corpus.data, Arc::clone(&s3)).unwrap();
    let c4 = Catalog::discover(&src4, &corpus.data, Arc::clone(&s4)).unwrap();
    assert_ne!(c3.digest(), c4.digest(), "the schema is part of the digest");
    let spec = KeySpec::Exact {
        columns: cols(&["k_dec"]),
    };
    let lim = TransientLimits::default();
    let cache = TransientCache::new(1 << 30);
    let at_scale_3 = cache
        .get_or_build(&c3, &spec, || {
            TransientIndex::build(&src3, &c3, &spec, &lim)
        })
        .unwrap();
    let at_scale_4 = cache
        .get_or_build(&c4, &spec, || {
            TransientIndex::build(&src4, &c4, &spec, &lim)
        })
        .unwrap();
    let st = cache.stats().unwrap();
    assert_eq!((st.builds, st.hits), (2, 0), "no stale hit across schemas");

    // Keys at scale 4 for values present in the corpus (1.500, 1.501).
    let batch = RecordBatch::try_from_iter(vec![(
        "k_dec",
        Arc::new(
            Decimal128Array::from(vec![15_000i128, 15_010])
                .with_precision_and_scale(20, 4)
                .unwrap(),
        ) as ArrayRef,
    )])
    .unwrap();
    let mut b = RunKeysBuilder::new(&cols(&["k_dec"]), &s4).unwrap();
    b.push_batch(&batch).unwrap();
    let rk4 = b.finish();
    let c4 = Arc::new(c4);
    // The scale-3 index is refused by a scale-4 planner (its catalog digest
    // differs, and its segments record scale 3).
    assert!(SegmentPlanner::new(Arc::clone(&c4))
        .with_transient(&[Arc::clone(&at_scale_3)])
        .is_err());
    // The honest scale-4 index plans the keys and finds them.
    let planner = SegmentPlanner::new(Arc::clone(&c4));
    let honest = planner
        .with_transient(&[at_scale_4])
        .unwrap()
        .plan(std::slice::from_ref(&spec), &rk4, None)
        .unwrap();
    assert!(honest.row_groups_selected > 0);
    // Keys encoded at scale 4 are refused by a scale-3 planner.
    let c3 = Arc::new(c3);
    let p3 = SegmentPlanner::new(Arc::clone(&c3));
    let err = p3
        .with_transient(&[at_scale_3])
        .unwrap()
        .plan(std::slice::from_ref(&spec), &rk4, None)
        .unwrap_err();
    assert!(
        matches!(&err, Error::KeySpec(m) if m.contains("dataset declares")),
        "{err}"
    );
}

/// A bloom spec's `fpr` survives the JSON round trip a segment header makes
/// (`serde_json`'s `float_roundtrip`, contract v2.1), so a transient bloom
/// index at any rate opens as the spec it was built for: `SpecSegments` and
/// the build crate compare the header's spec with `==`.
#[test]
fn bloom_fpr_survives_the_segment_header_round_trip() {
    let mut rates: Vec<f64> = (2..2000u32).map(|n| 1.0 / f64::from(n)).collect();
    rates.extend((1..1000u32).map(|k| f64::from(k) / 1000.0));
    let mut x = 0.5f64;
    for _ in 0..200 {
        x *= 0.9;
        rates.push(x);
    }
    for &fpr in &rates {
        let s = KeySpec::Bloom {
            columns: cols(&["a"]),
            fpr,
        };
        let back: KeySpec = serde_json::from_str(&serde_json::to_string(&s).unwrap()).unwrap();
        assert_eq!(back, s, "fpr {fpr:e} does not survive the round trip");
    }
    let corpus = typed::write_corpus(6, 3, 40, 3);
    let source = ParquetSource::new().with_schema(typed::dataset_schema());
    let r = Replayer::unindexed(
        &source,
        &corpus.data,
        &bellwether_source::Partitioning::None,
    )
    .unwrap();
    for fpr in [1.0 / 3.0, 0.07, 0.9f64.powi(37)] {
        let spec = KeySpec::Bloom {
            columns: cols(&["k_i64"]),
            fpr,
        };
        assert_eq!(r.transient(&spec).unwrap().spec(), &spec);
    }
}
