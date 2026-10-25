//! Invariant 1 through replay, on corpora written in-test (so it runs in CI
//! without the bench corpus).
//!
//! The oracle reads the Parquet files directly and keeps the row groups that
//! hold a queried key; it shares no code with the index or the canonical
//! encoding. Every check is two-sided where the spec is exact: an exact (or
//! temporal) spec on the run's columns, and the per-key intersection of a
//! range and an exact spec, must select exactly the oracle's row groups, so
//! a break that drops row groups (a false negative) and one that adds them
//! (an intersection turned into a union) both fail. Inexact specs (bloom,
//! range, prefix-only intersections) must contain the oracle's row groups,
//! and an intersection must lie inside each of its operands.

// Test values are small counts and generated ids below 2^63, so the casts
// between integer widths are exact here.
#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;

use bellwether_replay::{Baseline, Replayer};
use bellwether_types::KeySpec;
use common::{CorpusSpec, Val};
use parquet::file::properties::ReaderProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::serialized_reader::ReadOptionsBuilder;
use parquet::file::statistics::Statistics;
use proptest::prelude::*;

fn cols(c: &[&str]) -> Vec<String> {
    c.iter().map(|s| (*s).to_string()).collect()
}

fn exact(c: &[&str]) -> KeySpec {
    KeySpec::Exact { columns: cols(c) }
}

/// Checks every spec family against the oracle for entity-id runs; returns
/// how many runs hit the corpus at all.
fn check_entity_runs(spec: &CorpusSpec, runs: usize, seed: u64) -> usize {
    let corpus = common::write_corpus(spec);
    let source = corpus.source();
    let replayer = Replayer::unindexed(
        &source,
        &corpus.data,
        &bellwether_source::Partitioning::None,
    )
    .unwrap();
    let runs = common::entity_runs(&corpus, runs, seed);
    let (keys, workload) = common::record(&["entity_id"], &runs);
    let bloom = KeySpec::Bloom {
        columns: cols(&["entity_id"]),
        fpr: 0.05,
    };
    let range = KeySpec::Range {
        column: "entity_id".into(),
    };
    let prefix = exact(&["entity_id", "batch_id"]);
    let temporal = KeySpec::Temporal {
        columns: cols(&["entity_id"]),
        ts_column: "version_ts".into(),
    };
    let footer = common::footer_bytes(&corpus.files());
    let mut nonempty = 0;
    for ((_, tuples), run) in runs.iter().zip(workload.runs()) {
        let rk = workload.run_keys(&keys, run).unwrap();
        let oracle = common::oracle(&corpus, &["entity_id"], tuples);
        nonempty += usize::from(!oracle.is_empty());
        // Every cost is checked against footers read with the parquet
        // crate, not against the catalog the cost came from.
        let plan = |specs: &[KeySpec]| {
            let p = replayer.plan_of_keys(specs, &rk).unwrap();
            let got = common::cells(&p);
            let cost = replayer.cost_of_keys(specs, &run.run_id, &rk).unwrap();
            assert_eq!(
                (cost.files, cost.row_groups, cost.bytes),
                common::cost_of_cells(&got, &footer),
                "{} under {specs:?}: cost is not the footer sum of its cells",
                run.run_id
            );
            assert_eq!(cost.keys, tuples.len() as u64);
            got
        };
        // Exact paths: equal to the oracle, both ways.
        for specs in [
            vec![exact(&["entity_id"])],
            vec![temporal.clone()],
            vec![prefix.clone()],
            vec![range.clone(), exact(&["entity_id"])],
            vec![bloom.clone(), prefix.clone()],
        ] {
            assert_eq!(plan(&specs), oracle, "{} under {specs:?}", run.run_id);
        }
        // Inexact paths: contain the oracle; intersections lie inside each
        // operand.
        let b = plan(std::slice::from_ref(&bloom));
        let r = plan(std::slice::from_ref(&range));
        let br = plan(&[bloom.clone(), range.clone()]);
        for (what, got) in [("bloom", &b), ("range", &r), ("bloom ∩ range", &br)] {
            let missing: Vec<_> = oracle.difference(got).collect();
            assert!(
                missing.is_empty(),
                "{} under {what} misses {missing:?}",
                run.run_id
            );
        }
        assert!(br.is_subset(&b) && br.is_subset(&r), "{}", run.run_id);
    }
    nonempty
}

#[test]
fn every_spec_family_contains_the_oracle_and_exact_paths_equal_it() {
    let hits = check_entity_runs(&CorpusSpec::default(), 24, 11);
    assert!(
        hits >= 12,
        "only {hits} of 24 runs hit the corpus; the test is vacuous"
    );
}

#[test]
fn composite_runs_intersect_single_column_specs_per_key() {
    let corpus = common::write_corpus(&CorpusSpec::default());
    let source = corpus.source();
    let replayer = Replayer::unindexed(
        &source,
        &corpus.data,
        &bellwether_source::Partitioning::None,
    )
    .unwrap();
    // Runs over (entity_id, batch_id): real pairs and recombined ones, so
    // the per-key intersection matters.
    let pairs: Vec<Vec<Val>> = common::distinct_tuples(&corpus, &["entity_id", "batch_id"])
        .into_iter()
        .collect();
    let mut rng = common::Rng(5);
    let runs: Vec<(String, BTreeSet<Vec<Val>>)> = (0..12)
        .map(|i| {
            let mut t = BTreeSet::new();
            for _ in 0..=(i * 3) {
                let a = &pairs[rng.below(pairs.len() as u64) as usize];
                let b = &pairs[rng.below(pairs.len() as u64) as usize];
                t.insert(vec![a[0].clone(), b[1].clone()]);
            }
            (format!("pair-{i:02}"), t)
        })
        .collect();
    let (keys, workload) = common::record(&["entity_id", "batch_id"], &runs);
    let footer = common::footer_bytes(&corpus.files());
    let mut strictly_smaller = 0;
    for ((_, tuples), run) in runs.iter().zip(workload.runs()) {
        let rk = workload.run_keys(&keys, run).unwrap();
        let oracle = common::oracle(&corpus, &["entity_id", "batch_id"], tuples);
        let plan = |specs: &[KeySpec]| {
            let got = common::cells(&replayer.plan_of_keys(specs, &rk).unwrap());
            let cost = replayer.cost_of_keys(specs, &run.run_id, &rk).unwrap();
            assert_eq!(
                (cost.files, cost.row_groups, cost.bytes),
                common::cost_of_cells(&got, &footer),
                "{} under {specs:?}",
                run.run_id
            );
            got
        };
        assert_eq!(
            plan(&[exact(&["entity_id", "batch_id"])]),
            oracle,
            "{}",
            run.run_id
        );
        assert_eq!(
            plan(&[exact(&["batch_id", "entity_id"])]),
            oracle,
            "projection in the spec's order, {}",
            run.run_id
        );
        // The per-key intersection of two exact single-column specs is
        // exactly the union over tuples t of oracle(a_t) ∩ oracle(b_t),
        // computed here from the files, tuple by tuple.
        let mut per_key = BTreeSet::new();
        for t in tuples {
            let a = common::oracle(
                &corpus,
                &["entity_id"],
                &BTreeSet::from([vec![t[0].clone()]]),
            );
            let b = common::oracle(
                &corpus,
                &["batch_id"],
                &BTreeSet::from([vec![t[1].clone()]]),
            );
            per_key.extend(a.intersection(&b).cloned());
        }
        let e = plan(&[exact(&["entity_id"])]);
        let b = plan(&[exact(&["batch_id"])]);
        let both = plan(&[exact(&["entity_id"]), exact(&["batch_id"])]);
        assert_eq!(both, per_key, "{}: per-key intersection", run.run_id);
        assert!(oracle.is_subset(&both), "{}", run.run_id);
        // Per key, not per run: the intersection of the per-column unions
        // can only be larger, and on recombined pairs it is.
        let per_run: BTreeSet<_> = e.intersection(&b).cloned().collect();
        assert!(both.is_subset(&per_run), "{}", run.run_id);
        strictly_smaller += usize::from(both.len() < per_run.len());
    }
    assert!(
        strictly_smaller > 0,
        "no run distinguishes the per-key from the per-run intersection; the test is vacuous"
    );
}

/// The bench's `parquet_stats` strategy, implemented independently with the
/// `parquet` crate's typed statistics and filters: keep a row group when a
/// key lies within its `entity_id` footer bounds and passes its embedded
/// filter (when it has one).
fn independent_parquet_stats(
    corpus: &common::Corpus,
    keys: &BTreeSet<u64>,
) -> BTreeSet<(String, u32)> {
    let mut out = BTreeSet::new();
    for path in corpus.files() {
        let options = ReadOptionsBuilder::new()
            .with_reader_properties(
                ReaderProperties::builder()
                    .set_read_bloom_filter(true)
                    .build(),
            )
            .build();
        let reader =
            SerializedFileReader::new_with_options(File::open(&path).unwrap(), options).unwrap();
        for rg in 0..reader.num_row_groups() {
            let meta = reader.metadata().row_group(rg);
            let range = match meta.column(0).statistics() {
                Some(Statistics::Int64(s)) => match (s.min_opt(), s.max_opt()) {
                    (Some(lo), Some(hi)) => Some((*lo as u64, *hi as u64)),
                    _ => None,
                },
                _ => None,
            };
            let rgr = reader.get_row_group(rg).unwrap();
            let bloom = rgr.get_column_bloom_filter(0);
            let keep = keys.iter().any(|&k| {
                range.is_none_or(|(lo, hi)| lo <= k && k <= hi)
                    && bloom.is_none_or(|f| f.check(&(k as i64)))
            });
            if keep {
                out.insert((path.to_str().unwrap().to_string(), rg as u32));
            }
        }
    }
    out
}

#[test]
fn parquet_stats_baseline_equals_an_independent_implementation() {
    let corpus = common::write_corpus(&CorpusSpec {
        files: 5,
        bloom_every: 2,
        ..CorpusSpec::default()
    });
    let source = corpus.source();
    let replayer = Replayer::unindexed(
        &source,
        &corpus.data,
        &bellwether_source::Partitioning::None,
    )
    .unwrap();
    let runs = common::entity_runs(&corpus, 16, 3);
    let (keys, workload) = common::record(&["entity_id"], &runs);
    let stats = replayer
        .replay_baseline(Baseline::ParquetStats, &workload, &keys)
        .unwrap();
    let raw = replayer
        .replay_baseline(Baseline::RawScan, &workload, &keys)
        .unwrap();
    let catalog = replayer.catalog();
    let bytes: BTreeMap<(String, u32), u64> = catalog
        .row_groups()
        .iter()
        .map(|rg| {
            (
                (rg.meta.file.path.clone(), rg.meta.index_in_file),
                rg.meta.bytes,
            )
        })
        .collect();
    let mut pruned_somewhere = false;
    for (((_, tuples), s), r) in runs.iter().zip(&stats).zip(&raw) {
        let ks: BTreeSet<u64> = tuples
            .iter()
            .map(|t| match t[0] {
                Val::U(v) => v,
                _ => unreachable!(),
            })
            .collect();
        let want = independent_parquet_stats(&corpus, &ks);
        let want_bytes: u64 = want.iter().map(|c| bytes[c]).sum();
        let want_files: BTreeSet<&String> = want.iter().map(|(f, _)| f).collect();
        assert_eq!(
            (s.files, s.row_groups, s.bytes),
            (want_files.len() as u64, want.len() as u64, want_bytes),
            "{}",
            s.run_id
        );
        assert_eq!(r.row_groups, catalog.len() as u64);
        assert_eq!(r.bytes, catalog.bytes_total());
        pruned_somewhere |= s.row_groups < r.row_groups;
    }
    assert!(
        pruned_somewhere,
        "the statistics baseline never pruned; the test is vacuous"
    );
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 12, .. ProptestConfig::default() })]

    #[test]
    fn invariant_1_holds_on_random_layouts(
        files in 1usize..4,
        row_groups in 1usize..4,
        rows in 1usize..60,
        entities in 1u64..300,
        seed in any::<u64>(),
        bloom_every in 0usize..3,
    ) {
        let _hits = check_entity_runs(
            &CorpusSpec {
                files,
                row_groups_per_file: row_groups,
                rows_per_row_group: rows,
                entities,
                seed,
                bloom_every,
                null_row_groups: Vec::new(),
            },
            8,
            seed ^ 0x5eed,
        );
    }
}
