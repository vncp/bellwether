//! End-to-end tests: generator determinism and golden hashes, workload hash
//! stability, ground truth against a brute-force count, planted flags that
//! follow the layout, and a harness plus report smoke run. Everything is
//! written under `CARGO_TARGET_TMPDIR`.

#![allow(
    clippy::unwrap_used,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::many_single_char_names,
    clippy::too_many_lines,
    clippy::doc_markdown
)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use arrow::array::{
    Array, Decimal128Array, Int64Array, StringArray, TimestampMicrosecondArray, UInt64Array,
};
use bellwether_bench::corpus::{day_start_us, Corpus, GenParams};
use bellwether_bench::generate::{generate, memory_bound_bytes};
use bellwether_bench::harness::{run_all, DuckDbMode, RunOptions, ScanFault};
use bellwether_bench::report::render;
use bellwether_bench::slo::{evaluate, Outcome, SloReport};
use bellwether_bench::stats::quantile_sorted;
use bellwether_bench::truth::{
    compute, write_ground_truth, Expected, RULE_BATCH_STAMP, RULE_DECLARED_UNUSED, RULE_LOOKBACK,
    RULE_LOW_SELECTIVITY, RULE_SCATTERED_TIME,
};
use bellwether_bench::workload::{generate_workload, hash_keys, read_keys, WorkloadParams};
use bellwether_types::Fanout;

/// Every check passed except RSS growth, which measures the whole process:
/// the other tests in this binary run in parallel in the same process, so
/// that check is exercised alone in `tests/streaming_guard.rs`.
fn passed_except_rss(report: &SloReport) -> bool {
    report
        .checks
        .iter()
        .filter(|c| c.name != "generator_rss_growth")
        .all(|c| c.outcome != Outcome::Fail)
}

fn tmp(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("bellwether-bench")
        .join(name);
    if dir.exists() {
        std::fs::remove_dir_all(&dir).unwrap();
    }
    dir
}

fn tiny(seed: u64) -> GenParams {
    GenParams {
        seed,
        files: 8,
        row_groups_per_file: 3,
        rows_per_row_group: 250,
        entities: 1_500,
        churn: 0.5,
        days: 2,
        payload_bytes: 32,
        late_fraction: 0.05,
    }
}

/// A scaled-down recipe layout: 8 files per day, one version per entity per
/// day, late rows. Every non-deferred pattern must be planted.
fn recipe_like(seed: u64) -> GenParams {
    GenParams {
        seed,
        files: 24,
        row_groups_per_file: 2,
        rows_per_row_group: 250,
        entities: 8_000,
        churn: 0.5,
        days: 3,
        payload_bytes: 32,
        late_fraction: 0.05,
    }
}

/// Every file under `root`, relative path to bytes, skipping measurements.
fn tree_bytes(root: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut out = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path.file_name().unwrap() != "generator-stats.json" {
                let rel = path.strip_prefix(root).unwrap().display().to_string();
                out.insert(rel, std::fs::read(&path).unwrap());
            }
        }
    }
    out
}

#[test]
fn generator_is_deterministic() {
    let (a, b, c) = (tmp("det-a"), tmp("det-b"), tmp("det-c"));
    generate(&tiny(9), &a).unwrap();
    generate(&tiny(9), &b).unwrap();
    generate(&tiny(10), &c).unwrap();
    let (ta, tb, tc) = (tree_bytes(&a), tree_bytes(&b), tree_bytes(&c));
    assert_eq!(ta.len(), 9, "eight data files plus corpus.json");
    assert_eq!(ta, tb, "same seed must produce identical bytes");
    assert_ne!(ta, tc, "a different seed must change the data");
    // Regenerating into an existing corpus replaces it byte for byte.
    generate(&tiny(9), &c).unwrap();
    assert_eq!(tree_bytes(&c), ta);
}

/// BLAKE3 over every decoded value of a corpus, in manifest order: entity,
/// batch, version_ts, amount as big-endian integers; status and payload as
/// length-prefixed bytes. Independent of Parquet encoding.
fn decoded_hash(corpus: &Corpus) -> String {
    let mut h = blake3::Hasher::new();
    for file in &corpus.files {
        for rg in 0..file.row_groups.len() {
            let (batches, _) = file.reader.read(rg, None).unwrap();
            for b in batches {
                let e = b.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
                let s = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                let t = b
                    .column(2)
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                let st = b.column(3).as_any().downcast_ref::<StringArray>().unwrap();
                let a = b
                    .column(4)
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap();
                let p = b.column(5).as_any().downcast_ref::<StringArray>().unwrap();
                for i in 0..b.num_rows() {
                    h.update(&e.value(i).to_be_bytes());
                    h.update(&s.value(i).to_be_bytes());
                    h.update(&t.value(i).to_be_bytes());
                    for v in [st.value(i), p.value(i)] {
                        h.update(&(v.len() as u64).to_be_bytes());
                        h.update(v.as_bytes());
                    }
                    h.update(&a.value(i).to_be_bytes());
                }
            }
        }
    }
    h.finalize().to_hex().to_string()
}

/// `tiny(9)`'s `corpus.json` (parameters and layout).
const GOLDEN_TINY_CORPUS_ID: &str =
    "994cd75fc80e0c0b3bb2d85145898fcc36c01ad2d678b3a30637de5d9533f3d8";
/// Every decoded value of `tiny(9)`: changes only if the generated data
/// changes (RNG, row model, schema).
const GOLDEN_TINY_DECODED: &str =
    "1ec62858abfb73755f90d908f6ccf2b0c6a22e61bc2beffe639fbda862871cfc";
/// The Parquet bytes of `tiny(9)`'s data files, in manifest order. Also
/// changes when the Parquet encoder changes; if only this one moves after a
/// `parquet` upgrade, re-pin it and say so in the PR.
const GOLDEN_TINY_FILES: &str = "be2ce95792645a7546fa11bb8559f40eb6a9554ac47a66938d425f58ec1bcdb2";
/// The workload hash of `golden_workload()` on `tiny(9)`.
const GOLDEN_TINY_WORKLOAD: &str =
    "650ba5c93b3a99279f5b67b6321d9326cdf3d5ea064e35a8cd7ed570161ad990";
/// The keys of that workload's first run, decoded from its Parquet file.
const GOLDEN_TINY_FIRST_RUN_KEYS: &str =
    "de49c180a08a992ebebe54fd9aebca3cd3701128a5655e968620a4a8a5f472f6";

fn golden_workload() -> WorkloadParams {
    WorkloadParams {
        seed: 5,
        runs: 20,
        p50: 10,
        p95: 300,
        max: 700,
    }
}

#[test]
fn goldens_pin_generator_and_workload() {
    let dir = tmp("golden");
    generate(&tiny(9), &dir).unwrap();
    let corpus = Corpus::open(&dir).unwrap();
    let mut files = blake3::Hasher::new();
    for f in &corpus.files {
        files.update(&std::fs::read(&f.path).unwrap());
    }
    let workload = generate_workload(&dir, &golden_workload()).unwrap();
    let first_keys = read_keys(&dir.join(&workload.runs[0].file)).unwrap();
    let got = [
        ("corpus id", corpus.id.to_hex(), GOLDEN_TINY_CORPUS_ID),
        ("decoded", decoded_hash(&corpus), GOLDEN_TINY_DECODED),
        (
            "files",
            files.finalize().to_hex().to_string(),
            GOLDEN_TINY_FILES,
        ),
        ("workload", workload.hash.to_hex(), GOLDEN_TINY_WORKLOAD),
        (
            "first run keys",
            hash_keys(&first_keys).to_hex(),
            GOLDEN_TINY_FIRST_RUN_KEYS,
        ),
    ];
    let wrong: Vec<String> = got
        .iter()
        .filter(|(_, have, want)| have != want)
        .map(|(name, have, want)| format!("{name}: got {have}, pinned {want}"))
        .collect();
    assert!(
        wrong.is_empty(),
        "golden hashes moved:\n{}",
        wrong.join("\n")
    );
}

#[test]
fn generator_layout_and_buffered_bound() {
    let small = GenParams {
        files: 2,
        days: 1,
        ..tiny(1)
    };
    let large = GenParams {
        files: 16,
        days: 4,
        ..tiny(1)
    };
    let s = generate(&small, &tmp("stream-small")).unwrap();
    let dir = tmp("stream-large");
    let l = generate(&large, &dir).unwrap();
    assert!(s.peak_buffered_bytes <= memory_bound_bytes(&small));
    assert!(l.peak_buffered_bytes <= memory_bound_bytes(&large));
    // Eight times the data must not hold more than one row group's worth more.
    assert!(
        (l.peak_buffered_bytes as f64) < 1.5 * s.peak_buffered_bytes as f64,
        "peak grew with corpus size: {} vs {}",
        l.peak_buffered_bytes,
        s.peak_buffered_bytes
    );
    let corpus = Corpus::open(&dir).unwrap();
    assert_eq!(corpus.files.len(), 16);
    assert!(corpus.files.iter().all(|f| f.row_groups.len() == 3));
    assert!(corpus.files[0]
        .fingerprint
        .path
        .starts_with("data/day=2026-01-01/"));
    assert!(corpus.files[15]
        .fingerprint
        .path
        .starts_with("data/day=2026-01-04/"));
}

/// BLAKE3 of the keys 1, 2, 3 as 8-byte big-endian values.
const GOLDEN_KEYS_1_2_3: &str = "9db0159695bf1dfc95fba48b80b288934d39e768a49820811337b388d89cef4b";

#[test]
fn workload_hash_is_stable() {
    // The key-set hash is BLAKE3 over 8-byte big-endian keys, pinned to a
    // literal so the definition cannot drift across commits.
    assert_eq!(
        hash_keys(&[1, 2, 3]).to_hex(),
        blake3::hash(&[1u64, 2, 3].map(u64::to_be_bytes).concat())
            .to_hex()
            .to_string()
    );
    assert_eq!(hash_keys(&[1, 2, 3]).to_hex(), GOLDEN_KEYS_1_2_3);

    let dir = tmp("workload");
    generate(&tiny(3), &dir).unwrap();
    let params = golden_workload();
    let a = generate_workload(&dir, &params).unwrap();
    let a_json = std::fs::read(dir.join("workload.json")).unwrap();
    let b = generate_workload(&dir, &params).unwrap();
    let b_json = std::fs::read(dir.join("workload.json")).unwrap();
    assert_eq!(a, b);
    assert_eq!(a_json, b_json);
    assert_eq!(a.sizes_p50_p95_max, [10, 300, 700]);
    assert!(!a.clamped);

    // Hashes recompute from the key files.
    let mut sizes = Vec::new();
    for run in &a.runs {
        let keys = read_keys(&dir.join(&run.file)).unwrap();
        assert_eq!(keys.len() as u64, run.keys);
        assert_eq!(hash_keys(&keys), run.hash);
        sizes.push(run.keys);
    }
    sizes.sort_unstable();
    assert_eq!(quantile_sorted(&sizes, 0.5), Some(10));
    assert_eq!(quantile_sorted(&sizes, 0.95), Some(300));
    assert_eq!(sizes.last().copied(), Some(700));

    let other = generate_workload(&dir, &WorkloadParams { seed: 6, ..params }).unwrap();
    assert_ne!(other.hash, a.hash);
}

/// `value -> (row groups, files, rows)` the slow way.
#[derive(Default)]
struct Brute {
    rgs: BTreeSet<(usize, usize)>,
    files: BTreeSet<usize>,
    rows: u64,
}

fn brute_fanout(map: &BTreeMap<Vec<String>, Brute>) -> ([f64; 3], [f64; 3], u64) {
    let mut rgs: Vec<u64> = map.values().map(|b| b.rgs.len() as u64).collect();
    let mut files: Vec<u64> = map.values().map(|b| b.files.len() as u64).collect();
    rgs.sort_unstable();
    files.sort_unstable();
    let q = |v: &[u64]| {
        [
            quantile_sorted(v, 0.5).unwrap() as f64,
            quantile_sorted(v, 0.95).unwrap() as f64,
            *v.last().unwrap() as f64,
        ]
    };
    (q(&files), q(&rgs), map.len() as u64)
}

fn assert_fanout(f: &Fanout, brute: &BTreeMap<Vec<String>, Brute>) {
    let (files, rgs, distinct) = brute_fanout(brute);
    assert_eq!(f.distinct, distinct, "{:?}", f.columns);
    assert_eq!(f.files_per_value, files, "{:?}", f.columns);
    assert_eq!(f.row_groups_per_value, rgs, "{:?}", f.columns);
    assert!(!f.sampled);
}

fn fanout_of<'a>(expected: &'a Expected, rule: &str) -> &'a Fanout {
    expected.get(rule).unwrap().fanout.as_ref().unwrap()
}

fn fix_fanout_of<'a>(expected: &'a Expected, rule: &str) -> &'a Fanout {
    expected.get(rule).unwrap().fix_fanout.as_ref().unwrap()
}

fn check_ground_truth(params: &GenParams, name: &str) -> Expected {
    let dir = tmp(name);
    generate(params, &dir).unwrap();
    let corpus = Corpus::open(&dir).unwrap();
    let expected = compute(&corpus).unwrap();

    let mut maps: [BTreeMap<Vec<String>, Brute>; 5] = Default::default();
    // (file, lo, hi) of version_ts per row group, from the rows themselves.
    let mut ranges: Vec<(usize, i64, i64)> = Vec::new();
    let mut late = 0u64;
    let mut rows = 0u64;
    for (fi, file) in corpus.files.iter().enumerate() {
        let day = corpus
            .manifest
            .days
            .iter()
            .find(|d| (d.first_file..d.first_file + d.files).contains(&(fi as u32)))
            .unwrap();
        for ri in 0..file.row_groups.len() {
            let (mut lo, mut hi) = (i64::MAX, i64::MIN);
            let (batches, _) = file.reader.read(ri, None).unwrap();
            for b in batches {
                let e = b.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
                let s = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                let t = b
                    .column(2)
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                let st = b.column(3).as_any().downcast_ref::<StringArray>().unwrap();
                for i in 0..b.num_rows() {
                    let (ev, sv, tv) = (e.value(i).to_string(), s.value(i).to_string(), t.value(i));
                    let stv = st.value(i).to_string();
                    let keys = [
                        vec![ev.clone()],
                        vec![sv.clone()],
                        vec![ev.clone(), sv],
                        vec![stv.clone()],
                        vec![stv, ev],
                    ];
                    for (map, key) in maps.iter_mut().zip(keys) {
                        let v = map.entry(key).or_default();
                        v.rgs.insert((fi, ri));
                        v.files.insert(fi);
                        v.rows += 1;
                    }
                    late += u64::from(tv < day_start_us(day.day));
                    lo = lo.min(tv);
                    hi = hi.max(tv);
                    rows += 1;
                }
            }
            ranges.push((fi, lo, hi));
        }
    }
    let [entity, batch, composite, status, status_entity] = maps;

    assert_eq!(expected.rows_total, rows);
    assert_fanout(fanout_of(&expected, RULE_BATCH_STAMP), &batch);
    assert_fanout(fix_fanout_of(&expected, RULE_BATCH_STAMP), &composite);
    assert_fanout(fanout_of(&expected, RULE_LOW_SELECTIVITY), &status);
    assert_fanout(
        fix_fanout_of(&expected, RULE_LOW_SELECTIVITY),
        &status_entity,
    );
    assert_fanout(fanout_of(&expected, RULE_LOOKBACK), &entity);
    let v = expected
        .get(RULE_LOOKBACK)
        .unwrap()
        .versions
        .as_ref()
        .unwrap();
    let equal = entity
        .values()
        .filter(|b| b.rgs.len() as u64 == b.rows)
        .count() as u64;
    assert_eq!(v.entities, entity.len() as u64);
    assert_eq!(v.entities_fanout_equals_versions, equal);
    let mean = entity.values().map(|b| b.rows).sum::<u64>() as f64 / entity.len() as f64;
    assert!((v.mean_versions_per_entity - mean).abs() < 1e-9);

    // Time scatter: the shared overlap depth, recomputed here independently
    // from row-derived ranges (closed microsecond ranges; a sentinel is
    // longer than 8x the median and reaches outside the span of the others,
    // at most 4 excluded and none when more qualify; union over sorted
    // ranges; sortedness over every consecutive within-file pair, ascending
    // without overlapping).
    let scatter = expected
        .get(RULE_SCATTERED_TIME)
        .unwrap()
        .time_scatter
        .as_ref()
        .unwrap();
    assert_eq!(scatter.late_rows, late);
    let len = |lo: i64, hi: i64| u64::try_from(hi - lo + 1).unwrap();
    let mut lengths: Vec<u64> = ranges.iter().map(|&(_, lo, hi)| len(lo, hi)).collect();
    lengths.sort_unstable();
    let limit = 8 * lengths[(lengths.len() - 1) / 2];
    let bulk = ranges.iter().filter(|&&(_, lo, hi)| len(lo, hi) <= limit);
    let span_lo = bulk.clone().map(|r| r.1).min().unwrap();
    let span_hi = bulk.map(|r| r.2).max().unwrap();
    let outlier =
        |&(_, lo, hi): &(usize, i64, i64)| len(lo, hi) > limit && (lo < span_lo || hi > span_hi);
    let outliers = ranges.iter().filter(|r| outlier(r)).count();
    let excluded = if outliers <= 4 { outliers } else { 0 };
    let mut kept: Vec<(i64, i64)> = ranges
        .iter()
        .filter(|r| excluded == 0 || !outlier(r))
        .map(|&(_, lo, hi)| (lo, hi))
        .collect();
    assert_eq!(scatter.excluded, excluded as u64);
    let covered: u64 = kept.iter().map(|&(lo, hi)| len(lo, hi)).sum();
    kept.sort_unstable();
    let mut union = 0u64;
    let (mut a, mut b) = kept[0];
    for &(lo, hi) in &kept[1..] {
        if lo <= b + 1 {
            b = b.max(hi);
        } else {
            union += len(a, b);
            (a, b) = (lo, hi);
        }
    }
    union += len(a, b);
    let (mut pairs, mut sorted) = (0u64, 0u64);
    for w in ranges.windows(2) {
        if w[0].0 == w[1].0 {
            pairs += 1;
            sorted += u64::from(w[1].1 >= w[0].2);
        }
    }
    assert_eq!((scatter.covered, scatter.union), (covered, union));
    assert_eq!((scatter.sorted_pairs, scatter.pairs), (sorted, pairs));
    assert!((scatter.depth - covered as f64 / union as f64).abs() < 1e-12);
    assert!(
        scatter.depth >= 1.0,
        "a union is never longer than its parts"
    );
    expected
}

#[test]
fn ground_truth_matches_brute_force_with_repeat_versions() {
    // 3 000 rows per day over 750 active entities: four versions per day.
    check_ground_truth(&tiny(11), "truth-repeat");
}

#[test]
fn ground_truth_matches_brute_force_one_version_per_day() {
    // 3 000 rows per day = churn * entities: fanout equals version count.
    let params = GenParams {
        entities: 6_000,
        ..tiny(12)
    };
    let expected = check_ground_truth(&params, "truth-single");
    let v = expected
        .get(RULE_LOOKBACK)
        .unwrap()
        .versions
        .clone()
        .unwrap();
    assert_eq!(v.entities_fanout_equals_versions, v.entities);
    let fix = fix_fanout_of(&expected, RULE_BATCH_STAMP);
    assert_eq!(fix.row_groups_per_value, [1.0, 1.0, 1.0]);
}

fn planted(expected: &Expected, rule: &str) -> bool {
    expected.get(rule).unwrap().planted
}

#[test]
fn planted_flags_follow_the_layout() {
    // One file per day and no late rows: a batch value stays in one file and
    // row-group ranges have no gaps, so neither pattern is planted.
    let flat = GenParams {
        files: 2,
        days: 2,
        late_fraction: 0.0,
        entities: 1_500,
        ..tiny(31)
    };
    let e = check_ground_truth(&flat, "planted-flat");
    assert!(
        !planted(&e, RULE_BATCH_STAMP),
        "{:?}",
        e.get(RULE_BATCH_STAMP)
    );
    assert!(
        !planted(&e, RULE_SCATTERED_TIME),
        "{:?}",
        e.get(RULE_SCATTERED_TIME)
    );

    // The recipe layout plants all four measured patterns. Two of them stay
    // deferred because the benchmark lacks the input their fix needs; the
    // measured value is kept either way.
    let e = check_ground_truth(&recipe_like(32), "planted-recipe");
    for rule in [
        RULE_BATCH_STAMP,
        RULE_LOW_SELECTIVITY,
        RULE_SCATTERED_TIME,
        RULE_LOOKBACK,
    ] {
        let a = e.get(rule).unwrap();
        assert!(a.planted, "{rule} not planted: {}", a.criterion);
        assert!(a.fix.is_some(), "{rule} has no recorded fix");
    }
    for rule in [RULE_BATCH_STAMP, RULE_SCATTERED_TIME] {
        assert_eq!(e.get(rule).unwrap().deferred, None, "{rule}");
    }
    let low = e.get(RULE_LOW_SELECTIVITY).unwrap();
    assert!(low.deferred.as_deref().unwrap().contains("only entity_id"));
    let lookback = e.get(RULE_LOOKBACK).unwrap();
    assert!(lookback.deferred.as_deref().unwrap().contains("query plan"));
    let unused = e.get(RULE_DECLARED_UNUSED).unwrap();
    assert!(!unused.planted);
    assert!(unused.deferred.is_some());
}

/// The corpora on which the profiler's lints once disagreed with these
/// planted flags (both rounds of the PR 17 review, generated by the same
/// parameters). Both now come from the shared decisions in
/// `bellwether_source::stats`; the flags are pinned here, and the profiler's
/// ground-truth test checks its lints against them.
#[test]
fn review_corpora_keep_their_measured_flags() {
    // The review's parameters, as `bellwether-bench generate` flags.
    let base = GenParams {
        seed: 0,
        files: 0,
        row_groups_per_file: 1,
        rows_per_row_group: 500,
        entities: 8_000,
        churn: 0.5,
        days: 0,
        payload_bytes: 32,
        late_fraction: 0.0,
    };
    for (name, params, batch, scatter) in [
        // 4 days of 16 writers and 32 late rows: 12 of 64 within-file pairs
        // out of order.
        (
            "x1-scatter-fp",
            GenParams {
                seed: 50,
                files: 64,
                row_groups_per_file: 2,
                rows_per_row_group: 2_000,
                entities: 640_000,
                churn: 0.1,
                days: 4,
                payload_bytes: 8,
                late_fraction: 0.000_16,
            },
            true,
            true,
        ),
        (
            "x2-scatter-miss",
            GenParams {
                seed: 41,
                files: 24,
                row_groups_per_file: 2,
                rows_per_row_group: 250,
                days: 3,
                late_fraction: 0.003,
                ..base
            },
            true,
            true,
        ),
        // One day: time concordance is undefined, so no batch stamp.
        (
            "x3-oneday",
            GenParams {
                seed: 3,
                files: 8,
                days: 1,
                ..base
            },
            false,
            false,
        ),
        // Two days of 8 writers, no late rows: a stamp, but every file ascends.
        (
            "x4-twoday",
            GenParams {
                seed: 4,
                files: 16,
                days: 2,
                ..base
            },
            true,
            false,
        ),
        // Second round: two days of 5 and of 7 writers, where a narrowness
        // gate once kept the profiler silent; the decision is now shared.
        (
            "r2-d2-f5-rg1",
            GenParams {
                seed: 21,
                files: 5,
                days: 2,
                ..base
            },
            true,
            false,
        ),
        (
            "r2-d2-f7-rg1",
            GenParams {
                seed: 22,
                files: 7,
                days: 2,
                ..base
            },
            true,
            false,
        ),
        // Fewer entities than batch values, where a partner-distinct gate
        // once kept the profiler silent.
        (
            "r2-few-entities-wide",
            GenParams {
                seed: 25,
                files: 24,
                row_groups_per_file: 2,
                rows_per_row_group: 50,
                entities: 100,
                churn: 1.0,
                days: 3,
                payload_bytes: 400,
                late_fraction: 0.0,
            },
            true,
            false,
        ),
        // Third round: every row a distinct entity, so entity_id is a row id;
        // the workload supplies it, so it is still the partner on both sides.
        (
            "r3-rowid-71",
            GenParams {
                seed: 71,
                files: 4,
                row_groups_per_file: 1,
                rows_per_row_group: 400,
                entities: 50_000_000,
                churn: 0.001,
                days: 2,
                payload_bytes: 400,
                late_fraction: 0.0,
            },
            true,
            false,
        ),
        (
            "r3-rowid-72",
            GenParams {
                seed: 72,
                files: 4,
                row_groups_per_file: 1,
                rows_per_row_group: 1_000,
                entities: 50_000_000,
                churn: 0.001,
                days: 2,
                payload_bytes: 400,
                late_fraction: 0.0,
            },
            true,
            false,
        ),
        (
            "r3-rowid-73",
            GenParams {
                seed: 73,
                files: 4,
                row_groups_per_file: 2,
                rows_per_row_group: 500,
                entities: 50_000_000,
                churn: 0.001,
                days: 2,
                payload_bytes: 400,
                late_fraction: 0.0,
            },
            true,
            false,
        ),
        (
            "r3-rowid-74",
            GenParams {
                seed: 74,
                files: 4,
                row_groups_per_file: 1,
                rows_per_row_group: 3_000,
                entities: 50_000_000,
                churn: 0.001,
                days: 2,
                payload_bytes: 400,
                late_fraction: 0.0,
            },
            true,
            false,
        ),
        // Hourly row groups with 22 late rows widening 18 ranges by 1-3 days:
        // all inside the data's span, so none is a sentinel.
        (
            "r2-hourly-late",
            GenParams {
                seed: 31,
                files: 3,
                row_groups_per_file: 24,
                rows_per_row_group: 500,
                entities: 30_000,
                churn: 0.5,
                days: 3,
                payload_bytes: 16,
                late_fraction: 0.000_5,
            },
            false,
            true,
        ),
    ] {
        let dir = tmp(name);
        generate(&params, &dir).unwrap();
        let e = compute(&Corpus::open(&dir).unwrap()).unwrap();
        assert_eq!(
            planted(&e, RULE_BATCH_STAMP),
            batch,
            "{name}: {}",
            e.get(RULE_BATCH_STAMP).unwrap().criterion
        );
        assert_eq!(
            planted(&e, RULE_SCATTERED_TIME),
            scatter,
            "{name}: {}",
            e.get(RULE_SCATTERED_TIME).unwrap().criterion
        );
    }
}

#[test]
fn harness_and_report_smoke() {
    let dir = tmp("harness");
    let corpus = dir.join("corpus");
    generate(&tiny(21), &corpus).unwrap();
    generate_workload(
        &corpus,
        &WorkloadParams {
            seed: 1,
            runs: 20,
            p50: 5,
            p95: 100,
            max: 400,
        },
    )
    .unwrap();
    write_ground_truth(&corpus).unwrap();
    let options = RunOptions {
        scan: true,
        duckdb: DuckDbMode::Auto,
        fault: ScanFault::None,
    };
    let results = run_all(&[corpus.as_path()], &dir.join("results.json"), &options).unwrap();
    let report = evaluate(&results);
    assert!(passed_except_rss(&report), "{}", report.render());
    for name in ["same_rows", "bytes_read_matches_plan", "plan_cost_reported"] {
        assert_eq!(
            report.outcome(name),
            Some(Outcome::Pass),
            "{}",
            report.render()
        );
    }
    let c = &results.corpora[0];
    assert_eq!(c.runs.len(), 20);
    for run in &c.runs {
        assert!(run.funnel.oracle <= run.funnel.after_stats.unwrap());
        assert!(run.funnel.after_stats.unwrap() <= run.funnel.row_groups_total);
        assert!(run.funnel.after_index.is_none());
        for r in run.results.iter().filter(|r| r.cost.is_some()) {
            let planned = r.cost.as_ref().unwrap().bytes;
            assert_eq!(r.bytes_consumed, Some(planned), "{}", r.strategy);
            assert_eq!(r.bytes_read, Some(planned), "{}", r.strategy);
            assert_eq!(r.bytes_read_minus_planned, Some(0));
        }
    }
    assert!(c.ground_truth.is_some());
    assert_ne!(results.environment.rustc, "");
    assert_eq!(results.environment.corpus_parameters.len(), 1);

    // DataFusion is a plan strategy when compiled in; its plan sits between
    // the oracle and raw scan, and its rows match the oracle exactly.
    let datafusion = c
        .strategies
        .iter()
        .find(|s| s.name == "datafusion")
        .unwrap();
    assert_eq!(datafusion.available, cfg!(feature = "datafusion"));
    if datafusion.available {
        for run in &c.runs {
            let r = run
                .results
                .iter()
                .find(|r| r.strategy == "datafusion")
                .unwrap();
            assert_eq!(r.error, None);
            assert_eq!(r.false_negative_row_groups, Some(0));
            let rgs = r.cost.as_ref().unwrap().row_groups;
            assert!(run.oracle.row_groups <= rgs && rgs <= run.funnel.row_groups_total);
            assert_eq!(r.matched_rows, Some(run.oracle.matched_rows));
        }
    }

    // DuckDB runs only with the feature and a CLI present; either way it is
    // listed, and when it ran it must agree with the oracle exactly.
    let duck = c.strategies.iter().find(|s| s.name == "duckdb").unwrap();
    if duck.available {
        assert!(results.environment.duckdb_cli.is_some());
        for run in &c.runs {
            let r = run.results.iter().find(|r| r.strategy == "duckdb").unwrap();
            assert_eq!(r.error, None);
            assert_eq!(r.matched_rows, Some(run.oracle.matched_rows));
            assert_eq!(
                r.amount_total.as_deref(),
                Some(run.oracle.amount_total.as_str())
            );
        }
    } else {
        assert!(duck
            .note
            .as_deref()
            .unwrap_or("")
            .starts_with("unavailable"));
        let required = RunOptions {
            scan: false,
            duckdb: DuckDbMode::Required,
            fault: ScanFault::None,
        };
        assert!(run_all(&[corpus.as_path()], &dir.join("r2.json"), &required).is_err());
    }

    let html = render(&results);
    assert!(html.contains("<svg"));
    assert!(!html.contains("<script"));
    assert!(html.contains("prefers-color-scheme: dark"));
}
