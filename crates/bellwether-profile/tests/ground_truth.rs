//! Ground-truth alignment with the benchmark (WP10): on every corpus the
//! bench generator wrote, with its `expected.json`, the measured fanout must
//! equal the ground truth exactly when unsampled and lie within the stated
//! error when sampled, and the lints must use the rule ids the recovery
//! scorer matches, firing on planted patterns and never on unplanted ones.
//!
//! The bench crate is not merged, so it is not a dependency (not even a
//! dev-dependency; `dev/check-layering.py` forbids the edge). This test
//! parses `expected.json` itself and **skips with a message** when no corpus
//! is present. Corpora are found in `$BELLWETHER_BENCH_CORPORA` (directories
//! separated by `:`), else under `benches/out/*/` in the workspace, the
//! bench's default output. A corpus above `$BELLWETHER_GT_MAX_ROWS` rows
//! (default one million) is skipped with a message: the brute-force oracle
//! holds every distinct key. Generate one with the bench binary:
//!
//! ```text
//! bellwether-bench generate --out benches/out/small --days 4 --files 64 \
//!     --row-groups-per-file 2 --rows-per-row-group 2000 --entities 640000 --churn 0.1
//! bellwether-bench workload --corpus benches/out/small --runs 48 --p50 40 --p95 20000 --max 60000
//! bellwether-bench ground-truth --corpus benches/out/small
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::cast_precision_loss,
    clippy::too_many_lines
)]

mod common;

use std::path::{Path, PathBuf};

use bellwether_profile::hll::Hll;
use bellwether_profile::sampler::high_water_at_most;
use bellwether_profile::{
    lints, profile, FanoutEntry, ProfileOptions, ProfileReport, DEFAULT_HLL_PRECISION,
};
use bellwether_source::ParquetSource;
use bellwether_types::{Fanout, Source};
use common::{oracle, path_str, temp_store};
use serde::Deserialize;

const EXPECTED_FORMAT: &str = "bellwether-bench-expected/3";
/// The key columns the ground truth measures.
const KEY_COLUMNS: [&str; 3] = ["entity_id", "batch_id", "status"];

#[derive(Deserialize)]
struct Expected {
    format: String,
    files_total: u64,
    row_groups_total: u64,
    rows_total: u64,
    anti_patterns: Vec<AntiPattern>,
}

#[derive(Deserialize)]
struct AntiPattern {
    rule: String,
    columns: Vec<String>,
    planted: bool,
    #[serde(default)]
    fanout: Option<Fanout>,
    #[serde(default)]
    fix_fanout: Option<Fanout>,
}

#[derive(Deserialize)]
struct WorkloadJson {
    sizes_p50_p95_max: [u64; 3],
}

fn corpora() -> Vec<PathBuf> {
    let dirs: Vec<PathBuf> = if let Ok(v) = std::env::var("BELLWETHER_BENCH_CORPORA") {
        v.split(':')
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .collect()
    } else {
        let out = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benches/out");
        std::fs::read_dir(&out)
            .map(|it| it.filter_map(|e| Some(e.ok()?.path())).collect())
            .unwrap_or_default()
    };
    let mut found: Vec<PathBuf> = dirs
        .into_iter()
        .filter(|d| d.join("expected.json").is_file() && d.join("data").is_dir())
        .collect();
    found.sort();
    found
}

fn skip_message() {
    eprintln!(
        "ground_truth: SKIPPED, no bench corpus with expected.json found in \
         $BELLWETHER_BENCH_CORPORA or benches/out/*/ (see this file's docs to generate one)"
    );
}

/// Corpora above this many rows are skipped: the brute-force oracle holds
/// every distinct key, and the bench's large recipe reaches 4.1M rows.
/// `$BELLWETHER_GT_MAX_ROWS` overrides it.
fn max_rows() -> u64 {
    std::env::var("BELLWETHER_GT_MAX_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000)
}

fn too_large(corpus: &Path, expected: &Expected) -> bool {
    let cap = max_rows();
    let skip = expected.rows_total > cap;
    if skip {
        eprintln!(
            "ground_truth: {} SKIPPED, {} rows exceed the cap of {cap} (set BELLWETHER_GT_MAX_ROWS)",
            corpus.display(),
            expected.rows_total
        );
    }
    skip
}

/// A table size that keeps every ground-truth key unsampled: at least the
/// largest distinct count `expected.json` records (sampler marks double, so
/// the largest mark at or below twice it is above it).
fn unsampled_table(expected: &Expected) -> usize {
    let most = expected
        .anti_patterns
        .iter()
        .flat_map(|a| a.fanout.iter().chain(a.fix_fanout.iter()))
        .map(|f| f.distinct)
        .max()
        .unwrap_or(0);
    usize::try_from(most.saturating_mul(2).max(1_000)).unwrap()
}

fn find<'a>(report: &'a ProfileReport, columns: &[String]) -> Option<&'a FanoutEntry> {
    report.fanout.iter().find(|e| {
        e.fanout.columns.len() == columns.len()
            && columns.iter().all(|c| e.fanout.columns.contains(c))
    })
}

fn load(corpus: &Path) -> Expected {
    let e: Expected =
        serde_json::from_slice(&std::fs::read(corpus.join("expected.json")).unwrap()).unwrap();
    assert_eq!(e.format, EXPECTED_FORMAT, "{}", corpus.display());
    e
}

fn run(corpus: &Path, options: ProfileOptions) -> ProfileReport {
    let (_s, store) = temp_store();
    // The profiler runs with the bench's recorded workload, as the benchmark
    // decides with it: its supplied columns enter the shared partner rule.
    assert!(
        corpus.join("workload.json").is_file(),
        "{}: no workload.json; generate one with bellwether-bench workload",
        corpus.display()
    );
    let workload = Some(corpus.to_path_buf());
    profile(
        &ParquetSource::new(),
        &path_str(&corpus.join("data")),
        &store,
        &ProfileOptions {
            workload,
            columns: Some(KEY_COLUMNS.map(String::from).to_vec()),
            ..options
        },
    )
    .unwrap()
}

#[test]
fn unsampled_fanout_equals_ground_truth_and_lints_follow_planted_flags() {
    let corpora = corpora();
    if corpora.is_empty() {
        skip_message();
        return;
    }
    for corpus in corpora {
        let expected = load(&corpus);
        if too_large(&corpus, &expected) {
            continue;
        }
        let report = run(
            &corpus,
            ProfileOptions {
                max_tracked_per_key: Some(unsampled_table(&expected)),
                ..ProfileOptions::for_source("parquet")
            },
        );
        let name = corpus.display();
        assert_eq!(report.files_total, expected.files_total, "{name}");
        assert_eq!(report.row_groups_total, expected.row_groups_total, "{name}");
        assert_eq!(report.rows_total, expected.rows_total, "{name}");
        // Composites are matched by column set below, which is sound only
        // because the key columns hold no nulls (ADR 014 makes order matter
        // with nulls); check that from the footers.
        let footers = ParquetSource::new();
        for file in footers.discover(&path_str(&corpus.join("data"))).unwrap() {
            for rg in footers.row_groups(&file).unwrap() {
                for s in rg
                    .stats
                    .iter()
                    .filter(|s| KEY_COLUMNS.contains(&s.column.as_str()))
                {
                    assert_eq!(s.null_count, Some(0), "{name}: {} has nulls", s.column);
                }
            }
        }
        let mut checked = 0;
        for ap in &expected.anti_patterns {
            for truth in ap.fanout.iter().chain(ap.fix_fanout.iter()) {
                let got = find(&report, &truth.columns)
                    .unwrap_or_else(|| panic!("{name}: no fanout for {:?}", truth.columns));
                assert!(
                    !got.fanout.sampled,
                    "{name}: {:?} must be unsampled",
                    truth.columns
                );
                let mut want = truth.clone();
                want.columns.clone_from(&got.fanout.columns);
                assert_eq!(got.fanout, want, "{name}: {:?}", truth.columns);
                checked += 1;
            }
            let fired = report
                .lints
                .iter()
                .any(|l| l.rule == ap.rule && l.columns == ap.columns);
            let on: Vec<&Vec<String>> = report
                .lints
                .iter()
                .filter(|l| l.rule == ap.rule)
                .map(|l| &l.columns)
                .collect();
            match ap.rule.as_str() {
                // The shared measures decide both the planted flag and the
                // lint: exactly the planted column, and nowhere else.
                lints::BATCH_STAMP_KEY | lints::SCATTERED_TIME_COLUMN => {
                    let want: Vec<&Vec<String>> = if ap.planted {
                        vec![&ap.columns]
                    } else {
                        Vec::new()
                    };
                    assert_eq!(on, want, "{name}: {} planted={}", ap.rule, ap.planted);
                }
                // Fires on the planted column exactly when planted. On
                // another column it may fire when the catalog criterion holds
                // there (a single-day batch column touching every row group
                // is genuinely low-selectivity), but never on a column the
                // ground truth plants as a batch stamp: a batch stamp is not
                // a key at all.
                lints::LOW_SELECTIVITY_SINGLE_COLUMN => {
                    assert_eq!(
                        fired, ap.planted,
                        "{name}: {} planted={}",
                        ap.rule, ap.planted
                    );
                    let stamps: Vec<&Vec<String>> = expected
                        .anti_patterns
                        .iter()
                        .filter(|a| a.rule == lints::BATCH_STAMP_KEY && a.planted)
                        .map(|a| &a.columns)
                        .collect();
                    for cols in &on {
                        assert!(
                            !stamps.contains(cols),
                            "{name}: low selectivity on a batch stamp {cols:?}"
                        );
                        if *cols != &ap.columns {
                            eprintln!("ground_truth: {name}: low selectivity also on {cols:?}");
                        }
                    }
                }
                // Out of scope (needs a consumer plan) or deferred (needs a
                // manifest): never emitted here.
                _ => assert!(
                    report.lints.iter().all(|l| l.rule != ap.rule),
                    "{name}: {}",
                    ap.rule
                ),
            }
            if !ap.planted {
                assert!(!fired, "{name}: {} fired on an unplanted pattern", ap.rule);
            }
        }
        assert!(
            report
                .lints
                .iter()
                .all(|l| !(l.rule == lints::BATCH_STAMP_KEY && l.columns == ["entity_id"])),
            "{name}: batch_stamp_key fired on entity_id"
        );
        if corpus.join("workload.json").is_file() {
            let w: WorkloadJson =
                serde_json::from_slice(&std::fs::read(corpus.join("workload.json")).unwrap())
                    .unwrap();
            assert_eq!(
                report.workload.as_ref().unwrap().sizes,
                w.sizes_p50_p95_max,
                "{name}"
            );
        }
        eprintln!(
            "ground_truth: {name}: {checked} fanouts equal expected.json; lints {:?}",
            report.lints.iter().map(|l| &l.rule).collect::<Vec<_>>()
        );
    }
}

/// Checks every key in `keys` of a profile run at `tracked` values per key:
/// sampled, `distinct` within four `HyperLogLog` standard errors of the
/// oracle, p50 and p95 inside the DKW band, max at most the true max.
fn check_sampled(
    corpus: &Path,
    expected: &Expected,
    tracked: usize,
    keys: &[&[&str]],
    truth: &[(Vec<&str>, common::Oracle)],
) {
    let rse = Hll::relative_standard_error(DEFAULT_HLL_PRECISION);
    let report = run(
        corpus,
        ProfileOptions {
            max_tracked_per_key: Some(tracked),
            ..ProfileOptions::for_source("parquet")
        },
    );
    for key in keys {
        let t = &truth.iter().find(|(k, _)| k == key).unwrap().1;
        let cols: Vec<String> = key.iter().map(|s| (*s).to_string()).collect();
        let e = find(&report, &cols).unwrap();
        let f = &e.fanout;
        if t.distinct <= high_water_at_most(tracked) as u64 {
            // Fits the table: exact, whatever the cap.
            assert!(
                !f.sampled,
                "{key:?}: {} values fit {tracked} tracked",
                t.distinct
            );
            assert_eq!(f.distinct, t.distinct, "{key:?}");
            assert_eq!(f.row_groups_per_value, t.row_groups_per_value(), "{key:?}");
            continue;
        }
        assert!(
            f.sampled,
            "{key:?} should be sampled at {tracked} tracked values"
        );
        // The oracle agrees with expected.json where both measure.
        for ap in &expected.anti_patterns {
            for g in ap.fanout.iter().chain(ap.fix_fanout.iter()) {
                if g.columns.len() == cols.len() && cols.iter().all(|c| g.columns.contains(c)) {
                    assert_eq!(g.distinct, t.distinct);
                    assert_eq!(g.row_groups_per_value, t.row_groups_per_value());
                    assert_eq!(g.files_per_value, t.files_per_value());
                }
            }
        }
        let err = (f.distinct as f64 - t.distinct as f64).abs() / t.distinct as f64;
        assert!(
            err <= 4.0 * rse,
            "{key:?}: distinct {} vs {} ({err:.4})",
            f.distinct,
            t.distinct
        );
        let eps = e.sample.quantile_rank_error.unwrap();
        for (i, q) in [0.5, 0.95].into_iter().enumerate() {
            let (lo, hi) = (t.rg_quantile(q - eps), t.rg_quantile(q + eps));
            let v = f.row_groups_per_value[i];
            assert!(
                lo <= v && v <= hi,
                "{key:?} rg q{q}: {v} outside [{lo}, {hi}]"
            );
            let (lo, hi) = (t.file_quantile(q - eps), t.file_quantile(q + eps));
            let v = f.files_per_value[i];
            assert!(
                lo <= v && v <= hi,
                "{key:?} files q{q}: {v} outside [{lo}, {hi}]"
            );
        }
        assert!(f.row_groups_per_value[2] <= *t.rgs.last().unwrap() as f64);
        assert!(f.files_per_value[2] <= *t.files.last().unwrap() as f64);
        eprintln!(
            "ground_truth: {} {key:?} sampled at {:.3} ({tracked} tracked): distinct {} vs {} ({:.2}%), rank error {eps:.3}",
            corpus.display(),
            e.sample.rate,
            f.distinct,
            t.distinct,
            err * 100.0
        );
    }
    // Sampled measurements lower the confidence of the lints they back: a
    // lint resting on its column or its named composite, either sampled, is
    // low.
    for l in &report.lints {
        if l.rule != lints::BATCH_STAMP_KEY && l.rule != lints::LOW_SELECTIVITY_SINGLE_COLUMN {
            continue;
        }
        let rests_on_sample = report.fanout.iter().any(|e| {
            let label = format!("({})", e.fanout.columns.join(", "));
            e.fanout.sampled
                && (e.fanout.columns == l.columns
                    || (e.fanout.columns.len() > 1 && l.message.contains(&label)))
        });
        if rests_on_sample {
            assert!((l.confidence - lints::LOW).abs() < 1e-9, "{l:?}");
        }
    }
}

#[test]
fn sampled_fanout_is_within_the_stated_error() {
    let corpora = corpora();
    if corpora.is_empty() {
        skip_message();
        return;
    }
    let all: Vec<Vec<&str>> = vec![
        vec!["entity_id"],
        vec!["batch_id"],
        vec!["entity_id", "batch_id"],
        vec!["status", "entity_id"],
    ];
    for corpus in corpora {
        let expected = load(&corpus);
        if too_large(&corpus, &expected) {
            continue;
        }
        let truth: Vec<(Vec<&str>, common::Oracle)> = all
            .iter()
            .cloned()
            .zip(oracle(&corpus.join("data"), &all))
            .collect();
        // A table of 112 values samples even batch_id (144 to 288 values).
        check_sampled(
            &corpus,
            &expected,
            112,
            &[&["batch_id"], &["entity_id", "batch_id"], &["entity_id"]],
            &truth,
        );
        let entity = &truth[0].1;
        if entity.distinct >= 5_000 {
            check_sampled(
                &corpus,
                &expected,
                2_000,
                &[
                    &["entity_id"],
                    &["entity_id", "batch_id"],
                    &["status", "entity_id"],
                ],
                &truth,
            );
        }
    }
}
