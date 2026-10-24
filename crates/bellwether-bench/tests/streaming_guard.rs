//! The ADR 018 streaming guard must be able to see retention.
//!
//! This file is its own test binary with a single test, so no other test
//! shares the process while RSS is measured.

#![allow(clippy::unwrap_used)]

use std::path::Path;

use bellwether_bench::generate::{generate_with, rss_growth_bound_bytes, GenOptions};
use bellwether_bench::slo::{generator_checks, smoke_params, Outcome};

#[test]
fn rss_guard_passes_streaming_and_catches_retention() {
    let (params, _) = smoke_params();

    // The smoke corpus must be large enough that keeping it in memory breaks
    // the bound several times over; otherwise retention would be invisible.
    let corpus_bytes = params.estimated_row_group_bytes()
        * u64::from(params.files)
        * u64::from(params.row_groups_per_file);
    let bound = rss_growth_bound_bytes(&params);
    assert!(
        corpus_bytes >= 3 * bound,
        "smoke corpus {corpus_bytes} B is less than 3x the RSS bound {bound} B"
    );

    let root = Path::new(env!("CARGO_TARGET_TMPDIR")).join("bellwether-bench-guard");
    if root.exists() {
        std::fs::remove_dir_all(&root).unwrap();
    }

    let streaming = generate_with(&params, &root.join("streaming"), GenOptions::default()).unwrap();
    let retaining = generate_with(
        &params,
        &root.join("retaining"),
        GenOptions {
            retain_row_groups: true,
        },
    )
    .unwrap();
    std::fs::remove_dir_all(&root).unwrap();

    let ok = generator_checks("streaming", &params, Some(&streaming));
    let bad = generator_checks("retaining", &params, Some(&retaining));
    let rss = |checks: &[bellwether_bench::slo::Check]| {
        checks
            .iter()
            .find(|c| c.name == "generator_rss_growth")
            .cloned()
            .unwrap()
    };
    let (ok, bad) = (rss(&ok), rss(&bad));
    println!("streaming: {}\nretaining: {}", ok.detail, bad.detail);
    if streaming.rss.is_none() {
        // No probe on this platform: the guard reports a skip, which strict
        // mode turns into a failure on Linux, where CI runs.
        assert_eq!(ok.outcome, Outcome::Skip);
        assert_ne!(
            std::env::consts::OS,
            "linux",
            "Linux must have an RSS probe"
        );
        return;
    }
    assert_eq!(ok.outcome, Outcome::Pass, "{}", ok.detail);
    assert_eq!(bad.outcome, Outcome::Fail, "{}", bad.detail);
}
