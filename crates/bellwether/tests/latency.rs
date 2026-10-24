//! The observe tier's budget (docs/architecture.md): `why` with no workload
//! under 100 ms.
//!
//! Asserted only in optimized builds and only when
//! `BELLWETHER_OBSERVE_LATENCY_CORPUS` names a corpus written by
//! `bellwether bench generate` (its `data/` folder is read); otherwise the
//! test says why it skipped. CI's latency job runs it with
//! `cargo test --release` on the corpus that job generates, `replay-small`
//! (4 days, 64 files, 2 row groups of 2,000 rows each), which is not the
//! pre-registered small rung. Each `why` builds a fresh source, so no footer
//! cache carries over between calls; only the operating system's file cache
//! is warm after the first call.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::{Duration, Instant};

use bellwether::{Engine, EngineOptions, WhyRequest};

/// The observe-tier budget from docs/architecture.md.
const OBSERVE_BUDGET: Duration = Duration::from_millis(100);

#[test]
fn why_without_a_workload_meets_the_observe_budget() {
    if cfg!(debug_assertions) {
        eprintln!("skipped: the observe budget is asserted in optimized builds only");
        return;
    }
    let Some(corpus) = std::env::var_os("BELLWETHER_OBSERVE_LATENCY_CORPUS") else {
        eprintln!("skipped: set BELLWETHER_OBSERVE_LATENCY_CORPUS to a bench corpus");
        return;
    };
    let data = std::path::Path::new(&corpus).join("data");
    let index = tempfile::tempdir().unwrap();
    let engine = Engine::open(
        data.to_str().unwrap(),
        EngineOptions {
            index: Some(index.path().to_str().unwrap().to_string()),
            ..EngineOptions::default()
        },
    )
    .unwrap();
    // Warm the operating system's file cache.
    engine.why(&WhyRequest::default()).unwrap();
    let mut times: Vec<Duration> = (0..7)
        .map(|_| {
            let t = Instant::now();
            engine.why(&WhyRequest::default()).unwrap();
            t.elapsed()
        })
        .collect();
    times.sort();
    let p50 = times[times.len() / 2];
    eprintln!("why (no workload): p50 {p50:?}, all {times:?}");
    assert!(
        p50 <= OBSERVE_BUDGET,
        "observe tier p50 {p50:?} exceeds the {OBSERVE_BUDGET:?} budget"
    );
}
