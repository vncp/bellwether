//! The observe tier's budget over MCP (deliverable 7): `observe.why` under
//! 100 ms, measured twice: in process by calling the handler's core
//! ([`bellwether_mcp::Server::call`]), and over a stdio round trip to the
//! `bellwether-mcp` binary (request written, response line read).
//!
//! Asserted only in optimized builds and only when
//! `BELLWETHER_OBSERVE_LATENCY_CORPUS` names a corpus written by
//! `bellwether bench generate` (its `data/` folder is read); otherwise the
//! test says why it skipped. CI's latency job runs it with
//! `cargo test --release` on `replay-small`, the same corpus the facade's
//! observe-budget test reads (`crates/bellwether/tests/latency.rs`, whose
//! caching notes apply here too: the first call computes the profile's
//! footer and fanout artifacts and the timed calls reuse them).

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::time::{Duration, Instant};

use common::Client;
use serde_json::json;

/// The observe-tier budget from docs/architecture.md.
const OBSERVE_BUDGET: Duration = Duration::from_millis(100);

const EXE: &str = env!("CARGO_BIN_EXE_bellwether-mcp");

fn p50(mut times: Vec<Duration>) -> Duration {
    times.sort();
    times[times.len() / 2]
}

#[test]
fn observe_why_meets_the_budget_in_process_and_over_stdio() {
    if cfg!(debug_assertions) {
        eprintln!("skipped: the observe budget is asserted in optimized builds only");
        return;
    }
    let Some(corpus) = std::env::var_os("BELLWETHER_OBSERVE_LATENCY_CORPUS") else {
        eprintln!("skipped: set BELLWETHER_OBSERVE_LATENCY_CORPUS to a bench corpus");
        return;
    };
    let data = std::path::Path::new(&corpus).join("data");
    let root = data.to_str().unwrap();

    // In process: the handler's synchronous core.
    let index = tempfile::tempdir().unwrap();
    let srv = bellwether_mcp::Server::new(&bellwether_mcp::Config {
        root: root.to_string(),
        engine: bellwether::EngineOptions {
            index: Some(index.path().to_str().unwrap().to_string()),
            ..bellwether::EngineOptions::default()
        },
        budget: bellwether_mcp::Budget::default(),
        join: None,
    })
    .unwrap();
    assert!(!srv.call("observe.why", json!({})).is_error());
    let in_process = p50((0..7)
        .map(|_| {
            let t = Instant::now();
            let r = srv.call("observe.why", json!({}));
            let d = t.elapsed();
            assert!(!r.is_error(), "{}", r.text);
            d
        })
        .collect());

    // Over stdio: the binary, after the handshake.
    let index = tempfile::tempdir().unwrap();
    let mut c = Client::spawn(EXE, &["--index", index.path().to_str().unwrap(), root]);
    c.initialize();
    assert_eq!(c.call("observe.why", json!({}))["isError"], false);
    let stdio = p50((0..7)
        .map(|_| {
            let t = Instant::now();
            let r = c.call("observe.why", json!({}));
            let d = t.elapsed();
            assert_eq!(r["isError"], false, "{r}");
            d
        })
        .collect());
    let (status, _, _) = c.close();
    assert!(status.success());

    eprintln!("observe.why (no keys): p50 in process {in_process:?}, over stdio {stdio:?}");
    assert!(
        in_process <= OBSERVE_BUDGET,
        "in-process observe.why p50 {in_process:?} exceeds the {OBSERVE_BUDGET:?} budget"
    );
    assert!(
        stdio <= OBSERVE_BUDGET,
        "stdio observe.why p50 {stdio:?} exceeds the {OBSERVE_BUDGET:?} budget"
    );
}
