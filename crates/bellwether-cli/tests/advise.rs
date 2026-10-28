//! End-to-end tests for `bellwether advise` (CLI, `Engine::advise`, MCP `score.advise`, WP13a / M6):
//! - ADR 010 parity: CLI `bellwether advise --json` equals MCP `score.advise` byte for byte.
//! - Multi-run and multi-workload Pareto frontier, bloom FPR descent, anchor checks, and discarded reasons.
//! - `--apply` (and `--signing-key`) verifies and applies the recommended frontier configuration and
//!   emits an intact signed `Receipt` chain in the ledger.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::similar_names,
    clippy::assert_is_empty
)]

mod common;

use bellwether::AdviseResult;
use common::{check_records, index_dir, keys, path, run, scattered, typed};
use serde_json::{json, Value};

const CLOCK: &str = "2026-10-27T12:00:00.000Z";

#[test]
fn advise_cli_and_mcp_parity_and_apply_to_signed_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    scattered(&data, 3, 4, 25, 400);
    let kf1 = dir.path().join("k1.parquet");
    let kf2 = dir.path().join("k2.parquet");
    keys(&kf1, "id", &[5, 77]);
    keys(&kf2, "id", &[123, 250, 399]);

    let (d, k1, k2) = (path(&data), path(&kf1), path(&kf2));

    // Record a 2-run workload via `why --from k1 --from k2`
    let why_out = run(
        &[
            "--clock", CLOCK, "--json", "why", d, "--from", k1, "--from", k2,
        ],
        0,
    );
    let why_v: Value = serde_json::from_str(&why_out).unwrap();
    let workload_hash = why_v["workload"]["hash"].as_str().unwrap().to_string();

    // 1. Run `bellwether advise --workload <hash> --json` and compare with MCP `score.advise`
    let cli_json = run(
        &[
            "--clock",
            CLOCK,
            "--json",
            "advise",
            d,
            "--workload",
            &workload_hash,
            "--max-keys",
            "2",
        ],
        0,
    );
    let res: AdviseResult = typed(&cli_json);
    assert!(res.recommended.is_some());
    assert!(
        res.frontier.len() >= 2,
        "baseline + at least 1 admitted frontier member"
    );
    assert!(res.frontier[0].is_baseline);
    assert!(!res.bloom_sweeps.is_empty(), "bloom FPR descent ran");
    assert!(!res.anchors.is_empty(), "anchor runs selected");

    // Every record embedded in the JSON matches the ledger on disk
    let v: Value = serde_json::from_str(&cli_json).unwrap();
    let recs = check_records(&index_dir(&data), &v);
    assert!(recs.len() >= 3, "Evidence + Proposals + Replays stored");

    // Compare with MCP `score.advise` under the same clock
    let srv = bellwether_mcp::Server::new(&bellwether_mcp::Config {
        root: d.to_string(),
        engine: bellwether::EngineOptions {
            clock: Some(CLOCK.into()),
            ..bellwether::EngineOptions::default()
        },
        budget: bellwether_mcp::Budget::default(),
        join: None,
        workspace: false,
    })
    .unwrap();
    let reply = srv.call(
        "score.advise",
        json!({
            "workload": workload_hash,
            "max_keys": 2,
            "wait": true
        }),
    );
    assert!(!reply.is_error(), "{}", reply.text);
    assert_eq!(
        reply.text + "\n",
        cli_json,
        "ADR 010 parity for score.advise"
    );
    assert!(reply.envelope.budget.spent.replays >= 1);

    // 2. Human output of `bellwether advise` prints baseline, Pareto frontier, bloom descent, and recommended
    let human_out = run(&["advise", d, "--workload", &workload_hash], 0);
    assert!(human_out.contains("baseline a_0:"), "{human_out}");
    assert!(human_out.contains("Pareto frontier"), "{human_out}");
    assert!(human_out.contains("Bloom FPR descent"), "{human_out}");
    assert!(human_out.contains("recommended:"), "{human_out}");

    // 3. `bellwether advise --apply --signing-key` verifies, applies, and signs the receipt
    let key_file = dir.path().join("ci.key");
    let pub_file = dir.path().join("ci.key.pub");
    let trust_file = dir.path().join("trusted.toml");
    run(
        &["keys", "generate", "--out", path(&key_file), "--name", "ci"],
        0,
    );
    run(
        &[
            "keys",
            "trust",
            "--file",
            path(&trust_file),
            "--add",
            path(&pub_file),
            "--valid-from",
            "2026-01-01T00:00:00Z",
        ],
        0,
    );

    let applied_json = run(
        &[
            "--clock",
            CLOCK,
            "--json",
            "advise",
            d,
            "--workload",
            &workload_hash,
            "--apply",
            "--signing-key",
            path(&key_file),
        ],
        0,
    );
    let applied_res: AdviseResult = typed(&applied_json);
    assert_eq!(applied_res.sequence, 0);
    assert!(applied_res.sequence_after >= 1);
    assert!(!applied_res.receipts.is_empty());
    assert!(!applied_res.signatures.is_empty());

    // Verify the receipt with `receipt show --require-signature`
    let receipt_hex = applied_res.receipts[0].to_hex();
    let show_out = run(
        &[
            "receipt",
            "show",
            d,
            &receipt_hex,
            "--trusted-keys",
            path(&trust_file),
            "--require-signature",
        ],
        0,
    );
    assert!(show_out.contains("signed"), "{show_out}");
}
