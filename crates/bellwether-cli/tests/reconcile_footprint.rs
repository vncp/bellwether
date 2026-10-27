//! ADR 018 measured on the operating system's books, not the meter's:
//! the physical memory of a `reconcile` process over row groups 100,000
//! rows wide stays within its stated bound, the budget plus one decoded
//! batch plus a fixed allowance over the same binary's own baseline (the
//! same reconcile over one row group of 1,000 rows).
//!
//! Until row groups were decoded in batches, a row group was decoded whole,
//! and the allocator kept the freed row-group-sized buffers dirty: this
//! corpus peaked at 862 MB of physical footprint under a 4 MiB budget over
//! 9 passes, with the meter at 0.3 MiB (`docs/measured/2026-10-27-performance.md`,
//! the matched before and after). The process is measured by
//! `/usr/bin/time` (`-l` on macOS, the peak physical footprint; `-v` on
//! Linux, the peak resident set), so allocator retention, which a counting
//! allocator cannot see, counts.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::path::Path;
use std::process::Command;

use common::path;
use serde_json::Value;

const MIB: u64 = 1024 * 1024;

/// What the bound allows beyond the baseline reconcile, the budget, and
/// the decoded batch: the footers and fingerprints of one file, the
/// partition key hashing buffers, and allocator slack.
const FIXED: u64 = 24 * MIB;

/// Peak physical memory of `args` run by the `bellwether` binary, and its
/// stdout. Fails (never skips) when `/usr/bin/time` is missing.
fn measured(args: &[&str]) -> (u64, String) {
    let bin = env!("CARGO_BIN_EXE_bellwether");
    let flag = if cfg!(target_os = "macos") {
        "-l"
    } else {
        "-v"
    };
    let out = Command::new("/usr/bin/time")
        .arg(flag)
        .arg(bin)
        .args(args)
        .output()
        .expect("/usr/bin/time runs (install the `time` package on Linux)");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "{args:?} failed\nstderr: {stderr}\nstdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let peak = stderr
        .lines()
        .find_map(|l| {
            let l = l.trim();
            if cfg!(target_os = "macos") {
                l.strip_suffix("peak memory footprint")
                    .and_then(|n| n.trim().parse::<u64>().ok())
            } else {
                l.strip_prefix("Maximum resident set size (kbytes):")
                    .and_then(|n| n.trim().parse::<u64>().ok())
                    .map(|kb| kb * 1024)
            }
        })
        .unwrap_or_else(|| panic!("no peak memory line in /usr/bin/time's output:\n{stderr}"));
    (peak, String::from_utf8(out.stdout).unwrap())
}

fn reconcile(data: &Path, index: &Path, budget: &str) -> (u64, Value) {
    let (peak, stdout) = measured(&[
        "reconcile",
        path(data),
        path(data),
        "--key",
        "entity_id",
        "--latest-by",
        "version_ts",
        "--totals",
        "amount",
        "--group-by",
        "status",
        "--memory-budget",
        budget,
        "--index",
        path(index),
        "--json",
    ]);
    let v: Value = serde_json::from_str(stdout.trim()).unwrap();
    (peak, v["outcome"]["memory"].clone())
}

/// A generated ledger of one file of `groups` row groups of `rows` rows.
fn generate(out: &Path, groups: &str, rows: &str, entities: &str) -> std::path::PathBuf {
    let (_, stdout) = measured(&[
        "bench",
        "generate",
        "--out",
        path(out),
        "--days",
        "1",
        "--files",
        "1",
        "--row-groups-per-file",
        groups,
        "--rows-per-row-group",
        rows,
        "--entities",
        entities,
    ]);
    assert!(stdout.contains("wrote corpus"), "{stdout}");
    out.join("data")
}

#[test]
fn reconcile_physical_memory_stays_within_the_budget_plus_one_batch() {
    let dir = tempfile::tempdir().unwrap();
    let data = generate(&dir.path().join("corpus"), "4", "100000", "50000");
    // The baseline is the same reconcile over one row group of 1,000 rows:
    // the process, its threads, and the code it pages in, which Linux's
    // resident set counts and macOS's footprint does not, so what is left
    // is what grows with the data.
    let small = generate(&dir.path().join("small"), "1", "1000", "500");

    for (budget, multi_pass) in [("4M", true), ("64M", false)] {
        let (baseline, _) = reconcile(&small, &dir.path().join(format!("small-{budget}")), budget);
        let index = dir.path().join(format!("idx-{budget}"));
        let (peak, memory) = reconcile(&data, &index, budget);
        let field = |k: &str| memory[k].as_u64().unwrap();
        let (budget_bytes, state, batch) = (
            field("budget_bytes"),
            field("peak_state_bytes"),
            field("peak_batch_bytes"),
        );
        assert_eq!(field("passes") > 1, multi_pass, "{memory}");
        // One batch is at most READ_BATCH_ROWS rows, never the row group.
        assert!(
            batch < 16 * MIB,
            "a decoded batch of {batch} bytes: {memory}"
        );
        let stated = baseline + budget_bytes + batch + FIXED;
        let tracked = baseline + state + 4 * batch + FIXED;
        eprintln!(
            "budget {budget}: peak {peak} B, baseline {baseline} B, stated bound {stated} B, \
             meter-tracked bound {tracked} B ({memory})"
        );
        assert!(
            peak <= stated,
            "budget {budget}: peak physical memory {peak} B exceeds the stated bound {stated} B \
             ({memory})"
        );
        // Physical memory follows the meter: nothing outside it grows.
        assert!(
            peak <= tracked,
            "budget {budget}: peak physical memory {peak} B exceeds the meter's peak plus four \
             batches ({tracked} B, {memory})"
        );
    }
}
