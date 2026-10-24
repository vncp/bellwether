//! Command-line behavior: `--assert-slo` is accepted only where it means
//! something, and `workload --preregistered` writes the pre-registered
//! workload exactly. Runs the built binary.

#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::process::Command;

use bellwether_bench::corpus::GenParams;
use bellwether_bench::generate::generate;
use bellwether_bench::workload::{WorkloadManifest, WorkloadParams};

fn bench() -> Command {
    Command::new(env!("CARGO_BIN_EXE_bellwether-bench"))
}

#[test]
fn assert_slo_is_a_usage_error_off_run() {
    for args in [
        &["generate", "--assert-slo", "--dry-run"][..],
        &["workload", "--assert-slo"][..],
        &["ground-truth", "--corpus", "x", "--assert-slo"][..],
        &["report", "--assert-slo"][..],
        &["--assert-slo", "generate", "--dry-run"][..],
        &["--results", "results.json"][..],
    ] {
        let out = bench().args(args).output().unwrap();
        assert_eq!(out.status.code(), Some(2), "{args:?} was accepted");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains("Usage"), "{args:?}: {stderr}");
    }
    // `run --assert-slo` is the place for it after a run.
    let out = bench().args(["run", "--help"]).output().unwrap();
    assert!(String::from_utf8_lossy(&out.stdout).contains("--assert-slo"));
}

#[test]
fn preregistered_workload_flag_writes_the_pinned_values() {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("bellwether-bench-cli");
    if dir.exists() {
        std::fs::remove_dir_all(&dir).unwrap();
    }
    let params = GenParams {
        seed: 1,
        files: 4,
        row_groups_per_file: 2,
        rows_per_row_group: 200,
        entities: 800,
        churn: 0.5,
        days: 2,
        payload_bytes: 16,
        late_fraction: 0.0,
    };
    generate(&params, &dir).unwrap();
    let out = bench()
        .args(["workload", "--preregistered", "--corpus"])
        .arg(&dir)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let manifest: WorkloadManifest =
        serde_json::from_slice(&std::fs::read(dir.join("workload.json")).unwrap()).unwrap();
    assert_eq!(
        manifest.params,
        WorkloadParams {
            seed: 11,
            runs: 200,
            p50: 50,
            p95: 10_000,
            max: 100_000,
        }
    );
    assert_eq!(manifest.runs.len(), 200);
    std::fs::remove_dir_all(&dir).unwrap();
}
