//! `index --plan-work`, `--worker`, `--commit` through the binary (ADR
//! 030): worker processes run concurrently against a `file://` index, the
//! commit equals a single-machine `index` of the same work, lookups plan
//! the same, and every refusal names the command that repairs it.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

mod common;

use std::path::Path;
use std::process::Command;

use common::{corpus, keys, path, run};
use serde_json::Value;

const CLOCK: &str = "2026-10-26T12:00:00.000Z";

fn bin() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_bellwether"))
}

/// Runs the binary, returns (exit code, stdout, stderr).
fn exec(args: &[&str]) -> (i32, String, String) {
    let out = Command::new(bin()).args(args).output().unwrap();
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8(out.stdout).unwrap(),
        String::from_utf8(out.stderr).unwrap(),
    )
}

fn manifest(index: &Path) -> Value {
    let mut v: Value =
        serde_json::from_slice(&std::fs::read(index.join("manifest.json")).unwrap()).unwrap();
    let o = v.as_object_mut().unwrap();
    o.remove("committed_at");
    o.remove("commit_nonce");
    o.remove("created_at");
    for t in v["segment_tombstones"].as_array_mut().into_iter().flatten() {
        t.as_object_mut().unwrap().remove("since");
    }
    v
}

fn locate(root: &Path, index: &Path, keys: &Path, extra: &[&str]) -> Value {
    let url = format!("file://{}", index.display());
    let mut args = vec![
        "--clock",
        CLOCK,
        "--json",
        "--index",
        &url,
        "locate",
        path(root),
        "--keys",
        path(keys),
    ];
    args.extend_from_slice(extra);
    let mut v: Value = serde_json::from_str(&run(&args, 0)).unwrap();
    v.as_object_mut().unwrap().remove("evidence");
    v
}

#[test]
fn worker_processes_commit_what_index_builds_and_refusals_name_the_repair() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("data");
    corpus(&root, 6, 4, 50, 300);
    let single = tmp.path().join("single");
    let multi = tmp.path().join("multi");
    let single_url = format!("file://{}", single.display());
    let multi_url = format!("file://{}", multi.display());
    let key_args = [
        "--key",
        "exact:id",
        "--key",
        "temporal:id@ts",
        "--key",
        "range:ts",
        "--key",
        "bloom:payload@0.01",
    ];

    let mut a = vec!["--index", single_url.as_str(), "index", path(&root)];
    a.extend_from_slice(&key_args);
    run(&a, 0);

    // Plan: nothing committed, plan.json written, the commands printed.
    let out = tmp.path().join("plan");
    let mut a = vec!["--index", multi_url.as_str(), "index", path(&root)];
    a.extend_from_slice(&key_args);
    a.extend_from_slice(&["--plan-work", path(&out), "--items", "3"]);
    let human = run(&a, 0);
    assert!(human.contains("nothing committed"), "{human}");
    assert!(human.contains("--worker"), "{human}");
    assert!(human.contains("--commit"), "{human}");
    let plan = out.join("plan.json");
    assert!(plan.exists());
    assert!(!multi.join("manifest.json").exists());

    // The same directory again is refused (a plan is never overwritten).
    let (code, _, err) = exec(&a);
    assert_eq!(code, 2, "{err}");
    assert!(err.contains("plan.json exists"), "{err}");

    // Commit before any worker: every item named, with the command.
    let (code, _, err) = exec(&["index", "--commit", path(&plan)]);
    assert_eq!(code, 4, "{err}");
    assert!(err.contains("items 0, 1, 2 have no done file"), "{err}");
    assert!(
        err.contains(&format!(
            "bellwether index --worker {} --item 0",
            plan.display()
        )),
        "{err}"
    );
    assert!(err.contains("Nothing was committed"), "{err}");

    // Source flags are the plan's: refused beside --worker.
    let (code, _, err) = exec(&[
        "--format",
        "csv",
        "index",
        "--worker",
        path(&plan),
        "--item",
        "0",
    ]);
    assert_eq!(code, 2, "{err}");
    assert!(err.contains("pass only --index"), "{err}");

    // Three worker processes at once (the plan records the index).
    let children: Vec<_> = (0..3)
        .map(|i| {
            Command::new(bin())
                .args(["--json", "index", "--worker", path(&plan), "--item"])
                .arg(i.to_string())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap()
        })
        .collect();
    for c in children {
        let o = c.wait_with_output().unwrap();
        assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
        let v: Value = serde_json::from_slice(&o.stdout).unwrap();
        assert_eq!(v["written"], "new", "{v}");
    }
    // A rerun uploads the same bytes and keeps the equal done file.
    let v: Value = serde_json::from_str(&run(
        &[
            "--json",
            "index",
            "--worker",
            path(&plan),
            "--item",
            "1",
            "--threads",
            "1",
        ],
        0,
    ))
    .unwrap();
    assert_eq!(v["written"], "unchanged", "{v}");

    let human = run(&["index", "--commit", path(&plan)], 0);
    assert!(human.contains("3 items committed once"), "{human}");
    assert!(human.contains("worker ms"), "{human}");
    assert!(human.contains("committed manifest sequence 1"), "{human}");
    assert_eq!(manifest(&single), manifest(&multi));

    let k = tmp.path().join("keys.parquet");
    keys(&k, "id", &[1, 7, 150, 299, 5000]);
    assert_eq!(
        locate(&root, &single, &k, &[]),
        locate(&root, &multi, &k, &[])
    );
    assert_eq!(
        locate(&root, &single, &k, &["--window", "1970-01-01..1970-01-01"]),
        locate(&root, &multi, &k, &["--window", "1970-01-01..1970-01-01"])
    );

    // A second commit of the same plan: stale, already committed.
    let (code, _, err) = exec(&["index", "--commit", path(&plan)]);
    assert_eq!(code, 4, "{err}");
    assert!(
        err.contains("work plan stale: its segments are already committed"),
        "{err}"
    );

    // A stale plan: a build commits between plan and commit.
    corpus(&tmp.path().join("more"), 1, 2, 50, 300);
    std::fs::rename(
        tmp.path().join("more/part-000.parquet"),
        root.join("part-100.parquet"),
    )
    .unwrap();
    let out2 = tmp.path().join("plan2");
    let mut a = vec!["--index", multi_url.as_str(), "index", path(&root)];
    a.extend_from_slice(&key_args);
    a.extend_from_slice(&["--plan-work", path(&out2)]);
    run(&a, 0);
    let plan2 = out2.join("plan.json");
    run(&["index", "--worker", path(&plan2), "--item", "0"], 0);
    corpus(&tmp.path().join("more2"), 1, 1, 50, 300);
    std::fs::rename(
        tmp.path().join("more2/part-000.parquet"),
        root.join("part-101.parquet"),
    )
    .unwrap();
    let mut a = vec!["--index", multi_url.as_str(), "index", path(&root)];
    a.extend_from_slice(&key_args);
    run(&a, 0);
    let (code, _, err) = exec(&["index", "--commit", path(&plan2)]);
    assert_eq!(code, 4, "{err}");
    assert!(
        err.contains("work plan stale; run --plan-work again"),
        "{err}"
    );
    assert!(err.contains("row-group space"), "{err}");

    // Another index store: not published there.
    let elsewhere = tmp.path().join("elsewhere");
    std::fs::create_dir_all(&elsewhere).unwrap();
    let (code, _, err) = exec(&[
        "--index",
        path(&elsewhere),
        "index",
        "--worker",
        path(&plan2),
        "--item",
        "0",
    ]);
    assert_eq!(code, 2, "{err}");
    assert!(err.contains("is not in the index store"), "{err}");
}
