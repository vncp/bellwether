//! The two CLI surfaces `scripts/perf.sh` measures through (WP22a):
//! `index --threads`, whose segments must not depend on the thread count,
//! and `bench read`, which reads a lookup's planned row groups (or a whole
//! folder) through the benchmark's byte-counting reader.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::collections::BTreeSet;
use std::path::Path;

use common::{bw, corpus, keys, path, run};
use serde_json::Value;

fn json(s: &str) -> Value {
    serde_json::from_str(s.trim()).unwrap()
}

fn segment_files(index: &Path) -> BTreeSet<String> {
    std::fs::read_dir(index.join("segments"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect()
}

#[test]
fn index_threads_writes_the_same_segments_at_any_thread_count() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 6, 3, 200, 500);
    let mut seen = Vec::new();
    for threads in ["1", "3"] {
        let index = dir.path().join(format!("idx-{threads}"));
        let v = json(&run(
            &[
                "index",
                path(&data),
                "--index",
                path(&index),
                "--key",
                "exact:id",
                "--key",
                "temporal:id@ts",
                "--threads",
                threads,
                "--json",
            ],
            0,
        ));
        assert_eq!(v["row_groups_read"], 18);
        seen.push((v["segments_written"].clone(), segment_files(&index)));
    }
    assert_eq!(seen[0], seen[1]);
    assert!(!seen[0].1.is_empty());

    // Zero threads is a usage error that names the range.
    let out = bw()
        .args(["index", path(&data), "--key", "exact:id", "--threads", "0"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("--threads") && err.contains("1.."), "{err}");
}

#[test]
fn bench_read_reads_exactly_the_planned_row_groups() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 4, 4, 100, 1600);
    let index = dir.path().join("idx");
    run(
        &[
            "index",
            path(&data),
            "--index",
            path(&index),
            "--key",
            "exact:id",
        ],
        0,
    );
    // Ids 0..1600 are written once each, in order: ids 150 and 1450 live in
    // row groups 1 of the first file and 2 of the last.
    let key_file = dir.path().join("keys.parquet");
    keys(&key_file, "id", &[150, 1450]);
    let lookup = run(
        &[
            "locate",
            path(&data),
            "--index",
            path(&index),
            "--keys",
            path(&key_file),
            "--json",
        ],
        0,
    );
    let plan = json(&lookup);
    let plan_file = dir.path().join("plan.json");
    std::fs::write(&plan_file, &lookup).unwrap();

    let read = json(&run(
        &["bench", "read", "--plan", path(&plan_file), "--json"],
        0,
    ));
    let r = &read["read"];
    assert_eq!(r["row_groups"], plan["planned"]["row_groups"]);
    assert_eq!(r["files"], plan["planned"]["files"]);
    assert_eq!(r["rows"], 200);
    // Every planned column chunk is fetched once, and nothing else is.
    assert_eq!(r["bytes_fetched"], plan["planned"]["bytes"]);

    let all = json(&run(&["bench", "read", "--all", path(&data), "--json"], 0));
    assert_eq!(all["read"]["row_groups"], 16);
    assert_eq!(all["read"]["rows"], 1600);

    // Human output says what was read.
    let human = run(&["bench", "read", "--all", path(&data)], 0);
    assert!(
        human.starts_with("read ") && human.contains("16 row groups"),
        "{human}"
    );

    // JSON that is not a lookup's output is refused with what to pass.
    let not_plan = dir.path().join("other.json");
    std::fs::write(&not_plan, "{\"root\": \"x\"}").unwrap();
    let out = bw()
        .args(["bench", "read", "--plan", path(&not_plan)])
        .output()
        .unwrap();
    assert_ne!(out.status.code(), Some(0));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("--json output of locate"), "{err}");

    // One of --plan or --all is required.
    let out = bw().args(["bench", "read"]).output().unwrap();
    assert_eq!(out.status.code(), Some(2));
}
