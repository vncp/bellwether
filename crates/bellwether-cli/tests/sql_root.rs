//! A table root at the CLI (review items 4 and 12): the connection never
//! reaches stdout or stderr, and usage errors exit 2.
#![allow(clippy::unwrap_used)]

use std::process::Command;

const TOKEN: &str = "TOKEN-c41d";

fn bw(args: &[&str]) -> (i32, String, String) {
    let o = Command::new(env!("CARGO_BIN_EXE_bellwether"))
        .args(args)
        .output()
        .unwrap();
    (
        o.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&o.stdout).into_owned(),
        String::from_utf8_lossy(&o.stderr).into_owned(),
    )
}

#[test]
fn the_connection_never_prints_and_usage_exits_2() {
    let d = tempfile::tempdir().unwrap();
    let dir = d.path().join(TOKEN);
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("t.sqlite");
    let c = rusqlite::Connection::open(&db).unwrap();
    c.execute_batch(
        "CREATE TABLE t (id BIGINT NOT NULL, v TEXT); INSERT INTO t VALUES (1, 'a'), (2, 'b')",
    )
    .unwrap();
    let keys = d.path().join("k.csv");
    std::fs::write(&keys, "id\n1\n").unwrap();
    let root = format!("sql://sqlite/main.t?db={}", db.display());
    let index = d.path().join("ledger");
    let index = index.to_str().unwrap();
    let (code, out, err) = bw(&["why", &root, "--index", index]);
    assert_eq!(code, 0, "{err}");
    assert!(!out.contains(TOKEN) && !err.contains(TOKEN), "{out}{err}");
    let missing = format!("sql://sqlite/main.t?db={}/gone.sqlite", dir.display());
    let (code, out, err) = bw(&["why", &missing, "--index", index]);
    assert_ne!(code, 0);
    assert!(!out.contains(TOKEN) && !err.contains(TOKEN), "{out}{err}");
    let (code, _, err) = bw(&["why", &root]);
    assert_eq!(code, 2, "{err}");
    assert!(err.contains("--index") && !err.contains(TOKEN), "{err}");
    let (code, _, err) = bw(&["index", &root, "--key", "exact:id", "--index", index]);
    assert_eq!(code, 2, "{err}");
}
