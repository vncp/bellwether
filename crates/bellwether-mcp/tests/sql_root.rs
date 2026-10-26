//! A tool argument never names a database (threat model, ADR 027):
//! `score.verify` refuses rewrite outputs that are `sql://` roots.
#![allow(clippy::unwrap_used)]

mod common;

use common::*;

#[test]
fn score_verify_refuses_table_outputs() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data, 1, 1, 10, 10);
    let srv = server(&data);
    let r = srv.call(
        "score.verify",
        serde_json::json!({
            "proposal": "0".repeat(64),
            "wait": true,
            "rewrite": {
                "before": "sql://sqlite/main.t?db=/tmp/TOKEN-x.sqlite",
                "after": data.to_string_lossy(),
                "key": ["id"],
            },
        }),
    );
    assert!(r.text.contains("invalid_argument"), "{}", r.text);
    assert!(r.text.contains("rewrite"), "{}", r.text);
}
