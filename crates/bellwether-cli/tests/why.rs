//! `why` through the CLI: the printed line is the typed result and every
//! record in it is the ledger's stored bytes (the in-process ADR 010 test is
//! in `review_probe.rs`), and the recommendation on a planted
//! `batch_stamp_key` corpus indexed on its batch stamp.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use bellwether::types::Record;
use bellwether::{CulpritReason, WhyReport};
use common::{check_records, corpus, index_dir, keys, path, planted, run, typed};

/// The CLI's `--json` line parses into the typed result and re-serializes
/// to the same bytes, and every record in it is byte for byte what the
/// ledger stores.
#[test]
fn why_json_is_the_typed_result_and_records_are_the_stored_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 2, 3, 30, 200);
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[7, 8, 150]);
    let out = run(&["--json", "why", path(&data)], 0);
    let r: WhyReport = typed(&out);
    assert_eq!(r.candidates.len(), 0);
    let out = run(&["--json", "why", path(&data), "--from", path(&kf)], 0);
    let r: WhyReport = typed(&out);
    let v: serde_json::Value = serde_json::from_str(&out).unwrap();
    let recs = check_records(&index_dir(&data), &v);
    // A proposal and a replay per candidate.
    assert_eq!(recs.len(), 2 * r.candidates.len());
    assert_eq!(
        recs.iter()
            .filter(|(_, rec)| matches!(rec, Record::Replay(_)))
            .count(),
        r.candidates.len()
    );
}

#[test]
fn why_over_a_batch_stamp_index_recommends_the_true_key() {
    let dir = tempfile::tempdir().unwrap();
    let data = planted(&dir.path().join("corpus"), 6);
    // The current index is on the batch stamp, as in docs/agent-loop.md.
    run(&["index", path(&data), "--key", "exact:batch_id"], 0);
    let workload = dir.path().join("corpus");
    let out = run(
        &[
            "--json",
            "why",
            path(&data),
            "--bench-workload",
            path(&workload),
        ],
        0,
    );
    let r: WhyReport = typed(&out);
    let culprit = r.culprit.as_ref().unwrap();
    assert_eq!(culprit.columns, vec!["entity_id".to_string()]);
    assert_eq!(culprit.reason, CulpritReason::NoIndexServes);
    let rec = r.recommended.as_ref().unwrap();
    assert_eq!(rec.spec.columns(), ["entity_id".to_string()]);
    assert!(rec.proposed.bytes < rec.current.bytes);
    assert_eq!(rec.current.row_groups, 6 * r.dataset.row_groups);
    // Human output leads with the funnel.
    let human = run(
        &["why", path(&data), "--bench-workload", path(&workload)],
        0,
    );
    let lines: Vec<&str> = human.lines().collect();
    assert!(lines[0].starts_with("why "), "{human}");
    assert!(lines[1].starts_with("funnel:"), "{human}");
    assert!(human.contains("culprit (entity_id): no index serves this lookup"));
}
