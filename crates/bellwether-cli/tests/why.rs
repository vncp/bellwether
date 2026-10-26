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
    // The profile names the batch stamp, with its measured fanout.
    let culprit = r.culprit.as_ref().unwrap();
    assert_eq!(culprit.columns, vec!["batch_id".to_string()]);
    assert!(
        matches!(&culprit.reason, CulpritReason::Lint { rule, .. } if rule == "batch_stamp_key"),
        "{culprit:?}"
    );
    let fanout = culprit
        .fanout
        .as_ref()
        .expect("the profile measured batch_id");
    assert_eq!(fanout.columns, vec!["batch_id".to_string()]);
    assert!(r.evidence.recorded);
    let v: serde_json::Value = serde_json::from_str(&out).unwrap();
    for (_, rec) in check_records(&index_dir(&data), &v) {
        if let Record::Proposal(p) = rec {
            assert_eq!(p.evidence, vec![r.evidence.hash.unwrap()]);
        }
    }
    let rec = r.recommended.as_ref().unwrap();
    assert_eq!(rec.spec.columns(), ["entity_id".to_string()]);
    assert!(rec.proposed.bytes < rec.current.bytes);
    // The batch-stamp index serves no entity_id run, so each run is priced
    // as the files' own min/max statistics read it: at most every row
    // group, and the replay says so.
    assert_eq!(Some(rec.current), r.current);
    assert!(rec.current.row_groups <= 6 * r.dataset.row_groups);
    assert!(r.full_scan.is_none(), "a key spec is current");
    let floor = r.candidates.iter().find(|c| c.spec == rec.spec).unwrap();
    assert!(matches!(&floor.replay.record, Record::Replay(rp) if rp.unserved.is_some()));
    // Human output leads with the funnel.
    let human = run(
        &["why", path(&data), "--bench-workload", path(&workload)],
        0,
    );
    let lines: Vec<&str> = human.lines().collect();
    assert!(lines[0].starts_with("why "), "{human}");
    assert!(lines[1].starts_with("funnel:"), "{human}");
    assert!(
        human.contains("culprit (batch_id): lint batch_stamp_key"),
        "{human}"
    );
    assert!(human.contains("\nfanout: "), "{human}");
    assert!(human.contains("  apply it: bellwether fix "), "{human}");
}

/// A read-only folder with no `--index`: `why` exits 0, the Evidence comes
/// back unrecorded, and the human output says how to record it.
#[test]
fn why_on_a_read_only_folder_exits_0_with_the_evidence_unrecorded() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 2, 3, 30, 200);
    std::fs::set_permissions(&data, std::fs::Permissions::from_mode(0o555)).unwrap();
    let json = std::panic::catch_unwind(|| run(&["--json", "why", path(&data)], 0));
    let human = std::panic::catch_unwind(|| run(&["why", path(&data)], 0));
    std::fs::set_permissions(&data, std::fs::Permissions::from_mode(0o755)).unwrap();
    let r: WhyReport = typed(&json.unwrap());
    assert!(!r.evidence.recorded && r.evidence.hash.is_none());
    let human = human.unwrap();
    assert!(human.contains("evidence  not recorded: "), "{human}");
    assert!(human.contains("--index"), "{human}");
    assert!(!index_dir(&data).exists());
}

/// Count runs ahead of their locates, over an index on the key without
/// counts: the counts candidate's funnel row says what it is priced for
/// (count runs read nothing), not the floor note of a plain exact key,
/// which it no longer is.
#[test]
fn the_counts_candidate_names_what_it_is_priced_for() {
    let dir = tempfile::tempdir().unwrap();
    let corpus_dir = dir.path().join("corpus");
    let data = planted(&corpus_dir, 6);
    run(&["index", path(&data), "--key", "exact:entity_id"], 0);
    // Every other run counts its keys (`query` is the bench workload's
    // optional run field; the workload hash covers only the key sets).
    let wl = corpus_dir.join("workload.json");
    let mut v: serde_json::Value = serde_json::from_slice(&std::fs::read(&wl).unwrap()).unwrap();
    for (i, r) in v["runs"].as_array_mut().unwrap().iter_mut().enumerate() {
        if i % 2 == 0 {
            r["query"] = "count".into();
        }
    }
    std::fs::write(&wl, serde_json::to_vec(&v).unwrap()).unwrap();
    let human = run(
        &["why", path(&data), "--bench-workload", path(&corpus_dir)],
        0,
    );
    let row = human
        .lines()
        .find(|l| l.trim_start().starts_with("exact:entity_id,counts "))
        .unwrap_or_else(|| panic!("no counts candidate row:\n{human}"));
    assert!(
        row.contains("count runs answered from per-key counts, no data read"),
        "{row}"
    );
    assert!(!row.contains("floor"), "{row}");
}
