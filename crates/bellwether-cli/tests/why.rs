//! `why` through the CLI: the printed line is the typed result and every
//! record in it is the ledger's stored bytes (the in-process ADR 010 test is
//! in `review_probe.rs`), and the recommendation on a planted
//! `batch_stamp_key` corpus indexed on its batch stamp.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

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
    assert_eq!(rec.current.row_groups, 6 * r.dataset.row_groups);
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

/// Regression tests for real-data defects 7, 8, 9, 10 and strategy request 3:
/// - Defect 7 & 8: when every candidate touches nearly every row group,
///   `no_discriminating_column` fires on the profile and `why` with keys names
///   the layout as the culprit together with the lowest-fanout candidate.
/// - Defect 9 + Strategy 3: `why --mode locate|latest|asof` with
///   `--latest-by`, `--at`, and `--lookback` prices candidates under temporal
///   lookups via the replay crate's temporal path, matching `latest` and
///   `asof` plans.
/// - Defect 10: the "no recorded workload" assumption clause does not appear
///   when a key file (`--from`) is supplied.
#[test]
fn realdata_defects_why_layout_verdict_temporal_modes_and_supplied_columns() {
    use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampSecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use bellwether::PlanResult;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();

    // Build a time-ordered taxi-like dataset: 4 files, 1 row group each, where
    // every pickup zone (1..=4) appears in every file, and timestamps advance
    // file by file so the newest row per zone is in the last file (or file 1
    // at --at 150).
    let taxi = dir.path().join("taxi");
    std::fs::create_dir_all(&taxi).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("zone", DataType::Int64, false),
        Field::new("vendor", DataType::Int64, false),
        Field::new(
            "pickup_ts",
            DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
            false,
        ),
    ]));
    for f_idx in 0..4i64 {
        let mut zones = Vec::new();
        let mut vendors = Vec::new();
        let mut tss = Vec::new();
        for z in 1..=4i64 {
            for v in 1..=2i64 {
                zones.push(z);
                vendors.push(v);
                tss.push((f_idx + 1) * 100 + z);
            }
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(zones)) as ArrayRef,
                Arc::new(Int64Array::from(vendors)) as ArrayRef,
                Arc::new(TimestampSecondArray::from(tss).with_timezone("UTC")) as ArrayRef,
            ],
        )
        .unwrap();
        let file = std::fs::File::create(taxi.join(format!("part-{f_idx}.parquet"))).unwrap();
        let mut w = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }

    // Defect 8: `why` with no keys emits `no_discriminating_column`.
    let bare_why: WhyReport = typed(&run(&["--json", "why", path(&taxi)], 0));
    assert!(
        bare_why
            .profile
            .lints
            .iter()
            .any(|l| l.rule == "no_discriminating_column"),
        "expected no_discriminating_column lint, got {:?}",
        bare_why.profile.lints
    );

    // Defect 7: `why --from` in locate mode says the layout is the cause and names the lowest-fanout candidate.
    let zone_keys = dir.path().join("zones.parquet");
    keys(&zone_keys, "zone", &[1, 2, 3, 4]);
    let locate_why_json = run(
        &["--json", "why", path(&taxi), "--from", path(&zone_keys)],
        0,
    );
    let locate_why: WhyReport = typed(&locate_why_json);
    assert!(
        matches!(
            locate_why.culprit.as_ref().map(|c| &c.reason),
            Some(CulpritReason::Layout { .. })
        ),
        "expected Layout culprit, got {:?}",
        locate_why.culprit
    );
    let locate_why_human = run(&["why", path(&taxi), "--from", path(&zone_keys)], 0);
    assert!(
        locate_why_human.contains("the layout is the cause")
            && locate_why_human.contains("lowest-fanout candidate"),
        "{locate_why_human}"
    );

    // Defect 9 + Strategy 3: `why --mode latest --latest-by pickup_ts` prices temporal candidates
    // and recommends `temporal:zone@pickup_ts`, reading 1 of 4 row groups.
    let latest_why: WhyReport = typed(&run(
        &[
            "--json",
            "why",
            path(&taxi),
            "--from",
            path(&zone_keys),
            "--mode",
            "latest",
            "--latest-by",
            "pickup_ts",
        ],
        0,
    ));
    let rec = latest_why
        .recommended
        .as_ref()
        .expect("temporal candidate saves row groups in latest mode");
    assert_eq!(
        bellwether::spec::label(&rec.spec),
        "temporal:zone@pickup_ts"
    );
    assert_eq!(rec.proposed.row_groups, 1);
    assert_eq!(rec.current.row_groups, 4);

    // With `--lookback 150s`, latest touches files 2 and 3 (timestamps 301..304 and 401..404, within 150s of 404): 2 row groups.
    let lookback_why: WhyReport = typed(&run(
        &[
            "--json",
            "why",
            path(&taxi),
            "--from",
            path(&zone_keys),
            "--mode",
            "latest",
            "--latest-by",
            "pickup_ts",
            "--lookback",
            "150s",
        ],
        0,
    ));
    let lb_rec = lookback_why.recommended.as_ref().unwrap();
    assert_eq!(lb_rec.proposed.row_groups, 2);

    // `--mode asof --at 250` selects file 1 (timestamps 201..204 <= 250): 1 row group.
    let asof_why: WhyReport = typed(&run(
        &[
            "--json",
            "why",
            path(&taxi),
            "--from",
            path(&zone_keys),
            "--mode",
            "asof",
            "--latest-by",
            "pickup_ts",
            "--at",
            "250",
        ],
        0,
    ));
    let asof_rec = asof_why.recommended.as_ref().unwrap();
    assert_eq!(asof_rec.proposed.row_groups, 1);

    // Index `temporal:zone@pickup_ts` and verify `why`'s predicted bytes equal `latest` and `asof` plans.
    run(
        &["index", path(&taxi), "--key", "temporal:zone@pickup_ts"],
        0,
    );
    let latest_plan: PlanResult = typed(&run(
        &["--json", "latest", path(&taxi), "--keys", path(&zone_keys)],
        0,
    ));
    assert_eq!(latest_plan.planned, rec.proposed);
    let asof_plan: PlanResult = typed(&run(
        &[
            "--json",
            "asof",
            path(&taxi),
            "--keys",
            path(&zone_keys),
            "--at",
            "250",
        ],
        0,
    ));
    assert_eq!(asof_plan.planned, asof_rec.proposed);

    // Defect 10: when a key file is supplied, `low_selectivity_single_column` must NOT say
    // "no recorded workload".
    let gh = dir.path().join("gh");
    std::fs::create_dir_all(&gh).unwrap();
    let gh_schema = Arc::new(Schema::new(vec![
        Field::new("type", DataType::Utf8, false),
        Field::new("repo_id", DataType::Int64, false),
    ]));
    let mut types_col = Vec::new();
    let mut repos_col = Vec::new();
    // 4 row groups of 10 rows each: `type` has 2 values ("Push", "Watch") in every row group,
    // while `repo_id` is distinct per row group so `(repo_id, type)` has p95 = 1.
    for rg in 0..4i64 {
        for i in 0..10i64 {
            types_col.push(if i % 2 == 0 { "Push" } else { "Watch" });
            repos_col.push(rg * 100 + i);
        }
    }
    let gh_batch = RecordBatch::try_new(
        Arc::clone(&gh_schema),
        vec![
            Arc::new(StringArray::from(types_col)) as ArrayRef,
            Arc::new(Int64Array::from(repos_col)) as ArrayRef,
        ],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(10))
        .build();
    let gh_file = std::fs::File::create(gh.join("events.parquet")).unwrap();
    let mut gh_w = ArrowWriter::try_new(gh_file, gh_schema, Some(props)).unwrap();
    gh_w.write(&gh_batch).unwrap();
    gh_w.close().unwrap();

    let repo_keys = dir.path().join("repo_keys.parquet");
    keys(&repo_keys, "repo_id", &[1, 102]);
    let gh_why = run(&["why", path(&gh), "--from", path(&repo_keys)], 0);
    assert!(gh_why.contains("low_selectivity_single_column"), "{gh_why}");
    assert!(
        !gh_why.contains("no recorded workload"),
        "lint claimed no recorded workload when --from was given:\n{gh_why}"
    );
}
