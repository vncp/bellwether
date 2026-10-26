//! Regressions from the first run on real public data
//! (`docs/measured/2026-10-24-realdata.md`). Each test names the defect it
//! pins; each fails on the code that run measured.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

mod common;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Decimal128Array, Float64Array, Int64Array, RecordBatch, StringArray, StructArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether::types::{KeySpec, Record};
use bellwether::{
    FixOutcome, FixResult, LookupKind, PlanResult, ReconcileResult, Refusal, Score, WhyReport,
};
use common::{bw, check_records, corpus, index_dir, keys, path, run, scattered, typed};
use parquet::arrow::ArrowWriter;

/// One Parquet file at `file` holding `columns`, one row group.
fn table(file: &Path, columns: Vec<(&str, ArrayRef)>) {
    std::fs::create_dir_all(file.parent().unwrap()).unwrap();
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(n, a)| Field::new(*n, a.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        columns.into_iter().map(|(_, a)| a).collect(),
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(File::create(file).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// `(id, amount)` rows with `amount` a Float64 and a Decimal128(10, 2) copy.
fn amounts(dir: &Path, cents: &[i64]) {
    let ids: Vec<i64> = (0..i64::try_from(cents.len()).unwrap()).collect();
    #[allow(clippy::cast_precision_loss)]
    let floats: Vec<f64> = cents.iter().map(|c| *c as f64 / 100.0).collect();
    table(
        &dir.join("part-0.parquet"),
        vec![
            ("id", Arc::new(Int64Array::from(ids)) as ArrayRef),
            ("fare", Arc::new(Float64Array::from(floats))),
            (
                "fare_dec",
                Arc::new(
                    Decimal128Array::from(cents.iter().map(|c| i128::from(*c)).collect::<Vec<_>>())
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ),
            ),
        ],
    );
}

/// Defect 3: the human `reconcile` names the first mismatching keys, the
/// same keys `--json` carries.
#[test]
fn defect_3_human_reconcile_names_the_mismatching_keys() {
    let dir = tempfile::tempdir().unwrap();
    let (left, right) = (dir.path().join("left"), dir.path().join("right"));
    corpus(&left, 1, 2, 20, 40);
    corpus(&right, 1, 2, 20, 39);
    let args = [
        "reconcile",
        path(&left),
        path(&right),
        "--key",
        "id,payload",
    ];
    let json: ReconcileResult = typed(&run(&[&["--json"][..], &args[..]].concat(), 1));
    let human = run(&args, 1);
    let first = json.outcome.mismatches.first().expect("a kept mismatch");
    let key = match first {
        bellwether::Mismatch::MissingOnLeft { key, .. }
        | bellwether::Mismatch::MissingOnRight { key, .. }
        | bellwether::Mismatch::RowHashDiffers { key, .. }
        | bellwether::Mismatch::NoComparableVersion { key, .. } => key,
        bellwether::Mismatch::TotalDiffers { .. } => panic!("{first:?}"),
    };
    let tuple: Vec<&str> = key.values.iter().map(|v| v.as_deref().unwrap()).collect();
    let shown = format!("({})", tuple.join(", "));
    assert!(human.contains(&shown), "{shown} not in\n{human}");
    assert!(human.contains("mismatching keys (id, payload)"), "{human}");
}

/// Defect 4: a key proposal whose replay predicts no saving is refused with
/// `nothing_to_gain` (exit 3) citing the replay, writes no certificate and
/// no receipt, and goes through with `--allow-no-gain`.
#[test]
fn defect_4_fix_refuses_a_proposal_that_saves_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    // Row group r holds ids 20r .. 20r+19 (mod 100): one key per row group
    // touches all six, so even the floor reads everything.
    corpus(&data, 2, 3, 20, 100);
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[1, 25, 45, 65, 85]);
    let why: WhyReport = typed(&run(
        &["--json", "why", path(&data), "--from", path(&kf)],
        0,
    ));
    assert!(why.recommended.is_none(), "{:?}", why.recommended);
    let workload = why.workload.unwrap().hash.to_hex();
    let p = run(
        &[
            "propose",
            path(&data),
            "--evidence",
            &why.evidence.hash.unwrap().to_hex(),
            "--by",
            "test",
            "--key",
            "exact:id",
        ],
        0,
    );
    let proposal = p.trim().trim_start_matches("proposal ").to_string();
    let fix = |extra: &[&str], code: i32| {
        let mut args = vec![
            "fix",
            path(&data),
            "--proposal",
            &proposal,
            "--workload",
            &workload,
        ];
        args.extend_from_slice(extra);
        run(&args, code)
    };
    let r: FixResult = typed(&fix(&["--json"], 3));
    let Some(Score::Replayed { replay, .. }) = &r.score else {
        panic!("{:?}", r.score)
    };
    let FixOutcome::Refused {
        refusal:
            Refusal::NothingToGain {
                replay: cited,
                current_bytes,
                proposed_bytes,
                ..
            },
    } = &r.outcome
    else {
        panic!("{:?}", r.outcome)
    };
    assert_eq!(*cited, replay.hash, "the refusal cites the score's replay");
    assert_eq!(current_bytes, proposed_bytes);
    assert!(r.certificate.is_none(), "nothing verified");
    let human = fix(&[], 3);
    assert!(human.contains("nothing_to_gain"), "{human}");
    assert!(human.contains("--allow-no-gain"), "{human}");
    // Past the gate the loop runs as before.
    let r: FixResult = typed(&fix(&["--json", "--allow-no-gain"], 0));
    assert!(
        matches!(r.outcome, FixOutcome::Applied { .. }),
        "{:?}",
        r.outcome
    );
}

/// Defect 5: `--memory-budget` sets the comparison budget, and both outputs
/// report the passes and the rows read per side.
#[test]
fn defect_5_memory_budget_sets_the_passes_and_both_outputs_report_them() {
    let dir = tempfile::tempdir().unwrap();
    let (left, right) = (dir.path().join("left"), dir.path().join("right"));
    corpus(&left, 2, 2, 50, 1_000);
    corpus(&right, 2, 2, 50, 1_000);
    let base = ["reconcile", path(&left), path(&right), "--key", "id"];
    let roomy: ReconcileResult = typed(&run(&[&["--json"][..], &base[..]].concat(), 0));
    assert_eq!(roomy.outcome.memory.passes, 1);
    let tight_args = [&base[..], &["--memory-budget", "4096"][..]].concat();
    let tight: ReconcileResult = typed(&run(&[&["--json"][..], &tight_args[..]].concat(), 0));
    let m = tight.outcome.memory;
    assert_eq!(m.budget_bytes, 4096);
    assert!(m.passes > 1, "{m:?}");
    // 200 rows a side, read once per pass.
    assert_eq!(m.restarts, 0, "{m:?}");
    assert_eq!(m.rows_read, [200 * u64::from(m.passes); 2]);
    let human = run(&tight_args, 0);
    // The count with thousands separators, as every human count prints.
    let passes = m.passes.to_string();
    let grouped: Vec<String> = passes
        .as_bytes()
        .rchunks(3)
        .rev()
        .map(|c| String::from_utf8(c.to_vec()).unwrap())
        .collect();
    let line = format!(
        "read {} passes over each side under a 4,096-byte state budget",
        grouped.join(",")
    );
    assert!(human.contains(&line), "{line} not in\n{human}");
    assert!(human.contains("rows read on the left"), "{human}");
    let zero = bw()
        .args([&base[..], &["--memory-budget", "0"][..]].concat())
        .output()
        .unwrap();
    assert_eq!(zero.status.code(), Some(2));
}

/// Defect 6 (as superseded by strategy request 4): `--totals` on a bare
/// float column is a usage error (exit 2, one line showing the `:dec(S)`
/// form); a passing run over a decimal prints the totals.
#[test]
fn defect_6_float_totals_are_a_usage_error_and_decimal_totals_are_printed() {
    let dir = tempfile::tempdir().unwrap();
    let (left, right) = (dir.path().join("left"), dir.path().join("right"));
    amounts(&left, &[1_050, 299, 10_001]);
    amounts(&right, &[1_050, 299, 10_001]);
    let out = bw()
        .args([
            "reconcile",
            path(&left),
            path(&right),
            "--key",
            "id",
            "--totals",
            "fare",
        ])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let err = String::from_utf8(out.stderr).unwrap();
    assert_eq!(err.trim_end().lines().count(), 1, "{err}");
    assert!(err.contains("--totals \"fare\" is Float64"), "{err}");
    assert!(err.contains("--totals fare:dec(4)"), "{err}");
    assert!(!err.contains("key spec error"), "{err}");
    let human = run(
        &[
            "reconcile",
            path(&left),
            path(&right),
            "--key",
            "id",
            "--totals",
            "fare_dec",
        ],
        0,
    );
    assert!(human.contains("reconcile: PASS"), "{human}");
    // 10.50 + 2.99 + 100.01, on both sides.
    let total = human
        .lines()
        .find(|l| l.contains("fare_dec"))
        .unwrap_or_else(|| panic!("no total line in\n{human}"));
    assert_eq!(total.matches("113.50").count(), 2, "{total}");
}

/// Taxi-shaped time-ordered corpus: 3 files, 2 row groups per file (6 row
/// groups total). Every pickup zone `pu` in `1..=4` appears in every row
/// group, so a point lookup over all 4 zones reads all 6 row groups. The
/// newest trip per zone lives in only 2 of the 6 row groups (zone 1's newest
/// in file 1 rg 1, zones 2..=4 in file 2 rg 1), and as of 1970-01-01T00:00:30Z
/// (`ts <= 30_000_000` µs) the winning versions live in 2 row groups (file 0
/// rg 1 and file 1 rg 0).
fn taxi_shaped(dir: &Path) {
    use parquet::file::properties::WriterProperties;

    std::fs::create_dir_all(dir).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("pu", DataType::Int64, false),
        Field::new(
            "pickup_ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("note", DataType::Utf8, false),
    ]));
    // Per file: two row groups of (pu, pickup_ts_us).
    let files = [
        (
            "yellow-01.parquet",
            [
                vec![
                    (1, 10_000_000),
                    (2, 10_000_000),
                    (3, 10_000_000),
                    (4, 10_000_000),
                ],
                vec![
                    (1, 20_000_000),
                    (2, 20_000_000),
                    (3, 25_000_000),
                    (4, 20_000_000),
                ],
            ],
        ),
        (
            "yellow-02.parquet",
            [
                vec![
                    (1, 30_000_000),
                    (2, 30_000_000),
                    (3, 22_000_000),
                    (4, 30_000_000),
                ],
                vec![
                    (1, 90_000_000),
                    (2, 40_000_000),
                    (3, 40_000_000),
                    (4, 40_000_000),
                ],
            ],
        ),
        (
            "yellow-03.parquet",
            [
                vec![
                    (1, 50_000_000),
                    (2, 50_000_000),
                    (3, 50_000_000),
                    (4, 50_000_000),
                ],
                vec![
                    (1, 60_000_000),
                    (2, 80_000_000),
                    (3, 80_000_000),
                    (4, 80_000_000),
                ],
            ],
        ),
    ];
    for (name, groups) in files {
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(4))
            .build();
        let file = File::create(dir.join(name)).unwrap();
        let mut w = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for g in groups {
            let pu: Vec<i64> = g.iter().map(|r| r.0).collect();
            let ts: Vec<i64> = g.iter().map(|r| r.1).collect();
            // Later trips carry longer notes, so row groups holding
            // different versions differ in bytes, not only in which.
            let notes: Vec<String> = g
                .iter()
                .map(|(p, t)| {
                    format!(
                        "z{p}@{t}{}",
                        "x".repeat(usize::try_from(t / 1_000_000).unwrap())
                    )
                })
                .collect();
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(pu)) as ArrayRef,
                    Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
                    Arc::new(StringArray::from(notes)),
                ],
            )
            .unwrap();
            w.write(&batch).unwrap();
            w.flush().unwrap();
        }
        w.close().unwrap();
    }
}

/// Defect 9: on time-ordered data where every key appears in every row group,
/// point-lookup `why --from` recommends nothing (floor = all row groups),
/// while `why --from --latest-by` (and `--at`) prices candidates under
/// temporal and point lookups, recommends `temporal:<cols>@<ts>`, and its
/// predicted bytes equal `bellwether latest` / `asof` planned bytes after
/// indexing or `fix`.
#[test]
fn defect_9_why_latest_by_prices_temporal_lookups_and_matches_latest_and_asof_plans() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("taxi");
    taxi_shaped(&data);
    let kf = dir.path().join("pu.parquet");
    keys(&kf, "pu", &[1, 2, 3, 4]);

    // Point-only `why --from`: every key is in every row group, so the floor
    // reads all 6 row groups and nothing is recommended.
    let locate_only: WhyReport = typed(&run(
        &["--json", "why", path(&data), "--from", path(&kf)],
        0,
    ));
    assert!(locate_only.recommended.is_none());
    assert_eq!(locate_only.candidates[0].proposed.row_groups, 6);

    // `why --from --latest-by pickup_ts`: recommends `temporal:pu@pickup_ts`,
    // reports both latest (2 of 6 row groups) and locate (6 of 6) costs.
    let why_latest: WhyReport = typed(&run(
        &[
            "--json",
            "why",
            path(&data),
            "--from",
            path(&kf),
            "--latest-by",
            "pickup_ts",
        ],
        0,
    ));
    let wl_info = why_latest.workload.as_ref().unwrap();
    assert_eq!(wl_info.query_kind, "latest");
    assert_eq!(wl_info.lookup, Some(LookupKind::Latest));
    assert_eq!(why_latest.current.unwrap().row_groups, 6);
    assert_eq!(why_latest.current_locate.unwrap().row_groups, 6);
    let rec = why_latest
        .recommended
        .as_ref()
        .expect("recommends temporal key");
    assert_eq!(
        rec.spec,
        KeySpec::Temporal {
            columns: vec!["pu".into()],
            ts_column: "pickup_ts".into(),
            counts: false,
        }
    );
    assert_eq!(rec.lookup, LookupKind::Latest);
    assert_eq!(rec.proposed.row_groups, 2);
    assert!(rec.proposed.bytes < rec.current.bytes);
    let floor = &why_latest.candidates[0];
    assert_eq!(floor.spec, rec.spec);
    assert_eq!(floor.proposed, rec.proposed);
    assert_eq!(
        floor
            .locate
            .expect("point-lookup cost kept on candidate")
            .row_groups,
        6
    );
    let human = run(
        &[
            "why",
            path(&data),
            "--from",
            path(&kf),
            "--latest-by",
            "pickup_ts",
        ],
        0,
    );
    assert!(human.contains("funnel: 1 run (latest)"), "{human}");
    assert!(
        human.contains("floor (latest): only row groups holding the winning version"),
        "{human}"
    );
    assert!(human.contains("locate would read 6 row groups"), "{human}");
    assert!(
        human.contains("recommended (for latest): temporal:pu@pickup_ts reads"),
        "{human}"
    );

    // `why --from --latest-by pickup_ts --at`: prices `asof` (2 row groups in
    // files 1 and 2) alongside point lookups.
    let at = "1970-01-01T00:00:30Z";
    let why_asof: WhyReport = typed(&run(
        &[
            "--json",
            "why",
            path(&data),
            "--from",
            path(&kf),
            "--latest-by",
            "pickup_ts",
            "--at",
            at,
        ],
        0,
    ));
    let asof_rec = why_asof.recommended.as_ref().unwrap();
    assert_eq!(
        asof_rec.lookup,
        LookupKind::Asof {
            at: at.into(),
            ts: 30_000_000,
            ts_column: "pickup_ts".into(),
        }
    );
    assert_eq!(asof_rec.proposed.row_groups, 2);
    // The asof winners live in other row groups than the latest winners, of
    // other sizes: an asof priced as latest would not match.
    assert_ne!(asof_rec.proposed.bytes, rec.proposed.bytes);
    let human_asof = run(
        &[
            "why",
            path(&data),
            "--from",
            path(&kf),
            "--latest-by",
            "pickup_ts",
            "--at",
            at,
        ],
        0,
    );
    assert!(
        human_asof.contains("recommended (for asof 1970-01-01T00:00:30Z): temporal:pu@pickup_ts"),
        "{human_asof}"
    );

    // Apply the recommended temporal key via `fix`: scores, verifies, builds,
    // attests, and `bellwether latest` / `asof` then plan byte-for-byte what
    // `why` predicted.
    let fix: FixResult = typed(&run(
        &[
            "--json",
            "fix",
            path(&data),
            "--proposal",
            &rec.proposal.to_hex(),
            "--workload",
            &wl_info.hash.to_hex(),
        ],
        0,
    ));
    assert!(matches!(fix.outcome, FixOutcome::Applied { .. }), "{fix:?}");

    let latest_plan: PlanResult = typed(&run(
        &["--json", "latest", path(&data), "--keys", path(&kf)],
        0,
    ));
    assert_eq!(latest_plan.planned, rec.proposed);

    let asof_plan: PlanResult = typed(&run(
        &[
            "--json",
            "asof",
            path(&data),
            "--keys",
            path(&kf),
            "--at",
            at,
        ],
        0,
    ));
    assert_eq!(asof_plan.planned, asof_rec.proposed);

    let locate_plan: PlanResult = typed(&run(
        &["--json", "locate", path(&data), "--keys", path(&kf)],
        0,
    ));
    assert_eq!(locate_plan.planned, floor.locate.unwrap());

    // Refusals: absent column, non-ordering column, and `--latest-by` without
    // a workload.
    let bad_col = bw()
        .args([
            "why",
            path(&data),
            "--from",
            path(&kf),
            "--latest-by",
            "no_such_col",
        ])
        .output()
        .unwrap();
    assert_eq!(bad_col.status.code(), Some(4));
    let err = String::from_utf8(bad_col.stderr).unwrap();
    assert!(err.contains("not in the dataset schema"), "{err}");

    let bad_type = bw()
        .args([
            "why",
            path(&data),
            "--from",
            path(&kf),
            "--latest-by",
            "note",
        ])
        .output()
        .unwrap();
    assert_eq!(bad_type.status.code(), Some(4));
    let err = String::from_utf8(bad_type.stderr).unwrap();
    assert!(err.contains("cannot order versions"), "{err}");

    let no_wl = bw()
        .args(["why", path(&data), "--latest-by", "pickup_ts"])
        .output()
        .unwrap();
    assert_eq!(no_wl.status.code(), Some(2));
}

/// Defect 12: a spec label wider than the `index` table's label column
/// keeps "complete at" aligned.
#[test]
fn defect_12_index_table_aligns_long_spec_labels() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    table(
        &data.join("part-0.parquet"),
        vec![
            (
                "pickup_location_identifier",
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            ),
            (
                "pickup_datetime_utc",
                Arc::new(
                    arrow::array::TimestampMicrosecondArray::from(vec![1, 2, 3])
                        .with_timezone("UTC"),
                ),
            ),
        ],
    );
    let out = run(
        &[
            "index",
            path(&data),
            "--key",
            "exact:pickup_location_identifier",
            "--key",
            "temporal:pickup_location_identifier@pickup_datetime_utc",
        ],
        0,
    );
    let table: Vec<&str> = out
        .lines()
        .skip_while(|l| !l.contains("key spec"))
        .take_while(|l| !l.is_empty())
        .collect();
    assert_eq!(table.len(), 3, "{out}");
    let width = table[0].len();
    for row in &table {
        assert_eq!(row.len(), width, "misaligned row {row:?} in\n{out}");
    }
}

/// Defect 13: every record one `fix` writes carries `created_at` from the
/// facade's clock in one format, UTC with milliseconds, certificates
/// included; a fixed clock reaches every record, normalized to that format.
#[test]
fn defect_13_every_record_of_a_fix_shares_the_clock_and_its_format() {
    let stamps_of = |clock: Option<&str>| {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        scattered(&data, 2, 3, 20, 100);
        let kf = dir.path().join("k.parquet");
        keys(&kf, "id", &[4, 9, 60]);
        let fixed: Vec<String> = clock
            .map(|c| vec!["--clock".to_string(), c.to_string()])
            .unwrap_or_default();
        let args = |rest: &[&str]| -> Vec<String> {
            fixed
                .iter()
                .cloned()
                .chain(rest.iter().map(ToString::to_string))
                .collect()
        };
        let run = |a: Vec<String>, code: i32| {
            let a: Vec<&str> = a.iter().map(String::as_str).collect();
            run(&a, code)
        };
        let why: WhyReport = typed(&run(
            args(&["--json", "why", path(&data), "--from", path(&kf)]),
            0,
        ));
        let rec = why.recommended.unwrap();
        let out = run(
            args(&[
                "--json",
                "fix",
                path(&data),
                "--proposal",
                &rec.proposal.to_hex(),
                "--workload",
                &why.workload.unwrap().hash.to_hex(),
            ]),
            0,
        );
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        let records = check_records(&index_dir(&data), &v);
        assert!(
            records
                .iter()
                .any(|(_, r)| matches!(r, Record::Certificate(_))),
            "the fix wrote a certificate"
        );
        let mut stamps = Vec::new();
        for (_, record) in &records {
            created_at(&serde_json::to_value(record).unwrap(), &mut stamps);
        }
        assert!(stamps.len() >= records.len(), "{stamps:?}");
        stamps
    };
    for stamp in stamps_of(None) {
        let b = stamp.as_bytes();
        let millis = b.len() == 24
            && b[10] == b'T'
            && b[19] == b'.'
            && b[20..23].iter().all(u8::is_ascii_digit)
            && b[23] == b'Z';
        assert!(millis, "{stamp:?} is not UTC with milliseconds");
    }
    for stamp in stamps_of(Some("2026-01-02T03:04:05.123456789+00:00")) {
        assert_eq!(stamp, "2026-01-02T03:04:05.123Z");
    }
}

/// Every `created_at` string anywhere in `v`.
fn created_at(v: &serde_json::Value, out: &mut Vec<String>) {
    match v {
        serde_json::Value::Object(m) => {
            for (k, x) in m {
                match (k.as_str(), x) {
                    ("created_at", serde_json::Value::String(s)) => out.push(s.clone()),
                    _ => created_at(x, out),
                }
            }
        }
        serde_json::Value::Array(a) => a.iter().for_each(|x| created_at(x, out)),
        _ => {}
    }
}

/// GH Archive's shape: one event per line, `actor` and `repo` objects.
fn nested_jsonl(file: &Path, rows: i64) {
    use std::fmt::Write as _;
    std::fs::create_dir_all(file.parent().unwrap()).unwrap();
    let mut s = String::new();
    for k in 0..rows {
        let _ = writeln!(
            s,
            r#"{{"id":{k},"type":"t{}","actor":{{"id":{},"login":"a{k}"}},"repo":{{"id":{},"name":"r"}}}}"#,
            k % 3,
            k % 7,
            k / 4
        );
    }
    std::fs::write(file, s).unwrap();
}

/// Defect 1: a dataset with nested columns indexes and plans on a flat key,
/// in JSON lines and in Parquet; the profiler lists each nested column with
/// its reason; a nested key column is refused naming the column, in a
/// sentence with no Rust debug output.
#[test]
fn defect_1_nested_columns_index_on_a_flat_key_and_a_nested_key_is_refused_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let json = dir.path().join("json");
    nested_jsonl(&json.join("events.jsonl"), 60);
    let parquet = dir.path().join("parquet");
    let repo = StructArray::from(vec![
        (
            Arc::new(Field::new("id", arrow::datatypes::DataType::Int64, true)),
            Arc::new(Int64Array::from_iter_values((0..40).map(|k| k / 4))) as ArrayRef,
        ),
        (
            Arc::new(Field::new("name", arrow::datatypes::DataType::Utf8, true)),
            Arc::new(StringArray::from_iter_values(
                (0..40).map(|k| format!("r{k}")),
            )),
        ),
    ]);
    table(
        &parquet.join("part-0.parquet"),
        vec![
            (
                "id",
                Arc::new(Int64Array::from_iter_values(0..40)) as ArrayRef,
            ),
            ("repo", Arc::new(repo)),
        ],
    );
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[3, 7]);
    for (data, format) in [(&json, "jsonl"), (&parquet, "parquet")] {
        run(
            &["--format", format, "index", path(data), "--key", "exact:id"],
            0,
        );
        let plan: bellwether::PlanResult = typed(&run(
            &[
                "--json",
                "--format",
                format,
                "locate",
                path(data),
                "--keys",
                path(&kf),
            ],
            0,
        ));
        assert!(plan.planned.row_groups >= 1, "{format}: {plan:?}");
        let why = run(
            &["--format", format, "why", path(data), "--from", path(&kf)],
            0,
        );
        assert!(
            why.contains("repo: a nested struct column, not a key candidate"),
            "{format}:\n{why}"
        );
        let out = bw()
            .args([
                "--format",
                format,
                "index",
                path(data),
                "--key",
                "exact:repo",
            ])
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(4), "{format}");
        let err = String::from_utf8(out.stderr).unwrap();
        assert!(err.contains("key column \"repo\""), "{format}: {err}");
        assert!(
            err.contains("nested struct column cannot be a key"),
            "{err}"
        );
        assert!(err.contains("flatten"), "{err}");
        assert!(!err.contains("Field {"), "no Debug output: {err}");
    }
}

/// Defect 2: a dotted name into a nested column is refused with what to do
/// (flatten first), wherever a column is named.
#[test]
fn defect_2_a_nested_field_path_is_refused_with_flatten_first() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("json");
    nested_jsonl(&data.join("events.jsonl"), 30);
    for args in [
        vec!["why", path(&data), "--columns", "repo.id"],
        vec!["index", path(&data), "--key", "exact:repo.id"],
    ] {
        let out = bw()
            .args([&["--format", "jsonl"][..], &args[..]].concat())
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(4), "{args:?}");
        let err = String::from_utf8(out.stderr).unwrap();
        assert!(
            err.contains(
                "\"repo\" is a nested struct column, and a nested field cannot be addressed"
            ),
            "{args:?}: {err}"
        );
        assert!(err.contains("SELECT repo.id AS repo_id"), "{err}");
    }
}

/// Columns `zone` (5 values) and `vendor` (3 values) in `files` files of 30
/// rows each: every value and every pair in every row group, the shape of
/// time-ordered trip data keyed by a low-cardinality column.
fn everywhere(dir: &Path, files: i64) {
    for f in 0..files {
        let ks: Vec<i64> = (f * 30..f * 30 + 30).collect();
        table(
            &dir.join(format!("part-{f}.parquet")),
            vec![
                (
                    "zone",
                    Arc::new(Int64Array::from_iter_values(ks.iter().map(|k| k % 5))) as ArrayRef,
                ),
                (
                    "vendor",
                    Arc::new(Int64Array::from_iter_values(ks.iter().map(|k| k % 3))),
                ),
            ],
        );
    }
}

/// Defects 7 and 8: where no key prunes, `why` says so without keys (lint
/// `no_discriminating_column`) and, with keys, names the layout as the
/// culprit instead of a missing index, with the lowest-fanout column.
#[test]
fn defects_7_8_why_names_the_layout_when_no_key_prunes() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    everywhere(&data, 4);
    let plain: WhyReport = typed(&run(&["--json", "why", path(&data)], 0));
    let lint = plain
        .profile
        .lints
        .iter()
        .find(|l| l.rule == "no_discriminating_column")
        .unwrap_or_else(|| panic!("{:?}", plain.profile.lints));
    assert!(
        lint.message.contains("4 of 4 row groups"),
        "{}",
        lint.message
    );
    assert!(
        lint.message.contains("not a missing index"),
        "{}",
        lint.message
    );
    let human = run(&["why", path(&data)], 0);
    assert!(human.contains("no_discriminating_column"), "{human}");

    let kf = dir.path().join("k.parquet");
    keys(&kf, "zone", &[0, 1, 2, 3, 4]);
    let why: WhyReport = typed(&run(
        &["--json", "why", path(&data), "--from", path(&kf)],
        0,
    ));
    let culprit = why.culprit.expect("a culprit");
    let bellwether::CulpritReason::Layout {
        least_row_groups,
        row_groups,
        lowest_fanout,
    } = &culprit.reason
    else {
        panic!("{culprit:?}")
    };
    assert_eq!((*least_row_groups, *row_groups), (4, 4));
    let lowest = lowest_fanout.as_ref().unwrap();
    assert_eq!(lowest.columns.len(), 1);
    let human = run(&["why", path(&data), "--from", path(&kf)], 0);
    let culprit_line = human
        .lines()
        .find(|l| l.starts_with("culprit"))
        .unwrap_or_else(|| panic!("{human}"));
    assert!(
        culprit_line.contains("the layout, not a missing index"),
        "{culprit_line}"
    );
    assert!(
        culprit_line.contains("lowest-fanout column"),
        "{culprit_line}"
    );
    assert!(!culprit_line.contains("no index serves"), "{culprit_line}");

    // Where the floor prunes, the culprit stays the missing index.
    let pruned = dir.path().join("pruned");
    corpus(&pruned, 2, 3, 20, 100);
    let kf = dir.path().join("k2.parquet");
    keys(&kf, "id", &[4, 9, 60]);
    let why: WhyReport = typed(&run(
        &["--json", "why", path(&pruned), "--from", path(&kf)],
        0,
    ));
    assert!(
        matches!(
            why.culprit.unwrap().reason,
            bellwether::CulpritReason::NoIndexServes
        ),
        "the floor prunes here"
    );
}

/// `kind` (3 values) touches every row group; `(repo, kind)` touches one:
/// `low_selectivity_single_column` fires on `kind`.
fn low_selectivity(dir: &Path) {
    for f in 0..4i64 {
        let ks: Vec<i64> = (f * 30..f * 30 + 30).collect();
        table(
            &dir.join(format!("part-{f}.parquet")),
            vec![
                (
                    "repo",
                    Arc::new(Int64Array::from_iter_values(ks.iter().map(|k| k / 3))) as ArrayRef,
                ),
                (
                    "kind",
                    Arc::new(Int64Array::from_iter_values(ks.iter().map(|k| k % 3))),
                ),
            ],
        );
    }
}

/// Defect 10: with a key file the lints know which columns lookups supply,
/// so none says there is no recorded workload; without one the assumption
/// is still stated.
#[test]
fn defect_10_a_key_file_informs_the_lints() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    low_selectivity(&data);
    let message = |r: &WhyReport| {
        r.profile
            .lints
            .iter()
            .find(|l| l.rule == "low_selectivity_single_column")
            .unwrap_or_else(|| panic!("{:?}", r.profile.lints))
            .message
            .clone()
    };
    let plain: WhyReport = typed(&run(&["--json", "why", path(&data)], 0));
    assert!(
        message(&plain).contains("no recorded workload"),
        "{}",
        message(&plain)
    );
    let kf = dir.path().join("k.parquet");
    keys(&kf, "repo", &[1, 20]);
    let with_keys: WhyReport = typed(&run(
        &["--json", "why", path(&data), "--from", path(&kf)],
        0,
    ));
    assert!(
        !message(&with_keys).contains("no recorded workload"),
        "{}",
        message(&with_keys)
    );
    let human = run(&["why", path(&data), "--from", path(&kf)], 0);
    assert!(!human.contains("no recorded workload"), "{human}");
}

/// Defect 11: a `.json` file of JSON lines is read and indexed; one whose
/// first line is an object but a later line is not is refused by name and
/// line; a pretty-printed document or an array is not JSON lines, and a
/// folder of nothing else names the accepted extensions.
#[test]
fn defect_11_dot_json_files_are_sniffed_for_json_lines() {
    let dir = tempfile::tempdir().unwrap();
    let good = dir.path().join("good");
    nested_jsonl(&good.join("2024-01-01-0.json"), 30);
    let why: WhyReport = typed(&run(
        &["--json", "--format", "jsonl", "why", path(&good)],
        0,
    ));
    assert_eq!(why.dataset.files, 1);
    assert_eq!(why.dataset.rows, 30);
    run(
        &[
            "--format",
            "jsonl",
            "index",
            path(&good),
            "--key",
            "exact:id",
        ],
        0,
    );
    // Indexed `.json` files are tracked like any other: no stale refusal.
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[3]);
    run(
        &[
            "--format",
            "jsonl",
            "locate",
            path(&good),
            "--keys",
            path(&kf),
        ],
        0,
    );

    // An index inside the data root writes `.json` files of its own (a
    // one-line JSON object is valid JSON lines); they are never data.
    let inner = dir.path().join("inner");
    nested_jsonl(&inner.join("2024-01-01-0.json"), 30);
    let idx = inner.join("idx");
    let with_index = |rest: &[&str], code: i32| {
        run(
            &[&["--index", path(&idx), "--format", "jsonl"][..], rest].concat(),
            code,
        )
    };
    with_index(&["index", path(&inner), "--key", "exact:id"], 0);
    with_index(&["locate", path(&inner), "--keys", path(&kf)], 0);
    let again = with_index(&["index", path(&inner), "--key", "exact:id"], 0);
    assert!(again.contains("nothing to do"), "{again}");
    let why: WhyReport = typed(&with_index(&["--json", "why", path(&inner)], 0));
    assert_eq!((why.dataset.files, why.untracked.len()), (1, 0));

    // A damaged line after a good first line is refused by name and line.
    let damaged = dir.path().join("damaged");
    std::fs::create_dir_all(&damaged).unwrap();
    std::fs::write(damaged.join("events.json"), "{\"id\": 1}\n{\"id\": \n").unwrap();
    let out = bw()
        .args(["--format", "jsonl", "why", path(&damaged)])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(4));
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(err.contains("events.json"), "{err}");
    assert!(err.contains("line 2 is not one"), "{err}");

    // A pretty-printed document, and a JSON array, are not JSON lines: not
    // read, and a folder of nothing else names what is accepted.
    let array = dir.path().join("array");
    std::fs::create_dir_all(&array).unwrap();
    std::fs::write(array.join("doc.json"), "[{\"id\": 1}]\n").unwrap();
    std::fs::write(array.join("pretty.json"), "{\n  \"id\": 1\n}\n").unwrap();
    let out = bw()
        .args(["--format", "jsonl", "why", path(&array)])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(4));
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(
        err.contains(".jsonl, .ndjson, and .json files whose first byte is"),
        "{err}"
    );
}

/// Brute force over the taxi-shaped files: the row groups holding a version
/// of zone `pu` in `1..=4` whose `pickup_ts` (µs) lies in the zone's window
/// `[hi − lookback, hi]`, `hi` being `asof` or the zone's newest version,
/// and their exact compressed bytes from the Parquet footers.
fn window_oracle(dir: &Path, asof: Option<i64>, lookback: i64) -> (u64, u64) {
    use arrow::array::AsArray;
    use arrow::datatypes::{Int64Type, TimestampMicrosecondType};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let mut cells: Vec<(u64, Vec<(i64, i64)>)> = Vec::new();
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
        .collect();
    files.sort();
    for f in files {
        let b = ParquetRecordBatchReaderBuilder::try_new(File::open(&f).unwrap()).unwrap();
        let meta = b.metadata().clone();
        for rg in 0..meta.num_row_groups() {
            let bytes = u64::try_from(meta.row_group(rg).compressed_size()).unwrap();
            let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&f).unwrap())
                .unwrap()
                .with_row_groups(vec![rg])
                .build()
                .unwrap();
            let mut rows = Vec::new();
            for batch in reader {
                let batch = batch.unwrap();
                let pu = batch.column(0).as_primitive::<Int64Type>();
                let ts = batch.column(1).as_primitive::<TimestampMicrosecondType>();
                rows.extend((0..batch.num_rows()).map(|i| (pu.value(i), ts.value(i))));
            }
            cells.push((bytes, rows));
        }
    }
    let newest = |z: i64| {
        cells
            .iter()
            .flat_map(|(_, r)| r.iter())
            .filter(|(p, _)| *p == z)
            .map(|(_, t)| *t)
            .max()
            .unwrap()
    };
    let (mut row_groups, mut bytes) = (0, 0);
    for (b, rows) in &cells {
        let hit = rows.iter().any(|(z, t)| {
            let hi = asof.unwrap_or_else(|| newest(*z));
            hi - lookback <= *t && *t <= hi
        });
        if hit {
            row_groups += 1;
            bytes += b;
        }
    }
    (row_groups, bytes)
}

/// Strategy request 3: `why --mode locate|latest|asof` with `--at` and
/// `--lookback` prices each candidate under the stated lookup through the
/// replay crate's temporal path. A lookback window reads every row group
/// holding a version of a key in `[end − N, end]`, which the brute-force
/// oracle confirms to the byte; inconsistent combinations are usage errors.
#[test]
fn request_3_why_mode_prices_latest_and_asof_lookback_windows() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("taxi");
    taxi_shaped(&data);
    let kf = dir.path().join("pu.parquet");
    keys(&kf, "pu", &[1, 2, 3, 4]);
    let why = |extra: &[&str]| -> WhyReport {
        let mut args = vec!["--json", "why", path(&data), "--from", path(&kf)];
        args.extend_from_slice(extra);
        typed(&run(&args, 0))
    };
    let temporal = KeySpec::Temporal {
        columns: vec!["pu".into()],
        ts_column: "pickup_ts".into(),
        counts: false,
    };
    let floor = |r: &WhyReport| {
        r.candidates
            .iter()
            .find(|c| c.spec == temporal)
            .map(|c| c.proposed)
            .expect("the temporal floor is priced")
    };

    // --mode latest is what --latest-by alone infers: the same workload.
    let inferred = why(&["--latest-by", "pickup_ts"]);
    let latest = why(&["--mode", "latest", "--latest-by", "pickup_ts"]);
    assert_eq!(
        latest.workload.as_ref().unwrap().hash,
        inferred.workload.as_ref().unwrap().hash
    );
    assert_eq!(floor(&latest).row_groups, 2);
    // --mode locate prices point lookups: every zone is in every row group.
    let locate = why(&["--mode", "locate"]);
    assert_eq!(locate.workload.as_ref().unwrap().query_kind, "locate");
    assert!(locate.recommended.is_none());

    // latest with a 30 s lookback: each zone's versions in [newest − 30 s,
    // newest] live in 3 row groups, where the newest alone lives in 2.
    let window = why(&[
        "--mode",
        "latest",
        "--latest-by",
        "pickup_ts",
        "--lookback",
        "30s",
    ]);
    let w = window.workload.as_ref().unwrap();
    assert_eq!(w.query_kind, "latest");
    assert_eq!(
        w.lookback,
        Some(bellwether::LookbackWindow {
            given: "30s".into(),
            length: 30_000_000,
        })
    );
    let (rgs, bytes) = window_oracle(&data, None, 30_000_000);
    assert_eq!(rgs, 3);
    assert_eq!(
        (floor(&window).row_groups, floor(&window).bytes),
        (rgs, bytes)
    );
    let rec = window.recommended.as_ref().expect("the window prunes");
    assert_eq!(rec.spec, temporal);
    assert_eq!(rec.lookback, w.lookback);
    assert_ne!(w.hash, latest.workload.as_ref().unwrap().hash);

    // asof at 30 s with a 20 s lookback: versions in [10 s, 30 s].
    let at = "1970-01-01T00:00:30Z";
    let asof = why(&[
        "--mode",
        "asof",
        "--latest-by",
        "pickup_ts",
        "--at",
        at,
        "--lookback",
        "20s",
    ]);
    let (rgs, bytes) = window_oracle(&data, Some(30_000_000), 20_000_000);
    assert_eq!(rgs, 3);
    assert_eq!((floor(&asof).row_groups, floor(&asof).bytes), (rgs, bytes));
    let w = asof.workload.as_ref().unwrap();
    assert_eq!(w.query_kind, "asof");
    assert!(matches!(
        w.lookup,
        Some(LookupKind::Asof { ts: 30_000_000, .. })
    ));
    // A recorded window workload replays as recorded under --workload with
    // the same options; other options are refused, not relabeled.
    let replay = |lookback: &str, code: i32| {
        run(
            &[
                "--json",
                "why",
                path(&data),
                "--workload",
                &w.hash.to_hex(),
                "--latest-by",
                "pickup_ts",
                "--at",
                at,
                "--lookback",
                lookback,
            ],
            code,
        )
    };
    let again: WhyReport = typed(&replay("20s", 0));
    assert_eq!(floor(&again), floor(&asof));
    assert_eq!(again.workload.as_ref().unwrap().hash, w.hash);
    replay("10s", 4);

    let human = run(
        &[
            "why",
            path(&data),
            "--from",
            path(&kf),
            "--mode",
            "asof",
            "--latest-by",
            "pickup_ts",
            "--at",
            at,
            "--lookback",
            "20s",
        ],
        0,
    );
    assert!(
        human.contains(&format!("funnel: 1 run (asof {at}, lookback 20s)")),
        "{human}"
    );
    assert!(
        human.contains("only row groups holding a version in each key's window"),
        "{human}"
    );

    // Inconsistent combinations are usage errors (exit 2) naming the fix.
    let usage = |extra: &[&str], says: &str| {
        let mut args = vec!["why", path(&data), "--from", path(&kf)];
        args.extend_from_slice(extra);
        let out = bw().args(&args).output().unwrap();
        assert_eq!(out.status.code(), Some(2), "{extra:?}");
        let err = String::from_utf8(out.stderr).unwrap();
        assert!(err.contains(says), "{extra:?}: {err}");
    };
    usage(
        &["--mode", "asof", "--latest-by", "pickup_ts"],
        "pass --at <instant>",
    );
    usage(&["--mode", "latest"], "pass --latest-by <column>");
    usage(
        &["--mode", "locate", "--latest-by", "pickup_ts"],
        "--mode locate prices point lookups",
    );
    usage(
        &["--mode", "latest", "--latest-by", "pickup_ts", "--at", at],
        "pass --mode asof",
    );
    usage(
        &["--latest-by", "pickup_ts", "--lookback", "3x"],
        "--lookback 252d",
    );
    // A length that is not a whole number of the column's unit, or longer
    // than it can measure, is a usage error with a lookback to pass, never
    // rounded: an integer column takes a bare integer.
    usage(
        &["--latest-by", "pu", "--lookback", "1d"],
        "for example --lookback 86400 if pu counts seconds",
    );
    usage(
        &["--latest-by", "pickup_ts", "--lookback", "99999999w"],
        "the longest lookback on it is 15250284w",
    );
    usage(
        &["--latest-by", "pickup_ts", "--lookback", "1.5d"],
        "is not a whole number",
    );

    // Indexed, the build planner prices the current temporal key's window:
    // the same rows and bytes as the oracle.
    run(&["index", path(&data), "--key", "temporal:pu@pickup_ts"], 0);
    let indexed = why(&["--latest-by", "pickup_ts", "--lookback", "30s"]);
    let current = indexed.current.unwrap();
    assert_eq!(
        (current.row_groups, current.bytes),
        window_oracle(&data, None, 30_000_000)
    );
}

/// `(id, fare)` rows with `fare` a Float64 (`None` is a null).
fn fares(dir: &Path, values: &[Option<f64>]) {
    let ids: Vec<i64> = (0..i64::try_from(values.len()).unwrap()).collect();
    table(
        &dir.join("part-0.parquet"),
        vec![
            ("id", Arc::new(Int64Array::from(ids)) as ArrayRef),
            ("fare", Arc::new(Float64Array::from(values.to_vec()))),
        ],
    );
}

/// Strategy request 4: `--totals fare:dec(2)` totals a Float64 column by
/// exact quantization: each value's exact binary value rounded half to
/// even at scale 2, summed as an `i128`. The spec is printed and recorded
/// in the certificate's `totals_delta` labels; NaN is an error naming the
/// key; a bare float stays a usage error showing the `:dec(S)` form.
#[test]
fn request_4_float_totals_by_exact_quantization() {
    let dir = tempfile::tempdir().unwrap();
    let side = |name: &str, values: &[Option<f64>]| {
        let d = dir.path().join(name);
        fares(&d, values);
        d
    };
    // 0.125 is a tie (to 0.12), 0.375 a tie (to 0.38), and 2.675 is
    // 2.67499999999999982236431605997495353221893310546875 (to 2.67): 3.17.
    // Summing the floats first and rounding the sum gives 3.18.
    let values = [Some(0.125), Some(0.375), Some(2.675), None];
    let left = side("left", &values);
    let right = side("right", &values);
    let args = |r: &Path, totals: &str| -> Vec<String> {
        [
            "reconcile",
            path(&left),
            path(r),
            "--key",
            "id",
            "--totals",
            totals,
        ]
        .iter()
        .map(ToString::to_string)
        .collect()
    };
    let json = |r: &Path, totals: &str, code: i32| -> ReconcileResult {
        let mut a = vec!["--json".to_string()];
        a.extend(args(r, totals));
        typed(&run(
            &a.iter().map(String::as_str).collect::<Vec<_>>(),
            code,
        ))
    };
    let pass = json(&right, "fare:dec(2)", 0);
    let t = &pass.outcome.totals[0];
    assert_eq!(t.column, "fare:dec(2)");
    assert_eq!(
        (t.left.render(), t.right.render()),
        ("3.17".into(), "3.17".into())
    );
    assert_eq!(t.left.scale, 2);
    let human = run(
        &args(&right, "fare:dec(2)")
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        0,
    );
    assert!(
        human.contains(
            "fare:dec(2): each float value's exact binary value rounded half to even at 2 decimal places"
        ),
        "{human}"
    );
    assert!(human.contains("3.17"), "{human}");

    // One cent apart: the certificate fails and its totals_delta label
    // records the quantization spec.
    let off = side("off", &[Some(0.125), Some(0.385), Some(2.675), None]);
    let fail = json(&off, "fare:dec(2)", 1);
    let Record::Certificate(c) = &fail.certificate.record else {
        panic!("{:?}", fail.certificate.record)
    };
    let (label, l, r) = &c.body.totals_delta[0];
    assert!(label.contains(r#""column":"fare:dec(2)""#), "{label}");
    assert_eq!((l.as_str(), r.as_str()), ("3.17", "3.18"));

    // NaN or infinity is an error naming the key (exit 4).
    let nan = side("nan", &[Some(0.125), Some(f64::NAN), Some(2.675), None]);
    let out = bw().args(args(&nan, "fare:dec(2)")).output().unwrap();
    assert_eq!(out.status.code(), Some(4));
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(err.contains("key (id=1) has NaN in \"fare\""), "{err}");
    assert!(err.contains("fare:dec(2) cannot total"), "{err}");

    // Usage errors (exit 2): a bare float shows the :dec(S) form; :dec on a
    // decimal, and a malformed scale, say what to write.
    let usage = |totals: &str, says: &str| {
        let out = bw().args(args(&right, totals)).output().unwrap();
        assert_eq!(out.status.code(), Some(2), "{totals}");
        let err = String::from_utf8(out.stderr).unwrap();
        assert_eq!(err.trim_end().lines().count(), 1, "{err}");
        assert!(err.contains(says), "{totals}: {err}");
    };
    usage("fare", "--totals fare:dec(4)");
    usage("fare:dec(x)", "write for example volume:dec(4)");
    usage("fare:dec(39)", "a scale from 0 to 38");
    usage("id:dec(2)", "quantizes a float column");
}

/// Strategy request 5: `--memory-budget` takes binary units (`8G`, `512M`)
/// as well as bytes; a malformed value is a usage error naming the forms.
#[test]
fn request_5_memory_budget_takes_human_units() {
    let dir = tempfile::tempdir().unwrap();
    let (left, right) = (dir.path().join("left"), dir.path().join("right"));
    corpus(&left, 1, 2, 20, 40);
    corpus(&right, 1, 2, 20, 40);
    let budget = |given: &str| -> u64 {
        let r: ReconcileResult = typed(&run(
            &[
                "--json",
                "reconcile",
                path(&left),
                path(&right),
                "--key",
                "id",
                "--memory-budget",
                given,
            ],
            0,
        ));
        r.outcome.memory.budget_bytes
    };
    assert_eq!(budget("268435456"), 268_435_456);
    assert_eq!(budget("512M"), 512 << 20);
    assert_eq!(budget("8G"), 8 << 30);
    assert_eq!(budget("64KiB"), 64 << 10);
    assert_eq!(budget("8GB"), 8 << 30);
    assert_eq!(budget("8 g"), 8 << 30);
    for bad in ["8X", "1.5G", "-1", "G", "99999999999T"] {
        let out = bw()
            .args([
                "reconcile",
                path(&left),
                path(&right),
                "--key",
                "id",
                &format!("--memory-budget={bad}"),
            ])
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(2), "{bad}");
        let err = String::from_utf8(out.stderr).unwrap();
        assert!(err.contains("--memory-budget"), "{bad}: {err}");
    }
    let out = bw()
        .args([
            "reconcile",
            path(&left),
            path(&right),
            "--key",
            "id",
            "--memory-budget",
            "8X",
        ])
        .output()
        .unwrap();
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(err.contains("binary unit, K, M, G, or T"), "{err}");
}

/// Four one-row-group files of ten rows, file `f` holding ids `f, f + 4,
/// ..., f + 36` (so each file's min/max spans nearly every id and only a
/// key prunes), with a struct, a list, and a map column beside the flat
/// `id`.
fn nested_lake(dir: &Path) {
    use arrow::array::{Int64Builder, ListArray, MapBuilder, StringBuilder};
    use arrow::datatypes::Int64Type;
    for f in 0..4i64 {
        let ids: Vec<i64> = (0..10).map(|i| f + 4 * i).collect();
        let repo = StructArray::from(vec![(
            Arc::new(Field::new("id", DataType::Int64, true)),
            Arc::new(Int64Array::from_iter_values(ids.iter().map(|k| k / 4))) as ArrayRef,
        )]);
        let tags = ListArray::from_iter_primitive::<Int64Type, _, _>(
            ids.iter().map(|k| Some(vec![Some(*k), Some(k + 1)])),
        );
        let mut labels = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        for k in &ids {
            labels.keys().append_value(format!("k{k}"));
            labels.values().append_value(*k);
            labels.append(true).unwrap();
        }
        table(
            &dir.join(format!("part-{f}.parquet")),
            vec![
                ("id", Arc::new(Int64Array::from(ids)) as ArrayRef),
                ("repo", Arc::new(repo)),
                ("tags", Arc::new(tags)),
                ("labels", Arc::new(labels.finish())),
            ],
        );
    }
}

/// Bake-off port (from the Argon defects implementation): `fix` leaves
/// columns with no canonical encoding out of the row hash, so a flat key on
/// a lake with struct, list, and map columns verifies and is applied; and
/// `propose` refuses a nested key column with the nested-column sentence.
#[test]
fn fix_applies_a_flat_key_on_a_lake_with_nested_columns_and_propose_refuses_a_nested_key() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("lake");
    nested_lake(&data);
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[3, 17]);
    let why: WhyReport = typed(&run(
        &["--json", "why", path(&data), "--from", path(&kf)],
        0,
    ));
    let rec = why
        .recommended
        .as_ref()
        .expect("exact:id prunes 4 files to 2");
    let fix: FixResult = typed(&run(
        &[
            "--json",
            "fix",
            path(&data),
            "--proposal",
            &rec.proposal.to_hex(),
            "--workload",
            &why.workload.as_ref().unwrap().hash.to_hex(),
        ],
        0,
    ));
    assert!(
        matches!(fix.outcome, FixOutcome::Applied { .. }),
        "{:?}",
        fix.outcome
    );
    let Some(bellwether::Written {
        record: Record::Certificate(c),
        ..
    }) = &fix.certificate
    else {
        panic!("{:?}", fix.certificate)
    };
    assert_eq!(c.body.ignored_columns, vec!["labels", "repo", "tags"]);

    let evidence = why.evidence.hash.unwrap().to_hex();
    let out = bw()
        .args([
            "propose",
            path(&data),
            "--evidence",
            &evidence,
            "--by",
            "test",
            "--key",
            "exact:repo",
        ])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(4));
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(err.contains("key column \"repo\""), "{err}");
    assert!(
        err.contains("nested struct column cannot be a key"),
        "{err}"
    );
    assert!(!err.contains("Field {"), "{err}");
}

/// Defect 14 (port): `docs/agent-loop.md` no longer says the engine that
/// writes the records does not exist.
#[test]
fn defect_14_agent_loop_doc_drops_the_stale_status_line() {
    let doc = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/agent-loop.md"),
    )
    .unwrap();
    assert!(!doc.contains("the engine that writes them does not yet"));
}

/// Defect 4 on the apply tier: `fix --certificate` re-scores and refuses a
/// proposal whose replay saves nothing, unless `--allow-no-gain`.
#[test]
fn defect_4_fix_certificate_refuses_a_proposal_that_saves_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 2, 3, 20, 100);
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[1, 25, 45, 65, 85]);
    let why: WhyReport = typed(&run(
        &["--json", "why", path(&data), "--from", path(&kf)],
        0,
    ));
    let workload = why.workload.unwrap().hash.to_hex();
    let p = run(
        &[
            "propose",
            path(&data),
            "--evidence",
            &why.evidence.hash.unwrap().to_hex(),
            "--by",
            "test",
            "--key",
            "exact:id",
        ],
        0,
    );
    let proposal = p.trim().trim_start_matches("proposal ").to_string();
    let verified: FixResult = typed(&run(
        &[
            "--json",
            "fix",
            path(&data),
            "--proposal",
            &proposal,
            "--workload",
            &workload,
            "--verify-only",
        ],
        0,
    ));
    let cert = verified.certificate.expect("verified").hash.to_hex();
    let refused: FixResult = typed(&run(
        &["--json", "fix", path(&data), "--certificate", &cert],
        3,
    ));
    assert!(
        matches!(
            refused.outcome,
            FixOutcome::Refused {
                refusal: Refusal::NothingToGain { .. }
            }
        ),
        "{:?}",
        refused.outcome
    );
    let applied: FixResult = typed(&run(
        &[
            "--json",
            "fix",
            path(&data),
            "--certificate",
            &cert,
            "--allow-no-gain",
        ],
        0,
    ));
    assert!(
        matches!(applied.outcome, FixOutcome::Applied { .. }),
        "{:?}",
        applied.outcome
    );
}

/// Defect 11: a `.json` file that is not JSON lines beside JSON lines files
/// is refused by name with the accepted extensions, never silently dropped.
#[test]
fn defect_11_a_non_json_lines_dot_json_beside_data_is_refused_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("mixed");
    nested_jsonl(&data.join("a.jsonl"), 5);
    std::fs::write(data.join("arr.json"), "[{\"id\": 1}]\n").unwrap();
    let out = bw()
        .args(["--format", "jsonl", "why", path(&data)])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(4));
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(err.contains("arr.json"), "{err}");
    assert!(err.contains("is not JSON lines"), "{err}");
    assert!(err.contains(".jsonl, .ndjson"), "{err}");
}

/// Defect 7 wording: when one value sits in few row groups but the keys
/// together cover them all, the culprit says so instead of claiming each
/// value is spread over nearly every row group.
#[test]
fn defect_7_layout_culprit_says_the_keys_together_cover_the_row_groups() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 2, 3, 20, 100);
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[1, 25, 45, 65, 85]);
    let human = run(&["why", path(&data), "--from", path(&kf)], 0);
    assert!(
        human.contains("the requested keys together cover nearly every row group"),
        "{human}"
    );
    assert!(!human.contains("each value is spread"), "{human}");
}
