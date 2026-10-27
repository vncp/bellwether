//! Regressions from the README captures (2026-10-25): the human output's
//! wording (units, singular counts, the sidecar that planned a lookup),
//! `why`'s unindexed cost (the files' own min/max statistics, not a full
//! scan) and `fix`'s gain gate on the same Replay, and a CSV `--schema`
//! whose timestamps name a time zone.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

mod common;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether::types::Record;
use bellwether::{FixOutcome, FixResult, Refusal, ReportResult, Score, WhyReport};
use common::{bw, corpus, keys, path, run, typed};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// One Parquet file whose row group `r` holds the ids `groups[r]`, each
/// row with a 200-byte payload so every row group has real bytes.
fn grouped(file: &Path, groups: &[Vec<i64>]) {
    std::fs::create_dir_all(file.parent().unwrap()).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(usize::MAX))
        .build();
    let mut w = ArrowWriter::try_new(
        File::create(file).unwrap(),
        Arc::clone(&schema),
        Some(props),
    )
    .unwrap();
    for ids in groups {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids.clone())) as ArrayRef,
                Arc::new(StringArray::from_iter_values(
                    ids.iter().map(|i| format!("{i:0>200}")),
                )),
            ],
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.flush().unwrap();
    }
    w.close().unwrap();
}

/// `propose --key <spec>` citing `why`'s Evidence; the proposal hash.
fn propose(data: &Path, why: &WhyReport, spec: &str) -> String {
    let out = run(
        &[
            "propose",
            path(data),
            "--evidence",
            &why.evidence.hash.unwrap().to_hex(),
            "--by",
            "test",
            "--key",
            spec,
        ],
        0,
    );
    out.trim().trim_start_matches("proposal ").to_string()
}

/// Item 4: on files sorted by the key, the files' own min/max statistics
/// already skip to the row groups holding the keys, so `exact:id` saves
/// nothing over them. The unindexed read is priced with them (the Replay
/// says so in `unserved`), the full scan is printed above it as context,
/// nothing is recommended, and `fix` refuses `nothing_to_gain` on the same
/// numbers.
#[test]
fn on_sorted_files_why_and_fix_measure_a_key_against_min_max_pruning() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    // One file, 8 row groups of 50 rows, ids 0..400 in order.
    corpus(&data, 1, 8, 50, 1_000_000);
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[5, 7, 120]);
    let why: WhyReport = typed(&run(
        &["--json", "why", path(&data), "--from", path(&kf)],
        0,
    ));
    let current = why.current.unwrap();
    let full = why.full_scan.expect("no index over Parquet: the full scan");
    assert_eq!(full.row_groups, 8, "the full scan reads every row group");
    assert_eq!(current.row_groups, 2, "ids 5, 7 in row group 0; 120 in 2");
    let floor = why
        .candidates
        .iter()
        .find(|c| c.spec.name() == "exact:id")
        .unwrap();
    let Record::Replay(rp) = &floor.replay.record else {
        panic!()
    };
    assert_eq!(rp.unserved.as_deref(), Some("parquet_stats"));
    assert!(
        floor.proposed.bytes < full.bytes,
        "the floor beats a full scan"
    );
    assert_eq!(
        floor.proposed, current,
        "but reads exactly what min/max pruning reads"
    );
    assert!(why.recommended.is_none(), "{:?}", why.recommended);

    let human = run(&["why", path(&data), "--from", path(&kf)], 0);
    assert!(human.contains("funnel: 1 run,"), "{human}");
    assert!(human.contains("  full scan "), "{human}");
    assert!(
        human.contains("current: no index (min/max pruning)"),
        "{human}"
    );
    assert!(!human.contains("current (no index)"), "{human}");
    assert!(
        human.contains(
            "recommended: none; no candidate reads fewer bytes than the files' own min/max \
             statistics already read without an index"
        ),
        "{human}"
    );
    assert!(
        human.contains("min/max statistics already narrow it to 2 of the 8 row groups"),
        "{human}"
    );

    let proposal = propose(&data, &why, "exact:id");
    let workload = why.workload.as_ref().unwrap().hash.to_hex();
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
    let Some(Score::Replayed {
        replay,
        current: scored,
        proposed,
    }) = &r.score
    else {
        panic!("{:?}", r.score)
    };
    assert_eq!(*scored, current, "fix scores what why priced");
    assert_eq!(*proposed, floor.proposed);
    assert!(matches!(&replay.record, Record::Replay(rp) if rp.unserved.is_some()));
    let FixOutcome::Refused {
        refusal:
            Refusal::NothingToGain {
                current_bytes,
                proposed_bytes,
                ..
            },
    } = &r.outcome
    else {
        panic!("{:?}", r.outcome)
    };
    assert_eq!(
        (*current_bytes, *proposed_bytes),
        (current.bytes, current.bytes)
    );
    assert!(r.certificate.is_none(), "nothing verified");
    let human = fix(&[], 3);
    assert!(
        human.contains("a lookup no key serves is priced as the files' own min/max statistics"),
        "{human}"
    );
    assert!(human.contains("--allow-no-gain"), "{human}");

    // A verify-only run is not gated; applying its certificate re-scores
    // with the same min/max pricing and refuses the same way.
    let verified: FixResult = typed(&fix(&["--json", "--verify-only"], 0));
    let cert = verified.certificate.expect("verified").hash.to_hex();
    let applied: FixResult = typed(&run(
        &["--json", "fix", path(&data), "--certificate", &cert],
        3,
    ));
    let Some(Score::Replayed {
        current: rescored, ..
    }) = &applied.score
    else {
        panic!("{:?}", applied.score)
    };
    assert_eq!(*rescored, current, "the rescore prices as why did");
    assert!(
        matches!(
            applied.outcome,
            FixOutcome::Refused {
                refusal: Refusal::NothingToGain { .. }
            }
        ),
        "{:?}",
        applied.outcome
    );
}

/// Item 1 (r1): over an index whose key serves none of the runs, the
/// current cost is the files' own min/max pricing of those runs: the funnel
/// labels it so and the culprit names min/max as what narrows the lookup,
/// never a time window the request does not have.
#[test]
fn over_an_index_that_serves_nothing_the_funnel_names_min_max_pruning() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 1, 8, 50, 1_000_000);
    run(&["index", path(&data), "--key", "exact:batch"], 0);
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[5, 7, 120]);
    let why: WhyReport = typed(&run(
        &["--json", "why", path(&data), "--from", path(&kf)],
        0,
    ));
    assert_eq!(why.unserved.as_deref(), Some("parquet_stats"));
    assert!(why.full_scan.is_none(), "an index exists");
    assert_eq!(why.current.unwrap().row_groups, 2);
    let human = run(&["why", path(&data), "--from", path(&kf)], 0);
    assert!(
        human.contains("current (min/max where no key serves)"),
        "{human}"
    );
    assert!(
        human.contains("min/max statistics already narrow it to 2 of the 8 row groups"),
        "{human}"
    );
    assert!(!human.contains("time window"), "{human}");
}

/// Round 2: when the current key serves every run, no run is priced by
/// min/max, so the funnel's current row is plain `current` and no line
/// speaks of min/max pruning.
#[test]
fn over_an_index_that_serves_every_run_the_current_row_is_plain() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 1, 8, 50, 1_000_000);
    run(&["index", path(&data), "--key", "exact:id"], 0);
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[5, 7, 120]);
    let why: WhyReport = typed(&run(
        &["--json", "why", path(&data), "--from", path(&kf)],
        0,
    ));
    assert_eq!(why.unserved, None, "every run is served");
    assert!(why.full_scan.is_none());
    let human = run(&["why", path(&data), "--from", path(&kf)], 0);
    let current = human
        .lines()
        .find(|l| l.trim_start().starts_with("current"))
        .unwrap_or_else(|| panic!("{human}"));
    assert!(current.trim_start().starts_with("current "), "{current}");
    // The funnel, culprit and recommendation (the lints below may discuss
    // statistics on their own).
    let head = human.split("\nfanout:").next().unwrap();
    assert!(!head.contains("min/max"), "{head}");
}

/// The funnel and the fanout table end at their last cell: with no sketch
/// column, no line of `why` pads past `exact` or `bytes`, and a funnel row
/// with no note (the plain `current` row over an index that serves every
/// run) ends at its size.
#[test]
fn why_tables_carry_no_trailing_padding() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 1, 8, 50, 1_000_000);
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[5, 7, 120]);
    let no_padding = |human: &str| {
        for line in human.lines() {
            assert_eq!(line, line.trim_end(), "trailing padding in: {line:?}");
        }
    };

    let human = run(&["why", path(&data), "--from", path(&kf)], 0);
    assert!(human.contains("\nfanout:"), "{human}");
    let header = human
        .lines()
        .find(|l| l.trim_start().starts_with("key ") && l.contains("files/value"))
        .unwrap_or_else(|| panic!("{human}"));
    assert!(
        header.ends_with("sample"),
        "no sketch column here: {header:?}"
    );
    no_padding(&human);

    run(&["index", path(&data), "--key", "exact:id"], 0);
    let human = run(&["why", path(&data), "--from", path(&kf)], 0);
    let current = human
        .lines()
        .find(|l| l.trim_start().starts_with("current "))
        .unwrap_or_else(|| panic!("{human}"));
    let unit = current.split_whitespace().last().unwrap();
    assert!(
        ["B", "kB", "MB", "GB", "TB"].contains(&unit),
        "the current row has no note here, so it ends at its size: {current:?}"
    );
    no_padding(&human);
}

/// Item 4, the other side: where min/max pruning keeps row groups the key
/// does not hold, `why` still recommends the key, priced against them, and
/// `fix` applies it. Row group `r` holds ids `r` and `1000 - r`, so its
/// range `[r, 1000 - r]` covers every smaller id.
/// Items 1 and 2 ride along: `fix`'s score line prints each size with its
/// unit once, and `report` counts one receipt in the singular.
#[test]
fn where_min_max_keeps_extra_row_groups_the_key_is_recommended_and_applied() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let groups: Vec<Vec<i64>> = (0..8)
        .map(|r| {
            (0..20)
                .map(|i| if i % 2 == 0 { r } else { 1000 - r })
                .collect()
        })
        .collect();
    grouped(&data.join("part-000.parquet"), &groups);
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[3]);
    let why: WhyReport = typed(&run(
        &["--json", "why", path(&data), "--from", path(&kf)],
        0,
    ));
    let current = why.current.unwrap();
    assert_eq!(current.row_groups, 4, "ranges [0..3, 997..1000] hold 3");
    let rec = why
        .recommended
        .clone()
        .expect("exact:id reads one row group");
    assert_eq!(rec.proposed.row_groups, 1);
    assert_eq!(rec.current, current, "measured against min/max pruning");
    let human = run(&["why", path(&data), "--from", path(&kf)], 0);
    assert!(human.contains("with min/max pruning"), "{human}");

    let proposal = propose(&data, &why, "exact:id");
    let workload = why.workload.as_ref().unwrap().hash.to_hex();
    let human = run(
        &[
            "fix",
            path(&data),
            "--proposal",
            &proposal,
            "--workload",
            &workload,
        ],
        0,
    );
    let score = human
        .lines()
        .find(|l| l.contains("1 score"))
        .unwrap_or_else(|| panic!("{human}"));
    assert!(
        score.contains("over the workload (4 -> 1 row group)"),
        "{score}"
    );
    for unit in ["B", "kB", "MB", "GB", "TB"] {
        assert!(!score.contains(&format!("{unit} bytes")), "{score}");
    }
    assert!(
        human.contains("a lookup no key serves is priced as the files' own min/max statistics"),
        "{human}"
    );
    // The receipt cites a Replay whose numbers are why's, run by run: the
    // min/max-pruned current and the key's cost.
    let ledger = data.join(".bellwether").join("ledger");
    let read = |hash: &str| -> Record {
        serde_json::from_slice(&std::fs::read(ledger.join(format!("{hash}.rec"))).unwrap()).unwrap()
    };
    let receipts: Vec<Record> = std::fs::read_dir(&ledger)
        .unwrap()
        .map(|e| std::fs::read(e.unwrap().path()).unwrap())
        .filter(|b| b.starts_with(br#"{"record":"receipt""#))
        .map(|b| serde_json::from_slice(&b).unwrap())
        .collect();
    let [Record::Receipt(receipt)] = receipts.as_slice() else {
        panic!("{receipts:?}")
    };
    let Record::Replay(cited) = read(&receipt.replay.to_hex()) else {
        panic!()
    };
    let Some(Record::Replay(predicted)) = why
        .candidates
        .iter()
        .find(|c| c.replay.hash == rec.replay)
        .map(|c| c.replay.record.clone())
    else {
        panic!()
    };
    assert_eq!(
        (&cited.current, &cited.proposed, &cited.unserved),
        (&predicted.current, &predicted.proposed, &predicted.unserved)
    );
    assert_eq!(cited.current[0].bytes, rec.current.bytes);
    assert_eq!(cited.proposed[0].bytes, rec.proposed.bytes);

    let html = dir.path().join("report.html");
    let out = run(&["report", path(&data), "--html", path(&html)], 0);
    let json: ReportResult = typed(&run(
        &["--json", "report", path(&data), "--html", path(&html)],
        0,
    ));
    assert_eq!(json.receipts.len(), 1);
    assert!(
        out.contains(": 1 receipt (1 with realized bytes), "),
        "{out}"
    );
    let evidence = match json.evidence.len() {
        1 => "1 profile evidence record".to_string(),
        n => format!("{n} profile evidence records"),
    };
    assert!(out.contains(&evidence), "{out}");
    assert!(!out.contains("1 receipts") && !out.contains(" 1 profile evidence records"));
}

/// Item 3: a bitemporal `asof` that the bitemporal key alone plans names
/// that key on the specs line, never "raw scan".
#[test]
fn a_lookup_planned_by_a_bitemporal_key_alone_names_it() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 2, 4, 20, 10);
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[3]);
    run(&["index", path(&data), "--key", "temporal2:id@ts,batch"], 0);
    let human = run(
        &[
            "asof",
            path(&data),
            "--keys",
            path(&kf),
            "--valid",
            "100",
            "--known",
            "1",
        ],
        0,
    );
    assert!(!human.contains("raw scan"), "{human}");
    assert!(human.contains("specs: temporal2:id@ts,batch\n"), "{human}");
    assert!(human.contains(": 1 key on (id)"), "{human}");
    // A plain locate is not served by the bitemporal key: the refusal names
    // the specs and columns as text, never Debug, and the key to declare.
    let out = common::bw()
        .args(["locate", path(&data), "--keys", path(&kf)])
        .output()
        .unwrap();
    let err = String::from_utf8(out.stderr).unwrap();
    assert_ne!(out.status.code(), Some(0), "{err}");
    assert!(
        err.contains(
            "none of the key specs (temporal2:id) serves a lookup on id; declare exact:id"
        ),
        "{err}"
    );
    assert!(!err.contains("[\"") && !err.contains("[["), "{err}");
}

/// A CSV folder of `(id, ts)` rows and a `--schema` file declaring `ts` a
/// microsecond timestamp in `zone`.
fn csv_with_zone(dir: &Path, zone: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    let data = dir.join(format!("csv-{}", zone.replace('/', "_")));
    std::fs::create_dir_all(&data).unwrap();
    let rows: String = std::iter::once("id,ts\n".to_string())
        .chain((0..20).map(|i| format!("{},2024-01-01T00:00:{i:02}\n", i % 5)))
        .collect();
    std::fs::write(data.join("a.csv"), rows).unwrap();
    let schema = dir.join(format!("schema-{}.json", zone.replace('/', "_")));
    std::fs::write(
        &schema,
        format!(
            r#"[{{"name": "id", "type": "Int64"}},
                {{"name": "ts", "type": "Timestamp(Microsecond, Some(\"{zone}\"))"}}]"#
        ),
    )
    .unwrap();
    (data, schema)
}

/// Item 5: a CSV `--schema` timestamp in `UTC` is read as `+00:00`, so
/// `why` and `index` both work; any other named zone gets one answer from
/// both: refused with the decoder's own message when the build cannot parse
/// it (no `chrono-tz`), accepted by both when it can.
#[test]
fn a_csv_schema_time_zone_gets_the_same_answer_from_why_and_index() {
    let dir = tempfile::tempdir().unwrap();
    let (data, schema) = csv_with_zone(dir.path(), "UTC");
    let base = ["--format", "csv", "--schema", path(&schema)];
    run(&[&base[..], &["why", path(&data)]].concat(), 0);
    run(
        &[&base[..], &["index", path(&data), "--key", "exact:id"]].concat(),
        0,
    );
    // Over Parquet the schema keeps "UTC" as written: an index built with it
    // is not rewritten by the next `index` (no empty commit).
    let pq = dir.path().join("pq");
    std::fs::create_dir_all(&pq).unwrap();
    let ts_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&ts_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(
                arrow::array::TimestampMicrosecondArray::from(vec![1, 2, 3]).with_timezone("UTC"),
            ),
        ],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(
        File::create(pq.join("a.parquet")).unwrap(),
        Arc::clone(&ts_schema),
        None,
    )
    .unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let pq_schema = dir.path().join("schema-pq.json");
    std::fs::write(
        &pq_schema,
        r#"[{"name": "id", "type": "Int64", "nullable": false},
            {"name": "ts", "type": "Timestamp(Microsecond, Some(\"UTC\"))", "nullable": false}]"#,
    )
    .unwrap();
    let index = |code| {
        run(
            &[
                "--schema",
                path(&pq_schema),
                "index",
                path(&pq),
                "--key",
                "exact:id",
            ],
            code,
        )
    };
    index(0);
    let manifest = std::fs::read(pq.join(".bellwether").join("manifest.json")).unwrap();
    let again = index(0);
    assert_eq!(
        std::fs::read(pq.join(".bellwether").join("manifest.json")).unwrap(),
        manifest,
        "no commit: {again}"
    );

    let zone = "America/New_York";
    let (data, schema) = csv_with_zone(dir.path(), zone);
    let base = ["--format", "csv", "--schema", path(&schema)];
    let why = bw()
        .args([&base[..], &["why", path(&data)]].concat())
        .output()
        .unwrap();
    let index = bw()
        .args([&base[..], &["index", path(&data), "--key", "exact:id"]].concat())
        .output()
        .unwrap();
    if let Err(e) = zone.parse::<arrow::array::timezone::Tz>() {
        let (why_err, index_err) = (
            String::from_utf8(why.stderr).unwrap(),
            String::from_utf8(index.stderr).unwrap(),
        );
        assert_eq!(why.status.code(), Some(4), "{why_err}");
        assert_eq!(index.status.code(), Some(4), "{index_err}");
        assert!(why_err.contains(&e.to_string()), "{why_err}");
        assert_eq!(why_err, index_err, "the same refusal from both");
        assert!(why_err.contains("\"+05:30\""), "says what to write");
    } else {
        assert_eq!(why.status.code(), Some(0));
        assert_eq!(index.status.code(), Some(0));
    }
}

/// The `fix` header names every spec a key proposal applies, not only the
/// first: `propose --key exact:id --key bloom:id@0.01` is one commit of two
/// specs.
#[test]
fn the_fix_header_names_every_spec_of_the_proposal() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    common::scattered(&data, 2, 4, 25, 101);
    let kf = dir.path().join("k.parquet");
    keys(&kf, "id", &[3, 50]);
    let d = path(&data);
    let why: serde_json::Value =
        serde_json::from_str(&run(&["why", d, "--from", path(&kf), "--json"], 0)).unwrap();
    let (ev, workload) = (
        why["evidence"]["hash"].as_str().unwrap(),
        why["workload"]["hash"].as_str().unwrap(),
    );
    let proposed: serde_json::Value = serde_json::from_str(&run(
        &[
            "propose",
            d,
            "--evidence",
            ev,
            "--key",
            "exact:id",
            "--key",
            "bloom:id@0.01",
            "--json",
        ],
        0,
    ))
    .unwrap();
    let proposal = proposed["proposal"]["hash"].as_str().unwrap();
    let human = run(
        &[
            "fix",
            d,
            "--proposal",
            proposal,
            "--workload",
            workload,
            "--verify-only",
        ],
        0,
    );
    let header = human.lines().next().unwrap();
    assert!(
        header.contains(": keys exact:id and bloom:id@0.01, applied in one commit, against"),
        "{human}"
    );
}
