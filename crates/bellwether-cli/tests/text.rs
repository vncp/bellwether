//! Text keys (B5) end to end through the CLI and the MCP server: text
//! lookups recorded as an ordinary workload by `why --contains/--has/
//! --prefix` (and by `--from --mode`), the `R-text-1` detector in the
//! profile Evidence (fires without a serving text key, silent once one is
//! applied and on a key workload), text keys as ordinary candidates with
//! the `text` column (distinct tokens), then `fix` (score, verify on the
//! rows the lookups ask for, apply, receipt) and `locate --contains` through
//! the applied key, its plan holding every row group with a match. Every
//! step that has an MCP tool returns the CLI's `--json` line byte for byte
//! (ADR 010).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

mod common;

use std::collections::BTreeSet;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use common::{keys, path, run};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde_json::{json, Value};

const CLOCK: &str = "2026-10-24T12:00:00.000Z";

/// Row `k`'s document: every row says "quarterly", rows 40..=44 also say
/// "Guidance raised", row 100 "Straße", and every third row is null.
fn doc(k: i64) -> Option<String> {
    if k % 3 == 2 {
        return None;
    }
    let mut d = format!("doc {k} quarterly report on revenue");
    if (40..=44).contains(&k) {
        d.push_str("; Guidance raised");
    }
    if k == 100 {
        d.push_str(", Straße office");
    }
    Some(d)
}

/// 2 files × 3 row groups × 30 rows of (id, body).
fn text_corpus(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("body", DataType::Utf8, true),
    ]));
    let mut k = 0i64;
    for f in 0..2 {
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(usize::MAX))
            .build();
        let file = File::create(dir.join(format!("part-{f}.parquet"))).unwrap();
        let mut w = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        for _ in 0..3 {
            let ks: Vec<i64> = (k..k + 30).collect();
            k += 30;
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(ks.clone())) as ArrayRef,
                    Arc::new(ks.iter().map(|k| doc(*k)).collect::<StringArray>()),
                ],
            )
            .unwrap();
            w.write(&batch).unwrap();
            w.flush().unwrap();
        }
        w.close().unwrap();
    }
}

/// The (file, row group) of every row holding `needle`, folded as the
/// definition folds this corpus (ASCII case and ß).
fn truth(needle: &str) -> BTreeSet<(String, u64)> {
    let fold = |s: &str| s.to_lowercase().replace('ß', "ss");
    (0..180)
        .filter(|k| doc(*k).is_some_and(|d| fold(&d).contains(&fold(needle))))
        .map(|k| {
            (
                format!("part-{}.parquet", k / 90),
                u64::try_from((k % 90) / 30).unwrap(),
            )
        })
        .collect()
}

fn planned(plan: &Value) -> BTreeSet<(String, u64)> {
    let mut out = BTreeSet::new();
    for e in plan["evidence"]["record"]["body"]["plan"]["entries"]
        .as_array()
        .unwrap()
    {
        let file = e["file"]["path"].as_str().unwrap();
        let name = Path::new(file).file_name().unwrap().to_str().unwrap();
        for rg in e["row_groups"].as_array().unwrap() {
            out.insert((name.to_string(), rg.as_u64().unwrap()));
        }
    }
    out
}

fn server(data: &Path) -> bellwether_mcp::Server {
    bellwether_mcp::Server::new(&bellwether_mcp::Config {
        root: path(data).to_string(),
        engine: bellwether::EngineOptions {
            clock: Some(CLOCK.into()),
            ..bellwether::EngineOptions::default()
        },
        budget: bellwether_mcp::Budget::default(),
        join: None,
    })
    .unwrap()
}

fn cli(args: &[&str], code: i32) -> String {
    run(&[&["--clock", CLOCK, "--json"], args].concat(), code)
}

/// Calls `tool`, then the CLI; both must print the same bytes.
fn both(
    srv: &bellwether_mcp::Server,
    tool: &str,
    args: Value,
    cli_args: &[&str],
    code: i32,
) -> Value {
    let r = srv.call(tool, args);
    let m = r.text + "\n";
    let c = cli(cli_args, code);
    assert_eq!(c, m, "{tool}: the CLI line and the MCP text differ");
    serde_json::from_str(&m).unwrap()
}

fn rules(why: &Value) -> Vec<String> {
    why["profile"]["lints"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| l["rule"].as_str().unwrap().to_string())
        .collect()
}

fn evidence_rules(why: &Value) -> Vec<String> {
    why["evidence"]["evidence"]["body"]["lints"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| l["rule"].as_str().unwrap().to_string())
        .collect()
}

#[test]
fn text_lookups_are_priced_fixed_receipted_and_located() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    text_corpus(&data);
    let d = path(&data);
    let srv = server(&data);

    // why --contains/--has = score.why with `text`: one run per lookup.
    let why = both(
        &srv,
        "score.why",
        json!({"text": [
            {"column": "body", "op": "contains", "value": "GUIDANCE"},
            {"column": "body", "op": "has_token", "value": "strasse"},
        ], "wait": true}),
        &[
            "why",
            d,
            "--contains",
            "body=GUIDANCE",
            "--has",
            "body=strasse",
        ],
        0,
    );
    let workload = why["workload"]["hash"].as_str().unwrap().to_string();
    assert_eq!(why["workload"]["runs"], 2);
    assert_eq!(why["workload"]["query_kind"], "contains,has_token");
    assert_eq!(why["workload"]["lookup"]["kind"], "text_runs");
    // R-text-1 fires, in the profile and in the recorded Evidence, which
    // cites the workload.
    assert!(rules(&why).contains(&"R-text-1".to_string()), "{why}");
    assert!(evidence_rules(&why).contains(&"R-text-1".to_string()));
    let inputs = why["evidence"]["evidence"]["provenance"]["inputs"].to_string();
    assert!(inputs.contains(&workload), "{inputs}");
    // The candidates: the words dictionary (has_token runs exist) and the
    // trigram key, each with its `text` column.
    let candidates = why["candidates"].as_array().unwrap();
    let specs: Vec<String> = candidates
        .iter()
        .map(|c| bellwether::spec::label(&serde_json::from_value(c["spec"].clone()).unwrap()))
        .collect();
    assert_eq!(
        specs,
        ["text:body,tokenizer=words", "text:body,tokenizer=trigram"]
    );
    for c in candidates {
        assert_eq!(c["kind_column"]["kind"], "text");
        assert!(
            c["kind_column"]["value"]
                .as_str()
                .unwrap()
                .parse::<u64>()
                .unwrap()
                > 0
        );
        assert_eq!(c["kind_column"]["exactness"]["kind"], "exact");
    }
    let words_label = candidates[0]["kind_column"]["label"].as_str().unwrap();
    assert!(words_label.starts_with("distinct words"), "{words_label}");
    // 180 rows, 120 of them non-null, each with its own `doc <k>` number:
    // more than one distinct word per two rows.
    assert!(words_label.contains("; high entropy: "), "{words_label}");
    let tri_label = candidates[1]["kind_column"]["label"].as_str().unwrap();
    assert!(
        tri_label.contains("the median trigram is in "),
        "{tri_label}"
    );
    // The trigram key serves both runs and reads less than no index.
    let rec = &why["recommended"];
    assert_eq!(rec["spec"]["tokenizer"], "trigram", "{why}");
    let proposal = rec["proposal"].as_str().unwrap().to_string();
    // The human report shows the text column and the lint.
    let human = run(
        &[
            "--clock",
            CLOCK,
            "why",
            d,
            "--contains",
            "body=GUIDANCE",
            "--has",
            "body=strasse",
        ],
        0,
    );
    assert!(
        human.contains("text lookups (has_token, contains) on body"),
        "{human}"
    );
    assert!(human.contains("text: distinct trigrams"), "{human}");
    assert!(human.contains("R-text-1 [body]"), "{human}");

    // propose.key = propose --key for a text key: an ordinary Proposal.
    let ev = why["evidence"]["hash"].as_str().unwrap().to_string();
    let proposed = both(
        &srv,
        "propose.key",
        json!({"spec": "text:body,tokenizer=trigram", "evidence": [ev], "by": "agent"}),
        &[
            "propose",
            d,
            "--evidence",
            &ev,
            "--by",
            "agent",
            "--key",
            "text:body,tokenizer=trigram",
        ],
        0,
    );
    assert_eq!(proposed["outcome"], "written");

    // score.verify = fix --verify-only: verify compares the rows the
    // lookups ask for.
    let verified = both(
        &srv,
        "score.verify",
        json!({"proposal": proposal, "workload": workload, "wait": true}),
        &[
            "fix",
            d,
            "--proposal",
            &proposal,
            "--workload",
            &workload,
            "--verify-only",
        ],
        0,
    );
    assert_eq!(verified["outcome"]["kind"], "verified", "{verified}");
    // fix: score, verify, apply, receipt.
    let fixed = cli(
        &["fix", d, "--proposal", &proposal, "--workload", &workload],
        0,
    );
    let fixed: Value = serde_json::from_str(&fixed).unwrap();
    assert_eq!(fixed["outcome"]["kind"], "applied", "{fixed}");
    let receipts = cli(&["ledger", "receipts", d], 0);
    assert!(
        receipts.contains("text:body") || receipts.contains("\"trigram\""),
        "{receipts}"
    );

    // locate --contains = observe.text, through the applied key: every row
    // group with a match is planned, and the plan says the residual is
    // mandatory.
    let located = both(
        &srv,
        "observe.text",
        json!({"column": "body", "op": "contains", "value": "GUIDANCE"}),
        &["locate", d, "--contains", "body=GUIDANCE"],
        0,
    );
    assert_eq!(located["lookup"]["exact_row_groups"], false);
    let want = truth("guidance");
    let got = planned(&located);
    assert!(want.is_subset(&got), "{want:?} not in {got:?}");
    assert!(got.len() < 6, "the key prunes: {got:?}");
    let human = run(&["locate", d, "--contains", "body=GUIDANCE"], 0);
    assert!(human.contains("residual MANDATORY"), "{human}");
    // An argument too short to prune: every row group, and the plan says so.
    let short: Value =
        serde_json::from_str(&cli(&["locate", d, "--contains", "body=ab"], 0)).unwrap();
    assert_eq!(short["planned"]["row_groups"], 6);
    assert!(short["lookup"]["unpruned"]
        .as_str()
        .is_some_and(|m| m.contains("shorter than 3 bytes")));
    let human = run(&["locate", d, "--contains", "body=ab"], 0);
    assert!(
        human.contains("not pruned: the argument is shorter than 3 bytes"),
        "{human}"
    );
    // A case- and ß-folded word: `has` through the trigram key.
    let has = run(&["--json", "locate", d, "--has", "body=STRASSE"], 0);
    let has: Value = serde_json::from_str(&has).unwrap();
    assert!(truth("strasse").is_subset(&planned(&has)));

    // The same lookups again: a text key serves them, so R-text-1 is
    // silent.
    let again: Value = serde_json::from_str(&cli(
        &[
            "why",
            d,
            "--contains",
            "body=GUIDANCE",
            "--has",
            "body=strasse",
        ],
        0,
    ))
    .unwrap();
    assert!(!rules(&again).contains(&"R-text-1".to_string()), "{again}");
    // A key workload never fires it.
    let kf = dir.path().join("ids.parquet");
    keys(&kf, "id", &[1, 2, 3]);
    let ids: Value = serde_json::from_str(&cli(&["why", d, "--from", path(&kf)], 0)).unwrap();
    assert!(!rules(&ids).contains(&"R-text-1".to_string()), "{ids}");
}

/// `why --from <values> --mode contains|has-token|has-prefix` records the
/// key file as one text run; usage errors say what to pass.
#[test]
fn a_key_file_of_values_is_priced_as_a_text_run() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    text_corpus(&data);
    let d = path(&data);
    let vf = dir.path().join("values.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec!["guidance", "revenue"])) as ArrayRef],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(File::create(&vf).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let v: Value = serde_json::from_str(&cli(
        &["why", d, "--from", path(&vf), "--mode", "has-token"],
        0,
    ))
    .unwrap();
    assert_eq!(v["workload"]["query_kind"], "has_token");
    assert_eq!(v["workload"]["runs"], 1);
    assert!(rules(&v).contains(&"R-text-1".to_string()));
    // has_prefix runs: the words dictionary is a candidate, then trigrams.
    let p: Value = serde_json::from_str(&cli(&["why", d, "--prefix", "body=Guid"], 0)).unwrap();
    assert_eq!(p["workload"]["query_kind"], "has_prefix");
    let specs: Vec<&str> = p["candidates"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["spec"]["tokenizer"].as_str().unwrap())
        .collect();
    assert_eq!(specs, ["words", "trigram"]);
    assert_eq!(p["recommended"]["spec"]["tokenizer"], "words");
    // Usage errors (exit 2), each naming what to pass.
    let e = bellwether_cmd(&["why", d, "--contains", "body=abc", "--latest-by", "id"]);
    assert!(e.contains("text lookups say their own match"), "{e}");
    let e = bellwether_cmd(&["why", d, "--prefix", "body=foo b"]);
    assert!(e.contains("takes the start of one word"), "{e}");
    let e = bellwether_cmd(&["why", d, "--mode", "contains"]);
    assert!(e.contains("--from <values.parquet>"), "{e}");
    let e = bellwether_cmd(&["why", d, "--has", "body=two words"]);
    assert!(e.contains("pass one word"), "{e}");
    let e = bellwether_cmd(&["why", d, "--contains", "nocolumn"]);
    assert!(e.contains("COL=TEXT"), "{e}");
}

/// Runs the binary expecting the usage exit code (2); returns stderr.
fn bellwether_cmd(args: &[&str]) -> String {
    let out = common::bw().args(args).output().unwrap();
    assert_eq!(out.status.code(), Some(2), "{args:?}: {out:?}");
    String::from_utf8(out.stderr).unwrap()
}
