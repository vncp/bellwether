//! Agg keys (B7) on the command line and over MCP: `aggregate`, `why
//! --mode aggregate --value V --fn F`, their refusals, and the human
//! output of `aggregate`, `fix` (the agg cells checked against the rows)
//! and `reconcile` (which side an agg key answered). ADR 010: each MCP
//! tool's text is the CLI's `--json` line byte for byte (`observe.aggregate`,
//! `score.why` with `aggregate`), refusals included.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

mod common;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Decimal128Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use common::{check_records, index_dir, path, run};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde_json::{json, Value};

const CLOCK: &str = "2026-10-26T12:00:00.000Z";

/// Two files of two row groups of twelve rows: `account` = row % 4, an
/// `amount` (six decimals, both signs) null in every fifth row, a unique
/// `entry`.
fn corpus(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("account", DataType::Int64, false),
        Field::new("amount", DataType::Decimal128(18, 6), true),
        Field::new("entry", DataType::Int64, false),
    ]));
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(1 << 20))
        .build();
    let mut k = 0i64;
    for f in 0..2 {
        let file = File::create(dir.join(format!("part-{f}.parquet"))).unwrap();
        let mut w = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props.clone())).unwrap();
        for _ in 0..2 {
            let (mut acc, mut amt, mut ent) = (Vec::new(), Vec::new(), Vec::new());
            for _ in 0..12 {
                acc.push(k % 4);
                amt.push((k % 5 != 0).then(|| i128::from(k * 1_000_001 - 7_000_000)));
                ent.push(k);
                k += 1;
            }
            let cols: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(acc)),
                Arc::new(
                    Decimal128Array::from(amt)
                        .with_precision_and_scale(18, 6)
                        .unwrap(),
                ),
                Arc::new(Int64Array::from(ent)),
            ];
            w.write(&RecordBatch::try_new(Arc::clone(&schema), cols).unwrap())
                .unwrap();
            w.flush().unwrap();
        }
        w.close().unwrap();
    }
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

fn human(args: &[&str], code: i32) -> String {
    run(&[&["--clock", CLOCK], args].concat(), code)
}

/// Calls `tool` in process first, then the CLI; asserts the CLI exits
/// `code` and prints the tool's text; returns it parsed.
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
    serde_json::from_str(&c).unwrap()
}

/// A refusal both ways: the MCP refusal's code is `code_name` and its
/// message is the one the CLI prints to stderr (exit `code`); returns (MCP
/// next step, CLI stderr).
fn refused(
    srv: &bellwether_mcp::Server,
    tool: &str,
    args: Value,
    cli_args: &[&str],
    code: i32,
    code_name: &str,
) -> (String, String) {
    let r = srv.call(tool, args);
    let refusal = r.envelope.refusal.expect("a refusal");
    assert_eq!(refusal.code.as_str(), code_name, "{refusal:?}");
    let out = assert_cmd::Command::cargo_bin("bellwether")
        .unwrap()
        .args([&["--clock", CLOCK, "--json"], cli_args].concat())
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(code));
    assert_eq!(out.stdout, b"");
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(
        stderr.contains(&refusal.message),
        "CLI: {stderr}\nMCP: {}",
        refusal.message
    );
    (refusal.next, stderr)
}

const AGG: &str = "agg:account@amount{sum,count,min,max}";

#[test]
fn aggregates_are_byte_identical_over_cli_and_mcp() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data);
    let kf = dir.path().join("k.parquet");
    common::keys(&kf, "account", &[1, 3, 9]);
    let (d, k) = (path(&data), path(&kf));
    let srv = server(&data);

    // Before any key: observe.aggregate is refused as aggregate is, naming
    // both declarations.
    let (next, stderr) = refused(
        &srv,
        "observe.aggregate",
        json!({"keys": k, "value": "amount"}),
        &["aggregate", d, "--keys", k, "--value", "amount"],
        4,
        "not_indexed",
    );
    assert!(
        stderr.contains("exact:account,counts") && stderr.contains("agg:account@amount"),
        "{stderr}"
    );
    assert!(next.contains("bellwether index"), "{next}");

    // score.why with aggregate = why --mode aggregate --value --fn: the
    // counted key is the candidate first.
    let why = both(
        &srv,
        "score.why",
        json!({"keys": k, "wait": true,
               "aggregate": {"value": "amount", "functions": ["sum", "count", "min", "max"]}}),
        &[
            "why",
            d,
            "--from",
            k,
            "--value",
            "amount",
            "--fn",
            "sum,count,min,max",
        ],
        0,
    );
    check_records(&index_dir(&data), &why);
    assert_eq!(
        why["workload"]["lookup"]["kind"], "aggregate",
        "{}",
        why["workload"]
    );
    assert!(why["profile"]["lints"]
        .as_array()
        .unwrap()
        .iter()
        .any(|l| l["rule"] == "R-agg-1"));

    run(
        &["index", d, "--key", "exact:account,counts", "--key", AGG],
        0,
    );
    // observe.aggregate = aggregate --json, with and without --fn.
    let a = both(
        &srv,
        "observe.aggregate",
        json!({"keys": k, "value": "amount", "functions": ["sum", "count", "min", "max"]}),
        &[
            "aggregate",
            d,
            "--keys",
            k,
            "--value",
            "amount",
            "--fn",
            "sum,count,min,max",
        ],
        0,
    );
    assert_eq!(a["complete"], true);
    assert_eq!(a["groups"].as_array().unwrap().len(), 3);
    assert_eq!(a["groups"][2]["rows"], 0, "account 9 has no row");
    let d2 = both(
        &srv,
        "observe.aggregate",
        json!({"keys": k, "value": "amount"}),
        &["aggregate", d, "--keys", k, "--value", "amount"],
        0,
    );
    assert_eq!(d2["functions"], json!(["sum", "count"]));
    // A value the agg key does not aggregate: the same refusal both ways.
    let (_, stderr) = refused(
        &srv,
        "observe.aggregate",
        json!({"keys": k, "value": "entry"}),
        &["aggregate", d, "--keys", k, "--value", "entry"],
        4,
        "not_indexed",
    );
    assert!(stderr.contains("agg:account@entry"), "{stderr}");
}

#[test]
fn aggregate_fix_and_reconcile_human_output() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data);
    let kf = dir.path().join("k.parquet");
    common::keys(&kf, "account", &[1, 3]);
    let (d, k) = (path(&data), path(&kf));
    run(&["index", d, "--key", "exact:account,counts"], 0);
    let why: Value = serde_json::from_str(&cli(
        &[
            "why",
            d,
            "--from",
            k,
            "--value",
            "amount",
            "--fn",
            "sum,count,min,max",
        ],
        0,
    ))
    .unwrap();
    let wl = why["workload"]["hash"].as_str().unwrap().to_string();
    let c = why["candidates"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["spec"]["kind"] == "agg")
        .unwrap_or_else(|| panic!("{why}"));
    let p = c["proposal"]["hash"].as_str().unwrap().to_string();
    assert_eq!(c["kind_column"]["kind"], "agg");
    let human_why = human(
        &[
            "why",
            d,
            "--from",
            k,
            "--value",
            "amount",
            "--fn",
            "sum,count,min,max",
        ],
        0,
    );
    assert!(
        human_why.contains(AGG) || human_why.contains("agg:account@amount"),
        "{human_why}"
    );

    // fix says what it checked the agg cells against.
    let fixed = human(&["fix", d, "--proposal", &p, "--workload", &wl], 0);
    assert!(
        fixed.contains("agg cells checked against the rows: agg:account@amount"),
        "{fixed}"
    );
    assert!(fixed.contains("0 differ"), "{fixed}");

    let agg = human(
        &[
            "aggregate",
            d,
            "--keys",
            k,
            "--value",
            "amount",
            "--fn",
            "sum,count,min,max",
        ],
        0,
    );
    // Account 1: rows 1, 5, ..., 45; amounts k * 1.000001 - 7, null when k
    // is a multiple of 5 (k = 5, 25, 45).
    let rows: Vec<i64> = (1..48).step_by(4).collect();
    let sum: i128 = rows
        .iter()
        .filter(|k| *k % 5 != 0)
        .map(|k| i128::from(*k) * 1_000_001 - 7_000_000)
        .sum();
    let shown = format!("{}.{:06}", sum / 1_000_000, (sum % 1_000_000).abs());
    assert!(agg.contains(&shown), "{shown}: {agg}");

    // reconcile names the side its agg key answered.
    let right = dir.path().join("right");
    corpus(&right);
    let rec = human(
        &[
            "reconcile",
            d,
            path(&right),
            "--key",
            "account",
            "--ignore",
            "amount,entry",
            "--totals",
            "amount",
        ],
        0,
    );
    assert!(
        rec.contains(
            "the left side's per-key rows and totals came from its index's agg:account@amount"
        ),
        "{rec}"
    );
    assert!(rec.contains("R-agg-1"), "{rec}");
}

/// Writes a key file of `(account, entry)` pairs.
fn pair_keys(path: &Path, pairs: &[(i64, i64)]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("account", DataType::Int64, false),
        Field::new("entry", DataType::Int64, false),
    ]));
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(pairs.iter().map(|p| p.0))),
        Arc::new(Int64Array::from_iter_values(pairs.iter().map(|p| p.1))),
    ];
    let mut w =
        ArrowWriter::try_new(File::create(path).unwrap(), Arc::clone(&schema), None).unwrap();
    w.write(&RecordBatch::try_new(schema, cols).unwrap())
        .unwrap();
    w.close().unwrap();
}

/// Every next step the agg surfaces print can be followed as printed: the
/// counted key's `fix … --allow-no-gain` line from `why` on a fresh folder;
/// the partial answer's `index --key 'agg:…{fns}'`; a sum of a column with
/// none refused (exit 2) naming `--fn` and the functions it has; a spec the
/// dataset refuses named once (exit 4, as for every kind); reconcile saying
/// why each side was read and the `--ignore` list that lets the agg key
/// answer; and `why` naming no culprit once the agg key answers.
#[test]
fn agg_next_steps_can_be_followed_as_printed() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data);
    let kf = dir.path().join("k.parquet");
    common::keys(&kf, "account", &[1, 3]);
    let (d, k) = (path(&data), path(&kf));
    let why_args = [
        "why",
        d,
        "--from",
        k,
        "--value",
        "amount",
        "--fn",
        "sum,count",
    ];

    // A fresh folder: why names the counted key's fix with --allow-no-gain.
    let first = human(&why_args, 0);
    let line = first
        .lines()
        .find(|l| l.contains("--allow-no-gain"))
        .unwrap_or_else(|| panic!("{first}"));
    assert!(
        line.contains("first apply the counted key the agg key needs"),
        "{line}"
    );
    let cmd = line.split("allow that: ").nth(1).unwrap();
    let args: Vec<&str> = cmd.split_whitespace().collect();
    assert_eq!(args[..2], ["bellwether", "fix"], "{cmd}");
    human(&args[1..], 0);
    assert!(
        first.contains("then run why again: it proposes the agg key"),
        "{first}"
    );
    // Following it: why proposes the agg key; applying it, the culprit is
    // that nothing is read.
    let why: Value = serde_json::from_str(&cli(&why_args, 0)).unwrap();
    let c = why["candidates"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["spec"]["kind"] == "agg")
        .unwrap_or_else(|| panic!("{why}"));
    let p = c["proposal"]["hash"].as_str().unwrap().to_string();
    let wl = why["workload"]["hash"].as_str().unwrap().to_string();
    human(&["fix", d, "--proposal", &p, "--workload", &wl], 0);
    let again = human(&why_args, 0);
    assert!(
        again.contains(
            "none: the index answers the aggregates from agg:account@amount{sum,count} beside \
             its counted key; nothing is read"
        ),
        "{again}"
    );

    // A sum of a column with none: refused as a usage error naming --fn.
    let label = dir.path().join("label");
    common::corpus(&label, 1, 2, 10, 4);
    let lk = dir.path().join("lk.parquet");
    common::keys(&lk, "id", &[1]);
    run(&["index", path(&label), "--key", "exact:id,counts"], 0);
    let out = common::bw()
        .args([
            "aggregate",
            path(&label),
            "--keys",
            path(&lk),
            "--value",
            "payload",
            "--fn",
            "sum,count",
        ])
        .output()
        .unwrap();
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert_eq!(out.status.code(), Some(2), "{stderr}");
    assert!(
        stderr.contains("payload is Utf8, which has no sum"),
        "{stderr}"
    );
    assert!(stderr.contains("agg:id@payload{count,min,max}"), "{stderr}");
    assert!(stderr.contains("check --fn"), "{stderr}");
    // The same refusal at index time: named once.
    let out = common::bw()
        .args([
            "index",
            path(&label),
            "--key",
            "exact:id,counts",
            "--key",
            "agg:id@payload{sum,count}",
        ])
        .output()
        .unwrap();
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert_eq!(out.status.code(), Some(4), "{stderr}");
    assert_eq!(stderr.matches("key spec error").count(), 1, "{stderr}");
    assert!(
        stderr.contains("agg:id@payload: payload is Utf8"),
        "{stderr}"
    );
    // ... and the spec it names instead is accepted.
    run(
        &[
            "index",
            path(&label),
            "--key",
            "exact:id,counts",
            "--key",
            "agg:id@payload{count,min,max}",
        ],
        0,
    );

    // A partial answer (keys on (account, entry), the agg key on account):
    // the index command it prints carries the functions and is accepted.
    let pk = dir.path().join("pk.parquet");
    pair_keys(&pk, &[(1, 1), (3, 7)]);
    run(&["index", d, "--key", "exact:account,entry,counts"], 0);
    let partial = human(
        &[
            "aggregate",
            d,
            "--keys",
            path(&pk),
            "--value",
            "amount",
            "--fn",
            "sum,count",
        ],
        0,
    );
    let line = partial
        .lines()
        .find(|l| l.contains("bellwether index "))
        .unwrap_or_else(|| panic!("{partial}"));
    assert!(
        line.ends_with("/data --key 'agg:account,entry@amount{sum,count}'"),
        "{line}"
    );
    run(
        &[
            "index",
            d,
            "--key",
            "exact:account,counts",
            "--key",
            "exact:account,entry,counts",
            "--key",
            "agg:account@amount{sum,count}",
            "--key",
            "agg:account,entry@amount{sum,count}",
        ],
        0,
    );
    let whole = human(
        &[
            "aggregate",
            d,
            "--keys",
            path(&pk),
            "--value",
            "amount",
            "--fn",
            "sum,count",
        ],
        0,
    );
    assert!(whole.contains("no data file was read"), "{whole}");

    // Reconcile comparing the total row by row: each side read, and why.
    let right = dir.path().join("right");
    corpus(&right);
    run(
        &[
            "index",
            path(&right),
            "--key",
            "exact:account,counts",
            "--key",
            "agg:account@amount{sum,count}",
        ],
        0,
    );
    let rec = human(
        &[
            "reconcile",
            d,
            path(&right),
            "--key",
            "account",
            "--ignore",
            "entry",
            "--totals",
            "amount",
        ],
        0,
    );
    for side in ["left", "right"] {
        assert!(
            rec.contains(&format!(
                "the {side} side was read, not answered from its index's \
                 agg:account@amount{{sum,count}}: amount is compared row by row"
            )),
            "{rec}"
        );
    }
    assert!(rec.contains("--ignore amount,entry"), "{rec}");
}
