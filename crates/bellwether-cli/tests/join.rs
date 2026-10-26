//! Join keys (B3) on the command line and over MCP: `index --key
//! join:...`, `join semijoin|overlap|verify`, `why --mode semijoin --join`,
//! and their refusals. ADR 010: each MCP tool's text is the CLI's `--json`
//! line byte for byte (`observe.semijoin`, `observe.join_overlap`,
//! `observe.join_verify`, `score.why` with `join`), refusals included; the
//! server takes the join key from its operator (`--join`), never from a
//! tool argument. The human output names both steps, the `join` column,
//! and, for a stale key, the command that rebuilds it.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::cast_possible_wrap
)]

mod common;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use common::{check_records, index_dir, path, run};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde_json::{json, Value};

const CLOCK: &str = "2026-10-26T12:00:00.000Z";
const JOIN: &str = "join:orders.order_id=payables.paid_order";

fn write(path: &Path, key: &str, ids: &[i64], per_rg: usize) {
    let schema = Arc::new(Schema::new(vec![
        Field::new(key, DataType::Int64, true),
        Field::new("amount", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from((0..ids.len() as i64).collect::<Vec<_>>())),
        ],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(per_rg))
        .build();
    let mut w = ArrowWriter::try_new(File::create(path).unwrap(), schema, Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// orders 0..200 clustered; payables for 100..300 with 1:N skew, shuffled.
fn corpus(root: &Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let (l, r) = (root.join("orders"), root.join("payables"));
    std::fs::create_dir_all(&l).unwrap();
    std::fs::create_dir_all(&r).unwrap();
    write(
        &l.join("o.parquet"),
        "order_id",
        &(0..200).collect::<Vec<_>>(),
        10,
    );
    let mut pay = Vec::new();
    for id in 100..300i64 {
        for _ in 0..=(id % 3) {
            pay.push(id);
        }
    }
    let n = pay.len();
    let shuffled: Vec<i64> = (0..n).map(|i| pay[(i * 53) % n]).collect();
    write(&r.join("p.parquet"), "paid_order", &shuffled, 25);
    (
        std::fs::canonicalize(l).unwrap(),
        std::fs::canonicalize(r).unwrap(),
    )
}

fn keys(p: &Path, ids: &[i64]) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "order_id",
        DataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(File::create(p).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn server(data: &Path, join: Option<&str>) -> bellwether_mcp::Server {
    bellwether_mcp::Server::new(&bellwether_mcp::Config {
        root: path(data).to_string(),
        engine: bellwether::EngineOptions {
            clock: Some(CLOCK.into()),
            ..bellwether::EngineOptions::default()
        },
        budget: bellwether_mcp::Budget::default(),
        join: join.map(str::to_string),
    })
    .unwrap()
}

fn cli(args: &[&str], code: i32) -> String {
    run(&[&["--clock", CLOCK, "--json"], args].concat(), code)
}

fn human(args: &[&str], code: i32) -> String {
    run(&[&["--clock", CLOCK], args].concat(), code)
}

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

fn refused(
    srv: &bellwether_mcp::Server,
    tool: &str,
    args: Value,
    cli_args: &[&str],
    code: i32,
    code_name: &str,
) -> (Value, String) {
    let r = srv.call(tool, args);
    let refusal = r.envelope.refusal.expect("a refusal");
    assert_eq!(refusal.code.as_str(), code_name, "{refusal:?}");
    let out = assert_cmd::Command::cargo_bin("bellwether")
        .unwrap()
        .args([&["--clock", CLOCK, "--json"], cli_args].concat())
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(code));
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(
        stderr.contains(&refusal.message),
        "CLI: {stderr}\nMCP: {}",
        refusal.message
    );
    (refusal.fields, stderr)
}

#[test]
fn join_tools_are_byte_identical_over_cli_and_mcp() {
    let dir = tempfile::tempdir().unwrap();
    let (l, r) = corpus(dir.path());
    let k = dir.path().join("k.parquet");
    keys(&k, &[5, 99, 100, 150, 199, 250]);
    let (d, rd, kp) = (path(&l), path(&r), path(&k));
    run(&["index", rd, "--key", "exact:paid_order"], 0);
    run(&["index", d, "--key", "exact:order_id"], 0);
    let srv = server(&l, Some("payables.paid_order"));

    // why --mode semijoin --join = score.why with join.
    let why = both(
        &srv,
        "score.why",
        json!({"keys": kp, "join": true, "wait": true}),
        &[
            "why",
            d,
            "--from",
            kp,
            "--mode",
            "semijoin",
            "--join",
            "payables.paid_order",
        ],
        0,
    );
    check_records(&index_dir(&l), &why);
    assert_eq!(why["workload"]["query_kind"], "semijoin");
    assert_eq!(why["candidates"][0]["spec"]["kind"], "join");
    assert_eq!(why["candidates"][0]["kind_column"]["kind"], "join");
    assert_eq!(why["recommended"]["spec"]["kind"], "join");
    // Two runs (why --from repeated = score.why more_keys): R-join-1.
    let k2 = dir.path().join("k2.parquet");
    keys(&k2, &[120, 130]);
    let k2p = path(&k2);
    let two = both(
        &srv,
        "score.why",
        json!({"keys": kp, "more_keys": [k2p], "join": true, "wait": true}),
        &[
            "why",
            d,
            "--from",
            kp,
            "--from",
            k2p,
            "--mode",
            "semijoin",
            "--join",
            "payables.paid_order",
        ],
        0,
    );
    assert_eq!(two["workload"]["runs"], 2);
    assert!(two["profile"]["lints"]
        .as_array()
        .unwrap()
        .iter()
        .any(|l| l["rule"] == "R-join-1"));
    assert!(!why["profile"]["lints"]
        .as_array()
        .unwrap()
        .iter()
        .any(|l| l["rule"] == "R-join-1"));

    // Before the join key: observe.semijoin is refused like join semijoin.
    let (fields, stderr) = refused(
        &srv,
        "observe.semijoin",
        json!({"keys": kp}),
        &[
            "join",
            "semijoin",
            d,
            "--join",
            "payables.paid_order",
            "--keys",
            kp,
        ],
        4,
        "not_indexed",
    );
    assert_eq!(fields["needs"], JOIN);
    assert!(stderr.contains(JOIN), "{stderr}");

    run(&["index", d, "--key", JOIN], 0);
    let s = both(
        &srv,
        "observe.semijoin",
        json!({"keys": kp}),
        &[
            "join",
            "semijoin",
            d,
            "--join",
            "payables.paid_order",
            "--keys",
            kp,
        ],
        0,
    );
    assert_eq!(s["matched"], 3);
    assert_eq!(s["left"]["root"], d);
    assert_eq!(s["right"]["root"], rd);
    let o = both(
        &srv,
        "observe.join_overlap",
        json!({}),
        &["join", "overlap", d, "--join", "payables.paid_order"],
        0,
    );
    assert_eq!(o["both"], 100);
    assert_eq!(o["left_only"], 100);
    assert_eq!(o["right_only"], 100);
    let v = both(
        &srv,
        "observe.join_verify",
        json!({}),
        &["join", "verify", d, "--join", "payables.paid_order"],
        0,
    );
    assert_eq!(v["pass"], true);

    // The right commits again: stale_join both ways, with the command.
    write(&r.join("p2.parquet"), "paid_order", &[5], 25);
    run(&["index", rd], 0);
    let (fields, stderr) = refused(
        &srv,
        "observe.semijoin",
        json!({"keys": kp}),
        &[
            "join",
            "semijoin",
            d,
            "--join",
            "payables.paid_order",
            "--keys",
            kp,
        ],
        4,
        "stale_join",
    );
    assert_eq!(fields["side"], "right");
    assert!(
        stderr.contains(&format!("bellwether index {d}")),
        "{stderr}"
    );
    let v = both(
        &srv,
        "observe.join_verify",
        json!({}),
        &["join", "verify", d],
        1,
    );
    assert_eq!(v["pass"], false);
    assert_eq!(v["joins"][0]["state"]["kind"], "stale_right");

    // A server started without --join refuses, naming the flag.
    let bare = server(&l, None);
    let r = bare.call("observe.semijoin", json!({"keys": kp}));
    let refusal = r.envelope.refusal.unwrap();
    assert_eq!(refusal.code.as_str(), "invalid_argument");
    assert!(refusal.message.contains("--join"), "{}", refusal.message);
    // No tool takes the right dataset as an argument.
    let r = srv.call(
        "observe.semijoin",
        json!({"keys": kp, "right": "elsewhere"}),
    );
    assert_eq!(r.envelope.refusal.unwrap().code.as_str(), "invalid_input");
}

#[test]
fn join_human_output_and_usage_errors() {
    let dir = tempfile::tempdir().unwrap();
    let (l, r) = corpus(dir.path());
    let k = dir.path().join("k.parquet");
    keys(&k, &[5, 150, 199]);
    let (d, rd, kp) = (path(&l), path(&r), path(&k));
    run(&["index", rd, "--key", "exact:paid_order"], 0);
    run(&["index", d, "--key", "exact:order_id"], 0);
    let w = human(
        &[
            "why",
            d,
            "--from",
            kp,
            "--mode",
            "semijoin",
            "--join",
            "payables.paid_order",
        ],
        0,
    );
    assert!(w.contains("join: asked keys both datasets hold"), "{w}");
    assert!(w.contains("recommended (for semijoin with payables.paid_order): join:orders.order_id=payables.paid_order"), "{w}");
    assert!(w.contains("no join key serves this semi-join"), "{w}");
    let header = w
        .lines()
        .find(|l| l.contains("row groups") && l.contains("bytes"))
        .unwrap();
    assert!(header.trim_end().ends_with("join"), "{header}");
    // Review item 7e: the join cell is short (asked keys both hold, of
    // those asked), its meaning is printed once under the table, and the
    // dataset row's cell says why it holds no join value.
    let row = w
        .lines()
        .find(|l| l.trim_start().starts_with(JOIN))
        .unwrap();
    assert!(row.contains(" 2 of 3  floor (semijoin)"), "{row}");
    assert_eq!(
        w.matches("join: asked keys both datasets hold, of those asked")
            .count(),
        1,
        "{w}"
    );
    let dataset = w
        .lines()
        .find(|l| l.trim_start().starts_with("dataset"))
        .unwrap();
    assert!(
        dataset.ends_with("200 rows (no join value: the dataset, not a candidate)"),
        "{dataset}"
    );
    assert!(
        !w.lines().any(|l| l != l.trim_end()),
        "trailing blanks: {w}"
    );
    // Declaring the join key with the left exact key in one call.
    let idx = human(&["index", d, "--key", "exact:order_id", "--key", JOIN], 0);
    assert!(idx.contains("join keys"), "{idx}");
    assert!(
        idx.contains("rebuilt; join:orders.order_id=payables.paid_order: fresh"),
        "{idx}"
    );
    assert!(idx.contains(&format!("right dataset {rd}")), "{idx}");
    let s = human(
        &[
            "join",
            "semijoin",
            d,
            "--join",
            "payables.paid_order",
            "--keys",
            kp,
        ],
        0,
    );
    assert!(
        s.contains("semijoin join:orders.order_id=payables.paid_order: 2 of 3 asked keys are on both datasets"),
        "{s}"
    );
    assert!(s.contains("step 1") && s.contains("step 2"), "{s}");
    assert!(
        s.contains("residual: keep the rows whose (paid_order) is one of the 2 shared keys"),
        "{s}"
    );
    assert!(s.contains("the other 1 key is missing from one dataset or both, so a semi-join keeps no row of it"), "{s}");
    let o = human(&["join", "overlap", d, "--join", JOIN], 0);
    assert!(o.contains("both sides") && o.contains("right only"), "{o}");
    let w = human(
        &[
            "why",
            d,
            "--from",
            kp,
            "--mode",
            "semijoin",
            "--join",
            "payables.paid_order",
        ],
        0,
    );
    assert!(w.contains("(semijoin with payables.paid_order)"), "{w}");
    assert!(w.contains("each cost below sums both steps"), "{w}");
    // stale on the left, then refreshed by index
    write(&l.join("o2.parquet"), "order_id", &[1000], 10);
    run(&["index", d], 0);
    let v = human(&["join", "verify", d], 0);
    assert!(v.contains("every join key is fresh"), "{v}");
    // usage
    run(
        &["why", d, "--from", kp, "--join", "payables.paid_order"],
        2,
    );
    run(&["why", d, "--from", kp, "--mode", "semijoin"], 2);
    run(&["join", "semijoin", d, "--keys", kp], 2);
    // Review item 7f: a --join mistake is a usage error (exit 2), with the
    // spelling to use.
    for args in [
        vec!["join", "overlap", d, "--join", "payables"],
        vec![
            "why", d, "--from", kp, "--mode", "semijoin", "--join", "payables",
        ],
    ] {
        let out = assert_cmd::Command::cargo_bin("bellwether")
            .unwrap()
            .args(&args)
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(2), "{args:?}");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains("payables.order_id"), "{stderr}");
    }
}

#[test]
fn fix_on_a_join_proposal_prints_both_certificates() {
    let dir = tempfile::tempdir().unwrap();
    let (l, r) = corpus(dir.path());
    let k = dir.path().join("k.parquet");
    keys(&k, &[5, 150, 199, 250]);
    let (d, rd, kp) = (path(&l), path(&r), path(&k));
    run(&["index", rd, "--key", "exact:paid_order"], 0);
    run(&["index", d, "--key", "exact:order_id"], 0);
    let why: Value = serde_json::from_str(&cli(
        &[
            "why",
            d,
            "--from",
            kp,
            "--mode",
            "semijoin",
            "--join",
            "payables.paid_order",
        ],
        0,
    ))
    .unwrap();
    let p = why["recommended"]["proposal"].as_str().unwrap().to_string();
    let w = why["workload"]["hash"].as_str().unwrap().to_string();
    let fixed = human(&["fix", d, "--proposal", &p, "--workload", &w], 0);
    assert!(
        fixed.contains(&format!("right dataset {rd}: pass")),
        "{fixed}"
    );
    assert!(
        fixed.contains("rows compared on both datasets: those of the 2 of 4 asked keys"),
        "{fixed}"
    );
    assert!(fixed.contains("receipt"), "{fixed}");
    let v = human(&["join", "verify", d], 0);
    assert!(v.contains("every join key is fresh"), "{v}");
}

/// Review of PR #32, reader items a, b, g, and i, on the command line.
#[test]
fn join_reader_pass_states_the_cause_and_the_next_step() {
    let dir = tempfile::tempdir().unwrap();
    let (l, r) = corpus(dir.path());
    let (d, rd) = (path(&l), path(&r));
    let counted = format!("{JOIN},counts");
    run(&["index", rd, "--key", "exact:paid_order,counts"], 0);
    // g: a counted key's index line names it with `,counts`.
    let idx = human(
        &[
            "index",
            d,
            "--key",
            "exact:order_id,counts",
            "--key",
            &counted,
        ],
        0,
    );
    assert!(idx.contains(&format!("rebuilt; {counted}: fresh")), "{idx}");
    // a: a right file nobody indexed: `join verify` says stale on the right,
    // why, and both commands, and exits 1.
    write(&r.join("p2.parquet"), "paid_order", &[5], 25);
    let v = human(&["join", "verify", d], 1);
    assert!(v.contains("1 join key is stale"), "{v}");
    assert!(v.contains("stale on the right"), "{v}");
    assert!(
        v.contains("1 new file the index does not cover (p2.parquet)"),
        "{v}"
    );
    assert!(
        v.contains(&format!("bellwether index {rd} && bellwether index {d}")),
        "{v}"
    );
    // b: the left's own commit is reported beside the join key it could
    // not rebuild, with the cause and the command; exit 1.
    write(&l.join("o2.parquet"), "order_id", &[1000], 10);
    let idx = human(&["index", d], 1);
    assert!(idx.contains("committed manifest sequence 3"), "{idx}");
    assert!(
        idx.contains(&format!("{counted}: not rebuilt: {rd} has 1 new file")),
        "{idx}"
    );
    // The cause is on the right (its files moved on), so the next step
    // indexes the right first, then the left (review round 2).
    assert!(
        idx.contains(&format!(
            "run `bellwether index {rd} && bellwether index {d}` to rebuild it"
        )),
        "{idx}"
    );
    run(&["index", rd], 0);
    let idx = human(&["index", d], 0);
    assert!(idx.contains(&format!("rebuilt; {counted}: fresh")), "{idx}");
    assert!(human(&["join", "verify", d], 0).contains("every join key is fresh"));
    // The right lacks the exact key a counted join key reads: the next step
    // declares it there (`needs`), then rebuilds on the left; and a right
    // never indexed names the same key, never a placeholder.
    for indexed in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let (l, r) = corpus(dir.path());
        let (d, rd) = (path(&l), path(&r));
        if indexed {
            run(&["index", rd, "--key", "exact:paid_order"], 0);
        }
        let idx = human(
            &[
                "index",
                d,
                "--key",
                "exact:order_id,counts",
                "--key",
                &counted,
            ],
            1,
        );
        assert!(idx.contains("committed manifest sequence 1"), "{idx}");
        assert!(
            idx.contains(&format!("{counted}: not rebuilt: {rd}")),
            "{idx}"
        );
        // The join key was named on this call and is not declared yet, so
        // the command names it again; followed literally, it is fresh.
        let next = format!(
            "bellwether index {rd} --key exact:paid_order,counts && bellwether index {d} --key \
             {counted}"
        );
        assert!(
            idx.contains(&format!("run `{next}` to rebuild it")),
            "{indexed}: {idx}"
        );
        assert!(!idx.contains("<spec>"), "{idx}");
        let mut out = String::new();
        for step in next.split(" && ") {
            let words: Vec<&str> = step.split(' ').skip(1).collect();
            out = human(&words, 0);
        }
        assert!(out.contains(&format!("rebuilt; {counted}: fresh")), "{out}");
    }
    // i: a link beside the root to a folder elsewhere is refused by name,
    // as a usage error.
    let outside = dir.path().join("outside").join("extern");
    std::fs::create_dir_all(&outside).unwrap();
    write(&outside.join("x.parquet"), "paid_order", &[1, 2], 10);
    run(
        &["index", path(&outside), "--key", "exact:paid_order,counts"],
        0,
    );
    std::os::unix::fs::symlink(&outside, l.parent().unwrap().join("linked")).unwrap();
    let out = assert_cmd::Command::cargo_bin("bellwether")
        .unwrap()
        .args([
            "index",
            d,
            "--key",
            "join:orders.order_id=linked.paid_order,counts",
        ])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("the right dataset linked is") && stderr.contains("a link to"),
        "{stderr}"
    );
    assert!(stderr.contains("not beside the data root"), "{stderr}");
    assert!(stderr.contains("move or copy linked there"), "{stderr}");
}

/// Review of PR #32, reader item d, on the time-series corpus: a semi-join
/// workload's culprit is the missing join key, never a single-column key
/// lint advising a key the dataset already has.
#[test]
fn a_semijoin_culprit_is_the_missing_join_key_on_the_market_corpus() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("market");
    let o = path(&out);
    run(
        &[
            "bench",
            "joins",
            "--shape",
            "market",
            "--out",
            o,
            "--size",
            "200",
            "--symbols",
            "40",
            "--rows-per-row-group",
            "500",
            "--keysets",
            "300",
        ],
        0,
    );
    let (t, q) = (out.join("trades"), out.join("quotes"));
    run(&["index", path(&q), "--key", "exact:symbol,minute"], 0);
    run(&["index", path(&t), "--key", "exact:symbol,minute"], 0);
    let keys = out.join("workload").join("semi-300.parquet");
    let w = human(
        &[
            "why",
            path(&t),
            "--from",
            path(&keys),
            "--mode",
            "semijoin",
            "--join",
            "quotes.symbol,minute",
        ],
        0,
    );
    let culprit = w.lines().find(|l| l.starts_with("culprit")).unwrap();
    assert!(
        culprit.starts_with("culprit (symbol, minute): no join key serves this semi-join"),
        "{culprit}"
    );
    assert!(culprit.contains("join:trades.symbol,minute=quotes.symbol,minute is not declared"));
}
