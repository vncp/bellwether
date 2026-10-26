//! The `sketch` sidecar through the binary and the MCP server: `index --key
//! sketch:...`, `sketch cardinality` (from the index and `--transient`),
//! `sketch overlap`, and `why --sketches-only`; each `--json` line is the
//! MCP tool's text byte for byte (ADR 010), refusals included; the human
//! output labels every sketch number approximate and says what was read.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

mod common;

use std::path::Path;

use common::{corpus, path, run};
use serde_json::{json, Value};

const CLOCK: &str = "2026-10-24T12:00:00.000Z";

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

fn both(
    srv: &bellwether_mcp::Server,
    tool: &str,
    args: Value,
    cli_args: &[&str],
    code: i32,
) -> Value {
    let r = srv.call(tool, args);
    let line = cli(cli_args, code);
    assert_eq!(line, format!("{}\n", r.text), "{tool}: CLI and MCP differ");
    serde_json::from_str(&r.text).unwrap()
}

#[test]
fn sketch_tools_return_the_cli_json_line_and_label_every_number_approximate() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    // id = k % 200, batch = k / 50: 4 files x 3 row groups x 60 rows.
    corpus(&data, 4, 3, 60, 200);
    let d = path(&data);

    // Before any sketch: refused (not_indexed), naming the declaration and
    // --transient on the CLI and transient on the MCP server.
    run(&["index", d, "--key", "exact:id"], 0);
    let srv = server(&data);
    let refused = srv.call("observe.cardinality", json!({"items": ["batch"]}));
    assert!(
        refused.text.contains("no complete sketch holding batch"),
        "{}",
        refused.text
    );
    let env = serde_json::to_string(&refused.envelope).unwrap();
    assert!(env.contains("not_indexed"), "{env}");
    assert!(env.contains("transient: true"), "{env}");
    let human = common::bw()
        .args(["sketch", "cardinality", d, "--column", "batch"])
        .output()
        .unwrap();
    assert_eq!(human.status.code(), Some(4));
    let msg = String::from_utf8_lossy(&human.stderr);
    assert!(msg.contains("no complete sketch holding batch"), "{msg}");
    assert!(msg.contains("--key sketch:batch"), "{msg}");
    assert!(msg.contains("--transient"), "{msg}");

    // Transient: reads every file and says so.
    let srv = server(&data);
    let t = both(
        &srv,
        "observe.cardinality",
        json!({"items": ["batch", "id+batch"], "transient": true}),
        &[
            "sketch",
            "cardinality",
            d,
            "--column",
            "batch",
            "--column",
            "id+batch",
            "--transient",
        ],
        0,
    );
    assert_eq!(t["approximate"], true);
    assert_eq!(t["files_read"], 4);
    assert_eq!(t["row_groups_read"], 12);
    assert_eq!(t["items"][0]["source"], "transient");

    // Build the sketch, then the same answers from the index, no reads.
    run(
        &["index", d, "--key", "sketch:batch,id,id+batch,minhash=64"],
        0,
    );
    let srv = server(&data);
    let c = both(
        &srv,
        "observe.cardinality",
        json!({}),
        &["sketch", "cardinality", d],
        0,
    );
    assert_eq!(c["files_read"], 0);
    let items: Vec<&str> = c["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["cardinality"]["item"].as_str().unwrap())
        .collect();
    assert_eq!(items, vec!["batch", "id", "id+batch"]);
    // 720 rows: batch 0..14 (15 values), id 0..199 (exact via exact:id).
    let batch = &c["items"][0]["cardinality"];
    assert_eq!(batch["approximate"], true);
    assert_eq!(batch["rows"], 720);
    assert_eq!(batch["distinct"], 15);
    let id = &c["items"][1]["cardinality"];
    assert_eq!(id["exact_distinct"], 200);
    assert_eq!(id["rows_per_value_exact"], true);
    // Predicted (transient) equals built (index) for the same items.
    assert_eq!(t["items"][0]["cardinality"], c["items"][0]["cardinality"]);

    let o = both(
        &srv,
        "observe.overlap",
        json!({"left": "batch", "right": "id"}),
        &["sketch", "overlap", d, "batch", "id"],
        0,
    );
    assert_eq!(o["overlap"]["method"], "minhash");
    assert_eq!(o["overlap"]["approximate"], true);
    // batch 0..14 ⊂ id 0..199: Jaccard 15/200.
    let j = o["overlap"]["jaccard"].as_f64().unwrap();
    let (lo, hi) = (
        o["overlap"]["jaccard_low"].as_f64().unwrap(),
        o["overlap"]["jaccard_high"].as_f64().unwrap(),
    );
    assert!(lo <= 0.075 && 0.075 <= hi, "{j} [{lo}, {hi}]");

    // why --sketches-only: no data file read; the sketch column carries it.
    let w = both(
        &srv,
        "observe.why",
        json!({"sketches_only": true}),
        &["why", d, "--sketches-only"],
        0,
    );
    assert_eq!(w["from_sketches"], true);
    assert_eq!(w["reads"]["files"], 0);
    assert_eq!(w["reads"]["row_groups"], 0);
    assert_eq!(w["sketch"].as_array().unwrap().len(), 3);
    let full = both(&srv, "observe.why", json!({}), &["why", d], 0);
    assert_eq!(full["from_sketches"], false);

    // Refusals: typed on the server, exit 4 with the same message on the CLI.
    for (tool, args, cli_args) in [
        (
            "observe.cardinality",
            json!({"items": ["payload"]}),
            vec!["sketch", "cardinality", d, "--column", "payload"],
        ),
        (
            "observe.overlap",
            json!({"left": "batch", "right": "payload"}),
            vec!["sketch", "overlap", d, "batch", "payload"],
        ),
    ] {
        let r = srv.call(tool, args);
        let out = common::bw().args(&cli_args).output().unwrap();
        assert_eq!(out.status.code(), Some(4), "{tool}");
        let msg = String::from_utf8_lossy(&out.stderr);
        assert!(
            msg.contains(&r.text) || r.text.contains("no complete sketch"),
            "{tool}: {msg} vs {}",
            r.text
        );
        assert!(msg.contains("no complete sketch holding"), "{msg}");
    }

    // Human output: approximate, what was read, never used to plan.
    let h = run(&["sketch", "cardinality", d], 0);
    assert!(
        h.contains("approximate, from sketches; no data file read"),
        "{h}"
    );
    assert!(h.contains("batch: ≈15 distinct values"), "{h}");
    assert!(h.contains("200 distinct values (exact"), "{h}");
    assert!(
        h.contains("read from sketch:batch,id,id+batch,minhash=64"),
        "{h}"
    );
    let h = run(&["sketch", "overlap", d, "batch", "id"], 0);
    assert!(h.contains("approximate (MinHash, 64 signatures)"), "{h}");
    let h = run(&["why", d, "--sketches-only"], 0);
    assert!(
        h.contains("not measured: --sketches-only opens no data file, footers included"),
        "{h}"
    );
    assert!(
        h.contains("data read: 0 files, 0 row groups; footers of 0 files"),
        "{h}"
    );
    assert!(
        h.contains("dataset from the index's manifest and sketches"),
        "{h}"
    );
    assert!(h.contains("drop --sketches-only and pass --from"), "{h}");
    assert!(h.contains("repeat ≥≈"), "{h}");
    let h = run(&["why", d], 0);
    assert!(h.contains("footers of 4 files"), "{h}");
    assert!(h.contains("rows/value of ≈15"), "{h}");
    let h = run(&["index", d], 0);
    assert!(h.contains("never used to plan"), "{h}");
    // A sketch is not a proposable key.
    let p = common::bw()
        .args([
            "propose",
            d,
            "--key",
            "sketch:batch",
            "--evidence",
            &"0".repeat(64),
            "--by",
            "a",
        ])
        .output()
        .unwrap();
    assert_ne!(p.status.code(), Some(0));
}

#[test]
fn sketches_only_refuses_without_a_sketch_and_with_a_workload() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 2, 1, 20, 50);
    let d = path(&data);
    run(&["index", d, "--key", "exact:id"], 0);
    let out = common::bw()
        .args(["why", d, "--sketches-only"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(4));
    let e = String::from_utf8_lossy(&out.stderr);
    assert!(e.contains("no complete sketch"), "{e}");
    assert!(e.contains("--key sketch:"), "{e}");
    let k = dir.path().join("k.parquet");
    common::keys(&k, "id", &[1]);
    let out = common::bw()
        .args(["why", d, "--sketches-only", "--from", path(&k)])
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Refusals name real columns and a next step that works: `sketch
/// cardinality` with no sketch names a declaration over the dataset's
/// id-like columns (on the CLI and, with the root, over MCP); a column the
/// dataset does not have lists the columns it has (never suggesting to
/// sketch it); and `--transient` with nothing to count is a usage error
/// naming `--column`.
#[test]
fn sketch_refusals_name_real_columns() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 2, 1, 20, 50);
    let d = path(&data);
    run(&["index", d, "--key", "exact:id"], 0);

    let out = common::bw()
        .args(["sketch", "cardinality", d])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(4));
    let e = String::from_utf8_lossy(&out.stderr);
    assert!(e.contains("--key sketch:id,batch,payload"), "{e}");
    assert!(!e.contains('<'), "{e}");
    let srv = server(&data);
    let r = srv.call("observe.cardinality", json!({}));
    let env = serde_json::to_string(&r.envelope).unwrap();
    // The next step names the (canonical) root.
    let canonical = std::fs::canonicalize(&data).unwrap();
    assert!(
        env.contains(&format!(
            "bellwether index {} --key sketch:id,batch,payload",
            canonical.display()
        )),
        "{env}"
    );
    // The suggested next step works.
    run(&["index", d, "--key", "sketch:id,batch,payload"], 0);
    run(&["sketch", "cardinality", d], 0);

    let out = common::bw()
        .args(["sketch", "cardinality", d, "--column", "nope"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(4));
    let e = String::from_utf8_lossy(&out.stderr);
    assert!(e.contains("\"nope\" is not in the dataset"), "{e}");
    assert!(e.contains("id, batch, ts, payload"), "{e}");
    assert!(!e.contains("--key sketch:nope"), "{e}");
    let out = common::bw()
        .args(["index", d, "--key", "sketch:nope"])
        .output()
        .unwrap();
    let e = String::from_utf8_lossy(&out.stderr);
    assert!(!e.contains("sketch sketch:"), "{e}");
    assert!(e.contains("sketch:nope: column \"nope\""), "{e}");
    assert!(e.contains("id, batch, ts, payload"), "{e}");

    let fresh = tempfile::tempdir().unwrap();
    let data = fresh.path().join("data");
    corpus(&data, 2, 1, 20, 50);
    let d = path(&data);
    run(&["index", d, "--key", "exact:id"], 0);
    let out = common::bw()
        .args(["sketch", "cardinality", d, "--transient"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let e = String::from_utf8_lossy(&out.stderr);
    assert!(e.contains("--column"), "{e}");
    assert!(e.contains("id, batch, payload"), "{e}");
}

/// A damaged sketch segment (one byte flipped) is refused by `sketch
/// cardinality` and `sketch overlap`, on the CLI and over MCP, with the
/// cause and the same next step `why` gives: `bellwether index ROOT --key
/// <the declared sketch>`, which then rebuilds it.
#[test]
fn a_damaged_sketch_names_the_index_command_that_rebuilds_it() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 3, 2, 30, 50);
    let d = path(&data);
    run(&["index", d, "--key", "exact:id"], 0);
    run(&["index", d, "--key", "sketch:id,batch"], 0);
    let idx = data.join(".bellwether");
    let m: Value =
        serde_json::from_slice(&std::fs::read(idx.join("manifest.json")).unwrap()).unwrap();
    let id = m["segments"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["kind"] == "sketch")
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let seg = idx.join("segments").join(format!("{id}.seg"));
    let mut bytes = std::fs::read(&seg).unwrap();
    bytes[20] ^= 0x01;
    std::fs::write(&seg, bytes).unwrap();

    let step = "--key sketch:id,batch` (re-declaring";
    let srv = server(&data);
    for (tool, args, cli_args) in [
        (
            "observe.cardinality",
            json!({}),
            vec!["sketch", "cardinality", d],
        ),
        (
            "observe.overlap",
            json!({"left": "id", "right": "batch"}),
            vec!["sketch", "overlap", d, "id", "batch"],
        ),
    ] {
        let out = common::bw().args(&cli_args).output().unwrap();
        assert_eq!(out.status.code(), Some(4), "{tool}");
        let e = String::from_utf8_lossy(&out.stderr);
        assert!(e.contains("the sketch is unavailable"), "{tool}: {e}");
        assert!(e.contains("bellwether index "), "{tool}: {e}");
        assert!(e.contains(step), "{tool}: {e}");
        let r = srv.call(tool, args);
        assert!(r.text.contains(step), "{tool}: {}", r.text);
    }
    // `why` gives the same step and keeps working.
    let h = run(&["why", d], 0);
    assert!(h.contains(step), "{h}");
    // The step works.
    let h = run(&["index", d, "--key", "sketch:id,batch"], 0);
    assert!(h.contains("rebuilt sketch:id,batch"), "{h}");
    run(&["sketch", "cardinality", d], 0);
    run(&["sketch", "overlap", d, "id", "batch"], 0);
}
