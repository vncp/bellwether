//! `expr` keys end to end in the build crate, against an oracle that
//! decodes every file with the `parquet` crate and reads each JSON document
//! with `serde_json`, never the expression evaluator under test:
//!
//! - invariant 1: every row group holding a looked-up derived value is
//!   planned, by an exact key (which plans exactly those row groups) and by
//!   a bloom key over `lower(...)`;
//! - the residual (`ExprIn`) recomputed per row keeps exactly the rows the
//!   oracle says hold a key;
//! - a lookup spelled differently but normalizing equal plans the same;
//! - a near miss (another path, another function, the virtual column's
//!   name, a lookup kind an expr key does not answer) is refused with
//!   `Error::KeySpec` naming the declared expr keys, never guessed.

#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::similar_names,
    clippy::doc_markdown,
    clippy::needless_range_loop
)]

mod common;

use std::collections::BTreeSet;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bellwether_build::{Builder, Lookup, Planner};
use bellwether_core::expr::{self, Over, Typed};
use bellwether_types::{Error, KeySpec, Predicate};
use common::{enc_str, memory_store, parquet_opts, plan_rgs, write_parquet, Scratch};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

const FILES: usize = 3;
const RGS: usize = 5;
const ROWS: usize = 40;
const SYM: &str = "json_get(payload,'$.symbol')";
const LOW: &str = "lower(json_get(payload,'$.symbol'))";

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, true),
    ]))
}

/// Deterministic generator (SplitMix64).
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// One payload cell: mostly a document with a `symbol` member clustered by
/// row group (in mixed case), and every way the value can be null or odd.
fn cell(rng: &mut Rng, g: usize) -> Option<String> {
    let base = (g + rng.below(2) as usize) % 10;
    let mut s: String = format!("ab{base}");
    if rng.below(2) == 0 {
        s = s.to_uppercase();
    }
    if rng.below(3) == 0 {
        s = format!("Ab{base}");
    }
    Some(match rng.below(20) {
        0 => return None,                         // a null cell
        1 => r#"{"venue":"X"}"#.to_string(),      // member missing
        2 => format!(r#"{{"symbol":"{s}""#),      // not a document
        3 => r#"{"symbol":{"x":1}}"#.to_string(), // an object
        4 => r#"{"symbol":null}"#.to_string(),    // a JSON null
        5 => format!(r#"{{"symbol":{base}}}"#),   // a number: its text
        6 => format!(r#"{{ "a" : [1, {{"symbol":"no"}}], "symbol" : "{s}" }}"#),
        7 => format!(r#"{{"symbol":"{}"}}"#, s.replace('b', "\\u0062")),
        _ => format!(r#"{{"seq":{},"symbol":"{s}"}}"#, rng.below(1000)),
    })
}

fn write_corpus(dir: &Path) -> Vec<String> {
    let mut rng = Rng(7);
    let mut paths = Vec::new();
    for f in 0..FILES {
        let mut payload = Vec::new();
        for r in 0..RGS {
            let g = f * RGS + r;
            for _ in 0..ROWS {
                payload.push(cell(&mut rng, g));
            }
        }
        let n = payload.len();
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from_iter_values(0..n as i64)) as ArrayRef,
                Arc::new(StringArray::from(payload)),
            ],
        )
        .unwrap();
        let path = dir.join(format!("part-{f}.parquet"));
        write_parquet(&path, &batch, ROWS);
        paths.push(path.to_str().unwrap().to_string());
    }
    paths
}

/// The oracle's value of `$.symbol`: serde_json's reading of the document.
fn oracle(doc: Option<&str>) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(doc?).ok()?;
    match v.get("symbol")? {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Every row: `(file, row group, batch of the row group, row, oracle value)`.
fn decoded(paths: &[String]) -> Vec<(String, u32, RecordBatch)> {
    let mut out = Vec::new();
    for p in paths {
        let n = ParquetRecordBatchReaderBuilder::try_new(File::open(p).unwrap())
            .unwrap()
            .metadata()
            .num_row_groups();
        for rg in 0..n {
            let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(p).unwrap())
                .unwrap()
                .with_row_groups(vec![rg])
                .build()
                .unwrap();
            for b in reader {
                out.push((p.clone(), rg as u32, b.unwrap()));
            }
        }
    }
    out
}

fn values(b: &RecordBatch, lower: bool) -> Vec<Option<String>> {
    let a = b.column_by_name("payload").unwrap().as_string::<i32>();
    (0..a.len())
        .map(|i| {
            let v = oracle((!a.is_null(i)).then(|| a.value(i)));
            if lower {
                v.map(|s| s.chars().flat_map(char::to_lowercase).collect())
            } else {
                v
            }
        })
        .collect()
}

fn specs() -> Vec<KeySpec> {
    vec![
        expr::spec("sym", SYM, Over::Counts).unwrap(),
        expr::spec("sym_lower", LOW, Over::Bloom(0.01)).unwrap(),
        KeySpec::Exact {
            columns: vec!["id".into()],
            counts: false,
            bounds: None,
        },
    ]
}

fn indexed() -> (
    Scratch,
    Vec<String>,
    Planner<bellwether_store::ObjectStoreBackend>,
) {
    let s = Scratch::new();
    let dir = s.path("data");
    std::fs::create_dir_all(&dir).unwrap();
    let paths = write_corpus(&dir);
    let store = memory_store();
    let b = Builder::new(Arc::clone(&store), "idx");
    common::build_ok(
        &b,
        dir.to_str().unwrap(),
        &specs(),
        &parquet_opts(&schema()),
    );
    let planner = Planner::open(Arc::clone(&store), "idx").unwrap();
    (s, paths, planner)
}

fn lookup(text: &str, keys: &[&str]) -> Lookup {
    Lookup::Keys {
        columns: vec![text.to_string()],
        keys: keys.iter().map(|k| enc_str(k)).collect(),
    }
}

#[test]
fn plans_hold_every_row_group_the_decoded_files_say_holds_a_key() {
    let (_s, paths, planner) = indexed();
    let rows = decoded(&paths);
    let mut nonempty = 0;
    let cases: Vec<(&str, bool, Vec<&str>)> = vec![
        (SYM, false, vec!["ab3"]),
        (SYM, false, vec!["AB3", "Ab7"]),
        (SYM, false, vec!["ab0", "AB1", "Ab2", "ab9", "ZZ9"]),
        (SYM, false, vec!["4"]),
        (SYM, false, vec!["no"]),
        (SYM, false, vec!["ZZ9"]),
        (LOW, true, vec!["ab3"]),
        (LOW, true, vec!["ab5", "ab6", "zz"]),
        (LOW, true, vec!["AB5"]),
    ];
    for (text, lower, keys) in cases {
        let plan = planner.plan(&specs(), &lookup(text, &keys)).unwrap();
        let want: BTreeSet<&str> = keys.iter().copied().collect();
        let mut holding = BTreeSet::new();
        let mut kept_rows = 0usize;
        let mut true_rows = 0usize;
        let Some(Predicate::ExprIn { expr: e, values: v }) = &plan.residual else {
            panic!("{text}: residual {:?}", plan.residual)
        };
        assert_eq!(*e, expr::normalize(text).unwrap());
        let enc: BTreeSet<Vec<u8>> = keys.iter().map(|k| enc_str(k)).collect();
        assert_eq!(v.iter().cloned().collect::<BTreeSet<_>>(), enc, "{text}");
        let planned = plan_rgs(&plan);
        let typed = Typed::parse(e, &schema()).unwrap();
        for (p, rg, b) in &rows {
            let truth = values(b, lower);
            let hits = truth
                .iter()
                .filter(|v| v.as_deref().is_some_and(|s| want.contains(s)))
                .count();
            if hits > 0 {
                holding.insert((p.clone(), *rg));
                true_rows += hits;
            }
            // The residual, recomputed per row by the evaluator, on the
            // planned row groups only.
            if planned.contains(&(p.clone(), *rg)) {
                let derived = typed.eval(b).unwrap();
                let mut buf = Vec::new();
                for i in 0..b.num_rows() {
                    buf.clear();
                    let present = bellwether_core::encoding::encode_array_value_as(
                        derived.as_ref(),
                        i,
                        typed.data_type(),
                        &mut buf,
                    )
                    .unwrap();
                    if present && v.contains(&buf) {
                        kept_rows += 1;
                        assert!(
                            truth[i].as_deref().is_some_and(|s| want.contains(s)),
                            "{text} {keys:?}: residual keeps {p} rg {rg} row {i} ({:?})",
                            truth[i]
                        );
                    }
                }
            }
        }
        let missing: Vec<_> = holding.difference(&planned).collect();
        assert!(missing.is_empty(), "{text} {keys:?}: missed {missing:?}");
        if !lower {
            assert_eq!(
                planned, holding,
                "{text} {keys:?}: an exact key plans exactly"
            );
        }
        assert_eq!(kept_rows, true_rows, "{text} {keys:?}: residual rows");
        nonempty += usize::from(!holding.is_empty());
    }
    assert!(nonempty >= 6, "the cases must hit data: {nonempty}");
}

#[test]
fn a_differently_spelled_equal_expression_plans_the_same() {
    let (_s, _paths, planner) = indexed();
    let keys = ["ab3", "AB4"];
    let a = planner.plan(&specs(), &lookup(SYM, &keys)).unwrap();
    for spelled in [
        "JSON_GET( payload , '$.symbol' )",
        "json_get(payload,'$[\"symbol\"]')",
        "json_get(\"payload\",'$[''symbol'']')",
    ] {
        let b = planner.plan(&specs(), &lookup(spelled, &keys)).unwrap();
        assert_eq!(a, b, "{spelled}");
    }
}

#[test]
fn a_near_miss_is_refused_naming_the_expr_keys_never_guessed() {
    let (_s, _paths, planner) = indexed();
    for near in [
        "json_get(payload,'$.Symbol')",
        "json_get(payload,'$.symbol[0]')",
        "substr(json_get(payload,'$.symbol'),1,3)",
        "lower(json_get(payload,'$.venue'))",
    ] {
        let e = planner.plan(&specs(), &lookup(near, &["ab3"])).unwrap_err();
        let Error::KeySpec(m) = &e else {
            panic!("{near}: {e:?}")
        };
        assert!(
            m.contains("no expr key indexes")
                && m.contains("expr:sym=json_get(payload,'$.symbol'):exact")
                && m.contains("expr:sym_lower=")
                && m.contains("bellwether index /")
                && m.contains(" --key 'expr:")
                && !m.contains("<name>"),
            "{near}: {m}"
        );
    }
    // The virtual column's name is not a lookup column.
    let e = planner
        .plan(&specs(), &lookup("sym", &["ab3"]))
        .unwrap_err();
    assert!(
        matches!(&e, Error::KeySpec(m) if m.contains("virtual column") && m.contains(SYM)),
        "{e:?}"
    );
    // An expression beside another column is refused.
    let e = planner
        .plan(
            &specs(),
            &Lookup::Keys {
                columns: vec![SYM.into(), "id".into()],
                keys: vec![],
            },
        )
        .unwrap_err();
    assert!(
        matches!(&e, Error::KeySpec(m) if m.contains("alone")),
        "{e:?}"
    );
    // A latest lookup on an expression: an expr key answers locate only.
    let e = planner
        .plan(
            &specs(),
            &Lookup::Latest {
                live: None,
                columns: vec![SYM.into()],
                keys: vec![enc_str("ab3")],
            },
        )
        .unwrap_err();
    assert!(
        matches!(&e, Error::KeySpec(m) if m.contains("point lookups (locate) only")),
        "{e:?}"
    );
    // With only an exact key on a dataset column, nothing serves an
    // expression: refused, with the index command to declare one.
    let only_id = &specs()[2..];
    let e = planner.plan(only_id, &lookup(SYM, &["ab3"])).unwrap_err();
    assert!(
        matches!(&e, Error::KeySpec(m) if m.contains("no expr key indexes") && m.contains(r"--key 'expr:json_get_payload_symbol=json_get(payload,'\''$.symbol'\''):exact'")),
        "{e:?}"
    );
}

/// A spec that does not record every vector this binary pins for the
/// functions it calls (here none at all) is refused when it is indexed, and
/// a manifest edited to drop them is refused when it is planned with.
#[test]
fn a_spec_without_its_conformance_vectors_is_refused_at_index_and_plan() {
    let mut bare = expr::spec("sym", SYM, Over::Exact).unwrap();
    if let KeySpec::Expr { conformance, .. } = &mut bare {
        conformance.clear();
    }
    let s = Scratch::new();
    let dir = s.path("data");
    std::fs::create_dir_all(&dir).unwrap();
    write_corpus(&dir);
    let store = memory_store();
    let b = Builder::new(Arc::clone(&store), "idx");
    let e = b
        .build(
            dir.to_str().unwrap(),
            std::slice::from_ref(&bare),
            &parquet_opts(&schema()),
        )
        .unwrap_err();
    assert!(
        matches!(&e, Error::KeySpec(m) if m.contains("does not record the conformance vector")),
        "{e:?}"
    );
    // Built with its vectors, then the manifest's copy loses them.
    let full = expr::spec("sym", SYM, Over::Exact).unwrap();
    common::build_ok(
        &b,
        dir.to_str().unwrap(),
        std::slice::from_ref(&full),
        &parquet_opts(&schema()),
    );
    bellwether_store::commit(
        &*store,
        "idx",
        &bellwether_store::StagedSegments::new(),
        |m| {
            let mut m = m.clone();
            m.schema.key_specs = vec![bare.clone()];
            Ok(m)
        },
    )
    .unwrap();
    let planner = Planner::open(Arc::clone(&store), "idx").unwrap();
    let specs = planner.declared_specs().to_vec();
    let e = planner.plan(&specs, &lookup(SYM, &["ab3"])).unwrap_err();
    assert!(
        matches!(&e, Error::KeySpec(m) if m.contains("does not record the conformance vector")),
        "{e:?}"
    );
}

/// `expr:sym=...:exact,counts` answers `count(*) where json_get(...) in
/// (...)` from the index: the rows equal the decoded files' rows holding a
/// value, a lookup's plan carries the same exact count, and the count needs
/// a key over that very expression (a near miss is refused; without any expr
/// key there is nothing to count with).
#[test]
fn counts_on_an_expr_key_equal_the_decoded_rows() {
    let (_s, paths, planner) = indexed();
    let rows = decoded(&paths);
    for keys in [
        vec!["ab3"],
        vec!["AB3", "Ab7", "ab0"],
        vec!["4", "ZZ9"],
        vec!["ZZ9"],
    ] {
        let want: BTreeSet<&str> = keys.iter().copied().collect();
        let (mut n, mut rgs) = (0u64, BTreeSet::new());
        for (p, rg, b) in &rows {
            let hits = values(b, false)
                .iter()
                .filter(|v| v.as_deref().is_some_and(|s| want.contains(s)))
                .count() as u64;
            if hits > 0 {
                n += hits;
                rgs.insert((p.clone(), *rg));
            }
        }
        let enc: Vec<Vec<u8>> = keys.iter().map(|k| enc_str(k)).collect();
        let c = planner
            .count(&specs(), &[SYM.to_string()], &enc)
            .unwrap()
            .expect("the counting expr key counts");
        assert_eq!(c.rows, n, "{keys:?}");
        assert_eq!(c.row_groups, rgs.len() as u64, "{keys:?}");
        assert_eq!(c.counted_by, specs()[0]);
        let plan = planner.plan(&specs(), &lookup(SYM, &keys)).unwrap();
        assert_eq!(plan.rows.map(|q| q.value), Some(n), "{keys:?}");
    }
    // Spelled another way, the same count.
    let enc = vec![enc_str("ab3")];
    let a = planner
        .count(&specs(), &[SYM.to_string()], &enc)
        .unwrap()
        .unwrap();
    let b = planner
        .count(
            &specs(),
            &["JSON_GET( payload, '$[\"symbol\"]' )".to_string()],
            &enc,
        )
        .unwrap()
        .unwrap();
    assert_eq!(a.rows, b.rows);
    // A bloom expr key counts nothing; a near miss is refused.
    assert!(planner
        .count(&specs(), &[LOW.to_string()], &enc)
        .unwrap()
        .is_none());
    let e = planner
        .count(
            &specs(),
            &["json_get(payload,'$.Symbol')".to_string()],
            &enc,
        )
        .unwrap_err();
    assert!(
        matches!(&e, Error::KeySpec(m) if m.contains("no expr key indexes")),
        "{e:?}"
    );
    assert!(planner
        .count(&specs()[2..], &[SYM.to_string()], &enc)
        .unwrap()
        .is_none());
}
