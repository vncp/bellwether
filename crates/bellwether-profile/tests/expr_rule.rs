//! `R-expr-1` (docs/rule-catalog.md): it fires when lookups supply a
//! function of a dataset column that no declared `expr` key is over, and
//! does not fire on plain columns or on an expression a declared `expr`
//! key serves (spelled any way that normalizes equal).

#![allow(clippy::unwrap_used, missing_docs)]

mod common;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use bellwether_core::expr::{self, Over};
use bellwether_profile::{lints, profile, ProfileOptions, ProfileReport};
use bellwether_source::ParquetSource;
use bellwether_store::{commit, StagedSegments};
use bellwether_types::{KeySpec, Lint};
use common::{path_str, temp_store};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

fn data(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    let n = 200i64;
    let batch = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..n)) as ArrayRef,
        ),
        (
            "payload",
            Arc::new(StringArray::from_iter_values(
                (0..n).map(|i| format!(r#"{{"symbol":"s{}"}}"#, i / 20)),
            )),
        ),
    ])
    .unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(50))
        .build();
    let mut w = ArrowWriter::try_new(
        File::create(dir.join("part-0.parquet")).unwrap(),
        batch.schema(),
        Some(props),
    )
    .unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn run(supplied: &[&str], declared: &[KeySpec]) -> ProfileReport {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("data");
    data(&root);
    let (_s, store) = temp_store();
    if !declared.is_empty() {
        commit(&*store, "", &StagedSegments::new(), |m| {
            let mut m = m.clone();
            m.source.root = path_str(&root);
            m.schema.key_specs = declared.to_vec();
            Ok(m)
        })
        .unwrap();
    }
    profile(
        &ParquetSource::new(),
        &path_str(&root),
        &store,
        &ProfileOptions {
            supplied_columns: supplied.iter().map(|s| (*s).to_string()).collect(),
            ..ProfileOptions::for_source("parquet")
        },
    )
    .unwrap()
}

fn expr_lints(r: &ProfileReport) -> Vec<&Lint> {
    r.lints
        .iter()
        .filter(|l| l.rule == lints::EXPR_RULE)
        .collect()
}

#[test]
fn fires_on_a_supplied_expression_no_expr_key_serves() {
    let r = run(&["JSON_GET(payload, '$.symbol')"], &[]);
    let found = expr_lints(&r);
    assert_eq!(found.len(), 1, "{:?}", r.lints);
    let l = found[0];
    assert_eq!(l.rule, "R-expr-1");
    assert_eq!(l.columns, vec!["payload".to_string()]);
    assert!((l.confidence - lints::HIGH).abs() < f64::EPSILON);
    assert!(
        l.message.contains("json_get(payload,'$.symbol')")
            && l.message.contains(
                "bellwether index --key \"expr:json_get_payload_symbol=json_get(payload,'$.symbol'):exact\""
            )
            && l.message.contains("bellwether why --from"),
        "{}",
        l.message
    );
    // A declared expr key over another expression: still fires, and names
    // the declared key and the normalization rule.
    let other = expr::spec("v", "json_get(payload,'$.venue')", Over::Exact).unwrap();
    let r = run(&["json_get(payload,'$.symbol')"], &[other]);
    let found = expr_lints(&r);
    assert_eq!(found.len(), 1, "{:?}", r.lints);
    assert!(
        found[0].message.contains("v = json_get(payload,'$.venue')")
            && found[0].message.contains("normalizes to the key's"),
        "{}",
        found[0].message
    );
}

#[test]
fn does_not_fire_on_plain_columns_or_a_served_expression() {
    // Plain columns, and a bare column that merely parses.
    assert_eq!(
        expr_lints(&run(&["id", "payload"], &[])),
        Vec::<&Lint>::new()
    );
    assert_eq!(expr_lints(&run(&[], &[])), Vec::<&Lint>::new());
    // Expressions an expr key could not be built on: a missing column, a
    // function at the wrong type, and a constant.
    for bad in [
        "lower(missing_col)",
        "lower(id)",
        "date_trunc(payload,day)",
        "lower('constant')",
    ] {
        assert_eq!(expr_lints(&run(&[bad], &[])), Vec::<&Lint>::new(), "{bad}");
    }
    // A declared expr key over the expression, supplied spelled otherwise.
    let served = expr::spec("sym", "json_get(payload,'$.symbol')", Over::Exact).unwrap();
    let r = run(&["json_get( payload , '$[\"symbol\"]' )"], &[served]);
    assert_eq!(expr_lints(&r), Vec::<&Lint>::new());
}
