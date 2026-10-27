//! Every condition of the reconcile's agg path (B7), enforced at the
//! verifier (the public `bellwether-verify` API), not only by the facade.
//! For each spec that must not be folded from an agg key, the indexed side
//! is read (its path is not `Agg`), the outcome equals the unindexed run's,
//! and `agg_not_used` names the agg key and the cause; an edit that the
//! spec leaves out of what is compared would fail the reconcile if the
//! side were folded, so a condition that stopped holding is caught. Also: a
//! multi-pass run with an agg side, and the one served control.

#![allow(clippy::unwrap_used, missing_docs)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use bellwether_build::{BuildOptions, Builder, SourceKind};
use bellwether_core::encoding::{encode_value_as, Value};
use bellwether_source::ParquetSource;
use bellwether_store::ObjectStoreBackend;
use bellwether_types::{AggregateFn, Hash, KeySpec, Predicate};
use bellwether_verify::{
    CertificateContext, CoverageRequest, IndexPlanner, Options, ReconcileSpec, Reconciler,
    Reconciliation, Side, SidePath, TotalColumn, WorkloadKeys,
};
use common::{batch_at, dirs, ledger, r, root, schema_at, write, write_batch, R};

fn ts(v: i64) -> Vec<u8> {
    let mut out = Vec::new();
    encode_value_as(
        &Value::Timestamp {
            value: v,
            unit: TimeUnit::Microsecond,
        },
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        &mut out,
    )
    .unwrap();
    out
}

fn utf8(s: &str) -> Vec<u8> {
    let mut out = Vec::new();
    encode_value_as(&Value::Utf8(s), &DataType::Utf8, &mut out).unwrap();
    out
}

/// Keys 1..=3, versions at ts 1000, 2000, 3000 (status `b` at 1000, else
/// `a`); `edit` adds one cent to the amount of (key, ts).
fn rows(edit: Option<(i64, i64)>) -> Vec<R> {
    let mut out = Vec::new();
    for k in 1..=3 {
        for v in 1..=3 {
            let mut x = r(
                k,
                1_000 * v,
                i128::from(k * 10 + v),
                if v == 1 { "b" } else { "a" },
            );
            if edit == Some((k, 1_000 * v)) {
                x.amount = x.amount.map(|a| a + 1);
            }
            out.push(x);
        }
    }
    out
}

fn canonical(dir: &std::path::Path) -> String {
    std::fs::canonicalize(dir)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string()
}

fn agg_specs(value: &str) -> Vec<KeySpec> {
    vec![
        KeySpec::Exact {
            columns: vec!["id".into()],
            counts: true,
            bounds: None,
        },
        KeySpec::Agg {
            columns: vec!["id".into()],
            value: value.into(),
            functions: vec![AggregateFn::Sum, AggregateFn::Count],
        },
    ]
}

/// The left side indexed with `exact:id,counts` + `agg:id@<value>{sum,count}`.
fn index(dir: &std::path::Path, value: &str) -> Arc<ObjectStoreBackend> {
    let store = Arc::new(ObjectStoreBackend::memory().unwrap());
    Builder::new(Arc::clone(&store), "idx")
        .build(
            &canonical(dir),
            &agg_specs(value),
            &BuildOptions::new(SourceKind::Parquet),
        )
        .unwrap();
    store
}

/// The reconcile of `left` (indexed, its source at `left_schema` or the
/// index's dataset schema) against `right`, and the same without the index.
fn both_dirs(
    left: &std::path::Path,
    right: &std::path::Path,
    spec: ReconcileSpec,
    value: &str,
    left_schema: Option<arrow::datatypes::SchemaRef>,
    options: Options,
) -> (Reconciliation, Reconciliation) {
    let store = index(left, value);
    let planner = IndexPlanner::open(&store, "idx").unwrap();
    let left_root = planner.source_root().to_string();
    let schema = left_schema.unwrap_or_else(|| Arc::clone(planner.schema()));
    let left_source = ParquetSource::new().with_schema(schema);
    let right_source = ParquetSource::new();
    let right_root = root(right);
    let rec = Reconciler::new(spec, options).unwrap();
    let ix = rec
        .certify(
            &Side::new(&left_source, &left_root).indexed(&planner),
            &Side::new(&right_source, &right_root),
            &CertificateContext::default(),
        )
        .unwrap()
        .outcome;
    let plain = rec
        .certify(
            &Side::new(&left_source, &left_root),
            &Side::new(&right_source, &right_root),
            &CertificateContext::default(),
        )
        .unwrap()
        .outcome;
    (ix, plain)
}

fn both(spec: ReconcileSpec, edit: Option<(i64, i64)>) -> (Reconciliation, Reconciliation) {
    let (l, rt) = dirs();
    write(l.path(), "x.parquet", &rows(None), 3);
    write(rt.path(), "x.parquet", &rows(edit), 3);
    both_dirs(
        l.path(),
        rt.path(),
        spec,
        "amount",
        None,
        Options::default(),
    )
}

fn base() -> ReconcileSpec {
    ReconcileSpec::new(["id"])
        .ignore(["ts", "status", "note", "amount"])
        .totals(["amount"])
}

/// The control: with only key columns compared and one decimal total, the
/// agg key stands in for the left side (no row read there), and a one-cent
/// edit is caught.
#[test]
fn control_the_agg_key_serves_and_catches_one_cent() {
    let (ix, plain) = both(base(), Some((2, 1_000)));
    assert!(
        matches!(ix.sides[0].path, SidePath::Agg { .. }),
        "{:?}",
        ix.sides[0].path
    );
    assert_eq!(ix.memory.rows_read[0], 0);
    assert!(ix.agg_not_used.is_empty(), "{:?}", ix.agg_not_used);
    assert_eq!((ix.pass, ix.counts.totals_differ), (false, 1));
    assert_eq!((plain.pass, plain.counts.totals_differ), (false, 1));
}

/// The side was read, the outcome is the unindexed one, and `agg_not_used`
/// names the left side's agg key with a cause containing `cause`.
fn assert_read(ix: &Reconciliation, plain: &Reconciliation, cause: &str) {
    assert!(
        !matches!(ix.sides[0].path, SidePath::Agg { .. }),
        "{cause}: folded from the agg key: {:?}",
        ix.sides[0].path
    );
    assert!(ix.memory.rows_read[0] > 0, "{cause}");
    assert_eq!(
        (ix.pass, ix.counts.totals_differ, ix.counts.mismatched),
        (
            plain.pass,
            plain.counts.totals_differ,
            plain.counts.mismatched
        ),
        "{cause}: differs from the unindexed run"
    );
    let [n] = ix.agg_not_used.as_slice() else {
        panic!("{cause}: {:?}", ix.agg_not_used)
    };
    assert_eq!(n.side, "left");
    assert_eq!(n.spec, "agg:id@amount{sum,count}");
    assert!(
        n.causes.iter().any(|c| c.contains(cause)),
        "{cause}: {:?}",
        n.causes
    );
}

/// A row filter selects rows inside [1500, 2500]; the edit is outside it,
/// so the reconcile passes. Folding every row from the agg key would fail.
#[test]
fn a_row_filter_is_never_folded_from_an_agg_key() {
    let spec = base().rows_where(vec![Predicate::Between {
        column: "ts".into(),
        lo: ts(1_500),
        hi: ts(2_500),
    }]);
    let (ix, plain) = both(spec, Some((2, 1_000)));
    assert!(plain.pass);
    assert_read(
        &ix,
        &plain,
        "a row filter keeps only some of each key's rows",
    );
}

/// The rows a workload's runs read (a row filter named after a workload)
/// are a scope of their own: never folded.
#[test]
fn a_workload_rows_scope_is_never_folded_from_an_agg_key() {
    let spec = base()
        .rows_where(vec![Predicate::Between {
            column: "ts".into(),
            lo: ts(1_500),
            hi: ts(2_500),
        }])
        .coverage(CoverageRequest::WorkloadRows {
            workload: Hash::of(b"w"),
            age: Duration::ZERO,
        });
    let (ix, plain) = both(spec, Some((2, 1_000)));
    assert!(plain.pass);
    assert_read(
        &ix,
        &plain,
        "only the rows a workload's runs read are compared",
    );
}

/// Keys that are values of an expression (`lower(status)` in `a`) scope
/// rows by that value; the edit is on a `b` row, so the reconcile passes.
#[test]
fn an_expression_scope_is_never_folded_from_an_agg_key() {
    let spec = base()
        .coverage(CoverageRequest::Workload(WorkloadKeys {
            workload: Hash::of(b"expr"),
            age: Duration::ZERO,
            keys: vec![utf8("a")],
        }))
        .keys_of("lower(status)");
    let (ix, plain) = both(spec, Some((2, 1_000)));
    assert!(plain.pass, "{:?}", plain.mismatches);
    assert_read(
        &ix,
        &plain,
        "the keys are values of the expression lower(status)",
    );
}

/// A version rule keeps the newest row per key; the edit is on an older
/// version, so the reconcile passes. Folding every row would fail.
#[test]
fn a_version_rule_is_never_folded_from_an_agg_key() {
    let spec = ReconcileSpec::new(["id"])
        .latest_by("ts")
        .ignore(["status", "note", "amount"])
        .totals(["amount"]);
    let (ix, plain) = both(spec, Some((2, 1_000)));
    assert!(plain.pass);
    assert_read(&ix, &plain, "the newest row per key by ts");
}

/// Totals grouped by a non-key column cannot come from per-key sums.
#[test]
fn a_group_by_other_than_the_key_is_never_folded() {
    let (ix, plain) = both(base().group_by(["status"]), Some((2, 1_000)));
    assert_read(&ix, &plain, "totals are grouped by status");
}

/// A compared non-key column needs every row's value; the cause names it
/// and the next step names the `ignore` list that lets the agg key answer.
#[test]
fn a_compared_non_key_column_is_never_folded_and_the_next_step_ignores_it() {
    let spec = ReconcileSpec::new(["id"])
        .ignore(["ts", "status"])
        .totals(["amount"]);
    let (ix, plain) = both(spec, Some((2, 1_000)));
    assert!(!plain.pass);
    assert_read(&ix, &plain, "amount, note are compared row by row");
    let next = ix.agg_not_used[0].next.as_deref().unwrap();
    assert!(next.contains("--ignore amount,note,status,ts"), "{next}");
}

/// The total alone compared row by row: the next step says ignoring it
/// compares it only through its exact total.
#[test]
fn a_compared_total_names_ignore_total_as_the_next_step() {
    let spec = ReconcileSpec::new(["id"])
        .ignore(["ts", "status", "note"])
        .totals(["amount"]);
    let (ix, plain) = both(spec, None);
    assert!(plain.pass);
    assert_read(&ix, &plain, "amount is compared row by row");
    let next = ix.agg_not_used[0].next.as_deref().unwrap();
    assert!(
        next.contains("--ignore amount,note,status,ts")
            && next.contains("compares amount only through its exact total"),
        "{next}"
    );
}

/// The side's types must be the index's: the left side read at scale 4
/// while its index holds scale-2 sums is read, never folded (folding would
/// total cents as ten-thousandths and fail against the right side).
#[test]
fn a_side_typed_unlike_its_index_is_never_folded() {
    let (l, rt) = dirs();
    write(l.path(), "x.parquet", &rows(None), 3);
    write_batch(rt.path(), "x.parquet", &batch_at(&rows(None), 4), 3);
    let (ix, plain) = both_dirs(
        l.path(),
        rt.path(),
        base(),
        "amount",
        Some(schema_at(4)),
        Options::default(),
    );
    assert!(plain.pass, "{:?}", plain.mismatches);
    assert_read(
        &ix,
        &plain,
        "amount is Decimal128(18, 4) on this side and Decimal128(18, 2) in its index",
    );
}

/// A quantized float total is never folded from an agg key's float sum,
/// which is never exact.
#[test]
fn a_quantized_float_total_is_never_folded() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("price", DataType::Float64, true),
    ]));
    let batch = |bump: f64| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2, 3])) as ArrayRef,
                Arc::new(Float64Array::from(vec![0.25, 1.5 + bump, 2.0, 3.0])),
            ],
        )
        .unwrap()
    };
    let (l, rt) = dirs();
    write_batch(l.path(), "x.parquet", &batch(0.0), 2);
    write_batch(rt.path(), "x.parquet", &batch(0.0), 2);
    let spec = ReconcileSpec::new(["id"])
        .ignore(["price"])
        .with_totals([TotalColumn::quantized("price", 2)]);
    let (ix, plain) = both_dirs(l.path(), rt.path(), spec, "price", None, Options::default());
    assert!(plain.pass);
    assert!(!matches!(ix.sides[0].path, SidePath::Agg { .. }));
    assert_eq!(ix.pass, plain.pass);
    let [n] = ix.agg_not_used.as_slice() else {
        panic!("{:?}", ix.agg_not_used)
    };
    assert!(
        n.causes
            .iter()
            .any(|c| c.contains("price is totalled quantized")),
        "{:?}",
        n.causes
    );
}

/// Over several passes the agg side's keys fold into the same partitions
/// as the right side's rows: the outcome equals the one-pass outcome, a
/// pass and a planted one-cent difference alike.
#[test]
fn a_multi_pass_reconcile_folds_the_agg_side_into_the_right_partitions() {
    let (l, rt, bad) = (
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    );
    let rows = ledger(3_000, 2);
    let mut edited = rows.clone();
    edited[1_234].amount = edited[1_234].amount.map(|a| a + 1);
    write(l.path(), "a.parquet", &rows, 500);
    write(rt.path(), "a.parquet", &rows, 700);
    write(bad.path(), "a.parquet", &edited, 700);
    let spec = || {
        ReconcileSpec::new(["id"])
            .ignore(["ts", "status", "note", "amount"])
            .totals(["amount"])
    };
    let small = Options {
        memory_budget: 64 * 1024,
        max_examples: 100,
    };
    for (right, pass) in [(&rt, true), (&bad, false)] {
        let (many, plain) = both_dirs(
            l.path(),
            right.path(),
            spec(),
            "amount",
            None,
            small.clone(),
        );
        assert!(matches!(many.sides[0].path, SidePath::Agg { .. }));
        assert!(many.memory.passes > 1, "{:?}", many.memory);
        assert_eq!(many.pass, pass, "{:?}", many.mismatches);
        assert_eq!(many.counts, plain.counts);
        assert_eq!(many.totals, plain.totals);
        assert_eq!(many.counts.keys_compared, 3_000);
        assert_eq!(many.counts.totals_differ, u64::from(!pass));
    }
}
