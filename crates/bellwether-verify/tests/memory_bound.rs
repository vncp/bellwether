//! ADR 018: the comparison state stays within the budget at any pass count,
//! and the result does not depend on the pass count.

#![allow(clippy::unwrap_used, missing_docs)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::DataType;
use bellwether_core::encoding::{encode_tuple_as, Value};
use bellwether_types::Hash;
use bellwether_verify::{
    CoverageRequest, MemoryReport, Options, ReconcileSpec, Reconciliation, VerifyError,
    WorkloadKeys,
};
use common::{dirs, ledger, reconcile_with, write, write_batch, R};

fn options(memory_budget: u64) -> Options {
    Options {
        memory_budget,
        max_examples: 1_000,
    }
}

/// Everything but the memory report, which depends on the budget.
fn strip(mut o: Reconciliation) -> Reconciliation {
    o.memory = MemoryReport::default();
    o
}

/// 3,000 keys with two versions, a few edits, a few one-sided keys, and a
/// few null versions, in several files and row groups per side.
fn corpus() -> (tempfile::TempDir, tempfile::TempDir) {
    let (l, r) = dirs();
    let rows = ledger(3_000, 2);
    let mut right: Vec<R> = Vec::with_capacity(rows.len());
    for (i, x) in rows.iter().enumerate() {
        let mut x = x.clone();
        match i % 97 {
            0 => continue,
            1 => x.note = Some("edited".into()),
            2 => x.ts = None,
            3 => x.amount = Some(x.amount.unwrap() + 3),
            _ => {}
        }
        right.push(x);
    }
    right.push(common::r(9_999, 1, 1, "a"));
    write(l.path(), "a.parquet", &rows[..2_500], 300);
    write(l.path(), "b.parquet", &rows[2_500..], 500);
    write(r.path(), "a.parquet", &right, 700);
    (l, r)
}

fn specs() -> Vec<ReconcileSpec> {
    let keys: Vec<Vec<u8>> = (0..3_000)
        .step_by(7)
        .map(|i| encode_tuple_as(&[Value::Int(i)], &[DataType::Int64]).unwrap())
        .collect();
    vec![
        ReconcileSpec::new(["id"]),
        ReconcileSpec::new(["id"])
            .totals(["amount"])
            .group_by(["status"]),
        ReconcileSpec::new(["id"])
            .latest_by("ts")
            .totals(["amount"])
            .group_by(["status"]),
        ReconcileSpec::new(["id"])
            .latest_by("ts")
            .coverage(CoverageRequest::Sampled {
                fraction: 0.5,
                seed: 3,
            }),
        ReconcileSpec::new(["id"])
            .totals(["amount"])
            .coverage(CoverageRequest::Workload(WorkloadKeys {
                workload: Hash::of(b"w"),
                age: Duration::ZERO,
                keys,
            })),
    ]
}

#[test]
fn the_memory_bound_holds_at_p_gt_1_and_results_equal_the_p_1_results() {
    let (l, r) = corpus();
    for spec in specs() {
        // 100 examples: they persist across passes, and small budgets must
        // leave the per-pass state room to shrink into.
        let few = |memory_budget| Options {
            memory_budget,
            max_examples: 100,
        };
        let one = reconcile_with(l.path(), r.path(), spec.clone(), few(1 << 30)).unwrap();
        assert_eq!(one.memory.passes, 1, "{spec:?}");
        assert!(!one.pass, "the corpus has differences");
        for budget in [96 * 1024, 64 * 1024] {
            let many = reconcile_with(l.path(), r.path(), spec.clone(), few(budget)).unwrap();
            assert!(
                many.memory.passes > 1,
                "{spec:?} at {budget}: {:?}",
                many.memory
            );
            assert!(
                many.memory.peak_state_bytes <= budget,
                "{spec:?}: peak {} over budget {budget}",
                many.memory.peak_state_bytes
            );
            assert!(many.memory.peak_state_bytes > 0);
            if many.memory.restarts == 0 {
                assert_eq!(
                    many.memory.rows_read,
                    [
                        one.sides[0].rows * u64::from(many.memory.passes),
                        one.sides[1].rows * u64::from(many.memory.passes)
                    ],
                    "each pass reads every row once"
                );
            }
            assert_eq!(strip(many), strip(one.clone()), "{spec:?} at {budget}");
        }
    }
}

fn long_keys(n: usize, width: usize, edit: Option<usize>) -> RecordBatch {
    let keys: Vec<String> = (0..n).map(|i| format!("{i:0>width$}")).collect();
    let vals: Vec<i64> = (0..n)
        .map(|i| {
            if Some(i) == edit {
                -1
            } else {
                i64::try_from(i).unwrap()
            }
        })
        .collect();
    RecordBatch::try_from_iter([
        ("k", Arc::new(StringArray::from(keys)) as ArrayRef),
        ("v", Arc::new(Int64Array::from(vals))),
    ])
    .unwrap()
}

#[test]
fn an_underestimate_restarts_with_more_passes_and_still_holds_the_bound() {
    // The pass estimate assumes 32-byte string keys; these are 1,500 bytes.
    let (l, r) = dirs();
    write_batch(l.path(), "x.parquet", &long_keys(1_500, 1_500, None), 500);
    write_batch(
        r.path(),
        "x.parquet",
        &long_keys(1_500, 1_500, Some(77)),
        500,
    );
    let spec = ReconcileSpec::new(["k"]);
    let one = reconcile_with(l.path(), r.path(), spec.clone(), options(1 << 30)).unwrap();
    let budget = 300 * 1024;
    let out = reconcile_with(l.path(), r.path(), spec, options(budget)).unwrap();
    assert!(out.memory.restarts >= 1, "{:?}", out.memory);
    assert!(out.memory.peak_state_bytes <= budget, "{:?}", out.memory);
    assert_eq!(out.counts.mismatched, 1);
    assert_eq!(strip(out), strip(one));
}

#[test]
fn a_budget_that_cannot_hold_one_key_is_an_error() {
    let (l, r) = dirs();
    let rows = ledger(50, 1);
    write(l.path(), "x.parquet", &rows, 10);
    write(r.path(), "x.parquet", &rows, 10);
    let err =
        reconcile_with(l.path(), r.path(), ReconcileSpec::new(["id"]), options(100)).unwrap_err();
    assert!(matches!(err, VerifyError::Budget(_)), "{err}");
    let err =
        reconcile_with(l.path(), r.path(), ReconcileSpec::new(["id"]), options(0)).unwrap_err();
    assert!(matches!(err, VerifyError::Budget(_)), "{err}");
}

#[test]
fn too_many_groups_for_the_budget_is_an_error_not_a_loop() {
    let (l, r) = dirs();
    // One group per key: the group table alone outgrows half the budget.
    let rows = ledger(2_000, 1);
    write(l.path(), "x.parquet", &rows, 500);
    write(r.path(), "x.parquet", &rows, 500);
    let spec = ReconcileSpec::new(["id"])
        .totals(["amount"])
        .group_by(["note"]);
    let err = reconcile_with(l.path(), r.path(), spec, options(128 * 1024)).unwrap_err();
    assert!(
        matches!(err, VerifyError::Budget(ref m) if m.contains("group_by")),
        "{err}"
    );
}

#[test]
fn an_integer_key_range_in_statistics_bounds_the_pass_estimate() {
    // 1,000 dense ids with 8 versions each: 8,000 rows but at most 1,001
    // distinct keys, which the id column's min and max prove.
    let (l, r) = dirs();
    let rows = ledger(1_000, 8);
    write(l.path(), "x.parquet", &rows, 1_000);
    write(r.path(), "x.parquet", &rows, 1_000);
    let budget = 700 * 1024;
    let out = reconcile_with(
        l.path(),
        r.path(),
        ReconcileSpec::new(["id"]),
        options(budget),
    )
    .unwrap();
    assert!(out.pass);
    assert_eq!(
        (out.memory.passes, out.memory.restarts),
        (1, 0),
        "{:?}",
        out.memory
    );
    assert!(out.memory.peak_state_bytes <= budget);
    // Keyed by the note column (one string per id: the same keys), there is
    // no range bound, the estimate falls back to rows, and it plans several
    // passes where one would have done.
    let spec = ReconcileSpec::new(["note"]);
    let out = reconcile_with(l.path(), r.path(), spec, options(budget)).unwrap();
    assert!(out.memory.passes > 1, "{:?}", out.memory);
}
