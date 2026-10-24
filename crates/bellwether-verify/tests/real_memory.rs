//! ADR 018 measured, not modelled: the real heap peak of a multi-pass
//! reconcile stays within the budget plus one row group's batches. The
//! meter is an accounting model; this test catches state that grows or is
//! retained without going through it (for example a pass's map kept alive).
//!
//! One test per binary: the global allocator's counters are process-wide.

#![allow(clippy::unwrap_used, missing_docs)]

use std::alloc::System;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether_source::ParquetSource;
use bellwether_verify::{Options, ReconcileSpec, Reconciler, Side};
use cap::Cap;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

#[global_allocator]
static ALLOC: Cap<System> = Cap::new(System, usize::MAX);

const KEYS: i64 = 600_000;
const GROUP: i64 = 50_000;
const MIB: u64 = 1024 * 1024;

/// Writes `id, v` for every key, one row group per batch so the writer
/// never holds more than one.
fn write(dir: &Path, edit: Option<i64>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let file = std::fs::File::create(dir.join("x.parquet")).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_size(usize::try_from(GROUP).unwrap())
        .build();
    let mut w = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
    for start in (0..KEYS).step_by(usize::try_from(GROUP).unwrap()) {
        let ids: Vec<i64> = (start..start + GROUP).collect();
        let vs: Vec<i64> = ids
            .iter()
            .map(|&i| if Some(i) == edit { -1 } else { i * 3 })
            .collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)) as ArrayRef,
                Arc::new(Int64Array::from(vs)),
            ],
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.flush().unwrap();
    }
    w.close().unwrap();
}

#[test]
fn the_real_heap_peak_stays_within_the_budget_plus_one_batch() {
    let (l, r) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    write(l.path(), None);
    write(r.path(), Some(12_345));
    let (ls, rs) = (ParquetSource::new(), ParquetSource::new());
    let (lr, rr) = (
        l.path().to_str().unwrap().to_string(),
        r.path().to_str().unwrap().to_string(),
    );
    let budget = 64 * MIB;
    let reconciler = Reconciler::new(
        ReconcileSpec::new(["id"]),
        Options {
            memory_budget: budget,
            ..Options::default()
        },
    )
    .unwrap();

    let max_before = ALLOC.max_allocated() as u64;
    let before = ALLOC.allocated() as u64;
    let out = reconciler
        .run(&Side::new(&ls, &lr), &Side::new(&rs, &rr))
        .unwrap();
    let peak = ALLOC.max_allocated() as u64;

    assert_eq!(out.counts.mismatched, 1);
    assert!(out.memory.passes > 1, "{:?}", out.memory);
    assert!(
        peak > max_before,
        "the run must set the process peak ({peak} vs {max_before} before) for the \
         measurement to mean anything"
    );
    let growth = peak - before;
    // One batch alive at a time, plus its decode buffers, plus the footer
    // cache and fixed overhead.
    let allowance = budget + 4 * out.memory.peak_batch_bytes + 16 * MIB;
    assert!(
        growth <= allowance,
        "heap grew {growth} bytes over a {budget}-byte budget ({:?})",
        out.memory
    );
    // The meter over-approximates what the allocator holds for the state.
    assert!(
        growth <= out.memory.peak_state_bytes + 4 * out.memory.peak_batch_bytes + 16 * MIB,
        "heap grew {growth} bytes, the meter peaked at {}",
        out.memory.peak_state_bytes
    );
    eprintln!(
        "real heap growth {growth} B, meter peak {} B, budget {budget} B, {} passes",
        out.memory.peak_state_bytes, out.memory.passes
    );
}
