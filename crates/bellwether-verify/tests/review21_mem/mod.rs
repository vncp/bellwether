//! Shared body of the review's real-heap probes (one test per binary,
//! because the counting allocator is process-wide).

#![allow(
    clippy::unwrap_used,
    clippy::pedantic,
    clippy::cloned_ref_to_slice_refs,
    missing_docs,
    dead_code
)]

use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Decimal128Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_source::ParquetSource;
use bellwether_verify::{Options, ReconcileSpec, Reconciler, Reconciliation, Side};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

pub const MIB: u64 = 1024 * 1024;

pub fn schema(string_key: bool) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "k",
            if string_key {
                DataType::Utf8
            } else {
                DataType::Int64
            },
            false,
        ),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("amt", DataType::Decimal128(18, 2), true),
        Field::new("grp", DataType::Utf8, false),
    ]))
}

/// `keys` keys, `versions` versions each, written one row group per batch.
pub fn write(dir: &Path, keys: i64, versions: i64, string_key: bool, edit: Option<i64>) {
    let schema = schema(string_key);
    let file = std::fs::File::create(dir.join("x.parquet")).unwrap();
    let group = 40_000i64;
    let props = WriterProperties::builder()
        .set_max_row_group_size(usize::try_from(group).unwrap())
        .build();
    let mut w = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
    for v in 0..versions {
        let mut start = 0;
        while start < keys {
            let end = (start + group).min(keys);
            let ids: Vec<i64> = (start..end).collect();
            let k: ArrayRef = if string_key {
                Arc::new(StringArray::from(
                    ids.iter()
                        .map(|i| format!("customer-{i:0>54}"))
                        .collect::<Vec<_>>(),
                ))
            } else {
                Arc::new(Int64Array::from(ids.clone()))
            };
            let ts: Vec<i64> = ids.iter().map(|_| 1_000 * (v + 1)).collect();
            let amt: Vec<i128> = ids
                .iter()
                .map(|&i| {
                    if Some(i) == edit && v == versions - 1 {
                        -1
                    } else {
                        i128::from(i % 1_000 + v)
                    }
                })
                .collect();
            let grp: Vec<&str> = ids
                .iter()
                .map(|i| ["a", "b", "c"][usize::try_from(i % 3).unwrap()])
                .collect();
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    k,
                    Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
                    Arc::new(
                        Decimal128Array::from(amt)
                            .with_precision_and_scale(18, 2)
                            .unwrap(),
                    ),
                    Arc::new(StringArray::from(grp)),
                ],
            )
            .unwrap();
            w.write(&batch).unwrap();
            w.flush().unwrap();
            start = end;
        }
    }
    w.close().unwrap();
}

pub fn run(l: &Path, r: &Path, spec: ReconcileSpec, budget: u64) -> Reconciliation {
    let (ls, rs) = (ParquetSource::new(), ParquetSource::new());
    let (lr, rr) = (
        l.to_str().unwrap().to_string(),
        r.to_str().unwrap().to_string(),
    );
    Reconciler::new(
        spec,
        Options {
            memory_budget: budget,
            ..Options::default()
        },
    )
    .unwrap()
    .run(&Side::new(&ls, &lr), &Side::new(&rs, &rr))
    .unwrap()
}

/// Prints and checks one measurement.
pub fn report(
    name: &str,
    budget: u64,
    before: u64,
    max_before: u64,
    peak: u64,
    out: &Reconciliation,
) {
    let growth = peak.saturating_sub(before);
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_wrap)]
    {
        eprintln!(
            "R21MEM {name}: budget {budget} B; real growth {growth} B ({:.1}% of budget); \
             meter peak {} B; peak batch {} B; growth - budget - batch = {} B; \
             passes {} restarts {}; set process peak: {}; mismatched {}",
            growth as f64 * 100.0 / budget as f64,
            out.memory.peak_state_bytes,
            out.memory.peak_batch_bytes,
            growth as i64 - budget as i64 - out.memory.peak_batch_bytes as i64,
            out.memory.passes,
            out.memory.restarts,
            peak > max_before,
            out.counts.mismatched,
        );
    }
    assert!(peak > max_before, "the run must set the process peak");
    assert!(out.memory.passes > 1, "{:?}", out.memory);
    assert!(out.memory.peak_state_bytes <= budget);
}
