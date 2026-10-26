//! The `expr` corpus (`bellwether bench expr`): its ground truth equals an
//! independent oracle that decodes every data file with the `parquet` crate
//! and reads every JSON document with `serde_json`, the three selectivities
//! touch the row groups the layout promises, and a second run writes the
//! same bytes.

#![allow(clippy::unwrap_used, clippy::cast_possible_truncation)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::Path;

use arrow::array::AsArray;
use arrow::datatypes::TimestampMicrosecondType;
use bellwether_bench::corpus::DAY_US;
use bellwether_bench::expr::{
    generate_expr, start_us, ExprParams, ExprShape, ExprTruth, EXPR_TRUTH_FORMAT, MEDIUM_SPAN,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn params(shape: ExprShape) -> ExprParams {
    ExprParams {
        files: 3,
        row_groups_per_file: 10,
        rows_per_row_group: 80,
        ..ExprParams::small(shape)
    }
}

/// `(file, row group) -> (member values, timestamps)`, decoded.
type Decoded = BTreeMap<(String, u64), (Vec<Option<String>>, Vec<i64>)>;

fn decode(out: &Path, shape: ExprShape) -> Decoded {
    let mut rows = BTreeMap::new();
    let data = out.join("data");
    let mut names: Vec<String> = std::fs::read_dir(&data)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_str().unwrap().to_string())
        .collect();
    names.sort();
    let (json, ts) = match shape {
        ExprShape::Ledger => ("attrs", "version_ts"),
        ExprShape::Market => ("payload", "ts"),
    };
    for name in names {
        let path = data.join(&name);
        let n = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap())
            .unwrap()
            .metadata()
            .num_row_groups();
        for rg in 0..n {
            let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap())
                .unwrap()
                .with_row_groups(vec![rg])
                .build()
                .unwrap();
            let (mut vals, mut times) = (Vec::new(), Vec::new());
            for b in reader {
                let b = b.unwrap();
                let docs = b.column_by_name(json).unwrap().as_string::<i32>();
                let t = b
                    .column_by_name(ts)
                    .unwrap()
                    .as_primitive::<TimestampMicrosecondType>();
                for i in 0..b.num_rows() {
                    let v: serde_json::Value = serde_json::from_str(docs.value(i)).unwrap();
                    vals.push(
                        v.get(shape.member())
                            .and_then(|m| m.as_str().map(String::from)),
                    );
                    times.push(t.value(i));
                }
            }
            rows.insert((name.clone(), rg as u64), (vals, times));
        }
    }
    rows
}

#[test]
fn truth_equals_an_independent_decode_for_both_shapes() {
    for shape in [ExprShape::Ledger, ExprShape::Market] {
        let dir = tempfile::tempdir().unwrap();
        let p = params(shape);
        let summary = generate_expr(dir.path(), &p).unwrap();
        assert_eq!(summary.row_groups, 30);
        assert_eq!(summary.rows, 30 * 80);
        let truth: ExprTruth =
            serde_json::from_slice(&std::fs::read(dir.path().join("workload/truth.json")).unwrap())
                .unwrap();
        assert_eq!(truth.format, EXPR_TRUTH_FORMAT);
        let decoded = decode(dir.path(), shape);
        assert_eq!(decoded.len(), 30);
        let nulls: usize = decoded
            .values()
            .map(|(v, _)| v.iter().filter(|x| x.is_none()).count())
            .sum();
        assert_eq!(summary.null_rows, nulls as u64);
        let ids: Vec<&str> = truth.queries.iter().map(|q| q.id.as_str()).collect();
        match shape {
            ExprShape::Ledger => assert_eq!(ids, ["rare", "medium", "wide"]),
            ExprShape::Market => assert_eq!(ids, ["rare", "medium", "day", "wide"]),
        }
        for q in &truth.queries {
            let mut rgs = BTreeSet::new();
            let mut rows = 0u64;
            for (cell, (vals, times)) in &decoded {
                let hits = if q.id == "day" {
                    assert_eq!(q.expression, "date_trunc(ts,day)");
                    let day: i64 = q.keys[0].parse().unwrap();
                    times
                        .iter()
                        .filter(|t| {
                            (**t - start_us()).div_euclid(DAY_US) == (day - start_us()) / DAY_US
                        })
                        .count()
                } else {
                    assert_eq!(q.expression, shape.expression());
                    vals.iter()
                        .filter(|v| v.as_ref().is_some_and(|s| q.keys.contains(s)))
                        .count()
                };
                if hits > 0 {
                    rgs.insert(cell.clone());
                    rows += hits as u64;
                }
            }
            let want: BTreeSet<(String, u64)> = q.row_groups.iter().cloned().collect();
            assert_eq!(rgs, want, "{shape:?} {}", q.id);
            assert_eq!(rows, q.rows, "{shape:?} {}", q.id);
            // The selectivities the layout promises.
            match q.id.as_str() {
                "rare" => assert!(rgs.len() <= q.keys.len(), "{shape:?} rare: {}", rgs.len()),
                "medium" => assert!(
                    rgs.len() as u64 <= q.keys.len() as u64 * MEDIUM_SPAN
                        && rgs.len() > q.keys.len(),
                    "{shape:?} medium: {}",
                    rgs.len()
                ),
                "wide" => assert_eq!(rgs.len(), 30, "{shape:?} wide"),
                _ => {}
            }
            // The key file is named by the expression and holds the keys.
            let kf = dir.path().join(&q.keys_file);
            let b = ParquetRecordBatchReaderBuilder::try_new(File::open(&kf).unwrap())
                .unwrap()
                .build()
                .unwrap()
                .next()
                .unwrap()
                .unwrap();
            assert_eq!(b.schema().field(0).name(), &q.expression);
            assert_eq!(b.num_rows(), q.keys.len());
        }
    }
}

#[test]
fn a_second_run_writes_the_same_bytes() {
    let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let p = params(ExprShape::Market);
    generate_expr(a.path(), &p).unwrap();
    generate_expr(b.path(), &p).unwrap();
    for rel in [
        "data/part-0000.parquet",
        "data/part-0002.parquet",
        "workload/truth.json",
        "workload/keys-rare.parquet",
    ] {
        assert_eq!(
            std::fs::read(a.path().join(rel)).unwrap(),
            std::fs::read(b.path().join(rel)).unwrap(),
            "{rel}"
        );
    }
}
