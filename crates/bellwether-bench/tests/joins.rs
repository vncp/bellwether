//! The join corpora (B3): deterministic, skewed as documented, and their
//! truth equal to an independent decode of both datasets' files.

#![allow(clippy::unwrap_used, missing_docs, clippy::cast_possible_truncation)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::Path;

use arrow::array::{Array, AsArray};
use arrow::datatypes::Int64Type;
use bellwether_bench::joins::{generate_joins, JoinParams, JoinShape, JoinTruth, JoinWorkload};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn small(shape: JoinShape) -> JoinParams {
    JoinParams {
        size: if shape == JoinShape::Ledger {
            6_000
        } else {
            120
        },
        symbols: 40,
        rows_per_row_group: 400,
        row_groups_per_file: 3,
        keysets: vec![10, 100, 1_000],
        ..JoinParams::preset(shape)
    }
}

type Cells = BTreeMap<String, (BTreeSet<(String, u32)>, u64)>;

/// Per join key (rendered), the (file, row group) cells holding it and its
/// rows, decoded with `parquet`.
fn decode(dir: &Path, columns: &[String]) -> Cells {
    let mut out: Cells = BTreeMap::new();
    let mut paths: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    paths.sort();
    for p in paths {
        let rel = p.file_name().unwrap().to_str().unwrap().to_string();
        let n = ParquetRecordBatchReaderBuilder::try_new(File::open(&p).unwrap())
            .unwrap()
            .metadata()
            .num_row_groups();
        for rg in 0..n {
            let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&p).unwrap())
                .unwrap()
                .with_row_groups(vec![rg])
                .build()
                .unwrap();
            for b in reader {
                let b = b.unwrap();
                for row in 0..b.num_rows() {
                    let key: Vec<String> = columns
                        .iter()
                        .map(|c| {
                            let a = b.column_by_name(c).unwrap();
                            match a.data_type() {
                                arrow::datatypes::DataType::Utf8 => {
                                    a.as_string::<i32>().value(row).to_string()
                                }
                                _ => a.as_primitive::<Int64Type>().value(row).to_string(),
                            }
                        })
                        .collect();
                    let e = out.entry(key.join("|")).or_default();
                    e.0.insert((rel.clone(), rg as u32));
                    e.1 += 1;
                }
            }
        }
    }
    out
}

fn asked(path: &Path, columns: &[String]) -> Vec<String> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
        .unwrap()
        .build()
        .unwrap();
    let mut out = Vec::new();
    for b in reader {
        let b = b.unwrap();
        for row in 0..b.num_rows() {
            let key: Vec<String> = columns
                .iter()
                .map(|c| {
                    let a = b.column_by_name(c).unwrap();
                    match a.data_type() {
                        arrow::datatypes::DataType::Utf8 => {
                            a.as_string::<i32>().value(row).to_string()
                        }
                        _ => a.as_primitive::<Int64Type>().value(row).to_string(),
                    }
                })
                .collect();
            out.push(key.join("|"));
        }
    }
    out
}

#[test]
fn the_truth_is_the_decoded_files_and_the_corpus_is_deterministic() {
    for shape in [JoinShape::Ledger, JoinShape::Market] {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("c");
        let truth = generate_joins(&out, &small(shape)).unwrap();
        let w: JoinWorkload =
            serde_json::from_slice(&std::fs::read(out.join("workload/joins.json")).unwrap())
                .unwrap();
        let written: JoinTruth =
            serde_json::from_slice(&std::fs::read(out.join("workload/truth.json")).unwrap())
                .unwrap();
        assert_eq!(written, truth);
        let (l, r) = shape.datasets();
        assert_eq!((w.left.as_str(), w.right.as_str()), (l, r));
        assert_eq!(
            w.spec,
            format!(
                "join:{l}.{}={r}.{}",
                shape.columns().join(","),
                shape.columns().join(",")
            )
        );
        let (lk, rk) = (
            decode(&out.join(l), &w.columns),
            decode(&out.join(r), &w.columns),
        );
        let s = &truth.summary;
        assert_eq!(s.left.keys, lk.len() as u64);
        assert_eq!(s.right.keys, rk.len() as u64);
        assert_eq!(s.left.rows, lk.values().map(|v| v.1).sum::<u64>());
        assert_eq!(s.right.rows, rk.values().map(|v| v.1).sum::<u64>());
        let both: Vec<&String> = lk.keys().filter(|k| rk.contains_key(*k)).collect();
        assert_eq!(s.both, both.len() as u64);
        // 1:N skew: the most right rows per shared key is well above the
        // median, and some keys are one-sided each way.
        assert!(
            s.right_rows_per_key[2] > 2 * s.right_rows_per_key[0],
            "{s:?}"
        );
        assert!(lk.len() > both.len() && rk.len() > both.len(), "{s:?}");
        for (run, t) in w.runs.iter().zip(&truth.runs) {
            assert_eq!(run.id, t.id);
            let keys = asked(&out.join(&run.file), &w.columns);
            assert_eq!(keys.len() as u64, run.keys);
            assert_eq!(keys.iter().collect::<BTreeSet<_>>().len(), keys.len());
            let matched: Vec<&String> = keys
                .iter()
                .filter(|k| lk.contains_key(*k) && rk.contains_key(*k))
                .collect();
            assert_eq!(t.matched, matched.len() as u64, "{shape:?} {}", run.id);
            for (side, cells, got) in [("left", &lk, &t.left), ("right", &rk, &t.right)] {
                let rgs: BTreeSet<(String, u32)> = matched
                    .iter()
                    .flat_map(|k| cells[*k].0.iter().cloned())
                    .collect();
                let rows: u64 = matched.iter().map(|k| cells[*k].1).sum();
                let located: BTreeSet<(String, u32)> = keys
                    .iter()
                    .filter_map(|k| cells.get(k))
                    .flat_map(|v| v.0.iter().cloned())
                    .collect();
                let what = format!("{shape:?} {} {side}", run.id);
                assert_eq!(
                    got.row_groups,
                    rgs.into_iter().collect::<Vec<_>>(),
                    "{what}"
                );
                assert_eq!(got.rows, rows, "{what}");
                assert_eq!(got.located, located.len() as u64, "{what}");
            }
        }
        // Deterministic: the same parameters write the same bytes.
        let again = dir.path().join("d");
        assert_eq!(generate_joins(&again, &small(shape)).unwrap(), truth);
        for name in [l, r] {
            let a = std::fs::read(out.join(name).join("part-0000.parquet")).unwrap();
            let b = std::fs::read(again.join(name).join("part-0000.parquet")).unwrap();
            assert_eq!(a, b, "{shape:?} {name}");
        }
        // A corpus is never overwritten.
        assert!(generate_joins(&out, &small(shape)).is_err());
    }
}
