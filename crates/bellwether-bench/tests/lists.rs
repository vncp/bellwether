//! The list corpora (B4): deterministic, skewed as documented, and their
//! truth equal to an independent decode of the written files.

#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::many_single_char_names
)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::{Path, PathBuf};

use arrow::array::{Array, AsArray};
use arrow::datatypes::Int64Type;
use bellwether_bench::lists::{generate_lists, ListParams, ListShape, ListTruth};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn small(shape: ListShape) -> ListParams {
    ListParams {
        files: 6,
        row_groups_per_file: 3,
        rows_per_row_group: 200,
        vocabulary: 2_000,
        head: 10,
        terms: vec![10, 100, 1_000],
        ..ListParams::preset(shape)
    }
}

fn files(dir: &Path, out: &mut Vec<PathBuf>) {
    for e in std::fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        if p.is_dir() {
            files(&p, out);
        } else if p.extension().is_some_and(|x| x == "parquet") {
            out.push(p);
        }
    }
}

/// (file relative to data/, row group) -> rows as element strings.
fn decode(out: &Path, column: &str) -> BTreeMap<(String, u32), Vec<Option<Vec<String>>>> {
    let data = out.join("data");
    let mut paths = Vec::new();
    files(&data, &mut paths);
    let mut m = BTreeMap::new();
    for p in paths {
        let rel = p
            .strip_prefix(&data)
            .unwrap()
            .components()
            .map(|c| c.as_os_str().to_str().unwrap())
            .collect::<Vec<_>>()
            .join("/");
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
            let mut rows = Vec::new();
            for b in reader {
                let b = b.unwrap();
                let l = b.column_by_name(column).unwrap().as_list::<i32>();
                for i in 0..b.num_rows() {
                    rows.push((!l.is_null(i)).then(|| {
                        let v = l.value(i);
                        (0..v.len())
                            .map(|j| match v.data_type() {
                                arrow::datatypes::DataType::Int64 => {
                                    v.as_primitive::<Int64Type>().value(j).to_string()
                                }
                                _ => v.as_string::<i32>().value(j).to_string(),
                            })
                            .collect()
                    }));
                }
            }
            m.insert((rel.clone(), rg as u32), rows);
        }
    }
    m
}

fn terms(path: &Path) -> BTreeSet<String> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
        .unwrap()
        .build()
        .unwrap();
    let mut out = BTreeSet::new();
    for b in reader {
        let b = b.unwrap();
        let c = b.column(0);
        for i in 0..c.len() {
            out.insert(match c.data_type() {
                arrow::datatypes::DataType::Int64 => {
                    c.as_primitive::<Int64Type>().value(i).to_string()
                }
                _ => c.as_string::<i32>().value(i).to_string(),
            });
        }
    }
    out
}

#[test]
fn truth_equals_an_independent_decode_in_both_shapes() {
    for shape in [ListShape::Ledger, ListShape::Market] {
        let dir = tempfile::tempdir().unwrap();
        let p = small(shape);
        let truth = generate_lists(dir.path(), &p).unwrap();
        let column = shape.list_column();
        let rows = decode(dir.path(), column);
        assert_eq!(rows.len() as u64, truth.summary.row_groups);
        let on_disk: ListTruth =
            serde_json::from_slice(&std::fs::read(dir.path().join("workload/truth.json")).unwrap())
                .unwrap();
        assert_eq!(on_disk, truth);
        // element statistics
        let mut per: BTreeMap<String, BTreeSet<(String, u32)>> = BTreeMap::new();
        let (mut nulls, mut empties) = (0, 0);
        for (at, rs) in &rows {
            for r in rs {
                match r {
                    None => nulls += 1,
                    Some(v) if v.is_empty() => empties += 1,
                    Some(v) => {
                        for e in v {
                            per.entry(e.clone()).or_default().insert(at.clone());
                        }
                    }
                }
            }
        }
        assert_eq!(per.len() as u64, truth.summary.distinct_elements);
        assert_eq!(
            (nulls, empties),
            (truth.summary.null_lists, truth.summary.empty_lists)
        );
        assert!(nulls > 0 && empties > 0);
        // skew: the most frequent value is in every row group, the median
        // value in a few
        let [p50, _, max] = truth.summary.row_groups_per_element;
        assert_eq!(max, truth.summary.row_groups);
        assert!(p50 * 4 <= truth.summary.row_groups, "p50 {p50}");
        // every run: its rows and row groups, by value
        for (run, t) in on_disk_runs(dir.path()).iter().zip(&truth.runs) {
            let (file, all) = run;
            let want = terms(&dir.path().join(file));
            let (mut hit_rows, mut hit_rgs) = (0u64, Vec::new());
            for (at, rs) in &rows {
                let mut here = false;
                for r in rs.iter().flatten() {
                    let ok = if *all {
                        want.iter().all(|w| r.contains(w))
                    } else {
                        r.iter().any(|e| want.contains(e))
                    };
                    if ok {
                        hit_rows += 1;
                        here = true;
                    }
                }
                if here {
                    hit_rgs.push(at.clone());
                }
            }
            let got: Vec<(String, u32)> = t
                .row_groups
                .iter()
                .map(|g| (g.file.clone(), g.row_group))
                .collect();
            assert_eq!((t.rows, got), (hit_rows, hit_rgs), "{shape:?} {}", t.id);
            assert!(t.rows > 0, "{shape:?} {}: a run with no match", t.id);
        }
    }
}

fn on_disk_runs(out: &Path) -> Vec<(String, bool)> {
    let w: serde_json::Value =
        serde_json::from_slice(&std::fs::read(out.join("workload/lists.json")).unwrap()).unwrap();
    w["runs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            (
                r["file"].as_str().unwrap().to_string(),
                r["query_kind"] == "contains_all",
            )
        })
        .collect()
}

#[test]
fn generation_is_deterministic_and_refuses_bad_parameters() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let p = small(ListShape::Market);
    generate_lists(a.path(), &p).unwrap();
    generate_lists(b.path(), &p).unwrap();
    let (mut fa, mut fb) = (Vec::new(), Vec::new());
    files(a.path(), &mut fa);
    files(b.path(), &mut fb);
    fa.sort();
    fb.sort();
    assert_eq!(fa.len(), fb.len());
    for (x, y) in fa.iter().zip(&fb) {
        assert_eq!(
            std::fs::read(x).unwrap(),
            std::fs::read(y).unwrap(),
            "{x:?}"
        );
    }
    // the same --out twice is refused
    let e = generate_lists(a.path(), &p).unwrap_err().to_string();
    assert!(e.contains("empty --out"), "{e}");
    // more terms than the vocabulary
    let c = tempfile::tempdir().unwrap();
    let e = generate_lists(
        c.path(),
        &ListParams {
            terms: vec![5_000],
            ..small(ListShape::Ledger)
        },
    )
    .unwrap_err()
    .to_string();
    assert!(e.contains("5000 terms"), "{e}");
}
