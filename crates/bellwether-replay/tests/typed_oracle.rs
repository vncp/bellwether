//! Invariant 1 through replay on the typed corpus (adopted from the PR 18
//! review's probes): nulls, `+0.0`/`-0.0` in different row groups, NaN,
//! `Decimal128(10,2)` and `Decimal128(12,3)` files under a dataset
//! `Decimal128(20,3)`, `u64 >= 2^63` and `u32 >= 2^31` in files carrying
//! embedded bloom filters, strings with NUL, and keys absent from every file.
//! The oracle shares no code with bellwether; every cost is checked against
//! footers read with the `parquet` crate.

#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]

mod typed;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Decimal128Array, Float64Array, Int64Array, RecordBatch, StringArray, UInt32Array,
    UInt64Array,
};
use bellwether_replay::{BaselinePlanner, KeyInput, MemoryKeySets, Replayer, WorkloadRecorder};
use bellwether_source::{FingerprintPolicy, ParquetSource};
use bellwether_types::KeySpec;
use typed::{cells, cost_of_cells, Cell, Corpus, V};

struct Run {
    id: &'static str,
    columns: Vec<&'static str>,
    keys: Vec<Vec<Option<V>>>,
    batch: RecordBatch,
}

fn one(id: &'static str, col: &'static str, arr: ArrayRef, keys: Vec<Option<V>>) -> Run {
    Run {
        id,
        columns: vec![col],
        keys: keys.into_iter().map(|k| vec![k]).collect(),
        batch: RecordBatch::try_from_iter(vec![(col, arr)]).unwrap(),
    }
}

fn runs() -> Vec<Run> {
    let i = |v: &[Option<i64>]| Arc::new(Int64Array::from(v.to_vec())) as ArrayRef;
    let f = |v: &[Option<f64>]| Arc::new(Float64Array::from(v.to_vec())) as ArrayRef;
    let s = |v: &[Option<&str>]| Arc::new(StringArray::from(v.to_vec())) as ArrayRef;
    let dec = |v: &[Option<i128>], p: u8, sc: i8| {
        Arc::new(
            Decimal128Array::from(v.to_vec())
                .with_precision_and_scale(p, sc)
                .unwrap(),
        ) as ArrayRef
    };
    let iv = |v: &[Option<i64>]| v.iter().map(|x| x.map(V::I)).collect::<Vec<_>>();
    let mut out = vec![
        {
            let v = [
                Some(-5),
                Some(0),
                Some(7),
                Some(999),
                None,
                Some(i64::MIN),
                Some(1000),
                Some(1150),
            ];
            one("i64", "k_i64", i(&v), iv(&v))
        },
        {
            let v = [Some(123_456_789), Some(987_654_321)];
            one("i64-absent", "k_i64", i(&v), iv(&v))
        },
        one("f64-pos0", "k_f64", f(&[Some(0.0)]), vec![Some(V::F(0.0))]),
        one(
            "f64-neg0",
            "k_f64",
            f(&[Some(-0.0)]),
            vec![Some(V::F(-0.0))],
        ),
        one(
            "f64-nan",
            "k_f64",
            f(&[Some(f64::NAN)]),
            vec![Some(V::F(f64::NAN))],
        ),
        one(
            "f64-mix",
            "k_f64",
            f(&[Some(1.5), Some(42.0), None]),
            vec![Some(V::F(1.5)), Some(V::F(42.0)), None],
        ),
        // Recorded at scale 2, 3, and 4; the dataset scale is 3.
        one(
            "dec-s2",
            "k_dec",
            dec(&[Some(150), Some(999), Some(-100)], 10, 2),
            vec![Some(V::D(1500)), Some(V::D(9990)), Some(V::D(-1000))],
        ),
        one(
            "dec-s3",
            "k_dec",
            dec(&[Some(1501), Some(123_457), Some(7)], 12, 3),
            vec![Some(V::D(1501)), Some(V::D(123_457)), Some(V::D(7))],
        ),
        one(
            "dec-s4",
            "k_dec",
            dec(&[Some(22_500), Some(0)], 15, 4),
            vec![Some(V::D(2250)), Some(V::D(0))],
        ),
        one(
            "str",
            "k_str",
            s(&[
                Some("a"),
                Some("a\0b"),
                Some("nope"),
                Some(""),
                None,
                Some("s17"),
            ]),
            vec![
                Some(V::S("a".into())),
                Some(V::S("a\0b".into())),
                Some(V::S("nope".into())),
                Some(V::S(String::new())),
                None,
                Some(V::S("s17".into())),
            ],
        ),
        one(
            "u64",
            "k_u64",
            Arc::new(UInt64Array::from(vec![(1u64 << 63) + 5, u64::MAX, 3, 150])),
            vec![
                Some(V::U((1 << 63) + 5)),
                Some(V::U(u64::MAX)),
                Some(V::U(3)),
                Some(V::U(150)),
            ],
        ),
        one(
            "u32",
            "k_u32",
            Arc::new(UInt32Array::from(vec![(1u32 << 31) + 3, u32::MAX, 8, 150])),
            vec![
                Some(V::U((1 << 31) + 3)),
                Some(V::U(u64::from(u32::MAX))),
                Some(V::U(8)),
                Some(V::U(150)),
            ],
        ),
    ];
    let pairs: Vec<(Option<i64>, Option<&str>)> = vec![
        (Some(0), Some("a")),
        (Some(7), Some("a")),
        (Some(7), Some("\u{e9}")),
        (Some(-5), Some("")),
        (Some(7), Some("zz")),
        (Some(1000), Some("s1")),
    ];
    out.push(pair_run("pair", &pairs));
    out.push(pair_run("pair-null", &[(Some(0), None), (None, Some("a"))]));
    out
}

fn pair_run(id: &'static str, pairs: &[(Option<i64>, Option<&str>)]) -> Run {
    let a: Vec<Option<i64>> = pairs.iter().map(|p| p.0).collect();
    let b: Vec<Option<&str>> = pairs.iter().map(|p| p.1).collect();
    Run {
        id,
        columns: vec!["k_i64", "k_str"],
        keys: pairs
            .iter()
            .map(|(x, y)| vec![x.map(V::I), y.map(|s| V::S(s.to_string()))])
            .collect(),
        batch: RecordBatch::try_from_iter(vec![
            ("k_i64", Arc::new(Int64Array::from(a)) as ArrayRef),
            ("k_str", Arc::new(StringArray::from(b)) as ArrayRef),
        ])
        .unwrap(),
    }
}

fn cols(c: &[&str]) -> Vec<String> {
    c.iter().map(|s| (*s).to_string()).collect()
}

fn exact(c: &[&str]) -> KeySpec {
    KeySpec::Exact {
        columns: cols(c),
        counts: false,
        bounds: None,
    }
}

enum Want {
    /// Two-sided: exactly the oracle.
    Equal,
    /// One-sided: contains the oracle.
    Contains,
    /// Exactly a given set (per-key intersection of exact operands).
    Exactly(BTreeSet<Cell>),
}

fn configs(run: &Run, corpus: &Corpus) -> Vec<(Vec<KeySpec>, Want)> {
    if run.columns.len() == 2 {
        // The per-key intersection of two exact single-column specs,
        // computed from the oracle tuple by tuple.
        let mut per_key = BTreeSet::new();
        for t in &run.keys {
            if t.iter().any(Option::is_none) {
                continue;
            }
            let a = typed::oracle(corpus, &["k_i64"], &[vec![t[0].clone()]]);
            let b = typed::oracle(corpus, &["k_str"], &[vec![t[1].clone()]]);
            per_key.extend(a.intersection(&b).cloned());
        }
        return vec![
            (vec![exact(&["k_i64", "k_str"])], Want::Equal),
            (vec![exact(&["k_str", "k_i64"])], Want::Equal),
            (
                vec![KeySpec::Temporal { tombstone: None,
                    columns: cols(&["k_i64", "k_str"]),
                    ts_column: "ts".into(),
                    counts: false,
                }],
                Want::Equal,
            ),
            (
                vec![exact(&["k_i64"]), exact(&["k_str"])],
                Want::Exactly(per_key),
            ),
            (
                vec![
                    KeySpec::Bloom {
                        columns: cols(&["k_i64"]),
                        fpr: 0.05,
                        bounds: None,
                    },
                    exact(&["k_str"]),
                ],
                Want::Contains,
            ),
            (
                vec![KeySpec::Range {
                    column: "k_i64".into(),
                }],
                Want::Contains,
            ),
        ];
    }
    let c = run.columns[0];
    let other = if c == "k_str" { "k_i64" } else { "k_str" };
    let bloom = KeySpec::Bloom {
        columns: cols(&[c]),
        fpr: 0.05,
        bounds: None,
    };
    let range = KeySpec::Range { column: c.into() };
    let prefix = exact(&[c, other]);
    vec![
        (vec![exact(&[c])], Want::Equal),
        (
            vec![KeySpec::Temporal { tombstone: None,
                columns: cols(&[c]),
                ts_column: "ts".into(),
                counts: false,
            }],
            Want::Equal,
        ),
        (vec![prefix.clone()], Want::Equal),
        (vec![range.clone(), exact(&[c])], Want::Equal),
        (vec![bloom.clone(), prefix], Want::Equal),
        (vec![bloom.clone()], Want::Contains),
        (vec![range.clone()], Want::Contains),
        (vec![bloom, range], Want::Contains),
    ]
}

#[test]
fn invariant_1_two_sided_on_the_typed_corpus_with_footer_bytes() {
    let corpus = typed::write_corpus(6, 3, 40, 2026);
    let schema = typed::dataset_schema();
    let source = ParquetSource::new().with_schema(Arc::clone(&schema));
    let replayer = Replayer::unindexed(
        &source,
        &corpus.data,
        &bellwether_source::Partitioning::None,
    )
    .unwrap();
    let footer = typed::footer_bytes(&corpus);
    let keys = MemoryKeySets::new();
    let mut rec = WorkloadRecorder::new(&keys, Arc::clone(&schema));
    let runs = runs();
    for r in &runs {
        rec.record(r.id, KeyInput::Batch(&r.batch)).unwrap();
    }
    let workload = rec.finish().unwrap();
    let by_id: BTreeMap<&str, &Run> = runs.iter().map(|r| (r.id, r)).collect();
    let mut checked = 0;
    let mut failures: Vec<String> = Vec::new();
    for wr in workload.runs() {
        let run = by_id[wr.run_id.as_str()];
        let rk = workload.run_keys(&keys, wr).unwrap();
        let oracle = typed::oracle(&corpus, &run.columns, &run.keys);
        for (specs, want) in configs(run, &corpus) {
            let names: Vec<String> = specs.iter().map(KeySpec::name).collect();
            let plan = replayer.plan_of_keys(&specs, &rk).unwrap();
            let got = cells(&plan);
            let cost = replayer.cost_of_keys(&specs, run.id, &rk).unwrap();
            let (f, g, b) = cost_of_cells(&got, &footer);
            if !cost.exact || (cost.files, cost.row_groups, cost.bytes) != (f, g, b) {
                failures.push(format!(
                    "{} {names:?}: cost {cost:?} but footers give files {f} rgs {g} bytes {b}",
                    run.id
                ));
            }
            let missing: Vec<&Cell> = oracle.difference(&got).collect();
            if !missing.is_empty() {
                failures.push(format!(
                    "{} {names:?}: FALSE NEGATIVE, misses {missing:?}",
                    run.id
                ));
            }
            match &want {
                Want::Equal if got != oracle => failures.push(format!(
                    "{} {names:?}: exact path selects {} cells, oracle {} (extra {:?})",
                    run.id,
                    got.len(),
                    oracle.len(),
                    got.difference(&oracle).collect::<Vec<_>>()
                )),
                Want::Exactly(set) if &got != set => failures.push(format!(
                    "{} {names:?}: per-key intersection selects {} cells, want {}",
                    run.id,
                    got.len(),
                    set.len()
                )),
                _ => {}
            }
            checked += 1;
        }
        // The statistics baseline contains the oracle; bytes are footer sums.
        let stats = BaselinePlanner::parquet_stats(
            replayer.catalog(),
            &wr.columns,
            FingerprintPolicy::Tail64K,
        )
        .unwrap();
        let plan = stats.plan(&rk, None).unwrap();
        let got = cells(&plan);
        let missing: Vec<&Cell> = oracle.difference(&got).collect();
        if !missing.is_empty() {
            failures.push(format!(
                "{} ParquetStats: FALSE NEGATIVE, misses {missing:?}",
                run.id
            ));
        }
        let (_, _, b) = cost_of_cells(&got, &footer);
        if plan.bytes != b {
            failures.push(format!(
                "{} ParquetStats bytes {} != {b}",
                run.id, plan.bytes
            ));
        }
    }
    // Non-vacuity: the +0.0 probe reaches the file that holds only -0.0.
    let rk_pos = workload
        .run_keys(&keys, workload.run("f64-pos0").unwrap())
        .unwrap();
    let pos = cells(
        &replayer
            .plan_of_keys(&[exact(&["k_f64"])], &rk_pos)
            .unwrap(),
    );
    let only_neg_file = corpus.files()[1].to_str().unwrap().to_string();
    assert!(
        pos.iter().any(|(f, _)| f == &only_neg_file),
        "+0.0 probe must reach the file that holds only -0.0"
    );
    assert!(
        checked > 80,
        "checked only {checked} (run, configuration) pairs"
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// The statistics baseline probes the embedded filters of `UInt64` and
/// `UInt32` columns with the value's bit pattern: a key at or above `2^63`
/// (`2^31`) lives in an even file whose row group carries a filter, and
/// probing it by value would prune that row group (a false negative).
#[test]
fn unsigned_keys_above_the_signed_range_pass_their_embedded_filters() {
    let corpus = typed::write_corpus(6, 3, 40, 2026);
    let schema = typed::dataset_schema();
    let source = ParquetSource::new().with_schema(Arc::clone(&schema));
    let replayer = Replayer::unindexed(
        &source,
        &corpus.data,
        &bellwether_source::Partitioning::None,
    )
    .unwrap();
    let keys = MemoryKeySets::new();
    let mut rec = WorkloadRecorder::new(&keys, Arc::clone(&schema));
    let u64s = [(1u64 << 63) + 5, u64::MAX];
    let u32s = [(1u32 << 31) + 3, u32::MAX];
    rec.record(
        "u64-high",
        KeyInput::Batch(
            &RecordBatch::try_from_iter(vec![(
                "k_u64",
                Arc::new(UInt64Array::from(u64s.to_vec())) as ArrayRef,
            )])
            .unwrap(),
        ),
    )
    .unwrap();
    rec.record(
        "u32-high",
        KeyInput::Batch(
            &RecordBatch::try_from_iter(vec![(
                "k_u32",
                Arc::new(UInt32Array::from(u32s.to_vec())) as ArrayRef,
            )])
            .unwrap(),
        ),
    )
    .unwrap();
    let workload = rec.finish().unwrap();
    let total = replayer.catalog().len();
    for wr in workload.runs() {
        let rk = workload.run_keys(&keys, wr).unwrap();
        let col = wr.columns[0].as_str();
        let want: Vec<Vec<Option<V>>> = if col == "k_u64" {
            u64s.iter().map(|v| vec![Some(V::U(*v))]).collect()
        } else {
            u32s.iter()
                .map(|v| vec![Some(V::U(u64::from(*v)))])
                .collect()
        };
        let oracle = typed::oracle(&corpus, &[col], &want);
        assert!(!oracle.is_empty(), "{col}: the planted keys are present");
        // The planted row groups sit in even files, which carry filters.
        for (path, rg) in &oracle {
            let meta = replayer
                .catalog()
                .row_groups()
                .iter()
                .find(|r| &r.meta.file.path == path && r.meta.index_in_file == *rg)
                .unwrap();
            assert!(
                meta.meta
                    .stats
                    .iter()
                    .any(|s| s.column == col && s.has_bloom),
                "{col}: row group {path}:{rg} must carry an embedded filter"
            );
        }
        let stats = BaselinePlanner::parquet_stats(
            replayer.catalog(),
            &wr.columns,
            FingerprintPolicy::Tail64K,
        )
        .unwrap();
        let got = cells(&stats.plan(&rk, None).unwrap());
        let missing: Vec<&Cell> = oracle.difference(&got).collect();
        assert!(missing.is_empty(), "{col}: ParquetStats misses {missing:?}");
        assert!(
            got.len() < total,
            "{col}: the baseline must prune something"
        );
    }
}
