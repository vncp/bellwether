//! Review probe for PR #21: an independent oracle.
//!
//! Random two-sided data (composite Int64 + Utf8 key with nulls and an
//! escaped NUL byte, nullable timestamps with ties, a Decimal128 total
//! written at two file scales, a Decimal256 total, groups, an ignorable
//! column), random edits, and every coverage kind. The expected result is
//! computed from typed rows only: no `bellwether_verify` item and no
//! `bellwether_core` encoder is used to compute an expectation. The key
//! bytes the sample hashes and the result names are built here by hand from
//! `docs/format.md`. The same inputs are run at one pass and at several, and
//! the whole result (minus the memory report) must be identical.

#![allow(
    clippy::unwrap_used,
    clippy::pedantic,
    clippy::cloned_ref_to_slice_refs,
    missing_docs,
    dead_code
)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    ArrayRef, Decimal128Array, Decimal256Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{i256, DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_source::ParquetSource;
use bellwether_types::{Coverage, Hash};
use bellwether_verify::{
    CoverageRequest, MemoryReport, Mismatch, Options, ReconcileSpec, Reconciler, Reconciliation,
    Side, VerifyError, WorkloadKeys,
};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use proptest::prelude::*;
use xxhash_rust::xxh3::xxh3_64_with_seed;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Row {
    k1: Option<i64>,
    k2: Option<String>,
    ts: Option<i64>,
    amt: Option<i128>, // cents, dataset scale 2
    big: Option<i128>, // tenths, Decimal256 scale 1
    grp: String,
    note: Option<String>,
}

type Key = (Option<i64>, Option<String>);

fn key_of(r: &Row) -> Key {
    (r.k1, r.k2.clone())
}

// ----- hand-built canonical key bytes (docs/format.md) -----

fn enc_key(k: &Key) -> Vec<u8> {
    let mut b = Vec::new();
    match k.0 {
        None => b.push(0x00),
        Some(v) => {
            b.push(0x02);
            #[allow(clippy::cast_sign_loss)]
            let u = (v as u64) ^ (1u64 << 63);
            b.extend_from_slice(&u.to_be_bytes());
        }
    }
    match &k.1 {
        None => b.push(0x00),
        Some(s) => {
            b.push(0x06);
            for &c in s.as_bytes() {
                b.push(c);
                if c == 0 {
                    b.push(0xFF);
                }
            }
            b.extend_from_slice(&[0x00, 0x00]);
        }
    }
    b
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

// ----- writing -----

fn schema(amt_scale: i8) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k1", DataType::Int64, true),
        Field::new("k2", DataType::Utf8, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("amt", DataType::Decimal128(18, amt_scale), true),
        Field::new("big", DataType::Decimal256(40, 1), true),
        Field::new("grp", DataType::Utf8, false),
        Field::new("note", DataType::Utf8, true),
    ]))
}

fn write(dir: &Path, name: &str, rows: &[Row], amt_scale: i8, rg: usize) {
    let factor = if amt_scale == 3 { 10 } else { 1 };
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.k1).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.k2.as_deref()).collect::<Vec<_>>(),
        )),
        Arc::new(
            TimestampMicrosecondArray::from(rows.iter().map(|r| r.ts).collect::<Vec<_>>())
                .with_timezone("UTC"),
        ),
        Arc::new(
            Decimal128Array::from(
                rows.iter()
                    .map(|r| r.amt.map(|a| a * factor))
                    .collect::<Vec<_>>(),
            )
            .with_precision_and_scale(18, amt_scale)
            .unwrap(),
        ),
        Arc::new(
            Decimal256Array::from(
                rows.iter()
                    .map(|r| r.big.map(i256::from_i128))
                    .collect::<Vec<_>>(),
            )
            .with_precision_and_scale(40, 1)
            .unwrap(),
        ),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.grp.as_str()).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.note.as_deref()).collect::<Vec<_>>(),
        )),
    ];
    let batch = RecordBatch::try_new(schema(amt_scale), cols).unwrap();
    let file = std::fs::File::create(dir.join(name)).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(rg.max(1)))
        .build();
    let mut w = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// a.parquet at the dataset scale 2, b.parquet at scale 2 or 3.
fn write_side(dir: &Path, rows: &[Row], cut: usize, b_scale3: bool, rg: usize) {
    let cut = cut.min(rows.len());
    write(dir, "a.parquet", &rows[..cut], 2, rg);
    write(
        dir,
        "b.parquet",
        &rows[cut..],
        if b_scale3 { 3 } else { 2 },
        rg + 1,
    );
}

// ----- the oracle -----

#[derive(Clone, Debug)]
enum Scope {
    Full,
    Workload(BTreeSet<Key>),
    Sampled { fraction: f64, seed: u64 },
}

impl Scope {
    fn includes(&self, k: &Key) -> bool {
        match self {
            Self::Full => true,
            Self::Workload(keys) => keys.contains(k),
            Self::Sampled { fraction, seed } => {
                if *fraction >= 1.0 {
                    return true;
                }
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let t = (*fraction * 18_446_744_073_709_551_616.0) as u64;
                xxh3_64_with_seed(&enc_key(k), *seed) < t
            }
        }
    }
}

type View = (
    Option<i64>,
    Option<String>,
    Option<i64>,
    Option<i128>,
    Option<i128>,
    String,
    Option<String>,
);

fn view(r: &Row, ignore_note: bool) -> View {
    (
        r.k1,
        r.k2.clone(),
        r.ts,
        r.amt,
        r.big,
        r.grp.clone(),
        if ignore_note { None } else { r.note.clone() },
    )
}

/// The compared rows of one key on one side, or `None` when the side has
/// rows but no comparable version.
fn compared<'a>(rows: &[&'a Row], latest: bool) -> Option<Vec<&'a Row>> {
    if !latest {
        return Some(rows.to_vec());
    }
    let newest = rows.iter().filter_map(|r| r.ts).max()?;
    Some(
        rows.iter()
            .copied()
            .filter(|r| r.ts == Some(newest))
            .collect(),
    )
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Expected {
    missing_left: BTreeSet<String>,
    missing_right: BTreeSet<String>,
    differs: BTreeSet<String>,
    no_version: BTreeSet<String>,
    /// (group, column) -> (left, right, left_rows, right_rows)
    totals: BTreeMap<(String, String), (i128, i128, u64, u64)>,
    keys_compared: u64,
    absent_both: u64,
    sampled_total: u64,
}

fn predict(
    left: &[Row],
    right: &[Row],
    latest: bool,
    ignore_note: bool,
    scope: &Scope,
) -> Expected {
    let mut by_key: BTreeMap<Key, [Vec<&Row>; 2]> = BTreeMap::new();
    for (s, rows) in [left, right].into_iter().enumerate() {
        for r in rows {
            by_key.entry(key_of(r)).or_default()[s].push(r);
        }
    }
    let mut e = Expected::default();
    let mut groups: BTreeMap<String, [(i128, i128, u64); 2]> = BTreeMap::new();
    let mut present_in_scope = 0u64;
    let mut all_keys = 0u64;
    for (k, [l, r]) in &by_key {
        all_keys += 1;
        if !scope.includes(k) {
            continue;
        }
        present_in_scope += 1;
        let name = hex(&enc_key(k));
        let (cl, cr) = (compared(l, latest), compared(r, latest));
        let (pl, pr) = (!l.is_empty(), !r.is_empty());
        if (pl && cl.is_none()) || (pr && cr.is_none()) {
            e.no_version.insert(name);
        } else if !pl {
            e.missing_left.insert(name);
        } else if !pr {
            e.missing_right.insert(name);
        } else {
            let mut vl: Vec<View> = cl
                .clone()
                .unwrap()
                .iter()
                .map(|x| view(x, ignore_note))
                .collect();
            let mut vr: Vec<View> = cr
                .clone()
                .unwrap()
                .iter()
                .map(|x| view(x, ignore_note))
                .collect();
            vl.sort();
            vr.sort();
            if vl != vr {
                e.differs.insert(name);
            }
        }
        for (s, c) in [cl, cr].into_iter().enumerate() {
            for row in c.unwrap_or_default() {
                let g = &mut groups.entry(row.grp.clone()).or_default()[s];
                g.0 += row.amt.unwrap_or(0);
                g.1 += row.big.unwrap_or(0);
                g.2 += 1;
            }
        }
    }
    for (g, [l, r]) in groups {
        e.totals
            .insert((g.clone(), "amt".into()), (l.0, r.0, l.2, r.2));
        e.totals.insert((g, "big".into()), (l.1, r.1, l.2, r.2));
    }
    match scope {
        Scope::Full => e.keys_compared = present_in_scope,
        Scope::Workload(keys) => {
            e.keys_compared = keys.len() as u64;
            e.absent_both = keys.len() as u64 - present_in_scope;
        }
        Scope::Sampled { .. } => {
            e.keys_compared = present_in_scope;
            e.sampled_total = all_keys;
        }
    }
    e
}

fn observed(out: &Reconciliation) -> Expected {
    let mut e = Expected::default();
    for m in &out.mismatches {
        match m {
            Mismatch::MissingOnLeft { key, .. } => {
                e.missing_left.insert(key.canonical.clone());
            }
            Mismatch::MissingOnRight { key, .. } => {
                e.missing_right.insert(key.canonical.clone());
            }
            Mismatch::RowHashDiffers { key, .. } => {
                e.differs.insert(key.canonical.clone());
            }
            Mismatch::NoComparableVersion { key, .. } => {
                e.no_version.insert(key.canonical.clone());
            }
            Mismatch::TotalDiffers { .. } => {}
        }
    }
    for t in &out.totals {
        let g = t.group.values[0].1.clone().unwrap();
        let v = |x: &bellwether_verify::ExactTotal| x.unscaled.to_string().parse::<i128>().unwrap();
        e.totals.insert(
            (g, t.column.clone()),
            (v(&t.left), v(&t.right), t.left_rows, t.right_rows),
        );
    }
    e.keys_compared = out.counts.keys_compared;
    e.absent_both = out.counts.absent_both;
    e
}

// ----- strategies -----

fn row() -> impl Strategy<Value = Row> {
    (
        prop_oneof![9 => (0i64..6).prop_map(|i| Some(i * 1_000_003 - 2_000_000)), 1 => Just(None)],
        prop_oneof![4 => Just(Some("a".to_string())), 3 => Just(Some("a\0z".to_string())), 2 => Just(Some("b".to_string())), 1 => Just(None)],
        prop_oneof![8 => (0i64..4).prop_map(Some), 1 => Just(None)],
        prop_oneof![8 => (-50i128..50).prop_map(Some), 1 => Just(None)],
        prop_oneof![8 => (-20i128..20).prop_map(Some), 1 => Just(None)],
        prop::sample::select(vec!["g1", "g2", "g3"]),
        prop::option::of(prop::sample::select(vec!["x", "y"])),
    )
        .prop_map(|(k1, k2, ts, amt, big, grp, note)| Row {
            k1,
            k2,
            ts,
            amt,
            big,
            grp: grp.into(),
            note: note.map(Into::into),
        })
}

#[derive(Clone, Debug)]
enum Edit {
    Note(usize),
    Amt(usize, i128),
    NullAmt(usize),
    Big(usize, i128),
    Delete(usize),
    Duplicate(usize),
    Add(Row),
    Ts(usize, Option<i64>),
    Grp(usize),
    K2(usize),
}

fn edit() -> impl Strategy<Value = Edit> {
    prop_oneof![
        any::<usize>().prop_map(Edit::Note),
        (any::<usize>(), 1i128..4).prop_map(|(i, d)| Edit::Amt(i, d)),
        any::<usize>().prop_map(Edit::NullAmt),
        (any::<usize>(), 1i128..4).prop_map(|(i, d)| Edit::Big(i, d)),
        any::<usize>().prop_map(Edit::Delete),
        any::<usize>().prop_map(Edit::Duplicate),
        row().prop_map(Edit::Add),
        (any::<usize>(), prop::option::of(0i64..5)).prop_map(|(i, t)| Edit::Ts(i, t)),
        any::<usize>().prop_map(Edit::Grp),
        any::<usize>().prop_map(Edit::K2),
    ]
}

fn apply(rows: &mut Vec<Row>, e: &Edit) {
    let len = rows.len();
    let at = |i: &usize| i % len.max(1);
    match e {
        Edit::Add(r) => rows.push(r.clone()),
        _ if len == 0 => {}
        Edit::Note(i) => {
            let t = &mut rows[at(i)];
            t.note = Some(format!("{}!", t.note.clone().unwrap_or_default()));
        }
        Edit::Amt(i, d) => {
            let t = &mut rows[at(i)];
            t.amt = Some(t.amt.unwrap_or(0) + d);
        }
        Edit::NullAmt(i) => rows[at(i)].amt = None,
        Edit::Big(i, d) => {
            let t = &mut rows[at(i)];
            t.big = Some(t.big.unwrap_or(0) + d);
        }
        Edit::Delete(i) => {
            rows.remove(at(i));
        }
        Edit::Duplicate(i) => {
            let c = rows[at(i)].clone();
            rows.push(c);
        }
        Edit::Ts(i, ts) => rows[at(i)].ts = *ts,
        Edit::Grp(i) => {
            let t = &mut rows[at(i)];
            t.grp = if t.grp == "g1" {
                "g2".into()
            } else {
                "g1".into()
            };
        }
        Edit::K2(i) => {
            let t = &mut rows[at(i)];
            t.k2 = match t.k2.as_deref() {
                Some("a") => Some("b".into()),
                _ => Some("a".into()),
            };
        }
    }
}

fn strip(mut o: Reconciliation) -> Reconciliation {
    o.memory = MemoryReport::default();
    o
}

fn run(
    l: &Path,
    r: &Path,
    spec: &ReconcileSpec,
    budget: u64,
) -> Result<Reconciliation, VerifyError> {
    // The dataset scale is 2 (a.parquet's), declared: inferred, a side
    // whose b.parquet is at scale 3 would take scale 3 (the widest, M5), and
    // two sides at different scales are refused as a schema error.
    let (ls, rs) = (
        ParquetSource::new().with_schema(schema(2)),
        ParquetSource::new().with_schema(schema(2)),
    );
    let (lr, rr) = (
        l.to_str().unwrap().to_string(),
        r.to_str().unwrap().to_string(),
    );
    Reconciler::new(
        spec.clone(),
        Options {
            memory_budget: budget,
            max_examples: 100_000,
        },
    )?
    .run(&Side::new(&ls, &lr), &Side::new(&rs, &rr))
}

fn cases() -> u32 {
    std::env::var("R21_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(48)
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(), ..ProptestConfig::default() })]

    #[test]
    fn oracle_r2_agrees_at_every_pass_count_and_coverage(
        base in prop::collection::vec(row(), 0..44),
        edits in prop::collection::vec(edit(), 0..7),
        latest in any::<bool>(),
        ignore_note in any::<bool>(),
        cover in 0u8..3,
        frac in prop_oneof![Just(1.0f64), 0.2f64..0.9],
        seed in any::<u64>(),
        picks in prop::collection::vec(any::<usize>(), 1..6),
        cuts in (0usize..44, 0usize..44),
        scale3 in (any::<bool>(), any::<bool>()),
        rg in 1usize..7,
    ) {
        let mut right = base.clone();
        for e in &edits {
            apply(&mut right, e);
        }
        // Amended semantics: a null key element under latest_by is refused,
        // so latest_by cases use null-free keys (nulls stay for the rest).
        let mut base = base;
        if latest {
            for x in base.iter_mut().chain(right.iter_mut()) {
                x.k1 = Some(x.k1.unwrap_or(-7));
                x.k2 = Some(x.k2.clone().unwrap_or_else(|| "n".into()));
            }
        }
        let (l, r) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        write_side(l.path(), &base, cuts.0, scale3.0, rg);
        write_side(r.path(), &right, cuts.1, scale3.1, rg + 2);

        // Coverage. Workload keys: some present non-null keys, plus one absent.
        let present: Vec<Key> = base
            .iter()
            .chain(&right)
            .map(key_of)
            .filter(|k| k.0.is_some() && k.1.is_some())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let (scope, request) = match cover {
            0 => (Scope::Full, CoverageRequest::Full),
            1 => {
                let mut keys: BTreeSet<Key> = BTreeSet::new();
                for p in &picks {
                    if !present.is_empty() {
                        keys.insert(present[p % present.len()].clone());
                    }
                }
                keys.insert((Some(77), Some("absent".into())));
                let req = CoverageRequest::Workload(WorkloadKeys {
                    workload: Hash::of(b"oracle"),
                    age: Duration::ZERO,
                    keys: keys.iter().map(enc_key).collect(),
                });
                (Scope::Workload(keys), req)
            }
            _ => (
                Scope::Sampled { fraction: frac, seed },
                CoverageRequest::Sampled { fraction: frac, seed },
            ),
        };
        let mut spec = ReconcileSpec::new(["k1", "k2"])
            .totals(["amt", "big"])
            .group_by(["grp"])
            .coverage(request);
        if latest {
            spec = spec.latest_by("ts");
        }
        if ignore_note {
            spec = spec.ignore(["note"]);
        }
        let expected = predict(&base, &right, latest, ignore_note, &scope);

        // Amended semantics: a scoped coverage that compares nothing while
        // data exists is refused.
        let vacuous = match &scope {
            Scope::Full => false,
            Scope::Workload(keys) => expected.absent_both == keys.len() as u64,
            Scope::Sampled { .. } => expected.keys_compared == 0 && expected.sampled_total > 0,
        };
        if vacuous {
            let res = run(l.path(), r.path(), &spec, 1 << 30);
            prop_assert!(matches!(res, Err(VerifyError::NothingCompared(_))), "{:?}", res.map(|o| o.counts));
            return Ok(());
        }
        let one = run(l.path(), r.path(), &spec, 1 << 30).unwrap();
        prop_assert_eq!(one.memory.passes, 1);
        let got = observed(&one);
        prop_assert_eq!(&got.missing_left, &expected.missing_left);
        prop_assert_eq!(&got.missing_right, &expected.missing_right);
        prop_assert_eq!(&got.differs, &expected.differs);
        prop_assert_eq!(&got.no_version, &expected.no_version);
        prop_assert_eq!(&got.totals, &expected.totals);
        prop_assert_eq!(got.keys_compared, expected.keys_compared);
        prop_assert_eq!(got.absent_both, expected.absent_both);
        prop_assert_eq!(one.counts.missing_left, expected.missing_left.len() as u64);
        prop_assert_eq!(one.counts.missing_right, expected.missing_right.len() as u64);
        prop_assert_eq!(one.counts.mismatched, (expected.differs.len() + expected.no_version.len()) as u64);
        prop_assert_eq!(one.counts.no_comparable_version, expected.no_version.len() as u64);
        let totals_differ = expected
            .totals
            .values()
            .filter(|(a, b, _, _)| a != b)
            .count() as u64;
        prop_assert_eq!(one.counts.totals_differ, totals_differ);
        let clean = expected.missing_left.is_empty()
            && expected.missing_right.is_empty()
            && expected.differs.is_empty()
            && expected.no_version.is_empty()
            && totals_differ == 0;
        prop_assert_eq!(one.pass, clean);
        match (&scope, &one.coverage) {
            (Scope::Full, Coverage::Full) => {}
            (Scope::Workload(keys), Coverage::Workload { keys: n, workload }) => {
                prop_assert_eq!(*n, keys.len() as u64);
                prop_assert_eq!(*workload, Hash::of(b"oracle"));
            }
            (Scope::Sampled { fraction, .. }, Coverage::Sampled { keys, fraction: f }) => {
                prop_assert_eq!(*keys, expected.keys_compared);
                #[allow(clippy::cast_precision_loss)]
                let want = if expected.sampled_total == 0 {
                    *fraction
                } else {
                    expected.keys_compared as f64 / expected.sampled_total as f64
                };
                prop_assert!((f - want).abs() < 1e-12, "fraction {} want {}", f, want);
            }
            (s, c) => prop_assert!(false, "scope {:?} reported coverage {:?}", s, c),
        }
        // Every pass count gives the same whole result.
        for budget in [48 * 1024, 20 * 1024, 10 * 1024] {
            match run(l.path(), r.path(), &spec, budget) {
                Ok(many) => {
                    prop_assert!(many.memory.peak_state_bytes <= budget);
                    eprintln!(
                        "R21PASSES budget={budget} passes={} restarts={}",
                        many.memory.passes, many.memory.restarts
                    );
                    prop_assert_eq!(strip(many), strip(one.clone()));
                }
                Err(VerifyError::Budget(m)) => {
                    eprintln!("budget {budget} too small for this case: {m}");
                }
                Err(e) => prop_assert!(false, "budget {}: {}", budget, e),
            }
        }
    }
}
