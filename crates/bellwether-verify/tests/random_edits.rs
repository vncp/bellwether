//! Random edits to a copy produce exactly the mismatches an independent
//! model predicts. The model compares typed rows, never hashes, and knows
//! nothing about passes, so it also checks the result is the same at any
//! pass count.

#![allow(clippy::unwrap_used, missing_docs)]

mod common;

use std::collections::{BTreeMap, BTreeSet};

use bellwether_verify::{Mismatch, Options, ReconcileSpec, VerifyError};
use common::{dirs, reconcile_with, write, R};
use proptest::prelude::*;

#[derive(Clone, Debug)]
enum Edit {
    Note(usize),
    Amount(usize, i128),
    NullAmount(usize),
    Delete(usize),
    Duplicate(usize),
    Add(R),
    Ts(usize, Option<i64>),
    Status(usize),
}

fn row() -> impl Strategy<Value = R> {
    (
        // Few distinct ids, spread wide so the key-range bound from
        // statistics cannot shrink the pass estimate.
        prop_oneof![9 => (0i64..12).prop_map(|i| Some(i * 1_000_003)), 1 => Just(None)],
        prop_oneof![8 => (0i64..4).prop_map(Some), 1 => Just(None)],
        prop_oneof![8 => (-50i128..50).prop_map(Some), 1 => Just(None)],
        prop::sample::select(vec!["a", "b"]),
        prop::option::of(prop::sample::select(vec!["x", "y"])),
    )
        .prop_map(|(id, ts, amount, status, note)| R {
            id,
            ts,
            amount,
            status: status.into(),
            note: note.map(Into::into),
        })
}

fn edit() -> impl Strategy<Value = Edit> {
    prop_oneof![
        any::<usize>().prop_map(Edit::Note),
        (any::<usize>(), 1i128..5).prop_map(|(i, d)| Edit::Amount(i, d)),
        any::<usize>().prop_map(Edit::NullAmount),
        any::<usize>().prop_map(Edit::Delete),
        any::<usize>().prop_map(Edit::Duplicate),
        row().prop_map(Edit::Add),
        (any::<usize>(), prop::option::of(0i64..6)).prop_map(|(i, t)| Edit::Ts(i, t)),
        any::<usize>().prop_map(Edit::Status),
    ]
}

fn apply(rows: &mut Vec<R>, edit: &Edit) {
    let len = rows.len();
    let at = |index: &usize| index % len.max(1);
    match edit {
        Edit::Add(row) => rows.push(row.clone()),
        _ if len == 0 => {}
        Edit::Note(index) => {
            let target = &mut rows[at(index)];
            target.note = Some(format!("{}!", target.note.clone().unwrap_or_default()));
        }
        Edit::Amount(index, delta) => {
            let target = &mut rows[at(index)];
            target.amount = Some(target.amount.unwrap_or(0) + delta);
        }
        Edit::NullAmount(index) => rows[at(index)].amount = None,
        Edit::Delete(index) => {
            rows.remove(at(index));
        }
        Edit::Duplicate(index) => {
            let copy = rows[at(index)].clone();
            rows.push(copy);
        }
        Edit::Ts(index, ts) => rows[at(index)].ts = *ts,
        Edit::Status(index) => {
            let target = &mut rows[at(index)];
            target.status = if target.status == "a" {
                "b".into()
            } else {
                "a".into()
            };
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Predicted {
    missing_left: BTreeSet<Option<i64>>,
    missing_right: BTreeSet<Option<i64>>,
    differs: BTreeSet<Option<i64>>,
    no_version: BTreeSet<Option<i64>>,
    totals: BTreeSet<String>,
}

/// The compared rows of one key on one side: `None` when the key has rows
/// but no comparable version, else the sorted multiset.
fn compared(rows: &[R], latest: bool) -> Option<Vec<R>> {
    if !latest {
        let mut v = rows.to_vec();
        v.sort();
        return Some(v);
    }
    let newest = rows.iter().filter_map(|r| r.ts).max()?;
    let mut v: Vec<R> = rows
        .iter()
        .filter(|r| r.ts == Some(newest))
        .cloned()
        .collect();
    v.sort();
    Some(v)
}

fn predict(left: &[R], right: &[R], latest: bool) -> Predicted {
    let mut by_key: BTreeMap<Option<i64>, [Vec<R>; 2]> = BTreeMap::new();
    for (s, rows) in [left, right].into_iter().enumerate() {
        for r in rows {
            by_key.entry(r.id).or_default()[s].push(r.clone());
        }
    }
    let mut p = Predicted::default();
    let mut sums: BTreeMap<String, [i128; 2]> = BTreeMap::new();
    for (k, [l, r]) in &by_key {
        let (cl, cr) = (compared(l, latest), compared(r, latest));
        let (pl, pr) = (!l.is_empty(), !r.is_empty());
        if (pl && cl.is_none()) || (pr && cr.is_none()) {
            p.no_version.insert(*k);
        } else if !pl {
            p.missing_left.insert(*k);
        } else if !pr {
            p.missing_right.insert(*k);
        } else if cl != cr {
            p.differs.insert(*k);
        }
        for (s, c) in [cl, cr].into_iter().enumerate() {
            for row in c.unwrap_or_default() {
                sums.entry(row.status.clone()).or_default()[s] += row.amount.unwrap_or(0);
            }
        }
    }
    p.totals = sums
        .into_iter()
        .filter(|(_, [a, b])| a != b)
        .map(|(g, _)| g)
        .collect();
    p
}

fn observed(ms: &[Mismatch]) -> Predicted {
    let id = |values: &[Option<String>]| values[0].as_ref().map(|v| v.parse::<i64>().unwrap());
    let mut p = Predicted::default();
    for m in ms {
        match m {
            Mismatch::MissingOnLeft { key, .. } => {
                p.missing_left.insert(id(&key.values));
            }
            Mismatch::MissingOnRight { key, .. } => {
                p.missing_right.insert(id(&key.values));
            }
            Mismatch::RowHashDiffers { key, .. } => {
                p.differs.insert(id(&key.values));
            }
            Mismatch::NoComparableVersion { key, .. } => {
                p.no_version.insert(id(&key.values));
            }
            Mismatch::TotalDiffers { group, .. } => {
                p.totals.insert(group.values[0].1.clone().unwrap());
            }
        }
    }
    p
}

fn split_write(dir: &std::path::Path, rows: &[R], cut: usize, rg: usize) {
    let cut = cut.min(rows.len());
    write(dir, "a.parquet", &rows[..cut], rg);
    write(dir, "b.parquet", &rows[cut..], rg);
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]

    #[test]
    fn the_mismatch_set_equals_the_predicted_set(
        base in prop::collection::vec(row(), 0..48),
        edits in prop::collection::vec(edit(), 0..8),
        latest in any::<bool>(),
        cuts in (0usize..48, 0usize..48),
        rg in 1usize..9,
    ) {
        let mut right = base.clone();
        for e in &edits {
            apply(&mut right, e);
        }
        let (l, r) = dirs();
        split_write(l.path(), &base, cuts.0, rg);
        split_write(r.path(), &right, cuts.1, rg + 2);
        let mut spec = ReconcileSpec::new(["id"]).totals(["amount"]).group_by(["status"]);
        if latest {
            spec = spec.latest_by("ts");
        }
        let expected = predict(&base, &right, latest);
        // Under latest_by a null key element is refused, at any pass count.
        let null_key = latest && base.iter().chain(&right).any(|x| x.id.is_none());
        for budget in [1u64 << 30, 24 * 1024] {
            let options = Options { memory_budget: budget, max_examples: 10_000 };
            let result = reconcile_with(l.path(), r.path(), spec.clone(), options);
            if null_key {
                prop_assert!(matches!(result, Err(VerifyError::NullKey { .. })), "{:?}", result.map(|o| o.counts));
                continue;
            }
            let out = result.unwrap();
            prop_assert_eq!(observed(&out.mismatches), expected.clone());
            prop_assert_eq!(out.counts.missing_left, expected.missing_left.len() as u64);
            prop_assert_eq!(out.counts.missing_right, expected.missing_right.len() as u64);
            prop_assert_eq!(
                out.counts.mismatched,
                (expected.differs.len() + expected.no_version.len()) as u64
            );
            prop_assert_eq!(out.counts.no_comparable_version, expected.no_version.len() as u64);
            let clean = expected == Predicted::default();
            prop_assert_eq!(out.pass, clean);
            if budget < 1 << 20 && base.len().max(right.len()) >= 40 {
                // The small budget forces several passes on larger cases.
                prop_assert!(out.memory.passes > 1, "{:?}", out.memory);
                prop_assert!(out.memory.peak_state_bytes <= budget);
            }
        }
    }
}
