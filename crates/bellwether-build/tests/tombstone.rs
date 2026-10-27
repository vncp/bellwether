//! Tombstones (B8): `latest`/`asof` with `live` through a temporal or
//! bitemporal key with a tombstone, against rows decoded with the `parquet`
//! crate.
//!
//! The rule written out here, independently: per key, the rows at the key's
//! newest time at or before the instant (over every live row group,
//! invariant 5), then those whose tombstone column is not null and is not
//! the tombstone value. That is `asof` followed by the filter, row for row
//! (condition (d) of the B8 brief): a key whose newest row is a tombstone
//! has no row, so has a key whose only rows are tombstones, and a key
//! retracted and then written again has the later live row.
//!
//! - invariant 1: every row group holding an answer row is planned, and the
//!   residual over the planned rows is exactly the answer;
//! - the tombstones only ever remove row groups from the plan without them,
//!   and for `latest` the plan is exactly the row groups holding an answer;
//! - an incomplete tombstone sidecar (the state a binary without tombstones
//!   leaves after appending row groups) plans as without tombstones, with
//!   the residual still exact, until a build completes it;
//! - removing the spec with its tombstone removes only the sidecar.

#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::too_many_lines
)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bellwether_build::{completeness, Lookup, Planner};
use bellwether_core::encoding::Value;
use bellwether_store::{commit, StagedSegments};
use bellwether_types::{KeySpec, Tombstone};
use common::*;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("ts", DataType::Int64, true),
        Field::new("void", DataType::Boolean, true),
        Field::new("status", DataType::Utf8, true),
        Field::new("valid", DataType::Int64, true),
        Field::new("known", DataType::Int64, true),
        Field::new("v", DataType::Int64, false),
    ]))
}

/// `(id, ts, void, valid, known)`; `status` is `VOID` where `void` is true,
/// `OK` where false, null where null.
type R = (
    Option<i64>,
    Option<i64>,
    Option<bool>,
    Option<i64>,
    Option<i64>,
);

fn write(s: &Scratch, name: &str, rows: &[R], per_rg: usize) {
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.2.map(|v| if v { "VOID" } else { "OK" }))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.3).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.4).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from_iter_values(0..rows.len() as i64)),
        ],
    )
    .unwrap();
    write_parquet(&s.path(name), &batch, per_rg);
}

/// A small linear congruential generator: the corpus is the same on every
/// run.
struct Lcg(u64);

impl Lcg {
    fn below(&mut self, n: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % n
    }
}

const NEWEST_TOMBSTONE: i64 = 100;
const ONLY_TOMBSTONES: i64 = 101;
const RESURRECTED: i64 = 102;
const NULL_AT_NEWEST: i64 = 103;
const TIE: i64 = 104;

/// The keys every test names, and the rows that make them what they are.
fn special(file: usize) -> Vec<R> {
    let r = |id, ts, void: Option<bool>| (Some(id), Some(ts), void, Some(ts), Some(ts + 1));
    match file {
        0 => vec![
            r(NEWEST_TOMBSTONE, 10, Some(false)),
            r(ONLY_TOMBSTONES, 4, Some(true)),
            r(RESURRECTED, 10, Some(false)),
            r(NULL_AT_NEWEST, 10, Some(false)),
            r(TIE, 30, Some(true)),
        ],
        1 => vec![
            r(NEWEST_TOMBSTONE, 30, Some(true)),
            r(ONLY_TOMBSTONES, 25, Some(true)),
            r(RESURRECTED, 20, Some(true)),
            r(NULL_AT_NEWEST, 30, None),
            r(TIE, 30, Some(false)),
        ],
        _ => vec![
            r(RESURRECTED, 35, Some(false)),
            r(TIE, 20, Some(false)),
            // a null key and a null time are never versions
            (None, Some(99), Some(false), Some(99), Some(99)),
            (Some(RESURRECTED), None, Some(false), None, None),
        ],
    }
}

/// Three files of random rows over ids 0..12, five rows per row group,
/// each with the special keys' rows spread into it.
fn corpus(s: &Scratch, files: &[usize]) {
    for &f in files {
        let mut g = Lcg(f as u64 + 7);
        let mut rows: Vec<R> = Vec::new();
        for i in 0..40 {
            let id = (g.below(13) != 0).then(|| g.below(12) as i64);
            let ts = (g.below(9) != 0).then(|| g.below(40) as i64);
            let void = match g.below(7) {
                0 => None,
                1 | 2 => Some(true),
                _ => Some(false),
            };
            let known = ts.map(|t| t + g.below(5) as i64);
            rows.push((id, ts, void, ts, known));
            if i % 8 == 0 {
                rows.extend(special(f).into_iter().skip(i / 8).take(1));
            }
        }
        rows.extend(special(f).into_iter().skip(5));
        write(s, &format!("f{f}.parquet"), &rows, 5);
    }
}

fn tomb(value: Option<&str>) -> Tombstone {
    Tombstone {
        column: if value.is_some() { "status" } else { "void" }.into(),
        value: value.map(str::to_string),
    }
}

fn temporal_spec(value: Option<&str>) -> KeySpec {
    KeySpec::Temporal {
        columns: cols(&["id"]),
        ts_column: "ts".into(),
        counts: false,
        tombstone: Some(tomb(value)),
    }
}

fn i64_of(r: &Row, c: &str) -> Option<i64> {
    let b = r.get(c)?;
    let tail: [u8; 8] = b[b.len() - 8..].try_into().unwrap();
    Some((u64::from_be_bytes(tail) ^ (1 << 63)) as i64)
}

/// Whether a row is live: its `void` is false (a null is not live).
fn live(r: &Row) -> bool {
    r.get("void") == Some(&enc(&Value::Boolean(false), &DataType::Boolean))
}

/// The answer by the rule: `asof` (or `latest`), then the filter.
fn truth(rows: &[Row], keys: &[Vec<u8>], asof: Option<i64>) -> Vec<Row> {
    let id = cols(&["id"]);
    let asof_bytes = asof.map(enc_i64);
    latest_truth(rows, &id, keys, &id, "ts", asof_bytes.as_deref())
        .into_iter()
        .filter(live)
        .collect()
}

fn all_rows(s: &Scratch, files: &[usize]) -> Vec<Row> {
    files
        .iter()
        .flat_map(|f| parquet_rows(&s.path(&format!("f{f}.parquet")), &schema()))
        .collect()
}

fn ids(v: &[i64]) -> Vec<Vec<u8>> {
    v.iter().map(|i| enc_i64(*i)).collect()
}

fn keysets() -> Vec<Vec<Vec<u8>>> {
    let mut out: Vec<Vec<Vec<u8>>> = (0..12).chain(100..105).map(|i| ids(&[i])).collect();
    out.push(ids(&(0..12).chain(100..105).collect::<Vec<_>>()));
    out.push(ids(&[NEWEST_TOMBSTONE, RESURRECTED, 3, 999]));
    out
}

const INSTANTS: [Option<i64>; 9] = [
    None,
    Some(0),
    Some(4),
    Some(10),
    Some(19),
    Some(20),
    Some(29),
    Some(30),
    Some(39),
];

/// Plans `keys` at `asof` with and without the tombstone and checks both
/// against the rule; returns the live plan's row groups and outcome.
fn check(
    p: &Planner<bellwether_store::ObjectStoreBackend>,
    specs: &[KeySpec],
    rows: &[Row],
    keys: &[Vec<u8>],
    asof: Option<i64>,
    t: &Tombstone,
) -> (BTreeSet<(String, u32)>, bellwether_build::LiveOutcome) {
    let lookup = |live: Option<Tombstone>| match asof {
        None => Lookup::Latest {
            columns: cols(&["id"]),
            keys: keys.to_vec(),
            live,
        },
        Some(ts) => Lookup::AsOf {
            columns: cols(&["id"]),
            keys: keys.to_vec(),
            ts,
            live,
        },
    };
    let (plan, _, outcome) = p
        .plan_ordered_live(specs, &lookup(Some(t.clone())))
        .unwrap();
    let plain = p.plan(specs, &lookup(None)).unwrap();
    let want = sorted(truth(rows, keys, asof));
    let what = format!("{} keys at {asof:?}", keys.len());
    let planned = plan_rgs(&plan);
    let needed: BTreeSet<(String, u32)> = want.iter().map(|r| (r.path.clone(), r.rg)).collect();
    // invariant 1, and the residual over the planned rows is the answer
    assert!(needed.is_subset(&planned), "{what}: missing row groups");
    let got = sorted(apply_residual(
        plan.residual.as_ref().unwrap(),
        planned_rows(&plan, rows),
    ));
    assert_eq!(got, want, "{what}: live residual");
    // asof then the filter, through the plan without tombstones
    let filtered = sorted(
        apply_residual(plain.residual.as_ref().unwrap(), planned_rows(&plain, rows))
            .into_iter()
            .filter(live)
            .collect(),
    );
    assert_eq!(filtered, want, "{what}: asof + filter");
    // tombstones only remove row groups
    assert!(planned.is_subset(&plan_rgs(&plain)), "{what}");
    if asof.is_none() && outcome.pruned_by.is_some() {
        assert_eq!(planned, needed, "{what}: latest is exact");
    }
    (planned, outcome)
}

#[test]
fn live_lookups_are_the_lookup_then_the_filter_on_decoded_files() {
    let s = Scratch::new();
    corpus(&s, &[0, 1, 2]);
    let rows = all_rows(&s, &[0, 1, 2]);
    for value in [None, Some("VOID")] {
        let spec = temporal_spec(value);
        let store = memory_store();
        build_ok(
            &builder(&store),
            &s.root_str(),
            std::slice::from_ref(&spec),
            &parquet_opts(&schema()),
        );
        let p = planner(&store);
        assert!(p.completeness(&spec).is_some());
        let specs = vec![spec.clone()];
        let t = tomb(value);
        let mut pruned_somewhere = false;
        for keys in keysets() {
            for asof in INSTANTS {
                let (_, outcome) = check(&p, &specs, &rows, &keys, asof, &t);
                assert_eq!(
                    outcome.pruned_by.as_deref(),
                    Some("tombstone:id@ts"),
                    "the complete sidecar prunes"
                );
                pruned_somewhere |= outcome.row_groups_pruned > 0;
                if asof.is_none() {
                    // keys retracted = keys with a version and no answer
                    let with_version: BTreeSet<Vec<u8>> =
                        latest_truth(&rows, &cols(&["id"]), &keys, &cols(&["id"]), "ts", None)
                            .iter()
                            .map(|r| r.get("id").unwrap().clone())
                            .collect();
                    let answered: BTreeSet<Vec<u8>> = truth(&rows, &keys, None)
                        .iter()
                        .map(|r| r.get("id").unwrap().clone())
                        .collect();
                    assert_eq!(outcome.keys_with_version, with_version.len() as u64);
                    assert_eq!(
                        outcome.keys_retracted,
                        (with_version.len() - answered.len()) as u64
                    );
                }
            }
        }
        assert!(pruned_somewhere);
        // The named cases, by hand.
        let one = |id: i64, asof: Option<i64>| {
            let (planned, _) = check(&p, &specs, &rows, &ids(&[id]), asof, &t);
            let ts: Vec<Option<i64>> = truth(&rows, &ids(&[id]), asof)
                .iter()
                .map(|r| i64_of(r, "ts"))
                .collect();
            (planned.len(), ts)
        };
        // newest row a tombstone: no row, nothing read; before it, the
        // live row
        assert_eq!(one(NEWEST_TOMBSTONE, None), (0, vec![]));
        assert_eq!(one(NEWEST_TOMBSTONE, Some(29)).1, vec![Some(10)]);
        // only tombstones: no row at any instant
        for asof in INSTANTS {
            assert_eq!(one(ONLY_TOMBSTONES, asof).1, vec![], "{asof:?}");
        }
        assert_eq!(one(ONLY_TOMBSTONES, None).0, 0);
        // resurrected: the later live row; between, no row; before, the
        // first live row
        assert_eq!(one(RESURRECTED, None), (1, vec![Some(35)]));
        assert_eq!(one(RESURRECTED, Some(29)).1, vec![]);
        assert_eq!(one(RESURRECTED, Some(19)).1, vec![Some(10)]);
        // a null tombstone column at the newest time is not live
        assert_eq!(one(NULL_AT_NEWEST, None), (0, vec![]));
        // a tie at the newest time, one live and one tombstone: the live
        // row's row group alone
        assert_eq!(one(TIE, None), (1, vec![Some(30)]));
    }
}

/// `temporal2:id@valid,known,tombstone=void`: the bitemporal winner, then
/// the filter.
#[test]
fn live_bitemporal_lookups_are_the_winner_then_the_filter() {
    let s = Scratch::new();
    corpus(&s, &[0, 1, 2]);
    let rows = all_rows(&s, &[0, 1, 2]);
    let spec = KeySpec::Bitemporal {
        columns: cols(&["id"]),
        valid_column: "valid".into(),
        known_column: "known".into(),
        tombstone: Some(tomb(None)),
    };
    let store = memory_store();
    build_ok(
        &builder(&store),
        &s.root_str(),
        std::slice::from_ref(&spec),
        &parquet_opts(&schema()),
    );
    let p = planner(&store);
    let specs = vec![spec];
    let bitemporal_truth = |keys: &[Vec<u8>], valid: Option<i64>, known: Option<i64>| {
        let keys: BTreeSet<&Vec<u8>> = keys.iter().collect();
        let q = |r: &Row| -> Option<(Vec<u8>, i64, i64)> {
            let k = r.get("id")?.clone();
            let (v, n) = (i64_of(r, "valid")?, i64_of(r, "known")?);
            (keys.contains(&k) && valid.is_none_or(|x| v <= x) && known.is_none_or(|x| n <= x))
                .then_some((k, v, n))
        };
        let mut best: BTreeMap<Vec<u8>, (i64, i64)> = BTreeMap::new();
        for r in &rows {
            if let Some((k, v, n)) = q(r) {
                let e = best.entry(k).or_insert((v, n));
                *e = (*e).max((v, n));
            }
        }
        sorted(
            rows.iter()
                .filter(|r| q(r).is_some_and(|(k, v, n)| best.get(&k) == Some(&(v, n))))
                .filter(|r| live(r))
                .cloned()
                .collect(),
        )
    };
    let mut pruned = 0;
    for keys in keysets() {
        for valid in [None, Some(10), Some(20), Some(30)] {
            for known in [None, Some(11), Some(31), Some(40)] {
                let lookup = |live: Option<Tombstone>| Lookup::Bitemporal {
                    columns: cols(&["id"]),
                    clocks: Some(("valid".into(), "known".into())),
                    keys: keys.clone(),
                    valid,
                    known,
                    live,
                };
                let (plan, _, outcome) = p
                    .plan_ordered_live(&specs, &lookup(Some(tomb(None))))
                    .unwrap();
                let plain = p.plan(&specs, &lookup(None)).unwrap();
                let want = bitemporal_truth(&keys, valid, known);
                let needed: BTreeSet<(String, u32)> =
                    want.iter().map(|r| (r.path.clone(), r.rg)).collect();
                let planned = plan_rgs(&plan);
                assert!(needed.is_subset(&planned));
                assert!(planned.is_subset(&plan_rgs(&plain)));
                let got = sorted(apply_residual(
                    plan.residual.as_ref().unwrap(),
                    planned_rows(&plan, &rows),
                ));
                assert_eq!(got, want, "{valid:?} {known:?}");
                assert_eq!(
                    outcome.pruned_by.as_deref(),
                    Some("tombstone:id@valid,known")
                );
                pruned += outcome.row_groups_pruned;
            }
        }
    }
    assert!(pruned > 0);
}

/// Condition (c): an incomplete tombstone sidecar, as a binary without
/// tombstones leaves it after appending row groups (it builds the key's own
/// segments over them and keeps the sidecar's segments as they were), plans
/// as without tombstones, with the residual still exact, and a build
/// completes it.
#[test]
fn an_incomplete_sidecar_plans_without_pruning_until_a_build_completes_it() {
    let s = Scratch::new();
    corpus(&s, &[0, 1]);
    let spec = temporal_spec(None);
    let store = memory_store();
    let b = builder(&store);
    let opts = parquet_opts(&schema());
    build_ok(&b, &s.root_str(), std::slice::from_ref(&spec), &opts);
    corpus(&s, &[2]);
    build_ok(&b, &s.root_str(), std::slice::from_ref(&spec), &opts);
    let rows = all_rows(&s, &[0, 1, 2]);
    // Drop the tombstone segments over f2's row groups.
    let m = b.manifest().unwrap().unwrap();
    let f2: BTreeSet<u32> = m
        .rowgroup_space
        .files
        .iter()
        .filter(|f| f.fingerprint.path.ends_with("f2.parquet"))
        .flat_map(|f| f.rowgroup_ids.iter().map(|i| i.0))
        .collect();
    commit(&*store, "idx", &StagedSegments::new(), |cur| {
        let mut next = cur.clone();
        next.segments.retain(|sg| {
            sg.kind.as_deref() != Some("tombstone")
                || !(sg.rowgroup_range.start.0..sg.rowgroup_range.end.0).any(|i| f2.contains(&i))
        });
        Ok(next)
    })
    .unwrap();
    let p = planner(&store);
    let plain = spec.without_tombstone();
    assert!(p.completeness(&plain).is_some());
    assert_eq!(p.tombstone_completeness(&spec), None);
    assert_eq!(p.completeness(&spec), None);
    let (usable, excluded) = p.usable_specs();
    assert_eq!(usable, vec![plain.clone()], "planned without its tombstone");
    assert!(excluded.is_empty());
    assert_eq!(p.declared_specs(), vec![spec.clone()], "still declared");
    for keys in keysets() {
        for asof in INSTANTS {
            let (planned, outcome) = check(&p, &usable, &rows, &keys, asof, &tomb(None));
            assert_eq!(outcome.pruned_by, None, "nothing pruned by tombstones");
            assert_eq!(outcome.row_groups_pruned, 0);
            let lookup = match asof {
                None => Lookup::Latest {
                    columns: cols(&["id"]),
                    keys: keys.clone(),
                    live: None,
                },
                Some(ts) => Lookup::AsOf {
                    columns: cols(&["id"]),
                    keys: keys.clone(),
                    ts,
                    live: None,
                },
            };
            assert_eq!(planned, plan_rgs(&p.plan(&usable, &lookup).unwrap()));
        }
    }
    // A build declaring the specs as the manifest does completes it.
    let r = build_ok(&b, &s.root_str(), &p.declared_specs(), &opts);
    let p = planner(&store);
    assert_eq!(p.tombstone_completeness(&spec), r.sequence);
    assert_eq!(p.usable_specs().0, vec![spec.clone()]);
    let (_, outcome) = check(
        &p,
        std::slice::from_ref(&spec),
        &rows,
        &ids(&[NEWEST_TOMBSTONE]),
        None,
        &tomb(None),
    );
    assert_eq!(outcome.pruned_by.as_deref(), Some("tombstone:id@ts"));
    assert_eq!(outcome.keys_retracted, 1);
}

/// The manifest declares the spec without its tombstone and the tombstone
/// as a sidecar; removing the spec with its tombstone removes only the
/// sidecar, and removing the plain spec removes both.
#[test]
fn the_manifest_holds_the_plain_spec_and_a_tombstone_sidecar() {
    let s = Scratch::new();
    corpus(&s, &[0]);
    let spec = temporal_spec(Some("VOID"));
    let plain = spec.without_tombstone();
    let store = memory_store();
    let b = builder(&store);
    build_ok(
        &b,
        &s.root_str(),
        std::slice::from_ref(&spec),
        &parquet_opts(&schema()),
    );
    let m = b.manifest().unwrap().unwrap();
    assert_eq!(m.schema.key_specs, vec![plain.clone()]);
    let decl: Vec<_> = m
        .schema
        .sidecars
        .iter()
        .filter(|d| d.kind == "tombstone")
        .collect();
    assert_eq!(decl.len(), 1);
    assert_eq!(decl[0].name(), "tombstone:id@ts");
    assert_eq!(
        decl[0].params,
        BTreeMap::from([
            ("column".to_string(), "status".to_string()),
            ("value".to_string(), "VOID".to_string())
        ])
    );
    assert!(m
        .segments
        .iter()
        .any(|sg| sg.kind.as_deref() == Some("tombstone") && sg.key_spec == "tombstone:id@ts"));
    assert!(m
        .segments
        .iter()
        .any(|sg| sg.kind.is_none() && sg.key_spec == plain.identity()));
    assert!(completeness(&m, &spec).is_some());
    // with its tombstone: only the sidecar goes
    b.remove_spec(&spec).unwrap();
    let m = b.manifest().unwrap().unwrap();
    assert_eq!(m.schema.key_specs, vec![plain.clone()]);
    assert!(m.schema.sidecars.iter().all(|d| d.kind != "tombstone"));
    assert!(m
        .segments
        .iter()
        .all(|sg| sg.kind.as_deref() != Some("tombstone")));
    assert!(completeness(&m, &plain).is_some());
    // declared again, then removed without it: both go
    build_ok(
        &b,
        &s.root_str(),
        std::slice::from_ref(&spec),
        &parquet_opts(&schema()),
    );
    b.remove_spec(&plain).unwrap();
    let m = b.manifest().unwrap().unwrap();
    assert!(m.schema.key_specs.is_empty());
    assert!(m.schema.sidecars.iter().all(|d| d.kind != "tombstone"));
    assert!(m.segments.is_empty(), "{:?}", m.segments);
}

/// A tombstone column that cannot mark one is refused when declared, with
/// what to declare instead.
#[test]
fn a_tombstone_that_cannot_mark_a_row_is_refused_when_declared() {
    let s = Scratch::new();
    corpus(&s, &[0]);
    let store = memory_store();
    let b = builder(&store);
    for (t, want) in [
        (
            Tombstone {
                column: "status".into(),
                value: None,
            },
            "needs a boolean column",
        ),
        (
            Tombstone {
                column: "v".into(),
                value: Some("x".into()),
            },
            "is not an integer",
        ),
        (
            Tombstone {
                column: "nope".into(),
                value: None,
            },
            "nope",
        ),
        (
            Tombstone {
                column: "ts".into(),
                value: Some("1".into()),
            },
            "key or time column",
        ),
    ] {
        let spec = KeySpec::Temporal {
            columns: cols(&["id"]),
            ts_column: "ts".into(),
            counts: false,
            tombstone: Some(t),
        };
        let err = b
            .build(
                &s.root_str(),
                std::slice::from_ref(&spec),
                &parquet_opts(&schema()),
            )
            .unwrap_err()
            .to_string();
        assert!(err.contains(want), "{err}");
    }
    assert!(b.manifest().unwrap().is_none(), "nothing committed");
}
