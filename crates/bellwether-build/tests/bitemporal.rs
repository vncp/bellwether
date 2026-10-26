//! WP21 item 5: bitemporal as-of through a bitemporal key spec
//! (`temporal2:id@valid,known`, the `bitemporal` layout at M5). Ground
//! truth: rows decoded with the
//! `parquet` crate and the rule written out here: per key, among rows whose
//! valid time is at or before V and whose known time is at or before K
//! (nulls never qualify), the greatest valid time, then the greatest known
//! time; every row at that pair is returned.

#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::similar_names,
    clippy::too_many_lines
)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_build::{BuildOptions, Lookup, SourceKind};
use bellwether_core::sidecar::{SidecarDef, SidecarKind};
use bellwether_types::KeySpec;
use common::*;

fn schema() -> SchemaRef {
    let ts = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("valid", ts.clone(), true),
        Field::new("known", ts, true),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn def() -> SidecarDef {
    SidecarDef {
        kind: SidecarKind::Bitemporal,
        columns: vec!["id".into()],
        time_columns: vec!["valid".into(), "known".into()],
    }
}

/// `temporal2:id@valid,known`.
fn spec() -> KeySpec {
    KeySpec::Bitemporal {
        columns: cols(&["id"]),
        valid_column: "valid".into(),
        known_column: "known".into(),
    }
}

fn specs() -> [KeySpec; 2] {
    [exact(&["id"]), spec()]
}

type R = (Option<i64>, Option<i64>, Option<i64>);

fn write(s: &Scratch, name: &str, rows: &[R]) {
    let ts = |f: fn(&R) -> Option<i64>| {
        Arc::new(
            TimestampMicrosecondArray::from(rows.iter().map(f).collect::<Vec<_>>())
                .with_timezone("UTC"),
        )
    };
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            ts(|r| r.1),
            ts(|r| r.2),
            Arc::new(Int64Array::from_iter_values(0..rows.len() as i64)),
        ],
    )
    .unwrap();
    write_parquet(&s.path(name), &batch, 2);
}

/// Quarter-end values (valid) learned on report dates (known), then
/// restated; a late row; a tie across row groups; nulls in either clock.
fn corpus(s: &Scratch) {
    write(
        s,
        "a.parquet",
        &[
            // id 1: Q1 (valid 100) reported at 120, Q2 (valid 200) at 220.
            (Some(1), Some(100), Some(120)),
            (Some(1), Some(200), Some(220)),
            // id 2: a value valid at 100, known at 105.
            (Some(2), Some(100), Some(105)),
            // id 3: null valid and null known never win.
            (Some(3), None, Some(50)),
            (Some(3), Some(40), None),
            (Some(3), Some(30), Some(35)),
            // id 4: a value and its restatement in one row group, so the
            // residual alone must pick the later known time.
            (Some(4), Some(50), Some(60)),
            (Some(4), Some(50), Some(70)),
        ],
    );
    write(
        s,
        "b.parquet",
        &[
            // id 1: Q1 restated at 300; Q2 restated at 260 and again at 400.
            (Some(1), Some(100), Some(300)),
            (Some(1), Some(200), Some(260)),
            (Some(1), Some(200), Some(400)),
            // id 2: a late row: valid 90, learned only at 500.
            (Some(2), Some(90), Some(500)),
            // id 2: the same (valid, known) as in a.parquet, a tie across
            // files.
            (Some(2), Some(100), Some(105)),
            // A null key is never a version.
            (None, Some(999), Some(999)),
        ],
    );
}

fn us(r: &Row, c: &str) -> Option<i64> {
    let b = r.get(c)?;
    let tail: [u8; 8] = b[b.len() - 8..].try_into().unwrap();
    Some((u64::from_be_bytes(tail) ^ (1 << 63)) as i64)
}

fn truth(
    rows: &[Row],
    keys: &BTreeSet<Vec<u8>>,
    valid: Option<i64>,
    known: Option<i64>,
) -> Vec<Row> {
    let mut best: BTreeMap<Vec<u8>, (i64, i64)> = BTreeMap::new();
    let qualifies = |r: &Row| -> Option<(Vec<u8>, i64, i64)> {
        let k = r.get("id")?.clone();
        let (v, n) = (us(r, "valid")?, us(r, "known")?);
        (keys.contains(&k) && valid.is_none_or(|x| v <= x) && known.is_none_or(|x| n <= x))
            .then_some((k, v, n))
    };
    for r in rows {
        if let Some((k, v, n)) = qualifies(r) {
            let e = best.entry(k).or_insert((v, n));
            if (v, n) > *e {
                *e = (v, n);
            }
        }
    }
    rows.iter()
        .filter(|r| qualifies(r).is_some_and(|(k, v, n)| best.get(&k) == Some(&(v, n))))
        .cloned()
        .collect()
}

#[test]
fn the_newest_valid_version_known_by_k_wins_for_every_v_and_k() {
    let s = Scratch::new();
    corpus(&s);
    let mut all = parquet_rows(&s.path("a.parquet"), &schema());
    all.extend(parquet_rows(&s.path("b.parquet"), &schema()));
    let store = memory_store();
    builder(&store)
        .build(
            &s.root_str(),
            &specs(),
            &BuildOptions::new(SourceKind::Parquet),
        )
        .unwrap();
    let p = planner(&store);
    assert_eq!(p.sidecar_completeness(&def()), Some(1));
    let ids: Vec<Vec<u8>> = [1, 2, 3, 4].iter().map(|i| enc_i64(*i)).collect();
    let points: Vec<Option<i64>> = [
        None,
        Some(0),
        Some(35),
        Some(90),
        Some(100),
        Some(105),
        Some(150),
        Some(200),
        Some(220),
        Some(259),
        Some(260),
        Some(300),
        Some(399),
        Some(400),
        Some(500),
        Some(i64::MAX),
    ]
    .into_iter()
    .collect();
    let mut restated_seen = false;
    for &valid in &points {
        for &known in &points {
            for keys in [&ids[..1], &ids[1..2], &ids[2..3], &ids[..]] {
                let key_set: BTreeSet<Vec<u8>> = keys.iter().cloned().collect();
                let plan = p
                    .plan(
                        &specs(),
                        &Lookup::Bitemporal {
                            columns: cols(&["id"]),
                            clocks: None,
                            keys: keys.to_vec(),
                            valid,
                            known,
                        },
                    )
                    .unwrap();
                let want = truth(&all, &key_set, valid, known);
                let got =
                    apply_residual(plan.residual.as_ref().unwrap(), planned_rows(&plan, &all));
                assert_eq!(
                    sorted(got),
                    sorted(want.clone()),
                    "V={valid:?} K={known:?} {keys:?}"
                );
                // Exactly the row groups holding a winner.
                let rgs: BTreeSet<(String, u32)> =
                    want.iter().map(|r| (r.path.clone(), r.rg)).collect();
                assert_eq!(plan_rgs(&plan), rgs, "V={valid:?} K={known:?}");
                if keys == &ids[..1] && valid == Some(150) {
                    // Q1 as known at 299 is the original, at 300 the
                    // restatement.
                    let price: Vec<Option<i64>> = want.iter().map(|r| us(r, "known")).collect();
                    match known {
                        Some(k) if (120..300).contains(&k) => assert_eq!(price, vec![Some(120)]),
                        Some(k) if k >= 300 => {
                            assert_eq!(price, vec![Some(300)]);
                            restated_seen = true;
                        }
                        None => assert_eq!(price, vec![Some(300)]),
                        _ => assert_eq!(price, Vec::<Option<i64>>::new()),
                    }
                }
            }
        }
    }
    assert!(restated_seen);
}

#[test]
fn a_rewritten_file_s_versions_no_longer_win() {
    let s = Scratch::new();
    corpus(&s);
    let store = memory_store();
    builder(&store)
        .build(
            &s.root_str(),
            &specs(),
            &BuildOptions::new(SourceKind::Parquet),
        )
        .unwrap();
    // b.parquet is rewritten without its restatements: its old row-group
    // ids are retired, and the original reports win again.
    write(&s, "b.parquet", &[(Some(9), Some(1), Some(1))]);
    builder(&store)
        .build(
            &s.root_str(),
            &specs(),
            &BuildOptions::new(SourceKind::Parquet),
        )
        .unwrap();
    let p = planner(&store);
    assert!(p.sidecar_completeness(&def()).is_some());
    let mut all = parquet_rows(&s.path("a.parquet"), &schema());
    all.extend(parquet_rows(&s.path("b.parquet"), &schema()));
    let keys = vec![enc_i64(1)];
    let plan = p
        .plan(
            &specs(),
            &Lookup::Bitemporal {
                columns: cols(&["id"]),
                clocks: None,
                keys: keys.clone(),
                valid: None,
                known: None,
            },
        )
        .unwrap();
    let got = apply_residual(plan.residual.as_ref().unwrap(), planned_rows(&plan, &all));
    assert_eq!(
        sorted(got),
        sorted(truth(&all, &keys.into_iter().collect(), None, None))
    );
    assert!(plan_rgs(&plan)
        .iter()
        .all(|(f, _)| f.ends_with("a.parquet")));

    // Without the sidecar, the lookup is refused with what to declare.
    let bare = memory_store();
    builder(&bare)
        .build(
            &s.root_str(),
            &[exact(&["id"])],
            &BuildOptions::new(SourceKind::Parquet),
        )
        .unwrap();
    let err = planner(&bare)
        .plan(
            &[],
            &Lookup::Bitemporal {
                columns: cols(&["id"]),
                clocks: None,
                keys: vec![enc_i64(1)],
                valid: None,
                known: None,
            },
        )
        .unwrap_err();
    assert!(err.to_string().contains("temporal2:"), "{err}");
}

/// A bitemporal key holds only the rows where both clocks are set, so it
/// never serves a plain lookup: `locate` beside it plans every row group
/// holding the key, the ones whose rows have a null clock included (id 3's
/// first row group holds only a row with a null valid time).
#[test]
fn a_bitemporal_key_never_narrows_a_plain_lookup() {
    let s = Scratch::new();
    corpus(&s);
    let mut all = parquet_rows(&s.path("a.parquet"), &schema());
    all.extend(parquet_rows(&s.path("b.parquet"), &schema()));
    let store = memory_store();
    builder(&store)
        .build(
            &s.root_str(),
            &specs(),
            &BuildOptions::new(SourceKind::Parquet),
        )
        .unwrap();
    let p = planner(&store);
    for id in [1, 2, 3, 4] {
        let key = enc_i64(id);
        let plan = p
            .plan(
                &specs(),
                &Lookup::Keys {
                    columns: cols(&["id"]),
                    keys: vec![key.clone()],
                },
            )
            .unwrap();
        let want = holding(&all, |r| r.get("id") == Some(&key));
        assert_eq!(plan_rgs(&plan), want, "id {id}");
    }
    let three = holding(&all, |r| r.get("id") == Some(&enc_i64(3)));
    assert_eq!(
        three.len(),
        2,
        "id 3 sits in two row groups, one without a version"
    );
}
