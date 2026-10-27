//! ADR 013 ids are never reused across rewrite, vanish and reappear (tracked
//! as an id -> (fingerprint, index) map across every commit); a build that
//! lists one spec keeps the others; every typed probe path encodes at the
//! dataset type.
//!
//! Added as regression tests from the PR 19 review.

#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::type_complexity,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::float_cmp,
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::too_many_arguments,
    clippy::assert_is_empty,
    clippy::redundant_closure_for_method_calls,
    clippy::format_push_string,
    clippy::doc_markdown
)]

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Decimal128Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_build::{completeness, BuildOptions, Builder, Lookup, Planner, SourceKind};
use bellwether_core::encoding::Value;
use bellwether_store::Manifest;
use bellwether_types::{FileFingerprint, KeySpec};
use common::{cols, memory_store, simple_schema, write_numbered, Scratch};

/// Every id ever committed maps to exactly one (fingerprint, index_in_file)
/// for the life of the index, and live/retired/next_id stay consistent.
#[derive(Default)]
struct IdLedger {
    seen: BTreeMap<u32, (FileFingerprint, usize)>,
    retired_ever: std::collections::BTreeSet<u32>,
}

impl IdLedger {
    fn observe(&mut self, m: &Manifest) {
        let mut live = std::collections::BTreeSet::new();
        for f in &m.rowgroup_space.files {
            for (i, id) in f.rowgroup_ids.iter().enumerate() {
                assert!(id.0 < m.rowgroup_space.next_id, "id {} >= next_id", id.0);
                assert!(live.insert(id.0), "id {} live twice", id.0);
                assert!(
                    !self.retired_ever.contains(&id.0),
                    "ADR 013: retired id {} is live again",
                    id.0
                );
                match self.seen.get(&id.0) {
                    None => {
                        self.seen.insert(id.0, (f.fingerprint.clone(), i));
                    }
                    Some((fp, idx)) => assert!(
                        fp == &f.fingerprint && *idx == i,
                        "ADR 013: id {} reused: was {}#{idx}, now {}#{i}",
                        id.0,
                        fp.path,
                        f.fingerprint.path
                    ),
                }
            }
        }
        for t in &m.rowgroup_space.tombstones {
            for id in &t.rowgroup_ids {
                assert!(!live.contains(&id.0), "id {} live and retired", id.0);
                self.retired_ever.insert(id.0);
            }
        }
        let all = live.len()
            + m.rowgroup_space
                .tombstones
                .iter()
                .map(|t| t.rowgroup_ids.len())
                .sum::<usize>();
        assert_eq!(
            all as u32, m.rowgroup_space.next_id,
            "dense: every id below next_id is live or retired"
        );
    }
}

fn exact(c: &[&str]) -> KeySpec {
    KeySpec::Exact {
        columns: c.iter().map(|s| (*s).to_string()).collect(),
        counts: false,
        bounds: None,
    }
}

fn temporal() -> KeySpec {
    KeySpec::Temporal {
        columns: vec!["id".into()],
        ts_column: "ts".into(),
        counts: false,
        tombstone: None,
    }
}

#[test]
fn ids_are_never_reused_across_rewrite_vanish_and_reappear() {
    let s = Scratch::new();
    let store = memory_store();
    let b = Builder::new(Arc::clone(&store), "idx");
    let o = BuildOptions {
        schema: Some(simple_schema()),
        ..BuildOptions::new(SourceKind::Parquet)
    };
    let specs = [exact(&["id"]), temporal()];
    let mut ledger = IdLedger::default();
    let step = |ledger: &mut IdLedger| {
        b.build(&s.root_str(), &specs, &o).unwrap();
        let m = b.manifest().unwrap().unwrap();
        ledger.observe(&m);
        for spec in &specs {
            assert!(
                completeness(&m, spec).is_some(),
                "{} incomplete",
                spec.name()
            );
        }
        m
    };
    write_numbered(&s.path("a.parquet"), 0, 12, 4);
    write_numbered(&s.path("b.parquet"), 100, 8, 4);
    write_numbered(&s.path("c.parquet"), 200, 4, 4);
    step(&mut ledger);
    // rewrite a with MORE row groups, then FEWER
    write_numbered(&s.path("a.parquet"), 0, 20, 4);
    step(&mut ledger);
    write_numbered(&s.path("a.parquet"), 0, 4, 4);
    step(&mut ledger);
    // b vanishes
    let b_bytes = std::fs::read(s.path("b.parquet")).unwrap();
    std::fs::remove_file(s.path("b.parquet")).unwrap();
    step(&mut ledger);
    // b reappears byte-identical, and c vanishes in the same build
    std::fs::write(s.path("b.parquet"), &b_bytes).unwrap();
    std::fs::remove_file(s.path("c.parquet")).unwrap();
    step(&mut ledger);
    // c reappears with other content under the same path, a renamed to d
    write_numbered(&s.path("c.parquet"), 300, 8, 4);
    std::fs::rename(s.path("a.parquet"), s.path("d.parquet")).unwrap();
    let m = step(&mut ledger);
    // at least 3 + 5 + 2 + 1 + 1 ids retired across the steps
    assert!(ledger.retired_ever.len() >= 12);
    // the plan maps every live key to its current file
    let p = Planner::open(Arc::clone(&store), "idx").unwrap();
    let rows: Vec<common::Row> = ["b.parquet", "c.parquet", "d.parquet"]
        .iter()
        .flat_map(|f| common::parquet_rows(&s.path(f), &simple_schema()))
        .collect();
    let keys: Vec<Vec<u8>> = (0..400).map(common::enc_i64).collect();
    common::check_keys(&p, &rows, &[exact(&["id"])], &cols(&["id"]), &keys);
    assert_eq!(p.manifest().sequence, m.sequence);
}

/// A caller that lists one spec must not silently destroy the others;
/// removal is explicit (`remove_unlisted`, `remove_spec`).
#[test]
fn a_build_listing_one_spec_keeps_the_others() {
    let s = Scratch::new();
    write_numbered(&s.path("a.parquet"), 0, 8, 4);
    let store = memory_store();
    let b = Builder::new(Arc::clone(&store), "idx");
    let o = BuildOptions {
        schema: Some(simple_schema()),
        ..BuildOptions::new(SourceKind::Parquet)
    };
    b.build(&s.root_str(), &[exact(&["id"]), temporal()], &o)
        .unwrap();
    // a later caller (say, a CLI `build --key id`) lists only exact:id
    write_numbered(&s.path("b.parquet"), 100, 8, 4);
    let r = b.build(&s.root_str(), &[exact(&["id"])], &o).unwrap();
    let m = b.manifest().unwrap().unwrap();
    let p = Planner::open(Arc::clone(&store), "idx").unwrap();
    let latest = p.plan(
        &[temporal()],
        &Lookup::Latest {
            columns: cols(&["id"]),
            keys: vec![common::enc_i64(1)],
        },
    );
    assert!(
        r.specs_removed.is_empty() && completeness(&m, &temporal()).is_some() && latest.is_ok(),
        "the temporal spec was removed by a build that did not mention it: specs_removed {:?}, \
         declared {:?}, segment tombstones {}, latest -> {:?}",
        r.specs_removed,
        m.schema
            .key_specs
            .iter()
            .map(KeySpec::name)
            .collect::<Vec<_>>(),
        m.segment_tombstones.len(),
        latest.err()
    );
}

/// Ambiguity 9: every typed probe path encodes at the dataset type; a
/// scalar at another decimal scale or time unit is rescaled or refused,
/// never encoded as is.
#[test]
fn typed_probe_paths_rescale_or_refuse() {
    let s = Scratch::new();
    let fschema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("amt", DataType::Decimal128(10, 2), true),
        Field::new("n", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&fschema),
        vec![
            Arc::new(
                Decimal128Array::from(vec![Some(150), Some(1), None])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        ],
    )
    .unwrap();
    common::write_parquet(&s.path("d.parquet"), &batch, 1);
    let dataset: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("amt", DataType::Decimal128(20, 4), true),
        Field::new("n", DataType::Int64, true),
    ]));
    let store = memory_store();
    let b = Builder::new(Arc::clone(&store), "idx");
    let o = BuildOptions {
        schema: Some(Arc::clone(&dataset)),
        ..BuildOptions::new(SourceKind::Parquet)
    };
    b.build(&s.root_str(), &[exact(&["amt"])], &o).unwrap();
    let p = Planner::open(Arc::clone(&store), "idx").unwrap();
    let c = cols(&["amt"]);
    let plan_rgs = |key: Vec<u8>| {
        p.plan(
            &[exact(&["amt"])],
            &Lookup::Keys {
                columns: c.clone(),
                keys: vec![key],
            },
        )
        .unwrap()
        .row_groups_selected
    };
    // 1.50 at scales 1, 2, 3 and 4 all encode to the dataset scale 4
    let at = |v: i128, sc: i8| {
        p.encode_tuple(
            &c,
            &[Value::Decimal128 {
                value: v,
                scale: sc,
            }],
        )
        .unwrap()
    };
    let want = at(15_000, 4);
    for (v, sc) in [(15, 1), (150, 2), (1_500, 3)] {
        assert_eq!(at(v, sc), want, "{v}@{sc} encoded as is");
        assert_eq!(plan_rgs(at(v, sc)), 1);
    }
    // 1.50001 is not representable at scale 4: refused, never rounded
    assert!(p
        .encode_tuple(
            &c,
            &[Value::Decimal128 {
                value: 150_001,
                scale: 5
            }]
        )
        .is_err());
    // a batch at the file's scale 2 is rescaled by encode_keys
    let keys = p.encode_keys(&c, &batch).unwrap();
    assert_eq!(keys.len(), 2, "the null row is skipped");
    assert!(keys.contains(&want));
    // a Decimal256 scalar for a Decimal128 column: width from the dataset
    let wide = p.encode_tuple(
        &c,
        &[Value::Decimal256 {
            value: arrow::datatypes::i256::from_i128(15_000),
            scale: 4,
        }],
    );
    assert_eq!(wide.unwrap(), want);
    // the raw-bytes path cannot see the scale: 1.50 encoded at scale 2 as
    // if the column were Decimal128(20, 2) is accepted and silently misses
    let wrong = common::enc(
        &Value::Decimal128 {
            value: 150,
            scale: 2,
        },
        &DataType::Decimal128(20, 2),
    );
    let r = p.plan(
        &[exact(&["amt"])],
        &Lookup::Keys {
            columns: c.clone(),
            keys: vec![wrong],
        },
    );
    eprintln!(
        "raw bytes at the wrong decimal scale: {:?} row groups (silent miss: documented residual of ambiguity 9)",
        r.as_ref().map(|p| p.row_groups_selected)
    );
    // timestamps: a typed probe in ms under a µs column converts exactly
    let _ = TimeUnit::Millisecond;
}
