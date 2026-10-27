//! A plan that does not belong to its inputs is a typed `AdapterError`
//! from every adapter, never a panic or an engine's error string (WP16
//! item 7, `tests/corrupt.rs`): an unknown file, an out-of-range row
//! group, a residual over a column not in the schema, canonical bytes at
//! the wrong width, a malformed read order; every byte of every canonical
//! value in a residual flipped is refused exactly when it does not decode,
//! and otherwise the value the adapters translate re-encodes to the
//! flipped bytes; and a DataFusion scan description edited after it was
//! printed does not load.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::doc_markdown,
    clippy::type_complexity
)]

mod common;

use arrow::datatypes::DataType;
use bellwether_adapters::residual::{scalar_json, value_type};
use bellwether_adapters::{adapter, datafusion, AdapterError, EmitOptions, ENGINES};
use bellwether_core::encoding::{decode_tuple, encode_value_as, Scalar};
use bellwether_core::segment::ColumnType;
use bellwether_types::{Predicate, ScanPlan};
use serde_json::Value as Json;

fn opts(c: &common::Corpus) -> EmitOptions {
    EmitOptions {
        row_offsets: Some(common::row_offsets(c)),
        partitions: Some(common::partitions(c)),
        ..EmitOptions::default()
    }
}

fn each(plan: &ScanPlan, o: &EmitOptions, check: impl Fn(&str, Result<(), AdapterError>)) {
    for e in ENGINES {
        let r = adapter(e)
            .unwrap()
            .emit(plan, &common::dataset(), o)
            .map(|_| ());
        check(e, r);
    }
}

#[test]
fn an_unknown_file_is_refused_by_name() {
    let tmp = tempfile::tempdir().unwrap();
    let c = common::corpus(tmp.path());
    let mut plan = common::locate(&c, &["id"], &common::ids(&[1]));
    plan.entries[0].file.path = format!("{}/nowhere.parquet", c.root.display());
    each(&plan, &opts(&c), |e, r| {
        assert!(
            matches!(r, Err(AdapterError::UnknownFile { .. })),
            "{e}: {r:?}"
        );
    });
    // With the footer hint, a file whose size is not the fingerprint's.
    let mut plan = common::locate(&c, &["id"], &common::ids(&[1]));
    plan.entries[0].file.size += 1;
    let o = EmitOptions {
        footer_hint: true,
        ..opts(&c)
    };
    let r = adapter("datafusion")
        .unwrap()
        .emit(&plan, &common::dataset(), &o);
    let msg = format!("{r:?}");
    assert!(matches!(r, Err(AdapterError::UnknownFile { .. })), "{msg}");
    assert!(msg.contains("changed since it was planned"), "{msg}");
}

#[test]
fn an_out_of_range_row_group_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let c = common::corpus(tmp.path());
    let mut plan = common::locate(&c, &["id"], &common::ids(&[1]));
    plan.entries[0].row_groups.insert(99);
    each(&plan, &opts(&c), |e, r| {
        assert!(
            matches!(
                r,
                Err(AdapterError::RowGroupOutOfRange { row_group: 99, .. })
            ),
            "{e}: {r:?}"
        );
    });
    // Also when the counts come from the footer.
    let o = EmitOptions {
        row_offsets: None,
        footer_hint: true,
        ..opts(&c)
    };
    let r = adapter("datafusion")
        .unwrap()
        .emit(&plan, &common::dataset(), &o);
    assert!(
        matches!(r, Err(AdapterError::RowGroupOutOfRange { .. })),
        "{r:?}"
    );
}

#[test]
fn a_residual_over_an_unknown_column_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let c = common::corpus(tmp.path());
    let mut plan = common::locate(&c, &["id"], &common::ids(&[1]));
    if let Some(Predicate::In { columns, .. }) = &mut plan.residual {
        columns[0] = "nope".into();
    }
    each(&plan, &opts(&c), |e, r| {
        assert!(
            matches!(&r, Err(AdapterError::UnknownColumn { column, .. }) if column == "nope"),
            "{e}: {r:?}"
        );
    });
}

#[test]
fn canonical_bytes_at_the_wrong_width_are_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let c = common::corpus(tmp.path());
    // A Decimal256 encoding on a Decimal128 column; a millisecond timestamp
    // on the microsecond column; a truncated integer.
    let wide = {
        let mut b = vec![0x05, 0x20];
        b.extend([0u8; 32]);
        b[2] = 0x80;
        b
    };
    let ms = common::enc(
        bellwether_core::encoding::Value::Timestamp {
            value: 1,
            unit: arrow::datatypes::TimeUnit::Millisecond,
        },
        &common::dt("ts_ms"),
    );
    for (col, bytes) in [("d2", wide), ("ts", ms), ("id", vec![0x02, 0x80, 0])] {
        let plan = common::plan(
            &c,
            &[(0usize, 0u32)].into(),
            Some(Predicate::In {
                columns: vec![col.into()],
                values: vec![bytes],
            }),
        );
        each(&plan, &opts(&c), |e, r| {
            assert!(
                matches!(r, Err(AdapterError::Canonical { .. })),
                "{e} {col}: {r:?}"
            );
        });
    }
}

#[test]
fn a_malformed_read_order_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let c = common::corpus(tmp.path());
    let mut plan = common::locate(&c, &["id"], &common::ids(&[1]));
    plan.order = Some(vec![(7, 0)]);
    each(&plan, &opts(&c), |e, r| {
        assert!(matches!(r, Err(AdapterError::Plan(_))), "{e}: {r:?}");
    });
}

#[test]
fn without_row_counts_a_strict_subset_is_refused_not_widened() {
    let tmp = tempfile::tempdir().unwrap();
    let c = common::corpus(tmp.path());
    let plan = common::locate(&c, &["id"], &common::ids(&[1]));
    let o = EmitOptions {
        row_offsets: None,
        ..opts(&c)
    };
    each(&plan, &o, |e, r| {
        assert!(
            matches!(r, Err(AdapterError::RowOffsetsNeeded { .. })),
            "{e}: {r:?}"
        );
    });
}

/// `bytes` decoded as a key tuple of `types` the way the adapters decode
/// it, independently of them.
fn decoded(bytes: &[u8], types: &[DataType]) -> Option<Vec<Option<Scalar>>> {
    let cts: Vec<ColumnType> = types.iter().map(|t| ColumnType::of(t).unwrap()).collect();
    decode_tuple(bytes, &cts).ok()
}

/// The canonical bytes of a decoded tuple: each element's encoding, `0x00`
/// for a null.
fn reencoded(tuple: &[Option<Scalar>], types: &[DataType]) -> Vec<u8> {
    let mut out = Vec::new();
    for (v, t) in tuple.iter().zip(types) {
        match v {
            Some(s) => encode_value_as(&s.as_value(), t, &mut out).unwrap(),
            None => out.push(0x00),
        }
    }
    out
}

#[test]
fn a_flipped_residual_byte_is_refused_iff_it_does_not_decode_and_otherwise_reencodes() {
    let tmp = tempfile::tempdir().unwrap();
    let c = common::corpus(tmp.path());
    let mut checked = 0;
    for case in common::cases(&c) {
        let Some(Predicate::And { all }) = &case.plan.residual else {
            continue;
        };
        let Some(Predicate::In { columns, values }) = all.first() else {
            continue;
        };
        let Some(v) = values.first() else { continue };
        let types: Vec<DataType> = columns
            .iter()
            .map(|c| value_type(&common::dt(c)).clone())
            .collect();
        let with = |bytes: Vec<u8>| {
            let mut plan = case.plan.clone();
            plan.residual = Some(Predicate::In {
                columns: columns.clone(),
                values: vec![bytes],
            });
            plan
        };
        // What each engine says of the unflipped key: an engine that
        // refuses it may refuse a flipped one for the same reason.
        let baseline: Vec<bool> = ENGINES
            .iter()
            .map(|e| {
                matches!(
                    adapter(e)
                        .unwrap()
                        .emit(&with(v.clone()), &common::dataset(), &opts(&c)),
                    Err(AdapterError::Unsupported { .. })
                )
            })
            .collect();
        for i in 0..v.len() {
            for flip in [0x01u8, 0x80, 0xff] {
                let mut bad = v.clone();
                bad[i] ^= flip;
                let plan = with(bad.clone());
                let tuple = decoded(&bad, &types);
                if let Some(t) = &tuple {
                    assert_eq!(
                        reencoded(t, &types),
                        bad,
                        "{} byte {i}: decode is not exact",
                        case.name
                    );
                }
                let nan = tuple
                    .iter()
                    .flatten()
                    .flatten()
                    .any(|s| matches!(s, Scalar::Float(f) if f.is_nan()));
                for (e, refused_before) in ENGINES.iter().zip(&baseline) {
                    let r = adapter(e)
                        .unwrap()
                        .emit(&plan, &common::dataset(), &opts(&c));
                    let at = format!("{e} {} byte {i} ^{flip:02x}", case.name);
                    match (&tuple, r) {
                        (None, Err(AdapterError::Canonical { .. })) => {}
                        (None, other) => {
                            panic!("{at}: bytes that do not decode were not refused: {other:?}")
                        }
                        (Some(_), Err(AdapterError::Unsupported { .. }))
                            if *refused_before || (*e == "duckdb" && nan) => {}
                        (Some(t), Ok(_)) if *e == "datafusion" => {
                            // The literal the scan carries is the decoded
                            // value, typed (a tuple with a null matches
                            // nothing and is not listed).
                            let printed: Json = serde_json::from_str(
                                &datafusion::scan_config(&plan, &common::dataset(), &opts(&c))
                                    .unwrap()
                                    .to_json()
                                    .unwrap(),
                            )
                            .unwrap();
                            let want: Vec<Json> = if t.iter().any(Option::is_none) {
                                Vec::new()
                            } else {
                                vec![Json::Array(
                                    t.iter()
                                        .zip(&types)
                                        .map(|(s, dt)| scalar_json(s.as_ref().unwrap(), dt))
                                        .collect(),
                                )]
                            };
                            assert_eq!(
                                printed["residual"]["stages"][0]["rows"],
                                Json::Array(want),
                                "{at}"
                            );
                        }
                        (Some(_), Ok(_)) => {}
                        (Some(_), other) => {
                            panic!("{at}: a value that decodes was refused: {other:?}")
                        }
                    }
                }
                checked += 1;
            }
        }
    }
    assert!(checked > 100, "{checked}");
}

#[test]
fn an_edited_datafusion_description_does_not_load() {
    let tmp = tempfile::tempdir().unwrap();
    let c = common::corpus(tmp.path());
    let o = EmitOptions {
        footer_hint: true,
        ..opts(&c)
    };
    let mut loaded = 0;
    for case in common::cases(&c) {
        let Ok(cfg) = datafusion::scan_config(&case.plan, &common::dataset(), &o) else {
            continue;
        };
        let printed = cfg.to_json().unwrap();
        // As printed, it loads, and prints the same again.
        let back = datafusion::from_json(&printed).unwrap_or_else(|e| panic!("{}: {e}", case.name));
        assert_eq!(back.to_json().unwrap(), printed, "{}", case.name);
        loaded += 1;
    }
    assert!(loaded >= 20, "{loaded}");
    let plan = common::locate(&c, &["id"], &common::ids(&[1, 2]));
    let printed: Json = serde_json::from_str(
        &datafusion::scan_config(&plan, &common::dataset(), &o)
            .unwrap()
            .to_json()
            .unwrap(),
    )
    .unwrap();
    let skip = printed["files"][0]["access"]
        .as_array()
        .unwrap()
        .iter()
        .position(|a| a == "skip")
        .expect("a strict subset of the file's row groups");
    // Each edit, and the field the refusal names first (fields are compared
    // in the description's order; the options edit changes the Parquet
    // options the rebuilt scan prints).
    let edits: Vec<(&str, Box<dyn Fn(&mut Json)>)> = vec![
        (
            "files",
            Box::new(move |d: &mut Json| d["files"][0]["access"][skip] = "scan".into()),
        ),
        (
            "parquet",
            Box::new(|d: &mut Json| d["parquet"]["pruning"] = true.into()),
        ),
        (
            "projection",
            Box::new(|d: &mut Json| d["projection"] = serde_json::json!(["id"])),
        ),
        (
            "residual",
            Box::new(|d: &mut Json| d["residual"]["stages"][0]["rows"][0][0]["value"] = "3".into()),
        ),
        (
            "parquet",
            Box::new(|d: &mut Json| d["source"]["options"]["engine_pruning"] = true.into()),
        ),
        (
            "format",
            Box::new(|d: &mut Json| d["format"] = "bellwether.datafusion/0".into()),
        ),
    ];
    for (field, edit) in edits {
        let mut d = printed.clone();
        edit(&mut d);
        match datafusion::from_json(&serde_json::to_string(&d).unwrap()) {
            Err(e @ AdapterError::Description(_)) => {
                let msg = e.to_string();
                assert!(
                    msg.contains(&format!("{field:?}")) || field == "format",
                    "{field}: {msg}"
                );
                assert!(msg.contains("emit it again"), "{field}: {msg}");
            }
            other => panic!("{field}: an edited description loaded or failed untyped: {other:?}"),
        }
    }
    // A schema edited so the plan's key bytes no longer decode at the
    // column's type: refused by the adapter's own check, typed.
    let mut d = printed.clone();
    let id = d["source"]["schema"]
        .as_array()
        .unwrap()
        .iter()
        .position(|c| c["name"] == "id")
        .unwrap();
    d["source"]["schema"][id]["type"] = "Utf8".into();
    let r = datafusion::from_json(&serde_json::to_string(&d).unwrap());
    assert!(matches!(r, Err(AdapterError::Canonical { .. })), "{r:?}");
    assert!(matches!(
        datafusion::from_json("{"),
        Err(AdapterError::Description(_))
    ));
}

#[test]
fn refusals_say_what_to_do_next() {
    let tmp = tempfile::tempdir().unwrap();
    let c = common::corpus(tmp.path());
    let case = common::cases(&c)
        .into_iter()
        .find(|k| k.name.starts_with("text has token"))
        .unwrap();
    for e in ["duckdb", "spark"] {
        let err = adapter(e)
            .unwrap()
            .emit(&case.plan, &common::dataset(), &opts(&c))
            .unwrap_err()
            .to_string();
        assert!(err.contains("emit for DataFusion"), "{e}: {err}");
        assert!(!err.contains("Predicate"), "{e}: raw Debug in {err}");
    }
}
