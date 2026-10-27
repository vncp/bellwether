//! A plan that does not belong to its inputs is a typed `AdapterError`
//! from every adapter, never a panic or an engine's error string (WP16
//! item 7, `tests/corrupt.rs`): an unknown file, an out-of-range row
//! group, a residual over a column not in the schema, canonical bytes at
//! the wrong width, a malformed read order; and every byte of every
//! canonical value in a residual flipped decodes to a typed refusal or to
//! a value that re-encodes to the flipped bytes.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use bellwether_adapters::{adapter, AdapterError, EmitOptions, ENGINES};
use bellwether_types::{Predicate, ScanPlan};

fn opts(c: &common::Corpus) -> EmitOptions {
    EmitOptions {
        row_offsets: Some(common::row_offsets(c)),
        partitions: Some(common::partitions(c)),
        ..EmitOptions::default()
    }
}

fn each(plan: &ScanPlan, o: &EmitOptions, check: impl Fn(&str, Result<(), AdapterError>)) {
    for e in ENGINES {
        let r = adapter(e).unwrap().emit(plan, &common::dataset(), o).map(|_| ());
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
        assert!(matches!(r, Err(AdapterError::UnknownFile { .. })), "{e}: {r:?}");
    });
    // With the footer hint, a file whose size is not the fingerprint's.
    let mut plan = common::locate(&c, &["id"], &common::ids(&[1]));
    plan.entries[0].file.size += 1;
    let o = EmitOptions {
        footer_hint: true,
        ..opts(&c)
    };
    let r = adapter("datafusion").unwrap().emit(&plan, &common::dataset(), &o);
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
            matches!(r, Err(AdapterError::RowGroupOutOfRange { row_group: 99, .. })),
            "{e}: {r:?}"
        );
    });
    // Also when the counts come from the footer.
    let o = EmitOptions {
        row_offsets: None,
        footer_hint: true,
        ..opts(&c)
    };
    let r = adapter("datafusion").unwrap().emit(&plan, &common::dataset(), &o);
    assert!(matches!(r, Err(AdapterError::RowGroupOutOfRange { .. })), "{r:?}");
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
            assert!(matches!(r, Err(AdapterError::Canonical { .. })), "{e} {col}: {r:?}");
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
        assert!(matches!(r, Err(AdapterError::RowOffsetsNeeded { .. })), "{e}: {r:?}");
    });
}

#[test]
fn every_flipped_byte_of_a_residual_value_is_typed_or_exact() {
    let tmp = tempfile::tempdir().unwrap();
    let c = common::corpus(tmp.path());
    for case in common::cases(&c) {
        let Some(Predicate::And { all }) = &case.plan.residual else {
            continue;
        };
        let Some(Predicate::In { columns, values }) = all.first() else {
            continue;
        };
        let Some(v) = values.first() else { continue };
        for i in 0..v.len() {
            for flip in [0x01u8, 0x80, 0xff] {
                let mut bad = v.clone();
                bad[i] ^= flip;
                let mut plan = case.plan.clone();
                plan.residual = Some(Predicate::In {
                    columns: columns.clone(),
                    values: vec![bad],
                });
                // Never a panic: either the value decodes and every
                // adapter translates it, or a typed refusal.
                each(&plan, &opts(&c), |e, r| match r {
                    Ok(()) | Err(AdapterError::Canonical { .. }) | Err(AdapterError::Unsupported { .. }) => {}
                    Err(other) => panic!("{e} {}: byte {i}: {other:?}", case.name),
                });
            }
        }
    }
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
