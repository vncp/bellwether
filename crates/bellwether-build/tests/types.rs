//! Types: keys encode at the dataset schema's types whatever the file's
//! types; a key column whose dataset type changes rebuilds that spec and
//! never mixes encodings; an equality probe for float zero uses both
//! encodings (`docs/format.md`, statistics semantics).

#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::cast_possible_wrap,
    clippy::assert_is_empty,
    clippy::too_many_lines,
    clippy::type_complexity,
    clippy::cast_precision_loss,
    clippy::redundant_closure_for_method_calls,
    clippy::format_push_string
)]

mod common;

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Decimal128Array, Float64Array, Int64Array, RecordBatch, TimestampMicrosecondArray,
    TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_build::{completeness, CompactOptions, Lookup};
use bellwether_core::segment::ColumnType;
use bellwether_core::SegmentReader;
use bellwether_types::{Predicate, Store};
use common::*;

fn float_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "val",
        DataType::Float64,
        true,
    )]))
}

#[test]
fn a_zero_probe_finds_both_zeros() {
    let s = Scratch::new();
    let path = s.path("z.parquet");
    let batch = RecordBatch::try_new(
        float_schema(),
        vec![Arc::new(Float64Array::from(vec![-0.0, 0.0, 1.0, -2.0]))],
    )
    .unwrap();
    write_parquet(&path, &batch, 1);
    let store = memory_store();
    build_ok(
        &builder(&store),
        &s.root_str(),
        &[exact(&["val"]), range("val")],
        &parquet_opts(&float_schema()),
    );
    let rows = parquet_rows(&path, &float_schema());
    let p = planner(&store);
    let val = cols(&["val"]);
    for probe in [0.0, -0.0] {
        for spec in [exact(&["val"]), range("val")] {
            let lookup = Lookup::Keys {
                columns: val.clone(),
                keys: vec![enc_f64(probe)],
            };
            let plan = p.plan(std::slice::from_ref(&spec), &lookup).unwrap();
            assert_eq!(
                plan_rgs(&plan).iter().map(|x| x.1).collect::<Vec<_>>(),
                vec![0, 1],
                "{probe} via {}",
                spec.name()
            );
            let Some(Predicate::In { values, .. }) = &plan.residual else {
                panic!()
            };
            assert_eq!(values.len(), 2, "both encodings in the residual");
            let got = apply_residual(plan.residual.as_ref().unwrap(), planned_rows(&plan, &rows));
            assert_eq!(got.len(), 2);
        }
    }
    // a range starting at +0.0 includes -0.0, and one ending at -0.0
    // includes +0.0
    for (lo, hi) in [(0.0, 0.5), (-1.0, -0.0), (0.0, 0.0)] {
        let lookup = Lookup::Between {
            column: "val".into(),
            lo: enc_f64(lo),
            hi: enc_f64(hi),
        };
        for spec in [exact(&["val"]), range("val")] {
            let plan = p.plan(std::slice::from_ref(&spec), &lookup).unwrap();
            assert!(
                plan_rgs(&plan)
                    .iter()
                    .map(|x| x.1)
                    .collect::<Vec<_>>()
                    .starts_with(&[0, 1]),
                "[{lo}, {hi}] via {}",
                spec.name()
            );
            let got = apply_residual(plan.residual.as_ref().unwrap(), planned_rows(&plan, &rows));
            assert_eq!(got.len(), 2, "[{lo}, {hi}] via {}", spec.name());
        }
    }
}

fn amounts(scale_schema: &SchemaRef, values: &[i128], precision: u8, scale: i8) -> RecordBatch {
    RecordBatch::try_new(
        Arc::clone(scale_schema),
        vec![
            Arc::new(Int64Array::from(
                (0..values.len() as i64).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(
                Decimal128Array::from(values.to_vec())
                    .with_precision_and_scale(precision, scale)
                    .unwrap(),
            ),
        ],
    )
    .unwrap()
}

fn amount_schema(precision: u8, scale: i8) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("amount", DataType::Decimal128(precision, scale), true),
    ]))
}

fn recorded_types(
    store: &bellwether_store::ObjectStoreBackend,
    name: &str,
) -> Vec<Vec<ColumnType>> {
    let m = bellwether_build::Builder::new(Arc::new(store.clone()), "idx")
        .manifest()
        .unwrap()
        .unwrap();
    m.segments
        .iter()
        .filter(|x| x.key_spec == name)
        .map(|x| {
            let bytes = store
                .get(&bellwether_store::segment_path("idx", &x.id))
                .unwrap()
                .0;
            SegmentReader::open(bytes)
                .unwrap()
                .header()
                .key_types
                .clone()
                .unwrap()
        })
        .collect()
}

#[test]
fn a_key_type_change_rebuilds_that_spec_and_never_mixes_encodings() {
    let s = Scratch::new();
    let file_schema = amount_schema(10, 2);
    for (f, vals) in [
        ("a.parquet", vec![150, 299, -5]),
        ("b.parquet", vec![150, 7, 1_000]),
    ] {
        write_parquet(&s.path(f), &amounts(&file_schema, &vals, 10, 2), 2);
    }
    let store = memory_store();
    let b = builder(&store);
    let specs = [exact(&["amount"]), exact(&["id"])];
    build_ok(&b, &s.root_str(), &specs, &parquet_opts(&file_schema));
    assert_eq!(
        recorded_types(&store, "exact:amount")[0],
        vec![ColumnType::Decimal128 {
            precision: 10,
            scale: 2
        }]
    );

    // the dataset now declares amount at scale 4: the amount spec is
    // rebuilt over every row group, the id spec is untouched
    let wide = amount_schema(20, 4);
    let two = bellwether_build::BuildOptions {
        max_segment_row_groups: 2,
        ..parquet_opts(&wide)
    };
    let r = build_ok(&b, &s.root_str(), &specs, &two);
    assert_eq!(r.specs_rebuilt, vec!["exact:amount".to_string()]);
    assert_eq!(r.row_groups_read, 4);
    let types = recorded_types(&store, "exact:amount");
    assert!(!types.is_empty());
    assert!(
        types.iter().all(|t| t
            == &vec![ColumnType::Decimal128 {
                precision: 20,
                scale: 4
            }]),
        "no segment of the old encoding is live: {types:?}"
    );
    let m = b.manifest().unwrap().unwrap();
    assert_eq!(completeness(&m, &exact(&["amount"])), Some(2));
    assert_eq!(completeness(&m, &exact(&["id"])), Some(1));
    // 1.50 at the new scale is 15000
    let rows: Vec<Row> = ["a.parquet", "b.parquet"]
        .iter()
        .flat_map(|f| parquet_rows(&s.path(f), &wide))
        .collect();
    let p = planner(&store);
    let key = enc(
        &bellwether_core::encoding::Value::Decimal128 {
            value: 15_000,
            scale: 4,
        },
        &DataType::Decimal128(20, 4),
    );
    let plan = check_keys(&p, &rows, &[exact(&["amount"])], &cols(&["amount"]), &[key]);
    assert_eq!(plan.entries.len(), 2);
    // compaction merges the rebuilt segments cleanly
    let c = b.compact(&CompactOptions::default()).unwrap();
    assert_eq!(c.merges, 1);
    assert_eq!(recorded_types(&store, "exact:amount").len(), 1);

    // with exact:id removed, `id` is no longer a key column, and its type
    // changing rebuilds nothing
    assert!(b.remove_spec(&exact(&["id"])).unwrap().is_some());
    let narrower_id = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("amount", DataType::Decimal128(20, 4), true),
    ]));
    let r = build_ok(
        &b,
        &s.root_str(),
        &[exact(&["amount"])],
        &parquet_opts(&narrower_id),
    );
    assert!(r.specs_rebuilt.is_empty());
    assert!(r.specs_removed.is_empty());
}

#[test]
fn files_at_other_scales_and_units_encode_as_the_dataset_type() {
    let s = Scratch::new();
    // a.parquet: amount Decimal(10, 2) = 1.50, ts in milliseconds
    let a = RecordBatch::try_from_iter(vec![
        (
            "amount",
            Arc::new(
                Decimal128Array::from(vec![150])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ) as ArrayRef,
        ),
        ("ts", Arc::new(TimestampMillisecondArray::from(vec![1_000]))),
    ])
    .unwrap();
    // b.parquet: amount Decimal(12, 3) = 1.500, ts in microseconds
    let b_batch = RecordBatch::try_from_iter(vec![
        (
            "amount",
            Arc::new(
                Decimal128Array::from(vec![1_500])
                    .with_precision_and_scale(12, 3)
                    .unwrap(),
            ) as ArrayRef,
        ),
        (
            "ts",
            Arc::new(TimestampMicrosecondArray::from(vec![1_000_000])),
        ),
    ])
    .unwrap();
    write_parquet(&s.path("a.parquet"), &a, 10);
    write_parquet(&s.path("b.parquet"), &b_batch, 10);
    let dataset = Arc::new(Schema::new(vec![
        Field::new("amount", DataType::Decimal128(20, 3), true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
    ]));
    let store = memory_store();
    build_ok(
        &builder(&store),
        &s.root_str(),
        &[exact(&["amount"]), exact(&["ts"])],
        &parquet_opts(&dataset),
    );
    let p = planner(&store);
    let amount = enc(
        &bellwether_core::encoding::Value::Decimal128 {
            value: 1_500,
            scale: 3,
        },
        &DataType::Decimal128(20, 3),
    );
    let lookup = Lookup::Keys {
        columns: cols(&["amount"]),
        keys: vec![amount],
    };
    assert_eq!(
        p.plan(&[exact(&["amount"])], &lookup)
            .unwrap()
            .entries
            .len(),
        2
    );
    let ts = enc(
        &bellwether_core::encoding::Value::Timestamp {
            value: 1_000_000,
            unit: TimeUnit::Microsecond,
        },
        &DataType::Timestamp(TimeUnit::Microsecond, None),
    );
    let lookup = Lookup::Keys {
        columns: cols(&["ts"]),
        keys: vec![ts],
    };
    assert_eq!(p.plan(&[exact(&["ts"])], &lookup).unwrap().entries.len(), 2);
    // a probe encoded at the file's millisecond unit is refused, not a miss
    let ms = enc(
        &bellwether_core::encoding::Value::Timestamp {
            value: 1_000,
            unit: TimeUnit::Millisecond,
        },
        &DataType::Timestamp(TimeUnit::Millisecond, None),
    );
    let lookup = Lookup::Keys {
        columns: cols(&["ts"]),
        keys: vec![ms],
    };
    assert!(p.plan(&[exact(&["ts"])], &lookup).is_err());
    // keys can be encoded from a batch at the dataset types by the planner
    let keys = p.encode_keys(&cols(&["ts"]), &a).unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(
        p.plan(
            &[exact(&["ts"])],
            &Lookup::Keys {
                columns: cols(&["ts"]),
                keys
            }
        )
        .unwrap()
        .entries
        .len(),
        2
    );
}

#[test]
fn a_value_not_representable_in_the_dataset_type_fails_the_build() {
    let s = Scratch::new();
    // 1.234 cannot be represented at the dataset's scale 2: an error, never
    // a rounded key
    let a = RecordBatch::try_from_iter(vec![(
        "amount",
        Arc::new(
            Decimal128Array::from(vec![1_234])
                .with_precision_and_scale(10, 3)
                .unwrap(),
        ) as ArrayRef,
    )])
    .unwrap();
    write_parquet(&s.path("a.parquet"), &a, 10);
    let dataset = Arc::new(Schema::new(vec![Field::new(
        "amount",
        DataType::Decimal128(10, 2),
        true,
    )]));
    let store = memory_store();
    let b = builder(&store);
    assert!(b
        .build(
            &s.root_str(),
            &[exact(&["amount"])],
            &parquet_opts(&dataset)
        )
        .is_err());
    assert!(b.manifest().unwrap().is_none(), "nothing was committed");
}
