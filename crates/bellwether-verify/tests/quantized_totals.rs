//! Float totals by explicit quantization (`col:dec(S)`): each value's exact
//! binary value rounded half to even at scale `S`, summed as an `i128`.
//!
//! The oracle is a decimal copy of the same column. A float written as
//! `k / 10^S` (with `|k|` well below 2^53) is the nearest double to that
//! decimal, so quantizing it at `S` gives back `k`, and the quantized float
//! totals must equal the decimal column's exact totals in every group,
//! under `latest_by`, and at any pass count.

#![allow(clippy::unwrap_used, missing_docs, clippy::cast_precision_loss)]

mod common;

use std::sync::Arc;

use arrow::array::{ArrayRef, Decimal128Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether_verify::{
    GroupTotal, Mismatch, Options, ReconcileSpec, TotalColumn, VerifyError, MAX_QUANTIZE_SCALE,
};
use common::{dirs, reconcile_with, write_batch};

/// `(id, ts, grp, cents)`.
type Row = (i64, i64, &'static str, Option<i64>);

/// `(id, ts, grp, price, price_dec)` with `price = cents / 100` and
/// `price_dec` the same cents at scale 2. `None` cents is a null in both.
fn batch(rows: &[Row]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("grp", DataType::Utf8, false),
        Field::new("price", DataType::Float64, true),
        Field::new("price_dec", DataType::Decimal128(18, 2), true),
    ]));
    let col = |f: fn(&Row) -> i64| {
        Arc::new(Int64Array::from(rows.iter().map(f).collect::<Vec<_>>())) as ArrayRef
    };
    RecordBatch::try_new(
        schema,
        vec![
            col(|r| r.0),
            col(|r| r.1),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter()
                    .map(|r| r.3.map(|c| c as f64 / 100.0))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(
                Decimal128Array::from(rows.iter().map(|r| r.3.map(i128::from)).collect::<Vec<_>>())
                    .with_precision_and_scale(18, 2)
                    .unwrap(),
            ),
        ],
    )
    .unwrap()
}

/// Totals of `column` keyed by group values, rendered left and right.
fn by_group(totals: &[GroupTotal], column: &str) -> Vec<(String, String, String)> {
    totals
        .iter()
        .filter(|t| t.column == column)
        .map(|t| {
            (
                format!("{:?}", t.group.values),
                t.left.render(),
                t.right.render(),
            )
        })
        .collect()
}

#[test]
fn quantized_float_totals_equal_the_decimal_copy_in_every_group_and_at_any_pass_count() {
    let (l, r) = dirs();
    let groups = ["a", "b", "c"];
    let mut left = Vec::new();
    for id in 0..2_000i64 {
        for version in 0..2 {
            let cents = (id * 7_919 + version * 13) % 2_000_003 - 1_000_000;
            let cents = (id % 11 != 0).then_some(cents);
            left.push((id, version, groups[usize::try_from(id % 3).unwrap()], cents));
        }
    }
    let mut right = left.clone();
    // One cent more for one key's newest version, and its group differs.
    right[2 * 500 + 1].3 = right[2 * 500 + 1].3.map(|c| c + 1);
    write_batch(l.path(), "a.parquet", &batch(&left), 300);
    write_batch(r.path(), "a.parquet", &batch(&right), 700);
    for latest in [false, true] {
        let mut spec = ReconcileSpec::new(["id"])
            .totals(["price_dec"])
            .with_totals([TotalColumn::quantized("price", 2)])
            .group_by(["grp"])
            .ignore(["price", "price_dec"]);
        if latest {
            spec = spec.latest_by("ts");
        }
        let opts = |memory_budget| Options {
            memory_budget,
            max_examples: 100,
        };
        let one = reconcile_with(l.path(), r.path(), spec.clone(), opts(1 << 30)).unwrap();
        assert_eq!(one.memory.passes, 1);
        let quantized = by_group(&one.totals, "price:dec(2)");
        assert_eq!(quantized.len(), 3);
        assert_eq!(quantized, by_group(&one.totals, "price_dec"));
        // The differing group is reported under the quantized label too.
        let differ: Vec<&str> = one
            .mismatches
            .iter()
            .filter_map(|m| match m {
                Mismatch::TotalDiffers { column, .. } => Some(column.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(differ, vec!["price_dec", "price:dec(2)"], "latest {latest}");
        let many = reconcile_with(l.path(), r.path(), spec, opts(64 * 1024)).unwrap();
        assert!(many.memory.passes > 1, "{:?}", many.memory);
        assert_eq!(many.totals, one.totals);
    }
}

#[test]
fn quantized_totals_round_each_value_half_to_even_before_summing() {
    let (l, r) = dirs();
    // 0.125 → 0.12, 0.375 → 0.38, 2.675 (below the tie) → 2.67: 3.17,
    // where rounding the float sum would give 3.18.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("x", DataType::Float64, true),
    ]));
    let b = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Float64Array::from(vec![0.125, 0.375, 2.675])),
        ],
    )
    .unwrap();
    write_batch(l.path(), "a.parquet", &b, 10);
    write_batch(r.path(), "a.parquet", &b, 10);
    let spec = ReconcileSpec::new(["id"]).with_totals([TotalColumn::quantized("x", 2)]);
    let o = reconcile_with(l.path(), r.path(), spec, Options::default()).unwrap();
    assert!(o.pass);
    assert_eq!(o.totals[0].left.render(), "3.17");
    assert_eq!(o.totals[0].column, "x:dec(2)");
}

#[test]
fn a_nan_or_infinite_value_is_an_error_naming_its_key() {
    for bad in [f64::NAN, f64::INFINITY, 1e300] {
        let (l, r) = dirs();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("x", DataType::Float64, true),
        ]));
        let b = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 42])),
                Arc::new(Float64Array::from(vec![1.0, bad])),
            ],
        )
        .unwrap();
        write_batch(l.path(), "a.parquet", &b, 10);
        write_batch(r.path(), "a.parquet", &b, 10);
        let spec = ReconcileSpec::new(["id"]).with_totals([TotalColumn::quantized("x", 4)]);
        let e = reconcile_with(l.path(), r.path(), spec, Options::default()).unwrap_err();
        let VerifyError::Quantize { key, total, .. } = &e else {
            panic!("{e}")
        };
        assert_eq!(
            (key.as_str(), total.as_str()),
            ("(id=42)", "x:dec(4)"),
            "{e}"
        );
    }
}

#[test]
fn a_bare_float_or_a_quantized_decimal_is_refused() {
    let (l, r) = dirs();
    write_batch(l.path(), "a.parquet", &batch(&[(1, 1, "a", Some(5))]), 10);
    write_batch(r.path(), "a.parquet", &batch(&[(1, 1, "a", Some(5))]), 10);
    let run = |spec: ReconcileSpec| {
        reconcile_with(l.path(), r.path(), spec, Options::default())
            .unwrap_err()
            .to_string()
    };
    let bare = run(ReconcileSpec::new(["id"]).totals(["price"]));
    assert!(bare.contains("price:dec(4)"), "{bare}");
    let dec = run(ReconcileSpec::new(["id"]).with_totals([TotalColumn::quantized("price_dec", 2)]));
    assert!(dec.contains("drop :dec(..)"), "{dec}");
    let wide = run(ReconcileSpec::new(["id"])
        .with_totals([TotalColumn::quantized("price", MAX_QUANTIZE_SCALE + 1)]));
    assert!(wide.contains("at most 38"), "{wide}");
}

#[test]
fn total_specs_parse_and_print_the_same_spelling() {
    assert_eq!(
        TotalColumn::parse("volume:dec(4)"),
        Ok(TotalColumn::quantized("volume", 4))
    );
    assert_eq!(
        TotalColumn::parse("amount"),
        Ok(TotalColumn::decimal("amount"))
    );
    assert_eq!(
        TotalColumn::parse("a:b:dec(0)"),
        Ok(TotalColumn::quantized("a:b", 0))
    );
    assert_eq!(TotalColumn::quantized("volume", 4).label(), "volume:dec(4)");
    for bad in [
        "v:dec(",
        "v:dec()",
        "v:dec(-1)",
        "v:dec(39)",
        ":dec(2)",
        "v:dec(2)x",
        "",
    ] {
        assert!(TotalColumn::parse(bad).is_err(), "{bad}");
    }
}
