//! Spark: a JSON manifest that `shims/spark/bellwether_plan.py` reads with
//! `read(spark, manifest_path) -> DataFrame`.
//!
//! File level only: Spark opens the planned files and no other, but its
//! Parquet reader prunes or reads their row groups by its own statistics,
//! and the shim pins none, so Spark's bytes are not the plan's and are not
//! claimed to be. When the plan reads a strict subset of
//! a file's row groups, the shim keeps only those row groups' rows by
//! `_metadata.row_index` ranges from the row counts, so the rows are
//! exactly the plan's. The residual is the typed form of
//! [`crate::residual::Residual::to_json`]; the shim builds every literal
//! from it with Spark's typed constructors, never from SQL text.

use arrow::datatypes::{DataType, TimeUnit};
use bellwether_core::encoding::Scalar;
use bellwether_types::ScanPlan;
use serde_json::{json, Value as Json};

use crate::keys::{self, complete_zero_classes, zero_refusal, KeyEquality};
use crate::layout::{layout, partition_scalar, row_restrictions};
use crate::residual::{compile, scalar_json, value_type, Cond, Residual, Stage};
use crate::{AdapterError, ColumnDef, EmitOptions, Result};

const E: &str = "spark";

/// The manifest's format name; the shim refuses any other.
pub const FORMAT: &str = "bellwether.spark/1";

fn unsupported(what: String, reason: &str) -> AdapterError {
    AdapterError::Unsupported {
        engine: E,
        what,
        reason: reason.to_string(),
        next: "emit for DataFusion or DuckDB".into(),
    }
}

/// The Spark SQL type a Parquet column Arrow wrote for `dt` reads as, in
/// DDL spelling.
fn spark_type(dt: &DataType) -> Result<String> {
    Ok(match value_type(dt) {
        DataType::Boolean => "boolean".into(),
        DataType::Int8 => "tinyint".into(),
        DataType::Int16 | DataType::UInt8 => "smallint".into(),
        DataType::Int32 | DataType::UInt16 => "int".into(),
        DataType::Int64 | DataType::UInt32 => "bigint".into(),
        DataType::UInt64 => "decimal(20,0)".into(),
        DataType::Float32 => "float".into(),
        DataType::Float64 => "double".into(),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => {
            if *p > 38 || *s < 0 {
                return Err(unsupported(
                    format!("a {dt} column"),
                    "has more than 38 digits or a negative scale, which Spark has no type for",
                ));
            }
            format!("decimal({p},{s})")
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string".into(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "binary".into(),
        DataType::Date32 | DataType::Date64 => "date".into(),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            return Err(unsupported(
                format!("a {dt} column"),
                "holds nanoseconds, and Spark's timestamps hold microseconds",
            ))
        }
        DataType::Timestamp(_, None) => "timestamp_ntz".into(),
        DataType::Timestamp(_, Some(_)) => "timestamp".into(),
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            format!("array<{}>", spark_type(f.data_type())?)
        }
        DataType::Map(f, _) => match f.data_type() {
            DataType::Struct(kv) if kv.len() == 2 => format!(
                "map<{},{}>",
                spark_type(kv[0].data_type())?,
                spark_type(kv[1].data_type())?
            ),
            other => return Err(unsupported(format!("a map of {other}"), "has no Spark type")),
        },
        DataType::Struct(fields) => {
            let parts = fields
                .iter()
                .map(|f| Ok(format!("{}:{}", backtick(f.name()), spark_type(f.data_type())?)))
                .collect::<Result<Vec<_>>>()?;
            format!("struct<{}>", parts.join(","))
        }
        other => {
            return Err(unsupported(
                format!("a {other} column"),
                "has no Spark type that reads it exactly",
            ))
        }
    })
}

fn backtick(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn is_float(dt: &DataType) -> bool {
    matches!(
        value_type(dt),
        DataType::Float16 | DataType::Float32 | DataType::Float64
    )
}

/// Refuses what the shim cannot evaluate exactly.
fn check(residual: &Residual) -> Result<()> {
    for s in &residual.stages {
        match s {
            Stage::Filter(Cond::Text { column, .. }) => {
                return Err(AdapterError::Unsupported {
                    engine: E,
                    what: format!("the text match on {column:?}"),
                    reason: "folds case by full Unicode case folding and splits words at UAX #29 \
                             boundaries, which Spark's lower() and string functions do not \
                             reproduce"
                        .into(),
                    next: "emit for DataFusion, which evaluates it with Bellwether's own \
                           definition"
                        .into(),
                })
            }
            Stage::Filter(Cond::ExprIn { expr, .. }) => {
                return Err(AdapterError::Unsupported {
                    engine: E,
                    what: format!("the expression residual {expr}"),
                    reason: "uses Bellwether's expression language, which Spark's functions do \
                             not reproduce bit for bit"
                        .into(),
                    next: "emit for DataFusion, which evaluates it with Bellwether's own \
                           definition"
                        .into(),
                })
            }
            Stage::Filter(Cond::In { columns, raw, .. }) if !complete_zero_classes(raw.iter()) => {
                return Err(zero_refusal(E, &format!("the In key list on {}", columns.join(", "))));
            }
            Stage::Filter(Cond::ContainsAny { column, raw, .. }) if !complete_zero_classes(raw.iter()) => {
                return Err(zero_refusal(E, &format!("the ContainsAny values on list column {column:?}")));
            }
            Stage::Filter(Cond::ContainsAll { column, raw, .. })
                if raw.iter().any(|g| !complete_zero_classes(g.iter())) =>
            {
                return Err(zero_refusal(E, &format!("the ContainsAll values on list column {column:?}")));
            }
            Stage::Filter(Cond::Between {
                column, lo, hi, data_type, ..
            }) if is_float(data_type)
                && [lo, hi]
                    .iter()
                    .any(|s| matches!(s, Scalar::Float(f) if *f == 0.0 || f.is_nan())) =>
            {
                return Err(AdapterError::Unsupported {
                    engine: E,
                    what: format!("the Between range on float column {column:?}, bounded by a zero or NaN"),
                    reason: "orders -0.0 before +0.0 and NaN last in the plan, which Spark's \
                             comparison does not"
                        .into(),
                    next: "emit for DataFusion, which orders floats as the plan's canonical bytes do".into(),
                });
            }
            Stage::Filter(Cond::Live { column, tombstone, .. })
                if matches!(tombstone, Scalar::Float(f) if *f == 0.0) =>
            {
                return Err(zero_refusal(E, &format!("the Live tombstone on column {column:?}")));
            }
            _ => {}
        }
    }
    Ok(())
}

/// The residual JSON with each `NotOlderThan`'s bounds reduced to one per
/// SQL equality class (Spark joins compare `-0.0` equal to `+0.0`).
fn residual_json(residual: &Residual) -> Result<Json> {
    let mut out = residual.to_json();
    for (i, s) in residual.stages.iter().enumerate() {
        if let Stage::NotOlderThan {
            columns,
            types,
            ts_type,
            bounds,
            ..
        } = s
        {
            let kept = keys::sql_bounds(bounds, E, &format!("the NotOlderThan bounds on {}", columns.join(", ")))?;
            out["stages"][i]["bounds"] = Json::Array(
                kept.iter()
                    .map(|b| {
                        json!({
                            "key": b.key.iter().zip(types).map(|(k, t)| k.as_ref().map_or(Json::Null, |k| scalar_json(k, t))).collect::<Vec<_>>(),
                            "ts": scalar_json(&b.ts, ts_type),
                        })
                    })
                    .collect(),
            );
        }
    }
    Ok(out)
}

/// The plan as the Spark shim's manifest (see the module docs).
pub fn manifest(plan: &ScanPlan, schema: &[ColumnDef], opts: &EmitOptions) -> Result<Json> {
    let residual = compile(plan.residual.as_ref(), schema, opts.residual, E)?;
    check(&residual)?;
    let lay = layout(plan, schema, opts)?;
    let restrictions = row_restrictions(&lay, E)?;
    for c in schema {
        if c.name.starts_with("__bellwether_") {
            return Err(unsupported(
                format!("column {:?}", c.name),
                "uses the __bellwether_ prefix the shim's helper columns use",
            ));
        }
    }
    let keys_file = match &opts.keys_file {
        Some(p) => {
            keys::check(p, &residual, KeyEquality::Sql)?;
            Some(p.display().to_string())
        }
        None => None,
    };
    let mut needed: Vec<String> = lay.output.clone();
    for c in residual.columns() {
        if !needed.contains(&c) {
            needed.push(c);
        }
    }
    // The schema the shim reads the files with: the needed stored columns.
    let mut read_schema = Vec::new();
    let mut types = serde_json::Map::new();
    for c in &needed {
        let dt = crate::residual::column_type(schema, c, "the read")?;
        let t = spark_type(dt)?;
        types.insert(c.clone(), Json::String(t.clone()));
        if !lay.partition_columns.contains(c) {
            read_schema.push(format!("{} {t}", backtick(c)));
        }
    }
    let files = lay
        .files
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let partition_values = lay
                .partition_columns
                .iter()
                .zip(&f.partition_values)
                .map(|(c, v)| {
                    let dt = value_type(crate::residual::column_type(schema, c, "a partition column")?).clone();
                    let s = partition_scalar(c, v.as_deref(), &dt)?;
                    Ok((c.clone(), s.map_or(Json::Null, |s| scalar_json(&s, &dt))))
                })
                .collect::<Result<serde_json::Map<_, _>>>()?;
            Ok(json!({
                "path": f.location,
                "plan_path": f.path,
                "row_groups": f.row_groups,
                "rows": restrictions.get(&i).cloned().flatten(),
                "partition_values": partition_values,
                "planned_bytes": f.planned_bytes,
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    let read_order: Vec<Json> = if opts.ordered {
        if lay.interleaved {
            lay.read_order
                .iter()
                .filter_map(|(fi, rg)| {
                    lay.files[*fi]
                        .row_group_range(*rg)
                        .map(|(a, b)| json!({ "file": fi, "rows": [a, b] }))
                })
                .collect()
        } else {
            (0..lay.files.len()).map(|fi| json!({ "file": fi })).collect()
        }
    } else {
        Vec::new()
    };
    let float_keys: Vec<String> = schema
        .iter()
        .filter(|c| is_float(&c.data_type))
        .map(|c| c.name.clone())
        .collect();
    Ok(json!({
        "format": FORMAT,
        "manifest_version": plan.manifest_version,
        "planned": {
            "bytes": plan.bytes,
            "row_groups": plan.row_groups_selected,
            "files": lay.files.len(),
        },
        "realized": "file level: Spark opens only the planned files; it prunes or reads their \
                     row groups by its own statistics, and the shim pins none",
        "files": files,
        "partition_columns": lay.partition_columns,
        "read_schema": read_schema.join(", "),
        "types": types,
        "float_columns": float_keys,
        "columns": lay.output,
        "residual": residual_json(&residual)?,
        "keys_file": keys_file,
        "ordered": opts.ordered,
        "read_order": read_order,
    }))
}
