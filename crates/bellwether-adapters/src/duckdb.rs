//! DuckDB: one SQL statement over `read_parquet` of the planned files.
//!
//! DuckDB cannot be told which row groups to read. When the plan reads a
//! strict subset of a file's row groups, the statement keeps only those
//! row groups' rows by `file_row_number` ranges taken from the row counts
//! ([`crate::EmitOptions::row_offsets`]); DuckDB may or may not use that
//! filter to skip row groups, so the bytes DuckDB reads are measured, never
//! assumed (`tests/duckdb.rs` counts them at an HTTP range server).
//!
//! Shape (each stage is one common table expression, in the residual's
//! order):
//!
//! ```sql
//! WITH bw_files(...) AS (VALUES ...)             -- partition values, typed
//!    , bw_s0 AS (SELECT t."a", f."p", t.filename AS "__bellwether_file", ...
//!                FROM read_parquet([...], union_by_name = true, hive_partitioning = false) AS t
//!                JOIN bw_files AS f ON t.filename = f."__bellwether_file"
//!                WHERE (t.filename <> '...' OR t.file_row_number BETWEEN 0 AND 99))
//!    , bw_s1 AS (SELECT * FROM bw_s0 WHERE "k" IN (...))                  -- In
//!    , bw_s2 AS (SELECT s.* FROM bw_s1 AS s LEFT JOIN bw_b2 AS b ON ...
//!                WHERE b."__bellwether_bound" IS NULL OR s."ts" >= b."__bellwether_bound")
//!    , bw_s3 AS (SELECT * FROM bw_s2 WHERE "k" IS NOT NULL AND "ts" IS NOT NULL
//!                QUALIFY rank() OVER (PARTITION BY "k" ORDER BY "ts" DESC) = 1)
//!    , bw_s4 AS (SELECT * FROM bw_s3 WHERE "void" IS NOT NULL AND "void" <> TRUE)
//! SELECT "a", "p" FROM bw_s4
//! ```
//!
//! `rank() = 1`, never `row_number() = 1`: every row at a key's newest
//! timestamp is kept. A float key column is also partitioned by its sign
//! bit, because DuckDB groups `-0.0` with `+0.0` and the plan does not.
//! Partition columns come from the index's own reading of each path, typed
//! by the exact cast the source applies, not from DuckDB's Hive inference
//! (which types `007` as a number and cannot read path templates).

use std::fmt::Write as _;

use arrow::datatypes::DataType;
use bellwether_core::encoding::Scalar;
use bellwether_types::ScanPlan;

use crate::keys::{self, complete_zero_classes, zero_refusal, KeyEquality};
use crate::layout::{layout, partition_scalar, row_restrictions};
use crate::residual::{compile, value_type, Cond, Stage};
use crate::sql::{duckdb_literal, duckdb_type, ident, string};
use crate::{AdapterError, ColumnDef, EmitOptions, Result};

const E: &str = "duckdb";
const FILE: &str = "\"__bellwether_file\"";
const ROW: &str = "\"__bellwether_row\"";
const BOUND: &str = "\"__bellwether_bound\"";

/// An inline `IN` list holds at most this many values; a longer list (or
/// a composite key) is a `VALUES` relation semi-joined on the key.
pub const INLINE_IN_MAX: usize = 1024;

fn is_float(dt: &DataType) -> bool {
    matches!(
        value_type(dt),
        DataType::Float16 | DataType::Float32 | DataType::Float64
    )
}

/// The DuckDB type of any dataset type, nested ones included (for the
/// typed empty result of a plan that reads nothing).
fn column_type_name(dt: &DataType) -> Result<String> {
    Ok(match value_type(dt) {
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::FixedSizeList(f, _) => format!("{}[]", column_type_name(f.data_type())?),
        DataType::Map(f, _) => match f.data_type() {
            DataType::Struct(kv) if kv.len() == 2 => format!(
                "MAP({}, {})",
                column_type_name(kv[0].data_type())?,
                column_type_name(kv[1].data_type())?
            ),
            other => return Err(AdapterError::Plan(format!("a map of {other}"))),
        },
        DataType::Struct(fields) => {
            let parts = fields
                .iter()
                .map(|f| Ok(format!("{} {}", ident(f.name(), E)?, column_type_name(f.data_type())?)))
                .collect::<Result<Vec<_>>>()?;
            format!("STRUCT({})", parts.join(", "))
        }
        other => duckdb_type(other)?,
    })
}

fn literal_or_null(s: Option<&Scalar>, dt: &DataType) -> Result<String> {
    match s {
        Some(s) => duckdb_literal(s, dt),
        None => Ok(format!("CAST(NULL AS {})", duckdb_type(dt)?)),
    }
}

/// `VALUES` rows of typed literals, one `(...)` per row.
fn values_rows<'a>(
    rows: impl Iterator<Item = Vec<(Option<&'a Scalar>, &'a DataType)>>,
) -> Result<String> {
    let mut out = Vec::new();
    for row in rows {
        let cells = row
            .into_iter()
            .map(|(s, t)| literal_or_null(s, t))
            .collect::<Result<Vec<_>>>()?;
        out.push(format!("({})", cells.join(", ")));
    }
    Ok(out.join(", "))
}

/// The partition key of `columns` under SQL grouping: a float column is
/// split by its sign bit too.
fn partition_keys(columns: &[String], schema: &[ColumnDef]) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for c in columns {
        let q = ident(c, E)?;
        let dt = crate::residual::column_type(schema, c, "LatestPerKey")?;
        if is_float(dt) {
            out.push(format!("{q}, signbit({q})"));
        } else {
            out.push(q);
        }
    }
    Ok(out)
}

/// A row test as a SQL boolean over the stage's input (`None` when the
/// test needs a relation instead: a long or composite `In`).
fn cond_sql(c: &Cond) -> Result<Option<String>> {
    Ok(Some(match c {
        Cond::In {
            columns,
            types,
            rows,
            raw,
        } => {
            if !complete_zero_classes(raw.iter()) {
                return Err(zero_refusal(E, "the residual's key list"));
            }
            if rows.is_empty() {
                return Ok(Some("FALSE".into()));
            }
            if columns.len() > 1 || rows.len() > INLINE_IN_MAX {
                return Ok(None);
            }
            let lits = rows
                .iter()
                .map(|r| duckdb_literal(&r[0], &types[0]))
                .collect::<Result<Vec<_>>>()?;
            format!("{} IN ({})", ident(&columns[0], E)?, lits.join(", "))
        }
        Cond::Between {
            column,
            data_type,
            lo,
            hi,
        } => {
            let zero_or_nan = |s: &Scalar| matches!(s, Scalar::Float(f) if *f == 0.0 || f.is_nan());
            if zero_or_nan(lo) || zero_or_nan(hi) {
                return Err(AdapterError::Unsupported {
                    engine: E,
                    what: format!("a range on float column {column:?} bounded by a zero or NaN"),
                    reason: "orders -0.0 before +0.0 and NaN last in the plan, which SQL \
                             comparison does not"
                        .into(),
                    next: "emit for DataFusion".into(),
                });
            }
            format!(
                "{} BETWEEN {} AND {}",
                ident(column, E)?,
                duckdb_literal(lo, data_type)?,
                duckdb_literal(hi, data_type)?
            )
        }
        Cond::Text { column, .. } => {
            return Err(AdapterError::Unsupported {
                engine: E,
                what: format!("the text match on {column:?}"),
                reason: "folds case by full Unicode case folding and splits words at UAX #29 \
                         boundaries, which DuckDB's lower() and string functions do not \
                         reproduce"
                    .into(),
                next: "emit for DataFusion, which evaluates it with Bellwether's own definition, \
                       or emit with --residual keys-only and filter the text yourself"
                    .into(),
            })
        }
        Cond::ExprIn { expr, .. } => {
            return Err(AdapterError::Unsupported {
                engine: E,
                what: format!("the expression residual {expr}"),
                reason: "uses Bellwether's expression language (its json_get, lower, and hash \
                         are defined bit for bit), which DuckDB's functions do not reproduce"
                    .into(),
                next: "emit for DataFusion, which evaluates it with Bellwether's own definition"
                    .into(),
            })
        }
        Cond::ContainsAny {
            column,
            element,
            map,
            values,
            raw,
        } => {
            if !complete_zero_classes(raw.iter()) {
                return Err(zero_refusal(E, "the residual's list membership"));
            }
            if values.is_empty() {
                return Ok(Some("FALSE".into()));
            }
            let lits = values
                .iter()
                .map(|v| duckdb_literal(v, element))
                .collect::<Result<Vec<_>>>()?;
            format!(
                "list_has_any({}, [{}])",
                list_expr(column, *map)?,
                lits.join(", ")
            )
        }
        Cond::ContainsAll {
            column,
            element,
            map,
            groups,
            raw,
        } => {
            for g in raw {
                if !complete_zero_classes(g.iter()) {
                    return Err(zero_refusal(E, "the residual's list membership"));
                }
            }
            let list = list_expr(column, *map)?;
            let mut parts = vec![format!("{} IS NOT NULL", ident(column, E)?)];
            for g in groups {
                let lits = g
                    .iter()
                    .map(|v| duckdb_literal(v, element))
                    .collect::<Result<Vec<_>>>()?;
                parts.push(format!("list_has_any({list}, [{}])", lits.join(", ")));
            }
            format!("({})", parts.join(" AND "))
        }
        Cond::Live {
            column,
            data_type,
            tombstone,
        } => {
            if matches!(tombstone, Scalar::Float(f) if *f == 0.0) {
                return Err(zero_refusal(E, "the tombstone value"));
            }
            let q = ident(column, E)?;
            format!(
                "({q} IS NOT NULL AND {q} <> {})",
                duckdb_literal(tombstone, data_type)?
            )
        }
    }))
}

fn list_expr(column: &str, map: bool) -> Result<String> {
    let q = ident(column, E)?;
    Ok(if map { format!("map_keys({q})") } else { q })
}

/// Refuses dataset columns the statement's helper names would shadow.
fn check_names(schema: &[ColumnDef], need_virtual: bool) -> Result<()> {
    for c in schema {
        if c.name.starts_with("__bellwether_") {
            return Err(AdapterError::Unsupported {
                engine: E,
                what: format!("column {:?}", c.name),
                reason: "uses the __bellwether_ prefix the statement's helper columns use".into(),
                next: "rename the column".into(),
            });
        }
        if need_virtual && (c.name == "filename" || c.name == "file_row_number") {
            return Err(AdapterError::Unsupported {
                engine: E,
                what: format!("column {:?}", c.name),
                reason: "shadows the read_parquet virtual column this plan needs to restrict \
                         rows to the planned row groups (or to order them, or to attach \
                         partition values)"
                    .into(),
                next: "emit for DataFusion, which pins row groups directly".into(),
            });
        }
    }
    Ok(())
}

/// The plan as one DuckDB SQL statement (see the module docs).
#[allow(clippy::too_many_lines)]
pub fn sql(plan: &ScanPlan, schema: &[ColumnDef], opts: &EmitOptions) -> Result<String> {
    let residual = compile(plan.residual.as_ref(), schema, opts.residual, E)?;
    let lay = layout(plan, schema, opts)?;
    let restrictions = row_restrictions(&lay, E)?;
    let restricted = restrictions.values().any(Option::is_some);
    let partitioned = !lay.partition_columns.is_empty();
    let need_file = partitioned || restricted || opts.ordered;
    check_names(schema, need_file)?;
    let keys_file = match &opts.keys_file {
        Some(p) => Some((p, keys::check(p, &residual, KeyEquality::Sql)?)),
        None => None,
    };
    let mut out = String::new();
    let _ = writeln!(
        out,
        "-- bellwether plan at manifest {}: {} row groups in {} files, {} bytes planned; \
         residual {}",
        plan.manifest_version.replace(['\n', '\r'], " "),
        plan.row_groups_selected,
        lay.files.len(),
        plan.bytes,
        residual.mode.as_str()
    );
    if lay.files.is_empty() {
        let cols = lay
            .output
            .iter()
            .map(|c| {
                let dt = crate::residual::column_type(schema, c, "the output")?;
                Ok(format!("CAST(NULL AS {}) AS {}", column_type_name(dt)?, ident(c, E)?))
            })
            .collect::<Result<Vec<_>>>()?;
        let _ = write!(out, "SELECT {} WHERE FALSE", cols.join(", "));
        return Ok(out);
    }
    // Columns the scan reads: the output, then whatever the residual needs.
    let mut needed: Vec<String> = lay.output.clone();
    for c in residual.columns() {
        if !needed.contains(&c) {
            needed.push(c);
        }
    }
    let mut ctes: Vec<String> = Vec::new();
    if partitioned {
        let types = lay
            .partition_columns
            .iter()
            .map(|c| crate::residual::column_type(schema, c, "a partition column").map(|t| value_type(t).clone()))
            .collect::<Result<Vec<_>>>()?;
        let mut rows = Vec::with_capacity(lay.files.len());
        for f in &lay.files {
            let mut cells = vec![string(&f.location)];
            for ((c, v), t) in lay.partition_columns.iter().zip(&f.partition_values).zip(&types) {
                let s = partition_scalar(c, v.as_deref(), t)?;
                cells.push(literal_or_null(s.as_ref(), t)?);
            }
            rows.push(format!("({})", cells.join(", ")));
        }
        let names = std::iter::once(Ok(FILE.to_string()))
            .chain(lay.partition_columns.iter().map(|c| ident(c, E)))
            .collect::<Result<Vec<_>>>()?;
        ctes.push(format!(
            "bw_files AS (SELECT * FROM (VALUES {}) AS v({}))",
            rows.join(", "),
            names.join(", ")
        ));
    }
    // The scan.
    let mut select = Vec::new();
    for c in &needed {
        let q = ident(c, E)?;
        if lay.partition_columns.contains(c) {
            select.push(format!("f.{q}"));
        } else {
            select.push(format!("t.{q}"));
        }
    }
    if need_file {
        select.push(format!("t.filename AS {FILE}"));
        select.push(format!("t.file_row_number AS {ROW}"));
    }
    let files = lay
        .files
        .iter()
        .map(|f| string(&f.location))
        .collect::<Vec<_>>()
        .join(", ");
    let mut scan = format!(
        "bw_s0 AS (SELECT {} FROM read_parquet([{files}], union_by_name = true, \
         hive_partitioning = false) AS t",
        select.join(", ")
    );
    if partitioned {
        let _ = write!(scan, " JOIN bw_files AS f ON t.filename = f.{FILE}");
    }
    let mut wheres = Vec::new();
    for (i, r) in &restrictions {
        if let Some(ranges) = r {
            let f = &lay.files[*i];
            let ranges = if ranges.is_empty() {
                "FALSE".to_string()
            } else {
                ranges
                    .iter()
                    .map(|(a, b)| format!("t.file_row_number BETWEEN {a} AND {b}"))
                    .collect::<Vec<_>>()
                    .join(" OR ")
            };
            wheres.push(format!("(t.filename <> {} OR {ranges})", string(&f.location)));
        }
    }
    if !wheres.is_empty() {
        let _ = write!(scan, " WHERE {}", wheres.join(" AND "));
    }
    scan.push(')');
    ctes.push(scan);
    // The residual, one stage per relation.
    let mut prev = "bw_s0".to_string();
    for (n, stage) in residual.stages.iter().enumerate() {
        let me = format!("bw_s{}", n + 1);
        let body = match stage {
            Stage::Filter(c) => match cond_sql(c)? {
                Some(test) => format!("SELECT * FROM {prev} WHERE {test}"),
                None => {
                    let Cond::In {
                        columns,
                        types,
                        rows,
                        ..
                    } = c
                    else {
                        unreachable!("only In needs a relation")
                    };
                    let keys_rel = match &keys_file {
                        Some((path, _)) => format!(
                            "(SELECT {} FROM read_parquet({}))",
                            columns
                                .iter()
                                .map(|c| ident(c, E))
                                .collect::<Result<Vec<_>>>()?
                                .join(", "),
                            string(&path.display().to_string())
                        ),
                        None => {
                            let kname = format!("bw_k{}", n + 1);
                            let names = columns
                                .iter()
                                .map(|c| ident(c, E))
                                .collect::<Result<Vec<_>>>()?;
                            let vals = values_rows(rows.iter().map(|r| {
                                r.iter().map(Some).zip(types.iter()).collect::<Vec<_>>()
                            }))?;
                            ctes.push(format!(
                                "{kname} AS (SELECT * FROM (VALUES {vals}) AS v({}))",
                                names.join(", ")
                            ));
                            kname
                        }
                    };
                    let on = columns
                        .iter()
                        .map(|c| {
                            let q = ident(c, E)?;
                            Ok(format!("s.{q} = k.{q}"))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    format!(
                        "SELECT s.* FROM {prev} AS s SEMI JOIN {keys_rel} AS k ON {}",
                        on.join(" AND ")
                    )
                }
            },
            Stage::NotOlderThan {
                columns,
                types,
                ts_column,
                ts_type,
                bounds,
            } => {
                let bounds = keys::sql_bounds(bounds, E)?;
                if bounds.is_empty() {
                    format!("SELECT * FROM {prev}")
                } else {
                    let bname = format!("bw_b{}", n + 1);
                    let mut names = columns
                        .iter()
                        .map(|c| ident(c, E))
                        .collect::<Result<Vec<_>>>()?;
                    names.push(BOUND.to_string());
                    let vals = values_rows(bounds.iter().map(|b| {
                        let mut row: Vec<(Option<&Scalar>, &DataType)> =
                            b.key.iter().map(Option::as_ref).zip(types.iter()).collect();
                        row.push((Some(&b.ts), ts_type));
                        row
                    }))?;
                    ctes.push(format!(
                        "{bname} AS (SELECT * FROM (VALUES {vals}) AS v({}))",
                        names.join(", ")
                    ));
                    let on = columns
                        .iter()
                        .map(|c| {
                            let q = ident(c, E)?;
                            Ok(format!("s.{q} = b.{q}"))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    format!(
                        "SELECT s.* FROM {prev} AS s LEFT JOIN {bname} AS b ON {} WHERE \
                         b.{BOUND} IS NULL OR s.{} >= b.{BOUND}",
                        on.join(" AND "),
                        ident(ts_column, E)?
                    )
                }
            }
            Stage::LatestPerKey { columns, ts_column } => {
                let ts = ident(ts_column, E)?;
                let mut not_null = columns
                    .iter()
                    .map(|c| Ok(format!("{} IS NOT NULL", ident(c, E)?)))
                    .collect::<Result<Vec<_>>>()?;
                not_null.push(format!("{ts} IS NOT NULL"));
                format!(
                    "SELECT * FROM {prev} WHERE {} QUALIFY rank() OVER (PARTITION BY {} ORDER BY \
                     {ts} DESC) = 1",
                    not_null.join(" AND "),
                    partition_keys(columns, schema)?.join(", ")
                )
            }
        };
        ctes.push(format!("{me} AS ({body})"));
        prev = me;
    }
    let output = lay
        .output
        .iter()
        .map(|c| ident(c, E))
        .collect::<Result<Vec<_>>>()?;
    let _ = write!(out, "WITH {}\nSELECT {} FROM {prev}", ctes.join(",\n"), output.join(", "));
    if opts.ordered {
        let mut cases = Vec::new();
        if lay.interleaved {
            for (pos, (fi, rg)) in lay.read_order.iter().enumerate() {
                let f = &lay.files[*fi];
                if let Some((a, b)) = f.row_group_range(*rg) {
                    cases.push(format!(
                        "WHEN {FILE} = {} AND {ROW} BETWEEN {a} AND {b} THEN {pos}",
                        string(&f.location)
                    ));
                }
            }
        } else {
            for (pos, f) in lay.files.iter().enumerate() {
                cases.push(format!("WHEN {FILE} = {} THEN {pos}", string(&f.location)));
            }
        }
        if !cases.is_empty() {
            let _ = write!(out, " ORDER BY CASE {} END, {ROW}", cases.join(" "));
        }
    }
    Ok(out)
}
