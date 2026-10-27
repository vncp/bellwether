//! The plan's residual, compiled once for every engine: each canonical
//! encoding decoded to a typed value at its column's dataset type
//! (`bellwether_core::encoding::decode`), and the predicate flattened into
//! the ordered stages every engine runs.
//!
//! A [`Predicate::And`] is a sequence (the reference evaluator folds it
//! left to right), so the stages keep its order exactly: row filters,
//! `NotOlderThan` (a per-key lower bound), `LatestPerKey` (every row at its
//! key's newest timestamp), then `Live`. An engine translates each stage in
//! turn and never reorders across a `LatestPerKey`, whose output depends on
//! the rows that reach it.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use bellwether_core::encoding::{decode, decode_tuple, Scalar};
use bellwether_core::segment::ColumnType;
use bellwether_types::{Predicate, TextOp};
use serde_json::{json, Value as Json};

use crate::{AdapterError, ColumnDef, ResidualMode, Result};

/// A row test.
#[derive(Clone, Debug)]
pub enum Cond {
    /// The column tuple is one of `rows` (`Predicate::In`). A listed tuple
    /// holding a null matches nothing (ADR 014) and is not in `rows`.
    In {
        /// The columns.
        columns: Vec<String>,
        /// Their dataset types.
        types: Vec<DataType>,
        /// The tuples, typed.
        rows: Vec<Vec<Scalar>>,
        /// The tuples' canonical bytes as the plan lists them.
        raw: Vec<Vec<u8>>,
    },
    /// `lo <= column <= hi` (`Predicate::Between`).
    Between {
        /// The column.
        column: String,
        /// Its dataset type.
        data_type: DataType,
        /// Lower bound, inclusive.
        lo: Scalar,
        /// Upper bound, inclusive.
        hi: Scalar,
    },
    /// A text match (`Predicate::Text`): case-folded, a null never matches.
    Text {
        /// The string column.
        column: String,
        /// The match.
        operator: TextOp,
        /// The arguments as given.
        values: Vec<String>,
    },
    /// The expression's value is one of `values` (`Predicate::ExprIn`).
    ExprIn {
        /// The expression in normalized form.
        expr: String,
        /// Its value type.
        data_type: DataType,
        /// The columns it reads.
        inputs: Vec<String>,
        /// The values, typed.
        values: Vec<Scalar>,
        /// Their canonical bytes.
        raw: Vec<Vec<u8>>,
    },
    /// The list (or map, by key) holds one of `values`
    /// (`Predicate::ContainsAny`).
    ContainsAny {
        /// The list or map column.
        column: String,
        /// The element (or map key) type.
        element: DataType,
        /// Whether the column is a map (membership is by key).
        map: bool,
        /// The values, typed.
        values: Vec<Scalar>,
        /// Their canonical bytes.
        raw: Vec<Vec<u8>>,
    },
    /// For every group, the list holds one of the group's values
    /// (`Predicate::ContainsAll`).
    ContainsAll {
        /// The list or map column.
        column: String,
        /// The element (or map key) type.
        element: DataType,
        /// Whether the column is a map.
        map: bool,
        /// The groups, typed.
        groups: Vec<Vec<Scalar>>,
        /// Their canonical bytes.
        raw: Vec<Vec<Vec<u8>>>,
    },
    /// The column is not null and not the tombstone (`Predicate::Live`).
    Live {
        /// The tombstone column.
        column: String,
        /// Its dataset type.
        data_type: DataType,
        /// The value that marks a tombstone.
        tombstone: Scalar,
    },
}

/// One per-key lower bound of a `NotOlderThan`.
#[derive(Clone, Debug)]
pub struct Bound {
    /// The key tuple (a `None` element is a null in the plan's tuple,
    /// which matches no row).
    pub key: Vec<Option<Scalar>>,
    /// The timestamp lower bound.
    pub ts: Scalar,
    /// The key's canonical bytes.
    pub raw_key: Vec<u8>,
}

/// One stage of the residual, in the order an engine applies it.
#[derive(Clone, Debug)]
pub enum Stage {
    /// Keep the rows the test holds for.
    Filter(Cond),
    /// Drop a row whose key is listed and whose timestamp is null or older
    /// than the key's bound; rows of unlisted keys (and rows with a null
    /// key) are kept.
    NotOlderThan {
        /// Key columns.
        columns: Vec<String>,
        /// Their dataset types.
        types: Vec<DataType>,
        /// The timestamp column.
        ts_column: String,
        /// Its dataset type.
        ts_type: DataType,
        /// Per-key bounds, in the plan's order.
        bounds: Vec<Bound>,
    },
    /// Keep every row whose timestamp equals its key's greatest timestamp
    /// among the rows that reach this stage (ties all kept); a row with a
    /// null key column or a null timestamp is dropped.
    LatestPerKey {
        /// Key columns.
        columns: Vec<String>,
        /// The timestamp column.
        ts_column: String,
    },
}

/// The compiled residual.
#[derive(Clone, Debug)]
pub struct Residual {
    /// Which part of the plan's residual this is.
    pub mode: ResidualMode,
    /// The stages, in order.
    pub stages: Vec<Stage>,
}

impl Residual {
    /// Every column a stage reads, deduplicated, in first-use order.
    #[must_use]
    pub fn columns(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        let mut push = |c: &str| {
            if !out.iter().any(|x| x == c) {
                out.push(c.to_string());
            }
        };
        for s in &self.stages {
            match s {
                Stage::Filter(c) => match c {
                    Cond::In { columns, .. } => columns.iter().for_each(|c| push(c)),
                    Cond::Between { column, .. }
                    | Cond::Text { column, .. }
                    | Cond::ContainsAny { column, .. }
                    | Cond::ContainsAll { column, .. }
                    | Cond::Live { column, .. } => push(column),
                    Cond::ExprIn { inputs, .. } => inputs.iter().for_each(|c| push(c)),
                },
                Stage::NotOlderThan {
                    columns, ts_column, ..
                }
                | Stage::LatestPerKey { columns, ts_column } => {
                    columns.iter().for_each(|c| push(c));
                    push(ts_column);
                }
            }
        }
        out
    }

    /// Whether a stage depends on other rows (a ranking or a join), so an
    /// engine does not return rows in read order without sorting.
    #[must_use]
    pub fn reorders(&self) -> bool {
        self.stages.iter().any(|s| !matches!(s, Stage::Filter(_)))
    }

    /// The residual as typed JSON: each stage with its literals typed (see
    /// [`scalar_json`]), in order. The Spark shim reads this form.
    #[must_use]
    pub fn to_json(&self) -> Json {
        let stages: Vec<Json> = self.stages.iter().map(stage_json).collect();
        json!({ "mode": self.mode.as_str(), "stages": stages })
    }
}

/// `dt` without dictionary encoding (the encoding sees decoded values).
#[must_use]
pub fn value_type(dt: &DataType) -> &DataType {
    match dt {
        DataType::Dictionary(_, v) => value_type(v),
        other => other,
    }
}

/// The dataset schema as an Arrow schema (every column nullable).
#[must_use]
pub fn arrow_schema(schema: &[ColumnDef]) -> Schema {
    Schema::new(
        schema
            .iter()
            .map(|c| Field::new(&c.name, c.data_type.clone(), true))
            .collect::<Vec<_>>(),
    )
}

pub(crate) fn column_type<'a>(
    schema: &'a [ColumnDef],
    column: &str,
    context: &str,
) -> Result<&'a DataType> {
    schema
        .iter()
        .find(|c| c.name == column)
        .map(|c| &c.data_type)
        .ok_or_else(|| AdapterError::UnknownColumn {
            column: column.to_string(),
            context: context.to_string(),
        })
}

fn recorded(dt: &DataType, column: &str, what: &str) -> Result<ColumnType> {
    ColumnType::of(dt).map_err(|e| AdapterError::Canonical {
        column: column.to_string(),
        what: what.to_string(),
        reason: e.to_string(),
    })
}

fn decode_one(bytes: &[u8], dt: &DataType, column: &str, what: &str) -> Result<Scalar> {
    let ct = recorded(dt, column, what)?;
    decode(bytes, &ct).map_err(|e| AdapterError::Canonical {
        column: column.to_string(),
        what: what.to_string(),
        reason: e.to_string(),
    })
}

fn decode_key(
    bytes: &[u8],
    types: &[DataType],
    columns: &[String],
    what: &str,
) -> Result<Vec<Option<Scalar>>> {
    let cts = types
        .iter()
        .zip(columns)
        .map(|(t, c)| recorded(t, c, what))
        .collect::<Result<Vec<_>>>()?;
    decode_tuple(bytes, &cts).map_err(|e| AdapterError::Canonical {
        column: columns.join(","),
        what: what.to_string(),
        reason: e.to_string(),
    })
}

fn types_of(schema: &[ColumnDef], columns: &[String], context: &str) -> Result<Vec<DataType>> {
    if columns.is_empty() {
        return Err(AdapterError::Plan(format!("{context} names no column")));
    }
    columns
        .iter()
        .map(|c| column_type(schema, c, context).map(|t| value_type(t).clone()))
        .collect()
}

fn flatten<'a>(p: &'a Predicate, out: &mut Vec<&'a Predicate>) {
    match p {
        Predicate::And { all } => all.iter().for_each(|q| flatten(q, out)),
        other => out.push(other),
    }
}

/// Compiles the plan's residual for `engine` (named in refusals), keeping
/// the part `mode` asks for. `None` compiles to no stage.
#[allow(clippy::too_many_lines)]
pub fn compile(
    residual: Option<&Predicate>,
    schema: &[ColumnDef],
    mode: ResidualMode,
    engine: &'static str,
) -> Result<Residual> {
    let mut flat = Vec::new();
    if let Some(p) = residual {
        flatten(p, &mut flat);
    }
    let mut stages = Vec::with_capacity(flat.len());
    for p in flat {
        let stage = match p {
            Predicate::In { columns, values } => {
                let types = types_of(schema, columns, "In")?;
                let mut rows = Vec::with_capacity(values.len());
                for v in values {
                    let key = decode_key(v, &types, columns, "In list")?;
                    // A tuple holding a null matches nothing (ADR 014).
                    if let Some(row) = key.into_iter().collect::<Option<Vec<_>>>() {
                        rows.push(row);
                    }
                }
                Stage::Filter(Cond::In {
                    columns: columns.clone(),
                    types,
                    rows,
                    raw: values.clone(),
                })
            }
            Predicate::Between { column, lo, hi } => {
                let dt = value_type(column_type(schema, column, "Between")?).clone();
                Stage::Filter(Cond::Between {
                    column: column.clone(),
                    lo: decode_one(lo, &dt, column, "Between lower bound")?,
                    hi: decode_one(hi, &dt, column, "Between upper bound")?,
                    data_type: dt,
                })
            }
            Predicate::LatestPerKey { columns, ts_column } => {
                types_of(schema, columns, "LatestPerKey")?;
                column_type(schema, ts_column, "LatestPerKey timestamp")?;
                Stage::LatestPerKey {
                    columns: columns.clone(),
                    ts_column: ts_column.clone(),
                }
            }
            Predicate::NotOlderThan {
                columns,
                ts_column,
                bounds,
            } => {
                let types = types_of(schema, columns, "NotOlderThan")?;
                let ts_type =
                    value_type(column_type(schema, ts_column, "NotOlderThan timestamp")?).clone();
                let bounds = bounds
                    .iter()
                    .map(|(k, t)| {
                        Ok(Bound {
                            key: decode_key(k, &types, columns, "NotOlderThan key")?,
                            ts: decode_one(t, &ts_type, ts_column, "NotOlderThan bound")?,
                            raw_key: k.clone(),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Stage::NotOlderThan {
                    columns: columns.clone(),
                    types,
                    ts_column: ts_column.clone(),
                    ts_type,
                    bounds,
                }
            }
            Predicate::And { .. } => unreachable!("flattened above"),
            Predicate::Text {
                column,
                operator,
                values,
            } => {
                let dt = value_type(column_type(schema, column, "Text")?);
                if !matches!(
                    dt,
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                ) {
                    return Err(AdapterError::Canonical {
                        column: column.clone(),
                        what: "Text".into(),
                        reason: format!("a text match needs a string column, not {dt}"),
                    });
                }
                Stage::Filter(Cond::Text {
                    column: column.clone(),
                    operator: *operator,
                    values: values.clone(),
                })
            }
            Predicate::ExprIn { expr, values } => {
                let typed = bellwether_core::expr::Typed::parse(expr, &arrow_schema(schema))
                    .map_err(|e| AdapterError::Canonical {
                        column: expr.clone(),
                        what: "ExprIn expression".into(),
                        reason: e.to_string(),
                    })?;
                let dt = typed.data_type().clone();
                let decoded = values
                    .iter()
                    .map(|v| decode_one(v, &dt, expr, "ExprIn value"))
                    .collect::<Result<Vec<_>>>()?;
                Stage::Filter(Cond::ExprIn {
                    expr: expr.clone(),
                    data_type: dt,
                    inputs: typed.input_columns(),
                    values: decoded,
                    raw: values.clone(),
                })
            }
            Predicate::ContainsAny { column, values } => {
                let (element, map) = list_element(schema, column, "ContainsAny")?;
                let decoded = values
                    .iter()
                    .map(|v| decode_one(v, &element, column, "ContainsAny value"))
                    .collect::<Result<Vec<_>>>()?;
                Stage::Filter(Cond::ContainsAny {
                    column: column.clone(),
                    element,
                    map,
                    values: decoded,
                    raw: values.clone(),
                })
            }
            Predicate::ContainsAll { column, values } => {
                let (element, map) = list_element(schema, column, "ContainsAll")?;
                let groups = values
                    .iter()
                    .map(|g| {
                        if g.is_empty() {
                            return Err(AdapterError::Plan(format!(
                                "a ContainsAll group on {column:?} lists no value"
                            )));
                        }
                        g.iter()
                            .map(|v| decode_one(v, &element, column, "ContainsAll value"))
                            .collect::<Result<Vec<_>>>()
                    })
                    .collect::<Result<Vec<_>>>()?;
                Stage::Filter(Cond::ContainsAll {
                    column: column.clone(),
                    element,
                    map,
                    groups,
                    raw: values.clone(),
                })
            }
            Predicate::Live { column, tombstone } => {
                let dt = value_type(column_type(schema, column, "Live")?).clone();
                Stage::Filter(Cond::Live {
                    column: column.clone(),
                    tombstone: decode_one(tombstone, &dt, column, "Live tombstone")?,
                    data_type: dt,
                })
            }
            Predicate::SemiJoin { .. } | Predicate::Join { .. } | Predicate::WindowPartition { .. } => {
                return Err(AdapterError::Unsupported {
                    engine,
                    what: "a relational residual (SemiJoin, Join, WindowPartition)".into(),
                    reason: "belongs to a consumer plan priced by relational replay, not to a \
                             lookup's scan plan"
                        .into(),
                    next: "emit the plan of a locate, latest, or asof lookup".into(),
                })
            }
        };
        stages.push(stage);
    }
    let stages = match mode {
        ResidualMode::Full => stages,
        ResidualMode::KeysOnly => stages
            .into_iter()
            .filter(|s| matches!(s, Stage::Filter(Cond::In { .. })))
            .collect(),
        ResidualMode::None => Vec::new(),
    };
    Ok(Residual { mode, stages })
}

/// The element type of a list column (or the key type of a map column)
/// and whether it is a map.
fn list_element(schema: &[ColumnDef], column: &str, what: &str) -> Result<(DataType, bool)> {
    let dt = value_type(column_type(schema, column, what)?);
    let element = bellwether_core::list::element_type(dt).ok_or_else(|| AdapterError::Canonical {
        column: column.to_string(),
        what: what.to_string(),
        reason: format!("a membership test needs a list or map column, not {dt}"),
    })?;
    Ok((
        value_type(element).clone(),
        matches!(dt, DataType::Map(_, _)),
    ))
}

fn unit_name(u: TimeUnit) -> &'static str {
    match u {
        TimeUnit::Second => "s",
        TimeUnit::Millisecond => "ms",
        TimeUnit::Microsecond => "us",
        TimeUnit::Nanosecond => "ns",
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// A typed literal as JSON, exact for every type: integers, decimals,
/// times, and timestamps as decimal strings of their integer form (with
/// the scale or unit beside), floats as their IEEE bits (and a readable
/// form), binary as hex. `dt` is the column's dataset type.
#[must_use]
pub fn scalar_json(s: &Scalar, dt: &DataType) -> Json {
    let type_name = match value_type(dt) {
        DataType::Boolean => "boolean",
        DataType::Int8 => "int8",
        DataType::Int16 => "int16",
        DataType::Int32 => "int32",
        DataType::Int64 => "int64",
        DataType::UInt8 => "uint8",
        DataType::UInt16 => "uint16",
        DataType::UInt32 => "uint32",
        DataType::UInt64 => "uint64",
        DataType::Float16 => "float16",
        DataType::Float32 => "float32",
        DataType::Float64 => "float64",
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "decimal",
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "utf8",
        DataType::FixedSizeBinary(_) | DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            "binary"
        }
        DataType::Date32 => "date32",
        DataType::Date64 => "date64",
        DataType::Time32(_) | DataType::Time64(_) => "time",
        DataType::Timestamp(_, _) => "timestamp",
        _ => "unknown",
    };
    match s {
        Scalar::Boolean(b) => json!({ "type": type_name, "value": b }),
        Scalar::Int(v) => json!({ "type": type_name, "value": v.to_string() }),
        Scalar::UInt(v) => json!({ "type": type_name, "value": v.to_string() }),
        Scalar::Float(f) => json!({
            "type": type_name,
            "bits": format!("{:016x}", f.to_bits()),
            "value": format!("{f:?}"),
        }),
        Scalar::Decimal128 { value, scale } => {
            let p = match dt {
                DataType::Decimal128(p, _) | DataType::Decimal256(p, _) => *p,
                _ => 38,
            };
            json!({ "type": type_name, "precision": p, "scale": scale, "unscaled": value.to_string() })
        }
        Scalar::Decimal256 { value, scale } => {
            let p = match dt {
                DataType::Decimal128(p, _) | DataType::Decimal256(p, _) => *p,
                _ => 76,
            };
            json!({ "type": type_name, "precision": p, "scale": scale, "unscaled": value.to_string() })
        }
        Scalar::Utf8(v) => json!({ "type": type_name, "value": v }),
        Scalar::Binary(b) => json!({ "type": type_name, "hex": hex(b) }),
        Scalar::Date32(d) => json!({ "type": type_name, "days": d }),
        Scalar::Date64(ms) => json!({ "type": type_name, "ms": ms.to_string() }),
        Scalar::Time { value, unit } => {
            json!({ "type": type_name, "unit": unit_name(*unit), "value": value.to_string() })
        }
        Scalar::Timestamp { value, unit } => {
            let tz = match value_type(dt) {
                DataType::Timestamp(_, tz) => tz.as_deref().map(str::to_string),
                _ => None,
            };
            json!({ "type": type_name, "unit": unit_name(*unit), "tz": tz, "value": value.to_string() })
        }
    }
}

fn cond_json(c: &Cond) -> Json {
    match c {
        Cond::In {
            columns,
            types,
            rows,
            ..
        } => json!({
            "op": "in",
            "columns": columns,
            "rows": rows.iter().map(|r| r.iter().zip(types).map(|(s, t)| scalar_json(s, t)).collect::<Vec<_>>()).collect::<Vec<_>>(),
        }),
        Cond::Between {
            column,
            data_type,
            lo,
            hi,
        } => json!({
            "op": "between",
            "column": column,
            "lo": scalar_json(lo, data_type),
            "hi": scalar_json(hi, data_type),
        }),
        Cond::Text {
            column,
            operator,
            values,
        } => json!({ "op": "text", "column": column, "operator": operator, "values": values }),
        Cond::ExprIn {
            expr,
            data_type,
            values,
            ..
        } => json!({
            "op": "expr_in",
            "expr": expr,
            "values": values.iter().map(|v| scalar_json(v, data_type)).collect::<Vec<_>>(),
        }),
        Cond::ContainsAny {
            column,
            element,
            map,
            values,
            ..
        } => json!({
            "op": "contains_any",
            "column": column,
            "map": map,
            "values": values.iter().map(|v| scalar_json(v, element)).collect::<Vec<_>>(),
        }),
        Cond::ContainsAll {
            column,
            element,
            map,
            groups,
            ..
        } => json!({
            "op": "contains_all",
            "column": column,
            "map": map,
            "groups": groups.iter().map(|g| g.iter().map(|v| scalar_json(v, element)).collect::<Vec<_>>()).collect::<Vec<_>>(),
        }),
        Cond::Live {
            column,
            data_type,
            tombstone,
        } => json!({ "op": "live", "column": column, "tombstone": scalar_json(tombstone, data_type) }),
    }
}

fn stage_json(s: &Stage) -> Json {
    match s {
        Stage::Filter(c) => cond_json(c),
        Stage::NotOlderThan {
            columns,
            types,
            ts_column,
            ts_type,
            bounds,
        } => json!({
            "op": "not_older_than",
            "columns": columns,
            "ts_column": ts_column,
            "bounds": bounds.iter().map(|b| json!({
                "key": b.key.iter().zip(types).map(|(k, t)| k.as_ref().map_or(Json::Null, |k| scalar_json(k, t))).collect::<Vec<_>>(),
                "ts": scalar_json(&b.ts, ts_type),
            })).collect::<Vec<_>>(),
        }),
        Stage::LatestPerKey { columns, ts_column } => json!({
            "op": "latest_per_key",
            "columns": columns,
            "ts_column": ts_column,
            "ties": "all rows at the newest timestamp are kept",
        }),
    }
}

/// The arrow schema as a shared reference (for evaluators that need one).
#[must_use]
pub fn shared_schema(schema: &[ColumnDef]) -> Arc<Schema> {
    Arc::new(arrow_schema(schema))
}
