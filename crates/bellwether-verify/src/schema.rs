//! The shared dataset schema of two sides.
//!
//! Each side encodes its values at its own dataset types (`Source::schema`,
//! the single source of column types, `docs/format.md`). Two sides are
//! comparable on a column only when their dataset types produce the same
//! canonical bytes for the same value: the same [`Class`]. Within a class,
//! types differ only in range or precision, which each side checks against
//! its own dataset type. Anything else is an error, never a pass: to compare
//! across, say, two decimal scales, build one side's source with the other's
//! dataset schema (`with_schema`), and every value is then rescaled exactly
//! or refused.

use std::collections::{BTreeSet, HashMap, HashSet};

use arrow::array::Array;
use arrow::datatypes::{DataType, Schema, TimeUnit};
use bellwether_core::encoding;
use bellwether_types::Predicate;

use crate::error::{Result, VerifyError};
use crate::spec::ReconcileSpec;

/// What a dataset type's canonical encoding depends on (ADR 016): two types
/// of one class encode every value they can both hold to identical bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Class {
    Boolean,
    Int,
    UInt,
    Float,
    /// Width tag (16 or 32 bytes) and scale; precision only bounds values.
    Decimal {
        wide: bool,
        scale: i8,
    },
    Utf8,
    Binary,
    Date32,
    Date64,
    Time(TimeUnit),
    /// The zone is not part of the encoding: values are UTC instants.
    Timestamp(TimeUnit),
}

/// The class of a dataset type, or an error naming why it has none.
pub(crate) fn class(dt: &DataType) -> Result<Class> {
    encoding::check_supported(dt).map_err(|e| VerifyError::Schema(e.to_string()))?;
    Ok(match dt {
        DataType::Dictionary(_, v) => return class(v),
        DataType::Boolean => Class::Boolean,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Class::Int,
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => Class::UInt,
        DataType::Float16 | DataType::Float32 | DataType::Float64 => Class::Float,
        DataType::Decimal128(_, s) => Class::Decimal {
            wide: false,
            scale: *s,
        },
        DataType::Decimal256(_, s) => Class::Decimal {
            wide: true,
            scale: *s,
        },
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Class::Utf8,
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => Class::Binary,
        DataType::Date32 => Class::Date32,
        DataType::Date64 => Class::Date64,
        DataType::Time32(u) | DataType::Time64(u) => Class::Time(*u),
        DataType::Timestamp(u, _) => Class::Timestamp(*u),
        other => return Err(VerifyError::Schema(encoding::no_encoding(other))),
    })
}

/// A total column: a decimal at its dataset scale, or a float quantized at
/// an explicit scale.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TotalCol {
    /// The label totals and certificates name it by: the column, or
    /// `col:dec(S)` when quantized.
    pub name: String,
    /// Index into [`Plan::projection`].
    pub proj: usize,
    /// `true` for Decimal256 (an `i256` total), `false` for Decimal128 and
    /// quantized floats.
    pub wide: bool,
    /// The scale every total is expressed at: the dataset scale, or the
    /// quantization scale.
    pub scale: i8,
    /// Whether this is a float column quantized at `scale`.
    pub quantize: bool,
}

/// The columns a reconcile reads and how it uses them, resolved against
/// both sides' dataset schemas.
#[derive(Clone, Debug)]
pub(crate) struct Plan {
    /// Columns read from every row group, in the left schema's order.
    pub projection: Vec<String>,
    /// Per side, the dataset type of each projected column.
    pub types: [Vec<DataType>; 2],
    /// Projection indices of the key columns, in key order.
    pub key: Vec<usize>,
    /// Projection index of the version column.
    pub latest_by: Option<usize>,
    /// Projection indices of the hashed columns, in the shared order (the
    /// left schema's order minus ignored columns).
    pub hashed: Vec<usize>,
    /// The total columns, in spec order.
    pub totals: Vec<TotalCol>,
    /// Projection indices of the group-by columns, in spec order.
    pub group: Vec<usize>,
    /// Hashed column names, in hash order.
    pub hashed_names: Vec<String>,
    /// Ignored columns, sorted, as recorded on the certificate.
    pub ignored: Vec<String>,
    /// The row filter ([`ReconcileSpec::row_filter`]), over projection
    /// indices: a row is compared when any of these keeps it.
    pub filter: Option<Vec<RowTest>>,
}

/// One residual predicate over projection indices, with its canonical
/// operands ([`ReconcileSpec::row_filter`]).
#[derive(Clone, Debug)]
pub(crate) enum RowTest {
    /// Keeps every row (`LatestPerKey`: the version rule, not a row test).
    All,
    /// The tuple of `cols` is one of `values`.
    In {
        cols: Vec<usize>,
        values: HashSet<Vec<u8>>,
    },
    /// The value of `col` is in `[lo, hi]`.
    Between {
        col: usize,
        lo: Vec<u8>,
        hi: Vec<u8>,
    },
    /// A row whose `cols` tuple has a bound keeps a `ts` value at or above
    /// it; other rows are kept.
    NotOlderThan {
        cols: Vec<usize>,
        ts: usize,
        bounds: HashMap<Vec<u8>, Vec<u8>>,
    },
    /// Every test keeps the row.
    And(Vec<RowTest>),
    /// The string at `col` matches one of `args` (already case-folded)
    /// under `op` (core's `text::matches_any`); a null never matches.
    Text {
        col: usize,
        op: bellwether_types::TextOp,
        args: Vec<String>,
    },
    /// The value of an expression over `cols` (name, projection index), at
    /// the left side's types, is one of `values` (`ExprIn`).
    ExprIn {
        typed: Box<bellwether_core::expr::Typed>,
        cols: Vec<(String, usize)>,
        values: HashSet<Vec<u8>>,
    },
    /// The list in `col` holds an element in `values` (B4).
    ContainsAny {
        col: usize,
        values: HashSet<Vec<u8>>,
    },
    /// The list in `col` holds, for every group, an element in it (B4).
    ContainsAll {
        col: usize,
        groups: Vec<HashSet<Vec<u8>>>,
    },
}

impl RowTest {
    /// The test a residual predicate makes of one row. A relational
    /// predicate (`SemiJoin`, `Join`, `WindowPartition`) is priced by replay
    /// and never a row filter; it is refused.
    fn compile(p: &Predicate, index: &dyn Fn(&str) -> usize, types: &[DataType]) -> Result<Self> {
        let cols = |c: &[String]| c.iter().map(|c| index(c)).collect();
        Ok(match p {
            Predicate::In { columns, values } => Self::In {
                cols: cols(columns),
                values: values.iter().cloned().collect(),
            },
            Predicate::Between { column, lo, hi } => Self::Between {
                col: index(column),
                lo: lo.clone(),
                hi: hi.clone(),
            },
            Predicate::NotOlderThan {
                columns,
                ts_column,
                bounds,
            } => Self::NotOlderThan {
                cols: cols(columns),
                ts: index(ts_column),
                bounds: bounds.iter().cloned().collect(),
            },
            Predicate::LatestPerKey { .. } => Self::All,
            Predicate::And { all } => Self::And(
                all.iter()
                    .map(|p| Self::compile(p, index, types))
                    .collect::<Result<_>>()?,
            ),
            Predicate::ExprIn { expr, values } => {
                let parsed = bellwether_core::expr::Expr::parse(expr)
                    .map_err(|e| VerifyError::InvalidSpec(e.to_string()))?;
                let cols: Vec<(String, usize)> = parsed
                    .columns()
                    .into_iter()
                    .map(|c| {
                        let i = index(&c);
                        (c, i)
                    })
                    .collect();
                let schema = Schema::new(
                    cols.iter()
                        .map(|(c, i)| arrow::datatypes::Field::new(c, types[*i].clone(), true))
                        .collect::<Vec<_>>(),
                );
                let checked = bellwether_core::expr::Typed::new(parsed, &schema)
                    .map_err(|e| VerifyError::InvalidSpec(e.to_string()))?;
                Self::ExprIn {
                    typed: Box::new(checked),
                    cols,
                    values: values.iter().cloned().collect(),
                }
            }
            Predicate::SemiJoin { .. }
            | Predicate::Join { .. }
            | Predicate::WindowPartition { .. } => {
                return Err(VerifyError::InvalidSpec(
                    "a row filter holds only the residual of a lookup (in, between, \
                     not-older-than, latest-per-key, contains-any, contains-all); a semi-join, join, or window partition \
                     is priced by replay and cannot filter the rows a reconciliation compares"
                        .into(),
                ))
            }
            Predicate::Text {
                column,
                operator,
                values,
            } => Self::Text {
                col: index(column),
                op: *operator,
                args: values
                    .iter()
                    .map(|v| bellwether_core::text::fold(v))
                    .collect(),
            },
            Predicate::ContainsAny { column, values } => Self::ContainsAny {
                col: index(column),
                values: values.iter().cloned().collect(),
            },
            Predicate::ContainsAll { column, values } => Self::ContainsAll {
                col: index(column),
                groups: values.iter().map(|g| g.iter().cloned().collect()).collect(),
            },
        })
    }

    /// Whether this test keeps row `row` of `cols` (projected, at `types`);
    /// `buf` is scratch.
    pub(crate) fn keeps(
        &self,
        cols: &[&dyn Array],
        types: &[DataType],
        row: usize,
        buf: &mut Vec<u8>,
    ) -> bellwether_types::Result<bool> {
        // The canonical tuple of `idx` into `buf`; false when one is null.
        let tuple = |idx: &[usize], buf: &mut Vec<u8>| -> bellwether_types::Result<bool> {
            buf.clear();
            for &i in idx {
                if !encoding::encode_array_value_as(cols[i], row, &types[i], buf)? {
                    return Ok(false);
                }
            }
            Ok(true)
        };
        Ok(match self {
            Self::All => true,
            Self::In { cols: idx, values } => tuple(idx, buf)? && values.contains(buf.as_slice()),
            Self::Between { col, lo, hi } => {
                tuple(std::slice::from_ref(col), buf)?
                    && lo.as_slice() <= buf.as_slice()
                    && buf.as_slice() <= hi.as_slice()
            }
            Self::NotOlderThan {
                cols: idx,
                ts,
                bounds,
            } => {
                if !tuple(idx, buf)? {
                    return Ok(true);
                }
                let Some(bound) = bounds.get(buf.as_slice()) else {
                    return Ok(true);
                };
                tuple(std::slice::from_ref(ts), buf)? && buf.as_slice() >= bound.as_slice()
            }
            Self::And(all) => {
                for t in all {
                    if !t.keeps(cols, types, row, buf)? {
                        return Ok(false);
                    }
                }
                true
            }
            Self::Text { col, op, args } => bellwether_core::text::value_at(cols[*col], row)?
                .is_some_and(|v| bellwether_core::text::matches_any(*op, args, v)),
            Self::ExprIn {
                typed,
                cols: inputs,
                values,
            } => {
                let named: Vec<(&str, &dyn Array)> =
                    inputs.iter().map(|(c, i)| (c.as_str(), cols[*i])).collect();
                typed
                    .encode_row(&named, row)?
                    .is_some_and(|v| values.contains(&v))
            }
            Self::ContainsAny { col, values } => {
                let elements = list_elements(cols[*col], &types[*col], row)?;
                elements.iter().any(|e| values.contains(e))
            }
            Self::ContainsAll { col, groups } => {
                let elements = list_elements(cols[*col], &types[*col], row)?;
                groups
                    .iter()
                    .all(|g| elements.iter().any(|e| g.contains(e)))
            }
        })
    }
}

/// The canonical element keys of row `row` of list column `array`, whose
/// dataset type is `dt` (ADR 014: none for a null or empty list).
fn list_elements(
    array: &dyn Array,
    dt: &DataType,
    row: usize,
) -> bellwether_types::Result<Vec<Vec<u8>>> {
    let element = bellwether_core::list::element_type(dt).ok_or_else(|| {
        bellwether_types::Error::KeySpec(format!(
            "a membership test reads a list column; this column has type {dt}"
        ))
    })?;
    bellwether_core::list::row_element_keys(array, row, element)
}

/// The columns `p` reads.
fn predicate_columns(p: &Predicate, out: &mut Vec<String>) {
    match p {
        Predicate::In { columns, .. } => out.extend(columns.iter().cloned()),
        Predicate::Between { column, .. }
        | Predicate::Text { column, .. }
        | Predicate::ContainsAny { column, .. }
        | Predicate::ContainsAll { column, .. } => {
            out.push(column.clone());
        }
        Predicate::NotOlderThan {
            columns, ts_column, ..
        } => {
            out.extend(columns.iter().cloned());
            out.push(ts_column.clone());
        }
        Predicate::And { all } => {
            for q in all {
                predicate_columns(q, out);
            }
        }
        // An expression that does not parse reads nothing here; compiling
        // the filter refuses it.
        Predicate::ExprIn { expr, .. } => {
            out.extend(
                bellwether_core::expr::Expr::parse(expr)
                    .map(|e| e.columns())
                    .unwrap_or_default(),
            );
        }
        // Join and window predicates are refused by `RowTest::compile`;
        // they name no column a row test reads.
        Predicate::LatestPerKey { .. }
        | Predicate::SemiJoin { .. }
        | Predicate::Join { .. }
        | Predicate::WindowPartition { .. } => {}
    }
}

impl Plan {
    /// Resolves `spec` against the left and right dataset schemas.
    pub(crate) fn resolve(spec: &ReconcileSpec, left: &Schema, right: &Schema) -> Result<Self> {
        no_duplicate_names("left", left)?;
        no_duplicate_names("right", right)?;
        let ignored: BTreeSet<&String> = spec.ignored_columns.iter().collect();
        let hashed_names = compared_columns(left, right, &ignored)?;
        let projection = projection(spec, left, right, &hashed_names)?;
        let list_only = filter_only_columns(spec, &hashed_names);
        let types = side_types(&projection, left, right, &list_only)?;
        let index = |c: &str| -> usize {
            projection
                .iter()
                .position(|p| p == c)
                .unwrap_or_else(|| unreachable!("every role column is projected"))
        };
        let latest_by = match &spec.latest_by {
            Some(c) => {
                let i = index(c);
                encoding::check_ts_type(&types[0][i]).map_err(|_| {
                    VerifyError::Schema(format!(
                        "latest_by column {c:?} of type {} cannot order versions; it must be a \
                         timestamp, date, time, or integer",
                        types[0][i]
                    ))
                })?;
                Some(i)
            }
            None => None,
        };
        let mut totals = Vec::with_capacity(spec.total_columns.len());
        for t in &spec.total_columns {
            let c = &t.column;
            let i = index(c);
            let (wide, scale) = match (class(&types[0][i])?, t.quantize) {
                (Class::Decimal { wide, scale }, None) => (wide, scale),
                (Class::Float, None) => {
                    return Err(VerifyError::InvalidSpec(format!(
                        "total column {c:?} is {}, and totals are exact; quantize it at an \
                         explicit scale, for example {c}:dec(4) (each value rounded half to \
                         even at 4 decimal places, then summed exactly)",
                        types[0][i]
                    )))
                }
                (Class::Float, Some(s)) => (
                    false,
                    i8::try_from(s).map_err(|_| {
                        VerifyError::InvalidSpec(format!("{}: scale out of range", t.label()))
                    })?,
                ),
                (Class::Decimal { .. }, Some(_)) => {
                    return Err(VerifyError::InvalidSpec(format!(
                        "total column {c:?} is {}, a decimal summed exactly at its own scale; \
                         drop :dec(..) from {}",
                        types[0][i],
                        t.label()
                    )))
                }
                _ => {
                    return Err(VerifyError::InvalidSpec(format!(
                        "total column {c:?} is {}, not a decimal or a float; totals are exact \
                         decimal sums",
                        types[0][i]
                    )))
                }
            };
            totals.push(TotalCol {
                name: t.label(),
                proj: i,
                wide,
                scale,
                quantize: t.quantize.is_some(),
            });
        }
        let filter = spec
            .row_filter
            .as_ref()
            .map(|residuals| {
                residuals
                    .iter()
                    .map(|p| RowTest::compile(p, &index, &types[0]))
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?;
        Ok(Self {
            key: spec.key_columns.iter().map(|c| index(c)).collect(),
            latest_by,
            hashed: hashed_names.iter().map(|c| index(c)).collect(),
            totals,
            group: spec.group_by.iter().map(|c| index(c)).collect(),
            hashed_names,
            ignored: ignored.into_iter().cloned().collect(),
            filter,
            projection,
            types,
        })
    }

    /// The dataset types of the key columns on `side`.
    pub(crate) fn key_types(&self, side: usize) -> Vec<DataType> {
        self.key
            .iter()
            .map(|&i| self.types[side][i].clone())
            .collect()
    }

    /// The dataset types of the group-by columns on `side`.
    pub(crate) fn group_types(&self, side: usize) -> Vec<DataType> {
        self.group
            .iter()
            .map(|&i| self.types[side][i].clone())
            .collect()
    }
}

/// The hashed columns: the left schema's columns minus ignored ones, each
/// of which must exist on the right, and no right column left over.
fn compared_columns(
    left: &Schema,
    right: &Schema,
    ignored: &BTreeSet<&String>,
) -> Result<Vec<String>> {
    for c in ignored {
        if left.field_with_name(c).is_err() && right.field_with_name(c).is_err() {
            return Err(VerifyError::InvalidSpec(format!(
                "ignored column {c:?} is in neither dataset"
            )));
        }
    }
    let one_sided = |name: &str, side: &str| {
        VerifyError::Schema(format!(
            "column {name:?} is present on the {side} only; add it to ignored_columns to leave \
             it out of the comparison"
        ))
    };
    let mut hashed = Vec::new();
    for f in left.fields() {
        if ignored.contains(f.name()) {
            continue;
        }
        if right.field_with_name(f.name()).is_err() {
            return Err(one_sided(f.name(), "left"));
        }
        hashed.push(f.name().clone());
    }
    for f in right.fields() {
        if !ignored.contains(f.name()) && left.field_with_name(f.name()).is_err() {
            return Err(one_sided(f.name(), "right"));
        }
    }
    Ok(hashed)
}

/// The columns to read: the hashed ones plus every column the spec uses by
/// role (the version, totals, groups, and row filter may be ignored
/// columns), in the left schema's order. Role columns must exist on both
/// sides.
fn projection(
    spec: &ReconcileSpec,
    left: &Schema,
    right: &Schema,
    hashed: &[String],
) -> Result<Vec<String>> {
    let mut needed: HashSet<&str> = hashed.iter().map(String::as_str).collect();
    let mut filtered: Vec<String> = Vec::new();
    for p in spec.row_filter.iter().flatten() {
        predicate_columns(p, &mut filtered);
    }
    // The columns an expression-keyed workload's expression reads.
    if let Some(e) = &spec.keys_of {
        filtered.extend(
            bellwether_core::expr::Expr::parse(e)
                .map_err(|e| VerifyError::InvalidSpec(e.to_string()))?
                .columns(),
        );
    }
    for c in spec
        .key_columns
        .iter()
        .chain(spec.latest_by.iter())
        .chain(spec.total_columns.iter().map(|t| &t.column))
        .chain(&spec.group_by)
        .chain(filtered.iter())
    {
        for (side, schema) in [("left", left), ("right", right)] {
            if schema.field_with_name(c).is_err() {
                return Err(VerifyError::Schema(format!(
                    "column {c:?} is not in the {side} dataset"
                )));
            }
        }
        needed.insert(c.as_str());
    }
    Ok(left
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .filter(|n| needed.contains(n.as_str()))
        .collect())
}

/// The columns only the row filter reads: never hashed, keyed, versioned,
/// totalled, or grouped by. A list column may be read this way, to test
/// membership (a list key's residual, B4); its two sides must then share an
/// element class.
fn filter_only_columns(spec: &ReconcileSpec, hashed: &[String]) -> BTreeSet<String> {
    let mut filtered: Vec<String> = Vec::new();
    for p in spec.row_filter.iter().flatten() {
        predicate_columns(p, &mut filtered);
    }
    filtered
        .into_iter()
        .filter(|c| {
            !hashed.contains(c)
                && !spec.key_columns.contains(c)
                && spec.latest_by.as_ref() != Some(c)
                && !spec.total_columns.iter().any(|t| &t.column == c)
                && !spec.group_by.contains(c)
        })
        .collect()
}

/// Each side's dataset type per projected column, which must share a class.
/// A column in `list_only` (read only by a membership test) may instead be
/// a list whose element types share a class on both sides.
fn side_types(
    projection: &[String],
    left: &Schema,
    right: &Schema,
    list_only: &BTreeSet<String>,
) -> Result<[Vec<DataType>; 2]> {
    let mut types: [Vec<DataType>; 2] = [Vec::new(), Vec::new()];
    for name in projection {
        let lt = left
            .field_with_name(name)
            .map_err(|e| VerifyError::Schema(e.to_string()))?
            .data_type();
        let rt = right
            .field_with_name(name)
            .map_err(|e| VerifyError::Schema(e.to_string()))?
            .data_type();
        let of = |dt: &DataType| -> Result<Class> {
            match bellwether_core::list::element_type(dt) {
                Some(e) if list_only.contains(name) => class(e),
                _ => class(dt),
            }
        };
        let lc = of(lt).map_err(|e| unsupported(name, lt, &e))?;
        let rc = of(rt).map_err(|e| unsupported(name, rt, &e))?;
        if lc != rc {
            return Err(VerifyError::Schema(format!(
                "column {name:?} is {lt} on the left and {rt} on the right, which do not share a \
                 canonical encoding; read both sides under one dataset schema giving it a type \
                 both convert to exactly"
            )));
        }
        types[0].push(lt.clone());
        types[1].push(rt.clone());
    }
    Ok(types)
}

fn unsupported(name: &str, dt: &DataType, e: &VerifyError) -> VerifyError {
    VerifyError::Schema(format!(
        "column {name:?} has type {dt}, which cannot be compared ({e}); add it to \
         ignored_columns to leave it out"
    ))
}

fn no_duplicate_names(side: &str, schema: &Schema) -> Result<()> {
    let mut seen = HashSet::new();
    for f in schema.fields() {
        if !seen.insert(f.name()) {
            return Err(VerifyError::Schema(format!(
                "the {side} dataset has two columns named {:?}",
                f.name()
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;

    fn schema(fields: &[(&str, DataType)]) -> Schema {
        Schema::new(
            fields
                .iter()
                .map(|(n, t)| Field::new(*n, t.clone(), true))
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn classes_ignore_range_and_zone_but_not_scale_or_unit() {
        assert_eq!(
            class(&DataType::Int32).unwrap(),
            class(&DataType::Int64).unwrap()
        );
        assert_ne!(
            class(&DataType::Int64).unwrap(),
            class(&DataType::UInt64).unwrap()
        );
        assert_eq!(
            class(&DataType::Decimal128(10, 2)).unwrap(),
            class(&DataType::Decimal128(38, 2)).unwrap()
        );
        assert_ne!(
            class(&DataType::Decimal128(10, 2)).unwrap(),
            class(&DataType::Decimal128(10, 3)).unwrap()
        );
        assert_ne!(
            class(&DataType::Decimal128(38, 2)).unwrap(),
            class(&DataType::Decimal256(38, 2)).unwrap()
        );
        assert_eq!(
            class(&DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap(),
            class(&DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into())
            ))
            .unwrap()
        );
        assert_ne!(
            class(&DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap(),
            class(&DataType::Timestamp(TimeUnit::Millisecond, None)).unwrap()
        );
        assert_eq!(
            class(&DataType::Utf8).unwrap(),
            class(&DataType::LargeUtf8).unwrap()
        );
        assert!(class(&DataType::Duration(TimeUnit::Second)).is_err());
    }

    #[test]
    fn hash_order_is_the_left_schema_order_minus_ignored() {
        let l = schema(&[
            ("b", DataType::Int64),
            ("a", DataType::Utf8),
            ("x", DataType::Int64),
        ]);
        let r = schema(&[
            ("a", DataType::Utf8),
            ("x", DataType::Int64),
            ("b", DataType::Int64),
        ]);
        let spec = ReconcileSpec::new(["a"]).ignore(["x"]);
        let plan = Plan::resolve(&spec, &l, &r).unwrap();
        assert_eq!(plan.hashed_names, ["b", "a"]);
        assert_eq!(plan.ignored, ["x"]);
    }

    #[test]
    fn one_sided_and_incompatible_columns_are_errors() {
        let l = schema(&[("id", DataType::Int64), ("extra", DataType::Int64)]);
        let r = schema(&[("id", DataType::Int64)]);
        let spec = ReconcileSpec::new(["id"]);
        assert!(matches!(
            Plan::resolve(&spec, &l, &r),
            Err(VerifyError::Schema(m)) if m.contains("left only")
        ));
        assert!(matches!(
            Plan::resolve(&spec, &r, &l),
            Err(VerifyError::Schema(m)) if m.contains("right only")
        ));
        // Ignoring a one-sided column is allowed.
        Plan::resolve(&spec.clone().ignore(["extra"]), &l, &r).unwrap();
        let l = schema(&[("id", DataType::Int64), ("v", DataType::Decimal128(18, 2))]);
        let r = schema(&[("id", DataType::Int64), ("v", DataType::Decimal128(18, 4))]);
        assert!(matches!(
            Plan::resolve(&spec, &l, &r),
            Err(VerifyError::Schema(m)) if m.contains("canonical encoding")
        ));
    }
}
